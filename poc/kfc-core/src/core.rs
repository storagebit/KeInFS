// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! `FsCore` — the portable, async filesystem engine.
//!
//! Every operation is an `async fn` returning `Result<_, FsErrno>`. The core
//! holds NO transport types and NO `#[cfg(target_os)]`. A transport drives it
//! by spawning a task per FUSE request that awaits one of these methods and
//! completes the kernel reply — so the dispatch thread never blocks (the old
//! `sync_channel(1).recv()` bridge at `poc/kfc/src/mount.rs:197-202` is gone)
//! and concurrent requests run in parallel over the sharded state.

use crate::coherence::{decode_event, CoherenceSink, NoopSink};
use crate::error::FsErrno;
use crate::metadata::{DynError, MetadataClient};
use crate::object::ObjectEngine;
use crate::state::{FileHandle, FsTables, Inode, InodeState};
use crate::stripe_cache::{StripeCache, DEFAULT_STRIPE_CACHE_BUDGET_BYTES};
use crate::types::{Attr, Capabilities, DesiredKernelConfig, DirEntry, FileKind, OpenedFile, ROOT_INO};
use keinctl::proto::{MetadataInvalidationEvent, NamespaceDomainEntry, NamespaceEntryKind};
use ksc::client::CompletionMode;
use ksc::object::ObjectClientOptions;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

/// Attribute/entry TTL handed to the kernel. Kept at 1s for Phase-1 parity with
/// the original client; Phase 2 (task #9) raises this to ~30s when NATS
/// coherence is active and the kernel notifier is wired.
const ATTR_TTL: Duration = Duration::from_secs(1);
/// In-process metadata freshness window (size + child-list).
const META_CACHE_TTL: Duration = Duration::from_secs(1);
/// Object-client pool sizing, matching the original bounds.
const OBJECT_CLIENT_POOL_MIN: usize = 4;
const OBJECT_CLIENT_POOL_MAX: usize = 16;
/// Page size advertised to the kernel; the negotiated `max_write` is sized to
/// match so the kernel issues large I/O.
const ADVERTISED_BLKSIZE: u32 = 1 << 20;

/// Construction parameters for [`FsCore`].
#[derive(Clone, Debug)]
pub struct FsConfig {
    pub kms_endpoints: Vec<String>,
    pub namespace_id: String,
    pub bucket_id: String,
    pub read_completion_mode: CompletionMode,
    pub write_completion_mode: CompletionMode,
    pub metadata_notification_nats_url: Option<String>,
    pub metadata_notification_subject: String,
}

/// The portable filesystem core.
pub struct FsCore {
    namespace_id: String,
    bucket_id: String,
    coherent_data_cache: bool,
    metadata: MetadataClient,
    objects: ObjectEngine,
    tables: FsTables,
    uid: u32,
    gid: u32,
    desired_kernel_config: DesiredKernelConfig,
    capabilities: RwLock<Capabilities>,
    sink: RwLock<Arc<dyn CoherenceSink>>,
    nats_url: Option<String>,
    nats_subject: String,
    /// Tier-B RAM stripe cache for the ranged read path (kills sequential-read
    /// re-fetch amplification; see [`StripeCache`]).
    stripe_cache: StripeCache,
}

impl FsCore {
    /// Connect to KMS + the KSC object engine, seed the root inode, and return a
    /// shared core. The NATS coherence loop is started separately by
    /// [`FsCore::spawn_coherence_loop`] once a transport sink is installed.
    pub async fn connect(config: FsConfig) -> Result<Arc<Self>, DynError> {
        let object_options = ObjectClientOptions {
            read_completion_mode: config.read_completion_mode,
            write_completion_mode: config.write_completion_mode,
            metadata_notification_nats_url: config.metadata_notification_nats_url.clone(),
            metadata_notification_subject: config.metadata_notification_subject.clone(),
            ..ObjectClientOptions::default()
        };
        let pool_size = object_client_pool_size();
        let metadata = MetadataClient::connect(&config.kms_endpoints).await?;
        let objects =
            ObjectEngine::connect(&config.kms_endpoints, object_options, pool_size).await?;
        let bucket_entry_id = metadata.bucket_entry_id(&config.bucket_id).await?;

        let tables = FsTables::new();
        let root = Inode::new(InodeState {
            ino: ROOT_INO,
            parent: ROOT_INO,
            name: String::new(),
            entry_id: bucket_entry_id,
            key: String::new(),
            kind: FileKind::Directory,
            size: 0,
            size_loaded_at: Some(Instant::now()),
            children_loaded_at: None,
        });
        tables.insert_inode(root);

        // SAFETY: getuid/getgid are always-succeed syscalls with no preconditions.
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };

        Ok(Arc::new(Self {
            coherent_data_cache: config.metadata_notification_nats_url.is_some(),
            namespace_id: config.namespace_id,
            bucket_id: config.bucket_id,
            metadata,
            objects,
            tables,
            uid,
            gid,
            desired_kernel_config: DesiredKernelConfig::default(),
            capabilities: RwLock::new(Capabilities::default()),
            sink: RwLock::new(Arc::new(NoopSink)),
            nats_url: config.metadata_notification_nats_url,
            nats_subject: config.metadata_notification_subject,
            stripe_cache: StripeCache::new(DEFAULT_STRIPE_CACHE_BUDGET_BYTES),
        }))
    }

    // ----- transport wiring -------------------------------------------------

    /// The kernel-config the transport should try to negotiate in `init`.
    pub fn desired_kernel_config(&self) -> DesiredKernelConfig {
        self.desired_kernel_config
    }

    /// Record what the kernel actually granted (called by the transport after
    /// `init`). The core sizes future windows to this.
    pub fn set_capabilities(&self, caps: Capabilities) {
        *self.capabilities.write().expect("caps lock poisoned") = caps;
    }

    pub fn capabilities(&self) -> Capabilities {
        *self.capabilities.read().expect("caps lock poisoned")
    }

    /// Install the transport's kernel-cache invalidation sink.
    pub fn set_coherence_sink(&self, sink: Arc<dyn CoherenceSink>) {
        *self.sink.write().expect("sink lock poisoned") = sink;
    }

    /// True when out-of-band coherence (NATS) is configured.
    pub fn is_coherent(&self) -> bool {
        self.coherent_data_cache
    }

    /// Start the NATS invalidation loop if a URL was configured. Returns
    /// immediately; the loop runs on the provided runtime handle.
    pub fn spawn_coherence_loop(self: &Arc<Self>, handle: &tokio::runtime::Handle) {
        let Some(url) = self.nats_url.clone() else {
            return;
        };
        let subject = self.nats_subject.clone();
        let core = Arc::clone(self);
        handle.spawn(async move {
            core.coherence_loop(url, subject).await;
        });
    }

    async fn coherence_loop(self: Arc<Self>, url: String, subject: String) {
        use futures_util::StreamExt;
        let client = match async_nats::connect(url.as_str()).await {
            Ok(client) => client,
            Err(err) => {
                eprintln!("KFC coherence: cannot connect to NATS {url}: {err}");
                return;
            }
        };
        let mut sub = match client.subscribe(subject.clone()).await {
            Ok(sub) => sub,
            Err(err) => {
                eprintln!("KFC coherence: cannot subscribe to {subject}: {err}");
                return;
            }
        };
        while let Some(message) = sub.next().await {
            if let Some(event) = decode_event(message.payload.as_ref()) {
                self.apply_invalidation(event);
            }
        }
    }

    // ----- attribute helpers ------------------------------------------------

    fn node_attr(&self, node: &InodeState) -> Attr {
        let now = SystemTime::now();
        Attr {
            ino: node.ino,
            size: node.size,
            kind: node.kind,
            perm: match node.kind {
                FileKind::Directory => 0o755,
                FileKind::RegularFile => 0o644,
            },
            nlink: match node.kind {
                FileKind::Directory => 2,
                FileKind::RegularFile => 1,
            },
            uid: self.uid,
            gid: self.gid,
            blksize: ADVERTISED_BLKSIZE,
            atime: now,
            mtime: now,
            ctime: now,
        }
    }

    /// TTL the transport should attach to entry/attr replies.
    pub fn attr_ttl(&self) -> Duration {
        ATTR_TTL
    }

    fn fresh(loaded_at: Option<Instant>) -> bool {
        loaded_at
            .map(|at| at.elapsed() <= META_CACHE_TTL)
            .unwrap_or(false)
    }

    fn child_key(parent_key: &str, name: &str) -> String {
        if parent_key.is_empty() {
            name.to_string()
        } else {
            format!("{parent_key}/{name}")
        }
    }

    fn collection_entry_id(parent_key: &str, name: &str) -> String {
        let key = Self::child_key(parent_key, name);
        format!("kfc:collection:{}", key.replace('/', ":"))
    }

    fn ensure_dir(node: &InodeState) -> Result<(), FsErrno> {
        if matches!(node.kind, FileKind::Directory) {
            Ok(())
        } else {
            Err(FsErrno::NotDir)
        }
    }

    fn snapshot(&self, ino: u64) -> Result<InodeState, FsErrno> {
        self.tables
            .inode(ino)
            .map(|inode| inode.snapshot())
            .ok_or(FsErrno::NoEntry)
    }

    // ----- namespace population ---------------------------------------------

    /// Insert-or-update a child inode under `parent`. Returns its snapshot.
    /// Mirrors `populate_child` in the original (`mount.rs:514`), including the
    /// re-link-by-key and re-link-by-entry coherence paths.
    fn populate_child(&self, parent: &InodeState, entry: NamespaceDomainEntry) -> InodeState {
        let kind = match NamespaceEntryKind::try_from(entry.kind)
            .unwrap_or(NamespaceEntryKind::Unspecified)
        {
            NamespaceEntryKind::Object => FileKind::RegularFile,
            _ => FileKind::Directory,
        };
        let key = Self::child_key(&parent.key, &entry.name);

        // Already known by entry_id? This is the dominant steady-state path:
        // once a directory is listed, every child has a by_entry mapping, so
        // re-listings after the meta-cache TTL expires land here. It must adopt
        // the denormalized listing size exactly like the by_key branch below,
        // otherwise the freshly listed size_bytes is silently discarded and
        // readdirplus keeps reporting a stale (or zero) size for the child.
        if let Some(existing) = self.tables.by_entry.get(&entry.entry_id).map(|e| *e) {
            if let Some(inode) = self.tables.inode(existing) {
                let mut s = inode.state.write().expect("inode lock poisoned");
                // Adopt the denormalized listing size when present; a nonzero
                // object size is authoritative (skip the lazy resolve).
                if matches!(kind, FileKind::RegularFile) && entry.size_bytes > 0 {
                    s.size = entry.size_bytes;
                    s.size_loaded_at = Some(Instant::now());
                } else if matches!(kind, FileKind::RegularFile)
                    && !Self::fresh(s.size_loaded_at)
                    && !s.entry_id.starts_with("pending:")
                {
                    // size_bytes == 0 (pre-change/back-compat object) AND the
                    // inode's size is not already fresh/authoritative: keep the
                    // last-known size and re-arm the lazy resolve. Guarding on
                    // freshness preserves a size just set by create()/write()/
                    // truncate()/commit() — a stat of a just-created, not-yet-
                    // flushed empty file must NOT re-arm a resolve (its entry_id
                    // is still "pending:<key>" and resolve_object_size would error
                    // with no manifest). Genuinely-stale 0s still re-resolve.
                    s.size_loaded_at = None;
                }
                return s.clone();
            }
        }
        // Known by key (e.g. entry_id changed via delete+recreate)? Reuse the
        // by_key inode ONLY when both the existing inode and the incoming entry
        // are RegularFile. Reusing across a kind change would retype a live inode
        // in place (corrupting an open fd's kind/size); when kinds disagree, fall
        // through to a fresh alloc so each kind keeps its own identity.
        if let Some(existing) = self.tables.by_key.get(&key).map(|e| *e) {
            if let Some(inode) = self.tables.inode(existing) {
                let reuse = matches!(
                    inode.state.read().expect("inode lock poisoned").kind,
                    FileKind::RegularFile
                ) && matches!(kind, FileKind::RegularFile);
                if reuse {
                    let snapshot = {
                        let mut s = inode.state.write().expect("inode lock poisoned");
                        // Drop a stale by_entry mapping (e.g. the "pending:<key>"
                        // alias create() installed) before relinking to the real
                        // entry_id; otherwise remove_inode (which only clears the
                        // CURRENT entry_id) leaks the old alias, which could later
                        // resolve to a reused inode.
                        let old_entry_id = std::mem::replace(&mut s.entry_id, entry.entry_id.clone());
                        if !old_entry_id.is_empty() && old_entry_id != entry.entry_id {
                            self.tables.by_entry.remove(&old_entry_id);
                        }
                        s.parent = parent.ino;
                        s.name = entry.name.clone();
                        s.kind = kind;
                        // Adopt the denormalized listing size when present; a nonzero
                        // object size is authoritative (skip the lazy resolve).
                        if entry.size_bytes > 0 {
                            s.size = entry.size_bytes;
                            s.size_loaded_at = Some(Instant::now());
                        } else if !Self::fresh(s.size_loaded_at) {
                            // size_bytes == 0 and not freshly authoritative:
                            // re-arm the lazy resolve (back-compat object).
                            s.size_loaded_at = None;
                        }
                        s.children_loaded_at = None;
                        s.clone()
                    };
                    self.tables.by_entry.insert(entry.entry_id, existing);
                    return snapshot;
                }
            }
        }
        // Brand new. KMS denormalizes the object byte length into the listing
        // (size_bytes), so readdirplus reports the real size with no per-file
        // ResolveObjectRead. A nonzero size on an object is authoritative ->
        // mark it fresh so getattr/refresh_file_size skip the resolve.
        // size_bytes == 0 (dir, or an object committed before this field
        // existed) keeps size_loaded_at: None so refresh_file_size still
        // resolves lazily (back-compat).
        let is_file = matches!(kind, FileKind::RegularFile);
        let size_loaded_at = if is_file && entry.size_bytes > 0 {
            Some(Instant::now())
        } else {
            None
        };
        // Defensively zero the size for non-files so a directory can never carry
        // a bogus size locally, regardless of what KMS puts in size_bytes.
        let size = if is_file { entry.size_bytes } else { 0 };
        let ino = self.tables.alloc_ino();
        let state = InodeState {
            ino,
            parent: parent.ino,
            name: entry.name.clone(),
            entry_id: entry.entry_id,
            key,
            kind,
            size,
            size_loaded_at,
            children_loaded_at: None,
        };
        let snapshot = state.clone();
        self.tables.insert_inode(Inode::new(state));
        snapshot
    }

    async fn refresh_file_size(&self, ino: u64) -> Result<InodeState, FsErrno> {
        let node = self.snapshot(ino)?;
        if !matches!(node.kind, FileKind::RegularFile) || Self::fresh(node.size_loaded_at) {
            return Ok(node);
        }
        let size = self
            .metadata
            .resolve_object_size(&self.bucket_id, &node.key)
            .await
            .map_err(|err| crate::error::classify(&*err))?;
        if let Some(inode) = self.tables.inode(ino) {
            let mut s = inode.state.write().expect("inode lock poisoned");
            s.size = size;
            s.size_loaded_at = Some(Instant::now());
            return Ok(s.clone());
        }
        Err(FsErrno::NoEntry)
    }

    fn prune_missing_children(&self, parent_ino: u64, live_entry_ids: &HashSet<String>) {
        let open_inos: HashSet<u64> = self
            .tables
            .handles
            .iter()
            .map(|h| h.value().ino)
            .collect();
        let stale: Vec<u64> = self
            .tables
            .by_ino
            .iter()
            .filter_map(|entry| {
                let s = entry.value().snapshot();
                if s.parent == parent_ino
                    && s.ino != parent_ino
                    && !live_entry_ids.contains(&s.entry_id)
                    && !open_inos.contains(&s.ino)
                {
                    Some(s.ino)
                } else {
                    None
                }
            })
            .collect();
        for ino in stale {
            self.tables.remove_inode(ino);
        }
    }

    async fn list_children(&self, parent: &InodeState) -> Result<Vec<InodeState>, FsErrno> {
        Self::ensure_dir(parent)?;
        if Self::fresh(parent.children_loaded_at) {
            let mut nodes: Vec<InodeState> = self
                .tables
                .by_ino
                .iter()
                .filter_map(|entry| {
                    let s = entry.value().snapshot();
                    (s.parent == parent.ino && s.ino != parent.ino).then_some(s)
                })
                .collect();
            nodes.sort_by(|a, b| a.name.cmp(&b.name));
            return Ok(nodes);
        }
        let entries = self
            .metadata
            .list_children_all(&self.namespace_id, &parent.entry_id, 256)
            .await
            .map_err(|err| crate::error::classify(&*err))?;
        let live: std::collections::HashSet<String> =
            entries.iter().map(|e| e.entry_id.clone()).collect();
        self.prune_missing_children(parent.ino, &live);
        let mut nodes = Vec::with_capacity(entries.len());
        for entry in entries {
            nodes.push(self.populate_child(parent, entry));
        }
        nodes.sort_by(|a, b| a.name.cmp(&b.name));
        if let Some(inode) = self.tables.inode(parent.ino) {
            inode
                .state
                .write()
                .expect("inode lock poisoned")
                .children_loaded_at = Some(Instant::now());
        }
        Ok(nodes)
    }

    // ----- public FUSE operations -------------------------------------------

    pub async fn lookup(&self, parent: u64, name: &str) -> Result<Attr, FsErrno> {
        let parent_node = self.snapshot(parent)?;
        let children = self.list_children(&parent_node).await?;
        let child = children
            .into_iter()
            .find(|c| c.name == name)
            .ok_or(FsErrno::NoEntry)?;
        let child = if matches!(child.kind, FileKind::RegularFile) {
            self.refresh_file_size(child.ino).await?
        } else {
            child
        };
        Ok(self.node_attr(&child))
    }

    pub async fn getattr(&self, ino: u64) -> Result<Attr, FsErrno> {
        let node = self.snapshot(ino)?;
        let node = if matches!(node.kind, FileKind::RegularFile) {
            self.refresh_file_size(ino).await?
        } else {
            node
        };
        Ok(self.node_attr(&node))
    }

    /// setattr. Phase 1 keeps the original posture: chmod/chown is rejected
    /// (EPERM) under DefaultPermissions; only truncate (size) is honored.
    /// Re-examined in task #15 (POSIX gaps).
    pub async fn setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        fh: Option<u64>,
    ) -> Result<Attr, FsErrno> {
        if mode.is_some() || uid.is_some() || gid.is_some() {
            return Err(FsErrno::Perm);
        }
        if let Some(size) = size {
            return self.truncate(ino, size, fh).await;
        }
        Ok(self.node_attr(&self.snapshot(ino)?))
    }

    async fn truncate(&self, ino: u64, size: u64, fh: Option<u64>) -> Result<Attr, FsErrno> {
        let node = self.snapshot(ino)?;
        if !matches!(node.kind, FileKind::RegularFile) {
            return Err(FsErrno::IsDir);
        }
        let size = usize::try_from(size).map_err(|_| FsErrno::TooBig)?;
        if let Some(fh) = fh {
            self.truncate_handle(ino, fh, size)?;
        } else {
            // No open handle: load the current contents, resize, commit, drop.
            // (Loading first matters: truncating to a smaller non-zero size must
            // preserve the surviving prefix — opening empty would zero it.)
            let fh = self.open_staged_handle(&node, false).await?;
            let truncate_result = self.truncate_handle(ino, fh, size);
            let commit_result = match truncate_result {
                Ok(()) => self.commit_handle(fh).await,
                Err(e) => Err(e),
            };
            self.tables.handles.remove(&fh);
            commit_result?;
        }
        Ok(self.node_attr(&self.snapshot(ino)?))
    }

    fn truncate_handle(&self, ino: u64, fh: u64, size: usize) -> Result<(), FsErrno> {
        let handle = self.tables.handle(fh).ok_or(FsErrno::BadHandle)?;
        handle
            .staged_buffer()
            .ok_or(FsErrno::BadHandle)?
            .lock()
            .expect("handle lock poisoned")
            .set_len(size);
        handle.set_dirty(true);
        if let Some(inode) = self.tables.inode(ino) {
            let mut s = inode.state.write().expect("inode lock poisoned");
            s.size = size as u64;
            s.size_loaded_at = Some(Instant::now());
        }
        Ok(())
    }

    pub async fn opendir(&self, ino: u64) -> Result<(), FsErrno> {
        Self::ensure_dir(&self.snapshot(ino)?)
    }

    /// Create a directory by persisting a real collection namespace entry as a
    /// child of `parent`'s namespace entry. The recursive child listing surfaces
    /// it naturally on the next `list_children`.
    pub async fn mkdir(&self, parent: u64, name: &str, _mode: u32) -> Result<Attr, FsErrno> {
        let parent_node = self.snapshot(parent)?;
        Self::ensure_dir(&parent_node)?;
        if self
            .list_children(&parent_node)
            .await?
            .iter()
            .any(|c| c.name == name)
        {
            return Err(FsErrno::Exists);
        }
        let entry_id = Self::collection_entry_id(&parent_node.key, name);
        let entry = self
            .metadata
            .create_collection(&self.namespace_id, &parent_node.entry_id, &entry_id, name)
            .await
            .map_err(|err| crate::error::classify(&*err))?;
        let node = self.populate_child(&parent_node, entry);
        self.touch_children(parent);
        Ok(self.node_attr(&node))
    }

    pub async fn readdir(&self, ino: u64) -> Result<Vec<DirEntry>, FsErrno> {
        let node = self.snapshot(ino)?;
        let children = self.list_children(&node).await?;
        let mut out = Vec::with_capacity(children.len() + 2);
        // Populate "." and ".." attrs so readdirplus can return them in one pass.
        let dot_attr = self.node_attr(&node);
        let dotdot_attr = self
            .tables
            .inode(node.parent)
            .map(|parent| self.node_attr(&parent.snapshot()))
            .unwrap_or(dot_attr);
        out.push(DirEntry {
            ino: node.ino,
            name: ".".to_string(),
            kind: FileKind::Directory,
            attr: Some(dot_attr),
        });
        out.push(DirEntry {
            ino: node.parent,
            name: "..".to_string(),
            kind: FileKind::Directory,
            attr: Some(dotdot_attr),
        });
        for child in children {
            let attr = Some(self.node_attr(&child));
            out.push(DirEntry {
                ino: child.ino,
                name: child.name,
                kind: child.kind,
                attr,
            });
        }
        Ok(out)
    }

    pub async fn open(&self, ino: u64, flags: i32) -> Result<OpenedFile, FsErrno> {
        let node = self.snapshot(ino)?;
        if !matches!(node.kind, FileKind::RegularFile) {
            return Err(FsErrno::IsDir);
        }
        let truncate = flags & libc::O_TRUNC != 0;
        let writable = (flags & libc::O_ACCMODE) != libc::O_RDONLY;
        let fh = if writable || truncate {
            // Writable/truncate: stage in memory (whole-object RMW unless O_TRUNC).
            self.open_staged_handle(&node, truncate).await?
        } else {
            // Read-only: a ranged handle. No whole-object fetch at open — reads
            // pull only the stripes they touch.
            let fh = self.tables.alloc_fh();
            self.tables
                .handles
                .insert(fh, FileHandle::new_ranged(node.ino, node.key.clone()));
            fh
        };
        Ok(self.opened(fh))
    }

    pub async fn create(
        &self,
        parent: u64,
        name: &str,
        _mode: u32,
        _flags: i32,
    ) -> Result<(Attr, OpenedFile), FsErrno> {
        let parent_node = self.snapshot(parent)?;
        Self::ensure_dir(&parent_node)?;
        let key = Self::child_key(&parent_node.key, name);
        if self.tables.by_key.contains_key(&key) {
            return Err(FsErrno::Exists);
        }
        let ino = self.tables.alloc_ino();
        let state = InodeState {
            ino,
            parent,
            name: name.to_string(),
            entry_id: format!("pending:{key}"),
            key: key.clone(),
            kind: FileKind::RegularFile,
            size: 0,
            size_loaded_at: Some(Instant::now()),
            children_loaded_at: None,
        };
        let attr = self.node_attr(&state);
        self.tables.insert_inode(Inode::new(state));
        self.touch_children(parent);
        let fh = self.tables.alloc_fh();
        self.tables
            .handles
            .insert(fh, FileHandle::new_staged(ino, key, true, Vec::new(), true));
        Ok((attr, self.opened(fh)))
    }

    pub async fn read(&self, fh: u64, offset: u64, size: u32) -> Result<Vec<u8>, FsErrno> {
        let handle = self.tables.handle(fh).ok_or(FsErrno::BadHandle)?;
        match handle.staged_buffer() {
            // Writable/truncate handle: serve from the staged in-memory buffer.
            Some(buffer) => Ok(buffer
                .lock()
                .expect("handle lock poisoned")
                .read_range(offset as usize, size as usize)),
            // Read-only handle: stripe-granular ranged read — no whole-object
            // fetch. The Phase 2 win: a 4 KiB pread of a 10 GiB object reads
            // only the touched stripe(s).
            None => self.read_ranged_cached(&handle.key, offset, size).await,
        }
    }

    /// Read-only (Ranged handle) read served through the Tier-B stripe cache.
    ///
    /// On a cache miss it fetches the FULL covering stripe (`W` bytes — the same
    /// stripe `get_object_range` would have fetched anyway) and caches it, so the
    /// next ~128 KiB FUSE read landing in that stripe hits RAM instead of
    /// re-fetching + re-EC-decoding it. `W` is learned from the EC profile on the
    /// first read; the mount is single-bucket so it is constant thereafter.
    async fn read_ranged_cached(
        &self,
        key: &str,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, FsErrno> {
        let size = size as u64;
        if size == 0 {
            return Ok(Vec::new());
        }
        let width = self.stripe_cache.stripe_width();
        if width == 0 {
            // Width not learned yet: serve the exact range and learn W. The next
            // read caches stripe-aligned.
            let (payload, learned) = self
                .objects
                .get_object_range(&self.bucket_id, key, offset, size)
                .await?;
            self.stripe_cache.observe_stripe_width(learned);
            return Ok(payload);
        }

        let end = offset.saturating_add(size);
        let mut out: Vec<u8> = Vec::with_capacity(size as usize);
        let mut pos = offset;
        while pos < end {
            let stripe_index = pos / width;
            let stripe_start = stripe_index * width;
            let within = (pos - stripe_start) as usize;

            let stripe = match self.stripe_cache.get(key, stripe_index) {
                Some(bytes) => bytes,
                None => self.fetch_stripe(key, stripe_index, stripe_start, width).await?,
            };

            if within >= stripe.len() {
                break; // reading past EOF (short final stripe / empty object)
            }
            let take = ((end - pos) as usize).min(stripe.len() - within);
            out.extend_from_slice(&stripe[within..within + take]);
            pos += take as u64;

            // A stripe shorter than the full width is the last stripe => EOF.
            if stripe.len() < width as usize {
                break;
            }
        }
        Ok(out)
    }

    /// Fetch one stripe on a cache miss, de-duplicated single-flight so that `N`
    /// concurrent FUSE reads missing the SAME stripe issue exactly one
    /// `W`-byte fetch + EC decode instead of `N` of them (the cold-start
    /// fan-out amplification this cache exists to kill). The leader fetches and
    /// caches; followers wait on the shared per-stripe gate and then re-check
    /// the cache, sharing the leader's `Arc<Vec<u8>>`.
    async fn fetch_stripe(
        &self,
        key: &str,
        stripe_index: u64,
        stripe_start: u64,
        width: u64,
    ) -> Result<Arc<Vec<u8>>, FsErrno> {
        let gate = self.stripe_cache.in_flight_gate(key, stripe_index);
        let _permit = gate.lock().await;
        // A leader may have populated the cache while we waited for the gate.
        if let Some(bytes) = self.stripe_cache.get(key, stripe_index) {
            return Ok(bytes);
        }
        let (payload, learned) = self
            .objects
            .get_object_range(&self.bucket_id, key, stripe_start, width)
            .await?;
        self.stripe_cache.observe_stripe_width(learned);
        let bytes = Arc::new(payload);
        // put() skips empty payloads (reads at/past EOF), so a 0-byte stripe is
        // returned to the caller but never pollutes the cache.
        self.stripe_cache.put(key, stripe_index, Arc::clone(&bytes));
        self.stripe_cache.clear_in_flight(key, stripe_index);
        Ok(bytes)
    }

    pub async fn write(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<u32, FsErrno> {
        let handle = self.tables.handle(fh).ok_or(FsErrno::BadHandle)?;
        // A read-only (ranged) handle has no staged buffer and rejects writes.
        let buffer = handle.staged_buffer().ok_or(FsErrno::BadHandle)?;
        if !handle.writable {
            return Err(FsErrno::BadHandle);
        }
        let new_len = {
            let mut buf = buffer.lock().expect("handle lock poisoned");
            buf.write_at(offset as usize, data);
            buf.len() as u64
        };
        handle.set_dirty(true);
        if let Some(inode) = self.tables.inode(ino) {
            let mut s = inode.state.write().expect("inode lock poisoned");
            s.size = s.size.max(new_len);
            s.size_loaded_at = Some(Instant::now());
        }
        Ok(data.len() as u32)
    }

    pub async fn flush(&self, fh: u64) -> Result<(), FsErrno> {
        self.commit_handle(fh).await
    }

    pub async fn fsync(&self, fh: u64) -> Result<(), FsErrno> {
        self.commit_handle(fh).await
    }

    pub async fn release(&self, fh: u64) -> Result<(), FsErrno> {
        let result = self.commit_handle(fh).await;
        self.tables.handles.remove(&fh);
        result
    }

    pub async fn unlink(&self, parent: u64, name: &str) -> Result<(), FsErrno> {
        let parent_node = self.snapshot(parent)?;
        Self::ensure_dir(&parent_node)?;
        let key = Self::child_key(&parent_node.key, name);
        self.objects.delete_object(&self.bucket_id, &key).await?;
        // The object's bytes are gone — drop its cached stripes so a still-open
        // read-only Ranged handle (cache lookup is key+stripe_index only,
        // independent of inode lifecycle) cannot keep serving the deleted bytes,
        // and a delete-then-recreate at the same key has a local backstop rather
        // than relying solely on the NATS coherence event landing. Mirrors
        // commit_handle()'s invalidate_key(&handle.key).
        self.stripe_cache.invalidate_key(&key);
        if let Some(ino) = self.tables.by_key.get(&key).map(|e| *e) {
            self.tables.remove_inode(ino);
        }
        self.touch_children(parent);
        Ok(())
    }

    pub async fn rmdir(&self, parent: u64, name: &str) -> Result<(), FsErrno> {
        let parent_node = self.snapshot(parent)?;
        Self::ensure_dir(&parent_node)?;
        let children = self.list_children(&parent_node).await?;
        let child = children
            .into_iter()
            .find(|c| c.name == name)
            .ok_or(FsErrno::NoEntry)?;
        Self::ensure_dir(&child)?;
        if !self.list_children(&child).await?.is_empty() {
            return Err(FsErrno::NotEmpty);
        }
        self.metadata
            .delete_namespace_entry(&self.namespace_id, &child.entry_id)
            .await
            .map_err(|err| crate::error::classify(&*err))?;
        self.tables.remove_inode(child.ino);
        self.touch_children(parent);
        Ok(())
    }

    // ----- handle internals -------------------------------------------------

    /// Open a writable staged handle for `node`. `truncate` => empty dirty
    /// buffer; otherwise the whole object is loaded for read-modify-write
    /// staging (Phase 3 will stream this instead of buffering whole objects).
    async fn open_staged_handle(&self, node: &InodeState, truncate: bool) -> Result<u64, FsErrno> {
        let data = if truncate {
            Vec::new()
        } else {
            match self.objects.get_object(&self.bucket_id, &node.key).await {
                Ok(payload) => {
                    if let Some(inode) = self.tables.inode(node.ino) {
                        let mut s = inode.state.write().expect("inode lock poisoned");
                        s.size = payload.len() as u64;
                        s.size_loaded_at = Some(Instant::now());
                    }
                    payload
                }
                // A freshly-created-but-uncommitted object has no manifest yet;
                // treat as empty rather than failing the open.
                Err(FsErrno::NoEntry) => Vec::new(),
                Err(other) => return Err(other),
            }
        };
        let fh = self.tables.alloc_fh();
        self.tables.handles.insert(
            fh,
            FileHandle::new_staged(node.ino, node.key.clone(), true, data, truncate),
        );
        Ok(fh)
    }

    async fn commit_handle(&self, fh: u64) -> Result<(), FsErrno> {
        let handle = self.tables.handle(fh).ok_or(FsErrno::BadHandle)?;
        if !handle.is_dirty() {
            return Ok(());
        }
        // Ranged (read-only) handles are never dirty; only staged buffers commit.
        let Some(buffer) = handle.staged_buffer() else {
            return Ok(());
        };
        // Clone bytes out under the lock, then await the upload with no lock held.
        let payload = buffer.lock().expect("handle lock poisoned").data.clone();
        self.objects
            .put_object(&self.bucket_id, &handle.key, &payload)
            .await?;
        handle.set_dirty(false);
        // A commit is a new immutable object version — drop any cached stripes
        // for this key so subsequent ranged reads see the new bytes.
        self.stripe_cache.invalidate_key(&handle.key);
        if let Some(inode) = self.tables.inode(handle.ino) {
            let mut s = inode.state.write().expect("inode lock poisoned");
            s.size = payload.len() as u64;
            s.size_loaded_at = Some(Instant::now());
        }
        Ok(())
    }

    fn touch_children(&self, parent: u64) {
        if let Some(inode) = self.tables.inode(parent) {
            inode
                .state
                .write()
                .expect("inode lock poisoned")
                .children_loaded_at = Some(Instant::now());
        }
    }

    /// Per-open kernel cache hints. Phase 2: keep the kernel page cache as the
    /// primary read cache (`FOPEN_KEEP_CACHE`) and rely on the NATS-driven
    /// `notify_inval_inode` sink for coherence — no more forced `DIRECT_IO`,
    /// which previously bypassed the page cache whenever NATS was enabled.
    fn opened(&self, fh: u64) -> OpenedFile {
        OpenedFile {
            fh,
            keep_cache: true,
            direct_io: false,
        }
    }

    // ----- coherence --------------------------------------------------------

    /// Apply a KMS invalidation to the in-process caches and push a kernel-side
    /// invalidation through the transport sink. Ported from
    /// `apply_state_invalidation` (`mount.rs:1430`).
    pub fn apply_invalidation(&self, event: MetadataInvalidationEvent) {
        if !event.namespace_id.is_empty() && event.namespace_id != self.namespace_id {
            return;
        }
        let sink = Arc::clone(&*self.sink.read().expect("sink lock poisoned"));

        // Namespace-wide / empty event => mark everything stale (TTL-bounded),
        // never a destructive flush.
        if event.namespace_id.is_empty()
            || (event.key.is_empty()
                && event.entry_id.is_empty()
                && event.parent_entry_id.is_empty())
        {
            // Namespace-wide event: every cached stripe is suspect.
            self.stripe_cache.clear();
            for entry in self.tables.by_ino.iter() {
                let mut s = entry.value().state.write().expect("inode lock poisoned");
                s.size_loaded_at = None;
                s.children_loaded_at = None;
                sink.inval_inode(s.ino);
            }
            return;
        }

        if !event.key.is_empty() {
            // Out-of-band mutation of this object: drop its cached stripes.
            self.stripe_cache.invalidate_key(&event.key);
            if let Some(ino) = self.tables.by_key.get(&event.key).map(|e| *e) {
                if let Some(inode) = self.tables.inode(ino) {
                    inode
                        .state
                        .write()
                        .expect("inode lock poisoned")
                        .size_loaded_at = None;
                    sink.inval_inode(ino);
                }
            }
            let parent_key = event.key.rsplit_once('/').map(|(p, _)| p).unwrap_or("");
            if let Some(parent_ino) = self.tables.by_key.get(parent_key).map(|e| *e) {
                self.mark_children_stale(parent_ino, &sink);
            }
        }
        if !event.entry_id.is_empty() {
            if let Some(ino) = self.tables.by_entry.get(&event.entry_id).map(|e| *e) {
                if let Some(inode) = self.tables.inode(ino) {
                    let mut s = inode.state.write().expect("inode lock poisoned");
                    s.size_loaded_at = None;
                    s.children_loaded_at = None;
                    sink.inval_inode(ino);
                }
            }
        }
        if !event.parent_entry_id.is_empty() {
            if let Some(parent_ino) = self.tables.by_entry.get(&event.parent_entry_id).map(|e| *e) {
                self.mark_children_stale(parent_ino, &sink);
            }
        }
    }

    fn mark_children_stale(&self, parent_ino: u64, sink: &Arc<dyn CoherenceSink>) {
        if let Some(inode) = self.tables.inode(parent_ino) {
            inode
                .state
                .write()
                .expect("inode lock poisoned")
                .children_loaded_at = None;
            sink.inval_inode(parent_ino);
        }
    }

    /// Number of currently open handles (observability / tests).
    pub fn open_handle_count(&self) -> usize {
        self.tables.handles.len()
    }
}

fn object_client_pool_size() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get().clamp(OBJECT_CLIENT_POOL_MIN, OBJECT_CLIENT_POOL_MAX))
        .unwrap_or(OBJECT_CLIENT_POOL_MIN)
}

