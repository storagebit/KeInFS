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
use crate::persistent_stripe_store::PersistentStripeStore;
use crate::state::{FileHandle, FsTables, HandleBuffer, Inode, InodeState};
use crate::persistent_stripe_store::DEFAULT_TIER_C_BUDGET_BYTES;
use crate::stripe_cache::{StripeCache, DEFAULT_STRIPE_CACHE_BUDGET_BYTES};
use crate::types::{Attr, Capabilities, DesiredKernelConfig, DirEntry, FileKind, OpenedFile, ROOT_INO};
use keinctl::proto::{MetadataInvalidationEvent, NamespaceDomainEntry, NamespaceEntryKind};
use ksc::client::CompletionMode;
use ksc::object::ObjectClientOptions;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// Tier-C disk stripe-cache directory. `None` (the default) DISABLES Tier-C
    /// entirely: the RAM Tier-B behaves exactly as before and no disk cache is
    /// created. When set, a cleared-on-mount disk victim cache is created below
    /// Tier-B in this directory (wiped fresh on every `connect()`).
    pub tier_c_cache_dir: Option<PathBuf>,
    /// Byte budget for the Tier-C disk cache when `tier_c_cache_dir` is set.
    pub tier_c_budget_bytes: u64,
    /// RAM byte budget for the Tier-B stripe cache (see [`StripeCache`]).
    /// Defaults to [`DEFAULT_STRIPE_CACHE_BUDGET_BYTES`].
    pub stripe_cache_budget_bytes: u64,
}

impl Default for FsConfig {
    fn default() -> Self {
        Self {
            kms_endpoints: Vec::new(),
            namespace_id: String::new(),
            bucket_id: String::new(),
            read_completion_mode: CompletionMode::Interrupt,
            write_completion_mode: CompletionMode::Interrupt,
            metadata_notification_nats_url: None,
            metadata_notification_subject: String::new(),
            // Tier-C OFF by default — opt-in only.
            tier_c_cache_dir: None,
            tier_c_budget_bytes: DEFAULT_TIER_C_BUDGET_BYTES,
            stripe_cache_budget_bytes: DEFAULT_STRIPE_CACHE_BUDGET_BYTES as u64,
        }
    }
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
    /// Tier-C disk victim cache below Tier-B. `None` when disabled (the default):
    /// every Tier-C call site short-circuits and behavior is exactly as before.
    /// When present it is a cleared-on-mount disk extension of Tier-B — Tier-B
    /// eviction writes back here, and a Tier-B miss checks here before the
    /// network (a hit promotes the stripe back into Tier-B). See
    /// [`PersistentStripeStore`].
    tier_c: Option<Arc<PersistentStripeStore>>,
    /// Monotonic cache-invalidation generation, bumped by EVERY invalidation
    /// (`invalidate_key` on commit/unlink/NATS and the namespace-wide `clear`).
    ///
    /// This is the ordering primitive that closes the lost-invalidation races
    /// between a slow stripe fetch/promote and a concurrent overwrite: a fetch
    /// snapshots this counter BEFORE it reads (from Tier-C or the network), and
    /// the subsequent `put_tier_b` only inserts into either tier if the counter
    /// is UNCHANGED. If any invalidation landed during the fetch window the
    /// stripe is potentially stale, so it is dropped rather than (re)inserted —
    /// a fetch that began before an invalidation can never complete after it.
    /// Conservative (an unrelated key's invalidation also skips the insert), but
    /// both tiers are best-effort, so a skipped insert only costs a future
    /// re-fetch, never correctness. Bumped under the same logical step that drops
    /// the entries, so an invalidation is never observed before its effect.
    ///
    /// `Arc` so the (bounded) detached Tier-C write-back task can hold a clone
    /// and re-check the generation immediately before it writes to disk.
    cache_invalidation_gen: Arc<AtomicU64>,
    /// Caps the number of concurrently in-flight Tier-C write-back tasks (and
    /// thus the evicted-stripe `Arc<Vec<u8>>`s they retain). Without a bound,
    /// sustained cold reads that outrun disk throughput would accumulate
    /// detached write-back tasks unboundedly, each holding ~`W` bytes — inflating
    /// memory past the very RAM budget Tier-B exists to enforce. `None` when
    /// Tier-C is disabled. Write-back is best-effort, so when no permit is
    /// available the victim is simply dropped (a future read re-fetches it).
    tier_c_writeback_permits: Option<Arc<tokio::sync::Semaphore>>,
}

/// Upper bound on concurrent Tier-C write-back tasks. Small: write-back is a
/// best-effort disk extension, not a correctness path, so a handful of permits
/// is enough to keep the disk busy without letting evicted stripes pile up in
/// RAM.
const TIER_C_WRITEBACK_CONCURRENCY: usize = 8;

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

        // Tier-C disk victim cache: only built when a directory was configured.
        // `new()` WIPES the directory (cleared-on-mount). A failure to prepare
        // the dir is non-fatal — Tier-C is best-effort, so we log and run with
        // Tier-B only rather than failing the whole mount.
        let tier_c = match config.tier_c_cache_dir {
            Some(dir) => {
                match PersistentStripeStore::new(dir.clone(), config.tier_c_budget_bytes).await {
                    Ok(store) => Some(store),
                    Err(err) => {
                        eprintln!(
                            "KFC Tier-C: disabling disk stripe cache at {}: {err}",
                            dir.display()
                        );
                        None
                    }
                }
            }
            None => None,
        };

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
            stripe_cache: StripeCache::new(
                usize::try_from(config.stripe_cache_budget_bytes)
                    .unwrap_or(DEFAULT_STRIPE_CACHE_BUDGET_BYTES),
            ),
            tier_c_writeback_permits: tier_c
                .is_some()
                .then(|| Arc::new(tokio::sync::Semaphore::new(TIER_C_WRITEBACK_CONCURRENCY))),
            tier_c,
            cache_invalidation_gen: Arc::new(AtomicU64::new(0)),
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
            .unwrap_or_else(|e| e.into_inner())
            .set_len(size)
            .map_err(|_| FsErrno::Io)?;
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
        let handle = FileHandle::new_staged(ino, key, true, Vec::new(), true)
            .map_err(|_| FsErrno::Io)?;
        self.tables.handles.insert(fh, handle);
        Ok((attr, self.opened(fh)))
    }

    pub async fn read(&self, fh: u64, offset: u64, size: u32) -> Result<Vec<u8>, FsErrno> {
        let handle = self.tables.handle(fh).ok_or(FsErrno::BadHandle)?;
        match handle.staged_buffer() {
            // Writable/truncate handle: serve from the staged temp file (a
            // bounded seek+read), so read-after-write within the same handle
            // still returns the written bytes. A genuine I/O error on the staged
            // temp file surfaces as EIO rather than a silent zero-filled short
            // read. Recover from a poisoned lock (a prior panic under the lock)
            // instead of cascading the panic, mirroring the Tier-C store.
            Some(buffer) => buffer
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .read_range(offset as usize, size as usize)
                .map_err(|_| FsErrno::Io),
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
        // The single-flight gate covers the WHOLE Tier-C-then-network miss path,
        // not just the network fetch: a leader holds it across the Tier-C lookup,
        // promote-into-Tier-B, AND (on a Tier-C miss) the network fetch, so the
        // de-dup of `N` concurrent missers of the same stripe still issues at most
        // one network fetch + EC decode. Followers wake, re-check Tier-B, and
        // share the leader's `Arc<Vec<u8>>`.
        let _permit = gate.lock().await;
        // A leader may have populated Tier-B while we waited for the gate.
        if let Some(bytes) = self.stripe_cache.get(key, stripe_index) {
            self.stripe_cache.clear_in_flight(key, stripe_index);
            return Ok(bytes);
        }
        // Snapshot the invalidation generation BEFORE any read. If an overwrite
        // invalidates this stripe while we are reading the OLD version (from
        // Tier-C or the network), the generation changes and `put_tier_b` below
        // refuses to (re)insert the now-stale bytes into either tier — closing
        // the lost-invalidation race the single-flight gate alone cannot
        // (invalidation does not take the gate). The fetched bytes are still
        // returned to THIS caller (it asked before the overwrite; serving the
        // version it was reading is fine), they just are not cached.
        let gen_at_fetch = self.cache_invalidation_gen.load(Ordering::Acquire);
        // Tier-C check BEFORE the network. On a hit, PROMOTE back into Tier-B
        // (which may itself evict cold stripes down to Tier-C) and return it as a
        // cache hit — no network, no EC decode.
        if let Some(store) = &self.tier_c {
            if let Some(bytes) = store.get(key, stripe_index).await {
                self.put_tier_b(key, stripe_index, Arc::clone(&bytes), gen_at_fetch)
                    .await;
                self.stripe_cache.clear_in_flight(key, stripe_index);
                return Ok(bytes);
            }
        }
        // Tier-C miss (or disabled): fetch from the network as before. On a
        // network error, ALWAYS clear the single-flight gate first so the
        // in_flight entry never leaks (a transient fetch failure must not strand
        // a stale gate that serializes — and is never reclaimed for — future
        // missers of this stripe).
        let (payload, learned) = match self
            .objects
            .get_object_range(&self.bucket_id, key, stripe_start, width)
            .await
        {
            Ok(ok) => ok,
            Err(err) => {
                self.stripe_cache.clear_in_flight(key, stripe_index);
                return Err(err);
            }
        };
        self.stripe_cache.observe_stripe_width(learned);
        let bytes = Arc::new(payload);
        // put_tier_b() skips empty payloads (reads at/past EOF), so a 0-byte
        // stripe is returned to the caller but never pollutes either tier. The
        // Tier-B insert may evict cold stripes — those are written back to Tier-C.
        // The generation guard inside put_tier_b drops the insert if an
        // invalidation raced the network fetch.
        self.put_tier_b(key, stripe_index, Arc::clone(&bytes), gen_at_fetch)
            .await;
        self.stripe_cache.clear_in_flight(key, stripe_index);
        Ok(bytes)
    }

    /// Insert a stripe into Tier-B and write any stripes Tier-B evicted (to stay
    /// within its RAM budget) back to the Tier-C disk victim cache.
    ///
    /// `gen_at_fetch` is the invalidation generation snapshotted by the caller
    /// BEFORE it read these bytes. If the live generation has moved on, an
    /// overwrite invalidated this key while the fetch was in flight, so the bytes
    /// are potentially stale: NEITHER tier is touched (no Tier-B insert, no
    /// Tier-C write-back). This is what makes the cache coherent against a
    /// concurrent overwrite — a stripe fetched before an invalidation can never
    /// be (re)inserted after it. See [`Self::cache_invalidation_gen`].
    ///
    /// On the in-generation path: the Tier-B `put` returns its eviction victims
    /// (collected under, then handed out from outside, its `size_lock`); the
    /// Tier-C writes happen OUTSIDE that lock on a spawned task so the read path
    /// is never blocked on disk I/O. The write-back is bounded by a semaphore
    /// (caps both task count and retained-stripe memory) and re-checks the
    /// generation immediately before each disk write so a write-back that began
    /// before an invalidation cannot land stale bytes after it. When Tier-C is
    /// disabled the victims are simply dropped (Tier-B alone, as before).
    async fn put_tier_b(&self, key: &str, stripe_index: u64, bytes: Arc<Vec<u8>>, gen_at_fetch: u64) {
        // Generation guard: if anything invalidated since the fetch began, drop
        // the bytes from BOTH tiers (do not insert into Tier-B, do not write back
        // to Tier-C). `Acquire` pairs with the `Release` bump in `bump_*` so the
        // invalidation's cache drop is visible before we decide.
        if self.cache_invalidation_gen.load(Ordering::Acquire) != gen_at_fetch {
            return;
        }
        let evicted = self.stripe_cache.put(key, stripe_index, bytes);
        let (Some(store), Some(permits)) = (&self.tier_c, &self.tier_c_writeback_permits) else {
            return;
        };
        if evicted.is_empty() {
            return;
        }
        let store = Arc::clone(store);
        // Bound concurrency: take a permit BEFORE spawning. Best-effort — if none
        // is available the victims are dropped rather than buffered unboundedly.
        let Ok(permit) = Arc::clone(permits).try_acquire_owned() else {
            return;
        };
        let gen_handle = Arc::clone(&self.cache_invalidation_gen);
        tokio::spawn(async move {
            let _permit = permit; // released on task completion
            for (vkey, vidx, vbytes) in evicted {
                // Re-check immediately before the disk write: an invalidation
                // that raced the eviction must win, so a stale victim never
                // persists to disk after the key was invalidated.
                if gen_handle.load(Ordering::Acquire) != gen_at_fetch {
                    break;
                }
                // Best-effort write-back; a failure just means a future read of
                // that stripe re-fetches from the network.
                let _ = store.put(&vkey, vidx, vbytes).await;
            }
        });
    }

    /// Bump the invalidation generation. MUST be called as the FIRST step of
    /// every per-key/namespace invalidation, BEFORE dropping any cache entries:
    /// a concurrent fetch that has already snapshotted the old generation and is
    /// mid-flight will then see the bump in its post-fetch re-check and refuse to
    /// (re)insert the stale stripe it was reading. `Release` so the bump is
    /// ordered before the cache mutations that follow (paired with the `Acquire`
    /// load in `put_tier_b` / the write-back task).
    fn bump_invalidation_gen(&self) {
        self.cache_invalidation_gen.fetch_add(1, Ordering::Release);
    }

    /// Coherent per-key invalidation across BOTH tiers, awaited inline. Bumps the
    /// invalidation generation FIRST (so a racing in-flight fetch cannot reinsert
    /// the old version), drops Tier-B synchronously, then drops Tier-C. Used on
    /// the async commit/unlink paths that already drop Tier-B in line.
    async fn invalidate_key_both_tiers(&self, key: &str) {
        self.bump_invalidation_gen();
        self.stripe_cache.invalidate_key(key);
        if let Some(store) = &self.tier_c {
            store.invalidate_key(key).await;
        }
    }

    /// Coherent per-key invalidation for the synchronous `apply_invalidation`
    /// coherence path (which cannot await). Bumps the generation and drops Tier-B
    /// synchronously; the Tier-C file unlink is deferred to a spawned task. The
    /// generation bump + Tier-B drop are synchronous, so the cross-tier window
    /// the reviewer flagged (miss Tier-B / hit stale Tier-C / promote back) is
    /// closed: a racing reader that snapshotted the OLD generation before this
    /// runs will fail its post-fetch generation re-check and NOT promote, and a
    /// reader that snapshots AFTER this runs sees the new generation (the stale
    /// Tier-C entry, if its unlink has not landed yet, is still gated out of
    /// re-insertion by the generation guard, and a direct Tier-C hit of it is
    /// promoted only if the generation is unchanged).
    fn invalidate_key_both_tiers_detached(&self, key: &str) {
        self.bump_invalidation_gen();
        self.stripe_cache.invalidate_key(key);
        let Some(store) = &self.tier_c else { return };
        let store = Arc::clone(store);
        let key = key.to_string();
        tokio::spawn(async move {
            store.invalidate_key(&key).await;
        });
    }

    /// Coherent namespace-wide wipe across BOTH tiers for the synchronous
    /// coherence path. Bumps the generation and clears Tier-B synchronously; the
    /// Tier-C directory wipe is deferred to a spawned task. No-op for Tier-C when
    /// disabled.
    fn clear_both_tiers_detached(&self) {
        self.bump_invalidation_gen();
        self.stripe_cache.clear();
        let Some(store) = &self.tier_c else { return };
        let store = Arc::clone(store);
        tokio::spawn(async move {
            store.clear().await;
        });
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
            let mut buf = buffer.lock().unwrap_or_else(|e| e.into_inner());
            buf.write_at(offset as usize, data).map_err(|_| FsErrno::Io)?;
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
        // commit_handle()'s invalidate_key(&handle.key). Bumps the invalidation
        // generation so an in-flight fetch of the old bytes cannot reinsert them.
        self.invalidate_key_both_tiers(&key).await;
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
    /// buffer; otherwise the existing object is staged for read-modify-write.
    ///
    /// RAM-bounded RMW seed: the existing object is copied into the staged temp
    /// file in bounded stripe-width-sized chunks via `get_object_range` (peak RAM
    /// = one chunk), NOT loaded whole into a `Vec`. A multi-GB existing file
    /// opened for append/edit therefore never materializes the whole object in
    /// client RAM — only one window at a time, written straight to disk.
    async fn open_staged_handle(&self, node: &InodeState, truncate: bool) -> Result<u64, FsErrno> {
        let fh = self.tables.alloc_fh();
        let handle = if truncate {
            FileHandle::new_staged(node.ino, node.key.clone(), true, Vec::new(), true)
                .map_err(|_| FsErrno::Io)?
        } else {
            match self.seed_rmw_buffer(&node.key).await {
                Ok((buffer, seeded_len)) => {
                    if let Some(inode) = self.tables.inode(node.ino) {
                        let mut s = inode.state.write().expect("inode lock poisoned");
                        s.size = seeded_len;
                        s.size_loaded_at = Some(Instant::now());
                    }
                    // RMW seed is the on-disk prefix, not yet dirty — only an
                    // actual write/truncate marks it dirty for commit.
                    FileHandle::from_buffer(node.ino, node.key.clone(), true, buffer, false)
                        .map_err(|_| FsErrno::Io)?
                }
                // A freshly-created-but-uncommitted object has no manifest yet;
                // treat as empty rather than failing the open.
                Err(FsErrno::NoEntry) => {
                    FileHandle::new_staged(node.ino, node.key.clone(), true, Vec::new(), false)
                        .map_err(|_| FsErrno::Io)?
                }
                Err(other) => return Err(other),
            }
        };
        self.tables.handles.insert(fh, handle);
        Ok(fh)
    }

    /// Copy the existing object at `key` into a fresh staged temp file in bounded
    /// stripe-width-sized chunks (RAM-bounded RMW seed). Returns the seeded buffer
    /// and its logical length. Peak RAM is one chunk, never the whole object.
    async fn seed_rmw_buffer(&self, key: &str) -> Result<(HandleBuffer, u64), FsErrno> {
        // Bounded copy window. `get_object_range` reports the EC stripe width on
        // the first fetch; we use it (clamped) as the per-iteration window so each
        // fetch is a whole number of stripes. Until it is known, use a sane
        // bounded default so a tiny object still fetches in one shot.
        const DEFAULT_WINDOW: u64 = 4 * 1024 * 1024;
        let mut buffer = HandleBuffer::new_empty().map_err(|_| FsErrno::Io)?;
        let mut offset: u64 = 0;
        let mut window = DEFAULT_WINDOW;
        loop {
            let (chunk, stripe_width) = self
                .objects
                .get_object_range(&self.bucket_id, key, offset, window)
                .await?;
            if chunk.is_empty() {
                break; // reached object end (ranged read clamps to logical length)
            }
            let read = chunk.len() as u64;
            buffer.seed_chunk(&chunk).map_err(|_| FsErrno::Io)?;
            offset = offset.saturating_add(read);
            // Align subsequent windows to the stripe width once known so each
            // fetch is stripe-aligned; keep RAM bounded to a few stripes.
            if stripe_width > 0 {
                window = stripe_width
                    .saturating_mul(4)
                    .clamp(stripe_width, DEFAULT_WINDOW.max(stripe_width));
            }
            // A short read (< requested window) means we hit the clamped end.
            if read < window {
                break;
            }
        }
        buffer.seed_finish().map_err(|_| FsErrno::Io)?;
        let seeded_len = buffer.len() as u64;
        Ok((buffer, seeded_len))
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
        // Streaming-writeback v1: read only the temp-file PATH + logical length
        // out under the lock (no whole-object clone), then stream from the temp
        // file in stripe-sized chunks with NO handle lock held. The put path
        // reads each stripe range from the file via a bounded pread, so at no
        // point is the whole object resident in RAM on the commit path. An empty
        // file (logical length 0) commits an empty object.
        //
        // TODO(streaming-writeback v2 / follow-up): overlap the network upload
        // WITH the user's write() calls — true stream-as-you-write — so flush is
        // not where the whole object is shipped. That dirty-stripe re-stream is a
        // larger distributed change (write-window backpressure + re-stream of
        // stripes dirtied after they were sent) and is explicitly out of scope
        // here; v1 streams from the staged temp file at flush.
        // Snapshot the temp-file PATH + authoritative logical length together,
        // under one lock acquisition, so they describe the SAME atomically-read
        // state. KSC keys the whole stripe loop off this `logical_len` (it does
        // NOT re-stat the file), so a concurrent write() that extends or shrinks
        // the temp file between this snapshot and the stream cannot change the
        // committed object length: the commit always writes exactly `logical_len`
        // bytes (short reads at a racing-shrink tail zero-fill rather than fail),
        // and the inode size set below agrees with it. Recover from a poisoned
        // lock rather than cascading a panic into the FUSE worker.
        let (path, logical_len) = buffer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .commit_source();
        self.objects
            .put_object_from_path(&self.bucket_id, &handle.key, &path, logical_len)
            .await?;
        handle.set_dirty(false);
        // A commit is a new immutable object version — drop any cached stripes
        // for this key so subsequent ranged reads see the new bytes (both tiers).
        // Bumps the invalidation generation so a concurrent ranged read still
        // fetching the OLD version cannot reinsert it after this commit.
        self.invalidate_key_both_tiers(&handle.key).await;
        if let Some(inode) = self.tables.inode(handle.ino) {
            let mut s = inode.state.write().expect("inode lock poisoned");
            s.size = logical_len;
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
            // Namespace-wide event: every cached stripe is suspect — both tiers.
            // Bumps the generation + clears Tier-B synchronously (Tier-C wipe is
            // deferred) so no in-flight fetch can reinsert a stale stripe.
            self.clear_both_tiers_detached();
            for entry in self.tables.by_ino.iter() {
                let mut s = entry.value().state.write().expect("inode lock poisoned");
                s.size_loaded_at = None;
                s.children_loaded_at = None;
                sink.inval_inode(s.ino);
            }
            return;
        }

        if !event.key.is_empty() {
            // Out-of-band mutation of this object: drop its cached stripes from
            // BOTH tiers. Bumps the generation + drops Tier-B synchronously
            // (Tier-C unlink deferred) so a racing reader cannot promote a stale
            // Tier-C stripe back into Tier-B.
            self.invalidate_key_both_tiers_detached(&event.key);
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

