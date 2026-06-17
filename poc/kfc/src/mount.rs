// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::MountConfig;
use crate::metadata::{DynError, boxed_error};

#[cfg(feature = "fuse")]
use crate::metadata::MetadataClient;
#[cfg(feature = "fuse")]
use fuser::{
    BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags,
    Generation, INodeNo, LockOwner, MountOption, OpenFlags, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
    WriteFlags,
};
#[cfg(feature = "fuse")]
use futures_util::StreamExt;
#[cfg(feature = "fuse")]
use keinctl::proto::{MetadataInvalidationEvent, NamespaceDomainEntry, NamespaceEntryKind};
#[cfg(feature = "fuse")]
use ksc::object::{ObjectClient, ObjectClientOptions};
#[cfg(feature = "fuse")]
use prost::Message;
#[cfg(feature = "fuse")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "fuse")]
use std::ffi::OsStr;
#[cfg(feature = "fuse")]
use std::fs::OpenOptions;
#[cfg(feature = "fuse")]
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(feature = "fuse")]
use std::path::{Path, PathBuf};
#[cfg(feature = "fuse")]
use std::sync::Arc;
#[cfg(feature = "fuse")]
use std::sync::Mutex;
#[cfg(feature = "fuse")]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
#[cfg(feature = "fuse")]
use std::time::{Duration, Instant, SystemTime};
#[cfg(feature = "fuse")]
use tokio::runtime::{Builder, Runtime};

#[cfg(feature = "fuse")]
const TTL: Duration = Duration::from_secs(1);
#[cfg(feature = "fuse")]
const ROOT_INO: u64 = 1;
#[cfg(feature = "fuse")]
const META_CACHE_TTL: Duration = Duration::from_secs(1);
#[cfg(feature = "fuse")]
const BLOB_CACHE_MAX_ENTRIES: usize = 128;
#[cfg(feature = "fuse")]
const WRITEBACK_CACHE_MAX_BYTES: usize = 8 << 20;
#[cfg(feature = "fuse")]
const OBJECT_CLIENT_POOL_MIN: usize = 4;
#[cfg(feature = "fuse")]
const OBJECT_CLIENT_POOL_MAX: usize = 16;

#[cfg(feature = "fuse")]
#[derive(Clone, Debug)]
enum NodeKind {
    Directory,
    File,
}

#[cfg(feature = "fuse")]
#[derive(Clone, Debug)]
struct Node {
    ino: u64,
    parent: u64,
    name: String,
    entry_id: String,
    key: String,
    kind: NodeKind,
    size: u64,
    size_loaded_at: Option<Instant>,
    children_loaded_at: Option<Instant>,
}

#[cfg(feature = "fuse")]
#[derive(Clone, Debug)]
struct SpillFile {
    path: PathBuf,
    len: usize,
}

#[cfg(feature = "fuse")]
#[derive(Clone, Debug)]
enum HandleBuffer {
    Shared(Arc<Vec<u8>>),
    Spill(SpillFile),
}

#[cfg(feature = "fuse")]
impl HandleBuffer {
    fn len(&self) -> usize {
        match self {
            Self::Shared(buffer) => buffer.len(),
            Self::Spill(spill) => spill.len,
        }
    }

    fn read_range(&self, offset: usize, size: usize) -> Result<Vec<u8>, Errno> {
        match self {
            Self::Shared(buffer) => {
                let end = buffer.len().min(offset.saturating_add(size));
                Ok(buffer.get(offset..end).unwrap_or(&[]).to_vec())
            }
            Self::Spill(spill) => {
                if offset >= spill.len {
                    return Ok(Vec::new());
                }
                let end = spill.len.min(offset.saturating_add(size));
                let mut file = OpenOptions::new()
                    .read(true)
                    .open(&spill.path)
                    .map_err(|_| Errno::EIO)?;
                file.seek(SeekFrom::Start(offset as u64))
                    .map_err(|_| Errno::EIO)?;
                let mut out = vec![0u8; end - offset];
                file.read_exact(&mut out).map_err(|_| Errno::EIO)?;
                Ok(out)
            }
        }
    }

    fn spill_path(&self) -> Option<&Path> {
        match self {
            Self::Shared(_) => None,
            Self::Spill(spill) => Some(spill.path.as_path()),
        }
    }

    fn small_cached_payload(&self) -> Result<Option<Arc<Vec<u8>>>, Errno> {
        match self {
            Self::Shared(buffer) => Ok(Some(Arc::clone(buffer))),
            Self::Spill(spill) if spill.len <= WRITEBACK_CACHE_MAX_BYTES => {
                let mut file = OpenOptions::new()
                    .read(true)
                    .open(&spill.path)
                    .map_err(|_| Errno::EIO)?;
                let mut payload = Vec::with_capacity(spill.len);
                file.read_to_end(&mut payload).map_err(|_| Errno::EIO)?;
                Ok(Some(Arc::new(payload)))
            }
            Self::Spill(_) => Ok(None),
        }
    }
}

#[cfg(feature = "fuse")]
#[derive(Clone, Debug)]
struct HandleState {
    ino: u64,
    key: String,
    buffer: HandleBuffer,
    dirty: bool,
}

#[cfg(feature = "fuse")]
#[derive(Default)]
struct FsState {
    nodes_by_ino: HashMap<u64, Node>,
    nodes_by_entry: HashMap<String, u64>,
    nodes_by_key: HashMap<String, u64>,
    handles: HashMap<u64, HandleState>,
    blob_cache: HashMap<String, Arc<Vec<u8>>>,
}

#[cfg(feature = "fuse")]
struct AsyncDriver {
    _runtime: Runtime,
    handle: tokio::runtime::Handle,
}

#[cfg(feature = "fuse")]
impl AsyncDriver {
    fn new() -> Result<Self, DynError> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .map_err(|err| boxed_error(err.to_string()))?;
        let handle = runtime.handle().clone();
        Ok(Self {
            _runtime: runtime,
            handle,
        })
    }

    fn block_on<F, T>(&self, future: F) -> Result<T, DynError>
    where
        F: std::future::Future<Output = Result<T, DynError>> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.handle.spawn(async move {
            let _ = tx.send(future.await);
        });
        rx.recv()
            .map_err(|err| boxed_error(format!("KFC async driver channel failed: {err}")))?
    }
}

#[cfg(feature = "fuse")]
#[derive(Clone)]
struct ObjectClientPool {
    clients: Arc<Vec<Arc<tokio::sync::Mutex<ObjectClient>>>>,
    next: Arc<AtomicUsize>,
}

#[cfg(feature = "fuse")]
impl ObjectClientPool {
    async fn connect(
        kms_endpoints: &[String],
        options: ObjectClientOptions,
        client_count: usize,
    ) -> Result<Self, DynError> {
        let mut clients = Vec::with_capacity(client_count);
        for _ in 0..client_count {
            let client = ObjectClient::connect_with_options(kms_endpoints, options.clone())
                .await
                .map_err(|err| boxed_error(err.to_string()))?;
            clients.push(Arc::new(tokio::sync::Mutex::new(client)));
        }
        Ok(Self {
            clients: Arc::new(clients),
            next: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn select(&self) -> Arc<tokio::sync::Mutex<ObjectClient>> {
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        Arc::clone(&self.clients[index % self.clients.len()])
    }

    async fn get_object(&self, bucket_id: &str, key: &str) -> Result<Vec<u8>, DynError> {
        let client = self.select();
        let mut client = client.lock().await;
        client
            .get_object_single_stripe(bucket_id, key)
            .await
            .map(|result| result.payload)
            .map_err(|err| boxed_error(err.to_string()))
    }

    async fn put_object(
        &self,
        bucket_id: &str,
        key: &str,
        payload: Vec<u8>,
    ) -> Result<(), DynError> {
        let client = self.select();
        let mut client = client.lock().await;
        client
            .put_object_single_stripe(bucket_id, key, &payload)
            .await
            .map(|_| ())
            .map_err(|err| boxed_error(err.to_string()))
    }

    async fn put_object_from_path(
        &self,
        bucket_id: &str,
        key: &str,
        path: &Path,
    ) -> Result<(), DynError> {
        let client = self.select();
        let mut client = client.lock().await;
        client
            .put_object_from_path(bucket_id, key, path)
            .await
            .map(|_| ())
            .map_err(|err| boxed_error(err.to_string()))
    }

    async fn delete_object(&self, bucket_id: &str, key: &str) -> Result<(), DynError> {
        let client = self.select();
        let mut client = client.lock().await;
        client
            .delete_object(bucket_id, key, &[])
            .await
            .map(|_| ())
            .map_err(|err| boxed_error(err.to_string()))
    }
}

#[cfg(feature = "fuse")]
fn spill_path_for_handle(fh: u64) -> PathBuf {
    std::env::temp_dir().join(format!("keinfs-kfc-{}-{}.spill", std::process::id(), fh))
}

#[cfg(feature = "fuse")]
fn create_spill_buffer(fh: u64, payload: &[u8]) -> Result<HandleBuffer, Errno> {
    let path = spill_path_for_handle(fh);
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|_| Errno::EIO)?;
    if !payload.is_empty() {
        file.write_all(payload).map_err(|_| Errno::EIO)?;
    }
    Ok(HandleBuffer::Spill(SpillFile {
        path,
        len: payload.len(),
    }))
}

#[cfg(feature = "fuse")]
fn materialize_spill_buffer(buffer: &mut HandleBuffer, fh: u64) -> Result<&mut SpillFile, Errno> {
    if let HandleBuffer::Shared(payload) = buffer {
        *buffer = create_spill_buffer(fh, payload.as_slice())?;
    }
    match buffer {
        HandleBuffer::Spill(spill) => Ok(spill),
        HandleBuffer::Shared(_) => Err(Errno::EIO),
    }
}

#[cfg(feature = "fuse")]
fn cleanup_spill_path(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[cfg(feature = "fuse")]
pub(crate) fn run_mount(config: MountConfig) -> Result<(), DynError> {
    let mountpoint = config.mountpoint.clone();
    let fs = KfcFs::new(config)?;
    let mut options = Config::default();
    options.n_threads = Some(
        std::thread::available_parallelism()
            .map(|parallelism| parallelism.get().min(32))
            .unwrap_or(4),
    );
    #[cfg(target_os = "linux")]
    {
        options.clone_fd = true;
    }
    options.mount_options = vec![
        MountOption::FSName("keinfs".to_string()),
        MountOption::Async,
        MountOption::NoAtime,
        MountOption::DefaultPermissions,
    ];
    fuser::mount2(fs, &mountpoint, &options).map_err(|err| boxed_error(err.to_string()))
}

#[cfg(not(feature = "fuse"))]
pub(crate) fn run_mount(_config: MountConfig) -> Result<(), DynError> {
    Err(boxed_error(
        "KFC mount support was built without the `fuse` feature",
    ))
}

#[cfg(feature = "fuse")]
struct KfcFs {
    namespace_id: String,
    bucket_id: String,
    coherent_data_cache: bool,
    driver: AsyncDriver,
    metadata: MetadataClient,
    read_clients: ObjectClientPool,
    write_clients: ObjectClientPool,
    next_ino: AtomicU64,
    next_fh: AtomicU64,
    state: Arc<Mutex<FsState>>,
}

#[cfg(feature = "fuse")]
impl KfcFs {
    fn new(config: MountConfig) -> Result<Self, DynError> {
        let driver = AsyncDriver::new()?;
        let object_options = ObjectClientOptions {
            read_completion_mode: config.read_completion_mode,
            write_completion_mode: config.write_completion_mode,
            metadata_notification_nats_url: config.metadata_notification_nats_url.clone(),
            metadata_notification_subject: config.metadata_notification_subject.clone(),
            ..ObjectClientOptions::default()
        };
        let object_client_pool_size = object_client_pool_size();
        let metadata = driver.block_on({
            let endpoints = config.kms_endpoints.clone();
            async move { MetadataClient::connect(&endpoints).await }
        })?;
        let read_clients = driver.block_on({
            let endpoints = config.kms_endpoints.clone();
            let object_options = object_options.clone();
            async move {
                ObjectClientPool::connect(&endpoints, object_options, object_client_pool_size).await
            }
        })?;
        let write_clients = driver.block_on({
            let endpoints = config.kms_endpoints.clone();
            let object_options = object_options.clone();
            async move {
                ObjectClientPool::connect(&endpoints, object_options, object_client_pool_size).await
            }
        })?;
        let bucket_entry_id = driver.block_on({
            let metadata = metadata.clone();
            let bucket_id = config.bucket_id.clone();
            async move { metadata.bucket_entry_id(&bucket_id).await }
        })?;
        let mut state = FsState::default();
        let root = Node {
            ino: ROOT_INO,
            parent: ROOT_INO,
            name: String::new(),
            entry_id: bucket_entry_id,
            key: String::new(),
            kind: NodeKind::Directory,
            size: 0,
            size_loaded_at: Some(Instant::now()),
            children_loaded_at: None,
        };
        state.nodes_by_entry.insert(root.entry_id.clone(), root.ino);
        state.nodes_by_key.insert(root.key.clone(), root.ino);
        state.nodes_by_ino.insert(root.ino, root);
        let state = Arc::new(Mutex::new(state));
        if let Some(nats_url) = config.metadata_notification_nats_url.clone() {
            spawn_state_invalidator(
                driver.handle.clone(),
                Arc::clone(&state),
                config.namespace_id.clone(),
                nats_url,
                config.metadata_notification_subject.clone(),
            );
        }
        Ok(Self {
            namespace_id: config.namespace_id,
            bucket_id: config.bucket_id,
            coherent_data_cache: config.metadata_notification_nats_url.is_some(),
            driver,
            metadata,
            read_clients,
            write_clients,
            next_ino: AtomicU64::new(ROOT_INO + 1),
            next_fh: AtomicU64::new(1),
            state,
        })
    }

    fn node(&self, ino: u64) -> Result<Node, Errno> {
        self.state
            .lock()
            .map_err(|_| Errno::EIO)?
            .nodes_by_ino
            .get(&ino)
            .cloned()
            .ok_or(Errno::ENOENT)
    }

    fn ensure_directory(node: &Node) -> Result<(), Errno> {
        if matches!(node.kind, NodeKind::Directory) {
            Ok(())
        } else {
            Err(Errno::ENOTDIR)
        }
    }

    fn child_key(parent_key: &str, name: &str) -> String {
        if parent_key.is_empty() {
            name.to_string()
        } else {
            format!("{parent_key}/{name}")
        }
    }

    fn collection_entry_id(parent: &Node, name: &str) -> String {
        let key = Self::child_key(&parent.key, name);
        format!("kfc:collection:{}", key.replace('/', ":"))
    }

    fn node_attr(&self, node: &Node) -> FileAttr {
        let now = SystemTime::now();
        let kind = match node.kind {
            NodeKind::Directory => FileType::Directory,
            NodeKind::File => FileType::RegularFile,
        };
        FileAttr {
            ino: INodeNo(node.ino),
            size: node.size,
            blocks: node.size.div_ceil(512),
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind,
            perm: match node.kind {
                NodeKind::Directory => 0o755,
                NodeKind::File => 0o644,
            },
            nlink: match node.kind {
                NodeKind::Directory => 2,
                NodeKind::File => 1,
            },
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 1 << 20,
            flags: 0,
        }
    }

    fn fresh(loaded_at: Option<Instant>) -> bool {
        loaded_at
            .map(|loaded_at| loaded_at.elapsed() <= META_CACHE_TTL)
            .unwrap_or(false)
    }

    fn populate_child(&self, parent: &Node, entry: NamespaceDomainEntry) -> Result<Node, Errno> {
        let kind = match NamespaceEntryKind::try_from(entry.kind)
            .unwrap_or(NamespaceEntryKind::Unspecified)
        {
            NamespaceEntryKind::Object => NodeKind::File,
            NamespaceEntryKind::Collection | NamespaceEntryKind::Bucket => NodeKind::Directory,
            _ => NodeKind::Directory,
        };
        let key = if parent.key.is_empty() {
            entry.name.clone()
        } else {
            format!("{}/{}", parent.key, entry.name)
        };
        let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
        if let Some(existing_ino) = state.nodes_by_entry.get(&entry.entry_id).copied() {
            return state
                .nodes_by_ino
                .get(&existing_ino)
                .cloned()
                .ok_or(Errno::ENOENT);
        }
        if let Some(existing_ino) = state.nodes_by_key.get(&key).copied() {
            let node_snapshot = {
                let node = state
                    .nodes_by_ino
                    .get_mut(&existing_ino)
                    .ok_or(Errno::ENOENT)?;
                node.entry_id = entry.entry_id.clone();
                node.parent = parent.ino;
                node.name = entry.name.clone();
                node.kind = kind.clone();
                node.size_loaded_at = None;
                node.children_loaded_at = None;
                node.clone()
            };
            state.nodes_by_entry.insert(entry.entry_id, existing_ino);
            return Ok(node_snapshot);
        }
        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
        let node = Node {
            ino,
            parent: parent.ino,
            name: entry.name.clone(),
            entry_id: entry.entry_id.clone(),
            key: key.clone(),
            kind,
            size: 0,
            size_loaded_at: None,
            children_loaded_at: None,
        };
        state.nodes_by_entry.insert(entry.entry_id, ino);
        state.nodes_by_key.insert(key, ino);
        state.nodes_by_ino.insert(ino, node.clone());
        Ok(node)
    }

    fn file_open_flags(&self) -> FopenFlags {
        if self.coherent_data_cache {
            FopenFlags::FOPEN_DIRECT_IO
        } else {
            FopenFlags::empty()
        }
    }

    fn prune_missing_children(
        &self,
        parent_ino: u64,
        live_entry_ids: &HashSet<String>,
    ) -> Result<(), Errno> {
        let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
        let open_inos = state
            .handles
            .values()
            .map(|handle| handle.ino)
            .collect::<HashSet<_>>();
        let stale_inos = state
            .nodes_by_ino
            .values()
            .filter(|node| {
                node.parent == parent_ino
                    && node.ino != parent_ino
                    && !live_entry_ids.contains(&node.entry_id)
                    && !open_inos.contains(&node.ino)
            })
            .map(|node| node.ino)
            .collect::<Vec<_>>();
        for ino in stale_inos {
            if let Some(node) = state.nodes_by_ino.remove(&ino) {
                state.nodes_by_entry.remove(&node.entry_id);
                state.nodes_by_key.remove(&node.key);
                state.blob_cache.remove(&node.key);
            }
        }
        Ok(())
    }

    fn refresh_file_size(&self, ino: u64) -> Result<Node, Errno> {
        {
            let state = self.state.lock().map_err(|_| Errno::EIO)?;
            let node = state.nodes_by_ino.get(&ino).cloned().ok_or(Errno::ENOENT)?;
            if !matches!(node.kind, NodeKind::File) {
                return Ok(node);
            }
            if Self::fresh(node.size_loaded_at) {
                return Ok(node);
            }
        }
        let bucket_id = self.bucket_id.clone();
        let key = self.node(ino)?.key;
        let size = self
            .driver
            .block_on({
                let metadata = self.metadata.clone();
                async move { metadata.resolve_object_size(&bucket_id, &key).await }
            })
            .map_err(|err| error_code(&err))?;
        let now = Instant::now();
        let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
        let node = state.nodes_by_ino.get_mut(&ino).ok_or(Errno::ENOENT)?;
        node.size = size;
        node.size_loaded_at = Some(now);
        Ok(node.clone())
    }

    fn list_children(&self, parent: &Node) -> Result<Vec<Node>, Errno> {
        Self::ensure_directory(parent)?;
        {
            let state = self.state.lock().map_err(|_| Errno::EIO)?;
            if let Some(cached_parent) = state.nodes_by_ino.get(&parent.ino) {
                if Self::fresh(cached_parent.children_loaded_at) {
                    let mut nodes = state
                        .nodes_by_ino
                        .values()
                        .filter(|node| node.parent == parent.ino && node.ino != parent.ino)
                        .cloned()
                        .collect::<Vec<_>>();
                    nodes.sort_by(|left, right| left.name.cmp(&right.name));
                    return Ok(nodes);
                }
            }
        }
        let namespace_id = self.namespace_id.clone();
        let parent_entry_id = parent.entry_id.clone();
        let entries = self
            .driver
            .block_on({
                let metadata = self.metadata.clone();
                async move {
                    metadata
                        .list_children_all(&namespace_id, &parent_entry_id, 256)
                        .await
                }
            })
            .map_err(|err| error_code(&err))?;
        let live_entry_ids = entries
            .iter()
            .map(|entry| entry.entry_id.clone())
            .collect::<HashSet<_>>();
        self.prune_missing_children(parent.ino, &live_entry_ids)?;
        let mut nodes = Vec::with_capacity(entries.len());
        for entry in entries {
            nodes.push(self.populate_child(parent, entry)?);
        }
        nodes.sort_by(|left, right| left.name.cmp(&right.name));
        let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
        if let Some(parent_node) = state.nodes_by_ino.get_mut(&parent.ino) {
            parent_node.children_loaded_at = Some(Instant::now());
        }
        Ok(nodes)
    }

    fn open_handle(&self, node: &Node, truncate: bool, writable: bool) -> Result<u64, Errno> {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        let buffer = if truncate {
            if let Ok(mut state) = self.state.lock() {
                state.blob_cache.remove(&node.key);
            }
            create_spill_buffer(fh, &[])?
        } else if writable {
            let cached = self
                .state
                .lock()
                .map_err(|_| Errno::EIO)?
                .blob_cache
                .get(&node.key)
                .cloned();
            if let Some(buffer) = cached {
                create_spill_buffer(fh, buffer.as_slice())?
            } else {
                let bucket_id = self.bucket_id.clone();
                let key = node.key.clone();
                let read_clients = self.read_clients.clone();
                let payload = self
                    .driver
                    .block_on(async move { read_clients.get_object(&bucket_id, &key).await })
                    .map_err(|err| error_code(&err))?;
                let now = Instant::now();
                {
                    let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
                    if let Some(node_state) = state.nodes_by_ino.get_mut(&node.ino) {
                        node_state.size = payload.len() as u64;
                        node_state.size_loaded_at = Some(now);
                    }
                    if payload.len() <= WRITEBACK_CACHE_MAX_BYTES {
                        if state.blob_cache.len() >= BLOB_CACHE_MAX_ENTRIES {
                            state.blob_cache.clear();
                        }
                        state
                            .blob_cache
                            .insert(node.key.clone(), Arc::new(payload.clone()));
                    }
                }
                create_spill_buffer(fh, &payload)?
            }
        } else {
            // Keep the cache probe in its own statement so the mutex guard is
            // dropped before the cold-read path needs to lock state again.
            let cached = self
                .state
                .lock()
                .map_err(|_| Errno::EIO)?
                .blob_cache
                .get(&node.key)
                .cloned();
            if let Some(buffer) = cached {
                HandleBuffer::Shared(buffer)
            } else {
                let bucket_id = self.bucket_id.clone();
                let key = node.key.clone();
                let read_clients = self.read_clients.clone();
                let payload = self
                    .driver
                    .block_on(async move { read_clients.get_object(&bucket_id, &key).await })
                    .map_err(|err| error_code(&err))?;
                let payload = Arc::new(payload);
                let now = Instant::now();
                let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
                if state.blob_cache.len() >= BLOB_CACHE_MAX_ENTRIES {
                    state.blob_cache.clear();
                }
                state
                    .blob_cache
                    .insert(node.key.clone(), Arc::clone(&payload));
                if let Some(node_state) = state.nodes_by_ino.get_mut(&node.ino) {
                    node_state.size = payload.len() as u64;
                    node_state.size_loaded_at = Some(now);
                }
                HandleBuffer::Shared(payload)
            }
        };
        self.state.lock().map_err(|_| Errno::EIO)?.handles.insert(
            fh,
            HandleState {
                ino: node.ino,
                key: node.key.clone(),
                buffer,
                dirty: truncate,
            },
        );
        Ok(fh)
    }

    fn truncate_open_handle(&self, ino: u64, fh: u64, size: u64) -> Result<(), Errno> {
        let size = usize::try_from(size).map_err(|_| Errno::EFBIG)?;
        let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
        let handle_key = state.handles.get(&fh).ok_or(Errno::EBADF)?.key.clone();
        state.blob_cache.remove(&handle_key);
        let handle = state.handles.get_mut(&fh).ok_or(Errno::EBADF)?;
        let spill = materialize_spill_buffer(&mut handle.buffer, fh)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&spill.path)
            .map_err(|_| Errno::EIO)?;
        file.set_len(size as u64).map_err(|_| Errno::EIO)?;
        spill.len = size;
        handle.dirty = true;
        if let Some(node) = state.nodes_by_ino.get_mut(&ino) {
            node.size = size as u64;
            node.size_loaded_at = Some(Instant::now());
        }
        Ok(())
    }

    fn truncate_file(&self, ino: u64, size: u64, fh: Option<u64>) -> Result<FileAttr, Errno> {
        let node = self.node(ino)?;
        if !matches!(node.kind, NodeKind::File) {
            return Err(Errno::EISDIR);
        }
        if let Some(fh) = fh {
            self.truncate_open_handle(ino, fh, size)?;
        } else {
            let fh = self.open_handle(&node, false, true)?;
            let truncate_result = self.truncate_open_handle(ino, fh, size);
            let commit_result = truncate_result.and_then(|_| commit_handle(self, fh));
            if let Ok(mut state) = self.state.lock() {
                if let Some(handle) = state.handles.remove(&fh) {
                    if let Some(path) = handle.buffer.spill_path() {
                        cleanup_spill_path(path);
                    }
                }
            }
            commit_result?;
        }
        Ok(self.node_attr(&self.node(ino)?))
    }
}

#[cfg(feature = "fuse")]
impl Filesystem for KfcFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let result = (|| -> Result<FileAttr, Errno> {
            let parent_node = self.node(parent.0)?;
            let children = self.list_children(&parent_node)?;
            let child = children
                .into_iter()
                .find(|child| child.name == name.to_string_lossy())
                .ok_or(Errno::ENOENT)?;
            let child = if matches!(child.kind, NodeKind::File) {
                self.refresh_file_size(child.ino)?
            } else {
                child
            };
            Ok(self.node_attr(&child))
        })();
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(code) => reply.error(code),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let result = (|| -> Result<FileAttr, Errno> {
            let node = self.node(ino.0)?;
            let node = if matches!(node.kind, NodeKind::File) {
                self.refresh_file_size(ino.0)?
            } else {
                node
            };
            Ok(self.node_attr(&node))
        })();
        match result {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(code) => reply.error(code),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let result = (|| -> Result<FileAttr, Errno> {
            if mode.is_some() || uid.is_some() || gid.is_some() {
                return Err(Errno::EPERM);
            }
            if let Some(size) = size {
                return self.truncate_file(ino.0, size, fh.map(|handle| handle.0));
            }
            let node = self.node(ino.0)?;
            Ok(self.node_attr(&node))
        })();
        match result {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(code) => reply.error(code),
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match self
            .node(ino.0)
            .and_then(|node| Self::ensure_directory(&node))
        {
            Ok(()) => reply.opened(FileHandle(0), FopenFlags::empty()),
            Err(code) => reply.error(code),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let result = (|| -> Result<FileAttr, Errno> {
            let parent_node = self.node(parent.0)?;
            Self::ensure_directory(&parent_node)?;
            let name = name.to_string_lossy().to_string();
            if self
                .list_children(&parent_node)?
                .into_iter()
                .any(|child| child.name == name)
            {
                return Err(Errno::EEXIST);
            }
            let entry = self
                .driver
                .block_on({
                    let metadata = self.metadata.clone();
                    let namespace_id = self.namespace_id.clone();
                    let parent_entry_id = parent_node.entry_id.clone();
                    let entry_id = Self::collection_entry_id(&parent_node, &name);
                    let entry_name = name.clone();
                    async move {
                        metadata
                            .create_collection(
                                &namespace_id,
                                &parent_entry_id,
                                &entry_id,
                                &entry_name,
                            )
                            .await
                    }
                })
                .map_err(|err| error_code(&err))?;
            let node = self.populate_child(&parent_node, entry)?;
            if let Ok(mut state) = self.state.lock() {
                if let Some(parent_node) = state.nodes_by_ino.get_mut(&parent.0) {
                    parent_node.children_loaded_at = Some(Instant::now());
                }
            }
            let mut attr = self.node_attr(&node);
            attr.perm = (mode & 0o777) as u16;
            Ok(attr)
        })();
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(code) => reply.error(code),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let result = (|| -> Result<(), Errno> {
            let node = self.node(ino.0)?;
            let children = self.list_children(&node)?;
            let mut entries = Vec::with_capacity(children.len() + 2);
            entries.push((node.ino, FileType::Directory, ".".to_string()));
            entries.push((node.parent, FileType::Directory, "..".to_string()));
            for child in children {
                let kind = match child.kind {
                    NodeKind::Directory => FileType::Directory,
                    NodeKind::File => FileType::RegularFile,
                };
                entries.push((child.ino, kind, child.name));
            }
            for (index, (child_ino, file_type, name)) in
                entries.into_iter().enumerate().skip(offset as usize)
            {
                if reply.add(INodeNo(child_ino), (index + 1) as u64, file_type, name) {
                    break;
                }
            }
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let result = (|| -> Result<u64, Errno> {
            let node = self.node(ino.0)?;
            if !matches!(node.kind, NodeKind::File) {
                return Err(Errno::EISDIR);
            }
            let truncate = flags.0 & libc::O_TRUNC != 0;
            let writable = (flags.0 & libc::O_ACCMODE) != libc::O_RDONLY;
            self.open_handle(&node, truncate, writable)
        })();
        match result {
            Ok(fh) => reply.opened(FileHandle(fh), self.file_open_flags()),
            Err(code) => reply.error(code),
        }
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let result = (|| -> Result<(FileAttr, u64), Errno> {
            let parent_node = self.node(parent.0)?;
            Self::ensure_directory(&parent_node)?;
            let name = name.to_string_lossy().to_string();
            let key = Self::child_key(&parent_node.key, &name);
            let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
            if state.nodes_by_key.contains_key(&key) {
                return Err(Errno::EEXIST);
            }
            let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
            let node = Node {
                ino,
                parent: parent.0,
                name,
                entry_id: format!("pending:{key}"),
                key,
                kind: NodeKind::File,
                size: 0,
                size_loaded_at: Some(Instant::now()),
                children_loaded_at: None,
            };
            let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
            state.nodes_by_entry.insert(node.entry_id.clone(), node.ino);
            state.nodes_by_key.insert(node.key.clone(), node.ino);
            state.nodes_by_ino.insert(node.ino, node.clone());
            if let Some(parent_node) = state.nodes_by_ino.get_mut(&parent.0) {
                parent_node.children_loaded_at = Some(Instant::now());
            }
            state.handles.insert(
                fh,
                HandleState {
                    ino: node.ino,
                    key: node.key.clone(),
                    buffer: create_spill_buffer(fh, &[])?,
                    dirty: true,
                },
            );
            let mut attr = self.node_attr(&node);
            attr.perm = (mode & 0o777) as u16;
            Ok((attr, fh))
        })();
        match result {
            Ok((attr, fh)) => reply.created(
                &TTL,
                &attr,
                Generation(0),
                FileHandle(fh),
                self.file_open_flags(),
            ),
            Err(code) => reply.error(code),
        }
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let result = (|| -> Result<Vec<u8>, Errno> {
            let state = self.state.lock().map_err(|_| Errno::EIO)?;
            let handle = state.handles.get(&fh.0).ok_or(Errno::EBADF)?;
            handle.buffer.read_range(offset as usize, size as usize)
        })();
        match result {
            Ok(data) => reply.data(&data),
            Err(code) => reply.error(code),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let result = (|| -> Result<u32, Errno> {
            let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
            let handle_key = state.handles.get(&fh.0).ok_or(Errno::EBADF)?.key.clone();
            state.blob_cache.remove(&handle_key);
            let handle = state.handles.get_mut(&fh.0).ok_or(Errno::EBADF)?;
            let start = offset as usize;
            let end = start.saturating_add(data.len());
            let spill = materialize_spill_buffer(&mut handle.buffer, fh.0)?;
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&spill.path)
                .map_err(|_| Errno::EIO)?;
            if spill.len < end {
                file.set_len(end as u64).map_err(|_| Errno::EIO)?;
                spill.len = end;
            }
            file.seek(SeekFrom::Start(start as u64))
                .map_err(|_| Errno::EIO)?;
            file.write_all(data).map_err(|_| Errno::EIO)?;
            handle.dirty = true;
            if let Some(node) = state.nodes_by_ino.get_mut(&ino.0) {
                node.size = node.size.max(end as u64);
                node.size_loaded_at = Some(Instant::now());
            }
            Ok(data.len() as u32)
        })();
        match result {
            Ok(written) => reply.written(written),
            Err(code) => reply.error(code),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        match commit_handle(self, fh.0) {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match commit_handle(self, fh.0) {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let result = commit_handle(self, fh.0);
        if let Ok(mut state) = self.state.lock() {
            if let Some(handle) = state.handles.remove(&fh.0) {
                if let Some(path) = handle.buffer.spill_path() {
                    cleanup_spill_path(path);
                }
            }
        }
        match result {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let result = (|| -> Result<(), Errno> {
            let parent_node = self.node(parent.0)?;
            Self::ensure_directory(&parent_node)?;
            let key = Self::child_key(&parent_node.key, &name.to_string_lossy());
            let bucket_id = self.bucket_id.clone();
            let write_clients = self.write_clients.clone();
            self.driver
                .block_on({
                    let key = key.clone();
                    async move { write_clients.delete_object(&bucket_id, &key).await }
                })
                .map_err(|err| error_code(&err))?;
            let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
            if let Some(ino) = state.nodes_by_key.remove(&key) {
                state.blob_cache.remove(&key);
                if let Some(node) = state.nodes_by_ino.remove(&ino) {
                    state.nodes_by_entry.remove(&node.entry_id);
                }
            }
            if let Some(parent_state) = state.nodes_by_ino.get_mut(&parent.0) {
                parent_state.children_loaded_at = Some(Instant::now());
            }
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let result = (|| -> Result<(), Errno> {
            let parent_node = self.node(parent.0)?;
            Self::ensure_directory(&parent_node)?;
            let name = name.to_string_lossy().to_string();
            let children = self.list_children(&parent_node)?;
            let child = children
                .into_iter()
                .find(|child| child.name == name)
                .ok_or(Errno::ENOENT)?;
            Self::ensure_directory(&child)?;
            if !self.list_children(&child)?.is_empty() {
                return Err(Errno::ENOTEMPTY);
            }
            let namespace_id = self.namespace_id.clone();
            let entry_id = child.entry_id.clone();
            self.driver
                .block_on({
                    let metadata = self.metadata.clone();
                    async move {
                        metadata
                            .delete_namespace_entry(&namespace_id, &entry_id)
                            .await
                    }
                })
                .map_err(|err| error_code(&err))?;
            let mut state = self.state.lock().map_err(|_| Errno::EIO)?;
            state.nodes_by_entry.remove(&child.entry_id);
            state.nodes_by_key.remove(&child.key);
            state.nodes_by_ino.remove(&child.ino);
            if let Some(parent_state) = state.nodes_by_ino.get_mut(&parent.0) {
                parent_state.children_loaded_at = Some(Instant::now());
            }
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 1 << 20, 255, 0);
    }
}

#[cfg(feature = "fuse")]
fn commit_handle(fs: &KfcFs, fh: u64) -> Result<(), Errno> {
    let handle = {
        let state = fs.state.lock().map_err(|_| Errno::EIO)?;
        state.handles.get(&fh).cloned().ok_or(Errno::EBADF)?
    };
    if !handle.dirty {
        return Ok(());
    }
    let bucket_id = fs.bucket_id.clone();
    let key = handle.key.clone();
    let write_clients = fs.write_clients.clone();
    match &handle.buffer {
        HandleBuffer::Shared(payload) => {
            let payload = payload.as_ref().clone();
            let upload_key = key.clone();
            fs.driver
                .block_on(async move {
                    write_clients
                        .put_object(&bucket_id, &upload_key, payload)
                        .await
                })
                .map_err(|err| error_code(&err))?;
        }
        HandleBuffer::Spill(spill) => {
            let spill_path = spill.path.clone();
            let upload_key = key.clone();
            fs.driver
                .block_on(async move {
                    write_clients
                        .put_object_from_path(&bucket_id, &upload_key, &spill_path)
                        .await
                })
                .map_err(|err| error_code(&err))?;
        }
    }
    let cached_payload = handle.buffer.small_cached_payload()?;
    let mut state = fs.state.lock().map_err(|_| Errno::EIO)?;
    if let Some(cached_payload) = cached_payload {
        if state.blob_cache.len() >= BLOB_CACHE_MAX_ENTRIES {
            state.blob_cache.clear();
        }
        state
            .blob_cache
            .insert(key.clone(), Arc::clone(&cached_payload));
        if let Some(path) = handle.buffer.spill_path() {
            cleanup_spill_path(path);
        }
        if let Some(node) = state.nodes_by_ino.get_mut(&handle.ino) {
            node.size = cached_payload.len() as u64;
            node.size_loaded_at = Some(Instant::now());
        }
        if let Some(open_handle) = state.handles.get_mut(&fh) {
            open_handle.dirty = false;
            open_handle.buffer = HandleBuffer::Shared(cached_payload);
        }
    } else {
        state.blob_cache.remove(&key);
        if let Some(node) = state.nodes_by_ino.get_mut(&handle.ino) {
            node.size = handle.buffer.len() as u64;
            node.size_loaded_at = Some(Instant::now());
        }
        if let Some(open_handle) = state.handles.get_mut(&fh) {
            open_handle.dirty = false;
        }
    }
    Ok(())
}

#[cfg(feature = "fuse")]
fn error_code(err: &DynError) -> Errno {
    let message = err.to_string().to_ascii_lowercase();
    eprintln!("KFC backend error: {}", err);
    if message.contains("not found")
        || message.contains("no manifest")
        || message.contains("enoent")
    {
        Errno::ENOENT
    } else if message.contains("not empty") {
        Errno::ENOTEMPTY
    } else if message.contains("not implemented") || message.contains("not supported") {
        Errno::ENOSYS
    } else {
        Errno::EIO
    }
}

#[cfg(feature = "fuse")]
fn object_client_pool_size() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| {
            parallelism
                .get()
                .clamp(OBJECT_CLIENT_POOL_MIN, OBJECT_CLIENT_POOL_MAX)
        })
        .unwrap_or(OBJECT_CLIENT_POOL_MIN)
}

#[cfg(feature = "fuse")]
fn spawn_state_invalidator(
    handle: tokio::runtime::Handle,
    state: Arc<Mutex<FsState>>,
    namespace_id: String,
    nats_url: String,
    subject: String,
) {
    handle.spawn(async move {
        state_invalidation_loop(state, namespace_id, nats_url, subject).await;
    });
}

#[cfg(feature = "fuse")]
async fn state_invalidation_loop(
    state: Arc<Mutex<FsState>>,
    namespace_id: String,
    nats_url: String,
    subject: String,
) {
    let client = match async_nats::connect(nats_url.as_str()).await {
        Ok(client) => client,
        Err(err) => {
            eprintln!("KFC metadata invalidation could not connect to NATS {nats_url}: {err}");
            return;
        }
    };
    let mut subscriber = match client.subscribe(subject.clone()).await {
        Ok(subscriber) => subscriber,
        Err(err) => {
            eprintln!("KFC metadata invalidation could not subscribe to {subject}: {err}");
            return;
        }
    };
    while let Some(message) = subscriber.next().await {
        if let Some(event) = decode_metadata_invalidation_event(message.payload.as_ref()) {
            apply_state_invalidation(&state, &namespace_id, event);
        }
    }
}

#[cfg(feature = "fuse")]
fn decode_metadata_invalidation_event(payload: &[u8]) -> Option<MetadataInvalidationEvent> {
    match MetadataInvalidationEvent::decode(payload) {
        Ok(event) => Some(event),
        Err(_) => String::from_utf8(payload.to_vec())
            .ok()
            .map(|namespace_id| MetadataInvalidationEvent {
                namespace_id,
                bucket_id: String::new(),
                key: String::new(),
                entry_id: String::new(),
                parent_entry_id: String::new(),
                event_kind: 0,
                version_id: String::new(),
            }),
    }
}

#[cfg(feature = "fuse")]
fn apply_state_invalidation(
    state: &Arc<Mutex<FsState>>,
    namespace_id: &str,
    event: MetadataInvalidationEvent,
) {
    if !event.namespace_id.is_empty() && event.namespace_id != namespace_id {
        return;
    }
    let mut state = match state.lock() {
        Ok(state) => state,
        Err(_) => return,
    };
    if event.namespace_id.is_empty()
        || (event.key.is_empty() && event.entry_id.is_empty() && event.parent_entry_id.is_empty())
    {
        state.blob_cache.clear();
        for node in state.nodes_by_ino.values_mut() {
            node.size_loaded_at = None;
            node.children_loaded_at = None;
        }
        return;
    }
    if !event.key.is_empty() {
        state.blob_cache.remove(&event.key);
        if let Some(ino) = state.nodes_by_key.get(&event.key).copied() {
            if let Some(node) = state.nodes_by_ino.get_mut(&ino) {
                node.size_loaded_at = None;
            }
        }
        let parent_key = event
            .key
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or("");
        if let Some(parent_ino) = state.nodes_by_key.get(parent_key).copied() {
            if let Some(node) = state.nodes_by_ino.get_mut(&parent_ino) {
                node.children_loaded_at = None;
            }
        }
    }
    if !event.entry_id.is_empty() {
        if let Some(ino) = state.nodes_by_entry.get(&event.entry_id).copied() {
            if let Some(node) = state.nodes_by_ino.get_mut(&ino) {
                node.size_loaded_at = None;
                node.children_loaded_at = None;
            }
        }
    }
    if !event.parent_entry_id.is_empty() {
        if let Some(parent_ino) = state.nodes_by_entry.get(&event.parent_entry_id).copied() {
            if let Some(node) = state.nodes_by_ino.get_mut(&parent_ino) {
                node.children_loaded_at = None;
            }
        }
    }
}

#[cfg(all(test, feature = "fuse"))]
mod tests {
    use super::*;

    fn sample_state() -> Arc<Mutex<FsState>> {
        let mut state = FsState::default();
        let root = Node {
            ino: ROOT_INO,
            parent: ROOT_INO,
            name: String::new(),
            entry_id: "root".to_string(),
            key: String::new(),
            kind: NodeKind::Directory,
            size: 0,
            size_loaded_at: Some(Instant::now()),
            children_loaded_at: Some(Instant::now()),
        };
        let file = Node {
            ino: ROOT_INO + 1,
            parent: ROOT_INO,
            name: "file.txt".to_string(),
            entry_id: "entry-1".to_string(),
            key: "file.txt".to_string(),
            kind: NodeKind::File,
            size: 7,
            size_loaded_at: Some(Instant::now()),
            children_loaded_at: None,
        };
        state.nodes_by_entry.insert(root.entry_id.clone(), root.ino);
        state.nodes_by_key.insert(root.key.clone(), root.ino);
        state.nodes_by_ino.insert(root.ino, root);
        state.nodes_by_entry.insert(file.entry_id.clone(), file.ino);
        state.nodes_by_key.insert(file.key.clone(), file.ino);
        state
            .blob_cache
            .insert(file.key.clone(), Arc::new(b"stale".to_vec()));
        state.nodes_by_ino.insert(file.ino, file);
        Arc::new(Mutex::new(state))
    }

    #[test]
    fn key_only_invalidation_clears_blob_size_and_parent_cache() {
        let state = sample_state();
        apply_state_invalidation(
            &state,
            "lab-ns",
            MetadataInvalidationEvent {
                namespace_id: "lab-ns".to_string(),
                bucket_id: "lab-8p2".to_string(),
                key: "file.txt".to_string(),
                entry_id: "entry-1".to_string(),
                parent_entry_id: String::new(),
                event_kind: 4,
                version_id: "v2".to_string(),
            },
        );
        let state = state.lock().expect("state");
        assert!(!state.blob_cache.contains_key("file.txt"));
        assert!(
            state
                .nodes_by_ino
                .get(&(ROOT_INO + 1))
                .expect("file node")
                .size_loaded_at
                .is_none()
        );
        assert!(
            state
                .nodes_by_ino
                .get(&ROOT_INO)
                .expect("root node")
                .children_loaded_at
                .is_none()
        );
    }
}
