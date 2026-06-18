// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! Sharded, lock-free-read filesystem state.
//!
//! This replaces the original single `Arc<std::sync::Mutex<FsState>>`
//! (`poc/kfc/src/mount.rs:369`) that serialized every FUSE op — including
//! holding the lock across spill-file I/O — down to one in-flight op. Here the
//! inode/dentry/key/handle tables are [`DashMap`]s (sharded, concurrent), and
//! each inode's mutable fields sit behind their own `RwLock`. An op on inode A
//! takes only A's lock; nothing process-wide is ever held, and no lock is held
//! across an `.await`.

use crate::types::{FileKind, ROOT_INO};
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

/// Mutable per-inode metadata. Cloned out under the lock for read; never held
/// across an await.
#[derive(Clone, Debug)]
pub(crate) struct InodeState {
    pub ino: u64,
    pub parent: u64,
    pub name: String,
    pub entry_id: String,
    pub key: String,
    pub kind: FileKind,
    pub size: u64,
    /// When the size was last resolved from KMS (TTL gate). `None` = stale.
    pub size_loaded_at: Option<Instant>,
    /// When this directory's children were last listed (TTL gate).
    pub children_loaded_at: Option<Instant>,
}

/// An inode: its mutable state behind a per-inode lock. The stable id lives in
/// `state.ino` (read via [`Inode::snapshot`]); no separate copy is kept.
pub(crate) struct Inode {
    pub state: RwLock<InodeState>,
}

impl Inode {
    pub fn new(state: InodeState) -> Arc<Self> {
        Arc::new(Self {
            state: RwLock::new(state),
        })
    }

    /// Cheap clone of the current state for use outside the lock.
    pub fn snapshot(&self) -> InodeState {
        self.state.read().expect("inode lock poisoned").clone()
    }
}

/// Per-open-handle staged contents.
///
/// Phase 1 keeps the staged buffer in memory (a `Vec<u8>`), behind a per-handle
/// lock, with whole-object read-on-open and whole-object commit-on-flush — i.e.
/// the original I/O *semantics*, but with the global lock and the blocking
/// dispatch bridge removed. Phase 2 replaces read-on-open with stripe-granular
/// ranged reads + the kernel page cache; Phase 3 reintroduces NVMe-backed
/// staging and streaming writeback (tasks #9/#10/#11).
pub(crate) struct HandleBuffer {
    pub data: Vec<u8>,
}

impl HandleBuffer {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn read_range(&self, offset: usize, size: usize) -> Vec<u8> {
        if offset >= self.data.len() {
            return Vec::new();
        }
        let end = self.data.len().min(offset.saturating_add(size));
        self.data[offset..end].to_vec()
    }

    pub fn write_at(&mut self, offset: usize, bytes: &[u8]) {
        let end = offset.saturating_add(bytes.len());
        if self.data.len() < end {
            self.data.resize(end, 0);
        }
        self.data[offset..end].copy_from_slice(bytes);
    }

    pub fn set_len(&mut self, len: usize) {
        self.data.resize(len, 0);
    }
}

/// The backing model of an open handle.
pub(crate) enum HandleData {
    /// Read-only handle: reads are served by stripe-granular ranged backend
    /// reads (Phase 2) — no whole-object load at open, no local staging.
    Ranged,
    /// Writable / truncate handle: contents staged in memory and committed as a
    /// whole object on flush (Phase 1 write semantics; Phase 3 streams).
    Staged(Mutex<HandleBuffer>),
}

/// An open file handle. A staged buffer (writable handles) is behind a
/// `std::sync::Mutex` held only for synchronous memory operations; backend
/// uploads clone the bytes out first, then await with no lock held.
pub(crate) struct FileHandle {
    pub ino: u64,
    pub key: String,
    pub writable: bool,
    pub data: HandleData,
    pub dirty: AtomicBool,
}

impl FileHandle {
    /// A read-only handle served via ranged backend reads.
    pub fn new_ranged(ino: u64, key: String) -> Arc<Self> {
        Arc::new(Self {
            ino,
            key,
            writable: false,
            data: HandleData::Ranged,
            dirty: AtomicBool::new(false),
        })
    }

    /// A writable/truncate handle with an in-memory staged buffer.
    pub fn new_staged(ino: u64, key: String, writable: bool, data: Vec<u8>, dirty: bool) -> Arc<Self> {
        Arc::new(Self {
            ino,
            key,
            writable,
            data: HandleData::Staged(Mutex::new(HandleBuffer::new(data))),
            dirty: AtomicBool::new(dirty),
        })
    }

    /// The staged buffer, or `None` for a ranged (read-only) handle.
    pub fn staged_buffer(&self) -> Option<&Mutex<HandleBuffer>> {
        match &self.data {
            HandleData::Staged(buffer) => Some(buffer),
            HandleData::Ranged => None,
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    pub fn set_dirty(&self, dirty: bool) {
        self.dirty.store(dirty, Ordering::Release);
    }
}

/// The whole filesystem's namespace + handle tables. All concurrent.
pub(crate) struct FsTables {
    pub by_ino: DashMap<u64, Arc<Inode>>,
    pub by_entry: DashMap<String, u64>,
    pub by_key: DashMap<String, u64>,
    pub handles: DashMap<u64, Arc<FileHandle>>,
    next_ino: AtomicU64,
    next_fh: AtomicU64,
}

impl FsTables {
    pub fn new() -> Self {
        Self {
            by_ino: DashMap::new(),
            by_entry: DashMap::new(),
            by_key: DashMap::new(),
            handles: DashMap::new(),
            next_ino: AtomicU64::new(ROOT_INO + 1),
            next_fh: AtomicU64::new(1),
        }
    }

    pub fn alloc_ino(&self) -> u64 {
        self.next_ino.fetch_add(1, Ordering::Relaxed)
    }

    pub fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    pub fn inode(&self, ino: u64) -> Option<Arc<Inode>> {
        self.by_ino.get(&ino).map(|entry| Arc::clone(entry.value()))
    }

    pub fn handle(&self, fh: u64) -> Option<Arc<FileHandle>> {
        self.handles.get(&fh).map(|entry| Arc::clone(entry.value()))
    }

    /// Insert an inode and index it by entry_id and key.
    pub fn insert_inode(&self, inode: Arc<Inode>) {
        let (ino, entry_id, key) = {
            let s = inode.state.read().expect("inode lock poisoned");
            (s.ino, s.entry_id.clone(), s.key.clone())
        };
        if !entry_id.is_empty() {
            self.by_entry.insert(entry_id, ino);
        }
        self.by_key.insert(key, ino);
        self.by_ino.insert(ino, inode);
    }

    /// Remove an inode and all its index entries.
    pub fn remove_inode(&self, ino: u64) -> Option<Arc<Inode>> {
        let inode = self.by_ino.remove(&ino).map(|(_, v)| v)?;
        let s = inode.snapshot();
        if !s.entry_id.is_empty() {
            self.by_entry.remove(&s.entry_id);
        }
        self.by_key.remove(&s.key);
        Some(inode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir_state(ino: u64, parent: u64, name: &str, key: &str) -> InodeState {
        InodeState {
            ino,
            parent,
            name: name.to_string(),
            entry_id: format!("entry-{ino}"),
            key: key.to_string(),
            kind: FileKind::Directory,
            size: 0,
            size_loaded_at: None,
            children_loaded_at: None,
        }
    }

    #[test]
    fn tables_index_by_ino_entry_and_key() {
        let tables = FsTables::new();
        let inode = Inode::new(dir_state(ROOT_INO, ROOT_INO, "", ""));
        tables.insert_inode(inode);
        assert!(tables.inode(ROOT_INO).is_some());
        assert_eq!(*tables.by_entry.get("entry-1").unwrap(), ROOT_INO);
        assert_eq!(*tables.by_key.get("").unwrap(), ROOT_INO);
        assert!(tables.remove_inode(ROOT_INO).is_some());
        assert!(tables.inode(ROOT_INO).is_none());
        assert!(tables.by_entry.get("entry-1").is_none());
    }

    #[test]
    fn handle_buffer_read_write_grow() {
        let mut buf = HandleBuffer::new(b"hello".to_vec());
        assert_eq!(buf.read_range(0, 5), b"hello");
        assert_eq!(buf.read_range(3, 100), b"lo");
        assert_eq!(buf.read_range(10, 4), b"");
        buf.write_at(5, b" world");
        assert_eq!(buf.len(), 11);
        assert_eq!(buf.read_range(0, 11), b"hello world");
        buf.set_len(5);
        assert_eq!(buf.read_range(0, 100), b"hello");
        buf.write_at(8, b"x"); // sparse grow zero-fills the gap
        assert_eq!(buf.len(), 9);
        assert_eq!(buf.data, b"hello\0\0\0x");
    }

    #[test]
    fn ids_are_monotonic_and_skip_root() {
        let tables = FsTables::new();
        assert_eq!(tables.alloc_ino(), ROOT_INO + 1);
        assert_eq!(tables.alloc_ino(), ROOT_INO + 2);
        assert_eq!(tables.alloc_fh(), 1);
        assert_eq!(tables.alloc_fh(), 2);
    }
}
