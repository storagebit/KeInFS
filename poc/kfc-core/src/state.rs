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
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
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

/// Per-open-handle staged contents, backed by a TEMPORARY FILE rather than an
/// in-RAM `Vec<u8>` (streaming-writeback v1).
///
/// The original design held the whole object in a `Vec<u8>` behind the
/// per-handle lock and committed it whole — a multi-GB write therefore needed
/// multi-GB of client RAM. This backs the writable handle with an
/// `O_CREAT|O_EXCL` temp file in the OS temp dir instead, so client RAM is
/// bounded regardless of object size: `write_at` seeks+writes the temp file,
/// `read_range` does a bounded seek+read, and commit streams the temp file to
/// the object store in stripe-sized chunks (see `commit_handle`). The OS gives
/// us sparse/extending-file semantics for free, so random/seeking writes and
/// gap-fills are correct without ever materializing the gap in RAM.
///
/// We track the logical length explicitly in `len`. As an INVARIANT this type
/// keeps `len` equal to the on-disk file size after every operation:
/// `File::set_len(n)` sets the on-disk size to exactly `n`, `write_at` grows both
/// the file and `len` by the same span (the OS sparse-extends any gap), and
/// `new()` seeds `len` from the bytes written. The commit path passes this `len`
/// as the authoritative object length so the streamed length never depends on a
/// re-stat (see `commit_source` / `ObjectEngine::put_object_from_path`). The
/// invariant is asserted in debug builds (`debug_assert_len_matches_file`) so any
/// future op that mutates one without the other is caught immediately.
///
/// NON-GOAL (v1): overlapping the network upload WITH the user's `write()` calls
/// (true stream-as-you-write). v1 streams from the temp file at flush. The
/// during-write streaming / dirty-stripe re-stream is a larger distributed
/// change and is the explicit follow-up — see the TODO in
/// `core.rs::commit_handle`.
pub(crate) struct HandleBuffer {
    file: File,
    path: PathBuf,
    /// Logical length in bytes. Invariant: equals the on-disk file size after
    /// every operation (see the type doc). The OS sparse-extends gaps, so a large
    /// logical length costs no RAM and no allocated blocks for the hole.
    len: u64,
}

impl HandleBuffer {
    /// Open a fresh empty temp file (`O_CREAT|O_EXCL`) for this handle. Used for
    /// truncating / freshly-created handles, and as the base for the bounded RMW
    /// seed ([`HandleBuffer::seed_from`]). No object bytes are buffered in RAM.
    pub fn new_empty() -> io::Result<Self> {
        let path = Self::unique_temp_path();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true) // O_CREAT|O_EXCL
            .open(&path)?;
        Ok(Self { file, path, len: 0 })
    }

    /// Open a fresh temp file (`O_CREAT|O_EXCL`) for this handle, seeded with the
    /// supplied initial contents (the read-modify-write prefix, or empty for a
    /// truncating / freshly-created handle). The seed is written ONCE here and
    /// then dropped; it is never retained in RAM beyond this call.
    ///
    /// Prefer [`HandleBuffer::seed_from`] for the RMW path so a multi-GB existing
    /// object is never fully resident in RAM; this whole-`Vec` constructor is kept
    /// only for already-in-RAM seeds (tests, small inline data).
    pub fn new(data: Vec<u8>) -> io::Result<Self> {
        let mut buf = Self::new_empty()?;
        if !data.is_empty() {
            buf.file.write_all(&data)?;
            buf.file.flush()?;
            buf.len = data.len() as u64;
        }
        buf.debug_assert_len_matches_file();
        Ok(buf)
    }

    /// Append a bounded chunk to the staged file while seeding it (RMW prefix
    /// copy). The caller drives a stripe-sized loop (`get_object_range` ->
    /// `seed_chunk`) so peak RAM is one stripe, not the whole object. The chunk is
    /// written at the current logical end; gaps are not expected during seeding.
    pub fn seed_chunk(&mut self, chunk: &[u8]) -> io::Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let at = self.len;
        self.write_at(at as usize, chunk)
    }

    /// Finish seeding: flush the staged prefix to disk and assert the invariant.
    pub fn seed_finish(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.debug_assert_len_matches_file();
        Ok(())
    }

    /// Debug-only invariant check: the tracked logical length must equal the
    /// on-disk file size. Guards against a future op mutating `len` and the file
    /// out of step (which would make the commit stream the wrong byte count).
    #[inline]
    fn debug_assert_len_matches_file(&self) {
        #[cfg(debug_assertions)]
        if let Ok(meta) = self.file.metadata() {
            debug_assert_eq!(
                meta.len(),
                self.len,
                "HandleBuffer logical length diverged from on-disk file size"
            );
        }
    }

    /// A process+time+counter-unique path under the OS temp dir. Mirrors the
    /// Tier-C store's temp-path discipline; the `O_EXCL` open is the real
    /// uniqueness guarantee, the counter just avoids needless collisions.
    fn unique_temp_path() -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        p.push(format!(
            "kfc-stage-{}-{}-{}",
            std::process::id(),
            nanos,
            seq
        ));
        p
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Bounded read of `[offset, offset+size)` from the temp file, clamped to the
    /// logical length. A read past EOF or into a never-written sparse hole inside
    /// the logical length returns zeros (the buffer is zero-initialized and a
    /// short physical read at the sparse tail is NOT an error). A genuine
    /// underlying I/O error (e.g. EIO on the staged temp file) is propagated so
    /// the caller can surface it as `EIO` instead of silently zero-filling — a
    /// corrupted/truncated staging file must not read back as a benign hole.
    pub fn read_range(&mut self, offset: usize, size: usize) -> io::Result<Vec<u8>> {
        let offset = offset as u64;
        if offset >= self.len || size == 0 {
            return Ok(Vec::new());
        }
        let end = self.len.min(offset.saturating_add(size as u64));
        let want = (end - offset) as usize;
        let mut out = vec![0u8; want];
        self.file.seek(SeekFrom::Start(offset))?;
        // read_exact would fail on a short read at the sparse tail; loop instead
        // so a hole (never-written gap inside the logical length) reads as the
        // zeros the buffer was initialized to. Only a real read error aborts.
        let mut filled = 0usize;
        while filled < want {
            match self.file.read(&mut out[filled..]) {
                Ok(0) => break, // physical EOF before logical EOF => sparse tail (zeros)
                Ok(n) => filled += n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// Write `bytes` at `offset`, extending (with an OS-sparse zero-filled gap)
    /// when `offset` is beyond the current length. Bounded: the only RAM touched
    /// is `bytes` itself, never the whole object.
    pub fn write_at(&mut self, offset: usize, bytes: &[u8]) -> io::Result<()> {
        let offset = offset as u64;
        // A gap beyond the current high-water mark is left as a sparse hole by
        // the OS (seeking past EOF then writing extends the file, the skipped
        // region reads back as zeros) — no explicit zero-fill, no RAM blowup.
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(bytes)?;
        let end = offset.saturating_add(bytes.len() as u64);
        if end > self.len {
            self.len = end;
        }
        self.debug_assert_len_matches_file();
        Ok(())
    }

    /// Shrink or grow the logical length. Growth zero-fills (via `set_len`, which
    /// the OS sparse-extends); shrink discards the tail. Updates the high-water
    /// mark to exactly `len`.
    pub fn set_len(&mut self, len: usize) -> io::Result<()> {
        let len = len as u64;
        self.file.set_len(len)?;
        self.len = len;
        self.debug_assert_len_matches_file();
        Ok(())
    }

    /// The commit source: the temp-file path plus the logical length. The put
    /// path reads stripe-sized ranges from this path directly (no whole-object
    /// RAM load). Returned by value so the caller can stream without holding the
    /// handle lock across the upload.
    pub fn commit_source(&self) -> (PathBuf, u64) {
        (self.path.clone(), self.len)
    }
}

impl Drop for HandleBuffer {
    /// Delete the temp file when the handle is released/closed/dropped. The
    /// `File` is closed by its own drop first; the unlink is best-effort (a
    /// failure cannot be surfaced from `drop`, and a leaked temp file is cleaned
    /// by the OS temp reaper, but in practice this removes it immediately).
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// The backing model of an open handle.
pub(crate) enum HandleData {
    /// Read-only handle: reads are served by stripe-granular ranged backend
    /// reads — no whole-object load at open, no local staging.
    Ranged,
    /// Writable / truncate handle: contents staged in a temp file (RAM-bounded)
    /// and streamed to the object store in stripe-sized chunks on flush
    /// (streaming-writeback v1).
    Staged(Mutex<HandleBuffer>),
}

/// An open file handle. A staged buffer (writable handles) is behind a
/// `std::sync::Mutex` held only for synchronous, bounded temp-file operations;
/// backend uploads read the temp-file path's logical length out first, then
/// stream from the file with no handle lock held.
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

    /// A writable/truncate handle backed by a temp file, seeded with `data` (the
    /// read-modify-write prefix, or empty). Fails if the temp file cannot be
    /// created or seeded.
    pub fn new_staged(
        ino: u64,
        key: String,
        writable: bool,
        data: Vec<u8>,
        dirty: bool,
    ) -> io::Result<Arc<Self>> {
        Self::from_buffer(ino, key, writable, HandleBuffer::new(data)?, dirty)
    }

    /// A writable/truncate handle wrapping an already-built [`HandleBuffer`]. Used
    /// by the RMW-open path, which seeds the temp file in bounded stripe-sized
    /// chunks (never a whole-object RAM `Vec`) and then hands the buffer here.
    pub fn from_buffer(
        ino: u64,
        key: String,
        writable: bool,
        buffer: HandleBuffer,
        dirty: bool,
    ) -> io::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            ino,
            key,
            writable,
            data: HandleData::Staged(Mutex::new(buffer)),
            dirty: AtomicBool::new(dirty),
        }))
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
        let mut buf = HandleBuffer::new(b"hello".to_vec()).expect("temp file");
        assert_eq!(buf.read_range(0, 5).unwrap(), b"hello");
        assert_eq!(buf.read_range(3, 100).unwrap(), b"lo");
        assert_eq!(buf.read_range(10, 4).unwrap(), b"");
        buf.write_at(5, b" world").unwrap();
        assert_eq!(buf.len(), 11);
        assert_eq!(buf.read_range(0, 11).unwrap(), b"hello world");
        buf.set_len(5).unwrap();
        assert_eq!(buf.read_range(0, 100).unwrap(), b"hello");
        buf.write_at(8, b"x").unwrap(); // sparse grow zero-fills the gap
        assert_eq!(buf.len(), 9);
        assert_eq!(buf.read_range(0, 100).unwrap(), b"hello\0\0\0x");
    }

    #[test]
    fn handle_buffer_sequential_append_readback() {
        let mut buf = HandleBuffer::new(Vec::new()).expect("temp file");
        let mut at = 0usize;
        for chunk in [&b"alpha"[..], b"-beta", b"-gamma"] {
            buf.write_at(at, chunk).unwrap();
            at += chunk.len();
        }
        assert_eq!(buf.len(), at);
        assert_eq!(buf.read_range(0, at).unwrap(), b"alpha-beta-gamma");
    }

    #[test]
    fn handle_buffer_random_seeking_write() {
        // Write at offset 0, then seek a MiB ahead and write — the gap is a
        // sparse hole that reads back as zeros, and both written regions are
        // intact. Random/seeking writes are correct because the temp file
        // supports seek+write natively.
        let mut buf = HandleBuffer::new(Vec::new()).expect("temp file");
        buf.write_at(0, b"head").unwrap();
        let far = 1 << 20; // 1 MiB
        buf.write_at(far, b"tail").unwrap();
        assert_eq!(buf.len(), far + 4);
        assert_eq!(buf.read_range(0, 4).unwrap(), b"head");
        assert_eq!(buf.read_range(far, 4).unwrap(), b"tail");
        // A read straddling the hole returns zeros for the gap.
        let gap = buf.read_range(4, 8).unwrap();
        assert_eq!(gap, vec![0u8; 8]);
    }

    #[test]
    fn handle_buffer_extend_with_gap_zero_fills() {
        let mut buf = HandleBuffer::new(b"ab".to_vec()).expect("temp file");
        buf.write_at(10, b"z").unwrap(); // gap [2,10) is zero-filled
        assert_eq!(buf.len(), 11);
        assert_eq!(buf.read_range(0, 11).unwrap(), b"ab\0\0\0\0\0\0\0\0z");
    }

    #[test]
    fn handle_buffer_truncate_shrink_and_grow() {
        let mut buf = HandleBuffer::new(b"abcdef".to_vec()).expect("temp file");
        buf.set_len(3).unwrap(); // shrink
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.read_range(0, 100).unwrap(), b"abc");
        buf.set_len(6).unwrap(); // grow zero-fills
        assert_eq!(buf.len(), 6);
        assert_eq!(buf.read_range(0, 100).unwrap(), b"abc\0\0\0");
    }

    #[test]
    fn handle_buffer_shrink_then_write_keeps_invariant() {
        // Regression for the state.rs:83-88 finding: a shrink followed by a write
        // inside the new length must keep on-disk size == logical length, so the
        // commit streams exactly `len` bytes (never trailing garbage).
        let mut buf = HandleBuffer::new(b"abcdef".to_vec()).expect("temp file");
        buf.set_len(3).unwrap();
        buf.write_at(1, b"X").unwrap();
        assert_eq!(buf.len(), 3);
        let (_path, logical_len) = buf.commit_source();
        assert_eq!(logical_len, 3);
        assert_eq!(buf.read_range(0, 100).unwrap(), b"aXc");
    }

    #[test]
    fn handle_buffer_seed_chunks_bounded() {
        // The RMW seed copies an existing object into the temp file in bounded
        // chunks (never a whole-object Vec). Driving seed_chunk repeatedly yields
        // the same contents and length as a single seed.
        let mut buf = HandleBuffer::new_empty().expect("temp file");
        for chunk in [&b"0123"[..], b"4567", b"89"] {
            buf.seed_chunk(chunk).unwrap();
        }
        buf.seed_finish().unwrap();
        assert_eq!(buf.len(), 10);
        assert_eq!(buf.read_range(0, 100).unwrap(), b"0123456789");
    }

    #[test]
    fn handle_buffer_removes_temp_file_on_drop() {
        let buf = HandleBuffer::new(b"bytes".to_vec()).expect("temp file");
        let (path, len) = buf.commit_source();
        assert_eq!(len, 5);
        assert!(path.exists(), "temp file should exist while handle is open");
        drop(buf);
        assert!(!path.exists(), "temp file must be removed on handle drop");
    }

    #[test]
    fn handle_buffer_is_file_backed_not_ram() {
        // Structural guarantee: the staged buffer holds NO whole-object Vec — its
        // backing is a temp file + a small logical-length scalar. A large logical
        // write (via a sparse grow) does not allocate that many bytes of RAM; the
        // type has no Vec field at all. We assert the type is small and pointer-/
        // handle-sized rather than object-sized.
        let mut buf = HandleBuffer::new(Vec::new()).expect("temp file");
        let huge = 1u64 << 32; // 4 GiB logical length, zero RAM
        buf.set_len(huge as usize).unwrap();
        assert_eq!(buf.len() as u64, huge);
        // The struct is File + PathBuf + u64 — no megabyte-scale inline buffer.
        assert!(
            std::mem::size_of::<HandleBuffer>() < 256,
            "HandleBuffer must be file-backed (small), not hold an inline object buffer"
        );
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
