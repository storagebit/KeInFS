// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! Transport-agnostic value types.
//!
//! `kfc-core` deliberately does NOT use `fuser` types so the core stays
//! decoupled from any one FUSE transport (fuser / io_uring / FUSE-T). The
//! transport crate maps these to/from its backend's native types at the
//! boundary.

use std::time::SystemTime;

/// Root inode number, fixed by the FUSE protocol.
pub const ROOT_INO: u64 = 1;

/// The kind of a filesystem node. KeInFS exposes buckets/collections as
/// directories and objects as regular files; there are no symlinks/devices.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    Directory,
    RegularFile,
}

/// A POSIX attribute snapshot, transport-neutral. Times default to "now" at the
/// transport boundary if a backend needs distinct a/m/c times; KeInFS does not
/// track per-file timestamps in the metadata plane yet.
#[derive(Clone, Copy, Debug)]
pub struct Attr {
    pub ino: u64,
    pub size: u64,
    pub kind: FileKind,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    /// Advertised block size; we advertise a large value so the kernel issues
    /// big I/O requests once `max_write` is negotiated up.
    pub blksize: u32,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
}

/// One directory entry. `attr` is populated for the readdirplus fast path so a
/// directory walk returns child attributes in a single pass (no per-child
/// getattr round-trip).
#[derive(Clone, Debug)]
pub struct DirEntry {
    pub ino: u64,
    pub name: String,
    pub kind: FileKind,
    pub attr: Option<Attr>,
}

/// The outcome of an `open`/`create`: a handle id plus per-open kernel cache
/// hints. `keep_cache` asks the kernel to retain the page cache across opens
/// (cheap re-reads); `direct_io` bypasses it (used only when an opt-out is
/// required). `keep_cache` is the default once NATS coherence is active.
#[derive(Clone, Copy, Debug)]
pub struct OpenedFile {
    pub fh: u64,
    pub keep_cache: bool,
    pub direct_io: bool,
}

/// What the core asks the kernel transport to negotiate during `init`. The
/// transport clamps each value to what the running kernel actually grants and
/// reports the granted reality back via [`Capabilities`], so the core sizes its
/// staging/prefetch windows to truth rather than to wishes.
#[derive(Clone, Copy, Debug)]
pub struct DesiredKernelConfig {
    pub max_write: u32,
    pub max_readahead: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub want_writeback_cache: bool,
    pub want_parallel_dirops: bool,
    pub want_readdirplus: bool,
    pub want_auto_inval_data: bool,
    pub want_async_read: bool,
    pub want_async_dio: bool,
    pub want_splice: bool,
}

impl Default for DesiredKernelConfig {
    fn default() -> Self {
        // Targets from the KFC v2 design (poc/kfc/DESIGN_KFC_V2.md §4).
        Self {
            max_write: 1 << 20,      // 1 MiB transfers; matches advertised blksize.
            max_readahead: 8 << 20,  // 8 MiB sequential read window.
            max_background: 128,
            congestion_threshold: 96, // ~75% of max_background.
            want_writeback_cache: true,
            want_parallel_dirops: true,
            want_readdirplus: true,
            want_auto_inval_data: true,
            want_async_read: true,
            want_async_dio: true,
            want_splice: true,
        }
    }
}

/// What the transport actually obtained from the kernel and which backend is
/// live. Filled in by the transport after `init` and handed to the core.
#[derive(Clone, Copy, Debug, Default)]
pub struct Capabilities {
    pub granted_max_write: u32,
    pub granted_max_readahead: u32,
    pub writeback_cache: bool,
    pub parallel_dirops: bool,
    pub readdirplus: bool,
    pub splice: bool,
    pub io_uring: bool,
    /// Human-readable backend identity for the observability tree, e.g.
    /// "fuser/dev-fuse", "fuser/io-uring", "fuser/fuse-t", "fuser/macfuse".
    pub backend: &'static str,
}
