// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! FUSE-over-io_uring backend — Linux, kernel >= 6.14.
//!
//! This is the Tier-1 max-throughput transport: kernel-managed io_uring command
//! rings (`FUSE_IO_URING_CMD_REGISTER` + `COMMIT_AND_FETCH`) with zero-copy and
//! one worker per pinned core. Its programming model is async (the `compio`
//! single-threaded runtime), so the entire implementation lives here behind the
//! `io-uring` feature + `target_os = "linux"` gate; the portable `kfc-core`
//! never sees it (FIRST_PRINCIPLES §1).
//!
//! It is the SECOND transport backend alongside [`crate::fuser_backend`]. The
//! FsCore op mappings here MUST mirror that backend op-for-op — the only
//! difference is the FUSE library underneath: `fractal-fuse` 0.4 instead of
//! `fuser`. fractal-fuse handlers are `async fn`s returning `FsResult<T>` (a
//! `Result<T, Errno=i32>`) on `!Send` compio futures, so each handler bridges
//! to the tokio-based [`FsCore`] by cloning the core `Arc`, copying any borrowed
//! args, spawning the (Send) FsCore future on the shared tokio runtime, and
//! awaiting the result over a runtime-agnostic [`tokio::sync::oneshot`] channel
//! (never `block_on` inside a handler — that would stall the compio worker).
//!
//! NOTE: this module compiles only on Linux with `--features io-uring` and a
//! toolchain new enough for fractal-fuse's edition 2024 (cargo >= 1.85); it is
//! never built on macOS.

use crate::{BackendKind, FuseBackend, MountOpts};
use fractal_fuse::{
    DirectoryEntry, DirectoryEntryPlus, FileAttr, FileType as FuseFileType, Filesystem, FsResult,
    FuseNotifier, Inode, MountOptions, ReplyAttr, ReplyCreate, ReplyEntry, ReplyInit, ReplyOpen,
    ReplyStatfs, Request, Session, SetAttr, SetAttrTime, Timestamp, EIO,
};
use kfc_core::{
    Attr, Capabilities, CoherenceSink, FileKind, FsCore, FsErrno, OpenedFile, DesiredKernelConfig,
};
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Block size advertised in statfs (cosmetic; KeInFS has no fixed capacity).
const STATFS_BSIZE: u32 = 1 << 20;
const STATFS_NAMELEN: u32 = 255;
/// FOPEN_* flag bits (uapi/linux/fuse.h). fractal-fuse threads `ReplyOpen.flags`
/// straight through to the kernel as the raw `fuse_open_out.open_flags`, so we
/// set the bits ourselves rather than via a typed flag set.
const FOPEN_DIRECT_IO: u32 = 1 << 0;
const FOPEN_KEEP_CACHE: u32 = 1 << 1;

/// Whether to use the FUSE-over-io_uring backend on this host.
///
/// This is the **opt-in gate for the io_uring backend**: it returns `true` only
/// when `KFC_IO_URING` is set to `1`/`true` AND the running kernel looks
/// >= 6.14 (the first release with `FUSE_OVER_IO_URING`). Otherwise it returns
/// `false` so [`crate::select_backend`] keeps the always-correct fuser backend
/// as the default. The hard kernel gate inside `fractal-fuse` (FUSE_INIT
/// negotiation) is authoritative; this probe just keeps the io_uring path off
/// the default selection path until explicitly enabled for testing.
pub(crate) fn kernel_supports_io_uring() -> bool {
    if !env_opt_in() {
        return false;
    }
    kernel_at_least(6, 14)
}

/// True when `KFC_IO_URING` is set to an affirmative value.
fn env_opt_in() -> bool {
    match std::env::var("KFC_IO_URING") {
        Ok(v) => {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true")
        }
        Err(_) => false,
    }
}

/// Parse `uname -r` (the kernel release) and test `major.minor >= want`.
fn kernel_at_least(want_major: u32, want_minor: u32) -> bool {
    let Some(release) = uname_release() else {
        return false;
    };
    let mut parts = release.split(|c: char| c == '.' || c == '-' || c == '_');
    let major: u32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    let minor: u32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    (major, minor) >= (want_major, want_minor)
}

/// Read the kernel release string via `uname(2)`.
fn uname_release() -> Option<String> {
    // SAFETY: zeroed `utsname` is a valid initial value; `uname` fills it.
    let mut uts: libc::utsname = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::uname(&mut uts) };
    if rc != 0 {
        return None;
    }
    let release: Vec<u8> = uts
        .release
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    String::from_utf8(release).ok()
}

pub(crate) struct UringBackend {}

impl UringBackend {
    pub fn new() -> Self {
        Self {}
    }
}

impl FuseBackend for UringBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::FuserIoUring
    }

    fn mount_blocking(
        self: Box<Self>,
        core: Arc<FsCore>,
        runtime: tokio::runtime::Runtime,
        mountpoint: PathBuf,
        opts: MountOpts,
    ) -> io::Result<()> {
        // Build the fractal-fuse mount options to mirror the fuser backend's
        // `build_config` as closely as the fractal-fuse API allows:
        //  * fs_name + default_permissions + allow_other map 1:1.
        //  * NoAtime: fractal-fuse has no typed builder for it, but its mount
        //    string is assembled from `custom_options`, so we pass the raw
        //    `noatime` VFS flag there — exactly what `MountOption::NoAtime`
        //    expands to. The core never tracks atime (node_attr stamps "now"),
        //    so suppressing kernel atime updates matches the fuser mount.
        //  * Async: the fuser backend sets `MountOption::Async`, but `async` is
        //    the kernel's default mount behavior (the opposite of `sync`) and
        //    fractal-fuse exposes no toggle for it, so omitting it is equivalent
        //    — the one parity gap, documented here rather than faked.
        //  * write_back: honor the core's desired writeback posture so the same
        //    core advertises the same writeback_cache truth on both backends
        //    (see `init` for why we must mirror this into the reported caps).
        let want = core.desired_kernel_config();
        let mount_options = MountOptions::new()
            .fs_name(opts.fs_name.clone())
            .default_permissions(true)
            .allow_other(opts.allow_other || opts.auto_unmount)
            .write_back(want.want_writeback_cache)
            .custom_options("noatime");
        let session = Session::new(mountpoint, mount_options)?;
        // Cap the io_uring ring-buffer footprint. Measured directly on
        // fractal-fuse 0.4.0 (32-core box, max_write=1 MiB): idle RSS scales with
        // the per-ring SUBMISSION QUEUE DEPTH, NOT worker count — 1/2/4/32
        // workers all sat at ~8 GiB, while queue_depth 32 -> ~1 GiB, 128 -> ~4
        // GiB, and fractal-fuse's default (~8192) -> ~8 GiB. The rings share one
        // buffer arena sized by queue depth. So we bound the queue depth by
        // default (the real lever) to keep the backend usable out of the box;
        // the --io-uring-workers override does NOT change RSS and is applied only
        // when set (it can bound CPU/context-switch on many-core hosts). Both are
        // fractal-fuse builder methods that consume and return the Session;
        // with_queue_depth takes a u16, so clamp into range.
        const DEFAULT_IO_URING_QUEUE_DEPTH: usize = 32; // ~1 GiB idle vs ~8 GiB uncapped
        let queue_depth = opts
            .io_uring_queue_depth
            .unwrap_or(DEFAULT_IO_URING_QUEUE_DEPTH)
            .clamp(1, u16::MAX as usize) as u16;
        let mut session = session.with_queue_depth(queue_depth);
        if let Some(workers) = opts.io_uring_workers {
            session = session.with_worker_count(workers.max(1));
        }

        let adapter = UringAdapter {
            core: Arc::clone(&core),
            tokio: runtime.handle().clone(),
            backend: self.kind(),
        };

        // Block on the rings until unmount. `runtime` stays owned here so the
        // spawned FsCore futures + the NATS coherence loop (wired in `init`)
        // keep being serviced by its worker threads for the whole session.
        let result = session.run(adapter);
        drop(runtime);
        result
    }
}

/// CoherenceSink over a fractal-fuse [`FuseNotifier`]: pushes invalidations into
/// the kernel page cache / dentry cache so `FOPEN_KEEP_CACHE` data is dropped on
/// out-of-band mutation. Mirrors [`crate::fuser_backend`]'s `FuserSink`.
///
/// `FuseNotifier` writes one-way `FUSE_NOTIFY_*` messages straight to /dev/fuse,
/// so it works identically under the io_uring transport. It is constructed from
/// the `Arc<OwnedFd>` handed to `init` (see [`UringAdapter::init`]).
struct UringSink {
    notifier: FuseNotifier,
}

impl UringSink {
    fn new(notifier: FuseNotifier) -> Self {
        Self { notifier }
    }
}

impl CoherenceSink for UringSink {
    fn inval_inode(&self, ino: u64) {
        // offset 0, len 0 => invalidate the whole inode's cached data + attrs.
        let _ = self.notifier.inval_inode(ino, 0, 0);
    }

    fn inval_entry(&self, parent_ino: u64, name: &str) {
        let _ = self.notifier.inval_entry(parent_ino, name.as_bytes());
    }
}

pub(crate) struct UringAdapter {
    core: Arc<FsCore>,
    tokio: tokio::runtime::Handle,
    backend: BackendKind,
}

// ----- type mappers ---------------------------------------------------------

fn to_fuse_file_type(kind: FileKind) -> FuseFileType {
    match kind {
        FileKind::Directory => FuseFileType::Directory,
        FileKind::RegularFile => FuseFileType::RegularFile,
    }
}

/// Convert a `SystemTime` to a fractal-fuse `Timestamp`. Pre-epoch times (never
/// produced by the core, which stamps "now") clamp to zero.
fn to_timestamp(t: SystemTime) -> Timestamp {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    Timestamp::new(d.as_secs(), d.subsec_nanos())
}

/// Mirror of `fuser_backend::to_file_attr`. fractal-fuse's `FileAttr.mode`
/// packs the file-type bits and permission bits together, so we OR the
/// `FileType::to_mode()` S_IFMT bits with the core's `perm`.
fn to_file_attr(a: &Attr) -> FileAttr {
    FileAttr {
        ino: a.ino,
        size: a.size,
        blocks: a.size.div_ceil(512),
        atime: to_timestamp(a.atime),
        mtime: to_timestamp(a.mtime),
        ctime: to_timestamp(a.ctime),
        mode: to_fuse_file_type(a.kind).to_mode() | a.perm as u32,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        rdev: 0,
        blksize: a.blksize,
    }
}

/// Defensive fallback if a readdirplus entry somehow lacks a cached attr (the
/// core populates attrs for every entry, so this is a guard, not a stat
/// round-trip). Mirrors `fuser_backend::fallback_file_attr`.
fn fallback_file_attr(ino: u64, kind: FileKind) -> FileAttr {
    let perm: u32 = match kind {
        FileKind::Directory => 0o755,
        FileKind::RegularFile => 0o644,
    };
    let nlink = match kind {
        FileKind::Directory => 2,
        FileKind::RegularFile => 1,
    };
    let now = to_timestamp(SystemTime::now());
    FileAttr {
        ino,
        size: 0,
        blocks: 0,
        atime: now,
        mtime: now,
        ctime: now,
        mode: to_fuse_file_type(kind).to_mode() | perm,
        nlink,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 1 << 20,
    }
}

/// Map a transport-neutral [`FsErrno`] to a fractal-fuse `Errno` (an `i32`).
/// `FsErrno::raw()` already yields the libc errno integer, and fractal-fuse's
/// `Errno` consts (ENOENT/EIO/...) are exactly those libc values, so we reuse it
/// directly rather than re-deriving the mapping.
fn errno(e: FsErrno) -> i32 {
    e.raw()
}

/// Per-open kernel cache hint bits, mirroring `fuser_backend::fopen_flags`.
fn fopen_flags(opened: &OpenedFile) -> u32 {
    let mut flags = 0u32;
    if opened.keep_cache {
        flags |= FOPEN_KEEP_CACHE;
    }
    if opened.direct_io {
        flags |= FOPEN_DIRECT_IO;
    }
    flags
}

/// Build a [`ReplyOpen`] from an [`OpenedFile`].
fn reply_open(opened: &OpenedFile) -> ReplyOpen {
    ReplyOpen {
        fh: opened.fh,
        flags: fopen_flags(opened),
        backing_id: 0,
    }
}

impl Filesystem for UringAdapter {
    async fn init(&self, _req: Request, fuse_dev_fd: Arc<std::os::fd::OwnedFd>) -> FsResult<ReplyInit> {
        let want: DesiredKernelConfig = self.core.desired_kernel_config();

        // Wire the kernel-cache invalidation sink now that the /dev/fuse fd is
        // available. fractal-fuse cleanly supports inode + entry invalidation
        // via `FuseNotifier` (one-way FUSE_NOTIFY_* writes), so we install it
        // and start the NATS coherence loop exactly as the fuser backend does in
        // its `mount_blocking` (moved here because the fd only exists at init).
        let notifier: FuseNotifier = FuseNotifier::from(fuse_dev_fd);
        self.core.set_coherence_sink(Arc::new(UringSink::new(notifier)));
        self.core.spawn_coherence_loop(&self.tokio);

        // fractal-fuse owns the FUSE_INIT capability negotiation internally
        // (session.rs `write_fuse_init_reply`): it unconditionally requests
        // ASYNC_READ, BIG_WRITES, AUTO_INVAL_DATA, DO_READDIRPLUS +
        // READDIRPLUS_AUTO, ASYNC_DIO, PARALLEL_DIROPS, MAX_PAGES, ATOMIC_O_TRUNC
        // and intersects them with what the kernel offers — but it does NOT
        // surface the negotiated result back to this handler. So, unlike the
        // fuser backend (which reads `config.capabilities()`), we cannot read
        // the granted flags here. We therefore report capabilities from the
        // desired config, mirroring what fractal-fuse actually requests:
        //  * writeback_cache: fractal-fuse advertises FUSE_WRITEBACK_CACHE iff
        //    `MountOptions.write_back` is set, which `mount_blocking` now wires
        //    to `want.want_writeback_cache` — so we report the same value here
        //    instead of a hardcoded `false`. This keeps the writeback posture
        //    consistent with the fuser backend (which honors the same core
        //    desire) rather than silently dropping it.
        //  * readdirplus/parallel_dirops: ALWAYS requested by fractal-fuse, so
        //    reported as granted.
        // granted_max_write/readahead are reported as what we asked for; the
        // session caps max_write at its own ceiling (16 MiB) and the kernel may
        // lower readahead, but neither value is returned to us to read back.
        // TODO: if fractal-fuse later surfaces the negotiated
        // fuse_init_out, replace these desired-config values with the granted
        // truth (as the fuser backend does).
        self.core.set_capabilities(Capabilities {
            granted_max_write: want.max_write,
            granted_max_readahead: want.max_readahead,
            writeback_cache: want.want_writeback_cache,
            parallel_dirops: want.want_parallel_dirops,
            readdirplus: want.want_readdirplus,
            splice: false,
            io_uring: true,
            backend: self.backend.name(),
        });

        eprintln!(
            "KFC v2 init [{}]: max_write={} max_readahead={} (fractal-fuse / FUSE_OVER_IO_URING)",
            self.backend.name(),
            want.max_write,
            want.max_readahead
        );

        Ok(ReplyInit {
            max_write: want.max_write,
            max_readahead: want.max_readahead,
            max_background: want.max_background,
            congestion_threshold: want.congestion_threshold,
        })
    }

    async fn lookup(&self, _req: Request, parent: Inode, name: &OsStr) -> FsResult<ReplyEntry> {
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        let name = name.to_string_lossy().into_owned();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.lookup(parent, &name).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(attr)) => Ok(ReplyEntry {
                ttl,
                attr: to_file_attr(&attr),
                generation: 0,
            }),
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: Inode,
        _fh: Option<u64>,
        _flags: u32,
    ) -> FsResult<ReplyAttr> {
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.getattr(inode).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(attr)) => Ok(ReplyAttr {
                ttl,
                attr: to_file_attr(&attr),
            }),
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn setattr(
        &self,
        _req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> FsResult<ReplyAttr> {
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        // fractal-fuse folds the FATTR_FH bit into `set_attr.fh`; prefer the
        // explicit handler `fh` arg (matches the fuser backend, which receives
        // fh separately) and fall back to the one carried in SetAttr.
        let fh = fh.or(set_attr.fh);
        let mode = set_attr.mode;
        let uid = set_attr.uid;
        let gid = set_attr.gid;
        let size = set_attr.size;
        let _atime: Option<SetAttrTime> = set_attr.atime;
        let _mtime: Option<SetAttrTime> = set_attr.mtime;
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.setattr(inode, mode, uid, gid, size, fh).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(attr)) => Ok(ReplyAttr {
                ttl,
                attr: to_file_attr(&attr),
            }),
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn opendir(&self, _req: Request, inode: Inode, _flags: u32) -> FsResult<ReplyOpen> {
        let core = Arc::clone(&self.core);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.opendir(inode).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(())) => Ok(ReplyOpen {
                fh: 0,
                flags: 0,
                backing_id: 0,
            }),
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> FsResult<ReplyEntry> {
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        let name = name.to_string_lossy().into_owned();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.mkdir(parent, &name, mode).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(attr)) => {
                let mut file_attr = to_file_attr(&attr);
                // Preserve the file-type bits, override the permission bits with
                // the requested mode (mirrors the fuser backend's perm override).
                file_attr.mode =
                    to_fuse_file_type(FileKind::Directory).to_mode() | (mode & 0o777);
                Ok(ReplyEntry {
                    ttl,
                    attr: file_attr,
                    generation: 0,
                })
            }
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn readdir(
        &self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntry>> {
        let core = Arc::clone(&self.core);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.readdir(inode).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(entries)) => {
                let mut out = Vec::with_capacity(entries.len());
                for (index, entry) in entries.into_iter().enumerate().skip(offset as usize) {
                    out.push(DirectoryEntry {
                        ino: entry.ino,
                        // Opaque resume cursor: 1-based index of the NEXT entry,
                        // matching the fuser backend's `(index + 1)` offset.
                        offset: (index + 1) as u64,
                        kind: to_fuse_file_type(entry.kind),
                        name: entry.name.into_bytes(),
                    });
                }
                Ok(out)
            }
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn readdirplus(
        &self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntryPlus>> {
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.readdir(inode).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(entries)) => {
                let mut out = Vec::with_capacity(entries.len());
                for (index, entry) in entries.into_iter().enumerate().skip(offset as usize) {
                    let attr = entry
                        .attr
                        .map(|a| to_file_attr(&a))
                        .unwrap_or_else(|| fallback_file_attr(entry.ino, entry.kind));
                    out.push(DirectoryEntryPlus {
                        ino: entry.ino,
                        offset: (index + 1) as u64,
                        kind: to_fuse_file_type(entry.kind),
                        name: entry.name.into_bytes(),
                        entry_ttl: ttl,
                        attr,
                        generation: 0,
                    });
                }
                Ok(out)
            }
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn open(&self, _req: Request, inode: Inode, flags: u32) -> FsResult<ReplyOpen> {
        let core = Arc::clone(&self.core);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.open(inode, flags as i32).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(opened)) => Ok(reply_open(&opened)),
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn create(
        &self,
        _req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> FsResult<ReplyCreate> {
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        let name = name.to_string_lossy().into_owned();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.create(parent, &name, mode, flags as i32).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok((attr, opened))) => {
                let mut file_attr = to_file_attr(&attr);
                file_attr.mode =
                    to_fuse_file_type(FileKind::RegularFile).to_mode() | (mode & 0o777);
                Ok(ReplyCreate {
                    ttl,
                    attr: file_attr,
                    generation: 0,
                    fh: opened.fh,
                    flags: fopen_flags(&opened),
                })
            }
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn read(
        &self,
        _req: Request,
        _inode: Inode,
        fh: u64,
        offset: u64,
        buf: &mut [u8],
    ) -> FsResult<usize> {
        let core = Arc::clone(&self.core);
        // Cap the request at the caller's buffer length; the core returns a
        // freshly-allocated Vec we then copy into `buf` (this can be zero-copied later).
        let size = buf.len() as u32;
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.read(fh, offset, size).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(data)) => {
                // `size` was derived from `buf.len()`, and core.read clamps its
                // returned Vec to `size` (state.rs read_range / read_ranged_cached),
                // so data.len() <= buf.len() always holds. The .min() below is a
                // guard, not a live truncation path; assert loudly in debug builds
                // so a future core regression that over-returns is caught here
                // rather than silently dropping the tail.
                debug_assert!(
                    data.len() <= buf.len(),
                    "core.read returned {} bytes for a {}-byte request",
                    data.len(),
                    buf.len()
                );
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                Ok(n)
            }
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn write(
        &self,
        _req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> FsResult<usize> {
        let core = Arc::clone(&self.core);
        // Copy the borrowed kernel buffer before moving into the task. With a
        // 1 MiB max_write this is one copy per MiB; this can be zero-copied later.
        let data = data.to_vec();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.write(inode, fh, offset, &data).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(written)) => Ok(written as usize),
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn flush(&self, _req: Request, _inode: Inode, fh: u64, _lock_owner: u64) -> FsResult<()> {
        let core = Arc::clone(&self.core);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.flush(fh).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn fsync(
        &self,
        _req: Request,
        _inode: Inode,
        fh: u64,
        _datasync: bool,
    ) -> FsResult<()> {
        let core = Arc::clone(&self.core);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.fsync(fh).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn release(
        &self,
        _req: Request,
        _inode: Inode,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        _flock_release: bool,
    ) -> FsResult<()> {
        let core = Arc::clone(&self.core);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.release(fh).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn releasedir(
        &self,
        _req: Request,
        _inode: Inode,
        _fh: u64,
        _flags: u32,
    ) -> FsResult<()> {
        // opendir returns fh=0 with no per-dir core state, so there is nothing to
        // release. The fuser backend has no releasedir impl either (it relies on
        // the trait default); fractal-fuse's default is ENOSYS, so we return Ok
        // explicitly to keep `closedir` clean.
        Ok(())
    }

    async fn unlink(&self, _req: Request, parent: Inode, name: &OsStr) -> FsResult<()> {
        let core = Arc::clone(&self.core);
        let name = name.to_string_lossy().into_owned();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.unlink(parent, &name).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn rmdir(&self, _req: Request, parent: Inode, name: &OsStr) -> FsResult<()> {
        let core = Arc::clone(&self.core);
        let name = name.to_string_lossy().into_owned();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tokio.spawn(async move {
            let r = core.rmdir(parent, &name).await;
            let _ = tx.send(r);
        });
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(errno(e)),
            Err(_) => Err(EIO),
        }
    }

    async fn statfs(&self, _req: Request, _inode: Inode) -> FsResult<ReplyStatfs> {
        // KeInFS exposes no fixed capacity; report a large advertised block size
        // (matching the per-file blksize) and unlimited free space. Mirrors the
        // fuser backend's statfs(0,0,0,0,0,BSIZE,NAMELEN,0).
        Ok(ReplyStatfs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 0,
            ffree: 0,
            bsize: STATFS_BSIZE,
            namelen: STATFS_NAMELEN,
            frsize: STATFS_BSIZE,
        })
    }

    fn forget(&self, _req: Request, _inode: Inode, _nlookup: u64) {
        // The core does not refcount inodes by lookup count (its lifecycle is
        // driven by the namespace cache + open-handle set), so forget is a
        // no-op, exactly as the fuser backend leaves it (no impl => default).
    }
}
