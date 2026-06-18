// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! The `fuser` backend: the always-correct Tier-2 transport.
//!
//! It bridges `fuser`'s synchronous `Filesystem` trait to the async
//! [`FsCore`] without ever blocking a FUSE worker thread: each callback clones
//! the core `Arc`, copies any borrowed arguments, and **spawns a tokio task**
//! that awaits the core op and completes the (Send) kernel reply. This deletes
//! the original `sync_channel(1).recv()` bridge (`poc/kfc/src/mount.rs:197-202`)
//! that parked a worker per backend RPC, so thousands of concurrent FUSE ops
//! map to cheap tokio tasks multiplexed over warm KSC h2 sessions.

use crate::{BackendKind, FuseBackend, MountOpts};
use fuser::{
    BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, InitFlags, KernelConfig, LockOwner, MountOption, Notifier, OpenFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyStatfs, ReplyWrite, Request, SessionACL, TimeOrNow, WriteFlags,
};
use kfc_core::{Attr, Capabilities, CoherenceSink, FileKind, FsCore, FsErrno, OpenedFile};
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

/// Block size advertised in statfs (cosmetic; KeInFS has no fixed capacity).
const STATFS_BSIZE: u32 = 1 << 20;
const STATFS_NAMELEN: u32 = 255;

pub(crate) struct FuserBackend {
    kind: BackendKind,
}

impl FuserBackend {
    pub fn new(kind: BackendKind) -> Self {
        Self { kind }
    }
}

impl FuseBackend for FuserBackend {
    fn kind(&self) -> BackendKind {
        self.kind
    }

    fn mount_blocking(
        self: Box<Self>,
        core: Arc<FsCore>,
        runtime: tokio::runtime::Runtime,
        mountpoint: PathBuf,
        opts: MountOpts,
    ) -> io::Result<()> {
        let adapter = FuserAdapter {
            core: Arc::clone(&core),
            rt: runtime.handle().clone(),
            backend: self.kind,
        };
        let config = build_config(&opts);
        // spawn_mount2 runs the session loop on a background thread and gives us
        // a Notifier we can use to push kernel-cache invalidations.
        let session = fuser::spawn_mount2(adapter, &mountpoint, &config)?;
        core.set_coherence_sink(Arc::new(FuserSink::new(session.notifier())));
        core.spawn_coherence_loop(runtime.handle());
        // Block until unmounted; `runtime` stays alive (owned here) so dispatched
        // tasks and the coherence loop keep running for the whole session.
        let result = session.join();
        drop(runtime);
        result
    }
}

fn build_config(opts: &MountOpts) -> Config {
    let mut config = Config::default();
    config.n_threads = Some(
        std::thread::available_parallelism()
            .map(|p| p.get().min(32))
            .unwrap_or(4),
    );
    // Per-worker cloned /dev/fuse fd for parallel request processing (Linux 4.5+).
    #[cfg(target_os = "linux")]
    {
        config.clone_fd = true;
    }
    let mut mount_options = vec![
        MountOption::FSName(opts.fs_name.clone()),
        MountOption::Async,
        MountOption::NoAtime,
        MountOption::DefaultPermissions,
    ];
    if opts.auto_unmount {
        mount_options.push(MountOption::AutoUnmount);
    }
    config.mount_options = mount_options;
    // `allow_other` is expressed via the session ACL in this fuser, not a mount
    // option. AutoUnmount requires acl != Owner, so it implies allow_other.
    if opts.allow_other || opts.auto_unmount {
        config.acl = SessionACL::All;
    }
    config
}

/// CoherenceSink over a fuser [`Notifier`]: pushes invalidations into the kernel
/// page cache / dentry cache so `FOPEN_KEEP_CACHE` data is dropped on
/// out-of-band mutation (Phase 2 flips opens to KEEP_CACHE to use this).
struct FuserSink {
    notifier: Notifier,
}

impl FuserSink {
    fn new(notifier: Notifier) -> Self {
        Self { notifier }
    }
}

impl CoherenceSink for FuserSink {
    fn inval_inode(&self, ino: u64) {
        // offset 0, len 0 => invalidate the whole inode's cached data + attrs.
        let _ = self.notifier.inval_inode(INodeNo(ino), 0, 0);
    }

    fn inval_entry(&self, parent_ino: u64, name: &str) {
        let _ = self.notifier.inval_entry(INodeNo(parent_ino), OsStr::new(name));
    }
}

struct FuserAdapter {
    core: Arc<FsCore>,
    rt: tokio::runtime::Handle,
    backend: BackendKind,
}

// ----- type mappers ---------------------------------------------------------

fn to_file_type(kind: FileKind) -> FileType {
    match kind {
        FileKind::Directory => FileType::Directory,
        FileKind::RegularFile => FileType::RegularFile,
    }
}

fn to_file_attr(a: &Attr) -> FileAttr {
    FileAttr {
        ino: INodeNo(a.ino),
        size: a.size,
        blocks: a.size.div_ceil(512),
        atime: a.atime,
        mtime: a.mtime,
        ctime: a.ctime,
        crtime: a.ctime,
        kind: to_file_type(a.kind),
        perm: a.perm,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        rdev: 0,
        blksize: a.blksize,
        flags: 0,
    }
}

/// A minimal FileAttr used if a readdirplus entry somehow lacks a cached attr
/// (the core populates attrs for every entry, so this is a defensive fallback
/// rather than a stat round-trip).
fn fallback_file_attr(ino: u64, kind: FileKind) -> FileAttr {
    FileAttr {
        ino: INodeNo(ino),
        size: 0,
        blocks: 0,
        atime: SystemTime::now(),
        mtime: SystemTime::now(),
        ctime: SystemTime::now(),
        crtime: SystemTime::now(),
        kind: to_file_type(kind),
        perm: match kind {
            FileKind::Directory => 0o755,
            FileKind::RegularFile => 0o644,
        },
        nlink: match kind {
            FileKind::Directory => 2,
            FileKind::RegularFile => 1,
        },
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 1 << 20,
        flags: 0,
    }
}

fn to_errno(e: FsErrno) -> Errno {
    match e {
        FsErrno::NoEntry => Errno::ENOENT,
        FsErrno::NotDir => Errno::ENOTDIR,
        FsErrno::IsDir => Errno::EISDIR,
        FsErrno::Exists => Errno::EEXIST,
        FsErrno::NotEmpty => Errno::ENOTEMPTY,
        FsErrno::BadHandle => Errno::EBADF,
        FsErrno::Perm => Errno::EPERM,
        FsErrno::NoSys => Errno::ENOSYS,
        FsErrno::Again => Errno::EAGAIN,
        FsErrno::TooBig => Errno::EFBIG,
        FsErrno::NameTooLong => Errno::ENAMETOOLONG,
        FsErrno::Inval => Errno::EINVAL,
        FsErrno::Io => Errno::EIO,
    }
}

fn fopen_flags(opened: &OpenedFile) -> FopenFlags {
    let mut flags = FopenFlags::empty();
    if opened.keep_cache {
        flags |= FopenFlags::FOPEN_KEEP_CACHE;
    }
    if opened.direct_io {
        flags |= FopenFlags::FOPEN_DIRECT_IO;
    }
    flags
}

impl Filesystem for FuserAdapter {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> io::Result<()> {
        let want = self.core.desired_kernel_config();
        let granted_max_write = match config.set_max_write(want.max_write) {
            Ok(v) | Err(v) => v,
        };
        let granted_max_readahead = match config.set_max_readahead(want.max_readahead) {
            Ok(v) | Err(v) => v,
        };
        let _ = config.set_max_background(want.max_background);
        let _ = config.set_congestion_threshold(want.congestion_threshold);

        // Only request capabilities we both want AND the kernel supports.
        // readdirplus + splice are deferred (no trait impl yet) to Phase 2/5.
        let avail = config.capabilities();
        let mut flags = InitFlags::FUSE_BIG_WRITES & avail;
        if want.want_async_read {
            flags |= InitFlags::FUSE_ASYNC_READ & avail;
        }
        if want.want_async_dio {
            flags |= InitFlags::FUSE_ASYNC_DIO & avail;
        }
        if want.want_parallel_dirops {
            flags |= InitFlags::FUSE_PARALLEL_DIROPS & avail;
        }
        if want.want_auto_inval_data {
            flags |= InitFlags::FUSE_AUTO_INVAL_DATA & avail;
        }
        if want.want_writeback_cache {
            flags |= InitFlags::FUSE_WRITEBACK_CACHE & avail;
        }
        if want.want_readdirplus {
            // readdirplus is implemented below; READDIRPLUS_AUTO lets the kernel
            // alternate between readdir and readdirplus by access pattern.
            flags |= InitFlags::FUSE_DO_READDIRPLUS & avail;
            flags |= InitFlags::FUSE_READDIRPLUS_AUTO & avail;
        }
        let _ = config.add_capabilities(flags);

        self.core.set_capabilities(Capabilities {
            granted_max_write,
            granted_max_readahead,
            writeback_cache: flags.contains(InitFlags::FUSE_WRITEBACK_CACHE),
            parallel_dirops: flags.contains(InitFlags::FUSE_PARALLEL_DIROPS),
            readdirplus: flags.contains(InitFlags::FUSE_DO_READDIRPLUS),
            splice: false,
            io_uring: avail.contains(InitFlags::FUSE_OVER_IO_URING),
            backend: self.backend.name(),
        });
        eprintln!(
            "KFC v2 init [{}]: max_write={granted_max_write} max_readahead={granted_max_readahead} caps={flags:?}",
            self.backend.name()
        );
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        let name = name.to_string_lossy().into_owned();
        self.rt.spawn(async move {
            match core.lookup(parent.0, &name).await {
                Ok(attr) => reply.entry(&ttl, &to_file_attr(&attr), Generation(0)),
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        self.rt.spawn(async move {
            match core.getattr(ino.0).await {
                Ok(attr) => reply.attr(&ttl, &to_file_attr(&attr)),
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
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
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        let fh = fh.map(|h| h.0);
        self.rt.spawn(async move {
            match core.setattr(ino.0, mode, uid, gid, size, fh).await {
                Ok(attr) => reply.attr(&ttl, &to_file_attr(&attr)),
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let core = Arc::clone(&self.core);
        self.rt.spawn(async move {
            match core.opendir(ino.0).await {
                Ok(()) => reply.opened(FileHandle(0), FopenFlags::empty()),
                Err(e) => reply.error(to_errno(e)),
            }
        });
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
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        let name = name.to_string_lossy().into_owned();
        self.rt.spawn(async move {
            match core.mkdir(parent.0, &name, mode).await {
                Ok(attr) => {
                    let mut file_attr = to_file_attr(&attr);
                    file_attr.perm = (mode & 0o777) as u16;
                    reply.entry(&ttl, &file_attr, Generation(0));
                }
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let core = Arc::clone(&self.core);
        self.rt.spawn(async move {
            match core.readdir(ino.0).await {
                Ok(entries) => {
                    for (index, entry) in entries.into_iter().enumerate().skip(offset as usize) {
                        let full = reply.add(
                            INodeNo(entry.ino),
                            (index + 1) as u64,
                            to_file_type(entry.kind),
                            entry.name,
                        );
                        if full {
                            break;
                        }
                    }
                    reply.ok();
                }
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    fn readdirplus(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        self.rt.spawn(async move {
            match core.readdir(ino.0).await {
                Ok(entries) => {
                    for (index, entry) in entries.into_iter().enumerate().skip(offset as usize) {
                        let attr = entry
                            .attr
                            .map(|a| to_file_attr(&a))
                            .unwrap_or_else(|| fallback_file_attr(entry.ino, entry.kind));
                        let full = reply.add(
                            INodeNo(entry.ino),
                            (index + 1) as u64,
                            entry.name,
                            &ttl,
                            &attr,
                            Generation(0),
                        );
                        if full {
                            break;
                        }
                    }
                    reply.ok();
                }
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let core = Arc::clone(&self.core);
        self.rt.spawn(async move {
            match core.open(ino.0, flags.0).await {
                Ok(opened) => reply.opened(FileHandle(opened.fh), fopen_flags(&opened)),
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let core = Arc::clone(&self.core);
        let ttl = self.core.attr_ttl();
        let name = name.to_string_lossy().into_owned();
        self.rt.spawn(async move {
            match core.create(parent.0, &name, mode, flags).await {
                Ok((attr, opened)) => {
                    let mut file_attr = to_file_attr(&attr);
                    file_attr.perm = (mode & 0o777) as u16;
                    reply.created(
                        &ttl,
                        &file_attr,
                        Generation(0),
                        FileHandle(opened.fh),
                        fopen_flags(&opened),
                    );
                }
                Err(e) => reply.error(to_errno(e)),
            }
        });
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
        let core = Arc::clone(&self.core);
        self.rt.spawn(async move {
            match core.read(fh.0, offset, size).await {
                Ok(data) => reply.data(&data),
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
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
        let core = Arc::clone(&self.core);
        // Copy the borrowed kernel buffer before moving into the task. With
        // 1 MiB max_write this is one copy per MiB; Phase 3 zero-copies it.
        let data = data.to_vec();
        self.rt.spawn(async move {
            match core.write(ino.0, fh.0, offset, &data).await {
                Ok(written) => reply.written(written),
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let core = Arc::clone(&self.core);
        self.rt.spawn(async move {
            match core.flush(fh.0).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let core = Arc::clone(&self.core);
        self.rt.spawn(async move {
            match core.fsync(fh.0).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(to_errno(e)),
            }
        });
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
        let core = Arc::clone(&self.core);
        self.rt.spawn(async move {
            match core.release(fh.0).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let core = Arc::clone(&self.core);
        let name = name.to_string_lossy().into_owned();
        self.rt.spawn(async move {
            match core.unlink(parent.0, &name).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let core = Arc::clone(&self.core);
        let name = name.to_string_lossy().into_owned();
        self.rt.spawn(async move {
            match core.rmdir(parent.0, &name).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(to_errno(e)),
            }
        });
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        // KeInFS exposes no fixed capacity; report a large advertised block size
        // (matching the per-file blksize) and unlimited free space.
        reply.statfs(0, 0, 0, 0, 0, STATFS_BSIZE, STATFS_NAMELEN, 0);
    }
}
