// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! FUSE-over-io_uring backend (Phase 5) — Linux, kernel >= 6.14.
//!
//! This is the Tier-1 max-throughput transport: kernel-managed io_uring command
//! rings (`FUSE_IO_URING_CMD_REGISTER` + `COMMIT_AND_FETCH`) with zero-copy and
//! one worker per pinned core. Its programming model is async, so the entire
//! implementation lives here behind the `io-uring` feature + `target_os =
//! "linux"` gate; the portable `kfc-core` never sees it (FIRST_PRINCIPLES §1).
//!
//! Task #14 lands the real `fractal-fuse`-based implementation and validates it
//! on the Linux KVM lab. Until then the probe returns `false` so the always-
//! correct fuser backend is selected, and `mount_blocking` is a guarded stub.
//!
//! NOTE: this module compiles only on Linux with `--features io-uring`; it is
//! never built on macOS.

use crate::{BackendKind, FuseBackend, MountOpts};
use kfc_core::FsCore;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

/// Whether the running kernel advertises FUSE-over-io_uring (>= 6.14, the
/// `FUSE_OVER_IO_URING` init flag). Returns `false` until the backend is
/// implemented, so [`crate::select_backend`] always falls back to fuser.
pub(crate) fn kernel_supports_io_uring() -> bool {
    false
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
        _core: Arc<FsCore>,
        _runtime: tokio::runtime::Runtime,
        _mountpoint: PathBuf,
        _opts: MountOpts,
    ) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "FUSE-over-io_uring backend (Phase 5 / task #14) is not yet implemented",
        ))
    }
}
