// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! # kfc-transport
//!
//! The FUSE transport shim for KFC v2. It owns the only dependency on a
//! concrete FUSE backend and selects one at runtime per OS, behind the
//! [`FuseBackend`] trait. The portable [`kfc_core::FsCore`] is unaware of which
//! backend is live.
//!
//! Phase 1 ships the always-correct `fuser` backend (pure-Rust `/dev/fuse` on
//! Linux; FUSE-T/macFUSE via `macos-no-mount` on macOS). Phase 5 adds a
//! FUSE-over-io_uring backend behind the `io-uring` feature, chosen only when
//! the kernel advertises `FUSE_OVER_IO_URING`.

use kfc_core::{FsConfig, FsCore};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

mod fuser_backend;
#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod uring_backend;

pub use kfc_core::{CompletionMode, DEFAULT_METADATA_NOTIFICATION_SUBJECT};

/// Which concrete kernel backend a [`FuseBackend`] drives. Informational for
/// the observability tree; the data path is identical (KP2-direct to KST).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    /// fuser over `/dev/fuse` (Linux) — the always-correct Tier-2 path.
    FuserDevFuse,
    /// fuser over FUSE-over-io_uring (Linux >= 6.14) — Tier-1 max throughput.
    FuserIoUring,
    /// fuser attaching to a FUSE-T (kext-less, NFSv4-localhost) mount (macOS).
    FuserFuseT,
    /// fuser attaching to a macFUSE (kext) mount (macOS).
    FuserMacFuse,
}

impl BackendKind {
    pub fn name(self) -> &'static str {
        match self {
            BackendKind::FuserDevFuse => "fuser/dev-fuse",
            BackendKind::FuserIoUring => "fuser/io-uring",
            BackendKind::FuserFuseT => "fuser/fuse-t",
            BackendKind::FuserMacFuse => "fuser/macfuse",
        }
    }
}

/// Mount-time options independent of the chosen backend.
#[derive(Clone, Debug)]
pub struct MountOpts {
    pub fs_name: String,
    pub allow_other: bool,
    pub auto_unmount: bool,
}

impl Default for MountOpts {
    fn default() -> Self {
        Self {
            fs_name: "keinfs".to_string(),
            allow_other: false,
            auto_unmount: false,
        }
    }
}

/// A mountable FUSE backend. `mount_blocking` takes ownership of the tokio
/// runtime so dispatched request tasks and the NATS coherence loop stay alive
/// for the whole session, and blocks until the filesystem is unmounted.
pub trait FuseBackend: Send {
    fn kind(&self) -> BackendKind;

    fn mount_blocking(
        self: Box<Self>,
        core: Arc<FsCore>,
        runtime: tokio::runtime::Runtime,
        mountpoint: PathBuf,
        opts: MountOpts,
    ) -> io::Result<()>;
}

/// Build the multi-threaded tokio runtime that drives the KSC h2 data path and
/// the dispatched FUSE request tasks. Sized to available parallelism (the
/// original client hard-coded 4 worker threads — see `mount.rs:182`).
fn build_runtime() -> io::Result<tokio::runtime::Runtime> {
    let workers = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
        .max(4);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(workers)
        .thread_name("kfc-rt")
        .build()
}

/// Select the fastest viable backend for this host at runtime.
///
/// Phase 1: always the `fuser` backend. On Linux that is `/dev/fuse`; the
/// io_uring backend (Phase 5) is chosen only when compiled in AND the kernel
/// advertises the capability. On macOS we probe for FUSE-T and fall back to
/// macFUSE — both drive the same fuser `macos-no-mount` path, so the choice is
/// currently informational (the divergence is the external mounter).
pub fn select_backend() -> Box<dyn FuseBackend> {
    // Tier-1: FUSE-over-io_uring, only when compiled in and the kernel supports
    // it. This early-return is genuine (it skips the fuser fallback below).
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        if uring_backend::kernel_supports_io_uring() {
            return Box::new(uring_backend::UringBackend::new());
        }
    }

    // Tier-2: the always-correct fuser backend. The reported kind differs per OS
    // (macOS probes FUSE-T vs macFUSE) but the mount path is the same.
    #[cfg(target_os = "macos")]
    let kind = detect_macos_backend();
    #[cfg(not(target_os = "macos"))]
    let kind = BackendKind::FuserDevFuse;

    Box::new(fuser_backend::FuserBackend::new(kind))
}

/// Best-effort detection of the installed macOS FUSE provider. FUSE-T is
/// preferred (kext-less). This only sets the reported backend identity; the
/// mount mechanism is the same fuser `macos-no-mount` path either way.
#[cfg(target_os = "macos")]
fn detect_macos_backend() -> BackendKind {
    // FUSE-T installs its dylib/launchd under /usr/local/lib/fuse-t or
    // /Library/Application Support/fuse-t.
    let fuse_t_present = std::path::Path::new("/usr/local/lib/libfuse-t.dylib").exists()
        || std::path::Path::new("/Library/Application Support/fuse-t").exists()
        || std::path::Path::new("/usr/local/bin/go-nfsv4").exists();
    if fuse_t_present {
        BackendKind::FuserFuseT
    } else {
        BackendKind::FuserMacFuse
    }
}

/// Top-level entry: connect the core, pick a backend, and mount (blocking until
/// unmount). This is what the `kfc` binary calls.
pub fn run_mount(config: FsConfig, mountpoint: PathBuf, opts: MountOpts) -> io::Result<()> {
    let runtime = build_runtime()?;
    let core = runtime
        .block_on(FsCore::connect(config))
        .map_err(|err| io::Error::other(err.to_string()))?;
    let backend = select_backend();
    eprintln!(
        "KFC v2 mounting {} via {}",
        mountpoint.display(),
        backend.kind().name()
    );
    backend.mount_blocking(core, runtime, mountpoint, opts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_names_are_stable() {
        assert_eq!(BackendKind::FuserDevFuse.name(), "fuser/dev-fuse");
        assert_eq!(BackendKind::FuserIoUring.name(), "fuser/io-uring");
    }

    #[test]
    fn select_backend_returns_a_backend() {
        // On a dev host with no kernel io_uring FUSE and no probe, this must not
        // panic and must yield a fuser backend.
        let backend = select_backend();
        let kind = backend.kind();
        assert!(matches!(
            kind,
            BackendKind::FuserDevFuse
                | BackendKind::FuserIoUring
                | BackendKind::FuserFuseT
                | BackendKind::FuserMacFuse
        ));
    }
}
