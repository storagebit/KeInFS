// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! # kfc-core
//!
//! The portable, transport-agnostic heart of the KeInFS FUSE client (KFC v2).
//!
//! `kfc-core` owns the inode model, the KMS control-flow, the KSC object data
//! path, caching, and NATS-driven coherence. It contains **no** dependency on
//! any FUSE transport library and **no** `#[cfg(target_os)]` gate — it compiles
//! and unit-tests on Linux, macOS, and Windows alike. This is the "run
//! anywhere" client core that FIRST_PRINCIPLES §1 explicitly grants the client
//! stack.
//!
//! A transport crate (`kfc-transport`) drives an [`FsCore`] by spawning one
//! task per FUSE request that awaits a core op and completes the kernel reply.
//! Object bytes always travel KSC↔KST over KP2 on raw HTTP/2 (§2/§5), never
//! relayed; gRPC is reserved for KMS control flow (§10).
//!
//! ## Layout
//! - [`FsCore`] — async filesystem operations over sharded state.
//! - [`FsConfig`] — construction parameters.
//! - [`CoherenceSink`] — the transport's kernel-cache invalidation hook.
//! - value types: [`Attr`], [`FileKind`], [`DirEntry`], [`OpenedFile`],
//!   [`Capabilities`], [`DesiredKernelConfig`], [`FsErrno`].

mod coherence;
mod core;
mod error;
mod metadata;
mod object;
mod state;
mod stripe_cache;
mod types;

pub use crate::coherence::{CoherenceSink, NoopSink};
pub use crate::core::{FsConfig, FsCore};
pub use crate::error::FsErrno;
pub use crate::metadata::{boxed_error, DynError};
pub use crate::types::{
    Attr, Capabilities, DesiredKernelConfig, DirEntry, FileKind, OpenedFile, ROOT_INO,
};

// Re-export the KSC completion-mode enum so transports and binaries can build
// an [`FsConfig`] without taking a direct KSC dependency.
pub use ksc::client::CompletionMode;
pub use ksc::object::DEFAULT_METADATA_NOTIFICATION_SUBJECT;
