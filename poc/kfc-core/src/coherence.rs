// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! Cache coherence sink.
//!
//! The NATS invalidation loop (driven from `keinfs.kms.events`) lives in the
//! portable core, but pushing an invalidation *into the kernel page cache* is a
//! transport capability. The transport implements [`CoherenceSink`] over its
//! backend's notifier (e.g. fuser's `Notifier::inval_inode`/`inval_entry`) and
//! hands it to the core. Phase 1 ships the in-process side and a no-op sink;
//! Phase 2 (task #9) wires the real kernel notifier so `FOPEN_KEEP_CACHE` data
//! is invalidated on out-of-band mutation instead of forcing `DIRECT_IO`.

use keinctl::proto::MetadataInvalidationEvent;
use prost::Message;

/// Pushes invalidations into the kernel-side cache of the active transport.
/// Implementations must be cheap and non-blocking; calls happen from the NATS
/// event loop.
pub trait CoherenceSink: Send + Sync + 'static {
    /// Invalidate cached data + attributes for one inode.
    fn inval_inode(&self, ino: u64);
    /// Invalidate a (parent, name) dentry mapping.
    fn inval_entry(&self, parent_ino: u64, name: &str);
}

/// A sink that drops every notification — used until a transport installs a
/// real one (and for the headless unit tests).
pub struct NoopSink;

impl CoherenceSink for NoopSink {
    fn inval_inode(&self, _ino: u64) {}
    fn inval_entry(&self, _parent_ino: u64, _name: &str) {}
}

/// Decode a KMS invalidation event. KMS emits the protobuf form; older/looser
/// publishers may emit a bare namespace-id string, which we treat as a
/// namespace-wide invalidation (mirrors `poc/kfc/src/mount.rs:1412`).
pub(crate) fn decode_event(payload: &[u8]) -> Option<MetadataInvalidationEvent> {
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
