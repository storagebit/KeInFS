// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! KSC object engine — the native KP2 data path.
//!
//! All object bytes travel KSC<->KST over KP2 on raw HTTP/2 (FIRST_PRINCIPLES
//! §2/§5); this engine never relays through a coordinator. Phase 1 keeps the
//! original round-robin pool of `Arc<tokio::Mutex<ObjectClient>>` and the
//! whole-object `get`/`put` semantics. Making a single `ObjectClient` shareable
//! on the hot path (so per-call locking disappears) is task #7 / prereq P1;
//! stripe-granular `get_object_range` is task #8 / prereq P0.

use crate::error::{classify, FsErrno};
use crate::metadata::DynError;
use ksc::object::{ObjectClient, ObjectClientOptions};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

type SharedClient = Arc<tokio::sync::Mutex<ObjectClient>>;

pub(crate) struct ObjectEngine {
    read_clients: Vec<SharedClient>,
    write_clients: Vec<SharedClient>,
    read_next: AtomicUsize,
    write_next: AtomicUsize,
}

impl ObjectEngine {
    pub async fn connect(
        kms_endpoints: &[String],
        options: ObjectClientOptions,
        pool_size: usize,
    ) -> Result<Self, DynError> {
        let read_clients = Self::connect_pool(kms_endpoints, &options, pool_size).await?;
        // The write pool shares ONE EC-encode-buffer recycling registry across all
        // its clients, so per-client buffer retention does not multiply by the
        // pool size (the 2026-06 write-RAM growth). Read clients never encode, so
        // they keep their own (unused) per-client free-lists.
        let write_clients =
            Self::connect_write_pool(kms_endpoints, &options, pool_size).await?;
        Ok(Self {
            read_clients,
            write_clients,
            read_next: AtomicUsize::new(0),
            write_next: AtomicUsize::new(0),
        })
    }

    async fn connect_pool(
        kms_endpoints: &[String],
        options: &ObjectClientOptions,
        pool_size: usize,
    ) -> Result<Vec<SharedClient>, DynError> {
        let mut clients = Vec::with_capacity(pool_size);
        for _ in 0..pool_size.max(1) {
            let client = ObjectClient::connect_with_options(kms_endpoints, options.clone()).await?;
            clients.push(Arc::new(tokio::sync::Mutex::new(client)));
        }
        Ok(clients)
    }

    /// Like [`Self::connect_pool`] but every client shares one EC-encode-buffer
    /// recycling registry, bounding write-path buffer retention to a single
    /// working set instead of `pool_size` copies.
    async fn connect_write_pool(
        kms_endpoints: &[String],
        options: &ObjectClientOptions,
        pool_size: usize,
    ) -> Result<Vec<SharedClient>, DynError> {
        let shared = ksc::object::new_shared_shard_freelists();
        let mut clients = Vec::with_capacity(pool_size);
        for _ in 0..pool_size.max(1) {
            let client = ObjectClient::connect_with_options_sharing_shard_pool(
                kms_endpoints,
                options.clone(),
                shared.clone(),
            )
            .await?;
            clients.push(Arc::new(tokio::sync::Mutex::new(client)));
        }
        Ok(clients)
    }

    fn select(pool: &[SharedClient], counter: &AtomicUsize) -> SharedClient {
        let index = counter.fetch_add(1, Ordering::Relaxed) % pool.len();
        Arc::clone(&pool[index])
    }

    /// Whole-object read. Retained as the in-RAM symmetric counterpart to
    /// [`ObjectEngine::put_object`]; NOT used on any hot path. The read-only
    /// reads use [`ObjectEngine::get_object_range`], and the RMW-open seed now
    /// streams via `get_object_range` in bounded chunks (see
    /// `core.rs::seed_rmw_buffer`) rather than loading the whole object — keep
    /// this scoped so a future caller does not reintroduce a whole-object RAM
    /// load on a hot path.
    #[allow(dead_code)]
    pub async fn get_object(&self, bucket_id: &str, key: &str) -> Result<Vec<u8>, FsErrno> {
        let client = Self::select(&self.read_clients, &self.read_next);
        let mut client = client.lock().await;
        client
            .get_object_single_stripe(bucket_id, key)
            .await
            .map(|result| result.payload)
            .map_err(|err| classify(&err))
    }

    /// Stripe-granular ranged read — fetches only the stripes the byte range
    /// `[offset, offset+len)` touches (KSC `get_object_range`), instead of the
    /// whole object. This is the Phase 2 read path.
    ///
    /// Returns `(payload, stripe_width)`; the caller's stripe cache aligns and
    /// keys on `stripe_width = data_fragments * fragment_bytes`.
    pub async fn get_object_range(
        &self,
        bucket_id: &str,
        key: &str,
        offset: u64,
        len: u64,
    ) -> Result<(Vec<u8>, u64), FsErrno> {
        let client = Self::select(&self.read_clients, &self.read_next);
        let mut client = client.lock().await;
        client
            .get_object_range(bucket_id, key, offset, len)
            .await
            .map(|result| {
                let stripe_width = (result.ec_profile.data_fragments as u64)
                    .saturating_mul(result.ec_profile.fragment_bytes as u64);
                (result.payload, stripe_width)
            })
            .map_err(|err| classify(&err))
    }

    /// Whole-object write from an in-RAM payload (a put = a new immutable
    /// version, FIRST_PRINCIPLES §12). Retained for the small/in-RAM callers and
    /// tests; the writable-handle commit path uses [`ObjectEngine::put_object_from_path`]
    /// so a multi-GB object is never resident in RAM.
    ///
    /// Currently unused by the commit path (which streams from the temp file) but
    /// kept as the small/in-RAM symmetric counterpart to `get_object`.
    #[allow(dead_code)]
    pub async fn put_object(
        &self,
        bucket_id: &str,
        key: &str,
        payload: &[u8],
    ) -> Result<(), FsErrno> {
        let client = Self::select(&self.write_clients, &self.write_next);
        let mut client = client.lock().await;
        client
            .put_object_single_stripe(bucket_id, key, payload)
            .await
            .map(|_| ())
            .map_err(|err| classify(&err))
    }

    /// Streaming-writeback v1 commit: write an object whose payload lives in a
    /// temp file at `path` (the staged writable handle). KSC's
    /// `put_object_from_path` reads each EC stripe range from the file via a
    /// bounded `seek`+`read` per stripe — so the whole object is never resident
    /// in RAM on the commit path either.
    ///
    /// `logical_len` is AUTHORITATIVE: the caller snapshots `(path, logical_len)`
    /// atomically under the handle lock and passes the length here, and KSC keys
    /// the stripe loop / `initiate_object_write` off THIS value rather than
    /// re-stat'ing the file. That closes the commit-vs-concurrent-write TOCTOU
    /// (a write extending or shrinking the temp file between snapshot and stream
    /// cannot change the committed length, and a racing shrink can no longer
    /// EOF-fail a per-stripe read — short reads zero-fill to the snapshot length).
    ///
    /// NOTE (v1 tradeoff): KSC's per-stripe `read_range` closure is a synchronous
    /// `FnMut`, so the per-stripe `pread` runs as blocking file I/O on the async
    /// task that drives the put. For v1 this is acceptable — the reads are
    /// bounded (one stripe at a time) and interleave with the network/EC work; a
    /// fully async (spawn_blocking) per-stripe producer is the documented
    /// follow-up alongside true stream-as-you-write.
    pub async fn put_object_from_path(
        &self,
        bucket_id: &str,
        key: &str,
        path: &Path,
        logical_len: u64,
    ) -> Result<(), FsErrno> {
        let client = Self::select(&self.write_clients, &self.write_next);
        let mut client = client.lock().await;
        client
            .put_object_from_path(bucket_id, key, path, logical_len)
            .await
            .map(|_| ())
            .map_err(|err| classify(&err))
    }

    pub async fn delete_object(&self, bucket_id: &str, key: &str) -> Result<(), FsErrno> {
        let client = Self::select(&self.write_clients, &self.write_next);
        let mut client = client.lock().await;
        client
            .delete_object(bucket_id, key, &[])
            .await
            .map(|_| ())
            .map_err(|err| classify(&err))
    }
}
