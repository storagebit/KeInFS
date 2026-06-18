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
        let write_clients = Self::connect_pool(kms_endpoints, &options, pool_size).await?;
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

    fn select(pool: &[SharedClient], counter: &AtomicUsize) -> SharedClient {
        let index = counter.fetch_add(1, Ordering::Relaxed) % pool.len();
        Arc::clone(&pool[index])
    }

    /// Whole-object read. Used for writable-handle read-modify-write staging;
    /// read-only handles use [`ObjectEngine::get_object_range`].
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

    /// Whole-object write (a put = a new immutable version, FIRST_PRINCIPLES
    /// §12). Phase 3 replaces this with streaming writeback.
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
