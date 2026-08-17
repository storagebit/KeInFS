// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use keinctl::proto::FailureDomain;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use tonic::Status;

#[derive(Clone, Debug, Default)]
pub(crate) struct ReservationBinRegistry {
    keys: Arc<RwLock<HashSet<ReservationBinKey>>>,
}

impl ReservationBinRegistry {
    pub(crate) async fn remember(&self, key: ReservationBinKey) {
        self.keys.write().await.insert(key);
    }

    pub(crate) async fn snapshot(&self) -> Vec<ReservationBinKey> {
        self.keys.read().await.iter().cloned().collect()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReservationBinGate {
    locks: Arc<RwLock<HashMap<ReservationBinKey, Arc<Mutex<()>>>>>,
}

impl ReservationBinGate {
    pub(crate) async fn acquire(&self, key: &ReservationBinKey) -> OwnedMutexGuard<()> {
        self.lock_for(key).await.lock_owned().await
    }

    pub(crate) async fn try_acquire(&self, key: &ReservationBinKey) -> Option<OwnedMutexGuard<()>> {
        self.lock_for(key).await.try_lock_owned().ok()
    }

    async fn lock_for(&self, key: &ReservationBinKey) -> Arc<Mutex<()>> {
        if let Some(lock) = self.locks.read().await.get(key).cloned() {
            return lock;
        }

        let mut locks = self.locks.write().await;
        locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ReservationBinKey {
    fragment_count: usize,
    failure_domain: i32,
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
impl ReservationBinKey {
    pub(crate) fn new(fragment_count: usize, failure_domain: FailureDomain) -> Self {
        Self {
            fragment_count,
            failure_domain: failure_domain as i32,
        }
    }

    pub(crate) fn fragment_count(&self) -> usize {
        self.fragment_count
    }

    pub(crate) fn failure_domain(&self) -> Result<FailureDomain, Status> {
        FailureDomain::try_from(self.failure_domain)
            .map_err(|_| Status::internal("invalid reservation bin failure_domain"))
    }

    pub(crate) fn failure_domain_raw(&self) -> i32 {
        self.failure_domain
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StorePhaseTiming {
    pub(crate) name: &'static str,
    pub(crate) elapsed: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct TimedStoreResult<T> {
    pub(crate) value: T,
    pub(crate) phase_timings: Vec<StorePhaseTiming>,
}

#[derive(Clone, Debug)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) struct ReservationMutationSpec {
    pub(crate) reservation_id: String,
    pub(crate) placement_indexes: Vec<u32>,
}
