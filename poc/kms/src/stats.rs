// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use keinbuild::BuildInfo;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const LATENCY_BUCKETS: usize = 32;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct KmsIdentity {
    pub(crate) build: BuildInfo,
    pub(crate) listen_addr: String,
    pub(crate) kas_endpoints: String,
    pub(crate) shard_id: String,
    pub(crate) public_endpoint: String,
    pub(crate) metadata_store: String,
    pub(crate) pid: u32,
    pub(crate) stats_root: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct LatencySummary {
    pub(crate) samples: u64,
    pub(crate) avg_us: u64,
    pub(crate) p50_us: u64,
    pub(crate) p95_us: u64,
    pub(crate) p99_us: u64,
    pub(crate) max_us: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RpcSnapshot {
    pub(crate) requests: u64,
    pub(crate) errors: u64,
    pub(crate) latency: LatencySummary,
    pub(crate) phases: BTreeMap<String, LatencySummary>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct BackgroundSnapshot {
    pub(crate) runs: u64,
    pub(crate) released_reservations: u64,
    pub(crate) run_latency: LatencySummary,
    pub(crate) release_latency: LatencySummary,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct KmsSnapshot {
    pub(crate) identity: KmsIdentity,
    pub(crate) uptime_ms: u64,
    pub(crate) started_unix_s: u64,
    pub(crate) total_requests: u64,
    pub(crate) total_errors: u64,
    pub(crate) create_namespace_requests: u64,
    pub(crate) list_namespaces_requests: u64,
    pub(crate) get_namespace_requests: u64,
    pub(crate) create_namespace_entry_requests: u64,
    pub(crate) create_ec_profile_requests: u64,
    pub(crate) list_ec_profiles_requests: u64,
    pub(crate) create_bucket_requests: u64,
    pub(crate) get_bucket_requests: u64,
    pub(crate) list_buckets_requests: u64,
    pub(crate) resolve_path_requests: u64,
    pub(crate) list_children_requests: u64,
    pub(crate) watch_entry_requests: u64,
    pub(crate) watch_prefix_requests: u64,
    pub(crate) resolve_shard_requests: u64,
    pub(crate) initiate_object_write_requests: u64,
    pub(crate) reserve_object_write_window_requests: u64,
    pub(crate) commit_object_write_window_requests: u64,
    pub(crate) commit_object_write_requests: u64,
    pub(crate) abort_object_write_requests: u64,
    pub(crate) repair_object_write_requests: u64,
    pub(crate) list_write_intents_requests: u64,
    pub(crate) get_write_intent_requests: u64,
    pub(crate) resolve_object_read_requests: u64,
    pub(crate) delete_object_requests: u64,
    pub(crate) lease_rebuild_tasks_requests: u64,
    pub(crate) commit_rebuild_requests: u64,
    pub(crate) lease_placement_tasks_requests: u64,
    pub(crate) commit_placement_task_requests: u64,
    pub(crate) fail_placement_task_requests: u64,
    pub(crate) list_placement_tasks_requests: u64,
    pub(crate) get_placement_task_requests: u64,
    pub(crate) report_target_failure_requests: u64,
    pub(crate) drain_target_requests: u64,
    pub(crate) preview_target_rebalance_requests: u64,
    pub(crate) enqueue_target_rebalance_requests: u64,
    pub(crate) recover_target_requests: u64,
    pub(crate) retire_target_requests: u64,
    pub(crate) get_target_placement_status_requests: u64,
    pub(crate) list_metadata_events_requests: u64,
    pub(crate) expired_write_intents: u64,
    pub(crate) reservation_cache_hits: u64,
    pub(crate) reservation_cache_misses: u64,
    pub(crate) reservation_cache_refills: u64,
    pub(crate) reservation_cache_depth: u64,
    pub(crate) route_discovery_lookups: u64,
    pub(crate) route_discovery_rpcs: u64,
    pub(crate) route_cache_hits: u64,
    pub(crate) route_cache_misses: u64,
    pub(crate) reservation_cache_shard_bypasses: u64,
    pub(crate) reservation_cache_serves: u64,
    pub(crate) rpcs: BTreeMap<String, RpcSnapshot>,
    pub(crate) background: BackgroundSnapshot,
    pub(crate) last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct RuntimeStatusSnapshot {
    service: &'static str,
    health: &'static str,
    ready: bool,
    uptime_ms: u64,
    started_unix_s: u64,
    pid: u32,
    total_requests: u64,
    total_errors: u64,
    shard_id: String,
    reservation_cache_depth: u64,
    last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct RuntimeEventRecord {
    observed_unix_ms: u64,
    service: &'static str,
    health: &'static str,
    message: String,
}

#[derive(Default)]
struct LatencyRecorder {
    samples: u64,
    total_us: u64,
    max_us: u64,
    buckets: [u64; LATENCY_BUCKETS],
}

impl LatencyRecorder {
    fn observe(&mut self, elapsed: Duration) {
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.samples += 1;
        self.total_us = self.total_us.saturating_add(micros);
        self.max_us = self.max_us.max(micros);
        self.buckets[latency_bucket_index(micros)] += 1;
    }

    fn snapshot(&self) -> LatencySummary {
        if self.samples == 0 {
            return LatencySummary {
                samples: 0,
                avg_us: 0,
                p50_us: 0,
                p95_us: 0,
                p99_us: 0,
                max_us: 0,
            };
        }
        LatencySummary {
            samples: self.samples,
            avg_us: self.total_us / self.samples,
            p50_us: percentile_from_buckets(&self.buckets, self.samples, 0.50),
            p95_us: percentile_from_buckets(&self.buckets, self.samples, 0.95),
            p99_us: percentile_from_buckets(&self.buckets, self.samples, 0.99),
            max_us: self.max_us,
        }
    }
}

struct RpcRuntimeStats {
    requests: AtomicU64,
    errors: AtomicU64,
    latency: Mutex<LatencyRecorder>,
    phases: Mutex<BTreeMap<String, LatencyRecorder>>,
}

impl RpcRuntimeStats {
    fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latency: Mutex::new(LatencyRecorder::default()),
            phases: Mutex::new(BTreeMap::new()),
        }
    }

    fn record_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_success(&self, elapsed: Duration) {
        self.latency.lock().unwrap().observe(elapsed);
    }

    fn record_error(&self, elapsed: Duration) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        self.latency.lock().unwrap().observe(elapsed);
    }

    fn record_phase(&self, phase: &str, elapsed: Duration) {
        let mut phases = self.phases.lock().unwrap();
        phases
            .entry(phase.to_string())
            .or_default()
            .observe(elapsed);
    }

    fn snapshot(&self) -> RpcSnapshot {
        let phases = self
            .phases
            .lock()
            .unwrap()
            .iter()
            .map(|(name, recorder)| (name.clone(), recorder.snapshot()))
            .collect();
        RpcSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            latency: self.latency.lock().unwrap().snapshot(),
            phases,
        }
    }
}

pub(crate) struct KmsStats {
    identity: KmsIdentity,
    started: Instant,
    started_unix_s: u64,
    total_requests: AtomicU64,
    total_errors: AtomicU64,
    expired_write_intents: AtomicU64,
    reservation_cache_hits: AtomicU64,
    reservation_cache_misses: AtomicU64,
    reservation_cache_refills: AtomicU64,
    reservation_cache_depth: AtomicU64,
    // Write-scale instrumentation. These surface the reserve-path
    // decomposition the lab needs to measure (route-discovery
    // amplification, cache effectiveness, synchronous KAS bypasses).
    route_discovery_lookups: AtomicU64,
    route_discovery_rpcs: AtomicU64,
    route_cache_hits: AtomicU64,
    route_cache_misses: AtomicU64,
    reservation_cache_shard_bypasses: AtomicU64,
    reservation_cache_serves: AtomicU64,
    reaper_runs: AtomicU64,
    reaper_released_reservations: AtomicU64,
    reaper_latency: Mutex<LatencyRecorder>,
    reaper_release_latency: Mutex<LatencyRecorder>,
    create_namespace: RpcRuntimeStats,
    list_namespaces: RpcRuntimeStats,
    get_namespace: RpcRuntimeStats,
    create_namespace_entry: RpcRuntimeStats,
    create_ec_profile: RpcRuntimeStats,
    list_ec_profiles: RpcRuntimeStats,
    create_bucket: RpcRuntimeStats,
    get_bucket: RpcRuntimeStats,
    list_buckets: RpcRuntimeStats,
    resolve_path: RpcRuntimeStats,
    list_children: RpcRuntimeStats,
    watch_entry: RpcRuntimeStats,
    watch_prefix: RpcRuntimeStats,
    resolve_shard: RpcRuntimeStats,
    initiate_object_write: RpcRuntimeStats,
    reserve_object_write_window: RpcRuntimeStats,
    commit_object_write_window: RpcRuntimeStats,
    commit_object_write: RpcRuntimeStats,
    commit_object: RpcRuntimeStats,
    abort_object_write: RpcRuntimeStats,
    repair_object_write: RpcRuntimeStats,
    list_write_intents: RpcRuntimeStats,
    get_write_intent: RpcRuntimeStats,
    resolve_object_read: RpcRuntimeStats,
    delete_object: RpcRuntimeStats,
    lease_rebuild_tasks: RpcRuntimeStats,
    commit_rebuild: RpcRuntimeStats,
    lease_placement_tasks: RpcRuntimeStats,
    commit_placement_task: RpcRuntimeStats,
    fail_placement_task: RpcRuntimeStats,
    list_placement_tasks: RpcRuntimeStats,
    get_placement_task: RpcRuntimeStats,
    report_target_failure: RpcRuntimeStats,
    drain_target: RpcRuntimeStats,
    preview_target_rebalance: RpcRuntimeStats,
    enqueue_target_rebalance: RpcRuntimeStats,
    recover_target: RpcRuntimeStats,
    retire_target: RpcRuntimeStats,
    get_target_placement_status: RpcRuntimeStats,
    list_metadata_events: RpcRuntimeStats,
    last_error: Mutex<Option<String>>,
}

impl KmsStats {
    pub(crate) fn new(identity: KmsIdentity) -> Arc<Self> {
        Arc::new(Self {
            identity,
            started: Instant::now(),
            started_unix_s: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            total_requests: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            expired_write_intents: AtomicU64::new(0),
            reservation_cache_hits: AtomicU64::new(0),
            reservation_cache_misses: AtomicU64::new(0),
            reservation_cache_refills: AtomicU64::new(0),
            reservation_cache_depth: AtomicU64::new(0),
            route_discovery_lookups: AtomicU64::new(0),
            route_discovery_rpcs: AtomicU64::new(0),
            route_cache_hits: AtomicU64::new(0),
            route_cache_misses: AtomicU64::new(0),
            reservation_cache_shard_bypasses: AtomicU64::new(0),
            reservation_cache_serves: AtomicU64::new(0),
            reaper_runs: AtomicU64::new(0),
            reaper_released_reservations: AtomicU64::new(0),
            reaper_latency: Mutex::new(LatencyRecorder::default()),
            reaper_release_latency: Mutex::new(LatencyRecorder::default()),
            create_namespace: RpcRuntimeStats::new(),
            list_namespaces: RpcRuntimeStats::new(),
            get_namespace: RpcRuntimeStats::new(),
            create_namespace_entry: RpcRuntimeStats::new(),
            create_ec_profile: RpcRuntimeStats::new(),
            list_ec_profiles: RpcRuntimeStats::new(),
            create_bucket: RpcRuntimeStats::new(),
            get_bucket: RpcRuntimeStats::new(),
            list_buckets: RpcRuntimeStats::new(),
            resolve_path: RpcRuntimeStats::new(),
            list_children: RpcRuntimeStats::new(),
            watch_entry: RpcRuntimeStats::new(),
            watch_prefix: RpcRuntimeStats::new(),
            resolve_shard: RpcRuntimeStats::new(),
            initiate_object_write: RpcRuntimeStats::new(),
            reserve_object_write_window: RpcRuntimeStats::new(),
            commit_object_write_window: RpcRuntimeStats::new(),
            commit_object_write: RpcRuntimeStats::new(),
            commit_object: RpcRuntimeStats::new(),
            abort_object_write: RpcRuntimeStats::new(),
            repair_object_write: RpcRuntimeStats::new(),
            list_write_intents: RpcRuntimeStats::new(),
            get_write_intent: RpcRuntimeStats::new(),
            resolve_object_read: RpcRuntimeStats::new(),
            delete_object: RpcRuntimeStats::new(),
            lease_rebuild_tasks: RpcRuntimeStats::new(),
            commit_rebuild: RpcRuntimeStats::new(),
            lease_placement_tasks: RpcRuntimeStats::new(),
            commit_placement_task: RpcRuntimeStats::new(),
            fail_placement_task: RpcRuntimeStats::new(),
            list_placement_tasks: RpcRuntimeStats::new(),
            get_placement_task: RpcRuntimeStats::new(),
            report_target_failure: RpcRuntimeStats::new(),
            drain_target: RpcRuntimeStats::new(),
            preview_target_rebalance: RpcRuntimeStats::new(),
            enqueue_target_rebalance: RpcRuntimeStats::new(),
            recover_target: RpcRuntimeStats::new(),
            retire_target: RpcRuntimeStats::new(),
            get_target_placement_status: RpcRuntimeStats::new(),
            list_metadata_events: RpcRuntimeStats::new(),
            last_error: Mutex::new(None),
        })
    }

    pub(crate) fn record_request(&self, kind: RpcKind) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.rpc(kind).record_request();
    }

    pub(crate) fn record_phase(&self, kind: RpcKind, phase: &str, elapsed: Duration) {
        self.rpc(kind).record_phase(phase, elapsed);
    }

    pub(crate) fn record_success(&self, kind: RpcKind, elapsed: Duration) {
        self.rpc(kind).record_success(elapsed);
    }

    pub(crate) fn record_error(
        &self,
        kind: RpcKind,
        elapsed: Duration,
        message: impl Into<String>,
    ) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        self.rpc(kind).record_error(elapsed);
        *self.last_error.lock().unwrap() = Some(message.into());
    }

    pub(crate) fn record_expired_write_intents(&self, count: usize) {
        self.expired_write_intents
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_reservation_cache_hit(&self) {
        self.reservation_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_reservation_cache_miss(&self) {
        self.reservation_cache_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_reservation_cache_refill(&self) {
        self.reservation_cache_refills
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_reservation_cache_depth(&self, depth: usize) {
        self.reservation_cache_depth
            .store(depth as u64, Ordering::Relaxed);
    }

    /// Record one allocation-shard route resolution and the number of
    /// `list_service_instances` RPCs it actually issued to KAS. With the
    /// uncached resolver this is ~6 RPCs/reserve; with the TTL route cache
    /// it should fall toward ~0 RPCs per lookup.
    pub(crate) fn record_route_discovery(&self, rpc_count: usize) {
        self.route_discovery_lookups.fetch_add(1, Ordering::Relaxed);
        self.route_discovery_rpcs
            .fetch_add(rpc_count as u64, Ordering::Relaxed);
    }

    /// Route TTL-cache hit (served from RAM, no KAS RPC).
    pub(crate) fn record_route_cache_hit(&self) {
        self.route_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Route TTL-cache miss (forced a fresh discovery).
    pub(crate) fn record_route_cache_miss(&self) {
        self.route_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// A foreground reserve that fell through to a synchronous KAS
    /// `reserve_stripe_batch` instead of draining the pool. On a multi-shard
    /// cluster this is the dominant write-scale bottleneck; the pre-staged
    /// RAM pool should drive it toward zero.
    pub(crate) fn record_reservation_cache_shard_bypass(&self) {
        self.reservation_cache_shard_bypasses
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A foreground reserve served from the pool branch (cache path),
    /// regardless of whether each stripe hit or had to refill on demand.
    pub(crate) fn record_reservation_cache_serve(&self) {
        self.reservation_cache_serves
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_reaper_run(&self, elapsed: Duration, released_reservations: usize) {
        self.reaper_runs.fetch_add(1, Ordering::Relaxed);
        self.reaper_released_reservations
            .fetch_add(released_reservations as u64, Ordering::Relaxed);
        self.reaper_latency.lock().unwrap().observe(elapsed);
    }

    pub(crate) fn record_reaper_release(&self, elapsed: Duration) {
        self.reaper_release_latency.lock().unwrap().observe(elapsed);
    }

    pub(crate) fn set_last_error(&self, message: impl Into<String>) {
        *self.last_error.lock().unwrap() = Some(message.into());
    }

    pub(crate) fn snapshot(&self) -> KmsSnapshot {
        let mut rpcs = BTreeMap::new();
        for kind in RpcKind::ALL {
            rpcs.insert(kind.name().to_string(), self.rpc(kind).snapshot());
        }
        KmsSnapshot {
            identity: self.identity.clone(),
            uptime_ms: self.started.elapsed().as_millis() as u64,
            started_unix_s: self.started_unix_s,
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            create_namespace_requests: self.create_namespace.requests.load(Ordering::Relaxed),
            list_namespaces_requests: self.list_namespaces.requests.load(Ordering::Relaxed),
            get_namespace_requests: self.get_namespace.requests.load(Ordering::Relaxed),
            create_namespace_entry_requests: self
                .create_namespace_entry
                .requests
                .load(Ordering::Relaxed),
            create_ec_profile_requests: self.create_ec_profile.requests.load(Ordering::Relaxed),
            list_ec_profiles_requests: self.list_ec_profiles.requests.load(Ordering::Relaxed),
            create_bucket_requests: self.create_bucket.requests.load(Ordering::Relaxed),
            get_bucket_requests: self.get_bucket.requests.load(Ordering::Relaxed),
            list_buckets_requests: self.list_buckets.requests.load(Ordering::Relaxed),
            resolve_path_requests: self.resolve_path.requests.load(Ordering::Relaxed),
            list_children_requests: self.list_children.requests.load(Ordering::Relaxed),
            watch_entry_requests: self.watch_entry.requests.load(Ordering::Relaxed),
            watch_prefix_requests: self.watch_prefix.requests.load(Ordering::Relaxed),
            resolve_shard_requests: self.resolve_shard.requests.load(Ordering::Relaxed),
            initiate_object_write_requests: self
                .initiate_object_write
                .requests
                .load(Ordering::Relaxed),
            reserve_object_write_window_requests: self
                .reserve_object_write_window
                .requests
                .load(Ordering::Relaxed),
            commit_object_write_window_requests: self
                .commit_object_write_window
                .requests
                .load(Ordering::Relaxed),
            commit_object_write_requests: self.commit_object_write.requests.load(Ordering::Relaxed),
            abort_object_write_requests: self.abort_object_write.requests.load(Ordering::Relaxed),
            repair_object_write_requests: self.repair_object_write.requests.load(Ordering::Relaxed),
            list_write_intents_requests: self.list_write_intents.requests.load(Ordering::Relaxed),
            get_write_intent_requests: self.get_write_intent.requests.load(Ordering::Relaxed),
            resolve_object_read_requests: self.resolve_object_read.requests.load(Ordering::Relaxed),
            delete_object_requests: self.delete_object.requests.load(Ordering::Relaxed),
            lease_rebuild_tasks_requests: self.lease_rebuild_tasks.requests.load(Ordering::Relaxed),
            commit_rebuild_requests: self.commit_rebuild.requests.load(Ordering::Relaxed),
            lease_placement_tasks_requests: self
                .lease_placement_tasks
                .requests
                .load(Ordering::Relaxed),
            commit_placement_task_requests: self
                .commit_placement_task
                .requests
                .load(Ordering::Relaxed),
            fail_placement_task_requests: self.fail_placement_task.requests.load(Ordering::Relaxed),
            list_placement_tasks_requests: self
                .list_placement_tasks
                .requests
                .load(Ordering::Relaxed),
            get_placement_task_requests: self.get_placement_task.requests.load(Ordering::Relaxed),
            report_target_failure_requests: self
                .report_target_failure
                .requests
                .load(Ordering::Relaxed),
            drain_target_requests: self.drain_target.requests.load(Ordering::Relaxed),
            preview_target_rebalance_requests: self
                .preview_target_rebalance
                .requests
                .load(Ordering::Relaxed),
            enqueue_target_rebalance_requests: self
                .enqueue_target_rebalance
                .requests
                .load(Ordering::Relaxed),
            recover_target_requests: self.recover_target.requests.load(Ordering::Relaxed),
            retire_target_requests: self.retire_target.requests.load(Ordering::Relaxed),
            get_target_placement_status_requests: self
                .get_target_placement_status
                .requests
                .load(Ordering::Relaxed),
            list_metadata_events_requests: self
                .list_metadata_events
                .requests
                .load(Ordering::Relaxed),
            expired_write_intents: self.expired_write_intents.load(Ordering::Relaxed),
            reservation_cache_hits: self.reservation_cache_hits.load(Ordering::Relaxed),
            reservation_cache_misses: self.reservation_cache_misses.load(Ordering::Relaxed),
            reservation_cache_refills: self.reservation_cache_refills.load(Ordering::Relaxed),
            reservation_cache_depth: self.reservation_cache_depth.load(Ordering::Relaxed),
            route_discovery_lookups: self.route_discovery_lookups.load(Ordering::Relaxed),
            route_discovery_rpcs: self.route_discovery_rpcs.load(Ordering::Relaxed),
            route_cache_hits: self.route_cache_hits.load(Ordering::Relaxed),
            route_cache_misses: self.route_cache_misses.load(Ordering::Relaxed),
            reservation_cache_shard_bypasses: self
                .reservation_cache_shard_bypasses
                .load(Ordering::Relaxed),
            reservation_cache_serves: self.reservation_cache_serves.load(Ordering::Relaxed),
            rpcs,
            background: BackgroundSnapshot {
                runs: self.reaper_runs.load(Ordering::Relaxed),
                released_reservations: self.reaper_released_reservations.load(Ordering::Relaxed),
                run_latency: self.reaper_latency.lock().unwrap().snapshot(),
                release_latency: self.reaper_release_latency.lock().unwrap().snapshot(),
            },
            last_error: self.last_error.lock().unwrap().clone(),
        }
    }

    fn rpc(&self, kind: RpcKind) -> &RpcRuntimeStats {
        match kind {
            RpcKind::CreateNamespace => &self.create_namespace,
            RpcKind::ListNamespaces => &self.list_namespaces,
            RpcKind::GetNamespace => &self.get_namespace,
            RpcKind::CreateNamespaceEntry => &self.create_namespace_entry,
            RpcKind::CreateEcProfile => &self.create_ec_profile,
            RpcKind::ListEcProfiles => &self.list_ec_profiles,
            RpcKind::CreateBucket => &self.create_bucket,
            RpcKind::GetBucket => &self.get_bucket,
            RpcKind::ListBuckets => &self.list_buckets,
            RpcKind::ResolvePath => &self.resolve_path,
            RpcKind::ListChildren => &self.list_children,
            RpcKind::WatchEntry => &self.watch_entry,
            RpcKind::WatchPrefix => &self.watch_prefix,
            RpcKind::ResolveShard => &self.resolve_shard,
            RpcKind::InitiateObjectWrite => &self.initiate_object_write,
            RpcKind::ReserveObjectWriteWindow => &self.reserve_object_write_window,
            RpcKind::CommitObjectWriteWindow => &self.commit_object_write_window,
            RpcKind::CommitObjectWrite => &self.commit_object_write,
            RpcKind::CommitObject => &self.commit_object,
            RpcKind::AbortObjectWrite => &self.abort_object_write,
            RpcKind::RepairObjectWrite => &self.repair_object_write,
            RpcKind::ListWriteIntents => &self.list_write_intents,
            RpcKind::GetWriteIntent => &self.get_write_intent,
            RpcKind::ResolveObjectRead => &self.resolve_object_read,
            RpcKind::DeleteObject => &self.delete_object,
            RpcKind::LeaseRebuildTasks => &self.lease_rebuild_tasks,
            RpcKind::CommitRebuild => &self.commit_rebuild,
            RpcKind::LeasePlacementTasks => &self.lease_placement_tasks,
            RpcKind::CommitPlacementTask => &self.commit_placement_task,
            RpcKind::FailPlacementTask => &self.fail_placement_task,
            RpcKind::ListPlacementTasks => &self.list_placement_tasks,
            RpcKind::GetPlacementTask => &self.get_placement_task,
            RpcKind::ReportTargetFailure => &self.report_target_failure,
            RpcKind::DrainTarget => &self.drain_target,
            RpcKind::PreviewTargetRebalance => &self.preview_target_rebalance,
            RpcKind::EnqueueTargetRebalance => &self.enqueue_target_rebalance,
            RpcKind::RecoverTarget => &self.recover_target,
            RpcKind::RetireTarget => &self.retire_target,
            RpcKind::GetTargetPlacementStatus => &self.get_target_placement_status,
            RpcKind::ListMetadataEvents => &self.list_metadata_events,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RpcKind {
    CreateNamespace,
    ListNamespaces,
    GetNamespace,
    CreateNamespaceEntry,
    CreateEcProfile,
    ListEcProfiles,
    CreateBucket,
    GetBucket,
    ListBuckets,
    ResolvePath,
    ListChildren,
    WatchEntry,
    WatchPrefix,
    ResolveShard,
    InitiateObjectWrite,
    ReserveObjectWriteWindow,
    CommitObjectWriteWindow,
    CommitObjectWrite,
    CommitObject,
    AbortObjectWrite,
    RepairObjectWrite,
    ListWriteIntents,
    GetWriteIntent,
    ResolveObjectRead,
    DeleteObject,
    LeaseRebuildTasks,
    CommitRebuild,
    LeasePlacementTasks,
    CommitPlacementTask,
    FailPlacementTask,
    ListPlacementTasks,
    GetPlacementTask,
    ReportTargetFailure,
    DrainTarget,
    PreviewTargetRebalance,
    EnqueueTargetRebalance,
    RecoverTarget,
    RetireTarget,
    GetTargetPlacementStatus,
    ListMetadataEvents,
}

impl RpcKind {
    const ALL: [Self; 40] = [
        Self::CreateNamespace,
        Self::ListNamespaces,
        Self::GetNamespace,
        Self::CreateNamespaceEntry,
        Self::CreateEcProfile,
        Self::ListEcProfiles,
        Self::CreateBucket,
        Self::GetBucket,
        Self::ListBuckets,
        Self::ResolvePath,
        Self::ListChildren,
        Self::WatchEntry,
        Self::WatchPrefix,
        Self::ResolveShard,
        Self::InitiateObjectWrite,
        Self::ReserveObjectWriteWindow,
        Self::CommitObjectWriteWindow,
        Self::CommitObjectWrite,
        Self::CommitObject,
        Self::AbortObjectWrite,
        Self::RepairObjectWrite,
        Self::ListWriteIntents,
        Self::GetWriteIntent,
        Self::ResolveObjectRead,
        Self::DeleteObject,
        Self::LeaseRebuildTasks,
        Self::CommitRebuild,
        Self::LeasePlacementTasks,
        Self::CommitPlacementTask,
        Self::FailPlacementTask,
        Self::ListPlacementTasks,
        Self::GetPlacementTask,
        Self::ReportTargetFailure,
        Self::DrainTarget,
        Self::PreviewTargetRebalance,
        Self::EnqueueTargetRebalance,
        Self::RecoverTarget,
        Self::RetireTarget,
        Self::GetTargetPlacementStatus,
        Self::ListMetadataEvents,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::CreateNamespace => "create_namespace",
            Self::ListNamespaces => "list_namespaces",
            Self::GetNamespace => "get_namespace",
            Self::CreateNamespaceEntry => "create_namespace_entry",
            Self::CreateEcProfile => "create_ec_profile",
            Self::ListEcProfiles => "list_ec_profiles",
            Self::CreateBucket => "create_bucket",
            Self::GetBucket => "get_bucket",
            Self::ListBuckets => "list_buckets",
            Self::ResolvePath => "resolve_path",
            Self::ListChildren => "list_children",
            Self::WatchEntry => "watch_entry",
            Self::WatchPrefix => "watch_prefix",
            Self::ResolveShard => "resolve_shard",
            Self::InitiateObjectWrite => "initiate_object_write",
            Self::ReserveObjectWriteWindow => "reserve_object_write_window",
            Self::CommitObjectWriteWindow => "commit_object_write_window",
            Self::CommitObjectWrite => "commit_object_write",
            Self::CommitObject => "commit_object",
            Self::AbortObjectWrite => "abort_object_write",
            Self::RepairObjectWrite => "repair_object_write",
            Self::ListWriteIntents => "list_write_intents",
            Self::GetWriteIntent => "get_write_intent",
            Self::ResolveObjectRead => "resolve_object_read",
            Self::DeleteObject => "delete_object",
            Self::LeaseRebuildTasks => "lease_rebuild_tasks",
            Self::CommitRebuild => "commit_rebuild",
            Self::LeasePlacementTasks => "lease_placement_tasks",
            Self::CommitPlacementTask => "commit_placement_task",
            Self::FailPlacementTask => "fail_placement_task",
            Self::ListPlacementTasks => "list_placement_tasks",
            Self::GetPlacementTask => "get_placement_task",
            Self::ReportTargetFailure => "report_target_failure",
            Self::DrainTarget => "drain_target",
            Self::PreviewTargetRebalance => "preview_target_rebalance",
            Self::EnqueueTargetRebalance => "enqueue_target_rebalance",
            Self::RecoverTarget => "recover_target",
            Self::RetireTarget => "retire_target",
            Self::GetTargetPlacementStatus => "get_target_placement_status",
            Self::ListMetadataEvents => "list_metadata_events",
        }
    }
}

pub(crate) struct Publisher {
    stop_tx: Option<mpsc::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl Publisher {
    pub(crate) fn spawn(
        stats: Arc<KmsStats>,
        root: impl AsRef<Path>,
        publish_interval: Duration,
    ) -> io::Result<Self> {
        let runtime_dir = root.as_ref().join(format!(
            "kms-{}-{}",
            stats.identity.shard_id, stats.identity.pid
        ));
        fs::create_dir_all(&runtime_dir)?;
        let (stop_tx, stop_rx) = mpsc::channel();
        let thread_runtime_dir = runtime_dir.clone();
        let join = thread::Builder::new()
            .name("kms-stats".to_string())
            .spawn(move || {
                let mut last_event_key: Option<(String, Option<String>)> = None;
                loop {
                    let snapshot = stats.snapshot();
                    let status = RuntimeStatusSnapshot::from_snapshot(&snapshot);
                    let _ = write_snapshot(&thread_runtime_dir, &snapshot);
                    let _ =
                        append_event_if_changed(&thread_runtime_dir, &status, &mut last_event_key);
                    if stop_rx.try_recv().is_ok() {
                        break;
                    }
                    thread::sleep(publish_interval);
                }
            })
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(Self {
            stop_tx: Some(stop_tx),
            join: Some(join),
        })
    }

    pub(crate) fn stop(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn write_snapshot(root: &Path, snapshot: &KmsSnapshot) -> io::Result<()> {
    fs::create_dir_all(root)?;
    fs::write(
        root.join("identity.toml"),
        toml::to_string_pretty(&snapshot.identity)
            .map_err(|err| io::Error::other(err.to_string()))?,
    )?;
    fs::write(
        root.join("status.toml"),
        toml::to_string_pretty(&RuntimeStatusSnapshot::from_snapshot(snapshot))
            .map_err(|err| io::Error::other(err.to_string()))?,
    )?;
    fs::write(
        root.join("summary.toml"),
        toml::to_string_pretty(snapshot).map_err(|err| io::Error::other(err.to_string()))?,
    )?;
    fs::write(
        root.join("summary"),
        serde_json::to_vec_pretty(snapshot).map_err(|err| io::Error::other(err.to_string()))?,
    )?;
    Ok(())
}

impl RuntimeStatusSnapshot {
    fn from_snapshot(snapshot: &KmsSnapshot) -> Self {
        let health = if snapshot.last_error.is_some() {
            "degraded"
        } else {
            "healthy"
        };
        Self {
            service: "kms",
            health,
            ready: true,
            uptime_ms: snapshot.uptime_ms,
            started_unix_s: snapshot.started_unix_s,
            pid: snapshot.identity.pid,
            total_requests: snapshot.total_requests,
            total_errors: snapshot.total_errors,
            shard_id: snapshot.identity.shard_id.clone(),
            reservation_cache_depth: snapshot.reservation_cache_depth,
            last_error: snapshot.last_error.clone(),
        }
    }
}

fn append_event_if_changed(
    root: &Path,
    status: &RuntimeStatusSnapshot,
    last_event_key: &mut Option<(String, Option<String>)>,
) -> io::Result<()> {
    let current_key = (status.health.to_string(), status.last_error.clone());
    if last_event_key.as_ref() == Some(&current_key) {
        return Ok(());
    }
    *last_event_key = Some(current_key);
    let message = status
        .last_error
        .clone()
        .unwrap_or_else(|| format!("service became {}", status.health));
    let event = RuntimeEventRecord {
        observed_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        service: status.service,
        health: status.health,
        message,
    };
    let line =
        serde_json::to_string(&event).map_err(|err| io::Error::other(err.to_string()))? + "\n";
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("events.jsonl"))?
        .write_all(line.as_bytes())?;
    Ok(())
}

fn latency_bucket_index(micros: u64) -> usize {
    if micros <= 1 {
        return 0;
    }
    let index = 64_u32.saturating_sub((micros - 1).leading_zeros()) as usize;
    index.min(LATENCY_BUCKETS - 1)
}

fn percentile_from_buckets(buckets: &[u64; LATENCY_BUCKETS], samples: u64, percentile: f64) -> u64 {
    if samples == 0 {
        return 0;
    }
    let rank = ((samples as f64) * percentile).ceil().max(1.0) as u64;
    let mut seen = 0_u64;
    for (index, count) in buckets.iter().enumerate() {
        seen = seen.saturating_add(*count);
        if seen >= rank {
            return bucket_upper_bound(index);
        }
    }
    bucket_upper_bound(LATENCY_BUCKETS - 1)
}

fn bucket_upper_bound(index: usize) -> u64 {
    if index == 0 {
        1
    } else {
        1_u64 << index.min(62)
    }
}
