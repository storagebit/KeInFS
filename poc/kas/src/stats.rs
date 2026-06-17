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
pub(crate) struct KasIdentity {
    pub(crate) build: BuildInfo,
    pub(crate) listen_addr: String,
    pub(crate) allocator_store: String,
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
pub(crate) struct KasSnapshot {
    pub(crate) identity: KasIdentity,
    pub(crate) uptime_ms: u64,
    pub(crate) started_unix_s: u64,
    pub(crate) total_requests: u64,
    pub(crate) total_errors: u64,
    pub(crate) upsert_service_instance_requests: u64,
    pub(crate) list_service_instances_requests: u64,
    pub(crate) get_service_instance_requests: u64,
    pub(crate) register_target_requests: u64,
    pub(crate) heartbeat_requests: u64,
    pub(crate) list_targets_requests: u64,
    pub(crate) set_target_state_requests: u64,
    pub(crate) list_reservations_requests: u64,
    pub(crate) get_reservation_requests: u64,
    pub(crate) reserve_stripe_requests: u64,
    pub(crate) reserve_stripe_batch_requests: u64,
    pub(crate) finalize_requests: u64,
    pub(crate) release_requests: u64,
    pub(crate) reclaim_target_granules_requests: u64,
    pub(crate) reserve_rebuild_requests: u64,
    pub(crate) reserve_replacement_requests: u64,
    pub(crate) reservation_reaper_runs: u64,
    pub(crate) reservation_reaper_released: u64,
    pub(crate) rpcs: BTreeMap<String, RpcSnapshot>,
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
    reservation_reaper_runs: u64,
    reservation_reaper_released: u64,
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

pub(crate) struct KasStats {
    identity: KasIdentity,
    started: Instant,
    started_unix_s: u64,
    total_requests: AtomicU64,
    total_errors: AtomicU64,
    upsert_service_instance: RpcRuntimeStats,
    list_service_instances: RpcRuntimeStats,
    get_service_instance: RpcRuntimeStats,
    register_target: RpcRuntimeStats,
    heartbeat_target: RpcRuntimeStats,
    list_targets: RpcRuntimeStats,
    set_target_state: RpcRuntimeStats,
    list_reservations: RpcRuntimeStats,
    get_reservation: RpcRuntimeStats,
    reserve_stripe_placement: RpcRuntimeStats,
    reserve_stripe_batch: RpcRuntimeStats,
    finalize_reservations: RpcRuntimeStats,
    release_reservations: RpcRuntimeStats,
    reclaim_target_granules: RpcRuntimeStats,
    reserve_rebuild_placement: RpcRuntimeStats,
    reserve_replacement_placement: RpcRuntimeStats,
    reservation_reaper_runs: AtomicU64,
    reservation_reaper_released: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl KasStats {
    pub(crate) fn new(identity: KasIdentity) -> Arc<Self> {
        Arc::new(Self {
            identity,
            started: Instant::now(),
            started_unix_s: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            total_requests: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            upsert_service_instance: RpcRuntimeStats::new(),
            list_service_instances: RpcRuntimeStats::new(),
            get_service_instance: RpcRuntimeStats::new(),
            register_target: RpcRuntimeStats::new(),
            heartbeat_target: RpcRuntimeStats::new(),
            list_targets: RpcRuntimeStats::new(),
            set_target_state: RpcRuntimeStats::new(),
            list_reservations: RpcRuntimeStats::new(),
            get_reservation: RpcRuntimeStats::new(),
            reserve_stripe_placement: RpcRuntimeStats::new(),
            reserve_stripe_batch: RpcRuntimeStats::new(),
            finalize_reservations: RpcRuntimeStats::new(),
            release_reservations: RpcRuntimeStats::new(),
            reclaim_target_granules: RpcRuntimeStats::new(),
            reserve_rebuild_placement: RpcRuntimeStats::new(),
            reserve_replacement_placement: RpcRuntimeStats::new(),
            reservation_reaper_runs: AtomicU64::new(0),
            reservation_reaper_released: AtomicU64::new(0),
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

    pub(crate) fn snapshot(&self) -> KasSnapshot {
        let mut rpcs = BTreeMap::new();
        for kind in RpcKind::ALL {
            rpcs.insert(kind.name().to_string(), self.rpc(kind).snapshot());
        }
        KasSnapshot {
            identity: self.identity.clone(),
            uptime_ms: self.started.elapsed().as_millis() as u64,
            started_unix_s: self.started_unix_s,
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            upsert_service_instance_requests: self
                .upsert_service_instance
                .requests
                .load(Ordering::Relaxed),
            list_service_instances_requests: self
                .list_service_instances
                .requests
                .load(Ordering::Relaxed),
            get_service_instance_requests: self
                .get_service_instance
                .requests
                .load(Ordering::Relaxed),
            register_target_requests: self.register_target.requests.load(Ordering::Relaxed),
            heartbeat_requests: self.heartbeat_target.requests.load(Ordering::Relaxed),
            list_targets_requests: self.list_targets.requests.load(Ordering::Relaxed),
            set_target_state_requests: self.set_target_state.requests.load(Ordering::Relaxed),
            list_reservations_requests: self.list_reservations.requests.load(Ordering::Relaxed),
            get_reservation_requests: self.get_reservation.requests.load(Ordering::Relaxed),
            reserve_stripe_requests: self
                .reserve_stripe_placement
                .requests
                .load(Ordering::Relaxed),
            reserve_stripe_batch_requests: self
                .reserve_stripe_batch
                .requests
                .load(Ordering::Relaxed),
            finalize_requests: self.finalize_reservations.requests.load(Ordering::Relaxed),
            release_requests: self.release_reservations.requests.load(Ordering::Relaxed),
            reclaim_target_granules_requests: self
                .reclaim_target_granules
                .requests
                .load(Ordering::Relaxed),
            reserve_rebuild_requests: self
                .reserve_rebuild_placement
                .requests
                .load(Ordering::Relaxed),
            reserve_replacement_requests: self
                .reserve_replacement_placement
                .requests
                .load(Ordering::Relaxed),
            reservation_reaper_runs: self.reservation_reaper_runs.load(Ordering::Relaxed),
            reservation_reaper_released: self.reservation_reaper_released.load(Ordering::Relaxed),
            rpcs,
            last_error: self.last_error.lock().unwrap().clone(),
        }
    }

    fn rpc(&self, kind: RpcKind) -> &RpcRuntimeStats {
        match kind {
            RpcKind::UpsertServiceInstance => &self.upsert_service_instance,
            RpcKind::ListServiceInstances => &self.list_service_instances,
            RpcKind::GetServiceInstance => &self.get_service_instance,
            RpcKind::RegisterTarget => &self.register_target,
            RpcKind::HeartbeatTarget => &self.heartbeat_target,
            RpcKind::ListTargets => &self.list_targets,
            RpcKind::SetTargetState => &self.set_target_state,
            RpcKind::ListReservations => &self.list_reservations,
            RpcKind::GetReservation => &self.get_reservation,
            RpcKind::ReserveStripePlacement => &self.reserve_stripe_placement,
            RpcKind::ReserveStripeBatch => &self.reserve_stripe_batch,
            RpcKind::FinalizeReservations => &self.finalize_reservations,
            RpcKind::ReleaseReservations => &self.release_reservations,
            RpcKind::ReclaimTargetGranules => &self.reclaim_target_granules,
            RpcKind::ReserveRebuildPlacement => &self.reserve_rebuild_placement,
            RpcKind::ReserveReplacementPlacement => &self.reserve_replacement_placement,
        }
    }

    pub(crate) fn record_reservation_reaper_run(&self, released: usize) {
        self.reservation_reaper_runs.fetch_add(1, Ordering::Relaxed);
        self.reservation_reaper_released
            .fetch_add(released as u64, Ordering::Relaxed);
    }

    pub(crate) fn set_last_error(&self, message: impl Into<String>) {
        *self.last_error.lock().unwrap() = Some(message.into());
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RpcKind {
    UpsertServiceInstance,
    ListServiceInstances,
    GetServiceInstance,
    RegisterTarget,
    HeartbeatTarget,
    ListTargets,
    SetTargetState,
    ListReservations,
    GetReservation,
    ReserveStripePlacement,
    ReserveStripeBatch,
    FinalizeReservations,
    ReleaseReservations,
    ReclaimTargetGranules,
    ReserveRebuildPlacement,
    ReserveReplacementPlacement,
}

impl RpcKind {
    const ALL: [Self; 16] = [
        Self::UpsertServiceInstance,
        Self::ListServiceInstances,
        Self::GetServiceInstance,
        Self::RegisterTarget,
        Self::HeartbeatTarget,
        Self::ListTargets,
        Self::SetTargetState,
        Self::ListReservations,
        Self::GetReservation,
        Self::ReserveStripePlacement,
        Self::ReserveStripeBatch,
        Self::FinalizeReservations,
        Self::ReleaseReservations,
        Self::ReclaimTargetGranules,
        Self::ReserveRebuildPlacement,
        Self::ReserveReplacementPlacement,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::UpsertServiceInstance => "upsert_service_instance",
            Self::ListServiceInstances => "list_service_instances",
            Self::GetServiceInstance => "get_service_instance",
            Self::RegisterTarget => "register_target",
            Self::HeartbeatTarget => "heartbeat_target",
            Self::ListTargets => "list_targets",
            Self::SetTargetState => "set_target_state",
            Self::ListReservations => "list_reservations",
            Self::GetReservation => "get_reservation",
            Self::ReserveStripePlacement => "reserve_stripe_placement",
            Self::ReserveStripeBatch => "reserve_stripe_batch",
            Self::FinalizeReservations => "finalize_reservations",
            Self::ReleaseReservations => "release_reservations",
            Self::ReclaimTargetGranules => "reclaim_target_granules",
            Self::ReserveRebuildPlacement => "reserve_rebuild_placement",
            Self::ReserveReplacementPlacement => "reserve_replacement_placement",
        }
    }
}

pub(crate) struct Publisher {
    stop_tx: Option<mpsc::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl Publisher {
    pub(crate) fn spawn(
        stats: Arc<KasStats>,
        root: impl AsRef<Path>,
        publish_interval: Duration,
    ) -> io::Result<Self> {
        let runtime_dir = root.as_ref().join(format!("kas-{}", stats.identity.pid));
        fs::create_dir_all(&runtime_dir)?;
        let (stop_tx, stop_rx) = mpsc::channel();
        let thread_runtime_dir = runtime_dir.clone();
        let join = thread::Builder::new()
            .name("kas-stats".to_string())
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

fn write_snapshot(root: &Path, snapshot: &KasSnapshot) -> io::Result<()> {
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
    fn from_snapshot(snapshot: &KasSnapshot) -> Self {
        let health = if snapshot.last_error.is_some() {
            "degraded"
        } else {
            "healthy"
        };
        Self {
            service: "kas",
            health,
            ready: true,
            uptime_ms: snapshot.uptime_ms,
            started_unix_s: snapshot.started_unix_s,
            pid: snapshot.identity.pid,
            total_requests: snapshot.total_requests,
            total_errors: snapshot.total_errors,
            reservation_reaper_runs: snapshot.reservation_reaper_runs,
            reservation_reaper_released: snapshot.reservation_reaper_released,
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
