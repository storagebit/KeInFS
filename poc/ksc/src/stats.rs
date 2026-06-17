// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::client::RequestPhaseTimes;
use crate::config::BenchmarkConfig;
use serde::Serialize;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const LATENCY_BUCKETS: usize = 32;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct KscIdentity {
    pub(crate) client_id: String,
    pub(crate) pid: u32,
    pub(crate) endpoints: String,
    pub(crate) endpoint_count: usize,
    pub(crate) transfer_mode: String,
    pub(crate) chunk_seed: u64,
    pub(crate) slot_base: u64,
    pub(crate) generation_start: u32,
    pub(crate) packed_count: usize,
    pub(crate) pack_max_payload_bytes: usize,
    pub(crate) key_count: usize,
    pub(crate) workers: usize,
    pub(crate) target_initial_inflight: usize,
    pub(crate) target_min_inflight: usize,
    pub(crate) target_additive_increase_every: usize,
    pub(crate) avoid_overlapping_writes: bool,
    pub(crate) duration_ms: u64,
    pub(crate) write_percent: u8,
    pub(crate) cleanup: bool,
    pub(crate) stats_root: String,
    pub(crate) runtime_dir: String,
    pub(crate) network_mode: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct KscSummarySnapshot {
    pub(crate) pid: u32,
    pub(crate) started_unix_s: u64,
    pub(crate) uptime_ms: u128,
    pub(crate) phase: String,
    pub(crate) active_workers: u64,
    pub(crate) peak_active_workers: u64,
    pub(crate) active_connections: u64,
    pub(crate) peak_active_connections: u64,
    pub(crate) total_connections_opened: u64,
    pub(crate) total_connection_failures: u64,
    pub(crate) total_requests: u64,
    pub(crate) total_errors: u64,
    pub(crate) rate_limit_events: u64,
    pub(crate) rate_limit_read: u64,
    pub(crate) rate_limit_write: u64,
    pub(crate) rate_limit_other: u64,
    pub(crate) read_packs: u64,
    pub(crate) write_packs: u64,
    pub(crate) delete_requests: u64,
    pub(crate) read_chunks: u64,
    pub(crate) write_chunks: u64,
    pub(crate) delete_chunks: u64,
    pub(crate) read_payload_bytes: u64,
    pub(crate) write_payload_bytes: u64,
    pub(crate) last_error: Option<String>,
    pub(crate) read_latency: LatencySummary,
    pub(crate) write_latency: LatencySummary,
    pub(crate) delete_latency: LatencySummary,
    pub(crate) read_phases: RequestPhaseSummary,
    pub(crate) write_phases: RequestPhaseSummary,
    pub(crate) delete_phases: RequestPhaseSummary,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct KscSnapshot {
    pub(crate) identity: KscIdentity,
    pub(crate) summary: KscSummarySnapshot,
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
pub(crate) struct RequestPhaseSummary {
    pub(crate) ready_wait: LatencySummary,
    pub(crate) request_prepare: LatencySummary,
    pub(crate) send_headers: LatencySummary,
    pub(crate) send_body: LatencySummary,
    pub(crate) wait_response: LatencySummary,
    pub(crate) collect_response: LatencySummary,
    pub(crate) protocol_decode: LatencySummary,
    pub(crate) payload_validate: LatencySummary,
}

pub(crate) struct KscStatsPublisher {
    pub(crate) runtime_dir: PathBuf,
    pub(crate) stop_tx: Option<mpsc::Sender<()>>,
    pub(crate) join: Option<JoinHandle<()>>,
}

struct LatencyRuntimeStats {
    samples: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
    buckets: [AtomicU64; LATENCY_BUCKETS],
}

struct RequestPhaseRuntimeStats {
    ready_wait: LatencyRuntimeStats,
    request_prepare: LatencyRuntimeStats,
    send_headers: LatencyRuntimeStats,
    send_body: LatencyRuntimeStats,
    wait_response: LatencyRuntimeStats,
    collect_response: LatencyRuntimeStats,
    protocol_decode: LatencyRuntimeStats,
    payload_validate: LatencyRuntimeStats,
}

impl RequestPhaseRuntimeStats {
    fn new() -> Self {
        Self {
            ready_wait: LatencyRuntimeStats::new(),
            request_prepare: LatencyRuntimeStats::new(),
            send_headers: LatencyRuntimeStats::new(),
            send_body: LatencyRuntimeStats::new(),
            wait_response: LatencyRuntimeStats::new(),
            collect_response: LatencyRuntimeStats::new(),
            protocol_decode: LatencyRuntimeStats::new(),
            payload_validate: LatencyRuntimeStats::new(),
        }
    }

    fn observe(&self, phases: &RequestPhaseTimes) {
        self.ready_wait.observe(phases.ready_wait);
        self.request_prepare.observe(phases.request_prepare);
        self.send_headers.observe(phases.send_headers);
        self.send_body.observe(phases.send_body);
        self.wait_response.observe(phases.wait_response);
        self.collect_response.observe(phases.collect_response);
        self.protocol_decode.observe(phases.protocol_decode);
        self.payload_validate.observe(phases.payload_validate);
    }

    fn snapshot(&self) -> RequestPhaseSummary {
        RequestPhaseSummary {
            ready_wait: self.ready_wait.snapshot(),
            request_prepare: self.request_prepare.snapshot(),
            send_headers: self.send_headers.snapshot(),
            send_body: self.send_body.snapshot(),
            wait_response: self.wait_response.snapshot(),
            collect_response: self.collect_response.snapshot(),
            protocol_decode: self.protocol_decode.snapshot(),
            payload_validate: self.payload_validate.snapshot(),
        }
    }
}

impl LatencyRuntimeStats {
    fn new() -> Self {
        Self {
            samples: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn observe(&self, elapsed: Duration) {
        let micros = elapsed.as_micros().max(1) as u64;
        self.samples.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(micros, Ordering::Relaxed);
        update_atomic_max(&self.max_us, micros);
        self.buckets[latency_bucket_index(micros)].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LatencySummary {
        let samples = self.samples.load(Ordering::Relaxed);
        if samples == 0 {
            return LatencySummary {
                samples: 0,
                avg_us: 0,
                p50_us: 0,
                p95_us: 0,
                p99_us: 0,
                max_us: 0,
            };
        }
        let total_us = self.total_us.load(Ordering::Relaxed);
        LatencySummary {
            samples,
            avg_us: total_us / samples,
            p50_us: percentile_from_buckets(&self.buckets, samples, 0.50),
            p95_us: percentile_from_buckets(&self.buckets, samples, 0.95),
            p99_us: percentile_from_buckets(&self.buckets, samples, 0.99),
            max_us: self.max_us.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct KscRuntimeStats {
    identity: Mutex<KscIdentity>,
    started: Instant,
    started_unix_s: u64,
    phase: Mutex<String>,
    active_workers: AtomicU64,
    peak_active_workers: AtomicU64,
    active_connections: AtomicU64,
    peak_active_connections: AtomicU64,
    total_connections_opened: AtomicU64,
    total_connection_failures: AtomicU64,
    total_requests: AtomicU64,
    total_errors: AtomicU64,
    rate_limit_events: AtomicU64,
    rate_limit_read: AtomicU64,
    rate_limit_write: AtomicU64,
    rate_limit_other: AtomicU64,
    read_packs: AtomicU64,
    write_packs: AtomicU64,
    delete_requests: AtomicU64,
    read_chunks: AtomicU64,
    write_chunks: AtomicU64,
    delete_chunks: AtomicU64,
    read_payload_bytes: AtomicU64,
    write_payload_bytes: AtomicU64,
    last_error: Mutex<Option<String>>,
    read_latency: LatencyRuntimeStats,
    write_latency: LatencyRuntimeStats,
    delete_latency: LatencyRuntimeStats,
    read_phases: RequestPhaseRuntimeStats,
    write_phases: RequestPhaseRuntimeStats,
    delete_phases: RequestPhaseRuntimeStats,
}

impl KscRuntimeStats {
    pub(crate) fn new(config: &BenchmarkConfig) -> Self {
        let pid = std::process::id();
        let started_unix_s = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            identity: Mutex::new(KscIdentity {
                client_id: config.client_id.clone(),
                pid,
                endpoints: config.endpoints.join(","),
                endpoint_count: config.endpoints.len(),
                transfer_mode: match config.transfer_mode {
                    crate::config::TransferMode::Single => "single".to_string(),
                    crate::config::TransferMode::Packed => "packed".to_string(),
                },
                chunk_seed: config.chunk_seed,
                slot_base: config.slot_base,
                generation_start: config.generation_start,
                packed_count: config.packed_count,
                pack_max_payload_bytes: config.pack_max_payload_bytes,
                key_count: config.key_count,
                workers: config.workers,
                target_initial_inflight: config.target_initial_inflight,
                target_min_inflight: config.target_min_inflight,
                target_additive_increase_every: config.target_additive_increase_every,
                avoid_overlapping_writes: config.avoid_overlapping_writes,
                duration_ms: config.duration.as_millis() as u64,
                write_percent: config.write_percent,
                cleanup: config.cleanup,
                stats_root: config.stats_root.display().to_string(),
                runtime_dir: String::new(),
                network_mode: "interrupt-driven".to_string(),
            }),
            started: Instant::now(),
            started_unix_s,
            phase: Mutex::new("init".to_string()),
            active_workers: AtomicU64::new(0),
            peak_active_workers: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            peak_active_connections: AtomicU64::new(0),
            total_connections_opened: AtomicU64::new(0),
            total_connection_failures: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            rate_limit_events: AtomicU64::new(0),
            rate_limit_read: AtomicU64::new(0),
            rate_limit_write: AtomicU64::new(0),
            rate_limit_other: AtomicU64::new(0),
            read_packs: AtomicU64::new(0),
            write_packs: AtomicU64::new(0),
            delete_requests: AtomicU64::new(0),
            read_chunks: AtomicU64::new(0),
            write_chunks: AtomicU64::new(0),
            delete_chunks: AtomicU64::new(0),
            read_payload_bytes: AtomicU64::new(0),
            write_payload_bytes: AtomicU64::new(0),
            last_error: Mutex::new(None),
            read_latency: LatencyRuntimeStats::new(),
            write_latency: LatencyRuntimeStats::new(),
            delete_latency: LatencyRuntimeStats::new(),
            read_phases: RequestPhaseRuntimeStats::new(),
            write_phases: RequestPhaseRuntimeStats::new(),
            delete_phases: RequestPhaseRuntimeStats::new(),
        }
    }

    pub(crate) fn set_runtime_dir(&self, runtime_dir: &Path) {
        if let Ok(mut identity) = self.identity.lock() {
            identity.runtime_dir = runtime_dir.display().to_string();
        }
    }

    pub(crate) fn set_phase(&self, phase: &str) {
        if let Ok(mut current) = self.phase.lock() {
            *current = phase.to_string();
        }
    }

    pub(crate) fn begin_worker(&self) {
        let active = self.active_workers.fetch_add(1, Ordering::Relaxed) + 1;
        update_atomic_max(&self.peak_active_workers, active);
    }

    pub(crate) fn finish_worker(&self) {
        self.active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn record_connection_opened(&self) {
        self.total_connections_opened
            .fetch_add(1, Ordering::Relaxed);
        let active = self.active_connections.fetch_add(1, Ordering::Relaxed) + 1;
        update_atomic_max(&self.peak_active_connections, active);
    }

    pub(crate) fn record_connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn record_connection_failure(&self, message: impl Into<String>) {
        self.total_connection_failures
            .fetch_add(1, Ordering::Relaxed);
        self.record_error(message);
    }

    pub(crate) fn record_rate_limit(&self, class: Option<&str>) {
        self.rate_limit_events.fetch_add(1, Ordering::Relaxed);
        match class {
            Some("read") => {
                self.rate_limit_read.fetch_add(1, Ordering::Relaxed);
            }
            Some("write") => {
                self.rate_limit_write.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.rate_limit_other.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn record_read(
        &self,
        chunks: usize,
        payload_bytes: usize,
        latency: Duration,
        phases: &RequestPhaseTimes,
    ) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.read_packs.fetch_add(1, Ordering::Relaxed);
        self.read_chunks.fetch_add(chunks as u64, Ordering::Relaxed);
        self.read_payload_bytes
            .fetch_add(payload_bytes as u64, Ordering::Relaxed);
        self.read_latency.observe(latency);
        self.read_phases.observe(phases);
    }

    pub(crate) fn record_write(
        &self,
        chunks: usize,
        payload_bytes: usize,
        latency: Duration,
        phases: &RequestPhaseTimes,
    ) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.write_packs.fetch_add(1, Ordering::Relaxed);
        self.write_chunks
            .fetch_add(chunks as u64, Ordering::Relaxed);
        self.write_payload_bytes
            .fetch_add(payload_bytes as u64, Ordering::Relaxed);
        self.write_latency.observe(latency);
        self.write_phases.observe(phases);
    }

    pub(crate) fn record_delete(
        &self,
        chunks: usize,
        latency: Duration,
        phases: &RequestPhaseTimes,
    ) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.delete_requests.fetch_add(1, Ordering::Relaxed);
        self.delete_chunks
            .fetch_add(chunks as u64, Ordering::Relaxed);
        self.delete_latency.observe(latency);
        self.delete_phases.observe(phases);
    }

    pub(crate) fn record_error(&self, message: impl Into<String>) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        let message = message.into();
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(message);
        }
    }

    pub(crate) fn snapshot(&self) -> KscSnapshot {
        let phase = self
            .phase
            .lock()
            .map(|phase| phase.clone())
            .unwrap_or_default();
        let identity = self
            .identity
            .lock()
            .map(|identity| identity.clone())
            .unwrap_or_else(|_| KscIdentity {
                client_id: "ksc".to_string(),
                pid: std::process::id(),
                endpoints: String::new(),
                endpoint_count: 0,
                transfer_mode: "unknown".to_string(),
                chunk_seed: 0,
                slot_base: 0,
                generation_start: 0,
                packed_count: 0,
                pack_max_payload_bytes: 0,
                key_count: 0,
                workers: 0,
                target_initial_inflight: 0,
                target_min_inflight: 0,
                target_additive_increase_every: 0,
                avoid_overlapping_writes: false,
                duration_ms: 0,
                write_percent: 0,
                cleanup: false,
                stats_root: String::new(),
                runtime_dir: String::new(),
                network_mode: "interrupt-driven".to_string(),
            });
        let last_error = self
            .last_error
            .lock()
            .map(|err| err.clone())
            .unwrap_or(None);
        KscSnapshot {
            identity,
            summary: KscSummarySnapshot {
                pid: std::process::id(),
                started_unix_s: self.started_unix_s,
                uptime_ms: self.started.elapsed().as_millis(),
                phase,
                active_workers: self.active_workers.load(Ordering::Relaxed),
                peak_active_workers: self.peak_active_workers.load(Ordering::Relaxed),
                active_connections: self.active_connections.load(Ordering::Relaxed),
                peak_active_connections: self.peak_active_connections.load(Ordering::Relaxed),
                total_connections_opened: self.total_connections_opened.load(Ordering::Relaxed),
                total_connection_failures: self.total_connection_failures.load(Ordering::Relaxed),
                total_requests: self.total_requests.load(Ordering::Relaxed),
                total_errors: self.total_errors.load(Ordering::Relaxed),
                rate_limit_events: self.rate_limit_events.load(Ordering::Relaxed),
                rate_limit_read: self.rate_limit_read.load(Ordering::Relaxed),
                rate_limit_write: self.rate_limit_write.load(Ordering::Relaxed),
                rate_limit_other: self.rate_limit_other.load(Ordering::Relaxed),
                read_packs: self.read_packs.load(Ordering::Relaxed),
                write_packs: self.write_packs.load(Ordering::Relaxed),
                delete_requests: self.delete_requests.load(Ordering::Relaxed),
                read_chunks: self.read_chunks.load(Ordering::Relaxed),
                write_chunks: self.write_chunks.load(Ordering::Relaxed),
                delete_chunks: self.delete_chunks.load(Ordering::Relaxed),
                read_payload_bytes: self.read_payload_bytes.load(Ordering::Relaxed),
                write_payload_bytes: self.write_payload_bytes.load(Ordering::Relaxed),
                last_error,
                read_latency: self.read_latency.snapshot(),
                write_latency: self.write_latency.snapshot(),
                delete_latency: self.delete_latency.snapshot(),
                read_phases: self.read_phases.snapshot(),
                write_phases: self.write_phases.snapshot(),
                delete_phases: self.delete_phases.snapshot(),
            },
        }
    }
}

pub(crate) fn spawn_stats_publisher(
    stats: Arc<KscRuntimeStats>,
    publish_interval: Duration,
    root_dir: &Path,
) -> io::Result<KscStatsPublisher> {
    if publish_interval.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KSC stats publish interval must be > 0",
        ));
    }
    let identity = stats
        .identity
        .lock()
        .map(|identity| identity.clone())
        .unwrap_or_else(|_| KscIdentity {
            client_id: "ksc".to_string(),
            pid: std::process::id(),
            endpoints: String::new(),
            endpoint_count: 0,
            transfer_mode: "unknown".to_string(),
            chunk_seed: 0,
            slot_base: 0,
            generation_start: 0,
            packed_count: 0,
            pack_max_payload_bytes: 0,
            key_count: 0,
            workers: 0,
            target_initial_inflight: 0,
            target_min_inflight: 0,
            target_additive_increase_every: 0,
            avoid_overlapping_writes: false,
            duration_ms: 0,
            write_percent: 0,
            cleanup: false,
            stats_root: String::new(),
            runtime_dir: String::new(),
            network_mode: "interrupt-driven".to_string(),
        });
    let runtime_dir = root_dir.join(format!(
        "{}-{}",
        sanitize_component(&identity.client_id),
        identity.pid
    ));
    fs::create_dir_all(&runtime_dir)?;
    stats.set_runtime_dir(&runtime_dir);
    write_stats_tree(&stats.snapshot(), &runtime_dir)?;

    let (stop_tx, stop_rx) = mpsc::channel();
    let runtime_dir_thread = runtime_dir.clone();
    let stats_thread = Arc::clone(&stats);
    let join = thread::Builder::new()
        .name("ksc-stats-publisher".to_string())
        .spawn(move || {
            loop {
                match stop_rx.recv_timeout(publish_interval) {
                    Ok(()) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        let _ = write_stats_tree(&stats_thread.snapshot(), &runtime_dir_thread);
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            let _ = write_stats_tree(&stats_thread.snapshot(), &runtime_dir_thread);
        })?;

    Ok(KscStatsPublisher {
        runtime_dir,
        stop_tx: Some(stop_tx),
        join: Some(join),
    })
}

pub(crate) fn write_stats_tree(snapshot: &KscSnapshot, root: &Path) -> io::Result<()> {
    write_text(
        &root.join("summary"),
        format!(
            concat!(
                "client_id={}\n",
                "endpoints={}\n",
                "endpoint_count={}\n",
                "pid={}\n",
                "phase={}\n",
                "uptime_ms={}\n",
                "active_workers={}\n",
                "peak_active_workers={}\n",
                "active_connections={}\n",
                "peak_active_connections={}\n",
                "total_connections_opened={}\n",
                "total_connection_failures={}\n",
                "total_requests={}\n",
                "total_errors={}\n",
                "rate_limit_events={}\n",
                "rate_limit_read={}\n",
                "rate_limit_write={}\n",
                "rate_limit_other={}\n",
                "read_packs={}\n",
                "write_packs={}\n",
                "delete_requests={}\n",
                "read_chunks={}\n",
                "write_chunks={}\n",
                "delete_chunks={}\n",
                "read_payload_bytes={}\n",
                "write_payload_bytes={}\n",
                "last_error={}\n"
            ),
            snapshot.identity.client_id,
            snapshot.identity.endpoints,
            snapshot.identity.endpoint_count,
            snapshot.summary.pid,
            snapshot.summary.phase,
            snapshot.summary.uptime_ms,
            snapshot.summary.active_workers,
            snapshot.summary.peak_active_workers,
            snapshot.summary.active_connections,
            snapshot.summary.peak_active_connections,
            snapshot.summary.total_connections_opened,
            snapshot.summary.total_connection_failures,
            snapshot.summary.total_requests,
            snapshot.summary.total_errors,
            snapshot.summary.rate_limit_events,
            snapshot.summary.rate_limit_read,
            snapshot.summary.rate_limit_write,
            snapshot.summary.rate_limit_other,
            snapshot.summary.read_packs,
            snapshot.summary.write_packs,
            snapshot.summary.delete_requests,
            snapshot.summary.read_chunks,
            snapshot.summary.write_chunks,
            snapshot.summary.delete_chunks,
            snapshot.summary.read_payload_bytes,
            snapshot.summary.write_payload_bytes,
            snapshot.summary.last_error.clone().unwrap_or_default(),
        ),
    )?;
    write_text(
        &root.join("config"),
        format!(
            concat!(
                "client_id={}\n",
                "endpoints={}\n",
                "endpoint_count={}\n",
                "transfer_mode={}\n",
                "chunk_seed={}\n",
                "slot_base={}\n",
                "generation_start={}\n",
                "packed_count={}\n",
                "pack_max_payload_bytes={}\n",
                "key_count={}\n",
                "workers={}\n",
                "target_initial_inflight={}\n",
                "target_min_inflight={}\n",
                "target_additive_increase_every={}\n",
                "avoid_overlapping_writes={}\n",
                "duration_ms={}\n",
                "write_percent={}\n",
                "cleanup={}\n",
                "network_mode={}\n",
                "stats_root={}\n",
                "runtime_dir={}\n"
            ),
            snapshot.identity.client_id,
            snapshot.identity.endpoints,
            snapshot.identity.endpoint_count,
            snapshot.identity.transfer_mode,
            snapshot.identity.chunk_seed,
            snapshot.identity.slot_base,
            snapshot.identity.generation_start,
            snapshot.identity.packed_count,
            snapshot.identity.pack_max_payload_bytes,
            snapshot.identity.key_count,
            snapshot.identity.workers,
            snapshot.identity.target_initial_inflight,
            snapshot.identity.target_min_inflight,
            snapshot.identity.target_additive_increase_every,
            snapshot.identity.avoid_overlapping_writes,
            snapshot.identity.duration_ms,
            snapshot.identity.write_percent,
            snapshot.identity.cleanup,
            snapshot.identity.network_mode,
            snapshot.identity.stats_root,
            snapshot.identity.runtime_dir,
        ),
    )?;
    write_text(
        &root.join("target"),
        format!(
            concat!(
                "endpoints={}\n",
                "endpoint_count={}\n",
                "active_connections={}\n",
                "peak_active_connections={}\n",
                "total_connections_opened={}\n",
                "total_connection_failures={}\n",
                "rate_limit_events={}\n",
                "rate_limit_read={}\n",
                "rate_limit_write={}\n",
                "rate_limit_other={}\n"
            ),
            snapshot.identity.endpoints,
            snapshot.identity.endpoint_count,
            snapshot.summary.active_connections,
            snapshot.summary.peak_active_connections,
            snapshot.summary.total_connections_opened,
            snapshot.summary.total_connection_failures,
            snapshot.summary.rate_limit_events,
            snapshot.summary.rate_limit_read,
            snapshot.summary.rate_limit_write,
            snapshot.summary.rate_limit_other,
        ),
    )?;
    write_text(
        &root.join("latency"),
        format!(
            concat!(
                "read_samples={}\n",
                "read_avg_us={}\n",
                "read_p50_us={}\n",
                "read_p95_us={}\n",
                "read_p99_us={}\n",
                "read_max_us={}\n",
                "write_samples={}\n",
                "write_avg_us={}\n",
                "write_p50_us={}\n",
                "write_p95_us={}\n",
                "write_p99_us={}\n",
                "write_max_us={}\n",
                "delete_samples={}\n",
                "delete_avg_us={}\n",
                "delete_p50_us={}\n",
                "delete_p95_us={}\n",
                "delete_p99_us={}\n",
                "delete_max_us={}\n",
            ),
            snapshot.summary.read_latency.samples,
            snapshot.summary.read_latency.avg_us,
            snapshot.summary.read_latency.p50_us,
            snapshot.summary.read_latency.p95_us,
            snapshot.summary.read_latency.p99_us,
            snapshot.summary.read_latency.max_us,
            snapshot.summary.write_latency.samples,
            snapshot.summary.write_latency.avg_us,
            snapshot.summary.write_latency.p50_us,
            snapshot.summary.write_latency.p95_us,
            snapshot.summary.write_latency.p99_us,
            snapshot.summary.write_latency.max_us,
            snapshot.summary.delete_latency.samples,
            snapshot.summary.delete_latency.avg_us,
            snapshot.summary.delete_latency.p50_us,
            snapshot.summary.delete_latency.p95_us,
            snapshot.summary.delete_latency.p99_us,
            snapshot.summary.delete_latency.max_us,
        ),
    )?;
    write_phase_file(
        &root.join("phases").join("read"),
        &snapshot.summary.read_phases,
    )?;
    write_phase_file(
        &root.join("phases").join("write"),
        &snapshot.summary.write_phases,
    )?;
    write_phase_file(
        &root.join("phases").join("delete"),
        &snapshot.summary.delete_phases,
    )?;
    write_text(
        &root.join("errors"),
        format!(
            concat!("total_errors={}\n", "last_error={}\n"),
            snapshot.summary.total_errors,
            snapshot.summary.last_error.clone().unwrap_or_default(),
        ),
    )
}

fn write_phase_file(path: &Path, summary: &RequestPhaseSummary) -> io::Result<()> {
    write_text(
        path,
        format!(
            concat!(
                "ready_wait_samples={}\n",
                "ready_wait_avg_us={}\n",
                "request_prepare_samples={}\n",
                "request_prepare_avg_us={}\n",
                "send_headers_samples={}\n",
                "send_headers_avg_us={}\n",
                "send_body_samples={}\n",
                "send_body_avg_us={}\n",
                "wait_response_samples={}\n",
                "wait_response_avg_us={}\n",
                "collect_response_samples={}\n",
                "collect_response_avg_us={}\n",
                "protocol_decode_samples={}\n",
                "protocol_decode_avg_us={}\n",
                "payload_validate_samples={}\n",
                "payload_validate_avg_us={}\n"
            ),
            summary.ready_wait.samples,
            summary.ready_wait.avg_us,
            summary.request_prepare.samples,
            summary.request_prepare.avg_us,
            summary.send_headers.samples,
            summary.send_headers.avg_us,
            summary.send_body.samples,
            summary.send_body.avg_us,
            summary.wait_response.samples,
            summary.wait_response.avg_us,
            summary.collect_response.samples,
            summary.collect_response.avg_us,
            summary.protocol_decode.samples,
            summary.protocol_decode.avg_us,
            summary.payload_validate.samples,
            summary.payload_validate.avg_us,
        ),
    )
}

fn write_text(path: &Path, content: String) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)
}

fn sanitize_component(value: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            sanitized.push(ch);
        } else {
            sanitized.push('-');
        }
    }
    if sanitized.is_empty() {
        "ksc".to_string()
    } else {
        sanitized
    }
}

fn update_atomic_max(slot: &AtomicU64, value: u64) {
    let mut current = slot.load(Ordering::Relaxed);
    while value > current {
        match slot.compare_exchange(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn latency_bucket_index(micros: u64) -> usize {
    let bucket = 63_u32.saturating_sub(micros.leading_zeros()) as usize;
    bucket.min(LATENCY_BUCKETS - 1)
}

fn percentile_from_buckets(
    buckets: &[AtomicU64; LATENCY_BUCKETS],
    samples: u64,
    percentile: f64,
) -> u64 {
    if samples == 0 {
        return 0;
    }
    let target = (samples as f64 * percentile).ceil().max(1.0) as u64;
    let mut seen = 0_u64;
    for (index, bucket) in buckets.iter().enumerate() {
        seen += bucket.load(Ordering::Relaxed);
        if seen >= target {
            return 1_u64 << index.min(63);
        }
    }
    1_u64 << (LATENCY_BUCKETS - 1)
}
