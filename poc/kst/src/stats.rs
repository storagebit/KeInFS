// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use keinbuild::BuildInfo;
use kix::{KixHardwareAcceleration, KixStatsHandle};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const LATENCY_BUCKETS: usize = 32;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TargetIdentity {
    pub build: BuildInfo,
    pub target_id: String,
    pub listen_addr: String,
    pub listen_backlog: u32,
    pub pid: u32,
    pub drive_id: u16,
    pub raw_device: String,
    pub raw_offset_bytes: u64,
    pub raw_slice_bytes: u64,
    pub media_device: String,
    pub media_offset_bytes: u64,
    pub media_slice_bytes: u64,
    pub layout_kind: String,
    pub extent_bytes: u32,
    pub packed_bytes: u32,
    pub key_slots: u64,
    pub publication_lanes: u64,
    pub numa_node: Option<i32>,
    pub shard_count: usize,
    pub lookup_mode: String,
    pub commit_mode: String,
    pub drive_mode: String,
    pub lookup_pin_cores: Vec<usize>,
    pub commit_pin_cores: Vec<usize>,
    pub drive_pin_cores: Vec<usize>,
    pub lookup_queue_depth: usize,
    pub commit_queue_depth: usize,
    pub drive_queue_depth: usize,
    pub read_ingress_mode: String,
    pub write_ingress_mode: String,
    pub read_ingress_workers: usize,
    pub write_ingress_workers: usize,
    pub read_ingress_pin_cores: Vec<usize>,
    pub write_ingress_pin_cores: Vec<usize>,
    pub read_ingress_queue_depth: usize,
    pub write_ingress_queue_depth: usize,
    pub direct_read_mode: String,
    pub direct_write_mode: String,
    pub direct_read_workers: usize,
    pub direct_write_workers: usize,
    pub direct_read_pin_cores: Vec<usize>,
    pub direct_write_pin_cores: Vec<usize>,
    pub direct_read_queue_depth: usize,
    pub direct_write_queue_depth: usize,
    pub max_packed_payload_bytes: usize,
    pub max_packed_write_request_bytes: usize,
    pub max_request_body_bytes: usize,
    pub max_connections: usize,
    pub max_active_streams: usize,
    pub max_read_streams: usize,
    pub max_write_streams: usize,
    pub h2_initial_window_bytes: u32,
    pub h2_initial_connection_window_bytes: u32,
    pub h2_max_frame_bytes: u32,
    pub h2_max_header_list_bytes: u32,
    pub h2_max_concurrent_streams: u32,
    pub h2_max_send_buffer_bytes: usize,
    pub cpu_arch: String,
    pub crc32_backend: String,
    pub crc32_accelerated: bool,
    pub crc32_detail: String,
    pub rebuild_required: bool,
    pub target_stats_runtime_dir: String,
    pub kix_stats_runtime_dir: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct LatencySummary {
    pub samples: u64,
    pub avg_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RpcStatsSnapshot {
    pub requests: u64,
    pub errors: u64,
    pub payload_bytes: u64,
    pub latency: LatencySummary,
    pub phases: BTreeMap<String, LatencySummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RpcGroupSnapshot {
    pub target_info: RpcStatsSnapshot,
    pub head: RpcStatsSnapshot,
    pub read: RpcStatsSnapshot,
    pub write: RpcStatsSnapshot,
    pub delete: RpcStatsSnapshot,
    pub stats: RpcStatsSnapshot,
    pub other: RpcStatsSnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct EmbeddedKixSnapshot {
    pub runtime_dir: String,
    pub total_live_entries: u64,
    pub total_get_ops: u64,
    pub total_get_hits: u64,
    pub total_get_misses: u64,
    pub total_upsert_ops: u64,
    pub total_delete_ops: u64,
    pub total_checkpoint_ops: u64,
    pub total_write_errors: u64,
    pub rebuild_required_drives: Vec<u16>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TargetStatsSnapshot {
    pub pid: u32,
    pub started_unix_s: u64,
    pub uptime_ms: u64,
    pub inflight_requests: u64,
    pub peak_inflight_requests: u64,
    pub total_requests: u64,
    pub total_errors: u64,
    pub read_payload_bytes: u64,
    pub write_payload_bytes: u64,
    pub last_error: Option<String>,
    pub connections: ConnectionStatsSnapshot,
    pub streams: StreamStatsSnapshot,
    pub kp2: Kp2StatsSnapshot,
    pub rpcs: RpcGroupSnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TargetLiveSnapshot {
    pub identity: TargetIdentity,
    pub stats: TargetStatsSnapshot,
    pub embedded_kix: EmbeddedKixSnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RuntimeStatusSnapshot {
    service: &'static str,
    health: &'static str,
    ready: bool,
    target_id: String,
    pid: u32,
    uptime_ms: u64,
    started_unix_s: u64,
    total_requests: u64,
    total_errors: u64,
    rebuild_required: bool,
    inflight_requests: u64,
    active_connections: u64,
    active_streams: u64,
    last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RuntimeEventRecord {
    observed_unix_ms: u64,
    service: &'static str,
    health: &'static str,
    message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ConnectionStatsSnapshot {
    pub active_connections: u64,
    pub peak_active_connections: u64,
    pub total_connections_accepted: u64,
    pub total_connections_completed: u64,
    pub total_connections_rejected: u64,
    pub total_accept_errors: u64,
    pub total_handshake_successes: u64,
    pub total_handshake_failures: u64,
    pub total_stream_rejections: u64,
    pub handshake_latency: LatencySummary,
    pub connection_lifetime: LatencySummary,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StreamStatsSnapshot {
    pub active_streams: u64,
    pub peak_active_streams: u64,
    pub active_read_streams: u64,
    pub peak_active_read_streams: u64,
    pub active_write_streams: u64,
    pub peak_active_write_streams: u64,
    pub total_stream_rejections: u64,
    pub read_stream_rejections: u64,
    pub write_stream_rejections: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Kp2StatsSnapshot {
    pub packed_write_requests: u64,
    pub packed_write_chunks: u64,
    pub packed_write_logical_payload_bytes: u64,
    pub packed_read_requests: u64,
    pub packed_read_chunks: u64,
    pub packed_read_logical_payload_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RpcKind {
    TargetInfo,
    Head,
    Read,
    Write,
    Delete,
    Stats,
    Other,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RequestPhase {
    BodyCollect,
    BodyStreamReceive,
    PublicationRetry,
    IngressQueueWait,
    ExecutionQueueWait,
    RouteExecute,
    RequestDecode,
    KixLookup,
    MediaHeaderValidate,
    MediaPayloadRead,
    MediaPayloadCopy,
    MediaCrc,
    MediaWritePrepare,
    MediaWriteIo,
    MediaFsync,
    KixPublish,
    LocationMap,
    ResponseEncode,
    ResponseSendHeaders,
    ResponseSendBody,
    ResponseSend,
}

impl RequestPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::BodyCollect => "body_collect",
            Self::BodyStreamReceive => "body_stream_receive",
            Self::PublicationRetry => "publication_retry",
            Self::IngressQueueWait => "ingress_queue_wait",
            Self::ExecutionQueueWait => "execution_queue_wait",
            Self::RouteExecute => "route_execute",
            Self::RequestDecode => "request_decode",
            Self::KixLookup => "kix_lookup",
            Self::MediaHeaderValidate => "media_header_validate",
            Self::MediaPayloadRead => "media_payload_read",
            Self::MediaPayloadCopy => "media_payload_copy",
            Self::MediaCrc => "media_crc",
            Self::MediaWritePrepare => "media_write_prepare",
            Self::MediaWriteIo => "media_write_io",
            Self::MediaFsync => "media_fsync",
            Self::KixPublish => "kix_publish",
            Self::LocationMap => "location_map",
            Self::ResponseEncode => "response_encode",
            Self::ResponseSendHeaders => "response_send_headers",
            Self::ResponseSendBody => "response_send_body",
            Self::ResponseSend => "response_send",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum StreamClass {
    Read,
    Write,
}

#[derive(Clone, Debug)]
pub(crate) struct TargetStatsConfig {
    pub root_dir: PathBuf,
    pub publish_interval: Duration,
}

pub(crate) struct TargetStatsPublisher {
    pub runtime_dir: PathBuf,
    pub stop_tx: Option<mpsc::Sender<()>>,
    pub join: Option<JoinHandle<()>>,
}

struct LatencyRuntimeStats {
    samples: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
    buckets: [AtomicU64; LATENCY_BUCKETS],
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
        let max_us = self.max_us.load(Ordering::Relaxed);
        LatencySummary {
            samples,
            avg_us: total_us / samples,
            p50_us: percentile_from_buckets(&self.buckets, samples, 0.50),
            p95_us: percentile_from_buckets(&self.buckets, samples, 0.95),
            p99_us: percentile_from_buckets(&self.buckets, samples, 0.99),
            max_us,
        }
    }
}

struct RpcRuntimeStats {
    requests: AtomicU64,
    errors: AtomicU64,
    payload_bytes: AtomicU64,
    latency: LatencyRuntimeStats,
    phases: RpcPhaseRuntimeStats,
}

struct RpcPhaseRuntimeStats {
    body_collect: LatencyRuntimeStats,
    body_stream_receive: LatencyRuntimeStats,
    publication_retry: LatencyRuntimeStats,
    ingress_queue_wait: LatencyRuntimeStats,
    execution_queue_wait: LatencyRuntimeStats,
    route_execute: LatencyRuntimeStats,
    request_decode: LatencyRuntimeStats,
    kix_lookup: LatencyRuntimeStats,
    media_header_validate: LatencyRuntimeStats,
    media_payload_read: LatencyRuntimeStats,
    media_payload_copy: LatencyRuntimeStats,
    media_crc: LatencyRuntimeStats,
    media_write_prepare: LatencyRuntimeStats,
    media_write_io: LatencyRuntimeStats,
    media_fsync: LatencyRuntimeStats,
    kix_publish: LatencyRuntimeStats,
    location_map: LatencyRuntimeStats,
    response_encode: LatencyRuntimeStats,
    response_send_headers: LatencyRuntimeStats,
    response_send_body: LatencyRuntimeStats,
    response_send: LatencyRuntimeStats,
}

impl RpcRuntimeStats {
    fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            payload_bytes: AtomicU64::new(0),
            latency: LatencyRuntimeStats::new(),
            phases: RpcPhaseRuntimeStats::new(),
        }
    }

    fn on_start(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    fn on_finish(&self, elapsed: Duration, payload_bytes: u64, errored: bool) {
        self.latency.observe(elapsed);
        if payload_bytes != 0 {
            self.payload_bytes
                .fetch_add(payload_bytes, Ordering::Relaxed);
        }
        if errored {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> RpcStatsSnapshot {
        RpcStatsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            payload_bytes: self.payload_bytes.load(Ordering::Relaxed),
            latency: self.latency.snapshot(),
            phases: self.phases.snapshot(),
        }
    }

    fn record_phase(&self, phase: RequestPhase, elapsed: Duration) {
        self.phases.record(phase, elapsed);
    }
}

impl RpcPhaseRuntimeStats {
    fn new() -> Self {
        Self {
            body_collect: LatencyRuntimeStats::new(),
            body_stream_receive: LatencyRuntimeStats::new(),
            publication_retry: LatencyRuntimeStats::new(),
            ingress_queue_wait: LatencyRuntimeStats::new(),
            execution_queue_wait: LatencyRuntimeStats::new(),
            route_execute: LatencyRuntimeStats::new(),
            request_decode: LatencyRuntimeStats::new(),
            kix_lookup: LatencyRuntimeStats::new(),
            media_header_validate: LatencyRuntimeStats::new(),
            media_payload_read: LatencyRuntimeStats::new(),
            media_payload_copy: LatencyRuntimeStats::new(),
            media_crc: LatencyRuntimeStats::new(),
            media_write_prepare: LatencyRuntimeStats::new(),
            media_write_io: LatencyRuntimeStats::new(),
            media_fsync: LatencyRuntimeStats::new(),
            kix_publish: LatencyRuntimeStats::new(),
            location_map: LatencyRuntimeStats::new(),
            response_encode: LatencyRuntimeStats::new(),
            response_send_headers: LatencyRuntimeStats::new(),
            response_send_body: LatencyRuntimeStats::new(),
            response_send: LatencyRuntimeStats::new(),
        }
    }

    fn record(&self, phase: RequestPhase, elapsed: Duration) {
        match phase {
            RequestPhase::BodyCollect => self.body_collect.observe(elapsed),
            RequestPhase::BodyStreamReceive => self.body_stream_receive.observe(elapsed),
            RequestPhase::PublicationRetry => self.publication_retry.observe(elapsed),
            RequestPhase::IngressQueueWait => self.ingress_queue_wait.observe(elapsed),
            RequestPhase::ExecutionQueueWait => self.execution_queue_wait.observe(elapsed),
            RequestPhase::RouteExecute => self.route_execute.observe(elapsed),
            RequestPhase::RequestDecode => self.request_decode.observe(elapsed),
            RequestPhase::KixLookup => self.kix_lookup.observe(elapsed),
            RequestPhase::MediaHeaderValidate => self.media_header_validate.observe(elapsed),
            RequestPhase::MediaPayloadRead => self.media_payload_read.observe(elapsed),
            RequestPhase::MediaPayloadCopy => self.media_payload_copy.observe(elapsed),
            RequestPhase::MediaCrc => self.media_crc.observe(elapsed),
            RequestPhase::MediaWritePrepare => self.media_write_prepare.observe(elapsed),
            RequestPhase::MediaWriteIo => self.media_write_io.observe(elapsed),
            RequestPhase::MediaFsync => self.media_fsync.observe(elapsed),
            RequestPhase::KixPublish => self.kix_publish.observe(elapsed),
            RequestPhase::LocationMap => self.location_map.observe(elapsed),
            RequestPhase::ResponseEncode => self.response_encode.observe(elapsed),
            RequestPhase::ResponseSendHeaders => self.response_send_headers.observe(elapsed),
            RequestPhase::ResponseSendBody => self.response_send_body.observe(elapsed),
            RequestPhase::ResponseSend => self.response_send.observe(elapsed),
        }
    }

    fn snapshot(&self) -> BTreeMap<String, LatencySummary> {
        let mut phases = BTreeMap::new();
        for (name, summary) in [
            (
                RequestPhase::BodyCollect.as_str(),
                self.body_collect.snapshot(),
            ),
            (
                RequestPhase::BodyStreamReceive.as_str(),
                self.body_stream_receive.snapshot(),
            ),
            (
                RequestPhase::PublicationRetry.as_str(),
                self.publication_retry.snapshot(),
            ),
            (
                RequestPhase::IngressQueueWait.as_str(),
                self.ingress_queue_wait.snapshot(),
            ),
            (
                RequestPhase::ExecutionQueueWait.as_str(),
                self.execution_queue_wait.snapshot(),
            ),
            (
                RequestPhase::RouteExecute.as_str(),
                self.route_execute.snapshot(),
            ),
            (
                RequestPhase::RequestDecode.as_str(),
                self.request_decode.snapshot(),
            ),
            (RequestPhase::KixLookup.as_str(), self.kix_lookup.snapshot()),
            (
                RequestPhase::MediaHeaderValidate.as_str(),
                self.media_header_validate.snapshot(),
            ),
            (
                RequestPhase::MediaPayloadRead.as_str(),
                self.media_payload_read.snapshot(),
            ),
            (
                RequestPhase::MediaPayloadCopy.as_str(),
                self.media_payload_copy.snapshot(),
            ),
            (RequestPhase::MediaCrc.as_str(), self.media_crc.snapshot()),
            (
                RequestPhase::MediaWritePrepare.as_str(),
                self.media_write_prepare.snapshot(),
            ),
            (
                RequestPhase::MediaWriteIo.as_str(),
                self.media_write_io.snapshot(),
            ),
            (
                RequestPhase::MediaFsync.as_str(),
                self.media_fsync.snapshot(),
            ),
            (
                RequestPhase::KixPublish.as_str(),
                self.kix_publish.snapshot(),
            ),
            (
                RequestPhase::LocationMap.as_str(),
                self.location_map.snapshot(),
            ),
            (
                RequestPhase::ResponseEncode.as_str(),
                self.response_encode.snapshot(),
            ),
            (
                RequestPhase::ResponseSendHeaders.as_str(),
                self.response_send_headers.snapshot(),
            ),
            (
                RequestPhase::ResponseSendBody.as_str(),
                self.response_send_body.snapshot(),
            ),
            (
                RequestPhase::ResponseSend.as_str(),
                self.response_send.snapshot(),
            ),
        ] {
            phases.insert(name.to_string(), summary);
        }
        phases
    }
}

pub(crate) struct TargetRuntimeStats {
    identity: Mutex<TargetIdentity>,
    started_at: Instant,
    started_unix_s: u64,
    total_requests: AtomicU64,
    inflight_requests: AtomicU64,
    peak_inflight_requests: AtomicU64,
    total_errors: AtomicU64,
    active_connections: AtomicU64,
    peak_active_connections: AtomicU64,
    total_connections_accepted: AtomicU64,
    total_connections_completed: AtomicU64,
    total_connections_rejected: AtomicU64,
    total_accept_errors: AtomicU64,
    total_handshake_successes: AtomicU64,
    total_handshake_failures: AtomicU64,
    total_stream_rejections: AtomicU64,
    active_read_streams: AtomicU64,
    peak_active_read_streams: AtomicU64,
    active_write_streams: AtomicU64,
    peak_active_write_streams: AtomicU64,
    read_stream_rejections: AtomicU64,
    write_stream_rejections: AtomicU64,
    packed_write_requests: AtomicU64,
    packed_write_chunks: AtomicU64,
    packed_write_logical_payload_bytes: AtomicU64,
    packed_read_requests: AtomicU64,
    packed_read_chunks: AtomicU64,
    packed_read_logical_payload_bytes: AtomicU64,
    handshake_latency: LatencyRuntimeStats,
    connection_lifetime: LatencyRuntimeStats,
    last_error: Mutex<Option<String>>,
    target_info: RpcRuntimeStats,
    head: RpcRuntimeStats,
    read: RpcRuntimeStats,
    write: RpcRuntimeStats,
    delete: RpcRuntimeStats,
    stats: RpcRuntimeStats,
    other: RpcRuntimeStats,
    kix_stats: KixStatsHandle,
}

impl TargetRuntimeStats {
    pub(crate) fn new(
        mut identity: TargetIdentity,
        hardware: KixHardwareAcceleration,
        kix_stats: KixStatsHandle,
    ) -> Arc<Self> {
        identity.cpu_arch = hardware.cpu_arch.to_string();
        identity.crc32_backend = hardware.crc32_backend.as_str().to_string();
        identity.crc32_accelerated = hardware.crc32_accelerated();
        identity.crc32_detail = hardware.crc32_detail().to_string();
        Arc::new(Self {
            identity: Mutex::new(identity),
            started_at: Instant::now(),
            started_unix_s: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            total_requests: AtomicU64::new(0),
            inflight_requests: AtomicU64::new(0),
            peak_inflight_requests: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            peak_active_connections: AtomicU64::new(0),
            total_connections_accepted: AtomicU64::new(0),
            total_connections_completed: AtomicU64::new(0),
            total_connections_rejected: AtomicU64::new(0),
            total_accept_errors: AtomicU64::new(0),
            total_handshake_successes: AtomicU64::new(0),
            total_handshake_failures: AtomicU64::new(0),
            total_stream_rejections: AtomicU64::new(0),
            active_read_streams: AtomicU64::new(0),
            peak_active_read_streams: AtomicU64::new(0),
            active_write_streams: AtomicU64::new(0),
            peak_active_write_streams: AtomicU64::new(0),
            read_stream_rejections: AtomicU64::new(0),
            write_stream_rejections: AtomicU64::new(0),
            packed_write_requests: AtomicU64::new(0),
            packed_write_chunks: AtomicU64::new(0),
            packed_write_logical_payload_bytes: AtomicU64::new(0),
            packed_read_requests: AtomicU64::new(0),
            packed_read_chunks: AtomicU64::new(0),
            packed_read_logical_payload_bytes: AtomicU64::new(0),
            handshake_latency: LatencyRuntimeStats::new(),
            connection_lifetime: LatencyRuntimeStats::new(),
            last_error: Mutex::new(None),
            target_info: RpcRuntimeStats::new(),
            head: RpcRuntimeStats::new(),
            read: RpcRuntimeStats::new(),
            write: RpcRuntimeStats::new(),
            delete: RpcRuntimeStats::new(),
            stats: RpcRuntimeStats::new(),
            other: RpcRuntimeStats::new(),
            kix_stats,
        })
    }

    pub(crate) fn set_runtime_dir(&self, runtime_dir: &Path) {
        if let Ok(mut identity) = self.identity.lock() {
            identity.target_stats_runtime_dir = runtime_dir.display().to_string();
        }
    }

    pub(crate) fn begin(&self, rpc: RpcKind) -> Instant {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        let inflight = self.inflight_requests.fetch_add(1, Ordering::Relaxed) + 1;
        update_atomic_max(&self.peak_inflight_requests, inflight);
        let stream_slot = match stream_class_for_rpc(rpc) {
            StreamClass::Read => (&self.active_read_streams, &self.peak_active_read_streams),
            StreamClass::Write => (&self.active_write_streams, &self.peak_active_write_streams),
        };
        let class_inflight = stream_slot.0.fetch_add(1, Ordering::Relaxed) + 1;
        update_atomic_max(stream_slot.1, class_inflight);
        self.rpc_stats(rpc).on_start();
        Instant::now()
    }

    pub(crate) fn finish(
        &self,
        rpc: RpcKind,
        started: Instant,
        payload_bytes: u64,
        error: Option<String>,
    ) {
        let errored = error.is_some();
        self.rpc_stats(rpc)
            .on_finish(started.elapsed(), payload_bytes, errored);
        self.inflight_requests.fetch_sub(1, Ordering::Relaxed);
        match stream_class_for_rpc(rpc) {
            StreamClass::Read => {
                self.active_read_streams.fetch_sub(1, Ordering::Relaxed);
            }
            StreamClass::Write => {
                self.active_write_streams.fetch_sub(1, Ordering::Relaxed);
            }
        }
        if let Some(message) = error {
            self.total_errors.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut last_error) = self.last_error.lock() {
                *last_error = Some(message);
            }
        }
    }

    pub(crate) fn record_phase(&self, rpc: RpcKind, phase: RequestPhase, elapsed: Duration) {
        self.rpc_stats(rpc).record_phase(phase, elapsed);
    }

    pub(crate) fn record_background_error(&self, message: impl Into<String>) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(message.into());
        }
    }

    pub(crate) fn begin_connection(&self) -> Instant {
        self.total_connections_accepted
            .fetch_add(1, Ordering::Relaxed);
        let active = self.active_connections.fetch_add(1, Ordering::Relaxed) + 1;
        update_atomic_max(&self.peak_active_connections, active);
        Instant::now()
    }

    pub(crate) fn finish_connection(&self, started: Instant) {
        self.connection_lifetime.observe(started.elapsed());
        self.total_connections_completed
            .fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn record_connection_rejected(&self) {
        self.total_connections_rejected
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_accept_error(&self, message: impl Into<String>) {
        self.total_accept_errors.fetch_add(1, Ordering::Relaxed);
        self.record_background_error(message);
    }

    pub(crate) fn record_handshake_success(&self, started: Instant) {
        self.handshake_latency.observe(started.elapsed());
        self.total_handshake_successes
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_handshake_failure(&self, started: Instant, message: impl Into<String>) {
        self.handshake_latency.observe(started.elapsed());
        self.total_handshake_failures
            .fetch_add(1, Ordering::Relaxed);
        self.record_background_error(message);
    }

    pub(crate) fn record_stream_rejection(&self, rpc: RpcKind) {
        self.total_stream_rejections.fetch_add(1, Ordering::Relaxed);
        match stream_class_for_rpc(rpc) {
            StreamClass::Read => {
                self.read_stream_rejections.fetch_add(1, Ordering::Relaxed);
            }
            StreamClass::Write => {
                self.write_stream_rejections.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn record_kp2_write(&self, chunks: usize, logical_payload_bytes: usize) {
        self.packed_write_requests.fetch_add(1, Ordering::Relaxed);
        self.packed_write_chunks
            .fetch_add(chunks as u64, Ordering::Relaxed);
        self.packed_write_logical_payload_bytes
            .fetch_add(logical_payload_bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_kp2_read(&self, chunks: usize, logical_payload_bytes: usize) {
        self.packed_read_requests.fetch_add(1, Ordering::Relaxed);
        self.packed_read_chunks
            .fetch_add(chunks as u64, Ordering::Relaxed);
        self.packed_read_logical_payload_bytes
            .fetch_add(logical_payload_bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> TargetLiveSnapshot {
        let identity = self
            .identity
            .lock()
            .map(|identity| identity.clone())
            .unwrap_or_else(|_| TargetIdentity {
                build: BuildInfo {
                    package_name: "keinfs-kst".to_string(),
                    binary_name: "kst".to_string(),
                    version: "unknown".to_string(),
                    release: 0,
                    git_sha: "unknown".to_string(),
                    git_dirty: false,
                    built_at_unix_s: 0,
                    build_profile: "unknown".to_string(),
                    target_triple: "unknown".to_string(),
                },
                target_id: "unknown".to_string(),
                listen_addr: "unknown".to_string(),
                listen_backlog: 0,
                pid: std::process::id(),
                drive_id: 0,
                raw_device: "unknown".to_string(),
                raw_offset_bytes: 0,
                raw_slice_bytes: 0,
                media_device: "unknown".to_string(),
                media_offset_bytes: 0,
                media_slice_bytes: 0,
                layout_kind: "unknown".to_string(),
                extent_bytes: 0,
                packed_bytes: 0,
                key_slots: 0,
                publication_lanes: 0,
                numa_node: None,
                shard_count: 0,
                lookup_mode: "unknown".to_string(),
                commit_mode: "unknown".to_string(),
                drive_mode: "unknown".to_string(),
                lookup_pin_cores: Vec::new(),
                commit_pin_cores: Vec::new(),
                drive_pin_cores: Vec::new(),
                lookup_queue_depth: 0,
                commit_queue_depth: 0,
                drive_queue_depth: 0,
                read_ingress_mode: "unknown".to_string(),
                write_ingress_mode: "unknown".to_string(),
                read_ingress_workers: 0,
                write_ingress_workers: 0,
                read_ingress_pin_cores: Vec::new(),
                write_ingress_pin_cores: Vec::new(),
                read_ingress_queue_depth: 0,
                write_ingress_queue_depth: 0,
                direct_read_mode: "unknown".to_string(),
                direct_write_mode: "unknown".to_string(),
                direct_read_workers: 0,
                direct_write_workers: 0,
                direct_read_pin_cores: Vec::new(),
                direct_write_pin_cores: Vec::new(),
                direct_read_queue_depth: 0,
                direct_write_queue_depth: 0,
                max_packed_payload_bytes: 0,
                max_packed_write_request_bytes: 0,
                max_request_body_bytes: 0,
                max_connections: 0,
                max_active_streams: 0,
                max_read_streams: 0,
                max_write_streams: 0,
                h2_initial_window_bytes: 0,
                h2_initial_connection_window_bytes: 0,
                h2_max_frame_bytes: 0,
                h2_max_header_list_bytes: 0,
                h2_max_concurrent_streams: 0,
                h2_max_send_buffer_bytes: 0,
                cpu_arch: "unknown".to_string(),
                crc32_backend: "unknown".to_string(),
                crc32_accelerated: false,
                crc32_detail: "unknown".to_string(),
                rebuild_required: false,
                target_stats_runtime_dir: String::new(),
                kix_stats_runtime_dir: String::new(),
            });
        let kix = self.kix_stats.snapshot();
        TargetLiveSnapshot {
            identity,
            stats: TargetStatsSnapshot {
                pid: std::process::id(),
                started_unix_s: self.started_unix_s,
                uptime_ms: self.started_at.elapsed().as_millis() as u64,
                inflight_requests: self.inflight_requests.load(Ordering::Relaxed),
                peak_inflight_requests: self.peak_inflight_requests.load(Ordering::Relaxed),
                total_requests: self.total_requests.load(Ordering::Relaxed),
                total_errors: self.total_errors.load(Ordering::Relaxed),
                read_payload_bytes: self.read.payload_bytes.load(Ordering::Relaxed),
                write_payload_bytes: self.write.payload_bytes.load(Ordering::Relaxed),
                last_error: self.last_error.lock().ok().and_then(|slot| slot.clone()),
                connections: ConnectionStatsSnapshot {
                    active_connections: self.active_connections.load(Ordering::Relaxed),
                    peak_active_connections: self.peak_active_connections.load(Ordering::Relaxed),
                    total_connections_accepted: self
                        .total_connections_accepted
                        .load(Ordering::Relaxed),
                    total_connections_completed: self
                        .total_connections_completed
                        .load(Ordering::Relaxed),
                    total_connections_rejected: self
                        .total_connections_rejected
                        .load(Ordering::Relaxed),
                    total_accept_errors: self.total_accept_errors.load(Ordering::Relaxed),
                    total_handshake_successes: self
                        .total_handshake_successes
                        .load(Ordering::Relaxed),
                    total_handshake_failures: self.total_handshake_failures.load(Ordering::Relaxed),
                    total_stream_rejections: self.total_stream_rejections.load(Ordering::Relaxed),
                    handshake_latency: self.handshake_latency.snapshot(),
                    connection_lifetime: self.connection_lifetime.snapshot(),
                },
                streams: StreamStatsSnapshot {
                    active_streams: self.inflight_requests.load(Ordering::Relaxed),
                    peak_active_streams: self.peak_inflight_requests.load(Ordering::Relaxed),
                    active_read_streams: self.active_read_streams.load(Ordering::Relaxed),
                    peak_active_read_streams: self.peak_active_read_streams.load(Ordering::Relaxed),
                    active_write_streams: self.active_write_streams.load(Ordering::Relaxed),
                    peak_active_write_streams: self
                        .peak_active_write_streams
                        .load(Ordering::Relaxed),
                    total_stream_rejections: self.total_stream_rejections.load(Ordering::Relaxed),
                    read_stream_rejections: self.read_stream_rejections.load(Ordering::Relaxed),
                    write_stream_rejections: self.write_stream_rejections.load(Ordering::Relaxed),
                },
                kp2: Kp2StatsSnapshot {
                    packed_write_requests: self.packed_write_requests.load(Ordering::Relaxed),
                    packed_write_chunks: self.packed_write_chunks.load(Ordering::Relaxed),
                    packed_write_logical_payload_bytes: self
                        .packed_write_logical_payload_bytes
                        .load(Ordering::Relaxed),
                    packed_read_requests: self.packed_read_requests.load(Ordering::Relaxed),
                    packed_read_chunks: self.packed_read_chunks.load(Ordering::Relaxed),
                    packed_read_logical_payload_bytes: self
                        .packed_read_logical_payload_bytes
                        .load(Ordering::Relaxed),
                },
                rpcs: RpcGroupSnapshot {
                    target_info: self.target_info.snapshot(),
                    head: self.head.snapshot(),
                    read: self.read.snapshot(),
                    write: self.write.snapshot(),
                    delete: self.delete.snapshot(),
                    stats: self.stats.snapshot(),
                    other: self.other.snapshot(),
                },
            },
            embedded_kix: EmbeddedKixSnapshot {
                runtime_dir: self
                    .identity
                    .lock()
                    .ok()
                    .map(|identity| identity.kix_stats_runtime_dir.clone())
                    .unwrap_or_default(),
                total_live_entries: kix.total_live_entries,
                total_get_ops: kix.total_get_ops,
                total_get_hits: kix.total_get_hits,
                total_get_misses: kix.total_get_misses,
                total_upsert_ops: kix.total_upsert_ops,
                total_delete_ops: kix.total_delete_ops,
                total_checkpoint_ops: kix.total_checkpoint_ops,
                total_write_errors: kix.total_write_errors,
                rebuild_required_drives: kix.rebuild_required_drives,
            },
        }
    }

    fn rpc_stats(&self, rpc: RpcKind) -> &RpcRuntimeStats {
        match rpc {
            RpcKind::TargetInfo => &self.target_info,
            RpcKind::Head => &self.head,
            RpcKind::Read => &self.read,
            RpcKind::Write => &self.write,
            RpcKind::Delete => &self.delete,
            RpcKind::Stats => &self.stats,
            RpcKind::Other => &self.other,
        }
    }
}

pub(crate) fn spawn_stats_publisher(
    stats: Arc<TargetRuntimeStats>,
    config: &TargetStatsConfig,
) -> io::Result<TargetStatsPublisher> {
    if config.root_dir.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KST stats root directory must not be empty",
        ));
    }
    if config.publish_interval.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "KST stats publish interval must be > 0",
        ));
    }

    let pid = std::process::id();
    let target_id = stats
        .identity
        .lock()
        .map(|identity| sanitize_component(&identity.target_id))
        .unwrap_or_else(|_| "kst".to_string());
    let runtime_dir = config.root_dir.join(format!("{target_id}-{pid}"));
    fs::create_dir_all(&runtime_dir)?;
    stats.set_runtime_dir(&runtime_dir);
    write_stats_tree(&stats.snapshot(), &runtime_dir)?;

    let (stop_tx, stop_rx) = mpsc::channel();
    let publish_interval = config.publish_interval;
    let stats_thread = Arc::clone(&stats);
    let runtime_dir_thread = runtime_dir.clone();
    let join = thread::Builder::new()
        .name("kst-stats-publisher".to_string())
        .spawn(move || {
            let mut last_event_key: Option<(String, Option<String>)> = None;
            loop {
                match stop_rx.recv_timeout(publish_interval) {
                    Ok(()) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        let snapshot = stats_thread.snapshot();
                        if let Err(err) = write_stats_tree(&snapshot, &runtime_dir_thread) {
                            stats_thread.record_background_error(format!(
                                "KST runtime tree update failed at {}: {}",
                                runtime_dir_thread.display(),
                                err
                            ));
                        }
                        let status = RuntimeStatusSnapshot::from_snapshot(&snapshot);
                        let _ = append_event_if_changed(
                            &runtime_dir_thread,
                            &status,
                            &mut last_event_key,
                        );
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            let snapshot = stats_thread.snapshot();
            let _ = write_stats_tree(&snapshot, &runtime_dir_thread);
            let status = RuntimeStatusSnapshot::from_snapshot(&snapshot);
            let _ = append_event_if_changed(&runtime_dir_thread, &status, &mut last_event_key);
        })?;

    Ok(TargetStatsPublisher {
        runtime_dir,
        stop_tx: Some(stop_tx),
        join: Some(join),
    })
}

pub(crate) fn write_stats_tree(snapshot: &TargetLiveSnapshot, root: &Path) -> io::Result<()> {
    fs::create_dir_all(root.join("rpcs"))?;
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

    write_text(
        &root.join("summary"),
        format!(
            concat!(
                "target_id={}\n",
                "listen_addr={}\n",
                "pid={}\n",
                "drive_id={}\n",
                "numa_node={}\n",
                "uptime_ms={}\n",
                "active_connections={}\n",
                "peak_active_connections={}\n",
                "total_connections_accepted={}\n",
                "total_connections_rejected={}\n",
                "total_handshake_failures={}\n",
                "inflight_requests={}\n",
                "peak_inflight_requests={}\n",
                "total_stream_rejections={}\n",
                "read_stream_rejections={}\n",
                "write_stream_rejections={}\n",
                "kp2_packed_write_requests={}\n",
                "kp2_packed_read_requests={}\n",
                "total_requests={}\n",
                "total_errors={}\n",
                "read_payload_bytes={}\n",
                "write_payload_bytes={}\n",
                "last_error={}\n"
            ),
            snapshot.identity.target_id,
            snapshot.identity.listen_addr,
            snapshot.identity.pid,
            snapshot.identity.drive_id,
            snapshot
                .identity
                .numa_node
                .map(|node| node.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            snapshot.stats.uptime_ms,
            snapshot.stats.connections.active_connections,
            snapshot.stats.connections.peak_active_connections,
            snapshot.stats.connections.total_connections_accepted,
            snapshot.stats.connections.total_connections_rejected,
            snapshot.stats.connections.total_handshake_failures,
            snapshot.stats.inflight_requests,
            snapshot.stats.peak_inflight_requests,
            snapshot.stats.connections.total_stream_rejections,
            snapshot.stats.streams.read_stream_rejections,
            snapshot.stats.streams.write_stream_rejections,
            snapshot.stats.kp2.packed_write_requests,
            snapshot.stats.kp2.packed_read_requests,
            snapshot.stats.total_requests,
            snapshot.stats.total_errors,
            snapshot.stats.read_payload_bytes,
            snapshot.stats.write_payload_bytes,
            snapshot.stats.last_error.clone().unwrap_or_default(),
        ),
    )?;
    write_text(
        &root.join("identity"),
        format!(
            concat!(
                "target_id={}\n",
                "listen_addr={}\n",
                "listen_backlog={}\n",
                "raw_device={}\n",
                "raw_offset_bytes={}\n",
                "raw_slice_bytes={}\n",
                "media_device={}\n",
                "media_offset_bytes={}\n",
                "media_slice_bytes={}\n",
                "layout_kind={}\n",
                "extent_bytes={}\n",
                "packed_bytes={}\n",
                "key_slots={}\n",
                "publication_lanes={}\n",
                "target_stats_runtime_dir={}\n",
                "kix_stats_runtime_dir={}\n"
            ),
            snapshot.identity.target_id,
            snapshot.identity.listen_addr,
            snapshot.identity.listen_backlog,
            snapshot.identity.raw_device,
            snapshot.identity.raw_offset_bytes,
            snapshot.identity.raw_slice_bytes,
            snapshot.identity.media_device,
            snapshot.identity.media_offset_bytes,
            snapshot.identity.media_slice_bytes,
            snapshot.identity.layout_kind,
            snapshot.identity.extent_bytes,
            snapshot.identity.packed_bytes,
            snapshot.identity.key_slots,
            snapshot.identity.publication_lanes,
            snapshot.identity.target_stats_runtime_dir,
            snapshot.identity.kix_stats_runtime_dir,
        ),
    )?;
    write_text(
        &root.join("config"),
        format!(
            concat!(
                "shard_count={}\n",
                "lookup_mode={}\n",
                "commit_mode={}\n",
                "drive_mode={}\n",
                "lookup_pin_cores={}\n",
                "commit_pin_cores={}\n",
                "drive_pin_cores={}\n",
                "lookup_queue_depth={}\n",
                "commit_queue_depth={}\n",
                "drive_queue_depth={}\n",
                "read_ingress_mode={}\n",
                "write_ingress_mode={}\n",
                "read_ingress_workers={}\n",
                "write_ingress_workers={}\n",
                "read_ingress_pin_cores={}\n",
                "write_ingress_pin_cores={}\n",
                "read_ingress_queue_depth={}\n",
                "write_ingress_queue_depth={}\n",
                "direct_read_mode={}\n",
                "direct_write_mode={}\n",
                "direct_read_workers={}\n",
                "direct_write_workers={}\n",
                "direct_read_pin_cores={}\n",
                "direct_write_pin_cores={}\n",
                "direct_read_queue_depth={}\n",
                "direct_write_queue_depth={}\n",
                "max_packed_payload_bytes={}\n",
                "max_packed_write_request_bytes={}\n",
                "max_request_body_bytes={}\n",
                "max_connections={}\n",
                "max_active_streams={}\n",
                "max_read_streams={}\n",
                "max_write_streams={}\n",
                "h2_initial_window_bytes={}\n",
                "h2_initial_connection_window_bytes={}\n",
                "h2_max_frame_bytes={}\n",
                "h2_max_header_list_bytes={}\n",
                "h2_max_concurrent_streams={}\n",
                "h2_max_send_buffer_bytes={}\n",
            ),
            snapshot.identity.shard_count,
            snapshot.identity.lookup_mode,
            snapshot.identity.commit_mode,
            snapshot.identity.drive_mode,
            join_usize(&snapshot.identity.lookup_pin_cores),
            join_usize(&snapshot.identity.commit_pin_cores),
            join_usize(&snapshot.identity.drive_pin_cores),
            snapshot.identity.lookup_queue_depth,
            snapshot.identity.commit_queue_depth,
            snapshot.identity.drive_queue_depth,
            snapshot.identity.read_ingress_mode,
            snapshot.identity.write_ingress_mode,
            snapshot.identity.read_ingress_workers,
            snapshot.identity.write_ingress_workers,
            join_usize(&snapshot.identity.read_ingress_pin_cores),
            join_usize(&snapshot.identity.write_ingress_pin_cores),
            snapshot.identity.read_ingress_queue_depth,
            snapshot.identity.write_ingress_queue_depth,
            snapshot.identity.direct_read_mode,
            snapshot.identity.direct_write_mode,
            snapshot.identity.direct_read_workers,
            snapshot.identity.direct_write_workers,
            join_usize(&snapshot.identity.direct_read_pin_cores),
            join_usize(&snapshot.identity.direct_write_pin_cores),
            snapshot.identity.direct_read_queue_depth,
            snapshot.identity.direct_write_queue_depth,
            snapshot.identity.max_packed_payload_bytes,
            snapshot.identity.max_packed_write_request_bytes,
            snapshot.identity.max_request_body_bytes,
            snapshot.identity.max_connections,
            snapshot.identity.max_active_streams,
            snapshot.identity.max_read_streams,
            snapshot.identity.max_write_streams,
            snapshot.identity.h2_initial_window_bytes,
            snapshot.identity.h2_initial_connection_window_bytes,
            snapshot.identity.h2_max_frame_bytes,
            snapshot.identity.h2_max_header_list_bytes,
            snapshot.identity.h2_max_concurrent_streams,
            snapshot.identity.h2_max_send_buffer_bytes,
        ),
    )?;
    write_text(
        &root.join("hardware"),
        format!(
            concat!(
                "cpu_arch={}\n",
                "crc32_backend={}\n",
                "crc32_accelerated={}\n",
                "crc32_detail={}\n"
            ),
            snapshot.identity.cpu_arch,
            snapshot.identity.crc32_backend,
            yes_no(snapshot.identity.crc32_accelerated),
            snapshot.identity.crc32_detail,
        ),
    )?;
    write_text(
        &root.join("embedded-kix"),
        format!(
            concat!(
                "runtime_dir={}\n",
                "total_live_entries={}\n",
                "total_get_ops={}\n",
                "total_get_hits={}\n",
                "total_get_misses={}\n",
                "total_upsert_ops={}\n",
                "total_delete_ops={}\n",
                "total_checkpoint_ops={}\n",
                "total_write_errors={}\n",
                "rebuild_required_drives={}\n"
            ),
            snapshot.embedded_kix.runtime_dir,
            snapshot.embedded_kix.total_live_entries,
            snapshot.embedded_kix.total_get_ops,
            snapshot.embedded_kix.total_get_hits,
            snapshot.embedded_kix.total_get_misses,
            snapshot.embedded_kix.total_upsert_ops,
            snapshot.embedded_kix.total_delete_ops,
            snapshot.embedded_kix.total_checkpoint_ops,
            snapshot.embedded_kix.total_write_errors,
            join_u16(&snapshot.embedded_kix.rebuild_required_drives),
        ),
    )?;
    write_text(
        &root.join("connections"),
        format!(
            concat!(
                "active_connections={}\n",
                "peak_active_connections={}\n",
                "total_connections_accepted={}\n",
                "total_connections_completed={}\n",
                "total_connections_rejected={}\n",
                "total_accept_errors={}\n",
                "total_handshake_successes={}\n",
                "total_handshake_failures={}\n",
                "handshake_latency_samples={}\n",
                "handshake_latency_avg_us={}\n",
                "handshake_latency_p50_us={}\n",
                "handshake_latency_p95_us={}\n",
                "handshake_latency_p99_us={}\n",
                "handshake_latency_max_us={}\n",
                "connection_lifetime_samples={}\n",
                "connection_lifetime_avg_us={}\n",
                "connection_lifetime_p50_us={}\n",
                "connection_lifetime_p95_us={}\n",
                "connection_lifetime_p99_us={}\n",
                "connection_lifetime_max_us={}\n"
            ),
            snapshot.stats.connections.active_connections,
            snapshot.stats.connections.peak_active_connections,
            snapshot.stats.connections.total_connections_accepted,
            snapshot.stats.connections.total_connections_completed,
            snapshot.stats.connections.total_connections_rejected,
            snapshot.stats.connections.total_accept_errors,
            snapshot.stats.connections.total_handshake_successes,
            snapshot.stats.connections.total_handshake_failures,
            snapshot.stats.connections.handshake_latency.samples,
            snapshot.stats.connections.handshake_latency.avg_us,
            snapshot.stats.connections.handshake_latency.p50_us,
            snapshot.stats.connections.handshake_latency.p95_us,
            snapshot.stats.connections.handshake_latency.p99_us,
            snapshot.stats.connections.handshake_latency.max_us,
            snapshot.stats.connections.connection_lifetime.samples,
            snapshot.stats.connections.connection_lifetime.avg_us,
            snapshot.stats.connections.connection_lifetime.p50_us,
            snapshot.stats.connections.connection_lifetime.p95_us,
            snapshot.stats.connections.connection_lifetime.p99_us,
            snapshot.stats.connections.connection_lifetime.max_us,
        ),
    )?;
    write_text(
        &root.join("streams"),
        format!(
            concat!(
                "active_streams={}\n",
                "peak_active_streams={}\n",
                "max_active_streams={}\n",
                "total_stream_rejections={}\n",
                "active_read_streams={}\n",
                "peak_active_read_streams={}\n",
                "max_read_streams={}\n",
                "read_stream_rejections={}\n",
                "active_write_streams={}\n",
                "peak_active_write_streams={}\n",
                "max_write_streams={}\n",
                "write_stream_rejections={}\n",
            ),
            snapshot.stats.streams.active_streams,
            snapshot.stats.streams.peak_active_streams,
            snapshot.identity.max_active_streams,
            snapshot.stats.streams.total_stream_rejections,
            snapshot.stats.streams.active_read_streams,
            snapshot.stats.streams.peak_active_read_streams,
            snapshot.identity.max_read_streams,
            snapshot.stats.streams.read_stream_rejections,
            snapshot.stats.streams.active_write_streams,
            snapshot.stats.streams.peak_active_write_streams,
            snapshot.identity.max_write_streams,
            snapshot.stats.streams.write_stream_rejections,
        ),
    )?;
    write_text(
        &root.join("kp2"),
        format!(
            concat!(
                "packed_write_requests={}\n",
                "packed_write_chunks={}\n",
                "packed_write_logical_payload_bytes={}\n",
                "packed_read_requests={}\n",
                "packed_read_chunks={}\n",
                "packed_read_logical_payload_bytes={}\n",
            ),
            snapshot.stats.kp2.packed_write_requests,
            snapshot.stats.kp2.packed_write_chunks,
            snapshot.stats.kp2.packed_write_logical_payload_bytes,
            snapshot.stats.kp2.packed_read_requests,
            snapshot.stats.kp2.packed_read_chunks,
            snapshot.stats.kp2.packed_read_logical_payload_bytes,
        ),
    )?;
    write_text(
        &root.join("errors"),
        format!(
            "total_errors={}\nlast_error={}\n",
            snapshot.stats.total_errors,
            snapshot.stats.last_error.clone().unwrap_or_default()
        ),
    )?;
    write_rpc_file(
        &root.join("rpcs/target-info"),
        &snapshot.stats.rpcs.target_info,
    )?;
    write_rpc_file(&root.join("rpcs/head"), &snapshot.stats.rpcs.head)?;
    write_rpc_file(&root.join("rpcs/read"), &snapshot.stats.rpcs.read)?;
    write_rpc_file(&root.join("rpcs/write"), &snapshot.stats.rpcs.write)?;
    write_rpc_file(&root.join("rpcs/delete"), &snapshot.stats.rpcs.delete)?;
    write_rpc_file(&root.join("rpcs/stats"), &snapshot.stats.rpcs.stats)?;
    write_rpc_file(&root.join("rpcs/other"), &snapshot.stats.rpcs.other)?;
    Ok(())
}

fn write_rpc_file(path: &Path, rpc: &RpcStatsSnapshot) -> io::Result<()> {
    let mut out = format!(
        concat!(
            "requests={}\n",
            "errors={}\n",
            "payload_bytes={}\n",
            "latency_samples={}\n",
            "latency_avg_us={}\n",
            "latency_p50_us={}\n",
            "latency_p95_us={}\n",
            "latency_p99_us={}\n",
            "latency_max_us={}\n"
        ),
        rpc.requests,
        rpc.errors,
        rpc.payload_bytes,
        rpc.latency.samples,
        rpc.latency.avg_us,
        rpc.latency.p50_us,
        rpc.latency.p95_us,
        rpc.latency.p99_us,
        rpc.latency.max_us,
    );
    for (name, summary) in &rpc.phases {
        out.push_str(&format!(
            concat!(
                "phase_{}_samples={}\n",
                "phase_{}_avg_us={}\n",
                "phase_{}_p50_us={}\n",
                "phase_{}_p95_us={}\n",
                "phase_{}_p99_us={}\n",
                "phase_{}_max_us={}\n"
            ),
            name,
            summary.samples,
            name,
            summary.avg_us,
            name,
            summary.p50_us,
            name,
            summary.p95_us,
            name,
            summary.p99_us,
            name,
            summary.max_us,
        ));
    }
    write_text(path, out)
}

fn write_text(path: &Path, content: String) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)
}

impl RuntimeStatusSnapshot {
    fn from_snapshot(snapshot: &TargetLiveSnapshot) -> Self {
        let health = if snapshot.identity.rebuild_required || snapshot.stats.last_error.is_some() {
            "degraded"
        } else {
            "healthy"
        };
        Self {
            service: "kst",
            health,
            ready: true,
            target_id: snapshot.identity.target_id.clone(),
            pid: snapshot.stats.pid,
            uptime_ms: snapshot.stats.uptime_ms,
            started_unix_s: snapshot.stats.started_unix_s,
            total_requests: snapshot.stats.total_requests,
            total_errors: snapshot.stats.total_errors,
            rebuild_required: snapshot.identity.rebuild_required,
            inflight_requests: snapshot.stats.inflight_requests,
            active_connections: snapshot.stats.connections.active_connections,
            active_streams: snapshot.stats.streams.active_streams,
            last_error: snapshot.stats.last_error.clone(),
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
    let mut target = (samples as f64 * percentile).ceil() as u64;
    if target == 0 {
        target = 1;
    }
    let mut seen = 0_u64;
    for (index, bucket) in buckets.iter().enumerate() {
        seen = seen.saturating_add(bucket.load(Ordering::Relaxed));
        if seen >= target {
            return 1_u64 << index;
        }
    }
    1_u64 << (LATENCY_BUCKETS - 1)
}

fn join_usize(values: &[usize]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn join_u16(values: &[u16]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn stream_class_for_rpc(rpc: RpcKind) -> StreamClass {
    match rpc {
        RpcKind::Write | RpcKind::Delete => StreamClass::Write,
        RpcKind::TargetInfo | RpcKind::Head | RpcKind::Read | RpcKind::Stats | RpcKind::Other => {
            StreamClass::Read
        }
    }
}

fn sanitize_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "kst".to_string()
    } else {
        out
    }
}
