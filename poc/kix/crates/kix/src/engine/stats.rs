// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use super::config::{KixError, KixStatsConfig, WorkerMode};
use super::runtime::{DriveRequest, ShardQueueSet, WorkQueue};
use super::KixStatsHandle;
use crate::arena::DriveConfig;
use crate::hardware::KixHardwareAcceleration;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const LATENCY_BUCKETS: usize = 32;

#[derive(Clone, Debug)]
pub struct KixStatsSnapshot {
    pub pid: u32,
    pub started_unix_s: u64,
    pub uptime_ms: u128,
    pub shard_count: usize,
    pub drive_count: usize,
    pub hardware: KixHardwareAcceleration,
    pub total_live_entries: u64,
    pub total_get_ops: u64,
    pub total_get_hits: u64,
    pub total_get_misses: u64,
    pub total_upsert_ops: u64,
    pub total_delete_ops: u64,
    pub total_snapshot_ops: u64,
    pub total_append_batches: u64,
    pub total_appended_deltas: u64,
    pub total_checkpoint_ops: u64,
    pub total_checkpoint_entries: u64,
    pub total_enqueue_retries: u64,
    pub total_write_errors: u64,
    pub total_shard_errors: u64,
    pub rebuild_required_drives: Vec<u16>,
    pub shards: Vec<ShardStatsSnapshot>,
    pub drives: Vec<DriveStatsSnapshot>,
}

#[derive(Clone, Debug)]
pub struct ShardStatsSnapshot {
    pub shard_id: usize,
    pub numa_node: Option<i32>,
    pub lookup_worker_mode: &'static str,
    pub lookup_pin_core: Option<usize>,
    pub lookup_queue_capacity: usize,
    pub lookup_queue_depth: usize,
    pub lookup_enqueued: u64,
    pub lookup_dequeued: u64,
    pub lookup_enqueue_retries: u64,
    pub commit_worker_mode: &'static str,
    pub commit_pin_core: Option<usize>,
    pub commit_queue_capacity: usize,
    pub commit_queue_depth: usize,
    pub commit_enqueued: u64,
    pub commit_dequeued: u64,
    pub commit_enqueue_retries: u64,
    pub get_ops: u64,
    pub get_hits: u64,
    pub get_misses: u64,
    pub get_latency: LatencyStatsSnapshot,
    pub upsert_ops: u64,
    pub upsert_latency: LatencyStatsSnapshot,
    pub delete_ops: u64,
    pub delete_latency: LatencyStatsSnapshot,
    pub snapshot_ops: u64,
    pub snapshot_latency: LatencyStatsSnapshot,
    pub live_entries: u64,
    pub errors: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DriveStatsSnapshot {
    pub drive_id: u16,
    pub worker_mode: &'static str,
    pub pin_core: Option<usize>,
    pub numa_node: Option<i32>,
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub enqueued: u64,
    pub dequeued: u64,
    pub enqueue_retries: u64,
    pub append_batches: u64,
    pub appended_deltas: u64,
    pub append_latency: LatencyStatsSnapshot,
    pub checkpoint_ops: u64,
    pub checkpoint_entries: u64,
    pub checkpoint_latency: LatencyStatsSnapshot,
    pub write_errors: u64,
    pub last_error: Option<String>,
    pub arena_path: PathBuf,
    pub arena_offset_bytes: u64,
    pub arena_len_bytes: Option<u64>,
    pub arena_io_mode: &'static str,
}

#[derive(Clone, Debug, Default)]
pub struct LatencyStatsSnapshot {
    pub samples: u64,
    pub avg_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

pub(super) struct QueueRuntimeStats {
    capacity: usize,
    pub(super) enqueued: AtomicU64,
    pub(super) dequeued: AtomicU64,
    pub(super) enqueue_retries: AtomicU64,
}

impl QueueRuntimeStats {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            enqueued: AtomicU64::new(0),
            dequeued: AtomicU64::new(0),
            enqueue_retries: AtomicU64::new(0),
        }
    }

    fn snapshot<T>(&self, queue: &WorkQueue<T>) -> QueueStatsSnapshot {
        QueueStatsSnapshot {
            capacity: self.capacity,
            depth: queue.depth(),
            enqueued: self.enqueued.load(Ordering::Relaxed),
            dequeued: self.dequeued.load(Ordering::Relaxed),
            enqueue_retries: self.enqueue_retries.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug)]
struct QueueStatsSnapshot {
    capacity: usize,
    depth: usize,
    enqueued: u64,
    dequeued: u64,
    enqueue_retries: u64,
}

pub(super) struct LatencyRuntimeStats {
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

    pub(super) fn observe(&self, elapsed: Duration) {
        let micros = elapsed.as_micros().max(1) as u64;
        self.samples.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(micros, Ordering::Relaxed);
        update_atomic_max(&self.max_us, micros);
        self.buckets[latency_bucket_index(micros)].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LatencyStatsSnapshot {
        let samples = self.samples.load(Ordering::Relaxed);
        if samples == 0 {
            return LatencyStatsSnapshot::default();
        }
        let total_us = self.total_us.load(Ordering::Relaxed);
        let max_us = self.max_us.load(Ordering::Relaxed);
        LatencyStatsSnapshot {
            samples,
            avg_us: total_us / samples,
            p50_us: percentile_from_buckets(&self.buckets, samples, 0.50),
            p95_us: percentile_from_buckets(&self.buckets, samples, 0.95),
            p99_us: percentile_from_buckets(&self.buckets, samples, 0.99),
            max_us,
        }
    }
}

pub(super) struct ShardRuntimeStats {
    pub(super) shard_id: usize,
    pub(super) numa_node: Option<i32>,
    pub(super) lookup_worker_mode: WorkerMode,
    pub(super) lookup_pin_core: Option<usize>,
    pub(super) lookup_queue: Arc<QueueRuntimeStats>,
    pub(super) commit_worker_mode: WorkerMode,
    pub(super) commit_pin_core: Option<usize>,
    pub(super) commit_queue: Arc<QueueRuntimeStats>,
    pub(super) get_ops: AtomicU64,
    pub(super) get_hits: AtomicU64,
    pub(super) get_misses: AtomicU64,
    pub(super) upsert_ops: AtomicU64,
    pub(super) delete_ops: AtomicU64,
    pub(super) snapshot_ops: AtomicU64,
    pub(super) get_latency: LatencyRuntimeStats,
    pub(super) upsert_latency: LatencyRuntimeStats,
    pub(super) delete_latency: LatencyRuntimeStats,
    pub(super) snapshot_latency: LatencyRuntimeStats,
    pub(super) live_entries: AtomicU64,
    errors: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl ShardRuntimeStats {
    pub(super) fn new(
        shard_id: usize,
        lookup_worker_mode: WorkerMode,
        commit_worker_mode: WorkerMode,
        lookup_pin_core: Option<usize>,
        commit_pin_core: Option<usize>,
        numa_node: Option<i32>,
        lookup_queue_capacity: usize,
        commit_queue_capacity: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            shard_id,
            numa_node,
            lookup_worker_mode,
            lookup_pin_core,
            lookup_queue: Arc::new(QueueRuntimeStats::new(lookup_queue_capacity)),
            commit_worker_mode,
            commit_pin_core,
            commit_queue: Arc::new(QueueRuntimeStats::new(commit_queue_capacity)),
            get_ops: AtomicU64::new(0),
            get_hits: AtomicU64::new(0),
            get_misses: AtomicU64::new(0),
            upsert_ops: AtomicU64::new(0),
            delete_ops: AtomicU64::new(0),
            snapshot_ops: AtomicU64::new(0),
            get_latency: LatencyRuntimeStats::new(),
            upsert_latency: LatencyRuntimeStats::new(),
            delete_latency: LatencyRuntimeStats::new(),
            snapshot_latency: LatencyRuntimeStats::new(),
            live_entries: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_error: Mutex::new(None),
        })
    }

    pub(super) fn record_error(&self, message: impl Into<String>) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(message.into());
        }
    }

    fn snapshot(&self, queues: &ShardQueueSet) -> ShardStatsSnapshot {
        let lookup_queue = self.lookup_queue.snapshot(&queues.lookup);
        let commit_queue = self.commit_queue.snapshot(&queues.commit);
        ShardStatsSnapshot {
            shard_id: self.shard_id,
            numa_node: self.numa_node,
            lookup_worker_mode: self.lookup_worker_mode.as_str(),
            lookup_pin_core: self.lookup_pin_core,
            lookup_queue_capacity: lookup_queue.capacity,
            lookup_queue_depth: lookup_queue.depth,
            lookup_enqueued: lookup_queue.enqueued,
            lookup_dequeued: lookup_queue.dequeued,
            lookup_enqueue_retries: lookup_queue.enqueue_retries,
            commit_worker_mode: self.commit_worker_mode.as_str(),
            commit_pin_core: self.commit_pin_core,
            commit_queue_capacity: commit_queue.capacity,
            commit_queue_depth: commit_queue.depth,
            commit_enqueued: commit_queue.enqueued,
            commit_dequeued: commit_queue.dequeued,
            commit_enqueue_retries: commit_queue.enqueue_retries,
            get_ops: self.get_ops.load(Ordering::Relaxed),
            get_hits: self.get_hits.load(Ordering::Relaxed),
            get_misses: self.get_misses.load(Ordering::Relaxed),
            get_latency: self.get_latency.snapshot(),
            upsert_ops: self.upsert_ops.load(Ordering::Relaxed),
            upsert_latency: self.upsert_latency.snapshot(),
            delete_ops: self.delete_ops.load(Ordering::Relaxed),
            delete_latency: self.delete_latency.snapshot(),
            snapshot_ops: self.snapshot_ops.load(Ordering::Relaxed),
            snapshot_latency: self.snapshot_latency.snapshot(),
            live_entries: self.live_entries.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            last_error: self.last_error.lock().ok().and_then(|slot| slot.clone()),
        }
    }
}

pub(super) struct DriveRuntimeStats {
    pub(super) drive_id: u16,
    worker_mode: WorkerMode,
    pin_core: Option<usize>,
    numa_node: Option<i32>,
    pub(super) queue: Arc<QueueRuntimeStats>,
    pub(super) append_batches: AtomicU64,
    pub(super) appended_deltas: AtomicU64,
    pub(super) checkpoint_ops: AtomicU64,
    pub(super) checkpoint_entries: AtomicU64,
    pub(super) append_latency: LatencyRuntimeStats,
    pub(super) checkpoint_latency: LatencyRuntimeStats,
    write_errors: AtomicU64,
    last_error: Mutex<Option<String>>,
    pub(super) arena_path: PathBuf,
    arena_offset_bytes: u64,
    arena_len_bytes: Option<u64>,
    arena_io_mode: &'static str,
}

impl DriveRuntimeStats {
    pub(super) fn new(
        cfg: &DriveConfig,
        worker_mode: WorkerMode,
        pin_core: Option<usize>,
        queue_capacity: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            drive_id: cfg.id,
            worker_mode,
            pin_core,
            numa_node: cfg.numa_node,
            queue: Arc::new(QueueRuntimeStats::new(queue_capacity)),
            append_batches: AtomicU64::new(0),
            appended_deltas: AtomicU64::new(0),
            checkpoint_ops: AtomicU64::new(0),
            checkpoint_entries: AtomicU64::new(0),
            append_latency: LatencyRuntimeStats::new(),
            checkpoint_latency: LatencyRuntimeStats::new(),
            write_errors: AtomicU64::new(0),
            last_error: Mutex::new(None),
            arena_path: cfg.arena_path.clone(),
            arena_offset_bytes: cfg.arena_offset_bytes,
            arena_len_bytes: cfg.arena_len_bytes,
            arena_io_mode: cfg.io_mode.as_str(),
        })
    }

    pub(super) fn record_error(&self, message: impl Into<String>) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(message.into());
        }
    }

    fn snapshot(&self, queue: &WorkQueue<DriveRequest>) -> DriveStatsSnapshot {
        let queue = self.queue.snapshot(queue);
        DriveStatsSnapshot {
            drive_id: self.drive_id,
            worker_mode: self.worker_mode.as_str(),
            pin_core: self.pin_core,
            numa_node: self.numa_node,
            queue_capacity: queue.capacity,
            queue_depth: queue.depth,
            enqueued: queue.enqueued,
            dequeued: queue.dequeued,
            enqueue_retries: queue.enqueue_retries,
            append_batches: self.append_batches.load(Ordering::Relaxed),
            appended_deltas: self.appended_deltas.load(Ordering::Relaxed),
            append_latency: self.append_latency.snapshot(),
            checkpoint_ops: self.checkpoint_ops.load(Ordering::Relaxed),
            checkpoint_entries: self.checkpoint_entries.load(Ordering::Relaxed),
            checkpoint_latency: self.checkpoint_latency.snapshot(),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            last_error: self.last_error.lock().ok().and_then(|slot| slot.clone()),
            arena_path: self.arena_path.clone(),
            arena_offset_bytes: self.arena_offset_bytes,
            arena_len_bytes: self.arena_len_bytes,
            arena_io_mode: self.arena_io_mode,
        }
    }
}

pub(super) struct KixRuntimeStats {
    pub(super) pid: u32,
    pub(super) started_at: Instant,
    pub(super) started_unix_s: u64,
    pub(super) hardware: KixHardwareAcceleration,
    pub(super) shards: Vec<Arc<ShardRuntimeStats>>,
    pub(super) drives: Vec<Arc<DriveRuntimeStats>>,
    pub(super) rebuild_required_drives: Vec<u16>,
}

impl KixRuntimeStats {
    pub(super) fn snapshot(
        &self,
        shard_queues: &[ShardQueueSet],
        drive_queues: &HashMap<u16, Arc<WorkQueue<DriveRequest>>>,
    ) -> KixStatsSnapshot {
        let mut shard_snapshots = Vec::with_capacity(self.shards.len());
        for (stats, queue) in self.shards.iter().zip(shard_queues.iter()) {
            shard_snapshots.push(stats.snapshot(queue));
        }

        let mut drive_snapshots = self
            .drives
            .iter()
            .filter_map(|stats| {
                drive_queues
                    .get(&stats.drive_id)
                    .map(|queue| stats.snapshot(queue))
            })
            .collect::<Vec<_>>();
        drive_snapshots.sort_by_key(|snapshot| snapshot.drive_id);

        KixStatsSnapshot {
            pid: self.pid,
            started_unix_s: self.started_unix_s,
            uptime_ms: self.started_at.elapsed().as_millis(),
            shard_count: shard_snapshots.len(),
            drive_count: drive_snapshots.len(),
            hardware: self.hardware,
            total_live_entries: shard_snapshots
                .iter()
                .map(|snapshot| snapshot.live_entries)
                .sum(),
            total_get_ops: shard_snapshots
                .iter()
                .map(|snapshot| snapshot.get_ops)
                .sum(),
            total_get_hits: shard_snapshots
                .iter()
                .map(|snapshot| snapshot.get_hits)
                .sum(),
            total_get_misses: shard_snapshots
                .iter()
                .map(|snapshot| snapshot.get_misses)
                .sum(),
            total_upsert_ops: shard_snapshots
                .iter()
                .map(|snapshot| snapshot.upsert_ops)
                .sum(),
            total_delete_ops: shard_snapshots
                .iter()
                .map(|snapshot| snapshot.delete_ops)
                .sum(),
            total_snapshot_ops: shard_snapshots
                .iter()
                .map(|snapshot| snapshot.snapshot_ops)
                .sum(),
            total_append_batches: drive_snapshots
                .iter()
                .map(|snapshot| snapshot.append_batches)
                .sum(),
            total_appended_deltas: drive_snapshots
                .iter()
                .map(|snapshot| snapshot.appended_deltas)
                .sum(),
            total_checkpoint_ops: drive_snapshots
                .iter()
                .map(|snapshot| snapshot.checkpoint_ops)
                .sum(),
            total_checkpoint_entries: drive_snapshots
                .iter()
                .map(|snapshot| snapshot.checkpoint_entries)
                .sum(),
            total_enqueue_retries: shard_snapshots
                .iter()
                .map(|snapshot| snapshot.lookup_enqueue_retries + snapshot.commit_enqueue_retries)
                .sum::<u64>()
                + drive_snapshots
                    .iter()
                    .map(|snapshot| snapshot.enqueue_retries)
                    .sum::<u64>(),
            total_write_errors: drive_snapshots
                .iter()
                .map(|snapshot| snapshot.write_errors)
                .sum(),
            total_shard_errors: shard_snapshots.iter().map(|snapshot| snapshot.errors).sum(),
            rebuild_required_drives: self.rebuild_required_drives.clone(),
            shards: shard_snapshots,
            drives: drive_snapshots,
        }
    }
}

pub(super) struct StatsPublisherHandle {
    pub(super) runtime_dir: PathBuf,
    pub(super) stop_tx: Option<mpsc::Sender<()>>,
    pub(super) join: Option<JoinHandle<()>>,
}

pub(super) fn spawn_stats_publisher(
    stats: KixStatsHandle,
    config: &KixStatsConfig,
) -> Result<StatsPublisherHandle, KixError> {
    let runtime_dir = write_stats_tree(&stats.snapshot(), &config.root_dir)?;
    let interval = config.publish_interval;
    let root_dir = config.root_dir.clone();
    let runtime_dir_clone = runtime_dir.clone();
    let (stop_tx, stop_rx) = mpsc::channel();
    let join = thread::Builder::new()
        .name("kix-stats-publisher".to_string())
        .spawn(move || {
            loop {
                match stop_rx.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(err) = stats.write_tree(&root_dir) {
                            eprintln!(
                                "warning: KIX stats publisher could not refresh {}: {err}",
                                runtime_dir_clone.display()
                            );
                        }
                    }
                }
            }
            if let Err(err) = stats.write_tree(&root_dir) {
                eprintln!(
                    "warning: KIX stats publisher could not write final snapshot to {}: {err}",
                    runtime_dir_clone.display()
                );
            }
        })
        .map_err(KixError::Io)?;

    Ok(StatsPublisherHandle {
        runtime_dir,
        stop_tx: Some(stop_tx),
        join: Some(join),
    })
}

pub(super) fn write_stats_tree(
    snapshot: &KixStatsSnapshot,
    root: impl AsRef<Path>,
) -> io::Result<PathBuf> {
    let runtime_dir = root.as_ref().join(format!("kix-{}", snapshot.pid));
    std::fs::create_dir_all(runtime_dir.join("shards"))?;
    std::fs::create_dir_all(runtime_dir.join("drives"))?;

    write_text_file_atomic(runtime_dir.join("summary"), &format_summary(snapshot))?;
    write_text_file_atomic(runtime_dir.join("config"), &format_config(snapshot))?;
    write_text_file_atomic(runtime_dir.join("hardware"), &format_hardware(snapshot))?;

    for shard in &snapshot.shards {
        write_text_file_atomic(
            runtime_dir
                .join("shards")
                .join(format!("{}", shard.shard_id)),
            &format_shard_stats(shard),
        )?;
    }
    for drive in &snapshot.drives {
        write_text_file_atomic(
            runtime_dir
                .join("drives")
                .join(format!("{}", drive.drive_id)),
            &format_drive_stats(drive),
        )?;
    }

    Ok(runtime_dir)
}

fn format_summary(snapshot: &KixStatsSnapshot) -> String {
    format!(
        concat!(
            "pid={}\n",
            "started_unix_s={}\n",
            "uptime_ms={}\n",
            "shard_count={}\n",
            "drive_count={}\n",
            "cpu_arch={}\n",
            "crc32_backend={}\n",
            "crc32_accelerated={}\n",
            "rebuild_required_drives={}\n",
            "total_live_entries={}\n",
            "total_get_ops={}\n",
            "total_get_hits={}\n",
            "total_get_misses={}\n",
            "total_upsert_ops={}\n",
            "total_delete_ops={}\n",
            "total_snapshot_ops={}\n",
            "total_append_batches={}\n",
            "total_appended_deltas={}\n",
            "total_checkpoint_ops={}\n",
            "total_checkpoint_entries={}\n",
            "total_enqueue_retries={}\n",
            "total_shard_errors={}\n",
            "total_write_errors={}\n"
        ),
        snapshot.pid,
        snapshot.started_unix_s,
        snapshot.uptime_ms,
        snapshot.shard_count,
        snapshot.drive_count,
        snapshot.hardware.cpu_arch,
        snapshot.hardware.crc32_backend.as_str(),
        bool_flag(snapshot.hardware.crc32_accelerated()),
        csv_u16(&snapshot.rebuild_required_drives),
        snapshot.total_live_entries,
        snapshot.total_get_ops,
        snapshot.total_get_hits,
        snapshot.total_get_misses,
        snapshot.total_upsert_ops,
        snapshot.total_delete_ops,
        snapshot.total_snapshot_ops,
        snapshot.total_append_batches,
        snapshot.total_appended_deltas,
        snapshot.total_checkpoint_ops,
        snapshot.total_checkpoint_entries,
        snapshot.total_enqueue_retries,
        snapshot.total_shard_errors,
        snapshot.total_write_errors,
    )
}

fn format_config(snapshot: &KixStatsSnapshot) -> String {
    let mut out = String::new();
    out.push_str("shards=");
    out.push_str(&snapshot.shards.len().to_string());
    out.push('\n');
    out.push_str("drives=");
    out.push_str(&snapshot.drives.len().to_string());
    out.push('\n');
    out.push_str("cpu_arch=");
    out.push_str(snapshot.hardware.cpu_arch);
    out.push('\n');
    out.push_str("crc32_backend=");
    out.push_str(snapshot.hardware.crc32_backend.as_str());
    out.push('\n');
    out.push_str("crc32_accelerated=");
    out.push_str(bool_flag(snapshot.hardware.crc32_accelerated()));
    out.push('\n');
    for shard in &snapshot.shards {
        out.push_str(&format!(
            concat!(
                "shard.{}.lookup_mode={}\n",
                "shard.{}.lookup_pin_core={}\n",
                "shard.{}.commit_mode={}\n",
                "shard.{}.commit_pin_core={}\n",
                "shard.{}.numa_node={}\n"
            ),
            shard.shard_id,
            shard.lookup_worker_mode,
            shard.shard_id,
            option_usize(shard.lookup_pin_core),
            shard.shard_id,
            shard.commit_worker_mode,
            shard.shard_id,
            option_usize(shard.commit_pin_core),
            shard.shard_id,
            option_i32(shard.numa_node),
        ));
    }
    for drive in &snapshot.drives {
        out.push_str(&format!(
            concat!(
                "drive.{}.mode={}\n",
                "drive.{}.pin_core={}\n",
                "drive.{}.numa_node={}\n",
                "drive.{}.arena_io_mode={}\n",
                "drive.{}.arena_path={}\n",
                "drive.{}.arena_offset_bytes={}\n",
                "drive.{}.arena_len_bytes={}\n"
            ),
            drive.drive_id,
            drive.worker_mode,
            drive.drive_id,
            option_usize(drive.pin_core),
            drive.drive_id,
            option_i32(drive.numa_node),
            drive.drive_id,
            drive.arena_io_mode,
            drive.drive_id,
            drive.arena_path.display(),
            drive.drive_id,
            drive.arena_offset_bytes,
            drive.drive_id,
            option_u64(drive.arena_len_bytes),
        ));
    }
    out
}

fn format_hardware(snapshot: &KixStatsSnapshot) -> String {
    format!(
        concat!(
            "cpu_arch={}\n",
            "crc32_backend={}\n",
            "crc32_accelerated={}\n",
            "crc32_detail={}\n"
        ),
        snapshot.hardware.cpu_arch,
        snapshot.hardware.crc32_backend.as_str(),
        bool_flag(snapshot.hardware.crc32_accelerated()),
        snapshot.hardware.crc32_detail(),
    )
}

fn format_shard_stats(snapshot: &ShardStatsSnapshot) -> String {
    format!(
        concat!(
            "shard_id={}\n",
            "numa_node={}\n",
            "lookup_worker_mode={}\n",
            "lookup_pin_core={}\n",
            "lookup_queue_capacity={}\n",
            "lookup_queue_depth={}\n",
            "lookup_enqueued={}\n",
            "lookup_dequeued={}\n",
            "lookup_enqueue_retries={}\n",
            "commit_worker_mode={}\n",
            "commit_pin_core={}\n",
            "commit_queue_capacity={}\n",
            "commit_queue_depth={}\n",
            "commit_enqueued={}\n",
            "commit_dequeued={}\n",
            "commit_enqueue_retries={}\n",
            "get_ops={}\n",
            "get_hits={}\n",
            "get_misses={}\n",
            "get_latency_samples={}\n",
            "get_latency_avg_us={}\n",
            "get_latency_p50_us={}\n",
            "get_latency_p95_us={}\n",
            "get_latency_p99_us={}\n",
            "get_latency_max_us={}\n",
            "upsert_ops={}\n",
            "upsert_latency_samples={}\n",
            "upsert_latency_avg_us={}\n",
            "upsert_latency_p50_us={}\n",
            "upsert_latency_p95_us={}\n",
            "upsert_latency_p99_us={}\n",
            "upsert_latency_max_us={}\n",
            "delete_ops={}\n",
            "delete_latency_samples={}\n",
            "delete_latency_avg_us={}\n",
            "delete_latency_p50_us={}\n",
            "delete_latency_p95_us={}\n",
            "delete_latency_p99_us={}\n",
            "delete_latency_max_us={}\n",
            "snapshot_ops={}\n",
            "snapshot_latency_samples={}\n",
            "snapshot_latency_avg_us={}\n",
            "snapshot_latency_p50_us={}\n",
            "snapshot_latency_p95_us={}\n",
            "snapshot_latency_p99_us={}\n",
            "snapshot_latency_max_us={}\n",
            "live_entries={}\n",
            "errors={}\n",
            "last_error={}\n"
        ),
        snapshot.shard_id,
        option_i32(snapshot.numa_node),
        snapshot.lookup_worker_mode,
        option_usize(snapshot.lookup_pin_core),
        snapshot.lookup_queue_capacity,
        snapshot.lookup_queue_depth,
        snapshot.lookup_enqueued,
        snapshot.lookup_dequeued,
        snapshot.lookup_enqueue_retries,
        snapshot.commit_worker_mode,
        option_usize(snapshot.commit_pin_core),
        snapshot.commit_queue_capacity,
        snapshot.commit_queue_depth,
        snapshot.commit_enqueued,
        snapshot.commit_dequeued,
        snapshot.commit_enqueue_retries,
        snapshot.get_ops,
        snapshot.get_hits,
        snapshot.get_misses,
        snapshot.get_latency.samples,
        snapshot.get_latency.avg_us,
        snapshot.get_latency.p50_us,
        snapshot.get_latency.p95_us,
        snapshot.get_latency.p99_us,
        snapshot.get_latency.max_us,
        snapshot.upsert_ops,
        snapshot.upsert_latency.samples,
        snapshot.upsert_latency.avg_us,
        snapshot.upsert_latency.p50_us,
        snapshot.upsert_latency.p95_us,
        snapshot.upsert_latency.p99_us,
        snapshot.upsert_latency.max_us,
        snapshot.delete_ops,
        snapshot.delete_latency.samples,
        snapshot.delete_latency.avg_us,
        snapshot.delete_latency.p50_us,
        snapshot.delete_latency.p95_us,
        snapshot.delete_latency.p99_us,
        snapshot.delete_latency.max_us,
        snapshot.snapshot_ops,
        snapshot.snapshot_latency.samples,
        snapshot.snapshot_latency.avg_us,
        snapshot.snapshot_latency.p50_us,
        snapshot.snapshot_latency.p95_us,
        snapshot.snapshot_latency.p99_us,
        snapshot.snapshot_latency.max_us,
        snapshot.live_entries,
        snapshot.errors,
        option_string(snapshot.last_error.as_deref()),
    )
}

fn format_drive_stats(snapshot: &DriveStatsSnapshot) -> String {
    format!(
        concat!(
            "drive_id={}\n",
            "worker_mode={}\n",
            "pin_core={}\n",
            "numa_node={}\n",
            "queue_capacity={}\n",
            "queue_depth={}\n",
            "enqueued={}\n",
            "dequeued={}\n",
            "enqueue_retries={}\n",
            "append_batches={}\n",
            "appended_deltas={}\n",
            "append_latency_samples={}\n",
            "append_latency_avg_us={}\n",
            "append_latency_p50_us={}\n",
            "append_latency_p95_us={}\n",
            "append_latency_p99_us={}\n",
            "append_latency_max_us={}\n",
            "checkpoint_ops={}\n",
            "checkpoint_entries={}\n",
            "checkpoint_latency_samples={}\n",
            "checkpoint_latency_avg_us={}\n",
            "checkpoint_latency_p50_us={}\n",
            "checkpoint_latency_p95_us={}\n",
            "checkpoint_latency_p99_us={}\n",
            "checkpoint_latency_max_us={}\n",
            "write_errors={}\n",
            "last_error={}\n",
            "arena_io_mode={}\n",
            "arena_path={}\n",
            "arena_offset_bytes={}\n",
            "arena_len_bytes={}\n"
        ),
        snapshot.drive_id,
        snapshot.worker_mode,
        option_usize(snapshot.pin_core),
        option_i32(snapshot.numa_node),
        snapshot.queue_capacity,
        snapshot.queue_depth,
        snapshot.enqueued,
        snapshot.dequeued,
        snapshot.enqueue_retries,
        snapshot.append_batches,
        snapshot.appended_deltas,
        snapshot.append_latency.samples,
        snapshot.append_latency.avg_us,
        snapshot.append_latency.p50_us,
        snapshot.append_latency.p95_us,
        snapshot.append_latency.p99_us,
        snapshot.append_latency.max_us,
        snapshot.checkpoint_ops,
        snapshot.checkpoint_entries,
        snapshot.checkpoint_latency.samples,
        snapshot.checkpoint_latency.avg_us,
        snapshot.checkpoint_latency.p50_us,
        snapshot.checkpoint_latency.p95_us,
        snapshot.checkpoint_latency.p99_us,
        snapshot.checkpoint_latency.max_us,
        snapshot.write_errors,
        option_string(snapshot.last_error.as_deref()),
        snapshot.arena_io_mode,
        snapshot.arena_path.display(),
        snapshot.arena_offset_bytes,
        option_u64(snapshot.arena_len_bytes),
    )
}

fn write_text_file_atomic(path: impl AsRef<Path>, contents: &str) -> io::Result<()> {
    let path = path.as_ref();
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("KIX stats path {} has no parent directory", path.display()),
        )
    })?;
    std::fs::create_dir_all(parent)?;
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp_path = path.with_extension(format!("tmp-{}-{unique}", std::process::id()));
    std::fs::write(&tmp_path, contents)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

fn csv_u16(values: &[u16]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn option_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn option_i32(value: Option<i32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn option_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn option_string(value: Option<&str>) -> String {
    value.unwrap_or("none").replace('\n', "\\n")
}

fn bool_flag(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn latency_bucket_index(micros: u64) -> usize {
    let capped = micros.max(1);
    let bucket = 63_u32.saturating_sub(capped.leading_zeros()) as usize;
    bucket.min(LATENCY_BUCKETS - 1)
}

fn percentile_from_buckets(
    buckets: &[AtomicU64; LATENCY_BUCKETS],
    samples: u64,
    fraction: f64,
) -> u64 {
    if samples == 0 {
        return 0;
    }
    let target = ((samples as f64 * fraction).ceil() as u64).clamp(1, samples);
    let mut seen = 0_u64;
    for (idx, bucket) in buckets.iter().enumerate() {
        seen = seen.saturating_add(bucket.load(Ordering::Relaxed));
        if seen >= target {
            return 1_u64 << idx;
        }
    }
    1_u64 << (LATENCY_BUCKETS - 1)
}

fn update_atomic_max(slot: &AtomicU64, candidate: u64) {
    let mut current = slot.load(Ordering::Relaxed);
    while candidate > current {
        match slot.compare_exchange(current, candidate, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}
