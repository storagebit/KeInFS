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
pub(crate) struct KrsIdentity {
    pub(crate) build: BuildInfo,
    pub(crate) lease_owner: String,
    pub(crate) kms_endpoint: String,
    pub(crate) kas_endpoint: String,
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
pub(crate) struct KrsSnapshot {
    pub(crate) identity: KrsIdentity,
    pub(crate) uptime_ms: u64,
    pub(crate) started_unix_s: u64,
    pub(crate) lease_polls: u64,
    pub(crate) leased_tasks: u64,
    pub(crate) rebuilt_tasks: u64,
    pub(crate) failed_tasks: u64,
    pub(crate) rebuilt_bytes: u64,
    pub(crate) lease_cycle_latency: LatencySummary,
    pub(crate) task_latency: LatencySummary,
    pub(crate) phases: BTreeMap<String, LatencySummary>,
    pub(crate) active_task: Option<String>,
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
    lease_owner: String,
    lease_polls: u64,
    leased_tasks: u64,
    completed_tasks: u64,
    failed_tasks: u64,
    rebuilt_bytes: u64,
    active_task: Option<String>,
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

pub(crate) struct KrsStats {
    identity: KrsIdentity,
    started: Instant,
    started_unix_s: u64,
    lease_polls: AtomicU64,
    leased_tasks: AtomicU64,
    rebuilt_tasks: AtomicU64,
    failed_tasks: AtomicU64,
    rebuilt_bytes: AtomicU64,
    lease_cycle_latency: Mutex<LatencyRecorder>,
    task_latency: Mutex<LatencyRecorder>,
    phases: Mutex<BTreeMap<String, LatencyRecorder>>,
    active_task: Mutex<Option<String>>,
    last_error: Mutex<Option<String>>,
}

impl KrsStats {
    pub(crate) fn new(identity: KrsIdentity) -> Arc<Self> {
        Arc::new(Self {
            identity,
            started: Instant::now(),
            started_unix_s: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            lease_polls: AtomicU64::new(0),
            leased_tasks: AtomicU64::new(0),
            rebuilt_tasks: AtomicU64::new(0),
            failed_tasks: AtomicU64::new(0),
            rebuilt_bytes: AtomicU64::new(0),
            lease_cycle_latency: Mutex::new(LatencyRecorder::default()),
            task_latency: Mutex::new(LatencyRecorder::default()),
            phases: Mutex::new(BTreeMap::new()),
            active_task: Mutex::new(None),
            last_error: Mutex::new(None),
        })
    }

    pub(crate) fn record_poll(&self) {
        self.lease_polls.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_lease_cycle(&self, elapsed: Duration) {
        self.lease_cycle_latency.lock().unwrap().observe(elapsed);
    }

    pub(crate) fn record_phase(&self, phase: &str, elapsed: Duration) {
        let mut phases = self.phases.lock().unwrap();
        phases
            .entry(phase.to_string())
            .or_default()
            .observe(elapsed);
    }

    pub(crate) fn record_leased_tasks(&self, count: usize) {
        self.leased_tasks.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_rebuilt_task(&self, bytes: u64, elapsed: Duration) {
        self.rebuilt_tasks.fetch_add(1, Ordering::Relaxed);
        self.rebuilt_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.task_latency.lock().unwrap().observe(elapsed);
    }

    pub(crate) fn record_failed_task(&self, message: impl Into<String>, elapsed: Duration) {
        self.failed_tasks.fetch_add(1, Ordering::Relaxed);
        self.task_latency.lock().unwrap().observe(elapsed);
        *self.last_error.lock().unwrap() = Some(message.into());
    }

    pub(crate) fn set_active_task(&self, task: Option<String>) {
        *self.active_task.lock().unwrap() = task;
    }

    pub(crate) fn set_last_error(&self, message: impl Into<String>) {
        *self.last_error.lock().unwrap() = Some(message.into());
    }

    pub(crate) fn snapshot(&self) -> KrsSnapshot {
        let phases = self
            .phases
            .lock()
            .unwrap()
            .iter()
            .map(|(name, recorder)| (name.clone(), recorder.snapshot()))
            .collect();
        KrsSnapshot {
            identity: self.identity.clone(),
            uptime_ms: self.started.elapsed().as_millis() as u64,
            started_unix_s: self.started_unix_s,
            lease_polls: self.lease_polls.load(Ordering::Relaxed),
            leased_tasks: self.leased_tasks.load(Ordering::Relaxed),
            rebuilt_tasks: self.rebuilt_tasks.load(Ordering::Relaxed),
            failed_tasks: self.failed_tasks.load(Ordering::Relaxed),
            rebuilt_bytes: self.rebuilt_bytes.load(Ordering::Relaxed),
            lease_cycle_latency: self.lease_cycle_latency.lock().unwrap().snapshot(),
            task_latency: self.task_latency.lock().unwrap().snapshot(),
            phases,
            active_task: self.active_task.lock().unwrap().clone(),
            last_error: self.last_error.lock().unwrap().clone(),
        }
    }
}

pub(crate) struct Publisher {
    stop_tx: Option<mpsc::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl Publisher {
    pub(crate) fn spawn(
        stats: Arc<KrsStats>,
        root: impl AsRef<Path>,
        publish_interval: Duration,
    ) -> io::Result<Self> {
        let runtime_dir = root.as_ref().join(format!("krs-{}", stats.identity.pid));
        fs::create_dir_all(&runtime_dir)?;
        let (stop_tx, stop_rx) = mpsc::channel();
        let thread_runtime_dir = runtime_dir.clone();
        let join = thread::Builder::new()
            .name("krs-stats".to_string())
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

fn write_snapshot(root: &Path, snapshot: &KrsSnapshot) -> io::Result<()> {
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
    fn from_snapshot(snapshot: &KrsSnapshot) -> Self {
        let health = if snapshot.last_error.is_some() {
            "degraded"
        } else {
            "healthy"
        };
        Self {
            service: "krs",
            health,
            ready: true,
            uptime_ms: snapshot.uptime_ms,
            started_unix_s: snapshot.started_unix_s,
            pid: snapshot.identity.pid,
            lease_owner: snapshot.identity.lease_owner.clone(),
            lease_polls: snapshot.lease_polls,
            leased_tasks: snapshot.leased_tasks,
            completed_tasks: snapshot.rebuilt_tasks,
            failed_tasks: snapshot.failed_tasks,
            rebuilt_bytes: snapshot.rebuilt_bytes,
            active_task: snapshot.active_task.clone(),
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
