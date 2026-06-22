// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::client::{
    chunk_id_from_seed, payload_len_for_slot, splitmix64, synthetic_payload, ClientError,
    RequestPhaseTimes, TargetInfo, TargetSession,
};
use crate::config::{BenchmarkConfig, TransferMode};
use crate::stats::{spawn_stats_publisher, KscRuntimeStats, KscStatsPublisher};
use kp2::{
    encoded_write_request_len, PackedReadQuery, PackedWriteEntry, PackedWriteReply,
    PackedWriteRequest, WriteIdentity,
};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::{yield_now, JoinSet};

pub(crate) async fn run_benchmark(config: BenchmarkConfig) -> Result<(), ClientError> {
    let stats = Arc::new(KscRuntimeStats::new(&config));
    let mut stats_publisher = spawn_stats_publisher(
        Arc::clone(&stats),
        config.stats_publish_interval,
        &config.stats_root,
    )
    .map_err(|err| {
        ClientError::Transport(format!(
            "KSC could not start its runtime tree publisher under {}: {}",
            config.stats_root.display(),
            err
        ))
    })?;
    println!(
        concat!(
            "ksc_bench_client_id={}\n",
            "ksc_bench_endpoints={}\n",
            "ksc_bench_runtime_dir={}\n",
            "ksc_bench_transfer_mode={}\n",
            "ksc_bench_workers={}\n",
            "ksc_bench_inflight_streams_per_worker={}\n",
            "ksc_bench_target_initial_inflight={}\n",
            "ksc_bench_target_min_inflight={}\n",
            "ksc_bench_pack_max_payload_bytes={}\n",
            "ksc_bench_duration_s={}\n",
            "ksc_bench_write_percent={}\n",
            "ksc_bench_packed_count={}\n",
            "ksc_bench_key_count={}\n",
            "ksc_bench_avoid_overlapping_writes={}\n"
        ),
        config.client_id,
        config.endpoints.join(","),
        stats_publisher.runtime_dir.display(),
        match config.transfer_mode {
            TransferMode::Single => "single",
            TransferMode::Packed => "packed",
        },
        config.workers,
        config.inflight_streams_per_worker,
        config.target_initial_inflight,
        config.target_min_inflight,
        config.pack_max_payload_bytes,
        config.duration.as_secs(),
        config.write_percent,
        config.packed_count,
        config.key_count,
        config.avoid_overlapping_writes,
    );

    let result = async {
        stats.set_phase("connect");
        let targets = Arc::new(resolve_targets(&config, &stats).await?);
        let keyspace = Arc::new(build_keyspace(&config, &targets)?);

        stats.set_phase("prefill");
        prefill_keyspace(&targets, &config, &keyspace, &stats).await?;

        stats.set_phase("benchmark");
        let started = Instant::now();
        let deadline = started + config.duration;
        let mut join_set = JoinSet::new();
        for worker_index in 0..config.workers {
            let worker_config = config.clone();
            let worker_targets = Arc::clone(&targets);
            let worker_keyspace = Arc::clone(&keyspace);
            let worker_stats = Arc::clone(&stats);
            join_set.spawn(async move {
                run_worker(
                    worker_index,
                    worker_config,
                    worker_targets,
                    worker_keyspace,
                    worker_stats,
                    deadline,
                )
                .await
            });
        }

        let mut aggregate = WorkerTotals::default();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(worker_totals)) => aggregate.accumulate(&worker_totals),
                Ok(Err(err)) => {
                    stats.record_error(format!("KSC worker failed: {}", err));
                    return Err(err);
                }
                Err(err) => {
                    let message = format!("KSC worker task join failed: {}", err);
                    stats.record_error(message.clone());
                    return Err(ClientError::Transport(message));
                }
            }
        }
        let elapsed = started.elapsed();

        if config.cleanup {
            stats.set_phase("cleanup");
            cleanup_keyspace(&targets, &keyspace, &stats).await?;
        }
        stats.set_phase("complete");

        let snapshot = stats.snapshot();
        Ok((aggregate, elapsed, snapshot))
    }
    .await;

    stop_stats_publisher(&mut stats_publisher);

    let (aggregate, elapsed, snapshot) = result?;
    println!(
        concat!(
            "ksc_bench_elapsed_ms={}\n",
            "ksc_bench_pack_ops={}\n",
            "ksc_bench_pack_ops_s={:.2}\n",
            "ksc_bench_chunk_ops={}\n",
            "ksc_bench_chunk_ops_s={:.2}\n",
            "ksc_bench_read_packs={}\n",
            "ksc_bench_write_packs={}\n",
            "ksc_bench_read_chunks={}\n",
            "ksc_bench_write_chunks={}\n",
            "ksc_bench_read_MiB_s={:.2}\n",
            "ksc_bench_write_MiB_s={:.2}\n",
            "ksc_bench_rate_limit_events={}\n",
            "ksc_bench_total_errors={}\n",
            "ksc_bench_read_p50_us={}\n",
            "ksc_bench_read_p95_us={}\n",
            "ksc_bench_read_p99_us={}\n",
            "ksc_bench_read_phase_send_body_avg_us={}\n",
            "ksc_bench_read_phase_wait_response_avg_us={}\n",
            "ksc_bench_read_phase_collect_response_avg_us={}\n",
            "ksc_bench_read_phase_payload_validate_avg_us={}\n",
            "ksc_bench_write_p50_us={}\n",
            "ksc_bench_write_p95_us={}\n",
            "ksc_bench_write_p99_us={}\n",
            "ksc_bench_write_phase_send_body_avg_us={}\n",
            "ksc_bench_write_phase_wait_response_avg_us={}\n",
            "ksc_bench_write_phase_collect_response_avg_us={}\n",
            "ksc_bench_result=ok\n"
        ),
        elapsed.as_millis(),
        aggregate.total_packs(),
        aggregate.total_packs() as f64 / elapsed.as_secs_f64(),
        aggregate.total_chunks(),
        aggregate.total_chunks() as f64 / elapsed.as_secs_f64(),
        aggregate.read_packs,
        aggregate.write_packs,
        aggregate.read_chunks,
        aggregate.write_chunks,
        aggregate.read_payload_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        aggregate.write_payload_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        snapshot.summary.rate_limit_events,
        snapshot.summary.total_errors,
        snapshot.summary.read_latency.p50_us,
        snapshot.summary.read_latency.p95_us,
        snapshot.summary.read_latency.p99_us,
        snapshot.summary.read_phases.send_body.avg_us,
        snapshot.summary.read_phases.wait_response.avg_us,
        snapshot.summary.read_phases.collect_response.avg_us,
        snapshot.summary.read_phases.payload_validate.avg_us,
        snapshot.summary.write_latency.p50_us,
        snapshot.summary.write_latency.p95_us,
        snapshot.summary.write_latency.p99_us,
        snapshot.summary.write_phases.send_body.avg_us,
        snapshot.summary.write_phases.wait_response.avg_us,
        snapshot.summary.write_phases.collect_response.avg_us,
    );
    Ok(())
}

#[derive(Default)]
struct WorkerTotals {
    read_packs: u64,
    write_packs: u64,
    read_chunks: u64,
    write_chunks: u64,
    read_payload_bytes: u64,
    write_payload_bytes: u64,
}

impl WorkerTotals {
    fn accumulate(&mut self, other: &Self) {
        self.read_packs += other.read_packs;
        self.write_packs += other.write_packs;
        self.read_chunks += other.read_chunks;
        self.write_chunks += other.write_chunks;
        self.read_payload_bytes += other.read_payload_bytes;
        self.write_payload_bytes += other.write_payload_bytes;
    }

    fn total_packs(&self) -> u64 {
        self.read_packs + self.write_packs
    }

    fn total_chunks(&self) -> u64 {
        self.read_chunks + self.write_chunks
    }
}

#[derive(Clone)]
struct BenchTarget {
    index: usize,
    endpoint: String,
    info: TargetInfo,
    pacer: Arc<TargetPacer>,
}

struct TargetPacer {
    min_inflight: usize,
    max_inflight: usize,
    additive_increase_every: usize,
    desired_inflight: AtomicUsize,
    inflight: AtomicUsize,
    successes_since_increase: AtomicUsize,
    backoff_until_ms: AtomicU64,
}

impl TargetPacer {
    fn new(
        initial_inflight: usize,
        min_inflight: usize,
        max_inflight: usize,
        additive_increase_every: usize,
    ) -> Self {
        Self {
            min_inflight,
            max_inflight: max_inflight.max(min_inflight),
            additive_increase_every,
            desired_inflight: AtomicUsize::new(initial_inflight.max(min_inflight)),
            inflight: AtomicUsize::new(0),
            successes_since_increase: AtomicUsize::new(0),
            backoff_until_ms: AtomicU64::new(0),
        }
    }

    fn try_acquire(self: &Arc<Self>) -> Option<TargetPermit> {
        if now_ms() < self.backoff_until_ms.load(Ordering::Relaxed) {
            return None;
        }
        loop {
            let desired = self
                .desired_inflight
                .load(Ordering::Relaxed)
                .clamp(self.min_inflight, self.max_inflight);
            let inflight = self.inflight.load(Ordering::Relaxed);
            if inflight >= desired {
                return None;
            }
            if self
                .inflight
                .compare_exchange(inflight, inflight + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(TargetPermit {
                    pacer: Arc::clone(self),
                });
            }
        }
    }

    fn note_success(&self) {
        let successes = self
            .successes_since_increase
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let desired = self.desired_inflight.load(Ordering::Relaxed);
        let inflight = self.inflight.load(Ordering::Relaxed);
        if inflight >= desired
            && desired < self.max_inflight
            && successes >= self.additive_increase_every
            && self
                .successes_since_increase
                .compare_exchange(successes, 0, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            let _ = self.desired_inflight.compare_exchange(
                desired,
                desired + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
        }
    }

    fn note_rate_limit(&self, retry_after_ms: Option<u64>, header_max_inflight: Option<usize>) {
        self.successes_since_increase.store(0, Ordering::Relaxed);
        let current = self.desired_inflight.load(Ordering::Relaxed);
        let halved = current.saturating_sub((current / 2).max(1));
        let hinted = header_max_inflight.unwrap_or(current);
        let next = halved.min(hinted).max(self.min_inflight);
        self.desired_inflight.store(next, Ordering::Relaxed);
        if let Some(retry_after_ms) = retry_after_ms {
            self.backoff_until_ms
                .store(now_ms().saturating_add(retry_after_ms), Ordering::Relaxed);
        }
    }
}

struct TargetPermit {
    pacer: Arc<TargetPacer>,
}

impl Drop for TargetPermit {
    fn drop(&mut self) {
        self.pacer.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

struct KeyEntry {
    chunk_id: kp2::ChunkId,
    slot_index: u64,
    generation: AtomicU32,
    write_inflight: AtomicBool,
}

struct KeyWritePermit {
    key: Arc<KeyEntry>,
}

impl Drop for KeyWritePermit {
    fn drop(&mut self) {
        self.key.write_inflight.store(false, Ordering::Release);
    }
}

struct Keyspace {
    keys: Vec<Arc<KeyEntry>>,
    by_target: Vec<Vec<usize>>,
}

async fn run_worker(
    worker_index: usize,
    config: BenchmarkConfig,
    targets: Arc<Vec<BenchTarget>>,
    keyspace: Arc<Keyspace>,
    stats: Arc<KscRuntimeStats>,
    deadline: Instant,
) -> Result<WorkerTotals, ClientError> {
    stats.begin_worker();
    let mut sessions = Vec::with_capacity(targets.len());
    for target in targets.iter() {
        sessions.push(Arc::new(
            connect_with_stats(&target.endpoint, &stats).await?,
        ));
    }
    let mut rng = config.chunk_seed ^ ((worker_index as u64 + 1) * 0x9e37_79b9_7f4a_7c15);
    let mut totals = WorkerTotals::default();
    let mut join_set = JoinSet::new();

    loop {
        while join_set.len() < config.inflight_streams_per_worker && Instant::now() < deadline {
            let is_write = random_percent(&mut rng) < config.write_percent as u64;
            let Some((target_index, target_permit)) =
                select_target_with_permit(&targets, &keyspace, &mut rng)
            else {
                yield_now().await;
                break;
            };
            let op_seed =
                splitmix64(rng ^ (join_set.len() as u64).wrapping_mul(0x94d0_49bb_1331_11eb));
            let session = Arc::clone(&sessions[target_index]);
            let worker_config = config.clone();
            let worker_target = targets[target_index].clone();
            let worker_keyspace = Arc::clone(&keyspace);
            join_set.spawn(async move {
                execute_operation(
                    session,
                    worker_config,
                    worker_target,
                    worker_keyspace,
                    op_seed,
                    is_write,
                    target_permit,
                )
                .await
            });
        }

        let Some(result) = join_set.join_next().await else {
            break;
        };
        match result {
            Ok(Ok(OperationOutcome::Read {
                chunks,
                payload_bytes,
                latency,
                phases,
            })) => {
                stats.record_read(chunks, payload_bytes, latency, &phases);
                totals.read_packs += 1;
                totals.read_chunks += chunks as u64;
                totals.read_payload_bytes += payload_bytes as u64;
            }
            Ok(Ok(OperationOutcome::Write {
                chunks,
                payload_bytes,
                latency,
                phases,
            })) => {
                stats.record_write(chunks, payload_bytes, latency, &phases);
                totals.write_packs += 1;
                totals.write_chunks += chunks as u64;
                totals.write_payload_bytes += payload_bytes as u64;
            }
            Ok(Ok(OperationOutcome::RateLimited { class })) => {
                stats.record_rate_limit(class.as_deref());
            }
            Ok(Ok(OperationOutcome::Skipped)) => {}
            Ok(Err(err)) => {
                stats.record_error(format!("KSC worker error: {}", err));
                stats.finish_worker();
                for _ in sessions {
                    stats.record_connection_closed();
                }
                return Err(err);
            }
            Err(err) => {
                let message = format!("KSC worker operation join failed: {}", err);
                stats.record_error(message.clone());
                stats.finish_worker();
                for _ in sessions {
                    stats.record_connection_closed();
                }
                return Err(ClientError::Transport(message));
            }
        }
    }

    for _ in sessions {
        stats.record_connection_closed();
    }
    stats.finish_worker();
    Ok(totals)
}

enum OperationOutcome {
    Read {
        chunks: usize,
        payload_bytes: usize,
        latency: Duration,
        phases: RequestPhaseTimes,
    },
    Write {
        chunks: usize,
        payload_bytes: usize,
        latency: Duration,
        phases: RequestPhaseTimes,
    },
    RateLimited {
        class: Option<String>,
    },
    Skipped,
}

async fn execute_operation(
    session: Arc<TargetSession>,
    config: BenchmarkConfig,
    target: BenchTarget,
    keyspace: Arc<Keyspace>,
    mut rng: u64,
    is_write: bool,
    _target_permit: TargetPermit,
) -> Result<OperationOutcome, ClientError> {
    let started = Instant::now();
    if is_write {
        match issue_write_pack(session.as_ref(), &config, &target, &keyspace, &mut rng).await {
            Ok(Some((chunks, payload_bytes, phases))) => {
                target.pacer.note_success();
                Ok(OperationOutcome::Write {
                    chunks,
                    payload_bytes,
                    latency: started.elapsed(),
                    phases,
                })
            }
            Ok(None) => Ok(OperationOutcome::Skipped),
            Err(err) if err.is_rate_limited() => {
                target
                    .pacer
                    .note_rate_limit(err.retry_after_ms(), err.limit_max_inflight());
                Ok(OperationOutcome::RateLimited {
                    class: err.rate_limit_class(),
                })
            }
            Err(err) => Err(err),
        }
    } else {
        match issue_read_pack(session.as_ref(), &config, &target, &keyspace, &mut rng).await {
            Ok(Some((chunks, payload_bytes, phases))) => {
                target.pacer.note_success();
                Ok(OperationOutcome::Read {
                    chunks,
                    payload_bytes,
                    latency: started.elapsed(),
                    phases,
                })
            }
            Ok(None) => Ok(OperationOutcome::Skipped),
            Err(err) if err.is_rate_limited() => {
                target
                    .pacer
                    .note_rate_limit(err.retry_after_ms(), err.limit_max_inflight());
                Ok(OperationOutcome::RateLimited {
                    class: err.rate_limit_class(),
                })
            }
            Err(err) => Err(err),
        }
    }
}

async fn issue_write_pack(
    session: &TargetSession,
    config: &BenchmarkConfig,
    target: &BenchTarget,
    keyspace: &Keyspace,
    rng: &mut u64,
) -> Result<Option<(usize, usize, RequestPhaseTimes)>, ClientError> {
    if config.transfer_mode == TransferMode::Single {
        let Some((key, _permit)) =
            select_single_write_key(keyspace, target.index, config.avoid_overlapping_writes, rng)
        else {
            return Ok(None);
        };
        let generation = key.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let payload_len = payload_len_for_slot(&target.info, key.slot_index)
            .map_err(|err| ClientError::Protocol(err.to_string()))?;
        let payload = synthetic_payload(key.chunk_id, key.slot_index, generation, payload_len);
        let payload_bytes = payload.len();
        let phases = session
            .write_chunk(key.chunk_id, key.slot_index, generation, WriteIdentity::default(), payload)
            .await?;
        return Ok(Some((1, payload_bytes, phases)));
    }

    let Some(selected) = select_write_batch(keyspace, config, target, rng)? else {
        return Ok(None);
    };
    let mut entries = Vec::with_capacity(selected.entries.len());
    let mut payload_bytes = 0_usize;
    for selected_entry in &selected.entries {
        let generation = selected_entry
            .key
            .generation
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let payload = synthetic_payload(
            selected_entry.key.chunk_id,
            selected_entry.key.slot_index,
            generation,
            selected_entry.payload_len,
        );
        payload_bytes += payload.len();
        entries.push(PackedWriteEntry {
            chunk_id: selected_entry.key.chunk_id,
            slot_index: selected_entry.key.slot_index,
            generation,
            identity: WriteIdentity::default(),
            payload,
        });
    }
    let entry_count = entries.len();
    let timed_reply = session
        .packed_write(PackedWriteRequest { entries })
        .await?;
    validate_write_reply(&timed_reply.value, entry_count)?;
    Ok(Some((entry_count, payload_bytes, timed_reply.phases)))
}

async fn issue_read_pack(
    session: &TargetSession,
    config: &BenchmarkConfig,
    target: &BenchTarget,
    keyspace: &Keyspace,
    rng: &mut u64,
) -> Result<Option<(usize, usize, RequestPhaseTimes)>, ClientError> {
    if config.transfer_mode == TransferMode::Single {
        let Some(key) = select_single_read_key(keyspace, target.index, rng) else {
            return Ok(None);
        };
        let payload_len = payload_len_for_slot(&target.info, key.slot_index)
            .map_err(|err| ClientError::Protocol(err.to_string()))?;
        let mut timed_payload = session.read_chunk(key.chunk_id).await?;
        let expected_payload = synthetic_payload(
            key.chunk_id,
            timed_payload.value.slot_index,
            timed_payload.value.generation,
            payload_len,
        );
        let validate_started = Instant::now();
        if timed_payload.value.payload != expected_payload {
            return Err(ClientError::Protocol(format!(
                "KSC single read payload mismatch for chunk {}: logical_slot={} returned_slot={} returned_generation={}",
                hex::encode(key.chunk_id.0),
                key.slot_index,
                timed_payload.value.slot_index,
                timed_payload.value.generation,
            )));
        }
        timed_payload
            .phases
            .add_payload_validate(validate_started.elapsed());
        return Ok(Some((
            1,
            timed_payload.value.payload.len(),
            timed_payload.phases,
        )));
    }

    let Some(selected) = select_read_batch(keyspace, config, target, rng)? else {
        return Ok(None);
    };
    let chunk_ids = selected
        .keys
        .iter()
        .map(|key| key.chunk_id)
        .collect::<Vec<_>>();
    let mut timed_reply = session
        .packed_read(&PackedReadQuery { chunk_ids, ranges: None }, selected.payload_bytes)
        .await?;
    if timed_reply.value.entries.len() != selected.keys.len() {
        return Err(ClientError::Protocol(
            "KSC packed read response entry count does not match the request".to_string(),
        ));
    }

    let mut payload_bytes = 0_usize;
    let validate_started = Instant::now();
    for entry in &timed_reply.value.entries {
        if entry.status_code != 200 {
            return Err(ClientError::Protocol(format!(
                "KSC packed read returned status {} for chunk {}",
                entry.status_code,
                hex::encode(entry.chunk_id.0)
            )));
        }
        let Some(location) = &entry.location else {
            return Err(ClientError::Protocol(
                "KSC packed read omitted the location record for a successful chunk".to_string(),
            ));
        };
        let payload_len = payload_len_for_slot(&target.info, location.slot_index)
            .map_err(|err| ClientError::Protocol(err.to_string()))?;
        let expected_payload = synthetic_payload(
            entry.chunk_id,
            location.slot_index,
            location.generation,
            payload_len,
        );
        if entry.payload != expected_payload {
            return Err(ClientError::Protocol(format!(
                "KSC packed read payload mismatch for chunk {}",
                hex::encode(entry.chunk_id.0)
            )));
        }
        payload_bytes += entry.payload.len();
    }
    timed_reply
        .phases
        .add_payload_validate(validate_started.elapsed());
    Ok(Some((
        timed_reply.value.entries.len(),
        payload_bytes,
        timed_reply.phases,
    )))
}

async fn prefill_keyspace(
    targets: &[BenchTarget],
    config: &BenchmarkConfig,
    keyspace: &Keyspace,
    stats: &KscRuntimeStats,
) -> Result<(), ClientError> {
    for target in targets {
        if keyspace.by_target[target.index].is_empty() {
            continue;
        }
        let session = connect_with_stats(&target.endpoint, stats).await?;
        if config.transfer_mode == TransferMode::Single {
            for key_index in &keyspace.by_target[target.index] {
                let key = &keyspace.keys[*key_index];
                let started = Instant::now();
                let generation = key.generation.load(Ordering::Relaxed);
                let payload_len = payload_len_for_slot(&target.info, key.slot_index)
                    .map_err(|err| ClientError::Protocol(err.to_string()))?;
                let payload =
                    synthetic_payload(key.chunk_id, key.slot_index, generation, payload_len);
                let payload_bytes = payload.len();
                let phases = session
                    .write_chunk(key.chunk_id, key.slot_index, generation, WriteIdentity::default(), payload)
                    .await?;
                stats.record_write(1, payload_bytes, started.elapsed(), &phases);
            }
        } else {
            let mut cursor = 0_usize;
            while cursor < keyspace.by_target[target.index].len() {
                let (batch, next_cursor, payload_bytes) =
                    build_prefill_batch(keyspace, target, &config, cursor)?;
                let started = Instant::now();
                let mut entries = Vec::with_capacity(batch.len());
                for key in &batch {
                    let generation = key.generation.load(Ordering::Relaxed);
                    let payload_len = payload_len_for_slot(&target.info, key.slot_index)
                        .map_err(|err| ClientError::Protocol(err.to_string()))?;
                    let payload =
                        synthetic_payload(key.chunk_id, key.slot_index, generation, payload_len);
                    entries.push(PackedWriteEntry {
                        chunk_id: key.chunk_id,
                        slot_index: key.slot_index,
                        generation,
                        identity: WriteIdentity::default(),
                        payload,
                    });
                }
                let timed_reply = session
                    .packed_write(PackedWriteRequest { entries })
                    .await?;
                validate_write_reply(&timed_reply.value, batch.len())?;
                stats.record_write(
                    batch.len(),
                    payload_bytes,
                    started.elapsed(),
                    &timed_reply.phases,
                );
                cursor = next_cursor;
            }
        }
        stats.record_connection_closed();
    }
    Ok(())
}

async fn cleanup_keyspace(
    targets: &[BenchTarget],
    keyspace: &Keyspace,
    stats: &KscRuntimeStats,
) -> Result<(), ClientError> {
    for target in targets {
        if keyspace.by_target[target.index].is_empty() {
            continue;
        }
        let session = connect_with_stats(&target.endpoint, stats).await?;
        for key_index in &keyspace.by_target[target.index] {
            let key = &keyspace.keys[*key_index];
            let started = Instant::now();
            let phases = session.delete_chunk(key.chunk_id).await?;
            stats.record_delete(1, started.elapsed(), &phases);
        }
        stats.record_connection_closed();
    }
    Ok(())
}

async fn resolve_targets(
    config: &BenchmarkConfig,
    stats: &KscRuntimeStats,
) -> Result<Vec<BenchTarget>, ClientError> {
    let mut targets = Vec::with_capacity(config.endpoints.len());
    for (index, endpoint) in config.endpoints.iter().enumerate() {
        let session = connect_with_stats(endpoint, stats).await?;
        let info = match session.info().await {
            Ok(info) => info,
            Err(err) => {
                stats.record_connection_closed();
                return Err(err);
            }
        };
        stats.record_connection_closed();
        targets.push(BenchTarget {
            index,
            endpoint: endpoint.clone(),
            info,
            pacer: Arc::new(TargetPacer::new(
                config.target_initial_inflight,
                config.target_min_inflight,
                config.target_initial_inflight.saturating_mul(2),
                config.target_additive_increase_every,
            )),
        });
    }
    if targets.is_empty() {
        return Err(ClientError::Transport(
            "KSC benchmark requires at least one target endpoint".to_string(),
        ));
    }
    Ok(targets)
}

fn build_keyspace(
    config: &BenchmarkConfig,
    targets: &[BenchTarget],
) -> Result<Keyspace, ClientError> {
    let mut keys = Vec::with_capacity(config.key_count);
    let mut by_target = vec![Vec::new(); targets.len()];
    let mut local_slots = vec![config.slot_base; targets.len()];
    for index in 0..config.key_count {
        let target_index = index % targets.len();
        let slot_index = local_slots[target_index];
        local_slots[target_index] = local_slots[target_index].saturating_add(1);
        let chunk_id = chunk_id_from_seed(config.chunk_seed + index as u64);
        let _payload_len = payload_len_for_slot(&targets[target_index].info, slot_index)
            .map_err(|err| ClientError::Protocol(err.to_string()))?;
        let key = Arc::new(KeyEntry {
            chunk_id,
            slot_index,
            generation: AtomicU32::new(config.generation_start),
            write_inflight: AtomicBool::new(false),
        });
        let key_index = keys.len();
        keys.push(key);
        by_target[target_index].push(key_index);
    }
    Ok(Keyspace { keys, by_target })
}

fn build_prefill_batch(
    keyspace: &Keyspace,
    target: &BenchTarget,
    config: &BenchmarkConfig,
    cursor: usize,
) -> Result<(Vec<Arc<KeyEntry>>, usize, usize), ClientError> {
    let bucket = &keyspace.by_target[target.index];
    if cursor >= bucket.len() {
        return Ok((Vec::new(), cursor, 0));
    }
    let payload_limit = target
        .info
        .packed_payload_limit(config.pack_max_payload_bytes);
    let wire_limit = target.info.packed_write_body_limit();
    let mut next_cursor = cursor;
    let mut payload_bytes = 0_usize;
    let mut batch = Vec::with_capacity(config.packed_count);
    while next_cursor < bucket.len() && batch.len() < config.packed_count {
        let key = Arc::clone(&keyspace.keys[bucket[next_cursor]]);
        let payload_len = payload_len_for_slot(&target.info, key.slot_index)
            .map_err(|err| ClientError::Protocol(err.to_string()))?;
        if batch.is_empty() && payload_len > payload_limit {
            return Err(ClientError::Protocol(format!(
                "KSC packed payload limit {} is smaller than one target payload of {} bytes for {}",
                payload_limit, payload_len, target.endpoint
            )));
        }
        let next_payload_bytes = payload_bytes + payload_len;
        if next_payload_bytes > payload_limit {
            break;
        }
        let next_wire_bytes = encoded_write_request_len(batch.len() + 1, next_payload_bytes)
            .map_err(|err| ClientError::Protocol(err.to_string()))?;
        if next_wire_bytes > wire_limit {
            break;
        }
        payload_bytes += payload_len;
        batch.push(key);
        next_cursor += 1;
    }
    Ok((batch, next_cursor, payload_bytes))
}

fn select_target_with_permit(
    targets: &[BenchTarget],
    keyspace: &Keyspace,
    rng: &mut u64,
) -> Option<(usize, TargetPermit)> {
    if targets.is_empty() {
        return None;
    }
    let start = random_index(rng, targets.len());
    for offset in 0..targets.len() {
        let index = (start + offset) % targets.len();
        if keyspace.by_target[index].is_empty() {
            continue;
        }
        if let Some(permit) = targets[index].pacer.try_acquire() {
            return Some((index, permit));
        }
    }
    None
}

fn select_single_read_key(
    keyspace: &Keyspace,
    target_index: usize,
    rng: &mut u64,
) -> Option<Arc<KeyEntry>> {
    let bucket = keyspace.by_target.get(target_index)?;
    if bucket.is_empty() {
        return None;
    }
    let key_index = bucket[random_index(rng, bucket.len())];
    Some(Arc::clone(&keyspace.keys[key_index]))
}

fn select_single_write_key(
    keyspace: &Keyspace,
    target_index: usize,
    avoid_overlapping_writes: bool,
    rng: &mut u64,
) -> Option<(Arc<KeyEntry>, Option<KeyWritePermit>)> {
    let bucket = keyspace.by_target.get(target_index)?;
    if bucket.is_empty() {
        return None;
    }
    let start = random_index(rng, bucket.len());
    for offset in 0..bucket.len() {
        let key = Arc::clone(&keyspace.keys[bucket[(start + offset) % bucket.len()]]);
        if !avoid_overlapping_writes {
            return Some((key, None));
        }
        if key
            .write_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            let permit = KeyWritePermit {
                key: Arc::clone(&key),
            };
            return Some((key, Some(permit)));
        }
    }
    None
}

struct SelectedWriteEntry {
    key: Arc<KeyEntry>,
    payload_len: usize,
    _permit: Option<KeyWritePermit>,
}

struct SelectedWriteBatch {
    entries: Vec<SelectedWriteEntry>,
}

fn select_write_batch(
    keyspace: &Keyspace,
    config: &BenchmarkConfig,
    target: &BenchTarget,
    rng: &mut u64,
) -> Result<Option<SelectedWriteBatch>, ClientError> {
    let bucket = keyspace
        .by_target
        .get(target.index)
        .ok_or_else(|| ClientError::Protocol("KSC target index is out of range".to_string()))?;
    if bucket.is_empty() {
        return Ok(None);
    }
    let payload_limit = target
        .info
        .packed_payload_limit(config.pack_max_payload_bytes);
    let wire_limit = target.info.packed_write_body_limit();
    let start = random_index(rng, bucket.len());
    let mut entries = Vec::with_capacity(config.packed_count);
    let mut payload_bytes = 0_usize;
    for offset in 0..bucket.len() {
        if entries.len() >= config.packed_count {
            break;
        }
        let key = Arc::clone(&keyspace.keys[bucket[(start + offset) % bucket.len()]]);
        let payload_len = payload_len_for_slot(&target.info, key.slot_index)
            .map_err(|err| ClientError::Protocol(err.to_string()))?;
        if entries.is_empty() && payload_len > payload_limit {
            return Err(ClientError::Protocol(format!(
                "KSC packed payload limit {} is smaller than one target payload of {} bytes for {}",
                payload_limit, payload_len, target.endpoint
            )));
        }
        let next_payload_bytes = payload_bytes + payload_len;
        if next_payload_bytes > payload_limit {
            break;
        }
        let next_wire_bytes = encoded_write_request_len(entries.len() + 1, next_payload_bytes)
            .map_err(|err| ClientError::Protocol(err.to_string()))?;
        if next_wire_bytes > wire_limit {
            break;
        }
        let permit = if config.avoid_overlapping_writes {
            match key.write_inflight.compare_exchange(
                false,
                true,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => Some(KeyWritePermit {
                    key: Arc::clone(&key),
                }),
                Err(_) => continue,
            }
        } else {
            None
        };
        payload_bytes = next_payload_bytes;
        entries.push(SelectedWriteEntry {
            key,
            payload_len,
            _permit: permit,
        });
    }
    if entries.is_empty() {
        return Ok(None);
    }
    Ok(Some(SelectedWriteBatch { entries }))
}

struct SelectedReadBatch {
    keys: Vec<Arc<KeyEntry>>,
    payload_bytes: usize,
}

fn select_read_batch(
    keyspace: &Keyspace,
    config: &BenchmarkConfig,
    target: &BenchTarget,
    rng: &mut u64,
) -> Result<Option<SelectedReadBatch>, ClientError> {
    let bucket = keyspace
        .by_target
        .get(target.index)
        .ok_or_else(|| ClientError::Protocol("KSC target index is out of range".to_string()))?;
    if bucket.is_empty() {
        return Ok(None);
    }
    let payload_limit = target
        .info
        .packed_payload_limit(config.pack_max_payload_bytes);
    let start = random_index(rng, bucket.len());
    let mut keys = Vec::with_capacity(config.packed_count);
    let mut payload_bytes = 0_usize;
    for offset in 0..bucket.len() {
        if keys.len() >= config.packed_count {
            break;
        }
        let key = Arc::clone(&keyspace.keys[bucket[(start + offset) % bucket.len()]]);
        let payload_len = payload_len_for_slot(&target.info, key.slot_index)
            .map_err(|err| ClientError::Protocol(err.to_string()))?;
        if keys.is_empty() && payload_len > payload_limit {
            return Err(ClientError::Protocol(format!(
                "KSC packed payload limit {} is smaller than one target payload of {} bytes for {}",
                payload_limit, payload_len, target.endpoint
            )));
        }
        if payload_bytes + payload_len > payload_limit {
            break;
        }
        payload_bytes += payload_len;
        keys.push(key);
    }
    if keys.is_empty() {
        return Ok(None);
    }
    Ok(Some(SelectedReadBatch {
        keys,
        payload_bytes,
    }))
}

async fn connect_with_stats(
    endpoint: &str,
    stats: &KscRuntimeStats,
) -> Result<TargetSession, ClientError> {
    match TargetSession::connect(endpoint).await {
        Ok(session) => {
            stats.record_connection_opened();
            Ok(session)
        }
        Err(err) => {
            stats.record_connection_failure(format!("KSC connection failure: {}", err));
            Err(err)
        }
    }
}

fn validate_write_reply(
    reply: &PackedWriteReply,
    expected_count: usize,
) -> Result<(), ClientError> {
    if reply.entries.len() != expected_count {
        return Err(ClientError::Protocol(
            "KSC packed write reply did not return the expected entry count".to_string(),
        ));
    }
    for entry in &reply.entries {
        if !entry.success() {
            return Err(ClientError::Protocol(format!(
                "KSC packed write entry {} at slot {} generation {} failed: {}",
                hex::encode(entry.chunk_id.0),
                entry.slot_index,
                entry.requested_generation,
                entry.error.clone().unwrap_or_default()
            )));
        }
    }
    Ok(())
}

fn random_percent(state: &mut u64) -> u64 {
    *state = crate::client::splitmix64(*state);
    *state % 100
}

fn random_index(state: &mut u64, len: usize) -> usize {
    *state = crate::client::splitmix64(*state);
    (*state as usize) % len
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn stop_stats_publisher(publisher: &mut KscStatsPublisher) {
    if let Some(stop_tx) = publisher.stop_tx.take() {
        let _ = stop_tx.send(());
    }
    if let Some(join) = publisher.join.take() {
        let _ = join.join();
    }
}
