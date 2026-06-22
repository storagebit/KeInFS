// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::{BenchConfig, RecordMix};
use crate::ingress::IngressRuntime;
use crate::media::{media_checksum, MediaReadRequest, MediaStore, MediaWriteRequest};
use crate::topology::TopologyPlan;
use kix::{
    chunk_media_record_for_key, device_size_bytes, planned_chunk_media_span_bytes, ArenaIoMode,
    ChunkId, ChunkMediaLayoutKind, ChunkMediaLayoutSpec, DriveConfig, KixClient, LocationRecord,
};
use std::error::Error;
use std::io;
use std::sync::Arc;
use std::time::Instant;

const KIX_ARENA_ALIGN_BYTES: u64 = 4096;
const KIX_ARENA_FILE_HEADER_BYTES: u64 = KIX_ARENA_ALIGN_BYTES;
const KIX_ARENA_FRAME_HEADER_BYTES: u64 = KIX_ARENA_ALIGN_BYTES;
const KIX_ARENA_DELTA_BATCH_COUNT_BYTES: u64 = 4;
const KIX_ARENA_DELTA_ENTRY_BYTES: u64 = 64;
const KIX_ARENA_CHECKPOINT_ENTRY_BYTES: u64 = 32 + 28;
const KIX_ARENA_PLANNING_BATCH_SIZE: u64 = 2;
const KIX_ARENA_IDEAL_BATCH_SIZE: u64 = 64;

#[derive(Default)]
pub(crate) struct WorkerResult {
    pub(crate) total_ops: usize,
    pub(crate) read_ops: usize,
    pub(crate) write_ops: usize,
    pub(crate) media_read_ops: usize,
    pub(crate) media_write_ops: usize,
    pub(crate) read_logical_bytes: u64,
    pub(crate) read_stored_bytes: u64,
    pub(crate) write_logical_bytes: u64,
    pub(crate) write_stored_bytes: u64,
    pub(crate) read_samples: Vec<u64>,
    pub(crate) read_lookup_samples: Vec<u64>,
    pub(crate) read_media_samples: Vec<u64>,
    pub(crate) write_samples: Vec<u64>,
    pub(crate) write_media_samples: Vec<u64>,
    pub(crate) write_commit_samples: Vec<u64>,
    pub(crate) errors: Vec<String>,
}

#[derive(Clone, Copy)]
pub(crate) enum BenchRequest {
    Read {
        chunk_id: ChunkId,
    },
    Write {
        chunk_id: ChunkId,
        record: LocationRecord,
    },
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RequestKind {
    Read,
    Write,
}

#[derive(Clone, Copy)]
struct PendingRequest {
    request: BenchRequest,
    sample: bool,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct OperationMetrics {
    pub(crate) lookup_us: Option<u64>,
    pub(crate) media_read_us: Option<u64>,
    pub(crate) media_write_us: Option<u64>,
    pub(crate) kix_commit_us: Option<u64>,
    pub(crate) media_read_logical_bytes: u64,
    pub(crate) media_read_stored_bytes: u64,
    pub(crate) media_write_logical_bytes: u64,
    pub(crate) media_write_stored_bytes: u64,
}

impl BenchRequest {
    fn kind(self) -> RequestKind {
        match self {
            Self::Read { .. } => RequestKind::Read,
            Self::Write { .. } => RequestKind::Write,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ArenaBudgetEstimate {
    pub(crate) slice_bytes: u64,
    pub(crate) prefill_write_ops: u64,
    pub(crate) measured_write_ops: u64,
    pub(crate) total_write_ops: u64,
    pub(crate) live_entries: u64,
    pub(crate) checkpoint_bytes: u64,
    pub(crate) planning_batch_size: u64,
    pub(crate) planning_delta_bytes: u64,
    pub(crate) recommended_bytes: u64,
    pub(crate) worst_case_bytes: u64,
    pub(crate) ideal_bytes: u64,
}

impl ArenaBudgetEstimate {
    pub(crate) fn is_undersized(self) -> bool {
        self.slice_bytes < self.recommended_bytes
    }

    pub(crate) fn should_warn(self) -> bool {
        !self.is_undersized() && self.slice_bytes < self.worst_case_bytes
    }

    pub(crate) fn rejection_message(self) -> String {
        format!(
            concat!(
                "KIX raw arena slice {} is too small for this benchmark workload. ",
                "The current planning floor is {} based on {} prefill writes, {} measured writes, ",
                "a pessimistic effective append batch size of {}, and {} live entries for the final checkpoint. ",
                "Worst-case no-batching consumption is {} and the ideal fully-batched floor would be {}. ",
                "Increase --raw-slice-bytes or reduce --ops-per-thread, --write-percent, or --prefill-keys."
            ),
            format_binary_bytes(self.slice_bytes),
            format_binary_bytes(self.recommended_bytes),
            self.prefill_write_ops,
            self.measured_write_ops,
            self.planning_batch_size,
            self.live_entries,
            format_binary_bytes(self.worst_case_bytes),
            format_binary_bytes(self.ideal_bytes),
        )
    }

    pub(crate) fn warning_message(self) -> String {
        format!(
            concat!(
                "KIX raw arena slice {} clears the planning floor {} but remains below the no-batching ceiling {}. ",
                "If append batching collapses under load, this run can still exhaust the span. ",
                "Current workload model: {} prefill writes, {} measured writes, {} live entries."
            ),
            format_binary_bytes(self.slice_bytes),
            format_binary_bytes(self.recommended_bytes),
            format_binary_bytes(self.worst_case_bytes),
            self.prefill_write_ops,
            self.measured_write_ops,
            self.live_entries,
        )
    }
}

pub(crate) fn run_worker(
    seed: u64,
    client: Arc<KixClient>,
    config: BenchConfig,
    media_store: Option<Arc<MediaStore>>,
    ingress: Option<Arc<IngressRuntime>>,
) -> WorkerResult {
    let mut rng = seed ^ 0x9e37_79b9_7f4a_7c15;
    let mut result = WorkerResult::default();
    let mut pending: Vec<PendingRequest> = Vec::with_capacity(config.media_queue_depth.max(1));

    for op_idx in 0..config.ops_per_thread {
        rng = splitmix64(rng);
        let key_seed = rng % config.key_space;
        let chunk_id = ChunkId::from_seed(key_seed);
        let drive_id = (key_seed as usize % config.drives) as u16;
        let sample = op_idx % 64 == 0;
        let request = if (rng % 100) < config.write_percent as u64 {
            let record = make_record(&config, drive_id, key_seed, (op_idx as u32).wrapping_add(1));
            BenchRequest::Write { chunk_id, record }
        } else {
            BenchRequest::Read { chunk_id }
        };

        if !pending.is_empty()
            && (pending[0].request.kind() != request.kind()
                || pending.len() >= batch_limit_for_kind(request.kind(), &config))
        {
            if let Err(err) = flush_pending_requests(
                seed,
                client.as_ref(),
                media_store.as_deref(),
                ingress.as_deref(),
                &mut pending,
                &mut result,
            ) {
                result.errors.push(err);
                break;
            }
        }
        pending.push(PendingRequest { request, sample });
    }

    if result.errors.is_empty() && !pending.is_empty() {
        if let Err(err) = flush_pending_requests(
            seed,
            client.as_ref(),
            media_store.as_deref(),
            ingress.as_deref(),
            &mut pending,
            &mut result,
        ) {
            result.errors.push(err);
        }
    }

    result
}

pub(crate) fn prefill_working_set(
    client: &Arc<KixClient>,
    config: &BenchConfig,
    media_store: Option<&Arc<MediaStore>>,
) -> Result<(), Box<dyn Error>> {
    for key_seed in 0..config.prefill_keys {
        let drive_id = (key_seed as usize % config.drives) as u16;
        let record = make_record(config, drive_id, key_seed, 1);
        execute_request(
            client.as_ref(),
            media_store.map(Arc::as_ref),
            BenchRequest::Write {
                chunk_id: ChunkId::from_seed(key_seed),
                record,
            },
        )
        .map_err(io::Error::other)?;
    }
    Ok(())
}

fn batch_limit_for_kind(kind: RequestKind, config: &BenchConfig) -> usize {
    match kind {
        RequestKind::Read => config.media_read_batch_size.max(1),
        RequestKind::Write => config.media_write_batch_size.max(1),
    }
}

fn flush_pending_requests(
    seed: u64,
    client: &KixClient,
    media_store: Option<&MediaStore>,
    ingress: Option<&IngressRuntime>,
    pending: &mut Vec<PendingRequest>,
    result: &mut WorkerResult,
) -> Result<(), String> {
    if pending.is_empty() {
        return Ok(());
    }

    let requests = pending.iter().map(|item| item.request).collect::<Vec<_>>();
    let t0 = Instant::now();
    let batch_result = match ingress {
        Some(ingress) => ingress.submit_batch(requests.clone()),
        None => execute_request_batch(client, media_store, &requests),
    };
    let elapsed_us = t0.elapsed().as_micros() as u64;
    let metrics = batch_result.map_err(|err| {
        let first = pending[0].request;
        match first {
            BenchRequest::Read { chunk_id } => {
                format!(
                    "worker {seed} could not execute read batch starting at {chunk_id:?}: {err}"
                )
            }
            BenchRequest::Write { chunk_id, .. } => format!(
                "worker {seed} could not execute write batch starting at {chunk_id:?}: {err}"
            ),
        }
    })?;

    for ((pending_request, metrics), request) in pending
        .iter()
        .zip(metrics.into_iter())
        .zip(requests.into_iter())
    {
        match request {
            BenchRequest::Write { .. } => {
                result.write_ops += 1;
                if metrics.media_write_stored_bytes > 0 {
                    result.media_write_ops += 1;
                    result.write_logical_bytes += metrics.media_write_logical_bytes;
                    result.write_stored_bytes += metrics.media_write_stored_bytes;
                }
                if pending_request.sample {
                    if let Some(media_us) = metrics.media_write_us {
                        result.write_media_samples.push(media_us);
                    }
                    if let Some(commit_us) = metrics.kix_commit_us {
                        result.write_commit_samples.push(commit_us);
                    }
                    let phased = metrics
                        .media_write_us
                        .unwrap_or(0)
                        .saturating_add(metrics.kix_commit_us.unwrap_or(0));
                    result
                        .write_samples
                        .push(if phased > 0 { phased } else { elapsed_us });
                }
            }
            BenchRequest::Read { .. } => {
                result.read_ops += 1;
                if metrics.media_read_stored_bytes > 0 {
                    result.media_read_ops += 1;
                    result.read_logical_bytes += metrics.media_read_logical_bytes;
                    result.read_stored_bytes += metrics.media_read_stored_bytes;
                }
                if pending_request.sample {
                    if let Some(lookup_us) = metrics.lookup_us {
                        result.read_lookup_samples.push(lookup_us);
                    }
                    if let Some(media_us) = metrics.media_read_us {
                        result.read_media_samples.push(media_us);
                    }
                    let phased = metrics
                        .lookup_us
                        .unwrap_or(0)
                        .saturating_add(metrics.media_read_us.unwrap_or(0));
                    result
                        .read_samples
                        .push(if phased > 0 { phased } else { elapsed_us });
                }
            }
        }
        result.total_ops += 1;
    }
    pending.clear();
    Ok(())
}

pub(crate) fn execute_request(
    client: &KixClient,
    media_store: Option<&MediaStore>,
    request: BenchRequest,
) -> Result<OperationMetrics, String> {
    execute_request_batch(client, media_store, &[request])?
        .into_iter()
        .next()
        .ok_or_else(|| "KIX batch execution completed without returning metrics".to_string())
}

pub(crate) fn execute_request_batch(
    client: &KixClient,
    media_store: Option<&MediaStore>,
    requests: &[BenchRequest],
) -> Result<Vec<OperationMetrics>, String> {
    let mut metrics = vec![OperationMetrics::default(); requests.len()];
    let mut read_requests = Vec::new();
    let mut write_requests = Vec::new();

    for (index, request) in requests.iter().copied().enumerate() {
        match request {
            BenchRequest::Read { chunk_id } => {
                let t0 = Instant::now();
                let location = client
                    .get(chunk_id)
                    .map_err(|err| format!("could not resolve chunk {:?}: {}", chunk_id, err))?;
                metrics[index].lookup_us = Some(t0.elapsed().as_micros() as u64);
                if let Some(record) = location {
                    if media_store.is_some() {
                        metrics[index].media_read_logical_bytes = record.logical_length as u64;
                        metrics[index].media_read_stored_bytes = record.stored_length as u64;
                        read_requests.push(MediaReadRequest {
                            request_index: index,
                            record,
                        });
                    }
                }
            }
            BenchRequest::Write { chunk_id, record } => {
                if media_store.is_some() {
                    metrics[index].media_write_logical_bytes = record.logical_length as u64;
                    metrics[index].media_write_stored_bytes = record.stored_length as u64;
                    write_requests.push(MediaWriteRequest {
                        request_index: index,
                        chunk_id,
                        record,
                    });
                }
            }
        }
    }

    if let Some(media_store) = media_store {
        if !read_requests.is_empty() {
            for (request_index, latency_us) in
                media_store
                    .read_and_validate_batch(&read_requests)
                    .map_err(|err| format!("could not execute batched media reads: {err}"))?
            {
                metrics[request_index].media_read_us = Some(latency_us);
            }
        }
        if !write_requests.is_empty() {
            for (request_index, latency_us) in media_store
                .materialize_batch(&write_requests)
                .map_err(|err| format!("could not execute batched media writes: {err}"))?
            {
                metrics[request_index].media_write_us = Some(latency_us);
            }
        }
    }

    for (index, request) in requests.iter().copied().enumerate() {
        if let BenchRequest::Write { chunk_id, record } = request {
            let t0 = Instant::now();
            client.upsert(chunk_id, index as u64, record).map_err(|err| {
                format!(
                    "could not upsert chunk {:?} on drive {}: {}",
                    chunk_id, record.drive_id, err
                )
            })?;
            metrics[index].kix_commit_us = Some(t0.elapsed().as_micros() as u64);
        }
    }

    Ok(metrics)
}

pub(crate) fn make_record(
    config: &BenchConfig,
    drive_id: u16,
    key_seed: u64,
    generation: u32,
) -> LocationRecord {
    let chunk_id = ChunkId::from_seed(key_seed);
    let mut record =
        chunk_media_record_for_key(&chunk_media_layout(config), drive_id, key_seed, generation)
            .expect("KIX workload record planning must fit the configured chunk-media layout");
    record.checksum = media_checksum(&chunk_id, &record);
    record
}

pub(crate) fn build_drive_configs(
    config: &BenchConfig,
    topology: &TopologyPlan,
) -> Result<Vec<DriveConfig>, Box<dyn Error>> {
    let raw_device = config
        .raw_device
        .as_ref()
        .ok_or("KIX benchmarking requires --raw-device <block-device>")?;
    let total_bytes = device_size_bytes(raw_device)?;
    let remaining_bytes = total_bytes
        .checked_sub(config.raw_offset_bytes)
        .ok_or("raw offset exceeds raw device size")?;
    let slice_bytes = config.raw_slice_bytes.unwrap_or(remaining_bytes);
    if slice_bytes == 0 {
        return Err("raw slice bytes must be > 0".into());
    }
    let total_needed = config
        .raw_offset_bytes
        .checked_add(slice_bytes)
        .ok_or("raw slice layout overflow")?;
    if total_needed > total_bytes {
        return Err("raw slice layout exceeds raw device capacity".into());
    }

    Ok(vec![DriveConfig {
        id: 0,
        arena_path: raw_device.clone(),
        arena_offset_bytes: config.raw_offset_bytes,
        arena_len_bytes: Some(slice_bytes),
        numa_node: topology.raw_device_numa_node,
        io_mode: ArenaIoMode::DirectUring,
    }])
}

pub(crate) fn estimate_raw_arena_budget(
    config: &BenchConfig,
    drive_configs: &[DriveConfig],
) -> Result<Option<ArenaBudgetEstimate>, Box<dyn Error>> {
    if config.raw_device.is_none() {
        return Ok(None);
    }
    let drive = drive_configs
        .first()
        .ok_or("raw arena budgeting requires exactly one drive config")?;
    let slice_bytes = drive
        .arena_len_bytes
        .ok_or("raw arena budgeting requires a fixed raw arena span")?;

    let measured_write_ops = measured_write_ops(config)?;
    let total_write_ops = config
        .prefill_keys
        .checked_add(measured_write_ops)
        .ok_or("raw arena budgeting overflowed the planned write count")?;
    let live_entries = if measured_write_ops > 0 {
        config.prefill_keys.max(config.key_space)
    } else {
        config.prefill_keys
    };

    let checkpoint_bytes = if config.checkpoint_at_end {
        frame_bytes_for_payload(
            KIX_ARENA_DELTA_BATCH_COUNT_BYTES
                .checked_add(
                    live_entries
                        .checked_mul(KIX_ARENA_CHECKPOINT_ENTRY_BYTES)
                        .ok_or("raw arena budgeting overflowed the checkpoint payload size")?,
                )
                .ok_or("raw arena budgeting overflowed the checkpoint payload size")?,
        )?
    } else {
        0
    };

    let planning_delta_bytes = delta_log_bytes(total_write_ops, KIX_ARENA_PLANNING_BATCH_SIZE)?;
    let worst_case_delta_bytes = delta_log_bytes(total_write_ops, 1)?;
    let ideal_delta_bytes = delta_log_bytes(total_write_ops, KIX_ARENA_IDEAL_BATCH_SIZE)?;

    let recommended_bytes = KIX_ARENA_FILE_HEADER_BYTES
        .checked_add(planning_delta_bytes)
        .and_then(|value| value.checked_add(checkpoint_bytes))
        .ok_or("raw arena budgeting overflowed the recommended arena size")?;
    let worst_case_bytes = KIX_ARENA_FILE_HEADER_BYTES
        .checked_add(worst_case_delta_bytes)
        .and_then(|value| value.checked_add(checkpoint_bytes))
        .ok_or("raw arena budgeting overflowed the worst-case arena size")?;
    let ideal_bytes = KIX_ARENA_FILE_HEADER_BYTES
        .checked_add(ideal_delta_bytes)
        .and_then(|value| value.checked_add(checkpoint_bytes))
        .ok_or("raw arena budgeting overflowed the ideal arena size")?;

    Ok(Some(ArenaBudgetEstimate {
        slice_bytes,
        prefill_write_ops: config.prefill_keys,
        measured_write_ops,
        total_write_ops,
        live_entries,
        checkpoint_bytes,
        planning_batch_size: KIX_ARENA_PLANNING_BATCH_SIZE,
        planning_delta_bytes,
        recommended_bytes,
        worst_case_bytes,
        ideal_bytes,
    }))
}

pub(crate) fn planned_media_span_bytes(config: &BenchConfig) -> u64 {
    planned_chunk_media_span_bytes(&chunk_media_layout(config))
        .expect("KIX media planning must fit inside the configured chunk-media layout")
}

pub(crate) fn chunk_media_layout(config: &BenchConfig) -> ChunkMediaLayoutSpec {
    ChunkMediaLayoutSpec {
        layout_kind: match config.record_mix {
            RecordMix::Mixed => ChunkMediaLayoutKind::Mixed,
            RecordMix::PackedOnly => ChunkMediaLayoutKind::PackedOnly,
            RecordMix::ExtentOnly => ChunkMediaLayoutKind::ExtentOnly,
        },
        extent_bytes: config.extent_bytes,
        packed_bytes: config.packed_bytes,
        key_slots: config.prefill_keys.max(config.key_space),
    }
}

pub(crate) fn print_latency_summary(label: &str, samples: &mut [u64]) {
    if samples.is_empty() {
        println!("{label}_samples=0");
        return;
    }
    samples.sort_unstable();
    let p50 = percentile(samples, 0.50);
    let p95 = percentile(samples, 0.95);
    let p99 = percentile(samples, 0.99);
    println!("{label}_samples={}", samples.len());
    println!("{label}_p50_us={p50}");
    println!("{label}_p95_us={p95}");
    println!("{label}_p99_us={p99}");
}

fn percentile(values: &[u64], fraction: f64) -> u64 {
    let idx = ((values.len() - 1) as f64 * fraction) as usize;
    values[idx]
}

fn measured_write_ops(config: &BenchConfig) -> Result<u64, Box<dyn Error>> {
    let total_ops = (config.ops_per_thread as u128)
        .checked_mul(config.threads as u128)
        .ok_or("raw arena budgeting overflowed the total op count")?;
    let measured_writes = total_ops
        .checked_mul(config.write_percent as u128)
        .and_then(|value| value.checked_add(99))
        .ok_or("raw arena budgeting overflowed the write-op estimate")?
        / 100;
    measured_writes
        .try_into()
        .map_err(|_| "raw arena budgeting overflowed the measured write count".into())
}

fn delta_log_bytes(total_write_ops: u64, batch_size: u64) -> Result<u64, Box<dyn Error>> {
    if total_write_ops == 0 {
        return Ok(0);
    }
    let frame_payload = KIX_ARENA_DELTA_BATCH_COUNT_BYTES
        .checked_add(
            batch_size
                .checked_mul(KIX_ARENA_DELTA_ENTRY_BYTES)
                .ok_or("raw arena budgeting overflowed the delta payload size")?,
        )
        .ok_or("raw arena budgeting overflowed the delta payload size")?;
    let frame_bytes = frame_bytes_for_payload(frame_payload)?;
    let batch_count = total_write_ops.div_ceil(batch_size);
    batch_count
        .checked_mul(frame_bytes)
        .ok_or_else(|| "raw arena budgeting overflowed the delta-log span".into())
}

fn frame_bytes_for_payload(payload_bytes: u64) -> Result<u64, Box<dyn Error>> {
    let payload_disk_len = align_up_u64(payload_bytes, KIX_ARENA_ALIGN_BYTES)?;
    KIX_ARENA_FRAME_HEADER_BYTES
        .checked_add(payload_disk_len)
        .ok_or_else(|| "raw arena budgeting overflowed the frame size".into())
}

fn align_up_u64(value: u64, align: u64) -> Result<u64, Box<dyn Error>> {
    if align == 0 {
        return Err("raw arena budgeting requires a non-zero alignment".into());
    }
    let remainder = value % align;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(align - remainder)
            .ok_or_else(|| "raw arena budgeting overflowed the aligned size".into())
    }
}

fn format_binary_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * KIB;
    const GIB: f64 = 1024.0 * MIB;
    let bytes_f = bytes as f64;
    if bytes_f >= GIB {
        format!("{bytes} B ({:.2} GiB)", bytes_f / GIB)
    } else if bytes_f >= MIB {
        format!("{bytes} B ({:.2} MiB)", bytes_f / MIB)
    } else if bytes_f >= KIB {
        format!("{bytes} B ({:.2} KiB)", bytes_f / KIB)
    } else {
        format!("{bytes} B")
    }
}

pub(crate) fn average_bytes_per_op(total_bytes: u64, op_count: u64) -> u64 {
    if op_count == 0 {
        0
    } else {
        total_bytes / op_count
    }
}

pub(crate) fn bytes_per_second(total_bytes: u64, elapsed_seconds: f64) -> u64 {
    if elapsed_seconds <= 0.0 {
        0
    } else {
        (total_bytes as f64 / elapsed_seconds) as u64
    }
}

pub(crate) fn mib_per_second(total_bytes: u64, elapsed_seconds: f64) -> f64 {
    if elapsed_seconds <= 0.0 {
        0.0
    } else {
        total_bytes as f64 / elapsed_seconds / (1024.0 * 1024.0)
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kix::ArenaIoMode;
    use std::path::PathBuf;

    fn raw_drive_config(slice_bytes: u64) -> DriveConfig {
        DriveConfig {
            id: 0,
            arena_path: PathBuf::from("/dev/null"),
            arena_offset_bytes: 0,
            arena_len_bytes: Some(slice_bytes),
            numa_node: None,
            io_mode: ArenaIoMode::DirectUring,
        }
    }

    #[test]
    fn write_heavy_one_gib_span_is_rejected_by_planning_floor() {
        let mut config = BenchConfig::default();
        config.raw_device = Some(PathBuf::from("/dev/null"));
        config.raw_slice_bytes = Some(1 << 30);
        config.ops_per_thread = 25_000;
        config.threads = 16;
        config.write_percent = 100;
        config.key_space = 20_000;

        let budget = estimate_raw_arena_budget(&config, &[raw_drive_config(1 << 30)])
            .unwrap()
            .unwrap();

        assert!(budget.is_undersized());
        assert!(budget.recommended_bytes > budget.slice_bytes);
    }

    #[test]
    fn mixed_one_gib_span_warns_but_is_not_rejected() {
        let mut config = BenchConfig::default();
        config.raw_device = Some(PathBuf::from("/dev/null"));
        config.raw_slice_bytes = Some(1 << 30);
        config.ops_per_thread = 25_000;
        config.threads = 16;
        config.write_percent = 30;
        config.prefill_keys = 20_000;
        config.key_space = 20_000;

        let budget = estimate_raw_arena_budget(&config, &[raw_drive_config(1 << 30)])
            .unwrap()
            .unwrap();

        assert!(!budget.is_undersized());
        assert!(budget.should_warn());
    }

    #[test]
    fn default_extent_only_record_shape_is_one_mib() {
        let config = BenchConfig::default();
        let first = make_record(&config, 0, 0, 1);
        let second = make_record(&config, 0, 1, 1);

        assert_eq!(first.logical_length, 1024 * 1024);
        assert_eq!(first.stored_length, 1024 * 1024);
        assert_eq!(first.physical_offset, 8_192);
        assert_eq!(second.logical_length, 1024 * 1024);
        assert_eq!(second.stored_length, 1024 * 1024);
        assert_eq!(second.physical_offset, 8_192 + 1_048_576_u64 + 4_096_u64);
    }

    #[test]
    fn mixed_record_shape_can_model_extent_and_packed_paths_explicitly() {
        let mut config = BenchConfig::default();
        config.record_mix = RecordMix::Mixed;
        config.extent_bytes = 1_048_576;
        config.packed_bytes = 16_384;

        let first = make_record(&config, 0, 0, 1);
        let second = make_record(&config, 0, 1, 1);

        assert_eq!(first.logical_length, 1_048_576);
        assert_eq!(first.stored_length, 1_048_576);
        assert_eq!(first.physical_offset, 8_192);
        assert_eq!(second.logical_length, 16_384);
        assert_eq!(second.stored_length, 16_384);
        assert_eq!(second.physical_offset, 8_192 + 1_048_576_u64 + 4_096_u64);
    }
}
