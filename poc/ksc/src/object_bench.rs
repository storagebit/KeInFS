// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::{
    CompletionMode as CliCompletionMode, ObjectBenchmarkConfig, ObjectBenchmarkKeyShape,
    ObjectBenchmarkRunMode,
};
use crate::object_cli::print_phase_line;
use ksc::client::CompletionMode as ClientCompletionMode;
use ksc::object::{ObjectClient, ObjectClientOptions, ObjectPhaseTimes};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

/// Live, lock-free counters updated by every worker during the measured phase
/// so the CLIENT side of a load test is observable WHILE it runs. Previously the
/// benchmark emitted nothing until the final stdout summary — during a long
/// sustained run (e.g. a multi-TiB fill) there was no way to see client-side
/// ops/s, bytes, or error count without waiting for the end. A background
/// reporter snapshots these on an interval to stdout and (optionally) a JSON
/// file. Default off (interval 0) preserves prior behavior exactly.
#[derive(Default)]
struct BenchLiveStats {
    write_ops: AtomicU64,
    write_bytes: AtomicU64,
    read_ops: AtomicU64,
    read_bytes: AtomicU64,
    errors: AtomicU64,
    last_write_us: AtomicU64,
    last_read_us: AtomicU64,
}

impl BenchLiveStats {
    fn record_write(&self, bytes: u64, elapsed: Duration) {
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        self.write_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.last_write_us
            .store(elapsed.as_micros().min(u64::MAX as u128) as u64, Ordering::Relaxed);
    }
    fn record_read(&self, bytes: u64, elapsed: Duration) {
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.last_read_us
            .store(elapsed.as_micros().min(u64::MAX as u128) as u64, Ordering::Relaxed);
    }
    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    fn snapshot(&self) -> (u64, u64, u64, u64, u64, u64, u64) {
        (
            self.write_ops.load(Ordering::Relaxed),
            self.write_bytes.load(Ordering::Relaxed),
            self.read_ops.load(Ordering::Relaxed),
            self.read_bytes.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
            self.last_write_us.load(Ordering::Relaxed),
            self.last_read_us.load(Ordering::Relaxed),
        )
    }
}

#[derive(Clone, Copy, Debug)]
enum OpKind {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, Default)]
struct PhaseTotals {
    kms_initiate_us: u128,
    kms_commit_us: u128,
    kms_resolve_us: u128,
    ec_encode_us: u128,
    ec_reconstruct_us: u128,
    target_connect_us: u128,
    target_write_us: u128,
    target_read_us: u128,
    target_ready_wait_us: u128,
    target_request_prepare_us: u128,
    target_send_headers_us: u128,
    target_send_body_us: u128,
    target_wait_response_us: u128,
    target_collect_response_us: u128,
    target_protocol_decode_us: u128,
    target_payload_validate_us: u128,
}

#[derive(Clone, Debug, Default)]
struct BenchTotals {
    ops: usize,
    bytes: u64,
    errors: Vec<String>,
    latencies_us: Vec<u128>,
    phases: PhaseTotals,
    data_fragment_reads: usize,
    parity_fragment_reads: usize,
    reconstruct_invocations: usize,
    fast_path_reads: usize,
    degraded_reads: usize,
}

#[derive(Clone, Debug, Default)]
struct WorkerTotals {
    reads: BenchTotals,
    writes: BenchTotals,
}

pub(crate) async fn run_object_benchmark(
    config: ObjectBenchmarkConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = std::fs::read(&config.input_path)?;
    let run_stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let run_prefix = format!("{}/run-{run_stamp}", config.key_prefix);
    let prefill_keys = if config.existing_read_keys.is_empty() {
        build_prefill_keys(&run_prefix, config.prefill_keys, config.key_shape)
    } else {
        config.existing_read_keys.clone()
    };

    let prefill_started = Instant::now();
    let client_options = ObjectClientOptions {
        read_completion_mode: client_mode(config.read_completion_mode),
        write_completion_mode: client_mode(config.write_completion_mode),
        read_resolve_cache_ttl: config.read_resolve_cache_ttl,
        write_window_max_stripes: config.write_window_max_stripes,
        write_window_inflight_stripes: config.write_window_inflight_stripes,
        kms_grpc_max_message_bytes: config.kms_grpc_max_message_bytes,
        metadata_notification_nats_url: config.metadata_notification_nats_url.clone(),
        metadata_notification_subject: config.metadata_notification_subject.clone(),
        ..ObjectClientOptions::default()
    };
    // Prefill the read keys FIRST so the per-endpoint warm reads (and the
    // worker read mix) resolve against objects that already have a committed
    // head. Warming before prefill read read_keys[0] before it was ever
    // written, which aborted the run with "no committed current version".
    let mut prefill_client = Some(
        ObjectClient::connect_with_options(&config.kms_endpoints, client_options.clone()).await?,
    );
    if config.write_percent < 100 && config.existing_read_keys.is_empty() {
        for key in &prefill_keys {
            prefill_client
                .as_mut()
                .ok_or_else(|| std::io::Error::other("prefill client missing"))?
                .put_object_single_stripe(&config.bucket_id, key, &payload)
                .await?;
        }
    }
    // Prime every configured KMS endpoint instead of only warming whichever
    // endpoint the first ObjectClient happens to choose. Runs after prefill so
    // the warm read of read_keys[0] hits a committed object.
    for warm_index in 0..config.kms_endpoints.len().max(1) {
        let mut warm_client =
            ObjectClient::connect_with_options(&config.kms_endpoints, client_options.clone())
                .await?;
        let warm_prefix = format!("{run_prefix}/__kmswarm{warm_index}");
        warm_benchmark_prefixes(
            &mut warm_client,
            &config,
            &warm_prefix,
            &prefill_keys,
            &payload,
        )
        .await?;
    }
    let prefill_elapsed = prefill_started.elapsed();

    let bench_barrier = matches!(config.run_mode, ObjectBenchmarkRunMode::Timed { .. })
        .then(|| Arc::new(Barrier::new(config.workers + 1)));
    let live_stats = Arc::new(BenchLiveStats::default());
    let mut joins = JoinSet::new();
    for worker_index in 0..config.workers {
        let worker_config = config.clone();
        let worker_payload = payload.clone();
        let worker_run_prefix = run_prefix.clone();
        let worker_prefill_keys = prefill_keys.clone();
        let worker_barrier = bench_barrier.clone();
        let worker_live_stats = live_stats.clone();
        joins.spawn(async move {
            run_worker(
                worker_index,
                worker_config,
                worker_run_prefix,
                worker_prefill_keys,
                worker_payload,
                worker_barrier,
                worker_live_stats,
            )
            .await
        });
    }
    if let Some(barrier) = bench_barrier {
        barrier.wait().await;
    }
    let bench_started = Instant::now();

    // Live progress reporter: snapshots the shared counters every interval to
    // stdout + an optional JSON file, so a long sustained run is observable
    // client-side while it runs. Cancelled as soon as the workers finish.
    let progress_handle = (!config.progress_interval.is_zero()).then(|| {
        let reporter_stats = live_stats.clone();
        let interval = config.progress_interval;
        let stats_root = config.stats_root.clone();
        let started = bench_started;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                emit_progress(&reporter_stats, started, stats_root.as_deref());
            }
        })
    });

    let mut totals = WorkerTotals::default();
    while let Some(result) = joins.join_next().await {
        let worker = result.map_err(|err| {
            std::io::Error::other(format!("object-benchmark worker join failure: {err}"))
        })?;
        let worker = worker.map_err(|err| std::io::Error::other(err.to_string()))?;
        merge_bench_totals(&mut totals.reads, worker.reads);
        merge_bench_totals(&mut totals.writes, worker.writes);
    }
    let bench_elapsed = bench_started.elapsed();
    if let Some(handle) = progress_handle {
        handle.abort();
        // One final snapshot so the JSON/stdout progress reflects the full run.
        emit_progress(&live_stats, bench_started, config.stats_root.as_deref());
    }

    let total_ops = totals.reads.ops + totals.writes.ops;
    let total_bytes = totals.reads.bytes + totals.writes.bytes;
    let throughput_mib_s = mib_per_second(total_bytes, bench_elapsed);
    let (run_mode_label, ops_per_worker_label, warmup_ms_label) = match config.run_mode {
        ObjectBenchmarkRunMode::FixedOps { ops_per_worker } => ("fixed-ops", ops_per_worker, 0),
        ObjectBenchmarkRunMode::Timed { warmup, .. } => ("timed", 0, warmup.as_millis() as u64),
    };
    println!(
        "ksc_object_benchmark mode={} run_mode={} workers={} ops_per_worker={} warmup_ms={} total_ops={} logical_bytes={} throughput_mib_s={:.2} duration_ms={} errors={} read_key_source={} prefill_keys={} prefill_ms={} write_key_count={} key_prefix={} key_shape={} read_completion_mode={} write_completion_mode={} read_resolve_cache_ttl_ms={}",
        mode_label(config.write_percent),
        run_mode_label,
        config.workers,
        ops_per_worker_label,
        warmup_ms_label,
        total_ops,
        total_bytes,
        throughput_mib_s,
        bench_elapsed.as_millis(),
        totals.reads.errors.len() + totals.writes.errors.len(),
        if config.existing_read_keys.is_empty() {
            "prefill"
        } else {
            "existing"
        },
        prefill_keys.len(),
        prefill_elapsed.as_millis(),
        config.write_key_count.unwrap_or(0),
        run_prefix,
        key_shape_label(config.key_shape),
        config.read_completion_mode.as_str(),
        config.write_completion_mode.as_str(),
        config.read_resolve_cache_ttl.as_millis(),
    );

    if totals.writes.ops > 0 {
        print_kind_summary("write", &totals.writes, bench_elapsed);
    }
    if totals.reads.ops > 0 {
        print_kind_summary("read", &totals.reads, bench_elapsed);
    }

    if !totals.writes.errors.is_empty() || !totals.reads.errors.is_empty() {
        for error in totals
            .writes
            .errors
            .iter()
            .chain(totals.reads.errors.iter())
            .take(8)
        {
            println!("ksc_object_benchmark_error {error}");
        }
        return Err(format!(
            "object benchmark hit {} write errors and {} read errors",
            totals.writes.errors.len(),
            totals.reads.errors.len()
        )
        .into());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_worker(
    worker_index: usize,
    config: ObjectBenchmarkConfig,
    run_prefix: String,
    prefill_keys: Vec<String>,
    payload: Vec<u8>,
    bench_barrier: Option<Arc<Barrier>>,
    live_stats: Arc<BenchLiveStats>,
) -> Result<WorkerTotals, Box<dyn std::error::Error + Send + Sync>> {
    let mut totals = WorkerTotals::default();
    let mut client = ObjectClient::connect_with_options(
        &config.kms_endpoints,
        ObjectClientOptions {
            read_completion_mode: client_mode(config.read_completion_mode),
            write_completion_mode: client_mode(config.write_completion_mode),
            read_resolve_cache_ttl: config.read_resolve_cache_ttl,
            write_window_max_stripes: config.write_window_max_stripes,
            write_window_inflight_stripes: config.write_window_inflight_stripes,
            kms_grpc_max_message_bytes: config.kms_grpc_max_message_bytes,
            metadata_notification_nats_url: config.metadata_notification_nats_url.clone(),
            metadata_notification_subject: config.metadata_notification_subject.clone(),
            ..ObjectClientOptions::default()
        },
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;
    let mut op_index = 0_usize;
    match config.run_mode {
        ObjectBenchmarkRunMode::FixedOps { ops_per_worker } => {
            for _ in 0..ops_per_worker {
                run_one_op(
                    &mut client,
                    &config,
                    &run_prefix,
                    &prefill_keys,
                    &payload,
                    worker_index,
                    op_index,
                    &mut totals,
                    Some(&live_stats),
                )
                .await;
                op_index += 1;
            }
        }
        ObjectBenchmarkRunMode::Timed { warmup, duration } => {
            if !warmup.is_zero() {
                let mut warmup_totals = WorkerTotals::default();
                let warmup_deadline = Instant::now() + warmup;
                while Instant::now() < warmup_deadline {
                    // Warmup ops are excluded from the measured live counters.
                    run_one_op(
                        &mut client,
                        &config,
                        &run_prefix,
                        &prefill_keys,
                        &payload,
                        worker_index,
                        op_index,
                        &mut warmup_totals,
                        None,
                    )
                    .await;
                    op_index += 1;
                }
            }
            if let Some(barrier) = bench_barrier {
                barrier.wait().await;
            }
            let measured_deadline = Instant::now() + duration;
            while Instant::now() < measured_deadline {
                run_one_op(
                    &mut client,
                    &config,
                    &run_prefix,
                    &prefill_keys,
                    &payload,
                    worker_index,
                    op_index,
                    &mut totals,
                    Some(&live_stats),
                )
                .await;
                op_index += 1;
            }
        }
    }
    Ok(totals)
}

#[allow(clippy::too_many_arguments)]
async fn run_one_op(
    client: &mut ObjectClient,
    config: &ObjectBenchmarkConfig,
    run_prefix: &str,
    prefill_keys: &[String],
    payload: &[u8],
    worker_index: usize,
    op_index: usize,
    totals: &mut WorkerTotals,
    live_stats: Option<&BenchLiveStats>,
) {
    let op_kind = choose_kind(config.write_percent, worker_index, op_index);
    match op_kind {
        OpKind::Write => {
            let key = write_key(
                run_prefix,
                config.key_shape,
                config.write_key_count,
                worker_index,
                op_index,
            );
            let started = Instant::now();
            match client
                .put_object_single_stripe(&config.bucket_id, &key, payload)
                .await
            {
                Ok(result) => {
                    let elapsed = started.elapsed();
                    record_success(&mut totals.writes, payload.len() as u64, elapsed, result.phases);
                    if let Some(s) = live_stats {
                        s.record_write(payload.len() as u64, elapsed);
                    }
                }
                Err(err) => {
                    if let Some(s) = live_stats {
                        s.record_error();
                    }
                    totals.writes.errors.push(format!(
                        "worker={} op={} kind=write key={} err={}",
                        worker_index, op_index, key, err
                    ));
                }
            }
        }
        OpKind::Read => {
            let key = prefill_keys[(worker_index * 65_537 + op_index) % prefill_keys.len()].clone();
            let started = Instant::now();
            match client
                .get_object_single_stripe(&config.bucket_id, &key)
                .await
            {
                Ok(result) => {
                    if config.verify_reads && result.payload != payload {
                        if let Some(s) = live_stats {
                            s.record_error();
                        }
                        totals.reads.errors.push(format!(
                            "worker={} op={} kind=read key={} err=payload-mismatch",
                            worker_index, op_index, key
                        ));
                        return;
                    }
                    let elapsed = started.elapsed();
                    record_success(
                        &mut totals.reads,
                        result.payload.len() as u64,
                        elapsed,
                        result.phases,
                    );
                    if let Some(s) = live_stats {
                        s.record_read(result.payload.len() as u64, elapsed);
                    }
                    totals.reads.data_fragment_reads += result.data_fragment_reads;
                    totals.reads.parity_fragment_reads += result.parity_fragment_reads;
                    if result.reconstructed {
                        totals.reads.reconstruct_invocations += 1;
                        totals.reads.degraded_reads += 1;
                    } else {
                        totals.reads.fast_path_reads += 1;
                    }
                }
                Err(err) => {
                    if let Some(s) = live_stats {
                        s.record_error();
                    }
                    totals.reads.errors.push(format!(
                        "worker={} op={} kind=read key={} err={}",
                        worker_index, op_index, key, err
                    ));
                }
            }
        }
    }
}

/// Snapshot the shared live counters to stdout and, if a stats root is set, to
/// `<stats_root>/object-benchmark.json` (atomic write via tmp+rename). Mirrors
/// the daemon stats trees so a load test's client side is observable live.
fn emit_progress(stats: &BenchLiveStats, started: Instant, stats_root: Option<&std::path::Path>) {
    let elapsed = started.elapsed();
    let secs = elapsed.as_secs_f64().max(1e-6);
    let (wops, wbytes, rops, rbytes, errors, last_w_us, last_r_us) = stats.snapshot();
    let total_bytes = wbytes + rbytes;
    let mib_s = (total_bytes as f64) / (1024.0 * 1024.0) / secs;
    let ops_s = ((wops + rops) as f64) / secs;
    println!(
        "ksc_object_benchmark_progress elapsed_s={:.1} write_ops={} read_ops={} logical_bytes={} throughput_mib_s={:.2} ops_s={:.0} errors={} last_write_us={} last_read_us={}",
        secs, wops, rops, total_bytes, mib_s, ops_s, errors, last_w_us, last_r_us
    );
    if let Some(root) = stats_root {
        let json = format!(
            "{{\"elapsed_s\":{:.1},\"write_ops\":{},\"write_bytes\":{},\"read_ops\":{},\"read_bytes\":{},\"logical_bytes\":{},\"throughput_mib_s\":{:.2},\"ops_s\":{:.0},\"errors\":{},\"last_write_us\":{},\"last_read_us\":{}}}\n",
            secs, wops, wbytes, rops, rbytes, total_bytes, mib_s, ops_s, errors, last_w_us, last_r_us
        );
        let _ = std::fs::create_dir_all(root);
        let final_path = root.join("object-benchmark.json");
        let tmp_path = root.join("object-benchmark.json.tmp");
        if std::fs::write(&tmp_path, json.as_bytes()).is_ok() {
            let _ = std::fs::rename(&tmp_path, &final_path);
        }
    }
}

fn choose_kind(write_percent: u8, worker_index: usize, op_index: usize) -> OpKind {
    if write_percent == 100 {
        return OpKind::Write;
    }
    if write_percent == 0 {
        return OpKind::Read;
    }
    let ticket = (worker_index * 65_537 + op_index) % 100;
    if ticket < write_percent as usize {
        OpKind::Write
    } else {
        OpKind::Read
    }
}

fn build_prefill_keys(
    run_prefix: &str,
    count: usize,
    key_shape: ObjectBenchmarkKeyShape,
) -> Vec<String> {
    (0..count)
        .map(|index| match key_shape {
            ObjectBenchmarkKeyShape::FlatRoot => format!("{run_prefix}/k{index:04}.bin"),
            ObjectBenchmarkKeyShape::WarmTree => format!(
                "{run_prefix}/tree/read/dataset-{}/year-{}/month-{}/k{index:04}.bin",
                index % 8,
                2026 + (index % 2),
                (index % 12) + 1
            ),
        })
        .collect()
}

fn write_key(
    run_prefix: &str,
    key_shape: ObjectBenchmarkKeyShape,
    write_key_count: Option<usize>,
    worker_index: usize,
    op_index: usize,
) -> String {
    if let Some(write_key_count) = write_key_count {
        let slot = (worker_index * 65_537 + op_index) % write_key_count;
        return match key_shape {
            ObjectBenchmarkKeyShape::FlatRoot => {
                format!("{run_prefix}/write-set/k{slot:05}.bin")
            }
            ObjectBenchmarkKeyShape::WarmTree => format!(
                "{run_prefix}/tree/write-set/project-{}/bucket-{}/k{slot:05}.bin",
                slot % 8,
                slot % 64
            ),
        };
    }
    match key_shape {
        ObjectBenchmarkKeyShape::FlatRoot => {
            format!("{run_prefix}/w{worker_index:02}-o{op_index:04}.bin")
        }
        ObjectBenchmarkKeyShape::WarmTree => format!(
            "{run_prefix}/tree/write/project-{}/worker-{worker_index:02}/batch-{}/o{op_index:04}.bin",
            worker_index % 8,
            op_index / 64
        ),
    }
}

async fn warm_benchmark_prefixes(
    client: &mut ObjectClient,
    config: &ObjectBenchmarkConfig,
    run_prefix: &str,
    read_keys: &[String],
    payload: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    if config.write_percent < 100 && !read_keys.is_empty() {
        let key = &read_keys[0];
        let result = client
            .get_object_single_stripe(&config.bucket_id, key)
            .await?;
        if config.verify_reads && result.payload != payload {
            return Err(
                std::io::Error::other(format!("warm read payload mismatch for key {key}")).into(),
            );
        }
        return Ok(());
    }
    match config.key_shape {
        ObjectBenchmarkKeyShape::FlatRoot => {
            let key = format!("{run_prefix}/__warmup__.bin");
            client
                .put_object_single_stripe(&config.bucket_id, &key, payload)
                .await?;
        }
        ObjectBenchmarkKeyShape::WarmTree => {
            for worker_index in 0..config.workers {
                let key = write_key(
                    run_prefix,
                    ObjectBenchmarkKeyShape::WarmTree,
                    config.write_key_count,
                    worker_index,
                    0,
                );
                client
                    .put_object_single_stripe(&config.bucket_id, &key, payload)
                    .await?;
            }
        }
    }
    Ok(())
}

fn record_success(
    totals: &mut BenchTotals,
    bytes: u64,
    elapsed: Duration,
    phases: ObjectPhaseTimes,
) {
    totals.ops += 1;
    totals.bytes += bytes;
    totals.latencies_us.push(elapsed.as_micros());
    totals.phases.kms_initiate_us += phases.kms_initiate.as_micros();
    totals.phases.kms_commit_us += phases.kms_commit.as_micros();
    totals.phases.kms_resolve_us += phases.kms_resolve.as_micros();
    totals.phases.ec_encode_us += phases.ec_encode.as_micros();
    totals.phases.ec_reconstruct_us += phases.ec_reconstruct.as_micros();
    totals.phases.target_connect_us += phases.target_connect.as_micros();
    totals.phases.target_write_us += phases.target_write.as_micros();
    totals.phases.target_read_us += phases.target_read.as_micros();
    totals.phases.target_ready_wait_us += phases.target_ready_wait.as_micros();
    totals.phases.target_request_prepare_us += phases.target_request_prepare.as_micros();
    totals.phases.target_send_headers_us += phases.target_send_headers.as_micros();
    totals.phases.target_send_body_us += phases.target_send_body.as_micros();
    totals.phases.target_wait_response_us += phases.target_wait_response.as_micros();
    totals.phases.target_collect_response_us += phases.target_collect_response.as_micros();
    totals.phases.target_protocol_decode_us += phases.target_protocol_decode.as_micros();
    totals.phases.target_payload_validate_us += phases.target_payload_validate.as_micros();
}

fn merge_bench_totals(into: &mut BenchTotals, from: BenchTotals) {
    into.ops += from.ops;
    into.bytes += from.bytes;
    into.errors.extend(from.errors);
    into.latencies_us.extend(from.latencies_us);
    into.phases.kms_initiate_us += from.phases.kms_initiate_us;
    into.phases.kms_commit_us += from.phases.kms_commit_us;
    into.phases.kms_resolve_us += from.phases.kms_resolve_us;
    into.phases.ec_encode_us += from.phases.ec_encode_us;
    into.phases.ec_reconstruct_us += from.phases.ec_reconstruct_us;
    into.phases.target_connect_us += from.phases.target_connect_us;
    into.phases.target_write_us += from.phases.target_write_us;
    into.phases.target_read_us += from.phases.target_read_us;
    into.phases.target_ready_wait_us += from.phases.target_ready_wait_us;
    into.phases.target_request_prepare_us += from.phases.target_request_prepare_us;
    into.phases.target_send_headers_us += from.phases.target_send_headers_us;
    into.phases.target_send_body_us += from.phases.target_send_body_us;
    into.phases.target_wait_response_us += from.phases.target_wait_response_us;
    into.phases.target_collect_response_us += from.phases.target_collect_response_us;
    into.phases.target_protocol_decode_us += from.phases.target_protocol_decode_us;
    into.phases.target_payload_validate_us += from.phases.target_payload_validate_us;
    into.data_fragment_reads += from.data_fragment_reads;
    into.parity_fragment_reads += from.parity_fragment_reads;
    into.reconstruct_invocations += from.reconstruct_invocations;
    into.fast_path_reads += from.fast_path_reads;
    into.degraded_reads += from.degraded_reads;
}

fn print_kind_summary(kind: &str, totals: &BenchTotals, bench_elapsed: Duration) {
    let mut latencies = totals.latencies_us.clone();
    latencies.sort_unstable();
    let phase_averages = average_phases(&totals.phases, totals.ops);
    println!(
        "ksc_object_benchmark_{} ops={} logical_bytes={} throughput_mib_s={:.2} p50_us={} p95_us={} p99_us={} data_fragment_reads={} parity_fragment_reads={} fast_path_reads={} degraded_reads={} reconstruct_invocations={}",
        kind,
        totals.ops,
        totals.bytes,
        mib_per_second(totals.bytes, bench_elapsed),
        percentile(&latencies, 50),
        percentile(&latencies, 95),
        percentile(&latencies, 99),
        totals.data_fragment_reads,
        totals.parity_fragment_reads,
        totals.fast_path_reads,
        totals.degraded_reads,
        totals.reconstruct_invocations,
    );
    print_phase_line(
        &format!("ksc_object_benchmark_{}_phases_us", kind),
        phase_averages,
    );
}

fn average_phases(totals: &PhaseTotals, ops: usize) -> ObjectPhaseTimes {
    if ops == 0 {
        return ObjectPhaseTimes::default();
    }
    let ops = ops as u128;
    ObjectPhaseTimes {
        kms_initiate: Duration::from_micros((totals.kms_initiate_us / ops) as u64),
        kms_commit: Duration::from_micros((totals.kms_commit_us / ops) as u64),
        kms_resolve: Duration::from_micros((totals.kms_resolve_us / ops) as u64),
        ec_encode: Duration::from_micros((totals.ec_encode_us / ops) as u64),
        ec_reconstruct: Duration::from_micros((totals.ec_reconstruct_us / ops) as u64),
        target_connect: Duration::from_micros((totals.target_connect_us / ops) as u64),
        target_write: Duration::from_micros((totals.target_write_us / ops) as u64),
        target_read: Duration::from_micros((totals.target_read_us / ops) as u64),
        target_ready_wait: Duration::from_micros((totals.target_ready_wait_us / ops) as u64),
        target_request_prepare: Duration::from_micros(
            (totals.target_request_prepare_us / ops) as u64,
        ),
        target_send_headers: Duration::from_micros((totals.target_send_headers_us / ops) as u64),
        target_send_body: Duration::from_micros((totals.target_send_body_us / ops) as u64),
        target_wait_response: Duration::from_micros((totals.target_wait_response_us / ops) as u64),
        target_collect_response: Duration::from_micros(
            (totals.target_collect_response_us / ops) as u64,
        ),
        target_protocol_decode: Duration::from_micros(
            (totals.target_protocol_decode_us / ops) as u64,
        ),
        target_payload_validate: Duration::from_micros(
            (totals.target_payload_validate_us / ops) as u64,
        ),
    }
}

fn percentile(samples: &[u128], pct: usize) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let rank = ((samples.len() - 1) * pct) / 100;
    samples[rank]
}

fn mib_per_second(bytes: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    (bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
}

fn mode_label(write_percent: u8) -> &'static str {
    match write_percent {
        0 => "read-only",
        100 => "write-only",
        30 => "70r30w",
        _ => "mixed",
    }
}

fn key_shape_label(key_shape: ObjectBenchmarkKeyShape) -> &'static str {
    match key_shape {
        ObjectBenchmarkKeyShape::FlatRoot => "flat-root",
        ObjectBenchmarkKeyShape::WarmTree => "warm-tree",
    }
}

fn client_mode(mode: CliCompletionMode) -> ClientCompletionMode {
    match mode {
        CliCompletionMode::Interrupt => ClientCompletionMode::Interrupt,
        CliCompletionMode::HotPoll => ClientCompletionMode::HotPoll,
    }
}
