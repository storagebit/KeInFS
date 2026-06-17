// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::ModeBenchConfig;
use crate::metadata::DynError;
use ksc::client::CompletionMode;
use ksc::object::{ObjectClient, ObjectClientOptions, ObjectPhaseTimes};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

#[derive(Clone, Copy, Debug)]
struct ModeCase {
    read_completion_mode: CompletionMode,
    write_completion_mode: CompletionMode,
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
struct Totals {
    ops: usize,
    bytes: u64,
    latencies_us: Vec<u128>,
    phases: PhaseTotals,
    errors: Vec<String>,
}

pub(crate) async fn run_mode_bench(config: ModeBenchConfig) -> Result<(), DynError> {
    let payload = std::fs::read(&config.input_path)?;
    let cases = if config.matrix {
        vec![
            ModeCase {
                read_completion_mode: CompletionMode::Interrupt,
                write_completion_mode: CompletionMode::Interrupt,
            },
            ModeCase {
                read_completion_mode: CompletionMode::HotPoll,
                write_completion_mode: CompletionMode::Interrupt,
            },
            ModeCase {
                read_completion_mode: CompletionMode::Interrupt,
                write_completion_mode: CompletionMode::HotPoll,
            },
            ModeCase {
                read_completion_mode: CompletionMode::HotPoll,
                write_completion_mode: CompletionMode::HotPoll,
            },
        ]
    } else {
        vec![ModeCase {
            read_completion_mode: config.read_completion_mode,
            write_completion_mode: config.write_completion_mode,
        }]
    };

    for case in cases {
        run_case(&config, &payload, case).await?;
    }
    Ok(())
}

async fn run_case(
    config: &ModeBenchConfig,
    payload: &[u8],
    case: ModeCase,
) -> Result<(), DynError> {
    let options = ObjectClientOptions {
        read_completion_mode: case.read_completion_mode,
        write_completion_mode: case.write_completion_mode,
        metadata_notification_nats_url: config.metadata_notification_nats_url.clone(),
        metadata_notification_subject: config.metadata_notification_subject.clone(),
        ..ObjectClientOptions::default()
    };
    let run_stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let case_prefix = format!(
        "{}/run-{run_stamp}-r{}-w{}",
        config.key_prefix,
        case.read_completion_mode.as_str(),
        case.write_completion_mode.as_str()
    );
    let read_keys = (0..config.prefill_keys)
        .map(|index| format!("{case_prefix}/prefill/k{index:05}.bin"))
        .collect::<Vec<_>>();

    if config.write_percent < 100 {
        let mut prefill_client =
            ObjectClient::connect_with_options(&config.kms_endpoints, options.clone()).await?;
        for key in &read_keys {
            prefill_client
                .put_object_single_stripe(&config.bucket_id, key, payload)
                .await?;
        }
    }

    if !config.warmup.is_zero() {
        let warmup_deadline = Instant::now() + config.warmup;
        let barrier = Arc::new(Barrier::new(config.workers + 1));
        let mut joins = JoinSet::new();
        for worker_index in 0..config.workers {
            let worker_config = config.clone();
            let worker_payload = payload.to_vec();
            let worker_keys = read_keys.clone();
            let worker_prefix = case_prefix.clone();
            let worker_barrier = Arc::clone(&barrier);
            let worker_options = options.clone();
            joins.spawn(async move {
                let mut client = ObjectClient::connect_with_options(
                    &worker_config.kms_endpoints,
                    worker_options,
                )
                .await?;
                worker_barrier.wait().await;
                let mut op_index = 0_usize;
                while Instant::now() < warmup_deadline {
                    let _ = run_one_op(
                        &mut client,
                        &worker_config,
                        &worker_prefix,
                        &worker_keys,
                        &worker_payload,
                        worker_index,
                        op_index,
                    )
                    .await;
                    op_index += 1;
                }
                Ok::<(), DynError>(())
            });
        }
        barrier.wait().await;
        while let Some(result) = joins.join_next().await {
            result.map_err(|err| crate::metadata::boxed_error(err.to_string()))??;
        }
    }

    let measured_deadline = Instant::now() + config.duration;
    let barrier = Arc::new(Barrier::new(config.workers + 1));
    let mut joins = JoinSet::new();
    for worker_index in 0..config.workers {
        let worker_config = config.clone();
        let worker_payload = payload.to_vec();
        let worker_keys = read_keys.clone();
        let worker_prefix = case_prefix.clone();
        let worker_barrier = Arc::clone(&barrier);
        let worker_options = options.clone();
        joins.spawn(async move {
            let mut client =
                ObjectClient::connect_with_options(&worker_config.kms_endpoints, worker_options)
                    .await?;
            worker_barrier.wait().await;
            let mut totals = Totals::default();
            let mut op_index = 0_usize;
            while Instant::now() < measured_deadline {
                match run_one_op(
                    &mut client,
                    &worker_config,
                    &worker_prefix,
                    &worker_keys,
                    &worker_payload,
                    worker_index,
                    op_index,
                )
                .await
                {
                    Ok((latency, phases, bytes)) => {
                        totals.ops += 1;
                        totals.bytes += bytes;
                        totals.latencies_us.push(latency.as_micros());
                        accumulate_phases(&mut totals.phases, phases);
                    }
                    Err(err) => totals.errors.push(err.to_string()),
                }
                op_index += 1;
            }
            Ok::<Totals, DynError>(totals)
        });
    }
    barrier.wait().await;
    let started = Instant::now();
    let mut totals = Totals::default();
    while let Some(result) = joins.join_next().await {
        let worker = result.map_err(|err| crate::metadata::boxed_error(err.to_string()))??;
        totals.ops += worker.ops;
        totals.bytes += worker.bytes;
        totals.latencies_us.extend(worker.latencies_us);
        totals.errors.extend(worker.errors);
        merge_phase_totals(&mut totals.phases, &worker.phases);
    }
    let elapsed = started.elapsed();
    totals.latencies_us.sort_unstable();
    println!(
        "kfc_mode_bench case=read:{}_write:{} workers={} write_percent={} ops={} logical_bytes={} throughput_mib_s={:.2} duration_ms={} errors={} p50_us={} p95_us={} p99_us={}",
        case.read_completion_mode.as_str(),
        case.write_completion_mode.as_str(),
        config.workers,
        config.write_percent,
        totals.ops,
        totals.bytes,
        mib_per_second(totals.bytes, elapsed),
        elapsed.as_millis(),
        totals.errors.len(),
        percentile(&totals.latencies_us, 50),
        percentile(&totals.latencies_us, 95),
        percentile(&totals.latencies_us, 99),
    );
    print_phase_line(
        "kfc_mode_bench_phases_us",
        average_phases(&totals.phases, totals.ops),
    );
    for error in totals.errors.iter().take(8) {
        println!("kfc_mode_bench_error {error}");
    }
    if !totals.errors.is_empty() {
        return Err(crate::metadata::boxed_error(format!(
            "mode-bench hit {} errors",
            totals.errors.len()
        )));
    }
    Ok(())
}

async fn run_one_op(
    client: &mut ObjectClient,
    config: &ModeBenchConfig,
    case_prefix: &str,
    read_keys: &[String],
    payload: &[u8],
    worker_index: usize,
    op_index: usize,
) -> Result<(Duration, ObjectPhaseTimes, u64), DynError> {
    let op_kind = choose_kind(config.write_percent, worker_index, op_index);
    let started = Instant::now();
    match op_kind {
        OpKind::Read => {
            let key = &read_keys[op_index % read_keys.len()];
            let result = client
                .get_object_single_stripe(&config.bucket_id, key)
                .await
                .map_err(|err| crate::metadata::boxed_error(err.to_string()))?;
            if config.verify_reads && result.payload != payload {
                return Err(crate::metadata::boxed_error(format!(
                    "read verification failed for key {}",
                    key
                )));
            }
            Ok((
                started.elapsed(),
                result.phases,
                result.payload.len() as u64,
            ))
        }
        OpKind::Write => {
            let key = format!("{case_prefix}/write/w{worker_index:03}/o{op_index:08}.bin");
            let result = client
                .put_object_single_stripe(&config.bucket_id, &key, payload)
                .await
                .map_err(|err| crate::metadata::boxed_error(err.to_string()))?;
            Ok((started.elapsed(), result.phases, payload.len() as u64))
        }
    }
}

fn choose_kind(write_percent: u8, worker_index: usize, op_index: usize) -> OpKind {
    let selector = ((worker_index * 31 + op_index * 17) % 100) as u8;
    if selector < write_percent {
        OpKind::Write
    } else {
        OpKind::Read
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

fn accumulate_phases(totals: &mut PhaseTotals, phases: ObjectPhaseTimes) {
    totals.kms_initiate_us += phases.kms_initiate.as_micros();
    totals.kms_commit_us += phases.kms_commit.as_micros();
    totals.kms_resolve_us += phases.kms_resolve.as_micros();
    totals.ec_encode_us += phases.ec_encode.as_micros();
    totals.ec_reconstruct_us += phases.ec_reconstruct.as_micros();
    totals.target_connect_us += phases.target_connect.as_micros();
    totals.target_write_us += phases.target_write.as_micros();
    totals.target_read_us += phases.target_read.as_micros();
    totals.target_ready_wait_us += phases.target_ready_wait.as_micros();
    totals.target_request_prepare_us += phases.target_request_prepare.as_micros();
    totals.target_send_headers_us += phases.target_send_headers.as_micros();
    totals.target_send_body_us += phases.target_send_body.as_micros();
    totals.target_wait_response_us += phases.target_wait_response.as_micros();
    totals.target_collect_response_us += phases.target_collect_response.as_micros();
    totals.target_protocol_decode_us += phases.target_protocol_decode.as_micros();
    totals.target_payload_validate_us += phases.target_payload_validate.as_micros();
}

fn merge_phase_totals(dst: &mut PhaseTotals, src: &PhaseTotals) {
    dst.kms_initiate_us += src.kms_initiate_us;
    dst.kms_commit_us += src.kms_commit_us;
    dst.kms_resolve_us += src.kms_resolve_us;
    dst.ec_encode_us += src.ec_encode_us;
    dst.ec_reconstruct_us += src.ec_reconstruct_us;
    dst.target_connect_us += src.target_connect_us;
    dst.target_write_us += src.target_write_us;
    dst.target_read_us += src.target_read_us;
    dst.target_ready_wait_us += src.target_ready_wait_us;
    dst.target_request_prepare_us += src.target_request_prepare_us;
    dst.target_send_headers_us += src.target_send_headers_us;
    dst.target_send_body_us += src.target_send_body_us;
    dst.target_wait_response_us += src.target_wait_response_us;
    dst.target_collect_response_us += src.target_collect_response_us;
    dst.target_protocol_decode_us += src.target_protocol_decode_us;
    dst.target_payload_validate_us += src.target_payload_validate_us;
}

fn print_phase_line(prefix: &str, phases: ObjectPhaseTimes) {
    println!(
        "{} kms_initiate={} kms_commit={} kms_resolve={} ec_encode={} ec_reconstruct={} target_connect={} target_write={} target_read={} target_ready_wait={} target_request_prepare={} target_send_headers={} target_send_body={} target_wait_response={} target_collect_response={} target_protocol_decode={} target_payload_validate={}",
        prefix,
        phases.kms_initiate.as_micros(),
        phases.kms_commit.as_micros(),
        phases.kms_resolve.as_micros(),
        phases.ec_encode.as_micros(),
        phases.ec_reconstruct.as_micros(),
        phases.target_connect.as_micros(),
        phases.target_write.as_micros(),
        phases.target_read.as_micros(),
        phases.target_ready_wait.as_micros(),
        phases.target_request_prepare.as_micros(),
        phases.target_send_headers.as_micros(),
        phases.target_send_body.as_micros(),
        phases.target_wait_response.as_micros(),
        phases.target_collect_response.as_micros(),
        phases.target_protocol_decode.as_micros(),
        phases.target_payload_validate.as_micros(),
    );
}
