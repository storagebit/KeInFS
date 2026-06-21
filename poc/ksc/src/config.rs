// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use ksc::object::{
    DEFAULT_KMS_GRPC_MAX_MESSAGE_BYTES, DEFAULT_METADATA_NOTIFICATION_SUBJECT,
    DEFAULT_WRITE_WINDOW_INFLIGHT_STRIPES, DEFAULT_WRITE_WINDOW_MAX_STRIPES,
};

#[derive(Clone, Debug)]
pub(crate) enum Command {
    Smoke(SmokeConfig),
    EcBenchmark(EcBenchmarkConfig),
    Benchmark(BenchmarkConfig),
    PutObject(ObjectPutConfig),
    GetObject(ObjectGetConfig),
    DeleteObject(ObjectDeleteConfig),
    ObjectBenchmark(ObjectBenchmarkConfig),
}

#[derive(Clone, Debug)]
pub(crate) struct SmokeConfig {
    pub(crate) endpoint: String,
    pub(crate) chunk_seed: u64,
    pub(crate) slot_index: u64,
    pub(crate) generation: u32,
    pub(crate) packed_count: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct BenchmarkConfig {
    pub(crate) endpoints: Vec<String>,
    pub(crate) client_id: String,
    pub(crate) transfer_mode: TransferMode,
    pub(crate) chunk_seed: u64,
    pub(crate) slot_base: u64,
    pub(crate) generation_start: u32,
    pub(crate) packed_count: usize,
    pub(crate) pack_max_payload_bytes: usize,
    pub(crate) key_count: usize,
    pub(crate) workers: usize,
    pub(crate) inflight_streams_per_worker: usize,
    pub(crate) target_initial_inflight: usize,
    pub(crate) target_min_inflight: usize,
    pub(crate) target_additive_increase_every: usize,
    pub(crate) avoid_overlapping_writes: bool,
    pub(crate) duration: Duration,
    pub(crate) write_percent: u8,
    pub(crate) cleanup: bool,
    pub(crate) stats_root: PathBuf,
    pub(crate) stats_publish_interval: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct EcBenchmarkConfig {
    pub(crate) input_path: PathBuf,
    pub(crate) iterations: usize,
    pub(crate) warmup_iterations: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct ObjectPutConfig {
    pub(crate) kms_endpoints: Vec<String>,
    pub(crate) bucket_id: String,
    pub(crate) key: String,
    pub(crate) input_path: PathBuf,
    pub(crate) write_completion_mode: CompletionMode,
    pub(crate) write_window_max_stripes: usize,
    pub(crate) write_window_inflight_stripes: usize,
    pub(crate) kms_grpc_max_message_bytes: usize,
    pub(crate) metadata_notification_nats_url: Option<String>,
    pub(crate) metadata_notification_subject: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ObjectGetConfig {
    pub(crate) kms_endpoints: Vec<String>,
    pub(crate) bucket_id: String,
    pub(crate) key: String,
    pub(crate) output_path: PathBuf,
    pub(crate) read_completion_mode: CompletionMode,
    pub(crate) kms_grpc_max_message_bytes: usize,
    pub(crate) metadata_notification_nats_url: Option<String>,
    pub(crate) metadata_notification_subject: String,
    /// Byte-granular range read: fetch only `[range_offset, range_offset+range_length)`.
    pub(crate) range_offset: Option<u64>,
    pub(crate) range_length: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct ObjectDeleteConfig {
    pub(crate) kms_endpoints: Vec<String>,
    pub(crate) bucket_id: String,
    pub(crate) key: String,
    pub(crate) version_ids: Vec<String>,
    pub(crate) write_completion_mode: CompletionMode,
    pub(crate) kms_grpc_max_message_bytes: usize,
    pub(crate) metadata_notification_nats_url: Option<String>,
    pub(crate) metadata_notification_subject: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ObjectBenchmarkConfig {
    pub(crate) kms_endpoints: Vec<String>,
    pub(crate) bucket_id: String,
    pub(crate) input_path: PathBuf,
    pub(crate) workers: usize,
    pub(crate) run_mode: ObjectBenchmarkRunMode,
    pub(crate) write_percent: u8,
    pub(crate) prefill_keys: usize,
    pub(crate) existing_read_keys: Vec<String>,
    pub(crate) key_prefix: String,
    pub(crate) verify_reads: bool,
    pub(crate) key_shape: ObjectBenchmarkKeyShape,
    pub(crate) write_key_count: Option<usize>,
    pub(crate) read_completion_mode: CompletionMode,
    pub(crate) write_completion_mode: CompletionMode,
    pub(crate) read_resolve_cache_ttl: Duration,
    pub(crate) write_window_max_stripes: usize,
    pub(crate) write_window_inflight_stripes: usize,
    pub(crate) kms_grpc_max_message_bytes: usize,
    pub(crate) metadata_notification_nats_url: Option<String>,
    pub(crate) metadata_notification_subject: String,
    // Live observability gap-fix: when `progress_interval` > 0, the benchmark
    // prints a periodic progress line and (if `stats_root` is set) writes a JSON
    // snapshot so the CLIENT side of a load test is observable WHILE it runs,
    // not just from the final stdout summary. Default off preserves prior
    // stdout-only behavior.
    pub(crate) progress_interval: Duration,
    pub(crate) stats_root: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObjectBenchmarkRunMode {
    FixedOps {
        ops_per_worker: usize,
    },
    Timed {
        warmup: Duration,
        duration: Duration,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransferMode {
    Single,
    Packed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObjectBenchmarkKeyShape {
    FlatRoot,
    WarmTree,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletionMode {
    Interrupt,
    HotPoll,
}

impl CompletionMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Interrupt => "interrupt",
            Self::HotPoll => "hot-poll",
        }
    }
}

pub(crate) fn parse_args(args: Vec<String>) -> Result<Command, Box<dyn Error>> {
    let Some(subcommand) = args.first().map(String::as_str) else {
        return Err(arg_error(
            "missing subcommand; use `smoke`, `benchmark`, `put-object`, `get-object`, or `delete-object`",
        ));
    };
    match subcommand {
        "smoke" => parse_smoke_args(&args[1..]).map(Command::Smoke),
        "ec-benchmark" | "ec-bench" => {
            parse_ec_benchmark_args(&args[1..]).map(Command::EcBenchmark)
        }
        "benchmark" | "bench" => parse_benchmark_args(&args[1..]).map(Command::Benchmark),
        "put-object" => parse_put_object_args(&args[1..]).map(Command::PutObject),
        "get-object" => parse_get_object_args(&args[1..]).map(Command::GetObject),
        "delete-object" => parse_delete_object_args(&args[1..]).map(Command::DeleteObject),
        "object-benchmark" | "object-bench" => {
            parse_object_benchmark_args(&args[1..]).map(Command::ObjectBenchmark)
        }
        "--help" | "-h" => Err(arg_error(usage())),
        other => Err(arg_error(format!(
            "unknown subcommand `{other}`; use `smoke`, `ec-benchmark`, `benchmark`, `put-object`, `get-object`, `delete-object`, or `object-benchmark`"
        ))),
    }
}

fn parse_smoke_args(args: &[String]) -> Result<SmokeConfig, Box<dyn Error>> {
    let mut endpoint = "http://[::1]:18080".to_string();
    let mut chunk_seed = 7_u64;
    let mut slot_index = 0_u64;
    let mut generation = 1_u32;
    let mut packed_count = 1_usize;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--endpoint" => {
                i += 1;
                endpoint = args
                    .get(i)
                    .ok_or_else(|| missing_value("--endpoint"))?
                    .clone();
            }
            "--chunk-seed" => {
                i += 1;
                chunk_seed = args
                    .get(i)
                    .ok_or_else(|| missing_value("--chunk-seed"))?
                    .parse()?;
            }
            "--slot-index" => {
                i += 1;
                slot_index = args
                    .get(i)
                    .ok_or_else(|| missing_value("--slot-index"))?
                    .parse()?;
            }
            "--generation" => {
                i += 1;
                generation = args
                    .get(i)
                    .ok_or_else(|| missing_value("--generation"))?
                    .parse()?;
            }
            "--packed-count" => {
                i += 1;
                packed_count = args
                    .get(i)
                    .ok_or_else(|| missing_value("--packed-count"))?
                    .parse()?;
            }
            "--help" | "-h" => return Err(arg_error(smoke_usage())),
            other => return Err(arg_error(format!("unknown KSC smoke argument `{other}`"))),
        }
        i += 1;
    }
    if packed_count == 0 {
        return Err(arg_error("--packed-count must be > 0"));
    }
    Ok(SmokeConfig {
        endpoint,
        chunk_seed,
        slot_index,
        generation,
        packed_count,
    })
}

fn parse_benchmark_args(args: &[String]) -> Result<BenchmarkConfig, Box<dyn Error>> {
    let mut endpoints = Vec::new();
    let mut client_id = "ksc-bench".to_string();
    let mut transfer_mode = TransferMode::Single;
    let mut chunk_seed = 7_u64;
    let mut slot_base = 0_u64;
    let mut generation_start = 1_u32;
    let mut packed_count = 4_usize;
    let mut pack_max_payload_bytes = 16 * 1024 * 1024_usize;
    let mut key_count = 256_usize;
    let mut workers = 8_usize;
    let mut inflight_streams_per_worker = 8_usize;
    let mut target_initial_inflight = None;
    let mut target_min_inflight = 1_usize;
    let mut target_additive_increase_every = 256_usize;
    let mut avoid_overlapping_writes = true;
    let mut duration_secs = 15_u64;
    let mut write_percent = 30_u8;
    let mut cleanup = true;
    let mut stats_root = PathBuf::from("/run/keinfs/ksc");
    let mut stats_publish_ms = 250_u64;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--endpoint" => {
                i += 1;
                endpoints.push(
                    args.get(i)
                        .ok_or_else(|| missing_value("--endpoint"))?
                        .clone(),
                );
            }
            "--client-id" => {
                i += 1;
                client_id = args
                    .get(i)
                    .ok_or_else(|| missing_value("--client-id"))?
                    .clone();
            }
            "--transfer-mode" => {
                i += 1;
                transfer_mode = match args.get(i).map(String::as_str) {
                    Some("single") => TransferMode::Single,
                    Some("packed") => TransferMode::Packed,
                    Some(other) => {
                        return Err(arg_error(format!(
                            "unknown --transfer-mode value `{other}`"
                        )));
                    }
                    None => return Err(arg_error("missing value for --transfer-mode")),
                };
            }
            "--chunk-seed" => {
                i += 1;
                chunk_seed = args
                    .get(i)
                    .ok_or_else(|| missing_value("--chunk-seed"))?
                    .parse()?;
            }
            "--slot-base" => {
                i += 1;
                slot_base = args
                    .get(i)
                    .ok_or_else(|| missing_value("--slot-base"))?
                    .parse()?;
            }
            "--generation-start" => {
                i += 1;
                generation_start = args
                    .get(i)
                    .ok_or_else(|| missing_value("--generation-start"))?
                    .parse()?;
            }
            "--packed-count" => {
                i += 1;
                packed_count = args
                    .get(i)
                    .ok_or_else(|| missing_value("--packed-count"))?
                    .parse()?;
            }
            "--pack-max-payload-bytes" => {
                i += 1;
                pack_max_payload_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--pack-max-payload-bytes"))?
                    .parse()?;
            }
            "--key-count" => {
                i += 1;
                key_count = args
                    .get(i)
                    .ok_or_else(|| missing_value("--key-count"))?
                    .parse()?;
            }
            "--workers" => {
                i += 1;
                workers = args
                    .get(i)
                    .ok_or_else(|| missing_value("--workers"))?
                    .parse()?;
            }
            "--inflight-streams-per-worker" => {
                i += 1;
                inflight_streams_per_worker = args
                    .get(i)
                    .ok_or_else(|| missing_value("--inflight-streams-per-worker"))?
                    .parse()?;
            }
            "--target-initial-inflight" => {
                i += 1;
                target_initial_inflight = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--target-initial-inflight"))?
                        .parse()?,
                );
            }
            "--target-min-inflight" => {
                i += 1;
                target_min_inflight = args
                    .get(i)
                    .ok_or_else(|| missing_value("--target-min-inflight"))?
                    .parse()?;
            }
            "--target-additive-increase-every" => {
                i += 1;
                target_additive_increase_every = args
                    .get(i)
                    .ok_or_else(|| missing_value("--target-additive-increase-every"))?
                    .parse()?;
            }
            "--duration-secs" => {
                i += 1;
                duration_secs = args
                    .get(i)
                    .ok_or_else(|| missing_value("--duration-secs"))?
                    .parse()?;
            }
            "--write-percent" => {
                i += 1;
                write_percent = args
                    .get(i)
                    .ok_or_else(|| missing_value("--write-percent"))?
                    .parse()?;
            }
            "--stats-root" => {
                i += 1;
                stats_root =
                    PathBuf::from(args.get(i).ok_or_else(|| missing_value("--stats-root"))?);
            }
            "--stats-publish-ms" => {
                i += 1;
                stats_publish_ms = args
                    .get(i)
                    .ok_or_else(|| missing_value("--stats-publish-ms"))?
                    .parse()?;
            }
            "--cleanup" => cleanup = true,
            "--no-cleanup" => cleanup = false,
            "--avoid-overlapping-writes" => avoid_overlapping_writes = true,
            "--allow-overlapping-writes" => avoid_overlapping_writes = false,
            "--help" | "-h" => return Err(arg_error(benchmark_usage())),
            other => {
                return Err(arg_error(format!(
                    "unknown KSC benchmark argument `{other}`"
                )));
            }
        }
        i += 1;
    }

    if endpoints.is_empty() {
        endpoints.push("http://[::1]:18080".to_string());
    }
    if packed_count == 0 {
        return Err(arg_error("--packed-count must be > 0"));
    }
    if transfer_mode == TransferMode::Single && packed_count != 1 {
        return Err(arg_error(
            "--packed-count must be 1 when --transfer-mode=single",
        ));
    }
    if key_count == 0 {
        return Err(arg_error("--key-count must be > 0"));
    }
    if workers == 0 {
        return Err(arg_error("--workers must be > 0"));
    }
    if inflight_streams_per_worker == 0 {
        return Err(arg_error("--inflight-streams-per-worker must be > 0"));
    }
    if duration_secs == 0 {
        return Err(arg_error("--duration-secs must be > 0"));
    }
    if packed_count > key_count {
        return Err(arg_error(
            "--packed-count must be <= --key-count so a benchmark pack has enough live keys",
        ));
    }
    if write_percent > 100 {
        return Err(arg_error("--write-percent must be between 0 and 100"));
    }
    if pack_max_payload_bytes == 0 {
        return Err(arg_error("--pack-max-payload-bytes must be > 0"));
    }
    if stats_publish_ms == 0 {
        return Err(arg_error("--stats-publish-ms must be > 0"));
    }
    let target_initial_inflight =
        target_initial_inflight.unwrap_or(workers.saturating_mul(inflight_streams_per_worker));
    if target_initial_inflight == 0 {
        return Err(arg_error("--target-initial-inflight must be > 0"));
    }
    if target_min_inflight == 0 {
        return Err(arg_error("--target-min-inflight must be > 0"));
    }
    if target_min_inflight > target_initial_inflight {
        return Err(arg_error(
            "--target-min-inflight must be <= --target-initial-inflight",
        ));
    }
    if target_additive_increase_every == 0 {
        return Err(arg_error("--target-additive-increase-every must be > 0"));
    }

    Ok(BenchmarkConfig {
        endpoints,
        client_id,
        transfer_mode,
        chunk_seed,
        slot_base,
        generation_start,
        packed_count,
        pack_max_payload_bytes,
        key_count,
        workers,
        inflight_streams_per_worker,
        target_initial_inflight,
        target_min_inflight,
        target_additive_increase_every,
        avoid_overlapping_writes,
        duration: Duration::from_secs(duration_secs),
        write_percent,
        cleanup,
        stats_root,
        stats_publish_interval: Duration::from_millis(stats_publish_ms),
    })
}

fn parse_ec_benchmark_args(args: &[String]) -> Result<EcBenchmarkConfig, Box<dyn Error>> {
    let mut input_path = None;
    let mut iterations = 256_usize;
    let mut warmup_iterations = 32_usize;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--input" => {
                i += 1;
                input_path = Some(PathBuf::from(
                    args.get(i).ok_or_else(|| missing_value("--input"))?,
                ));
            }
            "--iterations" => {
                i += 1;
                iterations = args
                    .get(i)
                    .ok_or_else(|| missing_value("--iterations"))?
                    .parse()?;
            }
            "--warmup-iterations" => {
                i += 1;
                warmup_iterations = args
                    .get(i)
                    .ok_or_else(|| missing_value("--warmup-iterations"))?
                    .parse()?;
            }
            "--help" | "-h" => return Err(arg_error(ec_benchmark_usage())),
            other => {
                return Err(arg_error(format!(
                    "unknown KSC ec-benchmark argument `{other}`"
                )));
            }
        }
        i += 1;
    }
    if iterations == 0 {
        return Err(arg_error("--iterations must be > 0"));
    }
    Ok(EcBenchmarkConfig {
        input_path: input_path.ok_or_else(|| arg_error("--input is required"))?,
        iterations,
        warmup_iterations,
    })
}

fn parse_put_object_args(args: &[String]) -> Result<ObjectPutConfig, Box<dyn Error>> {
    let mut kms_endpoints = vec!["http://127.0.0.1:50060".to_string()];
    let mut bucket_id = String::new();
    let mut key = String::new();
    let mut input_path = None;
    let mut write_completion_mode = CompletionMode::Interrupt;
    let mut write_window_max_stripes = DEFAULT_WRITE_WINDOW_MAX_STRIPES;
    let mut write_window_inflight_stripes = DEFAULT_WRITE_WINDOW_INFLIGHT_STRIPES;
    let mut kms_grpc_max_message_bytes = DEFAULT_KMS_GRPC_MAX_MESSAGE_BYTES;
    let mut metadata_notification_nats_url = None;
    let mut metadata_notification_subject = DEFAULT_METADATA_NOTIFICATION_SUBJECT.to_string();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--kms-endpoint" => {
                i += 1;
                merge_endpoint_arg(
                    &mut kms_endpoints,
                    args.get(i).ok_or_else(|| missing_value("--kms-endpoint"))?,
                    "http://127.0.0.1:50060",
                );
            }
            "--bucket" => {
                i += 1;
                bucket_id = args
                    .get(i)
                    .ok_or_else(|| missing_value("--bucket"))?
                    .clone();
            }
            "--key" => {
                i += 1;
                key = args.get(i).ok_or_else(|| missing_value("--key"))?.clone();
            }
            "--input" => {
                i += 1;
                input_path = Some(PathBuf::from(
                    args.get(i).ok_or_else(|| missing_value("--input"))?,
                ));
            }
            "--write-completion-mode" => {
                i += 1;
                write_completion_mode = parse_completion_mode(
                    args.get(i)
                        .ok_or_else(|| missing_value("--write-completion-mode"))?,
                    "--write-completion-mode",
                )?;
            }
            "--write-window-max-stripes" => {
                i += 1;
                write_window_max_stripes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--write-window-max-stripes"))?
                    .parse()?;
            }
            "--write-window-inflight-stripes" => {
                i += 1;
                write_window_inflight_stripes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--write-window-inflight-stripes"))?
                    .parse()?;
            }
            "--kms-grpc-max-message-bytes" => {
                i += 1;
                kms_grpc_max_message_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--kms-grpc-max-message-bytes"))?
                    .parse()?;
            }
            "--metadata-notification-nats-url" => {
                i += 1;
                metadata_notification_nats_url = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--metadata-notification-nats-url"))?
                        .clone(),
                );
            }
            "--metadata-notification-subject" => {
                i += 1;
                metadata_notification_subject = args
                    .get(i)
                    .ok_or_else(|| missing_value("--metadata-notification-subject"))?
                    .clone();
            }
            "--help" | "-h" => return Err(arg_error(put_object_usage())),
            other => {
                return Err(arg_error(format!(
                    "unknown KSC put-object argument `{other}`"
                )));
            }
        }
        i += 1;
    }
    if bucket_id.is_empty() {
        return Err(arg_error("--bucket is required"));
    }
    if key.is_empty() {
        return Err(arg_error("--key is required"));
    }
    if write_window_max_stripes == 0 {
        return Err(arg_error("--write-window-max-stripes must be > 0"));
    }
    if write_window_inflight_stripes == 0 {
        return Err(arg_error("--write-window-inflight-stripes must be > 0"));
    }
    if kms_grpc_max_message_bytes == 0 {
        return Err(arg_error("--kms-grpc-max-message-bytes must be > 0"));
    }
    Ok(ObjectPutConfig {
        kms_endpoints,
        bucket_id,
        key,
        input_path: input_path.ok_or_else(|| arg_error("--input is required"))?,
        write_completion_mode,
        write_window_max_stripes,
        write_window_inflight_stripes,
        kms_grpc_max_message_bytes,
        metadata_notification_nats_url,
        metadata_notification_subject,
    })
}

fn parse_get_object_args(args: &[String]) -> Result<ObjectGetConfig, Box<dyn Error>> {
    let mut kms_endpoints = vec!["http://127.0.0.1:50060".to_string()];
    let mut bucket_id = String::new();
    let mut key = String::new();
    let mut output_path = None;
    let mut read_completion_mode = CompletionMode::Interrupt;
    let mut range_offset: Option<u64> = None;
    let mut range_length: Option<u64> = None;
    let mut kms_grpc_max_message_bytes = DEFAULT_KMS_GRPC_MAX_MESSAGE_BYTES;
    let mut metadata_notification_nats_url = None;
    let mut metadata_notification_subject = DEFAULT_METADATA_NOTIFICATION_SUBJECT.to_string();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--kms-endpoint" => {
                i += 1;
                merge_endpoint_arg(
                    &mut kms_endpoints,
                    args.get(i).ok_or_else(|| missing_value("--kms-endpoint"))?,
                    "http://127.0.0.1:50060",
                );
            }
            "--offset" => {
                i += 1;
                range_offset = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--offset"))?
                        .parse()?,
                );
            }
            "--length" => {
                i += 1;
                range_length = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--length"))?
                        .parse()?,
                );
            }
            "--bucket" => {
                i += 1;
                bucket_id = args
                    .get(i)
                    .ok_or_else(|| missing_value("--bucket"))?
                    .clone();
            }
            "--key" => {
                i += 1;
                key = args.get(i).ok_or_else(|| missing_value("--key"))?.clone();
            }
            "--output" => {
                i += 1;
                output_path = Some(PathBuf::from(
                    args.get(i).ok_or_else(|| missing_value("--output"))?,
                ));
            }
            "--read-completion-mode" => {
                i += 1;
                read_completion_mode = parse_completion_mode(
                    args.get(i)
                        .ok_or_else(|| missing_value("--read-completion-mode"))?,
                    "--read-completion-mode",
                )?;
            }
            "--kms-grpc-max-message-bytes" => {
                i += 1;
                kms_grpc_max_message_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--kms-grpc-max-message-bytes"))?
                    .parse()?;
            }
            "--metadata-notification-nats-url" => {
                i += 1;
                metadata_notification_nats_url = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--metadata-notification-nats-url"))?
                        .clone(),
                );
            }
            "--metadata-notification-subject" => {
                i += 1;
                metadata_notification_subject = args
                    .get(i)
                    .ok_or_else(|| missing_value("--metadata-notification-subject"))?
                    .clone();
            }
            "--help" | "-h" => return Err(arg_error(get_object_usage())),
            other => {
                return Err(arg_error(format!(
                    "unknown KSC get-object argument `{other}`"
                )));
            }
        }
        i += 1;
    }
    if bucket_id.is_empty() {
        return Err(arg_error("--bucket is required"));
    }
    if key.is_empty() {
        return Err(arg_error("--key is required"));
    }
    if kms_grpc_max_message_bytes == 0 {
        return Err(arg_error("--kms-grpc-max-message-bytes must be > 0"));
    }
    Ok(ObjectGetConfig {
        kms_endpoints,
        bucket_id,
        key,
        output_path: output_path.ok_or_else(|| arg_error("--output is required"))?,
        read_completion_mode,
        kms_grpc_max_message_bytes,
        metadata_notification_nats_url,
        metadata_notification_subject,
        range_offset,
        range_length,
    })
}

fn parse_delete_object_args(args: &[String]) -> Result<ObjectDeleteConfig, Box<dyn Error>> {
    let mut kms_endpoints = vec!["http://127.0.0.1:50060".to_string()];
    let mut bucket_id = String::new();
    let mut key = String::new();
    let mut version_ids = Vec::new();
    let mut write_completion_mode = CompletionMode::Interrupt;
    let mut kms_grpc_max_message_bytes = DEFAULT_KMS_GRPC_MAX_MESSAGE_BYTES;
    let mut metadata_notification_nats_url = None;
    let mut metadata_notification_subject = DEFAULT_METADATA_NOTIFICATION_SUBJECT.to_string();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--kms-endpoint" => {
                i += 1;
                merge_endpoint_arg(
                    &mut kms_endpoints,
                    args.get(i).ok_or_else(|| missing_value("--kms-endpoint"))?,
                    "http://127.0.0.1:50060",
                );
            }
            "--bucket" => {
                i += 1;
                bucket_id = args
                    .get(i)
                    .ok_or_else(|| missing_value("--bucket"))?
                    .clone();
            }
            "--key" => {
                i += 1;
                key = args.get(i).ok_or_else(|| missing_value("--key"))?.clone();
            }
            "--version-id" => {
                i += 1;
                version_ids.push(
                    args.get(i)
                        .ok_or_else(|| missing_value("--version-id"))?
                        .clone(),
                );
            }
            "--write-completion-mode" => {
                i += 1;
                write_completion_mode = parse_completion_mode(
                    args.get(i)
                        .ok_or_else(|| missing_value("--write-completion-mode"))?,
                    "--write-completion-mode",
                )?;
            }
            "--kms-grpc-max-message-bytes" => {
                i += 1;
                kms_grpc_max_message_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--kms-grpc-max-message-bytes"))?
                    .parse()?;
            }
            "--metadata-notification-nats-url" => {
                i += 1;
                metadata_notification_nats_url = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--metadata-notification-nats-url"))?
                        .clone(),
                );
            }
            "--metadata-notification-subject" => {
                i += 1;
                metadata_notification_subject = args
                    .get(i)
                    .ok_or_else(|| missing_value("--metadata-notification-subject"))?
                    .clone();
            }
            "--help" | "-h" => return Err(arg_error(delete_object_usage())),
            other => {
                return Err(arg_error(format!(
                    "unknown KSC delete-object argument `{other}`"
                )));
            }
        }
        i += 1;
    }
    if bucket_id.is_empty() {
        return Err(arg_error("--bucket is required"));
    }
    if key.is_empty() {
        return Err(arg_error("--key is required"));
    }
    if kms_grpc_max_message_bytes == 0 {
        return Err(arg_error("--kms-grpc-max-message-bytes must be > 0"));
    }
    Ok(ObjectDeleteConfig {
        kms_endpoints,
        bucket_id,
        key,
        version_ids,
        write_completion_mode,
        kms_grpc_max_message_bytes,
        metadata_notification_nats_url,
        metadata_notification_subject,
    })
}

fn parse_object_benchmark_args(args: &[String]) -> Result<ObjectBenchmarkConfig, Box<dyn Error>> {
    let mut kms_endpoints = vec!["http://127.0.0.1:50060".to_string()];
    let mut bucket_id = String::new();
    let mut input_path = None;
    let mut workers = 4_usize;
    let mut ops_per_worker = None;
    let mut duration_secs = None;
    let mut warmup_secs = 0_u64;
    let mut write_percent = 30_u8;
    let mut prefill_keys = 32_usize;
    let mut existing_read_keys = Vec::new();
    let mut key_prefix = "bench/object-benchmark".to_string();
    let mut verify_reads = true;
    let mut key_shape = ObjectBenchmarkKeyShape::FlatRoot;
    let mut write_key_count = None;
    let mut read_completion_mode = CompletionMode::Interrupt;
    let mut write_completion_mode = CompletionMode::Interrupt;
    let mut read_resolve_cache_ttl_secs = 60_u64;
    let mut write_window_max_stripes = DEFAULT_WRITE_WINDOW_MAX_STRIPES;
    let mut write_window_inflight_stripes = DEFAULT_WRITE_WINDOW_INFLIGHT_STRIPES;
    let mut kms_grpc_max_message_bytes = DEFAULT_KMS_GRPC_MAX_MESSAGE_BYTES;
    let mut metadata_notification_nats_url = None;
    let mut metadata_notification_subject = DEFAULT_METADATA_NOTIFICATION_SUBJECT.to_string();
    let mut progress_interval_secs = 0_u64;
    let mut stats_root: Option<PathBuf> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--kms-endpoint" => {
                i += 1;
                merge_endpoint_arg(
                    &mut kms_endpoints,
                    args.get(i).ok_or_else(|| missing_value("--kms-endpoint"))?,
                    "http://127.0.0.1:50060",
                );
            }
            "--bucket" => {
                i += 1;
                bucket_id = args
                    .get(i)
                    .ok_or_else(|| missing_value("--bucket"))?
                    .clone();
            }
            "--input" => {
                i += 1;
                input_path = Some(PathBuf::from(
                    args.get(i).ok_or_else(|| missing_value("--input"))?,
                ));
            }
            "--workers" => {
                i += 1;
                workers = args
                    .get(i)
                    .ok_or_else(|| missing_value("--workers"))?
                    .parse()?;
            }
            "--ops-per-worker" => {
                i += 1;
                ops_per_worker = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--ops-per-worker"))?
                        .parse()?,
                );
            }
            "--duration-secs" => {
                i += 1;
                duration_secs = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--duration-secs"))?
                        .parse()?,
                );
            }
            "--warmup-secs" => {
                i += 1;
                warmup_secs = args
                    .get(i)
                    .ok_or_else(|| missing_value("--warmup-secs"))?
                    .parse()?;
            }
            "--write-percent" => {
                i += 1;
                write_percent = args
                    .get(i)
                    .ok_or_else(|| missing_value("--write-percent"))?
                    .parse()?;
            }
            "--prefill-keys" => {
                i += 1;
                prefill_keys = args
                    .get(i)
                    .ok_or_else(|| missing_value("--prefill-keys"))?
                    .parse()?;
            }
            "--existing-read-key" => {
                i += 1;
                merge_values_arg(
                    &mut existing_read_keys,
                    args.get(i)
                        .ok_or_else(|| missing_value("--existing-read-key"))?,
                );
            }
            "--key-prefix" => {
                i += 1;
                key_prefix = args
                    .get(i)
                    .ok_or_else(|| missing_value("--key-prefix"))?
                    .clone();
            }
            "--progress-secs" => {
                i += 1;
                progress_interval_secs = args
                    .get(i)
                    .ok_or_else(|| missing_value("--progress-secs"))?
                    .parse()?;
            }
            "--stats-root" => {
                i += 1;
                stats_root = Some(PathBuf::from(
                    args.get(i).ok_or_else(|| missing_value("--stats-root"))?,
                ));
            }
            "--key-shape" => {
                i += 1;
                key_shape = match args.get(i).map(String::as_str) {
                    Some("flat-root") => ObjectBenchmarkKeyShape::FlatRoot,
                    Some("warm-tree") => ObjectBenchmarkKeyShape::WarmTree,
                    Some(other) => {
                        return Err(arg_error(format!("unknown --key-shape value `{other}`")));
                    }
                    None => return Err(arg_error("missing value for --key-shape")),
                };
            }
            "--write-key-count" => {
                i += 1;
                write_key_count = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--write-key-count"))?
                        .parse()?,
                );
            }
            "--read-completion-mode" => {
                i += 1;
                read_completion_mode = parse_completion_mode(
                    args.get(i)
                        .ok_or_else(|| missing_value("--read-completion-mode"))?,
                    "--read-completion-mode",
                )?;
            }
            "--write-completion-mode" => {
                i += 1;
                write_completion_mode = parse_completion_mode(
                    args.get(i)
                        .ok_or_else(|| missing_value("--write-completion-mode"))?,
                    "--write-completion-mode",
                )?;
            }
            "--read-resolve-cache-ttl-secs" => {
                i += 1;
                read_resolve_cache_ttl_secs = args
                    .get(i)
                    .ok_or_else(|| missing_value("--read-resolve-cache-ttl-secs"))?
                    .parse()?;
            }
            "--write-window-max-stripes" => {
                i += 1;
                write_window_max_stripes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--write-window-max-stripes"))?
                    .parse()?;
            }
            "--write-window-inflight-stripes" => {
                i += 1;
                write_window_inflight_stripes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--write-window-inflight-stripes"))?
                    .parse()?;
            }
            "--kms-grpc-max-message-bytes" => {
                i += 1;
                kms_grpc_max_message_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--kms-grpc-max-message-bytes"))?
                    .parse()?;
            }
            "--metadata-notification-nats-url" => {
                i += 1;
                metadata_notification_nats_url = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--metadata-notification-nats-url"))?
                        .clone(),
                );
            }
            "--metadata-notification-subject" => {
                i += 1;
                metadata_notification_subject = args
                    .get(i)
                    .ok_or_else(|| missing_value("--metadata-notification-subject"))?
                    .clone();
            }
            "--verify-reads" => verify_reads = true,
            "--no-verify-reads" => verify_reads = false,
            "--help" | "-h" => return Err(arg_error(object_benchmark_usage())),
            other => {
                return Err(arg_error(format!(
                    "unknown KSC object-benchmark argument `{other}`"
                )));
            }
        }
        i += 1;
    }

    if bucket_id.is_empty() {
        return Err(arg_error("--bucket is required"));
    }
    if workers == 0 {
        return Err(arg_error("--workers must be > 0"));
    }
    if write_percent > 100 {
        return Err(arg_error("--write-percent must be between 0 and 100"));
    }
    if write_percent < 100 && prefill_keys == 0 && existing_read_keys.is_empty() {
        return Err(arg_error(
            "--prefill-keys must be > 0 unless --existing-read-key is provided for read or mixed object benchmarks",
        ));
    }
    if let Some(write_key_count) = write_key_count {
        if write_key_count == 0 {
            return Err(arg_error("--write-key-count must be > 0"));
        }
    }
    if write_window_max_stripes == 0 {
        return Err(arg_error("--write-window-max-stripes must be > 0"));
    }
    if write_window_inflight_stripes == 0 {
        return Err(arg_error("--write-window-inflight-stripes must be > 0"));
    }
    if kms_grpc_max_message_bytes == 0 {
        return Err(arg_error("--kms-grpc-max-message-bytes must be > 0"));
    }
    if read_resolve_cache_ttl_secs == 0 {
        return Err(arg_error("--read-resolve-cache-ttl-secs must be > 0"));
    }
    let run_mode = match (ops_per_worker, duration_secs) {
        (Some(_), Some(_)) => {
            return Err(arg_error(
                "use either --ops-per-worker or --duration-secs, not both",
            ));
        }
        (Some(ops_per_worker), None) => {
            if ops_per_worker == 0 {
                return Err(arg_error("--ops-per-worker must be > 0"));
            }
            ObjectBenchmarkRunMode::FixedOps { ops_per_worker }
        }
        (None, Some(duration_secs)) => {
            if duration_secs == 0 {
                return Err(arg_error("--duration-secs must be > 0"));
            }
            ObjectBenchmarkRunMode::Timed {
                warmup: Duration::from_secs(warmup_secs),
                duration: Duration::from_secs(duration_secs),
            }
        }
        (None, None) => {
            if warmup_secs > 0 {
                return Err(arg_error(
                    "--warmup-secs requires --duration-secs for object benchmarks",
                ));
            }
            ObjectBenchmarkRunMode::FixedOps { ops_per_worker: 4 }
        }
    };
    Ok(ObjectBenchmarkConfig {
        kms_endpoints,
        bucket_id,
        input_path: input_path.ok_or_else(|| arg_error("--input is required"))?,
        workers,
        run_mode,
        write_percent,
        prefill_keys,
        existing_read_keys,
        key_prefix,
        verify_reads,
        key_shape,
        write_key_count,
        read_completion_mode,
        write_completion_mode,
        read_resolve_cache_ttl: Duration::from_secs(read_resolve_cache_ttl_secs),
        write_window_max_stripes,
        write_window_inflight_stripes,
        kms_grpc_max_message_bytes,
        metadata_notification_nats_url,
        metadata_notification_subject,
        progress_interval: Duration::from_secs(progress_interval_secs),
        stats_root,
    })
}

fn parse_completion_mode(
    value: &str,
    flag: &str,
) -> Result<CompletionMode, Box<dyn std::error::Error>> {
    match value {
        "interrupt" => Ok(CompletionMode::Interrupt),
        "hot-poll" | "hot_poll" | "hotpoll" => Ok(CompletionMode::HotPoll),
        other => Err(arg_error(format!(
            "unknown {flag} value `{other}`; use `interrupt` or `hot-poll`"
        ))),
    }
}

fn merge_endpoint_arg(endpoints: &mut Vec<String>, raw: &str, default_endpoint: &str) {
    if endpoints.len() == 1 && endpoints[0] == default_endpoint {
        endpoints.clear();
    }
    endpoints.extend(
        raw.split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
    );
}

fn merge_values_arg(values: &mut Vec<String>, raw: &str) {
    values.extend(
        raw.split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
    );
}

#[cfg(test)]
mod tests {
    use super::{parse_args, Command, ObjectBenchmarkRunMode};
    use std::time::Duration;

    fn parse_ok(args: &[&str]) -> Command {
        let argv = args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
        parse_args(argv).expect("parse should succeed")
    }

    #[test]
    fn object_benchmark_defaults_to_fixed_ops_mode() {
        let command = parse_ok(&[
            "object-benchmark",
            "--bucket",
            "lab-8p2",
            "--input",
            "/tmp/input.bin",
        ]);
        let Command::ObjectBenchmark(config) = command else {
            panic!("expected object benchmark command");
        };
        assert!(matches!(
            config.run_mode,
            ObjectBenchmarkRunMode::FixedOps { ops_per_worker: 4 }
        ));
    }

    #[test]
    fn object_benchmark_accepts_timed_mode() {
        let command = parse_ok(&[
            "object-benchmark",
            "--bucket",
            "lab-8p2",
            "--input",
            "/tmp/input.bin",
            "--duration-secs",
            "30",
            "--warmup-secs",
            "5",
            "--write-key-count",
            "1024",
        ]);
        let Command::ObjectBenchmark(config) = command else {
            panic!("expected object benchmark command");
        };
        assert!(matches!(
            config.run_mode,
            ObjectBenchmarkRunMode::Timed { .. }
        ));
        assert_eq!(config.write_key_count, Some(1024));
    }

    #[test]
    fn object_benchmark_rejects_mixed_fixed_and_timed_flags() {
        let argv = [
            "object-benchmark",
            "--bucket",
            "lab-8p2",
            "--input",
            "/tmp/input.bin",
            "--ops-per-worker",
            "8",
            "--duration-secs",
            "30",
        ]
        .iter()
        .map(|arg| arg.to_string())
        .collect::<Vec<_>>();
        let err = parse_args(argv).expect_err("parse should fail");
        assert!(err
            .to_string()
            .contains("either --ops-per-worker or --duration-secs"));
    }

    #[test]
    fn object_benchmark_accepts_multiple_kms_endpoints() {
        let command = parse_ok(&[
            "object-benchmark",
            "--kms-endpoint",
            "http://10.0.0.1:50060,http://10.0.0.2:50060",
            "--kms-endpoint",
            "http://10.0.0.3:50060",
            "--bucket",
            "lab-8p2",
            "--input",
            "/tmp/input.bin",
        ]);
        let Command::ObjectBenchmark(config) = command else {
            panic!("expected object benchmark command");
        };
        assert_eq!(
            config.kms_endpoints,
            vec![
                "http://10.0.0.1:50060".to_string(),
                "http://10.0.0.2:50060".to_string(),
                "http://10.0.0.3:50060".to_string()
            ]
        );
    }

    #[test]
    fn object_benchmark_accepts_existing_read_keys_and_cache_ttl() {
        let command = parse_ok(&[
            "object-benchmark",
            "--bucket",
            "lab-8p2",
            "--input",
            "/tmp/input.bin",
            "--write-percent",
            "0",
            "--prefill-keys",
            "0",
            "--existing-read-key",
            "bench/a.bin,bench/b.bin",
            "--read-resolve-cache-ttl-secs",
            "300",
        ]);
        let Command::ObjectBenchmark(config) = command else {
            panic!("expected object benchmark command");
        };
        assert_eq!(
            config.existing_read_keys,
            vec!["bench/a.bin".to_string(), "bench/b.bin".to_string()]
        );
        assert_eq!(config.read_resolve_cache_ttl, Duration::from_secs(300));
    }

    #[test]
    fn put_object_accepts_large_object_tuning_overrides() {
        let command = parse_ok(&[
            "put-object",
            "--bucket",
            "lab-8p2",
            "--key",
            "bench/huge.bin",
            "--input",
            "/tmp/input.bin",
            "--write-window-max-stripes",
            "8192",
            "--write-window-inflight-stripes",
            "64",
            "--kms-grpc-max-message-bytes",
            "268435456",
        ]);
        let Command::PutObject(config) = command else {
            panic!("expected put-object command");
        };
        assert_eq!(config.write_window_max_stripes, 8192);
        assert_eq!(config.write_window_inflight_stripes, 64);
        assert_eq!(config.kms_grpc_max_message_bytes, 268435456);
    }

    #[test]
    fn ec_benchmark_accepts_iterations() {
        let command = parse_ok(&[
            "ec-benchmark",
            "--input",
            "/tmp/input.bin",
            "--iterations",
            "99",
            "--warmup-iterations",
            "7",
        ]);
        let Command::EcBenchmark(config) = command else {
            panic!("expected ec benchmark command");
        };
        assert_eq!(config.iterations, 99);
        assert_eq!(config.warmup_iterations, 7);
    }
}

fn missing_value(flag: &str) -> Box<dyn Error> {
    arg_error(format!("missing value for {}", flag))
}

fn arg_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

fn smoke_usage() -> &'static str {
    concat!(
        "ksc smoke [options]\n",
        "  --endpoint <uri>       target endpoint, default http://[::1]:18080\n",
        "  --chunk-seed <u64>     deterministic starting chunk id seed, default 7\n",
        "  --slot-index <u64>     starting slot index, default 0\n",
        "  --generation <u32>     starting generation, default 1\n",
        "  --packed-count <n>     chunks per KP2 pack, default 4\n",
    )
}

fn benchmark_usage() -> &'static str {
    concat!(
        "ksc benchmark [options]\n",
        "  --endpoint <uri>         target endpoint; repeat for multi-target runs\n",
        "  --client-id <id>         runtime-tree client id, default ksc-bench\n",
        "  --transfer-mode single|packed benchmark transport shape, default single\n",
        "  --chunk-seed <u64>       deterministic chunk id seed, default 7\n",
        "  --slot-base <u64>        first slot in the benchmark keyspace, default 0\n",
        "  --generation-start <u32> initial generation for prefill, default 1\n",
        "  --packed-count <n>       chunks per KP2 pack, default 1 in single mode\n",
        "  --pack-max-payload-bytes <n>\n",
        "                           packed logical payload ceiling, default 16777216\n",
        "  --key-count <n>          live key count in the working set, default 256\n",
        "  --workers <n>            worker count / connection count, default 8\n",
        "  --inflight-streams-per-worker <n>\n",
        "                           per-connection KP2 streams kept in flight, default 8\n",
        "  --target-initial-inflight <n>\n",
        "                           initial per-target pacing cap, default workers*inflight\n",
        "  --target-min-inflight <n>\n",
        "                           minimum per-target pacing cap, default 1\n",
        "  --target-additive-increase-every <n>\n",
        "                           successful operations between pacing increases, default 256\n",
        "  --duration-secs <n>      measured run length, default 15\n",
        "  --write-percent <n>      write share 0..100, default 30\n",
        "  --stats-root <path>      runtime tree root, default /run/keinfs/ksc\n",
        "  --stats-publish-ms <n>   runtime tree publish interval, default 250\n",
        "  --avoid-overlapping-writes | --allow-overlapping-writes\n",
        "                           client-side same-key write overlap policy, default avoid\n",
        "  --cleanup | --no-cleanup delete prefilled keys after the run, default cleanup\n",
    )
}

fn ec_benchmark_usage() -> &'static str {
    concat!(
        "ksc ec-benchmark [options]\n",
        "  --input <path>              local file used as the EC payload\n",
        "  --iterations <n>            measured iterations, default 256\n",
        "  --warmup-iterations <n>     untimed warmup iterations, default 32\n",
    )
}

fn put_object_usage() -> &'static str {
    concat!(
        "ksc put-object [options]\n",
        "  --kms-endpoint <uri[,uri...]>  KMS gRPC endpoint(s), default http://127.0.0.1:50060\n",
        "  --bucket <id>         target bucket id\n",
        "  --key <name>          object key\n",
        "  --input <path>        local file to upload as a full object\n",
        "  --write-completion-mode <mode> interrupt|hot-poll, default interrupt\n",
        "  --write-window-max-stripes <n> max stripes reserved per write window, default 4096\n",
        "  --write-window-inflight-stripes <n> concurrent stripe uploads per window batch, default 16\n",
        "  --kms-grpc-max-message-bytes <n> control-plane gRPC message cap, default 134217728\n",
        "  --metadata-notification-nats-url <uri> subscribe to KMS invalidation events via NATS\n",
        "  --metadata-notification-subject <name> NATS subject for invalidations, default keinfs.kms.events\n",
    )
}

fn get_object_usage() -> &'static str {
    concat!(
        "ksc get-object [options]\n",
        "  --kms-endpoint <uri[,uri...]>  KMS gRPC endpoint(s), default http://127.0.0.1:50060\n",
        "  --bucket <id>         target bucket id\n",
        "  --key <name>          object key\n",
        "  --output <path>       local file to write\n",
        "  --read-completion-mode <mode>  interrupt|hot-poll, default interrupt\n",
        "  --kms-grpc-max-message-bytes <n> control-plane gRPC message cap, default 134217728\n",
        "  --metadata-notification-nats-url <uri> subscribe to KMS invalidation events via NATS\n",
        "  --metadata-notification-subject <name> NATS subject for invalidations, default keinfs.kms.events\n",
    )
}

fn delete_object_usage() -> &'static str {
    concat!(
        "ksc delete-object [options]\n",
        "  --kms-endpoint <uri[,uri...]>  KMS gRPC endpoint(s), default http://127.0.0.1:50060\n",
        "  --bucket <id>         target bucket id\n",
        "  --key <name>          object key\n",
        "  --version-id <id>     specific version id to delete; repeat to delete several versions\n",
        "                        if omitted, delete all committed versions under the key\n",
        "  --write-completion-mode <mode> interrupt|hot-poll, default interrupt\n",
        "  --kms-grpc-max-message-bytes <n> control-plane gRPC message cap, default 134217728\n",
        "  --metadata-notification-nats-url <uri> subscribe to KMS invalidation events via NATS\n",
        "  --metadata-notification-subject <name> NATS subject for invalidations, default keinfs.kms.events\n",
    )
}

fn object_benchmark_usage() -> &'static str {
    concat!(
        "ksc object-benchmark [options]\n",
        "  --kms-endpoint <uri[,uri...]>  KMS gRPC endpoint(s), default http://127.0.0.1:50060\n",
        "  --bucket <id>         target bucket id\n",
        "  --input <path>        local file used as the benchmark object payload\n",
        "  --workers <n>         concurrent worker count, default 4\n",
        "  --ops-per-worker <n>  fixed operation count per worker\n",
        "  --duration-secs <n>   measured steady-state duration per worker\n",
        "  --warmup-secs <n>     untimed warmup before duration mode, default 0\n",
        "  --write-percent <n>   write share 0..100, default 30\n",
        "  --prefill-keys <n>    object count prefilled for read or mixed runs, default 32\n",
        "  --existing-read-key <path[,path...]> reuse immutable existing object key(s) for reads instead of prefilling\n",
        "  --write-key-count <n> bounded write working set size; default unique write keys\n",
        "  --key-prefix <path>   object key prefix, default bench/object-benchmark\n",
        "  --key-shape <mode>    flat-root|warm-tree, default flat-root\n",
        "  --read-completion-mode <mode>  interrupt|hot-poll, default interrupt\n",
        "  --write-completion-mode <mode> interrupt|hot-poll, default interrupt\n",
        "  --read-resolve-cache-ttl-secs <n> client-side read resolve cache TTL, default 60\n",
        "  --write-window-max-stripes <n> max stripes reserved per write window, default 4096\n",
        "  --write-window-inflight-stripes <n> concurrent stripe uploads per window batch, default 16\n",
        "  --kms-grpc-max-message-bytes <n> control-plane gRPC message cap, default 134217728\n",
        "  --metadata-notification-nats-url <uri> subscribe to KMS invalidation events via NATS\n",
        "  --metadata-notification-subject <name> NATS subject for invalidations, default keinfs.kms.events\n",
        "  --verify-reads | --no-verify-reads compare read payloads to the input bytes, default verify\n",
        "  --progress-secs <n>   print a live progress line every n s during the run, default 0 (off)\n",
        "  --stats-root <dir>    write a live JSON snapshot (object-benchmark.json) to dir each progress interval\n",
    )
}

fn usage() -> &'static str {
    concat!(
        "ksc <subcommand> [options]\n",
        "  smoke      one packed KP2 write/read/delete verification pass\n",
        "  ec-benchmark isolated EC encode benchmark (legacy vs prepared path)\n",
        "  benchmark  sustained packed KP2 load generator\n",
        "  put-object object upload through KMS + KP2\n",
        "  get-object object read through KMS + KP2\n",
        "  delete-object object delete through KMS with target cleanup + allocator reclaim\n",
        "  object-benchmark concurrent object-path benchmark through KMS + KAS + KP2\n",
    )
}
