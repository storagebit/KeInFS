// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use ksc::client::CompletionMode;
use ksc::object::DEFAULT_METADATA_NOTIFICATION_SUBJECT;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_KMS_ENDPOINT: &str = "http://127.0.0.1:50060";
const DEFAULT_MOUNT_READ_COMPLETION_MODE: CompletionMode = CompletionMode::Interrupt;
const DEFAULT_MOUNT_WRITE_COMPLETION_MODE: CompletionMode = CompletionMode::HotPoll;
const DEFAULT_MODE_BENCH_READ_COMPLETION_MODE: CompletionMode = CompletionMode::Interrupt;
const DEFAULT_MODE_BENCH_WRITE_COMPLETION_MODE: CompletionMode = CompletionMode::HotPoll;

#[derive(Clone, Debug)]
pub(crate) enum Command {
    Mount(MountConfig),
    ModeBench(ModeBenchConfig),
}

#[derive(Clone, Debug)]
pub(crate) struct MountConfig {
    pub(crate) kms_endpoints: Vec<String>,
    pub(crate) namespace_id: String,
    pub(crate) bucket_id: String,
    pub(crate) mountpoint: PathBuf,
    pub(crate) read_completion_mode: CompletionMode,
    pub(crate) write_completion_mode: CompletionMode,
    pub(crate) metadata_notification_nats_url: Option<String>,
    pub(crate) metadata_notification_subject: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ModeBenchConfig {
    pub(crate) kms_endpoints: Vec<String>,
    pub(crate) bucket_id: String,
    pub(crate) input_path: PathBuf,
    pub(crate) workers: usize,
    pub(crate) warmup: Duration,
    pub(crate) duration: Duration,
    pub(crate) write_percent: u8,
    pub(crate) prefill_keys: usize,
    pub(crate) key_prefix: String,
    pub(crate) verify_reads: bool,
    pub(crate) matrix: bool,
    pub(crate) read_completion_mode: CompletionMode,
    pub(crate) write_completion_mode: CompletionMode,
    pub(crate) metadata_notification_nats_url: Option<String>,
    pub(crate) metadata_notification_subject: String,
}

pub(crate) fn parse_args(args: Vec<String>) -> Result<Command, Box<dyn Error>> {
    let Some(subcommand) = args.first().map(String::as_str) else {
        return Err(arg_error("missing subcommand; use `mount` or `mode-bench`"));
    };
    match subcommand {
        "mount" => parse_mount_args(&args[1..]).map(Command::Mount),
        "mode-bench" | "bench" => parse_mode_bench_args(&args[1..]).map(Command::ModeBench),
        "--help" | "-h" => Err(arg_error(usage())),
        other => Err(arg_error(format!(
            "unknown subcommand `{other}`; use `mount` or `mode-bench`"
        ))),
    }
}

fn parse_mount_args(args: &[String]) -> Result<MountConfig, Box<dyn Error>> {
    let mut kms_endpoints = default_kms_endpoints();
    let mut namespace_id = String::new();
    let mut bucket_id = String::new();
    let mut mountpoint = None;
    let mut read_completion_mode = DEFAULT_MOUNT_READ_COMPLETION_MODE;
    let mut write_completion_mode = DEFAULT_MOUNT_WRITE_COMPLETION_MODE;
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
                );
            }
            "--namespace-id" => {
                i += 1;
                namespace_id = args
                    .get(i)
                    .ok_or_else(|| missing_value("--namespace-id"))?
                    .clone();
            }
            "--bucket" | "--bucket-id" => {
                i += 1;
                let flag = args[i - 1].as_str();
                bucket_id = args.get(i).ok_or_else(|| missing_value(flag))?.clone();
            }
            "--mountpoint" => {
                i += 1;
                mountpoint = Some(PathBuf::from(
                    args.get(i).ok_or_else(|| missing_value("--mountpoint"))?,
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
            "--write-completion-mode" => {
                i += 1;
                write_completion_mode = parse_completion_mode(
                    args.get(i)
                        .ok_or_else(|| missing_value("--write-completion-mode"))?,
                    "--write-completion-mode",
                )?;
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
            "--help" | "-h" => return Err(arg_error(mount_usage())),
            other => return Err(arg_error(format!("unknown KFC mount argument `{other}`"))),
        }
        i += 1;
    }
    if namespace_id.is_empty() {
        return Err(arg_error("--namespace-id is required"));
    }
    if bucket_id.is_empty() {
        return Err(arg_error("--bucket or --bucket-id is required"));
    }
    Ok(MountConfig {
        kms_endpoints,
        namespace_id,
        bucket_id,
        mountpoint: mountpoint.ok_or_else(|| arg_error("--mountpoint is required"))?,
        read_completion_mode,
        write_completion_mode,
        metadata_notification_nats_url,
        metadata_notification_subject,
    })
}

fn parse_mode_bench_args(args: &[String]) -> Result<ModeBenchConfig, Box<dyn Error>> {
    let mut kms_endpoints = default_kms_endpoints();
    let mut bucket_id = String::new();
    let mut input_path = None;
    let mut workers = 8_usize;
    let mut warmup_secs = 5_u64;
    let mut duration_secs = 15_u64;
    let mut write_percent = 30_u8;
    let mut prefill_keys = 64_usize;
    let mut key_prefix = "bench/kfc-mode-bench".to_string();
    let mut verify_reads = true;
    let mut matrix = true;
    let mut read_completion_mode = DEFAULT_MODE_BENCH_READ_COMPLETION_MODE;
    let mut write_completion_mode = DEFAULT_MODE_BENCH_WRITE_COMPLETION_MODE;
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
                );
            }
            "--bucket" | "--bucket-id" => {
                i += 1;
                let flag = args[i - 1].as_str();
                bucket_id = args.get(i).ok_or_else(|| missing_value(flag))?.clone();
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
            "--warmup-secs" => {
                i += 1;
                warmup_secs = args
                    .get(i)
                    .ok_or_else(|| missing_value("--warmup-secs"))?
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
            "--prefill-keys" => {
                i += 1;
                prefill_keys = args
                    .get(i)
                    .ok_or_else(|| missing_value("--prefill-keys"))?
                    .parse()?;
            }
            "--key-prefix" => {
                i += 1;
                key_prefix = args
                    .get(i)
                    .ok_or_else(|| missing_value("--key-prefix"))?
                    .clone();
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
            "--matrix" => matrix = true,
            "--no-matrix" => matrix = false,
            "--verify-reads" => verify_reads = true,
            "--no-verify-reads" => verify_reads = false,
            "--help" | "-h" => return Err(arg_error(mode_bench_usage())),
            other => {
                return Err(arg_error(format!(
                    "unknown KFC mode-bench argument `{other}`"
                )));
            }
        }
        i += 1;
    }
    if bucket_id.is_empty() {
        return Err(arg_error("--bucket or --bucket-id is required"));
    }
    if workers == 0 {
        return Err(arg_error("--workers must be > 0"));
    }
    if duration_secs == 0 {
        return Err(arg_error("--duration-secs must be > 0"));
    }
    if write_percent > 100 {
        return Err(arg_error("--write-percent must be between 0 and 100"));
    }
    Ok(ModeBenchConfig {
        kms_endpoints,
        bucket_id,
        input_path: input_path.ok_or_else(|| arg_error("--input is required"))?,
        workers,
        warmup: Duration::from_secs(warmup_secs),
        duration: Duration::from_secs(duration_secs),
        write_percent,
        prefill_keys,
        key_prefix,
        verify_reads,
        matrix,
        read_completion_mode,
        write_completion_mode,
        metadata_notification_nats_url,
        metadata_notification_subject,
    })
}

fn parse_completion_mode(value: &str, flag: &str) -> Result<CompletionMode, Box<dyn Error>> {
    match value {
        "interrupt" => Ok(CompletionMode::Interrupt),
        "hot-poll" | "hot_poll" | "hotpoll" => Ok(CompletionMode::HotPoll),
        other => Err(arg_error(format!(
            "unknown {flag} value `{other}`; use `interrupt` or `hot-poll`"
        ))),
    }
}

fn default_kms_endpoints() -> Vec<String> {
    vec![DEFAULT_KMS_ENDPOINT.to_string()]
}

fn merge_endpoint_arg(endpoints: &mut Vec<String>, raw: &str) {
    if endpoints.len() == 1 && endpoints[0] == DEFAULT_KMS_ENDPOINT {
        endpoints.clear();
    }
    endpoints.extend(
        raw.split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
    );
}

fn missing_value(flag: &str) -> Box<dyn Error> {
    arg_error(format!("missing value for {}", flag))
}

fn arg_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

fn mount_usage() -> String {
    format!(
        "kfc mount [options]\n  --kms-endpoint <uri[,uri...]>  KMS gRPC endpoint(s), default {DEFAULT_KMS_ENDPOINT}\n  --namespace-id <id>            namespace id that owns the bucket\n  --bucket <id>                  bucket id to mount as the filesystem root\n  --bucket-id <id>               alias for --bucket, matches the rest of the CLI surface\n  --mountpoint <path>            mountpoint path\n  --read-completion-mode <mode>  interrupt|hot-poll, default {}\n  --write-completion-mode <mode> interrupt|hot-poll, default {}\n  --metadata-notification-nats-url <uri> subscribe to KMS invalidation events via NATS\n  --metadata-notification-subject <name> invalidation subject, default {}\n",
        DEFAULT_MOUNT_READ_COMPLETION_MODE.as_str(),
        DEFAULT_MOUNT_WRITE_COMPLETION_MODE.as_str(),
        DEFAULT_METADATA_NOTIFICATION_SUBJECT,
    )
}

fn mode_bench_usage() -> String {
    format!(
        "kfc mode-bench [options]\n  --kms-endpoint <uri[,uri...]>  KMS gRPC endpoint(s), default {DEFAULT_KMS_ENDPOINT}\n  --bucket <id>                  bucket id\n  --bucket-id <id>               alias for --bucket, matches the rest of the CLI surface\n  --input <path>                 local benchmark payload file\n  --workers <n>                  worker count, default 8\n  --warmup-secs <n>              warmup duration, default 5\n  --duration-secs <n>            measured duration, default 15\n  --write-percent <n>            write share 0..100, default 30\n  --prefill-keys <n>             read prefill object count, default 64\n  --key-prefix <path>            object key prefix, default bench/kfc-mode-bench\n  --matrix | --no-matrix         run all mode combinations, default matrix\n  --read-completion-mode <mode>  used when --no-matrix, default {}\n  --write-completion-mode <mode> used when --no-matrix, default {}\n  --metadata-notification-nats-url <uri> subscribe to KMS invalidation events via NATS\n  --metadata-notification-subject <name> invalidation subject, default {}\n  --verify-reads | --no-verify-reads compare reads to the input bytes, default verify\n",
        DEFAULT_MODE_BENCH_READ_COMPLETION_MODE.as_str(),
        DEFAULT_MODE_BENCH_WRITE_COMPLETION_MODE.as_str(),
        DEFAULT_METADATA_NOTIFICATION_SUBJECT,
    )
}

fn usage() -> String {
    "kfc <subcommand> [options]\n  mount       mount a KeinFS bucket through the KSC-backed FUSE client\n  mode-bench  compare interrupt vs hot-poll read/write completion modes\n".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_mount_accepts_bucket_id_alias_and_defaults() {
        let args = vec![
            "--namespace-id".to_string(),
            "lab-ns".to_string(),
            "--bucket-id".to_string(),
            "lab-8p2".to_string(),
            "--mountpoint".to_string(),
            "/tmp/kfc-mount".to_string(),
        ];

        let config = parse_mount_args(&args).expect("mount args should parse");
        assert_eq!(config.kms_endpoints, vec![DEFAULT_KMS_ENDPOINT.to_string()]);
        assert_eq!(config.namespace_id, "lab-ns");
        assert_eq!(config.bucket_id, "lab-8p2");
        assert_eq!(config.mountpoint, PathBuf::from("/tmp/kfc-mount"));
        assert_eq!(
            config.read_completion_mode,
            DEFAULT_MOUNT_READ_COMPLETION_MODE
        );
        assert_eq!(
            config.write_completion_mode,
            DEFAULT_MOUNT_WRITE_COMPLETION_MODE
        );
    }

    #[test]
    fn parse_mode_bench_accepts_bucket_id_alias_and_defaults() {
        let args = vec![
            "--bucket-id".to_string(),
            "lab-8p2".to_string(),
            "--input".to_string(),
            "/tmp/input.bin".to_string(),
        ];

        let config = parse_mode_bench_args(&args).expect("mode-bench args should parse");
        assert_eq!(config.kms_endpoints, vec![DEFAULT_KMS_ENDPOINT.to_string()]);
        assert_eq!(config.bucket_id, "lab-8p2");
        assert_eq!(config.input_path, PathBuf::from("/tmp/input.bin"));
        assert!(config.matrix);
        assert!(config.verify_reads);
        assert_eq!(
            config.read_completion_mode,
            DEFAULT_MODE_BENCH_READ_COMPLETION_MODE
        );
        assert_eq!(
            config.write_completion_mode,
            DEFAULT_MODE_BENCH_WRITE_COMPLETION_MODE
        );
    }
}
