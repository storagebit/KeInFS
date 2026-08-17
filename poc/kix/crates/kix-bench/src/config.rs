// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use kix::WorkerMode;
use std::error::Error;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub(crate) struct BenchConfig {
    pub(crate) benchmark_mode: BenchmarkMode,
    pub(crate) lookup_mode_name: ModeName,
    pub(crate) lookup_spins_before_yield: usize,
    pub(crate) commit_mode_name: ModeName,
    pub(crate) commit_spins_before_yield: usize,
    pub(crate) drive_mode_name: ModeName,
    pub(crate) drive_spins_before_yield: usize,
    pub(crate) shards: usize,
    pub(crate) threads: usize,
    pub(crate) drives: usize,
    pub(crate) ops_per_thread: usize,
    pub(crate) prefill_keys: u64,
    pub(crate) key_space: u64,
    pub(crate) write_percent: u8,
    pub(crate) checkpoint_at_end: bool,
    pub(crate) lookup_pin_cores: Vec<usize>,
    pub(crate) commit_pin_cores: Vec<usize>,
    pub(crate) drive_pin_cores: Vec<usize>,
    pub(crate) ingress_placement: IngressPlacement,
    pub(crate) ingress_queue_depth: usize,
    pub(crate) reserve_socket_cores: usize,
    pub(crate) netdevs: Vec<String>,
    pub(crate) steer_irqs: bool,
    pub(crate) stats_root: Option<PathBuf>,
    pub(crate) stats_publish_ms: u64,
    pub(crate) read_path: ReadPathMode,
    pub(crate) media_queue_depth: usize,
    pub(crate) media_read_batch_size: usize,
    pub(crate) media_write_batch_size: usize,
    pub(crate) media_flush_mode: MediaFlushMode,
    pub(crate) record_mix: RecordMix,
    pub(crate) packed_bytes: u32,
    pub(crate) extent_bytes: u32,
    pub(crate) media_raw_device: Option<PathBuf>,
    pub(crate) media_raw_offset_bytes: u64,
    pub(crate) media_raw_slice_bytes: Option<u64>,
    pub(crate) raw_device: Option<PathBuf>,
    pub(crate) raw_offset_bytes: u64,
    pub(crate) raw_slice_bytes: Option<u64>,
    pub(crate) recovery_live_entries: u64,
    pub(crate) recovery_delta_batches: usize,
    pub(crate) recovery_deltas_per_batch: usize,
    pub(crate) recovery_key_space: u64,
    pub(crate) recovery_delete_percent: u8,
    pub(crate) recovery_loops: usize,
    pub(crate) recovery_fault: RecoveryFault,
    pub(crate) recovery_auto_truncate: bool,
    pub(crate) recovery_auto_rebuild: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BenchmarkMode {
    Throughput,
    Recovery,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ModeName {
    Interrupt,
    Busy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadPathMode {
    LookupOnly,
    MediaRead,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MediaFlushMode {
    PerOp,
    PerBatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IngressPlacement {
    Direct,
    Local,
    Remote,
    Handoff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecordMix {
    Mixed,
    PackedOnly,
    ExtentOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryFault {
    None,
    TailCrc,
    ArenaHeader,
    FirstFrame,
    LaterFrame,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            benchmark_mode: BenchmarkMode::Throughput,
            lookup_mode_name: ModeName::Interrupt,
            lookup_spins_before_yield: 1024,
            commit_mode_name: ModeName::Interrupt,
            commit_spins_before_yield: 1024,
            drive_mode_name: ModeName::Interrupt,
            drive_spins_before_yield: 1024,
            shards: 4,
            threads: 4,
            drives: 1,
            ops_per_thread: 100_000,
            prefill_keys: 0,
            key_space: 250_000,
            write_percent: 30,
            checkpoint_at_end: true,
            lookup_pin_cores: Vec::new(),
            commit_pin_cores: Vec::new(),
            drive_pin_cores: Vec::new(),
            ingress_placement: IngressPlacement::Direct,
            ingress_queue_depth: 4096,
            reserve_socket_cores: 2,
            netdevs: Vec::new(),
            steer_irqs: false,
            stats_root: None,
            stats_publish_ms: 250,
            read_path: ReadPathMode::LookupOnly,
            media_queue_depth: 16,
            media_read_batch_size: 1,
            media_write_batch_size: 8,
            media_flush_mode: MediaFlushMode::PerBatch,
            record_mix: RecordMix::ExtentOnly,
            packed_bytes: 16 * 1024,
            extent_bytes: 1024 * 1024,
            media_raw_device: None,
            media_raw_offset_bytes: 0,
            media_raw_slice_bytes: None,
            raw_device: None,
            raw_offset_bytes: 0,
            raw_slice_bytes: None,
            recovery_live_entries: 20_000,
            recovery_delta_batches: 512,
            recovery_deltas_per_batch: 64,
            recovery_key_space: 40_000,
            recovery_delete_percent: 10,
            recovery_loops: 5,
            recovery_fault: RecoveryFault::None,
            recovery_auto_truncate: false,
            recovery_auto_rebuild: false,
        }
    }
}

pub(crate) fn parse_args(args: Vec<String>) -> Result<BenchConfig, Box<dyn Error>> {
    let mut config = BenchConfig::default();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--benchmark-mode" | "--scenario" => {
                i += 1;
                config.benchmark_mode = match args.get(i).map(String::as_str) {
                    Some("throughput") => BenchmarkMode::Throughput,
                    Some("recovery") => BenchmarkMode::Recovery,
                    Some(other) => return Err(format!("unknown benchmark mode: {other}").into()),
                    None => return Err("missing value for --benchmark-mode".into()),
                };
            }
            "--mode" | "--lookup-mode" => {
                i += 1;
                config.lookup_mode_name = match args.get(i).map(String::as_str) {
                    Some("interrupt") => ModeName::Interrupt,
                    Some("busy") => ModeName::Busy,
                    Some(other) => return Err(format!("unknown lookup mode: {other}").into()),
                    None => return Err("missing value for --lookup-mode".into()),
                };
            }
            "--commit-mode" => {
                i += 1;
                config.commit_mode_name = match args.get(i).map(String::as_str) {
                    Some("interrupt") => ModeName::Interrupt,
                    Some("busy") => ModeName::Busy,
                    Some(other) => return Err(format!("unknown commit mode: {other}").into()),
                    None => return Err("missing value for --commit-mode".into()),
                };
            }
            "--drive-mode" => {
                i += 1;
                config.drive_mode_name = match args.get(i).map(String::as_str) {
                    Some("interrupt") => ModeName::Interrupt,
                    Some("busy") => ModeName::Busy,
                    Some(other) => return Err(format!("unknown drive mode: {other}").into()),
                    None => return Err("missing value for --drive-mode".into()),
                };
            }
            "--shards" => {
                i += 1;
                config.shards = parse_usize(&args, i, "--shards")?;
            }
            "--threads" => {
                i += 1;
                config.threads = parse_usize(&args, i, "--threads")?;
            }
            "--drives" => {
                i += 1;
                config.drives = parse_usize(&args, i, "--drives")?;
            }
            "--ops-per-thread" => {
                i += 1;
                config.ops_per_thread = parse_usize(&args, i, "--ops-per-thread")?;
            }
            "--prefill-keys" => {
                i += 1;
                config.prefill_keys = parse_u64(&args, i, "--prefill-keys")?;
            }
            "--key-space" => {
                i += 1;
                config.key_space = parse_u64(&args, i, "--key-space")?;
            }
            "--spins-before-yield" | "--lookup-spins-before-yield" => {
                i += 1;
                config.lookup_spins_before_yield =
                    parse_usize(&args, i, "--lookup-spins-before-yield")?;
            }
            "--commit-spins-before-yield" => {
                i += 1;
                config.commit_spins_before_yield =
                    parse_usize(&args, i, "--commit-spins-before-yield")?;
            }
            "--drive-spins-before-yield" => {
                i += 1;
                config.drive_spins_before_yield =
                    parse_usize(&args, i, "--drive-spins-before-yield")?;
            }
            "--write-percent" => {
                i += 1;
                config.write_percent = parse_u8(&args, i, "--write-percent")?;
            }
            "--ingress-placement" => {
                i += 1;
                config.ingress_placement = match args.get(i).map(String::as_str) {
                    Some("direct") => IngressPlacement::Direct,
                    Some("local") => IngressPlacement::Local,
                    Some("remote") => IngressPlacement::Remote,
                    Some("handoff") => IngressPlacement::Handoff,
                    Some(other) => return Err(format!("unknown ingress placement: {other}").into()),
                    None => return Err("missing value for --ingress-placement".into()),
                };
            }
            "--ingress-queue-depth" => {
                i += 1;
                config.ingress_queue_depth = parse_usize(&args, i, "--ingress-queue-depth")?;
            }
            "--skip-checkpoint" => {
                config.checkpoint_at_end = false;
            }
            "--stats-root" => {
                i += 1;
                config.stats_root = Some(PathBuf::from(
                    args.get(i)
                        .ok_or("missing value for --stats-root")?
                        .as_str(),
                ));
            }
            "--stats-publish-ms" => {
                i += 1;
                config.stats_publish_ms = parse_u64(&args, i, "--stats-publish-ms")?;
            }
            "--read-path" => {
                i += 1;
                config.read_path = match args.get(i).map(String::as_str) {
                    Some("lookup") | Some("lookup-only") => ReadPathMode::LookupOnly,
                    Some("media") | Some("media-read") => ReadPathMode::MediaRead,
                    Some(other) => return Err(format!("unknown read path: {other}").into()),
                    None => return Err("missing value for --read-path".into()),
                };
            }
            "--media-queue-depth" => {
                i += 1;
                config.media_queue_depth = parse_usize(&args, i, "--media-queue-depth")?;
            }
            "--media-write-batch-size" => {
                i += 1;
                config.media_write_batch_size = parse_usize(&args, i, "--media-write-batch-size")?;
            }
            "--media-read-batch-size" => {
                i += 1;
                config.media_read_batch_size = parse_usize(&args, i, "--media-read-batch-size")?;
            }
            "--media-flush-mode" => {
                i += 1;
                config.media_flush_mode = match args.get(i).map(String::as_str) {
                    Some("per-op") => MediaFlushMode::PerOp,
                    Some("per-batch") => MediaFlushMode::PerBatch,
                    Some(other) => return Err(format!("unknown media flush mode: {other}").into()),
                    None => return Err("missing value for --media-flush-mode".into()),
                };
            }
            "--record-mix" => {
                i += 1;
                config.record_mix = match args.get(i).map(String::as_str) {
                    Some("mixed") => RecordMix::Mixed,
                    Some("packed") | Some("packed-only") => RecordMix::PackedOnly,
                    Some("extent") | Some("extent-only") => RecordMix::ExtentOnly,
                    Some(other) => return Err(format!("unknown record mix: {other}").into()),
                    None => return Err("missing value for --record-mix".into()),
                };
            }
            "--packed-bytes" => {
                i += 1;
                config.packed_bytes = parse_u32(&args, i, "--packed-bytes")?;
            }
            "--extent-bytes" => {
                i += 1;
                config.extent_bytes = parse_u32(&args, i, "--extent-bytes")?;
            }
            "--media-raw-device" => {
                i += 1;
                config.media_raw_device = Some(PathBuf::from(
                    args.get(i)
                        .ok_or("missing value for --media-raw-device")?
                        .as_str(),
                ));
            }
            "--media-raw-offset-bytes" => {
                i += 1;
                config.media_raw_offset_bytes = parse_u64(&args, i, "--media-raw-offset-bytes")?;
            }
            "--media-raw-slice-bytes" => {
                i += 1;
                config.media_raw_slice_bytes =
                    Some(parse_u64(&args, i, "--media-raw-slice-bytes")?);
            }
            "--raw-device" => {
                i += 1;
                config.raw_device = Some(PathBuf::from(
                    args.get(i)
                        .ok_or("missing value for --raw-device")?
                        .as_str(),
                ));
            }
            "--raw-offset-bytes" => {
                i += 1;
                config.raw_offset_bytes = parse_u64(&args, i, "--raw-offset-bytes")?;
            }
            "--raw-slice-bytes" => {
                i += 1;
                config.raw_slice_bytes = Some(parse_u64(&args, i, "--raw-slice-bytes")?);
            }
            "--recovery-live-entries" => {
                i += 1;
                config.recovery_live_entries = parse_u64(&args, i, "--recovery-live-entries")?;
            }
            "--recovery-delta-batches" => {
                i += 1;
                config.recovery_delta_batches = parse_usize(&args, i, "--recovery-delta-batches")?;
            }
            "--recovery-deltas-per-batch" => {
                i += 1;
                config.recovery_deltas_per_batch =
                    parse_usize(&args, i, "--recovery-deltas-per-batch")?;
            }
            "--recovery-key-space" => {
                i += 1;
                config.recovery_key_space = parse_u64(&args, i, "--recovery-key-space")?;
            }
            "--recovery-delete-percent" => {
                i += 1;
                config.recovery_delete_percent = parse_u8(&args, i, "--recovery-delete-percent")?;
            }
            "--recovery-loops" => {
                i += 1;
                config.recovery_loops = parse_usize(&args, i, "--recovery-loops")?;
            }
            "--recovery-fault" => {
                i += 1;
                config.recovery_fault = match args.get(i).map(String::as_str) {
                    Some("none") => RecoveryFault::None,
                    Some("tail-crc") => RecoveryFault::TailCrc,
                    Some("arena-header") => RecoveryFault::ArenaHeader,
                    Some("first-frame") => RecoveryFault::FirstFrame,
                    Some("later-frame") => RecoveryFault::LaterFrame,
                    Some(other) => return Err(format!("unknown recovery fault: {other}").into()),
                    None => return Err("missing value for --recovery-fault".into()),
                };
            }
            "--recovery-auto-truncate" => {
                config.recovery_auto_truncate = true;
            }
            "--recovery-auto-rebuild" => {
                config.recovery_auto_rebuild = true;
            }
            "--pin-cores" | "--lookup-pin-cores" | "--shard-pin-cores" => {
                i += 1;
                let raw = args.get(i).ok_or("missing value for --lookup-pin-cores")?;
                config.lookup_pin_cores = raw
                    .split(',')
                    .filter(|value| !value.is_empty())
                    .map(|value| value.parse::<usize>())
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "--commit-pin-cores" => {
                i += 1;
                let raw = args.get(i).ok_or("missing value for --commit-pin-cores")?;
                config.commit_pin_cores = raw
                    .split(',')
                    .filter(|value| !value.is_empty())
                    .map(|value| value.parse::<usize>())
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "--drive-pin-cores" => {
                i += 1;
                let raw = args.get(i).ok_or("missing value for --drive-pin-cores")?;
                config.drive_pin_cores = raw
                    .split(',')
                    .filter(|value| !value.is_empty())
                    .map(|value| value.parse::<usize>())
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "--reserve-socket-cores" => {
                i += 1;
                config.reserve_socket_cores = parse_usize(&args, i, "--reserve-socket-cores")?;
            }
            "--netdevs" => {
                i += 1;
                let raw = args.get(i).ok_or("missing value for --netdevs")?;
                config.netdevs = raw
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect();
            }
            "--steer-irqs" => {
                config.steer_irqs = true;
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
        i += 1;
    }
    Ok(config)
}

pub(crate) fn validate_config(config: &BenchConfig) -> Result<(), Box<dyn Error>> {
    if config.drives == 0 {
        return Err("--drives must be >= 1".into());
    }

    if config.key_space == 0 {
        return Err("--key-space must be >= 1".into());
    }

    if config.raw_device.is_none() {
        return Err(
            "KIX benchmarking is raw-device only. Provide --raw-device <block-device>. File-backed arena benchmarking is intentionally unsupported."
                .into(),
        );
    }

    if config.benchmark_mode == BenchmarkMode::Throughput
        && config.read_path == ReadPathMode::MediaRead
        && config.media_raw_device.is_none()
    {
        return Err(
            "KIX media-read benchmarking is raw-device only. Provide --media-raw-device <block-device>. File-backed media benchmarking is intentionally unsupported."
                .into(),
        );
    }

    if config.raw_device.is_some() && config.drives != 1 {
        return Err(format!(
            "KIX raw benchmarking no longer slices one physical device into {} virtual drives. With --raw-device, set --drives 1. If you want to benchmark multiple physical drives, run one device per benchmark process until explicit multi-device support exists.",
            config.drives
        )
        .into());
    }

    if config.benchmark_mode == BenchmarkMode::Throughput
        && config.media_raw_device.is_some()
        && config.drives != 1
    {
        return Err(format!(
            "KIX raw media benchmarking no longer slices one physical device into {} virtual drives. With --media-raw-device, set --drives 1 so one device means one drive.",
            config.drives
        )
        .into());
    }

    if config.steer_irqs
        && config.raw_device.is_none()
        && config.media_raw_device.is_none()
        && config.netdevs.is_empty()
    {
        return Err(
            "--steer-irqs needs at least one target to act on. Provide --raw-device, --media-raw-device, --netdevs, or a useful combination of them."
                .into(),
        );
    }

    validate_payload_size(config.packed_bytes, "--packed-bytes")?;
    validate_payload_size(config.extent_bytes, "--extent-bytes")?;
    if config.media_queue_depth == 0 {
        return Err("--media-queue-depth must be >= 1".into());
    }
    if config.media_write_batch_size == 0 {
        return Err("--media-write-batch-size must be >= 1".into());
    }
    if config.media_read_batch_size == 0 {
        return Err("--media-read-batch-size must be >= 1".into());
    }
    if config.media_read_batch_size > config.media_queue_depth {
        return Err(
            "--media-read-batch-size must be <= --media-queue-depth so a read wave fits inside the registered direct-I/O buffer set."
                .into(),
        );
    }
    if config.media_write_batch_size > config.media_queue_depth {
        return Err(
            "--media-write-batch-size must be <= --media-queue-depth so one write group fits into the registered direct-I/O buffer set."
                .into(),
        );
    }

    if config.benchmark_mode == BenchmarkMode::Recovery {
        validate_recovery_config(config)?;
    }

    Ok(())
}

fn validate_recovery_config(config: &BenchConfig) -> Result<(), Box<dyn Error>> {
    if config.raw_slice_bytes.is_none() {
        return Err(
            "KIX recovery benchmarking requires --raw-slice-bytes so the destructive arena test span is explicit."
                .into(),
        );
    }
    if config.recovery_live_entries == 0 {
        return Err("--recovery-live-entries must be >= 1".into());
    }
    if config.recovery_delta_batches == 0 {
        return Err("--recovery-delta-batches must be >= 1".into());
    }
    if config.recovery_deltas_per_batch == 0 {
        return Err("--recovery-deltas-per-batch must be >= 1".into());
    }
    if config.recovery_key_space < config.recovery_live_entries {
        return Err(
            "--recovery-key-space must be >= --recovery-live-entries so the checkpoint working set fits inside the modeled key domain."
                .into(),
        );
    }
    if config.recovery_loops == 0 {
        return Err("--recovery-loops must be >= 1".into());
    }
    if config.recovery_fault == RecoveryFault::ArenaHeader {
        return Err(
            "KIX currently rejects --recovery-fault arena-header on raw block devices because the header-level corruption injector is not yet trustworthy enough for benchmark use. Use --recovery-fault first-frame for rebuild-required benchmarking until that path is proven."
                .into(),
        );
    }
    if config.recovery_auto_truncate {
        match config.recovery_fault {
            RecoveryFault::TailCrc | RecoveryFault::LaterFrame => {}
            RecoveryFault::None => {
                return Err(
                    "--recovery-auto-truncate needs a tail fault to repair; use --recovery-fault tail-crc or --recovery-fault later-frame."
                        .into(),
                )
            }
            RecoveryFault::ArenaHeader | RecoveryFault::FirstFrame => {
                return Err(
                    "--recovery-auto-truncate only repairs recoverable tail damage. It cannot repair arena-header or first-frame corruption that correctly yields rebuild-required."
                        .into(),
                )
            }
        }
        if config.recovery_loops < 2 {
            return Err(
                "--recovery-auto-truncate needs --recovery-loops >= 2 so KIX can measure the damaged reopen and at least one clean reopen after truncation."
                    .into(),
            );
        }
    }
    if config.recovery_auto_rebuild {
        match config.recovery_fault {
            RecoveryFault::FirstFrame | RecoveryFault::ArenaHeader => {}
            RecoveryFault::None => {
                return Err(
                    "--recovery-auto-rebuild needs a rebuild-required fault; use --recovery-fault first-frame."
                        .into(),
                )
            }
            RecoveryFault::TailCrc | RecoveryFault::LaterFrame => {
                return Err(
                    "--recovery-auto-rebuild is only for rebuild-required faults. Tail faults should use --recovery-auto-truncate instead."
                        .into(),
                )
            }
        }
        if config.media_raw_device.is_none() {
            return Err(
                "--recovery-auto-rebuild requires --media-raw-device because rebuild must scan raw chunk media."
                    .into(),
            );
        }
        if config.recovery_loops < 2 {
            return Err(
                "--recovery-auto-rebuild needs --recovery-loops >= 2 so KIX can measure the damaged reopen and at least one clean reopen after rebuild."
                    .into(),
            );
        }
    }
    Ok(())
}

fn parse_usize(args: &[String], idx: usize, flag: &str) -> Result<usize, Box<dyn Error>> {
    Ok(args
        .get(idx)
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse()?)
}

fn parse_u64(args: &[String], idx: usize, flag: &str) -> Result<u64, Box<dyn Error>> {
    Ok(args
        .get(idx)
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse()?)
}

fn parse_u8(args: &[String], idx: usize, flag: &str) -> Result<u8, Box<dyn Error>> {
    Ok(args
        .get(idx)
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse()?)
}

fn parse_u32(args: &[String], idx: usize, flag: &str) -> Result<u32, Box<dyn Error>> {
    Ok(args
        .get(idx)
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse()?)
}

fn validate_payload_size(value: u32, flag: &str) -> Result<(), Box<dyn Error>> {
    if value == 0 {
        return Err(format!("{flag} must be > 0").into());
    }
    if value % 4096 != 0 {
        return Err(format!(
            "{flag} must be aligned to 4096 bytes so KIX can run the same workload shape on direct raw media without inventing a different record format"
        )
        .into());
    }
    Ok(())
}

impl BenchConfig {
    pub(crate) fn lookup_worker_mode(&self) -> WorkerMode {
        match self.lookup_mode_name {
            ModeName::Interrupt => WorkerMode::Interrupt,
            ModeName::Busy => WorkerMode::BusyPoll {
                spins_before_yield: self.lookup_spins_before_yield,
            },
        }
    }

    pub(crate) fn commit_worker_mode(&self) -> WorkerMode {
        match self.commit_mode_name {
            ModeName::Interrupt => WorkerMode::Interrupt,
            ModeName::Busy => WorkerMode::BusyPoll {
                spins_before_yield: self.commit_spins_before_yield,
            },
        }
    }

    pub(crate) fn drive_worker_mode(&self) -> WorkerMode {
        match self.drive_mode_name {
            ModeName::Interrupt => WorkerMode::Interrupt,
            ModeName::Busy => WorkerMode::BusyPoll {
                spins_before_yield: self.drive_spins_before_yield,
            },
        }
    }
}

impl BenchmarkMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Throughput => "throughput",
            Self::Recovery => "recovery",
        }
    }
}

impl ModeName {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Interrupt => "interrupt",
            Self::Busy => "busy",
        }
    }
}

impl ReadPathMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::LookupOnly => "lookup",
            Self::MediaRead => "media-read",
        }
    }
}

impl MediaFlushMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::PerOp => "per-op",
            Self::PerBatch => "per-batch",
        }
    }
}

impl IngressPlacement {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Local => "local",
            Self::Remote => "remote",
            Self::Handoff => "handoff",
        }
    }
}

impl RecordMix {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Mixed => "mixed",
            Self::PackedOnly => "packed-only",
            Self::ExtentOnly => "extent-only",
        }
    }
}

impl RecoveryFault {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::TailCrc => "tail-crc",
            Self::ArenaHeader => "arena-header",
            Self::FirstFrame => "first-frame",
            Self::LaterFrame => "later-frame",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn rejects_missing_raw_device() {
        let config = BenchConfig::default();
        let err = validate_config(&config).unwrap_err().to_string();
        assert!(err.contains("raw-device only"));
    }

    #[test]
    fn rejects_media_read_without_media_raw_device() {
        let mut config = BenchConfig::default();
        config.raw_device = Some(PathBuf::from("/dev/null"));
        config.read_path = ReadPathMode::MediaRead;
        let err = validate_config(&config).unwrap_err().to_string();
        assert!(err.contains("media-read benchmarking is raw-device only"));
    }

    #[test]
    fn accepts_raw_lookup_only_benchmark() {
        let mut config = BenchConfig::default();
        config.raw_device = Some(PathBuf::from("/dev/null"));
        validate_config(&config).unwrap();
    }

    #[test]
    fn accepts_recovery_benchmark_without_media_raw_device() {
        let mut config = BenchConfig::default();
        config.benchmark_mode = BenchmarkMode::Recovery;
        config.raw_device = Some(PathBuf::from("/dev/null"));
        config.raw_slice_bytes = Some(1 << 30);
        validate_config(&config).unwrap();
    }

    #[test]
    fn rejects_recovery_benchmark_without_explicit_slice() {
        let mut config = BenchConfig::default();
        config.benchmark_mode = BenchmarkMode::Recovery;
        config.raw_device = Some(PathBuf::from("/dev/null"));
        let err = validate_config(&config).unwrap_err().to_string();
        assert!(err.contains("--raw-slice-bytes"));
    }

    #[test]
    fn rejects_auto_truncate_without_recoverable_tail_fault() {
        let mut config = BenchConfig::default();
        config.benchmark_mode = BenchmarkMode::Recovery;
        config.raw_device = Some(PathBuf::from("/dev/null"));
        config.raw_slice_bytes = Some(1 << 30);
        config.recovery_auto_truncate = true;
        config.recovery_fault = RecoveryFault::FirstFrame;
        let err = validate_config(&config).unwrap_err().to_string();
        assert!(err.contains("cannot repair"));
    }

    #[test]
    fn rejects_arena_header_fault_until_the_injector_is_trustworthy() {
        let mut config = BenchConfig::default();
        config.benchmark_mode = BenchmarkMode::Recovery;
        config.raw_device = Some(PathBuf::from("/dev/null"));
        config.raw_slice_bytes = Some(1 << 30);
        config.recovery_fault = RecoveryFault::ArenaHeader;
        let err = validate_config(&config).unwrap_err().to_string();
        assert!(err.contains("not yet trustworthy"));
    }
}
