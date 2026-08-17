// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use kix::arena::preflight_drive_requirements;
use kix::{
    auto_drive_layout, detect_hardware_acceleration, device_numa_node, device_size_bytes,
    numa_node_cpu_list, online_numa_nodes, read_chunk_media_superblock, rebuild_from_chunk_media,
    ArenaIoMode, ChunkId, ChunkMediaRebuildSummary, ChunkMediaSpanConfig, DriveArena, DriveConfig,
    DriveRecovery, KixHardwareAcceleration, LocationRecord,
};
use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
struct ToolConfig {
    command: Command,
    raw_device: PathBuf,
    drive_id: u16,
    raw_offset_bytes: u64,
    raw_offset_explicit: bool,
    raw_slice_bytes: Option<u64>,
    media_raw_device: Option<PathBuf>,
    media_raw_offset_bytes: u64,
    media_raw_slice_bytes: Option<u64>,
}

#[derive(Clone, Debug)]
enum Command {
    Check {
        dump_entries: usize,
        fix: bool,
        allow_destructive_reset: bool,
    },
    Preflight,
    Format,
    Inspect {
        dump_entries: usize,
    },
    Verify,
    RepairTail,
}

#[derive(Clone)]
struct DeviceSummary {
    hardware: KixHardwareAcceleration,
    total_device_bytes: u64,
    effective_span_bytes: u64,
    span_end_bytes: u64,
    numa_node: Option<i32>,
    numa_cpu_list: Vec<usize>,
    online_numa_nodes: Vec<i32>,
    io_uring_disabled: Option<i32>,
    model: Option<String>,
    serial: Option<String>,
    logical_block_size_bytes: Option<u64>,
    physical_block_size_bytes: Option<u64>,
    minimum_io_size_bytes: Option<u64>,
    optimal_io_size_bytes: Option<u64>,
    scheduler: Option<String>,
    preflight_backend: Option<ArenaIoMode>,
    preflight_error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckState {
    Clean,
    TailCorruption,
    RebuildRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FixAction {
    None,
    RepairTail,
    RebuildFromMedia,
    DestructiveReset,
}

impl CheckState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::TailCorruption => "tail-corruption",
            Self::RebuildRequired => "rebuild-required",
        }
    }
}

impl FixAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::RepairTail => "repair-tail",
            Self::RebuildFromMedia => "rebuild-from-media",
            Self::DestructiveReset => "destructive-reset",
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_args(env::args().skip(1).collect())?;
    match &config.command {
        Command::Check {
            dump_entries,
            fix,
            allow_destructive_reset,
        } => run_check(&config, *dump_entries, *fix, *allow_destructive_reset)?,
        Command::Preflight => run_preflight(&config)?,
        Command::Format => run_format(&config)?,
        Command::Inspect { dump_entries } => run_inspect(&config, *dump_entries)?,
        Command::Verify => run_verify(&config)?,
        Command::RepairTail => run_repair_tail(&config)?,
    }
    Ok(())
}

fn run_check(
    config: &ToolConfig,
    dump_entries: usize,
    fix: bool,
    allow_destructive_reset: bool,
) -> Result<(), Box<dyn Error>> {
    let summary = collect_device_summary(config)?;
    print_common_summary(config, &summary);
    println!("command=check");
    println!("fix_requested={}", yes_no(fix));
    println!(
        "allow_destructive_reset={}",
        yes_no(allow_destructive_reset)
    );
    print_device_characteristics(&summary);
    print_preflight_status(&summary);
    print_media_span_status(config);

    if let Some(error) = &summary.preflight_error {
        println!("check_state=preflight-failed");
        println!("safe_fix_available=no");
        println!("destructive_fix_required=no");
        println!("recommended_action=fix the preflight failure before touching the arena");
        println!("result=blocked-preflight");
        return Err(format!("KIX check failed during startup preflight: {error}").into());
    }

    let drive = build_drive_config(config, summary.numa_node);
    let before = DriveArena::recover(&drive)?;
    let before_arena = if before.rebuild_required {
        None
    } else {
        Some(DriveArena::open_config(&drive)?)
    };
    let before_state = classify_recovery(&before);
    print_recovery_summary_with_prefix("before", &before, before_arena.as_ref());
    println!("check_state={}", before_state.as_str());
    println!(
        "safe_fix_available={}",
        yes_no(safe_fix_available(before_state, config))
    );
    println!(
        "destructive_fix_required={}",
        yes_no(destructive_fix_required(before_state, config))
    );
    println!(
        "recommended_action={}",
        recommended_action(
            before_state,
            config,
            summary.effective_span_bytes,
            allow_destructive_reset,
        )
    );

    if dump_entries > 0 {
        dump_recovered_entries_with_prefix("before", &before.entries, dump_entries);
    }

    if !fix {
        match before_state {
            CheckState::Clean => {
                println!("result=clean");
                return Ok(());
            }
            CheckState::TailCorruption => {
                println!("result=needs-repair");
                return Err(format!(
                    "KIX check found recoverable tail corruption. Run `kix check --raw-device {} --raw-offset-bytes {} --raw-slice-bytes {} --fix` to truncate back to replay_len.",
                    config.raw_device.display(),
                    config.raw_offset_bytes,
                    config.raw_slice_bytes.unwrap_or(summary.effective_span_bytes),
                )
                .into());
            }
            CheckState::RebuildRequired => {
                println!("result=needs-rebuild-or-reset");
                return Err(format!(
                    "{}",
                    rebuild_required_guidance(config, summary.effective_span_bytes)
                )
                .into());
            }
        }
    }

    let action = match before_state {
        CheckState::Clean => FixAction::None,
        CheckState::TailCorruption => {
            let mut arena = DriveArena::open_config(&drive)?;
            arena.truncate_to(before.replay_len)?;
            FixAction::RepairTail
        }
        CheckState::RebuildRequired => {
            if let Some(media_span) = media_span_config(config) {
                let rebuild_t0 = std::time::Instant::now();
                let rebuilt = rebuild_from_chunk_media(&media_span)?;
                print_media_rebuild_summary("rebuild", &rebuilt.summary);
                if rebuilt.summary.corrupt_headers > 0
                    || rebuilt.summary.corrupt_payloads > 0
                    || rebuilt.summary.layout_mismatches > 0
                {
                    println!("fix_action=blocked");
                    println!("result=blocked-rebuild-corruption");
                    return Err(format!(
                        "KIX check found rebuild-required state and scanned raw chunk media, but the rebuild source is not clean enough to trust: corrupt_headers={}, corrupt_payloads={}, layout_mismatches={}. Inspect the media span before rewriting the arena.",
                        rebuilt.summary.corrupt_headers,
                        rebuilt.summary.corrupt_payloads,
                        rebuilt.summary.layout_mismatches,
                    )
                    .into());
                }
                let rebuilt_recovery = DriveArena::rebuild_from_entries(
                    &drive,
                    rebuilt
                        .entries
                        .iter()
                        .map(|(&chunk_id, &record)| (chunk_id, record)),
                )?;
                print_recovery_summary_with_prefix("rebuild_checkpoint", &rebuilt_recovery, None);
                println!("rebuild_elapsed_us={}", rebuild_t0.elapsed().as_micros());
                FixAction::RebuildFromMedia
            } else {
                if !allow_destructive_reset {
                    println!("fix_action=blocked");
                    println!("result=blocked-destructive-reset-required");
                    return Err(
                        "KIX check refused to reset the arena because rebuild-required was detected and no raw chunk-media span was provided for rebuild. Re-run with --media-raw-device ... to rebuild from media, or use --allow-destructive-reset only if wiping the arena is acceptable."
                            .into(),
                    );
                }
                let _ = DriveArena::reset_config(&drive)?;
                FixAction::DestructiveReset
            }
        }
    };

    println!("fix_action={}", action.as_str());
    let after = DriveArena::recover(&drive)?;
    let after_arena = if after.rebuild_required {
        None
    } else {
        Some(DriveArena::open_config(&drive)?)
    };
    let after_state = classify_recovery(&after);
    print_recovery_summary_with_prefix("after", &after, after_arena.as_ref());
    println!("after_check_state={}", after_state.as_str());
    if dump_entries > 0 {
        dump_recovered_entries_with_prefix("after", &after.entries, dump_entries);
    }

    match after_state {
        CheckState::Clean => {
            println!(
                "result={}",
                if action == FixAction::None {
                    "noop-clean"
                } else {
                    "fixed-clean"
                }
            );
            Ok(())
        }
        CheckState::TailCorruption => {
            println!("result=repair-failed");
            Err("KIX check attempted repair, but the arena still reports tail corruption afterward.".into())
        }
        CheckState::RebuildRequired => {
            println!("result=repair-failed");
            Err("KIX check attempted repair, but the arena still reports rebuild-required afterward.".into())
        }
    }
}

fn run_preflight(config: &ToolConfig) -> Result<(), Box<dyn Error>> {
    let summary = collect_device_summary(config)?;
    print_common_summary(config, &summary);
    println!("command=preflight");
    print_device_characteristics(&summary);
    print_preflight_status(&summary);
    print_media_span_status(config);
    if let Some(error) = &summary.preflight_error {
        println!("result=preflight-failed");
        return Err(format!("KIX preflight failed: {error}").into());
    }
    println!("result=ok");
    Ok(())
}

fn run_format(config: &ToolConfig) -> Result<(), Box<dyn Error>> {
    // When no explicit geometry is given, discover the device size and lay out
    // the KIX arena at the aligned tail for maximum usable chunk-media capacity
    // -- the same split KST derives on its serve path. The operator only has to
    // point `kix format` at the raw device.
    let mut config = config.clone();
    let mut auto_layout = None;
    if !config.raw_offset_explicit
        && config.raw_slice_bytes.is_none()
        && config.media_raw_device.is_none()
    {
        let device_bytes = device_size_bytes(&config.raw_device)?;
        let layout = auto_drive_layout(device_bytes)?;
        config.raw_offset_bytes = layout.raw_offset_bytes;
        config.raw_slice_bytes = Some(layout.raw_slice_bytes);
        auto_layout = Some((device_bytes, layout));
    }
    let config = config;

    let summary = collect_device_summary(&config)?;
    if let Some(error) = &summary.preflight_error {
        return Err(format!("KIX format refused to run because preflight failed: {error}").into());
    }
    let drive = build_drive_config(&config, summary.numa_node);
    let arena = DriveArena::reset_config(&drive)?;

    print_common_summary(&config, &summary);
    println!("command=format");
    match &auto_layout {
        Some((device_bytes, layout)) => {
            println!("auto_layout=yes");
            println!("auto_device_bytes={}", device_bytes);
            println!("auto_chunk_media_span_bytes={}", layout.media_slice_bytes);
            println!("auto_arena_offset_bytes={}", layout.raw_offset_bytes);
            println!("auto_arena_span_bytes={}", layout.raw_slice_bytes);
        }
        None => println!("auto_layout=no"),
    }
    print_device_characteristics(&summary);
    print_preflight_status(&summary);
    print_media_span_status(&config);
    println!("result=formatted");
    println!("arena_write_head={}", arena.write_head());
    println!(
        "arena_fixed_span_bytes={}",
        arena
            .arena_len_bytes()
            .unwrap_or(summary.effective_span_bytes)
    );
    Ok(())
}

fn run_inspect(config: &ToolConfig, dump_entries: usize) -> Result<(), Box<dyn Error>> {
    let summary = collect_device_summary(config)?;
    let drive = build_drive_config(config, summary.numa_node);
    let recovery = DriveArena::recover(&drive)?;
    let arena = if recovery.rebuild_required {
        None
    } else {
        Some(DriveArena::open_config(&drive)?)
    };

    print_common_summary(config, &summary);
    println!("command=inspect");
    print_device_characteristics(&summary);
    print_preflight_status(&summary);
    print_media_span_status(config);
    print_recovery_summary_with_prefix("", &recovery, arena.as_ref());
    println!("check_state={}", classify_recovery(&recovery).as_str());
    if dump_entries > 0 {
        dump_recovered_entries_with_prefix("", &recovery.entries, dump_entries);
        if let Some(media_span) = media_span_config(config) {
            match rebuild_from_chunk_media(&media_span) {
                Ok(rebuilt) => {
                    println!("media_live_identities={}", rebuilt.identities.len());
                    for identity in rebuilt.identities.values().take(dump_entries) {
                        println!(
                            "media_identity object_id={} version={} stripe={} frag={}",
                            identity.object_id,
                            identity.object_version,
                            identity.stripe,
                            identity.frag
                        );
                    }
                }
                Err(err) => println!("media_rebuild_error={err}"),
            }
        }
    }
    Ok(())
}

fn run_verify(config: &ToolConfig) -> Result<(), Box<dyn Error>> {
    let summary = collect_device_summary(config)?;
    let drive = build_drive_config(config, summary.numa_node);
    let recovery = DriveArena::recover(&drive)?;
    let arena = if recovery.rebuild_required {
        None
    } else {
        Some(DriveArena::open_config(&drive)?)
    };

    print_common_summary(config, &summary);
    println!("command=verify");
    print_device_characteristics(&summary);
    print_preflight_status(&summary);
    print_media_span_status(config);
    print_recovery_summary_with_prefix("", &recovery, arena.as_ref());

    if let Some(error) = &summary.preflight_error {
        println!("result=verify-failed");
        return Err(format!("KIX verify failed during preflight: {error}").into());
    }
    if recovery.rebuild_required {
        println!("result=verify-failed");
        if let Some(media_span) = media_span_config(config) {
            let rebuilt = rebuild_from_chunk_media(&media_span)?;
            print_media_rebuild_summary("verify_rebuild", &rebuilt.summary);
            return Err(format!(
                "KIX arena verification failed because the arena reports rebuild-required. A raw chunk-media span was provided and scanned; if corrupt_headers={}, corrupt_payloads={}, and layout_mismatches={} stay at zero, `kix check --fix` can rebuild the arena from media.",
                rebuilt.summary.corrupt_headers,
                rebuilt.summary.corrupt_payloads,
                rebuilt.summary.layout_mismatches,
            )
            .into());
        }
        return Err(
            "KIX arena verification failed: the first usable frame is gone or invalid, so the span correctly reports rebuild-required. Provide --media-raw-device ... if you want KIX to dry-run or perform rebuild-from-media."
                .into(),
        );
    }
    if recovery.tail_corruption {
        println!("result=verify-failed");
        return Err(format!(
            "KIX arena verification failed: the tail is corrupted after replay_len={} ({}). Earlier frames are still usable. Run `kix check --raw-device {} --raw-offset-bytes {} --raw-slice-bytes {} --fix` if truncating the tail matches your intent.",
            recovery.replay_len,
            format_binary_bytes(recovery.replay_len),
            config.raw_device.display(),
            config.raw_offset_bytes,
            config.raw_slice_bytes.unwrap_or(summary.effective_span_bytes),
        )
        .into());
    }

    println!("result=verified-clean");
    Ok(())
}

fn run_repair_tail(config: &ToolConfig) -> Result<(), Box<dyn Error>> {
    let summary = collect_device_summary(config)?;
    if let Some(error) = &summary.preflight_error {
        return Err(
            format!("KIX tail repair refused to run because preflight failed: {error}").into(),
        );
    }
    let drive = build_drive_config(config, summary.numa_node);
    let before = DriveArena::recover(&drive)?;

    print_common_summary(config, &summary);
    println!("command=repair-tail");
    print_device_characteristics(&summary);
    print_preflight_status(&summary);
    print_media_span_status(config);
    print_recovery_summary_with_prefix("before", &before, None);

    if before.rebuild_required {
        println!("result=repair-blocked");
        return Err(
            "KIX tail repair refused to run because the arena already reports rebuild-required. Tail truncation only makes sense for recoverable tail damage, not for a dead first frame."
                .into(),
        );
    }

    if !before.tail_corruption {
        println!("result=noop-clean");
        println!("message=no tail corruption detected; nothing to repair");
        return Ok(());
    }

    let mut arena = DriveArena::open_config(&drive)?;
    arena.truncate_to(before.replay_len)?;
    let after = DriveArena::recover(&drive)?;

    print_recovery_summary_with_prefix("after", &after, None);

    if after.rebuild_required || after.tail_corruption {
        println!("result=repair-failed");
        return Err(
            "KIX tail repair wrote the truncate point but the arena still did not reopen cleanly. Stop here and inspect the raw span; this is no longer a simple torn-tail case."
                .into(),
        );
    }

    println!("result=repaired");
    Ok(())
}

fn collect_device_summary(config: &ToolConfig) -> Result<DeviceSummary, Box<dyn Error>> {
    validate_raw_device(&config.raw_device)?;
    if let Some(media_raw_device) = &config.media_raw_device {
        validate_raw_device(media_raw_device)?;
    }
    let total_device_bytes = device_size_bytes(&config.raw_device)?;
    if config.raw_offset_bytes > total_device_bytes {
        return Err(format!(
            "raw offset {} exceeds device size {} for {}",
            config.raw_offset_bytes,
            total_device_bytes,
            config.raw_device.display()
        )
        .into());
    }
    let effective_span_bytes = match config.raw_slice_bytes {
        Some(slice) => {
            if slice == 0 {
                return Err("--raw-slice-bytes must be > 0".into());
            }
            let end = config
                .raw_offset_bytes
                .checked_add(slice)
                .ok_or("raw offset + slice overflowed the device layout")?;
            if end > total_device_bytes {
                return Err(format!(
                    "raw span {} + {} exceeds device size {} for {}",
                    config.raw_offset_bytes,
                    slice,
                    total_device_bytes,
                    config.raw_device.display()
                )
                .into());
            }
            slice
        }
        None => total_device_bytes - config.raw_offset_bytes,
    };
    let span_end_bytes = config
        .raw_offset_bytes
        .checked_add(effective_span_bytes)
        .ok_or("raw offset + effective span overflowed the device layout")?;
    let hardware = detect_hardware_acceleration();
    let numa_node = device_numa_node(&config.raw_device)?;
    let numa_cpu_list = numa_node
        .and_then(|node| numa_node_cpu_list(node).ok())
        .unwrap_or_default();
    let online_numa_nodes = online_numa_nodes().unwrap_or_default();
    let io_uring_disabled = read_proc_i32("/proc/sys/kernel/io_uring_disabled");
    let device_name = block_device_name(&config.raw_device)?;
    let model = read_sysfs_string(&format!("/sys/class/block/{device_name}/device/model"));
    let serial = read_sysfs_string(&format!("/sys/class/block/{device_name}/device/serial"));
    let logical_block_size_bytes = read_sysfs_u64(&format!(
        "/sys/class/block/{device_name}/queue/logical_block_size"
    ));
    let physical_block_size_bytes = read_sysfs_u64(&format!(
        "/sys/class/block/{device_name}/queue/physical_block_size"
    ));
    let minimum_io_size_bytes = read_sysfs_u64(&format!(
        "/sys/class/block/{device_name}/queue/minimum_io_size"
    ));
    let optimal_io_size_bytes = read_sysfs_u64(&format!(
        "/sys/class/block/{device_name}/queue/optimal_io_size"
    ));
    let scheduler = read_sysfs_string(&format!("/sys/class/block/{device_name}/queue/scheduler"))
        .map(|raw| {
            raw.split_whitespace()
                .find_map(|token| {
                    token
                        .strip_prefix('[')
                        .and_then(|token| token.strip_suffix(']'))
                })
                .map(str::to_string)
                .unwrap_or(raw)
        });
    let preflight = preflight_drive_requirements(&build_drive_config(config, numa_node));
    let (preflight_backend, preflight_error) = match preflight {
        Ok(mode) => (Some(mode), None),
        Err(err) => (None, Some(err.to_string())),
    };

    Ok(DeviceSummary {
        hardware,
        total_device_bytes,
        effective_span_bytes,
        span_end_bytes,
        numa_node,
        numa_cpu_list,
        online_numa_nodes,
        io_uring_disabled,
        model,
        serial,
        logical_block_size_bytes,
        physical_block_size_bytes,
        minimum_io_size_bytes,
        optimal_io_size_bytes,
        scheduler,
        preflight_backend,
        preflight_error,
    })
}

fn build_drive_config(config: &ToolConfig, numa_node: Option<i32>) -> DriveConfig {
    DriveConfig {
        id: config.drive_id,
        arena_path: config.raw_device.clone(),
        arena_offset_bytes: config.raw_offset_bytes,
        arena_len_bytes: config.raw_slice_bytes,
        numa_node,
        io_mode: ArenaIoMode::DirectUring,
    }
}

fn validate_raw_device(path: &Path) -> Result<(), Box<dyn Error>> {
    let metadata = fs::metadata(path).map_err(|err| {
        format!(
            "KIX could not stat {}: {}. Check the path and your permissions.",
            path.display(),
            err
        )
    })?;
    if !metadata.file_type().is_block_device() {
        return Err(format!(
            "{} is not a raw block device. The KIX maintenance path is intentionally raw-device only.",
            path.display()
        )
        .into());
    }
    Ok(())
}

fn classify_recovery(recovery: &DriveRecovery) -> CheckState {
    if recovery.rebuild_required {
        CheckState::RebuildRequired
    } else if recovery.tail_corruption {
        CheckState::TailCorruption
    } else {
        CheckState::Clean
    }
}

fn recommended_action(
    state: CheckState,
    config: &ToolConfig,
    effective_span_bytes: u64,
    allow_destructive_reset: bool,
) -> String {
    match state {
        CheckState::Clean => "none".to_string(),
        CheckState::TailCorruption => format!(
            "run `kix check --raw-device {} --raw-offset-bytes {} --raw-slice-bytes {} --fix`",
            config.raw_device.display(),
            config.raw_offset_bytes,
            config.raw_slice_bytes.unwrap_or(effective_span_bytes),
        ),
        CheckState::RebuildRequired if config.media_raw_device.is_some() => format!(
            "run `{}`",
            rebuild_command(config, effective_span_bytes)
        ),
        CheckState::RebuildRequired if allow_destructive_reset => format!(
            "run `kix check --raw-device {} --raw-offset-bytes {} --raw-slice-bytes {} --fix --allow-destructive-reset` only if wiping the arena is acceptable",
            config.raw_device.display(),
            config.raw_offset_bytes,
            config.raw_slice_bytes.unwrap_or(effective_span_bytes),
        ),
        CheckState::RebuildRequired => rebuild_required_guidance(config, effective_span_bytes),
    }
}

fn safe_fix_available(state: CheckState, config: &ToolConfig) -> bool {
    match state {
        CheckState::Clean => false,
        CheckState::TailCorruption => true,
        CheckState::RebuildRequired => media_span_config(config).is_some(),
    }
}

fn destructive_fix_required(state: CheckState, config: &ToolConfig) -> bool {
    matches!(state, CheckState::RebuildRequired) && media_span_config(config).is_none()
}

fn print_common_summary(config: &ToolConfig, summary: &DeviceSummary) {
    println!("raw_device={}", config.raw_device.display());
    println!("drive_id={}", config.drive_id);
    println!("raw_offset_bytes={}", config.raw_offset_bytes);
    println!("raw_slice_bytes={}", summary.effective_span_bytes);
    println!("raw_span_end_bytes={}", summary.span_end_bytes);
    println!("raw_device_bytes={}", summary.total_device_bytes);
    println!(
        "raw_device_bytes_human={}",
        format_binary_bytes(summary.total_device_bytes)
    );
    println!(
        "raw_slice_bytes_human={}",
        format_binary_bytes(summary.effective_span_bytes)
    );
    println!(
        "media_raw_device={}",
        config
            .media_raw_device
            .as_ref()
            .map(|value| value.display().to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    println!("media_raw_offset_bytes={}", config.media_raw_offset_bytes);
    println!(
        "media_raw_slice_bytes={}",
        config
            .media_raw_slice_bytes
            .map(|value| value.to_string())
            .unwrap_or_else(|| "auto".to_string())
    );
    println!("kix_io_alignment_bytes=4096");
    println!("desired_arena_backend=direct-uring");
}

fn media_span_config(config: &ToolConfig) -> Option<ChunkMediaSpanConfig> {
    Some(ChunkMediaSpanConfig {
        media_path: config.media_raw_device.clone()?,
        media_offset_bytes: config.media_raw_offset_bytes,
        media_len_bytes: config.media_raw_slice_bytes,
    })
}

fn rebuild_command(config: &ToolConfig, effective_span_bytes: u64) -> String {
    format!(
        "kix check --raw-device {} --raw-offset-bytes {} --raw-slice-bytes {} --media-raw-device {} --media-raw-offset-bytes {}{} --fix",
        config.raw_device.display(),
        config.raw_offset_bytes,
        config.raw_slice_bytes.unwrap_or(effective_span_bytes),
        config
            .media_raw_device
            .as_deref()
            .expect("media_raw_device must exist for rebuild command")
            .display(),
        config.media_raw_offset_bytes,
        config
            .media_raw_slice_bytes
            .map(|value| format!(" --media-raw-slice-bytes {value}"))
            .unwrap_or_default(),
    )
}

fn rebuild_required_guidance(config: &ToolConfig, effective_span_bytes: u64) -> String {
    if config.media_raw_device.is_some() {
        format!(
            "KIX check found rebuild-required state. A raw chunk-media span was provided, so `--fix` will attempt rebuild-from-media before it falls back to anything destructive. Run `{}`.",
            rebuild_command(config, effective_span_bytes)
        )
    } else {
        format!(
            "KIX check found rebuild-required state. Provide --media-raw-device ... so KIX can rebuild from chunk media, or run `kix check --raw-device {} --raw-offset-bytes {} --raw-slice-bytes {} --fix --allow-destructive-reset` only if wiping the arena is acceptable.",
            config.raw_device.display(),
            config.raw_offset_bytes,
            config.raw_slice_bytes.unwrap_or(effective_span_bytes),
        )
    }
}

fn print_media_rebuild_summary(prefix: &str, summary: &ChunkMediaRebuildSummary) {
    println!("{prefix}_scanned_slots={}", summary.scanned_slots);
    println!("{prefix}_live_entries={}", summary.live_entries);
    println!("{prefix}_tombstones={}", summary.tombstones);
    println!("{prefix}_empty_slots={}", summary.empty_slots);
    println!("{prefix}_corrupt_headers={}", summary.corrupt_headers);
    println!("{prefix}_corrupt_payloads={}", summary.corrupt_payloads);
    println!("{prefix}_layout_mismatches={}", summary.layout_mismatches);
}

fn print_device_characteristics(summary: &DeviceSummary) {
    println!("cpu_arch={}", summary.hardware.cpu_arch);
    println!("crc32_backend={}", summary.hardware.crc32_backend.as_str());
    println!(
        "crc32_accelerated={}",
        yes_no(summary.hardware.crc32_accelerated())
    );
    println!("crc32_detail={}", summary.hardware.crc32_detail());
    println!(
        "kernel_io_uring_disabled={}",
        summary
            .io_uring_disabled
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!(
        "raw_device_numa_node={}",
        summary
            .numa_node
            .map(|node| node.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!(
        "raw_device_numa_cpus={}",
        join_usize_csv(&summary.numa_cpu_list)
    );
    println!(
        "online_numa_nodes={}",
        join_i32_csv(&summary.online_numa_nodes)
    );
    println!(
        "raw_device_model={}",
        summary.model.as_deref().unwrap_or("unknown")
    );
    println!(
        "raw_device_serial={}",
        summary.serial.as_deref().unwrap_or("unknown")
    );
    println!(
        "logical_block_size_bytes={}",
        option_u64(summary.logical_block_size_bytes)
    );
    println!(
        "physical_block_size_bytes={}",
        option_u64(summary.physical_block_size_bytes)
    );
    println!(
        "minimum_io_size_bytes={}",
        option_u64(summary.minimum_io_size_bytes)
    );
    println!(
        "optimal_io_size_bytes={}",
        option_u64(summary.optimal_io_size_bytes)
    );
    println!(
        "block_scheduler={}",
        summary.scheduler.as_deref().unwrap_or("unknown")
    );
}

fn print_preflight_status(summary: &DeviceSummary) {
    println!("preflight_ok={}", yes_no(summary.preflight_error.is_none()));
    println!(
        "arena_backend={}",
        summary
            .preflight_backend
            .map(|mode| mode.as_str())
            .unwrap_or("unavailable")
    );
    if let Some(error) = &summary.preflight_error {
        println!("preflight_error={}", sanitize_value(error));
    }
}

fn print_media_span_status(config: &ToolConfig) {
    let Some(media_span) = media_span_config(config) else {
        println!("media_superblock_present=no");
        return;
    };
    match read_chunk_media_superblock(&media_span) {
        Ok(superblock) => {
            println!("media_superblock_present=yes");
            println!(
                "media_layout_kind={}",
                superblock.layout.layout_kind.as_str()
            );
            println!("media_extent_bytes={}", superblock.layout.extent_bytes);
            println!("media_packed_bytes={}", superblock.layout.packed_bytes);
            println!("media_key_slots={}", superblock.layout.key_slots);
            println!("media_span_bytes={}", superblock.media_span_bytes);
        }
        Err(err) => {
            println!("media_superblock_present=no");
            println!(
                "media_superblock_error={}",
                sanitize_value(&err.to_string())
            );
        }
    }
}

fn print_recovery_summary_with_prefix(
    prefix: &str,
    recovery: &DriveRecovery,
    arena: Option<&DriveArena>,
) {
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}_")
    };
    println!(
        "{}rebuild_required={}",
        prefix,
        yes_no(recovery.rebuild_required)
    );
    println!(
        "{}tail_corruption={}",
        prefix,
        yes_no(recovery.tail_corruption)
    );
    println!("{}replay_len={}", prefix, recovery.replay_len);
    println!(
        "{}replay_len_human={}",
        prefix,
        format_binary_bytes(recovery.replay_len)
    );
    println!("{}applied_frames={}", prefix, recovery.applied_frames);
    println!("{}live_entries={}", prefix, recovery.entries.len());
    println!(
        "{}entries_digest=0x{:016x}",
        prefix,
        digest_entries(&recovery.entries)
    );
    if let Some(arena) = arena {
        println!("{}arena_write_head={}", prefix, arena.write_head());
        println!(
            "{}arena_fixed_span_bytes={}",
            prefix,
            arena.arena_len_bytes().unwrap_or(0)
        );
        println!(
            "{}arena_offset_bytes={}",
            prefix,
            arena.arena_offset_bytes()
        );
        println!(
            "{}arena_is_block_device={}",
            prefix,
            yes_no(arena.is_block_device())
        );
        println!(
            "{}arena_replay_gap_bytes={}",
            prefix,
            arena.write_head().saturating_sub(recovery.replay_len)
        );
    }
}

fn dump_recovered_entries_with_prefix(
    prefix: &str,
    entries: &std::collections::HashMap<ChunkId, LocationRecord>,
    limit: usize,
) {
    let mut rows = entries.iter().collect::<Vec<_>>();
    rows.sort_unstable_by(|left, right| left.0 .0.cmp(&right.0 .0));
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}_")
    };
    for (idx, (chunk_id, record)) in rows.into_iter().take(limit).enumerate() {
        println!("{prefix}entry[{idx}].chunk_id={}", chunk_id_hex(chunk_id));
        println!("{prefix}entry[{idx}].drive_id={}", record.drive_id);
        println!(
            "{prefix}entry[{idx}].location_kind={:?}",
            record.location_kind
        );
        println!(
            "{prefix}entry[{idx}].physical_offset={}",
            record.physical_offset
        );
        println!(
            "{prefix}entry[{idx}].logical_length={}",
            record.logical_length
        );
        println!(
            "{prefix}entry[{idx}].stored_length={}",
            record.stored_length
        );
        println!("{prefix}entry[{idx}].generation={}", record.generation);
        println!("{prefix}entry[{idx}].checksum=0x{:08x}", record.checksum);
    }
}

fn digest_entries(entries: &std::collections::HashMap<ChunkId, LocationRecord>) -> u64 {
    let mut rows = entries
        .iter()
        .map(|(chunk_id, record)| (*chunk_id, *record))
        .collect::<Vec<_>>();
    rows.sort_unstable_by(|left, right| left.0 .0.cmp(&right.0 .0));
    let mut digest = 0xcbf2_9ce4_8422_2325_u64;
    for (chunk_id, record) in rows {
        for byte in chunk_id.0 {
            digest ^= u64::from(byte);
            digest = digest.wrapping_mul(0x1000_0000_01b3);
        }
        for byte in record.encode() {
            digest ^= u64::from(byte);
            digest = digest.wrapping_mul(0x1000_0000_01b3);
        }
    }
    digest
}

fn block_device_name(path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(path
        .file_name()
        .ok_or_else(|| format!("{} has no block-device file name", path.display()))?
        .to_string_lossy()
        .into_owned())
}

fn read_sysfs_string(path: &str) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_sysfs_u64(path: &str) -> Option<u64> {
    read_sysfs_string(path).and_then(|raw| raw.parse::<u64>().ok())
}

fn read_proc_i32(path: &str) -> Option<i32> {
    read_sysfs_string(path).and_then(|raw| raw.parse::<i32>().ok())
}

fn sanitize_value(value: &str) -> String {
    value.replace('\n', " ").replace('\r', " ")
}

fn chunk_id_hex(chunk_id: &ChunkId) -> String {
    let mut out = String::with_capacity(64);
    for byte in &chunk_id.0 {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn option_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn join_usize_csv(values: &[usize]) -> String {
    if values.is_empty() {
        "unknown".to_string()
    } else {
        values
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn join_i32_csv(values: &[i32]) -> String {
    if values.is_empty() {
        "unknown".to_string()
    } else {
        values
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
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

fn parse_args(args: Vec<String>) -> Result<ToolConfig, Box<dyn Error>> {
    if args.is_empty() {
        return Err(usage().into());
    }

    let command = match args[0].as_str() {
        "check" => Command::Check {
            dump_entries: 0,
            fix: false,
            allow_destructive_reset: false,
        },
        "preflight" => Command::Preflight,
        "format" => Command::Format,
        "inspect" => Command::Inspect { dump_entries: 0 },
        "verify" => Command::Verify,
        "repair-tail" => Command::RepairTail,
        "--help" | "-h" | "help" => return Err(usage().into()),
        other => return Err(format!("unknown subcommand `{other}`\n\n{}", usage()).into()),
    };

    let mut config = ToolConfig {
        command,
        raw_device: PathBuf::new(),
        drive_id: 0,
        raw_offset_bytes: 0,
        raw_offset_explicit: false,
        raw_slice_bytes: None,
        media_raw_device: None,
        media_raw_offset_bytes: 0,
        media_raw_slice_bytes: None,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--raw-device" => {
                i += 1;
                config.raw_device = PathBuf::from(
                    args.get(i)
                        .ok_or("missing value for --raw-device")?
                        .as_str(),
                );
            }
            "--drive-id" => {
                i += 1;
                config.drive_id = parse_u16(&args, i, "--drive-id")?;
            }
            "--raw-offset-bytes" => {
                i += 1;
                config.raw_offset_bytes = parse_u64(&args, i, "--raw-offset-bytes")?;
                config.raw_offset_explicit = true;
            }
            "--raw-slice-bytes" => {
                i += 1;
                config.raw_slice_bytes = Some(parse_u64(&args, i, "--raw-slice-bytes")?);
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
            "--dump-entries" => {
                i += 1;
                let dump_entries = parse_usize(&args, i, "--dump-entries")?;
                match &mut config.command {
                    Command::Check {
                        dump_entries: value,
                        ..
                    }
                    | Command::Inspect {
                        dump_entries: value,
                    } => *value = dump_entries,
                    _ => {
                        return Err(
                            "--dump-entries is only valid with the `check` or `inspect` subcommand"
                                .into(),
                        )
                    }
                }
            }
            "--fix" => match &mut config.command {
                Command::Check { fix, .. } => *fix = true,
                _ => return Err("--fix is only valid with the `check` subcommand".into()),
            },
            "--allow-destructive-reset" => match &mut config.command {
                Command::Check {
                    allow_destructive_reset,
                    ..
                } => *allow_destructive_reset = true,
                _ => {
                    return Err(
                        "--allow-destructive-reset is only valid with the `check` subcommand"
                            .into(),
                    )
                }
            },
            "--help" | "-h" => return Err(usage().into()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage()).into()),
        }
        i += 1;
    }

    if config.raw_device.as_os_str().is_empty() {
        return Err(format!(
            "missing required --raw-device <block-device>\n\n{}",
            usage()
        )
        .into());
    }

    Ok(config)
}

fn parse_u16(args: &[String], idx: usize, flag: &str) -> Result<u16, Box<dyn Error>> {
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

fn parse_usize(args: &[String], idx: usize, flag: &str) -> Result<usize, Box<dyn Error>> {
    Ok(args
        .get(idx)
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse()?)
}

fn usage() -> String {
    [
        "kix: local raw-device KIX maintenance utility",
        "",
        "Subcommands:",
        "  check         comprehensive raw-device inspection; optional repair/reset actions",
        "  preflight     verify raw-device/direct-I/O requirements",
        "  format        reset the KIX arena header/span on a raw device",
        "  inspect       recover and report arena state; optional --dump-entries N",
        "  verify        fail nonzero if the arena reports tail corruption or rebuild-required",
        "  repair-tail   truncate a recoverable torn/corrupt tail to replay_len",
        "",
        "Common flags:",
        "  --raw-device <block-device>      raw block device path",
        "  --drive-id <n>                   logical drive id (default: 0)",
        "  --raw-offset-bytes <bytes>       arena offset inside the raw device (default: 0)",
        "  --raw-slice-bytes <bytes>        fixed arena span; defaults to the remaining device tail",
        "  --media-raw-device <block-device> optional raw chunk-media device for rebuild-from-media",
        "  --media-raw-offset-bytes <bytes> chunk-media span offset inside the raw device (default: 0)",
        "  --media-raw-slice-bytes <bytes>  fixed chunk-media span; defaults to the remaining device tail",
        "",
        "Check / inspect flags:",
        "  --dump-entries <n>               dump the first N recovered entries in sorted order",
        "",
        "Check-only flags:",
        "  --fix                            apply the safest available repair path",
        "  --allow-destructive-reset        allow `check --fix` to wipe and reinitialize a rebuild-required arena",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_check_fix_flags() {
        let config = parse_args(vec![
            "check".into(),
            "--raw-device".into(),
            "/dev/nvme0n1".into(),
            "--fix".into(),
            "--allow-destructive-reset".into(),
            "--dump-entries".into(),
            "5".into(),
        ])
        .unwrap();
        match config.command {
            Command::Check {
                dump_entries,
                fix,
                allow_destructive_reset,
            } => {
                assert_eq!(dump_entries, 5);
                assert!(fix);
                assert!(allow_destructive_reset);
            }
            _ => panic!("expected check command"),
        }
    }

    #[test]
    fn parses_inspect_dump_entries() {
        let config = parse_args(vec![
            "inspect".into(),
            "--raw-device".into(),
            "/dev/nvme0n1".into(),
            "--dump-entries".into(),
            "5".into(),
        ])
        .unwrap();
        match config.command {
            Command::Inspect { dump_entries } => assert_eq!(dump_entries, 5),
            _ => panic!("expected inspect command"),
        }
    }

    #[test]
    fn rejects_dump_entries_on_verify() {
        let err = parse_args(vec![
            "verify".into(),
            "--raw-device".into(),
            "/dev/nvme0n1".into(),
            "--dump-entries".into(),
            "5".into(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("only valid"));
    }

    #[test]
    fn rejects_fix_on_non_check_command() {
        let err = parse_args(vec![
            "verify".into(),
            "--raw-device".into(),
            "/dev/nvme0n1".into(),
            "--fix".into(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("only valid"));
    }

    #[test]
    fn requires_raw_device() {
        let err = parse_args(vec!["preflight".into()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing required --raw-device"));
    }

    #[test]
    fn classifies_recovery_states() {
        let mut recovery = DriveRecovery {
            drive_id: 0,
            entries: Default::default(),
            replay_len: 0,
            tail_corruption: false,
            rebuild_required: false,
            applied_frames: 0,
        };
        assert_eq!(classify_recovery(&recovery), CheckState::Clean);
        recovery.tail_corruption = true;
        assert_eq!(classify_recovery(&recovery), CheckState::TailCorruption);
        recovery.rebuild_required = true;
        assert_eq!(classify_recovery(&recovery), CheckState::RebuildRequired);
    }
}
