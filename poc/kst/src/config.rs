// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use kix::{ChunkMediaLayoutKind, WorkerMode};
use kp2::{max_encoded_write_request_bytes, MAX_PACK_PAYLOAD_BYTES};
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) enum Command {
    Serve(ServeConfig),
    Smoke(SmokeConfig),
}

#[derive(Clone, Debug)]
pub(crate) struct ServeConfig {
    pub(crate) listen_addr: SocketAddr,
    pub(crate) listen_backlog: u32,
    pub(crate) target_id: String,
    pub(crate) drive_id: u16,
    pub(crate) raw_device: PathBuf,
    pub(crate) raw_offset_bytes: u64,
    pub(crate) raw_offset_bytes_explicit: bool,
    pub(crate) raw_slice_bytes: Option<u64>,
    pub(crate) raw_slice_bytes_explicit: bool,
    pub(crate) media_raw_device: PathBuf,
    pub(crate) media_raw_offset_bytes: u64,
    pub(crate) media_raw_offset_bytes_explicit: bool,
    pub(crate) media_raw_slice_bytes: Option<u64>,
    pub(crate) layout_kind: ChunkMediaLayoutKind,
    pub(crate) layout_kind_explicit: bool,
    pub(crate) extent_bytes: u32,
    pub(crate) extent_bytes_explicit: bool,
    pub(crate) packed_bytes: u32,
    pub(crate) packed_bytes_explicit: bool,
    pub(crate) key_slots: Option<u64>,
    pub(crate) key_slots_explicit: bool,
    pub(crate) media_raw_slice_bytes_explicit: bool,
    pub(crate) shard_count: usize,
    pub(crate) lookup_mode: WorkerMode,
    pub(crate) commit_mode: WorkerMode,
    pub(crate) drive_mode: WorkerMode,
    pub(crate) lookup_pin_cores: Vec<usize>,
    pub(crate) commit_pin_cores: Vec<usize>,
    pub(crate) drive_pin_cores: Vec<usize>,
    pub(crate) lookup_queue_depth: usize,
    pub(crate) commit_queue_depth: usize,
    pub(crate) drive_queue_depth: usize,
    pub(crate) read_ingress_mode: WorkerMode,
    pub(crate) write_ingress_mode: WorkerMode,
    pub(crate) read_ingress_workers: usize,
    pub(crate) write_ingress_workers: usize,
    pub(crate) read_ingress_pin_cores: Vec<usize>,
    pub(crate) write_ingress_pin_cores: Vec<usize>,
    pub(crate) read_ingress_queue_depth: usize,
    pub(crate) write_ingress_queue_depth: usize,
    pub(crate) direct_read_mode: WorkerMode,
    pub(crate) direct_write_mode: WorkerMode,
    pub(crate) direct_read_workers: usize,
    pub(crate) direct_write_workers: usize,
    pub(crate) direct_read_pin_cores: Vec<usize>,
    pub(crate) direct_write_pin_cores: Vec<usize>,
    pub(crate) direct_read_queue_depth: usize,
    pub(crate) direct_write_queue_depth: usize,
    pub(crate) stats_root: PathBuf,
    pub(crate) kix_stats_root: PathBuf,
    pub(crate) stats_publish_interval: Duration,
    pub(crate) max_packed_payload_bytes: usize,
    pub(crate) max_packed_write_request_bytes: usize,
    pub(crate) max_message_bytes: usize,
    pub(crate) max_connections: usize,
    pub(crate) max_active_streams: usize,
    pub(crate) max_read_streams: usize,
    pub(crate) max_write_streams: usize,
    pub(crate) h2_initial_window_bytes: u32,
    pub(crate) h2_max_frame_bytes: u32,
    pub(crate) h2_max_header_list_bytes: u32,
    pub(crate) h2_max_concurrent_streams: u32,
    pub(crate) h2_max_send_buffer_bytes: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct SmokeConfig {
    pub(crate) endpoint: String,
    pub(crate) chunk_seed: u64,
    pub(crate) slot_index: u64,
    pub(crate) generation: u32,
}

#[derive(Clone, Copy, Debug)]
enum ModeName {
    Interrupt,
    Busy,
}

impl ModeName {
    fn worker_mode(self, spins_before_yield: usize) -> WorkerMode {
        match self {
            Self::Interrupt => WorkerMode::Interrupt,
            Self::Busy => WorkerMode::BusyPoll { spins_before_yield },
        }
    }
}

impl FromStr for ModeName {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "interrupt" => Ok(Self::Interrupt),
            "busy" => Ok(Self::Busy),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown worker mode: {other}"),
            )),
        }
    }
}

pub(crate) fn parse_args(args: Vec<String>) -> Result<Command, Box<dyn Error>> {
    let Some(subcommand) = args.first().map(String::as_str) else {
        return Err(arg_error("missing subcommand; use `serve` or `smoke`"));
    };
    match subcommand {
        "serve" => parse_serve_args(&args[1..]).map(Command::Serve),
        "smoke" => parse_smoke_args(&args[1..]).map(Command::Smoke),
        other => Err(arg_error(format!(
            "unknown subcommand `{other}`; use `serve` or `smoke`"
        ))),
    }
}

fn parse_serve_args(args: &[String]) -> Result<ServeConfig, Box<dyn Error>> {
    let mut listen_addr: SocketAddr = "[::1]:18080".parse()?;
    let mut listen_backlog = 4096_u32;
    let mut target_id = String::new();
    let mut drive_id = 0_u16;
    let mut raw_device = None;
    let mut raw_offset_bytes = 0_u64;
    let mut raw_offset_bytes_explicit = false;
    let mut raw_slice_bytes = None;
    let mut raw_slice_bytes_explicit = false;
    let mut media_raw_device = None;
    let mut media_raw_offset_bytes = 0_u64;
    let mut media_raw_offset_bytes_explicit = false;
    let mut media_raw_slice_bytes = None;
    let mut layout_kind = ChunkMediaLayoutKind::ExtentOnly;
    let mut layout_kind_explicit = false;
    let mut extent_bytes = 1024 * 1024;
    let mut extent_bytes_explicit = false;
    let mut packed_bytes = 16 * 1024;
    let mut packed_bytes_explicit = false;
    let mut key_slots = None;
    let mut key_slots_explicit = false;
    let mut media_raw_slice_bytes_explicit = false;
    let mut shard_count = 4_usize;
    let mut lookup_mode_name = ModeName::Busy;
    let mut commit_mode_name = ModeName::Interrupt;
    let mut drive_mode_name = ModeName::Interrupt;
    let mut lookup_spins_before_yield = 1024_usize;
    let mut commit_spins_before_yield = 1024_usize;
    let mut drive_spins_before_yield = 1024_usize;
    let mut lookup_pin_cores = Vec::new();
    let mut commit_pin_cores = Vec::new();
    let mut drive_pin_cores = Vec::new();
    let mut lookup_queue_depth = 4096_usize;
    let mut commit_queue_depth = 1024_usize;
    let mut drive_queue_depth = 1024_usize;
    let mut read_ingress_mode_name = ModeName::Interrupt;
    let mut write_ingress_mode_name = ModeName::Interrupt;
    let mut read_ingress_spins_before_yield = 1024_usize;
    let mut write_ingress_spins_before_yield = 1024_usize;
    let mut read_ingress_workers = 4_usize;
    let mut write_ingress_workers = 2_usize;
    let mut read_ingress_pin_cores = Vec::new();
    let mut write_ingress_pin_cores = Vec::new();
    let mut read_ingress_queue_depth = 2048_usize;
    let mut write_ingress_queue_depth = 1024_usize;
    let mut direct_read_mode_name = ModeName::Busy;
    let mut direct_write_mode_name = ModeName::Interrupt;
    let mut direct_read_spins_before_yield = 1024_usize;
    let mut direct_write_spins_before_yield = 1024_usize;
    let mut direct_read_workers = 8_usize;
    let mut direct_write_workers = 8_usize;
    let mut direct_read_pin_cores = Vec::new();
    let mut direct_write_pin_cores = Vec::new();
    let mut direct_read_queue_depth = 2048_usize;
    let mut direct_write_queue_depth = 2048_usize;
    let mut stats_root = PathBuf::from("/run/keinfs/kst");
    let mut kix_stats_root = PathBuf::from("/run/keinfs/kix");
    let mut stats_publish_ms = 250_u64;
    let mut max_message_bytes = 16 * 1024 * 1024_usize;
    let mut max_message_bytes_explicit = false;
    let mut max_connections = 4096_usize;
    let mut max_active_streams = 8192_usize;
    let mut max_read_streams = max_active_streams;
    let mut max_write_streams = max_active_streams;
    let mut h2_initial_window_bytes = 1024 * 1024_u32;
    let mut h2_max_frame_bytes = 1024 * 1024_u32;
    let mut h2_max_header_list_bytes = 32 * 1024_u32;
    let mut h2_max_concurrent_streams = 128_u32;
    let mut h2_max_send_buffer_bytes = 8 * 1024 * 1024_usize;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--listen" => {
                i += 1;
                listen_addr = args
                    .get(i)
                    .ok_or_else(|| missing_value("--listen"))?
                    .parse()?;
            }
            "--listen-backlog" => {
                i += 1;
                listen_backlog = args
                    .get(i)
                    .ok_or_else(|| missing_value("--listen-backlog"))?
                    .parse()?;
            }
            "--target-id" => {
                i += 1;
                target_id = args
                    .get(i)
                    .ok_or_else(|| missing_value("--target-id"))?
                    .clone();
            }
            "--drive-id" => {
                i += 1;
                drive_id = args
                    .get(i)
                    .ok_or_else(|| missing_value("--drive-id"))?
                    .parse()?;
            }
            "--raw-device" => {
                i += 1;
                raw_device = Some(PathBuf::from(
                    args.get(i).ok_or_else(|| missing_value("--raw-device"))?,
                ));
            }
            "--raw-offset-bytes" => {
                i += 1;
                raw_offset_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--raw-offset-bytes"))?
                    .parse()?;
                raw_offset_bytes_explicit = true;
            }
            "--raw-slice-bytes" => {
                i += 1;
                raw_slice_bytes = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--raw-slice-bytes"))?
                        .parse()?,
                );
                raw_slice_bytes_explicit = true;
            }
            "--media-raw-device" => {
                i += 1;
                media_raw_device = Some(PathBuf::from(
                    args.get(i)
                        .ok_or_else(|| missing_value("--media-raw-device"))?,
                ));
            }
            "--media-raw-offset-bytes" => {
                i += 1;
                media_raw_offset_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--media-raw-offset-bytes"))?
                    .parse()?;
                media_raw_offset_bytes_explicit = true;
            }
            "--media-raw-slice-bytes" => {
                i += 1;
                media_raw_slice_bytes = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--media-raw-slice-bytes"))?
                        .parse()?,
                );
                media_raw_slice_bytes_explicit = true;
            }
            "--record-mix" => {
                i += 1;
                layout_kind = match args.get(i).map(String::as_str) {
                    Some("extent-only") => ChunkMediaLayoutKind::ExtentOnly,
                    Some("packed-only") => ChunkMediaLayoutKind::PackedOnly,
                    Some("mixed") => ChunkMediaLayoutKind::Mixed,
                    Some(other) => {
                        return Err(arg_error(format!("unknown --record-mix value {other}")))
                    }
                    None => return Err(arg_error("missing value for --record-mix")),
                };
                layout_kind_explicit = true;
            }
            "--extent-bytes" => {
                i += 1;
                extent_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--extent-bytes"))?
                    .parse()?;
                extent_bytes_explicit = true;
            }
            "--packed-bytes" => {
                i += 1;
                packed_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--packed-bytes"))?
                    .parse()?;
                packed_bytes_explicit = true;
            }
            "--key-slots" => {
                i += 1;
                key_slots = Some(
                    args.get(i)
                        .ok_or_else(|| missing_value("--key-slots"))?
                        .parse()?,
                );
                key_slots_explicit = true;
            }
            "--shards" => {
                i += 1;
                shard_count = args
                    .get(i)
                    .ok_or_else(|| missing_value("--shards"))?
                    .parse()?;
            }
            "--lookup-mode" => {
                i += 1;
                lookup_mode_name = args
                    .get(i)
                    .ok_or_else(|| missing_value("--lookup-mode"))?
                    .parse::<ModeName>()?;
            }
            "--commit-mode" => {
                i += 1;
                commit_mode_name = args
                    .get(i)
                    .ok_or_else(|| missing_value("--commit-mode"))?
                    .parse::<ModeName>()?;
            }
            "--drive-mode" => {
                i += 1;
                drive_mode_name = args
                    .get(i)
                    .ok_or_else(|| missing_value("--drive-mode"))?
                    .parse::<ModeName>()?;
            }
            "--lookup-spins-before-yield" => {
                i += 1;
                lookup_spins_before_yield = args
                    .get(i)
                    .ok_or_else(|| missing_value("--lookup-spins-before-yield"))?
                    .parse()?;
            }
            "--commit-spins-before-yield" => {
                i += 1;
                commit_spins_before_yield = args
                    .get(i)
                    .ok_or_else(|| missing_value("--commit-spins-before-yield"))?
                    .parse()?;
            }
            "--drive-spins-before-yield" => {
                i += 1;
                drive_spins_before_yield = args
                    .get(i)
                    .ok_or_else(|| missing_value("--drive-spins-before-yield"))?
                    .parse()?;
            }
            "--lookup-pin-cores" => {
                i += 1;
                lookup_pin_cores = parse_cpu_list(
                    args.get(i)
                        .ok_or_else(|| missing_value("--lookup-pin-cores"))?,
                )?;
            }
            "--commit-pin-cores" => {
                i += 1;
                commit_pin_cores = parse_cpu_list(
                    args.get(i)
                        .ok_or_else(|| missing_value("--commit-pin-cores"))?,
                )?;
            }
            "--drive-pin-cores" => {
                i += 1;
                drive_pin_cores = parse_cpu_list(
                    args.get(i)
                        .ok_or_else(|| missing_value("--drive-pin-cores"))?,
                )?;
            }
            "--lookup-queue-depth" => {
                i += 1;
                lookup_queue_depth = args
                    .get(i)
                    .ok_or_else(|| missing_value("--lookup-queue-depth"))?
                    .parse()?;
            }
            "--commit-queue-depth" => {
                i += 1;
                commit_queue_depth = args
                    .get(i)
                    .ok_or_else(|| missing_value("--commit-queue-depth"))?
                    .parse()?;
            }
            "--drive-queue-depth" => {
                i += 1;
                drive_queue_depth = args
                    .get(i)
                    .ok_or_else(|| missing_value("--drive-queue-depth"))?
                    .parse()?;
            }
            "--read-ingress-mode" => {
                i += 1;
                read_ingress_mode_name = args
                    .get(i)
                    .ok_or_else(|| missing_value("--read-ingress-mode"))?
                    .parse::<ModeName>()?;
            }
            "--write-ingress-mode" => {
                i += 1;
                write_ingress_mode_name = args
                    .get(i)
                    .ok_or_else(|| missing_value("--write-ingress-mode"))?
                    .parse::<ModeName>()?;
            }
            "--read-ingress-spins-before-yield" => {
                i += 1;
                read_ingress_spins_before_yield = args
                    .get(i)
                    .ok_or_else(|| missing_value("--read-ingress-spins-before-yield"))?
                    .parse()?;
            }
            "--write-ingress-spins-before-yield" => {
                i += 1;
                write_ingress_spins_before_yield = args
                    .get(i)
                    .ok_or_else(|| missing_value("--write-ingress-spins-before-yield"))?
                    .parse()?;
            }
            "--read-ingress-workers" => {
                i += 1;
                read_ingress_workers = args
                    .get(i)
                    .ok_or_else(|| missing_value("--read-ingress-workers"))?
                    .parse()?;
            }
            "--write-ingress-workers" => {
                i += 1;
                write_ingress_workers = args
                    .get(i)
                    .ok_or_else(|| missing_value("--write-ingress-workers"))?
                    .parse()?;
            }
            "--read-ingress-pin-cores" => {
                i += 1;
                read_ingress_pin_cores = parse_cpu_list(
                    args.get(i)
                        .ok_or_else(|| missing_value("--read-ingress-pin-cores"))?,
                )?;
            }
            "--write-ingress-pin-cores" => {
                i += 1;
                write_ingress_pin_cores = parse_cpu_list(
                    args.get(i)
                        .ok_or_else(|| missing_value("--write-ingress-pin-cores"))?,
                )?;
            }
            "--read-ingress-queue-depth" => {
                i += 1;
                read_ingress_queue_depth = args
                    .get(i)
                    .ok_or_else(|| missing_value("--read-ingress-queue-depth"))?
                    .parse()?;
            }
            "--write-ingress-queue-depth" => {
                i += 1;
                write_ingress_queue_depth = args
                    .get(i)
                    .ok_or_else(|| missing_value("--write-ingress-queue-depth"))?
                    .parse()?;
            }
            "--direct-read-mode" => {
                i += 1;
                direct_read_mode_name = args
                    .get(i)
                    .ok_or_else(|| missing_value("--direct-read-mode"))?
                    .parse::<ModeName>()?;
            }
            "--direct-write-mode" => {
                i += 1;
                direct_write_mode_name = args
                    .get(i)
                    .ok_or_else(|| missing_value("--direct-write-mode"))?
                    .parse::<ModeName>()?;
            }
            "--direct-read-spins-before-yield" => {
                i += 1;
                direct_read_spins_before_yield = args
                    .get(i)
                    .ok_or_else(|| missing_value("--direct-read-spins-before-yield"))?
                    .parse()?;
            }
            "--direct-write-spins-before-yield" => {
                i += 1;
                direct_write_spins_before_yield = args
                    .get(i)
                    .ok_or_else(|| missing_value("--direct-write-spins-before-yield"))?
                    .parse()?;
            }
            "--direct-read-workers" => {
                i += 1;
                direct_read_workers = args
                    .get(i)
                    .ok_or_else(|| missing_value("--direct-read-workers"))?
                    .parse()?;
            }
            "--direct-write-workers" => {
                i += 1;
                direct_write_workers = args
                    .get(i)
                    .ok_or_else(|| missing_value("--direct-write-workers"))?
                    .parse()?;
            }
            "--direct-read-pin-cores" => {
                i += 1;
                direct_read_pin_cores = parse_cpu_list(
                    args.get(i)
                        .ok_or_else(|| missing_value("--direct-read-pin-cores"))?,
                )?;
            }
            "--direct-write-pin-cores" => {
                i += 1;
                direct_write_pin_cores = parse_cpu_list(
                    args.get(i)
                        .ok_or_else(|| missing_value("--direct-write-pin-cores"))?,
                )?;
            }
            "--direct-read-queue-depth" => {
                i += 1;
                direct_read_queue_depth = args
                    .get(i)
                    .ok_or_else(|| missing_value("--direct-read-queue-depth"))?
                    .parse()?;
            }
            "--direct-write-queue-depth" => {
                i += 1;
                direct_write_queue_depth = args
                    .get(i)
                    .ok_or_else(|| missing_value("--direct-write-queue-depth"))?
                    .parse()?;
            }
            "--stats-root" => {
                i += 1;
                stats_root =
                    PathBuf::from(args.get(i).ok_or_else(|| missing_value("--stats-root"))?);
            }
            "--kix-stats-root" => {
                i += 1;
                kix_stats_root = PathBuf::from(
                    args.get(i)
                        .ok_or_else(|| missing_value("--kix-stats-root"))?,
                );
            }
            "--stats-publish-ms" => {
                i += 1;
                stats_publish_ms = args
                    .get(i)
                    .ok_or_else(|| missing_value("--stats-publish-ms"))?
                    .parse()?;
            }
            "--max-message-bytes" => {
                i += 1;
                max_message_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--max-message-bytes"))?
                    .parse()?;
                max_message_bytes_explicit = true;
            }
            "--max-connections" => {
                i += 1;
                max_connections = args
                    .get(i)
                    .ok_or_else(|| missing_value("--max-connections"))?
                    .parse()?;
            }
            "--max-active-streams" => {
                i += 1;
                max_active_streams = args
                    .get(i)
                    .ok_or_else(|| missing_value("--max-active-streams"))?
                    .parse()?;
            }
            "--max-read-streams" => {
                i += 1;
                max_read_streams = args
                    .get(i)
                    .ok_or_else(|| missing_value("--max-read-streams"))?
                    .parse()?;
            }
            "--max-write-streams" => {
                i += 1;
                max_write_streams = args
                    .get(i)
                    .ok_or_else(|| missing_value("--max-write-streams"))?
                    .parse()?;
            }
            "--h2-initial-window-bytes" => {
                i += 1;
                h2_initial_window_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--h2-initial-window-bytes"))?
                    .parse()?;
            }
            "--h2-max-frame-bytes" => {
                i += 1;
                h2_max_frame_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--h2-max-frame-bytes"))?
                    .parse()?;
            }
            "--h2-max-header-list-bytes" => {
                i += 1;
                h2_max_header_list_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--h2-max-header-list-bytes"))?
                    .parse()?;
            }
            "--h2-max-concurrent-streams" => {
                i += 1;
                h2_max_concurrent_streams = args
                    .get(i)
                    .ok_or_else(|| missing_value("--h2-max-concurrent-streams"))?
                    .parse()?;
            }
            "--h2-max-send-buffer-bytes" => {
                i += 1;
                h2_max_send_buffer_bytes = args
                    .get(i)
                    .ok_or_else(|| missing_value("--h2-max-send-buffer-bytes"))?
                    .parse()?;
            }
            "--help" | "-h" => return Err(arg_error(serve_usage())),
            other => return Err(arg_error(format!("unknown serve argument `{other}`"))),
        }
        i += 1;
    }

    let raw_device =
        raw_device.ok_or_else(|| arg_error("KST needs --raw-device <block-device>"))?;
    let media_raw_device = media_raw_device.unwrap_or_else(|| raw_device.clone());
    if target_id.is_empty() {
        let basename = media_raw_device
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("target");
        target_id = format!("{basename}-d{drive_id}");
    }
    if key_slots == Some(0) {
        return Err(arg_error("KST requires --key-slots > 0"));
    }
    if shard_count == 0 {
        return Err(arg_error("KST requires --shards > 0"));
    }
    if lookup_queue_depth == 0 || commit_queue_depth == 0 || drive_queue_depth == 0 {
        return Err(arg_error("queue depths must be > 0"));
    }
    if read_ingress_workers == 0 || write_ingress_workers == 0 {
        return Err(arg_error("ingress worker counts must be > 0"));
    }
    if direct_read_workers == 0 || direct_write_workers == 0 {
        return Err(arg_error("direct execution worker counts must be > 0"));
    }
    if read_ingress_queue_depth == 0 || write_ingress_queue_depth == 0 {
        return Err(arg_error("ingress queue depths must be > 0"));
    }
    if direct_read_queue_depth == 0 || direct_write_queue_depth == 0 {
        return Err(arg_error("direct execution queue depths must be > 0"));
    }
    if listen_backlog == 0 {
        return Err(arg_error("--listen-backlog must be > 0"));
    }
    if max_message_bytes == 0 {
        return Err(arg_error("--max-message-bytes must be > 0"));
    }
    let packed_min_payload_bytes =
        smallest_packed_payload_bytes(layout_kind, extent_bytes, packed_bytes)?;
    let max_packed_write_request_bytes = max_encoded_write_request_bytes(packed_min_payload_bytes)
        .map_err(|err| {
            arg_error(format!(
                "KST could not derive the minimum KP2 packed write wire ceiling: {}",
                err
            ))
        })?;
    if max_message_bytes < max_packed_write_request_bytes {
        if max_message_bytes_explicit {
            return Err(arg_error(format!(
                "--max-message-bytes={} is too small for KP2 packed writes on this target. KST needs at least {} bytes of request-body headroom to carry {} bytes of logical payload plus KP2 framing overhead for the {} layout.",
                max_message_bytes,
                max_packed_write_request_bytes,
                MAX_PACK_PAYLOAD_BYTES,
                layout_kind.as_str(),
            )));
        }
        max_message_bytes = max_packed_write_request_bytes;
    }
    if max_connections == 0 {
        return Err(arg_error("--max-connections must be > 0"));
    }
    if max_active_streams == 0 {
        return Err(arg_error("--max-active-streams must be > 0"));
    }
    if max_read_streams == 0 {
        return Err(arg_error("--max-read-streams must be > 0"));
    }
    if max_write_streams == 0 {
        return Err(arg_error("--max-write-streams must be > 0"));
    }
    if max_read_streams > max_active_streams {
        return Err(arg_error(
            "--max-read-streams must be <= --max-active-streams",
        ));
    }
    if max_write_streams > max_active_streams {
        return Err(arg_error(
            "--max-write-streams must be <= --max-active-streams",
        ));
    }
    if h2_initial_window_bytes == 0 {
        return Err(arg_error("--h2-initial-window-bytes must be > 0"));
    }
    if !(16_384..=16_777_215).contains(&h2_max_frame_bytes) {
        return Err(arg_error(
            "--h2-max-frame-bytes must be within the HTTP/2 legal range 16384..=16777215",
        ));
    }
    if h2_max_header_list_bytes == 0 {
        return Err(arg_error("--h2-max-header-list-bytes must be > 0"));
    }
    if h2_max_concurrent_streams == 0 {
        return Err(arg_error("--h2-max-concurrent-streams must be > 0"));
    }
    if h2_max_send_buffer_bytes == 0 {
        return Err(arg_error("--h2-max-send-buffer-bytes must be > 0"));
    }

    if direct_read_pin_cores.is_empty() && !read_ingress_pin_cores.is_empty() {
        direct_read_pin_cores = read_ingress_pin_cores.clone();
    }
    if direct_write_pin_cores.is_empty() && !write_ingress_pin_cores.is_empty() {
        direct_write_pin_cores = write_ingress_pin_cores.clone();
    }

    Ok(ServeConfig {
        listen_addr,
        listen_backlog,
        target_id,
        drive_id,
        raw_device,
        raw_offset_bytes,
        raw_offset_bytes_explicit,
        raw_slice_bytes,
        raw_slice_bytes_explicit,
        media_raw_device,
        media_raw_offset_bytes,
        media_raw_offset_bytes_explicit,
        media_raw_slice_bytes,
        media_raw_slice_bytes_explicit,
        layout_kind,
        layout_kind_explicit,
        extent_bytes,
        extent_bytes_explicit,
        packed_bytes,
        packed_bytes_explicit,
        key_slots,
        key_slots_explicit,
        shard_count,
        lookup_mode: lookup_mode_name.worker_mode(lookup_spins_before_yield),
        commit_mode: commit_mode_name.worker_mode(commit_spins_before_yield),
        drive_mode: drive_mode_name.worker_mode(drive_spins_before_yield),
        lookup_pin_cores,
        commit_pin_cores,
        drive_pin_cores,
        lookup_queue_depth,
        commit_queue_depth,
        drive_queue_depth,
        read_ingress_mode: read_ingress_mode_name.worker_mode(read_ingress_spins_before_yield),
        write_ingress_mode: write_ingress_mode_name.worker_mode(write_ingress_spins_before_yield),
        read_ingress_workers,
        write_ingress_workers,
        read_ingress_pin_cores,
        write_ingress_pin_cores,
        read_ingress_queue_depth,
        write_ingress_queue_depth,
        direct_read_mode: direct_read_mode_name.worker_mode(direct_read_spins_before_yield),
        direct_write_mode: direct_write_mode_name.worker_mode(direct_write_spins_before_yield),
        direct_read_workers,
        direct_write_workers,
        direct_read_pin_cores,
        direct_write_pin_cores,
        direct_read_queue_depth,
        direct_write_queue_depth,
        stats_root,
        kix_stats_root,
        stats_publish_interval: Duration::from_millis(stats_publish_ms),
        max_packed_payload_bytes: MAX_PACK_PAYLOAD_BYTES,
        max_packed_write_request_bytes,
        max_message_bytes,
        max_connections,
        max_active_streams,
        max_read_streams,
        max_write_streams,
        h2_initial_window_bytes,
        h2_max_frame_bytes,
        h2_max_header_list_bytes,
        h2_max_concurrent_streams,
        h2_max_send_buffer_bytes,
    })
}

fn smallest_packed_payload_bytes(
    layout_kind: ChunkMediaLayoutKind,
    extent_bytes: u32,
    packed_bytes: u32,
) -> Result<usize, Box<dyn Error>> {
    let bytes = match layout_kind {
        ChunkMediaLayoutKind::ExtentOnly => extent_bytes as usize,
        ChunkMediaLayoutKind::PackedOnly => packed_bytes as usize,
        ChunkMediaLayoutKind::Mixed => (extent_bytes.min(packed_bytes)) as usize,
    };
    if bytes == 0 {
        return Err(arg_error(format!(
            "KST layout {} produced a zero-byte minimum payload, which makes no architectural sense",
            layout_kind.as_str()
        )));
    }
    Ok(bytes)
}

fn parse_smoke_args(args: &[String]) -> Result<SmokeConfig, Box<dyn Error>> {
    let mut endpoint = "http://[::1]:18080".to_string();
    let mut chunk_seed = 7_u64;
    let mut slot_index = 0_u64;
    let mut generation = 1_u32;

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
            "--help" | "-h" => return Err(arg_error(smoke_usage())),
            other => return Err(arg_error(format!("unknown smoke argument `{other}`"))),
        }
        i += 1;
    }

    Ok(SmokeConfig {
        endpoint,
        chunk_seed,
        slot_index,
        generation,
    })
}

fn parse_cpu_list(value: &str) -> Result<Vec<usize>, Box<dyn Error>> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for part in value.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((start, end)) = part.split_once('-') {
            let start: usize = start.parse()?;
            let end: usize = end.parse()?;
            if end < start {
                return Err(arg_error(format!(
                    "invalid CPU range {part}: end before start"
                )));
            }
            out.extend(start..=end);
        } else {
            out.push(part.parse()?);
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

pub(crate) fn serve_usage() -> &'static str {
    concat!(
        "kst serve --raw-device <block-device> [options]\n",
        "  --listen <addr>                  listen address, default [::1]:18080\n",
        "  --listen-backlog <count>         TCP listen backlog, default 4096\n",
        "  --target-id <id>                 human-readable target id\n",
        "  --drive-id <id>                  logical drive id, default 0\n",
        "  --raw-device <path>              raw KIX arena block device\n",
        "  --raw-offset-bytes <bytes>       raw KIX arena offset; default auto tail placement on same-device targets\n",
        "  --raw-slice-bytes <bytes>        raw KIX arena slice length; default auto-sized tail arena on same-device targets\n",
        "  --media-raw-device <path>        raw chunk-media block device; default is --raw-device\n",
        "  --media-raw-offset-bytes <bytes> raw chunk-media offset; default 0 on same-device auto-layout targets\n",
        "  --media-raw-slice-bytes <bytes>  raw chunk-media slice length; default front-of-device span or existing superblock span\n",
        "  --record-mix extent-only|packed-only|mixed\n",
        "  --extent-bytes <bytes>           extent slot payload bytes, default 1048576\n",
        "  --packed-bytes <bytes>           packed slot payload bytes, default 16384\n",
        "  --key-slots <count>              configured chunk-media slot count; default is discovered from existing media or derived from span\n",
        "  --lookup-mode interrupt|busy     default busy\n",
        "  --commit-mode interrupt|busy     default interrupt\n",
        "  --drive-mode interrupt|busy      default interrupt\n",
        "  --lookup-pin-cores <csv/range>   explicit lookup CPU pins\n",
        "  --commit-pin-cores <csv/range>   explicit commit CPU pins\n",
        "  --drive-pin-cores <csv/range>    explicit drive CPU pins\n",
        "  --read-ingress-mode interrupt|busy   default interrupt\n",
        "  --write-ingress-mode interrupt|busy  default interrupt\n",
        "  --read-ingress-spins-before-yield <n> default 1024\n",
        "  --write-ingress-spins-before-yield <n> default 1024\n",
        "  --read-ingress-workers <count>       default 4\n",
        "  --write-ingress-workers <count>      default 2\n",
        "  --read-ingress-pin-cores <csv/range> explicit read-ingress CPU pins\n",
        "  --write-ingress-pin-cores <csv/range> explicit write-ingress CPU pins\n",
        "  --read-ingress-queue-depth <count>   default 2048\n",
        "  --write-ingress-queue-depth <count>  default 1024\n",
        "  --direct-read-mode interrupt|busy    default busy\n",
        "  --direct-write-mode interrupt|busy   default interrupt\n",
        "  --direct-read-spins-before-yield <n> default 1024\n",
        "  --direct-write-spins-before-yield <n> default 1024\n",
        "  --direct-read-workers <count>        default 8\n",
        "  --direct-write-workers <count>       default 8\n",
        "  --direct-read-pin-cores <csv/range>  explicit direct-read CPU pins\n",
        "  --direct-write-pin-cores <csv/range> explicit direct-write CPU pins\n",
        "  --direct-read-queue-depth <count>    default 2048\n",
        "  --direct-write-queue-depth <count>   default 2048\n",
        "  --stats-root <path>              KST runtime root, default /run/keinfs/kst\n",
        "  --kix-stats-root <path>          embedded KIX runtime root, default /run/keinfs/kix\n",
        "  --stats-publish-ms <ms>          runtime publish interval, default 250\n",
        "  --max-message-bytes <bytes>      HTTP/2 request body limit, default 16777216\n",
        "  --max-connections <count>        target connection admission cap, default 4096\n",
        "  --max-active-streams <count>     target-wide active stream cap, default 8192\n",
        "  --max-read-streams <count>       target-wide read/control stream cap, default 8192\n",
        "  --max-write-streams <count>      target-wide write/delete stream cap, default 8192\n",
        "  --h2-initial-window-bytes <n>    HTTP/2 stream window, default 1048576\n",
        "  --h2-max-frame-bytes <n>         HTTP/2 max frame size, default 1048576\n",
        "  --h2-max-header-list-bytes <n>   HTTP/2 max header list size, default 32768\n",
        "  --h2-max-concurrent-streams <n>  per-connection concurrent stream cap, default 128\n",
        "  --h2-max-send-buffer-bytes <n>   per-stream send buffer cap, default 8388608\n"
    )
}

pub(crate) fn smoke_usage() -> &'static str {
    concat!(
        "kst smoke [options]\n",
        "  --endpoint <uri>                 target endpoint, default http://[::1]:18080\n",
        "  --chunk-seed <u64>               deterministic chunk id seed, default 7\n",
        "  --slot-index <u64>               target slot index, default 0\n",
        "  --generation <u32>               write generation, default 1\n"
    )
}

fn arg_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

fn missing_value(flag: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("missing value for {flag}"),
    )
}
