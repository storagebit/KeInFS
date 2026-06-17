// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

mod config;
mod execution;
mod ingress;
mod service;
mod stats;

use config::{parse_args, Command, ServeConfig};
use execution::{spawn_direct_execution_workers, DirectExecutionConfig, DirectExecutionKind};
use ingress::{spawn_ingress_workers, IngressConfig, IngressKind};
use keinbuild::build_info;
use kix::{
    device_numa_node, device_size_bytes, planned_chunk_media_span_bytes,
    read_chunk_media_superblock, ArenaIoMode, ChunkMediaHandle, ChunkMediaLayoutSpec,
    ChunkMediaSpanConfig, ChunkMediaWriteConfig, DriveConfig, KixConfig, KixEngine, KixStatsConfig,
    CHUNK_MEDIA_PUBLICATION_LANES,
};
use service::{build_slot_publications, run_smoke, serve_connection, TargetRouter, TargetState};
use stats::{spawn_stats_publisher, TargetIdentity, TargetRuntimeStats, TargetStatsConfig};
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::Semaphore;

const LAYOUT_ALIGNMENT_BYTES: u64 = 4096;
const AUTO_KIX_ARENA_MIN_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const AUTO_KIX_ARENA_MAX_BYTES: u64 = 256 * 1024 * 1024 * 1024;
const AUTO_KIX_ARENA_FRACTION_DIVISOR: u64 = 50;
const AUTO_KIX_MIN_MEDIA_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedTargetDriveLayout {
    raw_offset_bytes: u64,
    raw_slice_bytes: u64,
    media_offset_bytes: u64,
    media_slice_bytes: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let command = parse_args(args)?;
    match command {
        Command::Serve(config) => serve(config).await?,
        Command::Smoke(config) => {
            run_smoke(
                &config.endpoint,
                config.chunk_seed,
                config.slot_index,
                config.generation,
            )
            .await?;
        }
    }
    Ok(())
}

async fn serve(config: ServeConfig) -> Result<(), Box<dyn Error>> {
    let h2_initial_connection_window_bytes = derive_h2_initial_connection_window_bytes(
        config.h2_initial_window_bytes,
        config.h2_max_concurrent_streams,
    );
    let raw_device = canonical_device_path(&config.raw_device)?;
    let media_device = canonical_device_path(&config.media_raw_device)?;
    if raw_device != media_device {
        return Err(boxed_invalid_input(format!(
            "KST requires KIX arena and chunk media to live on the same physical drive. Got arena device {} and media device {}.",
            raw_device.display(),
            media_device.display()
        )));
    }
    let layout = resolve_target_drive_layout(&config, &raw_device, &media_device)?;
    let (media_layout, media_slice_bytes) = resolve_chunk_media_layout(
        &config,
        &media_device,
        layout.media_offset_bytes,
        layout.media_slice_bytes,
    )?;

    let numa_node = device_numa_node(&raw_device)?;
    let (
        lookup_pin_cores,
        commit_pin_cores,
        drive_pin_cores,
        read_ingress_pin_cores,
        write_ingress_pin_cores,
        direct_read_pin_cores,
        direct_write_pin_cores,
    ) = resolve_worker_pins(&config, numa_node)?;

    let drive_config = DriveConfig {
        id: config.drive_id,
        arena_path: raw_device.clone(),
        arena_offset_bytes: layout.raw_offset_bytes,
        arena_len_bytes: Some(layout.raw_slice_bytes),
        numa_node,
        io_mode: ArenaIoMode::DirectUring,
    };
    let kix_config = KixConfig {
        shard_count: config.shard_count,
        lookup_worker_mode: config.lookup_mode,
        commit_worker_mode: config.commit_mode,
        drive_worker_mode: config.drive_mode,
        drive_configs: vec![drive_config],
        lookup_pin_cores: lookup_pin_cores.clone(),
        commit_pin_cores: commit_pin_cores.clone(),
        drive_pin_cores: drive_pin_cores.clone(),
        shard_numa_node: numa_node,
        lookup_queue_depth: config.lookup_queue_depth,
        commit_queue_depth: config.commit_queue_depth,
        drive_queue_depth: config.drive_queue_depth,
        stats: Some(KixStatsConfig {
            root_dir: config.kix_stats_root.clone(),
            publish_interval: config.stats_publish_interval,
        }),
    };
    let engine = Arc::new(KixEngine::open(kix_config)?);
    let rebuild_required_drives = engine.rebuild_required_drives();
    if rebuild_required_drives.contains(&config.drive_id) {
        return Err(boxed_invalid_input(format!(
            "KST refuses to start on drive {} because KIX reports rebuild_required. Run `kix check --fix` on {} before starting the target.",
            config.drive_id,
            raw_device.display()
        )));
    }

    let media_config = ChunkMediaWriteConfig {
        drive_id: config.drive_id,
        span: ChunkMediaSpanConfig {
            media_path: media_device.clone(),
            media_offset_bytes: layout.media_offset_bytes,
            media_len_bytes: Some(media_slice_bytes),
        },
        layout: media_layout,
    };
    let media = ChunkMediaHandle::open(&media_config)?;

    let hardware = engine.hardware_acceleration();
    let target_identity = TargetIdentity {
        build: build_info!(),
        target_id: config.target_id.clone(),
        listen_addr: config.listen_addr.to_string(),
        listen_backlog: config.listen_backlog,
        pid: std::process::id(),
        drive_id: config.drive_id,
        raw_device: raw_device.display().to_string(),
        raw_offset_bytes: layout.raw_offset_bytes,
        raw_slice_bytes: layout.raw_slice_bytes,
        media_device: media_device.display().to_string(),
        media_offset_bytes: layout.media_offset_bytes,
        media_slice_bytes,
        layout_kind: media_layout.layout_kind.as_str().to_string(),
        extent_bytes: media_layout.extent_bytes,
        packed_bytes: media_layout.packed_bytes,
        key_slots: media_layout.key_slots,
        publication_lanes: CHUNK_MEDIA_PUBLICATION_LANES,
        numa_node,
        shard_count: config.shard_count,
        lookup_mode: worker_mode_name(config.lookup_mode).to_string(),
        commit_mode: worker_mode_name(config.commit_mode).to_string(),
        drive_mode: worker_mode_name(config.drive_mode).to_string(),
        lookup_pin_cores,
        commit_pin_cores,
        drive_pin_cores,
        lookup_queue_depth: config.lookup_queue_depth,
        commit_queue_depth: config.commit_queue_depth,
        drive_queue_depth: config.drive_queue_depth,
        read_ingress_mode: worker_mode_name(config.read_ingress_mode).to_string(),
        write_ingress_mode: worker_mode_name(config.write_ingress_mode).to_string(),
        read_ingress_workers: config.read_ingress_workers,
        write_ingress_workers: config.write_ingress_workers,
        read_ingress_pin_cores: read_ingress_pin_cores.clone(),
        write_ingress_pin_cores: write_ingress_pin_cores.clone(),
        read_ingress_queue_depth: config.read_ingress_queue_depth,
        write_ingress_queue_depth: config.write_ingress_queue_depth,
        direct_read_mode: worker_mode_name(config.direct_read_mode).to_string(),
        direct_write_mode: worker_mode_name(config.direct_write_mode).to_string(),
        direct_read_workers: config.direct_read_workers,
        direct_write_workers: config.direct_write_workers,
        direct_read_pin_cores: direct_read_pin_cores.clone(),
        direct_write_pin_cores: direct_write_pin_cores.clone(),
        direct_read_queue_depth: config.direct_read_queue_depth,
        direct_write_queue_depth: config.direct_write_queue_depth,
        max_packed_payload_bytes: config.max_packed_payload_bytes,
        max_packed_write_request_bytes: config.max_packed_write_request_bytes,
        max_request_body_bytes: config.max_message_bytes,
        max_connections: config.max_connections,
        max_active_streams: config.max_active_streams,
        max_read_streams: config.max_read_streams,
        max_write_streams: config.max_write_streams,
        h2_initial_window_bytes: config.h2_initial_window_bytes,
        h2_initial_connection_window_bytes,
        h2_max_frame_bytes: config.h2_max_frame_bytes,
        h2_max_header_list_bytes: config.h2_max_header_list_bytes,
        h2_max_concurrent_streams: config.h2_max_concurrent_streams,
        h2_max_send_buffer_bytes: config.h2_max_send_buffer_bytes,
        cpu_arch: String::new(),
        crc32_backend: String::new(),
        crc32_accelerated: false,
        crc32_detail: String::new(),
        rebuild_required: false,
        target_stats_runtime_dir: String::new(),
        kix_stats_runtime_dir: engine
            .stats_runtime_dir()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
    };
    let runtime_stats = TargetRuntimeStats::new(target_identity, hardware, engine.stats_handle());
    let mut stats_publisher = spawn_stats_publisher(
        Arc::clone(&runtime_stats),
        &TargetStatsConfig {
            root_dir: config.stats_root.clone(),
            publish_interval: config.stats_publish_interval,
        },
    )?;

    let slot_publications = build_slot_publications(
        media.layout(),
        media.layout().key_slots,
        engine.snapshot_entries(),
    )?;

    let router = Arc::new(TargetRouter {
        _engine: Arc::clone(&engine),
        client: engine.client(),
        media,
        stats: Arc::clone(&runtime_stats),
        slot_publications: Arc::new(slot_publications),
    });
    let read_ingress = spawn_ingress_workers(
        Arc::clone(&router),
        IngressConfig {
            kind: IngressKind::Read,
            mode: config.read_ingress_mode,
            worker_count: config.read_ingress_workers,
            queue_depth: config.read_ingress_queue_depth,
            pin_cores: read_ingress_pin_cores.clone(),
        },
    );
    let write_ingress = spawn_ingress_workers(
        Arc::clone(&router),
        IngressConfig {
            kind: IngressKind::Write,
            mode: config.write_ingress_mode,
            worker_count: config.write_ingress_workers,
            queue_depth: config.write_ingress_queue_depth,
            pin_cores: write_ingress_pin_cores.clone(),
        },
    );
    let direct_read_execution = spawn_direct_execution_workers(
        Arc::clone(&router),
        DirectExecutionConfig {
            kind: DirectExecutionKind::Read,
            mode: config.direct_read_mode,
            worker_count: config.direct_read_workers,
            queue_depth: config.direct_read_queue_depth,
            pin_cores: direct_read_pin_cores.clone(),
        },
    );
    let direct_write_execution = spawn_direct_execution_workers(
        Arc::clone(&router),
        DirectExecutionConfig {
            kind: DirectExecutionKind::Write,
            mode: config.direct_write_mode,
            worker_count: config.direct_write_workers,
            queue_depth: config.direct_write_queue_depth,
            pin_cores: direct_write_pin_cores.clone(),
        },
    );

    let state = Arc::new(TargetState {
        router,
        max_request_body_bytes: config.max_message_bytes,
        max_active_streams: config.max_active_streams,
        active_stream_limit: Arc::new(Semaphore::new(config.max_active_streams)),
        max_read_streams: config.max_read_streams,
        read_stream_limit: Arc::new(Semaphore::new(config.max_read_streams)),
        max_write_streams: config.max_write_streams,
        write_stream_limit: Arc::new(Semaphore::new(config.max_write_streams)),
        read_ingress,
        write_ingress,
        direct_read_execution,
        direct_write_execution,
        h2_initial_window_bytes: config.h2_initial_window_bytes,
        h2_initial_connection_window_bytes,
        h2_max_frame_bytes: config.h2_max_frame_bytes,
        h2_max_header_list_bytes: config.h2_max_header_list_bytes,
        h2_max_concurrent_streams: config.h2_max_concurrent_streams,
        h2_max_send_buffer_bytes: config.h2_max_send_buffer_bytes,
    });

    println!(
        concat!(
            "kst_target_id={}\n",
            "kst_listen_addr={}\n",
            "kst_drive_id={}\n",
            "kst_raw_device={}\n",
            "kst_raw_slice={}+{}\n",
            "kst_media_slice={}+{}\n",
            "kst_publication_lanes={}\n",
            "kst_numa_node={}\n",
            "kst_target_stats_runtime_dir={}\n",
            "kst_kix_stats_runtime_dir={}\n",
            "kst_listen_backlog={}\n",
            "kst_max_connections={}\n",
            "kst_max_active_streams={}\n",
            "kst_max_read_streams={}\n",
            "kst_max_write_streams={}\n",
            "kst_h2_max_concurrent_streams={}\n",
            "kst_h2_initial_connection_window_bytes={}\n",
            "kst_read_ingress_mode={}\n",
            "kst_write_ingress_mode={}\n",
            "kst_read_ingress_workers={}\n",
            "kst_write_ingress_workers={}\n",
            "kst_direct_read_mode={}\n",
            "kst_direct_write_mode={}\n",
            "kst_direct_read_workers={}\n",
            "kst_direct_write_workers={}\n",
            "kst_data_plane=http2-raw-h2\n",
            "kst_control_plane_rule=grpc-management-only\n"
        ),
        config.target_id,
        config.listen_addr,
        config.drive_id,
        raw_device.display(),
        layout.raw_offset_bytes,
        layout.raw_slice_bytes,
        layout.media_offset_bytes,
        media_slice_bytes,
        CHUNK_MEDIA_PUBLICATION_LANES,
        numa_node
            .map(|node| node.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        stats_publisher.runtime_dir.display(),
        engine
            .stats_runtime_dir()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        config.listen_backlog,
        config.max_connections,
        config.max_active_streams,
        config.max_read_streams,
        config.max_write_streams,
        config.h2_max_concurrent_streams,
        h2_initial_connection_window_bytes,
        worker_mode_name(config.read_ingress_mode),
        worker_mode_name(config.write_ingress_mode),
        config.read_ingress_workers,
        config.write_ingress_workers,
        worker_mode_name(config.direct_read_mode),
        worker_mode_name(config.direct_write_mode),
        config.direct_read_workers,
        config.direct_write_workers,
    );

    let listener = bind_listener(config.listen_addr, config.listen_backlog)?;
    let connection_limit = Arc::new(Semaphore::new(config.max_connections));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((socket, peer_addr)) => {
                        if let Err(err) = socket.set_nodelay(true) {
                            runtime_stats.record_background_error(format!(
                                "KST accepted {} but could not enable TCP_NODELAY: {}",
                                peer_addr, err
                            ));
                        }
                        let permit = match Arc::clone(&connection_limit).try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                runtime_stats.record_connection_rejected();
                                runtime_stats.record_background_error(format!(
                                    "KST rejected connection from {} because active connections hit the configured ceiling of {}",
                                    peer_addr, config.max_connections
                                ));
                                drop(socket);
                                continue;
                            }
                        };
                        let connection_started = runtime_stats.begin_connection();
                        let state = Arc::clone(&state);
                        tokio::spawn(async move {
                            let _connection_permit = permit;
                            if let Err(err) = serve_connection(socket, Arc::clone(&state)).await {
                                state.router.stats.record_background_error(format!(
                                    "KST connection from {} terminated with an HTTP/2 error: {}",
                                    peer_addr, err
                                ));
                            }
                            state.router.stats.finish_connection(connection_started);
                        });
                    }
                    Err(err) => {
                        runtime_stats.record_accept_error(format!(
                            "KST listener on {} failed to accept a connection: {}",
                            config.listen_addr, err
                        ));
                    }
                }
            }
        }
    }

    if let Some(stop_tx) = stats_publisher.stop_tx.take() {
        let _ = stop_tx.send(());
    }
    if let Some(join) = stats_publisher.join.take() {
        let _ = join.join();
    }
    Ok(())
}

fn derive_h2_initial_connection_window_bytes(
    h2_initial_window_bytes: u32,
    h2_max_concurrent_streams: u32,
) -> u32 {
    // Hyper/h2 enforces a signed 31-bit connection window limit, not full u32.
    const H2_PROTOCOL_MAX_WINDOW_SIZE: u64 = (1_u64 << 31) - 1;
    let desired = u64::from(h2_initial_window_bytes)
        .saturating_mul(u64::from(h2_max_concurrent_streams.max(1)));
    desired
        .min(H2_PROTOCOL_MAX_WINDOW_SIZE)
        .max(u64::from(h2_initial_window_bytes)) as u32
}

fn bind_listener(addr: std::net::SocketAddr, backlog: u32) -> io::Result<TcpListener> {
    let socket = match addr {
        std::net::SocketAddr::V4(_) => TcpSocket::new_v4()?,
        std::net::SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(backlog)
}

fn canonical_device_path(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    Ok(std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()))
}

fn resolve_target_drive_layout(
    config: &ServeConfig,
    raw_device: &Path,
    media_device: &Path,
) -> Result<ResolvedTargetDriveLayout, Box<dyn Error>> {
    if should_auto_layout_same_device(config, raw_device, media_device) {
        return auto_same_device_drive_layout(device_size_bytes(raw_device)?);
    }

    let raw_slice_bytes =
        resolve_slice_bytes(raw_device, config.raw_offset_bytes, config.raw_slice_bytes)?;
    let media_slice_bytes = resolve_slice_bytes(
        media_device,
        config.media_raw_offset_bytes,
        config.media_raw_slice_bytes,
    )?;
    ensure_non_overlapping(
        config.raw_offset_bytes,
        raw_slice_bytes,
        config.media_raw_offset_bytes,
        media_slice_bytes,
    )?;
    Ok(ResolvedTargetDriveLayout {
        raw_offset_bytes: config.raw_offset_bytes,
        raw_slice_bytes,
        media_offset_bytes: config.media_raw_offset_bytes,
        media_slice_bytes,
    })
}

fn should_auto_layout_same_device(
    config: &ServeConfig,
    raw_device: &Path,
    media_device: &Path,
) -> bool {
    raw_device == media_device
        && !config.raw_offset_bytes_explicit
        && !config.raw_slice_bytes_explicit
        && !config.media_raw_offset_bytes_explicit
        && !config.media_raw_slice_bytes_explicit
}

fn auto_same_device_drive_layout(
    device_bytes: u64,
) -> Result<ResolvedTargetDriveLayout, Box<dyn Error>> {
    ensure_aligned(device_bytes, "device size")?;
    if device_bytes <= AUTO_KIX_MIN_MEDIA_BYTES + LAYOUT_ALIGNMENT_BYTES {
        return Err(boxed_invalid_input(format!(
            "KST auto-layout needs more than {} bytes to leave room for both chunk media and a KIX arena; got {} bytes",
            AUTO_KIX_MIN_MEDIA_BYTES + LAYOUT_ALIGNMENT_BYTES,
            device_bytes,
        )));
    }

    let raw_slice_bytes = auto_kix_arena_slice_bytes(device_bytes)?;
    let media_slice_bytes = align_down(
        device_bytes.checked_sub(raw_slice_bytes).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "KST auto-layout underflow")
        })?,
        LAYOUT_ALIGNMENT_BYTES,
    );
    if media_slice_bytes == 0 {
        return Err(boxed_invalid_input(
            "KST auto-layout produced a zero-byte chunk-media span, which would be impressively useless",
        ));
    }
    if media_slice_bytes < AUTO_KIX_MIN_MEDIA_BYTES {
        return Err(boxed_invalid_input(format!(
            "KST auto-layout only left {} bytes for chunk media; need at least {} bytes",
            media_slice_bytes, AUTO_KIX_MIN_MEDIA_BYTES
        )));
    }
    let raw_offset_bytes = device_bytes.checked_sub(raw_slice_bytes).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "KST auto-layout raw offset underflow",
        )
    })?;
    ensure_non_overlapping(raw_offset_bytes, raw_slice_bytes, 0, media_slice_bytes)?;
    Ok(ResolvedTargetDriveLayout {
        raw_offset_bytes,
        raw_slice_bytes,
        media_offset_bytes: 0,
        media_slice_bytes,
    })
}

fn auto_kix_arena_slice_bytes(device_bytes: u64) -> Result<u64, Box<dyn Error>> {
    let proportional = align_down(
        device_bytes / AUTO_KIX_ARENA_FRACTION_DIVISOR,
        LAYOUT_ALIGNMENT_BYTES,
    );
    let max_allowed = align_down(
        device_bytes
            .checked_sub(AUTO_KIX_MIN_MEDIA_BYTES)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "KST auto-layout max arena underflow",
                )
            })?,
        LAYOUT_ALIGNMENT_BYTES,
    );
    if max_allowed == 0 {
        return Err(boxed_invalid_input(
            "KST auto-layout could not reserve any aligned bytes for the KIX arena",
        ));
    }
    let desired = proportional
        .max(AUTO_KIX_ARENA_MIN_BYTES)
        .min(AUTO_KIX_ARENA_MAX_BYTES)
        .min(max_allowed);
    if desired == 0 {
        return Err(boxed_invalid_input(
            "KST auto-layout derived a zero-byte KIX arena, which would be a neat trick and a terrible idea",
        ));
    }
    Ok(desired)
}

fn resolve_slice_bytes(
    device: &Path,
    offset_bytes: u64,
    configured_len: Option<u64>,
) -> Result<u64, Box<dyn Error>> {
    ensure_aligned(offset_bytes, "offset")?;
    let device_bytes = device_size_bytes(device)?;
    if offset_bytes >= device_bytes {
        return Err(boxed_invalid_input(format!(
            "KST offset {} exceeds device size {} on {}",
            offset_bytes,
            device_bytes,
            device.display()
        )));
    }
    let span = configured_len.unwrap_or(device_bytes - offset_bytes);
    if span == 0 {
        return Err(boxed_invalid_input("KST raw spans must be > 0 bytes"));
    }
    ensure_aligned(span, "slice length")?;
    if offset_bytes
        .checked_add(span)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "KST slice overflow"))?
        > device_bytes
    {
        return Err(boxed_invalid_input(format!(
            "KST span {}+{} exceeds device size {} on {}",
            offset_bytes,
            span,
            device_bytes,
            device.display()
        )));
    }
    Ok(span)
}

fn ensure_non_overlapping(
    raw_offset: u64,
    raw_len: u64,
    media_offset: u64,
    media_len: u64,
) -> Result<(), Box<dyn Error>> {
    let raw_end = raw_offset
        .checked_add(raw_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "raw KIX slice overflow"))?;
    let media_end = media_offset.checked_add(media_len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "raw chunk-media slice overflow",
        )
    })?;
    let overlaps = raw_offset < media_end && media_offset < raw_end;
    if overlaps {
        return Err(boxed_invalid_input(format!(
            "KST raw KIX slice {}+{} overlaps raw chunk-media slice {}+{}. One drive is one target, not one excuse for overlapping metadata and payload spans.",
            raw_offset, raw_len, media_offset, media_len
        )));
    }
    Ok(())
}

fn ensure_aligned(value: u64, label: &str) -> Result<(), Box<dyn Error>> {
    if value % LAYOUT_ALIGNMENT_BYTES != 0 {
        return Err(boxed_invalid_input(format!(
            "KST {} {} must be aligned to {} bytes",
            label, value, LAYOUT_ALIGNMENT_BYTES
        )));
    }
    Ok(())
}

fn align_down(value: u64, alignment_bytes: u64) -> u64 {
    value - (value % alignment_bytes)
}

fn resolve_worker_pins(
    config: &ServeConfig,
    numa_node: Option<i32>,
) -> Result<
    (
        Vec<usize>,
        Vec<usize>,
        Vec<usize>,
        Vec<usize>,
        Vec<usize>,
        Vec<usize>,
        Vec<usize>,
    ),
    Box<dyn Error>,
> {
    let available = if let Some(node) = numa_node {
        match kix::numa_node_cpu_list(node) {
            Ok(cpus) if !cpus.is_empty() => cpus,
            Ok(_) | Err(_) => all_online_cores()?,
        }
    } else {
        all_online_cores()?
    };
    if available.is_empty() {
        return Err(boxed_other(
            "KST could not discover any online CPU cores for worker placement",
        ));
    }

    let lookup = if config.lookup_pin_cores.is_empty() {
        assign_wrapped(&available, 0, config.shard_count)
    } else {
        config.lookup_pin_cores.clone()
    };
    let commit = if config.commit_pin_cores.is_empty() {
        assign_wrapped(&available, config.shard_count, config.shard_count)
    } else {
        config.commit_pin_cores.clone()
    };
    let drive = if config.drive_pin_cores.is_empty() {
        assign_wrapped(&available, config.shard_count * 2, 1)
    } else {
        config.drive_pin_cores.clone()
    };
    let read_ingress = if config.read_ingress_pin_cores.is_empty() {
        assign_wrapped(
            &available,
            config.shard_count * 2 + 1,
            config.read_ingress_workers,
        )
    } else {
        config.read_ingress_pin_cores.clone()
    };
    let write_ingress = if config.write_ingress_pin_cores.is_empty() {
        assign_wrapped(
            &available,
            config.shard_count * 2 + 1 + config.read_ingress_workers,
            config.write_ingress_workers,
        )
    } else {
        config.write_ingress_pin_cores.clone()
    };
    let direct_read = if config.direct_read_pin_cores.is_empty() {
        assign_wrapped(
            &available,
            config.shard_count * 2 + 1 + config.read_ingress_workers + config.write_ingress_workers,
            config.direct_read_workers,
        )
    } else {
        config.direct_read_pin_cores.clone()
    };
    let direct_write = if config.direct_write_pin_cores.is_empty() {
        assign_wrapped(
            &available,
            config.shard_count * 2
                + 1
                + config.read_ingress_workers
                + config.write_ingress_workers
                + config.direct_read_workers,
            config.direct_write_workers,
        )
    } else {
        config.direct_write_pin_cores.clone()
    };
    Ok((
        lookup,
        commit,
        drive,
        read_ingress,
        write_ingress,
        direct_read,
        direct_write,
    ))
}

fn all_online_cores() -> Result<Vec<usize>, Box<dyn Error>> {
    let cores = core_affinity::get_core_ids()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "KST could not enumerate CPU cores on this host",
            )
        })?
        .into_iter()
        .map(|core| core.id)
        .collect::<Vec<_>>();
    Ok(cores)
}

fn assign_wrapped(available: &[usize], start: usize, count: usize) -> Vec<usize> {
    if available.is_empty() || count == 0 {
        return Vec::new();
    }
    (0..count)
        .map(|idx| available[(start + idx) % available.len()])
        .collect()
}

fn worker_mode_name(mode: kix::WorkerMode) -> &'static str {
    match mode {
        kix::WorkerMode::Interrupt => "interrupt",
        kix::WorkerMode::BusyPoll { .. } => "busy",
    }
}

fn resolve_chunk_media_layout(
    config: &ServeConfig,
    media_device: &Path,
    media_offset_bytes: u64,
    requested_media_slice_bytes: u64,
) -> Result<(ChunkMediaLayoutSpec, u64), Box<dyn Error>> {
    let probe_span = ChunkMediaSpanConfig {
        media_path: media_device.to_path_buf(),
        media_offset_bytes,
        media_len_bytes: Some(requested_media_slice_bytes),
    };
    match read_chunk_media_superblock(&probe_span) {
        Ok(superblock) => {
            ensure_media_layout_matches_superblock(
                config,
                media_device,
                media_offset_bytes,
                superblock,
            )?;
            Ok((superblock.layout, superblock.media_span_bytes))
        }
        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
            let key_slots = match config.key_slots {
                Some(value) => value,
                None => derive_max_key_slots(
                    requested_media_slice_bytes,
                    config.layout_kind,
                    config.extent_bytes,
                    config.packed_bytes,
                )?,
            };
            Ok((
                ChunkMediaLayoutSpec {
                    layout_kind: config.layout_kind,
                    extent_bytes: config.extent_bytes,
                    packed_bytes: config.packed_bytes,
                    key_slots,
                },
                requested_media_slice_bytes,
            ))
        }
        Err(error) => Err(Box::new(error)),
    }
}

fn ensure_media_layout_matches_superblock(
    config: &ServeConfig,
    media_device: &Path,
    media_offset_bytes: u64,
    superblock: kix::ChunkMediaSuperblock,
) -> Result<(), Box<dyn Error>> {
    if config.media_raw_slice_bytes_explicit
        && config.media_raw_slice_bytes != Some(superblock.media_span_bytes)
    {
        return Err(boxed_invalid_input(format!(
            "KST media slice {} B for {} does not match the formatted chunk-media span {} B at {}+{}. Stop passing toy slice values and let KST use the real media geometry.",
            config.media_raw_slice_bytes.unwrap_or_default(),
            config.target_id,
            superblock.media_span_bytes,
            media_device.display(),
            media_offset_bytes,
        )));
    }
    if config.layout_kind_explicit && config.layout_kind != superblock.layout.layout_kind {
        return Err(boxed_invalid_input(format!(
            "KST record-mix {} for {} does not match the formatted chunk-media layout {} on {}+{}.",
            config.layout_kind.as_str(),
            config.target_id,
            superblock.layout.layout_kind.as_str(),
            media_device.display(),
            media_offset_bytes,
        )));
    }
    if config.extent_bytes_explicit && config.extent_bytes != superblock.layout.extent_bytes {
        return Err(boxed_invalid_input(format!(
            "KST extent-bytes {} for {} does not match the formatted chunk-media extent size {} on {}+{}.",
            config.extent_bytes,
            config.target_id,
            superblock.layout.extent_bytes,
            media_device.display(),
            media_offset_bytes,
        )));
    }
    if config.packed_bytes_explicit && config.packed_bytes != superblock.layout.packed_bytes {
        return Err(boxed_invalid_input(format!(
            "KST packed-bytes {} for {} does not match the formatted chunk-media packed size {} on {}+{}.",
            config.packed_bytes,
            config.target_id,
            superblock.layout.packed_bytes,
            media_device.display(),
            media_offset_bytes,
        )));
    }
    if config.key_slots_explicit && config.key_slots != Some(superblock.layout.key_slots) {
        return Err(boxed_invalid_input(format!(
            "KST key-slots {} for {} does not match the formatted chunk-media slot count {} on {}+{}.",
            config.key_slots.unwrap_or_default(),
            config.target_id,
            superblock.layout.key_slots,
            media_device.display(),
            media_offset_bytes,
        )));
    }
    Ok(())
}

fn derive_max_key_slots(
    span_bytes: u64,
    layout_kind: kix::ChunkMediaLayoutKind,
    extent_bytes: u32,
    packed_bytes: u32,
) -> Result<u64, Box<dyn Error>> {
    let mut low = 1_u64;
    let mut high = 1_u64;
    while planned_chunk_media_span_bytes(&ChunkMediaLayoutSpec {
        layout_kind,
        extent_bytes,
        packed_bytes,
        key_slots: high,
    })? <= span_bytes
    {
        if high >= u64::MAX / 2 {
            break;
        }
        high *= 2;
    }
    if high == 1
        && planned_chunk_media_span_bytes(&ChunkMediaLayoutSpec {
            layout_kind,
            extent_bytes,
            packed_bytes,
            key_slots: 1,
        })? > span_bytes
    {
        return Err(boxed_invalid_input(format!(
            "Chunk-media span {} B is too small for even one slot with layout {} (extent={} packed={}).",
            span_bytes,
            layout_kind.as_str(),
            extent_bytes,
            packed_bytes,
        )));
    }
    let mut best = 0_u64;
    while low <= high {
        let mid = low + ((high - low) / 2);
        let planned = planned_chunk_media_span_bytes(&ChunkMediaLayoutSpec {
            layout_kind,
            extent_bytes,
            packed_bytes,
            key_slots: mid,
        })?;
        if planned <= span_bytes {
            best = mid;
            low = mid.saturating_add(1);
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }
    if best == 0 {
        return Err(boxed_invalid_input(format!(
            "KST could not derive a valid key-slot count for chunk-media span {} B.",
            span_bytes
        )));
    }
    Ok(best)
}

fn boxed_invalid_input(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

fn boxed_other(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::Other, message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_layout_places_media_first_and_kix_at_tail() {
        let device_bytes = 6 * 1024_u64 * 1024 * 1024 * 1024 * 1024;
        let layout =
            auto_same_device_drive_layout(device_bytes).expect("auto layout should succeed");
        assert_eq!(layout.media_offset_bytes, 0);
        assert_eq!(
            layout.raw_offset_bytes + layout.raw_slice_bytes,
            device_bytes
        );
        assert_eq!(layout.media_slice_bytes, layout.raw_offset_bytes);
        assert!(layout.raw_slice_bytes >= AUTO_KIX_ARENA_MIN_BYTES);
        assert!(layout.raw_slice_bytes <= AUTO_KIX_ARENA_MAX_BYTES);
    }

    #[test]
    fn auto_layout_clamps_small_drives_but_leaves_media_budget() {
        let device_bytes = 24 * 1024_u64 * 1024 * 1024 * 1024;
        let layout = auto_same_device_drive_layout(device_bytes)
            .expect("small aligned device should still lay out");
        assert_eq!(layout.raw_slice_bytes, 16 * 1024_u64 * 1024 * 1024 * 1024);
        assert_eq!(layout.media_slice_bytes, 8 * 1024_u64 * 1024 * 1024 * 1024);
    }

    #[test]
    fn auto_layout_clamps_huge_drives_to_max_arena_budget() {
        let device_bytes = 20 * 1024_u64 * 1024 * 1024 * 1024 * 1024;
        let layout =
            auto_same_device_drive_layout(device_bytes).expect("large device should lay out");
        assert_eq!(layout.raw_slice_bytes, AUTO_KIX_ARENA_MAX_BYTES);
        assert_eq!(
            layout.raw_offset_bytes + layout.raw_slice_bytes,
            device_bytes
        );
    }

    #[test]
    fn auto_layout_rejects_tiny_same_device_targets() {
        let device_bytes = 4 * 1024_u64 * 1024 * 1024 * 1024;
        let error = auto_same_device_drive_layout(device_bytes)
            .expect_err("tiny device should be rejected");
        assert!(error.to_string().contains("auto-layout"));
    }
}
