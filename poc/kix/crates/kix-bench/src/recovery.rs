// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::{BenchConfig, RecoveryFault};
use crate::topology::TopologyPlan;
use crate::workload::{build_drive_configs, print_latency_summary};
use kix::arena::DeltaEntry;
use kix::{
    chunk_media_checksum, chunk_media_record_for_key, ensure_chunk_media_superblock,
    rebuild_from_chunk_media, write_chunk_media_record, write_chunk_media_tombstone, ChunkId,
    ChunkMediaLayoutKind, ChunkMediaLayoutSpec, ChunkMediaRebuildSummary, ChunkMediaSpanConfig,
    ChunkMediaWriteConfig, DriveArena, DriveConfig, DriveRecovery, LocationRecord,
};
use std::collections::HashMap;
use std::error::Error;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::Instant;

const FILE_HEADER_BYTES: u64 = 4096;
const FRAME_HEADER_BYTES: u64 = 4096;
const CHECKPOINT_ENTRY_BYTES: u64 = 32 + LocationRecord::ENCODED_LEN as u64;
const DELTA_ENTRY_BYTES: u64 = 64;

#[derive(Clone, Copy, Debug)]
struct FrameSpan {
    offset: u64,
    len: u64,
}

#[derive(Clone, Copy, Debug)]
struct RecoverySnapshot {
    entries: usize,
    digest: u64,
    replay_len: u64,
    applied_frames: usize,
}

#[derive(Clone, Copy, Debug)]
enum ExpectedRecovery {
    Clean(RecoverySnapshot),
    TailCorruption(RecoverySnapshot),
    RebuildRequired,
}

#[derive(Debug)]
struct PreparedRecovery {
    expected_clean: RecoverySnapshot,
    expected_fault: ExpectedRecovery,
    checkpoint_entries: u64,
    delta_delete_ops: u64,
    delta_upsert_ops: u64,
    frame_spans: Vec<FrameSpan>,
    checkpoint_frame_bytes: u64,
    delta_frame_bytes: u64,
    prepare_elapsed_us: u64,
    pre_fault_write_head: u64,
    fault_target_frame_index: Option<usize>,
    fault_target_frame_offset: Option<u64>,
    media_config: Option<ChunkMediaWriteConfig>,
}

#[derive(Default)]
struct RecoveryRunState {
    samples: Vec<u64>,
    repair_samples: Vec<u64>,
    rebuild_samples: Vec<u64>,
    tail_corruption_loops: usize,
    rebuild_required_loops: usize,
    clean_loops: usize,
    repairs_applied: usize,
    rebuilds_applied: usize,
    last_entries: usize,
    last_digest: u64,
    last_replay_len: u64,
    last_applied_frames: usize,
    last_tail_corruption: bool,
    last_rebuild_required: bool,
    last_rebuild_summary: Option<ChunkMediaRebuildSummary>,
}

pub(crate) fn run_recovery_benchmark(
    config: &BenchConfig,
    topology: &TopologyPlan,
) -> Result<(), Box<dyn Error>> {
    let drive_configs = build_drive_configs(config, topology)?;
    let drive = drive_configs
        .first()
        .ok_or("KIX recovery benchmark could not resolve a drive configuration")?;
    let prepared = prepare_recovery_arena(config, drive)?;
    inject_fault_if_needed(config, drive, &prepared)?;

    let mut state = RecoveryRunState::default();
    let mut expected = prepared.expected_fault;

    for loop_idx in 0..config.recovery_loops {
        let t0 = Instant::now();
        let recovered = DriveArena::recover(drive)?;
        let elapsed_us = t0.elapsed().as_micros() as u64;
        validate_recovery_result(loop_idx, expected, &recovered)?;
        state.samples.push(elapsed_us);
        state.last_entries = recovered.entries.len();
        state.last_digest = digest_entries(&recovered.entries);
        state.last_replay_len = recovered.replay_len;
        state.last_applied_frames = recovered.applied_frames;
        state.last_tail_corruption = recovered.tail_corruption;
        state.last_rebuild_required = recovered.rebuild_required;

        if recovered.rebuild_required {
            state.rebuild_required_loops += 1;
        } else if recovered.tail_corruption {
            state.tail_corruption_loops += 1;
        } else {
            state.clean_loops += 1;
        }

        if loop_idx == 0
            && config.recovery_auto_truncate
            && recovered.tail_corruption
            && !recovered.rebuild_required
        {
            let repair_t0 = Instant::now();
            let mut arena = DriveArena::open_config(drive)?;
            arena.truncate_to(recovered.replay_len)?;
            state
                .repair_samples
                .push(repair_t0.elapsed().as_micros() as u64);
            state.repairs_applied += 1;
            expected = match expected {
                ExpectedRecovery::TailCorruption(snapshot) => ExpectedRecovery::Clean(snapshot),
                other => other,
            };
        }
        if loop_idx == 0 && config.recovery_auto_rebuild && recovered.rebuild_required {
            let media_config = prepared.media_config.as_ref().ok_or(
                "KIX recovery benchmark was asked to auto-rebuild, but no chunk-media configuration exists.",
            )?;
            let rebuild_t0 = Instant::now();
            let rebuilt = rebuild_from_chunk_media(&media_config.span)?;
            if rebuilt.summary.corrupt_headers > 0
                || rebuilt.summary.corrupt_payloads > 0
                || rebuilt.summary.layout_mismatches > 0
            {
                return Err(format!(
                    "KIX media rebuild refused to checkpoint a partial result: corrupt_headers={}, corrupt_payloads={}, layout_mismatches={}",
                    rebuilt.summary.corrupt_headers,
                    rebuilt.summary.corrupt_payloads,
                    rebuilt.summary.layout_mismatches
                )
                .into());
            }
            let rebuilt_recovery = DriveArena::rebuild_from_entries(
                drive,
                rebuilt
                    .entries
                    .iter()
                    .map(|(&chunk_id, &record)| (chunk_id, record)),
            )?;
            validate_clean_rebuild_result(loop_idx, prepared.expected_clean, &rebuilt_recovery)?;
            let rebuilt_snapshot = snapshot_from_recovery(&rebuilt_recovery);
            state
                .rebuild_samples
                .push(rebuild_t0.elapsed().as_micros() as u64);
            state.rebuilds_applied += 1;
            state.last_rebuild_summary = Some(rebuilt.summary);
            expected = ExpectedRecovery::Clean(rebuilt_snapshot);
        }
    }

    println!("benchmark_mode={}", config.benchmark_mode.as_str());
    println!("recovery_fault={}", config.recovery_fault.as_str());
    println!(
        "recovery_auto_truncate={}",
        if config.recovery_auto_truncate {
            "yes"
        } else {
            "no"
        }
    );
    println!(
        "recovery_auto_rebuild={}",
        if config.recovery_auto_rebuild {
            "yes"
        } else {
            "no"
        }
    );
    println!("recovery_loops={}", config.recovery_loops);
    println!("recovery_live_entries={}", config.recovery_live_entries);
    println!("recovery_key_space={}", config.recovery_key_space);
    println!("recovery_delta_batches={}", config.recovery_delta_batches);
    println!(
        "recovery_deltas_per_batch={}",
        config.recovery_deltas_per_batch
    );
    println!("recovery_delete_percent={}", config.recovery_delete_percent);
    println!(
        "recovery_checkpoint_entries={}",
        prepared.checkpoint_entries
    );
    println!("recovery_delta_upsert_ops={}", prepared.delta_upsert_ops);
    println!("recovery_delta_delete_ops={}", prepared.delta_delete_ops);
    println!(
        "recovery_checkpoint_frame_bytes={}",
        prepared.checkpoint_frame_bytes
    );
    println!("recovery_delta_frame_bytes={}", prepared.delta_frame_bytes);
    println!("recovery_planned_frames={}", prepared.frame_spans.len());
    println!(
        "recovery_prepare_elapsed_us={}",
        prepared.prepare_elapsed_us
    );
    println!(
        "recovery_pre_fault_write_head={}",
        prepared.pre_fault_write_head
    );
    if let Some(node) = topology.raw_device_numa_node {
        println!("raw_device_numa_node={node}");
    }
    if let Some(index) = prepared.fault_target_frame_index {
        println!("recovery_fault_target_frame_index={index}");
    }
    if let Some(offset) = prepared.fault_target_frame_offset {
        println!("recovery_fault_target_frame_offset={offset}");
    }
    if let Some(index) = prepared.fault_target_frame_index {
        if let Some(frame) = prepared.frame_spans.get(index) {
            println!("recovery_fault_target_frame_bytes={}", frame.len);
        }
    }
    println!(
        "recovery_expected_clean_entries={}",
        prepared.expected_clean.entries
    );
    println!(
        "recovery_expected_clean_digest=0x{:016x}",
        prepared.expected_clean.digest
    );
    println!(
        "recovery_expected_clean_replay_len={}",
        prepared.expected_clean.replay_len
    );
    println!(
        "recovery_expected_clean_applied_frames={}",
        prepared.expected_clean.applied_frames
    );
    match prepared.expected_fault {
        ExpectedRecovery::Clean(snapshot) => {
            println!("recovery_expected_fault_state=clean");
            println!("recovery_expected_fault_entries={}", snapshot.entries);
            println!("recovery_expected_fault_digest=0x{:016x}", snapshot.digest);
            println!("recovery_expected_fault_replay_len={}", snapshot.replay_len);
            println!(
                "recovery_expected_fault_applied_frames={}",
                snapshot.applied_frames
            );
        }
        ExpectedRecovery::TailCorruption(snapshot) => {
            println!("recovery_expected_fault_state=tail-corruption");
            println!("recovery_expected_fault_entries={}", snapshot.entries);
            println!("recovery_expected_fault_digest=0x{:016x}", snapshot.digest);
            println!("recovery_expected_fault_replay_len={}", snapshot.replay_len);
            println!(
                "recovery_expected_fault_applied_frames={}",
                snapshot.applied_frames
            );
        }
        ExpectedRecovery::RebuildRequired => {
            println!("recovery_expected_fault_state=rebuild-required");
        }
    }
    println!("recovery_clean_loops={}", state.clean_loops);
    println!(
        "recovery_tail_corruption_loops={}",
        state.tail_corruption_loops
    );
    println!(
        "recovery_rebuild_required_loops={}",
        state.rebuild_required_loops
    );
    println!("recovery_repairs_applied={}", state.repairs_applied);
    println!("recovery_rebuilds_applied={}", state.rebuilds_applied);
    println!("recovery_last_entries={}", state.last_entries);
    println!("recovery_last_digest=0x{:016x}", state.last_digest);
    println!("recovery_last_replay_len={}", state.last_replay_len);
    println!("recovery_last_applied_frames={}", state.last_applied_frames);
    println!(
        "recovery_last_tail_corruption={}",
        if state.last_tail_corruption {
            "yes"
        } else {
            "no"
        }
    );
    println!(
        "recovery_last_rebuild_required={}",
        if state.last_rebuild_required {
            "yes"
        } else {
            "no"
        }
    );
    if let Some(summary) = state.last_rebuild_summary {
        println!(
            "recovery_last_rebuild_scanned_slots={}",
            summary.scanned_slots
        );
        println!(
            "recovery_last_rebuild_live_entries={}",
            summary.live_entries
        );
        println!("recovery_last_rebuild_tombstones={}", summary.tombstones);
        println!("recovery_last_rebuild_empty_slots={}", summary.empty_slots);
        println!(
            "recovery_last_rebuild_corrupt_headers={}",
            summary.corrupt_headers
        );
        println!(
            "recovery_last_rebuild_corrupt_payloads={}",
            summary.corrupt_payloads
        );
        println!(
            "recovery_last_rebuild_layout_mismatches={}",
            summary.layout_mismatches
        );
    }
    print_latency_summary("recovery_open", &mut state.samples);
    print_latency_summary("recovery_truncate_repair", &mut state.repair_samples);
    print_latency_summary("recovery_media_rebuild", &mut state.rebuild_samples);

    Ok(())
}

fn prepare_recovery_arena(
    config: &BenchConfig,
    drive: &DriveConfig,
) -> Result<PreparedRecovery, Box<dyn Error>> {
    let mut arena = DriveArena::reset_config(drive)?;
    let media_config = recovery_media_config(config);
    if let Some(media_config) = &media_config {
        ensure_chunk_media_superblock(media_config)?;
    }
    let mut entries = HashMap::new();
    let mut frame_spans = Vec::with_capacity(config.recovery_delta_batches + 1);
    let mut frame_snapshots = Vec::with_capacity(config.recovery_delta_batches + 1);
    let mut next_generation = 1_u32;
    let mut next_frame_offset = FILE_HEADER_BYTES;
    let t0 = Instant::now();

    for seed in 0..config.recovery_live_entries {
        let chunk_id = ChunkId::from_seed(seed);
        let record = make_recovery_record(config, seed, next_generation);
        next_generation = next_generation.wrapping_add(1);
        if let Some(media_config) = &media_config {
            write_chunk_media_record(media_config, chunk_id, record)?;
        }
        entries.insert(chunk_id, record);
    }

    let checkpoint_entries = entries
        .iter()
        .map(|(&chunk_id, &record)| (chunk_id, record))
        .collect::<Vec<_>>();
    arena.write_checkpoint(checkpoint_entries.iter().copied())?;
    let checkpoint_frame_bytes =
        frame_bytes_for_payload(4 + config.recovery_live_entries * CHECKPOINT_ENTRY_BYTES)?;
    next_frame_offset += record_frame(
        &mut frame_spans,
        &mut frame_snapshots,
        &entries,
        next_frame_offset,
        checkpoint_frame_bytes,
    );

    let mut delta_delete_ops = 0_u64;
    let mut delta_upsert_ops = 0_u64;
    let delta_frame_bytes =
        frame_bytes_for_payload(4 + config.recovery_deltas_per_batch as u64 * DELTA_ENTRY_BYTES)?;

    for batch_idx in 0..config.recovery_delta_batches {
        let mut deltas = Vec::with_capacity(config.recovery_deltas_per_batch);
        for delta_idx in 0..config.recovery_deltas_per_batch {
            let op_seed =
                batch_idx as u64 * config.recovery_deltas_per_batch as u64 + delta_idx as u64;
            let key_seed = splitmix64(op_seed ^ 0x6a09_e667_f3bc_c909) % config.recovery_key_space;
            let chunk_id = ChunkId::from_seed(key_seed);
            let should_delete = !entries.is_empty()
                && splitmix64(op_seed ^ 0xbb67_ae85_84ca_a73b) % 100
                    < config.recovery_delete_percent as u64;
            if should_delete && entries.contains_key(&chunk_id) {
                let deleted = entries
                    .remove(&chunk_id)
                    .ok_or("KIX recovery benchmark lost a live entry before deleting it")?;
                if let Some(media_config) = &media_config {
                    write_chunk_media_tombstone(media_config, chunk_id, deleted, next_generation)?;
                }
                next_generation = next_generation.wrapping_add(1);
                deltas.push(DeltaEntry::delete(chunk_id));
                delta_delete_ops += 1;
            } else {
                let record = make_recovery_record(config, key_seed, next_generation);
                next_generation = next_generation.wrapping_add(1);
                if let Some(media_config) = &media_config {
                    write_chunk_media_record(media_config, chunk_id, record)?;
                }
                deltas.push(DeltaEntry::upsert(chunk_id, record));
                entries.insert(chunk_id, record);
                delta_upsert_ops += 1;
            }
        }
        arena.append_delta_batch(deltas.iter().copied())?;
        next_frame_offset += record_frame(
            &mut frame_spans,
            &mut frame_snapshots,
            &entries,
            next_frame_offset,
            delta_frame_bytes,
        );
    }

    let expected_clean = *frame_snapshots
        .last()
        .ok_or("KIX recovery benchmark built no frame snapshots")?;
    let prepare_elapsed_us = t0.elapsed().as_micros() as u64;
    let pre_fault_write_head = next_frame_offset;
    let (expected_fault, fault_target_frame_index, fault_target_frame_offset) =
        expected_fault_state(config.recovery_fault, &frame_spans, &frame_snapshots)?;

    Ok(PreparedRecovery {
        expected_clean,
        expected_fault,
        checkpoint_entries: config.recovery_live_entries,
        delta_delete_ops,
        delta_upsert_ops,
        frame_spans,
        checkpoint_frame_bytes,
        delta_frame_bytes,
        prepare_elapsed_us,
        pre_fault_write_head,
        fault_target_frame_index,
        fault_target_frame_offset,
        media_config,
    })
}

fn expected_fault_state(
    fault: RecoveryFault,
    frame_spans: &[FrameSpan],
    frame_snapshots: &[RecoverySnapshot],
) -> Result<(ExpectedRecovery, Option<usize>, Option<u64>), Box<dyn Error>> {
    if frame_spans.len() != frame_snapshots.len() {
        return Err("KIX recovery benchmark frame planning drifted out of sync".into());
    }
    if frame_spans.is_empty() {
        return Err("KIX recovery benchmark cannot inject faults into an empty arena".into());
    }
    let clean = *frame_snapshots
        .last()
        .ok_or("missing clean recovery snapshot")?;
    let outcome = match fault {
        RecoveryFault::None => (ExpectedRecovery::Clean(clean), None, None),
        RecoveryFault::ArenaHeader => (ExpectedRecovery::RebuildRequired, None, None),
        RecoveryFault::FirstFrame => (
            ExpectedRecovery::RebuildRequired,
            Some(0),
            Some(frame_spans[0].offset),
        ),
        RecoveryFault::TailCrc => {
            let target = frame_spans.len() - 1;
            if target == 0 {
                (
                    ExpectedRecovery::RebuildRequired,
                    Some(target),
                    Some(frame_spans[target].offset),
                )
            } else {
                (
                    ExpectedRecovery::TailCorruption(frame_snapshots[target - 1]),
                    Some(target),
                    Some(frame_spans[target].offset),
                )
            }
        }
        RecoveryFault::LaterFrame => {
            if frame_spans.len() < 2 {
                return Err(
                    "KIX recovery benchmark needs at least one delta frame for --recovery-fault later-frame."
                        .into(),
                );
            }
            let target = 1;
            (
                ExpectedRecovery::TailCorruption(frame_snapshots[target - 1]),
                Some(target),
                Some(frame_spans[target].offset),
            )
        }
    };
    Ok(outcome)
}

fn inject_fault_if_needed(
    config: &BenchConfig,
    drive: &DriveConfig,
    prepared: &PreparedRecovery,
) -> Result<(), Box<dyn Error>> {
    let Some(target_frame_index) = prepared.fault_target_frame_index else {
        return Ok(());
    };
    let target_frame = prepared
        .frame_spans
        .get(target_frame_index)
        .ok_or("KIX recovery benchmark lost the target fault frame")?;

    match config.recovery_fault {
        RecoveryFault::None => {}
        RecoveryFault::ArenaHeader => {
            flip_one_byte_direct(&drive.arena_path, drive.arena_offset_bytes)?
        }
        RecoveryFault::FirstFrame | RecoveryFault::LaterFrame => flip_one_byte_direct(
            &drive.arena_path,
            drive.arena_offset_bytes + target_frame.offset,
        )?,
        RecoveryFault::TailCrc => {
            flip_one_byte_direct(
                &drive.arena_path,
                drive.arena_offset_bytes + target_frame.offset + 12,
            )?;
        }
    }
    Ok(())
}

#[repr(align(4096))]
struct AlignedBlock([u8; 4096]);

fn flip_one_byte_direct(path: &Path, offset: u64) -> Result<(), Box<dyn Error>> {
    let path_cstr = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        format!(
            "KIX fault injection path {} contains interior NUL bytes",
            path.display()
        )
    })?;
    let fd = unsafe {
        libc::open(
            path_cstr.as_ptr(),
            libc::O_RDWR | libc::O_DIRECT | libc::O_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }

    let result = flip_one_byte_direct_fd(fd, offset);
    let close_rc = unsafe { libc::close(fd) };
    if let Err(err) = result {
        return Err(err);
    }
    if close_rc < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

fn flip_one_byte_direct_fd(fd: libc::c_int, offset: u64) -> Result<(), Box<dyn Error>> {
    let block_offset = offset / 4096 * 4096;
    let block_index = (offset - block_offset) as usize;
    let mut block = AlignedBlock([0_u8; 4096]);
    let read_rc = unsafe {
        libc::pread(
            fd,
            block.0.as_mut_ptr().cast(),
            block.0.len(),
            block_offset as libc::off_t,
        )
    };
    if read_rc != block.0.len() as isize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "KIX fault injection could not read the aligned block at offset {} from the raw arena device",
                block_offset
            ),
        )
        .into());
    }
    block.0[block_index] ^= 0x5a;

    let write_rc = unsafe {
        libc::pwrite(
            fd,
            block.0.as_ptr().cast(),
            block.0.len(),
            block_offset as libc::off_t,
        )
    };
    if write_rc != block.0.len() as isize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            format!(
                "KIX fault injection could not write the aligned block at offset {} back to the raw arena device",
                block_offset
            ),
        )
        .into());
    }
    if unsafe { libc::fsync(fd) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }

    let mut verify = AlignedBlock([0_u8; 4096]);
    let verify_rc = unsafe {
        libc::pread(
            fd,
            verify.0.as_mut_ptr().cast(),
            verify.0.len(),
            block_offset as libc::off_t,
        )
    };
    if verify_rc != verify.0.len() as isize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "KIX fault injection could not verify the aligned block at offset {} after writing it",
                block_offset
            ),
        )
        .into());
    }
    if verify.0[block_index] != block.0[block_index] {
        return Err(format!(
            "KIX fault injection wrote byte offset {} inside block {} but the post-write verification read back 0x{:02x} instead of 0x{:02x}",
            block_index, block_offset, verify.0[block_index], block.0[block_index]
        )
        .into());
    }
    Ok(())
}

fn validate_recovery_result(
    loop_idx: usize,
    expected: ExpectedRecovery,
    recovered: &DriveRecovery,
) -> Result<(), Box<dyn Error>> {
    match expected {
        ExpectedRecovery::Clean(snapshot) => {
            if recovered.rebuild_required {
                return Err(format!(
                    "KIX recovery loop {loop_idx} expected a clean reopen, but the arena demanded rebuild-required"
                )
                .into());
            }
            if recovered.tail_corruption {
                return Err(format!(
                    "KIX recovery loop {loop_idx} expected a clean reopen, but KIX reported tail corruption"
                )
                .into());
            }
            validate_snapshot(loop_idx, snapshot, recovered)
        }
        ExpectedRecovery::TailCorruption(snapshot) => {
            if recovered.rebuild_required {
                return Err(format!(
                    "KIX recovery loop {loop_idx} expected recoverable tail damage, but KIX escalated to rebuild-required"
                )
                .into());
            }
            if !recovered.tail_corruption {
                return Err(format!(
                    "KIX recovery loop {loop_idx} expected tail corruption, but KIX reopened cleanly instead"
                )
                .into());
            }
            validate_snapshot(loop_idx, snapshot, recovered)
        }
        ExpectedRecovery::RebuildRequired => {
            if !recovered.rebuild_required {
                return Err(format!(
                    "KIX recovery loop {loop_idx} expected rebuild-required, but KIX accepted the arena"
                )
                .into());
            }
            if !recovered.entries.is_empty() {
                return Err(format!(
                    "KIX recovery loop {loop_idx} expected an empty recovery result under rebuild-required, but {} entries survived",
                    recovered.entries.len()
                )
                .into());
            }
            Ok(())
        }
    }
}

fn validate_snapshot(
    loop_idx: usize,
    expected: RecoverySnapshot,
    recovered: &DriveRecovery,
) -> Result<(), Box<dyn Error>> {
    if recovered.entries.len() != expected.entries {
        return Err(format!(
            "KIX recovery loop {loop_idx} expected {} recovered entries, but got {}",
            expected.entries,
            recovered.entries.len()
        )
        .into());
    }
    let digest = digest_entries(&recovered.entries);
    if digest != expected.digest {
        return Err(format!(
            "KIX recovery loop {loop_idx} expected digest 0x{:016x}, but got 0x{:016x}",
            expected.digest, digest
        )
        .into());
    }
    if recovered.replay_len != expected.replay_len {
        return Err(format!(
            "KIX recovery loop {loop_idx} expected replay_len {}, but got {}",
            expected.replay_len, recovered.replay_len
        )
        .into());
    }
    if recovered.applied_frames != expected.applied_frames {
        return Err(format!(
            "KIX recovery loop {loop_idx} expected {} applied frames, but got {}",
            expected.applied_frames, recovered.applied_frames
        )
        .into());
    }
    Ok(())
}

fn validate_clean_rebuild_result(
    loop_idx: usize,
    expected_clean: RecoverySnapshot,
    rebuilt: &DriveRecovery,
) -> Result<(), Box<dyn Error>> {
    if rebuilt.rebuild_required {
        return Err(format!(
            "KIX recovery loop {loop_idx} rebuilt the arena from chunk media, but the reopened arena still reports rebuild-required"
        )
        .into());
    }
    if rebuilt.tail_corruption {
        return Err(format!(
            "KIX recovery loop {loop_idx} rebuilt the arena from chunk media, but the reopened arena still reports tail corruption"
        )
        .into());
    }
    if rebuilt.entries.len() != expected_clean.entries {
        return Err(format!(
            "KIX recovery loop {loop_idx} rebuilt {} entries from chunk media, but expected {}",
            rebuilt.entries.len(),
            expected_clean.entries
        )
        .into());
    }
    let digest = digest_entries(&rebuilt.entries);
    if digest != expected_clean.digest {
        return Err(format!(
            "KIX recovery loop {loop_idx} rebuilt digest 0x{:016x}, but expected 0x{:016x}",
            digest, expected_clean.digest
        )
        .into());
    }
    Ok(())
}

fn snapshot_from_recovery(recovered: &DriveRecovery) -> RecoverySnapshot {
    RecoverySnapshot {
        entries: recovered.entries.len(),
        digest: digest_entries(&recovered.entries),
        replay_len: recovered.replay_len,
        applied_frames: recovered.applied_frames,
    }
}

fn record_frame(
    frame_spans: &mut Vec<FrameSpan>,
    frame_snapshots: &mut Vec<RecoverySnapshot>,
    entries: &HashMap<ChunkId, LocationRecord>,
    offset: u64,
    len: u64,
) -> u64 {
    let replay_len = offset + len;
    frame_spans.push(FrameSpan { offset, len });
    frame_snapshots.push(RecoverySnapshot {
        entries: entries.len(),
        digest: digest_entries(entries),
        replay_len,
        applied_frames: frame_spans.len(),
    });
    len
}

fn frame_bytes_for_payload(payload_bytes: u64) -> Result<u64, Box<dyn Error>> {
    Ok(FRAME_HEADER_BYTES + align_up_4096(payload_bytes)?)
}

fn align_up_4096(value: u64) -> Result<u64, Box<dyn Error>> {
    let remainder = value % 4096;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(4096 - remainder)
            .ok_or_else(|| "KIX recovery benchmark overflowed while aligning frame bytes".into())
    }
}

fn make_recovery_record(config: &BenchConfig, key_seed: u64, generation: u32) -> LocationRecord {
    let layout = recovery_chunk_media_layout(config);
    let mut record = chunk_media_record_for_key(&layout, 0, key_seed, generation)
        .expect("KIX recovery record planning must fit the recovery chunk-media layout");
    let chunk_id = ChunkId::from_seed(key_seed);
    record.checksum = chunk_media_checksum(&chunk_id, &record);
    record
}

fn recovery_media_config(config: &BenchConfig) -> Option<ChunkMediaWriteConfig> {
    let media_path = config.media_raw_device.clone()?;
    Some(ChunkMediaWriteConfig {
        drive_id: 0,
        span: ChunkMediaSpanConfig {
            media_path,
            media_offset_bytes: config.media_raw_offset_bytes,
            media_len_bytes: config.media_raw_slice_bytes,
        },
        layout: recovery_chunk_media_layout(config),
    })
}

fn recovery_chunk_media_layout(config: &BenchConfig) -> ChunkMediaLayoutSpec {
    ChunkMediaLayoutSpec {
        layout_kind: match config.record_mix {
            crate::config::RecordMix::Mixed => ChunkMediaLayoutKind::Mixed,
            crate::config::RecordMix::PackedOnly => ChunkMediaLayoutKind::PackedOnly,
            crate::config::RecordMix::ExtentOnly => ChunkMediaLayoutKind::ExtentOnly,
        },
        extent_bytes: config.extent_bytes,
        packed_bytes: config.packed_bytes,
        key_slots: config.recovery_live_entries.max(config.recovery_key_space),
    }
}

fn digest_entries(entries: &HashMap<ChunkId, LocationRecord>) -> u64 {
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

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BenchConfig, BenchmarkMode};
    use std::path::PathBuf;

    #[test]
    fn digest_changes_when_entries_change() {
        let mut entries = HashMap::new();
        let config = BenchConfig::default();
        let chunk_a = ChunkId::from_seed(1);
        let chunk_b = ChunkId::from_seed(2);
        entries.insert(chunk_a, make_recovery_record(&config, 1, 1));
        let first = digest_entries(&entries);
        entries.insert(chunk_b, make_recovery_record(&config, 2, 2));
        let second = digest_entries(&entries);
        assert_ne!(first, second);
    }

    #[test]
    fn tail_crc_fault_targets_previous_snapshot_when_delta_tail_exists() {
        let spans = vec![
            FrameSpan {
                offset: 4096,
                len: 8192,
            },
            FrameSpan {
                offset: 12288,
                len: 8192,
            },
        ];
        let snapshots = vec![
            RecoverySnapshot {
                entries: 10,
                digest: 11,
                replay_len: 12288,
                applied_frames: 1,
            },
            RecoverySnapshot {
                entries: 20,
                digest: 22,
                replay_len: 20480,
                applied_frames: 2,
            },
        ];
        let (expected, frame_idx, frame_offset) =
            expected_fault_state(RecoveryFault::TailCrc, &spans, &snapshots).unwrap();
        match expected {
            ExpectedRecovery::TailCorruption(snapshot) => {
                assert_eq!(snapshot.entries, 10);
                assert_eq!(snapshot.digest, 11);
            }
            _ => panic!("expected tail-corruption snapshot"),
        }
        assert_eq!(frame_idx, Some(1));
        assert_eq!(frame_offset, Some(12288));
    }

    #[test]
    fn first_frame_fault_requires_rebuild() {
        let spans = vec![FrameSpan {
            offset: 4096,
            len: 8192,
        }];
        let snapshots = vec![RecoverySnapshot {
            entries: 10,
            digest: 11,
            replay_len: 12288,
            applied_frames: 1,
        }];
        let (expected, frame_idx, frame_offset) =
            expected_fault_state(RecoveryFault::FirstFrame, &spans, &snapshots).unwrap();
        assert!(matches!(expected, ExpectedRecovery::RebuildRequired));
        assert_eq!(frame_idx, Some(0));
        assert_eq!(frame_offset, Some(4096));
    }

    #[test]
    fn recovery_prepare_uses_explicit_extent_shape() {
        let mut config = BenchConfig::default();
        config.benchmark_mode = BenchmarkMode::Recovery;
        config.raw_device = Some(PathBuf::from("/dev/null"));
        config.raw_slice_bytes = Some(1 << 30);
        let record = make_recovery_record(&config, 7, 3);
        assert_eq!(record.logical_length, 1024 * 1024);
        assert_eq!(record.stored_length, 1024 * 1024);
        assert_eq!(record.physical_offset, 8192 + 7 * (1024 * 1024 + 4096));
    }
}
