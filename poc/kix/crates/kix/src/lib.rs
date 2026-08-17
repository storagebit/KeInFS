// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

pub mod arena;
pub mod chunk_media;
pub mod engine;
pub mod hardware;
pub mod layout;
pub mod types;

pub use arena::{
    device_numa_node, device_size_bytes, numa_node_cpu_list, online_numa_nodes, ArenaIoMode,
    DriveArena, DriveConfig, DriveRecovery,
};
pub use chunk_media::{
    chunk_media_checksum, chunk_media_record_for_key, chunk_media_record_for_slot,
    chunk_media_slot_index_for_record, ensure_chunk_media_superblock,
    fill_chunk_media_payload_bytes, fill_chunk_media_slot_bytes, planned_chunk_media_span_bytes,
    read_chunk_media_payload, read_chunk_media_superblock, rebuild_from_chunk_media,
    validate_chunk_media_live_record, write_chunk_media_payload, write_chunk_media_record,
    write_chunk_media_tombstone, ChunkMediaHandle, ChunkMediaLayoutKind, ChunkMediaLayoutSpec,
    ChunkMediaReadResult, ChunkMediaReadTiming, ChunkMediaRebuildResult, ChunkMediaRebuildSummary,
    ChunkMediaSpanConfig, ChunkMediaSuperblock, ChunkMediaWriteConfig, ChunkMediaWriteResult,
    ChunkSelfDescribingIdentity,
    ChunkMediaWriteTiming, CHUNK_MEDIA_ALIGN_BYTES, CHUNK_MEDIA_PUBLICATION_LANES,
    CHUNK_MEDIA_SLOT_HEADER_BYTES, CHUNK_MEDIA_SUPERBLOCK_BYTES,
};
pub use engine::{
    DriveStatsSnapshot, KixClient, KixConfig, KixEngine, KixError, KixStatsConfig, KixStatsHandle,
    KixStatsSnapshot, LatencyStatsSnapshot, ShardStatsSnapshot, WorkerMode,
};
pub use hardware::{
    crc32_ieee, detect_hardware_acceleration, Crc32Backend, KixHardwareAcceleration,
};
pub use layout::{auto_drive_layout, AutoDriveLayout, LAYOUT_ALIGNMENT_BYTES};
pub use types::{ChunkId, LocationKind, LocationRecord};
