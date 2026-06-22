// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::hardware::crc32_ieee;
use crate::types::{ChunkId, LocationKind, LocationRecord};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub const CHUNK_MEDIA_ALIGN_BYTES: usize = 4096;
pub const CHUNK_MEDIA_SUPERBLOCK_BYTES: u64 = 4096;
pub const CHUNK_MEDIA_SLOT_HEADER_BYTES: u64 = 4096;
pub const CHUNK_MEDIA_PUBLICATION_LANES: u64 = 2;

const CHUNK_MEDIA_SUPERBLOCK_MAGIC: [u8; 8] = *b"KIXMSB01";
const CHUNK_MEDIA_SLOT_MAGIC: [u8; 8] = *b"KIXMSL01";
const CHUNK_MEDIA_VERSION: u16 = 2;
const BLKGETSIZE64_IOCTL: libc::c_ulong = 0x8008_1272;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChunkMediaLayoutKind {
    ExtentOnly,
    PackedOnly,
    Mixed,
}

impl ChunkMediaLayoutKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExtentOnly => "extent-only",
            Self::PackedOnly => "packed-only",
            Self::Mixed => "mixed",
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::ExtentOnly => 1,
            Self::PackedOnly => 2,
            Self::Mixed => 3,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::ExtentOnly),
            2 => Some(Self::PackedOnly),
            3 => Some(Self::Mixed),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkMediaLayoutSpec {
    pub layout_kind: ChunkMediaLayoutKind,
    pub extent_bytes: u32,
    pub packed_bytes: u32,
    pub key_slots: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkMediaSpanConfig {
    pub media_path: PathBuf,
    pub media_offset_bytes: u64,
    pub media_len_bytes: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkMediaWriteConfig {
    pub drive_id: u16,
    pub span: ChunkMediaSpanConfig,
    pub layout: ChunkMediaLayoutSpec,
}

pub struct ChunkMediaHandle {
    config: ChunkMediaWriteConfig,
    superblock: ChunkMediaSuperblock,
    file: File,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ChunkMediaReadTiming {
    pub header_validate: Duration,
    pub payload_read: Duration,
    pub payload_copy: Duration,
    pub crc: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkMediaReadResult {
    pub payload: Vec<u8>,
    pub timing: ChunkMediaReadTiming,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ChunkMediaWriteTiming {
    pub prepare: Duration,
    pub write_io: Duration,
    pub fsync: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkMediaWriteResult {
    pub record: LocationRecord,
    pub timing: ChunkMediaWriteTiming,
}

impl ChunkMediaHandle {
    pub fn open(config: &ChunkMediaWriteConfig) -> io::Result<Self> {
        let superblock = ensure_chunk_media_superblock(config)?;
        let file = open_direct_file(&config.span.media_path, true)?;
        Ok(Self {
            config: config.clone(),
            superblock,
            file,
        })
    }

    pub fn config(&self) -> &ChunkMediaWriteConfig {
        &self.config
    }

    pub fn span(&self) -> &ChunkMediaSpanConfig {
        &self.config.span
    }

    pub fn layout(&self) -> &ChunkMediaLayoutSpec {
        &self.superblock.layout
    }

    pub fn validate_live_record(
        &self,
        chunk_id: ChunkId,
        record: LocationRecord,
    ) -> io::Result<()> {
        let _ = read_validated_live_header(&self.file, &self.config.span, chunk_id, record)?;
        Ok(())
    }

    pub fn read_payload(&self, chunk_id: ChunkId, record: LocationRecord) -> io::Result<Vec<u8>> {
        self.read_payload_timed(chunk_id, record)
            .map(|result| result.payload)
    }

    pub fn write_payload(
        &self,
        slot_index: u64,
        chunk_id: ChunkId,
        generation: u32,
        payload: &[u8],
    ) -> io::Result<LocationRecord> {
        self.write_payload_timed(slot_index, chunk_id, generation, payload)
            .map(|result| result.record)
    }

    pub fn read_payload_timed(
        &self,
        chunk_id: ChunkId,
        record: LocationRecord,
    ) -> io::Result<ChunkMediaReadResult> {
        read_chunk_media_payload_with_file(&self.file, &self.config.span, chunk_id, record)
    }

    pub fn write_payload_timed(
        &self,
        slot_index: u64,
        chunk_id: ChunkId,
        generation: u32,
        payload: &[u8],
    ) -> io::Result<ChunkMediaWriteResult> {
        write_chunk_media_payload_with_file(
            &self.file,
            &self.config,
            self.superblock,
            slot_index,
            PublicationLaneSelector::AgainstCurrent(None),
            chunk_id,
            generation,
            ChunkSelfDescribingIdentity::default(),
            payload,
            WriteDurability::Barrier,
        )
    }

    pub fn write_payload_against_current_timed(
        &self,
        slot_index: u64,
        current_record: Option<LocationRecord>,
        chunk_id: ChunkId,
        generation: u32,
        payload: &[u8],
    ) -> io::Result<ChunkMediaWriteResult> {
        write_chunk_media_payload_with_file(
            &self.file,
            &self.config,
            self.superblock,
            slot_index,
            PublicationLaneSelector::AgainstCurrent(current_record),
            chunk_id,
            generation,
            ChunkSelfDescribingIdentity::default(),
            payload,
            WriteDurability::Barrier,
        )
    }

    /// Writes a payload into an explicitly chosen publication lane WITHOUT
    /// issuing a durability barrier.
    ///
    /// The returned record describes media that has been written but is not yet
    /// durable. The caller is responsible for invoking [`Self::fdatasync`] (a
    /// single barrier may cover many of these writes) BEFORE publishing the
    /// record's location into KIX. Callers reserve the lane under their own
    /// per-slot synchronization so concurrent writers never share a lane.
    pub fn write_payload_to_lane_unsynced(
        &self,
        slot_index: u64,
        publication_lane: u64,
        chunk_id: ChunkId,
        generation: u32,
        identity: ChunkSelfDescribingIdentity,
        payload: &[u8],
    ) -> io::Result<ChunkMediaWriteResult> {
        write_chunk_media_payload_with_file(
            &self.file,
            &self.config,
            self.superblock,
            slot_index,
            PublicationLaneSelector::Explicit(publication_lane),
            chunk_id,
            generation,
            identity,
            payload,
            WriteDurability::Deferred,
        )
    }

    /// Issues a single durability barrier on the shared media fd.
    ///
    /// Covers every preceding [`Self::write_payload_to_lane_unsynced`] write on
    /// this handle. Once it returns, all of those writes are durable and their
    /// KIX location records may be published.
    pub fn fdatasync(&self) -> io::Result<()> {
        direct_fdatasync(&self.file)
    }

    /// Computes the publication lane that a fresh write for `slot_index` would
    /// target given the currently published record. Mirrors the lane selection
    /// performed internally by [`Self::write_payload_against_current_timed`].
    pub fn next_publication_lane_for_slot(
        &self,
        slot_index: u64,
        current_record: Option<LocationRecord>,
    ) -> io::Result<u64> {
        next_publication_lane(self.layout(), slot_index, current_record)
    }

    pub fn write_tombstone(
        &self,
        chunk_id: ChunkId,
        record: LocationRecord,
        generation: u32,
    ) -> io::Result<()> {
        write_chunk_media_tombstone_with_file(
            &self.file,
            &self.config,
            self.superblock,
            chunk_id,
            record,
            generation,
        )
    }

    pub fn write_tombstone_against_current(
        &self,
        chunk_id: ChunkId,
        current_record: LocationRecord,
        generation: u32,
    ) -> io::Result<()> {
        write_chunk_media_tombstone_with_file(
            &self.file,
            &self.config,
            self.superblock,
            chunk_id,
            current_record,
            generation,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkMediaSuperblock {
    pub version: u16,
    pub layout: ChunkMediaLayoutSpec,
    pub media_span_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ChunkMediaRebuildSummary {
    pub scanned_slots: u64,
    pub live_entries: u64,
    pub tombstones: u64,
    pub empty_slots: u64,
    pub corrupt_headers: u64,
    pub corrupt_payloads: u64,
    pub layout_mismatches: u64,
}

/// Self-describing fragment identity recovered from the on-media slot header: the
/// owning object's id/version and the fragment's stripe/frag position. Lets
/// media-rebuild and GC reason about a fragment's owning object without a central index.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ChunkSelfDescribingIdentity {
    pub stripe: u16,
    pub object_id: u32,
    pub object_version: u16,
    pub frag: u16,
}

pub struct ChunkMediaRebuildResult {
    pub superblock: ChunkMediaSuperblock,
    pub entries: HashMap<ChunkId, LocationRecord>,
    pub identities: HashMap<ChunkId, ChunkSelfDescribingIdentity>,
    pub summary: ChunkMediaRebuildSummary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChunkMediaEntryState {
    Live,
    Deleted,
}

impl ChunkMediaEntryState {
    fn as_u8(self) -> u8 {
        match self {
            Self::Live => 1,
            Self::Deleted => 2,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Live),
            2 => Some(Self::Deleted),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
struct ChunkMediaSlotHeader {
    state: ChunkMediaEntryState,
    drive_id: u16,
    chunk_id: ChunkId,
    record: LocationRecord,
    // Self-describing fragment identity carried in the on-media header so a fragment
    // identifies its owning object for media-rebuild discovery and GC; surfaced by
    // rebuild_from_chunk_media as ChunkSelfDescribingIdentity. All within the CRC span [..80].
    stripe: u16,
    object_id: u32,
    object_version: u16,
    frag: u16,
}

#[derive(Clone, Copy)]
struct SlotGeometry {
    slot_base_offset: u64,
    payload_offset: u64,
    location_kind: LocationKind,
    logical_length: u32,
    stored_length: u32,
}

#[derive(Clone, Copy)]
struct SlotAddress {
    slot_index: u64,
    publication_lane: u64,
}

pub fn fill_chunk_media_payload_bytes(
    payload: &mut [u8],
    chunk_id: &ChunkId,
    record: &LocationRecord,
) {
    let generation = record.generation.to_le_bytes();
    let logical_len = record.logical_length.to_le_bytes();
    for (idx, byte) in payload.iter_mut().enumerate() {
        *byte = chunk_id.0[idx % chunk_id.0.len()]
            ^ generation[idx % generation.len()]
            ^ logical_len[idx % logical_len.len()]
            ^ (idx as u8).wrapping_mul(17);
    }
}

pub fn chunk_media_checksum(chunk_id: &ChunkId, record: &LocationRecord) -> u32 {
    let mut payload = vec![0_u8; record.stored_length as usize];
    fill_chunk_media_payload_bytes(&mut payload, chunk_id, record);
    let logical_len = (record.logical_length as usize).min(payload.len());
    crc32_ieee(&payload[..logical_len])
}

pub fn fill_chunk_media_slot_bytes(
    buffer: &mut [u8],
    chunk_id: ChunkId,
    record: LocationRecord,
) -> io::Result<()> {
    let total_len = CHUNK_MEDIA_SLOT_HEADER_BYTES as usize + record.stored_length as usize;
    if buffer.len() < total_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media slot buffer is {} bytes but needs at least {} bytes",
                buffer.len(),
                total_len
            ),
        ));
    }
    buffer[..total_len].fill(0);
    let header = ChunkMediaSlotHeader {
        state: ChunkMediaEntryState::Live,
        drive_id: record.drive_id,
        chunk_id,
        record,
        // Writers without object context (fill/record helpers) carry no identity.
        stripe: 0,
        object_id: 0,
        object_version: 0,
        frag: 0,
    };
    encode_slot_header(buffer, header)?;
    let payload = &mut buffer[CHUNK_MEDIA_SLOT_HEADER_BYTES as usize..total_len];
    fill_chunk_media_payload_bytes(payload, &chunk_id, &record);
    Ok(())
}

pub fn chunk_media_record_for_key(
    layout: &ChunkMediaLayoutSpec,
    drive_id: u16,
    key_seed: u64,
    generation: u32,
) -> io::Result<LocationRecord> {
    let geometry = slot_geometry(
        layout,
        key_seed,
        publication_lane_for_generation(generation),
    )?;
    Ok(match geometry.location_kind {
        LocationKind::Extent => LocationRecord::extent(
            drive_id,
            geometry.payload_offset,
            geometry.logical_length,
            geometry.stored_length,
            generation,
            0,
        ),
        LocationKind::PackedContainer => LocationRecord::packed(
            drive_id,
            geometry.payload_offset,
            geometry.logical_length,
            geometry.stored_length,
            generation,
            0,
        ),
    })
}

pub fn chunk_media_record_for_slot(
    layout: &ChunkMediaLayoutSpec,
    drive_id: u16,
    slot_index: u64,
    generation: u32,
) -> io::Result<LocationRecord> {
    chunk_media_record_for_key(layout, drive_id, slot_index, generation)
}

pub fn chunk_media_slot_index_for_record(
    layout: &ChunkMediaLayoutSpec,
    record: LocationRecord,
) -> io::Result<u64> {
    Ok(slot_address_for_record(layout, record)?.slot_index)
}

fn slot_address_for_record(
    layout: &ChunkMediaLayoutSpec,
    record: LocationRecord,
) -> io::Result<SlotAddress> {
    validate_layout(layout)?;
    if record.physical_offset < CHUNK_MEDIA_SUPERBLOCK_BYTES + CHUNK_MEDIA_SLOT_HEADER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media record at payload offset {} is too small to map to a valid slot",
                record.physical_offset
            ),
        ));
    }
    let slot_base_offset = record
        .physical_offset
        .checked_sub(CHUNK_MEDIA_SLOT_HEADER_BYTES)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "chunk-media slot base underflow",
            )
        })?;
    let relative_offset = slot_base_offset
        .checked_sub(CHUNK_MEDIA_SUPERBLOCK_BYTES)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "chunk-media slot base precedes the superblock",
            )
        })?;
    let extent_slot_bytes = slot_bytes(u64::from(layout.extent_bytes))?;
    let packed_slot_bytes = slot_bytes(u64::from(layout.packed_bytes))?;
    let address = match layout.layout_kind {
        ChunkMediaLayoutKind::ExtentOnly => {
            if record.location_kind != LocationKind::Extent
                || record.logical_length == 0
                || record.logical_length > layout.extent_bytes
                || record.stored_length != layout.extent_bytes
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "chunk-media record at payload offset {} does not match the configured extent-only layout",
                        record.physical_offset
                    ),
                ));
            }
            if relative_offset % extent_slot_bytes != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "chunk-media record at payload offset {} is misaligned for the configured extent-only layout",
                        record.physical_offset
                    ),
                ));
            }
            let physical_slot = relative_offset / extent_slot_bytes;
            SlotAddress {
                slot_index: physical_slot / CHUNK_MEDIA_PUBLICATION_LANES,
                publication_lane: physical_slot % CHUNK_MEDIA_PUBLICATION_LANES,
            }
        }
        ChunkMediaLayoutKind::PackedOnly => {
            if record.location_kind != LocationKind::PackedContainer
                || record.logical_length == 0
                || record.logical_length > layout.packed_bytes
                || record.stored_length != layout.packed_bytes
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "chunk-media record at payload offset {} does not match the configured packed-only layout",
                        record.physical_offset
                    ),
                ));
            }
            if relative_offset % packed_slot_bytes != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "chunk-media record at payload offset {} is misaligned for the configured packed-only layout",
                        record.physical_offset
                    ),
                ));
            }
            let physical_slot = relative_offset / packed_slot_bytes;
            SlotAddress {
                slot_index: physical_slot / CHUNK_MEDIA_PUBLICATION_LANES,
                publication_lane: physical_slot % CHUNK_MEDIA_PUBLICATION_LANES,
            }
        }
        ChunkMediaLayoutKind::Mixed => {
            let extent_group_bytes = CHUNK_MEDIA_PUBLICATION_LANES
                .checked_mul(extent_slot_bytes)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "mixed extent group overflow")
                })?;
            let packed_group_bytes = CHUNK_MEDIA_PUBLICATION_LANES
                .checked_mul(packed_slot_bytes)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "mixed packed group overflow")
                })?;
            let pair_stride = extent_group_bytes
                .checked_add(packed_group_bytes)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "mixed pair stride overflow")
                })?;
            let pair_index = relative_offset / pair_stride;
            let pair_offset = relative_offset % pair_stride;
            match record.location_kind {
                LocationKind::Extent
                    if record.logical_length > 0
                        && record.logical_length <= layout.extent_bytes
                        && record.stored_length == layout.extent_bytes
                        && pair_offset < extent_group_bytes
                        && pair_offset % extent_slot_bytes == 0 =>
                {
                    SlotAddress {
                        slot_index: pair_index.checked_mul(2).ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "extent slot index overflow",
                            )
                        })?,
                        publication_lane: pair_offset / extent_slot_bytes,
                    }
                }
                LocationKind::PackedContainer
                    if record.logical_length > 0
                        && record.logical_length <= layout.packed_bytes
                        && record.stored_length == layout.packed_bytes
                        && pair_offset >= extent_group_bytes
                        && pair_offset
                            .checked_sub(extent_group_bytes)
                            .is_some_and(|offset| offset % packed_slot_bytes == 0) =>
                {
                    let packed_offset = pair_offset - extent_group_bytes;
                    SlotAddress {
                        slot_index: pair_index
                            .checked_mul(2)
                            .and_then(|value| value.checked_add(1))
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "packed slot index overflow",
                                )
                            })?,
                        publication_lane: packed_offset / packed_slot_bytes,
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "chunk-media record at payload offset {} does not match the configured mixed layout",
                            record.physical_offset
                        ),
                    ));
                }
            }
        }
    };
    if address.slot_index >= layout.key_slots {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media record at payload offset {} maps to slot {} which exceeds key_slots {}",
                record.physical_offset, address.slot_index, layout.key_slots
            ),
        ));
    }
    Ok(address)
}

pub fn planned_chunk_media_span_bytes(layout: &ChunkMediaLayoutSpec) -> io::Result<u64> {
    validate_layout(layout)?;
    if layout.key_slots == 0 {
        return Ok(CHUNK_MEDIA_SUPERBLOCK_BYTES);
    }
    let last = slot_geometry(
        layout,
        layout.key_slots - 1,
        CHUNK_MEDIA_PUBLICATION_LANES - 1,
    )?;
    last.payload_offset
        .checked_add(last.stored_length as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "chunk-media span overflow"))
}

pub fn ensure_chunk_media_superblock(
    config: &ChunkMediaWriteConfig,
) -> io::Result<ChunkMediaSuperblock> {
    validate_layout(&config.layout)?;
    let span_bytes = resolve_effective_span_bytes(
        &config.span.media_path,
        config.span.media_offset_bytes,
        config.span.media_len_bytes,
    )?;
    let required_span = planned_chunk_media_span_bytes(&config.layout)?;
    if span_bytes < required_span {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media span {} B is too small for layout {} with {} key slots; need at least {} B",
                span_bytes,
                config.layout.layout_kind.as_str(),
                config.layout.key_slots,
                required_span,
            ),
        ));
    }

    let superblock = ChunkMediaSuperblock {
        version: CHUNK_MEDIA_VERSION,
        layout: config.layout,
        media_span_bytes: span_bytes,
    };
    let file = open_direct_file(&config.span.media_path, true)?;
    let mut block = AlignedIoBuffer::zeroed(CHUNK_MEDIA_ALIGN_BYTES)?;
    direct_read_exact(&file, block.as_mut_slice(), config.span.media_offset_bytes)?;
    if let Some(existing) = decode_superblock(block.as_slice()) {
        if existing != superblock {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "chunk-media superblock on {} does not match the requested layout (existing kind={}, extent_bytes={}, packed_bytes={}, key_slots={}, span_bytes={})",
                    config.span.media_path.display(),
                    existing.layout.layout_kind.as_str(),
                    existing.layout.extent_bytes,
                    existing.layout.packed_bytes,
                    existing.layout.key_slots,
                    existing.media_span_bytes,
                ),
            ));
        }
        return Ok(existing);
    }

    // Fresh-media bring-up now relies on the operator formatting path to
    // discard/zero the span instead of spraying zero slot headers across the
    // full medium one slot at a time.
    encode_superblock(block.as_mut_slice(), superblock)?;
    direct_write_exact(&file, block.as_slice(), config.span.media_offset_bytes)?;
    direct_fdatasync(&file)?;
    Ok(superblock)
}

pub fn read_chunk_media_superblock(
    span: &ChunkMediaSpanConfig,
) -> io::Result<ChunkMediaSuperblock> {
    let _ = resolve_effective_span_bytes(
        &span.media_path,
        span.media_offset_bytes,
        span.media_len_bytes,
    )?;
    let file = open_direct_file(&span.media_path, false)?;
    let mut block = AlignedIoBuffer::zeroed(CHUNK_MEDIA_ALIGN_BYTES)?;
    direct_read_exact(&file, block.as_mut_slice(), span.media_offset_bytes)?;
    decode_superblock(block.as_slice()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "chunk-media superblock is missing or invalid at {}+{}",
                span.media_path.display(),
                span.media_offset_bytes,
            ),
        )
    })
}

pub fn write_chunk_media_record(
    config: &ChunkMediaWriteConfig,
    chunk_id: ChunkId,
    record: LocationRecord,
) -> io::Result<()> {
    let expected_superblock = ensure_chunk_media_superblock(config)?;
    let geometry = slot_geometry_for_record(&expected_superblock.layout, record)?;
    let slot_len = CHUNK_MEDIA_SLOT_HEADER_BYTES
        .checked_add(record.stored_length as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "chunk-media slot overflow"))?;
    let mut block = AlignedIoBuffer::zeroed(slot_len as usize)?;
    let header = ChunkMediaSlotHeader {
        state: ChunkMediaEntryState::Live,
        drive_id: record.drive_id,
        chunk_id,
        record,
        // Writers without object context (fill/record helpers) carry no identity.
        stripe: 0,
        object_id: 0,
        object_version: 0,
        frag: 0,
    };
    encode_slot_header(block.as_mut_slice(), header)?;
    let payload = &mut block.as_mut_slice()[CHUNK_MEDIA_SLOT_HEADER_BYTES as usize
        ..CHUNK_MEDIA_SLOT_HEADER_BYTES as usize + record.stored_length as usize];
    fill_chunk_media_payload_bytes(payload, &chunk_id, &record);

    let file = open_direct_file(&config.span.media_path, true)?;
    direct_write_exact(
        &file,
        block.as_slice(),
        absolute_chunk_media_offset(&config.span, geometry.slot_base_offset)?,
    )?;
    direct_fdatasync(&file)
}

pub fn write_chunk_media_payload(
    config: &ChunkMediaWriteConfig,
    slot_index: u64,
    chunk_id: ChunkId,
    generation: u32,
    payload: &[u8],
) -> io::Result<LocationRecord> {
    let expected_superblock = ensure_chunk_media_superblock(config)?;
    let file = open_direct_file(&config.span.media_path, true)?;
    write_chunk_media_payload_with_file(
        &file,
        config,
        expected_superblock,
        slot_index,
        PublicationLaneSelector::AgainstCurrent(None),
        chunk_id,
        generation,
        ChunkSelfDescribingIdentity::default(),
        payload,
        WriteDurability::Barrier,
    )
    .map(|result| result.record)
}

pub fn write_chunk_media_tombstone(
    config: &ChunkMediaWriteConfig,
    chunk_id: ChunkId,
    record: LocationRecord,
    generation: u32,
) -> io::Result<()> {
    let expected_superblock = ensure_chunk_media_superblock(config)?;
    let file = open_direct_file(&config.span.media_path, true)?;
    write_chunk_media_tombstone_with_file(
        &file,
        config,
        expected_superblock,
        chunk_id,
        record,
        generation,
    )
}

pub fn rebuild_from_chunk_media(
    span: &ChunkMediaSpanConfig,
) -> io::Result<ChunkMediaRebuildResult> {
    let superblock = read_chunk_media_superblock(span)?;
    let file = open_direct_file(&span.media_path, false)?;
    let mut summary = ChunkMediaRebuildSummary::default();
    let mut latest_headers: HashMap<ChunkId, ChunkMediaSlotHeader> = HashMap::new();
    let mut entries: HashMap<ChunkId, LocationRecord> = HashMap::new();
    let mut header_block = AlignedIoBuffer::zeroed(CHUNK_MEDIA_SLOT_HEADER_BYTES as usize)?;

    for key_seed in 0..superblock.layout.key_slots {
        for publication_lane in 0..CHUNK_MEDIA_PUBLICATION_LANES {
            summary.scanned_slots += 1;
            let geometry = slot_geometry(&superblock.layout, key_seed, publication_lane)?;
            let header_offset = absolute_chunk_media_offset(span, geometry.slot_base_offset)?;
            direct_read_exact(&file, header_block.as_mut_slice(), header_offset)?;

            if is_zero_block(header_block.as_slice()) {
                summary.empty_slots += 1;
                continue;
            }

            let Some(header) = decode_slot_header(header_block.as_slice()) else {
                summary.corrupt_headers += 1;
                continue;
            };

            if !slot_header_matches_geometry(header, geometry) {
                summary.layout_mismatches += 1;
                continue;
            }

            match header.state {
                ChunkMediaEntryState::Deleted => {
                    summary.tombstones += 1;
                    update_latest_header(&mut latest_headers, header);
                }
                ChunkMediaEntryState::Live => {
                    let mut payload =
                        AlignedIoBuffer::zeroed(header.record.stored_length as usize)?;
                    direct_read_exact(
                        &file,
                        payload.as_mut_slice(),
                        absolute_chunk_media_offset(span, header.record.physical_offset)?,
                    )?;
                    let logical_len =
                        (header.record.logical_length as usize).min(payload.as_slice().len());
                    let observed = crc32_ieee(&payload.as_slice()[..logical_len]);
                    if observed != header.record.checksum {
                        summary.corrupt_payloads += 1;
                        continue;
                    }
                    summary.live_entries += 1;
                    update_latest_header(&mut latest_headers, header);
                }
            }
        }
    }

    let mut identities: HashMap<ChunkId, ChunkSelfDescribingIdentity> = HashMap::new();
    for (chunk_id, header) in latest_headers {
        if header.state == ChunkMediaEntryState::Live {
            entries.insert(chunk_id, header.record);
            identities.insert(
                chunk_id,
                ChunkSelfDescribingIdentity {
                    stripe: header.stripe,
                    object_id: header.object_id,
                    object_version: header.object_version,
                    frag: header.frag,
                },
            );
        }
    }

    Ok(ChunkMediaRebuildResult {
        superblock,
        entries,
        identities,
        summary,
    })
}

pub fn validate_chunk_media_live_record(
    span: &ChunkMediaSpanConfig,
    chunk_id: ChunkId,
    record: LocationRecord,
) -> io::Result<()> {
    let file = open_direct_file(&span.media_path, false)?;
    let _ = read_validated_live_header(&file, span, chunk_id, record)?;
    Ok(())
}

pub fn read_chunk_media_payload(
    span: &ChunkMediaSpanConfig,
    chunk_id: ChunkId,
    record: LocationRecord,
) -> io::Result<Vec<u8>> {
    let file = open_direct_file(&span.media_path, false)?;
    read_chunk_media_payload_with_file(&file, span, chunk_id, record).map(|result| result.payload)
}

/// Selects which publication lane a payload write targets within a slot.
///
/// `AgainstCurrent` preserves the historical behaviour of deriving the next
/// lane from the currently published record. `Explicit` lets a caller that has
/// already reserved a lane under its own synchronization (for example the KST
/// slot-publication state machine) pin the write to that lane without re-reading
/// the current record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationLaneSelector {
    AgainstCurrent(Option<LocationRecord>),
    Explicit(u64),
}

/// Controls whether a payload write issues its own durability barrier.
///
/// `Barrier` keeps the single-entry behaviour (write + `fdatasync`). `Deferred`
/// skips the barrier so that a caller batching several writes to the shared fd
/// can issue exactly one `fdatasync` covering all of them (see
/// [`ChunkMediaHandle::fdatasync`]). A deferred write is NOT durable until that
/// shared barrier succeeds, so callers must not publish a chunk's KIX location
/// record before the barrier completes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteDurability {
    Barrier,
    Deferred,
}

#[allow(clippy::too_many_arguments)]
fn write_chunk_media_payload_with_file(
    file: &File,
    config: &ChunkMediaWriteConfig,
    superblock: ChunkMediaSuperblock,
    slot_index: u64,
    lane: PublicationLaneSelector,
    chunk_id: ChunkId,
    generation: u32,
    identity: ChunkSelfDescribingIdentity,
    payload: &[u8],
    durability: WriteDurability,
) -> io::Result<ChunkMediaWriteResult> {
    let mut timing = ChunkMediaWriteTiming::default();
    if slot_index >= superblock.layout.key_slots {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media slot index {} exceeds configured key_slots {}",
                slot_index, superblock.layout.key_slots
            ),
        ));
    }
    let mut record = match lane {
        PublicationLaneSelector::AgainstCurrent(current_record) => {
            chunk_media_record_for_slot_against_current(
                &superblock.layout,
                config.drive_id,
                slot_index,
                current_record,
                generation,
            )?
        }
        PublicationLaneSelector::Explicit(publication_lane) => chunk_media_record_for_slot_to_lane(
            &superblock.layout,
            config.drive_id,
            slot_index,
            publication_lane,
            generation,
        )?,
    };
    if payload.is_empty() || payload.len() > record.stored_length as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media payload for slot {} is {} bytes, but layout allows at most {} bytes",
                slot_index,
                payload.len(),
                record.stored_length
            ),
        ));
    }
    record.logical_length = payload.len() as u32;
    record.checksum = crc32_ieee(payload);

    let prepare_started = Instant::now();
    let geometry = slot_geometry_for_record(&superblock.layout, record)?;
    let slot_len = CHUNK_MEDIA_SLOT_HEADER_BYTES
        .checked_add(record.stored_length as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "chunk-media slot overflow"))?;
    let mut block = AlignedIoBuffer::zeroed(slot_len as usize)?;
    let header = ChunkMediaSlotHeader {
        state: ChunkMediaEntryState::Live,
        drive_id: record.drive_id,
        chunk_id,
        record,
        // Carry the object identity supplied by the caller (KST threads it from the
        // KP2 write request; other writers pass default/zeros).
        stripe: identity.stripe,
        object_id: identity.object_id,
        object_version: identity.object_version,
        frag: identity.frag,
    };
    encode_slot_header(block.as_mut_slice(), header)?;
    let payload_dst = &mut block.as_mut_slice()[CHUNK_MEDIA_SLOT_HEADER_BYTES as usize
        ..CHUNK_MEDIA_SLOT_HEADER_BYTES as usize + record.stored_length as usize];
    payload_dst[..payload.len()].copy_from_slice(payload);
    timing.prepare = prepare_started.elapsed();

    let write_started = Instant::now();
    direct_write_exact(
        file,
        block.as_slice(),
        absolute_chunk_media_offset(&config.span, geometry.slot_base_offset)?,
    )?;
    timing.write_io = write_started.elapsed();
    if durability == WriteDurability::Barrier {
        let sync_started = Instant::now();
        direct_fdatasync(file)?;
        timing.fsync = sync_started.elapsed();
    }
    Ok(ChunkMediaWriteResult { record, timing })
}

fn write_chunk_media_tombstone_with_file(
    file: &File,
    config: &ChunkMediaWriteConfig,
    superblock: ChunkMediaSuperblock,
    chunk_id: ChunkId,
    record: LocationRecord,
    generation: u32,
) -> io::Result<()> {
    let tombstone_record = LocationRecord {
        generation,
        ..record
    };
    let geometry = slot_geometry_for_record(&superblock.layout, tombstone_record)?;
    let mut block = AlignedIoBuffer::zeroed(CHUNK_MEDIA_SLOT_HEADER_BYTES as usize)?;
    let header = ChunkMediaSlotHeader {
        state: ChunkMediaEntryState::Deleted,
        drive_id: record.drive_id,
        chunk_id,
        record: tombstone_record,
        stripe: 0,
        object_id: 0,
        object_version: 0,
        frag: 0,
    };
    encode_slot_header(block.as_mut_slice(), header)?;
    direct_write_exact(
        file,
        block.as_slice(),
        absolute_chunk_media_offset(&config.span, geometry.slot_base_offset)?,
    )?;
    direct_fdatasync(file)
}

fn read_chunk_media_payload_with_file(
    file: &File,
    span: &ChunkMediaSpanConfig,
    chunk_id: ChunkId,
    record: LocationRecord,
) -> io::Result<ChunkMediaReadResult> {
    let mut timing = ChunkMediaReadTiming::default();
    let header_started = Instant::now();
    let _ = read_validated_live_header(file, span, chunk_id, record)?;
    timing.header_validate = header_started.elapsed();
    let mut payload = AlignedIoBuffer::zeroed(record.stored_length as usize)?;
    let read_started = Instant::now();
    direct_read_exact(
        file,
        payload.as_mut_slice(),
        absolute_chunk_media_offset(span, record.physical_offset)?,
    )?;
    timing.payload_read = read_started.elapsed();
    let copy_started = Instant::now();
    let logical_len = (record.logical_length as usize).min(record.stored_length as usize);
    let payload = payload.as_slice()[..logical_len].to_vec();
    timing.payload_copy = copy_started.elapsed();
    let crc_started = Instant::now();
    let observed_crc = crc32_ieee(&payload);
    timing.crc = crc_started.elapsed();
    if observed_crc != record.checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "chunk-media payload checksum mismatch for chunk {:?}: expected {}, observed {}",
                chunk_id, record.checksum, observed_crc
            ),
        ));
    }
    Ok(ChunkMediaReadResult { payload, timing })
}

fn slot_header_matches_geometry(header: ChunkMediaSlotHeader, geometry: SlotGeometry) -> bool {
    header.record.location_kind == geometry.location_kind
        && header.record.physical_offset == geometry.payload_offset
        && header.record.logical_length > 0
        && header.record.logical_length <= geometry.logical_length
        && header.record.stored_length == geometry.stored_length
}

fn update_latest_header(
    latest_headers: &mut HashMap<ChunkId, ChunkMediaSlotHeader>,
    candidate: ChunkMediaSlotHeader,
) {
    match latest_headers.get(&candidate.chunk_id).copied() {
        Some(existing)
            if existing.record.generation > candidate.record.generation
                || (existing.record.generation == candidate.record.generation
                    && existing.state == ChunkMediaEntryState::Deleted
                    && candidate.state == ChunkMediaEntryState::Live) => {}
        _ => {
            latest_headers.insert(candidate.chunk_id, candidate);
        }
    }
}

fn read_validated_live_header(
    file: &File,
    span: &ChunkMediaSpanConfig,
    chunk_id: ChunkId,
    record: LocationRecord,
) -> io::Result<ChunkMediaSlotHeader> {
    if record.physical_offset < CHUNK_MEDIA_SLOT_HEADER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media payload offset {} is too small to have a preceding slot header",
                record.physical_offset
            ),
        ));
    }
    let header_offset = absolute_chunk_media_offset(
        span,
        record
            .physical_offset
            .checked_sub(CHUNK_MEDIA_SLOT_HEADER_BYTES)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "chunk-media header offset underflow",
                )
            })?,
    )?;
    let mut header_block = AlignedIoBuffer::zeroed(CHUNK_MEDIA_SLOT_HEADER_BYTES as usize)?;
    direct_read_exact(file, header_block.as_mut_slice(), header_offset)?;
    if is_zero_block(header_block.as_slice()) {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "chunk-media slot for chunk {:?} is empty at {}+{}",
                chunk_id,
                span.media_path.display(),
                header_offset
            ),
        ));
    }
    let header = decode_slot_header(header_block.as_slice()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "chunk-media slot header for chunk {:?} at {}+{} is invalid",
                chunk_id,
                span.media_path.display(),
                header_offset
            ),
        )
    })?;
    if header.state != ChunkMediaEntryState::Live {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "chunk-media slot for chunk {:?} is tombstoned at {}+{}",
                chunk_id,
                span.media_path.display(),
                header_offset
            ),
        ));
    }
    if header.chunk_id != chunk_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "chunk-media slot at {}+{} belongs to {:?}, not {:?}",
                span.media_path.display(),
                header_offset,
                header.chunk_id,
                chunk_id
            ),
        ));
    }
    if header.record != record {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "chunk-media header for chunk {:?} does not match the KIX location record at payload offset {}",
                chunk_id, record.physical_offset
            ),
        ));
    }
    Ok(header)
}

fn validate_layout(layout: &ChunkMediaLayoutSpec) -> io::Result<()> {
    if layout.key_slots == 0 {
        return Ok(());
    }
    if u64::from(layout.extent_bytes) % CHUNK_MEDIA_ALIGN_BYTES as u64 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media extent_bytes {} must be aligned to {} bytes",
                layout.extent_bytes, CHUNK_MEDIA_ALIGN_BYTES
            ),
        ));
    }
    if u64::from(layout.packed_bytes) % CHUNK_MEDIA_ALIGN_BYTES as u64 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media packed_bytes {} must be aligned to {} bytes",
                layout.packed_bytes, CHUNK_MEDIA_ALIGN_BYTES
            ),
        ));
    }
    Ok(())
}

fn slot_geometry_for_record(
    layout: &ChunkMediaLayoutSpec,
    record: LocationRecord,
) -> io::Result<SlotGeometry> {
    let address = slot_address_for_record(layout, record)?;
    slot_geometry(layout, address.slot_index, address.publication_lane)
}

fn chunk_media_record_for_slot_against_current(
    layout: &ChunkMediaLayoutSpec,
    drive_id: u16,
    slot_index: u64,
    current_record: Option<LocationRecord>,
    generation: u32,
) -> io::Result<LocationRecord> {
    let geometry = slot_geometry(
        layout,
        slot_index,
        next_publication_lane(layout, slot_index, current_record)?,
    )?;
    Ok(match geometry.location_kind {
        LocationKind::Extent => LocationRecord::extent(
            drive_id,
            geometry.payload_offset,
            geometry.logical_length,
            geometry.stored_length,
            generation,
            0,
        ),
        LocationKind::PackedContainer => LocationRecord::packed(
            drive_id,
            geometry.payload_offset,
            geometry.logical_length,
            geometry.stored_length,
            generation,
            0,
        ),
    })
}

fn chunk_media_record_for_slot_to_lane(
    layout: &ChunkMediaLayoutSpec,
    drive_id: u16,
    slot_index: u64,
    publication_lane: u64,
    generation: u32,
) -> io::Result<LocationRecord> {
    let geometry = slot_geometry(layout, slot_index, publication_lane)?;
    Ok(match geometry.location_kind {
        LocationKind::Extent => LocationRecord::extent(
            drive_id,
            geometry.payload_offset,
            geometry.logical_length,
            geometry.stored_length,
            generation,
            0,
        ),
        LocationKind::PackedContainer => LocationRecord::packed(
            drive_id,
            geometry.payload_offset,
            geometry.logical_length,
            geometry.stored_length,
            generation,
            0,
        ),
    })
}

fn next_publication_lane(
    layout: &ChunkMediaLayoutSpec,
    slot_index: u64,
    current_record: Option<LocationRecord>,
) -> io::Result<u64> {
    if let Some(record) = current_record {
        let address = slot_address_for_record(layout, record)?;
        if address.slot_index == slot_index {
            return Ok((address.publication_lane + 1) % CHUNK_MEDIA_PUBLICATION_LANES);
        }
    }
    Ok(0)
}

fn slot_geometry(
    layout: &ChunkMediaLayoutSpec,
    key_seed: u64,
    publication_lane: u64,
) -> io::Result<SlotGeometry> {
    if publication_lane >= CHUNK_MEDIA_PUBLICATION_LANES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media publication lane {} exceeds configured publication lanes {}",
                publication_lane, CHUNK_MEDIA_PUBLICATION_LANES
            ),
        ));
    }
    let extent_slot_bytes = slot_bytes(u64::from(layout.extent_bytes))?;
    let packed_slot_bytes = slot_bytes(u64::from(layout.packed_bytes))?;
    match layout.layout_kind {
        ChunkMediaLayoutKind::ExtentOnly => {
            let slot_stride = CHUNK_MEDIA_PUBLICATION_LANES
                .checked_mul(extent_slot_bytes)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "extent slot stride overflow")
                })?;
            let slot_base_offset = CHUNK_MEDIA_SUPERBLOCK_BYTES
                .checked_add(key_seed.checked_mul(slot_stride).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "extent slot offset overflow")
                })?)
                .and_then(|base| base.checked_add(publication_lane.checked_mul(extent_slot_bytes)?))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "extent slot offset overflow")
                })?;
            Ok(SlotGeometry {
                slot_base_offset,
                payload_offset: slot_base_offset + CHUNK_MEDIA_SLOT_HEADER_BYTES,
                location_kind: LocationKind::Extent,
                logical_length: layout.extent_bytes,
                stored_length: layout.extent_bytes,
            })
        }
        ChunkMediaLayoutKind::PackedOnly => {
            let slot_stride = CHUNK_MEDIA_PUBLICATION_LANES
                .checked_mul(packed_slot_bytes)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "packed slot stride overflow")
                })?;
            let slot_base_offset = CHUNK_MEDIA_SUPERBLOCK_BYTES
                .checked_add(key_seed.checked_mul(slot_stride).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "packed slot offset overflow")
                })?)
                .and_then(|base| base.checked_add(publication_lane.checked_mul(packed_slot_bytes)?))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "packed slot offset overflow")
                })?;
            Ok(SlotGeometry {
                slot_base_offset,
                payload_offset: slot_base_offset + CHUNK_MEDIA_SLOT_HEADER_BYTES,
                location_kind: LocationKind::PackedContainer,
                logical_length: layout.packed_bytes,
                stored_length: layout.packed_bytes,
            })
        }
        ChunkMediaLayoutKind::Mixed => {
            let extent_group_bytes = CHUNK_MEDIA_PUBLICATION_LANES
                .checked_mul(extent_slot_bytes)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "mixed extent group overflow")
                })?;
            let packed_group_bytes = CHUNK_MEDIA_PUBLICATION_LANES
                .checked_mul(packed_slot_bytes)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "mixed packed group overflow")
                })?;
            let pair_stride = extent_group_bytes
                .checked_add(packed_group_bytes)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "mixed pair stride overflow")
                })?;
            let pair_index = key_seed / 2;
            let pair_base = CHUNK_MEDIA_SUPERBLOCK_BYTES
                .checked_add(pair_index.checked_mul(pair_stride).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "mixed pair offset overflow")
                })?)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "mixed pair offset overflow")
                })?;
            if key_seed & 1 == 0 {
                let slot_base_offset = pair_base
                    .checked_add(publication_lane.checked_mul(extent_slot_bytes).ok_or_else(
                        || {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "mixed extent slot overflow",
                            )
                        },
                    )?)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "mixed extent slot overflow")
                    })?;
                Ok(SlotGeometry {
                    slot_base_offset,
                    payload_offset: slot_base_offset + CHUNK_MEDIA_SLOT_HEADER_BYTES,
                    location_kind: LocationKind::Extent,
                    logical_length: layout.extent_bytes,
                    stored_length: layout.extent_bytes,
                })
            } else {
                let slot_base_offset = pair_base
                    .checked_add(extent_group_bytes)
                    .and_then(|base| {
                        base.checked_add(publication_lane.checked_mul(packed_slot_bytes)?)
                    })
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "mixed packed slot overflow")
                    })?;
                Ok(SlotGeometry {
                    slot_base_offset,
                    payload_offset: slot_base_offset + CHUNK_MEDIA_SLOT_HEADER_BYTES,
                    location_kind: LocationKind::PackedContainer,
                    logical_length: layout.packed_bytes,
                    stored_length: layout.packed_bytes,
                })
            }
        }
    }
}

fn publication_lane_for_generation(generation: u32) -> u64 {
    u64::from(generation.saturating_sub(1)) % CHUNK_MEDIA_PUBLICATION_LANES
}

fn slot_bytes(payload_bytes: u64) -> io::Result<u64> {
    CHUNK_MEDIA_SLOT_HEADER_BYTES
        .checked_add(payload_bytes)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "chunk-media slot size overflow",
            )
        })
}

fn encode_superblock(buffer: &mut [u8], superblock: ChunkMediaSuperblock) -> io::Result<()> {
    if buffer.len() < CHUNK_MEDIA_SUPERBLOCK_BYTES as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "chunk-media superblock buffer is too small",
        ));
    }
    buffer.fill(0);
    buffer[0..8].copy_from_slice(&CHUNK_MEDIA_SUPERBLOCK_MAGIC);
    buffer[8..10].copy_from_slice(&superblock.version.to_le_bytes());
    buffer[10] = superblock.layout.layout_kind.as_u8();
    buffer[12..16].copy_from_slice(&superblock.layout.extent_bytes.to_le_bytes());
    buffer[16..20].copy_from_slice(&superblock.layout.packed_bytes.to_le_bytes());
    buffer[20..28].copy_from_slice(&superblock.layout.key_slots.to_le_bytes());
    buffer[28..36].copy_from_slice(&superblock.media_span_bytes.to_le_bytes());
    buffer[36..40].copy_from_slice(&(CHUNK_MEDIA_SUPERBLOCK_BYTES as u32).to_le_bytes());
    buffer[40..44].copy_from_slice(&(CHUNK_MEDIA_SLOT_HEADER_BYTES as u32).to_le_bytes());
    let crc = crc32_ieee(&buffer[..44]);
    buffer[44..48].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

fn decode_superblock(buffer: &[u8]) -> Option<ChunkMediaSuperblock> {
    if buffer.len() < CHUNK_MEDIA_SUPERBLOCK_BYTES as usize {
        return None;
    }
    if buffer[0..8] != CHUNK_MEDIA_SUPERBLOCK_MAGIC {
        return None;
    }
    let expected_crc = u32::from_le_bytes(buffer[44..48].try_into().ok()?);
    let observed_crc = crc32_ieee(&buffer[..44]);
    if expected_crc != observed_crc {
        return None;
    }
    let version = u16::from_le_bytes(buffer[8..10].try_into().ok()?);
    if version != CHUNK_MEDIA_VERSION {
        return None;
    }
    let layout_kind = ChunkMediaLayoutKind::from_u8(buffer[10])?;
    let extent_bytes = u32::from_le_bytes(buffer[12..16].try_into().ok()?);
    let packed_bytes = u32::from_le_bytes(buffer[16..20].try_into().ok()?);
    let key_slots = u64::from_le_bytes(buffer[20..28].try_into().ok()?);
    let media_span_bytes = u64::from_le_bytes(buffer[28..36].try_into().ok()?);
    Some(ChunkMediaSuperblock {
        version,
        layout: ChunkMediaLayoutSpec {
            layout_kind,
            extent_bytes,
            packed_bytes,
            key_slots,
        },
        media_span_bytes,
    })
}

fn encode_slot_header(buffer: &mut [u8], header: ChunkMediaSlotHeader) -> io::Result<()> {
    if buffer.len() < CHUNK_MEDIA_SLOT_HEADER_BYTES as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "chunk-media slot buffer is too small",
        ));
    }
    buffer.fill(0);
    buffer[0..8].copy_from_slice(&CHUNK_MEDIA_SLOT_MAGIC);
    buffer[8..10].copy_from_slice(&CHUNK_MEDIA_VERSION.to_le_bytes());
    buffer[10] = header.state.as_u8();
    buffer[11] = header.record.location_kind as u8;
    buffer[12..14].copy_from_slice(&header.drive_id.to_le_bytes());
    buffer[16..48].copy_from_slice(&header.chunk_id.0);
    buffer[48..56].copy_from_slice(&header.record.physical_offset.to_le_bytes());
    buffer[56..60].copy_from_slice(&header.record.logical_length.to_le_bytes());
    buffer[60..64].copy_from_slice(&header.record.stored_length.to_le_bytes());
    buffer[64..68].copy_from_slice(&header.record.generation.to_le_bytes());
    buffer[68..72].copy_from_slice(&header.record.checksum.to_le_bytes());
    // Self-describing fragment identity; fits the reserved regions [14..16] and
    // [72..80], inside the CRC span below.
    buffer[14..16].copy_from_slice(&header.stripe.to_le_bytes());
    buffer[72..76].copy_from_slice(&header.object_id.to_le_bytes());
    buffer[76..78].copy_from_slice(&header.object_version.to_le_bytes());
    buffer[78..80].copy_from_slice(&header.frag.to_le_bytes());
    let crc = crc32_ieee(&buffer[..80]);
    buffer[80..84].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

fn decode_slot_header(buffer: &[u8]) -> Option<ChunkMediaSlotHeader> {
    if buffer.len() < CHUNK_MEDIA_SLOT_HEADER_BYTES as usize {
        return None;
    }
    if buffer[0..8] != CHUNK_MEDIA_SLOT_MAGIC {
        return None;
    }
    let version = u16::from_le_bytes(buffer[8..10].try_into().ok()?);
    if version != CHUNK_MEDIA_VERSION {
        return None;
    }
    let expected_crc = u32::from_le_bytes(buffer[80..84].try_into().ok()?);
    let observed_crc = crc32_ieee(&buffer[..80]);
    if expected_crc != observed_crc {
        return None;
    }
    let state = ChunkMediaEntryState::from_u8(buffer[10])?;
    let location_kind = LocationKind::from_byte(buffer[11])?;
    let drive_id = u16::from_le_bytes(buffer[12..14].try_into().ok()?);
    let chunk_id = ChunkId(buffer[16..48].try_into().ok()?);
    let physical_offset = u64::from_le_bytes(buffer[48..56].try_into().ok()?);
    let logical_length = u32::from_le_bytes(buffer[56..60].try_into().ok()?);
    let stored_length = u32::from_le_bytes(buffer[60..64].try_into().ok()?);
    let generation = u32::from_le_bytes(buffer[64..68].try_into().ok()?);
    let checksum = u32::from_le_bytes(buffer[68..72].try_into().ok()?);
    let stripe = u16::from_le_bytes(buffer[14..16].try_into().ok()?);
    let object_id = u32::from_le_bytes(buffer[72..76].try_into().ok()?);
    let object_version = u16::from_le_bytes(buffer[76..78].try_into().ok()?);
    let frag = u16::from_le_bytes(buffer[78..80].try_into().ok()?);
    Some(ChunkMediaSlotHeader {
        state,
        drive_id,
        chunk_id,
        stripe,
        object_id,
        object_version,
        frag,
        record: LocationRecord {
            drive_id,
            location_kind,
            physical_offset,
            logical_length,
            stored_length,
            generation,
            checksum,
        },
    })
}

fn resolve_effective_span_bytes(
    path: &Path,
    offset_bytes: u64,
    len_bytes: Option<u64>,
) -> io::Result<u64> {
    validate_offset_alignment(offset_bytes)?;
    let file = open_direct_file(path, false)?;
    let total_bytes = storage_capacity_bytes(&file)?;
    if offset_bytes > total_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media offset {} exceeds device size {} for {}",
                offset_bytes,
                total_bytes,
                path.display()
            ),
        ));
    }
    let effective = len_bytes.unwrap_or(total_bytes - offset_bytes);
    if effective == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "chunk-media span must be > 0 bytes",
        ));
    }
    validate_offset_alignment(effective)?;
    let end = offset_bytes
        .checked_add(effective)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "chunk-media span overflow"))?;
    if end > total_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media span {}+{} exceeds device size {} for {}",
                offset_bytes,
                effective,
                total_bytes,
                path.display()
            ),
        ));
    }
    Ok(effective)
}

fn validate_offset_alignment(value: u64) -> io::Result<()> {
    if value % CHUNK_MEDIA_ALIGN_BYTES as u64 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk-media direct I/O requires {} to be aligned to {} bytes",
                value, CHUNK_MEDIA_ALIGN_BYTES
            ),
        ));
    }
    Ok(())
}

fn absolute_chunk_media_offset(span: &ChunkMediaSpanConfig, local_offset: u64) -> io::Result<u64> {
    span.media_offset_bytes
        .checked_add(local_offset)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "chunk-media offset overflow"))
}

fn open_direct_file(path: &Path, writable: bool) -> io::Result<File> {
    let path_cstr = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path {} contains interior NUL bytes", path.display()),
        )
    })?;
    let mut flags = libc::O_CLOEXEC;
    #[cfg(target_os = "linux")]
    {
        flags |= libc::O_DIRECT;
    }
    flags |= if writable {
        libc::O_RDWR
    } else {
        libc::O_RDONLY
    };
    let fd = unsafe { libc::open(path_cstr.as_ptr(), flags, 0o644) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn storage_capacity_bytes(file: &File) -> io::Result<u64> {
    let mut size = 0_u64;
    let rc = unsafe { libc::ioctl(file.as_raw_fd(), BLKGETSIZE64_IOCTL, &mut size) };
    if rc == 0 {
        Ok(size)
    } else {
        Ok(file.metadata()?.len())
    }
}

fn direct_read_exact(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<()> {
    let mut done = 0_usize;
    while done < buffer.len() {
        let rc = unsafe {
            libc::pread(
                file.as_raw_fd(),
                buffer[done..].as_mut_ptr().cast(),
                buffer.len() - done,
                (offset + done as u64) as libc::off_t,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        if rc == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "chunk-media direct read stopped early at {}+{}",
                    offset, done
                ),
            ));
        }
        done += rc as usize;
    }
    Ok(())
}

fn direct_write_exact(file: &File, buffer: &[u8], offset: u64) -> io::Result<()> {
    let mut done = 0_usize;
    while done < buffer.len() {
        let rc = unsafe {
            libc::pwrite(
                file.as_raw_fd(),
                buffer[done..].as_ptr().cast(),
                buffer.len() - done,
                (offset + done as u64) as libc::off_t,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        if rc == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "chunk-media direct write stopped early at {}+{}",
                    offset, done
                ),
            ));
        }
        done += rc as usize;
    }
    Ok(())
}

fn direct_fdatasync(file: &File) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    let rc = unsafe { libc::fdatasync(file.as_raw_fd()) };
    #[cfg(not(target_os = "linux"))]
    let rc = unsafe { libc::fsync(file.as_raw_fd()) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn is_zero_block(buffer: &[u8]) -> bool {
    buffer.iter().all(|byte| *byte == 0)
}

struct AlignedIoBuffer {
    ptr: *mut u8,
    len: usize,
}

impl AlignedIoBuffer {
    fn zeroed(len: usize) -> io::Result<Self> {
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "chunk-media aligned buffer length must be > 0",
            ));
        }
        let layout =
            std::alloc::Layout::from_size_align(len, CHUNK_MEDIA_ALIGN_BYTES).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid chunk-media aligned layout",
                )
            })?;
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "failed to allocate chunk-media aligned buffer",
            ));
        }
        Ok(Self { ptr, len })
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for AlignedIoBuffer {
    fn drop(&mut self) {
        let layout =
            std::alloc::Layout::from_size_align(self.len, CHUNK_MEDIA_ALIGN_BYTES).unwrap();
        unsafe {
            std::alloc::dealloc(self.ptr, layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, OpenOptions};
    use std::io::Read;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample_v2_header(stripe: u16, object_id: u32, object_version: u16, frag: u16) -> ChunkMediaSlotHeader {
        ChunkMediaSlotHeader {
            state: ChunkMediaEntryState::Live,
            drive_id: 7,
            chunk_id: ChunkId([0x5A; 32]),
            record: LocationRecord::extent(7, 8192, 1024, 1_048_576, 3, 0xABCD),
            stripe,
            object_id,
            object_version,
            frag,
        }
    }

    #[test]
    fn slot_header_v2_round_trips_self_describing_identity() {
        let header = sample_v2_header(3, 0x1234_5678, 2, 7);
        let mut buf = vec![0u8; CHUNK_MEDIA_SLOT_HEADER_BYTES as usize];
        encode_slot_header(&mut buf, header).unwrap();
        let decoded = decode_slot_header(&buf).expect("v2 header must decode");
        assert_eq!(decoded.stripe, 3);
        assert_eq!(decoded.object_id, 0x1234_5678);
        assert_eq!(decoded.object_version, 2);
        assert_eq!(decoded.frag, 7);
        assert_eq!(decoded.chunk_id, header.chunk_id);
        assert_eq!(decoded.record.generation, 3);
    }

    #[test]
    fn slot_header_v2_crc_covers_self_describing_fields() {
        let header = sample_v2_header(1, 0x1111_2222, 1, 4);
        let mut buf = vec![0u8; CHUNK_MEDIA_SLOT_HEADER_BYTES as usize];
        encode_slot_header(&mut buf, header).unwrap();
        // Flip a byte inside the object_id field [72..76]; the CRC over [..80] must catch it.
        buf[72] ^= 0xFF;
        assert!(
            decode_slot_header(&buf).is_none(),
            "CRC must cover the self-describing identity fields"
        );
    }

    #[test]
    fn slot_header_v1_media_is_rejected_by_the_version_gate() {
        let header = sample_v2_header(0, 0, 0, 0);
        let mut buf = vec![0u8; CHUNK_MEDIA_SLOT_HEADER_BYTES as usize];
        encode_slot_header(&mut buf, header).unwrap();
        // Downgrade the on-media version field to v1; decode must reject before reading payload.
        buf[8..10].copy_from_slice(&1u16.to_le_bytes());
        assert!(
            decode_slot_header(&buf).is_none(),
            "old v1 media must be rejected at the version gate"
        );
    }

    #[test]
    fn planned_span_includes_superblock_and_slot_headers() {
        let layout = ChunkMediaLayoutSpec {
            layout_kind: ChunkMediaLayoutKind::ExtentOnly,
            extent_bytes: 1024 * 1024,
            packed_bytes: 16 * 1024,
            key_slots: 2,
        };
        let span = planned_chunk_media_span_bytes(&layout).unwrap();
        assert_eq!(
            span,
            CHUNK_MEDIA_SUPERBLOCK_BYTES
                + 4 * (CHUNK_MEDIA_SLOT_HEADER_BYTES + 1024_u64 * 1024_u64)
        );
    }

    #[test]
    fn mixed_layout_offsets_leave_room_for_headers() {
        let layout = ChunkMediaLayoutSpec {
            layout_kind: ChunkMediaLayoutKind::Mixed,
            extent_bytes: 1024 * 1024,
            packed_bytes: 16 * 1024,
            key_slots: 2,
        };
        let extent = chunk_media_record_for_key(&layout, 0, 0, 1).unwrap();
        let packed = chunk_media_record_for_key(&layout, 0, 1, 1).unwrap();
        assert_eq!(
            extent.physical_offset,
            CHUNK_MEDIA_SUPERBLOCK_BYTES + CHUNK_MEDIA_SLOT_HEADER_BYTES
        );
        assert_eq!(
            packed.physical_offset,
            CHUNK_MEDIA_SUPERBLOCK_BYTES
                + 2 * (CHUNK_MEDIA_SLOT_HEADER_BYTES + 1024_u64 * 1024_u64)
                + CHUNK_MEDIA_SLOT_HEADER_BYTES
        );
    }

    #[test]
    fn successive_generations_alternate_publication_lanes_for_same_slot() {
        let layout = ChunkMediaLayoutSpec {
            layout_kind: ChunkMediaLayoutKind::ExtentOnly,
            extent_bytes: 1024 * 1024,
            packed_bytes: 16 * 1024,
            key_slots: 8,
        };
        let first = chunk_media_record_for_slot(&layout, 3, 5, 1).unwrap();
        let second = chunk_media_record_for_slot(&layout, 3, 5, 2).unwrap();
        let third = chunk_media_record_for_slot(&layout, 3, 5, 3).unwrap();
        assert_ne!(first.physical_offset, second.physical_offset);
        assert_eq!(first.physical_offset, third.physical_offset);
        assert_eq!(
            chunk_media_slot_index_for_record(&layout, first).unwrap(),
            5
        );
        assert_eq!(
            chunk_media_slot_index_for_record(&layout, second).unwrap(),
            5
        );
    }

    #[test]
    fn slot_index_roundtrip_is_constant_time_for_extent_only() {
        let layout = ChunkMediaLayoutSpec {
            layout_kind: ChunkMediaLayoutKind::ExtentOnly,
            extent_bytes: 1024 * 1024,
            packed_bytes: 16 * 1024,
            key_slots: 8,
        };
        let record = chunk_media_record_for_slot(&layout, 3, 5, 9).unwrap();
        assert_eq!(
            chunk_media_slot_index_for_record(&layout, record).unwrap(),
            5
        );
    }

    #[test]
    fn slot_index_roundtrip_is_constant_time_for_packed_only() {
        let layout = ChunkMediaLayoutSpec {
            layout_kind: ChunkMediaLayoutKind::PackedOnly,
            extent_bytes: 1024 * 1024,
            packed_bytes: 16 * 1024,
            key_slots: 8,
        };
        let record = chunk_media_record_for_slot(&layout, 3, 6, 9).unwrap();
        assert_eq!(
            chunk_media_slot_index_for_record(&layout, record).unwrap(),
            6
        );
    }

    #[test]
    fn slot_index_roundtrip_is_constant_time_for_mixed_layout() {
        let layout = ChunkMediaLayoutSpec {
            layout_kind: ChunkMediaLayoutKind::Mixed,
            extent_bytes: 1024 * 1024,
            packed_bytes: 16 * 1024,
            key_slots: 8,
        };
        let extent_record = chunk_media_record_for_slot(&layout, 3, 2, 9).unwrap();
        let packed_record = chunk_media_record_for_slot(&layout, 3, 3, 9).unwrap();
        assert_eq!(
            chunk_media_slot_index_for_record(&layout, extent_record).unwrap(),
            2
        );
        assert_eq!(
            chunk_media_slot_index_for_record(&layout, packed_record).unwrap(),
            3
        );
    }

    #[test]
    fn checksum_matches_generated_payload() {
        let layout = ChunkMediaLayoutSpec {
            layout_kind: ChunkMediaLayoutKind::ExtentOnly,
            extent_bytes: 1024 * 1024,
            packed_bytes: 16 * 1024,
            key_slots: 1,
        };
        let chunk_id = ChunkId::from_seed(7);
        let mut record = chunk_media_record_for_key(&layout, 0, 0, 9).unwrap();
        record.checksum = chunk_media_checksum(&chunk_id, &record);
        let observed = chunk_media_checksum(&chunk_id, &record);
        assert_eq!(record.checksum, observed);
    }

    #[test]
    fn superblock_roundtrip() {
        let superblock = ChunkMediaSuperblock {
            version: CHUNK_MEDIA_VERSION,
            layout: ChunkMediaLayoutSpec {
                layout_kind: ChunkMediaLayoutKind::PackedOnly,
                extent_bytes: 1024 * 1024,
                packed_bytes: 16 * 1024,
                key_slots: 123,
            },
            media_span_bytes: 456,
        };
        let mut buf = [0_u8; CHUNK_MEDIA_SUPERBLOCK_BYTES as usize];
        encode_superblock(&mut buf, superblock).unwrap();
        assert_eq!(decode_superblock(&buf), Some(superblock));
    }

    #[test]
    fn ensure_superblock_zeroes_unwritten_slot_headers() {
        let layout = ChunkMediaLayoutSpec {
            layout_kind: ChunkMediaLayoutKind::ExtentOnly,
            extent_bytes: 1024 * 1024,
            packed_bytes: 16 * 1024,
            key_slots: 4,
        };
        let span_bytes = planned_chunk_media_span_bytes(&layout).unwrap();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "kix-chunk-media-{}-{unique}.img",
            std::process::id()
        ));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(span_bytes).unwrap();
        drop(file);

        let garbage = vec![0x5a_u8; CHUNK_MEDIA_SLOT_HEADER_BYTES as usize];
        let mut dirty = OpenOptions::new().write(true).open(&path).unwrap();
        std::io::Write::write_all(&mut dirty, &garbage).unwrap();
        drop(dirty);

        let config = ChunkMediaWriteConfig {
            drive_id: 0,
            span: ChunkMediaSpanConfig {
                media_path: path.clone(),
                media_offset_bytes: 0,
                media_len_bytes: Some(span_bytes),
            },
            layout,
        };
        ensure_chunk_media_superblock(&config).unwrap();

        let mut clean = OpenOptions::new().read(true).open(&path).unwrap();
        let mut header = vec![0_u8; CHUNK_MEDIA_SLOT_HEADER_BYTES as usize];
        use std::io::Seek;
        use std::io::SeekFrom;
        clean
            .seek(SeekFrom::Start(CHUNK_MEDIA_SUPERBLOCK_BYTES))
            .unwrap();
        clean.read_exact(&mut header).unwrap();
        assert!(is_zero_block(&header));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rebuild_prefers_latest_live_generation_and_honors_tombstones() {
        let layout = ChunkMediaLayoutSpec {
            layout_kind: ChunkMediaLayoutKind::ExtentOnly,
            extent_bytes: 1024 * 1024,
            packed_bytes: 16 * 1024,
            key_slots: 4,
        };
        let span_bytes = planned_chunk_media_span_bytes(&layout).unwrap();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "kix-chunk-media-rebuild-{}-{unique}.img",
            std::process::id()
        ));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(span_bytes).unwrap();
        drop(file);

        let config = ChunkMediaWriteConfig {
            drive_id: 0,
            span: ChunkMediaSpanConfig {
                media_path: path.clone(),
                media_offset_bytes: 0,
                media_len_bytes: Some(span_bytes),
            },
            layout,
        };
        let chunk_live = ChunkId::from_seed(7);
        let chunk_deleted = ChunkId::from_seed(9);

        let handle = ChunkMediaHandle::open(&config).unwrap();
        let first = handle
            .write_payload_against_current_timed(1, None, chunk_live, 1, &vec![0x11; 1024 * 1024])
            .unwrap()
            .record;
        let second = handle
            .write_payload_against_current_timed(
                1,
                Some(first),
                chunk_live,
                2,
                &vec![0x22; 1024 * 1024],
            )
            .unwrap()
            .record;
        assert_ne!(first.physical_offset, second.physical_offset);
        let deleted = handle
            .write_payload_against_current_timed(
                2,
                None,
                chunk_deleted,
                1,
                &vec![0x33; 1024 * 1024],
            )
            .unwrap()
            .record;
        handle
            .write_tombstone_against_current(chunk_deleted, deleted, 2)
            .unwrap();

        let rebuilt = rebuild_from_chunk_media(&config.span).unwrap();
        assert_eq!(rebuilt.entries.get(&chunk_live), Some(&second));
        assert!(!rebuilt.entries.contains_key(&chunk_deleted));
        // The writer carries zero identity here, but rebuild must SURFACE the
        // self-describing identity for live entries and omit tombstoned ones.
        assert_eq!(
            rebuilt.identities.get(&chunk_live),
            Some(&ChunkSelfDescribingIdentity::default())
        );
        assert!(!rebuilt.identities.contains_key(&chunk_deleted));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn write_and_read_smaller_payload_in_extent_slot_roundtrips_logical_bytes() {
        let layout = ChunkMediaLayoutSpec {
            layout_kind: ChunkMediaLayoutKind::ExtentOnly,
            extent_bytes: 1024 * 1024,
            packed_bytes: 16 * 1024,
            key_slots: 2,
        };
        let span_bytes = planned_chunk_media_span_bytes(&layout).unwrap();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "kix-chunk-media-small-payload-{}-{unique}.img",
            std::process::id()
        ));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(span_bytes).unwrap();
        drop(file);

        let config = ChunkMediaWriteConfig {
            drive_id: 0,
            span: ChunkMediaSpanConfig {
                media_path: path.clone(),
                media_offset_bytes: 0,
                media_len_bytes: Some(span_bytes),
            },
            layout,
        };
        let superblock = ensure_chunk_media_superblock(&config).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let chunk_id = ChunkId::from_seed(42);
        let record = chunk_media_record_for_key(&layout, 0, 0, 1).unwrap();
        let payload = vec![0x5a_u8; 128 * 1024];

        let written = write_chunk_media_payload_with_file(
            &file,
            &config,
            superblock,
            0,
            PublicationLaneSelector::AgainstCurrent(Some(record)),
            chunk_id,
            1,
            &payload,
            WriteDurability::Barrier,
        )
        .unwrap();
        assert_eq!(written.record.logical_length as usize, payload.len());
        assert_eq!(written.record.stored_length as usize, 1024 * 1024);

        let roundtrip =
            read_chunk_media_payload_with_file(&file, &config.span, chunk_id, written.record)
                .unwrap();
        assert_eq!(roundtrip.payload, payload);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn deferred_lane_writes_share_a_single_barrier_and_roundtrip() {
        let layout = ChunkMediaLayoutSpec {
            layout_kind: ChunkMediaLayoutKind::ExtentOnly,
            extent_bytes: 1024 * 1024,
            packed_bytes: 16 * 1024,
            key_slots: 4,
        };
        let span_bytes = planned_chunk_media_span_bytes(&layout).unwrap();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "kix-chunk-media-deferred-{}-{unique}.img",
            std::process::id()
        ));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(span_bytes).unwrap();
        drop(file);

        let config = ChunkMediaWriteConfig {
            drive_id: 0,
            span: ChunkMediaSpanConfig {
                media_path: path.clone(),
                media_offset_bytes: 0,
                media_len_bytes: Some(span_bytes),
            },
            layout,
        };
        let handle = ChunkMediaHandle::open(&config).unwrap();

        // A fresh slot resolves to lane 0; the next publication flips to lane 1.
        assert_eq!(handle.next_publication_lane_for_slot(0, None).unwrap(), 0);

        let chunk_a = ChunkId::from_seed(101);
        let chunk_b = ChunkId::from_seed(202);
        let payload_a = vec![0xa1_u8; 64 * 1024];
        let payload_b = vec![0xb2_u8; 96 * 1024];

        // Write both entries WITHOUT a per-entry barrier, then one shared barrier.
        let written_a = handle
            .write_payload_to_lane_unsynced(0, 0, chunk_a, 1, &payload_a)
            .unwrap();
        let written_b = handle
            .write_payload_to_lane_unsynced(1, 0, chunk_b, 1, &payload_b)
            .unwrap();
        assert_eq!(written_a.timing.fsync, Duration::ZERO);
        assert_eq!(written_b.timing.fsync, Duration::ZERO);
        handle.fdatasync().unwrap();

        // The explicit-lane write lands in the same place the against-current
        // path would have chosen for a fresh slot.
        let against_current = chunk_media_record_for_slot(&layout, 0, 0, 1).unwrap();
        assert_eq!(
            written_a.record.physical_offset,
            against_current.physical_offset
        );

        let roundtrip_a = handle.read_payload(chunk_a, written_a.record).unwrap();
        let roundtrip_b = handle.read_payload(chunk_b, written_b.record).unwrap();
        assert_eq!(roundtrip_a, payload_a);
        assert_eq!(roundtrip_b, payload_b);

        // The next deferred write for slot 0 must flip to lane 1, leaving the
        // first generation readable until its KIX record is superseded.
        let next_lane = handle
            .next_publication_lane_for_slot(0, Some(written_a.record))
            .unwrap();
        assert_eq!(next_lane, 1);
        let payload_a2 = vec![0xc3_u8; 80 * 1024];
        let written_a2 = handle
            .write_payload_to_lane_unsynced(0, next_lane, chunk_a, 2, &payload_a2)
            .unwrap();
        handle.fdatasync().unwrap();
        assert_ne!(
            written_a2.record.physical_offset,
            written_a.record.physical_offset
        );
        assert_eq!(
            handle.read_payload(chunk_a, written_a.record).unwrap(),
            payload_a
        );
        assert_eq!(
            handle.read_payload(chunk_a, written_a2.record).unwrap(),
            payload_a2
        );

        fs::remove_file(path).unwrap();
    }
}
