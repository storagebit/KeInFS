// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::hardware::crc32_ieee;
use crate::types::{ChunkId, LocationRecord};
use libc;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};

const FILE_MAGIC: u32 = u32::from_le_bytes(*b"KIXA");
const FRAME_MAGIC: u32 = u32::from_le_bytes(*b"KIXF");
const VERSION: u32 = 2;
const KIX_IO_ALIGN: usize = 4096;
const FILE_HEADER_LEN: usize = KIX_IO_ALIGN;
const FRAME_HEADER_LEN: usize = KIX_IO_ALIGN;
const CHECKPOINT_ENTRY_LEN: usize = 32 + LocationRecord::ENCODED_LEN;
const DELTA_ENTRY_LEN: usize = 64;
const FLAG_FIXED_SPAN: u16 = 1;
const BLKGETSIZE64_IOCTL: libc::c_ulong = 0x8008_1272;

#[derive(Clone, Debug)]
pub struct DriveConfig {
    pub id: u16,
    pub arena_path: PathBuf,
    pub arena_offset_bytes: u64,
    pub arena_len_bytes: Option<u64>,
    pub numa_node: Option<i32>,
    pub io_mode: ArenaIoMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArenaIoMode {
    DirectUring,
}

impl ArenaIoMode {
    pub fn as_str(self) -> &'static str {
        "direct-uring"
    }
}

impl DriveConfig {
    pub fn file(id: u16, arena_path: PathBuf) -> Self {
        Self {
            id,
            arena_path,
            arena_offset_bytes: 0,
            arena_len_bytes: None,
            numa_node: None,
            io_mode: ArenaIoMode::DirectUring,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ArenaHeader {
    drive_id: u16,
    flags: u16,
    arena_bytes: u64,
    write_head: u64,
}

impl ArenaHeader {
    fn new(drive_id: u16, arena_bytes: Option<u64>, write_head: u64) -> Self {
        Self {
            drive_id,
            flags: if arena_bytes.is_some() {
                FLAG_FIXED_SPAN
            } else {
                0
            },
            arena_bytes: arena_bytes.unwrap_or(0),
            write_head,
        }
    }

    fn fixed_span_bytes(self) -> Option<u64> {
        if self.flags & FLAG_FIXED_SPAN != 0 {
            Some(self.arena_bytes)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum FrameKind {
    Checkpoint = 1,
    DeltaBatch = 2,
}

impl FrameKind {
    fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Checkpoint),
            2 => Some(Self::DeltaBatch),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum DeltaOp {
    Upsert = 1,
    Delete = 2,
}

impl DeltaOp {
    fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Upsert),
            2 => Some(Self::Delete),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeltaEntry {
    pub op: DeltaOp,
    pub chunk_id: ChunkId,
    pub record: Option<LocationRecord>,
}

impl DeltaEntry {
    pub fn upsert(chunk_id: ChunkId, record: LocationRecord) -> Self {
        Self {
            op: DeltaOp::Upsert,
            chunk_id,
            record: Some(record),
        }
    }

    pub fn delete(chunk_id: ChunkId) -> Self {
        Self {
            op: DeltaOp::Delete,
            chunk_id,
            record: None,
        }
    }

    fn encode(self) -> [u8; DELTA_ENTRY_LEN] {
        let mut out = [0_u8; DELTA_ENTRY_LEN];
        out[0] = self.op as u8;
        out[4..36].copy_from_slice(&self.chunk_id.0);
        if let Some(record) = self.record {
            out[36..64].copy_from_slice(&record.encode());
        }
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < DELTA_ENTRY_LEN {
            return None;
        }
        let op = DeltaOp::from_byte(bytes[0])?;
        let chunk_id = ChunkId(bytes[4..36].try_into().ok()?);
        let record = match op {
            DeltaOp::Upsert => Some(LocationRecord::decode(&bytes[36..64])?),
            DeltaOp::Delete => None,
        };
        Some(Self {
            op,
            chunk_id,
            record,
        })
    }
}

#[derive(Debug)]
pub struct DriveRecovery {
    pub drive_id: u16,
    pub entries: HashMap<ChunkId, LocationRecord>,
    pub replay_len: u64,
    pub tail_corruption: bool,
    pub rebuild_required: bool,
    pub applied_frames: usize,
}

#[cfg(target_os = "linux")]
struct DirectIoUring {
    read_ring: io_uring::IoUring,
    write_ring: io_uring::IoUring,
}

#[cfg(not(target_os = "linux"))]
struct DirectIoUring;

#[cfg(target_os = "linux")]
impl DirectIoUring {
    fn new() -> io::Result<Self> {
        Ok(Self {
            read_ring: io_uring::IoUring::new(16)?,
            write_ring: io_uring::IoUring::new(16)?,
        })
    }

    fn verify_runtime(&mut self) -> io::Result<()> {
        use io_uring::opcode;

        let entry = opcode::Nop::new().build();
        submit_single_entry(&mut self.read_ring, entry, "io_uring nop")?;
        complete_result(&mut self.read_ring, "io_uring nop", Some(0))?;
        Ok(())
    }

    fn read_exact(
        &mut self,
        file: &File,
        offset: u64,
        buf: &mut AlignedIoBuffer,
    ) -> io::Result<()> {
        self.submit_read(file, offset, buf)?;
        Ok(())
    }

    fn write_all(&mut self, file: &File, offset: u64, buf: &AlignedIoBuffer) -> io::Result<()> {
        self.submit_write(file, offset, buf)
    }

    fn sync_data(&mut self, file: &File) -> io::Result<()> {
        use io_uring::{opcode, types};

        let entry = opcode::Fsync::new(types::Fd(file.as_raw_fd())).build();
        submit_single_entry(&mut self.write_ring, entry, "io_uring fsync")?;
        complete_result(&mut self.write_ring, "io_uring fsync", None)?;
        Ok(())
    }

    fn submit_read(
        &mut self,
        file: &File,
        offset: u64,
        buf: &mut AlignedIoBuffer,
    ) -> io::Result<()> {
        use io_uring::{opcode, types};

        let entry = opcode::Read::new(
            types::Fd(file.as_raw_fd()),
            buf.as_mut_ptr(),
            buf.len() as u32,
        )
        .offset(offset)
        .build();
        submit_single_entry(&mut self.read_ring, entry, "io_uring read")?;
        complete_result(&mut self.read_ring, "io_uring read", Some(buf.len() as i32))?;
        Ok(())
    }

    fn submit_write(&mut self, file: &File, offset: u64, buf: &AlignedIoBuffer) -> io::Result<()> {
        use io_uring::{opcode, types};

        let entry = opcode::Write::new(types::Fd(file.as_raw_fd()), buf.as_ptr(), buf.len() as u32)
            .offset(offset)
            .build();
        submit_single_entry(&mut self.write_ring, entry, "io_uring write")?;
        complete_result(
            &mut self.write_ring,
            "io_uring write",
            Some(buf.len() as i32),
        )?;
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
impl DirectIoUring {
    fn new() -> io::Result<Self> {
        Ok(Self)
    }

    fn verify_runtime(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn read_exact(
        &mut self,
        file: &File,
        offset: u64,
        buf: &mut AlignedIoBuffer,
    ) -> io::Result<()> {
        direct_read_exact(file, buf.as_mut_slice(), offset)
    }

    fn write_all(&mut self, file: &File, offset: u64, buf: &AlignedIoBuffer) -> io::Result<()> {
        direct_write_exact(file, buf.as_slice(), offset)
    }

    fn sync_data(&mut self, file: &File) -> io::Result<()> {
        direct_fdatasync(file)
    }
}

#[cfg(target_os = "linux")]
fn submit_single_entry(
    ring: &mut io_uring::IoUring,
    entry: io_uring::squeue::Entry,
    op_name: &str,
) -> io::Result<()> {
    unsafe {
        ring.submission().push(&entry).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("{op_name} submission queue is full"),
            )
        })?;
    }
    ring.submit_and_wait(1)?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn complete_result(
    ring: &mut io_uring::IoUring,
    op_name: &str,
    expected_len: Option<i32>,
) -> io::Result<i32> {
    let mut completion = ring.completion();
    let cqe = completion.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("{op_name} completed without a CQE"),
        )
    })?;
    let result = cqe.result();
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }
    if let Some(expected_len) = expected_len {
        if result != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("{op_name} completed only {result} bytes; expected {expected_len}"),
            ));
        }
    }
    Ok(result)
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
                "aligned I/O buffer length must be > 0",
            ));
        }
        let layout = std::alloc::Layout::from_size_align(len, KIX_IO_ALIGN).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid aligned I/O layout")
        })?;
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "failed to allocate aligned I/O buffer",
            ));
        }
        Ok(Self { ptr, len })
    }

    fn from_padded_bytes(bytes: &[u8]) -> io::Result<Self> {
        let aligned_len = align_up_usize(bytes.len(), KIX_IO_ALIGN)?;
        let mut buf = Self::zeroed(aligned_len)?;
        buf.as_mut_slice()[..bytes.len()].copy_from_slice(bytes);
        Ok(buf)
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    #[cfg(target_os = "linux")]
    fn as_ptr(&self) -> *const u8 {
        self.ptr.cast_const()
    }

    #[cfg(target_os = "linux")]
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    #[cfg(target_os = "linux")]
    fn len(&self) -> usize {
        self.len
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
                format!("direct read stopped early at {}+{}", offset, done),
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
                format!("direct write stopped early at {}+{}", offset, done),
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

impl Drop for AlignedIoBuffer {
    fn drop(&mut self) {
        if self.len == 0 {
            return;
        }
        let layout = std::alloc::Layout::from_size_align(self.len, KIX_IO_ALIGN).unwrap();
        unsafe {
            std::alloc::dealloc(self.ptr, layout);
        }
    }
}

fn is_aligned_u64(value: u64, align: u64) -> bool {
    align != 0 && value % align == 0
}

fn io_mode_display_name(io_mode: ArenaIoMode) -> &'static str {
    io_mode.as_str()
}

pub struct DriveArena {
    path: PathBuf,
    drive_id: u16,
    file: File,
    arena_offset_bytes: u64,
    arena_len_bytes: Option<u64>,
    write_head: u64,
    is_block_device: bool,
    io_mode: ArenaIoMode,
    uring: Option<DirectIoUring>,
}

impl DriveArena {
    pub fn open(path: impl AsRef<Path>, drive_id: u16) -> io::Result<Self> {
        Self::open_config(&DriveConfig::file(drive_id, path.as_ref().to_path_buf()))
    }

    pub fn reset(path: impl AsRef<Path>, drive_id: u16) -> io::Result<Self> {
        Self::reset_config(&DriveConfig::file(drive_id, path.as_ref().to_path_buf()))
    }

    pub fn recover_from_path(path: impl AsRef<Path>, drive_id: u16) -> io::Result<DriveRecovery> {
        Self::recover(&DriveConfig::file(drive_id, path.as_ref().to_path_buf()))
    }

    pub fn open_config(config: &DriveConfig) -> io::Result<Self> {
        let path = config.arena_path.clone();
        let existed = path.exists();
        let io_mode = ArenaIoMode::DirectUring;
        validate_io_layout(config, io_mode)?;
        let mut file = open_data_file(&path, !existed, io_mode)?;
        let metadata = file.metadata()?;
        let is_block_device = metadata.file_type().is_block_device();
        let arena_len_bytes = resolve_arena_len_bytes(
            &file,
            is_block_device,
            config.arena_offset_bytes,
            config.arena_len_bytes,
        )?;

        if !existed || (!is_block_device && metadata.len() == 0) {
            let write_head = FILE_HEADER_LEN as u64;
            let header = ArenaHeader::new(config.id, arena_len_bytes, write_head);
            write_arena_header_at(&mut file, io_mode, config.arena_offset_bytes, header)?;
            if !is_block_device && arena_len_bytes.is_none() {
                file.set_len(config.arena_offset_bytes + write_head)?;
            }
            sync_data_at(&file, io_mode)?;
            return Ok(Self {
                path,
                drive_id: config.id,
                file,
                arena_offset_bytes: config.arena_offset_bytes,
                arena_len_bytes,
                write_head,
                is_block_device,
                io_mode,
                uring: make_uring(io_mode)?,
            });
        }

        let header = read_arena_header(&mut file, io_mode, config.arena_offset_bytes)?;
        validate_arena_header(header, config.id, arena_len_bytes)?;
        Ok(Self {
            path,
            drive_id: config.id,
            file,
            arena_offset_bytes: config.arena_offset_bytes,
            arena_len_bytes,
            write_head: header.write_head,
            is_block_device,
            io_mode,
            uring: make_uring(io_mode)?,
        })
    }

    pub fn reset_config(config: &DriveConfig) -> io::Result<Self> {
        let path = config.arena_path.clone();
        let existed = path.exists();
        let io_mode = ArenaIoMode::DirectUring;
        validate_io_layout(config, io_mode)?;
        let mut file = open_data_file(&path, !existed, io_mode)?;
        let metadata = file.metadata()?;
        let is_block_device = metadata.file_type().is_block_device();
        let arena_len_bytes = resolve_arena_len_bytes(
            &file,
            is_block_device,
            config.arena_offset_bytes,
            config.arena_len_bytes,
        )?;
        let write_head = FILE_HEADER_LEN as u64;
        let header = ArenaHeader::new(config.id, arena_len_bytes, write_head);

        if !is_block_device && arena_len_bytes.is_none() {
            file.set_len(0)?;
        }
        write_arena_header_at(&mut file, io_mode, config.arena_offset_bytes, header)?;
        if !is_block_device && arena_len_bytes.is_none() {
            file.set_len(config.arena_offset_bytes + write_head)?;
        }
        sync_data_at(&file, io_mode)?;

        Ok(Self {
            path,
            drive_id: config.id,
            file,
            arena_offset_bytes: config.arena_offset_bytes,
            arena_len_bytes,
            write_head,
            is_block_device,
            io_mode,
            uring: make_uring(io_mode)?,
        })
    }

    pub fn recover(config: &DriveConfig) -> io::Result<DriveRecovery> {
        let path = config.arena_path.as_path();
        if !path.exists() {
            return Ok(DriveRecovery {
                drive_id: config.id,
                entries: HashMap::new(),
                replay_len: 0,
                tail_corruption: false,
                rebuild_required: false,
                applied_frames: 0,
            });
        }

        let io_mode = ArenaIoMode::DirectUring;
        validate_io_layout(config, io_mode)?;
        let mut file = open_read_file(path, io_mode)?;
        let metadata = file.metadata()?;
        let is_block_device = metadata.file_type().is_block_device();
        let arena_len_bytes = resolve_arena_len_bytes(
            &file,
            is_block_device,
            config.arena_offset_bytes,
            config.arena_len_bytes,
        )?;

        if !is_block_device && metadata.len() == 0 {
            return Ok(DriveRecovery {
                drive_id: config.id,
                entries: HashMap::new(),
                replay_len: 0,
                tail_corruption: false,
                rebuild_required: false,
                applied_frames: 0,
            });
        }

        let header = match read_arena_header(&mut file, io_mode, config.arena_offset_bytes) {
            Ok(header) => header,
            Err(_) => {
                return Ok(DriveRecovery {
                    drive_id: config.id,
                    entries: HashMap::new(),
                    replay_len: 0,
                    tail_corruption: false,
                    rebuild_required: true,
                    applied_frames: 0,
                });
            }
        };

        if validate_arena_header(header, config.id, arena_len_bytes).is_err() {
            return Ok(DriveRecovery {
                drive_id: config.id,
                entries: HashMap::new(),
                replay_len: 0,
                tail_corruption: false,
                rebuild_required: true,
                applied_frames: 0,
            });
        }

        let mut entries = HashMap::new();
        let mut tail_corruption = false;
        let rebuild_required = false;
        let mut applied_frames = 0;
        let mut offset = FILE_HEADER_LEN as u64;
        let total_len = header.write_head;
        let available_span_len = available_span_len(
            &file,
            is_block_device,
            config.arena_offset_bytes,
            arena_len_bytes,
        )?;
        let mut uring = make_uring(io_mode)?;

        while offset < total_len {
            if total_len - offset < FRAME_HEADER_LEN as u64
                || available_span_len.saturating_sub(offset) < FRAME_HEADER_LEN as u64
            {
                tail_corruption = true;
                break;
            }

            let frame_header = read_exact_at(
                &mut file,
                io_mode,
                uring.as_mut(),
                config.arena_offset_bytes + offset,
                FRAME_HEADER_LEN,
            )?;

            let magic = u32::from_le_bytes(frame_header[0..4].try_into().unwrap());
            if magic != FRAME_MAGIC {
                tail_corruption = true;
                break;
            }

            let Some(kind) = FrameKind::from_byte(frame_header[4]) else {
                tail_corruption = true;
                break;
            };

            let payload_len = u32::from_le_bytes(frame_header[8..12].try_into().unwrap()) as u64;
            let payload_crc = u32::from_le_bytes(frame_header[12..16].try_into().unwrap());

            let payload_disk_len = match align_up_u64(payload_len, KIX_IO_ALIGN as u64) {
                Ok(len) => len,
                Err(_) => {
                    tail_corruption = true;
                    break;
                }
            };

            if total_len - offset - (FRAME_HEADER_LEN as u64) < payload_disk_len
                || available_span_len.saturating_sub(offset + FRAME_HEADER_LEN as u64)
                    < payload_disk_len
            {
                tail_corruption = true;
                break;
            }

            let payload_block = match read_exact_at(
                &mut file,
                io_mode,
                uring.as_mut(),
                config.arena_offset_bytes + offset + FRAME_HEADER_LEN as u64,
                payload_disk_len as usize,
            ) {
                Ok(payload_block) => payload_block,
                Err(_) => {
                    tail_corruption = true;
                    break;
                }
            };
            let payload = &payload_block[..payload_len as usize];

            if crc32_ieee(payload) != payload_crc {
                tail_corruption = true;
                break;
            }

            let decoded_ok = match kind {
                FrameKind::Checkpoint => {
                    if let Some(decoded) = decode_checkpoint(payload) {
                        entries = decoded;
                        true
                    } else {
                        false
                    }
                }
                FrameKind::DeltaBatch => {
                    if let Some(deltas) = decode_delta_batch(payload) {
                        for delta in deltas {
                            apply_delta(&mut entries, delta);
                        }
                        true
                    } else {
                        false
                    }
                }
            };

            if !decoded_ok {
                tail_corruption = true;
                if applied_frames == 0 {
                    entries.clear();
                }
                break;
            }

            applied_frames += 1;
            offset += FRAME_HEADER_LEN as u64 + payload_disk_len;
        }

        Ok(DriveRecovery {
            drive_id: config.id,
            entries,
            replay_len: offset,
            tail_corruption,
            rebuild_required,
            applied_frames,
        })
    }

    pub fn rebuild_from_entries<I>(config: &DriveConfig, entries: I) -> io::Result<DriveRecovery>
    where
        I: IntoIterator<Item = (ChunkId, LocationRecord)>,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let mut arena = Self::reset_config(config)?;
        if !entries.is_empty() {
            arena.write_checkpoint(entries.iter().copied())?;
        }
        Self::recover(config)
    }

    pub fn append_delta(&mut self, delta: DeltaEntry) -> io::Result<()> {
        self.append_delta_batch(std::iter::once(delta))
    }

    pub fn append_delta_batch<I>(&mut self, deltas: I) -> io::Result<()>
    where
        I: IntoIterator<Item = DeltaEntry>,
    {
        let deltas: Vec<_> = deltas.into_iter().collect();
        let mut payload = Vec::with_capacity(4 + deltas.len() * DELTA_ENTRY_LEN);
        payload.extend_from_slice(&(deltas.len() as u32).to_le_bytes());
        for delta in deltas {
            payload.extend_from_slice(&delta.encode());
        }
        self.append_frame(FrameKind::DeltaBatch, &payload)
    }

    pub fn write_checkpoint<I>(&mut self, entries: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (ChunkId, LocationRecord)>,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let mut payload = Vec::with_capacity(4 + entries.len() * CHECKPOINT_ENTRY_LEN);
        payload.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for (chunk_id, record) in entries {
            payload.extend_from_slice(&chunk_id.0);
            payload.extend_from_slice(&record.encode());
        }
        self.append_frame(FrameKind::Checkpoint, &payload)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn drive_id(&self) -> u16 {
        self.drive_id
    }

    pub fn io_mode(&self) -> ArenaIoMode {
        self.io_mode
    }

    pub fn write_head(&self) -> u64 {
        self.write_head
    }

    pub fn arena_offset_bytes(&self) -> u64 {
        self.arena_offset_bytes
    }

    pub fn arena_len_bytes(&self) -> Option<u64> {
        self.arena_len_bytes
    }

    pub fn is_block_device(&self) -> bool {
        self.is_block_device
    }

    pub fn truncate_to(&mut self, len: u64) -> io::Result<()> {
        let len = align_up_u64(len.max(FILE_HEADER_LEN as u64), KIX_IO_ALIGN as u64)?;
        if let Some(arena_len_bytes) = self.arena_len_bytes {
            if len > arena_len_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "truncate target exceeds arena span",
                ));
            }
        }
        self.write_head = len;
        self.write_arena_header(ArenaHeader::new(
            self.drive_id,
            self.arena_len_bytes,
            self.write_head,
        ))?;
        if !self.is_block_device && self.arena_len_bytes.is_none() {
            self.file.set_len(self.arena_offset_bytes + len)?;
        }
        self.sync_data()?;
        Ok(())
    }

    fn append_frame(&mut self, kind: FrameKind, payload: &[u8]) -> io::Result<()> {
        let payload_disk_len = align_up_u64(payload.len() as u64, KIX_IO_ALIGN as u64)?;
        let frame_len = FRAME_HEADER_LEN as u64 + payload_disk_len;
        let next_write_head = self.write_head.checked_add(frame_len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "KIX arena write head overflow")
        })?;
        if let Some(arena_len_bytes) = self.arena_len_bytes {
            if next_write_head > arena_len_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "KIX arena span exhausted",
                ));
            }
        }

        let mut header = [0_u8; FRAME_HEADER_LEN];
        header[0..4].copy_from_slice(&FRAME_MAGIC.to_le_bytes());
        header[4] = kind as u8;
        header[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        let crc = crc32_ieee(payload);
        header[12..16].copy_from_slice(&crc.to_le_bytes());
        let mut frame_bytes = Vec::with_capacity(FRAME_HEADER_LEN + payload_disk_len as usize);
        frame_bytes.extend_from_slice(&header);
        frame_bytes.extend_from_slice(payload);
        frame_bytes.resize(FRAME_HEADER_LEN + payload_disk_len as usize, 0);
        self.write_all_at(self.arena_offset_bytes + self.write_head, &frame_bytes)?;
        self.write_arena_header(ArenaHeader::new(
            self.drive_id,
            self.arena_len_bytes,
            next_write_head,
        ))?;
        self.sync_data()?;
        self.write_head = next_write_head;
        Ok(())
    }

    fn write_arena_header(&mut self, header: ArenaHeader) -> io::Result<()> {
        let mut bytes = [0_u8; FILE_HEADER_LEN];
        bytes[0..4].copy_from_slice(&FILE_MAGIC.to_le_bytes());
        bytes[4..8].copy_from_slice(&VERSION.to_le_bytes());
        bytes[8..10].copy_from_slice(&header.drive_id.to_le_bytes());
        bytes[10..12].copy_from_slice(&header.flags.to_le_bytes());
        bytes[12..20].copy_from_slice(&header.arena_bytes.to_le_bytes());
        bytes[20..28].copy_from_slice(&header.write_head.to_le_bytes());
        self.write_all_at(self.arena_offset_bytes, &bytes)
    }

    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let aligned = AlignedIoBuffer::from_padded_bytes(bytes)?;
        if let Some(uring) = self.uring.as_mut() {
            return uring.write_all(&self.file, offset, &aligned);
        }
        Err(io::Error::new(
            io::ErrorKind::Other,
            "direct io_uring backend requested but no uring context is available",
        ))
    }

    fn sync_data(&mut self) -> io::Result<()> {
        if let Some(uring) = self.uring.as_mut() {
            return uring.sync_data(&self.file);
        }
        Err(io::Error::new(
            io::ErrorKind::Other,
            "direct io_uring backend requested but no uring context is available",
        ))
    }
}

pub fn device_size_bytes(path: impl AsRef<Path>) -> io::Result<u64> {
    let file = OpenOptions::new().read(true).open(path)?;
    let metadata = file.metadata()?;
    storage_capacity_bytes(&file, metadata.file_type().is_block_device())
}

pub fn device_numa_node(path: impl AsRef<Path>) -> io::Result<Option<i32>> {
    let device_name = path.as_ref().file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "device path has no file name")
    })?;
    let sysfs_path = Path::new("/sys/class/block")
        .join(device_name)
        .join("device/numa_node");
    match std::fs::read_to_string(sysfs_path) {
        Ok(raw) => {
            let value = raw
                .trim()
                .parse::<i32>()
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            if value < 0 {
                Ok(None)
            } else {
                Ok(Some(value))
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

pub fn numa_node_cpu_list(node: i32) -> io::Result<Vec<usize>> {
    let raw = std::fs::read_to_string(format!("/sys/devices/system/node/node{node}/cpulist"))?;
    parse_cpu_list(raw.trim())
}

pub fn online_numa_nodes() -> io::Result<Vec<i32>> {
    let raw = std::fs::read_to_string("/sys/devices/system/node/online")?;
    parse_node_list(raw.trim())
}

fn open_data_file(path: &Path, create_if_missing: bool, io_mode: ArenaIoMode) -> io::Result<File> {
    let _ = io_mode;
    open_direct_file(path, create_if_missing, true)
}

fn open_read_file(path: &Path, io_mode: ArenaIoMode) -> io::Result<File> {
    let _ = io_mode;
    open_direct_file(path, false, false)
}

fn open_direct_file(path: &Path, create_if_missing: bool, writable: bool) -> io::Result<File> {
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
    if create_if_missing {
        flags |= libc::O_CREAT;
    }
    let fd = unsafe { libc::open(path_cstr.as_ptr(), flags, 0o644) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn resolve_arena_len_bytes(
    file: &File,
    is_block_device: bool,
    arena_offset_bytes: u64,
    configured_len_bytes: Option<u64>,
) -> io::Result<Option<u64>> {
    let available = storage_capacity_bytes(file, is_block_device)?;
    if arena_offset_bytes > available {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "arena offset exceeds backing store capacity",
        ));
    }

    let arena_len_bytes = if let Some(len) = configured_len_bytes {
        Some(len)
    } else if is_block_device {
        Some(available - arena_offset_bytes)
    } else {
        None
    };

    if let Some(len) = arena_len_bytes {
        if len < FILE_HEADER_LEN as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "arena span must be at least as large as the KIX header",
            ));
        }
        if arena_offset_bytes + len > available {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "arena span exceeds backing store capacity",
            ));
        }
    }

    Ok(arena_len_bytes)
}

fn available_span_len(
    file: &File,
    is_block_device: bool,
    arena_offset_bytes: u64,
    arena_len_bytes: Option<u64>,
) -> io::Result<u64> {
    let available = storage_capacity_bytes(file, is_block_device)?;
    let tail = available.saturating_sub(arena_offset_bytes);
    Ok(match arena_len_bytes {
        Some(len) => len.min(tail),
        None => tail,
    })
}

fn align_up_u64(value: u64, align: u64) -> io::Result<u64> {
    if align == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "alignment must be > 0",
        ));
    }
    let remainder = value % align;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(align - remainder)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "aligned value overflow"))
    }
}

fn align_up_usize(value: usize, align: usize) -> io::Result<usize> {
    align_up_u64(value as u64, align as u64)?
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "aligned length overflow"))
}

fn storage_capacity_bytes(file: &File, is_block_device: bool) -> io::Result<u64> {
    if is_block_device {
        let mut size = 0_u64;
        let rc = unsafe { libc::ioctl(file.as_raw_fd(), BLKGETSIZE64_IOCTL, &mut size) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(size)
    } else {
        Ok(file.metadata()?.len())
    }
}

fn parse_cpu_list(raw: &str) -> io::Result<Vec<usize>> {
    let mut cpus = Vec::new();
    for part in raw.split(',').filter(|part| !part.is_empty()) {
        if let Some((start, end)) = part.split_once('-') {
            let start = start
                .parse::<usize>()
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            let end = end
                .parse::<usize>()
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            if end < start {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "NUMA CPU list range ends before it starts",
                ));
            }
            cpus.extend(start..=end);
        } else {
            cpus.push(
                part.parse::<usize>()
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?,
            );
        }
    }
    Ok(cpus)
}

fn parse_node_list(raw: &str) -> io::Result<Vec<i32>> {
    let mut nodes = Vec::new();
    for part in raw.split(',').filter(|part| !part.is_empty()) {
        if let Some((start, end)) = part.split_once('-') {
            let start = start
                .parse::<i32>()
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            let end = end
                .parse::<i32>()
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            if end < start {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "NUMA node range ends before it starts",
                ));
            }
            nodes.extend(start..=end);
        } else {
            nodes.push(
                part.parse::<i32>()
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?,
            );
        }
    }
    Ok(nodes)
}

fn read_arena_header(
    file: &mut File,
    io_mode: ArenaIoMode,
    arena_offset_bytes: u64,
) -> io::Result<ArenaHeader> {
    let mut uring = make_uring(io_mode)?;
    let bytes = read_exact_at(
        file,
        io_mode,
        uring.as_mut(),
        arena_offset_bytes,
        FILE_HEADER_LEN,
    )?;
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if magic != FILE_MAGIC || version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid KIX arena header",
        ));
    }
    Ok(ArenaHeader {
        drive_id: u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
        flags: u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
        arena_bytes: u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
        write_head: u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
    })
}

fn validate_arena_header(
    header: ArenaHeader,
    expected_drive_id: u16,
    expected_arena_len_bytes: Option<u64>,
) -> io::Result<()> {
    if header.drive_id != expected_drive_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected drive id in KIX arena header",
        ));
    }
    if header.write_head < FILE_HEADER_LEN as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid KIX arena write head",
        ));
    }

    match (expected_arena_len_bytes, header.fixed_span_bytes()) {
        (Some(expected), Some(actual)) if expected == actual => {
            if header.write_head > actual {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "KIX arena write head exceeds fixed span",
                ));
            }
            if !is_aligned_u64(header.write_head, KIX_IO_ALIGN as u64) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "KIX arena write head is not aligned to the direct-I/O granularity",
                ));
            }
            Ok(())
        }
        (None, None) => {
            if !is_aligned_u64(header.write_head, KIX_IO_ALIGN as u64) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "KIX arena write head is not aligned to the direct-I/O granularity",
                ));
            }
            Ok(())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "KIX arena span configuration mismatch",
        )),
    }
}

fn validate_io_layout(config: &DriveConfig, io_mode: ArenaIoMode) -> io::Result<()> {
    let _ = io_mode;
    if !is_aligned_u64(config.arena_offset_bytes, KIX_IO_ALIGN as u64) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "direct io_uring backend requires arena_offset_bytes to be aligned to {} bytes",
                KIX_IO_ALIGN
            ),
        ));
    }
    if let Some(len) = config.arena_len_bytes {
        if !is_aligned_u64(len, KIX_IO_ALIGN as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "direct io_uring backend requires arena_len_bytes to be aligned to {} bytes",
                    KIX_IO_ALIGN
                ),
            ));
        }
    }
    Ok(())
}

pub fn preflight_drive_requirements(config: &DriveConfig) -> io::Result<ArenaIoMode> {
    let existed = config.arena_path.exists();
    let io_mode = ArenaIoMode::DirectUring;
    validate_io_layout(config, io_mode).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "KIX startup preflight failed for drive {} at {} using backend {}: {}",
                config.id,
                config.arena_path.display(),
                io_mode_display_name(io_mode),
                err
            ),
        )
    })?;

    if !existed {
        if let Some(parent) = config.arena_path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!(
                        "KIX startup preflight failed for drive {} at {}: could not create parent directory {}: {}",
                        config.id,
                        config.arena_path.display(),
                        parent.display(),
                        err
                    ),
                )
            })?;
        }
    }

    let writable = true;
    let file = open_direct_file(&config.arena_path, !existed, writable).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "KIX startup preflight failed for drive {} at {} using backend {}: could not open the arena path: {}",
                config.id,
                config.arena_path.display(),
                io_mode_display_name(io_mode),
                err
            ),
        )
    })?;

    let metadata = file.metadata().map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "KIX startup preflight failed for drive {} at {} using backend {}: could not stat the arena path after opening it: {}",
                config.id,
                config.arena_path.display(),
                io_mode_display_name(io_mode),
                err
            ),
        )
    })?;
    let is_block_device = metadata.file_type().is_block_device();
    let capacity_bytes = storage_capacity_bytes(&file, is_block_device).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "KIX startup preflight failed for drive {} at {} using backend {}: could not determine arena capacity: {}",
                config.id,
                config.arena_path.display(),
                io_mode_display_name(io_mode),
                err
            ),
        )
    })?;
    let _ = resolve_arena_len_bytes(
        &file,
        is_block_device,
        config.arena_offset_bytes,
        config.arena_len_bytes,
    )
    .map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "KIX startup preflight failed for drive {} at {} using backend {}: the configured arena offset/span does not fit the backing store (capacity {} bytes): {}",
                config.id,
                config.arena_path.display(),
                io_mode_display_name(io_mode),
                capacity_bytes,
                err
            ),
        )
    })?;

    if io_mode == ArenaIoMode::DirectUring {
        let mut uring = make_uring(io_mode)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "KIX startup preflight failed for drive {} at {} using backend {}: io_uring context is unexpectedly unavailable after backend selection",
                    config.id,
                    config.arena_path.display(),
                    io_mode_display_name(io_mode),
                ),
            )
        })?;
        uring.verify_runtime().map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "KIX startup preflight failed for drive {} at {} using backend {}: io_uring created, but a NOP submission/completion probe did not succeed: {}",
                    config.id,
                    config.arena_path.display(),
                    io_mode_display_name(io_mode),
                    err
                ),
            )
        })?;
    }

    Ok(io_mode)
}

fn write_arena_header_at(
    file: &mut File,
    io_mode: ArenaIoMode,
    arena_offset_bytes: u64,
    header: ArenaHeader,
) -> io::Result<()> {
    let mut bytes = [0_u8; FILE_HEADER_LEN];
    bytes[0..4].copy_from_slice(&FILE_MAGIC.to_le_bytes());
    bytes[4..8].copy_from_slice(&VERSION.to_le_bytes());
    bytes[8..10].copy_from_slice(&header.drive_id.to_le_bytes());
    bytes[10..12].copy_from_slice(&header.flags.to_le_bytes());
    bytes[12..20].copy_from_slice(&header.arena_bytes.to_le_bytes());
    bytes[20..28].copy_from_slice(&header.write_head.to_le_bytes());
    let aligned = AlignedIoBuffer::from_padded_bytes(&bytes)?;
    let mut uring = make_uring(io_mode)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            "direct io_uring backend requested but no uring context is available",
        )
    })?;
    uring.write_all(file, arena_offset_bytes, &aligned)
}

fn sync_data_at(file: &File, io_mode: ArenaIoMode) -> io::Result<()> {
    let mut uring = make_uring(io_mode)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            "direct io_uring backend requested but no uring context is available",
        )
    })?;
    uring.sync_data(file)
}

fn make_uring(io_mode: ArenaIoMode) -> io::Result<Option<DirectIoUring>> {
    let _ = io_mode;
    DirectIoUring::new().map(Some).map_err(|err| {
        if err.kind() == io::ErrorKind::PermissionDenied {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "io_uring setup failed with EPERM; verify kernel io_uring policy or run KIX with sufficient privilege: {err}"
                ),
            )
        } else {
            err
        }
    })
}

fn read_exact_at(
    file: &mut File,
    io_mode: ArenaIoMode,
    uring: Option<&mut DirectIoUring>,
    offset: u64,
    len: usize,
) -> io::Result<Vec<u8>> {
    let _ = io_mode;
    let aligned_len = align_up_usize(len, KIX_IO_ALIGN)?;
    let mut buf = AlignedIoBuffer::zeroed(aligned_len)?;
    let uring = uring.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            "direct io_uring backend requested but no uring context is available",
        )
    })?;
    uring.read_exact(file, offset, &mut buf)?;
    Ok(buf.as_slice()[..len].to_vec())
}

fn decode_checkpoint(payload: &[u8]) -> Option<HashMap<ChunkId, LocationRecord>> {
    if payload.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes(payload[0..4].try_into().ok()?) as usize;
    let expected_len = 4 + count * CHECKPOINT_ENTRY_LEN;
    if payload.len() != expected_len {
        return None;
    }

    let mut out = HashMap::with_capacity(count);
    let mut offset = 4;
    for _ in 0..count {
        let chunk_id = ChunkId(payload[offset..offset + 32].try_into().ok()?);
        offset += 32;
        let record =
            LocationRecord::decode(&payload[offset..offset + LocationRecord::ENCODED_LEN])?;
        offset += LocationRecord::ENCODED_LEN;
        out.insert(chunk_id, record);
    }
    Some(out)
}

fn decode_delta_batch(payload: &[u8]) -> Option<Vec<DeltaEntry>> {
    if payload.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes(payload[0..4].try_into().ok()?) as usize;
    let expected_len = 4 + count * DELTA_ENTRY_LEN;
    if payload.len() != expected_len {
        return None;
    }

    let mut deltas = Vec::with_capacity(count);
    let mut offset = 4;
    for _ in 0..count {
        let delta = DeltaEntry::decode(&payload[offset..offset + DELTA_ENTRY_LEN])?;
        offset += DELTA_ENTRY_LEN;
        deltas.push(delta);
    }
    Some(deltas)
}

fn apply_delta(entries: &mut HashMap<ChunkId, LocationRecord>, delta: DeltaEntry) {
    match delta.op {
        DeltaOp::Upsert => {
            if let Some(record) = delta.record {
                match entries.get(&delta.chunk_id).copied() {
                    Some(existing) if existing.generation > record.generation => {}
                    _ => {
                        entries.insert(delta.chunk_id, record);
                    }
                }
            }
        }
        DeltaOp::Delete => {
            entries.remove(&delta.chunk_id);
        }
    }
}

#[cfg(test)]
mod tests;
