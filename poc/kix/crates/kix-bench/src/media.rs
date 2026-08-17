// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::{BenchConfig, MediaFlushMode, ReadPathMode};
use crate::workload::{chunk_media_layout, planned_media_span_bytes};
use kix::{
    chunk_media_checksum, crc32_ieee, ensure_chunk_media_superblock, fill_chunk_media_slot_bytes,
    ChunkId, ChunkMediaSpanConfig, ChunkMediaWriteConfig, LocationRecord,
    CHUNK_MEDIA_SLOT_HEADER_BYTES,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const MEDIA_IO_ALIGN: usize = 4096;

thread_local! {
    static DIRECT_MEDIA_CONTEXT: RefCell<Option<DirectMediaContextBinding>> = const { RefCell::new(None) };
}

pub(crate) struct MediaStore {
    description: String,
    drives: HashMap<u16, Arc<MediaDrive>>,
}

struct MediaDrive {
    path: PathBuf,
    span: ChunkMediaSpanConfig,
    file: File,
    direct_plan: DirectMediaPlan,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DirectMediaPlan {
    queue_depth: usize,
    read_batch_size: usize,
    write_batch_size: usize,
    flush_mode: MediaFlushMode,
    buffer_bytes: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct MediaReadRequest {
    pub(crate) request_index: usize,
    pub(crate) record: LocationRecord,
}

#[derive(Clone, Copy)]
pub(crate) struct MediaWriteRequest {
    pub(crate) request_index: usize,
    pub(crate) chunk_id: ChunkId,
    pub(crate) record: LocationRecord,
}

impl MediaStore {
    pub(crate) fn root_dir(&self) -> &str {
        &self.description
    }

    pub(crate) fn io_mode_name(&self) -> &'static str {
        "direct-uring"
    }

    pub(crate) fn materialize_batch(
        &self,
        requests: &[MediaWriteRequest],
    ) -> io::Result<Vec<(usize, u64)>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let drive = self.drive(requests[0].record.drive_id)?;
        if requests
            .iter()
            .any(|request| request.record.drive_id != requests[0].record.drive_id)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "batched media writes must target one drive at a time",
            ));
        }

        for request in requests {
            let absolute_offset = absolute_media_offset(drive, request.record)?;
            validate_direct_media_layout(request.record, absolute_offset)?;
        }
        with_direct_media_context(&drive.file, drive.direct_plan, |ctx| {
            ctx.write_batch(drive, requests)
        })
    }

    pub(crate) fn read_and_validate_batch(
        &self,
        requests: &[MediaReadRequest],
    ) -> io::Result<Vec<(usize, u64)>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let drive = self.drive(requests[0].record.drive_id)?;
        if requests
            .iter()
            .any(|request| request.record.drive_id != requests[0].record.drive_id)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "batched media reads must target one drive at a time",
            ));
        }

        for request in requests {
            let absolute_offset = absolute_media_offset(drive, request.record)?;
            validate_direct_media_layout(request.record, absolute_offset)?;
        }
        with_direct_media_context(&drive.file, drive.direct_plan, |ctx| {
            ctx.read_batch(drive, requests)
        })
    }

    fn drive(&self, drive_id: u16) -> io::Result<&Arc<MediaDrive>> {
        self.drives.get(&drive_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("media backing for drive {} is not configured", drive_id),
            )
        })
    }
}

pub(crate) fn build_media_store(
    config: &BenchConfig,
) -> Result<Option<Arc<MediaStore>>, Box<dyn Error>> {
    if config.read_path != ReadPathMode::MediaRead {
        return Ok(None);
    }

    let raw_device = config
        .media_raw_device
        .as_ref()
        .ok_or("KIX media-read benchmarking requires --media-raw-device <block-device>")?;
    let mut drives = HashMap::new();
    let required_span_bytes = planned_media_span_bytes(config);
    let layout = chunk_media_layout(config);
    let direct_plan = DirectMediaPlan {
        queue_depth: config.media_queue_depth.max(1),
        read_batch_size: config.media_read_batch_size.max(1),
        write_batch_size: config.media_write_batch_size.max(1),
        flush_mode: config.media_flush_mode,
        buffer_bytes: align_up_usize(
            (config.extent_bytes.max(config.packed_bytes)) as usize
                + kix::CHUNK_MEDIA_SLOT_HEADER_BYTES as usize,
            MEDIA_IO_ALIGN,
        )?,
    };
    let span = ChunkMediaSpanConfig {
        media_path: raw_device.clone(),
        media_offset_bytes: config.media_raw_offset_bytes,
        media_len_bytes: config.media_raw_slice_bytes,
    };
    let write_config = ChunkMediaWriteConfig {
        drive_id: 0,
        span: span.clone(),
        layout,
    };
    let superblock = ensure_chunk_media_superblock(&write_config)?;
    let slice_bytes = superblock.media_span_bytes;
    if slice_bytes == 0 {
        return Err("media raw slice bytes must be > 0".into());
    }
    if slice_bytes < required_span_bytes {
        return Err(format!(
            "media raw slice {} B is too small for the planned workload shape. KIX needs at least {} B to cover {} keys with record_mix={}, extent_bytes={}, packed_bytes={}. Increase --media-raw-slice-bytes or reduce --key-space / --prefill-keys.",
            slice_bytes,
            required_span_bytes,
            config.prefill_keys.max(config.key_space),
            config.record_mix.as_str(),
            config.extent_bytes,
            config.packed_bytes,
        )
        .into());
    }

    validate_direct_span_layout(config.media_raw_offset_bytes, slice_bytes)?;

    let file = open_direct_file(raw_device)?;
    preflight_direct_media_runtime(&file, direct_plan)?;
    drives.insert(
        0,
        Arc::new(MediaDrive {
            path: raw_device.clone(),
            span,
            file,
            direct_plan,
        }),
    );

    Ok(Some(Arc::new(MediaStore {
        description: format!(
            "{}@{}+{}",
            raw_device.display(),
            config.media_raw_offset_bytes,
            slice_bytes
        ),
        drives,
    })))
}

fn validate_direct_span_layout(offset_bytes: u64, len_bytes: u64) -> io::Result<()> {
    if offset_bytes % MEDIA_IO_ALIGN as u64 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "direct raw media I/O requires media_raw_offset_bytes to be aligned to {} bytes",
                MEDIA_IO_ALIGN
            ),
        ));
    }
    if len_bytes % MEDIA_IO_ALIGN as u64 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "direct raw media I/O requires media_raw_slice_bytes to be aligned to {} bytes",
                MEDIA_IO_ALIGN
            ),
        ));
    }
    Ok(())
}

fn validate_direct_media_layout(record: LocationRecord, absolute_offset: u64) -> io::Result<()> {
    if absolute_offset % MEDIA_IO_ALIGN as u64 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "direct raw media I/O requires the target offset {} to be aligned to {} bytes",
                absolute_offset, MEDIA_IO_ALIGN
            ),
        ));
    }
    if (record.stored_length as u64) % MEDIA_IO_ALIGN as u64 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "direct raw media I/O requires stored_length {} to be aligned to {} bytes",
                record.stored_length, MEDIA_IO_ALIGN
            ),
        ));
    }
    Ok(())
}

fn preflight_direct_media_runtime(file: &File, plan: DirectMediaPlan) -> io::Result<()> {
    let _ = DirectMediaThreadContext::new(file, plan)?;
    Ok(())
}

fn open_direct_file(path: &Path) -> io::Result<File> {
    let path_cstr = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path {} contains interior NUL bytes", path.display()),
        )
    })?;
    let flags = libc::O_CLOEXEC | libc::O_DIRECT | libc::O_RDWR;
    let fd = unsafe { libc::open(path_cstr.as_ptr(), flags, 0o644) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

pub(crate) fn media_checksum(chunk_id: &ChunkId, record: &LocationRecord) -> u32 {
    chunk_media_checksum(chunk_id, record)
}

fn with_direct_media_context<T>(
    file: &File,
    plan: DirectMediaPlan,
    f: impl FnOnce(&mut DirectMediaThreadContext) -> io::Result<T>,
) -> io::Result<T> {
    DIRECT_MEDIA_CONTEXT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let replace = slot
            .as_ref()
            .map(|binding| binding.file_fd != file.as_raw_fd() || binding.plan != plan)
            .unwrap_or(true);
        if replace {
            *slot = Some(DirectMediaContextBinding::new(file, plan)?);
        }
        f(&mut slot
            .as_mut()
            .expect("direct media context must exist")
            .context)
    })
}

struct DirectMediaContextBinding {
    file_fd: i32,
    plan: DirectMediaPlan,
    context: DirectMediaThreadContext,
}

impl DirectMediaContextBinding {
    fn new(file: &File, plan: DirectMediaPlan) -> io::Result<Self> {
        Ok(Self {
            file_fd: file.as_raw_fd(),
            plan,
            context: DirectMediaThreadContext::new(file, plan)?,
        })
    }
}

struct DirectMediaThreadContext {
    ring: io_uring::IoUring,
    plan: DirectMediaPlan,
    buffers: Vec<AlignedIoBuffer>,
}

impl DirectMediaThreadContext {
    fn new(file: &File, plan: DirectMediaPlan) -> io::Result<Self> {
        let ring_entries = required_ring_entries(plan.queue_depth);
        let mut ring = io_uring::IoUring::new(ring_entries)?;
        let entry = io_uring::opcode::Nop::new().build();
        unsafe {
            ring.submission().push(&entry).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "direct raw media context could not push an io_uring NOP probe",
                )
            })?;
        }
        ring.submit_and_wait(1)?;
        let mut completion = ring.completion();
        let cqe = completion.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "direct raw media context completed without a CQE",
            )
        })?;
        if cqe.result() < 0 {
            return Err(io::Error::from_raw_os_error(-cqe.result()));
        }
        drop(completion);

        let buffers = (0..plan.queue_depth.max(1))
            .map(|_| AlignedIoBuffer::zeroed(plan.buffer_bytes))
            .collect::<io::Result<Vec<_>>>()?;
        let iovecs = buffers
            .iter()
            .map(AlignedIoBuffer::as_iovec)
            .collect::<Vec<_>>();
        unsafe {
            ring.submitter().register_buffers(&iovecs)?;
        }
        ring.submitter().register_files(&[file.as_raw_fd()])?;

        Ok(Self {
            ring,
            plan,
            buffers,
        })
    }

    fn read_batch(
        &mut self,
        drive: &MediaDrive,
        requests: &[MediaReadRequest],
    ) -> io::Result<Vec<(usize, u64)>> {
        let mut results = Vec::with_capacity(requests.len());
        let batch_size = self
            .plan
            .read_batch_size
            .max(1)
            .min(self.plan.queue_depth.max(1));
        for batch in requests.chunks(batch_size) {
            let mut starts = Vec::with_capacity(batch.len());
            let mut expected = Vec::with_capacity(batch.len());
            let mut entries = Vec::with_capacity(batch.len());
            for (slot, request) in batch.iter().enumerate() {
                let offset = absolute_media_offset(drive, request.record)?;
                let len = request.record.stored_length as usize;
                self.ensure_supported_len(len)?;
                starts.push(Instant::now());
                expected.push(len as i32);
                let entry = io_uring::opcode::ReadFixed::new(
                    io_uring::types::Fixed(0),
                    self.buffers[slot].as_mut_ptr(),
                    len as u32,
                    slot as u16,
                )
                .offset(offset)
                .build()
                .user_data(slot as u64);
                entries.push(entry);
            }
            self.submit_entries(&entries, "direct raw media read")?;
            let latencies =
                self.collect_completions(batch.len(), "direct raw media read", &expected, &starts)?;
            for (slot, request) in batch.iter().enumerate() {
                let payload =
                    &self.buffers[slot].as_slice()[..request.record.stored_length as usize];
                let observed_crc = crc32_ieee(payload);
                if observed_crc != request.record.checksum {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "media read on drive {} at {}+{} returned checksum {} but KIX location record expected {}",
                            request.record.drive_id,
                            drive.path.display(),
                            absolute_media_offset(drive, request.record)?,
                            observed_crc,
                            request.record.checksum
                        ),
                    ));
                }
                results.push((request.request_index, latencies[slot]));
            }
        }
        Ok(results)
    }

    fn write_batch(
        &mut self,
        drive: &MediaDrive,
        requests: &[MediaWriteRequest],
    ) -> io::Result<Vec<(usize, u64)>> {
        match self.plan.flush_mode {
            MediaFlushMode::PerOp => self.write_batch_per_op(drive, requests),
            MediaFlushMode::PerBatch => self.write_batch_group_commit(drive, requests),
        }
    }

    fn write_batch_per_op(
        &mut self,
        drive: &MediaDrive,
        requests: &[MediaWriteRequest],
    ) -> io::Result<Vec<(usize, u64)>> {
        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            let start = Instant::now();
            let len = chunk_media_write_len(request.record)?;
            self.ensure_supported_len(len)?;
            let offset = absolute_media_header_offset(drive, request.record)?;
            self.prepare_write_payload(0, request.chunk_id, request.record)?;
            let entry = io_uring::opcode::WriteFixed::new(
                io_uring::types::Fixed(0),
                self.buffers[0].as_slice()[..len].as_ptr(),
                len as u32,
                0,
            )
            .offset(offset)
            .build()
            .user_data(0);
            self.submit_entries(&[entry], "direct raw media write")?;
            self.collect_completions(1, "direct raw media write", &[len as i32], &[start])?;
            self.sync_data()?;
            results.push((request.request_index, start.elapsed().as_micros() as u64));
        }
        Ok(results)
    }

    fn write_batch_group_commit(
        &mut self,
        drive: &MediaDrive,
        requests: &[MediaWriteRequest],
    ) -> io::Result<Vec<(usize, u64)>> {
        let mut results = Vec::with_capacity(requests.len());
        let batch_size = self
            .plan
            .write_batch_size
            .max(1)
            .min(self.plan.queue_depth.max(1));
        for batch in requests.chunks(batch_size) {
            let mut starts = Vec::with_capacity(batch.len());
            let mut expected = Vec::with_capacity(batch.len());
            let mut entries = Vec::with_capacity(batch.len());
            for (slot, request) in batch.iter().enumerate() {
                let len = chunk_media_write_len(request.record)?;
                self.ensure_supported_len(len)?;
                self.prepare_write_payload(slot, request.chunk_id, request.record)?;
                starts.push(Instant::now());
                expected.push(len as i32);
                let entry = io_uring::opcode::WriteFixed::new(
                    io_uring::types::Fixed(0),
                    self.buffers[slot].as_slice()[..len].as_ptr(),
                    len as u32,
                    slot as u16,
                )
                .offset(absolute_media_header_offset(drive, request.record)?)
                .build()
                .user_data(slot as u64);
                entries.push(entry);
            }
            self.submit_entries(&entries, "direct raw media write")?;
            self.collect_completions(batch.len(), "direct raw media write", &expected, &starts)?;
            self.sync_data()?;
            for (slot, request) in batch.iter().enumerate() {
                let latency_us = starts[slot].elapsed().as_micros() as u64;
                results.push((request.request_index, latency_us));
            }
        }
        Ok(results)
    }

    fn prepare_write_payload(
        &mut self,
        slot: usize,
        chunk_id: ChunkId,
        record: LocationRecord,
    ) -> io::Result<()> {
        let len = chunk_media_write_len(record)?;
        self.ensure_supported_len(len)?;
        let buffer = &mut self.buffers[slot].as_mut_slice()[..len];
        fill_chunk_media_slot_bytes(buffer, chunk_id, record)
    }

    fn ensure_supported_len(&self, len: usize) -> io::Result<()> {
        if len > self.plan.buffer_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "direct raw media I/O needs {} bytes but the registered fixed buffers are only {} bytes; increase --extent-bytes/--packed-bytes planning or the direct media buffer budget",
                    len, self.plan.buffer_bytes
                ),
            ));
        }
        Ok(())
    }

    fn submit_entries(
        &mut self,
        entries: &[io_uring::squeue::Entry],
        op_name: &str,
    ) -> io::Result<()> {
        {
            let mut submission = self.ring.submission();
            for entry in entries {
                unsafe {
                    submission.push(entry).map_err(|_| {
                        io::Error::other(format!(
                            "{op_name} submission queue is full; lower --media-queue-depth or increase the direct ring size"
                        ))
                    })?;
                }
            }
        }
        self.ring.submitter().submit_and_wait(entries.len())?;
        Ok(())
    }

    fn collect_completions(
        &mut self,
        want: usize,
        op_name: &str,
        expected_lengths: &[i32],
        starts: &[Instant],
    ) -> io::Result<Vec<u64>> {
        let mut completions = 0_usize;
        let mut latencies = vec![0_u64; want];
        while completions < want {
            let mut completion = self.ring.completion();
            let mut progressed = false;
            while let Some(cqe) = completion.next() {
                progressed = true;
                let slot = cqe.user_data() as usize;
                if slot >= want {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{op_name} returned an out-of-range completion slot {slot}"),
                    ));
                }
                let result = cqe.result();
                if result < 0 {
                    return Err(io::Error::from_raw_os_error(-result));
                }
                if result != expected_lengths[slot] {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "{op_name} completed only {result} bytes for slot {slot}; expected {}",
                            expected_lengths[slot]
                        ),
                    ));
                }
                latencies[slot] = starts[slot].elapsed().as_micros() as u64;
                completions += 1;
            }
            drop(completion);
            if completions < want && !progressed {
                self.ring.submitter().submit_and_wait(want - completions)?;
            }
        }
        Ok(latencies)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        let entry = io_uring::opcode::Fsync::new(io_uring::types::Fixed(0))
            .flags(io_uring::types::FsyncFlags::DATASYNC)
            .build()
            .user_data(u64::MAX);
        self.submit_entries(&[entry], "direct raw media fsync")?;
        self.collect_fsync_completion("direct raw media fsync")
    }

    fn collect_fsync_completion(&mut self, _op_name: &str) -> io::Result<()> {
        loop {
            let mut completion = self.ring.completion();
            if let Some(cqe) = completion.next() {
                let result = cqe.result();
                if result < 0 {
                    return Err(io::Error::from_raw_os_error(-result));
                }
                return Ok(());
            }
            drop(completion);
            self.ring.submitter().submit_and_wait(1)?;
        }
    }
}

fn absolute_media_offset(drive: &MediaDrive, record: LocationRecord) -> io::Result<u64> {
    drive
        .span
        .media_offset_bytes
        .checked_add(record.physical_offset)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "media offset overflow"))
}

fn absolute_media_header_offset(drive: &MediaDrive, record: LocationRecord) -> io::Result<u64> {
    let header_offset = record
        .physical_offset
        .checked_sub(CHUNK_MEDIA_SLOT_HEADER_BYTES)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "chunk-media header underflow")
        })?;
    drive
        .span
        .media_offset_bytes
        .checked_add(header_offset)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "media header offset overflow"))
}

fn chunk_media_write_len(record: LocationRecord) -> io::Result<usize> {
    (CHUNK_MEDIA_SLOT_HEADER_BYTES as usize)
        .checked_add(record.stored_length as usize)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "chunk-media write length overflow",
            )
        })
}

fn required_ring_entries(queue_depth: usize) -> u32 {
    (queue_depth.max(2) + 2).next_power_of_two() as u32
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
                "aligned media I/O buffer length must be > 0",
            ));
        }
        let layout = std::alloc::Layout::from_size_align(len, MEDIA_IO_ALIGN).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid aligned media I/O layout",
            )
        })?;
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "failed to allocate aligned media I/O buffer",
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

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    fn as_iovec(&self) -> libc::iovec {
        libc::iovec {
            iov_base: self.ptr.cast(),
            iov_len: self.len,
        }
    }
}

impl Drop for AlignedIoBuffer {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(self.len, MEDIA_IO_ALIGN).unwrap();
        unsafe {
            std::alloc::dealloc(self.ptr, layout);
        }
    }
}

fn align_up_usize(value: usize, align: usize) -> io::Result<usize> {
    if align == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "aligned media I/O requires a non-zero alignment",
        ));
    }
    let remainder = value % align;
    if remainder == 0 {
        Ok(value)
    } else {
        value.checked_add(align - remainder).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "aligned media I/O length overflowed",
            )
        })
    }
}
