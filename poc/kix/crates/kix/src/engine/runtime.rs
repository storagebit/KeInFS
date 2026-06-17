// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use super::config::{KixError, WorkerMode};
use super::stats::{DriveRuntimeStats, QueueRuntimeStats, ShardRuntimeStats};
use crate::arena::{DeltaEntry, DriveArena, DriveConfig, DriveRecovery};
use crate::types::{ChunkId, LocationRecord};
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

const DRIVE_APPEND_BATCH_LIMIT: usize = 128;
const DRIVE_APPEND_COALESCE_SPINS: usize = 256;

#[derive(Clone)]
pub(super) struct ShardQueueSet {
    pub(super) lookup: Arc<WorkQueue<LookupRequest>>,
    pub(super) commit: Arc<WorkQueue<CommitRequest>>,
}

pub(super) struct WorkQueue<T> {
    queue: ArrayQueue<T>,
    wait_lock: Mutex<()>,
    wait_cv: Condvar,
    alive: AtomicBool,
    stats: Arc<QueueRuntimeStats>,
}

impl<T> WorkQueue<T> {
    pub(super) fn with_capacity(capacity: usize, stats: Arc<QueueRuntimeStats>) -> Self {
        Self {
            queue: ArrayQueue::new(capacity.max(1)),
            wait_lock: Mutex::new(()),
            wait_cv: Condvar::new(),
            alive: AtomicBool::new(true),
            stats,
        }
    }

    pub(super) fn send(&self, mut item: T) -> Result<(), KixError> {
        loop {
            if !self.alive.load(Ordering::Acquire) {
                return Err(KixError::ChannelClosed);
            }
            match self.queue.push(item) {
                Ok(()) => {
                    self.stats.enqueued.fetch_add(1, Ordering::Relaxed);
                    self.wait_cv.notify_one();
                    return Ok(());
                }
                Err(rejected) => {
                    item = rejected;
                    self.stats.enqueue_retries.fetch_add(1, Ordering::Relaxed);
                    thread::yield_now();
                }
            }
        }
    }

    pub(super) fn try_recv(&self) -> Option<T> {
        let item = self.queue.pop();
        if item.is_some() {
            self.stats.dequeued.fetch_add(1, Ordering::Relaxed);
        }
        item
    }

    pub(super) fn recv_interrupt(&self) -> Option<T> {
        loop {
            if let Some(item) = self.queue.pop() {
                self.stats.dequeued.fetch_add(1, Ordering::Relaxed);
                return Some(item);
            }
            if !self.alive.load(Ordering::Acquire) {
                return None;
            }
            let mut guard = match self.wait_lock.lock() {
                Ok(guard) => guard,
                Err(_) => return None,
            };
            while self.queue.is_empty() && self.alive.load(Ordering::Acquire) {
                guard = match self.wait_cv.wait(guard) {
                    Ok(guard) => guard,
                    Err(_) => return None,
                };
            }
            drop(guard);
        }
    }

    pub(super) fn close(&self) {
        self.alive.store(false, Ordering::Release);
        self.wait_cv.notify_all();
    }

    pub(super) fn depth(&self) -> usize {
        self.queue.len()
    }
}

pub(super) struct ShardHandle {
    pub(super) lookup_queue: Arc<WorkQueue<LookupRequest>>,
    pub(super) commit_queue: Arc<WorkQueue<CommitRequest>>,
    pub(super) lookup_join: Option<JoinHandle<()>>,
    pub(super) commit_join: Option<JoinHandle<()>>,
}

pub(super) struct ShardState {
    live: DashMap<ChunkId, LocationRecord>,
}

impl ShardState {
    pub(super) fn new(initial_entries: Vec<(ChunkId, LocationRecord)>) -> Self {
        let live = DashMap::with_capacity(initial_entries.len().max(16));
        for (chunk_id, record) in initial_entries {
            live.insert(chunk_id, record);
        }
        Self { live }
    }

    pub(super) fn len(&self) -> usize {
        self.live.len()
    }

    pub(super) fn get(&self, chunk_id: &ChunkId) -> Option<LocationRecord> {
        self.live.get(chunk_id).map(|entry| *entry)
    }

    pub(super) fn upsert(&self, chunk_id: ChunkId, record: LocationRecord) -> usize {
        match self.live.entry(chunk_id) {
            dashmap::mapref::entry::Entry::Occupied(mut existing) => {
                if existing.get().generation <= record.generation {
                    existing.insert(record);
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(record);
            }
        }
        self.live.len()
    }

    pub(super) fn drive_id_for(&self, chunk_id: &ChunkId) -> Option<u16> {
        self.live.get(chunk_id).map(|entry| entry.drive_id)
    }

    pub(super) fn delete(&self, chunk_id: &ChunkId) -> usize {
        self.live.remove(chunk_id);
        self.live.len()
    }

    pub(super) fn snapshot_entries(&self) -> Vec<(ChunkId, LocationRecord)> {
        self.live
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect()
    }
}

pub(super) enum LookupRequest {
    Stop,
}

pub(super) enum CommitRequest {
    Stop,
}

pub(super) struct DriveArenaSet {
    pub(super) queues: Arc<HashMap<u16, Arc<WorkQueue<DriveRequest>>>>,
    joins: Mutex<Vec<JoinHandle<()>>>,
}

impl DriveArenaSet {
    pub(super) fn open(
        configs: &[DriveConfig],
        worker_mode: WorkerMode,
        drive_pin_cores: &[usize],
        queue_depth: usize,
        drive_stats: &[Arc<DriveRuntimeStats>],
    ) -> Result<(Self, Vec<DriveRecovery>), KixError> {
        let mut queues = HashMap::new();
        let mut joins = Vec::new();
        let mut recoveries = Vec::new();
        for (drive_index, cfg) in configs.iter().enumerate() {
            if let Some(parent) = cfg.arena_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let recovery = DriveArena::recover(cfg)?;
            let open_plan = DriveOpenPlan {
                reset_before_open: recovery.rebuild_required,
                truncate_to_replay_len: recovery.tail_corruption.then_some(recovery.replay_len),
            };
            let (startup_tx, startup_rx) = mpsc::sync_channel(0);
            let cfg = cfg.clone();
            let drive_id = cfg.id;
            let pin_core = drive_pin_cores.get(drive_index).copied();
            let drive_stats = Arc::clone(&drive_stats[drive_index]);
            let join = thread::Builder::new()
                .name(format!("kix-drive-{drive_id}"))
                .spawn(move || {
                    let startup = configure_worker_startup(
                        &format!("KIX drive appender {drive_id}"),
                        pin_core,
                        cfg.numa_node,
                        queue_depth,
                        Arc::clone(&drive_stats.queue),
                    );
                    let queue = match startup {
                        Ok(queue) => {
                            if startup_tx.send(Ok(Arc::clone(&queue))).is_err() {
                                return;
                            }
                            queue
                        }
                        Err(err) => {
                            let _ = startup_tx.send(Err(err));
                            return;
                        }
                    };

                    let writer = match open_drive_writer(&cfg, open_plan) {
                        Ok(writer) => writer,
                        Err(err) => {
                            drive_stats.record_error(format!(
                                "KIX drive appender {drive_id} failed to open arena {}: {err}",
                                cfg.arena_path.display()
                            ));
                            queue.close();
                            return;
                        }
                    };
                    drive_main(writer, queue, worker_mode, drive_stats);
                })
                .map_err(KixError::Io)?;
            let queue = startup_rx.recv().map_err(|_| KixError::ChannelClosed)??;
            queues.insert(drive_id, queue);
            joins.push(join);
            recoveries.push(recovery);
        }
        Ok((
            Self {
                queues: Arc::new(queues),
                joins: Mutex::new(joins),
            },
            recoveries,
        ))
    }

    pub(super) fn append_delta(&self, drive_id: u16, delta: DeltaEntry) -> Result<(), KixError> {
        let queue = self.queues.get(&drive_id).ok_or_else(|| {
            KixError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "KIX drive arena {drive_id} is not configured; verify drive ids and raw-slice layout"
                ),
            ))
        })?;
        let (resp_tx, resp_rx) = mpsc::sync_channel(0);
        queue.send(DriveRequest::Append {
            delta,
            resp: resp_tx,
        })?;
        match resp_rx.recv().map_err(|_| KixError::ChannelClosed)? {
            Ok(()) => Ok(()),
            Err(message) => Err(KixError::Io(io::Error::new(io::ErrorKind::Other, message))),
        }
    }

    pub(super) fn write_checkpoint(
        &self,
        drive_id: u16,
        entries: Vec<(ChunkId, LocationRecord)>,
    ) -> Result<(), KixError> {
        let queue = self.queues.get(&drive_id).ok_or_else(|| {
            KixError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "KIX drive arena {drive_id} is not configured; verify drive ids and raw-slice layout"
                ),
            ))
        })?;
        let (resp_tx, resp_rx) = mpsc::sync_channel(0);
        queue.send(DriveRequest::Checkpoint {
            entries,
            resp: resp_tx,
        })?;
        match resp_rx.recv().map_err(|_| KixError::ChannelClosed)? {
            Ok(()) => Ok(()),
            Err(message) => Err(KixError::Io(io::Error::new(io::ErrorKind::Other, message))),
        }
    }
}

impl Drop for DriveArenaSet {
    fn drop(&mut self) {
        for queue in self.queues.values() {
            let _ = queue.send(DriveRequest::Stop);
        }
        if let Ok(mut joins) = self.joins.lock() {
            for join in joins.drain(..) {
                let _ = join.join();
            }
        }
    }
}

pub(super) enum DriveRequest {
    Append {
        delta: DeltaEntry,
        resp: SyncSender<Result<(), String>>,
    },
    Checkpoint {
        entries: Vec<(ChunkId, LocationRecord)>,
        resp: SyncSender<Result<(), String>>,
    },
    Stop,
}

#[derive(Clone, Copy)]
struct DriveOpenPlan {
    reset_before_open: bool,
    truncate_to_replay_len: Option<u64>,
}

fn open_drive_writer(config: &DriveConfig, plan: DriveOpenPlan) -> io::Result<DriveArena> {
    let mut writer = if plan.reset_before_open {
        DriveArena::reset_config(config)?
    } else {
        DriveArena::open_config(config)?
    };
    if let Some(replay_len) = plan.truncate_to_replay_len {
        writer.truncate_to(replay_len)?;
    }
    Ok(writer)
}

fn drive_main(
    mut arena: DriveArena,
    queue: Arc<WorkQueue<DriveRequest>>,
    mode: WorkerMode,
    stats: Arc<DriveRuntimeStats>,
) {
    let mut deferred = None;
    let mut deltas = Vec::with_capacity(DRIVE_APPEND_BATCH_LIMIT);
    let mut resps = Vec::with_capacity(DRIVE_APPEND_BATCH_LIMIT);

    loop {
        let request = match deferred.take() {
            Some(request) => request,
            None => match recv_with_mode(&queue, mode) {
                Some(request) => request,
                None => break,
            },
        };

        match request {
            DriveRequest::Append { delta, resp } => {
                deltas.clear();
                resps.clear();
                deltas.push(delta);
                resps.push(resp);

                while deltas.len() < DRIVE_APPEND_BATCH_LIMIT {
                    match queue.try_recv() {
                        Some(DriveRequest::Append { delta, resp }) => {
                            deltas.push(delta);
                            resps.push(resp);
                        }
                        Some(other) => {
                            deferred = Some(other);
                            break;
                        }
                        None => {
                            let mut spins = 0_usize;
                            let mut found_more = false;
                            while spins < DRIVE_APPEND_COALESCE_SPINS
                                && deltas.len() < DRIVE_APPEND_BATCH_LIMIT
                            {
                                match queue.try_recv() {
                                    Some(DriveRequest::Append { delta, resp }) => {
                                        deltas.push(delta);
                                        resps.push(resp);
                                        found_more = true;
                                        break;
                                    }
                                    Some(other) => {
                                        deferred = Some(other);
                                        found_more = true;
                                        break;
                                    }
                                    None => {
                                        spins += 1;
                                        std::hint::spin_loop();
                                    }
                                }
                            }
                            if !found_more {
                                break;
                            }
                        }
                    }
                }

                stats.append_batches.fetch_add(1, Ordering::Relaxed);
                stats
                    .appended_deltas
                    .fetch_add(deltas.len() as u64, Ordering::Relaxed);
                let t0 = Instant::now();
                let reply = match arena.append_delta_batch(deltas.iter().copied()) {
                    Ok(()) => Ok(()),
                    Err(err) => {
                        let message = format!(
                            "KIX drive {} delta append failed at {}: {err}",
                            stats.drive_id,
                            stats.arena_path.display()
                        );
                        stats.record_error(message.clone());
                        Err(message)
                    }
                };
                stats.append_latency.observe(t0.elapsed());
                for resp in resps.drain(..) {
                    let _ = resp.send(reply.clone());
                }
            }
            DriveRequest::Checkpoint { entries, resp } => {
                stats.checkpoint_ops.fetch_add(1, Ordering::Relaxed);
                stats
                    .checkpoint_entries
                    .fetch_add(entries.len() as u64, Ordering::Relaxed);
                let t0 = Instant::now();
                let reply = match arena.write_checkpoint(entries) {
                    Ok(()) => Ok(()),
                    Err(err) => {
                        let message = format!(
                            "KIX drive {} checkpoint write failed at {}: {err}",
                            stats.drive_id,
                            stats.arena_path.display()
                        );
                        stats.record_error(message.clone());
                        Err(message)
                    }
                };
                stats.checkpoint_latency.observe(t0.elapsed());
                let _ = resp.send(reply);
            }
            DriveRequest::Stop => {
                queue.close();
                break;
            }
        }
    }
    queue.close();
}

pub(super) fn lookup_main(
    _shard_id: usize,
    queue: Arc<WorkQueue<LookupRequest>>,
    _shard_state: Arc<ShardState>,
    mode: WorkerMode,
    _stats: Arc<ShardRuntimeStats>,
) {
    while let Some(request) = recv_with_mode(&queue, mode) {
        match request {
            LookupRequest::Stop => break,
        }
    }
    queue.close();
}

pub(super) fn commit_main(
    _shard_id: usize,
    queue: Arc<WorkQueue<CommitRequest>>,
    _drive_arenas: Arc<DriveArenaSet>,
    _shard_state: Arc<ShardState>,
    mode: WorkerMode,
    _stats: Arc<ShardRuntimeStats>,
) {
    while let Some(request) = recv_with_mode(&queue, mode) {
        match request {
            CommitRequest::Stop => break,
        }
    }
    queue.close();
}

pub(super) fn configure_worker_startup<T>(
    _role: &str,
    pin_core: Option<usize>,
    numa_node: Option<i32>,
    queue_depth: usize,
    queue_stats: Arc<QueueRuntimeStats>,
) -> Result<Arc<WorkQueue<T>>, KixError> {
    maybe_pin_to_core(pin_core)?;
    set_current_thread_memory_policy(numa_node)?;
    Ok(Arc::new(WorkQueue::with_capacity(
        queue_depth.max(1),
        queue_stats,
    )))
}

fn recv_with_mode<T>(queue: &WorkQueue<T>, mode: WorkerMode) -> Option<T> {
    match mode {
        WorkerMode::Interrupt => queue.recv_interrupt(),
        WorkerMode::BusyPoll { spins_before_yield } => {
            let spins_before_yield = spins_before_yield.max(1);
            let mut spins = 0_usize;
            loop {
                if let Some(item) = queue.try_recv() {
                    return Some(item);
                }
                if !queue.alive.load(Ordering::Acquire) {
                    return None;
                }
                spins += 1;
                std::hint::spin_loop();
                if spins >= spins_before_yield {
                    spins = 0;
                    thread::yield_now();
                }
            }
        }
    }
}

fn maybe_pin_to_core(core_id: Option<usize>) -> Result<(), KixError> {
    let Some(core_id) = core_id else {
        return Ok(());
    };
    let Some(cores) = core_affinity::get_core_ids() else {
        return Ok(());
    };
    if let Some(core) = cores.into_iter().find(|core| core.id == core_id) {
        if core_affinity::set_for_current(core) {
            return Ok(());
        }
    }
    Err(KixError::Io(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("requested CPU core {core_id} is unavailable for KIX worker placement"),
    )))
}

fn set_current_thread_memory_policy(numa_node: Option<i32>) -> Result<(), KixError> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = numa_node;
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        let Some(numa_node) = numa_node else {
            return Ok(());
        };
        if numa_node < 0 {
            return Ok(());
        }
        let maxnode = (numa_node as usize) + 1;
        let bits_per_word = usize::BITS as usize;
        let mut nodemask = vec![0_usize; maxnode.div_ceil(bits_per_word)];
        nodemask[numa_node as usize / bits_per_word] |=
            1_usize << (numa_node as usize % bits_per_word);

        const MPOL_PREFERRED: libc::c_int = 1;
        let rc = unsafe {
            libc::syscall(
                libc::SYS_set_mempolicy,
                MPOL_PREFERRED,
                nodemask.as_ptr(),
                maxnode,
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(KixError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "failed to set KIX thread memory policy to NUMA node {numa_node}: {}",
                    io::Error::last_os_error()
                ),
            )))
        }
    }
}

pub(super) fn shard_for(chunk_id: &ChunkId, shard_count: usize) -> usize {
    let mut head = [0_u8; 8];
    head.copy_from_slice(&chunk_id.0[..8]);
    (u64::from_le_bytes(head) as usize) % shard_count
}
