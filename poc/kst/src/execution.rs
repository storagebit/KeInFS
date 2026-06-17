// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::service::{ServiceError, ServiceResponse, TargetRouter};
use crate::stats::{RequestPhase, RpcKind};
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};
use kix::{ChunkId, WorkerMode};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirectExecutionKind {
    Read,
    Write,
}

impl DirectExecutionKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

#[derive(Debug)]
pub(crate) struct DirectExecutionConfig {
    pub(crate) kind: DirectExecutionKind,
    pub(crate) mode: WorkerMode,
    pub(crate) worker_count: usize,
    pub(crate) queue_depth: usize,
    pub(crate) pin_cores: Vec<usize>,
}

pub(crate) struct DirectExecutionHandle {
    kind: DirectExecutionKind,
    sender: Sender<DirectExecutionTask>,
    queue_depth: usize,
}

impl DirectExecutionHandle {
    pub(crate) fn submit(
        &self,
        rpc: RpcKind,
        request: DirectExecutionRequest,
    ) -> Result<oneshot::Receiver<Result<ServiceResponse, ServiceError>>, DirectExecutionSubmitError>
    {
        let (tx, rx) = oneshot::channel();
        let task = DirectExecutionTask {
            rpc,
            enqueued_at: Instant::now(),
            request,
            completion: tx,
        };
        match self.sender.try_send(task) {
            Ok(()) => Ok(rx),
            Err(TrySendError::Full(_)) => Err(DirectExecutionSubmitError::QueueFull {
                kind: self.kind,
                queue_depth: self.queue_depth,
            }),
            Err(TrySendError::Disconnected(_)) => {
                Err(DirectExecutionSubmitError::WorkerGone { kind: self.kind })
            }
        }
    }
}

pub(crate) enum DirectExecutionRequest {
    Read {
        chunk_id: ChunkId,
    },
    Write {
        chunk_id: ChunkId,
        slot_index: u64,
        generation: u32,
        body: Vec<u8>,
    },
}

#[derive(Debug)]
pub(crate) enum DirectExecutionSubmitError {
    QueueFull {
        kind: DirectExecutionKind,
        queue_depth: usize,
    },
    WorkerGone {
        kind: DirectExecutionKind,
    },
}

pub(crate) fn spawn_direct_execution_workers(
    router: Arc<TargetRouter>,
    config: DirectExecutionConfig,
) -> DirectExecutionHandle {
    let (sender, receiver) = bounded::<DirectExecutionTask>(config.queue_depth);
    for worker_index in 0..config.worker_count {
        let worker_router = Arc::clone(&router);
        let worker_receiver = receiver.clone();
        let worker_mode = config.mode;
        let worker_kind = config.kind;
        let pin_core = config.pin_cores.get(worker_index).copied();
        let thread_name = format!("kst-direct-{}-{}", worker_kind.as_str(), worker_index);
        thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                pin_current_thread(pin_core);
                direct_execution_worker_loop(worker_router, worker_receiver, worker_mode);
            })
            .expect("KST direct execution worker thread spawn must succeed");
    }
    DirectExecutionHandle {
        kind: config.kind,
        sender,
        queue_depth: config.queue_depth,
    }
}

fn direct_execution_worker_loop(
    router: Arc<TargetRouter>,
    receiver: Receiver<DirectExecutionTask>,
    mode: WorkerMode,
) {
    while let Some(task) = next_task(&receiver, mode) {
        router.stats.record_phase(
            task.rpc,
            RequestPhase::ExecutionQueueWait,
            task.enqueued_at.elapsed(),
        );
        let result = match task.request {
            DirectExecutionRequest::Read { chunk_id } => {
                let started = Instant::now();
                let result = router.handle_direct_chunk_read(chunk_id);
                router
                    .stats
                    .record_phase(task.rpc, RequestPhase::RouteExecute, started.elapsed());
                result
            }
            DirectExecutionRequest::Write {
                chunk_id,
                slot_index,
                generation,
                body,
            } => {
                let started = Instant::now();
                let result =
                    router.handle_direct_chunk_write(chunk_id, slot_index, generation, body);
                router
                    .stats
                    .record_phase(task.rpc, RequestPhase::RouteExecute, started.elapsed());
                result
            }
        };
        let _ = task.completion.send(result);
    }
}

fn next_task(
    receiver: &Receiver<DirectExecutionTask>,
    mode: WorkerMode,
) -> Option<DirectExecutionTask> {
    match mode {
        WorkerMode::Interrupt => receiver.recv().ok(),
        WorkerMode::BusyPoll { spins_before_yield } => {
            let mut idle_spins = 0_usize;
            loop {
                match receiver.try_recv() {
                    Ok(task) => return Some(task),
                    Err(TryRecvError::Empty) => {
                        idle_spins += 1;
                        if idle_spins >= spins_before_yield.max(1) {
                            idle_spins = 0;
                            thread::yield_now();
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                    Err(TryRecvError::Disconnected) => return None,
                }
            }
        }
    }
}

fn pin_current_thread(core_id: Option<usize>) {
    let Some(core_id) = core_id else {
        return;
    };
    if let Some(core_ids) = core_affinity::get_core_ids() {
        if let Some(core) = core_ids.into_iter().find(|core| core.id == core_id) {
            core_affinity::set_for_current(core);
        }
    }
}

struct DirectExecutionTask {
    rpc: RpcKind,
    enqueued_at: Instant,
    request: DirectExecutionRequest,
    completion: oneshot::Sender<Result<ServiceResponse, ServiceError>>,
}
