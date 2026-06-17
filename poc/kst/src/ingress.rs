// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::service::{ServiceError, ServiceResponse, TargetRouter};
use crate::stats::{RequestPhase, RpcKind};
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};
use http::{HeaderMap, Method, Uri};
use kix::WorkerMode;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IngressKind {
    Read,
    Write,
}

impl IngressKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

#[derive(Debug)]
pub(crate) struct IngressConfig {
    pub(crate) kind: IngressKind,
    pub(crate) mode: WorkerMode,
    pub(crate) worker_count: usize,
    pub(crate) queue_depth: usize,
    pub(crate) pin_cores: Vec<usize>,
}

pub(crate) struct IngressHandle {
    kind: IngressKind,
    sender: Sender<IngressTask>,
    queue_depth: usize,
}

impl IngressHandle {
    pub(crate) fn submit(
        &self,
        rpc: RpcKind,
        request: IngressRequest,
    ) -> Result<oneshot::Receiver<Result<ServiceResponse, ServiceError>>, IngressSubmitError> {
        let (tx, rx) = oneshot::channel();
        let task = IngressTask {
            rpc,
            enqueued_at: Instant::now(),
            request,
            completion: tx,
        };
        match self.sender.try_send(task) {
            Ok(()) => Ok(rx),
            Err(TrySendError::Full(_)) => Err(IngressSubmitError::QueueFull {
                kind: self.kind,
                queue_depth: self.queue_depth,
            }),
            Err(TrySendError::Disconnected(_)) => {
                Err(IngressSubmitError::WorkerGone { kind: self.kind })
            }
        }
    }
}

pub(crate) enum IngressRequest {
    Buffered {
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Vec<u8>,
    },
}

#[derive(Debug)]
pub(crate) enum IngressSubmitError {
    QueueFull {
        kind: IngressKind,
        queue_depth: usize,
    },
    WorkerGone {
        kind: IngressKind,
    },
}

pub(crate) fn spawn_ingress_workers(
    router: Arc<TargetRouter>,
    config: IngressConfig,
) -> IngressHandle {
    let (sender, receiver) = bounded::<IngressTask>(config.queue_depth);
    for worker_index in 0..config.worker_count {
        let worker_router = Arc::clone(&router);
        let worker_receiver = receiver.clone();
        let worker_mode = config.mode;
        let worker_kind = config.kind;
        let pin_core = config.pin_cores.get(worker_index).copied();
        let thread_name = format!("kst-{}-ingress-{}", worker_kind.as_str(), worker_index);
        thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                pin_current_thread(pin_core);
                ingress_worker_loop(worker_router, worker_receiver, worker_mode);
            })
            .expect("KST ingress worker thread spawn must succeed");
    }
    IngressHandle {
        kind: config.kind,
        sender,
        queue_depth: config.queue_depth,
    }
}

fn ingress_worker_loop(
    router: Arc<TargetRouter>,
    receiver: Receiver<IngressTask>,
    mode: WorkerMode,
) {
    while let Some(task) = next_task(&receiver, mode) {
        router.stats.record_phase(
            task.rpc,
            RequestPhase::IngressQueueWait,
            task.enqueued_at.elapsed(),
        );
        let result = match task.request {
            IngressRequest::Buffered {
                method,
                uri,
                headers,
                body,
            } => {
                let started = Instant::now();
                let result = router.route_buffered(method, uri, headers, body);
                router
                    .stats
                    .record_phase(task.rpc, RequestPhase::RouteExecute, started.elapsed());
                result
            }
        };
        let _ = task.completion.send(result);
    }
}

fn next_task(receiver: &Receiver<IngressTask>, mode: WorkerMode) -> Option<IngressTask> {
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

struct IngressTask {
    rpc: RpcKind,
    enqueued_at: Instant,
    request: IngressRequest,
    completion: oneshot::Sender<Result<ServiceResponse, ServiceError>>,
}
