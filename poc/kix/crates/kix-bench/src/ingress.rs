// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::{BenchConfig, IngressPlacement};
use crate::media::MediaStore;
use crate::topology::TopologyPlan;
use crate::workload::{execute_request_batch, BenchRequest, OperationMetrics};
use kix::KixClient;
use std::io;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

pub(crate) struct IngressRuntime {
    entry_tx: SyncSender<IngressCommand>,
    stop_txs: Vec<SyncSender<IngressCommand>>,
    joins: Vec<JoinHandle<()>>,
}

impl IngressRuntime {
    pub(crate) fn open(
        config: &BenchConfig,
        topology: &TopologyPlan,
        client: Arc<KixClient>,
        media_store: Option<Arc<MediaStore>>,
    ) -> Result<Option<Self>, Box<dyn std::error::Error>> {
        match config.ingress_placement {
            IngressPlacement::Direct => Ok(None),
            IngressPlacement::Local => {
                let local_core = require_core(
                    topology.local_ingress_core,
                    "local ingress",
                    topology.raw_device_numa_node,
                )?;
                let (local_tx, local_rx) = mpsc::sync_channel(config.ingress_queue_depth.max(1));
                let local_join = spawn_ingress_worker(
                    "kix-ingress-local",
                    local_rx,
                    client,
                    media_store,
                    local_core,
                    topology.raw_device_numa_node,
                    None,
                )?;
                Ok(Some(Self {
                    entry_tx: local_tx.clone(),
                    stop_txs: vec![local_tx],
                    joins: vec![local_join],
                }))
            }
            IngressPlacement::Remote => {
                let remote_core = require_core(
                    topology.remote_ingress_core,
                    "remote ingress",
                    topology.remote_ingress_numa_node,
                )?;
                let (remote_tx, remote_rx) = mpsc::sync_channel(config.ingress_queue_depth.max(1));
                let remote_join = spawn_ingress_worker(
                    "kix-ingress-remote",
                    remote_rx,
                    client,
                    media_store,
                    remote_core,
                    topology.remote_ingress_numa_node,
                    None,
                )?;
                Ok(Some(Self {
                    entry_tx: remote_tx.clone(),
                    stop_txs: vec![remote_tx],
                    joins: vec![remote_join],
                }))
            }
            IngressPlacement::Handoff => {
                let local_core = require_core(
                    topology.local_ingress_core,
                    "local ingress",
                    topology.raw_device_numa_node,
                )?;
                let remote_core = require_core(
                    topology.remote_ingress_core,
                    "remote ingress",
                    topology.remote_ingress_numa_node,
                )?;
                let (local_tx, local_rx) = mpsc::sync_channel(config.ingress_queue_depth.max(1));
                let (remote_tx, remote_rx) = mpsc::sync_channel(config.ingress_queue_depth.max(1));
                let local_join = spawn_ingress_worker(
                    "kix-ingress-owner",
                    local_rx,
                    Arc::clone(&client),
                    media_store.clone(),
                    local_core,
                    topology.raw_device_numa_node,
                    None,
                )?;
                let remote_join = spawn_ingress_worker(
                    "kix-ingress-handoff",
                    remote_rx,
                    client,
                    media_store,
                    remote_core,
                    topology.remote_ingress_numa_node,
                    Some(local_tx.clone()),
                )?;
                Ok(Some(Self {
                    entry_tx: remote_tx.clone(),
                    stop_txs: vec![remote_tx, local_tx],
                    joins: vec![remote_join, local_join],
                }))
            }
        }
    }

    pub(crate) fn submit_batch(
        &self,
        requests: Vec<BenchRequest>,
    ) -> Result<Vec<OperationMetrics>, String> {
        let (resp_tx, resp_rx) = mpsc::sync_channel(0);
        self.entry_tx
            .send(IngressCommand::ExecuteBatch {
                requests,
                resp: resp_tx,
            })
            .map_err(|_| {
                "KIX ingress queue is closed; an ingress worker likely exited unexpectedly"
                    .to_string()
            })?;
        resp_rx.recv().map_err(|_| {
            "KIX ingress response channel closed before the request batch completed".to_string()
        })?
    }
}

impl Drop for IngressRuntime {
    fn drop(&mut self) {
        for tx in &self.stop_txs {
            let _ = tx.send(IngressCommand::Stop);
        }
        for join in self.joins.drain(..) {
            let _ = join.join();
        }
    }
}

enum IngressCommand {
    ExecuteBatch {
        requests: Vec<BenchRequest>,
        resp: SyncSender<Result<Vec<OperationMetrics>, String>>,
    },
    Stop,
}

fn spawn_ingress_worker(
    name: &str,
    rx: Receiver<IngressCommand>,
    client: Arc<KixClient>,
    media_store: Option<Arc<MediaStore>>,
    pin_core: usize,
    numa_node: Option<i32>,
    forward_to: Option<SyncSender<IngressCommand>>,
) -> Result<JoinHandle<()>, io::Error> {
    let thread_name = name.to_string();
    thread::Builder::new().name(thread_name.clone()).spawn(move || {
        if let Err(err) = maybe_pin_to_core(pin_core) {
            eprintln!("warning: {thread_name} could not pin to CPU core {pin_core}: {err}");
        }
        if let Err(err) = set_current_thread_memory_policy(numa_node) {
            eprintln!(
                "warning: {thread_name} could not bind memory allocations to NUMA node {}: {err}",
                option_i32(numa_node)
            );
        }

        while let Ok(command) = rx.recv() {
            match command {
                IngressCommand::ExecuteBatch { requests, resp } => {
                    if let Some(forward_to) = &forward_to {
                        let fallback_resp = resp.clone();
                        if forward_to
                            .send(IngressCommand::ExecuteBatch { requests, resp })
                            .is_err()
                        {
                            let _ = fallback_resp.send(Err(
                                "KIX owner-domain ingress queue is closed; the locality handoff path is unavailable"
                                    .to_string(),
                            ));
                        }
                        continue;
                    }

                    let result =
                        execute_request_batch(client.as_ref(), media_store.as_deref(), &requests);
                    let _ = resp.send(result);
                }
                IngressCommand::Stop => break,
            }
        }
    })
}

fn require_core(
    core_id: Option<usize>,
    label: &str,
    numa_node: Option<i32>,
) -> Result<usize, Box<dyn std::error::Error>> {
    core_id.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
            "KIX could not select a {} core for NUMA node {}. Check your pinning, reserve-socket-core budget, and raw-device locality.",
            label,
            option_i32(numa_node)
            ),
        )
        .into()
    })
}

fn maybe_pin_to_core(core_id: usize) -> io::Result<()> {
    let Some(cores) = core_affinity::get_core_ids() else {
        return Err(io::Error::other(
            "KIX ingress could not enumerate CPU cores for placement",
        ));
    };
    if let Some(core) = cores.into_iter().find(|core| core.id == core_id) {
        if core_affinity::set_for_current(core) {
            return Ok(());
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("requested CPU core {core_id} is unavailable for KIX ingress placement"),
    ))
}

fn set_current_thread_memory_policy(numa_node: Option<i32>) -> io::Result<()> {
    let Some(numa_node) = numa_node else {
        return Ok(());
    };
    if numa_node < 0 {
        return Ok(());
    }

    let maxnode = (numa_node as usize) + 1;
    let bits_per_word = usize::BITS as usize;
    let mut nodemask = vec![0_usize; maxnode.div_ceil(bits_per_word)];
    nodemask[numa_node as usize / bits_per_word] |= 1_usize << (numa_node as usize % bits_per_word);

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
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "failed to set KIX ingress memory policy to NUMA node {numa_node}: {}",
                io::Error::last_os_error()
            ),
        ))
    }
}

fn option_i32(value: Option<i32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}
