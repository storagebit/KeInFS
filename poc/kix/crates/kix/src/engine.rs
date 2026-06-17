// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

mod config;
mod runtime;
mod stats;

use crate::arena::preflight_drive_requirements;
use crate::hardware::{detect_hardware_acceleration, KixHardwareAcceleration};
use crate::types::{ChunkId, LocationRecord};
use runtime::{
    commit_main, configure_worker_startup, lookup_main, shard_for, CommitRequest, DriveArenaSet,
    DriveRequest, LookupRequest, ShardHandle, ShardQueueSet, ShardState, WorkQueue,
};
use stats::{
    spawn_stats_publisher, write_stats_tree, DriveRuntimeStats, KixRuntimeStats, ShardRuntimeStats,
    StatsPublisherHandle,
};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub use config::{KixConfig, KixError, KixStatsConfig, WorkerMode};
pub use stats::{DriveStatsSnapshot, KixStatsSnapshot, LatencyStatsSnapshot, ShardStatsSnapshot};

#[derive(Clone)]
pub struct KixClient {
    shard_states: Arc<Vec<Arc<ShardState>>>,
    shard_stats: Arc<Vec<Arc<ShardRuntimeStats>>>,
    drive_arenas: Arc<DriveArenaSet>,
}

#[derive(Clone)]
pub struct KixStatsHandle {
    runtime_stats: Arc<KixRuntimeStats>,
    shard_queues: Arc<Vec<ShardQueueSet>>,
    drive_queues: Arc<HashMap<u16, Arc<WorkQueue<DriveRequest>>>>,
}

impl KixStatsHandle {
    pub fn snapshot(&self) -> KixStatsSnapshot {
        self.runtime_stats
            .snapshot(self.shard_queues.as_ref(), &self.drive_queues)
    }

    pub fn write_tree(&self, root: impl AsRef<Path>) -> Result<PathBuf, KixError> {
        let snapshot = self.snapshot();
        write_stats_tree(&snapshot, root).map_err(KixError::Io)
    }
}

impl KixClient {
    pub fn get(&self, chunk_id: ChunkId) -> Result<Option<LocationRecord>, KixError> {
        let shard = self.shard_index_for(&chunk_id)?;
        let stats = &self.shard_stats[shard];
        let t0 = Instant::now();
        stats.get_ops.fetch_add(1, Ordering::Relaxed);
        let hit = self.shard_states[shard].get(&chunk_id);
        if hit.is_some() {
            stats.get_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.get_misses.fetch_add(1, Ordering::Relaxed);
        }
        stats.get_latency.observe(t0.elapsed());
        Ok(hit)
    }

    pub fn upsert(&self, chunk_id: ChunkId, record: LocationRecord) -> Result<(), KixError> {
        let shard = self.shard_index_for(&chunk_id)?;
        let stats = &self.shard_stats[shard];
        let t0 = Instant::now();
        stats.upsert_ops.fetch_add(1, Ordering::Relaxed);
        let result = self
            .drive_arenas
            .append_delta(
                record.drive_id,
                crate::arena::DeltaEntry::upsert(chunk_id, record),
            )
            .map(|_| {
                let live_entries = self.shard_states[shard].upsert(chunk_id, record);
                stats
                    .live_entries
                    .store(live_entries as u64, Ordering::Relaxed);
            });
        if let Err(err) = &result {
            stats.record_error(format!(
                "KIX shard {} could not persist upsert for drive {}: {err}",
                stats.shard_id, record.drive_id
            ));
        }
        stats.upsert_latency.observe(t0.elapsed());
        result
    }

    pub fn delete(&self, chunk_id: ChunkId) -> Result<(), KixError> {
        let shard = self.shard_index_for(&chunk_id)?;
        let stats = &self.shard_stats[shard];
        let t0 = Instant::now();
        stats.delete_ops.fetch_add(1, Ordering::Relaxed);
        let drive_id = self.shard_states[shard].drive_id_for(&chunk_id);
        let result = if let Some(drive_id) = drive_id {
            self.drive_arenas
                .append_delta(drive_id, crate::arena::DeltaEntry::delete(chunk_id))
                .map(|_| {
                    let live_entries = self.shard_states[shard].delete(&chunk_id);
                    stats
                        .live_entries
                        .store(live_entries as u64, Ordering::Relaxed);
                })
        } else {
            Ok(())
        };
        if let Err(err) = &result {
            stats.record_error(format!(
                "KIX shard {} could not persist delete for chunk on drive {}: {err}",
                stats.shard_id,
                drive_id.unwrap_or_default()
            ));
        }
        stats.delete_latency.observe(t0.elapsed());
        result
    }

    fn shard_index_for(&self, chunk_id: &ChunkId) -> Result<usize, KixError> {
        let shard = shard_for(chunk_id, self.shard_states.len());
        if self.shard_states.get(shard).is_some() {
            Ok(shard)
        } else {
            Err(KixError::ChannelClosed)
        }
    }
}

pub struct KixEngine {
    client: KixClient,
    stats: KixStatsHandle,
    stats_publisher: Option<StatsPublisherHandle>,
    shard_handles: Vec<ShardHandle>,
    shard_states: Arc<Vec<Arc<ShardState>>>,
    drive_arenas: Arc<DriveArenaSet>,
    rebuild_required_drives: HashSet<u16>,
}

impl KixEngine {
    pub fn open(config: KixConfig) -> Result<Self, KixError> {
        config.validate()?;
        let hardware = detect_hardware_acceleration();
        for cfg in &config.drive_configs {
            preflight_drive_requirements(cfg)?;
        }

        let drive_stats = config
            .drive_configs
            .iter()
            .enumerate()
            .map(|(drive_index, cfg)| {
                DriveRuntimeStats::new(
                    cfg,
                    config.drive_worker_mode,
                    config.drive_pin_cores.get(drive_index).copied(),
                    config.drive_queue_depth,
                )
            })
            .collect::<Vec<_>>();

        let (drive_arenas, recoveries) = DriveArenaSet::open(
            &config.drive_configs,
            config.drive_worker_mode,
            &config.drive_pin_cores,
            config.drive_queue_depth,
            &drive_stats,
        )?;
        let drive_arenas = Arc::new(drive_arenas);

        let mut rebuild_required_drives = HashSet::new();
        let mut initial_shards = (0..config.shard_count)
            .map(|_| Vec::<(ChunkId, LocationRecord)>::new())
            .collect::<Vec<_>>();

        for recovery in recoveries {
            if recovery.rebuild_required {
                rebuild_required_drives.insert(recovery.drive_id);
            }
            for (chunk_id, record) in recovery.entries {
                let shard = shard_for(&chunk_id, config.shard_count);
                initial_shards[shard].push((chunk_id, record));
            }
        }

        let shard_stats = (0..config.shard_count)
            .map(|shard_id| {
                ShardRuntimeStats::new(
                    shard_id,
                    config.lookup_worker_mode,
                    config.commit_worker_mode,
                    config.lookup_pin_cores.get(shard_id).copied(),
                    config.commit_pin_cores.get(shard_id).copied(),
                    config.shard_numa_node,
                    config.lookup_queue_depth,
                    config.commit_queue_depth,
                )
            })
            .collect::<Vec<_>>();

        let mut shard_handles = Vec::with_capacity(config.shard_count);
        let mut shard_queues = Vec::with_capacity(config.shard_count);
        let mut shard_states = Vec::with_capacity(config.shard_count);
        let shard_numa_node = config.shard_numa_node;

        for shard_id in 0..config.shard_count {
            let shard_state = Arc::new(ShardState::new(std::mem::take(
                &mut initial_shards[shard_id],
            )));
            shard_stats[shard_id].live_entries.store(
                shard_state.len() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            shard_states.push(Arc::clone(&shard_state));

            let lookup_mode = config.lookup_worker_mode;
            let lookup_pin_core = config.lookup_pin_cores.get(shard_id).copied();
            let lookup_queue_depth = config.lookup_queue_depth;
            let shard_stats = Arc::clone(&shard_stats[shard_id]);
            let (lookup_startup_tx, lookup_startup_rx) = mpsc::sync_channel(0);
            let lookup_state = Arc::clone(&shard_state);
            let lookup_stats = Arc::clone(&shard_stats);
            let lookup_join = std::thread::Builder::new()
                .name(format!("kix-shard-{shard_id}"))
                .spawn(move || {
                    let startup = configure_worker_startup(
                        &format!("KIX lookup worker {shard_id}"),
                        lookup_pin_core,
                        shard_numa_node,
                        lookup_queue_depth,
                        Arc::clone(&lookup_stats.lookup_queue),
                    );
                    let queue = match startup {
                        Ok(queue) => {
                            if lookup_startup_tx.send(Ok(Arc::clone(&queue))).is_err() {
                                return;
                            }
                            queue
                        }
                        Err(err) => {
                            let _ = lookup_startup_tx.send(Err(err));
                            return;
                        }
                    };
                    lookup_main(shard_id, queue, lookup_state, lookup_mode, lookup_stats);
                })
                .map_err(KixError::Io)?;
            let lookup_queue = lookup_startup_rx
                .recv()
                .map_err(|_| KixError::ChannelClosed)??;

            let commit_mode = config.commit_worker_mode;
            let commit_pin_core = config.commit_pin_cores.get(shard_id).copied();
            let commit_queue_depth = config.commit_queue_depth;
            let (commit_startup_tx, commit_startup_rx) = mpsc::sync_channel(0);
            let commit_state = Arc::clone(&shard_state);
            let commit_stats = Arc::clone(&shard_stats);
            let drive_arenas = Arc::clone(&drive_arenas);
            let commit_join = std::thread::Builder::new()
                .name(format!("kix-shard-commit-{shard_id}"))
                .spawn(move || {
                    let startup = configure_worker_startup(
                        &format!("KIX commit worker {shard_id}"),
                        commit_pin_core,
                        shard_numa_node,
                        commit_queue_depth,
                        Arc::clone(&commit_stats.commit_queue),
                    );
                    let queue = match startup {
                        Ok(queue) => {
                            if commit_startup_tx.send(Ok(Arc::clone(&queue))).is_err() {
                                return;
                            }
                            queue
                        }
                        Err(err) => {
                            let _ = commit_startup_tx.send(Err(err));
                            return;
                        }
                    };
                    commit_main(
                        shard_id,
                        queue,
                        drive_arenas,
                        commit_state,
                        commit_mode,
                        commit_stats,
                    );
                })
                .map_err(KixError::Io)?;
            let commit_queue = match commit_startup_rx
                .recv()
                .map_err(|_| KixError::ChannelClosed)?
            {
                Ok(queue) => queue,
                Err(err) => {
                    lookup_queue.close();
                    let _ = lookup_join.join();
                    return Err(err);
                }
            };

            shard_queues.push(ShardQueueSet {
                lookup: Arc::clone(&lookup_queue),
                commit: Arc::clone(&commit_queue),
            });
            shard_handles.push(ShardHandle {
                lookup_queue,
                commit_queue,
                lookup_join: Some(lookup_join),
                commit_join: Some(commit_join),
            });
        }

        let mut rebuild_required_sorted =
            rebuild_required_drives.iter().copied().collect::<Vec<_>>();
        rebuild_required_sorted.sort_unstable();
        let runtime_stats = Arc::new(KixRuntimeStats {
            pid: std::process::id(),
            started_at: Instant::now(),
            started_unix_s: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            hardware,
            shards: shard_stats,
            drives: drive_stats,
            rebuild_required_drives: rebuild_required_sorted,
        });
        let stats = KixStatsHandle {
            runtime_stats,
            shard_queues: Arc::new(shard_queues),
            drive_queues: Arc::clone(&drive_arenas.queues),
        };
        let shard_states = Arc::new(shard_states);
        let shard_stats = Arc::new(stats.runtime_stats.shards.clone());
        let stats_publisher = match &config.stats {
            Some(cfg) => Some(spawn_stats_publisher(stats.clone(), cfg)?),
            None => None,
        };

        Ok(Self {
            client: KixClient {
                shard_states: Arc::clone(&shard_states),
                shard_stats,
                drive_arenas: Arc::clone(&drive_arenas),
            },
            stats,
            stats_publisher,
            shard_handles,
            shard_states,
            drive_arenas,
            rebuild_required_drives,
        })
    }

    pub fn client(&self) -> KixClient {
        self.client.clone()
    }

    pub fn rebuild_required_drives(&self) -> Vec<u16> {
        let mut drives = self
            .rebuild_required_drives
            .iter()
            .copied()
            .collect::<Vec<_>>();
        drives.sort_unstable();
        drives
    }

    pub fn stats_handle(&self) -> KixStatsHandle {
        self.stats.clone()
    }

    pub fn stats_snapshot(&self) -> KixStatsSnapshot {
        self.stats.snapshot()
    }

    pub fn snapshot_entries(&self) -> Vec<(ChunkId, LocationRecord)> {
        let mut entries = Vec::new();
        for shard_state in self.shard_states.iter() {
            entries.extend(shard_state.snapshot_entries());
        }
        entries
    }

    pub fn hardware_acceleration(&self) -> KixHardwareAcceleration {
        self.stats.runtime_stats.hardware
    }

    pub fn write_stats_tree(&self, root: impl AsRef<Path>) -> Result<PathBuf, KixError> {
        self.stats.write_tree(root)
    }

    pub fn stats_runtime_dir(&self) -> Option<&Path> {
        self.stats_publisher
            .as_ref()
            .map(|publisher| publisher.runtime_dir.as_path())
    }

    pub fn checkpoint_all(&self) -> Result<(), KixError> {
        let mut grouped: HashMap<u16, Vec<(ChunkId, LocationRecord)>> = HashMap::new();

        for (shard_id, shard_state) in self.shard_states.iter().enumerate() {
            let stats = &self.stats.runtime_stats.shards[shard_id];
            let t0 = Instant::now();
            stats.snapshot_ops.fetch_add(1, Ordering::Relaxed);
            let snapshot = shard_state.snapshot_entries();
            stats.snapshot_latency.observe(t0.elapsed());
            for (chunk_id, record) in snapshot {
                grouped
                    .entry(record.drive_id)
                    .or_default()
                    .push((chunk_id, record));
            }
        }

        for (drive_id, entries) in grouped {
            self.drive_arenas.write_checkpoint(drive_id, entries)?;
        }

        Ok(())
    }
}

impl Drop for KixEngine {
    fn drop(&mut self) {
        if let Some(publisher) = &mut self.stats_publisher {
            if let Some(stop_tx) = publisher.stop_tx.take() {
                let _ = stop_tx.send(());
            }
            if let Some(join) = publisher.join.take() {
                let _ = join.join();
            }
        }
        for shard in &self.shard_handles {
            let _ = shard.lookup_queue.send(LookupRequest::Stop);
            let _ = shard.commit_queue.send(CommitRequest::Stop);
        }
        for shard in &mut self.shard_handles {
            if let Some(join) = shard.lookup_join.take() {
                let _ = join.join();
            }
            if let Some(join) = shard.commit_join.take() {
                let _ = join.join();
            }
        }
    }
}

#[cfg(test)]
mod tests;
