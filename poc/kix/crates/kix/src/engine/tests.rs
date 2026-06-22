// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use super::*;
use crate::arena::DriveConfig;
use crate::types::{ChunkId, LocationRecord};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static NEXT_TEST_DIR_ID: AtomicU64 = AtomicU64::new(0);

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(label: &str) -> Self {
        let unique = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "kix-engine-{label}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn drive_configs(root: &Path, drive_count: usize) -> Vec<DriveConfig> {
    (0..drive_count)
        .map(|drive_id| {
            DriveConfig::file(drive_id as u16, root.join(format!("drive-{drive_id}.kix")))
        })
        .collect()
}

fn test_config(root: &Path, shard_count: usize, drive_count: usize) -> KixConfig {
    KixConfig {
        shard_count,
        lookup_worker_mode: WorkerMode::Interrupt,
        commit_worker_mode: WorkerMode::Interrupt,
        drive_worker_mode: WorkerMode::Interrupt,
        drive_configs: drive_configs(root, drive_count),
        lookup_pin_cores: Vec::new(),
        commit_pin_cores: Vec::new(),
        drive_pin_cores: Vec::new(),
        shard_numa_node: None,
        lookup_queue_depth: 128,
        commit_queue_depth: 128,
        drive_queue_depth: 128,
        stats: None,
    }
}

#[test]
fn engine_checkpoint_roundtrip() {
    let dir = TestDir::new("checkpoint-roundtrip");
    let config = test_config(dir.path(), 4, 2);

    let engine = KixEngine::open(config.clone()).unwrap();
    let client = engine.client();

    let a = ChunkId::from_seed(1);
    let b = ChunkId::from_seed(2);
    let rec_a = LocationRecord::extent(0, 4096, 65536, 65536, 1, 100);
    let rec_b = LocationRecord::packed(1, 8192, 16384, 16384, 9, 200);

    client.upsert(a, 0, rec_a).unwrap();
    client.upsert(b, 1, rec_b).unwrap();
    engine.checkpoint_all().unwrap();
    drop(engine);

    let reopened = KixEngine::open(config).unwrap();
    let client = reopened.client();
    assert_eq!(client.get(a).unwrap(), Some(rec_a));
    assert_eq!(client.get(b).unwrap(), Some(rec_b));
}

#[test]
fn granule_inverse_tracks_owner_with_generation_fence() {
    let dir = TestDir::new("granule-inverse");
    let config = test_config(dir.path(), 2, 1);
    let engine = KixEngine::open(config).unwrap();
    let client = engine.client();
    let a = ChunkId::from_seed(10);
    let b = ChunkId::from_seed(11);
    let c = ChunkId::from_seed(12);
    // gen 1 at (drive 0, slot 5)
    client
        .upsert(a, 5, LocationRecord::extent(0, 4096, 65536, 65536, 1, 1))
        .unwrap();
    assert_eq!(client.lookup_granule_chunk(0, 5), Some((a, 1)));
    // a higher generation takes the slot
    client
        .upsert(b, 5, LocationRecord::extent(0, 4096, 65536, 65536, 3, 2))
        .unwrap();
    assert_eq!(client.lookup_granule_chunk(0, 5), Some((b, 3)));
    // a lower generation does NOT displace the owner (fence)
    client
        .upsert(c, 5, LocationRecord::extent(0, 4096, 65536, 65536, 2, 3))
        .unwrap();
    assert_eq!(client.lookup_granule_chunk(0, 5), Some((b, 3)));
    // unknown slot has no owner
    assert_eq!(client.lookup_granule_chunk(0, 999), None);
    // boot reseed is idempotent + generation-fenced
    client.seed_inverse(0, 7, a, 5);
    assert_eq!(client.lookup_granule_chunk(0, 7), Some((a, 5)));
    client.seed_inverse(0, 7, b, 4);
    assert_eq!(client.lookup_granule_chunk(0, 7), Some((a, 5)));
}

#[test]
fn corrupted_drive_marks_rebuild_required() {
    let dir = TestDir::new("corrupted-drive");
    let mut config = test_config(dir.path(), 2, 1);
    let configs = drive_configs(dir.path(), 1);
    std::fs::write(&configs[0].arena_path, b"bad-kix").unwrap();
    config.drive_configs = configs;

    let engine = KixEngine::open(config).unwrap();

    assert_eq!(engine.rebuild_required_drives(), vec![0]);
}

#[test]
fn delete_survives_restart() {
    let dir = TestDir::new("delete-survives-restart");
    let config = test_config(dir.path(), 2, 1);

    let engine = KixEngine::open(config.clone()).unwrap();
    let client = engine.client();
    let chunk = ChunkId::from_seed(44);
    let record = LocationRecord::packed(0, 16_384, 16_384, 16_384, 1, 99);

    client.upsert(chunk, 0, record).unwrap();
    client.delete(chunk).unwrap();
    drop(engine);

    let reopened = KixEngine::open(config).unwrap();
    assert_eq!(reopened.client().get(chunk).unwrap(), None);
}

#[test]
fn older_generation_cannot_overwrite_newer_live_record() {
    let dir = TestDir::new("older-generation-ignored");
    let config = test_config(dir.path(), 2, 1);

    let engine = KixEngine::open(config.clone()).unwrap();
    let client = engine.client();
    let chunk = ChunkId::from_seed(55);
    let newer = LocationRecord::packed(0, 16_384, 16_384, 16_384, 8, 222);
    let older = LocationRecord::packed(0, 32_768, 16_384, 16_384, 7, 111);

    client.upsert(chunk, 0, newer).unwrap();
    client.upsert(chunk, 0, older).unwrap();
    assert_eq!(client.get(chunk).unwrap(), Some(newer));
    drop(engine);

    let reopened = KixEngine::open(config).unwrap();
    assert_eq!(reopened.client().get(chunk).unwrap(), Some(newer));
}

#[test]
fn duplicate_drive_ids_are_rejected_before_startup() {
    let dir = TestDir::new("duplicate-drive-ids");
    let mut config = test_config(dir.path(), 2, 2);
    config.drive_configs[1].id = config.drive_configs[0].id;

    let err = config.validate().unwrap_err();

    assert!(err.to_string().contains("duplicate KIX drive id"));
}

#[test]
fn impossible_cpu_pin_is_rejected_before_workers_spawn() {
    let dir = TestDir::new("impossible-cpu-pin");
    let available = core_affinity::get_core_ids().unwrap();
    let impossible_core = available
        .iter()
        .map(|core| core.id)
        .max()
        .unwrap_or(0)
        .saturating_add(10_000);

    let mut config = test_config(dir.path(), 2, 1);
    config.lookup_pin_cores = vec![impossible_core];

    let err = config.validate().unwrap_err();

    assert!(err
        .to_string()
        .contains("KIX startup rejected lookup pinning"));
}

#[test]
fn stats_tree_contains_queue_and_latency_details() {
    let dir = TestDir::new("stats-tree");
    let stats_root = dir.path().join("stats");
    let mut config = test_config(dir.path(), 2, 1);
    config.stats = Some(KixStatsConfig {
        root_dir: stats_root.clone(),
        publish_interval: Duration::from_millis(10),
    });

    let engine = KixEngine::open(config).unwrap();
    let client = engine.client();
    let chunk = ChunkId::from_seed(7);
    let record = LocationRecord::packed(0, 8_192, 16_384, 16_384, 1, 77);

    client.upsert(chunk, 0, record).unwrap();
    assert_eq!(client.get(chunk).unwrap(), Some(record));
    engine.checkpoint_all().unwrap();

    let runtime_dir = engine.stats_runtime_dir().unwrap().to_path_buf();
    std::thread::sleep(Duration::from_millis(25));
    let summary = std::fs::read_to_string(runtime_dir.join("summary")).unwrap();
    let hardware = std::fs::read_to_string(runtime_dir.join("hardware")).unwrap();
    let shard_stats = std::fs::read_to_string(runtime_dir.join("shards/0")).unwrap();
    let drive_stats = std::fs::read_to_string(runtime_dir.join("drives/0")).unwrap();

    assert!(summary.contains("total_get_ops="));
    assert!(summary.contains("crc32_backend="));
    assert!(hardware.contains("crc32_backend="));
    assert!(hardware.contains("crc32_accelerated="));
    assert!(shard_stats.contains("lookup_queue_depth="));
    assert!(shard_stats.contains("commit_queue_depth="));
    assert!(shard_stats.contains("get_latency_p95_us="));
    assert!(drive_stats.contains("append_latency_p95_us="));
    assert!(drive_stats.contains("checkpoint_latency_p95_us="));
}
