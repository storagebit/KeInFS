// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use super::*;
use crate::types::{ChunkId, LocationRecord};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEST_DIR_ID: AtomicU64 = AtomicU64::new(0);

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(label: &str) -> Self {
        let unique = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("kix-arena-{label}-{}-{unique}", std::process::id()));
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

fn direct_test_path(label: &str) -> PathBuf {
    let unique = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
    let root = std::env::current_dir()
        .unwrap()
        .join("target")
        .join("kix-direct-tests");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!("{label}-{}-{unique}.kix", std::process::id()))
}

#[test]
fn checkpoint_then_delta_recovers_latest_state() {
    let dir = TestDir::new("checkpoint-then-delta");
    let path = dir.path().join("drive0.kix");
    let mut arena = DriveArena::open(&path, 0).unwrap();

    let a = ChunkId::from_seed(1);
    let b = ChunkId::from_seed(2);
    let rec_a = LocationRecord::extent(0, 4096, 65536, 65536, 1, 10);
    let rec_b = LocationRecord::packed(0, 8192, 16384, 16384, 7, 20);

    arena.append_delta(DeltaEntry::upsert(a, rec_a)).unwrap();
    arena.write_checkpoint([(a, rec_a)]).unwrap();
    arena.append_delta(DeltaEntry::upsert(b, rec_b)).unwrap();

    let recovered = DriveArena::recover_from_path(&path, 0).unwrap();
    assert!(!recovered.rebuild_required);
    assert!(!recovered.tail_corruption);
    assert_eq!(recovered.entries.get(&a), Some(&rec_a));
    assert_eq!(recovered.entries.get(&b), Some(&rec_b));
}

#[test]
fn tail_corruption_preserves_prior_frames() {
    let dir = TestDir::new("tail-corruption");
    let path = dir.path().join("drive0.kix");
    let mut arena = DriveArena::open(&path, 0).unwrap();

    let a = ChunkId::from_seed(11);
    let b = ChunkId::from_seed(12);
    let rec_a = LocationRecord::extent(0, 4096, 65536, 65536, 1, 10);
    let rec_b = LocationRecord::packed(0, 8192, 16384, 16384, 7, 20);

    arena.append_delta(DeltaEntry::upsert(a, rec_a)).unwrap();
    arena.append_delta(DeltaEntry::upsert(b, rec_b)).unwrap();
    drop(arena);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let len = file.metadata().unwrap().len();
    file.set_len(len - 8).unwrap();
    file.sync_all().unwrap();

    let recovered = DriveArena::recover_from_path(&path, 0).unwrap();
    assert!(recovered.tail_corruption);
    assert_eq!(recovered.entries.get(&a), Some(&rec_a));
    assert_eq!(recovered.entries.get(&b), None);
}

#[test]
fn truncating_to_replay_len_allows_clean_append_after_tail_corruption() {
    let dir = TestDir::new("truncate-and-append");
    let path = dir.path().join("drive0.kix");
    let mut arena = DriveArena::open(&path, 0).unwrap();

    let a = ChunkId::from_seed(91);
    let b = ChunkId::from_seed(92);
    let c = ChunkId::from_seed(93);
    let rec_a = LocationRecord::extent(0, 4096, 65536, 65536, 1, 10);
    let rec_b = LocationRecord::packed(0, 8192, 16384, 16384, 7, 20);
    let rec_c = LocationRecord::packed(0, 12_288, 16384, 16384, 8, 30);

    arena.append_delta(DeltaEntry::upsert(a, rec_a)).unwrap();
    arena.append_delta(DeltaEntry::upsert(b, rec_b)).unwrap();
    drop(arena);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let len = file.metadata().unwrap().len();
    file.set_len(len - 8).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let recovered = DriveArena::recover_from_path(&path, 0).unwrap();
    assert!(recovered.tail_corruption);

    let mut writer = DriveArena::open(&path, 0).unwrap();
    writer.truncate_to(recovered.replay_len).unwrap();
    writer.append_delta(DeltaEntry::upsert(c, rec_c)).unwrap();
    drop(writer);

    let replayed = DriveArena::recover_from_path(&path, 0).unwrap();
    assert_eq!(replayed.entries.get(&a), Some(&rec_a));
    assert_eq!(replayed.entries.get(&b), None);
    assert_eq!(replayed.entries.get(&c), Some(&rec_c));
}

#[test]
fn invalid_header_requires_rebuild() {
    let dir = TestDir::new("invalid-header");
    let path = dir.path().join("drive0.kix");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    file.write_all(b"not-kix").unwrap();
    file.sync_all().unwrap();

    let recovered = DriveArena::recover_from_path(&path, 0).unwrap();
    assert!(recovered.rebuild_required);
    assert!(recovered.entries.is_empty());
}

#[test]
fn malformed_first_frame_is_treated_as_tail_corruption() {
    let dir = TestDir::new("malformed-first-frame");
    let path = dir.path().join("drive0.kix");
    let mut arena = DriveArena::open(&path, 0).unwrap();

    let bogus_payload = 7_u32.to_le_bytes();
    arena
        .append_frame(FrameKind::DeltaBatch, &bogus_payload)
        .unwrap();
    drop(arena);

    let recovered = DriveArena::recover_from_path(&path, 0).unwrap();
    assert!(!recovered.rebuild_required);
    assert!(recovered.tail_corruption);
    assert!(recovered.entries.is_empty());
}

#[test]
fn malformed_later_frame_is_treated_as_tail_corruption() {
    let dir = TestDir::new("malformed-later-frame");
    let path = dir.path().join("drive0.kix");
    let mut arena = DriveArena::open(&path, 0).unwrap();

    let chunk = ChunkId::from_seed(101);
    let record = LocationRecord::extent(0, 4096, 65536, 65536, 1, 10);
    arena
        .append_delta(DeltaEntry::upsert(chunk, record))
        .unwrap();

    let bogus_payload = 7_u32.to_le_bytes();
    arena
        .append_frame(FrameKind::DeltaBatch, &bogus_payload)
        .unwrap();
    drop(arena);

    let recovered = DriveArena::recover_from_path(&path, 0).unwrap();
    assert!(!recovered.rebuild_required);
    assert!(recovered.tail_corruption);
    assert_eq!(recovered.entries.get(&chunk), Some(&record));
}

#[test]
fn delete_delta_is_replayed() {
    let dir = TestDir::new("delete-delta");
    let path = dir.path().join("drive0.kix");
    let mut arena = DriveArena::open(&path, 0).unwrap();

    let chunk = ChunkId::from_seed(77);
    let record = LocationRecord::packed(0, 12_288, 16_384, 16_384, 5, 17);

    arena
        .append_delta(DeltaEntry::upsert(chunk, record))
        .unwrap();
    arena.append_delta(DeltaEntry::delete(chunk)).unwrap();

    let recovered = DriveArena::recover_from_path(&path, 0).unwrap();
    assert!(!recovered.entries.contains_key(&chunk));
    assert!(!recovered.tail_corruption);
}

#[test]
#[ignore = "requires direct io_uring privilege or a permissive kernel io_uring policy"]
fn direct_uring_checkpoint_then_delta_recovers_latest_state() {
    let path = direct_test_path("direct-uring-checkpoint");
    let mut config = DriveConfig::file(0, path.clone());
    config.io_mode = ArenaIoMode::DirectUring;

    let mut arena = DriveArena::open_config(&config).unwrap();

    let a = ChunkId::from_seed(501);
    let b = ChunkId::from_seed(502);
    let rec_a = LocationRecord::extent(0, 4096, 65536, 65536, 1, 10);
    let rec_b = LocationRecord::packed(0, 8192, 16384, 16384, 7, 20);

    arena.append_delta(DeltaEntry::upsert(a, rec_a)).unwrap();
    arena.write_checkpoint([(a, rec_a)]).unwrap();
    arena.append_delta(DeltaEntry::upsert(b, rec_b)).unwrap();
    drop(arena);

    let recovered = DriveArena::recover(&config).unwrap();
    assert!(!recovered.rebuild_required);
    assert!(!recovered.tail_corruption);
    assert_eq!(recovered.entries.get(&a), Some(&rec_a));
    assert_eq!(recovered.entries.get(&b), Some(&rec_b));
    assert!(is_aligned_u64(recovered.replay_len, KIX_IO_ALIGN as u64));

    let _ = std::fs::remove_file(path);
}

#[test]
#[ignore = "requires direct io_uring privilege or a permissive kernel io_uring policy"]
fn direct_uring_delete_delta_is_replayed() {
    let path = direct_test_path("direct-uring-delete");
    let mut config = DriveConfig::file(0, path.clone());
    config.io_mode = ArenaIoMode::DirectUring;

    let mut arena = DriveArena::open_config(&config).unwrap();
    let chunk = ChunkId::from_seed(777);
    let record = LocationRecord::packed(0, 12_288, 16_384, 16_384, 5, 17);

    arena
        .append_delta(DeltaEntry::upsert(chunk, record))
        .unwrap();
    arena.append_delta(DeltaEntry::delete(chunk)).unwrap();
    drop(arena);

    let recovered = DriveArena::recover(&config).unwrap();
    assert!(!recovered.entries.contains_key(&chunk));
    assert!(!recovered.tail_corruption);
    assert!(!recovered.rebuild_required);

    let _ = std::fs::remove_file(path);
}

#[test]
fn direct_uring_preflight_rejects_misaligned_offsets() {
    let dir = TestDir::new("direct-preflight-misaligned");
    let mut config = DriveConfig::file(0, dir.path().join("drive0.kix"));
    config.io_mode = ArenaIoMode::DirectUring;
    config.arena_offset_bytes = 123;
    config.arena_len_bytes = Some((KIX_IO_ALIGN as u64) * 4);

    let err = preflight_drive_requirements(&config).unwrap_err();
    assert!(err.to_string().contains("arena_offset_bytes to be aligned"));
}
