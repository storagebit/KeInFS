// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use super::*;
use keinbuild::BuildInfo;

fn sample_identity(layout_kind: &str) -> TargetIdentity {
    TargetIdentity {
        build: BuildInfo {
            package_name: "keinfs-kst".to_string(),
            binary_name: "kst".to_string(),
            version: "0.1.0".to_string(),
            release: 1,
            git_sha: "deadbeef".to_string(),
            git_dirty: false,
            built_at_unix_s: 0,
            build_profile: "test".to_string(),
            target_triple: "x86_64-unknown-linux-gnu".to_string(),
        },
        target_id: "t0".to_string(),
        listen_addr: "127.0.0.1:18080".to_string(),
        listen_backlog: 4096,
        pid: 1,
        drive_id: 0,
        raw_device: "/dev/nvme0n1".to_string(),
        raw_offset_bytes: 0,
        raw_slice_bytes: 4096,
        media_device: "/dev/nvme0n1".to_string(),
        media_offset_bytes: 4096,
        media_slice_bytes: 8192,
        layout_kind: layout_kind.to_string(),
        extent_bytes: 1_048_576,
        packed_bytes: 16_384,
        key_slots: 64,
        publication_lanes: 2,
        numa_node: Some(0),
        shard_count: 4,
        lookup_mode: "busy".to_string(),
        commit_mode: "interrupt".to_string(),
        drive_mode: "interrupt".to_string(),
        lookup_pin_cores: vec![0, 1, 2, 3],
        commit_pin_cores: vec![4, 5, 6, 7],
        drive_pin_cores: vec![8],
        lookup_queue_depth: 4096,
        commit_queue_depth: 1024,
        drive_queue_depth: 1024,
        read_ingress_mode: "busy".to_string(),
        write_ingress_mode: "interrupt".to_string(),
        read_ingress_workers: 4,
        write_ingress_workers: 2,
        read_ingress_pin_cores: vec![9, 10, 11, 12],
        write_ingress_pin_cores: vec![13, 14],
        read_ingress_queue_depth: 2048,
        write_ingress_queue_depth: 1024,
        direct_read_mode: "busy".to_string(),
        direct_write_mode: "interrupt".to_string(),
        direct_read_workers: 8,
        direct_write_workers: 8,
        direct_read_pin_cores: vec![15, 16, 17, 18, 19, 20, 21, 22],
        direct_write_pin_cores: vec![23, 24, 25, 26, 27, 28, 29, 30],
        direct_read_queue_depth: 2048,
        direct_write_queue_depth: 2048,
        max_packed_payload_bytes: kp2::MAX_PACK_PAYLOAD_BYTES,
        max_packed_write_request_bytes: kp2::MAX_PACK_PAYLOAD_BYTES + 53_272,
        max_request_body_bytes: 16 * 1024 * 1024,
        max_connections: 4096,
        max_active_streams: 8192,
        max_read_streams: 8192,
        max_write_streams: 8192,
        h2_initial_window_bytes: 1024 * 1024,
        h2_initial_connection_window_bytes: 128 * 1024 * 1024,
        h2_max_frame_bytes: 1024 * 1024,
        h2_max_header_list_bytes: 32 * 1024,
        h2_max_concurrent_streams: 128,
        h2_max_send_buffer_bytes: 8 * 1024 * 1024,
        cpu_arch: "x86_64".to_string(),
        crc32_backend: "x86-pclmulqdq".to_string(),
        crc32_accelerated: true,
        crc32_detail: "detail".to_string(),
        rebuild_required: false,
        target_stats_runtime_dir: "/run/keinfs/kst/t0-1".to_string(),
        kix_stats_runtime_dir: "/run/keinfs/kix/kix-1".to_string(),
    }
}

#[test]
fn classify_rpc_routes_chunk_verbs() {
    assert!(matches!(
        classify_rpc(&Method::HEAD, "/v1/chunk/abcd"),
        RpcKind::Head
    ));
    assert!(matches!(
        classify_rpc(&Method::GET, "/v1/chunk/abcd"),
        RpcKind::Read
    ));
    assert!(matches!(
        classify_rpc(&Method::PUT, "/v1/chunk/abcd"),
        RpcKind::Write
    ));
    assert!(matches!(
        classify_rpc(&Method::DELETE, "/v1/chunk/abcd"),
        RpcKind::Delete
    ));
    assert!(matches!(
        classify_rpc(&Method::PUT, "/v1/kp2/chunk-pack"),
        RpcKind::Write
    ));
    assert!(matches!(
        classify_rpc(&Method::POST, "/v1/kp2/chunk-pack/read"),
        RpcKind::Read
    ));
}

#[test]
fn parse_chunk_id_requires_32_bytes_of_hex() {
    let chunk_id = parse_chunk_id_from_path(
        "/v1/chunk/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    )
    .expect("valid chunk id should parse");
    assert_eq!(chunk_id.0[0], 0);
    assert!(parse_chunk_id_from_path("/v1/chunk/abcd").is_err());
}

#[test]
fn payload_len_follows_layout_kind() {
    let extent = sample_identity("extent-only");
    let packed = sample_identity("packed-only");
    let mixed = sample_identity("mixed");
    assert_eq!(payload_len_for_slot(&extent, 7).unwrap(), 1_048_576);
    assert_eq!(payload_len_for_slot(&packed, 7).unwrap(), 16_384);
    assert_eq!(payload_len_for_slot(&mixed, 2).unwrap(), 1_048_576);
    assert_eq!(payload_len_for_slot(&mixed, 3).unwrap(), 16_384);
}

#[test]
fn build_slot_publications_seeds_current_owner() {
    let layout = kix::ChunkMediaLayoutSpec {
        layout_kind: kix::ChunkMediaLayoutKind::ExtentOnly,
        extent_bytes: 1_048_576,
        packed_bytes: 16_384,
        key_slots: 64,
    };
    let chunk_id = ChunkId::from_seed(7);
    let record = kix::chunk_media_record_for_slot(&layout, 0, 9, 3).unwrap();

    let publications = build_slot_publications(&layout, 64, vec![(chunk_id, record)]).unwrap();
    let owner = publications[9].current().unwrap();

    assert_eq!(owner.chunk_id, chunk_id);
    assert_eq!(owner.record, record);
}

#[test]
fn build_slot_publications_rejects_conflicting_live_slot_owners() {
    let layout = kix::ChunkMediaLayoutSpec {
        layout_kind: kix::ChunkMediaLayoutKind::ExtentOnly,
        extent_bytes: 1_048_576,
        packed_bytes: 16_384,
        key_slots: 64,
    };
    let first = (
        ChunkId::from_seed(1),
        kix::chunk_media_record_for_slot(&layout, 0, 11, 1).unwrap(),
    );
    let second = (
        ChunkId::from_seed(2),
        kix::chunk_media_record_for_slot(&layout, 0, 11, 2).unwrap(),
    );

    let err = build_slot_publications(&layout, 64, vec![first, second]).unwrap_err();

    assert!(err.to_string().contains("conflicting live slot owners"));
}

// --- SlotPublication lane/generation state machine (no media / io_uring) ---

fn extent_layout() -> kix::ChunkMediaLayoutSpec {
    kix::ChunkMediaLayoutSpec {
        layout_kind: kix::ChunkMediaLayoutKind::ExtentOnly,
        extent_bytes: 1_048_576,
        packed_bytes: 16_384,
        key_slots: 64,
    }
}

fn owner_for(slot: u64, seed: u64, generation: u32) -> PublishedSlotOwner {
    let layout = extent_layout();
    PublishedSlotOwner {
        chunk_id: ChunkId::from_seed(seed),
        record: kix::chunk_media_record_for_slot(&layout, 0, slot, generation).unwrap(),
    }
}

#[test]
fn reserve_on_empty_slot_uses_fresh_lane_and_commit_installs_owner() {
    let publication = SlotPublication::new(SlotPublicationState::default());
    let reservation = publication
        .reserve(|current| {
            assert!(current.is_none());
            Ok(7)
        })
        .unwrap();
    assert_eq!(reservation.lane, 7);
    assert_eq!(reservation.current, None);

    let owner = owner_for(5, 1, 1);
    publication.commit(owner).unwrap();
    assert_eq!(publication.current(), Some(owner));
}

#[test]
fn reserve_observes_committed_owner_record() {
    let owner = owner_for(5, 1, 1);
    let publication = SlotPublication::new(SlotPublicationState {
        current: Some(owner),
        busy: false,
    });
    let reservation = publication
        .reserve(|current| {
            assert_eq!(current, Some(owner.record));
            Ok((owner.record.generation as u64 + 1) % 2)
        })
        .unwrap();
    assert_eq!(reservation.current, Some(owner));
}

#[test]
fn rollback_releases_busy_without_changing_owner() {
    let owner = owner_for(5, 1, 1);
    let publication = SlotPublication::new(SlotPublicationState {
        current: Some(owner),
        busy: false,
    });
    let _ = publication.reserve(|_| Ok(1)).unwrap();
    publication.rollback();
    // The slot is idle again, so a fresh reservation succeeds without blocking,
    // and the original owner is untouched.
    let again = publication.reserve(|current| {
        assert_eq!(current, Some(owner.record));
        Ok(0)
    });
    assert!(again.is_ok());
    assert_eq!(publication.current(), Some(owner));
}

#[test]
fn commit_keeps_newer_generation_owner() {
    let publication = SlotPublication::new(SlotPublicationState::default());
    let newer = owner_for(5, 1, 5);
    let _ = publication.reserve(|_| Ok(1)).unwrap();
    publication.commit(newer).unwrap();
    assert_eq!(publication.current(), Some(newer));

    // A stale-generation commit for the same chunk must not roll the owner back.
    let older = owner_for(5, 1, 3);
    let _ = publication.reserve(|_| Ok(0)).unwrap();
    publication.commit(older).unwrap();
    assert_eq!(publication.current(), Some(newer));
}

#[test]
fn second_reserver_blocks_until_first_resolves() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let publication = Arc::new(SlotPublication::new(SlotPublicationState::default()));
    // First writer reserves and holds the in-flight publication.
    let first = publication.reserve(|_| Ok(0)).unwrap();
    assert_eq!(first.lane, 0);

    let second_done = Arc::new(AtomicBool::new(false));
    let publication_for_thread = Arc::clone(&publication);
    let flag = Arc::clone(&second_done);
    let handle = std::thread::spawn(move || {
        // This blocks inside reserve() until the first writer commits.
        let reservation = publication_for_thread.reserve(|_| Ok(1)).unwrap();
        flag.store(true, Ordering::SeqCst);
        reservation
    });

    // Give the second thread time to park on the condvar.
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(!second_done.load(Ordering::SeqCst));

    // Resolving the first reservation unblocks the second.
    publication.commit(owner_for(5, 1, 1)).unwrap();
    let second = handle.join().unwrap();
    assert!(second_done.load(Ordering::SeqCst));
    // The second reservation observed the committed owner from the first.
    assert_eq!(
        second.current.map(|owner| owner.chunk_id),
        Some(ChunkId::from_seed(1))
    );
}

#[test]
fn begin_delete_blocks_publication_and_clears_on_success() {
    let owner = owner_for(5, 1, 1);
    let publication = SlotPublication::new(SlotPublicationState {
        current: Some(owner),
        busy: false,
    });
    let observed = publication.begin_delete().unwrap();
    assert_eq!(observed, Some(owner));
    // Successful delete clears the owner.
    publication.finish_delete(true);
    assert_eq!(publication.current(), None);

    // A failed delete leaves the owner intact.
    let publication = SlotPublication::new(SlotPublicationState {
        current: Some(owner),
        busy: false,
    });
    let _ = publication.begin_delete().unwrap();
    publication.finish_delete(false);
    assert_eq!(publication.current(), Some(owner));
}
