// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use super::*;

fn profile() -> EcProfile {
    EcProfile::single_stripe_8_plus_2(
        "resilient",
        "isa-l-reed-solomon",
        FailureDomain::DriveDomainLab,
    )
}

fn payload() -> Vec<u8> {
    let len = DEFAULT_FRAGMENT_BYTES * 2 + 37;
    (0..len).map(|i| (i as u8).wrapping_mul(17)).collect()
}

#[test]
fn validates_single_stripe_profile() {
    let profile = profile();
    profile.validate_single_stripe().unwrap();
}

#[test]
fn accepts_smaller_power_of_two_fragment_sizes() {
    let profile = EcProfile {
        fragment_bytes: 128 * 1024,
        ..profile()
    };
    profile.validate_single_stripe().unwrap();
    let encoded = encode(
        &profile,
        &payload()[..profile.max_object_bytes().min(payload().len())],
    )
    .unwrap();
    assert!(verify(&profile, &encoded).unwrap());
}

#[test]
fn rejects_wrong_geometry() {
    let bad = EcProfile {
        data_fragments: 4,
        parity_fragments: 2,
        ..profile()
    };
    assert!(bad.validate_single_stripe().is_err());
}

#[test]
fn rejects_oversized_payload() {
    let profile = profile();
    let payload = vec![0_u8; profile.max_object_bytes() + 1];
    let error = encode(&profile, &payload).unwrap_err();
    assert!(matches!(error, KeeError::PayloadTooLarge { .. }));
}

#[test]
fn software_roundtrip_encode_verify() {
    let profile = profile();
    let encoded = encode(&profile, &payload()).unwrap();
    assert_eq!(encoded.len(), TOTAL_FRAGMENTS);
    assert!(verify(&profile, &encoded).unwrap());
}

#[test]
fn prepared_plan_encode_into_reuses_buffers() {
    let profile = profile();
    let engine = KeeEngine::new(profile.clone()).unwrap();
    let plan = engine.prepared_plan().unwrap();
    let mut shards = plan.allocate_output_buffers();
    let payload = payload();
    plan.encode_into(&payload, &mut shards).unwrap();
    assert_eq!(shards.len(), TOTAL_FRAGMENTS);
    assert!(verify(&profile, &shards).unwrap());

    for shard in &mut shards {
        shard.fill(0xAA);
    }
    plan.encode_into(&payload, &mut shards).unwrap();
    assert!(verify(&profile, &shards).unwrap());
}

#[test]
fn reconstructs_missing_data_and_parity_fragments() {
    let profile = profile();
    let encoded = encode(&profile, &payload()).unwrap();
    let mut fragments = encoded
        .into_iter()
        .enumerate()
        .map(|(index, fragment)| match index {
            1 | 9 => None,
            _ => Some(fragment),
        })
        .collect::<Vec<_>>();

    let reconstructed = reconstruct(&profile, &mut fragments).unwrap();
    assert_eq!(reconstructed.len(), TOTAL_FRAGMENTS);
    assert!(verify(&profile, &reconstructed).unwrap());
}

#[test]
fn prepared_plan_reconstructs_missing_fragments() {
    let profile = profile();
    let engine = KeeEngine::new(profile.clone()).unwrap();
    let plan = engine.prepared_plan().unwrap();
    let encoded = plan.encode(&payload()).unwrap();
    let mut fragments = encoded
        .into_iter()
        .enumerate()
        .map(|(index, fragment)| match index {
            2 | 8 => None,
            _ => Some(fragment),
        })
        .collect::<Vec<_>>();

    let reconstructed = plan.reconstruct(&mut fragments).unwrap();
    assert_eq!(reconstructed.len(), TOTAL_FRAGMENTS);
    assert!(verify(&profile, &reconstructed).unwrap());
}

#[test]
fn backend_inventory_is_actionable() {
    let inventory = hardware_inventory();
    let alias = backend_inventory();
    assert_eq!(inventory.cpu_arch, std::env::consts::ARCH);
    assert_eq!(inventory.primary_backend, BackendKind::IsaL);
    assert_eq!(inventory.fallback_backend, BackendKind::Software);
    assert_eq!(inventory, alias);
    if cfg!(feature = "isa-l-backend") {
        assert_eq!(inventory.selected_backend, BackendKind::IsaL);
        assert!(inventory.isa_l_feature_enabled);
    } else {
        assert_eq!(inventory.selected_backend, BackendKind::Software);
        assert!(!inventory.isa_l_feature_enabled);
    }
}
