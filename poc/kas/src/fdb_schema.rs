// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

#![allow(dead_code)]

const KEYSPACE_NAMESPACE: &[u8; 4] = b"KAS1";
const KEYSPACE_VERSION: u8 = 1;
const PREFIX_TARGET: u8 = 1;
const PREFIX_RESERVATION: u8 = 2;
const PREFIX_SERVICE_INSTANCE: u8 = 3;
const PREFIX_COORDINATION_LEASE: u8 = 4;
const PREFIX_RESERVATION_BIN: u8 = 5;
const PREFIX_ALLOCATOR_STATE: u8 = 6;
const PREFIX_TARGET_SPAN: u8 = 7;

pub(crate) fn target_key(target_id: &str) -> Vec<u8> {
    encode_key(PREFIX_TARGET, &[target_id])
}

pub(crate) fn target_prefix() -> Vec<u8> {
    encode_key(PREFIX_TARGET, &[])
}

/// Key for one chunk of a target's free-span list. Free spans are stored across
/// many small chunk values rather than inline in the target record so a target
/// whose spans fragment under churn never produces a single FDB value above the
/// 100 KB value limit (FdbError 2103). `chunk_index` is zero-padded so the keys
/// also sort numerically, though the loader sorts on the in-value index anyway.
pub(crate) fn target_span_chunk_key(target_id: &str, chunk_index: usize) -> Vec<u8> {
    encode_key(PREFIX_TARGET_SPAN, &[target_id, &format!("{chunk_index:010}")])
}

/// Prefix covering every span chunk for a single target (used to clear a
/// target's spans before rewriting them).
pub(crate) fn target_span_prefix(target_id: &str) -> Vec<u8> {
    encode_key(PREFIX_TARGET_SPAN, &[target_id])
}

/// Prefix covering every target's span chunks (used to bulk-load all spans).
pub(crate) fn target_span_all_prefix() -> Vec<u8> {
    encode_key(PREFIX_TARGET_SPAN, &[])
}

pub(crate) fn reservation_key(reservation_id: &str) -> Vec<u8> {
    encode_key(PREFIX_RESERVATION, &[reservation_id])
}

pub(crate) fn reservation_prefix() -> Vec<u8> {
    encode_key(PREFIX_RESERVATION, &[])
}

pub(crate) fn service_instance_key(instance_id: &str) -> Vec<u8> {
    encode_key(PREFIX_SERVICE_INSTANCE, &[instance_id])
}

pub(crate) fn service_instance_prefix() -> Vec<u8> {
    encode_key(PREFIX_SERVICE_INSTANCE, &[])
}

pub(crate) fn coordination_lease_key(lease_name: &str) -> Vec<u8> {
    encode_key(PREFIX_COORDINATION_LEASE, &[lease_name])
}

/// Base name for the allocator mutation coordination lease before per-shard
/// suffixing. Kept as a constant so the lease name derivation has a single
/// source of truth shared by the store and its tests.
pub(crate) const ALLOCATOR_MUTATION_LEASE_BASE: &str = "kas-allocator-mutation";

/// Derives the coordination lease name for the allocator mutation lock,
/// suffixed by `allocation_shard_id` so disjoint shards coordinate
/// independently. Sharded instances do not contend on a single cluster-wide
/// lease; unsharded instances fall back to the historical global name.
pub(crate) fn allocator_mutation_lease_name(allocation_shard_id: Option<&str>) -> String {
    match allocation_shard_id.map(str::trim).filter(|value| !value.is_empty()) {
        Some(shard) => format!("{ALLOCATOR_MUTATION_LEASE_BASE}/{shard}"),
        None => ALLOCATOR_MUTATION_LEASE_BASE.to_string(),
    }
}

/// Key for the allocator state stamp, suffixed by `allocation_shard_id` so a
/// mutation in one shard does not invalidate replica caches for other shards.
/// Unsharded instances keep the historical single-segment "stamp" key.
pub(crate) fn allocator_state_stamp_key(allocation_shard_id: Option<&str>) -> Vec<u8> {
    match allocation_shard_id.map(str::trim).filter(|value| !value.is_empty()) {
        Some(shard) => encode_key(PREFIX_ALLOCATOR_STATE, &["stamp", shard]),
        None => encode_key(PREFIX_ALLOCATOR_STATE, &["stamp"]),
    }
}

pub(crate) fn reservation_bin_member_key(bin_key: &str, reservation_id: &str) -> Vec<u8> {
    encode_key(PREFIX_RESERVATION_BIN, &[bin_key, reservation_id])
}

pub(crate) fn reservation_bin_prefix(bin_key: &str) -> Vec<u8> {
    encode_key(PREFIX_RESERVATION_BIN, &[bin_key])
}

fn encode_key(prefix: u8, segments: &[&str]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(
        KEYSPACE_NAMESPACE.len() + 2 + segments.iter().map(|value| value.len() + 4).sum::<usize>(),
    );
    encoded.extend_from_slice(KEYSPACE_NAMESPACE);
    encoded.push(KEYSPACE_VERSION);
    encoded.push(prefix);
    for segment in segments {
        encoded.extend_from_slice(&(segment.len() as u32).to_be_bytes());
        encoded.extend_from_slice(segment.as_bytes());
    }
    encoded
}

pub(crate) fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return end;
        }
    }
    let mut end = prefix.to_vec();
    end.push(0);
    end
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_name_is_global_when_unsharded() {
        assert_eq!(
            allocator_mutation_lease_name(None),
            ALLOCATOR_MUTATION_LEASE_BASE
        );
        // Empty / whitespace-only shard ids behave like the unsharded case so
        // we never produce a dangling "kas-allocator-mutation/" suffix.
        assert_eq!(
            allocator_mutation_lease_name(Some("")),
            ALLOCATOR_MUTATION_LEASE_BASE
        );
        assert_eq!(
            allocator_mutation_lease_name(Some("  ")),
            ALLOCATOR_MUTATION_LEASE_BASE
        );
    }

    #[test]
    fn lease_name_is_suffixed_per_shard() {
        assert_eq!(
            allocator_mutation_lease_name(Some("shard-a")),
            "kas-allocator-mutation/shard-a"
        );
        assert_ne!(
            allocator_mutation_lease_name(Some("shard-a")),
            allocator_mutation_lease_name(Some("shard-b"))
        );
        // Whitespace around the shard id is trimmed before suffixing.
        assert_eq!(
            allocator_mutation_lease_name(Some(" shard-a ")),
            "kas-allocator-mutation/shard-a"
        );
    }

    #[test]
    fn stamp_key_is_disjoint_per_shard() {
        let unsharded = allocator_state_stamp_key(None);
        let shard_a = allocator_state_stamp_key(Some("shard-a"));
        let shard_b = allocator_state_stamp_key(Some("shard-b"));

        // Distinct shards must map to distinct keys so a mutation in one shard
        // cannot invalidate the cached stamp of another.
        assert_ne!(shard_a, shard_b);
        // The sharded keys must also differ from the legacy global key.
        assert_ne!(shard_a, unsharded);
        assert_ne!(shard_b, unsharded);

        // All stamp keys share the allocator-state prefix/namespace framing.
        let prefix = encode_key(PREFIX_ALLOCATOR_STATE, &[]);
        assert!(shard_a.starts_with(&prefix));
        assert!(unsharded.starts_with(&prefix));
    }

    #[test]
    fn stamp_key_unsharded_matches_legacy_layout() {
        // Guard against accidentally changing the on-wire key for the
        // unsharded (legacy) path.
        assert_eq!(
            allocator_state_stamp_key(None),
            encode_key(PREFIX_ALLOCATOR_STATE, &["stamp"])
        );
        // Empty / whitespace shard ids resolve to the legacy key.
        assert_eq!(
            allocator_state_stamp_key(Some("")),
            allocator_state_stamp_key(None)
        );
        assert_eq!(
            allocator_state_stamp_key(Some("   ")),
            allocator_state_stamp_key(None)
        );
    }

    #[test]
    fn target_span_chunk_keys_are_grouped_and_disjoint() {
        let a0 = target_span_chunk_key("epyc-target-00", 0);
        let a1 = target_span_chunk_key("epyc-target-00", 1);
        let b0 = target_span_chunk_key("epyc-target-01", 0);

        // Distinct (target, chunk) pairs map to distinct keys.
        assert_ne!(a0, a1);
        assert_ne!(a0, b0);

        // A target's chunks all sit under its own span prefix, and every span
        // chunk sits under the global span prefix used by the bulk loader.
        let target_prefix0 = target_span_prefix("epyc-target-00");
        assert!(a0.starts_with(&target_prefix0));
        assert!(a1.starts_with(&target_prefix0));
        assert!(!b0.starts_with(&target_prefix0));
        let all = target_span_all_prefix();
        assert!(a0.starts_with(&all));
        assert!(b0.starts_with(&all));

        // Span chunks must live in a different keyspace than target metadata so
        // a `target_prefix()` scan never decodes a span chunk as a TargetRecord.
        assert!(!a0.starts_with(&target_prefix()));
        assert!(!target_key("epyc-target-00").starts_with(&target_span_all_prefix()));
    }
}
