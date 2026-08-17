// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

#![allow(dead_code)]

use std::io;

const KEYSPACE_VERSION: u8 = 1;
const PREFIX_BUCKET_CONTEXT: u8 = 1;
const PREFIX_WRITE_INTENT: u8 = 2;
const PREFIX_OBJECT_HEAD: u8 = 3;
const PREFIX_OBJECT_VERSION: u8 = 4;
const PREFIX_WRITE_INTENT_CHUNK: u8 = 5;
const PREFIX_OBJECT_VERSION_CHUNK: u8 = 6;
const PREFIX_NAMESPACE: u8 = 7;
const PREFIX_NAMESPACE_ENTRY: u8 = 8;
const PREFIX_EC_PROFILE: u8 = 9;
const PREFIX_BUCKET_RECORD: u8 = 10;
const PREFIX_PLACEMENT_TASK: u8 = 11;
const PREFIX_TARGET_CURRENT_FRAGMENT: u8 = 12;
const PREFIX_MAINTENANCE_MARKER: u8 = 13;
const PREFIX_NAMESPACE_PATH: u8 = 14;
const PREFIX_OBJECT_ID_COUNTER: u8 = 15;
const PREFIX_TARGET_REVERSE_LOG: u8 = 16;
const PREFIX_OBJECT_LEASE: u8 = 17;
const PREFIX_CLUSTER_CONFIG: u8 = 18;
const PREFIX_TARGET_INVENTORY: u8 = 19;
/// Pending-version markers + per-target presence rows for in-flight decentralized
/// writes. A `writing` marker exists if and only if its version may still commit; a
/// live head's `current_version_id` never has a marker (the commit clears the marker
/// subspace atomically with the head flip). The reaper cannot re-verify that invariant
/// for rows lacking bucket/key context, so the set of writers of this prefix is closed:
/// the write-begin mint, the segment-append expiry push + presence sets, the commit's
/// subspace clear, the reaper's claim/clean/finish, and the quiescent backfill
/// (mint-if-absent only). Nothing else may touch it.
const PREFIX_PENDING_VERSION: u8 = 20;

/// Per-version reclaim fence: set by the granule GC's authorize path in the same
/// transaction as any reclaim tombstone it stamps for the version, read (required
/// absent) by marker-fenced commits and segment appends. Deliberately its own
/// prefix, NOT under `PREFIX_PENDING_VERSION` — the commit clear_ranges the
/// pending-version subspace, and the fence must outlive the marker so a stalled
/// writer's late commit still sees that GC acted. Value: stamped_at_unix_ms (u64 BE).
const PREFIX_VERSION_RECLAIM_FENCE: u8 = 21;

pub(crate) fn bucket_context_key(bucket_id: &str) -> Vec<u8> {
    encode_key(PREFIX_BUCKET_CONTEXT, &[bucket_id])
}

/// Singleton key for the globally-monotonic object_id counter.
pub(crate) fn object_id_counter_key() -> Vec<u8> {
    encode_key(PREFIX_OBJECT_ID_COUNTER, &["object-id"])
}

/// Per-object write lease. Issued when a write begins (keyed by the minted object_id),
/// cleared when the object commits, and reaped once expired. It bounds the window in
/// which an in-flight write's granules are protected from reclamation.
pub(crate) fn object_lease_key(object_id: u32) -> Vec<u8> {
    encode_key(PREFIX_OBJECT_LEASE, &[&format!("{object_id:08x}")])
}

/// Prefix + range covering every write-lease row (used by the reaper to scan for
/// expired leases).
pub(crate) fn object_lease_range() -> (Vec<u8>, Vec<u8>) {
    let prefix = encode_key(PREFIX_OBJECT_LEASE, &[]);
    let end = prefix_end(&prefix);
    (prefix, end)
}

/// Per-target inventory record (the roster KMS owns now that KAS is removed). Keyed by
/// target_id; the value is a prost-encoded TargetRecord. GetClusterConfig + the
/// lease-fenced GC read this roster.
pub(crate) fn target_inventory_key(target_id: &str) -> Vec<u8> {
    encode_key(PREFIX_TARGET_INVENTORY, &[target_id])
}

/// Prefix + range covering every target-inventory row (used to list the whole roster).
pub(crate) fn target_inventory_range() -> (Vec<u8>, Vec<u8>) {
    let prefix = encode_key(PREFIX_TARGET_INVENTORY, &[]);
    let end = prefix_end(&prefix);
    (prefix, end)
}

/// Singleton key for the per-cluster salt folded into computed chunk ids and
/// placement weights. Minted once and never rewritten.
pub(crate) fn cluster_salt_key() -> Vec<u8> {
    encode_key(PREFIX_CLUSTER_CONFIG, &["chunk-id-salt"])
}

pub(crate) fn write_intent_key(intent_id: &str) -> Vec<u8> {
    encode_key(PREFIX_WRITE_INTENT, &[intent_id])
}

pub(crate) fn write_intent_range() -> (Vec<u8>, Vec<u8>) {
    (
        vec![KEYSPACE_VERSION, PREFIX_WRITE_INTENT],
        vec![KEYSPACE_VERSION, PREFIX_WRITE_INTENT + 1],
    )
}

pub(crate) fn write_intent_chunk_prefix(intent_id: &str) -> Vec<u8> {
    encode_key(PREFIX_WRITE_INTENT_CHUNK, &[intent_id])
}

pub(crate) fn write_intent_chunk_key(intent_id: &str, chunk_index: u32) -> Vec<u8> {
    encode_key(
        PREFIX_WRITE_INTENT_CHUNK,
        &[intent_id, &format!("{chunk_index:08x}")],
    )
}

pub(crate) fn object_head_key(bucket_id: &str, key_path: &str) -> Vec<u8> {
    encode_key(PREFIX_OBJECT_HEAD, &[bucket_id, key_path])
}

pub(crate) fn object_version_key(version_id: &str) -> Vec<u8> {
    encode_key(PREFIX_OBJECT_VERSION, &[version_id])
}

pub(crate) fn object_version_chunk_prefix(version_id: &str) -> Vec<u8> {
    encode_key(PREFIX_OBJECT_VERSION_CHUNK, &[version_id])
}

pub(crate) fn object_version_chunk_key(version_id: &str, chunk_index: u32) -> Vec<u8> {
    encode_key(
        PREFIX_OBJECT_VERSION_CHUNK,
        &[version_id, &format!("{chunk_index:08x}")],
    )
}

pub(crate) fn object_head_prefix(bucket_id: &str) -> Vec<u8> {
    encode_key(PREFIX_OBJECT_HEAD, &[bucket_id])
}

pub(crate) fn namespace_key(namespace_id: &str) -> Vec<u8> {
    encode_key(PREFIX_NAMESPACE, &[namespace_id])
}

pub(crate) fn namespace_entry_key(namespace_id: &str, entry_id: &str) -> Vec<u8> {
    encode_key(PREFIX_NAMESPACE_ENTRY, &[namespace_id, entry_id])
}

pub(crate) fn namespace_entry_prefix(namespace_id: &str) -> Vec<u8> {
    encode_key(PREFIX_NAMESPACE_ENTRY, &[namespace_id])
}

/// Secondary index mapping a namespace entry's hierarchy path to its
/// `entry_id`. Lets the write-intent parent lookup resolve a parent path with a
/// single point `get()` instead of a full-namespace range scan (which would
/// otherwise pull the whole namespace into the transaction read-conflict set).
///
/// This index MUST be written/cleared in the same transaction as the owning
/// `namespace_entry_key` so the two never drift; see `namespace_entry_key`
/// mutation sites in `store.rs` and `fdb_hot_store.rs`.
pub(crate) fn namespace_path_key(namespace_id: &str, path: &str) -> Vec<u8> {
    encode_key(PREFIX_NAMESPACE_PATH, &[namespace_id, path])
}

pub(crate) fn ec_profile_key(profile_id: &str) -> Vec<u8> {
    encode_key(PREFIX_EC_PROFILE, &[profile_id])
}

pub(crate) fn ec_profile_range() -> (Vec<u8>, Vec<u8>) {
    (
        vec![KEYSPACE_VERSION, PREFIX_EC_PROFILE],
        vec![KEYSPACE_VERSION, PREFIX_EC_PROFILE + 1],
    )
}

pub(crate) fn bucket_record_key(bucket_id: &str) -> Vec<u8> {
    encode_key(PREFIX_BUCKET_RECORD, &[bucket_id])
}

pub(crate) fn object_head_range() -> (Vec<u8>, Vec<u8>) {
    (
        vec![KEYSPACE_VERSION, PREFIX_OBJECT_HEAD],
        vec![KEYSPACE_VERSION, PREFIX_OBJECT_HEAD + 1],
    )
}

pub(crate) fn placement_task_key(task_id: &str) -> Vec<u8> {
    encode_key(PREFIX_PLACEMENT_TASK, &[task_id])
}

pub(crate) fn placement_task_range() -> (Vec<u8>, Vec<u8>) {
    (
        vec![KEYSPACE_VERSION, PREFIX_PLACEMENT_TASK],
        vec![KEYSPACE_VERSION, PREFIX_PLACEMENT_TASK + 1],
    )
}

pub(crate) fn target_current_fragment_key(
    target_id: &str,
    version_id: &str,
    stripe_index: u32,
    fragment_index: u32,
) -> Vec<u8> {
    encode_key(
        PREFIX_TARGET_CURRENT_FRAGMENT,
        &[
            target_id,
            version_id,
            &format!("{stripe_index:08x}"),
            &format!("{fragment_index:08x}"),
        ],
    )
}

pub(crate) fn target_current_fragment_prefix(target_id: &str) -> Vec<u8> {
    encode_key(PREFIX_TARGET_CURRENT_FRAGMENT, &[target_id])
}

pub(crate) fn target_current_fragment_range(target_id: &str) -> (Vec<u8>, Vec<u8>) {
    let prefix = target_current_fragment_prefix(target_id);
    let end = prefix_end(&prefix);
    (prefix, end)
}

/// Append-only per-target reverse log: (target_id) -> {generation, granule_index},
/// keyed by version_id so it is idempotent under FDB transaction retry and
/// range-scannable by target_id for rebuild/GC reverse lookup. Mirrors
/// target_current_fragment_key. The value (generation, granule_index) is encoded by
/// encode_reverse_log_value; object_id is deliberately absent (GC matches on the
/// version_id carried in the key).
pub(crate) fn target_reverse_log_key(
    target_id: &str,
    version_id: &str,
    stripe_index: u32,
    fragment_index: u32,
) -> Vec<u8> {
    encode_key(
        PREFIX_TARGET_REVERSE_LOG,
        &[
            target_id,
            version_id,
            &format!("{stripe_index:08x}"),
            &format!("{fragment_index:08x}"),
        ],
    )
}

pub(crate) fn target_reverse_log_prefix(target_id: &str) -> Vec<u8> {
    encode_key(PREFIX_TARGET_REVERSE_LOG, &[target_id])
}

pub(crate) fn target_reverse_log_range(target_id: &str) -> (Vec<u8>, Vec<u8>) {
    let prefix = target_reverse_log_prefix(target_id);
    let end = prefix_end(&prefix);
    (prefix, end)
}

pub(crate) fn maintenance_marker_key(marker_id: &str) -> Vec<u8> {
    encode_key(PREFIX_MAINTENANCE_MARKER, &[marker_id])
}

/// Shard segment for a pending-version key: two hex digits from an FNV-1a-64 hash of
/// the `%08x`-formatted object_id. Object ids are minted monotonically, so keying by
/// object_id alone would append every new marker to one hot range tail; the hash
/// spreads concurrent writers across 256 subranges. Deterministic, so every reader
/// and writer recomputes it from the object_id.
pub(crate) fn pending_version_shard(object_id: u32) -> String {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let formatted = format!("{object_id:08x}");
    let mut hash = FNV_OFFSET_BASIS;
    for byte in formatted.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{:02x}", hash & 0xff)
}

/// Pending-version marker row for one minted (object_id, object_version). Segments are
/// (shard, object_id, object_version), all fixed-width hex.
pub(crate) fn pending_version_key(object_id: u32, object_version: u32) -> Vec<u8> {
    encode_key(
        PREFIX_PENDING_VERSION,
        &[
            &pending_version_shard(object_id),
            &format!("{object_id:08x}"),
            &format!("{object_version:08x}"),
        ],
    )
}

/// Presence row recording that `target_id` holds reverse-log rows for this pending
/// version. Written blind in the same transaction as the target's reverse-log rows, so
/// presence can never under-cover rows. The marker key is a strict byte-prefix of its
/// presence rows, so the marker sorts first within the version's subspace.
pub(crate) fn pending_version_target_key(
    object_id: u32,
    object_version: u32,
    target_id: &str,
) -> Vec<u8> {
    let mut key = pending_version_key(object_id, object_version);
    push_segment(&mut key, target_id);
    key
}

/// The marker plus every presence row of one pending version.
pub(crate) fn pending_version_subspace_range(
    object_id: u32,
    object_version: u32,
) -> (Vec<u8>, Vec<u8>) {
    let marker = pending_version_key(object_id, object_version);
    let end = prefix_end(&marker);
    (marker, end)
}

/// Every pending version of one object (used by lease renewal, which is keyed by
/// object_id: one object_id maps to exactly one minted version).
pub(crate) fn pending_version_object_range(object_id: u32) -> (Vec<u8>, Vec<u8>) {
    let prefix = encode_key(
        PREFIX_PENDING_VERSION,
        &[
            &pending_version_shard(object_id),
            &format!("{object_id:08x}"),
        ],
    );
    let end = prefix_end(&prefix);
    (prefix, end)
}

/// The whole pending-version keyspace, across all shards (reaper discovery scan).
pub(crate) fn pending_version_scan_range() -> (Vec<u8>, Vec<u8>) {
    let prefix = encode_key(PREFIX_PENDING_VERSION, &[]);
    let end = prefix_end(&prefix);
    (prefix, end)
}

/// Reclaim-fence row for one (object_id, object_version). Same shard spreading as
/// the pending-version marker so the two rendezvous keys of a version live in
/// matching subranges.
pub(crate) fn version_reclaim_fence_key(object_id: u32, object_version: u32) -> Vec<u8> {
    encode_key(
        PREFIX_VERSION_RECLAIM_FENCE,
        &[
            &pending_version_shard(object_id),
            &format!("{object_id:08x}"),
            &format!("{object_version:08x}"),
        ],
    )
}

/// The whole reclaim-fence keyspace (janitor scan).
pub(crate) fn version_reclaim_fence_scan_range() -> (Vec<u8>, Vec<u8>) {
    let prefix = encode_key(PREFIX_VERSION_RECLAIM_FENCE, &[]);
    let end = prefix_end(&prefix);
    (prefix, end)
}

/// The whole reverse-log keyspace, across all targets (the fence backfill scan).
pub(crate) fn target_reverse_log_scan_range() -> (Vec<u8>, Vec<u8>) {
    let prefix = encode_key(PREFIX_TARGET_REVERSE_LOG, &[]);
    let end = prefix_end(&prefix);
    (prefix, end)
}

/// Decodes the version_id segment out of a reverse-log row key (fence backfill).
pub(crate) fn decode_reverse_log_key_version_id(key: &[u8]) -> Result<String, String> {
    let (prefix, segments) = decode_segments(key).map_err(|err| err.to_string())?;
    if prefix != PREFIX_TARGET_REVERSE_LOG {
        return Err(format!(
            "reverse-log key has prefix {prefix}, expected {PREFIX_TARGET_REVERSE_LOG}"
        ));
    }
    if segments.len() != 4 {
        return Err(format!(
            "reverse-log key has {} segments, expected 4",
            segments.len()
        ));
    }
    Ok(segments[1].clone())
}

/// Decodes (object_id, object_version) back out of a reclaim-fence key (janitor).
pub(crate) fn decode_version_reclaim_fence_key_parts(key: &[u8]) -> Result<(u32, u32), String> {
    let (prefix, segments) = decode_segments(key).map_err(|err| err.to_string())?;
    if prefix != PREFIX_VERSION_RECLAIM_FENCE {
        return Err(format!(
            "reclaim-fence key has prefix {prefix}, expected {PREFIX_VERSION_RECLAIM_FENCE}"
        ));
    }
    if segments.len() != 3 {
        return Err(format!(
            "reclaim-fence key has {} segments, expected 3",
            segments.len()
        ));
    }
    let object_id = u32::from_str_radix(&segments[1], 16)
        .map_err(|err| format!("reclaim-fence object_id segment: {err}"))?;
    let object_version = u32::from_str_radix(&segments[2], 16)
        .map_err(|err| format!("reclaim-fence object_version segment: {err}"))?;
    Ok((object_id, object_version))
}

/// Parses a pending-version marker or presence key back into
/// `(object_id, object_version, presence target_id)`. Marker keys carry three segments
/// (shard, object_id, object_version); presence rows carry the target_id as a fourth.
pub(crate) fn decode_pending_version_key_parts(
    key: &[u8],
) -> Result<(u32, u32, Option<String>), io::Error> {
    let (prefix, segments) = decode_segments(key)?;
    if prefix != PREFIX_PENDING_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("key prefix {prefix} is not the pending-version prefix"),
        ));
    }
    if segments.len() != 3 && segments.len() != 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pending-version key has {} segments, expected 3 (marker) or 4 (presence)",
                segments.len()
            ),
        ));
    }
    let object_id = u32::from_str_radix(&segments[1], 16).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("pending-version key object_id segment is not hex: {err}"),
        )
    })?;
    let object_version = u32::from_str_radix(&segments[2], 16).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("pending-version key object_version segment is not hex: {err}"),
        )
    })?;
    if segments[0] != pending_version_shard(object_id) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pending-version key shard segment does not match its object_id",
        ));
    }
    Ok((object_id, object_version, segments.get(3).cloned()))
}

/// Every reverse-log row one target holds for one version. Length-prefixed segments
/// guarantee version "7:1" never bleeds into "7:10" or "71:1".
pub(crate) fn target_reverse_log_version_range(
    target_id: &str,
    version_id: &str,
) -> (Vec<u8>, Vec<u8>) {
    let prefix = encode_key(PREFIX_TARGET_REVERSE_LOG, &[target_id, version_id]);
    let end = prefix_end(&prefix);
    (prefix, end)
}

/// Every occupancy-index row one target holds for one version.
pub(crate) fn target_current_fragment_version_range(
    target_id: &str,
    version_id: &str,
) -> (Vec<u8>, Vec<u8>) {
    let prefix = encode_key(PREFIX_TARGET_CURRENT_FRAGMENT, &[target_id, version_id]);
    let end = prefix_end(&prefix);
    (prefix, end)
}

/// The occupancy-index key paired with a reverse-log key. The two keyspaces share one
/// segment layout (target_id, version_id, stripe, fragment) and differ only in the
/// prefix byte, so the pairing is a one-byte rewrite; a unit test pins the layouts to
/// each other.
pub(crate) fn occupancy_key_for_reverse_log_key(rl_key: &[u8]) -> Vec<u8> {
    debug_assert!(rl_key.len() >= 2 && rl_key[1] == PREFIX_TARGET_REVERSE_LOG);
    let mut key = rl_key.to_vec();
    key[1] = PREFIX_TARGET_CURRENT_FRAGMENT;
    key
}

pub(crate) fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != 0xff {
            end[index] += 1;
            end.truncate(index + 1);
            return end;
        }
    }
    let mut unbounded = prefix.to_vec();
    unbounded.push(0);
    unbounded
}

fn encode_key(prefix: u8, segments: &[&str]) -> Vec<u8> {
    let mut encoded =
        Vec::with_capacity(2 + segments.iter().map(|value| value.len() + 4).sum::<usize>());
    encoded.push(KEYSPACE_VERSION);
    encoded.push(prefix);
    for segment in segments {
        push_segment(&mut encoded, segment);
    }
    encoded
}

fn push_segment(target: &mut Vec<u8>, value: &str) {
    let length = u32::try_from(value.len()).expect("segment length exceeds u32");
    target.extend_from_slice(&length.to_be_bytes());
    target.extend_from_slice(value.as_bytes());
}

#[allow(dead_code)]
pub(crate) fn decode_segments(encoded: &[u8]) -> Result<(u8, Vec<String>), io::Error> {
    if encoded.len() < 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "encoded key is too short",
        ));
    }
    if encoded[0] != KEYSPACE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported keyspace version {}", encoded[0]),
        ));
    }
    let prefix = encoded[1];
    let mut offset = 2usize;
    let mut segments = Vec::new();
    while offset < encoded.len() {
        if offset + 4 > encoded.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated segment length",
            ));
        }
        let length = u32::from_be_bytes([
            encoded[offset],
            encoded[offset + 1],
            encoded[offset + 2],
            encoded[offset + 3],
        ]) as usize;
        offset += 4;
        let end = offset.saturating_add(length);
        if end > encoded.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated segment payload",
            ));
        }
        let segment = std::str::from_utf8(&encoded[offset..end]).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid utf-8 segment: {err}"),
            )
        })?;
        segments.push(segment.to_string());
        offset = end;
    }
    Ok((prefix, segments))
}

#[cfg(test)]
mod tests {
    use super::{
        bucket_context_key, decode_pending_version_key_parts, decode_segments,
        namespace_entry_key, namespace_path_key, object_head_key, object_head_prefix,
        object_version_chunk_key, object_version_chunk_prefix, object_version_key,
        occupancy_key_for_reverse_log_key, pending_version_key, pending_version_object_range,
        pending_version_scan_range, pending_version_shard, pending_version_subspace_range,
        pending_version_target_key, target_current_fragment_key,
        target_current_fragment_version_range, target_reverse_log_key,
        target_reverse_log_version_range, write_intent_chunk_key, write_intent_chunk_prefix,
        write_intent_key, PREFIX_BUCKET_CONTEXT, PREFIX_NAMESPACE_PATH, PREFIX_OBJECT_HEAD,
        PREFIX_OBJECT_VERSION, PREFIX_OBJECT_VERSION_CHUNK, PREFIX_TARGET_CURRENT_FRAGMENT,
        PREFIX_WRITE_INTENT, PREFIX_WRITE_INTENT_CHUNK,
    };

    #[test]
    fn keys_round_trip_segments() {
        let cases = [
            (
                bucket_context_key("bucket-a"),
                PREFIX_BUCKET_CONTEXT,
                vec!["bucket-a"],
            ),
            (
                write_intent_key("intent-1"),
                PREFIX_WRITE_INTENT,
                vec!["intent-1"],
            ),
            (
                object_head_key("bucket-a", "dir/object.bin"),
                PREFIX_OBJECT_HEAD,
                vec!["bucket-a", "dir/object.bin"],
            ),
            (
                object_version_key("version-9"),
                PREFIX_OBJECT_VERSION,
                vec!["version-9"],
            ),
            (
                write_intent_chunk_key("intent-1", 7),
                PREFIX_WRITE_INTENT_CHUNK,
                vec!["intent-1", "00000007"],
            ),
            (
                object_version_chunk_key("version-9", 3),
                PREFIX_OBJECT_VERSION_CHUNK,
                vec!["version-9", "00000003"],
            ),
            (
                target_current_fragment_key("target-a", "version-9", 3, 7),
                PREFIX_TARGET_CURRENT_FRAGMENT,
                vec!["target-a", "version-9", "00000003", "00000007"],
            ),
        ];

        for (encoded, prefix, expected_segments) in cases {
            let (decoded_prefix, decoded_segments) = decode_segments(&encoded).unwrap();
            assert_eq!(decoded_prefix, prefix);
            assert_eq!(
                decoded_segments,
                expected_segments
                    .into_iter()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn object_head_prefix_is_prefix_of_object_head_keys() {
        let prefix = object_head_prefix("bucket-a");
        let object_key = object_head_key("bucket-a", "dir/object.bin");
        assert!(object_key.starts_with(&prefix));
    }

    #[test]
    fn chunk_prefixes_are_prefixes_of_chunk_keys() {
        assert!(write_intent_chunk_key("intent-1", 2)
            .starts_with(&write_intent_chunk_prefix("intent-1")));
        assert!(object_version_chunk_key("version-9", 4)
            .starts_with(&object_version_chunk_prefix("version-9")));
    }

    #[test]
    fn different_key_classes_do_not_collide() {
        assert_ne!(bucket_context_key("x"), write_intent_key("x"));
        assert_ne!(write_intent_key("x"), object_version_key("x"));
        assert_ne!(bucket_context_key("x"), object_head_key("x", "x"));
        assert_ne!(write_intent_key("x"), write_intent_chunk_key("x", 0));
        assert_ne!(object_version_key("x"), object_version_chunk_key("x", 0));
        assert_ne!(namespace_entry_key("ns", "x"), namespace_path_key("ns", "x"));
    }

    #[test]
    fn namespace_path_key_round_trips_and_is_deterministic() {
        let key = namespace_path_key("ns-1", "/a/b/c");
        let (prefix, segments) = decode_segments(&key).unwrap();
        assert_eq!(prefix, PREFIX_NAMESPACE_PATH);
        assert_eq!(segments, vec!["ns-1".to_string(), "/a/b/c".to_string()]);
        assert_eq!(key, namespace_path_key("ns-1", "/a/b/c"));
    }

    #[test]
    fn namespace_path_key_distinguishes_namespace_and_path() {
        // Length-prefixed segments prevent ("ns", "a/b") colliding with
        // ("ns/a", "b") or similar boundary ambiguities.
        assert_ne!(
            namespace_path_key("ns", "a/b"),
            namespace_path_key("ns/a", "b")
        );
        assert_ne!(
            namespace_path_key("ns-1", "/a"),
            namespace_path_key("ns-2", "/a")
        );
        assert_ne!(
            namespace_path_key("ns-1", "/a"),
            namespace_path_key("ns-1", "/b")
        );
    }

    #[test]
    fn pending_version_keys_round_trip() {
        let marker = pending_version_key(0x1234_5678, 42);
        let (object_id, object_version, target) =
            decode_pending_version_key_parts(&marker).unwrap();
        assert_eq!(object_id, 0x1234_5678);
        assert_eq!(object_version, 42);
        assert_eq!(target, None);

        let presence = pending_version_target_key(0x1234_5678, 42, "chunky-mcchunkface-07");
        let (object_id, object_version, target) =
            decode_pending_version_key_parts(&presence).unwrap();
        assert_eq!(object_id, 0x1234_5678);
        assert_eq!(object_version, 42);
        assert_eq!(target.as_deref(), Some("chunky-mcchunkface-07"));
    }

    #[test]
    fn pending_version_marker_sorts_first_in_its_subspace() {
        let marker = pending_version_key(7, 1);
        let presence_a = pending_version_target_key(7, 1, "a");
        let presence_b = pending_version_target_key(7, 1, "zz");
        let (begin, end) = pending_version_subspace_range(7, 1);
        for key in [&marker, &presence_a, &presence_b] {
            assert!(key.as_slice() >= begin.as_slice() && key.as_slice() < end.as_slice());
        }
        assert!(presence_a.starts_with(&marker));
        assert!(marker.as_slice() < presence_a.as_slice());
        assert!(presence_a.as_slice() < presence_b.as_slice());
        // Adjacent versions of the same object stay outside the subspace.
        let other_version = pending_version_key(7, 2);
        assert!(other_version.as_slice() >= end.as_slice() || other_version.as_slice() < begin.as_slice());
    }

    #[test]
    fn pending_version_object_range_covers_only_that_object() {
        let (begin, end) = pending_version_object_range(7);
        let mine = pending_version_key(7, 3);
        assert!(mine.as_slice() >= begin.as_slice() && mine.as_slice() < end.as_slice());
        // A different object lands outside (its shard segment usually differs too, but
        // the object_id segment alone must separate them even within one shard).
        let other = pending_version_key(8, 3);
        assert!(!(other.as_slice() >= begin.as_slice() && other.as_slice() < end.as_slice()));
    }

    #[test]
    fn pending_version_scan_range_spans_all_shards() {
        let (begin, end) = pending_version_scan_range();
        // Probe object ids until two distinct shard segments are seen, proving the
        // scan range covers markers regardless of shard placement.
        let mut shards = std::collections::HashSet::new();
        for object_id in 0u32..64 {
            shards.insert(pending_version_shard(object_id));
            let key = pending_version_key(object_id, 1);
            assert!(key.as_slice() >= begin.as_slice() && key.as_slice() < end.as_slice());
        }
        assert!(shards.len() > 1, "expected multiple shard segments");
    }

    #[test]
    fn pending_version_shard_is_deterministic_two_hex_digits() {
        for object_id in [0u32, 1, 0xffff_ffff, 0x1234_5678] {
            let shard = pending_version_shard(object_id);
            assert_eq!(shard, pending_version_shard(object_id));
            assert_eq!(shard.len(), 2);
            assert!(u8::from_str_radix(&shard, 16).is_ok());
        }
    }

    #[test]
    fn decode_pending_version_key_parts_rejects_foreign_and_malformed_keys() {
        assert!(decode_pending_version_key_parts(&object_head_key("b", "k")).is_err());
        assert!(decode_pending_version_key_parts(&[]).is_err());
        // A shard segment inconsistent with the object_id is rejected.
        let mut forged = super::encode_key(
            super::PREFIX_PENDING_VERSION,
            &["zz", "00000007", "00000001"],
        );
        assert!(decode_pending_version_key_parts(&forged).is_err());
        // Truncated to two segments is rejected.
        forged = super::encode_key(super::PREFIX_PENDING_VERSION, &["00", "00000007"]);
        assert!(decode_pending_version_key_parts(&forged).is_err());
    }

    #[test]
    fn reverse_log_version_range_respects_version_boundaries() {
        let inside = target_reverse_log_key("t", "7:1", 0, 0);
        let sibling_longer = target_reverse_log_key("t", "7:10", 0, 0);
        let sibling_wider = target_reverse_log_key("t", "71:1", 0, 0);
        let (begin, end) = target_reverse_log_version_range("t", "7:1");
        assert!(inside.as_slice() >= begin.as_slice() && inside.as_slice() < end.as_slice());
        for outside in [&sibling_longer, &sibling_wider] {
            assert!(
                !(outside.as_slice() >= begin.as_slice() && outside.as_slice() < end.as_slice()),
                "version boundary bled"
            );
        }
    }

    #[test]
    fn occupancy_range_and_pairing_mirror_the_reverse_log() {
        let rl_key = target_reverse_log_key("target-a", "9:2", 3, 7);
        let paired = occupancy_key_for_reverse_log_key(&rl_key);
        assert_eq!(
            paired,
            target_current_fragment_key("target-a", "9:2", 3, 7),
            "prefix-12 and prefix-16 key layouts must stay identical apart from the prefix byte"
        );
        let (begin, end) = target_current_fragment_version_range("target-a", "9:2");
        assert!(paired.as_slice() >= begin.as_slice() && paired.as_slice() < end.as_slice());
    }
}
