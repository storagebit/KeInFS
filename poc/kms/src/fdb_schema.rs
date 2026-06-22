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
        bucket_context_key, decode_segments, namespace_entry_key, namespace_path_key,
        object_head_key, object_head_prefix, object_version_chunk_key, object_version_chunk_prefix,
        object_version_key, target_current_fragment_key, write_intent_chunk_key,
        write_intent_chunk_prefix, write_intent_key, PREFIX_BUCKET_CONTEXT, PREFIX_NAMESPACE_PATH,
        PREFIX_OBJECT_HEAD, PREFIX_OBJECT_VERSION, PREFIX_OBJECT_VERSION_CHUNK,
        PREFIX_TARGET_CURRENT_FRAGMENT, PREFIX_WRITE_INTENT, PREFIX_WRITE_INTENT_CHUNK,
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
}
