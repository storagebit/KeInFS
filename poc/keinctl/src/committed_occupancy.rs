// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! Shared committed-granule-occupancy keyspace.
//!
//! This is the durable, TTL-independent record of which `(target, granule)` slots
//! hold a committed object fragment. It is **written by KMS** atomically inside the
//! `commit_object_write` transaction (the same txn that writes the manifest and the
//! per-fragment index) and **read by KAS** as an allocation constraint: a granule
//! with a committed marker is never returned to the allocator free pool by the
//! reservation reaper and is never handed out by a reserve.
//!
//! It lives in its own keyspace (`KCO1`), distinct from the private `KMS`/`KAS1`
//! keyspaces, because it is shared truth. The key encoder lives in `keinctl` — the
//! one crate both `kms` and `kas` already depend on — so the two sides cannot drift.
//!
//! Key layout (sorts by target, then by granule index):
//! ```text
//!   "KCO1" | version(1) | target_id.len()(u32 BE) | target_id | granule_index(u64 BE)
//! ```
//! Value layout (compact, no serde dependency in `keinctl`):
//! ```text
//!   generation(u32 BE) | version_id(utf-8, rest)
//! ```

const KCO_NAMESPACE: &[u8; 4] = b"KCO1";
const KCO_VERSION: u8 = 1;

/// Durable key for one committed granule on a target.
pub fn committed_granule_key(target_id: &str, granule_index: u64) -> Vec<u8> {
    let mut key = committed_granule_target_prefix(target_id);
    key.extend_from_slice(&granule_index.to_be_bytes());
    key
}

/// Prefix covering every committed granule on a single target (per-target scan).
pub fn committed_granule_target_prefix(target_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(KCO_NAMESPACE.len() + 2 + 4 + target_id.len());
    key.extend_from_slice(KCO_NAMESPACE);
    key.push(KCO_VERSION);
    key.extend_from_slice(&(target_id.len() as u32).to_be_bytes());
    key.extend_from_slice(target_id.as_bytes());
    key
}

/// Prefix covering every committed granule across all targets (bulk load).
pub fn committed_granule_all_prefix() -> Vec<u8> {
    let mut key = Vec::with_capacity(KCO_NAMESPACE.len() + 1);
    key.extend_from_slice(KCO_NAMESPACE);
    key.push(KCO_VERSION);
    key
}

/// Exclusive range end for a prefix scan (lexicographic successor).
pub fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return end;
        }
    }
    end.push(0);
    end
}

/// Decode a committed-granule key back into `(target_id, granule_index)`.
/// Returns `None` if the bytes are not a well-formed key in this keyspace.
pub fn decode_committed_granule_key(key: &[u8]) -> Option<(String, u64)> {
    let header = KCO_NAMESPACE.len() + 1;
    if key.len() < header + 4 {
        return None;
    }
    if &key[..KCO_NAMESPACE.len()] != KCO_NAMESPACE || key[KCO_NAMESPACE.len()] != KCO_VERSION {
        return None;
    }
    let len_at = header;
    let id_len = u32::from_be_bytes(key[len_at..len_at + 4].try_into().ok()?) as usize;
    let id_at = len_at + 4;
    let gi_at = id_at + id_len;
    if key.len() != gi_at + 8 {
        return None;
    }
    let target_id = String::from_utf8(key[id_at..gi_at].to_vec()).ok()?;
    let granule_index = u64::from_be_bytes(key[gi_at..gi_at + 8].try_into().ok()?);
    Some((target_id, granule_index))
}

/// The committed-granule marker value: which object version/generation owns the slot.
/// `generation` lets the write-time occupancy guard distinguish a legitimate
/// same-fragment rewrite from an overwrite of a different committed object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedGranule {
    pub version_id: String,
    pub generation: u32,
}

impl CommittedGranule {
    pub fn encode(&self) -> Vec<u8> {
        let mut value = Vec::with_capacity(4 + self.version_id.len());
        value.extend_from_slice(&self.generation.to_be_bytes());
        value.extend_from_slice(self.version_id.as_bytes());
        value
    }

    pub fn decode(value: &[u8]) -> Option<Self> {
        if value.len() < 4 {
            return None;
        }
        let generation = u32::from_be_bytes(value[..4].try_into().ok()?);
        let version_id = String::from_utf8(value[4..].to_vec()).ok()?;
        Some(Self {
            version_id,
            generation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_roundtrips_target_and_granule() {
        let key = committed_granule_key("epyc-target-07", 123_456_789);
        assert_eq!(
            decode_committed_granule_key(&key),
            Some(("epyc-target-07".to_string(), 123_456_789))
        );
    }

    #[test]
    fn granule_keys_sort_numerically_within_a_target() {
        // u64 BE encoding makes the durable keys sort by granule index, so a
        // per-target range scan returns granules in order.
        let a = committed_granule_key("t", 2);
        let b = committed_granule_key("t", 10);
        assert!(a < b, "granule 2 must sort before granule 10");
    }

    #[test]
    fn target_prefix_groups_only_that_target() {
        let p0 = committed_granule_target_prefix("epyc-target-00");
        let p1 = committed_granule_target_prefix("epyc-target-01");
        assert!(committed_granule_key("epyc-target-00", 5).starts_with(&p0));
        assert!(!committed_granule_key("epyc-target-00", 5).starts_with(&p1));
        // A target whose id is a prefix of another's must not capture it: the
        // 4-byte length field in front of the id makes "t" and "t0" disjoint.
        let pt = committed_granule_target_prefix("t");
        assert!(!committed_granule_key("t0", 0).starts_with(&pt));
    }

    #[test]
    fn all_prefix_covers_every_target() {
        let all = committed_granule_all_prefix();
        assert!(committed_granule_key("a", 0).starts_with(&all));
        assert!(committed_granule_key("z", u64::MAX).starts_with(&all));
    }

    #[test]
    fn value_roundtrips() {
        let v = CommittedGranule {
            version_id: "ver-abc".to_string(),
            generation: 7,
        };
        assert_eq!(CommittedGranule::decode(&v.encode()), Some(v));
    }

    #[test]
    fn range_end_is_lexicographic_successor() {
        let p = committed_granule_all_prefix();
        let end = prefix_range_end(&p);
        assert!(end > p);
        assert!(committed_granule_key("zzz", u64::MAX) < end);
    }
}
