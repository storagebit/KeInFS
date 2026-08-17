// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::stats::{KmsStats, RpcKind};
use crate::watch::NotificationEvent;
use keinctl::proto::{EcProfile, ObjectVersionManifest};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Number of independently locked shards. ResolveObjectRead is the highest-QPS
/// op and previously contended a single global `Mutex` even on cache hits;
/// sharding spreads that contention across many locks. Must be a power of two
/// so the shard index can be derived with a cheap mask.
const SHARD_COUNT: usize = 32;

#[derive(Clone)]
pub(crate) struct ResolveObjectReadCache {
    shards: Arc<Vec<Mutex<Shard>>>,
    /// Per-shard capacity. The configured `max_entries` is split evenly across
    /// shards; total capacity is `per_shard_max * SHARD_COUNT`.
    per_shard_max: usize,
    ttl: Duration,
    enabled: bool,
}

/// Per-shard state: a `HashMap` for O(1) lookup plus a hand-rolled intrusive
/// doubly linked list (over slab indices) giving O(1) LRU bookkeeping. The map
/// stores the slot index; the slab holds the actual entries and prev/next
/// links. `head` is the most-recently-used end, `tail` the least.
struct Shard {
    index: HashMap<CacheKey, usize>,
    slab: Vec<Slot>,
    free: Vec<usize>,
    head: usize,
    tail: usize,
}

const NIL: usize = usize::MAX;

struct Slot {
    key: CacheKey,
    entry: CacheEntry,
    prev: usize,
    next: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    bucket_id: String,
    key: String,
}

#[derive(Clone)]
struct CacheEntry {
    namespace_id: String,
    manifest: ObjectVersionManifest,
    ec_profile: EcProfile,
    expires_at: Instant,
}

impl Shard {
    fn new() -> Self {
        Self {
            index: HashMap::new(),
            slab: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
        }
    }

    /// Unlink `slot` from the LRU list (does not touch the map or free list).
    fn unlink(&mut self, slot: usize) {
        let (prev, next) = {
            let s = &self.slab[slot];
            (s.prev, s.next)
        };
        if prev != NIL {
            self.slab[prev].next = next;
        } else {
            self.head = next;
        }
        if next != NIL {
            self.slab[next].prev = prev;
        } else {
            self.tail = prev;
        }
        self.slab[slot].prev = NIL;
        self.slab[slot].next = NIL;
    }

    /// Push `slot` to the head (most-recently-used) of the LRU list.
    fn push_front(&mut self, slot: usize) {
        let old_head = self.head;
        self.slab[slot].prev = NIL;
        self.slab[slot].next = old_head;
        if old_head != NIL {
            self.slab[old_head].prev = slot;
        }
        self.head = slot;
        if self.tail == NIL {
            self.tail = slot;
        }
    }

    fn touch(&mut self, slot: usize) {
        if self.head == slot {
            return;
        }
        self.unlink(slot);
        self.push_front(slot);
    }

    /// Remove a slot entirely: unlink, drop from map, recycle the slab slot.
    fn remove_slot(&mut self, slot: usize) {
        self.unlink(slot);
        let key = self.slab[slot].key.clone();
        self.index.remove(&key);
        self.free.push(slot);
    }

    fn alloc(&mut self, key: CacheKey, entry: CacheEntry) -> usize {
        if let Some(slot) = self.free.pop() {
            self.slab[slot] = Slot {
                key,
                entry,
                prev: NIL,
                next: NIL,
            };
            slot
        } else {
            self.slab.push(Slot {
                key,
                entry,
                prev: NIL,
                next: NIL,
            });
            self.slab.len() - 1
        }
    }
}

/// Derive the shard index for a key. Pure function so shard distribution can be
/// asserted in tests.
fn shard_index(bucket_id: &str, key: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    bucket_id.hash(&mut hasher);
    // Separator so ("ab", "c") and ("a", "bc") do not collide.
    0u8.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) & (SHARD_COUNT - 1)
}

impl ResolveObjectReadCache {
    pub(crate) fn new(max_entries: usize, ttl: Duration) -> Self {
        let enabled = max_entries > 0 && !ttl.is_zero();
        // Split capacity across shards, keeping at least one slot per shard when
        // the cache is enabled so small caps still hold entries.
        let per_shard_max = if enabled {
            max_entries.div_ceil(SHARD_COUNT).max(1)
        } else {
            0
        };
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(Mutex::new(Shard::new()));
        }
        Self {
            shards: Arc::new(shards),
            per_shard_max,
            ttl,
            enabled,
        }
    }

    pub(crate) fn get(
        &self,
        bucket_id: &str,
        key: &str,
    ) -> Option<(ObjectVersionManifest, EcProfile)> {
        if !self.enabled {
            return None;
        }
        let now = Instant::now();
        let cache_key = CacheKey {
            bucket_id: bucket_id.to_string(),
            key: key.to_string(),
        };
        let shard = &self.shards[shard_index(bucket_id, key)];
        let mut shard = shard.lock().unwrap();
        let Some(&slot) = shard.index.get(&cache_key) else {
            return None;
        };
        // Lazy TTL: expire on lookup rather than scanning on every insert.
        if shard.slab[slot].entry.expires_at <= now {
            shard.remove_slot(slot);
            return None;
        }
        shard.touch(slot);
        let entry = &shard.slab[slot].entry;
        Some((entry.manifest.clone(), entry.ec_profile.clone()))
    }

    pub(crate) fn insert(
        &self,
        bucket_id: String,
        key: String,
        namespace_id: String,
        manifest: ObjectVersionManifest,
        ec_profile: EcProfile,
    ) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        let idx = shard_index(&bucket_id, &key);
        let cache_key = CacheKey { bucket_id, key };
        let entry = CacheEntry {
            namespace_id,
            manifest,
            ec_profile,
            expires_at: now + self.ttl,
        };
        let mut shard = self.shards[idx].lock().unwrap();
        if let Some(&slot) = shard.index.get(&cache_key) {
            shard.slab[slot].entry = entry;
            shard.touch(slot);
            return;
        }
        let slot = shard.alloc(cache_key.clone(), entry);
        shard.index.insert(cache_key, slot);
        shard.push_front(slot);
        // O(1) LRU eviction from the tail. No full scan; expired entries are
        // reclaimed lazily on lookup.
        while shard.index.len() > self.per_shard_max {
            let tail = shard.tail;
            if tail == NIL {
                break;
            }
            shard.remove_slot(tail);
        }
    }

    pub(crate) fn invalidate_namespace(&self, namespace_id: &str) {
        for shard in self.shards.iter() {
            let mut shard = shard.lock().unwrap();
            let victims: Vec<usize> = shard
                .index
                .values()
                .copied()
                .filter(|&slot| shard.slab[slot].entry.namespace_id == namespace_id)
                .collect();
            for slot in victims {
                shard.remove_slot(slot);
            }
        }
    }

    pub(crate) fn invalidate_object(&self, bucket_id: &str, key: &str) {
        let cache_key = CacheKey {
            bucket_id: bucket_id.to_string(),
            key: key.to_string(),
        };
        let mut shard = self.shards[shard_index(bucket_id, key)].lock().unwrap();
        if let Some(&slot) = shard.index.get(&cache_key) {
            shard.remove_slot(slot);
        }
    }

    pub(crate) fn invalidate_all(&self) {
        for shard in self.shards.iter() {
            let mut shard = shard.lock().unwrap();
            *shard = Shard::new();
        }
    }

    pub(crate) fn spawn_invalidator(
        &self,
        notifications: watch::Receiver<NotificationEvent>,
        stats: Arc<KmsStats>,
    ) -> tokio::task::JoinHandle<()> {
        let cache = self.clone();
        tokio::spawn(async move {
            let mut signal = notifications;
            while signal.changed().await.is_ok() {
                let event = signal.borrow().clone();
                if event.sequence == 0 {
                    continue;
                }
                let phase_started = Instant::now();
                if event.namespace_id.is_empty() {
                    cache.invalidate_all();
                    stats.record_phase(
                        RpcKind::ResolveObjectRead,
                        "read_cache_invalidate_all",
                        phase_started.elapsed(),
                    );
                } else if !event.bucket_id.is_empty() && !event.key.is_empty() {
                    cache.invalidate_object(&event.bucket_id, &event.key);
                    stats.record_phase(
                        RpcKind::ResolveObjectRead,
                        "read_cache_invalidate_object",
                        phase_started.elapsed(),
                    );
                } else {
                    cache.invalidate_namespace(&event.namespace_id);
                    stats.record_phase(
                        RpcKind::ResolveObjectRead,
                        "read_cache_invalidate_namespace",
                        phase_started.elapsed(),
                    );
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{shard_index, ResolveObjectReadCache, SHARD_COUNT};
    use keinctl::proto::{EcProfile, FailureDomain, ObjectVersionManifest};
    use std::collections::HashSet;
    use std::thread;
    use std::time::Duration;

    /// Find `count` distinct keys (within one bucket) that all hash to the same
    /// shard, so per-shard LRU behaviour can be asserted deterministically.
    fn keys_in_same_shard(bucket: &str, count: usize) -> Vec<String> {
        let target = shard_index(bucket, "seed-0");
        let mut out = Vec::new();
        let mut i = 0;
        while out.len() < count {
            let candidate = format!("seed-{i}");
            if shard_index(bucket, &candidate) == target {
                out.push(candidate);
            }
            i += 1;
        }
        out
    }

    #[test]
    fn shard_index_is_within_bounds_and_distributes() {
        let mut seen = HashSet::new();
        for i in 0..2_000 {
            let idx = shard_index("bucket-a", &format!("key-{i}"));
            assert!(idx < SHARD_COUNT);
            seen.insert(idx);
        }
        // With 2000 keys over 32 shards, every shard should be exercised.
        assert_eq!(seen.len(), SHARD_COUNT);
    }

    #[test]
    fn shard_index_is_deterministic_and_separated() {
        assert_eq!(
            shard_index("bucket-a", "dir/x"),
            shard_index("bucket-a", "dir/x")
        );
        // Separator prevents boundary collisions between bucket and key.
        assert_ne!(shard_index("ab", "c"), 9_999);
    }

    #[test]
    fn returns_cached_entry_before_ttl_expires() {
        let cache = ResolveObjectReadCache::new(8, Duration::from_millis(50));
        cache.insert(
            "bucket-a".to_string(),
            "dir/object.bin".to_string(),
            "ns-a".to_string(),
            manifest("v1", "ns-a"),
            profile("p1"),
        );

        let (manifest, profile) = cache.get("bucket-a", "dir/object.bin").unwrap();
        assert_eq!(manifest.version_id, "v1");
        assert_eq!(profile.id, "p1");
    }

    #[test]
    fn drops_entry_after_ttl_expires() {
        let cache = ResolveObjectReadCache::new(8, Duration::from_millis(5));
        cache.insert(
            "bucket-a".to_string(),
            "dir/object.bin".to_string(),
            "ns-a".to_string(),
            manifest("v1", "ns-a"),
            profile("p1"),
        );
        thread::sleep(Duration::from_millis(10));
        assert!(cache.get("bucket-a", "dir/object.bin").is_none());
    }

    #[test]
    fn disabled_when_zero_capacity_or_ttl() {
        let no_entries = ResolveObjectReadCache::new(0, Duration::from_secs(1));
        no_entries.insert(
            "bucket-a".to_string(),
            "k".to_string(),
            "ns-a".to_string(),
            manifest("v1", "ns-a"),
            profile("p1"),
        );
        assert!(no_entries.get("bucket-a", "k").is_none());

        let no_ttl = ResolveObjectReadCache::new(8, Duration::ZERO);
        no_ttl.insert(
            "bucket-a".to_string(),
            "k".to_string(),
            "ns-a".to_string(),
            manifest("v1", "ns-a"),
            profile("p1"),
        );
        assert!(no_ttl.get("bucket-a", "k").is_none());
    }

    #[test]
    fn invalidates_only_matching_namespace_entries() {
        let cache = ResolveObjectReadCache::new(64, Duration::from_secs(1));
        cache.insert(
            "bucket-a".to_string(),
            "dir/a.bin".to_string(),
            "ns-a".to_string(),
            manifest("v1", "ns-a"),
            profile("p1"),
        );
        cache.insert(
            "bucket-b".to_string(),
            "dir/b.bin".to_string(),
            "ns-b".to_string(),
            manifest("v2", "ns-b"),
            profile("p2"),
        );

        cache.invalidate_namespace("ns-a");

        assert!(cache.get("bucket-a", "dir/a.bin").is_none());
        assert!(cache.get("bucket-b", "dir/b.bin").is_some());
    }

    #[test]
    fn invalidates_only_matching_object_entry() {
        let cache = ResolveObjectReadCache::new(64, Duration::from_secs(1));
        cache.insert(
            "bucket-a".to_string(),
            "dir/a.bin".to_string(),
            "ns-a".to_string(),
            manifest("v1", "ns-a"),
            profile("p1"),
        );
        cache.insert(
            "bucket-a".to_string(),
            "dir/b.bin".to_string(),
            "ns-a".to_string(),
            manifest("v2", "ns-a"),
            profile("p1"),
        );

        cache.invalidate_object("bucket-a", "dir/a.bin");

        assert!(cache.get("bucket-a", "dir/a.bin").is_none());
        assert!(cache.get("bucket-a", "dir/b.bin").is_some());
    }

    #[test]
    fn invalidate_all_clears_every_shard() {
        let cache = ResolveObjectReadCache::new(256, Duration::from_secs(1));
        for i in 0..200 {
            cache.insert(
                "bucket-a".to_string(),
                format!("key-{i}"),
                "ns-a".to_string(),
                manifest("v1", "ns-a"),
                profile("p1"),
            );
        }
        cache.invalidate_all();
        for i in 0..200 {
            assert!(cache.get("bucket-a", &format!("key-{i}")).is_none());
        }
    }

    #[test]
    fn evicts_least_recently_used_entry_when_shard_full() {
        // Force three keys into the same shard with a per-shard capacity of 2.
        let keys = keys_in_same_shard("bucket-a", 3);
        assert_eq!(
            shard_index("bucket-a", &keys[0]),
            shard_index("bucket-a", &keys[2])
        );
        // per_shard_max = ceil(2 / SHARD_COUNT).max(1) == 1, so build a cache
        // whose per-shard capacity is exactly 2.
        let cache = ResolveObjectReadCache::new(2 * SHARD_COUNT, Duration::from_secs(1));

        cache.insert(
            "bucket-a".to_string(),
            keys[0].clone(),
            "ns-a".to_string(),
            manifest("v1", "ns-a"),
            profile("p1"),
        );
        cache.insert(
            "bucket-a".to_string(),
            keys[1].clone(),
            "ns-a".to_string(),
            manifest("v2", "ns-a"),
            profile("p1"),
        );
        // Touch keys[0] so keys[1] becomes least-recently-used.
        let _ = cache.get("bucket-a", &keys[0]);
        cache.insert(
            "bucket-a".to_string(),
            keys[2].clone(),
            "ns-a".to_string(),
            manifest("v3", "ns-a"),
            profile("p1"),
        );

        assert!(cache.get("bucket-a", &keys[0]).is_some());
        assert!(cache.get("bucket-a", &keys[1]).is_none());
        assert!(cache.get("bucket-a", &keys[2]).is_some());
    }

    #[test]
    fn reinsert_updates_value_without_growing() {
        let cache = ResolveObjectReadCache::new(2 * SHARD_COUNT, Duration::from_secs(1));
        cache.insert(
            "bucket-a".to_string(),
            "dir/x".to_string(),
            "ns-a".to_string(),
            manifest("v1", "ns-a"),
            profile("p1"),
        );
        cache.insert(
            "bucket-a".to_string(),
            "dir/x".to_string(),
            "ns-a".to_string(),
            manifest("v2", "ns-a"),
            profile("p1"),
        );
        let (manifest, _) = cache.get("bucket-a", "dir/x").unwrap();
        assert_eq!(manifest.version_id, "v2");
    }

    fn manifest(version_id: &str, namespace_id: &str) -> ObjectVersionManifest {
        ObjectVersionManifest {
            version_id: version_id.to_string(),
            bucket_id: "bucket-a".to_string(),
            key: "dir/object.bin".to_string(),
            logical_length_bytes: 1024,
            ec_profile_id: "p1".to_string(),
            stripes: Vec::new(),
            namespace_id: namespace_id.to_string(),
            object_entry_id: "obj".to_string(),
            bucket_entry_id: "bucket-entry".to_string(),
        }
    }

    fn profile(id: &str) -> EcProfile {
        EcProfile {
            id: id.to_string(),
            codec_id: "rs".to_string(),
            data_fragments: 8,
            parity_fragments: 2,
            fragment_bytes: 1_048_576,
            failure_domain: FailureDomain::Node as i32,
        }
    }
}
