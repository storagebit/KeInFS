// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! Tier-B RAM stripe cache for the ranged read path.
//!
//! Without it, every ~128 KiB FUSE read of a Ranged handle re-fetches (and
//! re-EC-decodes) the *entire* covering stripe: `get_object_range` pulls a full
//! `W = data_fragments * fragment_bytes` stripe, returns the small requested
//! slice, and discards the rest. A cold sequential read of an N-byte object
//! therefore moves ~`N * (W / fuse_read_size)` bytes off the targets — measured
//! at **64x** on the lab (a 128 MiB read served 8 GiB). This cache holds the
//! last-fetched stripes so reads landing in the same stripe hit RAM.
//!
//! Keyed by `(object key, stripe index)`. A put is a new immutable object
//! version, so stale stripes must be dropped: the cache is invalidated per-key on
//! both coherence (NATS) events and local commits. Backed by a sharded
//! [`DashMap`] (concurrent gets, no global lock) with a byte budget and
//! approximate-LRU eviction by access tick. The mount is single-bucket =>
//! a single EC profile => `W` is constant once learned.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Default RAM budget for cached stripes. Holds a healthy working set of 8 MiB
/// stripes (~32 of them) without unbounded growth; eviction is approximate-LRU.
pub const DEFAULT_STRIPE_CACHE_BUDGET_BYTES: usize = 256 * 1024 * 1024;

/// Unit separator — cannot appear in an object key path component, so it makes a
/// safe, prefix-scannable composite key (`<key>\u{1f}<stripe_index>`).
const KEY_SEP: char = '\u{1f}';

struct Entry {
    bytes: Arc<Vec<u8>>,
    last_tick: AtomicU64,
}

pub(crate) struct StripeCache {
    map: DashMap<String, Entry>,
    total_bytes: AtomicUsize,
    budget_bytes: usize,
    tick: AtomicU64,
    /// Serializes every size-mutating path (`put`, `evict_to_budget`,
    /// `invalidate_key`, `clear`) so that each `total_bytes` delta is applied as
    /// one critical section with the map mutation that justifies it. Without it,
    /// the `(map.insert/remove, fetch_add/fetch_sub)` pairs are independent RMWs
    /// that race: a `put` replacing an entry can interleave with an `evict`
    /// removing that same key, double-subtracting its length and underflowing
    /// `total_bytes` (an `AtomicUsize` `fetch_sub` wraps to ~`usize::MAX`), which
    /// would then make `total_bytes > budget` permanently true and evict the
    /// whole working set. `get` stays lock-free (it touches neither the map's
    /// contents nor `total_bytes`). `n` here is tiny (~budget/W ≈ 32 entries) so
    /// the contention cost is negligible.
    size_lock: Mutex<()>,
    /// Single-flight gate per composite key. A cold-stripe miss fetches a full
    /// `W`-byte stripe plus an EC decode; without de-dup, `N` concurrent FUSE
    /// reads that all miss the SAME stripe each issue that fetch+decode, moving
    /// `N*W` bytes off the targets — the exact read amplification this cache
    /// exists to kill, reappearing at fan-out width on first touch. Concurrent
    /// missers of the same stripe instead await one shared mutex, so only the
    /// leader fetches; the followers re-check the cache and get the shared
    /// `Arc<Vec<u8>>`.
    in_flight: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    /// Uniform stripe width `W` for this mount; 0 until the first ranged read
    /// learns it from the EC profile.
    stripe_width: AtomicU64,
}

impl StripeCache {
    pub(crate) fn new(budget_bytes: usize) -> Self {
        Self {
            map: DashMap::new(),
            total_bytes: AtomicUsize::new(0),
            budget_bytes: budget_bytes.max(1),
            tick: AtomicU64::new(0),
            size_lock: Mutex::new(()),
            in_flight: DashMap::new(),
            stripe_width: AtomicU64::new(0),
        }
    }

    /// The shared single-flight gate for one composite key. Concurrent missers
    /// of the same `(key, stripe_index)` get the same `Arc<Mutex<()>>` and
    /// serialize their fetches behind it; see the `in_flight` field comment.
    pub(crate) fn in_flight_gate(&self, key: &str, stripe_index: u64) -> Arc<tokio::sync::Mutex<()>> {
        let composed = Self::compose_key(key, stripe_index);
        self.in_flight
            .entry(composed)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Drop the single-flight gate for one composite key once the leader has
    /// populated the cache. Safe to call even if another waiter still holds the
    /// `Arc`: removing the map entry only stops *new* missers from joining this
    /// generation; in-progress waiters keep their cloned `Arc` alive and, on
    /// wake, find the now-cached stripe.
    pub(crate) fn clear_in_flight(&self, key: &str, stripe_index: u64) {
        let composed = Self::compose_key(key, stripe_index);
        self.in_flight.remove(&composed);
    }

    /// The learned stripe width `W`, or 0 if not yet known.
    pub(crate) fn stripe_width(&self) -> u64 {
        self.stripe_width.load(Ordering::Relaxed)
    }

    /// Record the learned stripe width (idempotent; ignores 0).
    pub(crate) fn observe_stripe_width(&self, width: u64) {
        if width > 0 {
            self.stripe_width.store(width, Ordering::Relaxed);
        }
    }

    pub(crate) fn compose_key(key: &str, stripe_index: u64) -> String {
        format!("{key}{KEY_SEP}{stripe_index}")
    }

    /// Fetch a cached stripe, bumping its LRU tick on hit.
    pub(crate) fn get(&self, key: &str, stripe_index: u64) -> Option<Arc<Vec<u8>>> {
        let composed = Self::compose_key(key, stripe_index);
        let entry = self.map.get(&composed)?;
        entry
            .last_tick
            .store(self.tick.fetch_add(1, Ordering::Relaxed), Ordering::Relaxed);
        Some(Arc::clone(&entry.bytes))
    }

    /// Insert (or replace) a stripe, then evict down to the byte budget.
    ///
    /// The whole insert-and-account-and-evict sequence runs under `size_lock`
    /// so `total_bytes` can never drift or underflow against the live map (see
    /// the field comment). Empty payloads are not cached — a 0-byte stripe (a
    /// read at/past EOF) only churns ticks and eviction without ever serving
    /// useful bytes.
    ///
    /// Returns the stripes that eviction dropped to stay within budget, as
    /// `(object key, stripe_index, bytes)`, so the caller can WRITE THEM BACK to
    /// the Tier-C disk victim cache **outside** `size_lock` (the eviction itself
    /// runs under the lock; the slow disk write must not). Tier-B alone ignores
    /// the return value. The victim's `Arc<Vec<u8>>` is preserved (not dropped)
    /// across the eviction precisely so it can be handed to Tier-C.
    pub(crate) fn put(
        &self,
        key: &str,
        stripe_index: u64,
        bytes: Arc<Vec<u8>>,
    ) -> Vec<(String, u64, Arc<Vec<u8>>)> {
        if bytes.is_empty() {
            return Vec::new();
        }
        let composed = Self::compose_key(key, stripe_index);
        let len = bytes.len();
        let tick = self.tick.fetch_add(1, Ordering::Relaxed);
        let _guard = self.size_lock.lock().expect("stripe cache size lock poisoned");
        if let Some(prev) = self.map.insert(
            composed,
            Entry {
                bytes,
                last_tick: AtomicU64::new(tick),
            },
        ) {
            self.total_bytes
                .fetch_sub(prev.bytes.len(), Ordering::Relaxed);
        }
        self.total_bytes.fetch_add(len, Ordering::Relaxed);
        self.evict_to_budget_locked()
    }

    /// Drop every cached stripe for `key` — a new object version invalidates them
    /// all (coherence event or local commit).
    pub(crate) fn invalidate_key(&self, key: &str) {
        let prefix = format!("{key}{KEY_SEP}");
        let _guard = self.size_lock.lock().expect("stripe cache size lock poisoned");
        let mut freed = 0usize;
        self.map.retain(|composed, entry| {
            if composed.starts_with(&prefix) {
                freed += entry.bytes.len();
                false
            } else {
                true
            }
        });
        if freed > 0 {
            self.total_bytes.fetch_sub(freed, Ordering::Relaxed);
        }
    }

    /// Drop everything (namespace-wide invalidation).
    pub(crate) fn clear(&self) {
        let _guard = self.size_lock.lock().expect("stripe cache size lock poisoned");
        self.map.clear();
        self.total_bytes.store(0, Ordering::Relaxed);
    }

    /// Approximate-LRU eviction: while over budget, drop the coldest entry. O(n)
    /// per eviction, but n is small (budget / W).
    ///
    /// MUST be called with `size_lock` held — it pairs each `map.remove` with the
    /// matching `total_bytes.fetch_sub` and relies on no concurrent `put`/`evict`
    /// touching the same accounting. `put` is the only caller and holds the lock.
    ///
    /// The victim key is `.clone()`'d out of the `iter()` first, then removed, so
    /// no shard read guard from `iter()` is alive when the write-locking `remove`
    /// runs (the `for` temporary is dropped at the end of the loop). NOTE: this
    /// correctness depends on draining the iterator fully — do NOT `break` out of
    /// the `for entry in self.map.iter()` loop while still holding `entry` and
    /// then call `self.map.remove()`: that would self-deadlock on the same shard.
    ///
    /// The min-`last_tick` victim selection assumes `tick` never wraps `u64`
    /// (centuries away at any realistic access rate); after a wrap the
    /// approximate-LRU ordering would no longer track true age, but correctness
    /// (never over budget) is unaffected.
    ///
    /// Returns each evicted stripe as `(object key, stripe_index, bytes)` so the
    /// caller can write it back to Tier-C. The composite map key is split on the
    /// trailing `KEY_SEP<index>` to recover the object key + stripe index; if a
    /// key somehow fails to parse it is simply not forwarded (Tier-C is
    /// best-effort), never panicked on.
    fn evict_to_budget_locked(&self) -> Vec<(String, u64, Arc<Vec<u8>>)> {
        let mut evicted = Vec::new();
        while self.total_bytes.load(Ordering::Relaxed) > self.budget_bytes {
            let mut victim: Option<(String, u64)> = None;
            for entry in self.map.iter() {
                let tick = entry.value().last_tick.load(Ordering::Relaxed);
                match &victim {
                    Some((_, coldest)) if *coldest <= tick => {}
                    _ => victim = Some((entry.key().clone(), tick)),
                }
            }
            let Some((victim_key, _)) = victim else { break };
            if let Some((composed, removed)) = self.map.remove(&victim_key) {
                self.total_bytes
                    .fetch_sub(removed.bytes.len(), Ordering::Relaxed);
                if let Some((key, idx)) = Self::split_key(&composed) {
                    evicted.push((key, idx, removed.bytes));
                }
            } else {
                break;
            }
        }
        evicted
    }

    /// Split a composite `"<key>\u{1f}<stripe_index>"` back into its parts. The
    /// separator cannot appear in an object key, so the LAST one delimits the
    /// numeric stripe index unambiguously.
    fn split_key(composed: &str) -> Option<(String, u64)> {
        let (key, idx) = composed.rsplit_once(KEY_SEP)?;
        let idx: u64 = idx.parse().ok()?;
        Some((key.to_string(), idx))
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    #[cfg(test)]
    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stripe(byte: u8, len: usize) -> Arc<Vec<u8>> {
        Arc::new(vec![byte; len])
    }

    #[test]
    fn hit_and_miss() {
        let cache = StripeCache::new(DEFAULT_STRIPE_CACHE_BUDGET_BYTES);
        assert!(cache.get("a", 0).is_none());
        cache.put("a", 0, stripe(7, 1024));
        let got = cache.get("a", 0).expect("hit");
        assert_eq!(got.len(), 1024);
        assert_eq!(got[0], 7);
        // Distinct stripe index is a separate slot.
        assert!(cache.get("a", 1).is_none());
    }

    #[test]
    fn stripe_width_learned_once() {
        let cache = StripeCache::new(DEFAULT_STRIPE_CACHE_BUDGET_BYTES);
        assert_eq!(cache.stripe_width(), 0);
        cache.observe_stripe_width(8 * 1024 * 1024);
        assert_eq!(cache.stripe_width(), 8 * 1024 * 1024);
        cache.observe_stripe_width(0); // ignored
        assert_eq!(cache.stripe_width(), 8 * 1024 * 1024);
    }

    #[test]
    fn invalidate_key_drops_only_that_key() {
        let cache = StripeCache::new(DEFAULT_STRIPE_CACHE_BUDGET_BYTES);
        cache.put("a", 0, stripe(1, 100));
        cache.put("a", 1, stripe(1, 100));
        cache.put("b", 0, stripe(2, 100));
        assert_eq!(cache.len(), 3);
        cache.invalidate_key("a");
        assert_eq!(cache.len(), 1);
        assert!(cache.get("a", 0).is_none());
        assert!(cache.get("a", 1).is_none());
        assert!(cache.get("b", 0).is_some());
        assert_eq!(cache.total_bytes(), 100);
    }

    #[test]
    fn key_prefix_is_not_confused() {
        // "a" must not match the "ab" key family.
        let cache = StripeCache::new(DEFAULT_STRIPE_CACHE_BUDGET_BYTES);
        cache.put("a", 0, stripe(1, 10));
        cache.put("ab", 0, stripe(2, 10));
        cache.invalidate_key("a");
        assert!(cache.get("a", 0).is_none());
        assert!(cache.get("ab", 0).is_some());
    }

    #[test]
    fn evicts_to_budget_dropping_coldest() {
        // Budget for 2 x 1000-byte stripes.
        let cache = StripeCache::new(2000);
        cache.put("k", 0, stripe(0, 1000));
        cache.put("k", 1, stripe(1, 1000));
        // Touch stripe 0 so 1 is now the coldest.
        let _ = cache.get("k", 0);
        cache.put("k", 2, stripe(2, 1000)); // over budget -> evict coldest (1)
        assert_eq!(cache.len(), 2);
        assert!(cache.get("k", 0).is_some(), "recently-used kept");
        assert!(cache.get("k", 2).is_some(), "newest kept");
        assert!(cache.get("k", 1).is_none(), "coldest evicted");
        assert!(cache.total_bytes() <= 2000);
    }

    #[test]
    fn put_returns_evicted_victims_for_tier_c_writeback() {
        // The victim cache wiring depends on put() handing the evicted stripes
        // back (key, stripe_index, bytes) so core.rs can write them to Tier-C.
        let cache = StripeCache::new(2000); // 2 x 1000-byte stripes
        assert!(cache.put("obj/a", 0, stripe(0, 1000)).is_empty(), "no evict");
        assert!(cache.put("obj/a", 1, stripe(1, 1000)).is_empty(), "no evict");
        let _ = cache.get("obj/a", 1); // make stripe 0 the coldest
        let evicted = cache.put("obj/a", 2, stripe(2, 1000)); // evicts stripe 0
        assert_eq!(evicted.len(), 1, "exactly one victim");
        let (vkey, vidx, vbytes) = &evicted[0];
        assert_eq!(vkey, "obj/a", "object key recovered from composite key");
        assert_eq!(*vidx, 0, "stripe index recovered (the coldest)");
        assert_eq!(vbytes.len(), 1000);
        assert_eq!(vbytes[0], 0, "victim bytes preserved, not dropped");
    }

    #[test]
    fn split_key_roundtrips_and_rejects_garbage() {
        let composed = StripeCache::compose_key("a/b/c", 42);
        assert_eq!(
            StripeCache::split_key(&composed),
            Some(("a/b/c".to_string(), 42))
        );
        // No separator at all -> None (not forwarded to Tier-C).
        assert_eq!(StripeCache::split_key("no-sep"), None);
    }

    #[test]
    fn clear_empties_everything() {
        let cache = StripeCache::new(DEFAULT_STRIPE_CACHE_BUDGET_BYTES);
        cache.put("a", 0, stripe(1, 100));
        cache.put("b", 0, stripe(2, 100));
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.total_bytes(), 0);
    }

    #[test]
    fn empty_payload_is_not_cached() {
        // A 0-byte stripe (read at/past EOF) must not pollute the cache.
        let cache = StripeCache::new(DEFAULT_STRIPE_CACHE_BUDGET_BYTES);
        cache.put("a", 0, Arc::new(Vec::new()));
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.total_bytes(), 0);
        assert!(cache.get("a", 0).is_none());
    }

    #[test]
    fn concurrent_put_and_replace_keep_accounting_consistent() {
        // Hammer the replace path (same key/index re-put repeatedly) and a churn
        // of fresh keys from many threads against a tiny budget that forces
        // continuous eviction. Without serialized size accounting, the
        // (map mutation, total_bytes delta) RMW pairs race and total_bytes can
        // underflow/wrap; afterwards we assert the invariant that total_bytes
        // exactly equals the sum of the live map's byte lengths and never
        // exceeds the budget.
        use std::thread;

        const BUDGET: usize = 20 * 1000;
        let cache = Arc::new(StripeCache::new(BUDGET));
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..2000u64 {
                    // Half the writes replace a small shared hot set (forces the
                    // insert-returns-Some path); half churn unique keys (forces
                    // eviction).
                    if i % 2 == 0 {
                        cache.put("hot", i % 4, stripe(t as u8, 1000));
                    } else {
                        cache.put("churn", t * 100_000 + i, stripe(t as u8, 1000));
                    }
                }
            }));
        }
        for h in handles {
            h.join().expect("worker panicked");
        }

        // Invariant: the counter matches the live map exactly, and we are within
        // budget. A wrapped/underflowed counter would blow past the budget or
        // mismatch the sum.
        let live: usize = cache.map.iter().map(|e| e.value().bytes.len()).sum();
        assert_eq!(
            cache.total_bytes(),
            live,
            "total_bytes drifted from the live map sum"
        );
        assert!(
            cache.total_bytes() <= BUDGET,
            "over budget: {} > {BUDGET}",
            cache.total_bytes()
        );
    }
}
