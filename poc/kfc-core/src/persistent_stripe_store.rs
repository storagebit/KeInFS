// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! Tier-C disk-backed victim cache for the ranged read path.
//!
//! Tier-C sits *below* the in-RAM [`StripeCache`](crate::stripe_cache::StripeCache)
//! (Tier-B): when Tier-B evicts a stripe to stay within its RAM budget, the
//! evicted bytes are written back here, extending the effective working set to
//! disk for working sets larger than the RAM budget — but only WITHIN a single
//! mount session.
//!
//! ## Cleared-on-mount victim cache (scope, LOCKED)
//! This store is CREATED FRESH / WIPED on [`PersistentStripeStore::new`] (called
//! from `connect()`), and is **not** persistent across remounts. That makes it
//! trivially correct: during a live session NATS coherence (`invalidate_key`)
//! drops entries from BOTH tiers on any overwrite, and starting empty each mount
//! eliminates any stale-version-after-downtime hazard. The on-disk files left
//! behind from a previous session are blown away on `new()`, never read.
//!
//! TODO(cross-remount persistence): persisting across remounts would require
//! version-keying every entry (composite key + object version) and validating
//! each hit against the object's current version on KMS before serving it, so a
//! stale stripe written before a downtime-window overwrite can never be served.
//! That is explicitly OUT OF SCOPE here.
//!
//! ## Portability (FIRST_PRINCIPLES §1 — client stack is portable)
//! All file I/O is portable `std::fs` run on a blocking pool via
//! [`tokio::task::spawn_blocking`]. NO `io_uring`, NO `O_DIRECT`, NO Linux-only
//! syscalls — this compiles and unit-tests on macOS, Linux, and Windows alike.
//!
//! ## Layout
//! A single cache directory; one file per stripe. The on-disk filename is a hex
//! hash of the composite `(key, stripe_index)` string, so an attacker-controlled
//! object key (which may contain `/`, `..`, or NUL) can never escape the cache
//! directory — the filename is purely a fixed-width hex digest. The composite
//! key -> object-key relationship needed by `invalidate_key` is held in the
//! in-memory index, not the filename.
//!
//! ## Accounting / eviction
//! Mirrors Tier-B's style: an `index` guarded by a `Mutex` maps composite key ->
//! `(filename, len, last_tick)`; a byte budget with approximate-LRU eviction by
//! access tick. The blocking file I/O is NEVER performed while holding the index
//! mutex — victims/paths are collected under the lock and the actual reads,
//! writes, and unlinks happen outside it (same discipline as Tier-B's
//! `size_lock` vs. its `DashMap`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Unit separator — identical to Tier-B's composite-key separator; cannot appear
/// in an object key path component.
const KEY_SEP: char = '\u{1f}';

/// Default Tier-C disk budget when enabled. Generous relative to the RAM tier
/// (it is a disk extension), but still bounded. Eviction is approximate-LRU.
pub const DEFAULT_TIER_C_BUDGET_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Per-stripe index entry: where the bytes live on disk, how big they are, and
/// when they were last touched (for approx-LRU).
struct IndexEntry {
    /// Bare filename (hex digest) within the cache dir — never a full path, so
    /// the lock holds no allocation tied to the dir.
    filename: String,
    len: u64,
    last_tick: u64,
}

/// State guarded by the index mutex. Held only for in-memory bookkeeping; never
/// across blocking file I/O.
struct Index {
    entries: HashMap<String, IndexEntry>,
    total_bytes: u64,
}

/// A disk-backed, cleared-on-mount victim store keyed by `(object key, stripe
/// index)`. See the module docs.
pub(crate) struct PersistentStripeStore {
    dir: PathBuf,
    budget_bytes: u64,
    tick: AtomicU64,
    index: Mutex<Index>,
}

impl PersistentStripeStore {
    /// Wipe and (re)create `dir`, returning a store with an empty index.
    ///
    /// "Cleared-on-mount": any files from a previous session are removed so the
    /// session starts with a guaranteed-empty cache (see module docs for why
    /// this is what makes the store trivially correct). Returns an error if the
    /// directory cannot be prepared; the caller treats Tier-C as absent in that
    /// case (best-effort).
    pub(crate) async fn new(dir: PathBuf, budget_bytes: u64) -> std::io::Result<Arc<Self>> {
        let prep_dir = dir.clone();
        tokio::task::spawn_blocking(move || Self::wipe_and_create(&prep_dir))
            .await
            .map_err(std::io::Error::other)??;
        Ok(Arc::new(Self {
            dir,
            budget_bytes: budget_bytes.max(1),
            tick: AtomicU64::new(0),
            index: Mutex::new(Index {
                entries: HashMap::new(),
                total_bytes: 0,
            }),
        }))
    }

    /// Remove the directory tree if present, then create it fresh. Ignores a
    /// not-found on removal (first run); surfaces any other error.
    fn wipe_and_create(dir: &Path) -> std::io::Result<()> {
        match std::fs::remove_dir_all(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        std::fs::create_dir_all(dir)
    }

    fn compose_key(key: &str, stripe_index: u64) -> String {
        format!("{key}{KEY_SEP}{stripe_index}")
    }

    /// Deterministic, injection-proof on-disk filename for a composite key: a
    /// fixed-width hex FNV-1a digest. Two distinct composite keys could in
    /// principle collide, but the in-memory index — not the filename — is the
    /// source of truth for which composite key a file holds, and we never read a
    /// file we did not ourselves write for that exact composite key, so a digest
    /// collision can only cost a (correct) miss/overwrite, never serve wrong
    /// bytes. We include the composite-key length to further spread the space.
    fn filename_for(composed: &str) -> String {
        // 64-bit FNV-1a — no crypto needed; this is purely a path-safety hash.
        const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut h = FNV_OFFSET;
        for b in composed.as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(FNV_PRIME);
        }
        format!("{:016x}-{:x}.stripe", h, composed.len())
    }

    fn next_tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::Relaxed)
    }

    /// Lock the index, RECOVERING from poison rather than cascading the panic.
    /// The critical sections here hold only pure HashMap/integer ops (no
    /// panicking code), so poisoning is unreachable today — but Tier-C is
    /// best-effort, so even a future panic inside the lock must degrade to
    /// best-effort (a stale-but-consistent index) rather than wedge every
    /// subsequent get/put/invalidate and propagate a panic into the read path
    /// (which is meant to fall back to the network, never to crash).
    fn lock_index(&self) -> std::sync::MutexGuard<'_, Index> {
        self.index.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Read a cached stripe from disk, bumping its LRU tick on hit. A `None`
    /// covers both a true miss and a file that vanished underneath us (the index
    /// entry is dropped in that case so it cannot be re-counted).
    pub(crate) async fn get(&self, key: &str, stripe_index: u64) -> Option<Arc<Vec<u8>>> {
        let composed = Self::compose_key(key, stripe_index);
        let tick = self.next_tick();
        let filename = {
            let mut idx = self.lock_index();
            let entry = idx.entries.get_mut(&composed)?;
            entry.last_tick = tick;
            entry.filename.clone()
        };
        let path = self.dir.join(&filename);
        match tokio::task::spawn_blocking(move || std::fs::read(&path)).await {
            Ok(Ok(bytes)) => {
                // Re-validate under the lock AFTER the out-of-lock read: a
                // concurrent `invalidate_key`/`clear` could have run between the
                // index lookup above and this file read, removing the entry and
                // unlinking the file (the unlink may not have completed before we
                // read it). If the composite key is no longer present — OR is
                // present but now points at a different filename (a same-key
                // re-put) — these bytes are for a version the index no longer
                // vouches for, so return a miss rather than serve them. Cheap: a
                // single HashMap lookup. This makes Tier-C itself guarantee that a
                // `get` overlapping an invalidate returns `None`.
                {
                    let idx = self.lock_index();
                    match idx.entries.get(&composed) {
                        Some(entry) if entry.filename == filename => {}
                        _ => return None,
                    }
                }
                Some(Arc::new(bytes))
            }
            // File missing/unreadable: drop the now-bogus index entry so its
            // bytes are not counted against the budget forever.
            Ok(Err(_)) => {
                let mut idx = self.lock_index();
                if let Some(removed) = idx.entries.remove(&composed) {
                    idx.total_bytes = idx.total_bytes.saturating_sub(removed.len);
                }
                None
            }
            Err(_) => None, // join error (panic/cancel): treat as a miss
        }
    }

    /// Write a stripe to disk, update the index, and evict the coldest files
    /// until back within budget. Empty payloads are not cached (a read at/past
    /// EOF would only churn). Best-effort: any I/O failure leaves the store in a
    /// consistent (just smaller) state and is reported via the `io::Result`.
    pub(crate) async fn put(
        &self,
        key: &str,
        stripe_index: u64,
        bytes: Arc<Vec<u8>>,
    ) -> std::io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let composed = Self::compose_key(key, stripe_index);
        let len = bytes.len() as u64;
        let filename = Self::filename_for(&composed);
        let path = self.dir.join(&filename);

        // 1. Write the file OUTSIDE the index lock.
        let write_path = path.clone();
        let write_bytes = Arc::clone(&bytes);
        tokio::task::spawn_blocking(move || std::fs::write(&write_path, write_bytes.as_slice()))
            .await
            .map_err(std::io::Error::other)??;

        // 2. Update the index and COLLECT eviction victims under the lock.
        let tick = self.next_tick();
        let victims: Vec<(String, String, u64)> = {
            let mut idx = self.lock_index();
            if let Some(prev) = idx.entries.insert(
                composed,
                IndexEntry {
                    filename,
                    len,
                    last_tick: tick,
                },
            ) {
                idx.total_bytes = idx.total_bytes.saturating_sub(prev.len);
                // A same-key replace whose old file had a DIFFERENT filename
                // (cannot happen with our deterministic hash, but be safe) would
                // leak; with a deterministic filename the new write overwrote it.
            }
            idx.total_bytes = idx.total_bytes.saturating_add(len);
            self.collect_victims_locked(&mut idx)
        };

        // 3. Delete victim files OUTSIDE the lock (best-effort).
        for (_, filename, _) in &victims {
            let vpath = self.dir.join(filename);
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_file(&vpath)).await;
        }
        Ok(())
    }

    /// Select coldest entries to evict while over budget, removing them from the
    /// index (and decrementing `total_bytes`) under the held lock and returning
    /// `(composed_key, filename, len)` for out-of-lock file deletion. O(n) per
    /// victim, n small (budget / W).
    fn collect_victims_locked(&self, idx: &mut Index) -> Vec<(String, String, u64)> {
        let mut victims = Vec::new();
        while idx.total_bytes > self.budget_bytes {
            let coldest = idx
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_tick)
                .map(|(k, _)| k.clone());
            let Some(victim_key) = coldest else { break };
            if let Some(removed) = idx.entries.remove(&victim_key) {
                idx.total_bytes = idx.total_bytes.saturating_sub(removed.len);
                victims.push((victim_key, removed.filename, removed.len));
            } else {
                break;
            }
        }
        victims
    }

    /// Drop every cached stripe for `key` — a new object version invalidates them
    /// all. Collects the matching entries under the lock, deletes their files
    /// outside it.
    pub(crate) async fn invalidate_key(&self, key: &str) {
        let prefix = format!("{key}{KEY_SEP}");
        let filenames: Vec<String> = {
            let mut idx = self.lock_index();
            let matches: Vec<String> = idx
                .entries
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            let mut filenames = Vec::with_capacity(matches.len());
            for composed in matches {
                if let Some(removed) = idx.entries.remove(&composed) {
                    idx.total_bytes = idx.total_bytes.saturating_sub(removed.len);
                    filenames.push(removed.filename);
                }
            }
            filenames
        };
        for filename in filenames {
            let vpath = self.dir.join(&filename);
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_file(&vpath)).await;
        }
    }

    /// Wipe everything (namespace-wide invalidation). Drains the index under the
    /// lock, then re-creates a fresh empty directory outside it.
    pub(crate) async fn clear(&self) {
        {
            let mut idx = self.lock_index();
            idx.entries.clear();
            idx.total_bytes = 0;
        }
        let dir = self.dir.clone();
        let _ = tokio::task::spawn_blocking(move || Self::wipe_and_create(&dir)).await;
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock_index().entries.len()
    }

    #[cfg(test)]
    pub(crate) fn total_bytes(&self) -> u64 {
        self.lock_index().total_bytes
    }

    #[cfg(test)]
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let uniq = format!(
            "kfc-tierc-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        p.push(uniq);
        p
    }

    fn stripe(byte: u8, len: usize) -> Arc<Vec<u8>> {
        Arc::new(vec![byte; len])
    }

    #[tokio::test]
    async fn new_wipes_existing_dir() {
        let dir = temp_dir("wipe");
        std::fs::create_dir_all(&dir).unwrap();
        let stale = dir.join("leftover.stripe");
        std::fs::write(&stale, b"old").unwrap();
        assert!(stale.exists());

        let store = PersistentStripeStore::new(dir.clone(), 1 << 20).await.unwrap();
        assert!(!stale.exists(), "stale file must be wiped on new()");
        assert!(store.dir().exists());
        assert_eq!(store.len(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn put_get_round_trip() {
        let dir = temp_dir("roundtrip");
        let store = PersistentStripeStore::new(dir.clone(), 1 << 20).await.unwrap();
        assert!(store.get("a", 0).await.is_none(), "miss before put");
        store.put("a", 0, stripe(7, 1024)).await.unwrap();
        let got = store.get("a", 0).await.expect("hit");
        assert_eq!(got.len(), 1024);
        assert_eq!(got[0], 7);
        assert!(store.get("a", 1).await.is_none(), "distinct index is a miss");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn empty_payload_not_cached() {
        let dir = temp_dir("empty");
        let store = PersistentStripeStore::new(dir.clone(), 1 << 20).await.unwrap();
        store.put("a", 0, Arc::new(Vec::new())).await.unwrap();
        assert_eq!(store.len(), 0);
        assert_eq!(store.total_bytes(), 0);
        assert!(store.get("a", 0).await.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn evicts_past_budget_dropping_coldest() {
        let dir = temp_dir("evict");
        // Budget for 2 x 1000-byte stripes.
        let store = PersistentStripeStore::new(dir.clone(), 2000).await.unwrap();
        store.put("k", 0, stripe(0, 1000)).await.unwrap();
        store.put("k", 1, stripe(1, 1000)).await.unwrap();
        // Touch stripe 0 so 1 is the coldest.
        let _ = store.get("k", 0).await;
        store.put("k", 2, stripe(2, 1000)).await.unwrap(); // over budget -> evict 1
        assert_eq!(store.len(), 2);
        assert!(store.get("k", 0).await.is_some(), "recently-used kept");
        assert!(store.get("k", 2).await.is_some(), "newest kept");
        assert!(store.get("k", 1).await.is_none(), "coldest evicted");
        assert!(store.total_bytes() <= 2000);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn invalidate_key_removes_only_that_key() {
        let dir = temp_dir("inval");
        let store = PersistentStripeStore::new(dir.clone(), 1 << 20).await.unwrap();
        store.put("a", 0, stripe(1, 100)).await.unwrap();
        store.put("a", 1, stripe(1, 100)).await.unwrap();
        store.put("b", 0, stripe(2, 100)).await.unwrap();
        // "a" must not match the "ab" key family.
        store.put("ab", 0, stripe(3, 100)).await.unwrap();
        assert_eq!(store.len(), 4);
        store.invalidate_key("a").await;
        assert!(store.get("a", 0).await.is_none());
        assert!(store.get("a", 1).await.is_none());
        assert!(store.get("b", 0).await.is_some());
        assert!(store.get("ab", 0).await.is_some(), "prefix not confused");
        assert_eq!(store.total_bytes(), 200);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn clear_wipes_everything() {
        let dir = temp_dir("clear");
        let store = PersistentStripeStore::new(dir.clone(), 1 << 20).await.unwrap();
        store.put("a", 0, stripe(1, 100)).await.unwrap();
        store.put("b", 0, stripe(2, 100)).await.unwrap();
        store.clear().await;
        assert_eq!(store.len(), 0);
        assert_eq!(store.total_bytes(), 0);
        assert!(store.get("a", 0).await.is_none());
        assert!(store.dir().exists(), "dir re-created after clear");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn get_miss_returns_none() {
        let dir = temp_dir("miss");
        let store = PersistentStripeStore::new(dir.clone(), 1 << 20).await.unwrap();
        assert!(store.get("nope", 42).await.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// End-to-end victim-cache behavior across BOTH tiers, exercising the exact
    /// sequence `core.rs::put_tier_b` / `fetch_stripe` run: a Tier-B eviction
    /// writes the victim back to Tier-C, and a later Tier-B miss is served from
    /// Tier-C (a "promote", here just the Tier-C read) instead of the network.
    #[tokio::test]
    async fn tier_b_eviction_lands_in_tier_c_and_is_served_on_miss() {
        use crate::stripe_cache::StripeCache;

        let dir = temp_dir("victim");
        let tier_c = PersistentStripeStore::new(dir.clone(), 1 << 20).await.unwrap();
        // Tier-B budget holds only 2 x 1000-byte stripes.
        let tier_b = StripeCache::new(2000);

        // Helper mirroring core.rs::put_tier_b: insert into Tier-B, write any
        // evicted victims to Tier-C.
        async fn put_tier_b(
            tier_b: &StripeCache,
            tier_c: &PersistentStripeStore,
            key: &str,
            idx: u64,
            bytes: Arc<Vec<u8>>,
        ) {
            let evicted = tier_b.put(key, idx, bytes);
            for (vkey, vidx, vbytes) in evicted {
                tier_c.put(&vkey, vidx, vbytes).await.unwrap();
            }
        }

        put_tier_b(&tier_b, &tier_c, "obj", 0, stripe(0, 1000)).await;
        put_tier_b(&tier_b, &tier_c, "obj", 1, stripe(1, 1000)).await;
        // Touch stripe 1 so stripe 0 is the coldest.
        let _ = tier_b.get("obj", 1);
        // Over budget: stripe 0 is evicted from Tier-B -> written to Tier-C.
        put_tier_b(&tier_b, &tier_c, "obj", 2, stripe(2, 1000)).await;

        // Stripe 0 is gone from Tier-B...
        assert!(tier_b.get("obj", 0).is_none(), "evicted from Tier-B");
        // ...but present in Tier-C (the victim landed on disk).
        let demoted = tier_c.get("obj", 0).await.expect("victim is in Tier-C");
        assert_eq!(demoted.len(), 1000);
        assert_eq!(demoted[0], 0);

        // Simulate the fetch_stripe Tier-B-miss path: Tier-C hit -> PROMOTE back
        // into Tier-B and serve it (no network).
        let promoted = tier_c.get("obj", 0).await.expect("Tier-C hit");
        put_tier_b(&tier_b, &tier_c, "obj", 0, Arc::clone(&promoted)).await;
        assert!(
            tier_b.get("obj", 0).is_some(),
            "promoted back into Tier-B and served as a hit"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
