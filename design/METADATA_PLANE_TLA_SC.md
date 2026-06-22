<!-- SPDX-License-Identifier: GPL-2.0-or-later -->
<!-- Target-state design proposal. PENDING ARCHITECT RATIFICATION — the FIRST_PRINCIPLES
     wording in §11 is PROPOSED, not yet applied. Branch redesign/decentralized-metadata. -->

# KeInFS Metadata + Allocation Plane Redesign — TLA/SC+ (proposal)

**Status:** proposal for ratification (2026-06-21). **Provenance:** synthesized by a 6-design
adversarial design tournament (lenses: computed-placement, sharded-replicated-index,
log-structured, client-authoritative, size-tiered, minimal-evolution), each independently
generated, attacked on scalability/correctness/operability, scored by a jury, and merged.
Jury ranking: **TLA/SC (78) > KCM computed-map (67) > SPLIT two-plane (64) > KP2A (56) >
KOX (54) > KLP (53)**; the winner grafts the best of KCM/SPLIT/KP2A and neutralizes the
critiqued flaws. **Independently cross-validated** by a 104-agent deep-research study of
Lustre/BeeGFS/GPFS/Ceph/DAOS (see Appendix A). Grounded in the measured 30 TB-lab failure
data (central-allocator 270 reserve/s ceiling; reservation-pool 5× churn; FDB 2.62 GB
logical → 139 GB disk ≈ 55× amplification; the write-vs-commit reaper-frees-committed-granule
race; small-object metadata domination).


# KeInFS Metadata + Allocation Plane — Recommended Design (TLA/SC+)

**Decision:** Adopt **Target-Local Allocation + Shrunk-Core (TLA/SC)** as the spine, grafting four ideas from the radical designs and neutralizing every fatal flaw the jury raised. Codename **TLA/SC+**. This is the *right* thing for KeInFS precisely because the measured failure points at one surgical inversion (occupancy → target, delete central allocator + per-fragment keys) rather than a from-scratch distributed DB.

The one-sentence thesis: **occupancy is a target-local fact established at the durable write; the central plane stores only bounded, slow-churn truth (heads + immutable manifests + a tiny inventory/topology core) in a shrunk FoundationDB; everything that was O(fragments) leaves the core and is either recorded in the per-object manifest or owned target-locally in KIX.**

---

## 1. Core architecture in one diagram-in-words

```
                  ┌─────────────────────────────────────────────┐
   compute        │  KSC (smart client)                          │
   placement ◄────┤   • caches: EC profile, inventory(~2k rows), │
   (advice)       │     topology_epoch, manifests                │
                  │   • KEE 8+2 encode                            │
                  │   • assembles manifest from write responses   │
                  └───┬──────────────────────────────┬───────────┘
       KP2 (h2) data  │                                │ KP2M (gRPC, control)
   ┌──────────────────▼────────┐            ┌──────────▼─────────────────────┐
   │ KST (one per drive)        │            │ KMS (shrunk metadata service)   │
   │  • self-allocates granule  │            │  • namespace/bucket/EC-profile  │
   │  • write+fdatasync+publish │            │  • object HEAD (mutable ptr)    │
   │  • returns (target,granule,│            │  • immutable per-version MANIFEST│
   │     generation) in 201     │            │  • per-target REVERSE LOG (grafted)│
   │  • GUARD D (hard backstop) │            │  • single-shot CommitObject     │
   │ ┌────────────────────────┐ │            │  • GC-by-reachability sweep     │
   │ │ KIX (RAM + raw arena)  │ │            └──────────┬──────────────────────┘
   │ │  • chunk_id→location   │ │                       │ FoundationDB (SHRUNK)
   │ │  • free-extent map     │ │            ┌──────────▼──────────────────────┐
   │ │  • granule→chunk inverse│ │            │ heads + manifests + reverse-log  │
   │ │  • self-describing hdr++│ │            │ + inventory/topology (~2k rows)  │
   │ └────────────────────────┘ │            │ NO fragment-index, NO reservations│
   └────────────────────────────┘            │ NO occupancy markers              │
                  ▲                            └───────────────────────────────────┘
                  │ NATS: inventory/topology invalidation + manifest invalidation
   KRS (rebuild/rebalance) — leases tasks from KMS reverse-log, KEE-reconstructs, writes via KST
```

**KAS is dissolved.** Its only surviving function (inventory + failure-domain labels + capacity hints) becomes the tiny **KIR** (KeInFS Inventory Registry), folded into KMS as a ~2000-row table. There is no central allocator, no reservation, no per-shard mutation lease, no reaper.

---

## 2. Allocation — target-local

Free space lives **entirely in the KST/KIX arena**, never in any central store.

- KIX gains a **free-extent map** (a run-list / coalesced-extent structure, *not* a flat per-granule bitmap — see §13 for the RAM math) over the drive's granule space, persisted as deltas on the **same append+fdatasync path** that already publishes locations.
- On a KP2 write the KST self-selects the next free granule under its **existing slot-scoped publication guard** plus a new **per-arena allocator latch**. *(Critique-fix: the jury correctly flagged that self-allocation is not "free" — it adds a per-drive allocation point coarser than the per-slot guard. We accept and bound it: the latch protects only the extent-map mutation, ~tens of ns, and 5000 clients fan out over ~2000 drives = avg <3 concurrent writers/drive. This is two orders of magnitude under the deleted 270/s central ceiling. It is measured in Phase 1, not assumed.)*
- The granule is marked occupied in KIX **atomically with the location upsert** via a **single combined KIX delta record** `{chunk_id, location, granule, generation, occupied}` — one fdatasync, one log entry. *(Critique-fix: the prior designs left free-map and location-map as two separately-synced structures with a crash window between them. We make them one delta so recovery never sees "written but free" or "free but occupied".)*

Occupancy is established at the write, by the only party that physically knows. This is the distilled lab lesson and the safety backbone.

---

## 3. Placement — computed *advice*, manifest is *authority* (the chosen tradeoff)

**Decision: hybrid. Placement is COMPUTED for writes and rebuild; LOOKED-UP (from the manifest) for reads.** This is the deliberate KeInFS-specific choice and it is the single most important correctness decision in the note.

Why not pure-computed (KCM/KLP):
- Pure CRUSH **degrades to looked-up at high fill** (the exact 30 TB regime), because a computed target may be full and the client has no free-space truth.
- Pure-computed reads after rebalance/rebuild **diverge from physical truth** unless you migrate committed data on every epoch bump — an unbounded rebalance tax.

Why not pure-lookup (old KAS reserve):
- That is the measured 270/s ceiling.

The hybrid:
1. **Write side:** KSC computes a deterministic, failure-domain-aware **candidate ordering** via rendezvous/HRW hashing over the cached inventory (weighted by capacity hint). It writes to the first k+m=10 candidates that accept; a full/slow/dead target returns a **typed rejection** and the client walks to the next candidate. Computation is a *hint that avoids a round-trip*; it is never authority.
2. **Read side:** the manifest records the **actual** `(target_id, granule_index, generation)` returned by the writes. Reads use the manifest, never recompute. This makes overfull/dead-target fallback and rebalance divergence safe **for free** — the jury's "computed reads degrade" wall simply does not exist because we never claim computed reads.

Failure-domain spreading is enforced client-side at candidate construction (distinct domains for the 10 fragments) **and audited server-side** (§9) so a buggy client cannot silently under-protect a stripe.

Rebalance: existing objects are pinned to their immutable manifests and **never move on topology change** — a target add only steers *new* writes (HRW minimal reshuffle). Target *loss* triggers KRS rebuild, not a global reshuffle. *(This is the right trade for a write-churn AI workload: we pay rebuild cost only for actual data loss, and accept that a freshly-added drive fills from new writes + explicit drain, see §14 Q4.)*

---

## 4. Object/fragment index — where, sharding, replication

Two levels, neither is an O(fragments) central keyspace:

**(a) object → fragment: the immutable per-version MANIFEST.** Lives in KMS/FDB, keyed `(namespace, bucket, key, version)`, sharded by namespace (existing one-namespace→one-shard rule, with path-range splitting promoted to mandatory — §8). Records per stripe: `epoch + [(target_id, granule_index, generation, fragment_crc) × 10] + ec_profile_id + logical_len`. Size is O(stripes), not O(fragments-in-cluster).
- **Large-object manifest chunking (critique-fix):** a manifest exceeding a size bound (e.g. 256 KiB ≈ ~6k fragment tuples) is split into **manifest segments** keyed `(…, version, segment_idx)`, committed in one FDB transaction when they fit under the 10 MB txn limit, or as an **append-then-seal** sequence with the head flip as the atomic seal for multi-txn giants. A 100 GB checkpoint (~100k fragments, ~4 MB) is one txn; a multi-TB checkpoint seals across segments. The head pointer flip is the single linearizable commit point regardless.

**(b) fragment → location:** has **no separate central structure** for the read path — location *is* in the manifest. KIX is the authoritative per-drive `chunk_id → location` map (already exists, RAM-resident).

**(c) reverse "what lived on dead target T?" — GRAFTED from the critiques' top salvage.** This is the flaw that sinks TLA/SC's naive form (full-manifest-scan rebuild) and KCM/KLP entirely. We **do not delete the reverse index; we relocate it cheaply**:
- KMS keeps a **per-target append-only Reverse Log**: on every CommitObject, append `(target_id) → {object_id, version, stripe, frag, generation}` for each of the 10 fragments. This is O(fragments) in *count* but it is **append-only, never updated, never read on the hot path, range-scannable by target_id**, and partitioned per target. It compacts trivially (tombstone on delete/supersede). It is *not* the old churny secondary index — there are no per-fragment *mutations*, no occupancy markers, no reservations.
- This restores today's bounded property: `ReportTargetFailure → rebuild tasks` is **O(fragments-on-that-target)**, a single range scan, not O(all-objects). Cost: ~1 extra append per fragment at commit (batched into the same FDB txn). Footprint: see §13 — it roughly doubles the central fragment-key count vs pure-TLA/SC, but it is append-only KV, which is exactly what FDB+Redwood handles without the 55x churn-amplification that killed the old reservation families.

Replication: FDB's own replication for all of the above.

---

## 5. Write path + per-object atomic commit + **proof of no write-vs-commit race**

**Protocol:**
1. KSC: `KP2M.BeginObject(ns,bucket,key)` → `{object_id (server-minted, monotonic), ec_profile, topology_epoch}`. One cheap RPC; **nothing central is mutated** (no write-intent row).
2. KSC KEE-encodes each stripe, computes candidates, issues 10 parallel KP2 PUTs. `chunk_id = H(object_id, version, stripe, frag)` — **content-version-scoped but NOT reused across physical locations** (see race-fix below).
3. Each KST: self-allocate granule → write payload → **fdatasync** → write the combined KIX delta (location+occupancy+granule+generation) → **fdatasync** → return `{target_id, granule_index, generation, fragment_crc, durable=true}` in the 201. **The granule is occupied the instant the response is sent.**
4. KSC assembles the manifest from the 10×N responses.
5. KSC: `KP2M.CommitObject(object_id, manifest)` → **one FDB transaction** that (a) writes the immutable manifest segment(s), (b) appends the per-target reverse-log entries, (c) **CAS-flips the object head** to this version conditional on the prior head. Per-object atomicity — an object store needs nothing more.

**Why there is NO write-vs-commit race (the heart of the note):**

The lab bug was: physical occupancy true at WRITE, durable occupancy record written LATER at central COMMIT, a central reaper freed the granule in the lag window. We eliminate the lag *structurally* by making three guarantees:

- **G1 — occupancy and durability are the same event at the same place.** A granule is occupied in KIX (durably, target-local) at step 3, *before any response*. No central record is consulted to establish occupancy. There is no central authority that can free it.
- **G2 — only the owning target frees, and only after proving non-reference, with a generation fence.** GC (§9) never frees a granule that any committed manifest references. The free decision is local and gated by **Guard D** (a granule→chunk inverse check that hard-rejects any write to a granule holding a live committed chunk of a different chunk_id). Guard D is built **first**, before any allocator change (it does not exist today — verified at chunk_media.rs; the inverse index is the keystone).
- **G3 — the GC grace window is fenced by a per-object LEASE HEARTBEAT, not a blind TTL.** This is the critique-fix that kills the relocated-reaper race that every other design left open. `BeginObject` returns a short-lived **write lease** (object_id + expiry). While a client is actively writing/committing it **heartbeats the lease** to KMS (cheap, batched). GC may reclaim an uncommitted granule's space **only if** (a) no committed manifest references it, **AND** (b) the object_id's lease is *expired and not heartbeating*, **AND** (c) the granule is older than a grace window. A slow-but-alive client holds its lease, so its in-flight granules are never reclaimed regardless of wall-clock duration. A *dead* client's lease expires and its orphans become collectable. **This converts the unbounded-write-duration TTL guess (the mechanism that failed in the lab) into a liveness signal.** A multi-hour checkpoint that keeps heartbeating is safe; a crashed client is reclaimed promptly.

**The decisive race, walked through and closed:**
> t1 KST self-allocates G, writes chunk_id=H(v1,…), fdatasyncs, returns durable. t2 client stalls. t3 GC sweep on the target: is G referenced by a committed manifest? No. Is object_id v1's lease expired? **NO — client is heartbeating.** → GC skips G. t4 client resumes, CommitObject lands referencing G. ✔ No free, no reuse, no loss.
>
> Alternative: client *crashes* at t2. Lease expires at t2+lease_ttl. GC at t3>expiry+grace: no manifest ref AND lease dead AND old → free G locally. The commit will never come (client is gone). ✔ Correct reclaim, no live data freed.

**chunk_id reuse fix (grafted from KP2A/KOX critiques):** on rebuild/re-placement KRS writes the *same logical fragment* to a *new* target/granule. To avoid two live copies of one chunk_id, the rebuilt fragment carries a **bumped generation** and the manifest is updated `(target,granule,generation)` in a single CAS; Guard D + generation fence make a stale-location read fail **loud** (version/generation mismatch → client re-resolves or KEE-reconstructs), never silently wrong.

**Crash consistency:** a crash before CommitObject = orphan granules, reclaimed via the lease-fenced GC. A crash mid-commit = FDB serializability (manifest+reverse-log+head flip are one txn; either fully visible or not). No write-intent row needed; the lease *is* the in-flight record, and it lives in RAM/short-lived KV, not as O(objects) durable churn.

---

## 6. Read path

1. `KP2M.ResolveObject(ns,bucket,key)` → immutable manifest (cached hard; immutable per version, only the head pointer moves; NATS invalidates on new version).
2. KSC reads the k data fragments directly over KP2 from the manifest's named `(target, granule, generation)`. ≤2 missing → pull parity, KEE-reconstruct.
3. Endpoint resolution: `target_id → endpoint` from the cached inventory. **Critique-fix:** inventory carries a **monotonic version**; every KP2M call and every KP2 read attaches the client's inventory version, and a target/KMS returns a `stale-inventory` typed error forcing a pull-refresh, so a missed NATS message can never cause a silent read from a dead endpoint.

One metadata round-trip (often zero, cache-served). Identical in shape to today's read path.

---

## 7. The minimal strongly-consistent core — **FDB stays, shrunk**

**Recommendation: keep FoundationDB; do not hand-roll consensus.** The jury reasoning is decisive here and the repo evidence backs it: the 139 GB / 55x amplification was driven by per-fragment reservation/occupancy **write churn**, which this design deletes. A HEAD + append-only-manifest + append-only-reverse-log workload is exactly what FDB does well. Building ~400–1024 Raft groups (KCM/KOX/KLP) replaces a measured-and-survivable problem with an unbounded *implementation-maturity* problem on the consistency-critical path — and co-resident Raft fsync on the data NVMe directly attacks the unique direct-path bet.

The core holds, all **bounded or append-only**:
- namespace / domain / bucket records, immutable EC-profile bindings — O(buckets).
- object **heads** (mutable pointer, key→current version) — O(objects), the irreducible name floor.
- immutable **manifests** (segmented) — O(versions), append-only.
- per-target **reverse log** — O(fragments) in count but append-only, range-scanned only on failure.
- **KIR**: inventory/topology/epoch — ~2000 rows, NATS-fanned.

**What LEFT FDB forever:** free spans, reservations, reservation bins, the mutation lease, the reaper, the per-fragment *secondary* index, committed-occupancy markers, write intents. These were the firehose.

---

## 8. Listing

Object heads are stored lexicographically by `(namespace, bucket, key)` in the namespace shard → prefix listing is a native FDB `getRange`, O(objects-in-prefix), touching only heads (never manifests/fragments/targets).

**Critique-fix (this is a *required* feature, not deferred):** path-range splitting is promoted from "planned" to **day-one mandatory**, because a single AI dataset bucket with billions of objects pins one shard otherwise. A bucket auto-splits by key-prefix range across shards when its head-count or write-rate crosses a watermark; `ShardMapEntry` already models prefix-start/end. Pagination continuation token = `(shard, last_key)`; the split protocol freezes the split-point, copies the upper range to the new shard, then flips the shard-map atomically, with continuation tokens validated against the shard-map version to avoid skip/duplicate across a mid-list split. This is the one genuinely new distributed-systems primitive we *must* build; it is far smaller than a whole Raft fleet and is scoped to range-split of an FDB-backed shard, not generic consensus.

---

## 9. Garbage collection

Two classes, both target-paced, neither a central reaper-with-a-cluster-lock:

**(1) Orphaned chunks (object never committed / aborted / superseded).** Each KST periodically enumerates KIX chunk_ids and asks KMS, batched, the **lease-fenced reachability query** of §5/G3: free a granule iff `(no committed manifest references it) AND (its object_id lease is expired) AND (older than grace)`. The free is a local KIX delta.
- **Critique-fix for the "GC has no reverse lookup" wall:** the reachability query is served by the **per-target reverse log** — KMS answers "is `(target,granule,generation)` referenced by a live head?" by a range scan of *that target's* reverse-log segment, not a full-manifest scan. The query is O(fragments-on-target-since-last-GC) with a generation cursor, paced and budgeted against a per-shard GC-RPC rate limit so it never starves ResolveObject.

**(2) Deleted/superseded versions.** Delete clears the head and tombstones the manifest version + its reverse-log entries; the referenced targets free those granules on the next sweep. Immutable manifests make this a clean reachability check (head lineage decides live vs orphan — the version-lineage the KP2A/KOX critiques flagged as missing is supplied by the head's version pointer).

**Packed small-object GC (see §13):** a shared container granule is freed only when **all** packed members are unreferenced. The per-container **refcount lives in KIX-local container state** (keyed by container_id, decremented on member delete), *not* in an immutable manifest — resolving the "shared occupancy vs target-local fact" contradiction the SPLIT critique raised. Container compaction (rewrite live members, retire the container) is a KRS background job with a rate budget; capacity-leak vs compaction-throughput is an explicit monitored SLO (§14 Q5).

---

## 10. Component fates

- **KAS — DISSOLVED.** Allocator/lease/reservation/free-span/reaper code deleted. Surviving function (inventory + failure-domain labels + capacity hints) → **KIR**, a ~2000-row table in KMS, NATS-fanned. Placement selection → a shared client-side HRW library.
- **KMS — KEPT, shrunk.** Namespace/bucket/EC-profile/head/immutable-manifest service over the shrunk FDB core. Gains: single-shot `CommitObject`, the per-target reverse log, the lease-fenced GC sweep, write-lease issue/heartbeat, mandatory path-range split. Loses: write-intents, reservation orchestration, the per-fragment secondary index, committed-occupancy markers.
- **KST — KEPT, grows the allocator.** Self-allocates granule, returns `(target,granule,generation)` in the KP2 201. Hosts **Guard D** (hard backstop).
- **KIX — KEPT, promoted.** Authoritative per-drive owner of free-extent map + occupancy + the **granule→chunk inverse** (for Guard D and for the free-map to learn supersession) + container refcounts. Single combined delta record. Self-describing slot header **widened** to carry `(object_id, version, stripe, frag)` (media format bump — verified there is header space but it needs a real format change; this is the keystone for media-rebuild discovery and for GC identity).
- **KSC — KEPT, grows.** Client-side HRW placement, manifest assembly from write responses, single-shot CommitObject, write-lease heartbeat, small-object packing buffer. Talks KMS for control, KST for data, **no allocator** (none exists).
- **KRS — KEPT.** Discovers work via the per-target reverse-log scan (bounded), computes replacement placement from the HRW library + inventory (avoiding the surviving 9 fragments' domains), KEE-reconstructs, writes via KST with bumped generation + manifest CAS. Also runs container compaction and any explicit drains.
- **KEE — unchanged library.** **keinctl/proto** — owns the extended KP2 write-response framing (in `poc/kp2`) and the new KP2M control RPCs.

---

## 11. FIRST_PRINCIPLES changes (exact wording)

**Principle 10 — rewrite.** Current: *"KMS owns namespace truth, KAS owns placement truth … Allocator ownership is partitioned by `allocation_shard_id`."* New:

> **10. KMS owns namespace and manifest truth; placement is computed by clients as advice; occupancy is a target-local fact.**
> KMS owns namespace/bucket/EC-profile/object-head/immutable-manifest truth. There is no central allocator. Placement is computed by the smart client from the cached inventory and EC profile as *advice*; the per-object manifest records the *actual* chosen targets and is the sole read authority. Free space and occupancy are owned by the KST/KIX that physically holds the data, established atomically at the durable write. KSC talks to KMS for control and to KST for data; it never contacts an allocator (there is none). The `allocation_shard_id` partitioning and the per-shard mutation lease are deleted.

**New principle (add as 14, renumber POC clauses).**
> **14. Occupancy is a target-local fact.** The durable record that a granule is occupied is owned by the KST/KIX that holds the bytes and is established by the same fdatasync that makes the data durable. No record outside the target may be authoritative for whether a granule is occupied. A granule is freed only by its owning target, only after proving no committed manifest references it and the writing object's lease is dead. (Distilled from the 30 TB-fill reaper-frees-committed-granule incident.)

**Principle 9 / KP2 — extend (no rewrite needed).** Note that the KP2 write **response** now carries `(target_id, granule_index, generation, fragment_crc)` and the on-media slot header carries `(object_id, version, stripe, frag)`; both framing changes live in `poc/kp2` / KIX media format, consumed everywhere. A sibling **KP2M** control gRPC (Begin/Commit/Resolve/List/ReachabilityQuery/LeaseHeartbeat/MapEpoch) is added — control-plane, so Principle 2 (data is KP2-not-gRPC) holds.

**Principle on the FDB substrate — clarify, not drop.** FDB remains the backend metadata substrate; it now stores only bounded/append-only families. Principle 1's mention of the FDB client as a Linux backend primitive is unchanged.

No change to one-drive-one-target, raw-device direct-I/O, direct native path, KEE-as-library, observability — all hold and are strengthened (target-owned space is the purest locality).

---

## 12. Phased migration (each phase lab-validatable; POC §17 = reformat freely)

- **Phase 0 — keystone + instrument (no behavior change).** Build **Guard D** (KIX granule→chunk inverse) and the widened self-describing slot header. Add KP2 write-response allocation fields (carried, ignored). *Validate Guard D fires on conflicting-chunk overwrite; validate media rebuild reads the new header.* **This lands the safety backstop before anything is deleted** — exactly the COMMITTED_OCCUPANCY phasing argument.
- **Phase 1 — target-local allocation (flagged).** KIX free-extent map + combined delta; KST self-allocates and returns the granule; KAS reserve becomes a no-op stub; KSC assembles the manifest from responses but **still** calls the existing KMS commit. *This alone deletes the 270/s ceiling and the 1456/s reaper churn — measure it on the 12-target EPYC box.*
- **Phase 2 — single-shot commit + lease.** Introduce `BeginObject`/write-lease/heartbeat and single-shot `CommitObject`; KMS stops writing write-intents/reservations/committed-occupancy markers and the per-fragment secondary index; **add the per-target reverse log**. Lease-fenced GC replaces any reaper.
- **Phase 3 — client placement + dissolve KAS.** Ship HRW `place()` + inventory/topology in KIR; KSC computes candidates with typed-rejection fallback; delete KAS service entirely; KRS sources work from the reverse log and computes replacement placement.
- **Phase 4 — listing split + small-object packing.** Promote path-range split to mandatory; ship packed-container small-object path + KIX container refcounts + KRS compaction.
- **Phase 5 — shrink FDB families / hardening.** Drop the dead FDB families, finalize manifest segmentation for multi-TB objects, capacity-aggregation observability.

Each phase wipes drives (POC §17) — no compat shims.

---

## 13. Scaling math — 25 PB / 5000 clients / small objects

**Large-object regime (1 MiB fragments, 8+2):** ~31e9 fragments. Old plane: ~25–31B fragment-index keys + ~equal occupancy markers → 55x FDB death. New plane: **zero central per-fragment *mutable* keys.** Fragment facts live in KIX free-maps + manifests; the only O(fragments) central data is the **append-only reverse log** (~31e9 entries × ~48 B ≈ 1.5 TB logical, 3x replicated, append-only so no churn amplification, range-scanned only on failure). Manifests = O(stripes): a 100 GB checkpoint ≈ 4 MB manifest (segmented), one commit.

**Per-target KIX RAM (critique-fix — this was mis-sized 1–2 orders of magnitude in the radical designs):** be honest. A 15.36 TB drive at 1 MiB fragments ≈ 15e6 chunks. Three RAM structures: `chunk_id→location` (~64 B), `granule→chunk inverse` (~16 B), free-extent map (coalesced run-list, *not* per-granule — KBs to low MB if not pathologically fragmented). ≈ **1.2–1.6 GB RAM/drive**, ~15–20 GB on a 12-drive node. This is real and budgeted. **Small objects make this explode unless packed** — hence packing is mandatory, not optional.

**Small-object regime (128 KiB) — the hard case.** 25 PB ≈ **1.9e11 objects**. Honest accounting (the jury's correction to TLA/SC's optimism):
- **Packing is load-bearing:** a 128 KiB object is sub-stripe. KSC packs ~64 objects into one 8+2 container stripe via the existing packed KP2 path. KIX then tracks **containers** (~25e6 of them), not 1.9e11 sub-fragments → per-drive KIX RAM stays bounded by *fragment* count, ~1.5 GB/drive, not 48 GB. The container refcount lives KIX-local (§9).
- **The irreducible floor remains the head/manifest count:** 1.9e11 heads. At ~64 B that is ~12 TB logical; manifests for packed objects are tiny tuples `(container_id, offset, len, crc)`. **This needs ~1000–2000 namespace/path-range shards** to keep per-shard key counts in the few-hundred-million range — *and we say so explicitly* (TLA/SC under-counted; we don't). But these are **FDB shards, not Raft groups** — adding FDB shards is operationally routine, and the workload is heads (mutable but single-key CAS) + append-only manifests/reverse-log, not the churn that amplified. The reverse-log adds ~1.9e11 append-only entries; tombstoned-and-compacted, this is the dominant central family and must be monitored.
- **Commit rate:** filling 25 PB of 128 KiB in 30 days ≈ 77k object-commits/s. Single-key head-CAS across ~1000 shards = ~77/s/shard average. **The real risk is a hot tenant on one shard** — neutralized by mandatory path-range split (§8) and by **KP2M MultiCommit** batching N packed-object manifests + N head-CAS into one FDB txn per shard, amortizing the commit the way the packed data path amortizes fsync.

**Ops/s & 5000 clients:** writes touch no central allocator; data path is NIC/media-bound (~2.8 GiB/s/target, unchanged). Each client computes placement locally (zero central cost) and hits KMS only for Begin/Commit/Resolve (cacheable, batchable). The deleted 270/s lease ceiling is gone; the new ceiling is FDB commit throughput, scaled by adding shards.

**Failure blast radius:** lose one drive → 1/2000 of fragments, all KEE-rebuildable; **rebuild discovery is O(fragments-on-that-target)** via the reverse log (the critical property restored). Lose one FDB shard → only that namespace/prefix-range's control ops stall; already-written data stays readable via cached immutable manifests. No single lease whose stall freezes all writes (the old blast radius is gone).

---

## 14. Top open questions for the human architect

1. **Write-lease heartbeat cost & cardinality.** At 5000 clients × many concurrent in-flight objects, what is the lease-heartbeat RPC volume and where does lease state live (RAM-only in KMS? short-TTL FDB?)? The race-elimination proof (G3) depends on this being cheap and reliable. **Needs the deep-research cross-check** against lease/lock-lease designs (how others bound heartbeat fan-in without re-creating a central hot path).
2. **Reverse-log footprint at 1.9e11 entries.** Confirm append-only + tombstone-compaction keeps FDB out of the 55x regime *for this family specifically* — it is the one surviving O(fragments) central structure. Needs a measured FDB Redwood amplification test on an append-only, range-scanned, periodically-tombstoned keyspace before Phase 2 commits to it. **Flag for deep-research + a real FDB micro-benchmark.**
3. **Per-arena allocator latch under max fan-in.** Phase 1 must *measure* the per-drive allocation latch contention at realistic concurrent-writers-per-drive, not assume it. If a hot computed-candidate set funnels many clients onto one drive, does the latch + typed-rejection fallback stay sub-millisecond?
4. **Capacity rebalance with no global allocator.** Pinned manifests mean a freshly-added drive only fills from new writes; near-full clusters can't auto-relieve. We propose explicit KRS-driven *drain* of near-full drives, but the drain trigger, rate-governor, and interaction with HRW weights need design. This is the weakest operational axis and the one the central allocator used to cover.
5. **Container compaction throughput vs small-object delete rate.** The shared-container model imports LSM-style write amplification; prove compaction keeps pace at delete-heavy AI churn, with a monitored capacity-leak SLO, or packing becomes a capacity death-spiral.
6. **Path-range split correctness under monotonic insert.** AI jobs write `shard-00000..99999` sequentially → the split point chases the write frontier. Validate the freeze-copy-flip protocol and continuation-token stability under sustained monotonic insert (the historically hardest case).
7. **Slot-header media format bump.** Widening the header to carry `(object_id, version, stripe, frag)` is a one-time format change with a per-read decode cost on the hot path; confirm the read-path header-validate budget tolerates it and that the CRC coverage extends correctly.

**Bottom line:** TLA/SC+ keeps the measured root-cause fix (occupancy → target, delete central allocator + per-fragment churn, keep shrunk FDB) and grafts exactly four things to close the fatal flaws every radical design left open: **(1) Guard D + generation fence built first**, **(2) a lease-fenced GC that converts the TTL guess into a liveness signal — the only design here that genuinely eliminates rather than relocates the reaper race**, **(3) a cheap append-only per-target reverse log so rebuild/GC discovery stays O(fragments-on-target)**, and **(4) mandatory packing + path-range split + MultiCommit for the small-object floor**. It is simultaneously the *right* thing and nearly the least disruptive — because the data pointed at a surgical inversion, not a rewrite.
---

## Appendix A — cross-check against the deep-research (Lustre/BeeGFS/GPFS/Ceph/DAOS)

An independent 104-agent, 22-primary-source study (24/25 claims survived 3-vote adversarial
verification) corroborates this design:

- **No production PB–EB system uses one central transactional DB for both metadata and
  allocation.** The recurring shape — *small strongly-consistent core + decentralized
  (target-local) allocation + computed placement + sharded data-location metadata* — is exactly
  TLA/SC+. **DAOS is the closest precedent** (tiny Raft pool-metadata service + per-target
  Versioning Object Store + algorithmic placement); TLA/SC+ keeps **FDB (shrunk)** instead of
  hand-rolling Raft, justified because the family that caused the 55× amplification (per-fragment
  reservation/occupancy churn) is *deleted*, leaving only bounded/append-only families FDB serves well.
- **GPFS is the canonical proof** (Schmuck & Haskin, FAST '02, text-verified) that free-space
  allocation needs no central serialization: per-region locks + a *loosely-consistent capacity
  HINT manager*, not an allocator-of-record. Maps onto target-local allocation (1 drive = 1 KST =
  1 KIX = 1 allocation domain) + the KIR capacity hints used for HRW weighting.
- **Computed placement is *advice*, not a silver bullet.** A 2013 Ceph claim that CRUSH
  "eliminates bottlenecks and scales linearly" was **refuted 0-3** — computed placement still
  needs (a) a cached consistent cluster map (→ our KIR core) and (b) a balancer (→ our KRS drain).
  This is precisely why §3 makes placement *computed-for-writes, manifest-authoritative-for-reads*.
- **Atomic commit without a global txn DB** (Ceph PG-primary + `last_complete` log; DAOS
  epoch-MVCC) confirms the principle that *the manifest/head commit is the sole linearization point
  and un-finalized fragments are invisible* — §5's race proof is the KeInFS-native realization
  (client-assembled manifest + single-shot CommitObject head-CAS; no per-stripe coordinator hop,
  honoring "coordinators never relay").

**How this design answers the research's four open questions:** (1) per-object commit model →
client-assembled-manifest + single-shot head-CAS (a third option, neither PG-primary nor a global
epoch service); (2) consistent listing → mandatory path-range split + getRange on heads +
shard-map-versioned continuation tokens (§8); (3) rebalancing cost → pinned manifests never move +
KRS-driven drain (§14 Q4, flagged as the weakest axis); (4) BeeGFS gap → not load-bearing for this design.

*Full cited research + this tournament's six designs and critiques are archived in the session
transcript; key sources: GPFS FAST '02, Ceph architecture/CRUSH docs, DAOS storage/transaction docs.*

---

## Appendix B — refinements (2026-06-21)

### B.1 Phase 0 scoping corrections (Phase 0 is smaller than first scoped)
- **Guard D already survives restart.** `build_slot_publications` rebuilds slot ownership from
  KIX-recovered `(chunk_id, location)` entries at boot (which KIX recovers by replaying arena
  deltas). So Phase 0's deliverable is the *direct* KIX `(drive,granule)→chunk` **inverse index**
  + the self-describing slot header — NOT a restart-durability fix (there is no restart hole).
- **The KP2 write reply already carries the allocation result.** `PackedWriteReplyEntry.location`
  (serialized in the ACK entry by `encode_write_reply`) already returns `drive_id`(=target),
  `slot_index`(=granule), `generation`, and `checksum`(=fragment CRC). So "extend KP2 to carry the
  allocation result so the client assembles the manifest" is **already satisfied** — the client can
  build the manifest from existing write replies. (`target_id` is `u16` today; widening to `u32`
  for >65 k targets is a later, separate concern.)

### B.2 Capacity: drop the 2× publication-lane allocation (future phase; keep 8+2 EC now)
Today `CHUNK_MEDIA_PUBLICATION_LANES = 2` — every logical granule is formatted with two physical
lanes (`physical_slot = slot_index*2 + lane`), so a 1 MiB fragment consumes ~2 MiB of media
(confirmed by the lab's ~1.96 MB/granule sizing). With rs-8-2 (1.25×) that is only **~40% raw→usable
efficiency** (2× lane × 1.25× EC). The two-lane scheme exists for atomic *in-place rewrite* +
torn-write protection during concurrent read.

In the redesign both are redundant: durability is EC's job (8+2 survives 2 losses), and write
atomicity moves to the immutable-manifest commit — writes go to a **fresh granule**, a generation
bump (rebuild) writes a **new** granule + manifest CAS, so a live fragment is never overwritten in
place while being read. With that fresh-granule / no-in-place-rewrite invariant (already enforced by
Guard D + the inverse index), the second lane buys nothing.

**Decision (2026-06-21):** keep rs-8-2 EC + 2 lanes for now (Phase 0 stays additive). **Add a later
phase: single-lane media** (`CHUNK_MEDIA_PUBLICATION_LANES 2→1`) — reclaims ~50% (efficiency
~40%→~80% with 8+2), **gated on the fresh-granule write invariant from Phases 1–3**. "Fortress"
durability = an explicit replication policy (mirrored placement / a replicated EC profile), NOT a
silent 2× media tax on all data. (Media-format + write-path behavioral change → its own
format-version bump + reformat; fine under POC §17.)
