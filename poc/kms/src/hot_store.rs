// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::store::{
    BucketWriteContext, CommittedObjectWrite, CommittedObjectWriteWindow, DeletedObject,
    ReservedObjectWriteWindow, TimedStoreResult,
};
use keinctl::proto::{
    EcProfile, FragmentRef, ObjectHead, ObjectVersionManifest, PlacementReservationRecord,
    StripeManifest, TargetRecord, WriteIntent, WriteIntentState,
};
use tonic::Status;

/// Outcome of a lease-fenced reclaim authorization for one occupied granule that a target
/// reported. Only `Authorized` (and `AlreadyReclaimed`, an idempotent re-attempt) permit
/// the physical free of the granule; the others are reasons to leave it alone.
// The variants are constructed only by the FoundationDB (Linux) store impl; a non-Linux
// host build sees the stub (which never builds them) but still matches on them.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OrphanReclaimDecision {
    /// The reverse-log row was absent and the object's write lease was absent/expired past
    /// the grace: the transaction wrote the reclaim tombstone. The granule may be freed.
    Authorized,
    /// The reverse-log row holds a committed value — the fragment is live. Do not free.
    Committed,
    /// The write lease is still valid (within grace): a write may be in flight. Skip.
    LeaseActive,
    /// The reverse-log row already holds the reclaim tombstone (a prior sweep authorized
    /// the free but the physical delete may not have completed). Re-issue the free.
    AlreadyReclaimed,
}

/// One orphan-version candidate the pending-version discovery scan yielded: a marker
/// whose protection lapsed (`writing` past expiry + claim grace) or whose reclamation
/// was already claimed but not finished (`reclaiming`, crash resume).
// Constructed only by the FoundationDB (Linux) store impl; the non-Linux stub never
// builds one but callers still destructure it.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct OrphanVersionCandidate {
    pub object_id: u32,
    pub object_version: u32,
    pub state: u8,
    pub expires_at_unix_ms: u64,
}

/// One presence-row target still holding rows for a claimed orphan version, with the
/// durable resume cursor (exclusive) its last completed clean page recorded in the
/// presence row's value. Cursor persistence is what keeps cleaning O(remaining rows):
/// the granule GC concurrently re-stamps tombstones into rows the cleaner cleared, so
/// a restart-from-range-start would re-page a growing tombstone prefix each time and
/// could burn every sweep's budget without ever reaching the uncleared tail.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct OrphanTargetResume {
    pub target_id: String,
    pub cursor: Option<Vec<u8>>,
}

/// Outcome of attempting to claim one orphan-version candidate for reclamation.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum OrphanVersionClaim {
    /// The marker flipped `writing -> reclaiming`; `targets` are the presence-row
    /// targets whose rows must now be cleaned.
    Claimed { targets: Vec<OrphanTargetResume> },
    /// The marker was already `reclaiming` (a prior claim crashed mid-clean); resume
    /// cleaning the remaining presence targets from their recorded cursors.
    Resumed { targets: Vec<OrphanTargetResume> },
    /// The writer renewed the marker or lease since discovery — leave it alone.
    SkippedWriterAlive,
    /// ANOMALY: the marker's version IS the live head. The marker subspace was cleared
    /// without touching any rows. Nonzero counts outside a mixed-binary rollout window
    /// mean markers are being stranded by commits that do not clear them.
    SkippedHeadLive,
    /// The marker vanished before the claim (committed, or another instance finished).
    Gone,
}

/// One bounded page of orphan-version cleaning on one target.
#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub(crate) struct OrphanVersionCleanPage {
    pub rows_cleared: u64,
    pub kco1_cleared: u64,
    pub tombstones_skipped: u64,
    pub unrecognized_skipped: u64,
    /// Resume key (exclusive) for the next page of this target's rows; None when the
    /// target's range was exhausted this page.
    pub next_cursor: Option<Vec<u8>>,
    /// True once this target's rows are fully cleaned and its presence row cleared.
    pub target_done: bool,
}

/// One object's single-shot commit arguments — taken by
/// [`HotMetadataStore::commit_object_single_shot`] and, per element, by
/// [`HotMetadataStore::commit_objects_single_shot_batch`].
// The fields are read only by the FoundationDB (Linux) store impl; the non-Linux
// stub build never touches them but callers still construct the struct.
#[allow(dead_code)]
pub(crate) struct SingleShotCommit {
    pub expected_prior_version: u32,
    pub manifest: ObjectVersionManifest,
    pub parent_entry_id: String,
    pub parent_path: String,
    pub topology_epoch: u64,
    pub omit_manifest: bool,
    pub seals_segments: bool,
    /// The commit requires this version's pending-version marker to be live in the
    /// writing state (the client ran BeginObject); absent or reclaiming rejects.
    /// `seals_segments` implies the same requirement independently of this flag.
    pub require_pending_version_marker: bool,
    /// Marker-fenced fast path: replace the O(fragments) per-reverse-log-row
    /// tombstone reads with one per-version reclaim-fence read (required absent).
    /// Sound only when every KMS writes the fence in `authorize_orphan_reclaim`,
    /// so the service gates it on `--commit-marker-fence-enabled` AND a required
    /// marker; the store additionally ignores it unless the marker is required.
    pub marker_fenced: bool,
}

#[tonic::async_trait]
pub(crate) trait HotMetadataStore: Send + Sync {
    async fn get_bucket_write_context(
        &self,
        bucket_id: String,
    ) -> Result<BucketWriteContext, Status>;

    /// Mints a globally-monotonic object_id and the numeric version for a write (the
    /// BeginObject RPC). version = prior head revision + 1 (1 if new).
    async fn mint_object_id(&self, bucket_id: &str, key: &str) -> Result<(u32, u32), Status>;

    async fn prepare_and_create_write_intent(
        &self,
        intent: WriteIntent,
        bucket_entry_id: String,
        bucket_path: String,
        parent_hint: Option<(String, String)>,
    ) -> Result<TimedStoreResult<WriteIntent>, Status>;

    async fn list_write_intents(&self) -> Result<Vec<WriteIntent>, Status>;

    async fn get_write_intent(&self, intent_id: String) -> Result<Option<WriteIntent>, Status>;

    async fn reserve_object_write_window(
        &self,
        intent_id: String,
        start_stripe_index: u32,
        reservations: Vec<PlacementReservationRecord>,
    ) -> Result<TimedStoreResult<ReservedObjectWriteWindow>, Status>;

    async fn commit_object_write_window(
        &self,
        intent_id: String,
        successful_fragments: Vec<FragmentRef>,
    ) -> Result<TimedStoreResult<CommittedObjectWriteWindow>, Status>;

    async fn commit_object_write(
        &self,
        intent_id: String,
        successful_fragments: Vec<FragmentRef>,
        finalization_sweep_after_ms: u64,
    ) -> Result<TimedStoreResult<CommittedObjectWrite>, Status>;

    /// Commits a freshly-written object in a single transaction: stores the
    /// manifest, appends the per-target reverse log, retains the committed-occupancy
    /// markers + secondary index, writes the namespace entry, and CAS-flips the head
    /// from `expected_prior_version` to `expected_prior_version + 1`. Create-only:
    /// refuses to overwrite an existing object. Idempotent under retry — a commit
    /// whose own version already won returns that head. The caller resolves and
    /// auto-creates the parent directory up front and passes it via
    /// `parent_entry_id`/`parent_path`. The version's pending-version marker is read as
    /// a fence and its whole subspace cleared atomically with the head flip: with
    /// `seals_segments` (the segmented seal) the marker MUST still be in the writing
    /// state — its absence means the orphan-version reaper claimed the version, and the
    /// seal fails instead of flipping a head over reclaimed rows (the marker is the one
    /// key both sides touch, so FDB serializes them). Without `seals_segments` (unary,
    /// batch element) an absent marker is legal — tools may commit without BeginObject,
    /// and a post-reap stale unary commit is independently safe because it reads every
    /// reverse-log row it writes — but a reclaiming marker still rejects (no commit
    /// while a clean is in progress). With `require_pending_version_marker` the marker
    /// must be live-writing even on the unary path, and with `marker_fenced` the
    /// per-row tombstone reads collapse to one per-version reclaim-fence read
    /// (see [`SingleShotCommit`]).
    async fn commit_object_single_shot(
        &self,
        commit: SingleShotCommit,
    ) -> Result<ObjectHead, Status>;

    /// Commits many independent objects in ONE FoundationDB transaction. All reads and
    /// validation for every object happen before any write, so the lease-fenced-GC
    /// read-conflict set is complete before mutation; per-object compare-and-swap is
    /// enforced individually, so one object's CAS miss (or a duplicate `(bucket, key)`
    /// inside the batch) rejects only that object — its slot holds the `Err` — while the
    /// surviving objects still commit in the one transaction. The returned vector is
    /// index-aligned with `commits`. A returned outer `Err` means the whole transaction
    /// failed (transport/FDB), not a per-object rejection.
    async fn commit_objects_single_shot_batch(
        &self,
        commits: Vec<SingleShotCommit>,
    ) -> Result<Vec<Result<ObjectHead, Status>>, Status>;

    /// Append-then-seal commit, segment phase: durably records ONE batch of stripes'
    /// per-fragment reverse-log + occupancy in a single bounded FoundationDB transaction,
    /// WITHOUT touching the head or clearing the lease — so the object stays invisible and
    /// lease-protected until the seal. Used for objects whose whole commit would overflow the
    /// 10 MB transaction limit: the client streams N of these (bounded by `version_id`) then a
    /// final seal (a `commit_object_single_shot` sealing the segments — empty stripes, so its
    /// fence/write loops are no-ops, leaving the namespace entry + head flip + lease/marker
    /// clear). Fences, reads before any write per the HARD INVARIANT: (1) the version's
    /// pending-version marker must exist in the `writing` state — absent or reclaiming means
    /// the write forfeited (lease lapsed, orphan-version reaper claimed it) and the append is
    /// rejected; appends never MINT the marker, only `begin_object_write` does, so a reclaimed
    /// version can never be resurrected by a zombie stream; (2) the lease-fenced-GC
    /// rendezvous — without `marker_fenced`, every reverse-log row this segment writes is
    /// read first and a reclaim tombstone on any row rejects the whole commit (that
    /// granule's bytes are gone); with `marker_fenced`, one per-version reclaim-fence read
    /// (required absent) replaces the per-row reads, since the GC stamps the fence in the
    /// same transaction as any tombstone for the version. Writes: the marker re-set with
    /// its expiry pushed to at least `marker_expires_at_unix_ms` (a healthy stream is never
    /// claim-eligible), the reverse-log + occupancy rows, and one presence row per distinct
    /// target in the segment — same transaction as that target's rows, so the reaper's
    /// target enumeration can never under-cover them. Idempotent under retry/resend (same
    /// keys overwritten; presence sets are blind).
    async fn append_manifest_segment(
        &self,
        version_id: String,
        stripes: Vec<StripeManifest>,
        marker_expires_at_unix_ms: u64,
        marker_fenced: bool,
    ) -> Result<(), Status>;

    /// Janitor for stranded reclaim-fence rows: fences written by the granule GC for
    /// versions the orphan-version reaper never claimed (their marker is already gone)
    /// have no other clearing path. Scans up to `row_limit` fence rows from `cursor`
    /// (None = range start) and clears each whose stamped_at is older than
    /// `older_than_ms` AND whose pending-version marker and write lease are both
    /// absent — a fence whose version could still see a late commit keeps rejecting
    /// it. Returns (cleared, next_cursor); next_cursor None means the scan wrapped,
    /// so unclearable rows (young fences, objects with a live lease) can never
    /// permanently mask clearable ones behind them.
    async fn sweep_reclaim_fences(
        &self,
        now_unix_ms: u64,
        older_than_ms: u64,
        row_limit: usize,
        cursor: Option<Vec<u8>>,
    ) -> Result<(u64, Option<Vec<u8>>), Status>;

    /// One-shot fence backfill for tombstones that predate the reclaim-fence key:
    /// walks the whole reverse-log keyspace and stamps the per-version fence for
    /// every reclaim-tombstone row found, in bounded transaction pages. Idempotent.
    /// Marker-fenced commits are only sound once this has completed on the cluster
    /// (the service gates the fenced fast path on it). Returns the number of fences
    /// stamped.
    async fn backfill_reclaim_fences(&self, now_unix_ms: u64) -> Result<u64, Status>;

    /// Returns the per-cluster salt, minting a fresh random one on first call and
    /// persisting it so every subsequent call — on any KMS instance — returns the same
    /// bytes. The salt scopes computed chunk ids and placement weights to this cluster
    /// and is stable for its lifetime.
    async fn get_or_init_cluster_salt(&self) -> Result<Vec<u8>, Status>;

    /// Durably begins one write attempt: in ONE transaction, issues the per-object
    /// write lease (writer liveness, keyed by object_id) and mints the pending-version
    /// marker (version liveness + orphan-reaper discovery, keyed by
    /// `(object_id, object_version)`, carrying the NORMALIZED head coordinates for the
    /// reaper's head-liveness cross-check). Both are blind sets: object_ids are minted
    /// fresh per write attempt and never reused, so neither key can pre-exist. The
    /// marker is cleared by the commit atomically with the head flip, or by the
    /// orphan-version reaper once the write forfeits; the lease keeps its existing
    /// lifecycle (cleared on commit, reaped on expiry).
    async fn begin_object_write(
        &self,
        object_id: u32,
        object_version: u32,
        bucket_id: &str,
        key: &str,
        expires_at_unix_ms: u64,
    ) -> Result<(), Status>;

    /// Merged write-begin for the decentralized path, replacing InitiateObjectWrite +
    /// the separate version-derivation transaction: in ONE transaction, derives the
    /// version from the object head, resolves — auto-creating any missing levels of —
    /// the parent collection (a caller hint short-circuits the walk after in-txn
    /// re-validation), and mints the pending-version marker + write lease. No
    /// write-intent row is created; the marker is the write's sole recovery record.
    /// Returns (object_id, version, parent_entry_id, parent_path).
    async fn begin_object_write_resolved(
        &self,
        bucket_id: &str,
        key: &str,
        namespace_id: &str,
        bucket_entry_id: &str,
        bucket_path: &str,
        parent_hint: Option<(String, String)>,
        expires_at_unix_ms: u64,
    ) -> Result<(u32, u32, String, String), Status>;

    /// Abandons an in-flight decentralized write: flips its pending-version marker
    /// `writing -> reclaiming` and clears the write lease in one transaction, so the
    /// orphan-version reaper retires the write's rows and granules without waiting out
    /// the lease TTL. Idempotent — an absent or already-reclaiming marker returns Ok
    /// (a forfeit racing a commit is settled by the marker's serialization; exactly
    /// one side wins).
    async fn forfeit_object_write(
        &self,
        object_id: u32,
        object_version: u32,
    ) -> Result<(), Status>;

    /// Heartbeat for an in-flight write: refreshes the lease AND pushes the
    /// pending-version marker's expiry forward (to the max of old and new) in one
    /// transaction. Refuses with `failed_precondition` — writing NOTHING — once the
    /// marker is absent or reclaiming: the write has forfeited, and a zombie writer's
    /// heartbeat must not resurrect the lease and stall reclamation forever.
    async fn renew_object_write(
        &self,
        object_id: u32,
        new_expires_at_unix_ms: u64,
    ) -> Result<(), Status>;

    /// Reaps up to `limit` write leases whose expiry plus `grace_ms` is at or before
    /// `now_unix_ms`, returning the number cleared. The grace MUST match the GC's reclaim
    /// grace so a lease survives long enough to gate reclaim eligibility (a lease deleted
    /// at bare expiry would let the GC act inside the window a slow-but-live write may
    /// still commit in). An expired-past-grace lease marks an abandoned write.
    async fn reap_expired_leases(
        &self,
        now_unix_ms: u64,
        grace_ms: u64,
        limit: usize,
    ) -> Result<usize, Status>;

    /// Authorizes (or declines) reclaiming one occupied granule a target reported, by its
    /// object identity `(object_id, object_version, stripe_index, fragment_index)` on
    /// `target_id`. Runs ONE FoundationDB transaction that reads the per-target reverse-log
    /// row and the object's write lease, and — when the row is absent and the lease is
    /// absent/expired past `lease_grace_ms` — writes the reclaim tombstone into the
    /// reverse-log row. Reading the row puts it in the transaction's read-conflict set, so
    /// a concurrent `commit_object_single_shot` (which reads the same row) cannot also
    /// commit: exactly one wins. `now_unix_ms` is snapshotted by the caller (the closure may
    /// re-run on FDB retry, so it must not re-read the clock).
    async fn authorize_orphan_reclaim(
        &self,
        target_id: String,
        object_id: u32,
        object_version: u32,
        stripe_index: u32,
        fragment_index: u32,
        now_unix_ms: u64,
        lease_grace_ms: u64,
    ) -> Result<OrphanReclaimDecision, Status>;

    // --- Orphan-version reaper (segmented-write crash cleanup) ---

    /// Discovery: pages the pending-version keyspace by RAW rows (markers + presence
    /// rows both count toward `row_limit`, since a version's presence rows may span
    /// pages) from `cursor` (exclusive), yielding claim candidates: `writing` markers
    /// whose expiry + `claim_grace_ms` has passed, plus EVERY `reclaiming` marker
    /// (crash resume). Snapshot reads — discovery must not conflict with the marker
    /// renewals riding every segment append. Returns the candidates, the next cursor
    /// (`None` once the scan wrapped), and the count of undecodable rows skipped —
    /// they never abort the scan, but the caller surfaces the count as reap errors so
    /// a corrupt keyspace is visible instead of silently unreapable.
    async fn scan_pending_versions(
        &self,
        cursor: Option<Vec<u8>>,
        row_limit: usize,
        now_unix_ms: u64,
        claim_grace_ms: u64,
    ) -> Result<(Vec<OrphanVersionCandidate>, Option<Vec<u8>>, u64), Status>;

    /// Claim: flips one candidate's marker `writing -> reclaiming` in a transaction
    /// that re-checks, in read order: the marker (absent => `Gone`; renewed since
    /// discovery => `SkippedWriterAlive`; already `reclaiming` => `Resumed`), the
    /// object's lease (alive within `lease_grace_ms` => `SkippedWriterAlive` — the
    /// heartbeat veto), and — when the marker carries head coordinates — the object
    /// head (`current_version_id` equal to this version => `SkippedHeadLive`: clear
    /// only the marker subspace, touch no rows). The marker write is what serializes
    /// this claim against every seal/append/renewal that reads the marker: exactly
    /// one side wins. `reclaiming` is terminal — nothing transitions it back.
    async fn claim_orphan_version(
        &self,
        object_id: u32,
        object_version: u32,
        now_unix_ms: u64,
        claim_grace_ms: u64,
        lease_grace_ms: u64,
    ) -> Result<OrphanVersionClaim, Status>;

    /// Clean, one bounded transaction: requires the marker in `reclaiming` (absent
    /// => another instance finished, returns done-with-no-work), then walks up to
    /// `row_limit` of this target's reverse-log rows for the version from `cursor`
    /// (exclusive). Committed-valued rows are cleared together with their paired
    /// occupancy key and KCO1 committed-granule marker (the granule index is
    /// recoverable only from the row value read in this same transaction — never a
    /// blind range clear). GC reclaim tombstones are SKIPPED: they are the granule
    /// GC's free-retry sentinel and the permanent fence against any later commit of
    /// this version. A non-final page records its resume cursor in the presence
    /// row's value in the same transaction, so progress through a target survives
    /// budget exhaustion and process death (see [`OrphanTargetResume`]). When the
    /// page exhausts the range, the target's stray occupancy range is cleared and
    /// its presence row deleted in the same transaction, making per-target
    /// completion durable for crash resume.
    async fn clean_orphan_version_target_page(
        &self,
        object_id: u32,
        object_version: u32,
        target_id: String,
        cursor: Option<Vec<u8>>,
        row_limit: usize,
    ) -> Result<OrphanVersionCleanPage, Status>;

    /// Finish: deletes the marker subspace once no presence rows remain. Returns
    /// false (and touches nothing) while presence rows still exist — the caller
    /// re-lists targets and resumes cleaning. Deleting (not tombstoning) the marker
    /// is safe: appends and seals treat an absent marker as fatal, a version's
    /// `(object_id, object_version)` is never re-minted, and a late marker-less
    /// unary commit is independently fenced by its reverse-log reads.
    async fn finish_orphan_version(
        &self,
        object_id: u32,
        object_version: u32,
    ) -> Result<bool, Status>;

    // --- Target inventory (KMS-owned roster; KAS removed) ---

    /// Upserts a target into the roster. Defaults an unspecified lifecycle to Active and
    /// stamps the heartbeat; the stored record is returned. Idempotent re-registration
    /// updates the record in place.
    async fn register_target(&self, target: TargetRecord) -> Result<TargetRecord, Status>;

    /// Updates a target's health + last-heartbeat timestamp. Errors if unknown.
    async fn heartbeat_target(
        &self,
        target_id: String,
        healthy: bool,
        observed_unix_ms: u64,
    ) -> Result<TargetRecord, Status>;

    /// Transitions a target's lifecycle state (Active/Draining/Unhealthy/Retired), with
    /// the health side-effects Active=>healthy / Unhealthy=>unhealthy. Errors if unknown.
    async fn set_target_lifecycle(
        &self,
        target_id: String,
        lifecycle_state: i32,
        now_unix_ms: u64,
    ) -> Result<TargetRecord, Status>;

    /// Returns the whole roster, sorted by target_id.
    async fn list_targets(&self) -> Result<Vec<TargetRecord>, Status>;

    /// Returns the object head for `(bucket_id, key_path)`, or None if absent. The
    /// decentralized read path uses this to get the object geometry (length + EC profile
    /// id + topology epoch) and then reconstructs the fragment layout by computation,
    /// instead of fetching a manifest.
    async fn get_object_head(
        &self,
        bucket_id: String,
        key_path: String,
    ) -> Result<Option<ObjectHead>, Status>;

    async fn abort_object_write(
        &self,
        intent_id: String,
        next_state: WriteIntentState,
    ) -> Result<WriteIntent, Status>;

    async fn repair_object_write(
        &self,
        intent_id: String,
        failed_fragments: Vec<FragmentRef>,
        replacement_reservation: PlacementReservationRecord,
    ) -> Result<WriteIntent, Status>;

    async fn mark_write_intent_reservations_finalized(
        &self,
        intent_id: String,
    ) -> Result<(), Status>;

    async fn list_pending_finalization_intents(
        &self,
        limit: usize,
        now_ms: u64,
    ) -> Result<Vec<WriteIntent>, Status>;

    async fn resolve_object_read(
        &self,
        bucket_id: String,
        key_path: String,
    ) -> Result<(ObjectVersionManifest, EcProfile), Status>;

    async fn delete_object(
        &self,
        bucket_id: String,
        key_path: String,
        version_ids: Vec<String>,
    ) -> Result<TimedStoreResult<DeletedObject>, Status>;
}
