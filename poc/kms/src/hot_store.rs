// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::store::{
    BucketWriteContext, CommittedObjectWrite, CommittedObjectWriteWindow, DeletedObject,
    ReservedObjectWriteWindow, TimedStoreResult,
};
use keinctl::proto::{
    EcProfile, FragmentRef, ObjectHead, ObjectVersionManifest, PlacementReservationRecord,
    WriteIntent, WriteIntentState,
};
use tonic::Status;

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
    /// `parent_entry_id`/`parent_path`.
    async fn commit_object_single_shot(
        &self,
        expected_prior_version: u32,
        manifest: ObjectVersionManifest,
        parent_entry_id: String,
        parent_path: String,
        topology_epoch: u64,
        omit_manifest: bool,
    ) -> Result<ObjectHead, Status>;

    /// Returns the per-cluster salt, minting a fresh random one on first call and
    /// persisting it so every subsequent call — on any KMS instance — returns the same
    /// bytes. The salt scopes computed chunk ids and placement weights to this cluster
    /// and is stable for its lifetime.
    async fn get_or_init_cluster_salt(&self) -> Result<Vec<u8>, Status>;

    /// Issues (or refreshes) the per-object write lease for `object_id`, expiring at
    /// `expires_at_unix_ms`. The lease marks a write as in-flight so its granules are
    /// protected from reclamation until it commits or the lease lapses.
    async fn issue_write_lease(
        &self,
        object_id: u32,
        expires_at_unix_ms: u64,
    ) -> Result<(), Status>;

    /// Clears the per-object write lease for `object_id` (called once the object has
    /// committed; the head now protects its granules). Idempotent.
    async fn clear_write_lease(&self, object_id: u32) -> Result<(), Status>;

    /// Reaps up to `limit` write leases that expired at or before `now_unix_ms`,
    /// returning the number cleared. An expired lease marks an abandoned write.
    async fn reap_expired_leases(&self, now_unix_ms: u64, limit: usize)
        -> Result<usize, Status>;

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
