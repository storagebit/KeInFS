// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::store::{
    BucketWriteContext, CommittedObjectWrite, CommittedObjectWriteWindow, DeletedObject,
    ReservedObjectWriteWindow, TimedStoreResult,
};
use keinctl::proto::{
    EcProfile, FragmentRef, ObjectVersionManifest, PlacementReservationRecord, WriteIntent,
    WriteIntentState,
};
use tonic::Status;

#[tonic::async_trait]
pub(crate) trait HotMetadataStore: Send + Sync {
    async fn get_bucket_write_context(
        &self,
        bucket_id: String,
    ) -> Result<BucketWriteContext, Status>;

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
