// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::store::{ReservationBinKey, ReservationMutationSpec, TimedStoreResult};
use keinctl::proto::{
    FailureDomain, PlacementReservationRecord, ReservationState, ServiceInstanceRecord,
    TargetGranule, TargetLifecycleState, TargetRecord,
};
use tonic::Status;

#[tonic::async_trait]
pub(crate) trait AllocatorStore: Send + Sync {
    async fn init(&self) -> Result<(), Status>;
    async fn reset_allocator_state(&self) -> Result<(), Status>;
    async fn register_target(&self, target: TargetRecord) -> Result<TargetRecord, Status>;
    async fn heartbeat_target(
        &self,
        target_id: String,
        healthy: bool,
        observed_unix_ms: u64,
    ) -> Result<TargetRecord, Status>;
    async fn upsert_service_instance(
        &self,
        instance: ServiceInstanceRecord,
    ) -> Result<ServiceInstanceRecord, Status>;
    async fn try_acquire_coordination_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        lease_ttl_ms: u64,
    ) -> Result<bool, Status>;
    async fn list_service_instances(
        &self,
        service_kind: Option<keinctl::proto::ServiceKind>,
        node_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ServiceInstanceRecord>, Status>;
    async fn get_service_instance(
        &self,
        instance_id: &str,
    ) -> Result<Option<ServiceInstanceRecord>, Status>;
    async fn list_targets(&self) -> Result<Vec<TargetRecord>, Status>;
    async fn set_target_state(
        &self,
        target_id: String,
        lifecycle_state: TargetLifecycleState,
    ) -> Result<TargetRecord, Status>;
    async fn list_reservations(
        &self,
        state: Option<ReservationState>,
        target_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PlacementReservationRecord>, Status>;
    async fn get_reservation(
        &self,
        reservation_id: &str,
    ) -> Result<Option<PlacementReservationRecord>, Status>;
    async fn reserve_stripe_placement(
        &self,
        reservation_id: String,
        fragment_count: usize,
        failure_domain: FailureDomain,
        excluded_target_ids: Vec<String>,
        reservation_ttl_ms: u64,
    ) -> Result<TimedStoreResult<PlacementReservationRecord>, Status>;
    async fn reserve_stripe_batch(
        &self,
        batch_size: usize,
        fragment_count: usize,
        failure_domain: FailureDomain,
        excluded_target_ids: Vec<String>,
        reservation_ttl_ms: u64,
    ) -> Result<TimedStoreResult<Vec<PlacementReservationRecord>>, Status>;
    async fn claim_reservation_bin_batch(
        &self,
        batch_size: usize,
        fragment_count: usize,
        failure_domain: FailureDomain,
        reservation_ttl_ms: u64,
    ) -> Result<TimedStoreResult<Vec<PlacementReservationRecord>>, Status>;
    async fn top_up_reservation_bin(
        &self,
        bin_key: &ReservationBinKey,
        reservation_ttl_ms: u64,
        low_watermark: usize,
        high_watermark: usize,
        top_up_chunk: usize,
    ) -> Result<TimedStoreResult<usize>, Status>;
    async fn reserve_rebuild_placement(
        &self,
        reservation_id: String,
        failed_target_id: String,
        failure_domain: FailureDomain,
        occupied_target_ids: Vec<String>,
    ) -> Result<TimedStoreResult<PlacementReservationRecord>, Status>;
    async fn reserve_replacement_placement(
        &self,
        reservation_id: String,
        replacement_count: usize,
        failure_domain: FailureDomain,
        excluded_target_ids: Vec<String>,
        reservation_ttl_ms: u64,
        required_target_ids: Vec<String>,
    ) -> Result<TimedStoreResult<PlacementReservationRecord>, Status>;
    async fn finalize_reservations(
        &self,
        reservation_id: String,
        placement_indexes: Vec<u32>,
    ) -> Result<PlacementReservationRecord, Status>;
    async fn finalize_reservations_batch(
        &self,
        mutations: Vec<ReservationMutationSpec>,
    ) -> Result<Vec<PlacementReservationRecord>, Status>;
    async fn release_reservations(
        &self,
        reservation_id: String,
        placement_indexes: Vec<u32>,
    ) -> Result<PlacementReservationRecord, Status>;
    async fn release_reservations_batch(
        &self,
        mutations: Vec<ReservationMutationSpec>,
    ) -> Result<Vec<PlacementReservationRecord>, Status>;
    async fn reclaim_target_granules(&self, granules: Vec<TargetGranule>) -> Result<u64, Status>;
    async fn release_expired_reservations(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<usize, Status>;
}
