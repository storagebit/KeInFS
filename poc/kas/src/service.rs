// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::allocator_store::AllocatorStore;
use crate::stats::{KasStats, RpcKind};
use crate::store::{
    ReservationBinGate, ReservationBinKey, ReservationBinRegistry, ReservationMutationSpec,
    StorePhaseTiming, TimedStoreResult,
};
use keinctl::proto::kas_server::Kas;
use keinctl::proto::{
    FinalizeReservationsBatchReply, FinalizeReservationsBatchRequest, FinalizeReservationsReply,
    FinalizeReservationsRequest, GetReservationReply, GetReservationRequest,
    GetServiceInstanceReply, GetServiceInstanceRequest, HeartbeatTargetReply,
    HeartbeatTargetRequest, ListReservationsReply, ListReservationsRequest,
    ListServiceInstancesReply, ListServiceInstancesRequest, ListTargetsReply, ListTargetsRequest,
    ReclaimTargetGranulesReply, ReclaimTargetGranulesRequest, RegisterTargetReply,
    RegisterTargetRequest, ReleaseReservationsBatchReply, ReleaseReservationsBatchRequest,
    ReleaseReservationsReply, ReleaseReservationsRequest, ReservationMutation, ReservationState,
    ReserveRebuildPlacementReply, ReserveRebuildPlacementRequest, ReserveReplacementPlacementReply,
    ReserveReplacementPlacementRequest, ReserveStripeBatchReply, ReserveStripeBatchRequest,
    ReserveStripePlacementReply, ReserveStripePlacementRequest, ServiceKind, SetTargetStateReply,
    SetTargetStateRequest, TargetLifecycleState, UpsertServiceInstanceReply,
    UpsertServiceInstanceRequest,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub(crate) struct KasService {
    pub(crate) store: Arc<dyn AllocatorStore>,
    pub(crate) stats: Arc<KasStats>,
    pub(crate) allocation_shard_id: String,
    pub(crate) service_instance: keinctl::proto::ServiceInstanceRecord,
    pub(crate) reservation_ttl_ms: u64,
    pub(crate) max_batch_size: usize,
    pub(crate) reservation_bins: ReservationBinRegistry,
    pub(crate) reservation_bin_gate: ReservationBinGate,
    pub(crate) reservation_bin_low_watermark: usize,
    pub(crate) reservation_bin_high_watermark: usize,
    pub(crate) reservation_bin_top_up_chunk: usize,
    pub(crate) reservation_bin_bypass_batch_size: usize,
    pub(crate) reservation_bin_refill_leader: Arc<AtomicBool>,
}

impl KasService {
    fn include_self_service_instance(
        &self,
        service_kind: ServiceKind,
        node_id: &str,
        limit: usize,
        instances: &mut Vec<keinctl::proto::ServiceInstanceRecord>,
    ) {
        if service_kind != ServiceKind::Unspecified && service_kind != ServiceKind::Kas {
            return;
        }
        if !node_id.is_empty() && node_id != self.service_instance.node_id {
            return;
        }
        if instances.iter().any(|instance| {
            instance.instance_id == self.service_instance.instance_id
                || instance.endpoint == self.service_instance.endpoint
        }) {
            return;
        }
        instances.push(self.service_instance.clone());
        instances.sort_by(|left, right| left.instance_id.cmp(&right.instance_id));
        if limit > 0 && instances.len() > limit {
            instances.truncate(limit);
        }
    }

    fn schedule_reservation_bin_top_up(&self, bin_key: ReservationBinKey, reservation_ttl_ms: u64) {
        if self.reservation_bin_high_watermark == 0
            || self.reservation_bin_high_watermark <= self.reservation_bin_low_watermark
            || !self.reservation_bin_refill_leader.load(Ordering::Relaxed)
        {
            return;
        }

        let store = self.store.clone();
        let stats = self.stats.clone();
        let reservation_bin_gate = self.reservation_bin_gate.clone();
        let low_watermark = self.reservation_bin_low_watermark;
        let high_watermark = self.reservation_bin_high_watermark;
        let top_up_chunk = self.reservation_bin_top_up_chunk;
        tokio::spawn(async move {
            let Some(_gate) = reservation_bin_gate.try_acquire(&bin_key).await else {
                return;
            };
            if let Err(err) = store
                .top_up_reservation_bin(
                    &bin_key,
                    reservation_ttl_ms,
                    low_watermark,
                    high_watermark,
                    top_up_chunk,
                )
                .await
            {
                stats.set_last_error(format!(
                    "KAS reservation bin top-up failed for fragment_count={} failure_domain={}: {err}",
                    bin_key.fragment_count(),
                    bin_key.failure_domain_raw(),
                ));
            }
        });
    }
}

fn mutation_spec_from_proto(mutation: ReservationMutation) -> ReservationMutationSpec {
    ReservationMutationSpec {
        reservation_id: mutation.reservation_id,
        placement_indexes: mutation.placement_indexes,
    }
}

#[tonic::async_trait]
impl Kas for KasService {
    async fn upsert_service_instance(
        &self,
        request: Request<UpsertServiceInstanceRequest>,
    ) -> Result<Response<UpsertServiceInstanceReply>, Status> {
        let kind = RpcKind::UpsertServiceInstance;
        let started = Instant::now();
        self.stats.record_request(kind);
        let phase_started = Instant::now();
        let instance = request.into_inner().instance.ok_or_else(|| {
            Status::invalid_argument("KAS UpsertServiceInstance requires instance")
        })?;
        self.stats
            .record_phase(kind, "request_decode", phase_started.elapsed());
        let phase_started = Instant::now();
        match self.store.upsert_service_instance(instance).await {
            Ok(instance) => {
                self.stats.record_phase(
                    kind,
                    "store_upsert_service_instance",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(UpsertServiceInstanceReply {
                    instance: Some(instance),
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_upsert_service_instance",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn list_service_instances(
        &self,
        request: Request<ListServiceInstancesRequest>,
    ) -> Result<Response<ListServiceInstancesReply>, Status> {
        let kind = RpcKind::ListServiceInstances;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let service_kind =
            ServiceKind::try_from(request.service_kind).unwrap_or(ServiceKind::Unspecified);
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .list_service_instances(
                (service_kind != ServiceKind::Unspecified).then_some(service_kind),
                (!request.node_id.is_empty()).then_some(request.node_id.as_str()),
                request.limit as usize,
            )
            .await
        {
            Ok(mut instances) => {
                self.include_self_service_instance(
                    service_kind,
                    request.node_id.as_str(),
                    request.limit as usize,
                    &mut instances,
                );
                self.stats.record_phase(
                    kind,
                    "store_list_service_instances",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ListServiceInstancesReply { instances }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_list_service_instances",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn get_service_instance(
        &self,
        request: Request<GetServiceInstanceRequest>,
    ) -> Result<Response<GetServiceInstanceReply>, Status> {
        let kind = RpcKind::GetServiceInstance;
        let started = Instant::now();
        self.stats.record_request(kind);
        let instance_id = request.into_inner().instance_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.store.get_service_instance(&instance_id).await {
            Ok(Some(instance)) => {
                self.stats.record_phase(
                    kind,
                    "store_get_service_instance",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(GetServiceInstanceReply {
                    instance: Some(instance),
                }))
            }
            Ok(None) => {
                self.stats.record_phase(
                    kind,
                    "store_get_service_instance",
                    phase_started.elapsed(),
                );
                kas_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::not_found(format!("unknown service instance {instance_id}")),
                )
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_get_service_instance",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn register_target(
        &self,
        request: Request<RegisterTargetRequest>,
    ) -> Result<Response<RegisterTargetReply>, Status> {
        let kind = RpcKind::RegisterTarget;
        let started = Instant::now();
        self.stats.record_request(kind);
        let phase_started = Instant::now();
        let target = match request.into_inner().target {
            Some(target) => target,
            None => {
                return kas_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::invalid_argument("KAS RegisterTarget requires target"),
                )
            }
        };
        self.stats
            .record_phase(kind, "request_decode", phase_started.elapsed());
        let phase_started = Instant::now();
        match self.store.register_target(target).await {
            Ok(target) => {
                self.stats
                    .record_phase(kind, "store_register_target", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(RegisterTargetReply {
                    target: Some(target),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_register_target", phase_started.elapsed());
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn heartbeat_target(
        &self,
        request: Request<HeartbeatTargetRequest>,
    ) -> Result<Response<HeartbeatTargetReply>, Status> {
        let kind = RpcKind::HeartbeatTarget;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .heartbeat_target(request.target_id, request.healthy, request.observed_unix_ms)
            .await
        {
            Ok(target) => {
                self.stats
                    .record_phase(kind, "store_heartbeat_target", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(HeartbeatTargetReply {
                    target: Some(target),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_heartbeat_target", phase_started.elapsed());
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn list_targets(
        &self,
        _request: Request<ListTargetsRequest>,
    ) -> Result<Response<ListTargetsReply>, Status> {
        let kind = RpcKind::ListTargets;
        let started = Instant::now();
        self.stats.record_request(kind);
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.store.list_targets().await {
            Ok(targets) => {
                // Content-derived epoch of the placement-relevant roster, so a client
                // computing placement can tell whether its snapshot is still current.
                let placement_targets = targets
                    .iter()
                    .map(keinctl::placement::PlacementTarget::from_record)
                    .collect::<Vec<_>>();
                let topology_epoch = keinctl::placement::topology_epoch(&placement_targets);
                self.stats
                    .record_phase(kind, "store_list_targets", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ListTargetsReply {
                    targets,
                    topology_epoch,
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_list_targets", phase_started.elapsed());
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn set_target_state(
        &self,
        request: Request<SetTargetStateRequest>,
    ) -> Result<Response<SetTargetStateReply>, Status> {
        let kind = RpcKind::SetTargetState;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let phase_started = Instant::now();
        let lifecycle_state = match TargetLifecycleState::try_from(request.lifecycle_state) {
            Ok(TargetLifecycleState::Unspecified) | Err(_) => {
                return kas_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::invalid_argument("invalid target lifecycle state"),
                )
            }
            Ok(state) => state,
        };
        self.stats
            .record_phase(kind, "request_validate", phase_started.elapsed());
        let phase_started = Instant::now();
        match self
            .store
            .set_target_state(request.target_id, lifecycle_state)
            .await
        {
            Ok(target) => {
                self.stats
                    .record_phase(kind, "store_set_target_state", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(SetTargetStateReply {
                    target: Some(target),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_set_target_state", phase_started.elapsed());
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn list_reservations(
        &self,
        request: Request<ListReservationsRequest>,
    ) -> Result<Response<ListReservationsReply>, Status> {
        let kind = RpcKind::ListReservations;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let state =
            ReservationState::try_from(request.state).unwrap_or(ReservationState::Unspecified);
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .list_reservations(
                (state != ReservationState::Unspecified).then_some(state),
                (!request.target_id.is_empty()).then_some(request.target_id.as_str()),
                request.limit as usize,
            )
            .await
        {
            Ok(reservations) => {
                self.stats
                    .record_phase(kind, "store_list_reservations", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ListReservationsReply { reservations }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_list_reservations", phase_started.elapsed());
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn get_reservation(
        &self,
        request: Request<GetReservationRequest>,
    ) -> Result<Response<GetReservationReply>, Status> {
        let kind = RpcKind::GetReservation;
        let started = Instant::now();
        self.stats.record_request(kind);
        let reservation_id = request.into_inner().reservation_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.store.get_reservation(&reservation_id).await {
            Ok(Some(reservation)) => {
                self.stats
                    .record_phase(kind, "store_get_reservation", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(GetReservationReply {
                    reservation: Some(reservation),
                }))
            }
            Ok(None) => {
                self.stats
                    .record_phase(kind, "store_get_reservation", phase_started.elapsed());
                kas_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::not_found(format!("unknown reservation {}", reservation_id)),
                )
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_get_reservation", phase_started.elapsed());
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn reserve_stripe_placement(
        &self,
        request: Request<ReserveStripePlacementRequest>,
    ) -> Result<Response<ReserveStripePlacementReply>, Status> {
        let kind = RpcKind::ReserveStripePlacement;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        if !request.allocation_shard_id.is_empty()
            && request.allocation_shard_id != self.allocation_shard_id
        {
            return kas_err(
                &self.stats,
                kind,
                &started,
                Status::unavailable(format!(
                    "allocator shard {} is not served by this KAS leader ({})",
                    request.allocation_shard_id, self.allocation_shard_id
                )),
            );
        }
        let phase_started = Instant::now();
        let failure_domain = match request.failure_domain.try_into() {
            Ok(domain) => domain,
            Err(_) => {
                return kas_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::invalid_argument("invalid failure_domain"),
                )
            }
        };
        self.stats
            .record_phase(kind, "request_validate", phase_started.elapsed());
        let phase_started = Instant::now();
        match self
            .store
            .reserve_stripe_placement(
                request.reservation_id,
                request.fragment_count as usize,
                failure_domain,
                request.excluded_target_ids,
                self.reservation_ttl_ms,
            )
            .await
        {
            Ok(TimedStoreResult {
                value: reservation,
                phase_timings,
            }) => {
                self.stats.record_phase(
                    kind,
                    "store_reserve_stripe_total",
                    phase_started.elapsed(),
                );
                record_store_phase_timings(
                    &self.stats,
                    kind,
                    "store_reserve_stripe",
                    &phase_timings,
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ReserveStripePlacementReply {
                    reservation: Some(reservation),
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_reserve_stripe_total",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn reserve_stripe_batch(
        &self,
        request: Request<ReserveStripeBatchRequest>,
    ) -> Result<Response<ReserveStripeBatchReply>, Status> {
        let kind = RpcKind::ReserveStripeBatch;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        if !request.allocation_shard_id.is_empty()
            && request.allocation_shard_id != self.allocation_shard_id
        {
            return kas_err(
                &self.stats,
                kind,
                &started,
                Status::unavailable(format!(
                    "allocator shard {} is not served by this KAS leader ({})",
                    request.allocation_shard_id, self.allocation_shard_id
                )),
            );
        }
        let phase_started = Instant::now();
        let failure_domain = match request.failure_domain.try_into() {
            Ok(domain) => domain,
            Err(_) => {
                return kas_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::invalid_argument("invalid failure_domain"),
                )
            }
        };
        let batch_size = (request.batch_size as usize).min(self.max_batch_size);
        let reservation_ttl_ms = request.reservation_ttl_ms.max(self.reservation_ttl_ms);
        self.stats
            .record_phase(kind, "request_validate", phase_started.elapsed());
        let fragment_count = request.fragment_count as usize;
        let mut reservations = Vec::with_capacity(batch_size);
        let shard_scoped_request = !request.allocation_shard_id.is_empty();
        let shared_hot_path =
            request.excluded_target_ids.is_empty() && batch_size > 0 && !shard_scoped_request;
        let shared_bin_enabled = self.reservation_bin_high_watermark > 0
            && self.reservation_bin_high_watermark > self.reservation_bin_low_watermark
            && self.reservation_bin_top_up_chunk > 0;
        let bypass_shared_bin = shared_hot_path
            && self.reservation_bin_bypass_batch_size > 0
            && batch_size >= self.reservation_bin_bypass_batch_size;
        let bin_key = (shared_hot_path && shared_bin_enabled && !bypass_shared_bin)
            .then(|| ReservationBinKey::new(fragment_count, failure_domain));
        let mut held_bin_guard = None;
        if let Some(bin_key) = bin_key.clone() {
            let bin_key = bin_key.clone();
            self.reservation_bins.remember(bin_key.clone()).await;
            held_bin_guard = Some(self.reservation_bin_gate.acquire(&bin_key).await);
            let phase_started = Instant::now();
            match self
                .store
                .claim_reservation_bin_batch(
                    batch_size,
                    fragment_count,
                    failure_domain,
                    reservation_ttl_ms,
                )
                .await
            {
                Ok(TimedStoreResult {
                    value: claimed,
                    phase_timings,
                }) => {
                    self.stats.record_phase(
                        kind,
                        "store_claim_reservation_bin_total",
                        phase_started.elapsed(),
                    );
                    record_store_phase_timings(
                        &self.stats,
                        kind,
                        "store_claim_reservation_bin",
                        &phase_timings,
                    );
                    reservations.extend(claimed);
                }
                Err(err) => {
                    self.stats.record_phase(
                        kind,
                        "store_claim_reservation_bin_total",
                        phase_started.elapsed(),
                    );
                    return kas_err(&self.stats, kind, &started, err);
                }
            }
        }

        let remaining = batch_size.saturating_sub(reservations.len());
        drop(held_bin_guard);
        if let Some(bin_key) = bin_key.clone() {
            self.schedule_reservation_bin_top_up(bin_key, reservation_ttl_ms);
        }
        if remaining == 0 {
            self.stats.record_success(kind, started.elapsed());
            return Ok(Response::new(ReserveStripeBatchReply { reservations }));
        }

        let allow_direct_fallback = shared_hot_path
            && remaining
                <= self
                    .reservation_bin_low_watermark
                    .saturating_div(2)
                    .max(fragment_count);
        if shared_hot_path
            && !bypass_shared_bin
            && !self.reservation_bin_refill_leader.load(Ordering::Relaxed)
            && !allow_direct_fallback
        {
            self.stats.record_success(kind, started.elapsed());
            return Ok(Response::new(ReserveStripeBatchReply { reservations }));
        }

        let phase_started = Instant::now();
        match self
            .store
            .reserve_stripe_batch(
                remaining,
                fragment_count,
                failure_domain,
                request.excluded_target_ids,
                reservation_ttl_ms,
            )
            .await
        {
            Ok(TimedStoreResult {
                value: fresh_reservations,
                phase_timings,
            }) => {
                reservations.extend(fresh_reservations);
                self.stats.record_phase(
                    kind,
                    "store_reserve_stripe_batch_total",
                    phase_started.elapsed(),
                );
                record_store_phase_timings(
                    &self.stats,
                    kind,
                    "store_reserve_stripe_batch",
                    &phase_timings,
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ReserveStripeBatchReply { reservations }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_reserve_stripe_batch_total",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn finalize_reservations(
        &self,
        request: Request<FinalizeReservationsRequest>,
    ) -> Result<Response<FinalizeReservationsReply>, Status> {
        let kind = RpcKind::FinalizeReservations;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .finalize_reservations(request.reservation_id, request.placement_indexes)
            .await
        {
            Ok(reservation) => {
                self.stats.record_phase(
                    kind,
                    "store_finalize_reservations",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(FinalizeReservationsReply {
                    reservation: Some(reservation),
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_finalize_reservations",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn finalize_reservations_batch(
        &self,
        request: Request<FinalizeReservationsBatchRequest>,
    ) -> Result<Response<FinalizeReservationsBatchReply>, Status> {
        let kind = RpcKind::FinalizeReservations;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        let mutations = request
            .mutations
            .into_iter()
            .map(mutation_spec_from_proto)
            .collect::<Vec<_>>();
        match self.store.finalize_reservations_batch(mutations).await {
            Ok(reservations) => {
                self.stats.record_phase(
                    kind,
                    "store_finalize_reservations_batch",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(FinalizeReservationsBatchReply {
                    reservations,
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_finalize_reservations_batch",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn release_reservations(
        &self,
        request: Request<ReleaseReservationsRequest>,
    ) -> Result<Response<ReleaseReservationsReply>, Status> {
        let kind = RpcKind::ReleaseReservations;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .release_reservations(request.reservation_id, request.placement_indexes)
            .await
        {
            Ok(reservation) => {
                self.stats.record_phase(
                    kind,
                    "store_release_reservations",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ReleaseReservationsReply {
                    reservation: Some(reservation),
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_release_reservations",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn release_reservations_batch(
        &self,
        request: Request<ReleaseReservationsBatchRequest>,
    ) -> Result<Response<ReleaseReservationsBatchReply>, Status> {
        let kind = RpcKind::ReleaseReservations;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        let mutations = request
            .mutations
            .into_iter()
            .map(mutation_spec_from_proto)
            .collect::<Vec<_>>();
        match self.store.release_reservations_batch(mutations).await {
            Ok(reservations) => {
                self.stats.record_phase(
                    kind,
                    "store_release_reservations_batch",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ReleaseReservationsBatchReply {
                    reservations,
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_release_reservations_batch",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn reclaim_target_granules(
        &self,
        request: Request<ReclaimTargetGranulesRequest>,
    ) -> Result<Response<ReclaimTargetGranulesReply>, Status> {
        let kind = RpcKind::ReclaimTargetGranules;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.store.reclaim_target_granules(request.granules).await {
            Ok(reclaimed_granules) => {
                self.stats.record_phase(
                    kind,
                    "store_reclaim_target_granules",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ReclaimTargetGranulesReply {
                    reclaimed_granules,
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_reclaim_target_granules",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn reserve_rebuild_placement(
        &self,
        request: Request<ReserveRebuildPlacementRequest>,
    ) -> Result<Response<ReserveRebuildPlacementReply>, Status> {
        let kind = RpcKind::ReserveRebuildPlacement;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let phase_started = Instant::now();
        let failure_domain = match request.failure_domain.try_into() {
            Ok(domain) => domain,
            Err(_) => {
                return kas_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::invalid_argument("invalid failure_domain"),
                )
            }
        };
        self.stats
            .record_phase(kind, "request_validate", phase_started.elapsed());
        let phase_started = Instant::now();
        match self
            .store
            .reserve_rebuild_placement(
                request.reservation_id,
                request.failed_target_id,
                failure_domain,
                request.occupied_target_ids,
            )
            .await
        {
            Ok(TimedStoreResult {
                value: reservation,
                phase_timings,
            }) => {
                self.stats.record_phase(
                    kind,
                    "store_reserve_rebuild_total",
                    phase_started.elapsed(),
                );
                record_store_phase_timings(
                    &self.stats,
                    kind,
                    "store_reserve_rebuild",
                    &phase_timings,
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ReserveRebuildPlacementReply {
                    reservation: Some(reservation),
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_reserve_rebuild_total",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn reserve_replacement_placement(
        &self,
        request: Request<ReserveReplacementPlacementRequest>,
    ) -> Result<Response<ReserveReplacementPlacementReply>, Status> {
        let kind = RpcKind::ReserveReplacementPlacement;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let phase_started = Instant::now();
        let failure_domain = match request.failure_domain.try_into() {
            Ok(domain) => domain,
            Err(_) => {
                return kas_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::invalid_argument("invalid failure_domain"),
                )
            }
        };
        self.stats
            .record_phase(kind, "request_validate", phase_started.elapsed());
        let phase_started = Instant::now();
        match self
            .store
            .reserve_replacement_placement(
                request.reservation_id,
                request.replacement_count.max(1) as usize,
                failure_domain,
                request.excluded_target_ids,
                request.reservation_ttl_ms,
                request.required_target_ids,
            )
            .await
        {
            Ok(TimedStoreResult {
                value: reservation,
                phase_timings,
            }) => {
                self.stats.record_phase(
                    kind,
                    "store_reserve_replacement_total",
                    phase_started.elapsed(),
                );
                record_store_phase_timings(
                    &self.stats,
                    kind,
                    "store_reserve_replacement",
                    &phase_timings,
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ReserveReplacementPlacementReply {
                    reservation: Some(reservation),
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_reserve_replacement_total",
                    phase_started.elapsed(),
                );
                kas_err(&self.stats, kind, &started, err)
            }
        }
    }
}

fn kas_err<T>(
    stats: &KasStats,
    kind: RpcKind,
    started: &Instant,
    err: Status,
) -> Result<T, Status> {
    stats.record_error(kind, started.elapsed(), err.to_string());
    Err(err)
}

fn record_store_phase_timings(
    stats: &KasStats,
    kind: RpcKind,
    prefix: &str,
    phases: &[StorePhaseTiming],
) {
    for phase in phases {
        stats.record_phase(kind, &format!("{prefix}.{}", phase.name), phase.elapsed);
    }
}
