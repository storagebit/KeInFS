// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

mod config;
mod stats;

use config::parse_args;
use keinbuild::{build_info, config_hash_hex, hostname_or_unknown};
use keinctl::proto::kas_client::KasClient;
use keinctl::proto::kms_client::KmsClient;
use keinctl::proto::{
    BuildInfo as ProtoBuildInfo, CommitPlacementTaskRequest, FailPlacementTaskRequest,
    FinalizeReservationsRequest, LeasePlacementTasksRequest, PlacementReservationRecord,
    PlacementTask, PlacementTaskKind, ReleaseReservationsRequest, ReserveRebuildPlacementRequest,
    ReserveReplacementPlacementRequest,
};
use ksc::client::TargetSession;
use ksc::object::{chunk_id_from_proto, kee_profile_from_control};
use stats::{KrsIdentity, KrsStats, Publisher};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

const KRS_GRPC_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug)]
struct PlacementExecutionError {
    permanent: bool,
    message: String,
}

impl PlacementExecutionError {
    fn transient(message: impl Into<String>) -> Self {
        Self {
            permanent: false,
            message: message.into(),
        }
    }

    fn permanent(message: impl Into<String>) -> Self {
        Self {
            permanent: true,
            message: message.into(),
        }
    }

    fn is_permanent(&self) -> bool {
        self.permanent
    }
}

impl std::fmt::Display for PlacementExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PlacementExecutionError {}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let config = parse_args(args)?;
    let build = build_info!();
    let started_at_unix_ms = now_unix_ms();

    let kms_channel = Endpoint::from_shared(config.kms_endpoint.clone())?
        .connect()
        .await?;
    let kas_channels = Arc::new(build_channels(&config.kas_endpoints())?);
    if kas_channels.is_empty() {
        return Err("KRS requires at least one KAS endpoint".into());
    }

    let stats = KrsStats::new(KrsIdentity {
        build: build.clone(),
        lease_owner: config.lease_owner.clone(),
        kms_endpoint: config.kms_endpoint.clone(),
        kas_endpoint: config.kas_endpoint.clone(),
        pid: std::process::id(),
        stats_root: config.stats_root.display().to_string(),
    });
    let publisher = Publisher::spawn(
        stats.clone(),
        &config.stats_root,
        config.stats_publish_interval,
    )?;
    let registry = tokio::spawn(service_registration_loop(
        kas_channels.clone(),
        keinctl::proto::ServiceInstanceRecord {
            instance_id: format!("krs:{}", config.lease_owner),
            service_kind: keinctl::proto::ServiceKind::Krs as i32,
            node_id: hostname_or_unknown(),
            endpoint: String::new(),
            package_name: build.package_name.clone(),
            build: Some(ProtoBuildInfo {
                package_name: build.package_name.clone(),
                binary_name: build.binary_name.clone(),
                version: build.version.clone(),
                release: build.release,
                git_sha: build.git_sha.clone(),
                git_dirty: build.git_dirty,
                built_at_unix_s: build.built_at_unix_s,
                build_profile: build.build_profile.clone(),
                target_triple: build.target_triple.clone(),
            }),
            config_hash: config_hash_hex(&config.fingerprint_source()),
            pid: std::process::id(),
            runtime_root: config.stats_root.display().to_string(),
            instance_label: config.lease_owner.clone(),
            started_at_unix_ms,
            heartbeat_at_unix_ms: started_at_unix_ms,
            heartbeat_interval_ms: 5_000,
        },
        stats.clone(),
        std::time::Duration::from_secs(5),
    ));

    let mut ticker = tokio::time::interval(config.poll_interval);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = ticker.tick() => {
                stats.record_poll();
                let cycle_started = Instant::now();
                if let Err(err) = process_lease_cycle(
                    kms_channel.clone(),
                    kas_channels.clone(),
                    config.lease_owner.clone(),
                    config.max_tasks,
                    config.lease_ttl.as_millis() as u64,
                    stats.clone(),
                ).await {
                    stats.set_last_error(format!("KRS lease cycle failed: {err}"));
                }
                stats.record_lease_cycle(cycle_started.elapsed());
            }
        }
    }

    registry.abort();
    publisher.stop();
    Ok(())
}

async fn service_registration_loop(
    kas_channels: Arc<Vec<Channel>>,
    mut instance: keinctl::proto::ServiceInstanceRecord,
    stats: std::sync::Arc<KrsStats>,
    interval: std::time::Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        instance.heartbeat_at_unix_ms = now_unix_ms();
        if let Err(err) = upsert_service_instance(&kas_channels, &instance).await {
            stats.set_last_error(format!("KRS service registration failed: {err}"));
        }
    }
}

async fn process_lease_cycle(
    kms_channel: Channel,
    kas_channels: Arc<Vec<Channel>>,
    lease_owner: String,
    max_tasks: u32,
    lease_ttl_ms: u64,
    stats: std::sync::Arc<KrsStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut kms = kms_client(kms_channel.clone());
    let phase_started = Instant::now();
    let leased = kms
        .lease_placement_tasks(LeasePlacementTasksRequest {
            lease_owner,
            max_tasks,
            lease_ttl_ms,
        })
        .await?
        .into_inner()
        .tasks;
    stats.record_phase("lease_rpc", phase_started.elapsed());
    stats.record_leased_tasks(leased.len());
    let mut batches = HashMap::<String, Vec<keinctl::proto::LeasedPlacementTask>>::new();
    for leased_task in leased {
        let task = leased_task
            .task
            .as_ref()
            .ok_or("KRS leased placement task missing task body")?;
        batches
            .entry(task.source_target_id.clone())
            .or_default()
            .push(leased_task);
    }
    let mut join_set = JoinSet::new();
    for leased_batch in batches.into_values() {
        let kms_channel = kms_channel.clone();
        let kas_channels = kas_channels.clone();
        let stats = stats.clone();
        join_set.spawn(async move {
            process_source_batch(kms_channel, kas_channels, leased_batch, stats).await
        });
    }
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(err)) => stats.set_last_error(format!("KRS source batch failed: {err}")),
            Err(err) => stats.set_last_error(format!("KRS batch join failed: {err}")),
        }
    }
    stats.set_active_task(None);
    Ok(())
}

async fn process_source_batch(
    kms_channel: Channel,
    kas_channels: Arc<Vec<Channel>>,
    leased_batch: Vec<keinctl::proto::LeasedPlacementTask>,
    stats: std::sync::Arc<KrsStats>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut work_items = Vec::with_capacity(leased_batch.len());
    for leased_task in leased_batch {
        let task = leased_task
            .task
            .ok_or("KRS leased placement task missing task body")?;
        let manifest = leased_task
            .manifest
            .ok_or("KRS leased placement task missing manifest")?;
        let ec_profile = leased_task
            .ec_profile
            .ok_or("KRS leased placement task missing ec_profile")?;
        work_items.push((task, manifest, ec_profile));
    }

    let shared_source_session = match shared_source_endpoint(&work_items) {
        Some(endpoint) => Some(TargetSession::connect(&endpoint).await.map_err(|err| {
            format!("KRS could not connect shared source session {endpoint}: {err}")
        })?),
        None => None,
    };

    let mut join_set = JoinSet::new();
    for (task, manifest, ec_profile) in work_items {
        let kms_channel = kms_channel.clone();
        let kas_channels = kas_channels.clone();
        let task_id = task.task_id.clone();
        stats.set_active_task(Some(task_id));
        let task_started = Instant::now();
        let stats = stats.clone();
        let task_for_result = task.clone();
        let source_session = shared_source_session.clone();
        join_set.spawn(async move {
            let result = execute_placement_task(
                kms_channel,
                kas_channels,
                task,
                manifest,
                ec_profile,
                source_session,
                stats,
            )
            .await;
            (task_for_result, result, task_started)
        });
    }

    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok((task, result, task_started)) => match result {
                Ok(rebuilt_bytes) => {
                    stats.record_rebuilt_task(rebuilt_bytes, task_started.elapsed())
                }
                Err(err) => {
                    let message = format!("KRS task {} failed: {err}", task.task_id);
                    stats.record_failed_task(message.clone(), task_started.elapsed());
                    if err.is_permanent() {
                        let phase_started = Instant::now();
                        let mut kms = kms_client(kms_channel.clone());
                        match kms
                            .fail_placement_task(FailPlacementTaskRequest {
                                task_id: task.task_id.clone(),
                                failure_reason: err.to_string(),
                            })
                            .await
                        {
                            Ok(_) => stats
                                .record_phase("kms_fail_placement_task", phase_started.elapsed()),
                            Err(fail_err) => stats.set_last_error(format!(
                                "{}; KRS could not mark task failed in KMS: {}",
                                message, fail_err
                            )),
                        }
                    }
                }
            },
            Err(err) => stats.set_last_error(format!("KRS task join failed: {err}")),
        }
    }

    Ok(())
}

fn shared_source_endpoint(
    work_items: &[(
        PlacementTask,
        keinctl::proto::ObjectVersionManifest,
        keinctl::proto::EcProfile,
    )],
) -> Option<String> {
    let mut endpoint = None::<String>;
    for (task, manifest, _) in work_items {
        let task_kind =
            PlacementTaskKind::try_from(task.task_kind).unwrap_or(PlacementTaskKind::Unspecified);
        if !matches!(
            task_kind,
            PlacementTaskKind::Rebalance | PlacementTaskKind::Evacuate
        ) {
            return None;
        }
        let source_fragment = source_fragment_from_manifest(task, manifest).ok()?;
        match &endpoint {
            Some(existing) if existing != &source_fragment.endpoint => return None,
            Some(_) => {}
            None => endpoint = Some(source_fragment.endpoint.clone()),
        }
    }
    endpoint
}

async fn execute_placement_task(
    kms_channel: Channel,
    kas_channels: Arc<Vec<Channel>>,
    task: PlacementTask,
    manifest: keinctl::proto::ObjectVersionManifest,
    ec_profile: keinctl::proto::EcProfile,
    source_session: Option<TargetSession>,
    stats: std::sync::Arc<KrsStats>,
) -> Result<u64, PlacementExecutionError> {
    let phase_started = Instant::now();
    let kee_profile = kee_profile_from_control(&ec_profile).map_err(|err| {
        PlacementExecutionError::permanent(format!(
            "bad EC profile in leased task {}: {}",
            task.task_id, err
        ))
    })?;
    let engine = kee::KeeEngine::new(kee_profile)
        .map_err(|err| PlacementExecutionError::permanent(err.to_string()))?;
    stats.record_phase("codec_prepare", phase_started.elapsed());
    let task_kind =
        PlacementTaskKind::try_from(task.task_kind).unwrap_or(PlacementTaskKind::Unspecified);
    if task_kind == PlacementTaskKind::Unspecified {
        return Err(PlacementExecutionError::permanent(format!(
            "placement task {} has unknown task kind {}",
            task.task_id, task.task_kind
        )));
    }
    let stripe = manifest
        .stripes
        .get(task.stripe_index as usize)
        .ok_or_else(|| {
            PlacementExecutionError::permanent(format!(
                "manifest {} has no stripe {}",
                manifest.version_id, task.stripe_index
            ))
        })?;
    let source_fragment = source_fragment_from_manifest(&task, &manifest)?;

    let replacement_payload = match task_kind {
        PlacementTaskKind::Rebuild => {
            let mut fragments = vec![None; stripe.fragments.len()];
            let phase_started = Instant::now();
            for plan in &stripe.fragments {
                if plan.fragment_index == task.fragment_index
                    && plan.target_id == task.source_target_id
                {
                    continue;
                }
                let session = match TargetSession::connect(&plan.endpoint).await {
                    Ok(session) => session,
                    Err(_) => continue,
                };
                let chunk_id = chunk_id_from_proto(&plan.chunk_id)
                    .map_err(|err| PlacementExecutionError::permanent(err.to_string()))?;
                if let Ok(reply) = session.read_chunk(chunk_id).await {
                    fragments[plan.fragment_index as usize] = Some(reply.value.payload);
                }
            }
            stats.record_phase("source_read", phase_started.elapsed());

            let phase_started = Instant::now();
            let rebuilt = engine
                .reconstruct(&mut fragments)
                .map_err(|err| PlacementExecutionError::permanent(err.to_string()))?;
            stats.record_phase("ec_reconstruct", phase_started.elapsed());
            rebuilt
                .get(task.fragment_index as usize)
                .ok_or_else(|| {
                    PlacementExecutionError::permanent(format!(
                        "rebuilt fragment {} is missing",
                        task.fragment_index
                    ))
                })?
                .clone()
        }
        PlacementTaskKind::Rebalance | PlacementTaskKind::Evacuate => {
            let session = if let Some(session) = source_session.clone() {
                session
            } else {
                let phase_started = Instant::now();
                let session = TargetSession::connect(&source_fragment.endpoint)
                    .await
                    .map_err(|err| PlacementExecutionError::transient(err.to_string()))?;
                stats.record_phase("source_connect", phase_started.elapsed());
                session
            };
            let chunk_id = chunk_id_from_proto(&source_fragment.chunk_id)
                .map_err(|err| PlacementExecutionError::permanent(err.to_string()))?;
            let phase_started = Instant::now();
            let reply = session
                .read_chunk(chunk_id)
                .await
                .map_err(|err| PlacementExecutionError::transient(err.to_string()))?;
            stats.record_phase("source_read", phase_started.elapsed());
            reply.value.payload
        }
        PlacementTaskKind::Unspecified => unreachable!(),
    };

    let occupied_target_ids = stripe
        .fragments
        .iter()
        .map(|fragment| fragment.target_id.clone())
        .collect::<Vec<_>>();
    let reservation_id = format!("placement-{}-{}", task.task_id, Uuid::new_v4());
    let required_target_ids = (!task.destination_target_id.is_empty())
        .then(|| vec![task.destination_target_id.clone()])
        .unwrap_or_default();
    let phase_started = Instant::now();
    let (reservation, reserve_phase, reservation_kas_channel) = reserve_task_placement(
        &kas_channels,
        task_kind,
        &reservation_id,
        &task.source_target_id,
        ec_profile.failure_domain,
        &occupied_target_ids,
        &required_target_ids,
    )
    .await?;
    stats.record_phase(reserve_phase, phase_started.elapsed());
    let placement = reservation.placements.first().cloned().ok_or_else(|| {
        PlacementExecutionError::transient(
            "KAS replacement reservation has no placements".to_string(),
        )
    })?;
    if !task.destination_target_id.is_empty() && placement.target_id != task.destination_target_id {
        return Err(PlacementExecutionError::permanent(format!(
            "KAS returned target {} but task {} requires {}",
            placement.target_id, task.task_id, task.destination_target_id
        )));
    }
    let generation = source_fragment.generation.saturating_add(1);
    let replacement_fragment = keinctl::proto::FragmentPlan {
        fragment_index: task.fragment_index,
        chunk_id: source_fragment.chunk_id.clone(),
        target_id: placement.target_id.clone(),
        endpoint: placement.endpoint.clone(),
        granule_index: placement.granule_index,
        generation,
        stripe_index: task.stripe_index,
    };

    let write_result = async {
        let phase_started = Instant::now();
        let session = TargetSession::connect(&placement.endpoint)
            .await
            .map_err(|err| PlacementExecutionError::transient(err.to_string()))?;
        stats.record_phase("replacement_connect", phase_started.elapsed());
        let chunk_id = chunk_id_from_proto(&replacement_fragment.chunk_id)
            .map_err(|err| PlacementExecutionError::permanent(err.to_string()))?;
        let phase_started = Instant::now();
        session
            .write_chunk(
                chunk_id,
                replacement_fragment.granule_index,
                replacement_fragment.generation,
                replacement_payload.clone(),
            )
            .await
            .map_err(|err| PlacementExecutionError::transient(err.to_string()))?;
        stats.record_phase("replacement_write", phase_started.elapsed());
        let mut kms = kms_client(kms_channel.clone());
        let phase_started = Instant::now();
        kms.commit_placement_task(CommitPlacementTaskRequest {
            task_id: task.task_id.clone(),
            replacement_fragment: Some(replacement_fragment.clone()),
        })
        .await
        .map_err(|err| PlacementExecutionError::transient(err.to_string()))?;
        stats.record_phase("kms_commit_placement_task", phase_started.elapsed());
        let mut kas = kas_client(reservation_kas_channel.clone());
        let phase_started = Instant::now();
        kas.finalize_reservations(FinalizeReservationsRequest {
            reservation_id: reservation_id.clone(),
            placement_indexes: vec![0],
        })
        .await
        .map_err(|err| PlacementExecutionError::transient(err.to_string()))?;
        stats.record_phase("kas_finalize_placement", phase_started.elapsed());
        if matches!(
            task_kind,
            PlacementTaskKind::Rebalance | PlacementTaskKind::Evacuate
        ) {
            let phase_started = Instant::now();
            let delete_session = if let Some(session) = source_session.clone() {
                Ok(session)
            } else {
                TargetSession::connect(&source_fragment.endpoint).await
            };
            if let Ok(session) = delete_session {
                let chunk_id = chunk_id_from_proto(&source_fragment.chunk_id)
                    .map_err(|err| PlacementExecutionError::permanent(err.to_string()))?;
                let _ = session.delete_chunk(chunk_id).await;
            }
            stats.record_phase("source_delete", phase_started.elapsed());
        }
        Ok::<(), PlacementExecutionError>(())
    }
    .await;

    if let Err(err) = write_result {
        let mut kas = kas_client(reservation_kas_channel);
        let phase_started = Instant::now();
        let _ = kas
            .release_reservations(ReleaseReservationsRequest {
                reservation_id,
                placement_indexes: vec![0],
            })
            .await;
        stats.record_phase("kas_release_placement", phase_started.elapsed());
        return Err(err);
    }

    Ok(replacement_payload.len() as u64)
}

fn source_fragment_from_manifest<'a>(
    task: &PlacementTask,
    manifest: &'a keinctl::proto::ObjectVersionManifest,
) -> Result<&'a keinctl::proto::FragmentPlan, PlacementExecutionError> {
    let stripe = manifest
        .stripes
        .get(task.stripe_index as usize)
        .ok_or_else(|| {
            PlacementExecutionError::permanent(format!(
                "manifest {} has no stripe {}",
                manifest.version_id, task.stripe_index
            ))
        })?;
    let source_fragment = stripe
        .fragments
        .get(task.fragment_index as usize)
        .ok_or_else(|| {
            PlacementExecutionError::permanent(format!(
                "source fragment {} is missing",
                task.fragment_index
            ))
        })?;
    if source_fragment.target_id != task.source_target_id {
        return Err(PlacementExecutionError::permanent(format!(
            "placement task {} source target mismatch: manifest has {}, task expects {}",
            task.task_id, source_fragment.target_id, task.source_target_id
        )));
    }
    Ok(source_fragment)
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn build_channels(endpoints: &[String]) -> Result<Vec<Channel>, Box<dyn std::error::Error>> {
    if endpoints.is_empty() {
        return Err("KRS requires at least one KAS endpoint".into());
    }
    Ok(endpoints
        .iter()
        .map(|endpoint| {
            Endpoint::from_shared(endpoint.clone()).map(|channel| channel.connect_lazy())
        })
        .collect::<Result<Vec<_>, _>>()?)
}

async fn upsert_service_instance(
    kas_channels: &[Channel],
    instance: &keinctl::proto::ServiceInstanceRecord,
) -> Result<(), tonic::Status> {
    let mut last_err = None;
    for channel in kas_channels {
        let mut kas = kas_client(channel.clone());
        match kas
            .upsert_service_instance(keinctl::proto::UpsertServiceInstanceRequest {
                instance: Some(instance.clone()),
            })
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err.unwrap_or_else(|| tonic::Status::unavailable("no KAS channels are available")))
}

async fn reserve_task_placement(
    kas_channels: &[Channel],
    task_kind: PlacementTaskKind,
    reservation_id: &str,
    source_target_id: &str,
    failure_domain: i32,
    occupied_target_ids: &[String],
    required_target_ids: &[String],
) -> Result<(PlacementReservationRecord, &'static str, Channel), PlacementExecutionError> {
    if matches!(task_kind, PlacementTaskKind::Rebuild) {
        if let Ok(result) = try_reserve_rebuild_placement(
            kas_channels,
            reservation_id,
            source_target_id,
            failure_domain,
            occupied_target_ids,
        )
        .await
        {
            return Ok(result);
        }
        return try_reserve_replacement_placement(
            kas_channels,
            reservation_id,
            failure_domain,
            occupied_target_ids,
            &[],
            "kas_reserve_rebuild_fallback",
        )
        .await;
    }

    try_reserve_replacement_placement(
        kas_channels,
        reservation_id,
        failure_domain,
        occupied_target_ids,
        required_target_ids,
        "kas_reserve_replacement",
    )
    .await
}

async fn try_reserve_rebuild_placement(
    kas_channels: &[Channel],
    reservation_id: &str,
    source_target_id: &str,
    failure_domain: i32,
    occupied_target_ids: &[String],
) -> Result<(PlacementReservationRecord, &'static str, Channel), PlacementExecutionError> {
    let mut last_err = None;
    for channel in kas_channels {
        let mut kas = kas_client(channel.clone());
        match kas
            .reserve_rebuild_placement(ReserveRebuildPlacementRequest {
                reservation_id: reservation_id.to_string(),
                failed_target_id: source_target_id.to_string(),
                failure_domain,
                occupied_target_ids: occupied_target_ids.to_vec(),
            })
            .await
        {
            Ok(reply) => {
                let reservation = reply.into_inner().reservation.ok_or_else(|| {
                    PlacementExecutionError::transient(
                        "KAS did not return rebuild reservation".to_string(),
                    )
                })?;
                return Ok((reservation, "kas_reserve_rebuild", channel.clone()));
            }
            Err(err) => {
                last_err = Some(PlacementExecutionError::transient(err.to_string()));
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        PlacementExecutionError::transient("no KAS endpoints were available".to_string())
    }))
}

async fn try_reserve_replacement_placement(
    kas_channels: &[Channel],
    reservation_id: &str,
    failure_domain: i32,
    occupied_target_ids: &[String],
    required_target_ids: &[String],
    phase_name: &'static str,
) -> Result<(PlacementReservationRecord, &'static str, Channel), PlacementExecutionError> {
    let mut last_err = None;
    for channel in kas_channels {
        let mut kas = kas_client(channel.clone());
        match kas
            .reserve_replacement_placement(ReserveReplacementPlacementRequest {
                reservation_id: reservation_id.to_string(),
                replacement_count: 1,
                failure_domain,
                excluded_target_ids: occupied_target_ids.to_vec(),
                reservation_ttl_ms: 30_000,
                required_target_ids: required_target_ids.to_vec(),
            })
            .await
        {
            Ok(reply) => {
                let reservation = reply.into_inner().reservation.ok_or_else(|| {
                    PlacementExecutionError::transient(
                        "KAS did not return replacement reservation".to_string(),
                    )
                })?;
                return Ok((reservation, phase_name, channel.clone()));
            }
            Err(err) => {
                last_err = Some(PlacementExecutionError::transient(err.to_string()));
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        PlacementExecutionError::transient("no KAS endpoints were available".to_string())
    }))
}

fn kms_client(channel: Channel) -> KmsClient<Channel> {
    KmsClient::new(channel)
        .max_decoding_message_size(KRS_GRPC_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(KRS_GRPC_MAX_MESSAGE_BYTES)
}

fn kas_client(channel: Channel) -> KasClient<Channel> {
    KasClient::new(channel)
        .max_decoding_message_size(KRS_GRPC_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(KRS_GRPC_MAX_MESSAGE_BYTES)
}
