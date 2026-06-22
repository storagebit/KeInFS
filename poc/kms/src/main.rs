// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

mod config;
mod fdb_hot_store;
mod fdb_schema;
mod hot_store;
mod read_cache;
mod service;
mod stats;
mod store;
mod watch;

use config::parse_args;
use fdb_hot_store::{maybe_boot_network, FdbHotStore};
use hot_store::HotMetadataStore;
use keinbuild::{build_info, config_hash_hex, hostname_or_unknown};
use keinctl::proto::kms_server::KmsServer;
use read_cache::ResolveObjectReadCache;
use service::{
    reap_expired_intents, reconcile_pending_reservations, reservation_mutation_dispatch_loop,
    AllocationRouteCache, KasEndpoint, KasEndpointBalancer, KmsService, ReservationCache,
    ReservationCacheConfig, ReservationMutationDispatcher,
};
use stats::{KmsIdentity, KmsStats, Publisher};
use store::KmsStore;
use tonic::transport::{Endpoint, Server};
use tonic::Request;
use tonic_health::server::health_reporter;
use watch::NotificationHub;

const KMS_GRPC_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
const RESERVATION_FINALIZER_GRACE: std::time::Duration = std::time::Duration::from_secs(15);

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let config = parse_args(args)?;
    let _fdb_network = maybe_boot_network()?;
    let build = build_info!();
    let started_at_unix_ms = now_unix_ms();
    let node_id = hostname_or_unknown();
    let config_hash = config_hash_hex(&config.fingerprint_source());

    let kas_channels = connect_kas_channels(&config.kas_endpoints).await?;

    let stats = KmsStats::new(KmsIdentity {
        build: build.clone(),
        listen_addr: config.listen_addr.to_string(),
        kas_endpoints: config.kas_endpoints.join(","),
        shard_id: config.shard_id.clone(),
        public_endpoint: config.public_endpoint.clone(),
        metadata_store: format!("foundationdb:{}", config.foundationdb_cluster_file),
        pid: std::process::id(),
        stats_root: config.stats_root.display().to_string(),
    });
    let publisher = Publisher::spawn(
        stats.clone(),
        &config.stats_root,
        config.stats_publish_interval,
    )?;

    let store = KmsStore::connect(&config.foundationdb_cluster_file).await?;
    store.init().await?;
    #[cfg(target_os = "linux")]
    if config.target_current_fragment_backfill_on_startup {
        let store = store.clone();
        tokio::spawn(async move {
            eprintln!("kms: target-current-fragment backfill started");
            match store.backfill_target_current_fragment_index().await {
                Ok(()) => eprintln!("kms: target-current-fragment backfill completed"),
                Err(err) => {
                    eprintln!("kms: target-current-fragment backfill failed: {err}");
                }
            }
        });
    }

    let notifications = NotificationHub::spawn(
        config.notification_subject.clone(),
        config.notification_nats_url.clone(),
        config.notification_mode,
        config.notification_poll_interval,
        stats.clone(),
    );
    let read_cache =
        ResolveObjectReadCache::new(config.read_cache_max_entries, config.read_cache_ttl);
    let read_cache_invalidator =
        read_cache.spawn_invalidator(notifications.subscribe(), stats.clone());
    let hot_store: std::sync::Arc<dyn HotMetadataStore> =
        std::sync::Arc::new(FdbHotStore::connect(&config.foundationdb_cluster_file)?);
    let (reservation_mutation_sender, reservation_mutation_receiver) =
        tokio::sync::mpsc::unbounded_channel();

    let service = KmsService {
        store: store.clone(),
        hot_store,
        notifications: notifications.clone(),
        read_cache: read_cache.clone(),
        kas_channels: KasEndpointBalancer::new(kas_channels.clone()),
        stats: stats.clone(),
        write_intent_ttl: config.write_intent_ttl,
        reservation_finalizer_grace: RESERVATION_FINALIZER_GRACE,
        large_write_initiate_gate: std::sync::Arc::new(tokio::sync::Semaphore::new(
            config.large_write_initiate_max_concurrency,
        )),
        reservation_cache: ReservationCache::new(ReservationCacheConfig {
            high_watermark: config.reservation_cache_high_watermark,
            low_watermark: config.reservation_cache_low_watermark,
            refill_batch: config.reservation_cache_refill_batch,
            reservation_ttl: config.reservation_cache_ttl,
            min_usable_ttl: config.reservation_cache_min_usable_ttl,
            refill_concurrency: config.reservation_cache_refill_concurrency,
            wait_timeout: config.reservation_cache_wait_timeout,
            stale_refill_after: config.reservation_cache_stale_refill,
            small_object_max_stripes: config.reservation_cache_small_object_max_stripes,
            single_window_seed_batch: config.reservation_cache_single_window_seed_batch,
            initiate_write_window_max_stripes: config.initiate_write_window_max_stripes,
        }),
        route_cache: AllocationRouteCache::new(config.allocation_route_cache_ttl, stats.clone()),
        write_profile_max_stripes: config.write_profile_max_stripes,
        write_profile_min_fragment_bytes: config.write_profile_min_fragment_bytes,
        reservation_mutation_batch_size: config.reservation_mutation_batch_size,
        reservation_mutation_dispatcher: ReservationMutationDispatcher::new(
            reservation_mutation_sender,
        ),
        kas_rpc_timeout: config.kas_rpc_timeout,
        kas_reserve_attempt_timeout: config.kas_reserve_attempt_timeout,
        bucket_write_contexts: std::sync::Arc::new(std::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        ec_profile_catalog: std::sync::Arc::new(std::sync::Mutex::new(None)),
        object_parent_contexts: std::sync::Arc::new(std::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
    };
    let registry_instance = keinctl::proto::ServiceInstanceRecord {
        instance_id: format!("kms:{}", config.public_endpoint),
        service_kind: keinctl::proto::ServiceKind::Kms as i32,
        node_id,
        endpoint: config.public_endpoint.clone(),
        package_name: build.package_name.clone(),
        build: Some(build_info_to_proto(&build)),
        config_hash,
        pid: std::process::id(),
        runtime_root: config.stats_root.display().to_string(),
        instance_label: config.shard_id.clone(),
        started_at_unix_ms,
        heartbeat_at_unix_ms: started_at_unix_ms,
        heartbeat_interval_ms: config.service_heartbeat_interval.as_millis() as u64,
    };

    let reaper = Some(tokio::spawn(expiry_reaper_loop(
        store.clone(),
        KasEndpointBalancer::new(kas_channels.clone()),
        stats.clone(),
        config.reservation_mutation_batch_size,
        config.expiry_reap_interval,
    )));
    let finalizer = Some(tokio::spawn(reservation_finalizer_loop(
        std::sync::Arc::clone(&service.hot_store),
        KasEndpointBalancer::new(kas_channels.clone()),
        stats.clone(),
        config.expiry_reap_interval,
        config.reservation_mutation_batch_size,
        config.kas_rpc_timeout.saturating_mul(3),
    )));
    let lease_reaper = Some(tokio::spawn(object_lease_reaper_loop(
        std::sync::Arc::clone(&service.hot_store),
        stats.clone(),
        config.expiry_reap_interval,
    )));
    let reservation_dispatcher = Some(tokio::spawn(reservation_mutation_dispatch_loop(
        std::sync::Arc::clone(&service.hot_store),
        KasEndpointBalancer::new(kas_channels.clone()),
        stats.clone(),
        reservation_mutation_receiver,
        config.reservation_mutation_dispatch_batch_size,
        config.reservation_mutation_dispatch_flush,
        config.reservation_mutation_batch_size,
        config.kas_rpc_timeout.saturating_mul(3),
    )));
    let registry = tokio::spawn(service_registration_loop(
        KasEndpointBalancer::new(kas_channels.clone()),
        registry_instance,
        stats.clone(),
        config.service_heartbeat_interval,
    ));
    let (health_reporter, health_service) = health_reporter();
    health_reporter.set_serving::<KmsServer<KmsService>>().await;

    Server::builder()
        .add_service(health_service)
        .add_service(
            KmsServer::new(service)
                .max_decoding_message_size(KMS_GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(KMS_GRPC_MAX_MESSAGE_BYTES),
        )
        .serve_with_shutdown(config.listen_addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    if let Some(reaper) = reaper {
        reaper.abort();
    }
    if let Some(finalizer) = finalizer {
        finalizer.abort();
    }
    if let Some(lease_reaper) = lease_reaper {
        lease_reaper.abort();
    }
    if let Some(reservation_dispatcher) = reservation_dispatcher {
        reservation_dispatcher.abort();
    }
    registry.abort();
    read_cache_invalidator.abort();
    publisher.stop();
    Ok(())
}

async fn expiry_reaper_loop(
    store: KmsStore,
    kas_channels: KasEndpointBalancer,
    stats: std::sync::Arc<KmsStats>,
    reservation_mutation_batch_size: usize,
    interval: std::time::Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut consecutive_errors = 0usize;
    loop {
        ticker.tick().await;
        if let Err(err) = reap_expired_intents(
            store.clone(),
            kas_channels.clone(),
            stats.clone(),
            reservation_mutation_batch_size,
        )
        .await
        {
            consecutive_errors = consecutive_errors.saturating_add(1);
            stats.set_last_error(format!("KMS expiry reaper failed: {err}"));
            tokio::time::sleep(reaper_error_backoff(interval, consecutive_errors)).await;
        } else {
            consecutive_errors = 0;
        }
    }
}

async fn service_registration_loop(
    kas_channels: KasEndpointBalancer,
    mut instance: keinctl::proto::ServiceInstanceRecord,
    stats: std::sync::Arc<KmsStats>,
    interval: std::time::Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        instance.heartbeat_at_unix_ms = now_unix_ms();
        let mut client = kas_channels.client();
        if let Err(err) = client
            .upsert_service_instance(Request::new(keinctl::proto::UpsertServiceInstanceRequest {
                instance: Some(instance.clone()),
            }))
            .await
        {
            stats.set_last_error(format!("KMS service registration failed: {err}"));
        }
    }
}

async fn object_lease_reaper_loop(
    hot_store: std::sync::Arc<dyn HotMetadataStore>,
    stats: std::sync::Arc<KmsStats>,
    interval: std::time::Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut consecutive_errors = 0usize;
    loop {
        ticker.tick().await;
        match hot_store.reap_expired_leases(now_unix_ms(), 256).await {
            Ok(_) => consecutive_errors = 0,
            Err(err) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                stats.set_last_error(format!("KMS write-lease reaper failed: {err}"));
                tokio::time::sleep(reaper_error_backoff(interval, consecutive_errors)).await;
            }
        }
    }
}

async fn reservation_finalizer_loop(
    hot_store: std::sync::Arc<dyn HotMetadataStore>,
    kas_channels: KasEndpointBalancer,
    stats: std::sync::Arc<KmsStats>,
    interval: std::time::Duration,
    reservation_mutation_batch_size: usize,
    rpc_timeout: std::time::Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut consecutive_errors = 0usize;
    loop {
        ticker.tick().await;
        if let Err(err) = reconcile_pending_reservations(
            hot_store.clone(),
            kas_channels.clone(),
            stats.clone(),
            128,
            reservation_mutation_batch_size,
            rpc_timeout,
            RESERVATION_FINALIZER_GRACE,
        )
        .await
        {
            consecutive_errors = consecutive_errors.saturating_add(1);
            stats.set_last_error(format!("KMS reservation finalizer failed: {err}"));
            tokio::time::sleep(reaper_error_backoff(interval, consecutive_errors)).await;
        } else {
            consecutive_errors = 0;
        }
    }
}

fn reaper_error_backoff(
    interval: std::time::Duration,
    consecutive_errors: usize,
) -> std::time::Duration {
    let shift = consecutive_errors.saturating_sub(1).min(5) as u32;
    let multiplier = 1u32 << shift;
    let scaled = interval.saturating_mul(multiplier);
    scaled.clamp(
        std::time::Duration::from_millis(250),
        std::time::Duration::from_secs(15),
    )
}

async fn connect_kas_channels(
    endpoints: &[String],
) -> Result<Vec<KasEndpoint>, Box<dyn std::error::Error>> {
    const KAS_GRPC_INITIAL_STREAM_WINDOW_BYTES: u32 = 4 * 1024 * 1024;
    const KAS_GRPC_INITIAL_CONNECTION_WINDOW_BYTES: u32 = 64 * 1024 * 1024;
    const KAS_GRPC_KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
    const KAS_GRPC_KEEPALIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    let mut channels = Vec::with_capacity(endpoints.len());
    let mut errors = Vec::new();
    for endpoint in endpoints {
        let endpoint = match Endpoint::from_shared(endpoint.clone()) {
            Ok(endpoint) => endpoint
                .initial_stream_window_size(KAS_GRPC_INITIAL_STREAM_WINDOW_BYTES)
                .initial_connection_window_size(KAS_GRPC_INITIAL_CONNECTION_WINDOW_BYTES)
                .http2_keep_alive_interval(KAS_GRPC_KEEPALIVE_INTERVAL)
                .keep_alive_timeout(KAS_GRPC_KEEPALIVE_TIMEOUT)
                .keep_alive_while_idle(true),
            Err(err) => {
                errors.push(format!("{endpoint} invalid: {err}"));
                continue;
            }
        };
        let endpoint_uri = endpoint.uri().to_string();
        match endpoint.connect().await {
            Ok(channel) => channels.push(KasEndpoint {
                endpoint: endpoint_uri,
                channel,
            }),
            Err(err) => errors.push(format!("{} connect failed: {}", endpoint_uri, err)),
        }
    }
    if channels.is_empty() {
        return Err(format!(
            "KMS could not connect to any KAS endpoint: {}",
            errors.join(" | ")
        )
        .into());
    }
    Ok(channels)
}

fn build_info_to_proto(build: &keinbuild::BuildInfo) -> keinctl::proto::BuildInfo {
    keinctl::proto::BuildInfo {
        package_name: build.package_name.clone(),
        binary_name: build.binary_name.clone(),
        version: build.version.clone(),
        release: build.release,
        git_sha: build.git_sha.clone(),
        git_dirty: build.git_dirty,
        built_at_unix_s: build.built_at_unix_s,
        build_profile: build.build_profile.clone(),
        target_triple: build.target_triple.clone(),
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
