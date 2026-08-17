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
    reap_expired_intents, reconcile_pending_reservations, run_orphan_gc_sweep,
    run_orphan_version_reap, AllocationRouteCache, KasEndpoint, KasEndpointBalancer, KmsService,
    ReservationCache, ReservationCacheConfig, ReservationMutationDispatcher,
};
use stats::{KmsIdentity, KmsStats, Publisher};
use store::KmsStore;
use tonic::transport::{Endpoint, Server};
use tonic::Request;
use tonic_health::server::health_reporter;
use watch::NotificationHub;

const KMS_GRPC_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
const RESERVATION_FINALIZER_GRACE: std::time::Duration = std::time::Duration::from_secs(15);
/// Grace beyond a write lease's expiry before its uncommitted granules become
/// reclaimable. It MUST exceed the worst-case clock skew between KMS instances plus the
/// commit latency, so a write whose lease just lapsed cannot have its in-flight granule
/// freed inside the window it might still commit in. The SAME grace gates the lease reaper
/// (so a lease survives long enough to gate reclaim) and the GC's eligibility check.
const ORPHAN_RECLAIM_GRACE_MS: u64 = 30_000;
/// Per-target ceiling on granules examined in one GC sweep, bounding a sweep to
/// O(roster) work regardless of drive fullness.
const ORPHAN_GC_MAX_GRANULES_PER_TARGET: usize = 4096;

/// A stranded reclaim fence is cleared only once it is a day old AND its version's
/// marker + lease are gone: late enough that no in-flight commit of that version can
/// still be racing, cheap enough that the rows (a few dozen bytes each, one per
/// GC-tombstoned orphan version) never accumulate meaningfully.
const VERSION_RECLAIM_FENCE_JANITOR_AGE_MS: u64 = 24 * 60 * 60 * 1000;
const VERSION_RECLAIM_FENCE_ROWS_PER_SWEEP: usize = 512;
/// Grace beyond a pending-version marker's expiry before the version becomes
/// claim-eligible for the orphan-version reaper. Deliberately much larger than
/// ORPHAN_RECLAIM_GRACE_MS: reap latency is irrelevant at this timescale, and the
/// wide window means a KMS instance whose clock runs ahead of the fleet cannot claim
/// a version whose writer is alive between renewals.
const ORPHAN_VERSION_CLAIM_GRACE_MS: u64 = 300_000;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let config = parse_args(args)?;
    let _fdb_network = maybe_boot_network(
        config.fdb_client_threads,
        &config.fdb_external_client_dir,
    )?;
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
        std::sync::Arc::new(FdbHotStore::connect(
            &config.foundationdb_cluster_file,
            config.fdb_client_threads,
        )?);
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
        topology_epoch_cache: std::sync::Arc::new(std::sync::Mutex::new(None)),
        commit_marker_fence_enabled: config.commit_marker_fence_enabled,
        commit_marker_fence_ready: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };
    // Fenced commits stay withheld (tolerant per-row reads keep serving) until every
    // tombstone stamped before this binary — which has no fence row — gets one. The
    // backfill is idempotent and re-runs on every start with the flag on.
    if config.commit_marker_fence_enabled {
        let backfill_store = std::sync::Arc::clone(&service.hot_store);
        let backfill_ready = std::sync::Arc::clone(&service.commit_marker_fence_ready);
        let backfill_stats = stats.clone();
        tokio::spawn(async move {
            match backfill_store.backfill_reclaim_fences(now_unix_ms()).await {
                Ok(stamped) => {
                    backfill_ready.store(true, std::sync::atomic::Ordering::Relaxed);
                    eprintln!(
                        "kms: reclaim-fence backfill complete ({stamped} fences stamped); marker-fenced commits active"
                    );
                }
                Err(err) => {
                    backfill_stats.set_last_error(format!(
                        "KMS reclaim-fence backfill failed; marker-fenced commits stay withheld: {err}"
                    ));
                }
            }
        });
    }
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

    // The reservation reaper/finalizer/dispatch loops + the KAS service-registration loop
    // all drive the central KAS allocator, which the decentralized model removes (KMS owns
    // the target inventory + the lease-fenced GC reclaims granules). They are not spawned;
    // their handles stay None so the shutdown path is a no-op for them. `_reservation_*`
    // are retained so the channel + instance values are explicitly dropped, not warned on.
    let reaper: Option<tokio::task::JoinHandle<()>> = None;
    let finalizer: Option<tokio::task::JoinHandle<()>> = None;
    let reservation_dispatcher: Option<tokio::task::JoinHandle<()>> = None;
    // The dispatch loop (the only consumer) is not spawned, and the KAS service-registration
    // loop is gone; explicitly drop their inputs so they are not flagged unused. The sender
    // half stays owned by the service (the legacy reserve path), unused on the decentralized path.
    drop(reservation_mutation_receiver);
    drop(registry_instance);
    let lease_reaper = Some(tokio::spawn(object_lease_reaper_loop(
        std::sync::Arc::clone(&service.hot_store),
        stats.clone(),
        config.expiry_reap_interval,
    )));
    let orphan_gc = Some(tokio::spawn(orphan_gc_loop(
        std::sync::Arc::clone(&service.hot_store),
        stats.clone(),
        config.expiry_reap_interval,
    )));
    let orphan_version_reaper = if config.orphan_version_reaper_enabled {
        Some(tokio::spawn(orphan_version_reaper_loop(
            std::sync::Arc::clone(&service.hot_store),
            stats.clone(),
            config.expiry_reap_interval,
        )))
    } else {
        None
    };
    let registry = tokio::spawn(async {});
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
    if let Some(orphan_gc) = orphan_gc {
        orphan_gc.abort();
    }
    if let Some(orphan_version_reaper) = orphan_version_reaper {
        orphan_version_reaper.abort();
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
        match hot_store
            .reap_expired_leases(now_unix_ms(), ORPHAN_RECLAIM_GRACE_MS, 256)
            .await
        {
            Ok(reaped) => {
                consecutive_errors = 0;
                stats.record_write_leases_reaped(reaped);
            }
            Err(err) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                stats.set_last_error(format!("KMS write-lease reaper failed: {err}"));
                tokio::time::sleep(reaper_error_backoff(interval, consecutive_errors)).await;
            }
        }
    }
}

/// Periodic lease-fenced orphan GC: each tick sweeps every active target's occupied
/// granules and reclaims those that are uncommitted (no reverse-log row) with an
/// absent/expired-past-grace write lease, freeing them under the FDB rendezvous + the
/// generation fence. Decoupled from the lease reaper so neither blocks the other.
async fn orphan_gc_loop(
    hot_store: std::sync::Arc<dyn HotMetadataStore>,
    stats: std::sync::Arc<KmsStats>,
    interval: std::time::Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut consecutive_errors = 0usize;
    // Per-target paging cursor carried across sweeps so a full drive is walked over
    // successive sweeps instead of re-examining only its lowest-slot granules each time.
    let mut resume_cursors: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    // Fence-janitor scan cursor: unclearable rows (young fences, live-lease objects)
    // must never permanently occupy the page window and mask clearable rows behind it.
    let mut fence_cursor: Option<Vec<u8>> = None;
    loop {
        ticker.tick().await;
        match run_orphan_gc_sweep(
            hot_store.clone(),
            ORPHAN_RECLAIM_GRACE_MS,
            ORPHAN_GC_MAX_GRANULES_PER_TARGET,
            &mut resume_cursors,
            stats.clone(),
        )
        .await
        {
            Ok(_) => consecutive_errors = 0,
            Err(err) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                stats.set_last_error(format!("KMS orphan GC sweep failed: {err}"));
                tokio::time::sleep(reaper_error_backoff(interval, consecutive_errors)).await;
                continue;
            }
        }
        // Fence janitor: reclaim-fence rows stamped by this sweep's authorize path
        // are normally cleared by the orphan-version reaper's finish; versions the
        // reaper never claims (their marker is already gone) would strand theirs.
        // Errors only surface in stats — the janitor must never stall the sweep.
        match hot_store
            .sweep_reclaim_fences(
                now_unix_ms(),
                VERSION_RECLAIM_FENCE_JANITOR_AGE_MS,
                VERSION_RECLAIM_FENCE_ROWS_PER_SWEEP,
                fence_cursor.take(),
            )
            .await
        {
            Ok((cleared, next_cursor)) => {
                fence_cursor = next_cursor;
                if cleared > 0 {
                    stats.record_reclaim_fences_cleared(cleared);
                }
            }
            Err(err) => stats.set_last_error(format!("KMS reclaim-fence janitor failed: {err}")),
        }
    }
}

/// Periodic orphan-version reaper: discovers pending-version markers whose protection
/// lapsed (segmented writes that died before their seal), claims them, clears their
/// reverse-log/occupancy/committed-granule rows per presence target, and deletes the
/// markers — after which the ordinary granule GC frees the physical space. Spawned
/// only when `orphan_version_reaper_enabled` is set (see its doc for the fleet
/// rollout precondition).
async fn orphan_version_reaper_loop(
    hot_store: std::sync::Arc<dyn HotMetadataStore>,
    stats: std::sync::Arc<KmsStats>,
    interval: std::time::Duration,
) {
    // A pid-derived startup jitter de-synchronizes the fleet's sweeps so instances
    // mostly do disjoint (though idempotent) work.
    let interval_ms = (interval.as_millis() as u64).max(1);
    let jitter_ms = u64::from(std::process::id()).wrapping_mul(2_654_435_761) % interval_ms;
    tokio::time::sleep(std::time::Duration::from_millis(jitter_ms)).await;
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut consecutive_errors = 0usize;
    // Discovery resume point, carried across sweeps (None restarts the scan).
    let mut scan_cursor: Option<Vec<u8>> = None;
    loop {
        ticker.tick().await;
        match run_orphan_version_reap(
            hot_store.clone(),
            ORPHAN_VERSION_CLAIM_GRACE_MS,
            ORPHAN_RECLAIM_GRACE_MS,
            &mut scan_cursor,
            stats.clone(),
        )
        .await
        {
            Ok(()) => consecutive_errors = 0,
            Err(err) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                stats.set_last_error(format!("KMS orphan-version reap failed: {err}"));
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
        // Connect lazily so KMS boots even when KAS is absent (the decentralized model
        // removes KAS; KMS owns the target inventory). Any residual KAS-only admin path
        // fails only if actually invoked.
        channels.push(KasEndpoint {
            endpoint: endpoint_uri,
            channel: endpoint.connect_lazy(),
        });
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
