// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

mod allocator_store;
mod config;
mod fdb_schema;
mod fdb_store;
mod service;
mod stats;
mod store;

use allocator_store::AllocatorStore;
use config::parse_args;
use fdb_store::{maybe_boot_network, FdbKasStore};
use keinbuild::{build_info, config_hash_hex, hostname_or_unknown};
use keinctl::proto::kas_server::KasServer;
use service::KasService;
use stats::{KasIdentity, KasStats, Publisher};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use store::{ReservationBinGate, ReservationBinKey, ReservationBinRegistry};
use tonic::transport::Server;
use tonic_health::server::health_reporter;

const RESERVATION_BIN_REFILL_LEASE_NAME: &str = "reservation_bin_refill";
const RESERVATION_REAPER_LEASE_NAME: &str = "reservation_reaper";
const KAS_GRPC_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let config = parse_args(args)?;
    let _fdb_network = maybe_boot_network()?;
    let build = build_info!();
    let started_at_unix_ms = now_unix_ms();
    let config_hash = config_hash_hex(&config.fingerprint_source());
    let node_id = hostname_or_unknown();

    let stats = KasStats::new(KasIdentity {
        build: build.clone(),
        listen_addr: config.listen_addr.to_string(),
        allocator_store: format!("foundationdb:{}", config.foundationdb_cluster_file),
        pid: std::process::id(),
        stats_root: config.stats_root.display().to_string(),
    });
    let publisher = Publisher::spawn(
        stats.clone(),
        &config.stats_root,
        config.stats_publish_interval,
    )?;

    let store: Arc<dyn AllocatorStore> = Arc::new(FdbKasStore::connect(
        &config.foundationdb_cluster_file,
        Some(config.allocation_shard_id.clone()),
    )?);
    if config.reset_allocator_state_and_exit {
        store.init().await?;
        store.reset_allocator_state().await?;
        publisher.stop();
        return Ok(());
    }
    let warm_store = store.clone();
    let warm_stats = stats.clone();
    let warmup = tokio::spawn(async move {
        if let Err(err) = warm_store.init().await {
            warm_stats.set_last_error(format!("KAS allocator warm-up failed: {err}"));
        }
    });
    let service_instance = keinctl::proto::ServiceInstanceRecord {
        instance_id: format!("kas:{}", config.public_endpoint),
        service_kind: keinctl::proto::ServiceKind::Kas as i32,
        node_id,
        endpoint: config.public_endpoint.clone(),
        package_name: build.package_name.clone(),
        build: Some(build_info_to_proto(&build)),
        config_hash,
        pid: std::process::id(),
        runtime_root: config.stats_root.display().to_string(),
        instance_label: config.allocation_shard_id.clone(),
        started_at_unix_ms,
        heartbeat_at_unix_ms: started_at_unix_ms,
        heartbeat_interval_ms: config.service_heartbeat_interval.as_millis() as u64,
    };
    let registry = tokio::spawn(service_registration_loop(
        store.clone(),
        service_instance.clone(),
        stats.clone(),
        config.service_heartbeat_interval,
    ));
    let reservation_bins = ReservationBinRegistry::default();
    let reservation_bin_gate = ReservationBinGate::default();
    if config.reservation_bin_high_watermark > 0 {
        let default_hot_bin =
            ReservationBinKey::new(10, keinctl::proto::FailureDomain::DriveDomainLab);
        reservation_bins.remember(default_hot_bin).await;
    }
    let reservation_bin_refill_leader = Arc::new(AtomicBool::new(false));

    let service = KasService {
        store: store.clone(),
        stats: stats.clone(),
        allocation_shard_id: config.allocation_shard_id.clone(),
        service_instance: service_instance.clone(),
        reservation_ttl_ms: config.reservation_ttl.as_millis() as u64,
        max_batch_size: config.reserve_batch_size,
        reservation_bins: reservation_bins.clone(),
        reservation_bin_gate: reservation_bin_gate.clone(),
        reservation_bin_low_watermark: config.reservation_bin_low_watermark,
        reservation_bin_high_watermark: config.reservation_bin_high_watermark,
        reservation_bin_top_up_chunk: config.reservation_bin_top_up_chunk,
        reservation_bin_bypass_batch_size: config.reservation_bin_bypass_batch_size,
        reservation_bin_refill_leader: reservation_bin_refill_leader.clone(),
    };

    let reaper = tokio::spawn(reservation_reaper_loop(
        store.clone(),
        stats.clone(),
        config.reservation_reap_interval,
        config.reservation_reap_limit,
        service_instance.instance_id.clone(),
    ));
    let bin_refiller = tokio::spawn(reservation_bin_refill_loop(
        store.clone(),
        reservation_bins,
        reservation_bin_gate.clone(),
        stats.clone(),
        config.reservation_bin_refill_interval,
        config.reservation_bin_low_watermark,
        config.reservation_bin_high_watermark,
        config.reservation_bin_top_up_chunk,
        config.reservation_ttl.as_millis() as u64,
        service_instance.instance_id.clone(),
        reservation_bin_refill_leader.clone(),
    ));
    let (health_reporter, health_service) = health_reporter();
    health_reporter.set_serving::<KasServer<KasService>>().await;

    Server::builder()
        .add_service(health_service)
        .add_service(
            KasServer::new(service)
                .max_decoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES),
        )
        .serve_with_shutdown(config.listen_addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    reaper.abort();
    bin_refiller.abort();
    registry.abort();
    warmup.abort();
    publisher.stop();
    Ok(())
}

async fn reservation_reaper_loop(
    store: Arc<dyn AllocatorStore>,
    stats: std::sync::Arc<KasStats>,
    interval: std::time::Duration,
    limit: usize,
    leader_id: String,
) {
    let lease_ttl_ms = std::cmp::max(interval.as_millis().saturating_mul(32) as u64, 30_000);
    let lease_refresh_ms = std::cmp::max(lease_ttl_ms / 2, interval.as_millis() as u64);
    let retry_backoff_ms = interval.as_millis().clamp(250, 5_000) as u64;
    let mut ticker = tokio::time::interval(interval);
    let mut next_lease_check_at_ms = 0u64;
    let mut is_leader = false;
    let mut consecutive_errors = 0usize;
    loop {
        ticker.tick().await;
        let now_ms = now_unix_ms();
        if now_ms >= next_lease_check_at_ms {
            is_leader = match store
                .try_acquire_coordination_lease(
                    RESERVATION_REAPER_LEASE_NAME,
                    &leader_id,
                    lease_ttl_ms,
                )
                .await
            {
                Ok(value) => {
                    next_lease_check_at_ms = now_ms.saturating_add(if value {
                        lease_refresh_ms
                    } else {
                        retry_backoff_ms
                    });
                    value
                }
                Err(err) => {
                    next_lease_check_at_ms = now_ms.saturating_add(retry_backoff_ms);
                    stats.set_last_error(format!(
                        "KAS reservation reaper lease acquisition failed: {err}"
                    ));
                    false
                }
            };
        }
        if !is_leader {
            continue;
        }
        match store
            .release_expired_reservations(now_unix_ms(), limit)
            .await
        {
            Ok(released) => {
                consecutive_errors = 0;
                stats.record_reservation_reaper_run(released);
            }
            Err(err) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                stats.set_last_error(format!("KAS reservation reaper failed: {err}"));
                tokio::time::sleep(reaper_error_backoff(interval, consecutive_errors)).await;
            }
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

async fn reservation_bin_refill_loop(
    store: Arc<dyn AllocatorStore>,
    registry: ReservationBinRegistry,
    reservation_bin_gate: ReservationBinGate,
    stats: std::sync::Arc<KasStats>,
    interval: std::time::Duration,
    low_watermark: usize,
    high_watermark: usize,
    top_up_chunk: usize,
    reservation_ttl_ms: u64,
    leader_id: String,
    refill_leader: Arc<AtomicBool>,
) {
    if high_watermark == 0 || high_watermark <= low_watermark {
        return;
    }
    let lease_ttl_ms = std::cmp::max(interval.as_millis().saturating_mul(32) as u64, 30_000);
    let lease_refresh_ms = std::cmp::max(lease_ttl_ms / 2, interval.as_millis() as u64);
    let retry_backoff_ms = interval.as_millis().clamp(250, 5_000) as u64;
    let mut ticker = tokio::time::interval(interval);
    let mut next_lease_check_at_ms = 0u64;
    let mut is_leader = false;
    loop {
        ticker.tick().await;
        let now_ms = now_unix_ms();
        if now_ms >= next_lease_check_at_ms {
            is_leader = match store
                .try_acquire_coordination_lease(
                    RESERVATION_BIN_REFILL_LEASE_NAME,
                    &leader_id,
                    lease_ttl_ms,
                )
                .await
            {
                Ok(value) => {
                    next_lease_check_at_ms = now_ms.saturating_add(if value {
                        lease_refresh_ms
                    } else {
                        retry_backoff_ms
                    });
                    value
                }
                Err(err) => {
                    next_lease_check_at_ms = now_ms.saturating_add(retry_backoff_ms);
                    refill_leader.store(false, std::sync::atomic::Ordering::Relaxed);
                    stats.set_last_error(format!(
                        "KAS reservation bin refill lease acquisition failed: {err}"
                    ));
                    false
                }
            };
        }
        refill_leader.store(is_leader, std::sync::atomic::Ordering::Relaxed);
        if !is_leader {
            continue;
        }
        for bin_key in registry.snapshot().await {
            let _gate = reservation_bin_gate.acquire(&bin_key).await;
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
                    "KAS reservation bin refill failed for fragment_count={} failure_domain={}: {err}",
                    bin_key.fragment_count(),
                    bin_key.failure_domain_raw(),
                ));
            }
        }
    }
}

async fn service_registration_loop(
    store: Arc<dyn AllocatorStore>,
    mut instance: keinctl::proto::ServiceInstanceRecord,
    stats: std::sync::Arc<KasStats>,
    interval: std::time::Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        instance.heartbeat_at_unix_ms = now_unix_ms();
        if let Err(err) = store.upsert_service_instance(instance.clone()).await {
            stats.set_last_error(format!("KAS service registration failed: {err}"));
        }
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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
