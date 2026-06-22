// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use serde::Deserialize;
use std::error::Error;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) listen_addr: SocketAddr,
    pub(crate) kas_endpoints: Vec<String>,
    pub(crate) foundationdb_cluster_file: String,
    pub(crate) service_heartbeat_interval: Duration,
    pub(crate) shard_id: String,
    pub(crate) public_endpoint: String,
    pub(crate) replica_endpoints: Vec<String>,
    pub(crate) notification_subject: String,
    pub(crate) notification_mode: NotificationMode,
    pub(crate) notification_nats_url: String,
    pub(crate) notification_poll_interval: Duration,
    pub(crate) stats_root: PathBuf,
    pub(crate) stats_publish_interval: Duration,
    pub(crate) write_intent_ttl: Duration,
    pub(crate) expiry_reap_interval: Duration,
    pub(crate) reservation_cache_high_watermark: usize,
    pub(crate) reservation_cache_low_watermark: usize,
    pub(crate) reservation_cache_refill_batch: usize,
    pub(crate) reservation_cache_ttl: Duration,
    pub(crate) reservation_cache_min_usable_ttl: Duration,
    pub(crate) reservation_cache_refill_concurrency: usize,
    pub(crate) reservation_cache_wait_timeout: Duration,
    pub(crate) reservation_cache_stale_refill: Duration,
    pub(crate) reservation_cache_small_object_max_stripes: usize,
    pub(crate) reservation_cache_single_window_seed_batch: usize,
    // TTL for the allocation-shard route cache. While fresh,
    // foreground reserves resolve shard count from RAM instead of issuing ~6
    // `list_service_instances` RPCs to KAS per reserve.
    pub(crate) allocation_route_cache_ttl: Duration,
    pub(crate) initiate_write_window_max_stripes: usize,
    pub(crate) large_write_initiate_max_concurrency: usize,
    pub(crate) write_profile_max_stripes: usize,
    pub(crate) write_profile_min_fragment_bytes: u32,
    pub(crate) reservation_mutation_batch_size: usize,
    pub(crate) reservation_mutation_dispatch_batch_size: usize,
    pub(crate) reservation_mutation_dispatch_flush: Duration,
    pub(crate) kas_rpc_timeout: Duration,
    pub(crate) kas_reserve_attempt_timeout: Duration,
    pub(crate) read_cache_max_entries: usize,
    pub(crate) read_cache_ttl: Duration,
    pub(crate) target_current_fragment_backfill_on_startup: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotificationMode {
    Nats,
    Poll,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    listen_addr: Option<String>,
    kas_endpoints: Option<Vec<String>>,
    foundationdb_cluster_file: Option<String>,
    service_heartbeat_ms: Option<u64>,
    shard_id: Option<String>,
    public_endpoint: Option<String>,
    replica_endpoints: Option<Vec<String>>,
    notification_subject: Option<String>,
    notification_mode: Option<String>,
    notification_nats_url: Option<String>,
    notification_poll_ms: Option<u64>,
    stats_root: Option<String>,
    stats_publish_ms: Option<u64>,
    write_intent_ttl_ms: Option<u64>,
    expiry_reap_ms: Option<u64>,
    reservation_cache_high_watermark: Option<usize>,
    reservation_cache_low_watermark: Option<usize>,
    reservation_cache_refill_batch: Option<usize>,
    reservation_cache_ttl_ms: Option<u64>,
    reservation_cache_min_usable_ttl_ms: Option<u64>,
    reservation_cache_refill_concurrency: Option<usize>,
    reservation_cache_wait_timeout_ms: Option<u64>,
    reservation_cache_stale_refill_ms: Option<u64>,
    reservation_cache_small_object_max_stripes: Option<usize>,
    reservation_cache_single_window_seed_batch: Option<usize>,
    allocation_route_cache_ttl_ms: Option<u64>,
    initiate_write_window_max_stripes: Option<usize>,
    large_write_initiate_max_concurrency: Option<usize>,
    write_profile_max_stripes: Option<usize>,
    write_profile_min_fragment_bytes: Option<u32>,
    reservation_mutation_batch_size: Option<usize>,
    reservation_mutation_dispatch_batch_size: Option<usize>,
    reservation_mutation_dispatch_flush_ms: Option<u64>,
    kas_rpc_timeout_ms: Option<u64>,
    kas_reserve_attempt_timeout_ms: Option<u64>,
    read_cache_max_entries: Option<usize>,
    read_cache_ttl_ms: Option<u64>,
    target_current_fragment_backfill_on_startup: Option<bool>,
}

impl Config {
    fn defaults() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            listen_addr: "127.0.0.1:50060".parse()?,
            kas_endpoints: vec!["http://127.0.0.1:50061".to_string()],
            foundationdb_cluster_file: "/etc/foundationdb/fdb.cluster".to_string(),
            service_heartbeat_interval: Duration::from_millis(30_000),
            shard_id: "kms-shard-0001".to_string(),
            public_endpoint: "http://127.0.0.1:50060".to_string(),
            replica_endpoints: Vec::new(),
            notification_subject: "keinfs.kms.events".to_string(),
            notification_mode: NotificationMode::Nats,
            notification_nats_url: "nats://127.0.0.1:4222".to_string(),
            notification_poll_interval: Duration::from_millis(500),
            stats_root: PathBuf::from("/run/keinfs/kms"),
            stats_publish_interval: Duration::from_millis(250),
            write_intent_ttl: Duration::from_millis(900_000),
            expiry_reap_interval: Duration::from_millis(1_000),
            reservation_cache_high_watermark: 524_288,
            reservation_cache_low_watermark: 131_072,
            reservation_cache_refill_batch: 131_072,
            reservation_cache_ttl: Duration::from_millis(120_000),
            reservation_cache_min_usable_ttl: Duration::from_millis(30_000),
            reservation_cache_refill_concurrency: 32,
            reservation_cache_wait_timeout: Duration::from_millis(30_000),
            reservation_cache_stale_refill: Duration::from_millis(180_000),
            reservation_cache_small_object_max_stripes: 64,
            reservation_cache_single_window_seed_batch: 4_096,
            allocation_route_cache_ttl: Duration::from_millis(5_000),
            initiate_write_window_max_stripes: 256,
            large_write_initiate_max_concurrency: 1,
            write_profile_max_stripes: 8,
            write_profile_min_fragment_bytes: 128 * 1024,
            reservation_mutation_batch_size: 1_024,
            reservation_mutation_dispatch_batch_size: 512,
            reservation_mutation_dispatch_flush: Duration::from_millis(2),
            kas_rpc_timeout: Duration::from_millis(20_000),
            kas_reserve_attempt_timeout: Duration::from_millis(10_000),
            read_cache_max_entries: 65_536,
            read_cache_ttl: Duration::from_millis(30_000),
            target_current_fragment_backfill_on_startup: true,
        })
    }

    fn apply_file(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        let raw = fs::read_to_string(path).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!("failed to read KMS config `{}`: {err}", path.display()),
            )
        })?;
        let file: FileConfig = toml::from_str(&raw).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse KMS config `{}`: {err}", path.display()),
            )
        })?;

        if let Some(value) = file.listen_addr {
            self.listen_addr = value.parse().map_err(|err| {
                arg_error(format!(
                    "invalid `listen_addr` in KMS config `{}`: {err}",
                    path.display()
                ))
            })?;
        }
        if let Some(value) = file.kas_endpoints {
            self.kas_endpoints = normalize_endpoint_list(value);
        }
        if let Some(value) = file.foundationdb_cluster_file {
            self.foundationdb_cluster_file = value;
        }
        if let Some(value) = file.service_heartbeat_ms {
            self.service_heartbeat_interval = Duration::from_millis(value.max(1_000));
        }
        if let Some(value) = file.shard_id {
            self.shard_id = value;
        }
        if let Some(value) = file.public_endpoint {
            self.public_endpoint = normalize_endpoint(value);
        }
        if let Some(value) = file.replica_endpoints {
            self.replica_endpoints = normalize_endpoint_list(value);
        }
        if let Some(value) = file.notification_subject {
            self.notification_subject = value;
        }
        if let Some(value) = file.notification_mode {
            self.notification_mode = parse_notification_mode(&value)?;
        }
        if let Some(value) = file.notification_nats_url {
            self.notification_nats_url = value;
        }
        if let Some(value) = file.notification_poll_ms {
            self.notification_poll_interval = Duration::from_millis(value.max(100));
        }
        if let Some(value) = file.stats_root {
            self.stats_root = PathBuf::from(value);
        }
        if let Some(value) = file.stats_publish_ms {
            self.stats_publish_interval = Duration::from_millis(value.max(50));
        }
        if let Some(value) = file.write_intent_ttl_ms {
            self.write_intent_ttl = Duration::from_millis(value.max(1_000));
        }
        if let Some(value) = file.expiry_reap_ms {
            self.expiry_reap_interval = Duration::from_millis(value.max(200));
        }
        if let Some(value) = file.reservation_cache_high_watermark {
            self.reservation_cache_high_watermark = value;
        }
        if let Some(value) = file.reservation_cache_low_watermark {
            self.reservation_cache_low_watermark = value;
        }
        if let Some(value) = file.reservation_cache_refill_batch {
            self.reservation_cache_refill_batch = value;
        }
        if let Some(value) = file.reservation_cache_ttl_ms {
            self.reservation_cache_ttl = Duration::from_millis(value.max(1_000));
        }
        if let Some(value) = file.reservation_cache_min_usable_ttl_ms {
            self.reservation_cache_min_usable_ttl = Duration::from_millis(value.max(1_000));
        }
        if let Some(value) = file.reservation_cache_refill_concurrency {
            self.reservation_cache_refill_concurrency = value;
        }
        if let Some(value) = file.reservation_cache_wait_timeout_ms {
            self.reservation_cache_wait_timeout = Duration::from_millis(value.max(500));
        }
        if let Some(value) = file.reservation_cache_stale_refill_ms {
            self.reservation_cache_stale_refill = Duration::from_millis(value.max(250));
        }
        if let Some(value) = file.reservation_cache_small_object_max_stripes {
            self.reservation_cache_small_object_max_stripes = value;
        }
        if let Some(value) = file.reservation_cache_single_window_seed_batch {
            self.reservation_cache_single_window_seed_batch = value;
        }
        if let Some(value) = file.allocation_route_cache_ttl_ms {
            self.allocation_route_cache_ttl = Duration::from_millis(value.max(250));
        }
        if let Some(value) = file.initiate_write_window_max_stripes {
            self.initiate_write_window_max_stripes = value;
        }
        if let Some(value) = file.large_write_initiate_max_concurrency {
            self.large_write_initiate_max_concurrency = value;
        }
        if let Some(value) = file.write_profile_max_stripes {
            self.write_profile_max_stripes = value;
        }
        if let Some(value) = file.write_profile_min_fragment_bytes {
            self.write_profile_min_fragment_bytes = value;
        }
        if let Some(value) = file.reservation_mutation_batch_size {
            self.reservation_mutation_batch_size = value;
        }
        if let Some(value) = file.reservation_mutation_dispatch_batch_size {
            self.reservation_mutation_dispatch_batch_size = value;
        }
        if let Some(value) = file.reservation_mutation_dispatch_flush_ms {
            self.reservation_mutation_dispatch_flush = Duration::from_millis(value.max(1));
        }
        if let Some(value) = file.kas_rpc_timeout_ms {
            self.kas_rpc_timeout = Duration::from_millis(value.max(1_000));
        }
        if let Some(value) = file.kas_reserve_attempt_timeout_ms {
            self.kas_reserve_attempt_timeout = Duration::from_millis(value.max(500));
        }
        if let Some(value) = file.read_cache_max_entries {
            self.read_cache_max_entries = value;
        }
        if let Some(value) = file.read_cache_ttl_ms {
            self.read_cache_ttl = Duration::from_millis(value.max(100));
        }
        if let Some(value) = file.target_current_fragment_backfill_on_startup {
            self.target_current_fragment_backfill_on_startup = value;
        }

        Ok(())
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.kas_endpoints.is_empty() {
            return Err(arg_error("--kas-endpoint must be set at least once"));
        }
        if self.foundationdb_cluster_file.trim().is_empty() {
            return Err(arg_error("--foundationdb-cluster-file must be set"));
        }
        if matches!(self.notification_mode, NotificationMode::Nats)
            && self.notification_nats_url.trim().is_empty()
        {
            return Err(arg_error(
                "--notification-nats-url must be set when --notification-mode=nats",
            ));
        }
        if self.reservation_cache_low_watermark >= self.reservation_cache_high_watermark {
            return Err(arg_error(
                "--reservation-cache-low-watermark must be < --reservation-cache-high-watermark",
            ));
        }
        if self.reservation_cache_refill_batch == 0 {
            return Err(arg_error("--reservation-cache-refill-batch must be > 0"));
        }
        if self.reservation_cache_refill_concurrency == 0 {
            return Err(arg_error(
                "--reservation-cache-refill-concurrency must be > 0",
            ));
        }
        if self.reservation_cache_stale_refill
            < self
                .reservation_cache_wait_timeout
                .max(self.kas_rpc_timeout)
        {
            return Err(arg_error(
                "--reservation-cache-stale-refill-ms must be >= max(--reservation-cache-wait-timeout-ms, --kas-rpc-timeout-ms)",
            ));
        }
        if self.reservation_cache_small_object_max_stripes == 0 {
            return Err(arg_error(
                "--reservation-cache-small-object-max-stripes must be > 0",
            ));
        }
        if self.reservation_cache_single_window_seed_batch == 0 {
            return Err(arg_error(
                "--reservation-cache-single-window-seed-batch must be > 0",
            ));
        }
        if self.initiate_write_window_max_stripes == 0 {
            return Err(arg_error("--initiate-write-window-max-stripes must be > 0"));
        }
        if self.large_write_initiate_max_concurrency == 0 {
            return Err(arg_error(
                "--large-write-initiate-max-concurrency must be > 0",
            ));
        }
        if self.write_profile_max_stripes == 0 {
            return Err(arg_error("--write-profile-max-stripes must be > 0"));
        }
        if self.write_profile_min_fragment_bytes == 0 {
            return Err(arg_error("--write-profile-min-fragment-bytes must be > 0"));
        }
        if self.reservation_mutation_batch_size == 0 {
            return Err(arg_error("--reservation-mutation-batch-size must be > 0"));
        }
        if self.reservation_mutation_dispatch_batch_size == 0 {
            return Err(arg_error(
                "--reservation-mutation-dispatch-batch-size must be > 0",
            ));
        }
        if self.kas_reserve_attempt_timeout > self.kas_rpc_timeout {
            return Err(arg_error(
                "--kas-reserve-attempt-timeout-ms must be <= --kas-rpc-timeout-ms",
            ));
        }
        Ok(())
    }

    pub(crate) fn fingerprint_source(&self) -> String {
        format!(
            concat!(
                "listen_addr={}\n",
                "kas_endpoints={}\n",
                "foundationdb_cluster_file={}\n",
                "service_heartbeat_ms={}\n",
                "shard_id={}\n",
                "public_endpoint={}\n",
                "replica_endpoints={}\n",
                "notification_subject={}\n",
                "notification_mode={}\n",
                "notification_nats_url={}\n",
                "notification_poll_ms={}\n",
                "stats_root={}\n",
                "stats_publish_ms={}\n",
                "write_intent_ttl_ms={}\n",
                "expiry_reap_ms={}\n",
                "reservation_cache_high_watermark={}\n",
                "reservation_cache_low_watermark={}\n",
                "reservation_cache_refill_batch={}\n",
                "reservation_cache_ttl_ms={}\n",
                "reservation_cache_min_usable_ttl_ms={}\n",
                "reservation_cache_refill_concurrency={}\n",
                "reservation_cache_wait_timeout_ms={}\n",
                "reservation_cache_stale_refill_ms={}\n",
                "reservation_cache_small_object_max_stripes={}\n",
                "reservation_cache_single_window_seed_batch={}\n",
                "allocation_route_cache_ttl_ms={}\n",
                "initiate_write_window_max_stripes={}\n",
                "large_write_initiate_max_concurrency={}\n",
                "write_profile_max_stripes={}\n",
                "write_profile_min_fragment_bytes={}\n",
                "reservation_mutation_batch_size={}\n",
                "reservation_mutation_dispatch_batch_size={}\n",
                "reservation_mutation_dispatch_flush_ms={}\n",
                "kas_rpc_timeout_ms={}\n",
                "kas_reserve_attempt_timeout_ms={}\n",
                "read_cache_max_entries={}\n",
                "read_cache_ttl_ms={}\n",
                "target_current_fragment_backfill_on_startup={}\n"
            ),
            self.listen_addr,
            self.kas_endpoints.join(","),
            self.foundationdb_cluster_file,
            self.service_heartbeat_interval.as_millis(),
            self.shard_id,
            self.public_endpoint,
            self.replica_endpoints.join(","),
            self.notification_subject,
            self.notification_mode.as_str(),
            self.notification_nats_url,
            self.notification_poll_interval.as_millis(),
            self.stats_root.display(),
            self.stats_publish_interval.as_millis(),
            self.write_intent_ttl.as_millis(),
            self.expiry_reap_interval.as_millis(),
            self.reservation_cache_high_watermark,
            self.reservation_cache_low_watermark,
            self.reservation_cache_refill_batch,
            self.reservation_cache_ttl.as_millis(),
            self.reservation_cache_min_usable_ttl.as_millis(),
            self.reservation_cache_refill_concurrency,
            self.reservation_cache_wait_timeout.as_millis(),
            self.reservation_cache_stale_refill.as_millis(),
            self.reservation_cache_small_object_max_stripes,
            self.reservation_cache_single_window_seed_batch,
            self.allocation_route_cache_ttl.as_millis(),
            self.initiate_write_window_max_stripes,
            self.large_write_initiate_max_concurrency,
            self.write_profile_max_stripes,
            self.write_profile_min_fragment_bytes,
            self.reservation_mutation_batch_size,
            self.reservation_mutation_dispatch_batch_size,
            self.reservation_mutation_dispatch_flush.as_millis(),
            self.kas_rpc_timeout.as_millis(),
            self.kas_reserve_attempt_timeout.as_millis(),
            self.read_cache_max_entries,
            self.read_cache_ttl.as_millis(),
            self.target_current_fragment_backfill_on_startup,
        )
    }
}

impl NotificationMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Nats => "nats",
            Self::Poll => "poll",
        }
    }
}

pub(crate) fn parse_args(args: Vec<String>) -> Result<Config, Box<dyn Error>> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Err(arg_error(usage()));
    }

    let mut config = Config::defaults()?;
    if let Some(path) = scan_config_path(&args)? {
        config.apply_file(&path)?;
    }

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                let _ = args.get(i).ok_or_else(|| missing_value("--config"))?;
            }
            "--listen" => {
                i += 1;
                config.listen_addr = args
                    .get(i)
                    .ok_or_else(|| missing_value("--listen"))?
                    .parse()
                    .map_err(|err| arg_error(format!("invalid --listen address: {err}")))?;
            }
            "--kas-endpoint" => {
                i += 1;
                config.kas_endpoints = args
                    .get(i)
                    .ok_or_else(|| missing_value("--kas-endpoint"))?
                    .split(',')
                    .map(|value| normalize_endpoint(value.to_string()))
                    .collect();
            }
            "--foundationdb-cluster-file" => {
                i += 1;
                config.foundationdb_cluster_file = args
                    .get(i)
                    .ok_or_else(|| missing_value("--foundationdb-cluster-file"))?
                    .clone();
            }
            "--service-heartbeat-ms" => {
                i += 1;
                config.service_heartbeat_interval = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--service-heartbeat-ms")?.max(1_000),
                );
            }
            "--shard-id" => {
                i += 1;
                config.shard_id = args
                    .get(i)
                    .ok_or_else(|| missing_value("--shard-id"))?
                    .clone();
            }
            "--public-endpoint" => {
                i += 1;
                config.public_endpoint = normalize_endpoint(
                    args.get(i)
                        .ok_or_else(|| missing_value("--public-endpoint"))?
                        .clone(),
                );
            }
            "--replica-endpoints" => {
                i += 1;
                config.replica_endpoints = args
                    .get(i)
                    .ok_or_else(|| missing_value("--replica-endpoints"))?
                    .split(',')
                    .filter(|value| !value.trim().is_empty())
                    .map(|value| normalize_endpoint(value.to_string()))
                    .collect();
            }
            "--notification-subject" => {
                i += 1;
                config.notification_subject = args
                    .get(i)
                    .ok_or_else(|| missing_value("--notification-subject"))?
                    .clone();
            }
            "--notification-mode" => {
                i += 1;
                config.notification_mode = parse_notification_mode(
                    args.get(i)
                        .ok_or_else(|| missing_value("--notification-mode"))?,
                )?;
            }
            "--notification-nats-url" => {
                i += 1;
                config.notification_nats_url = args
                    .get(i)
                    .ok_or_else(|| missing_value("--notification-nats-url"))?
                    .clone();
            }
            "--notification-poll-ms" => {
                i += 1;
                config.notification_poll_interval = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--notification-poll-ms")?.max(100),
                );
            }
            "--stats-root" => {
                i += 1;
                config.stats_root = PathBuf::from(
                    args.get(i)
                        .ok_or_else(|| missing_value("--stats-root"))?
                        .clone(),
                );
            }
            "--stats-publish-ms" => {
                i += 1;
                config.stats_publish_interval = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--stats-publish-ms")?.max(50),
                );
            }
            "--write-intent-ttl-ms" => {
                i += 1;
                config.write_intent_ttl = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--write-intent-ttl-ms")?.max(1_000),
                );
            }
            "--expiry-reap-ms" => {
                i += 1;
                config.expiry_reap_interval =
                    Duration::from_millis(parse_u64_arg(args.get(i), "--expiry-reap-ms")?.max(200));
            }
            "--reservation-cache-high-watermark" => {
                i += 1;
                config.reservation_cache_high_watermark =
                    parse_usize_arg(args.get(i), "--reservation-cache-high-watermark")?;
            }
            "--reservation-cache-low-watermark" => {
                i += 1;
                config.reservation_cache_low_watermark =
                    parse_usize_arg(args.get(i), "--reservation-cache-low-watermark")?;
            }
            "--reservation-cache-refill-batch" => {
                i += 1;
                config.reservation_cache_refill_batch =
                    parse_usize_arg(args.get(i), "--reservation-cache-refill-batch")?;
            }
            "--reservation-cache-ttl-ms" => {
                i += 1;
                config.reservation_cache_ttl = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--reservation-cache-ttl-ms")?.max(1_000),
                );
            }
            "--reservation-cache-min-usable-ttl-ms" => {
                i += 1;
                config.reservation_cache_min_usable_ttl = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--reservation-cache-min-usable-ttl-ms")?.max(1_000),
                );
            }
            "--reservation-cache-refill-concurrency" => {
                i += 1;
                config.reservation_cache_refill_concurrency =
                    parse_usize_arg(args.get(i), "--reservation-cache-refill-concurrency")?;
            }
            "--reservation-cache-wait-timeout-ms" => {
                i += 1;
                config.reservation_cache_wait_timeout = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--reservation-cache-wait-timeout-ms")?.max(500),
                );
            }
            "--reservation-cache-stale-refill-ms" => {
                i += 1;
                config.reservation_cache_stale_refill = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--reservation-cache-stale-refill-ms")?.max(250),
                );
            }
            "--reservation-cache-small-object-max-stripes" => {
                i += 1;
                config.reservation_cache_small_object_max_stripes =
                    parse_usize_arg(args.get(i), "--reservation-cache-small-object-max-stripes")?;
            }
            "--reservation-cache-single-window-seed-batch" => {
                i += 1;
                config.reservation_cache_single_window_seed_batch =
                    parse_usize_arg(args.get(i), "--reservation-cache-single-window-seed-batch")?;
            }
            "--allocation-route-cache-ttl-ms" => {
                i += 1;
                config.allocation_route_cache_ttl = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--allocation-route-cache-ttl-ms")?.max(250),
                );
            }
            "--initiate-write-window-max-stripes" => {
                i += 1;
                config.initiate_write_window_max_stripes =
                    parse_usize_arg(args.get(i), "--initiate-write-window-max-stripes")?;
            }
            "--large-write-initiate-max-concurrency" => {
                i += 1;
                config.large_write_initiate_max_concurrency =
                    parse_usize_arg(args.get(i), "--large-write-initiate-max-concurrency")?;
            }
            "--write-profile-max-stripes" => {
                i += 1;
                config.write_profile_max_stripes =
                    parse_usize_arg(args.get(i), "--write-profile-max-stripes")?;
            }
            "--write-profile-min-fragment-bytes" => {
                i += 1;
                config.write_profile_min_fragment_bytes =
                    parse_u32_arg(args.get(i), "--write-profile-min-fragment-bytes")?;
            }
            "--reservation-mutation-batch-size" => {
                i += 1;
                config.reservation_mutation_batch_size =
                    parse_usize_arg(args.get(i), "--reservation-mutation-batch-size")?;
            }
            "--reservation-mutation-dispatch-batch-size" => {
                i += 1;
                config.reservation_mutation_dispatch_batch_size =
                    parse_usize_arg(args.get(i), "--reservation-mutation-dispatch-batch-size")?;
            }
            "--reservation-mutation-dispatch-flush-ms" => {
                i += 1;
                config.reservation_mutation_dispatch_flush = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--reservation-mutation-dispatch-flush-ms")?.max(1),
                );
            }
            "--kas-rpc-timeout-ms" => {
                i += 1;
                config.kas_rpc_timeout = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--kas-rpc-timeout-ms")?.max(1_000),
                );
            }
            "--kas-reserve-attempt-timeout-ms" => {
                i += 1;
                config.kas_reserve_attempt_timeout = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--kas-reserve-attempt-timeout-ms")?.max(500),
                );
            }
            "--read-cache-max-entries" => {
                i += 1;
                config.read_cache_max_entries =
                    parse_usize_arg(args.get(i), "--read-cache-max-entries")?;
            }
            "--read-cache-ttl-ms" => {
                i += 1;
                config.read_cache_ttl = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--read-cache-ttl-ms")?.max(100),
                );
            }
            "--target-current-fragment-backfill-on-startup" => {
                i += 1;
                let value = args.get(i).ok_or_else(|| {
                    missing_value("--target-current-fragment-backfill-on-startup")
                })?;
                config.target_current_fragment_backfill_on_startup =
                    parse_bool_arg(value, "--target-current-fragment-backfill-on-startup")?;
            }
            flag => {
                return Err(arg_error(format!(
                    "unknown KMS option `{flag}`\n\n{}",
                    usage()
                )));
            }
        }
        i += 1;
    }

    config.validate()?;
    Ok(config)
}

fn scan_config_path(args: &[String]) -> Result<Option<PathBuf>, Box<dyn Error>> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--config" {
            let path = args
                .get(i + 1)
                .ok_or_else(|| missing_value("--config"))?
                .clone();
            return Ok(Some(PathBuf::from(path)));
        }
        i += 1;
    }
    Ok(None)
}

fn parse_notification_mode(value: &str) -> Result<NotificationMode, Box<dyn Error>> {
    match value.trim().to_ascii_lowercase().as_str() {
        "nats" => Ok(NotificationMode::Nats),
        "poll" => Ok(NotificationMode::Poll),
        other => Err(arg_error(format!(
            "unknown notification mode `{other}`; use `nats` or `poll`"
        ))),
    }
}

fn normalize_endpoint_list(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(normalize_endpoint)
        .collect::<Vec<_>>()
}

fn normalize_endpoint(value: String) -> String {
    let trimmed = value.trim().trim_end_matches('/').to_string();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed
    } else {
        format!("http://{trimmed}")
    }
}

fn parse_u64_arg(value: Option<&String>, flag: &str) -> Result<u64, Box<dyn Error>> {
    value
        .ok_or_else(|| missing_value(flag))?
        .parse::<u64>()
        .map_err(|err| arg_error(format!("invalid {flag} value: {err}")))
}

fn parse_usize_arg(value: Option<&String>, flag: &str) -> Result<usize, Box<dyn Error>> {
    value
        .ok_or_else(|| missing_value(flag))?
        .parse::<usize>()
        .map_err(|err| arg_error(format!("invalid {flag} value: {err}")))
}

fn parse_u32_arg(value: Option<&String>, flag: &str) -> Result<u32, Box<dyn Error>> {
    value
        .ok_or_else(|| missing_value(flag))?
        .parse::<u32>()
        .map_err(|err| arg_error(format!("invalid {flag} value: {err}")))
}

fn parse_bool_arg(value: &str, flag: &str) -> Result<bool, Box<dyn Error>> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(arg_error(format!(
            "invalid {flag} value `{other}`; use true or false"
        ))),
    }
}

fn usage() -> String {
    "kms [--config /etc/keinfs/kms.toml] [--listen 127.0.0.1:50060] [--kas-endpoint http://127.0.0.1:50061,http://127.0.0.1:50062] [--foundationdb-cluster-file /etc/foundationdb/fdb.cluster] [--service-heartbeat-ms 30000] [--shard-id kms-shard-0001] [--public-endpoint http://127.0.0.1:50060] [--replica-endpoints http://127.0.0.1:50062,http://127.0.0.1:50063] [--notification-subject keinfs.kms.events] [--notification-mode nats|poll] [--notification-nats-url nats://127.0.0.1:4222] [--notification-poll-ms 500] [--stats-root /run/keinfs/kms] [--stats-publish-ms 250] [--write-intent-ttl-ms 30000] [--expiry-reap-ms 1000] [--reservation-cache-high-watermark 8192] [--reservation-cache-low-watermark 2048] [--reservation-cache-refill-batch 2048] [--reservation-cache-ttl-ms 30000] [--reservation-cache-min-usable-ttl-ms 5000] [--reservation-cache-refill-concurrency 8] [--reservation-cache-wait-timeout-ms 15000] [--reservation-cache-stale-refill-ms 5000] [--reservation-cache-small-object-max-stripes 64] [--reservation-cache-single-window-seed-batch 4096] [--initiate-write-window-max-stripes 256] [--large-write-initiate-max-concurrency 1] [--write-profile-max-stripes 8] [--write-profile-min-fragment-bytes 131072] [--reservation-mutation-batch-size 1024] [--reservation-mutation-dispatch-batch-size 512] [--reservation-mutation-dispatch-flush-ms 2] [--kas-rpc-timeout-ms 5000] [--kas-reserve-attempt-timeout-ms 1000] [--read-cache-max-entries 65536] [--read-cache-ttl-ms 30000] [--target-current-fragment-backfill-on-startup true|false]"
        .to_string()
}

fn missing_value(flag: &str) -> Box<dyn Error> {
    arg_error(format!("missing value for {flag}"))
}

fn arg_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::<dyn Error>::from(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

#[cfg(test)]
mod tests {
    use super::{parse_args, Config, NotificationMode};
    use std::time::Duration;

    #[test]
    fn defaults_are_foundationdb_and_nats() {
        let defaults = Config::defaults().expect("defaults");
        assert_eq!(
            defaults.foundationdb_cluster_file,
            "/etc/foundationdb/fdb.cluster"
        );
        assert_eq!(defaults.notification_mode, NotificationMode::Nats);
        assert_eq!(defaults.reservation_cache_small_object_max_stripes, 64);
        assert_eq!(defaults.reservation_cache_single_window_seed_batch, 4_096);
        assert_eq!(defaults.large_write_initiate_max_concurrency, 1);
        assert_eq!(
            defaults.kas_reserve_attempt_timeout,
            Duration::from_millis(10_000)
        );
    }

    #[test]
    fn parse_foundationdb_args() {
        let parsed = parse_args(vec![
            "--listen".to_string(),
            "0.0.0.0:50060".to_string(),
            "--kas-endpoint".to_string(),
            "10.0.0.7:50061,10.0.0.8:50061".to_string(),
            "--foundationdb-cluster-file".to_string(),
            "/tmp/fdb.cluster".to_string(),
            "--notification-subject".to_string(),
            "bench.kms.events".to_string(),
            "--notification-mode".to_string(),
            "poll".to_string(),
            "--large-write-initiate-max-concurrency".to_string(),
            "2".to_string(),
            "--kas-reserve-attempt-timeout-ms".to_string(),
            "7000".to_string(),
            "--target-current-fragment-backfill-on-startup".to_string(),
            "false".to_string(),
        ])
        .expect("parsed args");
        assert_eq!(parsed.listen_addr.to_string(), "0.0.0.0:50060");
        assert_eq!(parsed.kas_endpoints.len(), 2);
        assert_eq!(parsed.foundationdb_cluster_file, "/tmp/fdb.cluster");
        assert_eq!(parsed.notification_subject, "bench.kms.events");
        assert_eq!(parsed.notification_mode, NotificationMode::Poll);
        assert_eq!(parsed.large_write_initiate_max_concurrency, 2);
        assert_eq!(
            parsed.kas_reserve_attempt_timeout,
            Duration::from_millis(7_000)
        );
        assert!(!parsed.target_current_fragment_backfill_on_startup);
    }
}
