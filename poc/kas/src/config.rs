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
    pub(crate) public_endpoint: String,
    pub(crate) allocation_shard_id: String,
    pub(crate) foundationdb_cluster_file: String,
    pub(crate) service_heartbeat_interval: Duration,
    pub(crate) stats_root: PathBuf,
    pub(crate) stats_publish_interval: Duration,
    pub(crate) reservation_ttl: Duration,
    pub(crate) reservation_reap_interval: Duration,
    pub(crate) reservation_reap_limit: usize,
    pub(crate) reserve_batch_size: usize,
    pub(crate) reservation_bin_refill_interval: Duration,
    pub(crate) reservation_bin_low_watermark: usize,
    pub(crate) reservation_bin_high_watermark: usize,
    pub(crate) reservation_bin_top_up_chunk: usize,
    pub(crate) reservation_bin_bypass_batch_size: usize,
    pub(crate) reset_allocator_state_and_exit: bool,
    /// Phase-2 write-scale opt-in (DESIGN_KAS_WRITE_SCALE.md §3 change #2/#4).
    ///
    /// When `false` (the default) the allocator keeps the historical
    /// acquire-lease / refresh / mutate / release-lease dance on every mutating
    /// op. When `true` the leader acquires the per-shard coordination lease once,
    /// renews it in the background at TTL/2, drops the per-op stamp read while the
    /// lease is held, and gives it up only on step-down / lost renewal / observed
    /// epoch bump. The behavior change is gated so the default build keeps the
    /// proven path and the new path is opt-in for lab validation + rollback.
    ///
    /// The epoch fence itself (the in-txn epoch read+assert that makes the
    /// leader-resident lease split-brain-safe) is *always* on — it is strictly
    /// stronger than the per-op stamp reload it replaces and is safe with either
    /// lease model. This flag only gates the lease-hold/renew/drop-stamp behavior.
    pub(crate) allocator_leader_resident_lease: bool,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    listen_addr: Option<String>,
    public_endpoint: Option<String>,
    allocation_shard_id: Option<String>,
    foundationdb_cluster_file: Option<String>,
    service_heartbeat_ms: Option<u64>,
    stats_root: Option<String>,
    stats_publish_ms: Option<u64>,
    reservation_ttl_ms: Option<u64>,
    reservation_reap_ms: Option<u64>,
    reservation_reap_limit: Option<usize>,
    reserve_batch_size: Option<usize>,
    reservation_bin_refill_ms: Option<u64>,
    reservation_bin_low_watermark: Option<usize>,
    reservation_bin_high_watermark: Option<usize>,
    reservation_bin_top_up_chunk: Option<usize>,
    reservation_bin_bypass_batch_size: Option<usize>,
    reset_allocator_state_and_exit: Option<bool>,
    allocator_leader_resident_lease: Option<bool>,
}

impl Config {
    fn defaults() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            listen_addr: "127.0.0.1:50061".parse()?,
            public_endpoint: "http://127.0.0.1:50061".to_string(),
            allocation_shard_id: "alloc-shard-00".to_string(),
            foundationdb_cluster_file: "/etc/foundationdb/fdb.cluster".to_string(),
            service_heartbeat_interval: Duration::from_millis(30_000),
            stats_root: PathBuf::from("/run/keinfs/kas"),
            stats_publish_interval: Duration::from_millis(250),
            reservation_ttl: Duration::from_millis(120_000),
            reservation_reap_interval: Duration::from_millis(1_000),
            reservation_reap_limit: 4_096,
            reserve_batch_size: 65_536,
            reservation_bin_refill_interval: Duration::from_millis(100),
            // The bin is a burst buffer, not a bulk pre-reservation: a few
            // thousand pre-reserved stripes absorb reserve latency without
            // pinning millions of granules (which fragments target free-span
            // lists and bloats FDB writes). The refiller tops up in bounded
            // sub-batches, so these need not stay under any single-txn limit.
            reservation_bin_low_watermark: 1_024,
            reservation_bin_high_watermark: 4_096,
            reservation_bin_top_up_chunk: 2_048,
            reservation_bin_bypass_batch_size: 2_048,
            reset_allocator_state_and_exit: false,
            // Default FALSE: keep the current per-op acquire/release lease path.
            // The leader-resident lease + dropped per-op stamp read is opt-in
            // (DESIGN_KAS_WRITE_SCALE.md §3 change #2/#4, §5 Phase 2).
            allocator_leader_resident_lease: false,
        })
    }

    fn apply_file(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        let raw = fs::read_to_string(path).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!("failed to read KAS config `{}`: {err}", path.display()),
            )
        })?;
        let file: FileConfig = toml::from_str(&raw).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse KAS config `{}`: {err}", path.display()),
            )
        })?;

        if let Some(value) = file.listen_addr {
            self.listen_addr = value.parse().map_err(|err| {
                arg_error(format!(
                    "invalid `listen_addr` in KAS config `{}`: {err}",
                    path.display()
                ))
            })?;
        }
        if let Some(value) = file.public_endpoint {
            self.public_endpoint = normalize_endpoint(value);
        }
        if let Some(value) = file.allocation_shard_id {
            self.allocation_shard_id = value.trim().to_string();
        }
        if let Some(value) = file.foundationdb_cluster_file {
            self.foundationdb_cluster_file = value;
        }
        if let Some(value) = file.service_heartbeat_ms {
            self.service_heartbeat_interval = Duration::from_millis(value.max(1_000));
        }
        if let Some(value) = file.stats_root {
            self.stats_root = PathBuf::from(value);
        }
        if let Some(value) = file.stats_publish_ms {
            self.stats_publish_interval = Duration::from_millis(value.max(50));
        }
        if let Some(value) = file.reservation_ttl_ms {
            self.reservation_ttl = Duration::from_millis(value.max(1_000));
        }
        if let Some(value) = file.reservation_reap_ms {
            self.reservation_reap_interval = Duration::from_millis(value.max(200));
        }
        if let Some(value) = file.reservation_reap_limit {
            self.reservation_reap_limit = value;
        }
        if let Some(value) = file.reserve_batch_size {
            self.reserve_batch_size = value;
        }
        if let Some(value) = file.reservation_bin_refill_ms {
            self.reservation_bin_refill_interval = Duration::from_millis(value.max(50));
        }
        if let Some(value) = file.reservation_bin_low_watermark {
            self.reservation_bin_low_watermark = value;
        }
        if let Some(value) = file.reservation_bin_high_watermark {
            self.reservation_bin_high_watermark = value;
        }
        if let Some(value) = file.reservation_bin_top_up_chunk {
            self.reservation_bin_top_up_chunk = value;
        }
        if let Some(value) = file.reservation_bin_bypass_batch_size {
            self.reservation_bin_bypass_batch_size = value;
        }
        if let Some(value) = file.reset_allocator_state_and_exit {
            self.reset_allocator_state_and_exit = value;
        }
        if let Some(value) = file.allocator_leader_resident_lease {
            self.allocator_leader_resident_lease = value;
        }

        Ok(())
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.foundationdb_cluster_file.trim().is_empty() {
            return Err(arg_error("--foundationdb-cluster-file must be set"));
        }
        if self.allocation_shard_id.trim().is_empty() {
            return Err(arg_error("--allocation-shard-id must be set"));
        }
        if self.reservation_reap_limit == 0 {
            return Err(arg_error("--reservation-reap-limit must be > 0"));
        }
        if self.reserve_batch_size == 0 {
            return Err(arg_error("--reserve-batch-size must be > 0"));
        }
        if self.reservation_bin_high_watermark > 0
            && self.reservation_bin_low_watermark >= self.reservation_bin_high_watermark
        {
            return Err(arg_error(
                "--reservation-bin-low-watermark must be < --reservation-bin-high-watermark",
            ));
        }
        if self.reservation_bin_top_up_chunk == 0 {
            return Err(arg_error("--reservation-bin-top-up-chunk must be > 0"));
        }
        Ok(())
    }

    pub(crate) fn fingerprint_source(&self) -> String {
        format!(
            concat!(
                "listen_addr={}\n",
                "public_endpoint={}\n",
                "allocation_shard_id={}\n",
                "foundationdb_cluster_file={}\n",
                "service_heartbeat_ms={}\n",
                "stats_root={}\n",
                "stats_publish_ms={}\n",
                "reservation_ttl_ms={}\n",
                "reservation_reap_ms={}\n",
                "reservation_reap_limit={}\n",
                "reserve_batch_size={}\n",
                "reservation_bin_refill_ms={}\n",
                "reservation_bin_low_watermark={}\n",
                "reservation_bin_high_watermark={}\n",
                "reservation_bin_top_up_chunk={}\n",
                "reservation_bin_bypass_batch_size={}\n",
                "reset_allocator_state_and_exit={}\n",
                "allocator_leader_resident_lease={}\n"
            ),
            self.listen_addr,
            self.public_endpoint,
            self.allocation_shard_id,
            self.foundationdb_cluster_file,
            self.service_heartbeat_interval.as_millis(),
            self.stats_root.display(),
            self.stats_publish_interval.as_millis(),
            self.reservation_ttl.as_millis(),
            self.reservation_reap_interval.as_millis(),
            self.reservation_reap_limit,
            self.reserve_batch_size,
            self.reservation_bin_refill_interval.as_millis(),
            self.reservation_bin_low_watermark,
            self.reservation_bin_high_watermark,
            self.reservation_bin_top_up_chunk,
            self.reservation_bin_bypass_batch_size,
            self.reset_allocator_state_and_exit,
            self.allocator_leader_resident_lease,
        )
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
            "--public-endpoint" => {
                i += 1;
                config.public_endpoint = normalize_endpoint(
                    args.get(i)
                        .ok_or_else(|| missing_value("--public-endpoint"))?
                        .clone(),
                );
            }
            "--allocation-shard-id" => {
                i += 1;
                config.allocation_shard_id = args
                    .get(i)
                    .ok_or_else(|| missing_value("--allocation-shard-id"))?
                    .trim()
                    .to_string();
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
            "--reservation-ttl-ms" => {
                i += 1;
                config.reservation_ttl = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--reservation-ttl-ms")?.max(1_000),
                );
            }
            "--reservation-reap-ms" => {
                i += 1;
                config.reservation_reap_interval = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--reservation-reap-ms")?.max(200),
                );
            }
            "--reservation-reap-limit" => {
                i += 1;
                config.reservation_reap_limit =
                    parse_usize_arg(args.get(i), "--reservation-reap-limit")?;
            }
            "--reserve-batch-size" => {
                i += 1;
                config.reserve_batch_size = parse_usize_arg(args.get(i), "--reserve-batch-size")?;
            }
            "--reservation-bin-refill-ms" => {
                i += 1;
                config.reservation_bin_refill_interval = Duration::from_millis(
                    parse_u64_arg(args.get(i), "--reservation-bin-refill-ms")?.max(50),
                );
            }
            "--reservation-bin-low-watermark" => {
                i += 1;
                config.reservation_bin_low_watermark =
                    parse_usize_arg(args.get(i), "--reservation-bin-low-watermark")?;
            }
            "--reservation-bin-high-watermark" => {
                i += 1;
                config.reservation_bin_high_watermark =
                    parse_usize_arg(args.get(i), "--reservation-bin-high-watermark")?;
            }
            "--reservation-bin-top-up-chunk" => {
                i += 1;
                config.reservation_bin_top_up_chunk =
                    parse_usize_arg(args.get(i), "--reservation-bin-top-up-chunk")?;
            }
            "--reservation-bin-bypass-batch-size" => {
                i += 1;
                config.reservation_bin_bypass_batch_size =
                    parse_usize_arg(args.get(i), "--reservation-bin-bypass-batch-size")?;
            }
            "--reset-allocator-state-and-exit" => {
                config.reset_allocator_state_and_exit = true;
            }
            "--allocator-leader-resident-lease" => {
                config.allocator_leader_resident_lease = true;
            }
            flag => {
                return Err(arg_error(format!(
                    "unknown KAS option `{flag}`\n\n{}",
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

fn normalize_endpoint(value: String) -> String {
    let trimmed = value.trim().trim_end_matches('/').to_string();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed
    } else {
        format!("http://{trimmed}")
    }
}

fn usage() -> String {
    "kas [--config /etc/keinfs/kas.toml] [--listen 127.0.0.1:50061] [--public-endpoint http://127.0.0.1:50061] [--allocation-shard-id alloc-shard-00] [--foundationdb-cluster-file /etc/foundationdb/fdb.cluster] [--service-heartbeat-ms 30000] [--stats-root /run/keinfs/kas] [--stats-publish-ms 250] [--reservation-ttl-ms 30000] [--reservation-reap-ms 1000] [--reservation-reap-limit 256] [--reserve-batch-size 4096] [--reservation-bin-refill-ms 250] [--reservation-bin-low-watermark 2048] [--reservation-bin-high-watermark 8192] [--reservation-bin-top-up-chunk 65536] [--reservation-bin-bypass-batch-size 2048] [--allocator-leader-resident-lease] [--reset-allocator-state-and-exit]"
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
    use super::{parse_args, Config};

    #[test]
    fn defaults_are_foundationdb_only() {
        let defaults = Config::defaults().expect("defaults");
        assert_eq!(
            defaults.foundationdb_cluster_file,
            "/etc/foundationdb/fdb.cluster"
        );
        assert_eq!(defaults.listen_addr.to_string(), "127.0.0.1:50061");
        // The leader-resident lease (Phase-2 write-scale opt-in) is OFF by
        // default so the default build keeps the proven per-op lease path.
        assert!(!defaults.allocator_leader_resident_lease);
    }

    #[test]
    fn leader_resident_lease_flag_is_opt_in() {
        let parsed = parse_args(vec![
            "--foundationdb-cluster-file".to_string(),
            "/tmp/fdb.cluster".to_string(),
            "--allocator-leader-resident-lease".to_string(),
        ])
        .expect("parsed args");
        assert!(parsed.allocator_leader_resident_lease);
    }

    #[test]
    fn parse_foundationdb_args() {
        let parsed = parse_args(vec![
            "--listen".to_string(),
            "0.0.0.0:50061".to_string(),
            "--foundationdb-cluster-file".to_string(),
            "/tmp/fdb.cluster".to_string(),
            "--reserve-batch-size".to_string(),
            "8192".to_string(),
        ])
        .expect("parsed args");
        assert_eq!(parsed.listen_addr.to_string(), "0.0.0.0:50061");
        assert_eq!(parsed.foundationdb_cluster_file, "/tmp/fdb.cluster");
        assert_eq!(parsed.reserve_batch_size, 8192);
    }
}
