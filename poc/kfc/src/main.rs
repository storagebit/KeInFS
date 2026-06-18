// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! KFC v2 binary: a thin CLI over the portable `kfc-core` engine and the
//! `kfc-transport` FUSE shim. `mount` drives a real FUSE mount (feature
//! `fuse`); `mode-bench` exercises the KSC object path directly.

mod bench;
mod config;
mod metadata;

use crate::bench::run_mode_bench;
use crate::config::{parse_args, Command, MountConfig};
use crate::metadata::{boxed_error, DynError};

fn main() -> Result<(), DynError> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match parse_args(args).map_err(|err| boxed_error(err.to_string()))? {
        Command::Mount(config) => run_mount(config)?,
        Command::ModeBench(config) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|err| boxed_error(err.to_string()))?;
            runtime.block_on(run_mode_bench(config))?;
        }
    }
    Ok(())
}

#[cfg(feature = "fuse")]
fn run_mount(config: MountConfig) -> Result<(), DynError> {
    let mountpoint = config.mountpoint.clone();
    let fs_config = kfc_core::FsConfig {
        kms_endpoints: config.kms_endpoints,
        namespace_id: config.namespace_id,
        bucket_id: config.bucket_id,
        read_completion_mode: config.read_completion_mode,
        write_completion_mode: config.write_completion_mode,
        metadata_notification_nats_url: config.metadata_notification_nats_url,
        metadata_notification_subject: config.metadata_notification_subject,
        // Tier-C disk stripe cache: opt-in via --tier-c-cache-dir, off by default.
        tier_c_cache_dir: config.tier_c_cache_dir,
        tier_c_budget_bytes: config.tier_c_budget_bytes,
        stripe_cache_budget_bytes: config.stripe_cache_budget_bytes,
    };
    kfc_transport::run_mount(fs_config, mountpoint, kfc_transport::MountOpts::default())
        .map_err(|err| boxed_error(err.to_string()))
}

#[cfg(not(feature = "fuse"))]
fn run_mount(_config: MountConfig) -> Result<(), DynError> {
    Err(boxed_error(
        "KFC mount support was built without the `fuse` feature",
    ))
}
