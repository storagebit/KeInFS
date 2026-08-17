// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::arena::DriveConfig;
use std::collections::HashSet;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug)]
pub enum KixError {
    Io(io::Error),
    ChannelClosed,
}

impl fmt::Display for KixError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::ChannelClosed => write!(
                f,
                "KIX work queue closed; a lookup worker, commit worker, or drive appender likely exited unexpectedly"
            ),
        }
    }
}

impl std::error::Error for KixError {}

impl From<io::Error> for KixError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum WorkerMode {
    Interrupt,
    BusyPoll { spins_before_yield: usize },
}

impl WorkerMode {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Interrupt => "interrupt",
            Self::BusyPoll { .. } => "busy",
        }
    }
}

#[derive(Clone, Debug)]
pub struct KixStatsConfig {
    pub root_dir: PathBuf,
    pub publish_interval: Duration,
}

#[derive(Clone, Debug)]
pub struct KixConfig {
    pub shard_count: usize,
    pub lookup_worker_mode: WorkerMode,
    pub commit_worker_mode: WorkerMode,
    pub drive_worker_mode: WorkerMode,
    pub drive_configs: Vec<DriveConfig>,
    pub lookup_pin_cores: Vec<usize>,
    pub commit_pin_cores: Vec<usize>,
    pub drive_pin_cores: Vec<usize>,
    pub shard_numa_node: Option<i32>,
    pub lookup_queue_depth: usize,
    pub commit_queue_depth: usize,
    pub drive_queue_depth: usize,
    pub stats: Option<KixStatsConfig>,
}

impl KixConfig {
    pub fn validate(&self) -> Result<(), KixError> {
        if self.shard_count == 0 {
            return Err(KixError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shard_count must be > 0",
            )));
        }
        if self.drive_configs.is_empty() {
            return Err(KixError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "at least one drive config is required",
            )));
        }
        if self.lookup_queue_depth == 0 {
            return Err(KixError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "lookup_queue_depth must be > 0",
            )));
        }
        if self.commit_queue_depth == 0 {
            return Err(KixError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "commit_queue_depth must be > 0",
            )));
        }
        if self.drive_queue_depth == 0 {
            return Err(KixError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "drive_queue_depth must be > 0",
            )));
        }
        if let Some(stats) = &self.stats {
            if stats.root_dir.as_os_str().is_empty() {
                return Err(KixError::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "KIX stats root directory must not be empty",
                )));
            }
            if stats.publish_interval.is_zero() {
                return Err(KixError::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "KIX stats publish interval must be > 0",
                )));
            }
        }
        validate_drive_ids(&self.drive_configs)?;
        validate_requested_cores("lookup", &self.lookup_pin_cores)?;
        validate_requested_cores("commit", &self.commit_pin_cores)?;
        validate_requested_cores("drive", &self.drive_pin_cores)?;
        Ok(())
    }
}

fn validate_drive_ids(configs: &[DriveConfig]) -> Result<(), KixError> {
    let mut seen = HashSet::new();
    for cfg in configs {
        if !seen.insert(cfg.id) {
            return Err(KixError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "duplicate KIX drive id {} in startup configuration; drive ids must be unique",
                    cfg.id
                ),
            )));
        }
    }
    Ok(())
}

fn validate_requested_cores(role: &str, requested: &[usize]) -> Result<(), KixError> {
    if requested.is_empty() {
        return Ok(());
    }
    let Some(available) = core_affinity::get_core_ids() else {
        return Err(KixError::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("KIX startup could not enumerate CPU cores while validating {role} pinning"),
        )));
    };
    let available = available
        .into_iter()
        .map(|core| core.id)
        .collect::<HashSet<_>>();
    let mut invalid = requested
        .iter()
        .copied()
        .filter(|core_id| !available.contains(core_id))
        .collect::<Vec<_>>();
    invalid.sort_unstable();
    invalid.dedup();
    if invalid.is_empty() {
        return Ok(());
    }
    Err(KixError::Io(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "KIX startup rejected {role} pinning because the following CPU cores are unavailable on this host: {}",
            invalid
                .iter()
                .map(|core| core.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
    )))
}
