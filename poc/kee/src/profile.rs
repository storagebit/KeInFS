// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::error::KeeError;

pub const DATA_FRAGMENTS: usize = 8;
pub const PARITY_FRAGMENTS: usize = 2;
pub const TOTAL_FRAGMENTS: usize = DATA_FRAGMENTS + PARITY_FRAGMENTS;
pub const DEFAULT_FRAGMENT_BYTES: usize = 1_048_576;
pub const MIN_FRAGMENT_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureDomain {
    DriveDomainLab,
    Node,
    Rack,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcProfile {
    pub id: String,
    pub codec_id: String,
    pub data_fragments: usize,
    pub parity_fragments: usize,
    pub fragment_bytes: usize,
    pub failure_domain: FailureDomain,
}

impl EcProfile {
    pub fn single_stripe_8_plus_2(
        id: impl Into<String>,
        codec_id: impl Into<String>,
        failure_domain: FailureDomain,
    ) -> Self {
        Self {
            id: id.into(),
            codec_id: codec_id.into(),
            data_fragments: DATA_FRAGMENTS,
            parity_fragments: PARITY_FRAGMENTS,
            fragment_bytes: DEFAULT_FRAGMENT_BYTES,
            failure_domain,
        }
    }

    pub fn validate(&self) -> Result<(), KeeError> {
        if self.id.trim().is_empty() {
            return Err(KeeError::InvalidProfile(
                "profile id must not be empty".to_string(),
            ));
        }
        if self.codec_id.trim().is_empty() {
            return Err(KeeError::InvalidProfile(
                "codec id must not be empty".to_string(),
            ));
        }
        if self.data_fragments == 0 {
            return Err(KeeError::InvalidProfile(
                "data_fragments must be greater than zero".to_string(),
            ));
        }
        if self.parity_fragments == 0 {
            return Err(KeeError::InvalidProfile(
                "parity_fragments must be greater than zero".to_string(),
            ));
        }
        if self.fragment_bytes == 0 {
            return Err(KeeError::InvalidProfile(
                "fragment_bytes must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_single_stripe(&self) -> Result<(), KeeError> {
        self.validate()?;
        if self.data_fragments != DATA_FRAGMENTS || self.parity_fragments != PARITY_FRAGMENTS {
            return Err(KeeError::InvalidProfile(format!(
                "current KEE POC requires exactly {DATA_FRAGMENTS}+{PARITY_FRAGMENTS} fragments"
            )));
        }
        if self.fragment_bytes < MIN_FRAGMENT_BYTES {
            return Err(KeeError::InvalidProfile(format!(
                "current KEE POC requires at least {MIN_FRAGMENT_BYTES} fragment bytes"
            )));
        }
        if !self.fragment_bytes.is_power_of_two() {
            return Err(KeeError::InvalidProfile(
                "current KEE POC requires a power-of-two fragment size".to_string(),
            ));
        }
        if self.fragment_bytes % 4096 != 0 {
            return Err(KeeError::InvalidProfile(
                "current KEE POC requires a 4 KiB-aligned fragment size".to_string(),
            ));
        }
        Ok(())
    }

    pub fn max_object_bytes(&self) -> usize {
        self.data_fragments * self.fragment_bytes
    }
}
