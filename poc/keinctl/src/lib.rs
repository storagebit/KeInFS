// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

pub mod proto {
    tonic::include_proto!("keinfs.control");
}

pub mod committed_occupancy;
pub mod placement;

use proto::{EcProfile, FailureDomain};
use std::fmt;

pub const SINGLE_STRIPE_DATA_FRAGMENTS: u32 = 8;
pub const SINGLE_STRIPE_PARITY_FRAGMENTS: u32 = 2;
pub const SINGLE_STRIPE_FRAGMENT_BYTES: u32 = 1024 * 1024;
pub const LAB_MIN_FRAGMENT_BYTES: u32 = 64 * 1024;
/// Reed-Solomon over GF(2^8) supports at most 256 total shards per stripe.
pub const MAX_TOTAL_FRAGMENTS: u32 = 256;

impl EcProfile {
    pub fn validate_single_stripe_lab(&self) -> Result<(), ProfileError> {
        // Any (k, m) the codec supports is accepted — the engine is generic over the
        // profile, and replicated small-object classes are (1, m). The fragment-size
        // and failure-domain rules below are what the lab slice actually constrains.
        if self.data_fragments == 0 {
            return Err(ProfileError(format!(
                "EC profile {} declares zero data fragments",
                self.id
            )));
        }
        if self.parity_fragments == 0 {
            return Err(ProfileError(format!(
                "EC profile {} declares zero parity fragments; a stripe must survive at least one loss",
                self.id
            )));
        }
        let total = self.data_fragments.saturating_add(self.parity_fragments);
        if total > MAX_TOTAL_FRAGMENTS {
            return Err(ProfileError(format!(
                "EC profile {} declares {} total fragments, over the {} the GF(2^8) codec supports",
                self.id, total, MAX_TOTAL_FRAGMENTS
            )));
        }
        if self.fragment_bytes < LAB_MIN_FRAGMENT_BYTES {
            return Err(ProfileError(format!(
                "EC profile {} declares fragment size {} bytes, but the first lab slice requires at least {}",
                self.id, self.fragment_bytes, LAB_MIN_FRAGMENT_BYTES
            )));
        }
        if !self.fragment_bytes.is_power_of_two() {
            return Err(ProfileError(format!(
                "EC profile {} declares fragment size {} bytes, but the first lab slice requires a power-of-two fragment size",
                self.id, self.fragment_bytes
            )));
        }
        if self.fragment_bytes % 4096 != 0 {
            return Err(ProfileError(format!(
                "EC profile {} declares fragment size {} bytes, but the first lab slice requires 4 KiB alignment",
                self.id, self.fragment_bytes
            )));
        }
        if self.failure_domain != FailureDomain::DriveDomainLab as i32 {
            return Err(ProfileError(format!(
                "EC profile {} uses failure domain {:?}, but the first lab slice requires drive-domain-lab",
                self.id,
                FailureDomain::try_from(self.failure_domain).unwrap_or(FailureDomain::Unspecified)
            )));
        }
        Ok(())
    }

    pub fn fragment_count(&self) -> u32 {
        self.data_fragments + self.parity_fragments
    }
}

#[derive(Debug)]
pub struct ProfileError(String);

impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ProfileError {}
