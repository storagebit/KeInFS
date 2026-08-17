// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::backend::{hardware_inventory, HardwareInventory};
use crate::error::KeeError;
use crate::isa_l_backend::{plan_supports, PreparedBackendPlan};
use crate::profile::EcProfile;
use reed_solomon_erasure::galois_8::ReedSolomon;

/// The codec behind a prepared plan. The selected backend's fast path may be
/// tuned for one fixed stripe shape (ISA-L's tables and pointer arrays are);
/// profiles outside that shape — replicated small-object classes especially —
/// run the dynamic software codec instead, whose (k, m) comes from the profile.
#[derive(Debug, Clone)]
enum PreparedCodec {
    Backend(PreparedBackendPlan),
    Dynamic(ReedSolomon),
}

#[derive(Debug, Clone)]
pub struct PreparedEcPlan {
    profile: EcProfile,
    inventory: HardwareInventory,
    prepared: PreparedCodec,
}

impl PreparedEcPlan {
    pub fn new(profile: EcProfile) -> Result<Self, KeeError> {
        profile.validate_single_stripe()?;
        crate::backend::maybe_warn_software_backend();
        let prepared = if plan_supports(&profile) {
            PreparedCodec::Backend(PreparedBackendPlan::new(&profile)?)
        } else {
            PreparedCodec::Dynamic(ReedSolomon::new(
                profile.data_fragments,
                profile.parity_fragments,
            )?)
        };
        Ok(Self {
            profile,
            inventory: hardware_inventory(),
            prepared,
        })
    }

    pub fn profile(&self) -> &EcProfile {
        &self.profile
    }

    pub fn inventory(&self) -> &HardwareInventory {
        &self.inventory
    }

    pub fn allocate_output_buffers(&self) -> Vec<Vec<u8>> {
        let total = self.profile.data_fragments + self.profile.parity_fragments;
        vec![vec![0_u8; self.profile.fragment_bytes]; total]
    }

    pub fn encode(&self, object: &[u8]) -> Result<Vec<Vec<u8>>, KeeError> {
        let mut shards = self.allocate_output_buffers();
        self.encode_into(object, &mut shards)?;
        Ok(shards)
    }

    pub fn encode_into(&self, object: &[u8], shards: &mut [Vec<u8>]) -> Result<(), KeeError> {
        match &self.prepared {
            PreparedCodec::Backend(plan) => plan.encode_into(object, shards),
            PreparedCodec::Dynamic(codec) => {
                crate::software::encode_into_with_codec(&self.profile, object, shards, codec)
            }
        }
    }

    pub fn reconstruct(&self, fragments: &mut [Option<Vec<u8>>]) -> Result<Vec<Vec<u8>>, KeeError> {
        match &self.prepared {
            PreparedCodec::Backend(plan) => plan.reconstruct(fragments),
            PreparedCodec::Dynamic(_) => crate::software::reconstruct(&self.profile, fragments),
        }
    }
}
