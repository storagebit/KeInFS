// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::backend::{hardware_inventory, HardwareInventory};
use crate::error::KeeError;
use crate::isa_l_backend::PreparedBackendPlan;
use crate::profile::{EcProfile, TOTAL_FRAGMENTS};

#[derive(Debug, Clone)]
pub struct PreparedEcPlan {
    profile: EcProfile,
    inventory: HardwareInventory,
    prepared: PreparedBackendPlan,
}

impl PreparedEcPlan {
    pub fn new(profile: EcProfile) -> Result<Self, KeeError> {
        profile.validate_single_stripe()?;
        crate::backend::maybe_warn_software_backend();
        let prepared = PreparedBackendPlan::new(&profile)?;
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
        vec![vec![0_u8; self.profile.fragment_bytes]; TOTAL_FRAGMENTS]
    }

    pub fn encode(&self, object: &[u8]) -> Result<Vec<Vec<u8>>, KeeError> {
        let mut shards = self.allocate_output_buffers();
        self.encode_into(object, &mut shards)?;
        Ok(shards)
    }

    pub fn encode_into(&self, object: &[u8], shards: &mut [Vec<u8>]) -> Result<(), KeeError> {
        self.prepared.encode_into(object, shards)
    }

    pub fn reconstruct(&self, fragments: &mut [Option<Vec<u8>>]) -> Result<Vec<Vec<u8>>, KeeError> {
        self.prepared.reconstruct(fragments)
    }
}
