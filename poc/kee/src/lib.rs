// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

mod backend;
mod error;
mod isa_l_backend;
mod prepared;
mod profile;
mod software;

pub use backend::{backend_inventory, hardware_inventory, BackendKind, HardwareInventory};
pub use error::KeeError;
pub use prepared::PreparedEcPlan;
pub use profile::{
    EcProfile, FailureDomain, DATA_FRAGMENTS, DEFAULT_FRAGMENT_BYTES, PARITY_FRAGMENTS,
    TOTAL_FRAGMENTS,
};

#[derive(Debug, Clone)]
pub struct KeeEngine {
    profile: EcProfile,
    inventory: HardwareInventory,
}

impl KeeEngine {
    pub fn new(profile: EcProfile) -> Result<Self, KeeError> {
        profile.validate_single_stripe()?;
        backend::maybe_warn_software_backend();
        Ok(Self {
            profile,
            inventory: hardware_inventory(),
        })
    }

    pub fn profile(&self) -> &EcProfile {
        &self.profile
    }

    pub fn inventory(&self) -> &HardwareInventory {
        &self.inventory
    }

    pub fn encode(&self, object: &[u8]) -> Result<Vec<Vec<u8>>, KeeError> {
        encode(&self.profile, object)
    }

    pub fn prepared_plan(&self) -> Result<PreparedEcPlan, KeeError> {
        PreparedEcPlan::new(self.profile.clone())
    }

    pub fn reconstruct(&self, fragments: &mut [Option<Vec<u8>>]) -> Result<Vec<Vec<u8>>, KeeError> {
        reconstruct(&self.profile, fragments)
    }

    pub fn verify(&self, fragments: &[Vec<u8>]) -> Result<bool, KeeError> {
        verify(&self.profile, fragments)
    }
}

pub fn encode(profile: &EcProfile, object: &[u8]) -> Result<Vec<Vec<u8>>, KeeError> {
    match hardware_inventory().selected_backend {
        // The tuned backend only handles its fixed stripe shape; other profiles
        // (replicated classes, alternate widths) run the dynamic software codec.
        BackendKind::IsaL if isa_l_backend::plan_supports(profile) => {
            isa_l_backend::encode(profile, object)
        }
        _ => software::encode(profile, object),
    }
}

pub fn reconstruct(
    profile: &EcProfile,
    fragments: &mut [Option<Vec<u8>>],
) -> Result<Vec<Vec<u8>>, KeeError> {
    match hardware_inventory().selected_backend {
        BackendKind::IsaL if isa_l_backend::plan_supports(profile) => {
            isa_l_backend::reconstruct(profile, fragments)
        }
        _ => software::reconstruct(profile, fragments),
    }
}

pub fn verify(profile: &EcProfile, fragments: &[Vec<u8>]) -> Result<bool, KeeError> {
    profile.validate_single_stripe()?;
    let total = profile.data_fragments + profile.parity_fragments;
    if fragments.len() != total {
        return Err(KeeError::FragmentCountMismatch {
            expected: total,
            actual: fragments.len(),
        });
    }
    for (index, fragment) in fragments.iter().enumerate() {
        if fragment.len() != profile.fragment_bytes {
            return Err(KeeError::FragmentSizeMismatch {
                index,
                expected: profile.fragment_bytes,
                actual: fragment.len(),
            });
        }
    }
    let payload = fragments[..profile.data_fragments]
        .iter()
        .flat_map(|fragment| fragment.iter().copied())
        .collect::<Vec<u8>>();
    let encoded = encode(profile, &payload)?;
    Ok(encoded == fragments)
}

#[cfg(test)]
mod tests;
