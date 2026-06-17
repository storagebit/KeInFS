// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::error::KeeError;
use crate::profile::EcProfile;

#[cfg(feature = "isa-l-backend")]
mod imp {
    use super::*;
    use crate::profile::{DATA_FRAGMENTS, PARITY_FRAGMENTS, TOTAL_FRAGMENTS};
    use std::collections::HashMap;
    use std::ptr;
    use std::sync::OnceLock;

    const INVALID_INDEX: usize = usize::MAX;

    #[derive(Debug)]
    struct PreparedBackendCore {
        encode_tables: Vec<u8>,
        decode_plans: HashMap<u16, PreparedDecodePlan>,
    }

    #[derive(Debug, Clone)]
    pub struct PreparedBackendPlan {
        profile: EcProfile,
        core: &'static PreparedBackendCore,
    }

    #[derive(Debug, Clone)]
    struct PreparedDecodePlan {
        rows: usize,
        missing_indexes: [usize; PARITY_FRAGMENTS],
        source_indexes: [usize; DATA_FRAGMENTS],
        decode_tables: Vec<u8>,
    }

    impl PreparedBackendPlan {
        pub fn new(profile: &EcProfile) -> Result<Self, KeeError> {
            profile.validate_single_stripe()?;
            Ok(Self {
                profile: profile.clone(),
                core: shared_core(),
            })
        }

        pub fn encode_into(&self, object: &[u8], shards: &mut [Vec<u8>]) -> Result<(), KeeError> {
            prepare_shards(&self.profile, object, shards)?;
            let (data, parity) = shards.split_at_mut(DATA_FRAGMENTS);
            ec_encode_with_tables(
                self.profile.fragment_bytes,
                DATA_FRAGMENTS,
                PARITY_FRAGMENTS,
                &self.core.encode_tables,
                data,
                parity,
            );
            Ok(())
        }

        pub fn reconstruct(
            &self,
            fragments: &mut [Option<Vec<u8>>],
        ) -> Result<Vec<Vec<u8>>, KeeError> {
            validate_fragment_slots(&self.profile, fragments)?;
            let missing = collect_missing_indexes(fragments);
            if missing.is_empty() {
                return take_all_fragments(fragments);
            }
            let plan = self
                .core
                .decode_plans
                .get(&erasure_key(&missing))
                .ok_or_else(|| {
                    KeeError::Codec(format!(
                        "no prepared ISA-L decode plan for missing set {:?}",
                        missing
                    ))
                })?;

            let mut data_ptrs = [ptr::null(); DATA_FRAGMENTS];
            for (slot, &source_index) in plan.source_indexes.iter().enumerate() {
                let source = fragments[source_index].as_ref().ok_or_else(|| {
                    KeeError::Codec(format!(
                        "prepared decode source fragment {} disappeared mid-rebuild",
                        source_index
                    ))
                })?;
                data_ptrs[slot] = source.as_ptr();
            }

            let mut recovered = (0..plan.rows)
                .map(|_| vec![0_u8; self.profile.fragment_bytes])
                .collect::<Vec<_>>();
            let mut output_ptrs = [ptr::null_mut(); PARITY_FRAGMENTS];
            for (slot, fragment) in recovered.iter_mut().enumerate() {
                output_ptrs[slot] = fragment.as_mut_ptr();
            }
            keisal::encode_data(
                self.profile.fragment_bytes,
                DATA_FRAGMENTS,
                plan.rows,
                &plan.decode_tables,
                &data_ptrs,
                &mut output_ptrs[..plan.rows],
            );

            for (recovered_fragment, &missing_index) in recovered
                .into_iter()
                .zip(plan.missing_indexes.iter().take(plan.rows))
            {
                fragments[missing_index] = Some(recovered_fragment);
            }
            take_all_fragments(fragments)
        }
    }

    impl PreparedBackendCore {
        fn build() -> Result<Self, KeeError> {
            let encode_matrix = keisal::generate_rs_matrix(TOTAL_FRAGMENTS, DATA_FRAGMENTS);
            let parity_rows = &encode_matrix[DATA_FRAGMENTS * DATA_FRAGMENTS..];
            let encode_tables = keisal::init_tables(DATA_FRAGMENTS, PARITY_FRAGMENTS, parity_rows);
            let decode_plans = build_decode_plans(&encode_matrix)?;
            Ok(Self {
                encode_tables,
                decode_plans,
            })
        }
    }

    fn shared_core() -> &'static PreparedBackendCore {
        static CORE: OnceLock<PreparedBackendCore> = OnceLock::new();
        CORE.get_or_init(|| {
            PreparedBackendCore::build()
                .expect("KeInFS ISA-L prepared core failed to initialize for fixed 8+2 geometry")
        })
    }

    fn build_decode_plans(
        encode_matrix: &[u8],
    ) -> Result<HashMap<u16, PreparedDecodePlan>, KeeError> {
        let mut plans = HashMap::new();
        for first in 0..TOTAL_FRAGMENTS {
            let single = [first];
            plans.insert(
                erasure_key(&single),
                build_decode_plan(encode_matrix, &single)?,
            );
            for second in first + 1..TOTAL_FRAGMENTS {
                let pair = [first, second];
                plans.insert(erasure_key(&pair), build_decode_plan(encode_matrix, &pair)?);
            }
        }
        Ok(plans)
    }

    fn build_decode_plan(
        encode_matrix: &[u8],
        missing: &[usize],
    ) -> Result<PreparedDecodePlan, KeeError> {
        let mut source_indexes = [INVALID_INDEX; DATA_FRAGMENTS];
        let mut next_source = 0;
        for index in 0..TOTAL_FRAGMENTS {
            if missing.contains(&index) {
                continue;
            }
            source_indexes[next_source] = index;
            next_source += 1;
            if next_source == DATA_FRAGMENTS {
                break;
            }
        }
        if next_source != DATA_FRAGMENTS {
            return Err(KeeError::TooFewFragmentsPresent {
                present: next_source,
                required: DATA_FRAGMENTS,
            });
        }

        let mut survivor_matrix = Vec::with_capacity(DATA_FRAGMENTS * DATA_FRAGMENTS);
        for &source_index in &source_indexes {
            survivor_matrix.extend_from_slice(matrix_row(encode_matrix, source_index));
        }
        let inverted = keisal::invert_matrix_owned(survivor_matrix, DATA_FRAGMENTS)
            .ok_or_else(|| KeeError::Codec("failed to invert ISA-L survivor matrix".to_string()))?;

        let rows = missing.len();
        let mut decode_matrix = vec![0_u8; rows * DATA_FRAGMENTS];
        for (row, &missing_index) in missing.iter().enumerate() {
            if missing_index < DATA_FRAGMENTS {
                decode_matrix[row * DATA_FRAGMENTS..(row + 1) * DATA_FRAGMENTS]
                    .copy_from_slice(matrix_row(&inverted, missing_index));
                continue;
            }

            for column in 0..DATA_FRAGMENTS {
                let mut value = 0_u8;
                for inner in 0..DATA_FRAGMENTS {
                    value ^= keisal::gf_multiply(
                        inverted[inner * DATA_FRAGMENTS + column],
                        encode_matrix[missing_index * DATA_FRAGMENTS + inner],
                    );
                }
                decode_matrix[row * DATA_FRAGMENTS + column] = value;
            }
        }

        let mut missing_indexes = [INVALID_INDEX; PARITY_FRAGMENTS];
        for (slot, &missing_index) in missing.iter().enumerate() {
            missing_indexes[slot] = missing_index;
        }

        Ok(PreparedDecodePlan {
            rows,
            missing_indexes,
            source_indexes,
            decode_tables: keisal::init_tables(DATA_FRAGMENTS, rows, &decode_matrix),
        })
    }

    fn matrix_row(matrix: &[u8], row: usize) -> &[u8] {
        let start = row * DATA_FRAGMENTS;
        &matrix[start..start + DATA_FRAGMENTS]
    }

    fn erasure_key(missing: &[usize]) -> u16 {
        let mut key = 0_u16;
        for &index in missing {
            key |= 1_u16 << index;
        }
        key
    }

    fn prepare_shards(
        profile: &EcProfile,
        object: &[u8],
        shards: &mut [Vec<u8>],
    ) -> Result<(), KeeError> {
        profile.validate_single_stripe()?;
        if object.len() > profile.max_object_bytes() {
            return Err(KeeError::PayloadTooLarge {
                actual: object.len(),
                maximum: profile.max_object_bytes(),
            });
        }
        if shards.len() != TOTAL_FRAGMENTS {
            return Err(KeeError::FragmentCountMismatch {
                expected: TOTAL_FRAGMENTS,
                actual: shards.len(),
            });
        }
        for shard in shards.iter_mut() {
            if shard.len() != profile.fragment_bytes {
                shard.resize(profile.fragment_bytes, 0);
            }
            shard.fill(0);
        }
        for (index, chunk) in object.chunks(profile.fragment_bytes).enumerate() {
            shards[index][..chunk.len()].copy_from_slice(chunk);
        }
        Ok(())
    }

    fn validate_fragment_slots(
        profile: &EcProfile,
        fragments: &[Option<Vec<u8>>],
    ) -> Result<(), KeeError> {
        profile.validate_single_stripe()?;
        if fragments.len() != TOTAL_FRAGMENTS {
            return Err(KeeError::FragmentCountMismatch {
                expected: TOTAL_FRAGMENTS,
                actual: fragments.len(),
            });
        }

        let missing = fragments
            .iter()
            .filter(|fragment| fragment.is_none())
            .count();
        if missing > PARITY_FRAGMENTS {
            return Err(KeeError::TooManyFragmentsMissing {
                missing,
                allowed: PARITY_FRAGMENTS,
            });
        }
        let present = fragments
            .iter()
            .filter(|fragment| fragment.is_some())
            .count();
        if present < DATA_FRAGMENTS {
            return Err(KeeError::TooFewFragmentsPresent {
                present,
                required: DATA_FRAGMENTS,
            });
        }
        for (index, fragment) in fragments.iter().enumerate() {
            if let Some(bytes) = fragment {
                if bytes.len() != profile.fragment_bytes {
                    return Err(KeeError::FragmentSizeMismatch {
                        index,
                        expected: profile.fragment_bytes,
                        actual: bytes.len(),
                    });
                }
            }
        }
        Ok(())
    }

    fn collect_missing_indexes(fragments: &[Option<Vec<u8>>]) -> Vec<usize> {
        fragments
            .iter()
            .enumerate()
            .filter_map(|(index, fragment)| fragment.is_none().then_some(index))
            .collect()
    }

    fn take_all_fragments(fragments: &mut [Option<Vec<u8>>]) -> Result<Vec<Vec<u8>>, KeeError> {
        let mut out = Vec::with_capacity(TOTAL_FRAGMENTS);
        for fragment in fragments.iter_mut() {
            out.push(fragment.take().ok_or_else(|| {
                KeeError::Codec("reconstruction completed with a missing shard".to_string())
            })?);
        }
        Ok(out)
    }

    fn ec_encode_with_tables(
        len: usize,
        k: usize,
        rows: usize,
        gftbls: &[u8],
        data: &[Vec<u8>],
        parity: &mut [Vec<u8>],
    ) {
        let mut data_ptrs = [ptr::null(); DATA_FRAGMENTS];
        for (slot, fragment) in data.iter().enumerate().take(k) {
            data_ptrs[slot] = fragment.as_ptr();
        }

        let mut coding_ptrs = [ptr::null_mut(); PARITY_FRAGMENTS];
        for (slot, fragment) in parity.iter_mut().enumerate().take(rows) {
            coding_ptrs[slot] = fragment.as_mut_ptr();
        }

        keisal::encode_data(
            len,
            k,
            rows,
            gftbls,
            &data_ptrs[..k],
            &mut coding_ptrs[..rows],
        );
    }

    pub fn encode(profile: &EcProfile, object: &[u8]) -> Result<Vec<Vec<u8>>, KeeError> {
        let plan = PreparedBackendPlan::new(profile)?;
        let mut shards = vec![vec![0_u8; profile.fragment_bytes]; TOTAL_FRAGMENTS];
        plan.encode_into(object, &mut shards)?;
        Ok(shards)
    }

    pub fn reconstruct(
        profile: &EcProfile,
        fragments: &mut [Option<Vec<u8>>],
    ) -> Result<Vec<Vec<u8>>, KeeError> {
        PreparedBackendPlan::new(profile)?.reconstruct(fragments)
    }
}

#[cfg(not(feature = "isa-l-backend"))]
mod imp {
    use super::*;
    use reed_solomon_erasure::galois_8::ReedSolomon;

    #[derive(Debug, Clone)]
    pub struct PreparedBackendPlan {
        profile: EcProfile,
        codec: ReedSolomon,
    }

    impl PreparedBackendPlan {
        pub fn new(profile: &EcProfile) -> Result<Self, KeeError> {
            profile.validate_single_stripe()?;
            Ok(Self {
                profile: profile.clone(),
                codec: ReedSolomon::new(
                    crate::profile::DATA_FRAGMENTS,
                    crate::profile::PARITY_FRAGMENTS,
                )?,
            })
        }

        pub fn encode_into(&self, object: &[u8], shards: &mut [Vec<u8>]) -> Result<(), KeeError> {
            crate::software::encode_into_with_codec(&self.profile, object, shards, &self.codec)
        }

        pub fn reconstruct(
            &self,
            fragments: &mut [Option<Vec<u8>>],
        ) -> Result<Vec<Vec<u8>>, KeeError> {
            crate::software::reconstruct(&self.profile, fragments)
        }
    }

    pub fn encode(profile: &EcProfile, object: &[u8]) -> Result<Vec<Vec<u8>>, KeeError> {
        crate::software::encode(profile, object)
    }

    pub fn reconstruct(
        profile: &EcProfile,
        fragments: &mut [Option<Vec<u8>>],
    ) -> Result<Vec<Vec<u8>>, KeeError> {
        crate::software::reconstruct(profile, fragments)
    }
}

#[allow(unused_imports)]
pub use imp::{encode, reconstruct, PreparedBackendPlan};
