// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::error::KeeError;
use crate::profile::EcProfile;
use reed_solomon_erasure::galois_8::ReedSolomon;

pub fn encode(profile: &EcProfile, object: &[u8]) -> Result<Vec<Vec<u8>>, KeeError> {
    let total = profile.data_fragments + profile.parity_fragments;
    let mut shards = vec![vec![0_u8; profile.fragment_bytes]; total];
    encode_into(profile, object, &mut shards)?;
    Ok(shards)
}

pub fn encode_into(
    profile: &EcProfile,
    object: &[u8],
    shards: &mut [Vec<u8>],
) -> Result<(), KeeError> {
    let codec = ReedSolomon::new(profile.data_fragments, profile.parity_fragments)?;
    encode_into_with_codec(profile, object, shards, &codec)
}

pub(crate) fn encode_into_with_codec(
    profile: &EcProfile,
    object: &[u8],
    shards: &mut [Vec<u8>],
    codec: &ReedSolomon,
) -> Result<(), KeeError> {
    profile.validate_single_stripe()?;
    if object.len() > profile.max_object_bytes() {
        return Err(KeeError::PayloadTooLarge {
            actual: object.len(),
            maximum: profile.max_object_bytes(),
        });
    }
    let total = profile.data_fragments + profile.parity_fragments;
    if shards.len() != total {
        return Err(KeeError::FragmentCountMismatch {
            expected: total,
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
    // The software codec writes parity in place and leaves the data shards as-is.
    codec.encode(shards)?;
    Ok(())
}

pub fn reconstruct(
    profile: &EcProfile,
    fragments: &mut [Option<Vec<u8>>],
) -> Result<Vec<Vec<u8>>, KeeError> {
    profile.validate_single_stripe()?;
    let total = profile.data_fragments + profile.parity_fragments;
    if fragments.len() != total {
        return Err(KeeError::FragmentCountMismatch {
            expected: total,
            actual: fragments.len(),
        });
    }

    let missing = fragments
        .iter()
        .filter(|fragment| fragment.is_none())
        .count();
    if missing > profile.parity_fragments {
        return Err(KeeError::TooManyFragmentsMissing {
            missing,
            allowed: profile.parity_fragments,
        });
    }

    let present = fragments
        .iter()
        .filter(|fragment| fragment.is_some())
        .count();
    if present < profile.data_fragments {
        return Err(KeeError::TooFewFragmentsPresent {
            present,
            required: profile.data_fragments,
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

    let mut shards = fragments.to_vec();
    let codec = ReedSolomon::new(profile.data_fragments, profile.parity_fragments)?;
    codec.reconstruct(&mut shards)?;
    let mut out = Vec::with_capacity(total);
    for shard in shards {
        out.push(shard.ok_or_else(|| {
            KeeError::Codec("reconstruction completed with a missing shard".to_string())
        })?);
    }
    Ok(out)
}
