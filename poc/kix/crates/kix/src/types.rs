// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use std::fmt;

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct ChunkId(pub [u8; 32]);

impl ChunkId {
    pub fn from_seed(seed: u64) -> Self {
        let mut x = seed;
        let mut out = [0_u8; 32];
        for slot in out.chunks_exact_mut(8) {
            x = splitmix64(x);
            slot.copy_from_slice(&x.to_le_bytes());
        }
        Self(out)
    }
}

impl fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkId(")?;
        for byte in &self.0[..8] {
            write!(f, "{byte:02x}")?;
        }
        write!(f, "..)")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum LocationKind {
    Extent = 1,
    PackedContainer = 2,
}

impl LocationKind {
    pub fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Extent),
            2 => Some(Self::PackedContainer),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocationRecord {
    pub drive_id: u16,
    pub location_kind: LocationKind,
    pub physical_offset: u64,
    pub logical_length: u32,
    pub stored_length: u32,
    pub generation: u32,
    pub checksum: u32,
}

impl LocationRecord {
    pub const ENCODED_LEN: usize = 28;

    pub fn extent(
        drive_id: u16,
        physical_offset: u64,
        logical_length: u32,
        stored_length: u32,
        generation: u32,
        checksum: u32,
    ) -> Self {
        Self {
            drive_id,
            location_kind: LocationKind::Extent,
            physical_offset,
            logical_length,
            stored_length,
            generation,
            checksum,
        }
    }

    pub fn packed(
        drive_id: u16,
        physical_offset: u64,
        logical_length: u32,
        stored_length: u32,
        generation: u32,
        checksum: u32,
    ) -> Self {
        Self {
            drive_id,
            location_kind: LocationKind::PackedContainer,
            physical_offset,
            logical_length,
            stored_length,
            generation,
            checksum,
        }
    }

    pub fn encode(self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0_u8; Self::ENCODED_LEN];
        out[0..2].copy_from_slice(&self.drive_id.to_le_bytes());
        out[2] = self.location_kind as u8;
        out[3] = 0;
        out[4..12].copy_from_slice(&self.physical_offset.to_le_bytes());
        out[12..16].copy_from_slice(&self.logical_length.to_le_bytes());
        out[16..20].copy_from_slice(&self.stored_length.to_le_bytes());
        out[20..24].copy_from_slice(&self.generation.to_le_bytes());
        out[24..28].copy_from_slice(&self.checksum.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::ENCODED_LEN {
            return None;
        }
        let drive_id = u16::from_le_bytes(bytes[0..2].try_into().ok()?);
        let location_kind = LocationKind::from_byte(bytes[2])?;
        let physical_offset = u64::from_le_bytes(bytes[4..12].try_into().ok()?);
        let logical_length = u32::from_le_bytes(bytes[12..16].try_into().ok()?);
        let stored_length = u32::from_le_bytes(bytes[16..20].try_into().ok()?);
        let generation = u32::from_le_bytes(bytes[20..24].try_into().ok()?);
        let checksum = u32::from_le_bytes(bytes[24..28].try_into().ok()?);
        Some(Self {
            drive_id,
            location_kind,
            physical_offset,
            logical_length,
            stored_length,
            generation,
            checksum,
        })
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}
