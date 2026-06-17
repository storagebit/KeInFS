// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! Canonical same-device drive layout.
//!
//! Splits a single raw block device into a chunk-media region at the front and
//! a KIX arena at the aligned tail, sized for maximum usable media capacity.
//! This is the one place the split is defined: the `kix format` tool formats
//! the arena here, and the KST serve path derives an identical layout, so the
//! offline format tool and the runtime never disagree about where the arena
//! lives.

use std::io;

/// Minimum bytes reserved for the chunk-media span.
const AUTO_KIX_MIN_MEDIA_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Upper bound on the KIX arena span.
const AUTO_KIX_ARENA_MAX_BYTES: u64 = 256 * 1024 * 1024 * 1024;
/// Lower bound on the KIX arena span.
const AUTO_KIX_ARENA_MIN_BYTES: u64 = 16 * 1024 * 1024 * 1024;
/// Default target arena span is roughly `device / 50`.
const AUTO_KIX_ARENA_FRACTION_DIVISOR: u64 = 50;
/// All derived offsets and slices are aligned down to this boundary.
pub const LAYOUT_ALIGNMENT_BYTES: u64 = 4096;

/// A resolved same-device split: chunk media at `[0, media_slice_bytes)` and
/// the KIX arena at `[raw_offset_bytes, raw_offset_bytes + raw_slice_bytes)`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutoDriveLayout {
    pub raw_offset_bytes: u64,
    pub raw_slice_bytes: u64,
    pub media_offset_bytes: u64,
    pub media_slice_bytes: u64,
}

fn align_down(value: u64, alignment: u64) -> u64 {
    value - (value % alignment)
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg.into())
}

fn auto_kix_arena_slice_bytes(device_bytes: u64) -> io::Result<u64> {
    let proportional = align_down(
        device_bytes / AUTO_KIX_ARENA_FRACTION_DIVISOR,
        LAYOUT_ALIGNMENT_BYTES,
    );
    let max_allowed = align_down(
        device_bytes
            .checked_sub(AUTO_KIX_MIN_MEDIA_BYTES)
            .ok_or_else(|| invalid("auto-layout max arena underflow"))?,
        LAYOUT_ALIGNMENT_BYTES,
    );
    if max_allowed == 0 {
        return Err(invalid(
            "auto-layout could not reserve any aligned bytes for the KIX arena",
        ));
    }
    let desired = proportional
        .max(AUTO_KIX_ARENA_MIN_BYTES)
        .min(AUTO_KIX_ARENA_MAX_BYTES)
        .min(max_allowed);
    if desired == 0 {
        return Err(invalid("auto-layout derived a zero-byte KIX arena"));
    }
    Ok(desired)
}

/// Derive the same-device media+arena split from the device size alone.
///
/// `device_bytes` must be aligned to [`LAYOUT_ALIGNMENT_BYTES`]. The arena is
/// placed at the aligned tail; everything before it is chunk media.
pub fn auto_drive_layout(device_bytes: u64) -> io::Result<AutoDriveLayout> {
    if device_bytes % LAYOUT_ALIGNMENT_BYTES != 0 {
        return Err(invalid(format!(
            "device size {device_bytes} is not aligned to {LAYOUT_ALIGNMENT_BYTES} bytes"
        )));
    }
    if device_bytes <= AUTO_KIX_MIN_MEDIA_BYTES + LAYOUT_ALIGNMENT_BYTES {
        return Err(invalid(format!(
            "auto-layout needs more than {} bytes to leave room for both chunk media and a KIX arena; got {}",
            AUTO_KIX_MIN_MEDIA_BYTES + LAYOUT_ALIGNMENT_BYTES,
            device_bytes,
        )));
    }
    let raw_slice_bytes = auto_kix_arena_slice_bytes(device_bytes)?;
    let media_slice_bytes = align_down(
        device_bytes
            .checked_sub(raw_slice_bytes)
            .ok_or_else(|| invalid("auto-layout media underflow"))?,
        LAYOUT_ALIGNMENT_BYTES,
    );
    if media_slice_bytes == 0 {
        return Err(invalid(
            "auto-layout produced a zero-byte chunk-media span",
        ));
    }
    if media_slice_bytes < AUTO_KIX_MIN_MEDIA_BYTES {
        return Err(invalid(format!(
            "auto-layout only left {media_slice_bytes} bytes for chunk media; need at least {AUTO_KIX_MIN_MEDIA_BYTES}"
        )));
    }
    let raw_offset_bytes = device_bytes
        .checked_sub(raw_slice_bytes)
        .ok_or_else(|| invalid("auto-layout raw offset underflow"))?;
    Ok(AutoDriveLayout {
        raw_offset_bytes,
        raw_slice_bytes,
        media_offset_bytes: 0,
        media_slice_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn places_media_first_and_arena_at_tail() {
        // 6.2 TiB DenseIO E5 NVMe used in the epyc/mithril lab.
        let dev = 6_801_330_364_416;
        let layout = auto_drive_layout(dev).unwrap();
        assert_eq!(layout.media_offset_bytes, 0);
        assert_eq!(layout.raw_offset_bytes, layout.media_slice_bytes);
        assert_eq!(layout.raw_offset_bytes + layout.raw_slice_bytes, dev);
        assert!(layout.raw_slice_bytes >= AUTO_KIX_ARENA_MIN_BYTES);
        assert!(layout.raw_slice_bytes <= AUTO_KIX_ARENA_MAX_BYTES);
        // device/50 aligned down -> 136 GiB-ish arena, ~6.66 TB media.
        assert_eq!(layout.raw_slice_bytes, 136_026_603_520);
        assert_eq!(layout.raw_offset_bytes, 6_665_303_760_896);
    }

    #[test]
    fn clamps_arena_to_max_on_huge_devices() {
        let dev = 100 * 1024 * 1024 * 1024 * 1024; // 100 TiB
        let layout = auto_drive_layout(dev).unwrap();
        assert_eq!(layout.raw_slice_bytes, AUTO_KIX_ARENA_MAX_BYTES);
        assert_eq!(layout.raw_offset_bytes + layout.raw_slice_bytes, dev);
    }

    #[test]
    fn rejects_tiny_devices() {
        assert!(auto_drive_layout(AUTO_KIX_MIN_MEDIA_BYTES).is_err());
    }

    #[test]
    fn rejects_unaligned_devices() {
        assert!(auto_drive_layout(6_801_330_364_417).is_err());
    }
}
