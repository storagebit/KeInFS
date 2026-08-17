// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! Transport-neutral filesystem error codes.
//!
//! The core never returns `fuser::Errno` — it returns [`FsErrno`], which each
//! transport maps to its backend's error type at the boundary. This keeps the
//! errno vocabulary identical across the `/dev/fuse`, io_uring, and FUSE-T
//! backends.

use std::error::Error;

/// The subset of POSIX errno values the KeInFS FUSE client produces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsErrno {
    /// ENOENT — no such file or directory.
    NoEntry,
    /// ENOTDIR — a path component is not a directory.
    NotDir,
    /// EISDIR — operation invalid on a directory.
    IsDir,
    /// EEXIST — already exists.
    Exists,
    /// ENOTEMPTY — directory not empty.
    NotEmpty,
    /// EBADF — bad file handle.
    BadHandle,
    /// EPERM — operation not permitted.
    Perm,
    /// ENOSYS — not implemented.
    NoSys,
    /// EAGAIN — try again (rate limited / would block).
    Again,
    /// EFBIG — file too big.
    TooBig,
    /// ENAMETOOLONG — name too long.
    NameTooLong,
    /// EINVAL — invalid argument.
    Inval,
    /// EIO — catch-all backend failure.
    Io,
}

impl FsErrno {
    /// Map to the raw libc errno integer. The transport uses this to build its
    /// backend error type.
    pub fn raw(self) -> i32 {
        match self {
            FsErrno::NoEntry => libc::ENOENT,
            FsErrno::NotDir => libc::ENOTDIR,
            FsErrno::IsDir => libc::EISDIR,
            FsErrno::Exists => libc::EEXIST,
            FsErrno::NotEmpty => libc::ENOTEMPTY,
            FsErrno::BadHandle => libc::EBADF,
            FsErrno::Perm => libc::EPERM,
            FsErrno::NoSys => libc::ENOSYS,
            FsErrno::Again => libc::EAGAIN,
            FsErrno::TooBig => libc::EFBIG,
            FsErrno::NameTooLong => libc::ENAMETOOLONG,
            FsErrno::Inval => libc::EINVAL,
            FsErrno::Io => libc::EIO,
        }
    }
}

/// Classify a backend error message into an [`FsErrno`]. KSC/KMS return string
/// errors today; until they carry structured codes this mirrors the heuristic
/// the original `kfc` used (`mount.rs:1342`) so behavior is unchanged.
pub fn classify(err: &(dyn Error + Send + Sync)) -> FsErrno {
    classify_message(&err.to_string())
}

/// Classify from a raw message string (testable without an Error object).
pub fn classify_message(message: &str) -> FsErrno {
    let m = message.to_ascii_lowercase();
    if m.contains("not found") || m.contains("no manifest") || m.contains("enoent") {
        FsErrno::NoEntry
    } else if m.contains("not empty") {
        FsErrno::NotEmpty
    } else if m.contains("not implemented") || m.contains("not supported") {
        FsErrno::NoSys
    } else if m.contains("rate limit") || m.contains("too many requests") || m.contains("429") {
        FsErrno::Again
    } else {
        FsErrno::Io
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_maps_known_phrases() {
        assert_eq!(classify_message("object not found"), FsErrno::NoEntry);
        assert_eq!(classify_message("KMS has no manifest"), FsErrno::NoEntry);
        assert_eq!(classify_message("directory not empty"), FsErrno::NotEmpty);
        assert_eq!(classify_message("RPC not implemented"), FsErrno::NoSys);
        assert_eq!(classify_message("HTTP 429 rate limit"), FsErrno::Again);
        assert_eq!(classify_message("connection reset"), FsErrno::Io);
    }

    #[test]
    fn raw_errno_roundtrips_to_libc() {
        assert_eq!(FsErrno::NoEntry.raw(), libc::ENOENT);
        assert_eq!(FsErrno::Io.raw(), libc::EIO);
        assert_eq!(FsErrno::NoSys.raw(), libc::ENOSYS);
    }
}
