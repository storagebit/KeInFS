// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeeError {
    InvalidProfile(String),
    PayloadTooLarge {
        actual: usize,
        maximum: usize,
    },
    FragmentCountMismatch {
        expected: usize,
        actual: usize,
    },
    FragmentSizeMismatch {
        index: usize,
        expected: usize,
        actual: usize,
    },
    TooManyFragmentsMissing {
        missing: usize,
        allowed: usize,
    },
    TooFewFragmentsPresent {
        present: usize,
        required: usize,
    },
    Codec(String),
}

impl Display for KeeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidProfile(message) => write!(f, "invalid EC profile: {message}"),
            Self::PayloadTooLarge { actual, maximum } => write!(
                f,
                "payload is {actual} bytes but the profile allows at most {maximum} bytes"
            ),
            Self::FragmentCountMismatch { expected, actual } => write!(
                f,
                "fragment count mismatch: expected {expected}, got {actual}"
            ),
            Self::FragmentSizeMismatch {
                index,
                expected,
                actual,
            } => write!(
                f,
                "fragment {index} has size {actual} bytes but expected {expected} bytes"
            ),
            Self::TooManyFragmentsMissing { missing, allowed } => write!(
                f,
                "{missing} fragments are missing but only {allowed} may be absent"
            ),
            Self::TooFewFragmentsPresent { present, required } => write!(
                f,
                "{present} fragments are present but at least {required} are required"
            ),
            Self::Codec(message) => write!(f, "codec error: {message}"),
        }
    }
}

impl Error for KeeError {}

impl From<reed_solomon_erasure::Error> for KeeError {
    fn from(value: reed_solomon_erasure::Error) -> Self {
        Self::Codec(value.to_string())
    }
}
