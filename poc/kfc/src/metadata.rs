// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! Re-export shim.
//!
//! The full KMS metadata client moved into `kfc-core` (KFC v2). The `kfc`
//! binary only needs the shared boxed-error type + constructor that `bench` and
//! `main` already reference as `crate::metadata::{DynError, boxed_error}`.

pub(crate) use kfc_core::{boxed_error, DynError};
