// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

fn main() -> Result<(), Box<dyn std::error::Error>> {
    keinbuild::emit_build_env("../../BUILD.toml")?;
    Ok(())
}
