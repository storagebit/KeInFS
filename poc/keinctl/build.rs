// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

fn main() -> Result<(), Box<dyn std::error::Error>> {
    keinbuild::emit_build_env("../../BUILD.toml")?;
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        // Records persisted in FDB as serde_json (e.g. NamespaceDomainEntry) must
        // tolerate fields added after they were written. Without this, a newly
        // added non-Option scalar (size_bytes) makes serde_json fail on any record
        // written before the field existed ("missing field"). Applying serde(default)
        // at the container level lets every proto-as-JSON record decode old data,
        // defaulting absent fields (size_bytes -> 0) instead of erroring.
        .field_attribute(
            "keinfs.control.NamespaceDomainEntry.size_bytes",
            "#[serde(default)]",
        )
        .compile_protos(&["proto/control.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/control.proto");
    Ok(())
}
