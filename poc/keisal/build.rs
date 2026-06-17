// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use std::env;
use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=KEISAL_LIB_DIR");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");

    if let Ok(lib_dir) = env::var("KEISAL_LIB_DIR") {
        println!("cargo:rustc-link-search=native={lib_dir}");
        println!("cargo:rustc-link-lib=isal");
        return;
    }

    for candidate in [
        "/usr/lib64/libisal.so",
        "/usr/lib/libisal.so",
        // Debian/Ubuntu multiarch (where `apt install libisal-dev` lands it).
        "/usr/lib/x86_64-linux-gnu/libisal.so",
        "/usr/lib/aarch64-linux-gnu/libisal.so",
        "/usr/local/lib64/libisal.so",
        "/usr/local/lib/libisal.so",
        // macOS / Homebrew for non-Linux dev compile-checks.
        "/opt/homebrew/lib/libisal.dylib",
        "/opt/homebrew/opt/isa-l/lib/libisal.dylib",
    ] {
        let candidate = Path::new(candidate);
        if candidate.exists() {
            if let Some(parent) = candidate.parent() {
                println!("cargo:rustc-link-search=native={}", parent.display());
                println!("cargo:rustc-link-lib=isal");
                return;
            }
        }
    }

    for package in ["isal", "libisal"] {
        if try_pkg_config(package) {
            return;
        }
    }

    panic!(
        "keisal could not locate Intel ISA-L; set KEISAL_LIB_DIR or install pkg-config metadata for isal/libisal"
    );
}

fn try_pkg_config(package: &str) -> bool {
    let output = match Command::new("pkg-config")
        .args(["--libs", package])
        .output()
    {
        Ok(output) => output,
        Err(_) => return false,
    };
    if !output.status.success() {
        return false;
    }
    let flags = String::from_utf8_lossy(&output.stdout);
    for token in flags.split_whitespace() {
        if let Some(path) = token.strip_prefix("-L") {
            println!("cargo:rustc-link-search=native={path}");
        } else if let Some(lib) = token.strip_prefix("-l") {
            println!("cargo:rustc-link-lib={lib}");
        }
    }
    true
}
