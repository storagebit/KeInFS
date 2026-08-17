// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildInfo {
    pub package_name: String,
    pub binary_name: String,
    pub version: String,
    pub release: u64,
    pub git_sha: String,
    pub git_dirty: bool,
    pub built_at_unix_s: u64,
    pub build_profile: String,
    pub target_triple: String,
}

#[macro_export]
macro_rules! build_info {
    () => {{
        $crate::BuildInfo {
            package_name: format!("keinfs-{}", env!("CARGO_PKG_NAME")),
            binary_name: option_env!("CARGO_BIN_NAME")
                .unwrap_or(env!("CARGO_PKG_NAME"))
                .to_string(),
            version: option_env!("KEINFS_BUILD_VERSION")
                .unwrap_or(env!("CARGO_PKG_VERSION"))
                .to_string(),
            release: option_env!("KEINFS_BUILD_RELEASE")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0),
            git_sha: option_env!("KEINFS_BUILD_GIT_SHA")
                .unwrap_or("unknown")
                .to_string(),
            git_dirty: option_env!("KEINFS_BUILD_GIT_DIRTY")
                .map(|value| matches!(value, "1" | "true" | "yes"))
                .unwrap_or(false),
            built_at_unix_s: option_env!("KEINFS_BUILD_UNIX_TIME")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0),
            build_profile: option_env!("KEINFS_BUILD_PROFILE")
                .unwrap_or("unknown")
                .to_string(),
            target_triple: option_env!("KEINFS_BUILD_TARGET")
                .unwrap_or("unknown")
                .to_string(),
        }
    }};
}

#[derive(Debug, Deserialize)]
struct BuildToml {
    keinfs: KeinfsBuildSection,
}

#[derive(Debug, Deserialize)]
struct KeinfsBuildSection {
    version: String,
    release: u64,
}

pub fn emit_build_env(manifest_relative_path: &str) -> Result<(), Box<dyn Error>> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let manifest_path = Path::new(&manifest_dir).join(manifest_relative_path);
    let build = load_build_toml(&manifest_path)?;

    println!("cargo:rerun-if-changed={}", manifest_path.display());
    for git_path in git_rerun_paths(&manifest_dir)? {
        println!("cargo:rerun-if-changed={}", git_path.display());
    }

    println!(
        "cargo:rustc-env=KEINFS_BUILD_VERSION={}",
        build.keinfs.version
    );
    println!(
        "cargo:rustc-env=KEINFS_BUILD_RELEASE={}",
        build.keinfs.release
    );
    println!(
        "cargo:rustc-env=KEINFS_BUILD_GIT_SHA={}",
        git_sha(&manifest_dir)
    );
    println!(
        "cargo:rustc-env=KEINFS_BUILD_GIT_DIRTY={}",
        if git_dirty(&manifest_dir) { "1" } else { "0" }
    );
    println!(
        "cargo:rustc-env=KEINFS_BUILD_UNIX_TIME={}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    );
    println!(
        "cargo:rustc-env=KEINFS_BUILD_PROFILE={}",
        std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string())
    );
    println!(
        "cargo:rustc-env=KEINFS_BUILD_TARGET={}",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string())
    );
    Ok(())
}

pub fn config_hash_hex(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub fn hostname_or_unknown() -> String {
    if let Ok(value) = std::env::var("HOSTNAME") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    for path in ["/etc/hostname", "/proc/sys/kernel/hostname"] {
        if let Ok(raw) = fs::read_to_string(path) {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }
    "unknown".to_string()
}

fn load_build_toml(path: &Path) -> Result<BuildToml, Box<dyn Error>> {
    let raw = fs::read_to_string(path)?;
    Ok(toml::from_str(&raw)?)
}

fn git_sha(manifest_dir: &str) -> String {
    git_output(manifest_dir, &["rev-parse", "--short=12", "HEAD"])
        .unwrap_or_else(|| "unknown".to_string())
}

fn git_dirty(manifest_dir: &str) -> bool {
    git_output(manifest_dir, &["status", "--porcelain"])
        .map(|value| !value.trim().is_empty())
        .unwrap_or(true)
}

fn git_output(manifest_dir: &str, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(manifest_dir)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    Some(text.trim().to_string())
}

fn git_rerun_paths(manifest_dir: &str) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let git_dir_text =
        git_output(manifest_dir, &["rev-parse", "--git-dir"]).unwrap_or_else(|| ".git".to_string());
    let git_dir = if Path::new(&git_dir_text).is_absolute() {
        PathBuf::from(git_dir_text)
    } else {
        Path::new(manifest_dir).join(git_dir_text)
    };
    let mut paths = vec![git_dir.join("HEAD"), git_dir.join("index")];
    if let Ok(head) = fs::read_to_string(git_dir.join("HEAD")) {
        if let Some(reference) = head.strip_prefix("ref:") {
            paths.push(git_dir.join(reference.trim()));
        }
    }
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::config_hash_hex;

    #[test]
    fn config_hash_is_stable() {
        assert_eq!(
            config_hash_hex("listen_addr=\"127.0.0.1:1\""),
            config_hash_hex("listen_addr=\"127.0.0.1:1\"")
        );
    }
}
