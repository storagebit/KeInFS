// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit
//
// Runtime-tree discovery. Each KeInFS service publishes its snapshot under
// `<runtime_root>/<service>/<instance-id>/summary`. The exporter walks the
// configured roots, finds the freshest instance dir per service (restarts leave
// stale `<id>-<oldpid>` dirs behind — we pick the most recently modified), and
// reads the snapshot. KST additionally has a `rpcs/` subdir of per-RPC files.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// A discovered service instance to scrape.
pub struct Instance {
    pub service: String,
    pub instance_id: String,
    pub summary_path: PathBuf,
    /// KST per-RPC phase files live in `<dir>/rpcs/<name>`.
    pub rpcs_dir: Option<PathBuf>,
    pub is_json: bool,
}

/// The standard service subdirs under a runtime root. KAS shards land in
/// `kas-NN`, so we also match any `kas*` dir.
const SERVICES: &[&str] = &["kms", "kas", "kst", "krs", "kix", "ksc", "kfc"];

/// Walk every configured root and return the freshest instance dir per service.
pub fn discover(roots: &[PathBuf]) -> Vec<Instance> {
    let mut out = Vec::new();
    for root in roots {
        let entries = match fs::read_dir(root) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for svc_entry in entries.flatten() {
            let svc_path = svc_entry.path();
            if !svc_path.is_dir() {
                continue;
            }
            let svc_dir = svc_entry.file_name().to_string_lossy().to_string();
            let service = classify_service(&svc_dir);
            let Some(service) = service else { continue };

            // A service dir holds one OR MANY distinct instances (e.g. the KST
            // dir contains all 12 targets: epyc-target-00, -01, ...). Each
            // instance may also have stale `<id>-<oldpid>` dirs from restarts.
            // Group dirs by their stable prefix (id minus the trailing -<pid>)
            // and keep the freshest dir per group, so we emit EVERY live
            // instance but skip post-restart duplicates of the same one.
            out.extend(freshest_instances(&svc_path, &service));
        }
    }
    out
}

fn classify_service(dir: &str) -> Option<String> {
    // `kas-00`, `kas-01` -> "kas"; otherwise an exact service match.
    if let Some(base) = dir.split('-').next() {
        if SERVICES.contains(&base) {
            return Some(base.to_string());
        }
    }
    if SERVICES.contains(&dir) {
        return Some(dir.to_string());
    }
    None
}

/// Return one `Instance` per distinct stable-prefix group under a service dir,
/// each being the most-recently-modified dir in its group (the live process).
fn freshest_instances(svc_path: &Path, service: &str) -> Vec<Instance> {
    // prefix -> (mtime, summary_path, instance_id)
    let mut groups: HashMap<String, (SystemTime, PathBuf, String)> = HashMap::new();
    let entries = match fs::read_dir(svc_path) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    for inst_entry in entries.flatten() {
        let inst_path = inst_entry.path();
        if !inst_path.is_dir() {
            continue;
        }
        let summary = inst_path.join("summary");
        let meta = match fs::metadata(&summary) {
            Ok(m) => m,
            Err(_) => continue,
        };
        let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let instance_id = inst_entry.file_name().to_string_lossy().to_string();
        let prefix = stable_prefix(&instance_id);
        match groups.get(&prefix) {
            Some((best_mtime, _, _)) if *best_mtime >= mtime => {}
            _ => {
                groups.insert(prefix, (mtime, summary, instance_id));
            }
        }
    }

    let is_json = service != "kst";
    groups
        .into_values()
        .map(|(_, summary_path, instance_id)| {
            let rpcs_dir = summary_path
                .parent()
                .map(|p| p.join("rpcs"))
                .filter(|d| d.is_dir());
            Instance {
                service: service.to_string(),
                instance_id,
                summary_path,
                rpcs_dir,
                is_json,
            }
        })
        .collect()
}

/// Strip a trailing `-<digits>` (the pid) so all restart dirs of one logical
/// instance share a prefix. `epyc-target-00-2512955` -> `epyc-target-00`;
/// `kms-kms-shard-0001-57748` -> `kms-kms-shard-0001`. A dir with no numeric
/// suffix groups under its full name.
fn stable_prefix(instance_id: &str) -> String {
    match instance_id.rsplit_once('-') {
        Some((head, tail)) if !tail.is_empty() && tail.chars().all(|c| c.is_ascii_digit()) => {
            head.to_string()
        }
        _ => instance_id.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn stable_prefix_strips_pid_suffix() {
        assert_eq!(stable_prefix("epyc-target-00-2512955"), "epyc-target-00");
        assert_eq!(stable_prefix("kms-kms-shard-0001-57748"), "kms-kms-shard-0001");
        assert_eq!(stable_prefix("kas-57481"), "kas");
        // no numeric suffix -> unchanged
        assert_eq!(stable_prefix("kfc"), "kfc");
    }

    #[test]
    fn discovers_all_distinct_instances_and_skips_restart_dupes() {
        // Regression: all 12 KST targets live as siblings under one `kst/` dir;
        // earlier logic collapsed them to a single instance. Also verify a
        // stale restart dir of the SAME target is skipped in favor of the
        // freshest.
        let tmp = std::env::temp_dir().join(format!("keinexport-disc-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        let kst = tmp.join("kst");
        for target in ["epyc-target-00", "epyc-target-01", "epyc-target-02"] {
            for pid in ["1000", "2000"] {
                let dir = kst.join(format!("{target}-{pid}"));
                fs::create_dir_all(&dir).unwrap();
                fs::write(dir.join("summary"), format!("target_id={target}\n")).unwrap();
            }
        }
        let found = discover(&[tmp.clone()]);
        let _ = fs::remove_dir_all(&tmp);
        // 3 distinct targets, NOT 6 (restart dupes collapsed), NOT 1.
        assert_eq!(found.len(), 3, "expected 3 distinct KST instances");
        assert!(found.iter().all(|i| i.service == "kst"));
    }
}

/// Read the KST per-RPC phase files in `rpcs/` as (rpc_name, text) pairs.
pub fn read_rpc_files(rpcs_dir: &Path) -> Vec<(String, String)> {
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(rpcs_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Ok(text) = fs::read_to_string(&path) {
                    let name = entry.file_name().to_string_lossy().to_string();
                    files.push((name, text));
                }
            }
        }
    }
    files
}
