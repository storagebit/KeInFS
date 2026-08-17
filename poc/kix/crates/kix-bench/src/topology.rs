// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::{BenchConfig, IngressPlacement};
use kix::{device_numa_node, numa_node_cpu_list, online_numa_nodes};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;

#[derive(Clone, Debug, Default)]
pub(crate) struct TopologyPlan {
    pub(crate) raw_device_numa_node: Option<i32>,
    pub(crate) owner_numa_node: Option<i32>,
    pub(crate) local_ingress_core: Option<usize>,
    pub(crate) remote_ingress_numa_node: Option<i32>,
    pub(crate) remote_ingress_core: Option<usize>,
    pub(crate) recommended_socket_cores: Vec<usize>,
}

pub(crate) fn plan_raw_device_topology(
    config: &mut BenchConfig,
) -> Result<TopologyPlan, Box<dyn Error>> {
    let Some(raw_device) = &config.raw_device else {
        if config.ingress_placement != IngressPlacement::Direct {
            return Err(format!(
                "--ingress-placement {} requires --raw-device so KIX can discover the owning NUMA domain instead of pretending",
                config.ingress_placement.as_str()
            )
            .into());
        }
        return Ok(TopologyPlan::default());
    };
    let node = match device_numa_node(raw_device)? {
        Some(node) => node,
        None => {
            let online_nodes = online_numa_nodes()?;
            if online_nodes.len() == 1 {
                let node = online_nodes[0];
                eprintln!(
                    "info: raw device {} did not report a NUMA node; falling back to the host's only online NUMA node {}",
                    raw_device.display(),
                    node
                );
                node
            } else {
                eprintln!(
                    "warning: raw device {} did not report a NUMA node and the host has multiple online NUMA nodes ({}). KIX will continue without auto-placement; set --pin-cores explicitly or teach the platform to expose device locality.",
                    raw_device.display(),
                    online_nodes
                        .iter()
                        .map(|node| node.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
                return Ok(TopologyPlan::default());
            }
        }
    };
    let local_cpus = numa_node_cpu_list(node)?;
    if local_cpus.is_empty() {
        return Ok(TopologyPlan {
            raw_device_numa_node: Some(node),
            owner_numa_node: Some(node),
            local_ingress_core: None,
            remote_ingress_numa_node: None,
            remote_ingress_core: None,
            recommended_socket_cores: Vec::new(),
        });
    }

    if config.lookup_pin_cores.is_empty() {
        config.lookup_pin_cores = local_cpus.iter().copied().take(config.shards).collect();
        eprintln!(
            "info: raw device {} reports NUMA node {}; auto-selected lookup pins {}",
            raw_device.display(),
            node,
            join_usize_csv(&config.lookup_pin_cores)
        );
    } else if config
        .lookup_pin_cores
        .iter()
        .any(|cpu| !local_cpus.contains(cpu))
    {
        eprintln!(
            "warning: raw device {} reports NUMA node {}, but explicit lookup pin set {} is not fully local to that node. Expected CPUs from {}. KIX will continue, but latency-sensitive lookup workers should match the drive's NUMA locality.",
            raw_device.display(),
            node,
            join_usize_csv(&config.lookup_pin_cores),
            join_usize_csv(&local_cpus)
        );
    }

    let remaining_after_lookups = subtract_cpus(&local_cpus, &config.lookup_pin_cores);
    if config.commit_pin_cores.is_empty() {
        if remaining_after_lookups.len() >= config.shards {
            config.commit_pin_cores = remaining_after_lookups
                .iter()
                .copied()
                .take(config.shards)
                .collect();
        } else if !remaining_after_lookups.is_empty() {
            config.commit_pin_cores = cycle_from_pool(&remaining_after_lookups, config.shards);
            eprintln!(
                "warning: raw device {} has only {} locality-matching CPUs left after assigning lookup workers. Commit workers will reuse locality-matching cores {}. This is valid, but it increases contention between publication work and hot lookups.",
                raw_device.display(),
                remaining_after_lookups.len(),
                join_usize_csv(&config.commit_pin_cores)
            );
        } else {
            config.commit_pin_cores = cycle_from_pool(&config.lookup_pin_cores, config.shards);
            eprintln!(
                "warning: raw device {} has no locality-matching CPUs left after assigning lookup workers. Commit workers will share lookup cores {}. Expect read-path contention.",
                raw_device.display(),
                join_usize_csv(&config.commit_pin_cores)
            );
        }
        eprintln!(
            "info: raw device {} reports NUMA node {}; auto-selected commit pins {}",
            raw_device.display(),
            node,
            join_usize_csv(&config.commit_pin_cores)
        );
    } else if config
        .commit_pin_cores
        .iter()
        .any(|cpu| !local_cpus.contains(cpu))
    {
        eprintln!(
            "warning: raw device {} reports NUMA node {}, but explicit commit pin set {} is not fully local to that node. Expected CPUs from {}. KIX will continue, but commit workers should stay in the drive's locality domain.",
            raw_device.display(),
            node,
            join_usize_csv(&config.commit_pin_cores),
            join_usize_csv(&local_cpus)
        );
    }

    let remaining_after_commits = subtract_cpus(&remaining_after_lookups, &config.commit_pin_cores);
    if config.drive_pin_cores.is_empty() {
        if remaining_after_commits.len() >= config.drives {
            config.drive_pin_cores = remaining_after_commits
                .iter()
                .copied()
                .take(config.drives)
                .collect();
        } else if !remaining_after_commits.is_empty() {
            config.drive_pin_cores = cycle_from_pool(&remaining_after_commits, config.drives);
            eprintln!(
                "warning: raw device {} has only {} locality-matching CPUs left after assigning lookup and commit workers. Drive appenders will reuse locality-matching cores {}. This is valid, but it increases write-path contention.",
                raw_device.display(),
                remaining_after_commits.len(),
                join_usize_csv(&config.drive_pin_cores)
            );
        } else {
            config.drive_pin_cores = cycle_from_pool(&config.commit_pin_cores, config.drives);
            eprintln!(
                "warning: raw device {} has no locality-matching CPUs left after assigning lookup and commit workers. Drive appenders will share commit cores {}. Expect write-path contention.",
                raw_device.display(),
                join_usize_csv(&config.drive_pin_cores)
            );
        }
        eprintln!(
            "info: raw device {} reports NUMA node {}; auto-selected drive pins {}",
            raw_device.display(),
            node,
            join_usize_csv(&config.drive_pin_cores)
        );
    } else if config
        .drive_pin_cores
        .iter()
        .any(|cpu| !local_cpus.contains(cpu))
    {
        eprintln!(
            "warning: raw device {} reports NUMA node {}, but explicit drive pin set {} is not fully local to that node. Expected CPUs from {}. KIX will continue, but durability workers should stay local to the drive complex.",
            raw_device.display(),
            node,
            join_usize_csv(&config.drive_pin_cores),
            join_usize_csv(&local_cpus)
        );
    }

    if shares_any_cpu(&config.lookup_pin_cores, &config.commit_pin_cores) {
        eprintln!(
            "warning: lookup pins {} and commit pins {} overlap. This deliberately measures contention inside a shard locality domain, not ideal steady-state placement.",
            join_usize_csv(&config.lookup_pin_cores),
            join_usize_csv(&config.commit_pin_cores)
        );
    }

    if shares_any_cpu(&config.lookup_pin_cores, &config.drive_pin_cores) {
        eprintln!(
            "warning: lookup pins {} and drive pins {} overlap. This deliberately measures core contention, not ideal steady-state placement.",
            join_usize_csv(&config.lookup_pin_cores),
            join_usize_csv(&config.drive_pin_cores)
        );
    }

    if shares_any_cpu(&config.commit_pin_cores, &config.drive_pin_cores) {
        eprintln!(
            "warning: commit pins {} and drive pins {} overlap. This deliberately measures durability-path contention, not ideal steady-state placement.",
            join_usize_csv(&config.commit_pin_cores),
            join_usize_csv(&config.drive_pin_cores)
        );
    }

    let cpu_to_node = build_cpu_to_numa_map()?;
    let owner_numa_node =
        derive_owner_numa_node(raw_device, node, config, &cpu_to_node)?.or(Some(node));
    let owner_cpu_pool = match owner_numa_node {
        Some(owner_node) if owner_node != node => numa_node_cpu_list(owner_node)?,
        _ => local_cpus.clone(),
    };
    let used_owner_cpus = config
        .lookup_pin_cores
        .iter()
        .chain(config.commit_pin_cores.iter())
        .chain(config.drive_pin_cores.iter())
        .copied()
        .collect::<Vec<_>>();
    let remaining_after_all = subtract_cpus(&owner_cpu_pool, &used_owner_cpus);
    let recommended_socket_cores = remaining_after_all
        .iter()
        .copied()
        .take(config.reserve_socket_cores)
        .collect::<Vec<_>>();
    if !recommended_socket_cores.is_empty() {
        eprintln!(
            "info: raw device {} reports NUMA node {}; reserved NIC/socket-local cores {} for future RSS/socket pollers",
            raw_device.display(),
            node,
            join_usize_csv(&recommended_socket_cores)
        );
    }

    let local_ingress_core = recommended_socket_cores
        .first()
        .copied()
        .or_else(|| remaining_after_all.first().copied())
        .or_else(|| config.lookup_pin_cores.first().copied());
    if let Some(core_id) = local_ingress_core {
        if config.lookup_pin_cores.contains(&core_id)
            || config.commit_pin_cores.contains(&core_id)
            || config.drive_pin_cores.contains(&core_id)
        {
            eprintln!(
                "warning: KIX selected CPU core {} as the local ingress core for NUMA node {}, but that core is already used by lookup, commit, or drive workers. This is valid for contention testing, not ideal steady-state placement.",
                core_id,
                owner_numa_node.unwrap_or(node)
            );
        } else {
            eprintln!(
                "info: raw device {} reports NUMA node {}; selected CPU core {} as the local ingress core",
                raw_device.display(),
                owner_numa_node.unwrap_or(node),
                core_id
            );
        }
    }

    let (remote_ingress_numa_node, remote_ingress_core) =
        select_remote_ingress(raw_device, owner_numa_node.unwrap_or(node), config)?;
    if matches!(
        config.ingress_placement,
        IngressPlacement::Remote | IngressPlacement::Handoff
    ) && remote_ingress_core.is_none()
    {
        return Err(format!(
            "raw device {} reports NUMA node {}, but no alternate NUMA node is available for ingress placement {}. Use --ingress-placement direct|local or provide a multi-NUMA host.",
            raw_device.display(),
            node,
            config.ingress_placement.as_str()
        )
        .into());
    }
    validate_ingress_placement(
        raw_device,
        config.ingress_placement,
        owner_numa_node,
        local_ingress_core,
        remote_ingress_core,
        &cpu_to_node,
    )?;

    Ok(TopologyPlan {
        raw_device_numa_node: Some(node),
        owner_numa_node,
        local_ingress_core,
        remote_ingress_numa_node,
        remote_ingress_core,
        recommended_socket_cores,
    })
}

pub(crate) fn join_usize_csv(values: &[usize]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn subtract_cpus(all: &[usize], used: &[usize]) -> Vec<usize> {
    all.iter()
        .copied()
        .filter(|cpu| !used.contains(cpu))
        .collect()
}

fn cycle_from_pool(pool: &[usize], count: usize) -> Vec<usize> {
    if pool.is_empty() || count == 0 {
        return Vec::new();
    }
    (0..count).map(|idx| pool[idx % pool.len()]).collect()
}

fn shares_any_cpu(lhs: &[usize], rhs: &[usize]) -> bool {
    lhs.iter().any(|cpu| rhs.contains(cpu))
}

fn build_cpu_to_numa_map() -> Result<BTreeMap<usize, i32>, Box<dyn Error>> {
    let mut cpu_to_node = BTreeMap::new();
    for node in online_numa_nodes()? {
        for cpu in numa_node_cpu_list(node)? {
            cpu_to_node.insert(cpu, node);
        }
    }
    Ok(cpu_to_node)
}

fn derive_owner_numa_node(
    raw_device: &std::path::Path,
    raw_device_numa_node: i32,
    config: &BenchConfig,
    cpu_to_node: &BTreeMap<usize, i32>,
) -> Result<Option<i32>, Box<dyn Error>> {
    let lookup_node = classify_cpu_set("lookup pins", &config.lookup_pin_cores, cpu_to_node)?;
    let commit_node = classify_cpu_set("commit pins", &config.commit_pin_cores, cpu_to_node)?;
    let drive_node = classify_cpu_set("drive pins", &config.drive_pin_cores, cpu_to_node)?;

    let roles = [
        ("lookup", lookup_node),
        ("commit", commit_node),
        ("drive", drive_node),
    ];
    let distinct_nodes = roles
        .iter()
        .filter_map(|(_, node)| *node)
        .collect::<BTreeSet<_>>();
    if distinct_nodes.len() > 1 {
        let detail = roles
            .iter()
            .filter_map(|(label, node)| node.map(|node| format!("{label}=node {node}")))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "raw device {} resolved to NUMA node {}, but KIX owner workers are split across locality domains ({detail}). Keep lookup, commit, and drive workers inside one NUMA domain per target, or benchmark those domains in separate runs instead of building a cross-node Frankenstein path.",
            raw_device.display(),
            raw_device_numa_node
        )
        .into());
    }

    let owner_numa_node = distinct_nodes.iter().next().copied();
    if let Some(owner_node) = owner_numa_node {
        if owner_node != raw_device_numa_node {
            eprintln!(
                "warning: raw device {} reports NUMA node {}, but KIX owner workers are pinned to NUMA node {}. KIX will continue because this is a coherent remote-owner benchmark shape, not a sane steady-state storage-target layout.",
                raw_device.display(),
                raw_device_numa_node,
                owner_node
            );
        }
    }
    Ok(owner_numa_node)
}

fn classify_cpu_set(
    label: &str,
    cpus: &[usize],
    cpu_to_node: &BTreeMap<usize, i32>,
) -> Result<Option<i32>, Box<dyn Error>> {
    if cpus.is_empty() {
        return Ok(None);
    }

    let unknown_cpus = cpus
        .iter()
        .copied()
        .filter(|cpu| !cpu_to_node.contains_key(cpu))
        .collect::<Vec<_>>();
    if !unknown_cpus.is_empty() {
        return Err(format!(
            "{label} {} include CPUs that are not present in the host NUMA topology: {}",
            join_usize_csv(cpus),
            join_usize_csv(&unknown_cpus)
        )
        .into());
    }

    let nodes = cpus
        .iter()
        .filter_map(|cpu| cpu_to_node.get(cpu).copied())
        .collect::<BTreeSet<_>>();
    if nodes.len() > 1 {
        let node_list = nodes
            .iter()
            .map(|node| node.to_string())
            .collect::<Vec<_>>()
            .join(",");
        return Err(format!(
            "{label} {} span NUMA nodes {}. KIX treats one target as one locality owner, so a single worker class must not be smeared across multiple NUMA domains.",
            join_usize_csv(cpus),
            node_list
        )
        .into());
    }
    Ok(nodes.iter().next().copied())
}

fn validate_ingress_placement(
    raw_device: &std::path::Path,
    ingress_placement: IngressPlacement,
    owner_numa_node: Option<i32>,
    local_ingress_core: Option<usize>,
    remote_ingress_core: Option<usize>,
    cpu_to_node: &BTreeMap<usize, i32>,
) -> Result<(), Box<dyn Error>> {
    let Some(owner_numa_node) = owner_numa_node else {
        if ingress_placement == IngressPlacement::Direct {
            return Ok(());
        }
        return Err(format!(
            "raw device {} did not yield a usable KIX owner NUMA domain, so ingress placement {} cannot be validated. Pin lookup, commit, and drive workers coherently first or use --ingress-placement direct.",
            raw_device.display(),
            ingress_placement.as_str()
        )
        .into());
    };

    let local_node = classify_optional_core("local ingress core", local_ingress_core, cpu_to_node)?;
    let remote_node =
        classify_optional_core("remote ingress core", remote_ingress_core, cpu_to_node)?;

    match ingress_placement {
        IngressPlacement::Direct => Ok(()),
        IngressPlacement::Local => {
            if local_node != Some(owner_numa_node) {
                return Err(format!(
                    "local ingress core {} resolved to NUMA node {}, but the KIX owner domain for raw device {} is NUMA node {}. Local ingress must enter on the owning domain instead of adding a fake cross-node hop before the benchmark even starts.",
                    option_core(local_ingress_core),
                    option_i32(local_node),
                    raw_device.display(),
                    owner_numa_node
                )
                .into());
            }
            Ok(())
        }
        IngressPlacement::Remote => {
            if remote_node == Some(owner_numa_node) {
                return Err(format!(
                    "remote ingress core {} resolved to NUMA node {}, which matches the KIX owner domain for raw device {}. That is not remote ingress; that is local ingress wearing a fake mustache.",
                    option_core(remote_ingress_core),
                    option_i32(remote_node),
                    raw_device.display()
                )
                .into());
            }
            Ok(())
        }
        IngressPlacement::Handoff => {
            if local_node != Some(owner_numa_node) {
                return Err(format!(
                    "handoff local ingress core {} resolved to NUMA node {}, but the KIX owner domain for raw device {} is NUMA node {}. The owner-side ingress worker must stay on the owning domain.",
                    option_core(local_ingress_core),
                    option_i32(local_node),
                    raw_device.display(),
                    owner_numa_node
                )
                .into());
            }
            if remote_node == Some(owner_numa_node) {
                return Err(format!(
                    "handoff remote ingress core {} resolved to NUMA node {}, which matches the KIX owner domain for raw device {}. A handoff path needs one wrong-domain ingress hop, not two local workers pretending to be different.",
                    option_core(remote_ingress_core),
                    option_i32(remote_node),
                    raw_device.display()
                )
                .into());
            }
            Ok(())
        }
    }
}

fn classify_optional_core(
    label: &str,
    core: Option<usize>,
    cpu_to_node: &BTreeMap<usize, i32>,
) -> Result<Option<i32>, Box<dyn Error>> {
    match core {
        Some(core_id) => classify_cpu_set(label, &[core_id], cpu_to_node),
        None => Ok(None),
    }
}

fn select_remote_ingress(
    raw_device: &std::path::Path,
    owner_node: i32,
    config: &BenchConfig,
) -> Result<(Option<i32>, Option<usize>), Box<dyn Error>> {
    let remote_node = online_numa_nodes()?
        .into_iter()
        .find(|node| *node != owner_node);
    let Some(remote_node) = remote_node else {
        return Ok((None, None));
    };

    let remote_cpus = numa_node_cpu_list(remote_node)?;
    if remote_cpus.is_empty() {
        eprintln!(
            "warning: alternate NUMA node {} exists for raw device {}, but it reports no CPUs. Remote ingress benchmarking is unavailable on this host.",
            remote_node,
            raw_device.display()
        );
        return Ok((Some(remote_node), None));
    }

    let used_cpus = config
        .lookup_pin_cores
        .iter()
        .chain(config.commit_pin_cores.iter())
        .chain(config.drive_pin_cores.iter())
        .copied()
        .collect::<Vec<_>>();
    let remote_core = remote_cpus
        .iter()
        .copied()
        .find(|cpu| !used_cpus.contains(cpu))
        .or_else(|| remote_cpus.first().copied());

    if let Some(core_id) = remote_core {
        if used_cpus.contains(&core_id) {
            eprintln!(
                "warning: KIX selected CPU core {} on alternate NUMA node {} as the remote ingress core for raw device {}, but that core is already used elsewhere. This deliberately measures cross-domain contention rather than ideal socket placement.",
                core_id,
                remote_node,
                raw_device.display()
            );
        } else {
            eprintln!(
                "info: raw device {} reports NUMA node {}; selected remote ingress core {} on alternate NUMA node {}",
                raw_device.display(),
                owner_node,
                core_id,
                remote_node
            );
        }
    }

    Ok((Some(remote_node), remote_core))
}

fn option_i32(value: Option<i32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn option_core(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cpu_map(entries: &[(usize, i32)]) -> BTreeMap<usize, i32> {
        entries.iter().copied().collect()
    }

    #[test]
    fn rejects_owner_worker_split_across_numa_nodes() {
        let mut config = BenchConfig::default();
        config.lookup_pin_cores = vec![0];
        config.commit_pin_cores = vec![64];
        config.drive_pin_cores = vec![65];

        let err = derive_owner_numa_node(
            std::path::Path::new("/dev/nvme0n1"),
            0,
            &config,
            &cpu_map(&[(0, 0), (64, 1), (65, 1)]),
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("split across locality domains"));
    }

    #[test]
    fn allows_coherent_remote_owner_placement() {
        let mut config = BenchConfig::default();
        config.lookup_pin_cores = vec![64];
        config.commit_pin_cores = vec![65];
        config.drive_pin_cores = vec![66];

        let owner = derive_owner_numa_node(
            std::path::Path::new("/dev/nvme0n1"),
            0,
            &config,
            &cpu_map(&[(64, 1), (65, 1), (66, 1)]),
        )
        .unwrap();

        assert_eq!(owner, Some(1));
    }

    #[test]
    fn rejects_fake_remote_ingress() {
        let err = validate_ingress_placement(
            std::path::Path::new("/dev/nvme0n1"),
            IngressPlacement::Remote,
            Some(0),
            None,
            Some(8),
            &cpu_map(&[(8, 0)]),
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("not remote ingress"));
    }

    #[test]
    fn accepts_valid_handoff_placement() {
        validate_ingress_placement(
            std::path::Path::new("/dev/nvme0n1"),
            IngressPlacement::Handoff,
            Some(0),
            Some(8),
            Some(64),
            &cpu_map(&[(8, 0), (64, 1)]),
        )
        .unwrap();
    }
}
