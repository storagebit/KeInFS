// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::BenchConfig;
use crate::topology::{join_usize_csv, TopologyPlan};
use kix::{numa_node_cpu_list, online_numa_nodes};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub(crate) struct InterruptTopologyReport {
    pub(crate) owner_numa_node: Option<i32>,
    pub(crate) raw_device_numa_node: Option<i32>,
    pub(crate) steer_irqs: bool,
    pub(crate) nvme_devices: Vec<IrqDeviceReport>,
    pub(crate) netdevs: Vec<NetdevIrqReport>,
}

#[derive(Clone, Debug)]
pub(crate) struct IrqDeviceReport {
    pub(crate) label: String,
    pub(crate) path: PathBuf,
    pub(crate) pci_path: PathBuf,
    pub(crate) target_cpus: Vec<usize>,
    pub(crate) irq_count: usize,
    pub(crate) local_irq_count: usize,
    pub(crate) remote_irq_count: usize,
    pub(crate) mixed_irq_count: usize,
    pub(crate) irqs: Vec<IrqReport>,
}

#[derive(Clone, Debug)]
pub(crate) struct NetdevIrqReport {
    pub(crate) name: String,
    pub(crate) pci_path: PathBuf,
    pub(crate) numa_node: Option<i32>,
    pub(crate) target_cpus: Vec<usize>,
    pub(crate) irq_count: usize,
    pub(crate) local_irq_count: usize,
    pub(crate) remote_irq_count: usize,
    pub(crate) mixed_irq_count: usize,
    pub(crate) rx_queue_count: usize,
    pub(crate) tx_queue_count: usize,
    pub(crate) rps_nonzero_queues: usize,
    pub(crate) xps_nonzero_queues: usize,
    pub(crate) irqs: Vec<IrqReport>,
}

#[derive(Clone, Debug)]
pub(crate) struct IrqReport {
    pub(crate) irq: u32,
    pub(crate) affinity_list: String,
    pub(crate) locality: IrqLocality,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IrqLocality {
    Local,
    Remote,
    Mixed,
    Unknown,
}

impl IrqLocality {
    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
            Self::Mixed => "mixed",
            Self::Unknown => "unknown",
        }
    }
}

impl InterruptTopologyReport {
    pub(crate) fn write_to_runtime_dir(&self, runtime_dir: &Path) -> io::Result<()> {
        write_text_file_atomic(runtime_dir.join("interrupts"), &self.format())
    }

    pub(crate) fn print_stdout_summary(&self) {
        println!("irq_steering={}", yes_no(self.steer_irqs));
        if let Some(node) = self.owner_numa_node {
            println!("irq_owner_numa_node={node}");
        }
        for (idx, device) in self.nvme_devices.iter().enumerate() {
            println!("nvme_irq_device_{idx}_label={}", device.label);
            println!("nvme_irq_device_{idx}_path={}", device.path.display());
            println!("nvme_irq_device_{idx}_count={}", device.irq_count);
            println!("nvme_irq_device_{idx}_local={}", device.local_irq_count);
            println!("nvme_irq_device_{idx}_remote={}", device.remote_irq_count);
            println!("nvme_irq_device_{idx}_mixed={}", device.mixed_irq_count);
            println!(
                "nvme_irq_device_{idx}_target_cpus={}",
                csv_or_none(&device.target_cpus)
            );
        }
        for report in &self.netdevs {
            let label = sanitize_key(&report.name);
            println!("netdev_{label}_numa_node={}", option_i32(report.numa_node));
            println!("netdev_{label}_irq_count={}", report.irq_count);
            println!("netdev_{label}_irq_local={}", report.local_irq_count);
            println!("netdev_{label}_irq_remote={}", report.remote_irq_count);
            println!("netdev_{label}_irq_mixed={}", report.mixed_irq_count);
            println!("netdev_{label}_rx_queues={}", report.rx_queue_count);
            println!("netdev_{label}_tx_queues={}", report.tx_queue_count);
            println!("netdev_{label}_rps_nonzero={}", report.rps_nonzero_queues);
            println!("netdev_{label}_xps_nonzero={}", report.xps_nonzero_queues);
            println!(
                "netdev_{label}_target_cpus={}",
                csv_or_none(&report.target_cpus)
            );
        }
    }

    fn format(&self) -> String {
        let mut out = String::new();
        out.push_str("steer_irqs=");
        out.push_str(yes_no(self.steer_irqs));
        out.push('\n');
        out.push_str("owner_numa_node=");
        out.push_str(&option_i32(self.owner_numa_node));
        out.push('\n');
        out.push_str("raw_device_numa_node=");
        out.push_str(&option_i32(self.raw_device_numa_node));
        out.push('\n');
        out.push_str("nvme_device_count=");
        out.push_str(&self.nvme_devices.len().to_string());
        out.push('\n');
        out.push_str("netdev_count=");
        out.push_str(&self.netdevs.len().to_string());
        out.push('\n');

        for (idx, device) in self.nvme_devices.iter().enumerate() {
            out.push_str(&format!(
                concat!(
                    "nvme.{}.label={}\n",
                    "nvme.{}.path={}\n",
                    "nvme.{}.pci_path={}\n",
                    "nvme.{}.target_cpus={}\n",
                    "nvme.{}.irq_count={}\n",
                    "nvme.{}.irq_local={}\n",
                    "nvme.{}.irq_remote={}\n",
                    "nvme.{}.irq_mixed={}\n"
                ),
                idx,
                device.label,
                idx,
                device.path.display(),
                idx,
                device.pci_path.display(),
                idx,
                csv_or_none(&device.target_cpus),
                idx,
                device.irq_count,
                idx,
                device.local_irq_count,
                idx,
                device.remote_irq_count,
                idx,
                device.mixed_irq_count,
            ));
            for irq in &device.irqs {
                out.push_str(&format!(
                    "nvme.{}.irq.{}={}:{}\n",
                    idx,
                    irq.irq,
                    irq.affinity_list,
                    irq.locality.as_str()
                ));
            }
        }

        for report in &self.netdevs {
            let label = sanitize_key(&report.name);
            out.push_str(&format!(
                concat!(
                    "netdev.{}.pci_path={}\n",
                    "netdev.{}.numa_node={}\n",
                    "netdev.{}.target_cpus={}\n",
                    "netdev.{}.irq_count={}\n",
                    "netdev.{}.irq_local={}\n",
                    "netdev.{}.irq_remote={}\n",
                    "netdev.{}.irq_mixed={}\n",
                    "netdev.{}.rx_queues={}\n",
                    "netdev.{}.tx_queues={}\n",
                    "netdev.{}.rps_nonzero_queues={}\n",
                    "netdev.{}.xps_nonzero_queues={}\n"
                ),
                label,
                report.pci_path.display(),
                label,
                option_i32(report.numa_node),
                label,
                csv_or_none(&report.target_cpus),
                label,
                report.irq_count,
                label,
                report.local_irq_count,
                label,
                report.remote_irq_count,
                label,
                report.mixed_irq_count,
                label,
                report.rx_queue_count,
                label,
                report.tx_queue_count,
                label,
                report.rps_nonzero_queues,
                label,
                report.xps_nonzero_queues,
            ));
            for irq in &report.irqs {
                out.push_str(&format!(
                    "netdev.{}.irq.{}={}:{}\n",
                    label,
                    irq.irq,
                    irq.affinity_list,
                    irq.locality.as_str()
                ));
            }
        }

        out
    }
}

pub(crate) fn inspect_and_maybe_steer_irqs(
    config: &BenchConfig,
    topology: &TopologyPlan,
) -> Result<Option<InterruptTopologyReport>, Box<dyn Error>> {
    if config.raw_device.is_none() && config.media_raw_device.is_none() && config.netdevs.is_empty()
    {
        return Ok(None);
    }

    let cpu_to_node = build_cpu_to_numa_map()?;
    let owner_numa_node = topology.owner_numa_node.or(topology.raw_device_numa_node);
    let nvme_target_cpus = preferred_nvme_irq_cpus(config, topology);
    let net_target_cpus = preferred_net_irq_cpus(config, topology);

    let mut nvme_devices = Vec::new();
    let mut seen_block_paths = BTreeSet::new();
    for (label, path) in [
        ("arena", config.raw_device.as_ref()),
        ("media", config.media_raw_device.as_ref()),
    ] {
        let Some(path) = path else {
            continue;
        };
        let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        if !seen_block_paths.insert(canonical.clone()) {
            continue;
        }
        let report = inspect_block_device(
            label,
            &canonical,
            owner_numa_node,
            &cpu_to_node,
            &nvme_target_cpus,
            config.steer_irqs,
        )?;
        nvme_devices.push(report);
    }

    let mut netdevs = Vec::new();
    for netdev in &config.netdevs {
        netdevs.push(inspect_netdev(
            netdev,
            owner_numa_node,
            &cpu_to_node,
            &net_target_cpus,
            config.steer_irqs,
        )?);
    }

    Ok(Some(InterruptTopologyReport {
        owner_numa_node,
        raw_device_numa_node: topology.raw_device_numa_node,
        steer_irqs: config.steer_irqs,
        nvme_devices,
        netdevs,
    }))
}

fn inspect_block_device(
    label: &str,
    path: &Path,
    owner_numa_node: Option<i32>,
    cpu_to_node: &BTreeMap<usize, i32>,
    target_cpus: &[usize],
    steer_irqs: bool,
) -> Result<IrqDeviceReport, Box<dyn Error>> {
    let pci_path = resolve_block_device_pci_path(path)?;
    let mut irqs = load_irq_reports(&pci_path, owner_numa_node, cpu_to_node)?;
    if steer_irqs && !target_cpus.is_empty() {
        steer_irq_list(&irqs, target_cpus)?;
        irqs = load_irq_reports(&pci_path, owner_numa_node, cpu_to_node)?;
    }
    let (local_irq_count, remote_irq_count, mixed_irq_count) = summarize_irq_locality(&irqs);
    Ok(IrqDeviceReport {
        label: label.to_string(),
        path: path.to_path_buf(),
        pci_path,
        target_cpus: target_cpus.to_vec(),
        irq_count: irqs.len(),
        local_irq_count,
        remote_irq_count,
        mixed_irq_count,
        irqs,
    })
}

fn inspect_netdev(
    name: &str,
    owner_numa_node: Option<i32>,
    cpu_to_node: &BTreeMap<usize, i32>,
    target_cpus: &[usize],
    steer_irqs: bool,
) -> Result<NetdevIrqReport, Box<dyn Error>> {
    let pci_path = fs::canonicalize(Path::new("/sys/class/net").join(name).join("device"))
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "KIX could not resolve netdev {} in /sys/class/net: {err}",
                    name
                ),
            )
        })?;
    let numa_node = read_optional_i32(
        &Path::new("/sys/class/net")
            .join(name)
            .join("device")
            .join("numa_node"),
    )?;
    let mut irqs = load_irq_reports(&pci_path, owner_numa_node, cpu_to_node)?;
    if steer_irqs && !target_cpus.is_empty() {
        steer_irq_list(&irqs, target_cpus)?;
        irqs = load_irq_reports(&pci_path, owner_numa_node, cpu_to_node)?;
    }

    let queues_dir = Path::new("/sys/class/net").join(name).join("queues");
    let (rx_queue_count, tx_queue_count, rps_nonzero_queues, xps_nonzero_queues) =
        inspect_queue_masks(&queues_dir)?;
    let (local_irq_count, remote_irq_count, mixed_irq_count) = summarize_irq_locality(&irqs);

    Ok(NetdevIrqReport {
        name: name.to_string(),
        pci_path,
        numa_node,
        target_cpus: target_cpus.to_vec(),
        irq_count: irqs.len(),
        local_irq_count,
        remote_irq_count,
        mixed_irq_count,
        rx_queue_count,
        tx_queue_count,
        rps_nonzero_queues,
        xps_nonzero_queues,
        irqs,
    })
}

fn preferred_nvme_irq_cpus(config: &BenchConfig, topology: &TopologyPlan) -> Vec<usize> {
    let mut cpus = Vec::new();
    cpus.extend(config.drive_pin_cores.iter().copied());
    cpus.extend(config.commit_pin_cores.iter().copied());
    cpus.extend(config.lookup_pin_cores.iter().copied());
    if let Some(core) = topology.local_ingress_core {
        cpus.push(core);
    }
    unique_cpus(&cpus)
}

fn preferred_net_irq_cpus(config: &BenchConfig, topology: &TopologyPlan) -> Vec<usize> {
    let mut cpus = Vec::new();
    if let Some(core) = topology.local_ingress_core {
        cpus.push(core);
    }
    cpus.extend(topology.recommended_socket_cores.iter().copied());
    if cpus.is_empty() {
        owner_cpu_set(config, topology)
    } else {
        unique_cpus(&cpus)
    }
}

fn owner_cpu_set(config: &BenchConfig, topology: &TopologyPlan) -> Vec<usize> {
    let mut cpus = Vec::new();
    cpus.extend(config.lookup_pin_cores.iter().copied());
    cpus.extend(config.commit_pin_cores.iter().copied());
    cpus.extend(config.drive_pin_cores.iter().copied());
    if let Some(core) = topology.local_ingress_core {
        cpus.push(core);
    }
    unique_cpus(&cpus)
}

fn unique_cpus(cpus: &[usize]) -> Vec<usize> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for cpu in cpus {
        if seen.insert(*cpu) {
            out.push(*cpu);
        }
    }
    out
}

fn resolve_block_device_pci_path(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let dev_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("KIX block device path {} has no file name", path.display()),
        )
    })?;
    let sys_device = fs::canonicalize(Path::new("/sys/class/block").join(dev_name).join("device"))
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "KIX could not resolve sysfs device path for block device {}: {err}",
                    path.display()
                ),
            )
        })?;
    find_irq_parent(&sys_device).ok_or_else(|| {
        format!(
            "KIX could not find an IRQ-bearing PCI parent for block device {} (sysfs path {})",
            path.display(),
            sys_device.display()
        )
        .into()
    })
}

fn find_irq_parent(path: &Path) -> Option<PathBuf> {
    for ancestor in path.ancestors() {
        if ancestor.join("msi_irqs").is_dir() {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

fn load_irq_reports(
    pci_path: &Path,
    owner_numa_node: Option<i32>,
    cpu_to_node: &BTreeMap<usize, i32>,
) -> Result<Vec<IrqReport>, Box<dyn Error>> {
    let irq_dir = pci_path.join("msi_irqs");
    let mut irqs = fs::read_dir(&irq_dir)
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "KIX could not enumerate IRQs under {}: {err}",
                    irq_dir.display()
                ),
            )
        })?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter_map(|name| name.parse::<u32>().ok())
        .collect::<Vec<_>>();
    irqs.sort_unstable();

    irqs.into_iter()
        .map(|irq| {
            let affinity_list = fs::read_to_string(
                Path::new("/proc/irq")
                    .join(irq.to_string())
                    .join("smp_affinity_list"),
            )
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("KIX could not read /proc/irq/{irq}/smp_affinity_list: {err}"),
                )
            })?
            .trim()
            .to_string();
            let locality = classify_irq_locality(&affinity_list, owner_numa_node, cpu_to_node);
            Ok(IrqReport {
                irq,
                affinity_list,
                locality,
            })
        })
        .collect()
}

fn summarize_irq_locality(irqs: &[IrqReport]) -> (usize, usize, usize) {
    let mut local = 0;
    let mut remote = 0;
    let mut mixed = 0;
    for irq in irqs {
        match irq.locality {
            IrqLocality::Local => local += 1,
            IrqLocality::Remote => remote += 1,
            IrqLocality::Mixed => mixed += 1,
            IrqLocality::Unknown => {}
        }
    }
    (local, remote, mixed)
}

fn steer_irq_list(irqs: &[IrqReport], target_cpus: &[usize]) -> Result<(), Box<dyn Error>> {
    let mut success_count = 0_usize;
    let mut first_error: Option<io::Error> = None;
    let mut skipped = Vec::new();
    for (idx, irq) in irqs.iter().enumerate() {
        let cpu = target_cpus[idx % target_cpus.len()];
        let affinity_path = Path::new("/proc/irq")
            .join(irq.irq.to_string())
            .join("smp_affinity_list");
        match fs::write(&affinity_path, format!("{cpu}")) {
            Ok(()) => success_count += 1,
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(io::Error::new(
                        err.kind(),
                        format!(
                            "KIX could not steer IRQ {} via {} to CPU {}: {err}",
                            irq.irq,
                            affinity_path.display(),
                            cpu
                        ),
                    ));
                }
                skipped.push(format!("irq {} -> cpu {} ({err})", irq.irq, cpu));
            }
        }
    }
    if success_count == 0 {
        return Err(first_error
            .unwrap_or_else(|| io::Error::other("KIX IRQ steering had no writable vectors"))
            .into());
    }
    if !skipped.is_empty() {
        let examples = skipped
            .iter()
            .take(4)
            .cloned()
            .collect::<Vec<_>>()
            .join("; ");
        eprintln!(
            "warning: KIX could not steer {}/{} IRQ vectors. The kernel kept their existing affinity. First examples: {}",
            skipped.len(),
            irqs.len(),
            examples
        );
    }
    Ok(())
}

fn inspect_queue_masks(queues_dir: &Path) -> Result<(usize, usize, usize, usize), Box<dyn Error>> {
    let mut rx_queue_count = 0;
    let mut tx_queue_count = 0;
    let mut rps_nonzero_queues = 0;
    let mut xps_nonzero_queues = 0;

    for entry in fs::read_dir(queues_dir).map_err(|err| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "KIX could not read queue directory {}: {err}",
                queues_dir.display()
            ),
        )
    })? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("rx-") {
            rx_queue_count += 1;
            if mask_file_nonzero(&entry.path().join("rps_cpus"))? {
                rps_nonzero_queues += 1;
            }
        } else if name.starts_with("tx-") {
            tx_queue_count += 1;
            if mask_file_nonzero(&entry.path().join("xps_cpus"))? {
                xps_nonzero_queues += 1;
            }
        }
    }

    Ok((
        rx_queue_count,
        tx_queue_count,
        rps_nonzero_queues,
        xps_nonzero_queues,
    ))
}

fn mask_file_nonzero(path: &Path) -> Result<bool, Box<dyn Error>> {
    if !path.exists() {
        return Ok(false);
    }
    let text = fs::read_to_string(path)?.trim().to_string();
    let stripped = text.replace(',', "");
    Ok(stripped.chars().any(|ch| ch != '0'))
}

fn classify_irq_locality(
    affinity_list: &str,
    owner_numa_node: Option<i32>,
    cpu_to_node: &BTreeMap<usize, i32>,
) -> IrqLocality {
    let Some(owner_numa_node) = owner_numa_node else {
        return IrqLocality::Unknown;
    };
    let Ok(cpus) = parse_cpu_list(affinity_list) else {
        return IrqLocality::Unknown;
    };
    if cpus.is_empty() {
        return IrqLocality::Unknown;
    }

    let mut saw_local = false;
    let mut saw_remote = false;
    for cpu in cpus {
        match cpu_to_node.get(&cpu) {
            Some(node) if *node == owner_numa_node => saw_local = true,
            Some(_) => saw_remote = true,
            None => return IrqLocality::Unknown,
        }
    }

    match (saw_local, saw_remote) {
        (true, false) => IrqLocality::Local,
        (false, true) => IrqLocality::Remote,
        (true, true) => IrqLocality::Mixed,
        (false, false) => IrqLocality::Unknown,
    }
}

fn parse_cpu_list(input: &str) -> Result<Vec<usize>, Box<dyn Error>> {
    let mut cpus = Vec::new();
    for part in input
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        if let Some((start, end)) = part.split_once('-') {
            let start = start.parse::<usize>()?;
            let end = end.parse::<usize>()?;
            if end < start {
                return Err(format!("invalid CPU range {part}").into());
            }
            for cpu in start..=end {
                cpus.push(cpu);
            }
        } else {
            cpus.push(part.parse::<usize>()?);
        }
    }
    Ok(cpus)
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

fn read_optional_i32(path: &Path) -> io::Result<Option<i32>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(path)?;
    let value = raw.trim().parse::<i32>().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("could not parse {} as i32: {err}", path.display()),
        )
    })?;
    if value < 0 {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

fn write_text_file_atomic(path: impl AsRef<Path>, contents: &str) -> io::Result<()> {
    let path = path.as_ref();
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("KIX output path {} has no parent directory", path.display()),
        )
    })?;
    fs::create_dir_all(parent)?;
    let tmp_path = path.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(&tmp_path, contents)?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

fn csv_or_none(values: &[usize]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        join_usize_csv(values)
    }
}

fn sanitize_key(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn option_i32(value: Option<i32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpu_list_ranges() {
        assert_eq!(
            parse_cpu_list("0-3,8,10-11").unwrap(),
            vec![0, 1, 2, 3, 8, 10, 11]
        );
    }

    #[test]
    fn classifies_mixed_irq_locality() {
        let cpu_to_node = BTreeMap::from([(0_usize, 0_i32), (1, 0), (64, 1)]);
        let locality = classify_irq_locality("0,64", Some(0), &cpu_to_node);
        assert_eq!(locality, IrqLocality::Mixed);
    }

    #[test]
    fn prefers_drive_cpus_for_nvme_irqs() {
        let mut config = BenchConfig::default();
        config.lookup_pin_cores = vec![0, 1];
        config.commit_pin_cores = vec![2, 3];
        config.drive_pin_cores = vec![8];
        let topology = TopologyPlan::default();

        assert_eq!(
            preferred_nvme_irq_cpus(&config, &topology),
            vec![8, 2, 3, 0, 1]
        );
    }
}
