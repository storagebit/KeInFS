// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

#[cfg(target_os = "linux")]
mod config;
#[cfg(target_os = "linux")]
mod ingress;
#[cfg(target_os = "linux")]
mod media;
#[cfg(target_os = "linux")]
mod recovery;
#[cfg(target_os = "linux")]
mod steering;
#[cfg(target_os = "linux")]
mod topology;
#[cfg(target_os = "linux")]
mod workload;

#[cfg(target_os = "linux")]
use crate::config::{parse_args, validate_config, BenchConfig, BenchmarkMode, IngressPlacement};
#[cfg(target_os = "linux")]
use crate::ingress::IngressRuntime;
#[cfg(target_os = "linux")]
use crate::media::build_media_store;
#[cfg(target_os = "linux")]
use crate::recovery::run_recovery_benchmark;
#[cfg(target_os = "linux")]
use crate::steering::inspect_and_maybe_steer_irqs;
#[cfg(target_os = "linux")]
use crate::topology::{join_usize_csv, plan_raw_device_topology};
#[cfg(target_os = "linux")]
use crate::workload::{
    average_bytes_per_op, build_drive_configs, bytes_per_second, estimate_raw_arena_budget,
    mib_per_second, planned_media_span_bytes, prefill_working_set, print_latency_summary,
    run_worker,
};
#[cfg(target_os = "linux")]
use kix::{KixConfig, KixEngine, KixStatsConfig};
#[cfg(target_os = "linux")]
use std::env;
#[cfg(target_os = "linux")]
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::thread;
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config: BenchConfig = parse_args(env::args().skip(1).collect())?;
    validate_config(&config)?;
    let topology = plan_raw_device_topology(&mut config)?;

    if config.benchmark_mode == BenchmarkMode::Recovery {
        return run_recovery_benchmark(&config, &topology);
    }

    let drive_configs = build_drive_configs(&config, &topology)?;
    let interrupt_report = inspect_and_maybe_steer_irqs(&config, &topology)?;
    let raw_arena_budget = estimate_raw_arena_budget(&config, &drive_configs)?;
    if let Some(budget) = raw_arena_budget {
        if budget.is_undersized() {
            return Err(budget.rejection_message().into());
        }
        if budget.should_warn() {
            eprintln!("warning: {}", budget.warning_message());
        }
    }
    let media_store = build_media_store(&config)?;

    let engine = KixEngine::open(KixConfig {
        shard_count: config.shards,
        lookup_worker_mode: config.lookup_worker_mode(),
        commit_worker_mode: config.commit_worker_mode(),
        drive_worker_mode: config.drive_worker_mode(),
        drive_configs,
        lookup_pin_cores: config.lookup_pin_cores.clone(),
        commit_pin_cores: config.commit_pin_cores.clone(),
        drive_pin_cores: config.drive_pin_cores.clone(),
        shard_numa_node: topology.raw_device_numa_node,
        lookup_queue_depth: 4096,
        commit_queue_depth: 4096,
        drive_queue_depth: 4096,
        stats: config.stats_root.as_ref().map(|root| KixStatsConfig {
            root_dir: root.clone(),
            publish_interval: Duration::from_millis(config.stats_publish_ms.max(1)),
        }),
    })?;
    let client = Arc::new(engine.client());

    if config.prefill_keys > 0 {
        prefill_working_set(&client, &config, media_store.as_ref())?;
    }

    let ingress =
        IngressRuntime::open(&config, &topology, Arc::clone(&client), media_store.clone())?
            .map(Arc::new);

    let start = Instant::now();
    let mut joins = Vec::with_capacity(config.threads);

    for worker_id in 0..config.threads {
        let client = Arc::clone(&client);
        let config = config.clone();
        let media_store = media_store.clone();
        let ingress = ingress.clone();
        joins.push(thread::spawn(move || {
            run_worker(worker_id as u64, client, config, media_store, ingress)
        }));
    }

    let mut read_samples = Vec::new();
    let mut read_lookup_samples = Vec::new();
    let mut read_media_samples = Vec::new();
    let mut write_samples = Vec::new();
    let mut write_media_samples = Vec::new();
    let mut write_commit_samples = Vec::new();
    let mut total_ops = 0_u64;
    let mut read_ops = 0_u64;
    let mut write_ops = 0_u64;
    let mut media_read_ops = 0_u64;
    let mut media_write_ops = 0_u64;
    let mut read_logical_bytes = 0_u64;
    let mut read_stored_bytes = 0_u64;
    let mut write_logical_bytes = 0_u64;
    let mut write_stored_bytes = 0_u64;
    let mut errors = Vec::new();
    for join in joins {
        let result = join.join().map_err(|_| "worker thread panicked")?;
        read_samples.extend(result.read_samples);
        read_lookup_samples.extend(result.read_lookup_samples);
        read_media_samples.extend(result.read_media_samples);
        write_samples.extend(result.write_samples);
        write_media_samples.extend(result.write_media_samples);
        write_commit_samples.extend(result.write_commit_samples);
        total_ops += result.total_ops as u64;
        read_ops += result.read_ops as u64;
        write_ops += result.write_ops as u64;
        media_read_ops += result.media_read_ops as u64;
        media_write_ops += result.media_write_ops as u64;
        read_logical_bytes += result.read_logical_bytes;
        read_stored_bytes += result.read_stored_bytes;
        write_logical_bytes += result.write_logical_bytes;
        write_stored_bytes += result.write_stored_bytes;
        errors.extend(result.errors);
    }

    if !errors.is_empty() {
        return Err(format!(
            "kix-bench observed {} worker errors; first error: {}",
            errors.len(),
            errors[0]
        )
        .into());
    }

    let elapsed = start.elapsed();
    if config.checkpoint_at_end {
        engine.checkpoint_all()?;
    }

    let throughput = total_ops as f64 / elapsed.as_secs_f64();
    let hardware = engine.hardware_acceleration();
    println!("lookup_mode={}", config.lookup_mode_name.as_str());
    println!("commit_mode={}", config.commit_mode_name.as_str());
    println!("drive_mode={}", config.drive_mode_name.as_str());
    println!("ingress_placement={}", config.ingress_placement.as_str());
    println!("shards={}", config.shards);
    println!("threads={}", config.threads);
    println!("drive_count={}", config.drives);
    println!("prefill_keys={}", config.prefill_keys);
    println!("arena_backend=direct-uring");
    println!("read_path={}", config.read_path.as_str());
    println!("media_queue_depth={}", config.media_queue_depth);
    println!("media_read_batch_size={}", config.media_read_batch_size);
    println!("media_write_batch_size={}", config.media_write_batch_size);
    println!("media_flush_mode={}", config.media_flush_mode.as_str());
    println!("record_mix={}", config.record_mix.as_str());
    println!("extent_payload_bytes={}", config.extent_bytes);
    println!("packed_payload_bytes={}", config.packed_bytes);
    println!(
        "planned_media_span_bytes={}",
        planned_media_span_bytes(&config)
    );
    println!("cpu_arch={}", hardware.cpu_arch);
    println!("crc32_backend={}", hardware.crc32_backend.as_str());
    println!(
        "crc32_accelerated={}",
        if hardware.crc32_accelerated() {
            "yes"
        } else {
            "no"
        }
    );
    if let Some(node) = topology.raw_device_numa_node {
        println!("raw_device_numa_node={node}");
    }
    if let Some(node) = topology.owner_numa_node {
        println!("owner_numa_node={node}");
    }
    if let Some(budget) = raw_arena_budget {
        println!("raw_arena_slice_bytes={}", budget.slice_bytes);
        println!("raw_arena_prefill_write_ops={}", budget.prefill_write_ops);
        println!("raw_arena_measured_write_ops={}", budget.measured_write_ops);
        println!("raw_arena_total_write_ops={}", budget.total_write_ops);
        println!("raw_arena_live_entries={}", budget.live_entries);
        println!("raw_arena_checkpoint_bytes={}", budget.checkpoint_bytes);
        println!(
            "raw_arena_planning_batch_size={}",
            budget.planning_batch_size
        );
        println!(
            "raw_arena_planning_delta_bytes={}",
            budget.planning_delta_bytes
        );
        println!("raw_arena_recommended_bytes={}", budget.recommended_bytes);
        println!("raw_arena_worst_case_bytes={}", budget.worst_case_bytes);
        println!("raw_arena_ideal_bytes={}", budget.ideal_bytes);
        println!(
            "raw_arena_budget_status={}",
            if budget.should_warn() {
                "planning-fit-below-worst-case"
            } else {
                "planning-fit"
            }
        );
    }
    if let Some(core_id) = topology.local_ingress_core {
        println!("local_ingress_core={core_id}");
    }
    if let Some(node) = topology.remote_ingress_numa_node {
        println!("remote_ingress_numa_node={node}");
    }
    if let Some(core_id) = topology.remote_ingress_core {
        println!("remote_ingress_core={core_id}");
    }
    if let Some(runtime_dir) = engine.stats_runtime_dir() {
        println!("stats_runtime_dir={}", runtime_dir.display());
        if let Some(report) = &interrupt_report {
            report.write_to_runtime_dir(runtime_dir)?;
        }
    }
    if let Some(store) = media_store.as_ref() {
        println!("media_io_mode={}", store.io_mode_name());
        let media_root = store.root_dir();
        println!("media_root={media_root}");
    }
    if !config.netdevs.is_empty() {
        println!("netdevs={}", config.netdevs.join(","));
    }
    if let Some(report) = &interrupt_report {
        report.print_stdout_summary();
    }
    if !config.lookup_pin_cores.is_empty() {
        println!(
            "lookup_pin_cores={}",
            join_usize_csv(&config.lookup_pin_cores)
        );
    }
    if !config.commit_pin_cores.is_empty() {
        println!(
            "commit_pin_cores={}",
            join_usize_csv(&config.commit_pin_cores)
        );
    }
    if !config.drive_pin_cores.is_empty() {
        println!(
            "drive_pin_cores={}",
            join_usize_csv(&config.drive_pin_cores)
        );
    }
    if !topology.recommended_socket_cores.is_empty() {
        println!(
            "recommended_socket_cores={}",
            join_usize_csv(&topology.recommended_socket_cores)
        );
    }
    if matches!(
        config.ingress_placement,
        IngressPlacement::Local | IngressPlacement::Remote | IngressPlacement::Handoff
    ) {
        println!("ingress_queue_depth={}", config.ingress_queue_depth);
    }
    println!("ops={total_ops}");
    println!("read_ops={read_ops}");
    println!("write_ops={write_ops}");
    println!("media_read_ops={media_read_ops}");
    println!("media_write_ops={media_write_ops}");
    println!("read_payload_logical_bytes={read_logical_bytes}");
    println!("read_payload_stored_bytes={read_stored_bytes}");
    println!("write_payload_logical_bytes={write_logical_bytes}");
    println!("write_payload_stored_bytes={write_stored_bytes}");
    println!(
        "read_payload_stored_bytes_per_media_op_avg={}",
        average_bytes_per_op(read_stored_bytes, media_read_ops)
    );
    println!(
        "write_payload_stored_bytes_per_media_op_avg={}",
        average_bytes_per_op(write_stored_bytes, media_write_ops)
    );
    println!(
        "read_payload_stored_Bps={}",
        bytes_per_second(read_stored_bytes, elapsed.as_secs_f64())
    );
    println!(
        "write_payload_stored_Bps={}",
        bytes_per_second(write_stored_bytes, elapsed.as_secs_f64())
    );
    println!(
        "total_payload_stored_Bps={}",
        bytes_per_second(
            read_stored_bytes.saturating_add(write_stored_bytes),
            elapsed.as_secs_f64()
        )
    );
    println!(
        "read_payload_stored_MiB_s={:.2}",
        mib_per_second(read_stored_bytes, elapsed.as_secs_f64())
    );
    println!(
        "write_payload_stored_MiB_s={:.2}",
        mib_per_second(write_stored_bytes, elapsed.as_secs_f64())
    );
    println!(
        "total_payload_stored_MiB_s={:.2}",
        mib_per_second(
            read_stored_bytes.saturating_add(write_stored_bytes),
            elapsed.as_secs_f64()
        )
    );
    println!("elapsed_s={:.3}", elapsed.as_secs_f64());
    println!("throughput_ops_s={throughput:.0}");
    print_latency_summary("read", &mut read_samples);
    print_latency_summary("read_lookup", &mut read_lookup_samples);
    print_latency_summary("read_media", &mut read_media_samples);
    print_latency_summary("write", &mut write_samples);
    print_latency_summary("write_media", &mut write_media_samples);
    print_latency_summary("write_commit", &mut write_commit_samples);

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("kix-bench is Linux-only; run it on a Linux host with io_uring support".into())
}
