// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::EcBenchmarkConfig;
use kee::{backend_inventory, EcProfile, FailureDomain, KeeEngine};
use std::hint::black_box;
use std::time::{Duration, Instant};

pub(crate) fn run_ec_benchmark(
    config: EcBenchmarkConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = std::fs::read(&config.input_path)?;
    let profile = EcProfile::single_stripe_8_plus_2(
        "bench-8p2",
        "isa-l-reed-solomon",
        FailureDomain::DriveDomainLab,
    );
    let engine = KeeEngine::new(profile.clone())?;
    let plan = engine.prepared_plan()?;
    let inventory = backend_inventory();

    let mut reusable_shards = plan.allocate_output_buffers();
    let missing_indexes = [1_usize, 9_usize];
    for _ in 0..config.warmup_iterations {
        black_box(engine.encode(black_box(&payload))?);
        plan.encode_into(black_box(&payload), black_box(&mut reusable_shards))?;
        let encoded = plan.encode(&payload)?;
        let mut legacy_fragments = erase_fragments(&encoded, &missing_indexes);
        black_box(engine.reconstruct(black_box(&mut legacy_fragments))?);
        let mut prepared_fragments = erase_fragments(&encoded, &missing_indexes);
        black_box(plan.reconstruct(black_box(&mut prepared_fragments))?);
    }

    let mut legacy_samples = Vec::with_capacity(config.iterations);
    let mut prepared_samples = Vec::with_capacity(config.iterations);
    let mut legacy_total = Duration::ZERO;
    let mut prepared_total = Duration::ZERO;
    let mut legacy_reconstruct_samples = Vec::with_capacity(config.iterations);
    let mut prepared_reconstruct_samples = Vec::with_capacity(config.iterations);
    let mut legacy_reconstruct_total = Duration::ZERO;
    let mut prepared_reconstruct_total = Duration::ZERO;

    for _ in 0..config.iterations {
        let started = Instant::now();
        let legacy = engine.encode(black_box(&payload))?;
        let elapsed = started.elapsed();
        legacy_total += elapsed;
        legacy_samples.push(elapsed.as_micros());
        black_box(&legacy);

        let started = Instant::now();
        plan.encode_into(black_box(&payload), black_box(&mut reusable_shards))?;
        let elapsed = started.elapsed();
        prepared_total += elapsed;
        prepared_samples.push(elapsed.as_micros());
        black_box(&reusable_shards);

        let erased = erase_fragments(&legacy, &missing_indexes);

        let mut legacy_fragments = erased.clone();
        let started = Instant::now();
        let reconstructed = engine.reconstruct(black_box(&mut legacy_fragments))?;
        let elapsed = started.elapsed();
        legacy_reconstruct_total += elapsed;
        legacy_reconstruct_samples.push(elapsed.as_micros());
        black_box(&reconstructed);

        let mut prepared_fragments = erased;
        let started = Instant::now();
        let reconstructed = plan.reconstruct(black_box(&mut prepared_fragments))?;
        let elapsed = started.elapsed();
        prepared_reconstruct_total += elapsed;
        prepared_reconstruct_samples.push(elapsed.as_micros());
        black_box(&reconstructed);
    }

    let legacy = engine.encode(&payload)?;
    plan.encode_into(&payload, &mut reusable_shards)?;
    if legacy != reusable_shards {
        return Err("prepared EC output diverged from the legacy encode path".into());
    }
    let mut reconstructed_legacy = erase_fragments(&legacy, &missing_indexes);
    let reconstructed_legacy = engine.reconstruct(&mut reconstructed_legacy)?;
    let mut reconstructed_prepared = erase_fragments(&legacy, &missing_indexes);
    let reconstructed_prepared = plan.reconstruct(&mut reconstructed_prepared)?;
    if reconstructed_legacy != reconstructed_prepared {
        return Err("prepared EC reconstruct diverged from the legacy path".into());
    }

    legacy_samples.sort_unstable();
    prepared_samples.sort_unstable();
    legacy_reconstruct_samples.sort_unstable();
    prepared_reconstruct_samples.sort_unstable();
    let legacy_avg = legacy_total.as_micros() / config.iterations as u128;
    let prepared_avg = prepared_total.as_micros() / config.iterations as u128;
    let delta_pct = if legacy_avg == 0 {
        0.0
    } else {
        100.0 - ((prepared_avg as f64 / legacy_avg as f64) * 100.0)
    };
    let legacy_reconstruct_avg = legacy_reconstruct_total.as_micros() / config.iterations as u128;
    let prepared_reconstruct_avg =
        prepared_reconstruct_total.as_micros() / config.iterations as u128;
    let reconstruct_delta_pct = if legacy_reconstruct_avg == 0 {
        0.0
    } else {
        100.0 - ((prepared_reconstruct_avg as f64 / legacy_reconstruct_avg as f64) * 100.0)
    };

    println!(
        "ksc_ec_benchmark backend={} detail=\"{}\" payload_bytes={} fragment_bytes={} iterations={} warmup_iterations={}",
        backend_label(&inventory),
        inventory.detail,
        payload.len(),
        profile.fragment_bytes,
        config.iterations,
        config.warmup_iterations,
    );
    println!(
        "ksc_ec_benchmark_legacy avg_us={} p50_us={} p95_us={} p99_us={}",
        legacy_avg,
        percentile(&legacy_samples, 50),
        percentile(&legacy_samples, 95),
        percentile(&legacy_samples, 99),
    );
    println!(
        "ksc_ec_benchmark_prepared avg_us={} p50_us={} p95_us={} p99_us={} delta_vs_legacy_pct={:.2}",
        prepared_avg,
        percentile(&prepared_samples, 50),
        percentile(&prepared_samples, 95),
        percentile(&prepared_samples, 99),
        delta_pct,
    );
    println!(
        "ksc_ec_reconstruct_legacy avg_us={} p50_us={} p95_us={} p99_us={} missing_indexes={:?}",
        legacy_reconstruct_avg,
        percentile(&legacy_reconstruct_samples, 50),
        percentile(&legacy_reconstruct_samples, 95),
        percentile(&legacy_reconstruct_samples, 99),
        missing_indexes,
    );
    println!(
        "ksc_ec_reconstruct_prepared avg_us={} p50_us={} p95_us={} p99_us={} delta_vs_legacy_pct={:.2}",
        prepared_reconstruct_avg,
        percentile(&prepared_reconstruct_samples, 50),
        percentile(&prepared_reconstruct_samples, 95),
        percentile(&prepared_reconstruct_samples, 99),
        reconstruct_delta_pct,
    );
    Ok(())
}

fn erase_fragments(fragments: &[Vec<u8>], missing_indexes: &[usize]) -> Vec<Option<Vec<u8>>> {
    fragments
        .iter()
        .enumerate()
        .map(|(index, fragment)| {
            if missing_indexes.contains(&index) {
                None
            } else {
                Some(fragment.clone())
            }
        })
        .collect()
}

fn percentile(samples: &[u128], pct: usize) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let rank = ((samples.len() - 1) * pct) / 100;
    samples[rank]
}

fn backend_label(inventory: &kee::HardwareInventory) -> &'static str {
    match inventory.selected_backend {
        kee::BackendKind::IsaL => "isa-l",
        kee::BackendKind::Software => "software",
    }
}
