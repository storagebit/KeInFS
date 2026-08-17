// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

mod bench;
mod client;
mod config;
mod ec_bench;
mod object_bench;
mod object_cli;
mod smoke;
mod stats;

use crate::bench::run_benchmark;
use crate::config::{parse_args, Command};
use crate::ec_bench::run_ec_benchmark;
use crate::object_bench::run_object_benchmark;
use crate::object_cli::{run_delete_object, run_get_object, run_put_object};
use crate::smoke::run_smoke;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match parse_args(args)? {
        Command::Smoke(config) => run_smoke(config).await?,
        Command::EcBenchmark(config) => run_ec_benchmark(config)?,
        Command::Benchmark(config) => run_benchmark(config).await?,
        Command::PutObject(config) => run_put_object(config).await?,
        Command::GetObject(config) => run_get_object(config).await?,
        Command::DeleteObject(config) => run_delete_object(config).await?,
        Command::ObjectBenchmark(config) => run_object_benchmark(config).await?,
    }
    Ok(())
}
