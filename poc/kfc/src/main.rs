// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

mod bench;
mod config;
mod metadata;
mod mount;

use crate::bench::run_mode_bench;
use crate::config::{parse_args, Command};
use crate::metadata::{boxed_error, DynError};

fn main() -> Result<(), DynError> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match parse_args(args).map_err(|err| boxed_error(err.to_string()))? {
        Command::Mount(config) => mount::run_mount(config)?,
        Command::ModeBench(config) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|err| boxed_error(err.to_string()))?;
            runtime.block_on(run_mode_bench(config))?;
        }
    }
    Ok(())
}
