// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit
//
// keinexport — a standalone Prometheus exporter for the KeInFS stack.
//
// Every KeInFS service (KMS, KAS, KST, KRS, KIX, KSC, KFC) already publishes a
// periodic `summary` snapshot to a runtime tree under `/run/keinfs/<svc>/<id>/`
// (or a configured stats root). This exporter discovers those trees, reads the
// freshest snapshot per instance on each scrape, converts it to Prometheus
// exposition format, and serves it on `/metrics`. It is a SIDECAR: it adds zero
// load to any service hot path (it only reads files a background publisher
// already writes), works uniformly across the JSON services and KST's
// key=value tree, and exports the full read/write I/O lifecycle — per-RPC
// request/error/latency and every named phase (KMS reserve route-resolve, KAS
// fence aborts + capacity, KST media-fsync / queue-wait, etc.).
//
// Usage:
//   keinexport [--listen 0.0.0.0:9909] [--root /run/keinfs] [--root /var/lib/keinfs/run] ...
//
// One exporter per node; point Prometheus at `<node>:9909/metrics`.

mod convert;
mod discover;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::Instant;

use convert::{json_snapshot_to_metrics, kst_kv_to_metrics, MetricSet};
use discover::{discover, read_rpc_files};

struct Config {
    listen: String,
    roots: Vec<PathBuf>,
}

fn parse_args() -> Result<Config, String> {
    let mut listen = "0.0.0.0:9909".to_string();
    let mut roots: Vec<PathBuf> = Vec::new();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--listen" => {
                i += 1;
                listen = args.get(i).ok_or("--listen needs a value")?.clone();
            }
            "--root" => {
                i += 1;
                roots.push(PathBuf::from(
                    args.get(i).ok_or("--root needs a value")?,
                ));
            }
            "--version" | "-V" => {
                let b = keinbuild::build_info!();
                println!("keinexport {} ({})", b.version, b.git_sha);
                std::process::exit(0);
            }
            "--help" | "-h" => {
                eprintln!(
                    "keinexport [--listen ADDR] [--root DIR]...\n  \
                     --listen  metrics bind address (default 0.0.0.0:9909)\n  \
                     --root    a runtime root to scan (repeatable; default /run/keinfs + /var/lib/keinfs/run)"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        i += 1;
    }
    if roots.is_empty() {
        // Both the tmpfs default and the keinfs-user home default used in the
        // OCI lab. Nonexistent roots are silently skipped during discovery.
        roots.push(PathBuf::from("/run/keinfs"));
        roots.push(PathBuf::from("/var/lib/keinfs/run"));
    }
    Ok(Config { listen, roots })
}

/// Build the full `/metrics` body for one scrape.
fn render_metrics(roots: &[PathBuf]) -> String {
    let started = Instant::now();
    let mut set = MetricSet::new();
    let mut instance_count = 0u64;

    for inst in discover(roots) {
        let raw = match std::fs::read_to_string(&inst.summary_path) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if inst.is_json {
            match serde_json::from_str::<serde_json::Value>(&raw) {
                Ok(v) => {
                    json_snapshot_to_metrics(&inst.service, &inst.instance_id, &v, &mut set);
                    instance_count += 1;
                }
                Err(_) => continue,
            }
        } else {
            // KST: flat key=value summary + a rpcs/ dir of per-RPC files.
            let rpc_files = inst
                .rpcs_dir
                .as_ref()
                .map(|d| read_rpc_files(d))
                .unwrap_or_default();
            kst_kv_to_metrics(&inst.instance_id, &raw, &rpc_files, &mut set);
            instance_count += 1;
        }
    }

    // Exporter self-metrics so the scrape itself is observable.
    set.add(
        "keinfs_exporter_instances_scraped",
        "gauge",
        "Number of service instances found and scraped",
        vec![],
        instance_count as f64,
    );
    set.add(
        "keinfs_exporter_scrape_duration_seconds",
        "gauge",
        "Time to build the metrics response",
        vec![],
        started.elapsed().as_secs_f64(),
    );
    set.render()
}

fn handle_client(mut stream: TcpStream, roots: &[PathBuf]) {
    let mut buf = [0u8; 2048];
    let n = stream.read(&mut buf).unwrap_or(0);
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = req
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let (status, content_type, body) = match path {
        p if p.starts_with("/metrics") => (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            render_metrics(roots),
        ),
        "/" | "/healthz" => ("200 OK", "text/plain; charset=utf-8", "ok\n".to_string()),
        _ => ("404 Not Found", "text/plain; charset=utf-8", "not found\n".to_string()),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn main() {
    let config = match parse_args() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("keinexport: {err}");
            std::process::exit(2);
        }
    };
    let listener = match TcpListener::bind(&config.listen) {
        Ok(l) => l,
        Err(err) => {
            eprintln!("keinexport: cannot bind {}: {err}", config.listen);
            std::process::exit(1);
        }
    };
    let b = keinbuild::build_info!();
    eprintln!(
        "keinexport {} ({}) serving /metrics on {} scanning {:?}",
        b.version, b.git_sha, config.listen, config.roots
    );
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_client(stream, &config.roots),
            Err(_) => continue,
        }
    }
}
