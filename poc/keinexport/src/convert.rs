// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit
//
// Snapshot -> Prometheus text-format conversion.
//
// Every KeInFS service already publishes a periodic `summary` snapshot to its
// runtime tree (`/run/keinfs/<svc>/<id>/summary`). The KMS/KAS/KRS/KSC snapshots
// are JSON; KST publishes a flat `key=value` `summary` plus a `rpcs/<name>`
// directory of `key=value` per-RPC phase trees. This module turns either form
// into Prometheus exposition text so a single scrape covers the full read/write
// I/O lifecycle of the whole stack without touching any hot path.
//
// Metric naming: every metric is `keinfs_<service>_<name>` with stable labels
// (`service`, `instance`, plus `shard`/`target_id` where known). Per-RPC and
// per-phase metrics carry `rpc=` / `phase=` labels so Grafana can break the
// lifecycle down by stage (e.g. the KMS reserve route-resolve phase, the KST
// media-fsync phase).

use std::collections::BTreeMap;
use std::fmt::Write as _;

/// One emitted Prometheus sample: fully-qualified metric name, label set, value.
pub struct Sample {
    pub metric: String,
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

/// Accumulates samples and renders them as Prometheus exposition text, grouping
/// by metric name with a single HELP/TYPE header per metric.
#[derive(Default)]
pub struct MetricSet {
    // metric name -> (type, help, samples)
    metrics: BTreeMap<String, (&'static str, String, Vec<Sample>)>,
    order: Vec<String>,
}

impl MetricSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(
        &mut self,
        metric: &str,
        kind: &'static str,
        help: &str,
        labels: Vec<(String, String)>,
        value: f64,
    ) {
        let entry = self.metrics.entry(metric.to_string()).or_insert_with(|| {
            self.order.push(metric.to_string());
            (kind, help.to_string(), Vec::new())
        });
        entry.2.push(Sample {
            metric: metric.to_string(),
            labels,
            value,
        });
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        for name in &self.order {
            let (kind, help, samples) = &self.metrics[name];
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} {kind}");
            for s in samples {
                out.push_str(&s.metric);
                if !s.labels.is_empty() {
                    out.push('{');
                    for (i, (k, v)) in s.labels.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        let _ = write!(out, "{k}=\"{}\"", escape_label(v));
                    }
                    out.push('}');
                }
                // Render integers without a trailing .0 for readability.
                if s.value.fract() == 0.0 && s.value.abs() < 1e15 {
                    let _ = writeln!(out, " {}", s.value as i64);
                } else {
                    let _ = writeln!(out, " {}", s.value);
                }
            }
        }
        out
    }
}

fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', " ")
}

/// Identity labels common to every metric from one instance.
fn base_labels(service: &str, instance: &str, extra: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut labels = vec![
        ("service".to_string(), service.to_string()),
        ("instance".to_string(), instance.to_string()),
    ];
    for (k, v) in extra {
        if !v.is_empty() {
            labels.push((k.to_string(), v.to_string()));
        }
    }
    labels
}

/// Convert a JSON snapshot (KMS / KAS / KRS / KSC daemon) into metrics.
///
/// `service` is the metric prefix segment (e.g. "kms"). `instance` is the unique
/// instance id (dir name). `shard`/`target` labels are pulled from `identity`.
pub fn json_snapshot_to_metrics(
    service: &str,
    instance: &str,
    value: &serde_json::Value,
    out: &mut MetricSet,
) {
    let obj = match value.as_object() {
        Some(o) => o,
        None => return,
    };

    // Identity -> labels (shard_id, target_id where present).
    let identity = obj.get("identity").and_then(|v| v.as_object());
    let shard = identity
        .and_then(|i| i.get("shard_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let target = identity
        .and_then(|i| i.get("target_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let base = base_labels(service, instance, &[("shard", shard), ("target_id", target)]);

    // `up`-style liveness gauge + uptime.
    out.add(
        &format!("keinfs_{service}_up"),
        "gauge",
        "1 if a snapshot was scraped for this instance",
        base.clone(),
        1.0,
    );

    // Walk every top-level field.
    for (key, val) in obj {
        match key.as_str() {
            "identity" => {}
            "rpcs" => {
                if let Some(rpcs) = val.as_object() {
                    for (rpc_name, rpc_val) in rpcs {
                        emit_rpc(service, &base, rpc_name, rpc_val, out);
                    }
                }
            }
            "background" => emit_background(service, &base, val, out),
            "last_error" => {} // string, not a metric
            other => {
                // Scalar counters/gauges (u64/f64) become metrics directly.
                if let Some(n) = as_number(val) {
                    let kind = metric_kind(other);
                    out.add(
                        &format!("keinfs_{service}_{}", sanitize(other)),
                        kind,
                        &format!("{service} {other}"),
                        base.clone(),
                        n,
                    );
                }
            }
        }
    }
}

fn emit_rpc(
    service: &str,
    base: &[(String, String)],
    rpc_name: &str,
    rpc_val: &serde_json::Value,
    out: &mut MetricSet,
) {
    let obj = match rpc_val.as_object() {
        Some(o) => o,
        None => return,
    };
    let mut rpc_labels = base.to_vec();
    rpc_labels.push(("rpc".to_string(), rpc_name.to_string()));

    if let Some(n) = obj.get("requests").and_then(as_number) {
        out.add(
            &format!("keinfs_{service}_rpc_requests_total"),
            "counter",
            "Total requests per RPC",
            rpc_labels.clone(),
            n,
        );
    }
    if let Some(n) = obj.get("errors").and_then(as_number) {
        out.add(
            &format!("keinfs_{service}_rpc_errors_total"),
            "counter",
            "Total errors per RPC",
            rpc_labels.clone(),
            n,
        );
    }
    if let Some(lat) = obj.get("latency") {
        emit_latency(
            &format!("keinfs_{service}_rpc_latency"),
            &rpc_labels,
            lat,
            out,
        );
    }
    if let Some(phases) = obj.get("phases").and_then(|v| v.as_object()) {
        for (phase_name, phase_val) in phases {
            let mut phase_labels = rpc_labels.clone();
            phase_labels.push(("phase".to_string(), phase_name.to_string()));
            emit_latency(
                &format!("keinfs_{service}_phase_latency"),
                &phase_labels,
                phase_val,
                out,
            );
        }
    }
}

fn emit_background(
    service: &str,
    base: &[(String, String)],
    val: &serde_json::Value,
    out: &mut MetricSet,
) {
    let obj = match val.as_object() {
        Some(o) => o,
        None => return,
    };
    for (k, v) in obj {
        if let Some(n) = as_number(v) {
            out.add(
                &format!("keinfs_{service}_background_{}", sanitize(k)),
                "gauge",
                &format!("{service} background {k}"),
                base.to_vec(),
                n,
            );
        } else if let Some(lat) = v.as_object().filter(|o| o.contains_key("p50_us")) {
            emit_latency(
                &format!("keinfs_{service}_background_{}", sanitize(k)),
                base,
                &serde_json::Value::Object(lat.clone()),
                out,
            );
        }
    }
}

/// Emit the percentile family from a LatencySummary {samples,avg_us,p50_us,...}.
fn emit_latency(
    base_name: &str,
    labels: &[(String, String)],
    lat: &serde_json::Value,
    out: &mut MetricSet,
) {
    let obj = match lat.as_object() {
        Some(o) => o,
        None => return,
    };
    let percentiles = [
        ("samples", "_samples", ""),
        ("avg_us", "_avg_microseconds", ""),
        ("p50_us", "_microseconds", "0.5"),
        ("p95_us", "_microseconds", "0.95"),
        ("p99_us", "_microseconds", "0.99"),
        ("max_us", "_microseconds", "1.0"),
    ];
    for (field, suffix, quantile) in percentiles {
        if let Some(n) = obj.get(field).and_then(as_number) {
            let mut lbls = labels.to_vec();
            if !quantile.is_empty() {
                lbls.push(("quantile".to_string(), quantile.to_string()));
            }
            let kind = if field == "samples" { "counter" } else { "gauge" };
            out.add(
                &format!("{base_name}{suffix}"),
                kind,
                "latency summary",
                lbls,
                n,
            );
        }
    }
}

/// Convert a KST flat `key=value` summary into metrics, plus its per-RPC phase
/// files (passed as (rpc_name, kv_text) pairs).
pub fn kst_kv_to_metrics(
    instance: &str,
    summary_kv: &str,
    rpc_files: &[(String, String)],
    out: &mut MetricSet,
) {
    let kv = parse_kv(summary_kv);
    let target_id = kv.get("target_id").cloned().unwrap_or_default();
    let base = base_labels("kst", instance, &[("target_id", &target_id)]);
    out.add(
        "keinfs_kst_up",
        "gauge",
        "1 if a snapshot was scraped for this KST",
        base.clone(),
        1.0,
    );
    for (k, v) in &kv {
        // Skip non-numeric identity strings; emit numeric counters/gauges.
        if let Ok(n) = v.parse::<f64>() {
            out.add(
                &format!("keinfs_kst_{}", sanitize(k)),
                metric_kind(k),
                &format!("kst {k}"),
                base.clone(),
                n,
            );
        }
    }
    for (rpc_name, text) in rpc_files {
        emit_kst_rpc(&base, rpc_name, text, out);
    }
}

fn emit_kst_rpc(
    base: &[(String, String)],
    rpc_name: &str,
    text: &str,
    out: &mut MetricSet,
) {
    let kv = parse_kv(text);
    let mut rpc_labels = base.to_vec();
    rpc_labels.push(("rpc".to_string(), rpc_name.to_string()));
    // Top-level rpc counters
    for (field, metric, kind) in [
        ("requests", "keinfs_kst_rpc_requests_total", "counter"),
        ("errors", "keinfs_kst_rpc_errors_total", "counter"),
        ("payload_bytes", "keinfs_kst_rpc_payload_bytes_total", "counter"),
    ] {
        if let Some(n) = kv.get(field).and_then(|v| v.parse::<f64>().ok()) {
            out.add(metric, kind, "kst per-rpc", rpc_labels.clone(), n);
        }
    }
    // Top-level latency percentiles: latency_p50_us etc.
    emit_kv_latency(
        "keinfs_kst_rpc_latency",
        &rpc_labels,
        &kv,
        "latency",
        out,
    );
    // Per-phase latency: phase_<name>_p50_us etc. Collect distinct phase names.
    let mut phases = std::collections::BTreeSet::new();
    for k in kv.keys() {
        if let Some(rest) = k.strip_prefix("phase_") {
            // strip the trailing _<metric> to get the phase name
            for suf in ["_samples", "_avg_us", "_p50_us", "_p95_us", "_p99_us", "_max_us"] {
                if let Some(name) = rest.strip_suffix(suf) {
                    phases.insert(name.to_string());
                }
            }
        }
    }
    for phase in phases {
        let mut phase_labels = rpc_labels.clone();
        phase_labels.push(("phase".to_string(), phase.clone()));
        emit_kv_latency(
            "keinfs_kst_phase_latency",
            &phase_labels,
            &kv,
            &format!("phase_{phase}"),
            out,
        );
    }
}

/// Emit percentile family from KST key=value latency fields named
/// `<prefix>_samples|avg_us|p50_us|p95_us|p99_us|max_us`.
fn emit_kv_latency(
    base_name: &str,
    labels: &[(String, String)],
    kv: &BTreeMap<String, String>,
    prefix: &str,
    out: &mut MetricSet,
) {
    let fields = [
        ("_samples", "_samples", ""),
        ("_avg_us", "_avg_microseconds", ""),
        ("_p50_us", "_microseconds", "0.5"),
        ("_p95_us", "_microseconds", "0.95"),
        ("_p99_us", "_microseconds", "0.99"),
        ("_max_us", "_microseconds", "1.0"),
    ];
    for (kv_suffix, metric_suffix, quantile) in fields {
        let key = format!("{prefix}{kv_suffix}");
        if let Some(n) = kv.get(&key).and_then(|v| v.parse::<f64>().ok()) {
            let mut lbls = labels.to_vec();
            if !quantile.is_empty() {
                lbls.push(("quantile".to_string(), quantile.to_string()));
            }
            let kind = if kv_suffix == "_samples" { "counter" } else { "gauge" };
            out.add(
                &format!("{base_name}{metric_suffix}"),
                kind,
                "kst latency summary",
                lbls,
                n,
            );
        }
    }
}

fn parse_kv(text: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for line in text.lines() {
        if let Some((k, v)) = line.split_once('=') {
            map.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    map
}

fn as_number(v: &serde_json::Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_u64().map(|u| u as f64))
}

/// Counters end in `_total`/`_requests`/`_hits`/etc.; everything else is a gauge.
/// Prometheus convention prefers `_total` suffix for counters, but the existing
/// snapshot field names are stable, so we classify heuristically and keep names.
fn metric_kind(name: &str) -> &'static str {
    let counterish = [
        "requests",
        "_total",
        "hits",
        "misses",
        "refills",
        "errors",
        "bypasses",
        "serves",
        "lookups",
        "rpcs",
        "accepted",
        "rejected",
        "rejections",
        "aborts",
        "failures",
        "released",
        "runs",
        "connections",
        "expired",
        "reads",
        "writes",
    ];
    if counterish.iter().any(|s| name.contains(s)) {
        "counter"
    } else {
        "gauge"
    }
}

/// Make a snapshot field name a valid Prometheus metric segment.
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_scalar_and_rpc_phases_become_metrics() {
        let snap = serde_json::json!({
            "identity": {"shard_id": "kms-01", "pid": 42},
            "total_requests": 1000,
            "reservation_cache_shard_bypasses": 0,
            "route_cache_hits": 9999,
            "rpcs": {
                "initiate_object_write": {
                    "requests": 500, "errors": 2,
                    "latency": {"samples": 500, "avg_us": 100, "p50_us": 80, "p95_us": 200, "p99_us": 300, "max_us": 999},
                    "phases": {
                        "reserve_route_resolve": {"samples": 500, "avg_us": 1, "p50_us": 1, "p95_us": 2, "p99_us": 3, "max_us": 9}
                    }
                }
            },
            "background": {"runs": 7, "run_latency": {"samples": 7, "avg_us": 0, "p50_us": 1, "p95_us": 1, "p99_us": 1, "max_us": 2}},
            "last_error": null
        });
        let mut set = MetricSet::new();
        json_snapshot_to_metrics("kms", "kms-01-42", &snap, &mut set);
        let text = set.render();
        // scalar counter
        assert!(text.contains("keinfs_kms_total_requests{service=\"kms\",instance=\"kms-01-42\",shard=\"kms-01\"} 1000"));
        // my new fix counter is exported
        assert!(text.contains("keinfs_kms_route_cache_hits"));
        assert!(text.contains("keinfs_kms_reservation_cache_shard_bypasses{service=\"kms\",instance=\"kms-01-42\",shard=\"kms-01\"} 0"));
        // per-rpc requests with rpc label
        assert!(text.contains("rpc=\"initiate_object_write\""));
        assert!(text.contains("keinfs_kms_rpc_requests_total"));
        // per-phase latency quantile carries phase + quantile labels
        assert!(text.contains("phase=\"reserve_route_resolve\""));
        assert!(text.contains("quantile=\"0.99\""));
        // HELP/TYPE headers present
        assert!(text.contains("# TYPE keinfs_kms_rpc_requests_total counter"));
        // up gauge
        assert!(text.contains("keinfs_kms_up{"));
    }

    #[test]
    fn kst_kv_summary_and_rpc_phases_become_metrics() {
        let summary = "target_id=epyc-target-00\nlisten_addr=0.0.0.0:18080\npid=2512955\nwrite_payload_bytes=1862708232192\ntotal_requests=1753420\ntotal_errors=24\nactive_connections=10\n";
        let write_rpc = "requests=1797981\nerrors=0\npayload_bytes=1927340359680\nlatency_samples=1797981\nlatency_avg_us=72212\nlatency_p50_us=32768\nlatency_p99_us=262144\nlatency_max_us=3120404\nphase_media_fsync_samples=1797880\nphase_media_fsync_p50_us=1\nphase_media_fsync_p99_us=2\nphase_media_fsync_max_us=2884\n";
        let mut set = MetricSet::new();
        kst_kv_to_metrics(
            "epyc-target-00-2512955",
            summary,
            &[("write".to_string(), write_rpc.to_string())],
            &mut set,
        );
        let text = set.render();
        assert!(text.contains("keinfs_kst_write_payload_bytes{service=\"kst\",instance=\"epyc-target-00-2512955\",target_id=\"epyc-target-00\"} 1862708232192"));
        assert!(text.contains("keinfs_kst_rpc_requests_total{") && text.contains("rpc=\"write\""));
        // per-phase media_fsync latency exported with phase label + quantile
        assert!(text.contains("phase=\"media_fsync\""));
        assert!(text.contains("keinfs_kst_phase_latency_microseconds"));
        // string identity field (listen_addr) is NOT emitted as a metric
        assert!(!text.contains("listen_addr"));
    }

    #[test]
    fn kas_capacity_gauge_is_exported() {
        let snap = serde_json::json!({
            "identity": {"shard_id": "alloc-shard-00"},
            "capacity_free_granules": 30000000_u64,
            "capacity_total_granules": 37990764_u64,
            "capacity_used_pct": 21.0,
            "fenced_commit_aborts": 0,
            "rpcs": {}
        });
        let mut set = MetricSet::new();
        json_snapshot_to_metrics("kas", "kas-00", &snap, &mut set);
        let text = set.render();
        assert!(text.contains("keinfs_kas_capacity_free_granules"));
        assert!(text.contains("keinfs_kas_capacity_used_pct"));
        assert!(text.contains("keinfs_kas_fenced_commit_aborts"));
    }
}
