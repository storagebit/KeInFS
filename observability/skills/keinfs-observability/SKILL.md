<!-- SPDX-License-Identifier: GPL-2.0-or-later -->
---
name: keinfs-observability
description: >-
  Observe and monitor a KeInFS object-storage cluster (KMS, KAS, KST, KIX, KRS).
  Use when asked to check cluster health, find/scrape KeInFS metrics, deploy or
  configure Prometheus + Grafana for KeInFS, locate the keinexport sidecar
  (:9909/metrics), open or interpret the KeInFS Grafana dashboards, read the
  /run/keinfs/<svc>/<id> runtime stat trees, or get a starting point for any
  "is the cluster healthy / where do I look" question. Entry point that routes
  to keinfs-io-lifecycle-debug (slow-I/O troubleshooting) and
  keinfs-metrics-reference (metric catalog).
---

# KeInFS Observability

The entry point for observing a KeInFS cluster. KeInFS is a native
object-storage stack: data plane KSC -> KP2/HTTP2 -> KST -> KIX/chunk-media;
control plane (gRPC) KMS (metadata) + KAS (allocator) + KRS (rebuild).

## Where the data is

Two layers. Both expose the **same** counters/latencies; pick by access.

1. **Runtime stat trees (on each node, no Prometheus needed)** — every service
   writes a live file tree under `/run/keinfs/<svc>/<id>/`:
   - KST: `/run/keinfs/kst/<target-id>-<pid>/` -> `summary`, `identity`,
     `connections`, `streams`, `rpcs/read`, `rpcs/write`, embedded KIX path.
   - KIX: `/run/keinfs/kix/kix-<pid>/` -> `summary`, `hardware`, `shards/<id>`,
     `drives/<id>`.
   - KMS: `/run/keinfs/kms/kms-<shard-id>-<pid>/summary`.
   - KAS: `/run/keinfs/kas/kas-<pid>/summary`.
   - KRS: `/run/keinfs/krs/krs-<pid>/summary`.
   - KSC: under its configured stats root -> `summary`, `latency`,
     `phases/read`, `phases/write`, `target`.

   Read them directly:
   ```bash
   cat /run/keinfs/kst/*/summary
   cat /run/keinfs/kst/*/rpcs/write
   cat /run/keinfs/kix/*/summary
   cat /run/keinfs/kms/*/summary
   # over SSH to a lab node:
   ssh <node> 'cat /run/keinfs/kst/*/summary'
   ```

2. **Prometheus (cluster-wide, historical)** — each node runs a `keinexport`
   sidecar that turns those trees into Prometheus series. A central Prometheus
   scrapes them; Grafana renders dashboards.

## keinexport — the per-node sidecar

Crate `poc/keinexport`. Serves `/metrics` on `:9909`. Reads the stat files (no
hot-path load). Already installed on lab nodes as `keinfs-exporter.service`.

```bash
keinexport --listen 0.0.0.0:9909 --root /run/keinfs --root /var/lib/keinfs/run
# verify a node's surface:
curl -s http://<node>:9909/metrics | grep -E '^keinfs_(kst|kix|kms|kas|krs)_' | head
curl -s http://<node>:9909/metrics | grep keinfs_kst_up
```

All series are named `keinfs_<service>_<name>` with stable labels: `service`,
`instance`, plus `target_id` (KST), `shard` (KMS), and `rpc` / `phase` /
`quantile` on the latency families. For the full catalog use the
**keinfs-metrics-reference** skill.

## The 6 Grafana dashboards (folder "KeInFS")

Reference them by UID. Start at the Overview, drill down via header links.

| UID | Title | Use it for |
|-----|-------|-----------|
| `keinfs-io-lifecycle` | I/O Lifecycle Overview | Top of funnel: throughput, op rate, cluster health, phase-by-phase latency of a WRITE and a READ through the whole stack. |
| `keinfs-io-drilldown` | I/O Lifecycle Drill-Down | Per-phase deep dive, templated by service/rpc/instance; every phase's percentile spread, request + error rates. |
| `keinfs-kst-overview` | KST Storage Targets Overview | Fleet view across all targets: per-target throughput, latency, connections, health table. |
| `keinfs-kst-detail` | KST Target Detail | One target (templated): full per-RPC phase decomposition, percentiles, admission/stream rejections. |
| `keinfs-kix-overview` | KIX Index Overview | Fleet index health: live entries, upsert rate, get hit ratio, rebuild-required, errors. |
| `keinfs-kix-detail` | KIX Index Detail | One KIX instance: op rates, delta-log/checkpoint durability, build info. |

Direct link: `http://<grafana>:3000/d/<uid>`.

## Quick health check (PromQL)

Query Prometheus HTTP API directly:
```bash
PROM=http://<box>:9090
q() { curl -s --data-urlencode "query=$1" "$PROM/api/v1/query" | jq -r '.data.result[] | "\(.metric)  \(.value[1])"'; }

q 'count(keinfs_kms_up) + count(keinfs_kas_up) + count(keinfs_kst_up)'   # services up
q 'sum(keinfs_kms_total_errors) + sum(keinfs_kas_total_errors) + sum(keinfs_kst_total_errors)'  # total errors
q 'sum(rate(keinfs_kst_write_payload_bytes[2m]))'    # cluster write B/s
q 'sum(rate(keinfs_kst_read_payload_bytes[2m]))'     # cluster read B/s
q 'count(keinfs_kix_info{rebuild_required_drives!="none"})'   # drives needing rebuild
q 'max(keinfs_kas_capacity_used_pct)'                # fullest allocator shard
```

## Deploy the central stack

**Docker (one command)** — on a box that can reach every node's `:9909`:
```bash
# edit observability/prometheus/prometheus.yml target lists for your nodes, then:
docker compose -f observability/docker-compose.yml up -d
# Grafana http://<box>:3000 (admin/admin first login), Prometheus http://<box>:9090
```
Dashboards + datasource auto-provision; no manual import.

**Bare metal** — `observability/install-baremetal.sh` installs the official
Prometheus + Grafana apt packages, drops `prometheus/prometheus.yml` into
`/etc/prometheus`, provisions the datasource + all 6 dashboards into Grafana,
and enables both services.

## Where to go next

- **A specific I/O is slow / low throughput / high latency** -> use the
  **keinfs-io-lifecycle-debug** skill (encodes the IO_LIFECYCLE phase map:
  which phase to inspect for each symptom and what healthy looks like).
- **"What does metric X mean / which metrics exist for service Y"** -> use the
  **keinfs-metrics-reference** skill (full per-service metric catalog).
- Canonical domain doc: `poc/IO_LIFECYCLE.md` (every latency phase mapped to
  where time is spent). Cluster overview: `CLAUDE.md` runtime observability
  section.
