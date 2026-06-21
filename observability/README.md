<!-- SPDX-License-Identifier: GPL-2.0-or-later -->
# KeInFS Observability Stack

Enterprise-class metrics + dashboards for the full KeInFS read/write I/O
lifecycle. Each node runs a `keinexport` Prometheus sidecar (`:9909/metrics`)
that converts the per-service runtime stat trees into labelled Prometheus
series; a central Prometheus scrapes them and Grafana renders the dashboards.

```
 KMS/KAS/KST/KIX/KRS  ──writes──▶  /run/keinfs/<svc>/<id>/summary
        │                                   │
        │                          keinexport (:9909)  ◀── reads files, no hot-path load
        ▼                                   │
   (data path)                       Prometheus (:9090)  ──▶  Grafana (:3000)
```

## Components

- **`keinexport`** — the per-node sidecar (crate `poc/keinexport`). Already
  deployed as `keinfs-exporter.service` on the lab nodes. Serves `/metrics`.
- **`prometheus/prometheus.yml`** — scrape config; edit the target lists.
- **`grafana/`** — datasource + dashboard provisioning, and the 6 dashboards.
- **`docker-compose.yml`** — one-command Prometheus + Grafana.

## Dashboards (folder "KeInFS" in Grafana)

| UID | Title | Purpose |
|-----|-------|---------|
| `keinfs-io-lifecycle` | I/O Lifecycle Overview | High-level: throughput, op rate, cluster health, and the phase-by-phase latency of a WRITE and a READ through the whole stack. Top-of-funnel; links down to the rest. |
| `keinfs-io-drilldown` | I/O Lifecycle Drill-Down | Per-phase deep dive, templated by service / rpc / instance. Every phase's percentile spread, request + error rates. |
| `keinfs-kst-overview` | KST Storage Targets Overview | Fleet view across all targets: per-target throughput, latency, connections, a health table. |
| `keinfs-kst-detail` | KST Target Detail | Single target (templated): full per-RPC phase decomposition, latency percentiles, admission/stream rejections. |
| `keinfs-kix-overview` | KIX Index Overview | Fleet index health: live entries, upsert rate, get hit ratio, rebuild-required, errors. |
| `keinfs-kix-detail` | KIX Index Detail | Single KIX instance: op rates, delta-log/checkpoint durability activity, build info. |

The two lifecycle dashboards are the entry point — start at the Overview, then
drill into the Drill-Down / KST / KIX dashboards via the header links.

## Deploy the central stack (Docker)

On the observability box (must reach every node's `:9909`):

```bash
# edit prometheus/prometheus.yml target lists for your nodes, then:
docker compose -f observability/docker-compose.yml up -d
# Grafana:    http://<box>:3000   (admin/admin first login — change it)
# Prometheus: http://<box>:9090
```

Dashboards + datasource auto-provision on first start; no manual import.

## Deploy without Docker (bare Prometheus + Grafana)

Use `install-baremetal.sh` (installs the official apt repos, drops this dir's
config into `/etc/prometheus` and `/etc/grafana`, enables both services).

## Per-node exporter (already done on the lab)

`keinexport` is built by `make build` and installed by `make install`; the
`keinfs-exporter.service` unit ships in `packaging/templates/systemd/`. Run one
per node:

```bash
keinexport --listen 0.0.0.0:9909 --root /run/keinfs --root /var/lib/keinfs/run
```

## Metric naming

`keinfs_<service>_<name>` with stable labels: `service`, `instance`, and where
known `shard`, `target_id`, plus `rpc` / `phase` / `quantile` on the latency
families. The lifecycle phases (e.g. KMS `reserve_route_resolve`,
`reservation_cache_acquire`; KST `media_fsync`, `execution_queue_wait`) are the
authoritative per-stage decomposition of an I/O.
