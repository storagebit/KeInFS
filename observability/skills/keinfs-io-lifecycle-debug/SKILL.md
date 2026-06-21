<!-- SPDX-License-Identifier: GPL-2.0-or-later -->
---
name: keinfs-io-lifecycle-debug
description: >-
  Diagnose a slow or low-throughput I/O in a KeInFS cluster by walking the
  per-phase latency map. Use when a KeInFS write or read is slow, throughput is
  low, p99/tail latency is high, an object PUT/GET is stalling, writes feel
  durability-bound, reads feel media-bound, or someone asks "where is the time
  going / which phase is the bottleneck". Encodes the IO_LIFECYCLE phase
  decomposition (KMS reserve / KST media_fsync, execution_queue_wait,
  media_payload_read / KIX kix_publish, etc.) and gives exact PromQL and stat-
  file commands per phase plus what a healthy value looks like.
---

# KeInFS I/O Lifecycle Debug

Flagship troubleshooting skill. A KeInFS I/O is decomposed into named **phases**
exposed as `keinfs_<svc>_phase_latency_microseconds{rpc=...,phase=...,quantile=...}`
(and in the `/run/keinfs/.../summary` & `rpcs/*` stat files). Find which phase
dominates, then act. Canonical reference: `poc/IO_LIFECYCLE.md`.

## How to read latency

- Latency is in **microseconds**. Quantiles are `0.5`, `0.95`, `0.99`, `1.0`
  (`1.0` = max). Inspect `0.99` for tail problems, `0.5` for steady state.
- Phase latency carries `rpc` + `phase`. On KST the data-path RPCs are
  `rpc="write"` and `rpc="read"` (also `head`, `delete`, `stats`, `target-info`,
  `other`). On KMS/KAS the `rpc` is the gRPC method.
- Pull the per-target / per-phase view from a dashboard or PromQL. The
  `keinfs-io-drilldown` (per-phase, templated) and `keinfs-kst-detail` (one
  target, full phase decomposition) dashboards already chart all of this.

```bash
PROM=http://<box>:9090
ph() { curl -s --data-urlencode "query=max by (phase) (keinfs_kst_phase_latency_microseconds{rpc=\"$1\",quantile=\"0.99\"})" \
        "$PROM/api/v1/query" | jq -r '.data.result[] | "\(.metric.phase)\t\(.value[1])"' | sort -t$'\t' -k2 -n -r; }
ph write    # rank WRITE phases by p99 µs, worst first
ph read     # rank READ phases by p99 µs, worst first
```
Or straight off a node (no Prometheus):
```bash
cat /run/keinfs/kst/*/rpcs/write     # write phase breakdown
cat /run/keinfs/kst/*/rpcs/read      # read phase breakdown
cat /run/keinfs/kst/*/summary
```

## Symptom -> phase map

### Slow / low-throughput WRITES

Rank `rpc="write"` phases. The dominant server-side write costs today are
`media_fsync`, `kix_publish`, `media_write_io` (per IO_LIFECYCLE). Match the
top phase:

| Top phase | Means | Healthy | Action |
|-----------|-------|---------|--------|
| `media_fsync` | raw-media `fdatasync` durability cost | ~1 µs on fast NVMe (the write-scale fix) | If high: the drive / its durability path. Check `kix-detail` drive append latency, NVMe health, write cache. |
| `kix_publish` | publishing the new LocationRecord into KIX (incl. superseded-owner delete) | low single-digit µs | If high: KIX is the write tax. Check KIX shard/drive append latency (`kix-detail`), `keinfs_kix_total_write_errors`. |
| `media_write_io` | direct-I/O payload write to chunk media | low | If high: raw write path / drive bandwidth saturated. |
| `media_write_prepare` | slot-header + buffer prep | negligible | Rarely dominates; if so, CPU/alloc pressure. |
| `execution_queue_wait` | residence in the direct write execution group | ~0 when not saturated | If high: write workers saturated or undersized — admission saturation, scale workers / reduce concurrency. |
| `body_stream_receive` | time for the 1 MiB body to arrive over HTTP/2 | low on a fast link | If high: network / client send is slow (check KSC `send_body`). |
| `ingress_queue_wait` | legacy buffered ingress queue | ~0 on the direct fast path | Non-zero here means you fell off the direct fast path. |

### Slow / low-throughput READS

Rank `rpc="read"` phases. Dominant read costs today are `media_payload_read`,
`media_header_validate`, `media_payload_copy`. KIX lookup is background noise on
the read fast path.

| Top phase | Means | Healthy | Action |
|-----------|-------|---------|--------|
| `media_payload_read` | raw read of the chunk payload from media | the bulk; bounded by drive read BW | If high beyond drive limits: contention / queue depth on the drive. |
| `media_header_validate` | validate slot header + record identity | low | If high: media validation overhead / CPU. |
| `media_payload_copy` | copy payload bytes into the result buffer | low | If high: memory bandwidth / NUMA placement. |
| `media_crc` | recompute CRC for validation | low (HW-accelerated; see `keinfs_kix_info` crc32 backend) | If high & crc backend isn't accelerated, that's the cause. |
| `kix_lookup` | in-memory live-index lookup | effectively noise | If high with low media cost: **KIX is suspect** (see below). |
| `publication_retry` | read caught a stale publication lane mid-write churn and retried | **0** | Non-zero = live read/write churn on hot keys; expected under heavy mixed load, but persistent high values signal publication-lane thrash. |
| `execution_queue_wait` | read execution group residence | ~0 | If high: read workers saturated/undersized. |

### KIX is the suspect (high kix_lookup or kix_publish)

```bash
cat /run/keinfs/kix/*/summary
cat /run/keinfs/kix/*/shards/*      # per-shard, drive append latency
cat /run/keinfs/kix/*/drives/*
# PromQL:
curl -s --data-urlencode 'query=sum(rate(keinfs_kix_total_get_hits[2m]))/clamp_min(sum(rate(keinfs_kix_total_get_ops[2m])),1)' "$PROM/api/v1/query"  # get hit ratio
curl -s --data-urlencode 'query=keinfs_kix_total_write_errors' "$PROM/api/v1/query"
curl -s --data-urlencode 'query=keinfs_kix_info{rebuild_required_drives!="none"}' "$PROM/api/v1/query"
```
- Write errors or `rebuild_required_drives != "none"` -> target is alive but
  structurally compromised.
- Drive append-latency spikes -> raw KIX arena durability is part of the write
  problem.

### Object-write path stalling (KSC <-> KMS/KAS), not the fragment path

If fragment-level KST phases look fine but object PUTs are slow, the cost is in
the control plane. The IO_LIFECYCLE write-scale fix made the KMS reserve phases
~1 µs at p50; if `reserve_*` dominates, suspect the cache/route path.

```bash
cat /run/keinfs/kms/*/summary
# KMS reserve phases (initiate_object_write):
curl -s --data-urlencode 'query=max by (phase) (keinfs_kms_phase_latency_microseconds{rpc="initiate_object_write",quantile="0.99"})' "$PROM/api/v1/query" \
  | jq -r '.data.result[]|"\(.metric.phase)\t\(.value[1])"' | sort -t$'\t' -k2 -nr
```
Phase interpretation (rpc `initiate_object_write`):
- `reservation_cache_acquire`, `reserve_cache_hit`/`reserve_cache_miss`,
  `reserve_cache_wait_for_refill`, `reserve_cache_direct_reserve` — the
  reservation cache. Healthy: hit at ~1 µs. If `reserve_cache_miss` /
  `wait_for_refill` dominate, the cache is starved -> refill rate / shard
  bypasses. Check:
  ```bash
  curl -s --data-urlencode 'query=keinfs_kms_reservation_cache_hits' "$PROM/api/v1/query"
  curl -s --data-urlencode 'query=keinfs_kms_reservation_cache_misses' "$PROM/api/v1/query"
  curl -s --data-urlencode 'query=keinfs_kms_reservation_cache_depth' "$PROM/api/v1/query"
  curl -s --data-urlencode 'query=sum(keinfs_kms_reservation_cache_shard_bypasses)' "$PROM/api/v1/query"
  ```
- `reserve_route_resolve` — resolving the route to the right shard/allocator.
  If this dominates, the **route cache** is the path; check
  `keinfs_kms_route_cache_hits` vs `keinfs_kms_route_cache_misses` and
  `keinfs_kms_route_discovery_rpcs`.
- `bucket_context_*`, `ec_profile_catalog_*`, `object_parent_*` — metadata
  lookups; the `*_cache_miss` variants going hot mean cold metadata caches.

KMS-level health signals (from `summary` / counters):
- rising `keinfs_kms_initiate_object_write_requests` with failing commits ->
  object path stalling between reservation and publish.
- rising `keinfs_kms_expired_write_intents` -> clients abandoning/timing out.
- rising `report_target_failure` + `lease_rebuild_tasks` -> active repair.

### Placement / allocator is the choke (KAS)

```bash
cat /run/keinfs/kas/*/summary
curl -s --data-urlencode 'query=max by (phase) (keinfs_kas_phase_latency_microseconds{rpc="reserve_stripe_batch",quantile="0.99"})' "$PROM/api/v1/query" \
  | jq -r '.data.result[]|"\(.metric.phase)\t\(.value[1])"' | sort -t$'\t' -k2 -nr
```
- `store_reserve_stripe_batch.persist_fdb` dominating -> FoundationDB write
  latency is the placement tax (vs `.plan_in_memory` = planning).
- high `keinfs_kas_reserve_stripe_requests` with rising errors -> placement is
  the choke point.
- `keinfs_kas_capacity_used_pct` near full / falling `capacity_free_granules`
  -> running out of space to reserve.
- high register/heartbeat churn -> unstable target inventory.

### Rebuild path slow (KRS)

```bash
cat /run/keinfs/krs/*/summary
```
- rising `failed_tasks` -> rebuild logic or replacement placement failing.
- `active_task` stuck -> fragment read / reconstruct / replacement write wedged.
- low `rebuilt_bytes` with rising leased tasks -> repair loop alive but
  ineffective.

### Client-side (KSC) — rule the client out first

```bash
cat <ksc-stats-root>/latency
cat <ksc-stats-root>/phases/write
cat <ksc-stats-root>/phases/read
```
- high `ready_wait` -> stream readiness / local session pressure.
- high `send_body` -> client body transmission cost (or network).
- high `wait_response` -> the server-side work above is real, or RTT — go to KST.
- high `collect_response` / `payload_validate` -> client-side collection / check.

## Procedure (do this in order)

1. **Confirm the symptom & scope.** Cluster-wide vs one target?
   `keinfs-io-lifecycle` overview for the funnel; `keinfs-kst-overview` health
   table to spot a single bad target.
2. **Rank phases** for the affected `rpc` (`ph write` / `ph read` above, or the
   `rpcs/*` stat file). The dominant phase is the bottleneck.
3. **Jump to the matching row** in the tables above; pull the deeper signal
   with the listed command.
4. **Compare to the baseline.** Validated single-target direct 1 MiB on the lab:
   read ~4100 MiB/s, write ~2833 MiB/s, 70/30 mixed ~2714 read + ~1162 write.
   Far below that with no single dominant phase -> suspect admission
   (`execution_queue_wait`, stream rejections) or the client.
5. **Check rejections / saturation** if no phase dominates:
   ```bash
   curl -s --data-urlencode 'query=keinfs_kst_total_stream_rejections' "$PROM/api/v1/query"
   curl -s --data-urlencode 'query=keinfs_kst_inflight_requests' "$PROM/api/v1/query"
   curl -s --data-urlencode 'query=keinfs_kst_total_handshake_failures' "$PROM/api/v1/query"
   ```
