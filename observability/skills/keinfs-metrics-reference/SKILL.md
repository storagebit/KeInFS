<!-- SPDX-License-Identifier: GPL-2.0-or-later -->
---
name: keinfs-metrics-reference
description: >-
  Look up KeInFS Prometheus metrics: the catalog of every keinfs_* metric
  family, its type, labels, and meaning, grouped by service (KMS, KAS, KST, KIX,
  KRS, exporter). Use when asked what a specific keinfs_ metric means, which
  metrics exist for a service, the labels on a metric, how to build a PromQL
  query against KeInFS metrics, or to find the right metric for a quantity
  (throughput, errors, cache hits, reservations, live entries, capacity, etc.).
  Authoritative list derived from the live exporter surface — do not invent
  metric names.
---

# KeInFS Metrics Reference

The metric catalog. Every series is `keinfs_<service>_<name>`. Derived from the
live `keinexport` (:9909) surface — these names are real; do not invent others.
For meaning of latency **phases** and slow-I/O triage, use the
**keinfs-io-lifecycle-debug** skill.

## Common conventions

- **Latency families** (per service) come as a trio:
  `..._microseconds{quantile="0.5|0.95|0.99|1.0"}` (gauge, `1.0` = max),
  `..._avg_microseconds` (gauge), `..._samples` (counter). Units: microseconds.
- **Stable labels:** `service`, `instance`. Plus `target_id` (KST), `shard`
  (KMS). Latency families add `rpc` (and `phase` on the phase-latency families).
- `..._requests`, `..._ops`, `..._total`, `..._errors` are **counters** — use
  `rate(...[2m])`. `..._depth`, `..._pct`, `_up`, `_count`, payload-bytes
  gauges, and `_latency_microseconds` are **gauges**.
- `keinfs_<svc>_up` = 1 if a snapshot was scraped for that instance.

---

## KST — Storage Target  (labels: `service`, `instance`, `target_id`)

Connections / streams / admission (counters unless noted):
- `keinfs_kst_active_connections`, `keinfs_kst_peak_active_connections`
- `keinfs_kst_inflight_requests`, `keinfs_kst_peak_inflight_requests`
- `keinfs_kst_total_connections_accepted`, `keinfs_kst_total_connections_rejected`
- `keinfs_kst_total_handshake_failures`
- `keinfs_kst_total_stream_rejections`,
  `keinfs_kst_read_stream_rejections`, `keinfs_kst_write_stream_rejections`
- `keinfs_kst_total_requests`, `keinfs_kst_total_errors`

Throughput (gauges; counter-like, use `rate`):
- `keinfs_kst_read_payload_bytes`, `keinfs_kst_write_payload_bytes` — bytes
  read/written. `rate(...[2m])` = B/s.
- `keinfs_kst_kp2_packed_read_requests`, `keinfs_kst_kp2_packed_write_requests`
  — packed-KP2 request counts.

Identity (gauges): `keinfs_kst_drive_id`, `keinfs_kst_numa_node`,
`keinfs_kst_pid`, `keinfs_kst_uptime_ms`, `keinfs_kst_up`.

Per-RPC (extra label `rpc` = `read|write|head|delete|stats|target-info|other`):
- `keinfs_kst_rpc_requests_total` (counter)
- `keinfs_kst_rpc_errors_total` (counter)
- `keinfs_kst_rpc_payload_bytes_total` (counter)
- `keinfs_kst_rpc_latency_microseconds` / `_avg_microseconds` / `_samples`
  — whole-RPC latency.

Per-phase (labels `rpc`, `phase`, `quantile`):
- `keinfs_kst_phase_latency_microseconds` / `_avg_microseconds` / `_samples`.
  Phases: `request_decode`, `body_stream_receive`, `body_collect`,
  `ingress_queue_wait`, `execution_queue_wait`, `route_execute`, `kix_lookup`,
  `media_write_prepare`, `media_write_io`, `media_fsync`, `media_header_validate`,
  `media_payload_read`, `media_payload_copy`, `media_crc`, `kix_publish`,
  `location_map`, `publication_retry`, `response_encode`, `response_send_headers`,
  `response_send_body`, `response_send`.

## KIX — Index  (labels: `service`, `instance`)

- `keinfs_kix_info` (gauge=1) — build/capability info; labels:
  `crc32_backend`, `crc32_accelerated`, `cpu_arch`, `rebuild_required_drives`
  (`"none"` when healthy). Alert on `rebuild_required_drives != "none"`.
- `keinfs_kix_total_live_entries` — live location-map entries (index size).
- Gets: `keinfs_kix_total_get_ops`, `keinfs_kix_total_get_hits`,
  `keinfs_kix_total_get_misses` (hit ratio = hits/ops).
- Mutations: `keinfs_kix_total_upsert_ops`, `keinfs_kix_total_delete_ops`.
- Durability: `keinfs_kix_total_append_batches`,
  `keinfs_kix_total_appended_deltas`, `keinfs_kix_total_checkpoint_ops`,
  `keinfs_kix_total_checkpoint_entries`, `keinfs_kix_total_snapshot_ops`.
- Errors / pressure: `keinfs_kix_total_write_errors`,
  `keinfs_kix_total_shard_errors`, `keinfs_kix_total_enqueue_retries`.
- Topology / identity (gauges): `keinfs_kix_shard_count`,
  `keinfs_kix_drive_count`, `keinfs_kix_pid`, `keinfs_kix_uptime_ms`,
  `keinfs_kix_started_unix_s`, `keinfs_kix_up`.
All `total_*` are counters — use `rate`.

## KMS — Metadata Service  (labels: `service`, `instance`, `shard`)

Aggregate: `keinfs_kms_total_requests`, `keinfs_kms_total_errors`,
`keinfs_kms_up`, `keinfs_kms_uptime_ms`, `keinfs_kms_started_unix_s`.

Latency:
- `keinfs_kms_rpc_latency_microseconds` / `_avg_microseconds` / `_samples`
  (label `rpc` = gRPC method) — whole-RPC.
- `keinfs_kms_phase_latency_microseconds` / `_avg_microseconds` / `_samples`
  (labels `rpc`, `phase`).
- `keinfs_kms_background_run_latency_*`, `keinfs_kms_background_release_latency_*`.
- Per-RPC counters: `keinfs_kms_rpc_requests_total`,
  `keinfs_kms_rpc_errors_total` (label `rpc`).

Reservation cache (write-scale hot path): `keinfs_kms_reservation_cache_hits`,
`_misses`, `_refills`, `_serves`, `_shard_bypasses` (counters),
`keinfs_kms_reservation_cache_depth` (gauge).

Route cache: `keinfs_kms_route_cache_hits`, `_misses`,
`keinfs_kms_route_discovery_lookups`, `keinfs_kms_route_discovery_rpcs`.

Background reaper: `keinfs_kms_background_runs`,
`keinfs_kms_background_released_reservations`, `keinfs_kms_expired_write_intents`.

Per-operation request counters (each a counter; `rate` for op rate). Object &
write path: `keinfs_kms_initiate_object_write_requests`,
`reserve_object_write_window_requests`, `commit_object_write_window_requests`,
`commit_object_write_requests`, `abort_object_write_requests`,
`repair_object_write_requests`, `resolve_object_read_requests`,
`delete_object_requests`, `get_write_intent_requests`,
`list_write_intents_requests`.
Namespace / bucket / EC: `create_namespace_requests`,
`create_namespace_entry_requests`, `get_namespace_requests`,
`list_namespaces_requests`, `list_children_requests`, `resolve_path_requests`,
`resolve_shard_requests`, `create_bucket_requests`, `get_bucket_requests`,
`list_buckets_requests`, `create_ec_profile_requests`,
`list_ec_profiles_requests`.
Placement tasks / rebalance / rebuild: `lease_placement_tasks_requests`,
`get_placement_task_requests`, `list_placement_tasks_requests`,
`commit_placement_task_requests`, `fail_placement_task_requests`,
`enqueue_target_rebalance_requests`, `preview_target_rebalance_requests`,
`get_target_placement_status_requests`, `lease_rebuild_tasks_requests`,
`commit_rebuild_requests`, `report_target_failure_requests`,
`drain_target_requests`, `recover_target_requests`, `retire_target_requests`.
Watch / events: `watch_entry_requests`, `watch_prefix_requests`,
`list_metadata_events_requests`.
(All above are `keinfs_kms_<name>`.)

## KAS — Allocator Service  (labels: `service`, `instance`)

Aggregate: `keinfs_kas_total_requests`, `keinfs_kas_total_errors`,
`keinfs_kas_up`, `keinfs_kas_uptime_ms`, `keinfs_kas_started_unix_s`.

Latency: `keinfs_kas_rpc_latency_microseconds` / `_avg_microseconds` /
`_samples` (label `rpc`); `keinfs_kas_phase_latency_microseconds` /
`_avg_microseconds` / `_samples` (labels `rpc`, `phase`);
`keinfs_kas_rpc_requests_total`, `keinfs_kas_rpc_errors_total` (label `rpc`).

Capacity: `keinfs_kas_capacity_total_granules`,
`keinfs_kas_capacity_free_granules`, `keinfs_kas_capacity_used_pct`,
`keinfs_kas_capacity_target_count` (target inventory size).

Reservations / placement (counters): `keinfs_kas_reserve_stripe_requests`,
`keinfs_kas_reserve_stripe_batch_requests`,
`keinfs_kas_reserve_rebuild_requests`,
`keinfs_kas_reserve_replacement_requests`, `keinfs_kas_get_reservation_requests`,
`keinfs_kas_list_reservations_requests`, `keinfs_kas_release_requests`,
`keinfs_kas_finalize_requests`, `keinfs_kas_reservation_reaper_runs`,
`keinfs_kas_reservation_reaper_released`.

Target inventory / lease: `keinfs_kas_register_target_requests`,
`keinfs_kas_heartbeat_requests`, `keinfs_kas_list_targets_requests`,
`keinfs_kas_set_target_state_requests`,
`keinfs_kas_reclaim_target_granules_requests`,
`keinfs_kas_get_service_instance_requests`,
`keinfs_kas_upsert_service_instance_requests`,
`keinfs_kas_list_service_instances_requests`,
`keinfs_kas_leader_renew_failures`, `keinfs_kas_fenced_commit_aborts`.

Useful KAS phases (rpc=`reserve_stripe_batch`):
`store_reserve_stripe_batch.plan_in_memory` (planning) vs
`store_reserve_stripe_batch.persist_fdb` (FoundationDB persist).

## KRS — Rebuild Daemon

KRS exposes its surface only via the runtime stat tree
`/run/keinfs/krs/krs-<pid>/summary` — **no `keinfs_krs_*` Prometheus family**
is present in the captured exporter surface. Read it directly:
```bash
cat /run/keinfs/krs/*/summary
```
Key fields (per `poc/IO_LIFECYCLE.md`): `failed_tasks` (rising = rebuild logic /
placement failing), `active_task` (stuck = read/reconstruct/replacement-write
wedged), `rebuilt_bytes` (low with rising leased tasks = repair loop alive but
ineffective), leased-task counts.

## Exporter self-metrics

- `keinfs_exporter_instances_scraped` (gauge) — service instances found/scraped.
- `keinfs_exporter_scrape_duration_seconds` (gauge) — time to build the response.

---

## Query cookbook

```bash
PROM=http://<box>:9090
q() { curl -s --data-urlencode "query=$1" "$PROM/api/v1/query" | jq -r '.data.result[]|"\(.metric)  \(.value[1])"'; }

q 'sum(rate(keinfs_kst_write_payload_bytes[2m]))'                 # cluster write B/s
q 'sum(rate(keinfs_kst_read_payload_bytes[2m]))'                  # cluster read B/s
q 'sum by (target_id) (rate(keinfs_kst_rpc_requests_total{rpc="write"}[2m]))'   # write op rate per target
q 'keinfs_kst_rpc_latency_microseconds{rpc="write",quantile="0.99"}'            # write p99 per target
q 'sum(rate(keinfs_kix_total_get_hits[2m]))/clamp_min(sum(rate(keinfs_kix_total_get_ops[2m])),1)'  # KIX get hit ratio
q 'keinfs_kms_reservation_cache_misses'                          # KMS reservation cache misses
q 'rate(keinfs_kms_route_cache_misses[2m])'                      # KMS route cache miss rate
q 'rate(keinfs_kas_reserve_stripe_requests[2m])'                 # KAS stripe reservation rate
q 'max(keinfs_kas_capacity_used_pct)'                            # fullest allocator
q 'sum(keinfs_kst_total_errors)+sum(keinfs_kms_total_errors)+sum(keinfs_kas_total_errors)'  # cluster errors
q 'count(keinfs_kix_info{rebuild_required_drives!="none"})'      # drives needing rebuild
```
