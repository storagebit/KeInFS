// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit
//
// Documented catalog of the keinfs_* metric surface. Every entry here is
// derived from the live `keinexport` metric dumps and from the naming logic in
// poc/keinexport/src/convert.rs — no metric name is invented. The catalog is
// served as the `keinfs://metric-catalog` MCP resource and underpins the
// io_lifecycle_latency phase ordering.

/** Prometheus metric type. */
export type MetricType = "counter" | "gauge";

export interface MetricDoc {
  name: string;
  type: MetricType;
  /** Labels that appear on this family (beyond service/instance). */
  labels: string[];
  /** Human meaning of the metric. */
  meaning: string;
}

/**
 * Labels every keinfs_* series carries. `shard` is present on KMS/KAS,
 * `target_id` on KST, and the latency families add `rpc`/`phase`/`quantile`.
 */
export const COMMON_LABELS = ["service", "instance"] as const;

/**
 * The documented catalog. Grouped by service for readability; the flat list is
 * exported as `METRIC_CATALOG`.
 */
const KMS_METRICS: MetricDoc[] = [
  { name: "keinfs_kms_up", type: "gauge", labels: ["shard"], meaning: "1 if a KMS snapshot was scraped for this instance." },
  { name: "keinfs_kms_uptime_ms", type: "gauge", labels: ["shard"], meaning: "Process uptime in milliseconds." },
  { name: "keinfs_kms_started_unix_s", type: "gauge", labels: ["shard"], meaning: "Process start time (unix seconds)." },
  { name: "keinfs_kms_total_requests", type: "counter", labels: ["shard"], meaning: "All gRPC requests served by this KMS shard." },
  { name: "keinfs_kms_total_errors", type: "counter", labels: ["shard"], meaning: "All gRPC errors across this KMS shard." },
  { name: "keinfs_kms_rpc_requests_total", type: "counter", labels: ["shard", "rpc"], meaning: "Requests per KMS RPC." },
  { name: "keinfs_kms_rpc_errors_total", type: "counter", labels: ["shard", "rpc"], meaning: "Errors per KMS RPC." },
  { name: "keinfs_kms_rpc_latency_microseconds", type: "gauge", labels: ["shard", "rpc", "quantile"], meaning: "Per-RPC latency percentile (quantile=0.5/0.95/0.99/1.0), microseconds." },
  { name: "keinfs_kms_rpc_latency_avg_microseconds", type: "gauge", labels: ["shard", "rpc"], meaning: "Per-RPC average latency, microseconds." },
  { name: "keinfs_kms_rpc_latency_samples", type: "counter", labels: ["shard", "rpc"], meaning: "Per-RPC latency sample count." },
  { name: "keinfs_kms_phase_latency_microseconds", type: "gauge", labels: ["shard", "rpc", "phase", "quantile"], meaning: "Per-phase latency percentile inside an RPC (the lifecycle decomposition), microseconds." },
  { name: "keinfs_kms_phase_latency_avg_microseconds", type: "gauge", labels: ["shard", "rpc", "phase"], meaning: "Per-phase average latency, microseconds." },
  { name: "keinfs_kms_phase_latency_samples", type: "counter", labels: ["shard", "rpc", "phase"], meaning: "Per-phase latency sample count." },
  { name: "keinfs_kms_reservation_cache_hits", type: "counter", labels: ["shard"], meaning: "Reservation pre-fetch cache hits on the write reserve path." },
  { name: "keinfs_kms_reservation_cache_misses", type: "counter", labels: ["shard"], meaning: "Reservation cache misses." },
  { name: "keinfs_kms_reservation_cache_refills", type: "counter", labels: ["shard"], meaning: "Reservation cache background refills." },
  { name: "keinfs_kms_reservation_cache_serves", type: "counter", labels: ["shard"], meaning: "Reservations served from cache." },
  { name: "keinfs_kms_reservation_cache_depth", type: "gauge", labels: ["shard"], meaning: "Current depth of the reservation cache." },
  { name: "keinfs_kms_reservation_cache_shard_bypasses", type: "counter", labels: ["shard"], meaning: "CRITICAL INVARIANT: must stay 0. Counts reservations that bypassed the allocation-shard owner (a placement-correctness violation)." },
  { name: "keinfs_kms_route_cache_hits", type: "counter", labels: ["shard"], meaning: "Allocation-shard route cache hits." },
  { name: "keinfs_kms_route_cache_misses", type: "counter", labels: ["shard"], meaning: "Allocation-shard route cache misses." },
  { name: "keinfs_kms_route_discovery_lookups", type: "counter", labels: ["shard"], meaning: "Route discovery lookups performed." },
  { name: "keinfs_kms_route_discovery_rpcs", type: "counter", labels: ["shard"], meaning: "Route discovery gRPC calls to KAS." },
  { name: "keinfs_kms_background_runs", type: "counter", labels: ["shard"], meaning: "Background maintenance loop runs." },
  { name: "keinfs_kms_background_released_reservations", type: "counter", labels: ["shard"], meaning: "Stale reservations released by the background loop." },
  { name: "keinfs_kms_background_run_latency_microseconds", type: "gauge", labels: ["shard", "quantile"], meaning: "Background loop run latency percentile." },
  { name: "keinfs_kms_background_release_latency_microseconds", type: "gauge", labels: ["shard", "quantile"], meaning: "Background reservation-release latency percentile." },
  { name: "keinfs_kms_expired_write_intents", type: "counter", labels: ["shard"], meaning: "Write intents that expired before commit." },
  // Per-RPC request counters (one scalar per RPC name) — full set from the dump.
  ...[
    "initiate_object_write", "commit_object_write", "commit_object_write_window", "abort_object_write",
    "reserve_object_write_window", "resolve_object_read", "delete_object", "repair_object_write",
    "create_namespace", "create_namespace_entry", "get_namespace", "list_namespaces", "list_children",
    "create_bucket", "get_bucket", "list_buckets", "create_ec_profile", "list_ec_profiles",
    "resolve_path", "resolve_shard", "watch_entry", "watch_prefix", "list_metadata_events",
    "get_write_intent", "list_write_intents", "commit_placement_task", "commit_rebuild",
    "get_placement_task", "list_placement_tasks", "lease_placement_tasks", "fail_placement_task",
    "lease_rebuild_tasks", "report_target_failure", "get_target_placement_status", "drain_target",
    "recover_target", "retire_target", "enqueue_target_rebalance", "preview_target_rebalance",
  ].map<MetricDoc>((rpc) => ({
    name: `keinfs_kms_${rpc}_requests`,
    type: "counter",
    labels: ["shard"],
    meaning: `Total ${rpc} RPCs served (per-RPC scalar counter).`,
  })),
];

const KAS_METRICS: MetricDoc[] = [
  { name: "keinfs_kas_up", type: "gauge", labels: [], meaning: "1 if a KAS snapshot was scraped for this instance." },
  { name: "keinfs_kas_uptime_ms", type: "gauge", labels: [], meaning: "Process uptime in milliseconds." },
  { name: "keinfs_kas_started_unix_s", type: "gauge", labels: [], meaning: "Process start time (unix seconds)." },
  { name: "keinfs_kas_total_requests", type: "counter", labels: [], meaning: "All gRPC requests served by KAS." },
  { name: "keinfs_kas_total_errors", type: "counter", labels: [], meaning: "All gRPC errors across KAS." },
  { name: "keinfs_kas_rpc_requests_total", type: "counter", labels: ["rpc"], meaning: "Requests per KAS RPC." },
  { name: "keinfs_kas_rpc_errors_total", type: "counter", labels: ["rpc"], meaning: "Errors per KAS RPC." },
  { name: "keinfs_kas_rpc_latency_microseconds", type: "gauge", labels: ["rpc", "quantile"], meaning: "Per-RPC latency percentile, microseconds." },
  { name: "keinfs_kas_rpc_latency_avg_microseconds", type: "gauge", labels: ["rpc"], meaning: "Per-RPC average latency, microseconds." },
  { name: "keinfs_kas_rpc_latency_samples", type: "counter", labels: ["rpc"], meaning: "Per-RPC latency sample count." },
  { name: "keinfs_kas_phase_latency_microseconds", type: "gauge", labels: ["rpc", "phase", "quantile"], meaning: "Per-phase latency percentile inside a KAS RPC, microseconds." },
  { name: "keinfs_kas_phase_latency_avg_microseconds", type: "gauge", labels: ["rpc", "phase"], meaning: "Per-phase average latency, microseconds." },
  { name: "keinfs_kas_phase_latency_samples", type: "counter", labels: ["rpc", "phase"], meaning: "Per-phase latency sample count." },
  { name: "keinfs_kas_capacity_used_pct", type: "gauge", labels: [], meaning: "Cluster allocator capacity used, percent." },
  { name: "keinfs_kas_capacity_free_granules", type: "gauge", labels: [], meaning: "Free allocation granules across all targets." },
  { name: "keinfs_kas_capacity_total_granules", type: "gauge", labels: [], meaning: "Total allocation granules across all targets." },
  { name: "keinfs_kas_capacity_target_count", type: "gauge", labels: [], meaning: "Number of targets known to the allocator." },
  { name: "keinfs_kas_fenced_commit_aborts", type: "counter", labels: [], meaning: "CRITICAL INVARIANT: commits aborted because a fenced (stale-leader) allocation shard was detected. Non-zero indicates leader-fencing events." },
  { name: "keinfs_kas_leader_renew_failures", type: "counter", labels: [], meaning: "Allocation-shard leader lease renewal failures." },
  { name: "keinfs_kas_reservation_reaper_runs", type: "counter", labels: [], meaning: "Reservation reaper loop runs." },
  { name: "keinfs_kas_reservation_reaper_released", type: "counter", labels: [], meaning: "Reservations released by the reaper." },
  ...[
    "reserve_stripe", "reserve_stripe_batch", "reserve_replacement", "reserve_rebuild",
    "finalize", "release", "get_reservation", "list_reservations", "register_target",
    "set_target_state", "list_targets", "reclaim_target_granules", "heartbeat",
    "get_service_instance", "list_service_instances", "upsert_service_instance",
  ].map<MetricDoc>((rpc) => ({
    name: `keinfs_kas_${rpc}_requests`,
    type: "counter",
    labels: [],
    meaning: `Total ${rpc} RPCs served (per-RPC scalar counter).`,
  })),
];

const KST_METRICS: MetricDoc[] = [
  { name: "keinfs_kst_up", type: "gauge", labels: ["target_id"], meaning: "1 if a KST snapshot was scraped for this target." },
  { name: "keinfs_kst_uptime_ms", type: "gauge", labels: ["target_id"], meaning: "Process uptime in milliseconds." },
  { name: "keinfs_kst_started_unix_s", type: "gauge", labels: ["target_id"], meaning: "Process start time (unix seconds)." },
  { name: "keinfs_kst_pid", type: "gauge", labels: ["target_id"], meaning: "Process id." },
  { name: "keinfs_kst_numa_node", type: "gauge", labels: ["target_id"], meaning: "NUMA node the target is pinned to." },
  { name: "keinfs_kst_drive_id", type: "gauge", labels: ["target_id"], meaning: "Backing raw-device drive id." },
  { name: "keinfs_kst_total_requests", type: "counter", labels: ["target_id"], meaning: "All KP2 requests served by this target." },
  { name: "keinfs_kst_total_errors", type: "counter", labels: ["target_id"], meaning: "All KP2 errors on this target." },
  { name: "keinfs_kst_inflight_requests", type: "gauge", labels: ["target_id"], meaning: "Currently in-flight KP2 requests." },
  { name: "keinfs_kst_peak_inflight_requests", type: "gauge", labels: ["target_id"], meaning: "Peak in-flight requests observed." },
  { name: "keinfs_kst_active_connections", type: "gauge", labels: ["target_id"], meaning: "Current HTTP/2 connections." },
  { name: "keinfs_kst_peak_active_connections", type: "gauge", labels: ["target_id"], meaning: "Peak HTTP/2 connections observed." },
  { name: "keinfs_kst_total_connections_accepted", type: "counter", labels: ["target_id"], meaning: "Connections accepted." },
  { name: "keinfs_kst_total_connections_rejected", type: "counter", labels: ["target_id"], meaning: "Connections rejected (admission)." },
  { name: "keinfs_kst_total_handshake_failures", type: "counter", labels: ["target_id"], meaning: "HTTP/2 handshake failures." },
  { name: "keinfs_kst_total_stream_rejections", type: "counter", labels: ["target_id"], meaning: "Total stream admission rejections." },
  { name: "keinfs_kst_read_stream_rejections", type: "counter", labels: ["target_id"], meaning: "Read-stream admission rejections." },
  { name: "keinfs_kst_write_stream_rejections", type: "counter", labels: ["target_id"], meaning: "Write-stream admission rejections." },
  { name: "keinfs_kst_write_payload_bytes", type: "counter", labels: ["target_id"], meaning: "Cumulative bytes written to media." },
  { name: "keinfs_kst_read_payload_bytes", type: "counter", labels: ["target_id"], meaning: "Cumulative bytes read from media." },
  { name: "keinfs_kst_kp2_packed_write_requests", type: "counter", labels: ["target_id"], meaning: "Packed-KP2 write requests served." },
  { name: "keinfs_kst_kp2_packed_read_requests", type: "counter", labels: ["target_id"], meaning: "Packed-KP2 read requests served." },
  { name: "keinfs_kst_rpc_requests_total", type: "counter", labels: ["target_id", "rpc"], meaning: "Requests per KST RPC (write/read/head/delete/stats/other)." },
  { name: "keinfs_kst_rpc_errors_total", type: "counter", labels: ["target_id", "rpc"], meaning: "Errors per KST RPC." },
  { name: "keinfs_kst_rpc_payload_bytes_total", type: "counter", labels: ["target_id", "rpc"], meaning: "Payload bytes per KST RPC." },
  { name: "keinfs_kst_rpc_latency_microseconds", type: "gauge", labels: ["target_id", "rpc", "quantile"], meaning: "Per-RPC latency percentile, microseconds." },
  { name: "keinfs_kst_rpc_latency_avg_microseconds", type: "gauge", labels: ["target_id", "rpc"], meaning: "Per-RPC average latency, microseconds." },
  { name: "keinfs_kst_rpc_latency_samples", type: "counter", labels: ["target_id", "rpc"], meaning: "Per-RPC latency sample count." },
  { name: "keinfs_kst_phase_latency_microseconds", type: "gauge", labels: ["target_id", "rpc", "phase", "quantile"], meaning: "Per-phase latency percentile inside a KST RPC (media/queue/kix decomposition), microseconds." },
  { name: "keinfs_kst_phase_latency_avg_microseconds", type: "gauge", labels: ["target_id", "rpc", "phase"], meaning: "Per-phase average latency, microseconds." },
  { name: "keinfs_kst_phase_latency_samples", type: "counter", labels: ["target_id", "rpc", "phase"], meaning: "Per-phase latency sample count." },
];

const KIX_METRICS: MetricDoc[] = [
  { name: "keinfs_kix_up", type: "gauge", labels: [], meaning: "1 if a KIX (raw-device index) snapshot was scraped for this instance." },
  { name: "keinfs_kix_info", type: "gauge", labels: ["crc32_backend", "crc32_accelerated", "cpu_arch", "rebuild_required_drives"], meaning: "KIX build/capability info gauge (value 1); string identity carried as labels." },
  { name: "keinfs_kix_uptime_ms", type: "gauge", labels: [], meaning: "Process uptime in milliseconds." },
  { name: "keinfs_kix_started_unix_s", type: "gauge", labels: [], meaning: "Process start time (unix seconds)." },
  { name: "keinfs_kix_pid", type: "gauge", labels: [], meaning: "Process id." },
  { name: "keinfs_kix_shard_count", type: "gauge", labels: [], meaning: "Number of index shards." },
  { name: "keinfs_kix_drive_count", type: "gauge", labels: [], meaning: "Number of backing drives." },
  { name: "keinfs_kix_total_live_entries", type: "counter", labels: [], meaning: "Live chunk-location entries in the in-memory map." },
  { name: "keinfs_kix_total_get_ops", type: "counter", labels: [], meaning: "Index get operations." },
  { name: "keinfs_kix_total_get_hits", type: "counter", labels: [], meaning: "Index get hits." },
  { name: "keinfs_kix_total_get_misses", type: "counter", labels: [], meaning: "Index get misses." },
  { name: "keinfs_kix_total_upsert_ops", type: "counter", labels: [], meaning: "Index upsert operations." },
  { name: "keinfs_kix_total_delete_ops", type: "counter", labels: [], meaning: "Index delete operations." },
  { name: "keinfs_kix_total_append_batches", type: "counter", labels: [], meaning: "Delta-log append batches." },
  { name: "keinfs_kix_total_appended_deltas", type: "counter", labels: [], meaning: "Delta entries appended to the persisted arena." },
  { name: "keinfs_kix_total_checkpoint_ops", type: "counter", labels: [], meaning: "Checkpoint operations." },
  { name: "keinfs_kix_total_checkpoint_entries", type: "counter", labels: [], meaning: "Entries written during checkpoints." },
  { name: "keinfs_kix_total_snapshot_ops", type: "counter", labels: [], meaning: "Snapshot operations." },
  { name: "keinfs_kix_total_enqueue_retries", type: "counter", labels: [], meaning: "Append-queue enqueue retries." },
  { name: "keinfs_kix_total_shard_errors", type: "counter", labels: [], meaning: "Per-shard errors." },
  { name: "keinfs_kix_total_write_errors", type: "counter", labels: [], meaning: "Durable-write errors." },
];

const EXPORTER_METRICS: MetricDoc[] = [
  { name: "keinfs_exporter_instances_scraped", type: "gauge", labels: [], meaning: "Number of instance snapshot trees the exporter read this scrape." },
  { name: "keinfs_exporter_scrape_duration_seconds", type: "gauge", labels: [], meaning: "Time the exporter spent reading runtime trees this scrape." },
];

/** Flat documented catalog, grouped logically but exported as one list. */
export const METRIC_CATALOG: MetricDoc[] = [
  ...KMS_METRICS,
  ...KAS_METRICS,
  ...KST_METRICS,
  ...KIX_METRICS,
  ...EXPORTER_METRICS,
];

/** Services recognized by this server, in lifecycle order. */
export const SERVICES = ["kms", "kas", "kst", "kix", "krs"] as const;
export type Service = (typeof SERVICES)[number];

/**
 * Return catalog entries for a given service prefix (kms/kas/kst/kix), or the
 * full catalog when no service is supplied.
 */
export function catalogForService(service?: string): MetricDoc[] {
  if (!service) return METRIC_CATALOG;
  const prefix = `keinfs_${service}_`;
  return METRIC_CATALOG.filter((m) => m.name.startsWith(prefix));
}
