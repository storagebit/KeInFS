#!/usr/bin/env node
// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit
//
// KeInFS Observability MCP server.
//
// Exposes a live KeInFS object-storage cluster to an LLM over the Model Context
// Protocol (stdio transport — the form Claude Code registers). It is a read-only
// lens over the central Prometheus that scrapes every node's `keinexport`
// sidecar; it never touches a KeInFS data or control path.
//
// Tools:
//   query_prometheus        instant PromQL query
//   query_prometheus_range  range PromQL query
//   list_metrics            list keinfs_* metric names (optionally per service)
//   cluster_health          synthesized healthy/degraded/critical verdict
//   io_lifecycle_latency    phase-by-phase latency of a write/read I/O (flagship)
//   top_phases              the N slowest phases cluster-wide by p99
//   list_targets            per-KST-target status table
//
// Resource:
//   keinfs://metric-catalog documented metric catalog (name/type/labels/meaning)

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

import { PrometheusClient, PrometheusError } from "./prometheus.js";
import {
  METRIC_CATALOG,
  catalogForService,
  SERVICES,
} from "./catalog.js";
import {
  lifecycleFor,
  type Direction,
  type LifecyclePhase,
} from "./lifecycle.js";

const PROMETHEUS_URL = process.env.PROMETHEUS_URL ?? "http://localhost:9090";
const prom = new PrometheusClient(PROMETHEUS_URL);

// ---------------------------------------------------------------------------
// Result helpers
// ---------------------------------------------------------------------------

/** Wrap a JSON-able value as an MCP text content result. */
function jsonResult(value: unknown) {
  return {
    content: [{ type: "text" as const, text: JSON.stringify(value, null, 2) }],
  };
}

/** Wrap an error as an MCP error result (isError=true). */
function errorResult(message: string) {
  return {
    isError: true,
    content: [{ type: "text" as const, text: message }],
  };
}

/** Run a handler, turning PrometheusError / unexpected errors into clean results. */
async function guard(fn: () => Promise<ReturnType<typeof jsonResult>>) {
  try {
    return await fn();
  } catch (err) {
    if (err instanceof PrometheusError) return errorResult(err.message);
    return errorResult(
      `unexpected error: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/** Round a number to at most `d` decimals, leaving integers clean. */
function round(n: number, d = 2): number {
  const f = 10 ** d;
  return Math.round(n * f) / f;
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

const server = new McpServer({
  name: "keinfs-observability",
  version: "0.1.0",
});

// --- Tool 1: query_prometheus ----------------------------------------------
server.registerTool(
  "query_prometheus",
  {
    title: "Query Prometheus (instant)",
    description:
      "Run an instant PromQL query against the KeInFS central Prometheus and " +
      "return the result series as {labels, value}. Use for point-in-time " +
      "questions (current rate, latest gauge, etc.). Metric names are " +
      "keinfs_<service>_<name>; see the keinfs://metric-catalog resource.",
    inputSchema: {
      query: z.string().describe("PromQL expression, e.g. sum(rate(keinfs_kst_write_payload_bytes[1m]))"),
      time: z
        .string()
        .optional()
        .describe("Optional evaluation time (RFC3339 or unix seconds). Defaults to now."),
    },
  },
  async ({ query, time }) =>
    guard(async () => {
      const res = await prom.instant(query, time);
      const series = res.result.map((s) => ({
        labels: s.metric,
        value: s.value?.[1] !== undefined ? Number(s.value[1]) : null,
        timestamp: s.value?.[0],
      }));
      return jsonResult({
        query,
        resultType: res.resultType,
        count: series.length,
        series,
      });
    }),
);

// --- Tool 2: query_prometheus_range ----------------------------------------
server.registerTool(
  "query_prometheus_range",
  {
    title: "Query Prometheus (range)",
    description:
      "Run a range PromQL query and return time-series matrices. Use to see how " +
      "a metric evolved over a window (throughput trends, latency over time).",
    inputSchema: {
      query: z.string().describe("PromQL expression."),
      start: z.string().describe("Start time (RFC3339 or unix seconds)."),
      end: z.string().describe("End time (RFC3339 or unix seconds)."),
      step: z
        .string()
        .describe("Resolution step, e.g. '15s', '1m', '5m'. Keep the point count reasonable."),
    },
  },
  async ({ query, start, end, step }) =>
    guard(async () => {
      const res = await prom.range(query, start, end, step);
      const series = res.result.map((s) => ({
        labels: s.metric,
        points: s.values.map(([t, v]) => ({ t, v: Number(v) })),
      }));
      return jsonResult({
        query,
        resultType: res.resultType,
        count: series.length,
        series,
      });
    }),
);

// --- Tool 3: list_metrics --------------------------------------------------
server.registerTool(
  "list_metrics",
  {
    title: "List KeInFS metric names",
    description:
      "List the keinfs_* metric names currently present in Prometheus, " +
      "optionally filtered to one service (kms|kas|kst|kix|krs).",
    inputSchema: {
      service: z
        .enum(SERVICES)
        .optional()
        .describe("Restrict to one service prefix. Omit for all keinfs_* metrics."),
    },
  },
  async ({ service }) =>
    guard(async () => {
      // Pull all __name__ values, then filter to the keinfs_ namespace.
      const names = await prom.labelValues("__name__");
      const prefix = service ? `keinfs_${service}_` : "keinfs_";
      const filtered = names.filter((n) => n.startsWith(prefix)).sort();
      return jsonResult({
        service: service ?? "all",
        count: filtered.length,
        metrics: filtered,
      });
    }),
);

// --- Tool 4: cluster_health ------------------------------------------------
server.registerTool(
  "cluster_health",
  {
    title: "KeInFS cluster health",
    description:
      "Synthesize a high-level health verdict (healthy | degraded | critical) " +
      "from several PromQL rollups: services up per type, total errors, " +
      "capacity used %, the two correctness invariants (reservation shard " +
      "bypasses and fenced commit aborts — both must be 0), KST targets up, " +
      "KIX instances up.",
    inputSchema: {},
  },
  async () =>
    guard(async () => {
      // Fire the rollup queries concurrently.
      const [
        kmsUp,
        kasUp,
        kstUp,
        kixUp,
        kmsErrors,
        kasErrors,
        kstErrors,
        capacityPct,
        shardBypasses,
        fenceAborts,
        kixWriteErrors,
        kixShardErrors,
        kixRebuild,
      ] = await Promise.all([
        prom.scalar("sum(keinfs_kms_up)"),
        prom.scalar("sum(keinfs_kas_up)"),
        prom.scalar("sum(keinfs_kst_up)"),
        prom.scalar("sum(keinfs_kix_up)"),
        prom.scalar("sum(keinfs_kms_total_errors)"),
        prom.scalar("sum(keinfs_kas_total_errors)"),
        prom.scalar("sum(keinfs_kst_total_errors)"),
        prom.scalar("max(keinfs_kas_capacity_used_pct)"),
        prom.scalar("sum(keinfs_kms_reservation_cache_shard_bypasses)"),
        prom.scalar("sum(keinfs_kas_fenced_commit_aborts)"),
        prom.scalar("sum(keinfs_kix_total_write_errors)"),
        prom.scalar("sum(keinfs_kix_total_shard_errors)"),
        // count of KIX instances reporting rebuild_required != "none"
        prom.scalar('count(keinfs_kix_info{rebuild_required_drives!="none"}) or vector(0)'),
      ]);

      const reasons: string[] = [];
      let verdict: "healthy" | "degraded" | "critical" = "healthy";
      const escalate = (to: "degraded" | "critical", reason: string) => {
        reasons.push(reason);
        if (to === "critical") verdict = "critical";
        else if (verdict !== "critical") verdict = "degraded";
      };

      const n = (v: number | null) => v ?? 0;

      // Liveness — no instance of a service type up at all is critical.
      if (n(kmsUp) === 0) escalate("critical", "no KMS instance is up");
      if (n(kasUp) === 0) escalate("critical", "no KAS instance is up");
      if (n(kstUp) === 0) escalate("critical", "no KST target is up");

      // Correctness invariants — these must be exactly 0.
      if (n(shardBypasses) > 0)
        escalate(
          "critical",
          `reservation_cache_shard_bypasses=${n(shardBypasses)} (placement-correctness invariant violated; must be 0)`,
        );
      if (n(fenceAborts) > 0)
        escalate(
          "critical",
          `fenced_commit_aborts=${n(fenceAborts)} (stale-leader fencing events; investigate allocation-shard leadership)`,
        );

      // Capacity.
      const cap = capacityPct === null ? null : round(n(capacityPct), 2);
      if (cap !== null) {
        if (cap >= 95) escalate("critical", `capacity used ${cap}% (>=95%)`);
        else if (cap >= 85) escalate("degraded", `capacity used ${cap}% (>=85%)`);
      }

      // Rebuild required.
      if (n(kixRebuild) > 0)
        escalate("degraded", `${n(kixRebuild)} KIX instance(s) report rebuild_required drives`);

      // Error totals (cumulative counters — surfaced, escalates to degraded if non-zero).
      const totalErrors = n(kmsErrors) + n(kasErrors) + n(kstErrors);
      const totalKixErrors = n(kixWriteErrors) + n(kixShardErrors);
      if (totalKixErrors > 0)
        escalate("degraded", `KIX errors present (write=${n(kixWriteErrors)}, shard=${n(kixShardErrors)})`);

      if (reasons.length === 0) reasons.push("all checks nominal");

      return jsonResult({
        verdict,
        reasons,
        prometheus: prom.url,
        services_up: {
          kms: n(kmsUp),
          kas: n(kasUp),
          kst_targets: n(kstUp),
          kix_instances: n(kixUp),
        },
        capacity_used_pct: cap,
        invariants: {
          reservation_cache_shard_bypasses: n(shardBypasses),
          fenced_commit_aborts: n(fenceAborts),
          ok: n(shardBypasses) === 0 && n(fenceAborts) === 0,
        },
        errors: {
          kms_total: n(kmsErrors),
          kas_total: n(kasErrors),
          kst_total: n(kstErrors),
          combined_control_data: totalErrors,
          kix_write: n(kixWriteErrors),
          kix_shard: n(kixShardErrors),
        },
        kix_rebuild_required_instances: n(kixRebuild),
      });
    }),
);

// --- Tool 5: io_lifecycle_latency (flagship) -------------------------------

/**
 * Build a `quantile` selector for a phase_latency family and fetch the p50/p99
 * across the cluster. We take the max across instances so the answer reflects
 * the worst place time is being spent right now, then label which instance.
 */
async function phaseQuantile(
  service: "kms" | "kas" | "kst",
  rpc: string,
  phase: string,
  quantile: "0.5" | "0.99",
): Promise<{ value: number | null; worst_instance: string | null }> {
  const metric = `keinfs_${service}_phase_latency_microseconds`;
  const sel = `${metric}{rpc="${rpc}",phase="${phase}",quantile="${quantile}"}`;
  // Worst (max) across instances, and which instance it was.
  const res = await prom.instant(
    `topk(1, ${sel})`,
  );
  if (!res.result.length) return { value: null, worst_instance: null };
  const top = res.result[0];
  const v = top.value?.[1] !== undefined ? Number(top.value[1]) : null;
  const inst = top.metric.instance ?? top.metric.target_id ?? null;
  return { value: v, worst_instance: inst };
}

server.registerTool(
  "io_lifecycle_latency",
  {
    title: "I/O lifecycle latency breakdown",
    description:
      "FLAGSHIP: return the phase-by-phase latency of a single write or read I/O " +
      "as it travels through the stack (KMS reserve/resolve phases -> KST " +
      "execution + media phases -> KMS commit phases for write), as an ordered " +
      "list with p50/p99 (microseconds) per phase, plus the worst instance for " +
      "each. Answers 'where is time spent in an I/O right now'. The p99 column " +
      "is the worst-instance p99 across the cluster; phases with no samples are " +
      "reported as null.",
    inputSchema: {
      direction: z.enum(["write", "read"]).describe("Which I/O direction to decompose."),
    },
  },
  async ({ direction }) =>
    guard(async () => {
      const phases: LifecyclePhase[] = lifecycleFor(direction as Direction);
      // Fetch p50 and p99 for every phase concurrently.
      const rows = await Promise.all(
        phases.map(async (p, idx) => {
          const [p50, p99] = await Promise.all([
            phaseQuantile(p.service, p.rpc, p.phase, "0.5"),
            phaseQuantile(p.service, p.rpc, p.phase, "0.99"),
          ]);
          return {
            order: idx + 1,
            service: p.service,
            rpc: p.rpc,
            phase: p.phase,
            description: p.description,
            p50_us: p50.value === null ? null : round(p50.value),
            p99_us: p99.value === null ? null : round(p99.value),
            worst_instance: p99.worst_instance ?? p50.worst_instance,
          };
        }),
      );

      // A naive sum-of-p99s gives a rough "where the budget goes" view. It is
      // not a true critical-path total (phases overlap and percentiles don't
      // add), so we surface it only as guidance, not a measured E2E latency.
      const measured = rows.filter((r) => r.p99_us !== null);
      const p99Budget = round(
        measured.reduce((acc, r) => acc + (r.p99_us ?? 0), 0),
      );
      const slowest = [...measured]
        .sort((a, b) => (b.p99_us ?? 0) - (a.p99_us ?? 0))
        .slice(0, 5)
        .map((r) => ({ service: r.service, rpc: r.rpc, phase: r.phase, p99_us: r.p99_us }));

      return jsonResult({
        direction,
        prometheus: prom.url,
        note:
          "p50/p99 are per-phase percentiles taken from the worst (topk-1) " +
          "instance per phase, in microseconds. sum_of_phase_p99_us is an " +
          "approximate budget, NOT a measured end-to-end latency (phases " +
          "overlap; percentiles are not additive).",
        phase_count: rows.length,
        phases_with_samples: measured.length,
        sum_of_phase_p99_us: p99Budget,
        top5_slowest_phases: slowest,
        phases: rows,
      });
    }),
);

// --- Tool 6: top_phases ----------------------------------------------------
server.registerTool(
  "top_phases",
  {
    title: "Top slowest phases (hotspots)",
    description:
      "Return the N slowest phases across the whole cluster right now, ranked by " +
      "p99 latency (microseconds). Spans KMS, KAS and KST phase_latency families. " +
      "Use to find where the cluster is spending the most time per stage.",
    inputSchema: {
      n: z
        .number()
        .int()
        .min(1)
        .max(50)
        .default(10)
        .describe("How many phases to return (default 10)."),
      service: z
        .enum(["kms", "kas", "kst"])
        .optional()
        .describe("Restrict to one service's phases. Omit to span all three."),
    },
  },
  async ({ n, service }) =>
    guard(async () => {
      const families = (service ? [service] : (["kms", "kas", "kst"] as const)).map(
        (s) => `keinfs_${s}_phase_latency_microseconds{quantile="0.99"}`,
      );
      // Union the selected families, then take the global top-N by value.
      const expr = `topk(${n}, ${families.join(" or ")})`;
      const res = await prom.instant(expr);
      const phases = res.result
        .map((s) => ({
          service: s.metric.service,
          rpc: s.metric.rpc,
          phase: s.metric.phase,
          instance: s.metric.instance,
          target_id: s.metric.target_id,
          shard: s.metric.shard,
          p99_us: s.value?.[1] !== undefined ? round(Number(s.value[1])) : null,
        }))
        .sort((a, b) => (b.p99_us ?? 0) - (a.p99_us ?? 0));
      return jsonResult({
        prometheus: prom.url,
        requested: n,
        service: service ?? "all",
        count: phases.length,
        top_phases: phases,
      });
    }),
);

// --- Tool 7: list_targets --------------------------------------------------
server.registerTool(
  "list_targets",
  {
    title: "List KST targets",
    description:
      "Per-KST-target status table: up, current write/read throughput (B/s over " +
      "the last minute), in-flight requests, active connections, total errors, " +
      "stream rejections. One row per storage target.",
    inputSchema: {
      window: z
        .string()
        .default("1m")
        .describe("Rate window for throughput, e.g. '1m', '5m' (default 1m)."),
    },
  },
  async ({ window }) =>
    guard(async () => {
      const w = window || "1m";
      // Pull each per-target series; key everything by target_id.
      const [up, writeBps, readBps, inflight, conns, errors, rejections] =
        await Promise.all([
          prom.instant("keinfs_kst_up"),
          prom.instant(`sum by (target_id) (rate(keinfs_kst_write_payload_bytes[${w}]))`),
          prom.instant(`sum by (target_id) (rate(keinfs_kst_read_payload_bytes[${w}]))`),
          prom.instant("keinfs_kst_inflight_requests"),
          prom.instant("keinfs_kst_active_connections"),
          prom.instant("keinfs_kst_total_errors"),
          prom.instant("keinfs_kst_total_stream_rejections"),
        ]);

      // Build a target_id -> row map.
      type Row = {
        target_id: string;
        instance: string | null;
        up: number;
        write_bps: number;
        read_bps: number;
        inflight: number;
        active_connections: number;
        total_errors: number;
        stream_rejections: number;
      };
      const rows = new Map<string, Row>();
      const ensure = (tid: string, instance?: string): Row => {
        let r = rows.get(tid);
        if (!r) {
          r = {
            target_id: tid,
            instance: instance ?? null,
            up: 0,
            write_bps: 0,
            read_bps: 0,
            inflight: 0,
            active_connections: 0,
            total_errors: 0,
            stream_rejections: 0,
          };
          rows.set(tid, r);
        }
        if (instance && !r.instance) r.instance = instance;
        return r;
      };
      const val = (s: { value?: [number, string] }) =>
        s.value?.[1] !== undefined ? Number(s.value[1]) : 0;

      for (const s of up.result) {
        const r = ensure(s.metric.target_id ?? "?", s.metric.instance);
        r.up = val(s);
      }
      for (const s of writeBps.result) ensure(s.metric.target_id ?? "?").write_bps = round(val(s));
      for (const s of readBps.result) ensure(s.metric.target_id ?? "?").read_bps = round(val(s));
      for (const s of inflight.result) ensure(s.metric.target_id ?? "?", s.metric.instance).inflight = val(s);
      for (const s of conns.result) ensure(s.metric.target_id ?? "?", s.metric.instance).active_connections = val(s);
      for (const s of errors.result) ensure(s.metric.target_id ?? "?", s.metric.instance).total_errors = val(s);
      for (const s of rejections.result) ensure(s.metric.target_id ?? "?", s.metric.instance).stream_rejections = val(s);

      const table = [...rows.values()].sort((a, b) =>
        a.target_id.localeCompare(b.target_id),
      );
      return jsonResult({
        prometheus: prom.url,
        window: w,
        target_count: table.length,
        targets_up: table.filter((r) => r.up === 1).length,
        targets: table,
      });
    }),
);

// --- Resource: keinfs://metric-catalog -------------------------------------
server.registerResource(
  "metric-catalog",
  "keinfs://metric-catalog",
  {
    title: "KeInFS metric catalog",
    description:
      "Documented catalog of the keinfs_* metric surface (name, type, labels, " +
      "meaning), derived from the live keinexport dumps and convert.rs. Read " +
      "this to learn what to query.",
    mimeType: "application/json",
  },
  async (uri) => ({
    contents: [
      {
        uri: uri.href,
        mimeType: "application/json",
        text: JSON.stringify(
          {
            description:
              "KeInFS Prometheus metric catalog. Naming: keinfs_<service>_<name>. " +
              "Common labels: service, instance. KMS/KAS add 'shard', KST adds " +
              "'target_id'. Latency families add 'rpc', 'phase', 'quantile' " +
              "(0.5/0.95/0.99/1.0). Latency values are in microseconds.",
            common_labels: ["service", "instance"],
            services: SERVICES,
            metric_count: METRIC_CATALOG.length,
            metrics: METRIC_CATALOG,
          },
          null,
          2,
        ),
      },
    ],
  }),
);

// Also expose a per-service catalog helper as a tool-callable convenience is
// unnecessary; catalogForService is used by the resource indirectly. Keep the
// import meaningful by exporting it for testing.
export { catalogForService };

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  // Stderr is safe for diagnostics; stdout is the MCP channel.
  process.stderr.write(
    `[keinfs-observability-mcp] connected (PROMETHEUS_URL=${PROMETHEUS_URL})\n`,
  );
}

main().catch((err) => {
  process.stderr.write(
    `[keinfs-observability-mcp] fatal: ${err instanceof Error ? err.stack : String(err)}\n`,
  );
  process.exit(1);
});
