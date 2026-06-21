// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit
//
// DAY-2 OPERATIONS tooling for the KeInFS observability MCP server.
//
// Where the base server is a read-only lens over Prometheus, this module turns
// the server into the operator's *reasoning copilot* for hardware replacement,
// node maintenance, and topology changes. Two classes of tool live here:
//
//   ADVISORY / READ  — synthesize current cluster state from Prometheus and
//                       emit an ordered, copy-pasteable runbook of `keinctl`
//                       commands (as strings, NOT executed) plus the metrics to
//                       watch between steps. Always safe.
//
//   MUTATING         — shell out to `keinctl`. Gated behind an explicit opt-in
//                       env var KEINFS_MCP_ALLOW_MUTATIONS=1 (default OFF). When
//                       OFF, the tool returns the exact argv it WOULD run and
//                       executes nothing. When ON, it runs `keinctl` via
//                       child_process.execFile (array args, NO shell) with a
//                       hard timeout and returns the structured result.
//
// Every `keinctl` subcommand/flag emitted here is taken verbatim from
// poc/keinctl/src/main.rs — no flag is invented. Mutating keinctl commands
// require `--confirm` (keinctl's own gate); we always include it on the
// mutating argv. Some replacement steps (physically swapping the drive,
// formatting the new raw device for KIX) are out-of-band and are surfaced as
// manual steps, never as fabricated keinctl subcommands.

import { execFile } from "node:child_process";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";

import { PrometheusClient, PrometheusError } from "./prometheus.js";

// ---------------------------------------------------------------------------
// Configuration (env)
// ---------------------------------------------------------------------------

/** Path to the keinctl binary (override for non-PATH installs). */
const KEINCTL_BIN = process.env.KEINCTL_BIN ?? "keinctl";

/** Optional keinctl context name; emitted as `--context <ctx>` when set. */
const KEINCTL_CONTEXT = process.env.KEINCTL_CONTEXT ?? "";

/** Mutations are OFF unless this is exactly "1". */
function mutationsAllowed(): boolean {
  return process.env.KEINFS_MCP_ALLOW_MUTATIONS === "1";
}

/** Hard cap on how long a keinctl child may run. */
const KEINCTL_TIMEOUT_MS = 30_000;

// ---------------------------------------------------------------------------
// Result helpers (mirrors index.ts so output shape is identical)
// ---------------------------------------------------------------------------

function jsonResult(value: unknown) {
  return {
    content: [{ type: "text" as const, text: JSON.stringify(value, null, 2) }],
  };
}

function errorResult(message: string) {
  return {
    isError: true,
    content: [{ type: "text" as const, text: message }],
  };
}

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

function round(n: number, d = 2): number {
  const f = 10 ** d;
  return Math.round(n * f) / f;
}

// ---------------------------------------------------------------------------
// keinctl argv construction + execution
// ---------------------------------------------------------------------------

/**
 * Build the full keinctl argv for a subcommand. The leading global flags
 * (`--context`, `--format json`) are prepended; `--confirm` is appended by the
 * caller for mutating commands. Returns the array passed to execFile — never a
 * shell string, so there is no interpolation/injection surface.
 */
function keinctlArgs(sub: string[], opts: { json?: boolean } = {}): string[] {
  const args: string[] = [];
  if (KEINCTL_CONTEXT) args.push("--context", KEINCTL_CONTEXT);
  if (opts.json !== false) args.push("--format", "json");
  args.push(...sub);
  return args;
}

/** Render an argv as a single human-readable command line (display only). */
function renderCommand(args: string[]): string {
  const quote = (a: string) => (/[\s"']/.test(a) ? JSON.stringify(a) : a);
  return [KEINCTL_BIN, ...args].map(quote).join(" ");
}

interface ExecResult {
  ran: boolean;
  command: string;
  argv: string[];
  exitCode: number | null;
  stdout: string;
  stderr: string;
  parsedJson?: unknown;
  timedOut?: boolean;
}

/** Promise wrapper over execFile with a hard timeout. Never throws. */
function execKeinctl(args: string[]): Promise<ExecResult> {
  const command = renderCommand(args);
  return new Promise((resolve) => {
    execFile(
      KEINCTL_BIN,
      args,
      { timeout: KEINCTL_TIMEOUT_MS, maxBuffer: 8 * 1024 * 1024 },
      (err, stdout, stderr) => {
        const out = stdout?.toString() ?? "";
        const errOut = stderr?.toString() ?? "";
        const result: ExecResult = {
          ran: true,
          command,
          argv: [KEINCTL_BIN, ...args],
          exitCode: err && typeof err.code === "number" ? err.code : err ? null : 0,
          stdout: out,
          stderr: errOut,
        };
        if (err && (err as NodeJS.ErrnoException & { killed?: boolean }).killed) {
          result.timedOut = true;
        }
        // keinctl --format json prints structured output on stdout; attach it
        // when it parses so the LLM gets typed fields, not just a string blob.
        const trimmed = out.trim();
        if (trimmed && (trimmed.startsWith("{") || trimmed.startsWith("["))) {
          try {
            result.parsedJson = JSON.parse(trimmed);
          } catch {
            /* leave stdout as-is */
          }
        }
        resolve(result);
      },
    );
  });
}

/**
 * The shared body for a mutating tool: when mutations are disabled, return the
 * exact argv that WOULD run and execute nothing; otherwise run keinctl and
 * return the structured result.
 */
async function runOrPreview(args: string[]) {
  const command = renderCommand(args);
  if (!mutationsAllowed()) {
    return jsonResult({
      executed: false,
      reason:
        "mutations disabled (set KEINFS_MCP_ALLOW_MUTATIONS=1 to enable). " +
        "The command below was NOT run.",
      wouldRun: command,
      argv: [KEINCTL_BIN, ...args],
    });
  }
  const result = await execKeinctl(args);
  return jsonResult({
    executed: true,
    ok: result.exitCode === 0,
    command: result.command,
    exitCode: result.exitCode,
    timedOut: result.timedOut ?? false,
    stdout: result.stdout,
    stderr: result.stderr,
    parsed: result.parsedJson,
  });
}

// ---------------------------------------------------------------------------
// Prometheus state probes used by the advisory tools
// ---------------------------------------------------------------------------

/**
 * Fetch the live, per-target view this module reasons over. Pulls liveness,
 * in-flight load, throughput and (cluster-wide) allocator capacity. `target_id`
 * is the join key; instance is carried for the runbook.
 */
interface TargetSnapshot {
  target_id: string;
  instance: string | null;
  up: number;
  inflight: number;
  write_bps: number;
  read_bps: number;
  total_errors: number;
}

async function snapshotTargets(
  prom: PrometheusClient,
  window: string,
): Promise<TargetSnapshot[]> {
  const [up, inflight, writeBps, readBps, errors] = await Promise.all([
    prom.instant("keinfs_kst_up"),
    prom.instant("keinfs_kst_inflight_requests"),
    prom.instant(`sum by (target_id) (rate(keinfs_kst_write_payload_bytes[${window}]))`),
    prom.instant(`sum by (target_id) (rate(keinfs_kst_read_payload_bytes[${window}]))`),
    prom.instant("keinfs_kst_total_errors"),
  ]);
  const rows = new Map<string, TargetSnapshot>();
  const ensure = (tid: string, instance?: string): TargetSnapshot => {
    let r = rows.get(tid);
    if (!r) {
      r = {
        target_id: tid,
        instance: instance ?? null,
        up: 0,
        inflight: 0,
        write_bps: 0,
        read_bps: 0,
        total_errors: 0,
      };
      rows.set(tid, r);
    }
    if (instance && !r.instance) r.instance = instance;
    return r;
  };
  const val = (s: { value?: [number, string] }) =>
    s.value?.[1] !== undefined ? Number(s.value[1]) : 0;
  for (const s of up.result) ensure(s.metric.target_id ?? "?", s.metric.instance).up = val(s);
  for (const s of inflight.result)
    ensure(s.metric.target_id ?? "?", s.metric.instance).inflight = val(s);
  for (const s of writeBps.result)
    ensure(s.metric.target_id ?? "?").write_bps = round(val(s));
  for (const s of readBps.result)
    ensure(s.metric.target_id ?? "?").read_bps = round(val(s));
  for (const s of errors.result)
    ensure(s.metric.target_id ?? "?", s.metric.instance).total_errors = val(s);
  return [...rows.values()].sort((a, b) => a.target_id.localeCompare(b.target_id));
}

/** A single ordered runbook step. */
interface RunbookStep {
  step: number;
  title: string;
  /** keinctl command(s) to run for this step, or [] for a manual/out-of-band step. */
  commands: string[];
  /** True when the step happens outside keinctl (swap drive, format raw device). */
  manual?: boolean;
  /** Metrics / PromQL to watch before proceeding to the next step. */
  watch: string[];
}

/** Number-stamp a list of step drafts in order. */
function numberSteps(steps: Omit<RunbookStep, "step">[]): RunbookStep[] {
  return steps.map((s, i) => ({ step: i + 1, ...s }));
}

// ---------------------------------------------------------------------------
// Tool registration
// ---------------------------------------------------------------------------

/**
 * Register all day-2 operations tools on the given server. `prom` is the shared
 * Prometheus client created in index.ts.
 */
export function registerDayOpsTools(server: McpServer, prom: PrometheusClient): void {
  // --- Tool: plan_target_replacement ---------------------------------------
  server.registerTool(
    "plan_target_replacement",
    {
      title: "Plan a drive/target replacement",
      description:
        "ADVISORY (read-only): produce the ordered runbook to replace a " +
        "failing/failed KST target's drive — drain -> wait for rebuild/migration " +
        "-> retire -> physically swap the drive -> format the new raw device for " +
        "KIX -> re-register -> recover/activate. Emits the exact keinctl commands " +
        "per step (as strings, NOT executed) and the metrics to watch between " +
        "steps. The plan is tailored to the target's current state pulled from " +
        "Prometheus.",
      inputSchema: {
        target_id: z
          .string()
          .describe("Target to replace, e.g. 'target-00'."),
        window: z
          .string()
          .default("1m")
          .describe("Rate window for the load snapshot, e.g. '1m', '5m'."),
      },
    },
    async ({ target_id, window }) =>
      guard(async () => {
        const w = window || "1m";
        const targets = await snapshotTargets(prom, w);
        const self = targets.find((t) => t.target_id === target_id) ?? null;
        const kixRebuild = await prom.scalar(
          'count(keinfs_kix_info{rebuild_required_drives!="none"}) or vector(0)',
        );

        // Tailor the opening note to what we can see about the target.
        let condition: string;
        if (!self) {
          condition =
            "target not visible in keinfs_kst_up — it is likely already down/" +
            "unreachable. Proceed straight to drain (KMS will evacuate its " +
            "fragments) then retire.";
        } else if (self.up !== 1) {
          condition = "target is reporting up=0 (down). Drain then retire.";
        } else if (self.inflight > 0 || self.write_bps > 0 || self.read_bps > 0) {
          condition =
            "target is still serving I/O. Drain will mark it Draining and " +
            "KMS will migrate its fragments before you retire.";
        } else {
          condition = "target is up but idle. Safe to drain immediately.";
        }

        const ctxNote = KEINCTL_CONTEXT
          ? `(commands include --context ${KEINCTL_CONTEXT})`
          : "(no KEINCTL_CONTEXT set; commands run against the default context)";

        const steps = numberSteps([
          {
            title: "Confirm current target state and cluster health",
            commands: [
              renderCommand(keinctlArgs(["target", "show", target_id])),
              renderCommand(keinctlArgs(["cluster", "status"])),
            ],
            watch: [
              `keinfs_kst_up{target_id="${target_id}"}`,
              "cluster_health verdict",
            ],
          },
          {
            title:
              "Drain the target (KMS marks it Draining and enqueues migration/" +
              "rebuild of its fragments to other targets in its shard)",
            commands: [
              renderCommand(keinctlArgs(["target", "drain", target_id]).concat("--confirm")),
            ],
            watch: [
              "keinctl placement summary  (pending_rebuild / pending_rebalance climbing then draining to 0)",
              `keinfs_kix_info{rebuild_required_drives!="none"}  (currently ${kixRebuild ?? 0} instance(s))`,
            ],
          },
          {
            title:
              "Wait for migration/rebuild to finish before removing the drive " +
              "(do NOT pull the drive while fragments are still being rebuilt off it)",
            commands: [
              renderCommand(keinctlArgs(["placement", "wait"])),
              renderCommand(keinctlArgs(["placement", "summary"])),
            ],
            watch: [
              "sum(rate(keinfs_krs_rebuilt_bytes[1m]))  (KRS rebuild throughput; climbs then settles)",
              "keinfs_krs_rebuilt_tasks / keinfs_krs_failed_tasks  (if KRS exports them)",
              "placement pending_* all 0",
            ],
          },
          {
            title:
              "Retire the target (removes it from allocation; it will no longer " +
              "receive placements)",
            commands: [
              renderCommand(keinctlArgs(["target", "retire", target_id]).concat("--confirm")),
            ],
            watch: [
              `keinfs_kst_up{target_id="${target_id}"}  (target stops being scraped)`,
              "cluster status: targets.retired increments",
            ],
          },
          {
            title:
              "Physically swap the failed drive (out-of-band; one physical " +
              "drive = one target, raw block device, no filesystem)",
            commands: [],
            manual: true,
            watch: [],
          },
          {
            title:
              "Format/prepare the new raw device for KIX (out-of-band drive-prep " +
              "step on the storage host — done with the node's KST/KIX drive-init " +
              "tooling, NOT a keinctl subcommand). Verify a clean KIX arena before " +
              "re-registering.",
            commands: [],
            manual: true,
            watch: [
              "keinfs_kix_up for the node (1 after the new arena comes online)",
              'keinfs_kix_info{rebuild_required_drives}  (should read "none" for the fresh drive)',
            ],
          },
          {
            title:
              "Re-register the replacement target into the same allocation shard. " +
              "Fill in the real endpoint/server/rack/shard/granule values for the " +
              "new drive (placeholders shown).",
            commands: [
              renderCommand(
                keinctlArgs([
                  "target",
                  "register",
                  "--target-id",
                  target_id,
                  "--endpoint",
                  "http://target-host.example.internal:18080",
                  "--server-id",
                  "server-00",
                  "--rack-id",
                  "rack-00",
                  "--allocation-shard-id",
                  "shard-00",
                  "--granule-count",
                  "<GRANULES_ON_NEW_DRIVE>",
                  "--lifecycle-state",
                  "active",
                ]).concat("--confirm"),
              ),
            ],
            watch: [
              `keinfs_kst_up{target_id="${target_id}"} == 1`,
              "keinfs_kas_capacity_free_granules increases by the new drive's granules",
              "keinfs_kas_capacity_target_count increments",
            ],
          },
          {
            title:
              "Recover/activate the target (clears Draining/Unhealthy; returns it " +
              "to the active allocation set)",
            commands: [
              renderCommand(keinctlArgs(["target", "recover", target_id]).concat("--confirm")),
              renderCommand(keinctlArgs(["target", "show", target_id])),
            ],
            watch: [
              "cluster status: targets.active increments, draining/unhealthy back to baseline",
              `keinfs_kst_up{target_id="${target_id}"} == 1, in-flight/throughput resuming`,
            ],
          },
        ]);

        return jsonResult({
          tool: "plan_target_replacement",
          target_id,
          prometheus: prom.url,
          context_note: ctxNote,
          observed: self
            ? {
                up: self.up,
                inflight: self.inflight,
                write_bps: self.write_bps,
                read_bps: self.read_bps,
                total_errors: self.total_errors,
                instance: self.instance,
              }
            : "target not present in metrics (down/unknown)",
          condition,
          kix_rebuild_required_instances: kixRebuild ?? 0,
          safety:
            "Steps with manual=true are out-of-band (physical swap, raw-device " +
            "format). keinctl commands are emitted as strings and are NOT run by " +
            "this tool. Mutating keinctl commands carry --confirm.",
          runbook: steps,
        });
      }),
  );

  // --- Tool: plan_node_maintenance -----------------------------------------
  server.registerTool(
    "plan_node_maintenance",
    {
      title: "Plan node maintenance (drain whole node)",
      description:
        "ADVISORY (read-only): drain every KST target on a node for maintenance " +
        "(kernel/firmware/hardware) and bring it back safely. Given the target " +
        "ids on the node, emits the ordered drain -> maintain -> recover runbook " +
        "with the keinctl commands per target (NOT executed) and the metrics to " +
        "watch. Drains happen one target at a time so the shard keeps enough " +
        "redundancy in flight.",
      inputSchema: {
        target_ids: z
          .array(z.string())
          .min(1)
          .describe("All KST target ids hosted on the node, e.g. ['target-00','target-01']."),
      },
    },
    async ({ target_ids }) =>
      guard(async () => {
        const targets = await snapshotTargets(prom, "1m");
        const known = new Set(targets.map((t) => t.target_id));
        const missing = target_ids.filter((t) => !known.has(t));

        const drainSteps = target_ids.map((tid) => ({
          title: `Drain ${tid} and wait for its fragments to migrate/rebuild`,
          commands: [
            renderCommand(keinctlArgs(["target", "drain", tid]).concat("--confirm")),
            renderCommand(keinctlArgs(["placement", "wait"])),
          ],
          watch: [
            "placement pending_rebuild/pending_rebalance -> 0 before the next drain",
            `keinfs_kst_up{target_id="${tid}"}`,
          ],
        }));

        const recoverSteps = target_ids.map((tid) => ({
          title: `Recover/activate ${tid} after the node is back`,
          commands: [
            renderCommand(keinctlArgs(["target", "recover", tid]).concat("--confirm")),
            renderCommand(keinctlArgs(["target", "show", tid])),
          ],
          watch: [
            `keinfs_kst_up{target_id="${tid}"} == 1`,
            "cluster status: targets.active back to baseline",
          ],
        }));

        const steps = numberSteps([
          {
            title: "Snapshot cluster health and capacity headroom before draining",
            commands: [
              renderCommand(keinctlArgs(["cluster", "status"])),
              renderCommand(keinctlArgs(["target", "list"])),
            ],
            watch: [
              "cluster_health verdict == healthy",
              "keinfs_kas_capacity_used_pct (ensure headroom to absorb the node's data)",
            ],
          },
          ...drainSteps,
          {
            title:
              "Perform node maintenance (out-of-band: reboot/firmware/hardware). " +
              "All listed targets are Draining/Retired-from-allocation at this point.",
            commands: [],
            manual: true,
            watch: ["node returns; KST processes restart and begin reporting keinfs_kst_up"],
          },
          ...recoverSteps,
          {
            title: "Final verification",
            commands: [
              renderCommand(keinctlArgs(["cluster", "status"])),
              renderCommand(keinctlArgs(["target", "list"])),
            ],
            watch: [
              "all node targets up=1",
              "placement pending_* == 0; no new failed tasks",
            ],
          },
        ]);

        return jsonResult({
          tool: "plan_node_maintenance",
          target_ids,
          prometheus: prom.url,
          targets_not_in_metrics: missing,
          note:
            "Drain targets ONE AT A TIME and wait for placement to quiesce between " +
            "each, so the allocation shard never loses too much redundancy at once.",
          safety:
            "keinctl commands are emitted as strings, NOT executed. Mutating " +
            "commands carry --confirm.",
          runbook: steps,
        });
      }),
  );

  // --- Tool: plan_ip_change / plan_topology_change -------------------------
  const topologyHandler = (kind: "ip" | "topology") =>
    async ({
      target_ids,
      old_endpoint,
      new_endpoint,
    }: {
      target_ids: string[];
      old_endpoint?: string;
      new_endpoint?: string;
    }) =>
      guard(async () => {
        const reRegister = target_ids.map((tid) => ({
          title:
            `Re-register ${tid} with its new endpoint into the SAME allocation ` +
            "shard (keep allocation-shard-id/server/rack identical; only the " +
            "endpoint changes). Fill in the real shard/server/rack/granule values.",
          commands: [
            renderCommand(
              keinctlArgs([
                "target",
                "register",
                "--target-id",
                tid,
                "--endpoint",
                new_endpoint ?? "http://new-host.example.internal:18080",
                "--server-id",
                "server-00",
                "--rack-id",
                "rack-00",
                "--allocation-shard-id",
                "shard-00",
                "--granule-count",
                "<GRANULES>",
                "--lifecycle-state",
                "active",
              ]).concat("--confirm"),
            ),
          ],
          watch: [
            `keinfs_kst_up{target_id="${tid}"} == 1 at the new endpoint`,
            "target list shows the updated endpoint",
          ],
        }));

        const steps = numberSteps([
          {
            title: "Capture the current topology and the targets affected",
            commands: [
              renderCommand(keinctlArgs(["cluster", "topology"])),
              renderCommand(keinctlArgs(["target", "list"])),
            ],
            watch: [
              old_endpoint
                ? `confirm which target_ids currently point at ${old_endpoint}`
                : "confirm the target_ids whose endpoint is changing",
            ],
          },
          ...reRegister,
          {
            title:
              "Refresh KMS/KAS shard route caches. KMS caches allocation-shard " +
              "routes; after the endpoint/shard topology moves, restart KAS first " +
              "(it owns the shard leases) THEN KMS (so its route cache re-discovers " +
              "the new endpoints). This is the known 'restart KAS then KMS to " +
              "refresh shard routes' settle step.",
            commands: [],
            manual: true,
            watch: [
              "keinfs_kms_route_cache_misses briefly rises then settles (re-discovery)",
              "keinfs_kms_route_discovery_rpcs increments (KMS re-querying KAS)",
              "keinfs_kms_reservation_cache_shard_bypasses stays 0 (must never be non-zero)",
            ],
          },
          {
            title:
              "Update the Prometheus scrape config / file_sd so the keinexport " +
              "sidecar at the new node address is scraped (edit the prometheus " +
              "targets file, e.g. file_sd_configs *.json, then reload Prometheus). " +
              "Out-of-band edit on the observability box.",
            commands: [],
            manual: true,
            watch: [
              "up{job=\"keinexport\"} == 1 for the new address in Prometheus",
              "keinfs_exporter_instances_scraped reflects the moved node",
            ],
          },
          {
            title: "Verify end to end",
            commands: [
              renderCommand(keinctlArgs(["cluster", "status"])),
              renderCommand(keinctlArgs(["cluster", "topology"])),
            ],
            watch: [
              "cluster_health verdict == healthy",
              "all affected targets up=1; placement clean",
            ],
          },
        ]);

        return jsonResult({
          tool: kind === "ip" ? "plan_ip_change" : "plan_topology_change",
          target_ids,
          old_endpoint: old_endpoint ?? null,
          new_endpoint: new_endpoint ?? null,
          prometheus: prom.url,
          config_to_edit: [
            "keinctl context (kms/kas endpoints + kst_http_endpoints) if the control-plane address moved",
            "Prometheus scrape config / file_sd targets for the keinexport sidecar (:9909)",
          ],
          settle_step:
            "Restart KAS first, then KMS, to refresh allocation-shard route caches.",
          safety:
            "keinctl commands are emitted as strings, NOT executed. Mutating " +
            "commands carry --confirm. The KAS/KMS restart and Prometheus edit are " +
            "manual out-of-band steps.",
          runbook: steps,
        });
      });

  server.registerTool(
    "plan_ip_change",
    {
      title: "Plan a node IP/endpoint change",
      description:
        "ADVISORY (read-only): the procedure when a node's IP/endpoint changes — " +
        "re-register its targets to the right allocation shard at the new " +
        "endpoint, refresh the KMS/KAS shard route caches (restart KAS then KMS), " +
        "and update the Prometheus scrape config. Emits the ordered runbook + the " +
        "config to edit; commands are strings, NOT executed.",
      inputSchema: {
        target_ids: z
          .array(z.string())
          .min(1)
          .describe("Target ids on the node whose endpoint is changing."),
        old_endpoint: z
          .string()
          .optional()
          .describe("Current endpoint, e.g. 'http://old-host.example.internal:18080'."),
        new_endpoint: z
          .string()
          .optional()
          .describe("New endpoint, e.g. 'http://new-host.example.internal:18080'."),
      },
    },
    topologyHandler("ip"),
  );

  server.registerTool(
    "plan_topology_change",
    {
      title: "Plan a topology change",
      description:
        "ADVISORY (read-only): same procedure as plan_ip_change for broader " +
        "topology moves (node relocated, endpoints re-addressed) — re-register " +
        "targets, refresh KMS/KAS route caches (restart KAS then KMS), update the " +
        "Prometheus scrape config. Runbook only; nothing is executed.",
      inputSchema: {
        target_ids: z
          .array(z.string())
          .min(1)
          .describe("Target ids whose placement/endpoint is moving."),
        old_endpoint: z
          .string()
          .optional()
          .describe("Old endpoint placeholder, e.g. 'http://old-host.example.internal:18080'."),
        new_endpoint: z
          .string()
          .optional()
          .describe("New endpoint placeholder, e.g. 'http://new-host.example.internal:18080'."),
      },
    },
    topologyHandler("topology"),
  );

  // --- Tool: capacity_forecast ---------------------------------------------
  server.registerTool(
    "capacity_forecast",
    {
      title: "Capacity forecast (time-to-full)",
      description:
        "ADVISORY (read-only): project time-to-full from current allocator " +
        "capacity used % and the cluster write byte-rate. Uses " +
        "keinfs_kas_capacity_used_pct + keinfs_kas_capacity_free_granules and " +
        "rate(keinfs_kst_write_payload_bytes[window]). Pure Prometheus math; no " +
        "side effects.",
      inputSchema: {
        window: z
          .string()
          .default("10m")
          .describe("Rate window for the write byte-rate, e.g. '10m', '1h'."),
      },
    },
    async ({ window }) =>
      guard(async () => {
        const w = window || "10m";
        const [usedPct, freeGran, totalGran, writeBps] = await Promise.all([
          prom.scalar("max(keinfs_kas_capacity_used_pct)"),
          prom.scalar("sum(keinfs_kas_capacity_free_granules)"),
          prom.scalar("sum(keinfs_kas_capacity_total_granules)"),
          prom.scalar(`sum(rate(keinfs_kst_write_payload_bytes[${w}]))`),
        ]);

        const used = usedPct === null ? null : round(usedPct, 2);
        const free = freeGran ?? null;
        const total = totalGran ?? null;
        const bps = writeBps ?? 0;

        // Granule size is not exported, so the byte-rate model projects against
        // the free *fraction*: time_to_full ≈ free_fraction * total_capacity /
        // write_rate. We can only express total capacity in bytes if we knew the
        // granule size; instead we project the % path: how long until used hits
        // 100% given the current % and the rate of % growth is unknown from a
        // single sample. So we provide two views: (a) a %-headroom statement and
        // (b) a byte-rate-vs-free-granule ratio that needs the granule size to
        // become a wall-clock ETA. We surface both honestly.
        const freeFraction =
          used === null ? null : round(Math.max(0, 100 - used) / 100, 4);

        // If the operator-side granule size is known it can be passed via the
        // forecast; absent it we report the inputs and a %-based caution.
        const projection: Record<string, unknown> = {
          capacity_used_pct: used,
          free_fraction: freeFraction,
          free_granules: free,
          total_granules: total,
          write_bytes_per_sec: round(bps),
        };

        // Best-effort wall-clock ETA *iff* we can derive bytes-per-granule from
        // total bytes — which we cannot from these metrics alone. Be explicit.
        let etaNote: string;
        if (used === null) {
          etaNote =
            "keinfs_kas_capacity_used_pct not present — cannot forecast. Check KAS is up.";
        } else if (bps <= 0) {
          etaNote =
            "no write traffic over the window — capacity is not currently growing; " +
            "time-to-full is effectively unbounded at the present rate.";
        } else if (free !== null && total !== null && total > 0) {
          etaNote =
            `at the current write rate the cluster is consuming capacity; ` +
            `${free} of ${total} granules are free (${freeFraction! * 100}% headroom). ` +
            "A wall-clock ETA needs the per-granule byte size (not exported here); " +
            "to get a precise ETA, divide free-byte capacity by " +
            "write_bytes_per_sec. Watch keinfs_kas_capacity_used_pct trend with " +
            "query_prometheus_range to read the slope directly.";
        } else {
          etaNote =
            "capacity counters incomplete; use query_prometheus_range on " +
            "keinfs_kas_capacity_used_pct to read the fill slope.";
        }

        return jsonResult({
          tool: "capacity_forecast",
          prometheus: prom.url,
          window: w,
          projection,
          eta_note: etaNote,
          recommended_followup:
            `query_prometheus_range("max(keinfs_kas_capacity_used_pct)", start, end, "${w}") ` +
            "to read the fill slope and extrapolate to 100%.",
          thresholds: { warn_pct: 85, critical_pct: 95 },
        });
      }),
  );

  // --- Tool: drain_readiness -----------------------------------------------
  server.registerTool(
    "drain_readiness",
    {
      title: "Drain readiness verdict",
      description:
        "ADVISORY (read-only): assess whether a target is safe to drain right " +
        "now. Checks the target's in-flight load and whether the OTHER targets " +
        "have capacity/headroom to absorb its data. Returns a ready/caution/" +
        "blocked verdict with reasons. Read-only — runs no mutation.",
      inputSchema: {
        target_id: z.string().describe("Target you intend to drain, e.g. 'target-00'."),
        window: z.string().default("1m").describe("Rate window for the load read."),
      },
    },
    async ({ target_id, window }) =>
      guard(async () => {
        const w = window || "1m";
        const [targets, usedPct, freeGran, totalGran] = await Promise.all([
          snapshotTargets(prom, w),
          prom.scalar("max(keinfs_kas_capacity_used_pct)"),
          prom.scalar("sum(keinfs_kas_capacity_free_granules)"),
          prom.scalar("sum(keinfs_kas_capacity_total_granules)"),
        ]);
        const self = targets.find((t) => t.target_id === target_id) ?? null;
        const others = targets.filter((t) => t.target_id !== target_id && t.up === 1);

        const reasons: string[] = [];
        type Verdict = "ready" | "caution" | "blocked";
        const v: { verdict: Verdict } = { verdict: "ready" };
        const escalate = (to: "caution" | "blocked", reason: string) => {
          reasons.push(reason);
          if (to === "blocked") v.verdict = "blocked";
          else if (v.verdict !== "blocked") v.verdict = "caution";
        };

        if (!self) {
          escalate(
            "caution",
            "target not visible in metrics (down/unknown) — draining is fine but " +
              "verify the right target_id with `keinctl target list`.",
          );
        } else {
          if (self.up !== 1) reasons.push("target is down (up=0): draining will evacuate its fragments.");
          if (self.inflight > 0)
            escalate(
              "caution",
              `target has ${self.inflight} in-flight request(s); draining now will ` +
                "interrupt them (clients retry, but expect a latency blip).",
            );
          if (self.total_errors > 0)
            reasons.push(`target has ${self.total_errors} cumulative errors (informational).`);
        }

        // Headroom on the rest of the cluster to absorb this target's data.
        const used = usedPct === null ? null : round(usedPct, 2);
        if (used !== null && used >= 95)
          escalate(
            "blocked",
            `cluster capacity_used_pct=${used}% (>=95%): the remaining targets may ` +
              "not have room to absorb the drained target's fragments. Add capacity " +
              "or reclaim first.",
          );
        else if (used !== null && used >= 85)
          escalate("caution", `cluster capacity_used_pct=${used}% (>=85%): headroom is tight.`);

        if (others.length === 0)
          escalate(
            "blocked",
            "no other targets are up to receive the drained fragments — draining " +
              "now would lose redundancy. Bring up peers first.",
          );
        else if (others.length < 2)
          escalate(
            "caution",
            `only ${others.length} other target(s) up; redundancy headroom is thin during the drain.`,
          );

        if (reasons.length === 0) reasons.push("no blockers — safe to drain.");

        return jsonResult({
          tool: "drain_readiness",
          target_id,
          prometheus: prom.url,
          verdict: v.verdict,
          reasons,
          observed: self
            ? { up: self.up, inflight: self.inflight, total_errors: self.total_errors }
            : "target not present in metrics",
          peers_up: others.length,
          cluster_capacity_used_pct: used,
          free_granules: freeGran ?? null,
          total_granules: totalGran ?? null,
          next_step:
            v.verdict === "blocked"
              ? "resolve the blocker(s) above before draining."
              : `when ready: ${renderCommand(
                  keinctlArgs(["target", "drain", target_id]).concat("--confirm"),
                )}`,
        });
      }),
  );

  // --- Tool: rebuild_status ------------------------------------------------
  server.registerTool(
    "rebuild_status",
    {
      title: "Rebuild status",
      description:
        "ADVISORY (read-only): synthesize rebuild progress across the cluster — " +
        "KIX rebuild_required drives, KRS rebuilt bytes/tasks and failed tasks " +
        "(when KRS exports them), and pending rebuild placement tasks. Tells you " +
        "whether a rebuild is in progress, making progress, or stuck.",
      inputSchema: {
        window: z
          .string()
          .default("1m")
          .describe("Rate window for rebuild throughput, e.g. '1m', '5m'."),
      },
    },
    async ({ window }) =>
      guard(async () => {
        const w = window || "1m";
        const [
          kixRebuildInstances,
          rebuiltBytesRate,
          rebuiltTasks,
          failedTasks,
          krsUp,
        ] = await Promise.all([
          prom.scalar('count(keinfs_kix_info{rebuild_required_drives!="none"}) or vector(0)'),
          // KRS metric families are optional in the catalog; `or vector(0)`
          // keeps the query well-defined when KRS is not exporting yet.
          prom.scalar(`sum(rate(keinfs_krs_rebuilt_bytes[${w}])) or vector(0)`),
          prom.scalar("sum(keinfs_krs_rebuilt_tasks) or vector(0)"),
          prom.scalar("sum(keinfs_krs_failed_tasks) or vector(0)"),
          prom.scalar("sum(keinfs_krs_up) or vector(0)"),
        ]);

        // Which KIX instances still flag a rebuild, and on which drives.
        const kixInfo = await prom.instant(
          'keinfs_kix_info{rebuild_required_drives!="none"}',
        );
        const rebuildDrives = kixInfo.result.map((s) => ({
          instance: s.metric.instance ?? null,
          rebuild_required_drives: s.metric.rebuild_required_drives ?? null,
        }));

        const bps = rebuiltBytesRate ?? 0;
        const rebuildingInstances = kixRebuildInstances ?? 0;

        let state: "idle" | "in_progress" | "progressing" | "stalled";
        const notes: string[] = [];
        if (rebuildingInstances === 0) {
          state = "idle";
          notes.push("no KIX instance reports rebuild_required drives.");
        } else if (bps > 0) {
          state = "progressing";
          notes.push(
            `rebuild active: ${rebuildingInstances} KIX instance(s) need rebuild and ` +
              `KRS is moving ${round(bps)} B/s.`,
          );
        } else if ((krsUp ?? 0) > 0) {
          state = "in_progress";
          notes.push(
            "KIX reports rebuild_required but KRS rebuilt-byte rate is ~0 — either " +
              "between tasks or KRS does not export rebuilt_bytes. Watch placement " +
              "pending_rebuild and KRS task counters.",
          );
        } else {
          state = "stalled";
          notes.push(
            "KIX needs rebuild but no KRS instance is up / no rebuild throughput — " +
              "rebuild is not progressing. Check that KRS is running and leasing tasks.",
          );
        }
        if ((failedTasks ?? 0) > 0)
          notes.push(`KRS reports ${failedTasks} failed rebuild task(s) — investigate.`);

        return jsonResult({
          tool: "rebuild_status",
          prometheus: prom.url,
          window: w,
          state,
          kix_rebuild_required_instances: rebuildingInstances,
          kix_rebuild_drives: rebuildDrives,
          krs: {
            up: krsUp ?? 0,
            rebuilt_bytes_per_sec: round(bps),
            rebuilt_tasks_total: rebuiltTasks ?? 0,
            failed_tasks_total: failedTasks ?? 0,
            note:
              "KRS metric families are optional; values default to 0 when KRS is " +
              "not exporting them. Cross-check with `keinctl placement summary`.",
          },
          notes,
          followup: renderCommand(keinctlArgs(["placement", "summary"])),
        });
      }),
  );

  // --- Tool: drain_target (MUTATING, gated) --------------------------------
  server.registerTool(
    "drain_target",
    {
      title: "Drain a target (MUTATING, gated)",
      description:
        "MUTATING — gated behind KEINFS_MCP_ALLOW_MUTATIONS=1 (default OFF). " +
        "Runs `keinctl target drain <target_id> --confirm`, which marks the " +
        "target Draining and enqueues migration/rebuild of its fragments. When " +
        "mutations are disabled this returns the exact command it WOULD run and " +
        "executes nothing.",
      inputSchema: {
        target_id: z.string().describe("Target to drain, e.g. 'target-00'."),
      },
    },
    async ({ target_id }) =>
      guard(async () => {
        const args = keinctlArgs(["target", "drain", target_id]).concat("--confirm");
        return runOrPreview(args);
      }),
  );

  // --- Tool: set_target_state (MUTATING, gated) ----------------------------
  server.registerTool(
    "set_target_state",
    {
      title: "Set target lifecycle state (MUTATING, gated)",
      description:
        "MUTATING — gated behind KEINFS_MCP_ALLOW_MUTATIONS=1 (default OFF). Move " +
        "a target between lifecycle states. keinctl exposes the transitions as " +
        "subcommands, so this maps state -> command: Draining => `target drain`, " +
        "Active => `target recover`, Retired => `target retire` (all with " +
        "--confirm). When mutations are disabled it returns the command it WOULD " +
        "run and executes nothing.",
      inputSchema: {
        target_id: z.string().describe("Target id, e.g. 'target-00'."),
        state: z
          .enum(["Active", "Draining", "Retired"])
          .describe(
            "Desired lifecycle state. Active->recover, Draining->drain, Retired->retire.",
          ),
      },
    },
    async ({ target_id, state }) =>
      guard(async () => {
        const sub =
          state === "Draining"
            ? ["target", "drain", target_id]
            : state === "Retired"
              ? ["target", "retire", target_id]
              : ["target", "recover", target_id]; // Active
        const args = keinctlArgs(sub).concat("--confirm");
        return runOrPreview(args);
      }),
  );

  // --- Tool: preview_rebalance (read-only dry-run, no gate) -----------------
  server.registerTool(
    "preview_rebalance",
    {
      title: "Preview a target rebalance (dry-run)",
      description:
        "SAFE dry-run: runs `keinctl target rebalance-preview` (the KMS " +
        "preview_target_rebalance RPC) to show what a rebalance WOULD move, " +
        "without enqueuing anything. Always allowed (no mutation gate). When " +
        "mutations are disabled it still executes if KEINCTL is reachable, since " +
        "the preview is non-mutating; if you prefer it never shells out, leave " +
        "mutations off and read the returned `wouldRun`.",
      inputSchema: {
        source_target_ids: z
          .array(z.string())
          .default([])
          .describe("Targets to move data OFF of, e.g. ['target-00']."),
        include_target_ids: z
          .array(z.string())
          .default([])
          .describe("Restrict destinations to these target ids (optional)."),
        exclude_target_ids: z
          .array(z.string())
          .default([])
          .describe("Exclude these target ids as destinations (optional)."),
        max_tasks: z
          .number()
          .int()
          .min(1)
          .max(100000)
          .default(256)
          .describe("Cap on the number of placement tasks to preview."),
      },
    },
    async ({ source_target_ids, include_target_ids, exclude_target_ids, max_tasks }) =>
      guard(async () => {
        const sub = ["target", "rebalance-preview"];
        if (source_target_ids.length)
          sub.push("--source-target-ids", source_target_ids.join(","));
        if (include_target_ids.length)
          sub.push("--include-target-ids", include_target_ids.join(","));
        if (exclude_target_ids.length)
          sub.push("--exclude-target-ids", exclude_target_ids.join(","));
        sub.push("--max-tasks", String(max_tasks));
        const args = keinctlArgs(sub);
        // Preview is non-mutating (no --confirm); run it directly when the
        // mutation gate is open, and ALSO when closed it is safe — but to keep a
        // single predictable rule (closed gate => no shelling out), we still
        // honor the gate here for least-surprise, returning wouldRun when off.
        if (!mutationsAllowed()) {
          return jsonResult({
            executed: false,
            reason:
              "shelling out is disabled (KEINFS_MCP_ALLOW_MUTATIONS != 1). This " +
              "rebalance-preview is a non-mutating dry-run; the command it WOULD " +
              "run is below. Enable the gate to execute it.",
            wouldRun: renderCommand(args),
            argv: [KEINCTL_BIN, ...args],
          });
        }
        const result = await execKeinctl(args);
        return jsonResult({
          executed: true,
          ok: result.exitCode === 0,
          dry_run: true,
          command: result.command,
          exitCode: result.exitCode,
          timedOut: result.timedOut ?? false,
          stdout: result.stdout,
          stderr: result.stderr,
          parsed: result.parsedJson,
        });
      }),
  );

  // --- Tool: enqueue_rebalance (MUTATING, gated) ---------------------------
  server.registerTool(
    "enqueue_rebalance",
    {
      title: "Enqueue a target rebalance (MUTATING, gated)",
      description:
        "MUTATING — gated behind KEINFS_MCP_ALLOW_MUTATIONS=1 (default OFF). Runs " +
        "`keinctl target rebalance-enqueue ... --confirm` (the KMS " +
        "enqueue_target_rebalance RPC) to actually create the rebalance placement " +
        "tasks. ALWAYS run preview_rebalance first. When mutations are disabled " +
        "this returns the exact command it WOULD run and executes nothing.",
      inputSchema: {
        source_target_ids: z
          .array(z.string())
          .default([])
          .describe("Targets to move data OFF of, e.g. ['target-00']."),
        include_target_ids: z
          .array(z.string())
          .default([])
          .describe("Restrict destinations to these target ids (optional)."),
        exclude_target_ids: z
          .array(z.string())
          .default([])
          .describe("Exclude these target ids as destinations (optional)."),
        max_tasks: z
          .number()
          .int()
          .min(1)
          .max(100000)
          .default(256)
          .describe("Cap on the number of placement tasks to create."),
      },
    },
    async ({ source_target_ids, include_target_ids, exclude_target_ids, max_tasks }) =>
      guard(async () => {
        const sub = ["target", "rebalance-enqueue"];
        if (source_target_ids.length)
          sub.push("--source-target-ids", source_target_ids.join(","));
        if (include_target_ids.length)
          sub.push("--include-target-ids", include_target_ids.join(","));
        if (exclude_target_ids.length)
          sub.push("--exclude-target-ids", exclude_target_ids.join(","));
        sub.push("--max-tasks", String(max_tasks));
        const args = keinctlArgs(sub).concat("--confirm");
        return runOrPreview(args);
      }),
  );
}
