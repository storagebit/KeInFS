<!-- SPDX-License-Identifier: GPL-2.0-or-later -->
# KeInFS Observability MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io) server that lets an
LLM (e.g. Claude Code) **observe and reason about a live KeInFS cluster**. It is
a read-only lens over the central Prometheus that scrapes every node's
`keinexport` sidecar (`:9909/metrics`); it never touches a KeInFS data or
control path.

```
 KeInFS nodes ──▶ keinexport (:9909) ──▶ Prometheus (:9090) ──▶ [ this MCP server ] ──▶ LLM
```

Metric naming is `keinfs_<service>_<name>` with stable labels `service`,
`instance`, plus `shard` (KMS/KAS), `target_id` (KST), and `rpc`/`phase`/
`quantile` on the latency families. Latency values are **microseconds**.

## Build

Requires Node 18+ (uses the global `fetch`; no HTTP dependency).

```bash
cd observability/mcp
npm install
npm run build      # tsc -> dist/index.js
```

## Configure

Environment variables:

| Var | Default | Meaning |
|-----|---------|---------|
| `PROMETHEUS_URL` | `http://localhost:9090` | Base URL of the central Prometheus. |
| `KEINFS_MCP_ALLOW_MUTATIONS` | _unset_ (**OFF**) | Day-2 **mutating** tools (`drain_target`, `set_target_state`, `enqueue_rebalance`) only shell out to `keinctl` when this is exactly `1`. Otherwise they return the exact command they **would** run and execute nothing. |
| `KEINCTL_BIN` | `keinctl` | Path to the `keinctl` binary used by the mutating + rebalance-preview tools. |
| `KEINCTL_CONTEXT` | _unset_ | If set, `--context <ctx>` is prepended to every emitted/executed `keinctl` command. |

Point `PROMETHEUS_URL` at the observability box, e.g. `http://obs-box:9090`.

> **Safety — mutations are OFF by default.** The read/advisory tools never touch
> the control plane; they only read Prometheus and emit runbooks (strings). The
> three mutating tools are gated behind `KEINFS_MCP_ALLOW_MUTATIONS=1`. With the
> gate closed they return a `wouldRun` command and run nothing. When open, they
> invoke `keinctl` via `child_process.execFile` with **array args (no shell, no
> string interpolation)** and a 30s timeout. keinctl itself also requires
> `--confirm` on every mutation, which these tools always supply. Some
> replacement steps (physically swapping a drive, formatting the new raw device
> for KIX, restarting KAS/KMS, editing the Prometheus scrape config) are
> out-of-band and are surfaced as **manual** runbook steps, not keinctl
> commands.

## Register with Claude Code

### Option A — `claude mcp add`

From the repo root, after `npm run build`:

```bash
claude mcp add keinfs-observability \
  --env PROMETHEUS_URL=http://localhost:9090 \
  -- node ./observability/mcp/dist/index.js
```

(Use an absolute path to `dist/index.js`, or run the command from the repo root
so the relative path resolves. Swap the `PROMETHEUS_URL` for your obs box.)

To also let the day-2 tools shell out to `keinctl`, add the gate + binary env
(leave the gate off to keep mutations disabled — the default):

```bash
claude mcp add keinfs-observability \
  --env PROMETHEUS_URL=http://localhost:9090 \
  --env KEINCTL_BIN=/opt/keinfs/bin/keinctl \
  --env KEINCTL_CONTEXT=default \
  --env KEINFS_MCP_ALLOW_MUTATIONS=1 \
  -- node ./observability/mcp/dist/index.js
```

### Option B — `.mcp.json`

A ready-to-copy stanza lives in [`.mcp.json`](./.mcp.json). To register it for
the whole project, merge it into the repo-root `.mcp.json` (paths there are
relative to the repo root, so use `./observability/mcp/dist/index.js`):

```json
{
  "mcpServers": {
    "keinfs-observability": {
      "command": "node",
      "args": ["./observability/mcp/dist/index.js"],
      "env": {
        "PROMETHEUS_URL": "http://localhost:9090"
      }
    }
  }
}
```

Then check it loaded:

```bash
claude mcp list
```

## Tools

| Tool | What it does |
|------|--------------|
| `query_prometheus` | Run an **instant** PromQL query; returns the result series as `{labels, value}`. |
| `query_prometheus_range` | Run a **range** PromQL query (`start`/`end`/`step`); returns time-series matrices. |
| `list_metrics` | List the `keinfs_*` metric names present in Prometheus, optionally filtered by service. |
| `cluster_health` | Synthesized `healthy` / `degraded` / `critical` verdict with reasons: services up per type, total errors, capacity used %, the two correctness invariants (reservation shard-bypasses + fenced commit aborts, both must be 0), KST targets up, KIX instances up. |
| `io_lifecycle_latency` | **Flagship.** Phase-by-phase latency of one `write` or `read` I/O through the stack (KMS reserve/resolve -> KST execution + media -> KMS commit), ordered, with p50/p99 (µs) per phase and the worst instance. Answers "where is time spent in an I/O right now". |
| `top_phases` | The N slowest phases cluster-wide right now, ranked by p99 (µs) — find hotspots. Optional per-service filter. |
| `list_targets` | Per-KST-target status table: up, write/read B/s, in-flight, connections, errors, stream rejections. |

### Day-2 operations tools

For **hardware replacement, maintenance, and topology changes**. The MCP server
is the operator's reasoning copilot here: the advisory tools synthesize current
state and emit an ordered runbook of `keinctl` commands; the mutating tools can
optionally execute them (gated — see [Configure](#configure)).

**Advisory / read (always safe — synthesize state + produce a step-by-step plan, run nothing):**

| Tool | What it does |
|------|--------------|
| `plan_target_replacement` | Ordered runbook to replace a failing/failed target's drive (drain → wait for rebuild/migration → retire → physically swap → format the new raw device for KIX → re-register → recover/activate), with the exact `keinctl` commands per step (as strings) and the metrics to watch between steps. Tailored to the target's current state read from Prometheus. |
| `plan_node_maintenance` | Drain a whole node's targets (one at a time, waiting for placement to quiesce) for maintenance and the safe order to bring them back. |
| `plan_ip_change` | Procedure when a node's IP/endpoint changes: re-register its targets to the right allocation shard at the new endpoint, the known **restart KAS then KMS** route-cache settle step, and updating the Prometheus scrape config. |
| `plan_topology_change` | Same as `plan_ip_change` for broader topology moves; lists the steps + which config to edit. |
| `capacity_forecast` | Projects time-to-full from `keinfs_kas_capacity_used_pct` + the write byte-rate (`rate(keinfs_kst_write_payload_bytes[…])`). Pure Prometheus math; points you at `query_prometheus_range` to read the fill slope. |
| `drain_readiness` | Read-only `ready`/`caution`/`blocked` verdict on whether a target is safe to drain now: its in-flight load and whether the other up targets have headroom to absorb its data. |
| `rebuild_status` | Synthesizes rebuild progress (KIX `rebuild_required_drives`, KRS `rebuilt_bytes`/`rebuilt_tasks`/`failed_tasks` when exported, pending rebuild tasks) into an `idle`/`in_progress`/`progressing`/`stalled` status. |

**Mutating (shell out to `keinctl`):**

| Tool | What it does | Gated? |
|------|--------------|--------|
| `preview_rebalance` | `keinctl target rebalance-preview` — a non-mutating **dry-run** of what a rebalance would move. | No (still honors the gate for shelling out: returns `wouldRun` when the gate is closed). |
| `drain_target` | `keinctl target drain <target_id> --confirm`. | **Yes** — `KEINFS_MCP_ALLOW_MUTATIONS=1`. |
| `set_target_state` | Move a target between states: `Draining`→`target drain`, `Active`→`target recover`, `Retired`→`target retire` (each with `--confirm`). | **Yes**. |
| `enqueue_rebalance` | `keinctl target rebalance-enqueue … --confirm` — actually creates the rebalance tasks (run `preview_rebalance` first). | **Yes**. |

With the gate closed, every mutating tool returns `{ executed: false, wouldRun: "<exact keinctl command>" }` and runs nothing.

## Resource

| URI | Contents |
|-----|----------|
| `keinfs://metric-catalog` | Documented metric catalog (name, type, labels, meaning) for the whole `keinfs_*` surface, derived from the live `keinexport` dumps and `poc/keinexport/src/convert.rs`. Read this to learn what to query. |

## Example session

> "Is the cluster healthy?" → `cluster_health`
>
> "Where is time going on a write right now?" → `io_lifecycle_latency(direction="write")`
>
> "What are the 10 slowest phases?" → `top_phases(n=10)`
>
> "Show me the storage targets." → `list_targets`
>
> "What's the write throughput?" → `query_prometheus("sum(rate(keinfs_kst_write_payload_bytes[1m]))")`
>
> "target-00's drive is failing — what's the replacement runbook?" → `plan_target_replacement(target_id="target-00")`
>
> "Is it safe to drain target-03 right now?" → `drain_readiness(target_id="target-03")`
>
> "When does the cluster run out of space?" → `capacity_forecast`
>
> "Is the rebuild making progress?" → `rebuild_status`
>
> "Drain target-00." → `drain_target(target_id="target-00")` (returns the command unless `KEINFS_MCP_ALLOW_MUTATIONS=1`)

## Design notes

- **Read-only by default.** The observability + advisory tools call only the
  Prometheus HTTP API (`/api/v1/query`, `/query_range`, `/label/__name__/values`).
  The day-2 **mutating** tools are the sole exception and are OFF unless
  `KEINFS_MCP_ALLOW_MUTATIONS=1`; even then they go through `keinctl` (which
  enforces `--confirm`), never a raw control-plane RPC.
- **No invented keinctl flags.** Every `keinctl` subcommand and flag the day-2
  tools emit is taken verbatim from `poc/keinctl/src/main.rs`. Out-of-band steps
  (drive swap, raw-device format, KAS/KMS restart, Prometheus scrape-config edit)
  are surfaced as manual runbook steps, not fabricated commands.
- **No invented metrics.** Every metric name and phase the server references is
  taken verbatim from the live `keinexport` output and `convert.rs`.
- **Worst-instance percentiles.** `io_lifecycle_latency` and the phase queries
  take `topk(1, …)` per phase so the answer reflects the worst place time is
  being spent, and names that instance. The lifecycle's `sum_of_phase_p99_us`
  is an approximate budget, **not** a measured end-to-end latency (phases
  overlap and percentiles are not additive).
