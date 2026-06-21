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

One environment variable:

| Var | Default | Meaning |
|-----|---------|---------|
| `PROMETHEUS_URL` | `http://localhost:9090` | Base URL of the central Prometheus. |

Point it at the observability box, e.g. `http://obs-box:9090`.

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

## Design notes

- **Read-only.** Only the Prometheus HTTP API (`/api/v1/query`,
  `/query_range`, `/label/__name__/values`) is called.
- **No invented metrics.** Every metric name and phase the server references is
  taken verbatim from the live `keinexport` output and `convert.rs`.
- **Worst-instance percentiles.** `io_lifecycle_latency` and the phase queries
  take `topk(1, …)` per phase so the answer reflects the worst place time is
  being spent, and names that instance. The lifecycle's `sum_of_phase_p99_us`
  is an approximate budget, **not** a measured end-to-end latency (phases
  overlap and percentiles are not additive).
