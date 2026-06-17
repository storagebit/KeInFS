# KIX Execution Backlog

This file is the explicit KIX implementation backlog for the prototype. It exists to prevent status theater and to keep the remaining work visible.

Legend:

- `[x]` implemented
- `[~]` in progress
- `[ ]` not started

## 1. Runtime And Correctness Baseline

- `[x]` Linux-only KIX storage-node prototype
- `[x]` startup preflight for `O_DIRECT`, arena alignment, span fit, and working `io_uring`
- `[x]` durable delta append and checkpoint path
- `[x]` replay, torn-tail truncation, malformed-frame containment, rebuild-required fallback
- `[x]` actionable startup/runtime error messages
- `[x]` runtime stats tree with summary, config, per-shard files, and per-drive files
- `[x]` queue-pressure counters and latency buckets in the runtime stats tree
- `[x]` local raw-device maintenance CLI for KIX preflight, check, format, inspect, verify, and tail repair

## 2. Benchmark Integrity

- `[x]` hot lookup benchmark for RAM-resident live-index hits
- `[x]` durable write benchmark for delta/checkpoint pressure
- `[x]` media-backed read benchmark path so `GET` measures KIX lookup plus actual storage I/O
- `[x]` media-backed write benchmark mode with separated data-write and index-commit timing
- `[x]` raw-arena budget preflight that rejects obviously undersized slices and reports sizing telemetry before startup
- `[x]` replay benchmark with large checkpoints, deep delta tails, and rebuild-required detection
- `[x]` benchmark failure mode for wrong-domain handoff instead of only synthetic placement
- `[x]` benchmark mode that reads data from a raw NVMe span instead of a regular file-backed media surrogate

## 3. Queueing And Worker Topology

- `[x]` separate shard workers and drive-appender workers
- `[x]` configurable busy-poll vs interrupt per worker class
- `[x]` explicit lookup queue and commit queue split inside shard ownership
- `[x]` op-class split so hot lookups can busy-poll while commit/control work remains interrupt or hybrid
- `[x]` bounded ingress queues per locality domain instead of one flat process ingress shape
- `[~]` configurable queue-depth matrix benchmarking; the raw-media path now measures independent read-wave and write-group sizes, but ingress / drive queue sweeps are still pending
- `[ ]` queue overflow policy beyond retry-and-yield

## 4. NUMA / PCIe / IRQ Locality

- `[x]` device NUMA discovery
- `[x]` core pinning for shard and drive workers
- `[x]` thread memory policy placement
- `[x]` per-domain buffer pools allocated from the local NUMA node in the benchmark ingress path
- `[~]` per-domain ingress workers with one-hop handoff in the benchmark path; real socket pollers and transport ownership are still pending
- `[~]` explicit NIC locality steering; KIX now inspects netdev IRQ topology and current RPS/XPS state, but it does not yet program full RSS / XPS / RPS queue ownership
- `[x]` explicit NVMe MSI-X / IRQ affinity steering for writable vectors, with clear runtime warnings when the kernel keeps managed vectors in place
- `[x]` automated locality validation that rejects obviously bad placement plans
- `[x]` measured local vs remote vs handoff benchmark matrix after real media reads are in place

## 5. Data Path Fidelity

- `[~]` actual storage-node read path model: ingress -> KIX lookup -> media read -> response; the benchmark now does direct KIX fast-path lookup plus raw-media reads, but there is still no real network transport
- `[~]` actual storage-node write path model: ingress -> media write -> KIX commit; the benchmark now does durable raw-media writes plus KIX publication, but there is still no real network transport
- `[x]` packed-container read/write simulation with `4 KiB` internal alignment (selectable via `--record-mix packed`/`packed-only` or `mixed`, sized by `--packed-bytes`; `CHUNK_MEDIA_ALIGN_BYTES` / `MEDIA_IO_ALIGN = 4096`)
- `[x]` extent read/write simulation for large objects (default `RecordMix::ExtentOnly`, configurable via `--record-mix` / `--extent-bytes`; default extent `1 MiB` on `4 KiB`-aligned slots)
- `[ ]` allocator interaction model so location-record production is not purely synthetic
- `[x]` checksum verification integrated into the storage-read benchmark path
- `[x]` rebuild-from-media benchmark with deterministic raw chunk-media spans and explicit refusal on partial scans

## 6. Raw Device And NVMe Focus

- `[x]` raw-device KIX arena support
- `[x]` `io_uring` direct backend for the KIX arena
- `[x]` raw-device media span benchmarking for actual data reads and writes
- `[x]` direct raw-media path using aligned `O_DIRECT` plus `io_uring` on Linux block devices
- `[x]` registered fixed buffers and fixed-file registration for the raw-media `io_uring` backend
- `[x]` direct client fast path that removes the old lookup-worker round-trip from steady-state reads and steady-state publication
- `[x]` separate raw spans for KIX arena vs benchmark media path
- `[ ]` explicit multi-device raw benchmark support using one physical device per configured drive, with no sliced-device fallback
- `[~]` queue-depth tuning for direct NVMe submission/completion pressure; the first read/write batch sweeps are in and already show that bigger is not automatically better
- `[ ]` hybrid polling policy for read completions vs write completions
- `[~]` direct measurement of fsync/group-commit cost vs microbatch size; March 19 write runs now cover `per-op`, batch `2`, and batch `8`

## 7. Observability

- `[x]` `/run`-style userspace runtime tree
- `[x]` per-shard and per-drive latency summaries
- `[x]` runtime hardware-acceleration inventory for the active CRC backend and software-fallback state
- `[ ]` Prometheus export from the same stats registry
- `[ ]` Unix-domain control socket for structured snapshots and admin commands
- `[ ]` optional Linux troubleshooting tracepoints for deep lab diagnostics, explicitly outside the core observability path
- `[x]` IRQ / RSS / NVMe vector visibility in the runtime tree
- `[ ]` checkpoint/replay progress reporting

## 8. Immediate Next Steps

1. Finish NIC RSS / XPS / RPS queue steering so netdev locality is controlled, not merely inspected and reported.
2. Remove the remaining idle lookup/commit-worker scaffolding now that the hot path is direct, so the code stops carrying dead furniture.
3. Extend the queue-depth / group-commit sweep across ingress, drive-appender, and mixed read-write scenarios instead of only the first raw-media cuts.
4. Expand rebuild-from-media coverage to mixed packed/extent layouts, larger slot counts, and media-corruption injection instead of only the first deterministic extent-layout runs.
5. Add explicit multi-device raw benchmark support using one physical device per configured drive.
6. Add Prometheus export and a Unix-domain control socket on top of the existing stats registry.
7. Expand the `kix` CLI from KIX-arena maintenance into a fuller local KeInFS node utility once allocator and chunk-media metadata exist.
