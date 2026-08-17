# KSC Design

## 1. Purpose

KSC is the smart native client runtime for KeInFS.

It exists so:

- native applications
- the FUSE client
- future SDK bindings

can all use the same direct data-path behavior against KST targets.

## 2. Responsibilities

KSC is responsible for:

- obtaining target placement from the control plane
- grouping chunk operations by target
- maintaining HTTP/2 sessions to KST targets
- pacing each target independently
- choosing single-chunk vs packed KP2 transactions
- honoring the `16 MiB` packed logical ceiling
- honoring the target-advertised packed write wire-body ceiling (today only in
  the benchmark harness; the production object path is not yet wired to the
  advertised wire-body limit)
- handling packed read responses
- later, handling pushed KP2 read bodies
- exposing client-side runtime and error reporting

KSC is not responsible for:

- acting like a coordinator
- pretending packed write atomicity exists when it does not
- hiding target locality from the rest of the native stack

## 3. Target-Grouping Model

KSC groups operations by target endpoint.

One physical drive is one target. Therefore, one server with many drives
contributes many target endpoints.

KSC must build one target bucket per target endpoint and then decide, per
bucket, whether to emit:

- single-chunk KP2 requests
- packed KP2 transactions

## 4. Packing Heuristics

### 4.1 Hard Rules

KSC must never:

- mix targets inside one KP2 pack
- exceed `16 MiB` logical payload per pack
- exceed the target-advertised packed write wire-body ceiling

### 4.2 Flush Reasons

KSC should record why a pack was flushed:

- size ceiling reached
- flush timer expired
- explicit caller flush
- request ordering barrier
- connection pressure

### 4.3 Initial Policy

Initial default policy:

- same-target writes: pack when more than one chunk is waiting and the pack will
  remain within `16 MiB`
- same-target reads: prefer packed query/read when multiple chunks are pending
  for the same target
- flush on timer if a pack is open but not filling fast enough

The exact timer value is not fixed here yet. It must be measured, not invented
in a conference-room mood swing.

## 5. Connection Model

KSC should maintain long-lived HTTP/2 sessions per target.

For each target, KSC should track:

- connection state
- stream pressure
- write-pack queue depth
- read-query queue depth
- observed latency
- backpressure and rejection events

KSC should not reconnect for every chunk like an intern who just discovered
TCP.

## 5.1 Pacing Model

KSC must pace by target, not with one node-wide hammer.

For each target, KSC should track:

- desired in-flight ceiling
- current in-flight count
- additive-increase progress
- target-local cooldown until timestamp
- rate-limit hints returned by KP2 headers

Implemented today only in the KSC benchmark harness (the production object
path does not yet pace or honor `429` / `x-kp2` rate-limit hints; it surfaces
the error):

- start each target at a configured in-flight ceiling
- increase slowly after enough successful operations
- cut the target ceiling down when the target returns `429`
- honor `x-kp2-limit-max-in-flight` and `x-kp2-retry-after-ms`
- avoid overlapping same-key writes on the client by default

This is not there to be clever. It is there so one angry target does not
force the whole client into a synchronized panic attack.

The KSC benchmark harness reads KST target identity for:

- `max_packed_payload_bytes`
- `max_packed_write_request_bytes`
- `max_request_body_bytes`

and keeps packed writes within both the logical and wire-body limits. The
production object path currently splits packs on the `16 MiB` logical ceiling
(`kp2::MAX_PACK_PAYLOAD_BYTES`) only.

## 6. Read and Write Paths

### 6.1 Write Path

1. Group by target.
2. Build one or more `KP2W` packs up to `16 MiB`.
3. Send `PUT /v1/kp2/chunk-pack` or fall back to single-chunk `PUT` where
   appropriate.
4. Record per-entry success/failure.

### 6.2 Read Path

1. Group requested chunks by target.
2. Build `KP2Q` read-query packs.
3. Send `POST /v1/kp2/chunk-pack/read`.
4. Parse `KP2R` response bodies.
5. Later, accept pushed `KP2R` bodies using HTTP/2 push semantics.

## 7. FUSE Integration

The FUSE client should sit on top of KSC, not beside it.

The current prototype for that is `poc/kfc`, which now uses the same object
path for persisted reads and writes. That prototype is still intentionally
narrow, but the ownership boundary is finally the right one.

That means:

- same target grouping
- same packing logic
- same retry policy
- same latency accounting
- same runtime tree shape

If FUSE and `libkeinfs` diverge into different client engines, that is how
performance bugs learn to reproduce only in production.

## 8. Observability

KSC should expose a live runtime tree under a path such as:

- `/run/keinfs/ksc/<client-id>-<pid>/`

The current KSC runtime tree is emitted by the benchmark path and exposes
aggregate counters only. Per-target breakdowns (connections/active streams by
target, per-target pacing ceiling, per-target cooldown state) and push-promise
counts are not implemented yet.

Minimum counters:

- connections (aggregate)
- active streams (aggregate)
- single-chunk writes
- single-chunk reads
- packed write requests
- packed write chunks
- packed read requests
- packed read chunks
- bytes sent and received
- flush reasons
- retries
- remote errors

Planned (not yet implemented):

- connections by target
- active streams by target
- per-target pacing ceiling
- per-target cooldown state
- push-promise counts

Minimum target-level behavior views:

- endpoints configured for the current run
- endpoint count
- target initial/minimum in-flight policy
- overlapping-write avoidance policy

Minimum latency views:

- target connection setup
- packed write submission
- packed read query submission
- packed response decode
- end-to-end caller latency

## 9. Busy-Poll vs Interrupt

KSC and KST must each evaluate busy-poll vs interrupt separately.

There is no law of nature saying the best choice is the same on both sides.

Initial stance:

- default to interrupt-driven network handling
- only adopt busy-poll where measurement shows a meaningful latency win
- account for dedicated-core tax honestly

Busy-poll that gains little and burns a core full-time is not a performance
feature. It is a heat source with a marketing team.

## 10. Open Items

- exact KSC flush timer defaults
- retry envelope for partial packed failures
- push-promise flow-control policy
- target session pooling policy under very high fan-in
- KSC-side NUMA and core-local networking strategy on storage-adjacent clients
- make packed-path pacing and pack-fill reporting first-class instead of letting
  the single-chunk path lend it clothes
