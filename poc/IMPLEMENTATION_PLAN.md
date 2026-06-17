# KeInFS POC Implementation Plan

This document defines the execution order for the remaining gaps between the
current POC baseline and a credible storage-target implementation.

It is intentionally sequenced. The remaining work items are not independent,
and reordering them carelessly would create the usual expensive nonsense:
faster wrong behavior, better benchmarks for the wrong path, or protocol work
stacked on top of unstable publication semantics.

## Current Baseline

The current POC already has four important properties:

- KIX runs on raw block-device slices with direct I/O and `io_uring`.
- KST and KSC expose live runtime trees with phase timing and readable
  counters.
- The direct single-chunk `1 MiB` target path now uses dedicated target-local
  execution groups instead of Tokio's generic blocking pool.
- The two-lane media layout is now backed by an authoritative per-slot owner
  index with generation-based compare-and-publish and compare-aware delete
  (`409` on owner mismatch), with crash-recovery owner seeding that rejects
  conflicting live slot owners (Slice 1; `poc/kst/src/service.rs:58-134`,
  `:1089-1106`, `:1380-1433`). This prevents the earlier read/write corruption
  seen under overlap.

Measured on March 19, 2026, on `10.0.0.20`, with one direct KST target on
`127.0.0.1:18083`, `8` KSC workers, `8` in-flight streams per worker,
`1 MiB` direct single-chunk traffic, the current KIX append change that
reduced arena appends to one durability fence per batch, and the dedicated
direct read/write execution groups in KST:

- direct `100%` read throughput: `4102.72 MiB/s`
- direct `100%` write throughput: `2833.09 MiB/s`
- direct `70/30` mixed throughput:
  - read `2713.73 MiB/s`
  - write `1162.47 MiB/s`
  - `total_errors=0`
- KST read phases:
  - `execution_queue_wait avg 44 us`
  - `media_header_validate avg 159 us`
  - `media_payload_read avg 593 us`
  - `media_payload_copy avg 189 us`
  - `media_crc avg 33 us`
- KST write phases:
  - `body_stream_receive avg 315 us`
  - `execution_queue_wait avg 1045 us`
  - `media_write_prepare avg 648 us`
  - `media_write_io avg 503 us`
  - `media_fsync avg 700 us`
  - `kix_publish avg 668 us`
  - `route_execute avg 2711 us`

Those numbers matter because they define what is already fixed and what still
dominates:

- ingress queue wait is no longer the main pig
- the direct path no longer depends on Tokio's generic blocking pool
- KIX lookup is not the bottleneck
- write cost is now real storage work, especially durability and publication
- read cost is now real media work, especially header validation, payload read,
  payload copying, and synchronous integrity validation

## First-Principle Rules For This Batch

- Native data traffic remains KP2 over raw HTTP/2, not gRPC.
- gRPC remains management and control plane only.
- One physical drive remains one target and one data endpoint.
- Benchmarking remains raw-device only.
- Resilience work is not optional and must not be traded away for prettier
  charts.
- Every new hot-path change must preserve actionable observability in the live
  runtime trees.

## Execution Order

### 1. Correct Publication Under Mixed Load

The two-lane media layout and slot-scoped publication guards now sit on top of
an authoritative per-slot owner index with generation-based compare-and-publish
and compare-aware delete. This slice is implemented.

The end state for this slice is in place:

- one authoritative current owner per logical slot
  (`PublishedSlotOwner` + `SlotPublication`, keyed by logical slot,
  `poc/kst/src/service.rs:58-62`)
- publish installs a new owner only if it wins by generation
  (`SlotPublication::commit` compare at `poc/kst/src/service.rs:121-134`)
- delete is refused with `409 CONFLICT` unless the slot's current owner chunk
  matches the chunk being deleted (`poc/kst/src/service.rs:1089-1106`)
- no silent slot takeover by an unrelated chunk under overlap: a superseded
  owner is retired from KIX as part of the single-write publish
  (`publish_reserved` at `poc/kst/src/service.rs:512-595`, retire at `:575-588`;
  packed publish + retire at `:781-816`)
- crash recovery seeds owners and rejects conflicting live slot owners for the
  same slot (`build_slot_publications` at `poc/kst/src/service.rs:1380-1433`,
  conflict rejection at `:1411-1423`)

Supporting pieces that landed:

- a slot-owner index keyed by logical slot
- compare-aware publish semantics on the KIX-facing write path, including
  tombstone-and-retire against the current owner
  (`tombstone_and_retire` / `write_tombstone_against_current`,
  `poc/kst/src/service.rs:1121-1148`;
  `poc/kix/crates/kix/src/chunk_media.rs:189,275`)
- compare-aware delete semantics on the KIX-facing delete path
  (`begin_delete` / `finish_delete` at `poc/kst/src/service.rs:149-170`)
- recovery that is authoritative for orphaned or superseded lanes
- the two-lane media layout preserved while publication truth improved

Remaining work inside this slice:

- extend reconciliation of media-written but KIX-unpublished lanes across the
  target and KIX during rebuild and recovery validation

Files expected to move first:

- `/Users/akrause/devel/local/KeInFS/poc/kst/src/service.rs`
- `/Users/akrause/devel/local/KeInFS/poc/kix/crates/kix/src/chunk_media.rs`
- `/Users/akrause/devel/local/KeInFS/poc/kix/crates/kix/src/engine.rs`
- `/Users/akrause/devel/local/KeInFS/poc/kix/crates/kix/src/engine/runtime.rs`

Acceptance:

- validated `70/30` and `50/50` mixed runs with overlapping hot keys
- zero payload mismatches
- zero stale-lane publication leaks after restart/rebuild validation

### 2. Final Hot-Path Execution Model

The current direct path already bypasses the old ingress queue detour, but it
still hands off into Tokio's generic blocking pool. That was the right POC
move because it removed obvious queue residency, but it is not the final
target execution model.

The required end state for this slice is:

- direct GET/PUT and direct KP2 operations become first-class target work
- target-local execution groups replace the generic blocking trampoline
- read and write execution domains are explicit and independently observable
- CPU and locality ownership stay aligned with the target rather than with a
  generic runtime pool

Implementation tasks:

- define target-local execution groups for direct read and direct write work
- move the current direct read/write blocking closures onto those groups
- keep body receipt in the HTTP/2 task and hand off only the publication/media
  section
- keep per-target concurrency and backpressure explicit

Files expected to move first:

- `/Users/akrause/devel/local/KeInFS/poc/kst/src/service.rs`
- `/Users/akrause/devel/local/KeInFS/poc/kst/src/ingress.rs`
- `/Users/akrause/devel/local/KeInFS/poc/kst/src/stats.rs`

Status:

- completed for the direct single-chunk `1 MiB` path on `10.0.0.20`
- direct GET/PUT now use dedicated target-local direct execution groups
- `execution_queue_wait` is published separately from `ingress_queue_wait`
- the earlier attempt to recycle ingress workers for the direct path was
  rejected because it reintroduced millisecond-scale queue residency

Remaining work inside this slice:

- move direct KP2 operations onto the same target-local execution model
- keep buffered ingress workers for packed and buffered routes only
- validate the same execution model on the EPYC multi-target host

### 3. More Mature Durability And Media-Read Handling

The current direct path is honest about durability, but it still leaves time on
the table in two places:

- write durability sequencing
- read-side extra copies and validation passes

The required end state for this slice is:

- KIX durability remains resilient, but no longer pays redundant fences
- media writes keep durable semantics without gratuitous extra work
- media reads validate in-place instead of copying more than necessary
- reusable aligned buffers replace per-request allocation churn where possible

Implementation tasks:

- keep the KIX one-sync append path if it remains benchmark-positive and
  recovery-clean
- evaluate target-local KIX group-commit policy without making rebuild from
  media a lie
- compute write CRC while filling aligned write buffers
- reuse aligned buffers on the direct path
- change media read handling to read once, validate header in-place, CRC
  in-place, and avoid extra `Vec` copies

Files expected to move first:

- `/Users/akrause/devel/local/KeInFS/poc/kix/crates/kix/src/arena.rs`
- `/Users/akrause/devel/local/KeInFS/poc/kix/crates/kix/src/chunk_media.rs`
- `/Users/akrause/devel/local/KeInFS/poc/kst/src/service.rs`

Acceptance:

- improved `media_fsync`, `kix_publish`, `media_write_prepare`,
  `media_payload_copy`, and `media_crc` phase timing
- no durability regression under replay, tail corruption, or rebuild tests

### 4. First-Class Packed KP2 Path

KP2 has a real shared spec and a functioning packed path. The server-side
execution is no longer a loop over direct single operations: `handle_kp2_write`
/ `stage_packed_writes` implement a true three-phase batch plan. Phase A
reserves a lane and writes each entry without a per-entry durability barrier
(`write_payload_to_lane_unsynced`), Phase B issues a single shared `fdatasync`
for the whole pack rather than one fsync per entry, and Phase C publishes each
durable entry with truthful per-entry replies
(`poc/kst/src/service.rs:601-696,734-745,748-873`). The remaining gap is
whole-pack request/response buffering.

The end state for this slice is partly in place:

- packed KP2 writes and reads are their own target operations
- per-entry results are explicit and truthful (Phase C per-entry replies)
- server-side slot conflict handling inside one pack is deterministic:
  duplicate-slot conflicts are pre-validated up front before any lane is
  reserved, returning `400 BAD_REQUEST` (`poc/kst/src/service.rs:614-628`)
- whole-pack buffering is still reduced on both request and response paths

Implemented:

- duplicate-slot conflicts pre-validated inside one request before any lane is
  reserved (`poc/kst/src/service.rs:614-628`)
- per-pack slot dedup already makes same-target batches a real execution plan
  rather than disguised singles, with a single shared durability barrier
  (`poc/kst/src/service.rs:660-693,734-745`)

Remaining work inside this slice:

- make packed response construction more streaming-oriented and reduce
  whole-pack request/response buffering
- keep the future push delivery path consistent with the same framing rules

Files expected to move first:

- `/Users/akrause/devel/local/KeInFS/poc/kp2/src/lib.rs`
- `/Users/akrause/devel/local/KeInFS/poc/kst/src/service.rs`
- `/Users/akrause/devel/local/KeInFS/poc/ksc/src/client.rs`
- `/Users/akrause/devel/local/KeInFS/poc/ksc/src/bench.rs`

Acceptance:

- full `16 MiB` packed logical transactions remain valid
- no `413` or framing-limit surprises under the advertised target contract
- packed path phase timing identifies request collection, execution, encoding,
  and send cost separately

### 5. Smarter KSC Pacing

KSC already has a useful benchmark-local pacing model, but the next step is to
make pacing and placement first-class client behavior rather than bench-only
scaffolding.

The required end state for this slice is:

- per-target pacing is reusable client infrastructure
- target feedback shapes concurrency without global self-harm
- packed and direct traffic can have different pacing policies
- overlap avoidance becomes part of client publication discipline, not just a
  benchmark feature flag

Implementation tasks:

- move target pacing logic out of benchmark-only structures
- introduce a target-map abstraction instead of modulo-based endpoint choice
- keep per-target session pools and per-target in-flight budgets
- adapt packed request sizing and retry timing to target-local feedback

Files expected to move first:

- `/Users/akrause/devel/local/KeInFS/poc/ksc/src/bench.rs`
- `/Users/akrause/devel/local/KeInFS/poc/ksc/src/client.rs`
- `/Users/akrause/devel/local/KeInFS/poc/ksc/src/stats.rs`

Acceptance:

- rate-limit events reduce only the affected target budget
- packed and direct paths expose separate useful pacing metrics
- multi-target runs no longer depend on benchmark-only endpoint selection

### 6. Real Multi-Target Behavior

The EPYC host now gives the correct physical model: `12` real drives can mean
`12` real targets on `12` endpoints. The POC must behave as a bag of real
targets rather than a single service pretending to be many things at once.

The required end state for this slice is:

- one physical drive remains one target
- KSC placement and pacing operate on real target identities
- one overloaded target does not poison unrelated targets
- observability remains target-local and comparable across endpoints

Implementation tasks:

- keep bring-up, validation, and benchmark scripts explicitly target-oriented
- extend KSC target mapping to real endpoint identity
- run interference benchmarks across the EPYC `12`-target layout
- document both per-target behavior and aggregate behavior

Files expected to move first:

- `/Users/akrause/devel/local/KeInFS/poc/ksc/src/bench.rs`
- `/Users/akrause/devel/local/KeInFS/poc/ksc/README.md`
- `/Users/akrause/devel/local/KeInFS/poc/kst/README.md`

Acceptance:

- `100%` read, `100%` write, and `70/30` mix across all real targets
- no fake drive slicing
- per-target and aggregate metrics both remain readable

## Why This Order

The order is deliberate:

1. publication correctness first
2. then the final target execution model
3. then durability and media-read refinement
4. then packed KP2 as a true target path
5. then client pacing as reusable infrastructure
6. then full multi-target behavior on the real endpoint model

Skipping ahead would only make the later debugging more expensive.

## Validation Discipline

Every slice must end with:

- updated runtime-tree counters and phase timing if the hot path changed
- raw-device validation only
- benchmark results for `100%` read, `100%` write, and `70/30` mix when the
  slice touches the steady-state path
- recovery and rebuild validation when the slice touches durability or
  publication semantics

The POC no longer gets to claim “good final implementation” by aspiration.
Every next claim must be measured and tied back to the target path that
actually changed.
