# KSC

KSC is the KeinFS Smart Client.

It is the native client-side runtime that speaks KP2 to storage targets and
uses coordinator/control-plane services for non-data operations.

## Scope

KSC is the client-side owner of:

- target grouping
- per-target pacing
- per-target overlap avoidance
- KP2 single-chunk and packed transaction emission
- direct same-target batching
- direct packed read handling
- future push-promise acceptance
- client-side observability

## File Map

- [Current I/O Lifecycle](/Users/akrause/devel/local/KeInFS/poc/IO_LIFECYCLE.md)
  Current end-to-end read and write flow through KSC, KP2, KST, KIX, and chunk media.
- [Metadata and Namespace Architecture](/Users/akrause/devel/local/KeInFS/poc/METADATA_NAMESPACE_ARCHITECTURE.md)
  Current namespace hierarchy, shard model, and control-plane direction.
- [KFC README](/Users/akrause/devel/local/KeInFS/poc/kfc/README.md)
  Current FUSE mount status and limits on top of the KSC object path.
- [KSC_Design.md](/Users/akrause/devel/local/KeInFS/poc/ksc/KSC_Design.md)
  Detailed smart-client design notes.
- [main.rs](/Users/akrause/devel/local/KeInFS/poc/ksc/src/main.rs)
  Current KSC CLI entry point dispatching the smoke, ec-benchmark, benchmark,
  put-object, get-object, delete-object, and object-benchmark subcommands.

## First Principles

- KSC talks directly to KST for native data traffic.
- KSC uses gRPC only where the management/control plane actually calls for it.
- KSC groups by target, not by sentiment.
- KSC must expose a live runtime tree with readable counters and latency data.
- FUSE and `libkeinfs` should use the same KSC core, not two divergent client
  personalities that rot in different corners.

## Current Prototype

The current `ksc` crate is no longer just a packed-path smoke toy.

Today it does six concrete things:

- fetches KST target identity from `GET /v1/info`
- supports one or many target endpoints in one benchmark run
- maintains one HTTP/2 session per worker per target
- applies per-target pacing based on local success history and KP2 rate-limit
  headers
- prevents overlapping same-key writes by default on the client side
- respects both the KP2 logical payload ceiling and the target-advertised
  packed write wire-body ceiling
- emits single-chunk or packed same-target KP2 traffic and validates the
  returned payloads

It already consumes framing and header rules from the shared
[KP2](/Users/akrause/devel/local/KeInFS/poc/kp2/README.md) crate instead of
carrying local copies of the wire format.

It now also carries the first real object path on top of that direct target
machinery:

- `put-object`
- `get-object`
- `delete-object`

And the current `KFC` FUSE mount now rides that same object path instead of
building a second southbound stack out of spite.

Those object operations use:

- `KMS` for namespace-aware initiate / resolve / commit
- `KEE` for `8+2` encode and reconstruct
- direct `KP2` traffic to `KST` for fragment movement
- a shared in-process resolve/payload cache, with optional NATS invalidation on
  the default `keinfs.kms.events` subject, so `KFC` pools stop relearning the
  same object metadata like idiots

The benchmark path also publishes a live runtime tree with:

- `summary`
- `latency`
- `phases/read`
- `phases/write`
- `phases/delete`
- `target`

That phase breakdown now matters, because it lets KSC say whether a slow write
is stuck in body send, response wait, or client-side validation instead of
smearing all transport cost into one vague latency number.

## Current Object Path

The current object slice is still intentionally opinionated, but it is no
longer the old `<= 8 MiB` toy:

- multi-stripe object IO is live
- full-object reads, writes, and deletes are live
- immutable per-bucket EC profile binding inside the namespace hierarchy
- latest-version reads only
- no range reads yet

Write path:

1. call `KMS InitiateObjectWrite`
2. encode with `KEE`
3. write `10` fragments directly to `10` `KST` targets
4. call `KMS CommitObjectWrite`

Read path:

1. call `KMS ResolveObjectRead`
2. read direct fragments from `KST`
3. reconstruct through `KEE` if needed

Delete path:

1. call `KMS DeleteObject`
2. let `KMS` drop the object head and publish metadata invalidation
3. clean fragments on `KST` and reclaim reservations through `KAS`

This is enough to exercise the real control plane without pretending the whole
object stack is already finished.

Compatibility note:

- today the CLI still speaks `bucket + key`
- internally `KMS` resolves that through namespace -> bucket -> object path

Live note:

- same-key delete + recreate is validated again on the VM control plane
- `KFC` coherence is validated against both single-endpoint and multi-endpoint
  `KMS` mount configurations with `NATS` invalidation enabled

## Command

```bash
cd /home/akrause/KeInFS/poc/ksc
./target/release/ksc smoke \
  --endpoint http://127.0.0.1:18083 \
  --chunk-seed 7 \
  --slot-index 9 \
  --generation 1 \
  --packed-count 4
```

Expected result:

- `ksc_smoke_protocol=kp2`
- `ksc_smoke_transfer=packed`
- `ksc_smoke_result=ok`

Object write:

```bash
./target/release/ksc put-object \
  --kms-endpoint http://127.0.0.1:50060 \
  --bucket lab-8p2 \
  --key object-a \
  --input /tmp/object-a.bin
```

Object read:

```bash
./target/release/ksc get-object \
  --kms-endpoint http://127.0.0.1:50060 \
  --bucket lab-8p2 \
  --key object-a \
  --output /tmp/object-a.out
```

Object delete:

```bash
./target/release/ksc delete-object \
  --kms-endpoint http://127.0.0.1:50060 \
  --bucket lab-8p2 \
  --key object-a
```

Object-path benchmark (the source of the benchmark numbers reported below):

```bash
./target/release/ksc object-benchmark \
  --kms-endpoint http://127.0.0.1:50060 \
  --bucket lab-8p2 \
  --workers 4 \
  --write-percent 30
```

The binary also exposes `ec-benchmark` (isolated EC encode benchmark) and
`benchmark` (sustained packed KP2 load generator); run `ksc <subcommand>
--help` for their options.

## Rate-Limit Behavior

KSC now does the first real version of per-target pacing.

Current behavior:

- KSC maintains a per-target in-flight ceiling instead of one global blind
  throttle
- successful completion slowly increases the ceiling for that target
- `429` plus KP2 limit headers reduce only the affected target ceiling
- `x-kp2-retry-after-ms` becomes a target-local cooldown instead of a
  whole-client nap
- same-key overlapping writes are avoided by default before they ever hit KST

Current limits:

- KSC does not yet resize packs dynamically in response to rate pressure
- the prefill path is still more conservative than the measured path and is not
  yet a model of elegance
- packed-path pacing still needs dedicated tuning instead of inheriting the
  single-chunk assumptions

## Packed Limit Semantics

KSC now treats packed limits as two separate rules:

- protocol limit: `<= 16 MiB` logical payload per KP2 transaction
- target limit: encoded packed write body must fit within the target-advertised
  `max_packed_write_request_bytes`

That split matters because a full `16 x 1 MiB` packed write is logically valid
but still needs a little extra wire-body headroom for the KP2 common header and
entry table. KSC now reads that limit from `GET /v1/info` and sizes packs
against both ceilings instead of tripping over a `413` like an amateur.

## Current 1 MiB Findings

On March 19, 2026, on `10.0.0.20`, against the current two-lane publication
model with `64` active streams (`8` workers x `8` in-flight), the current KIX
one-sync arena append path, and the new dedicated KST direct execution groups:

- `100% read`
  - `4102.72 MiB/s`
  - `wait_response avg 2739 us`
  - `collect_response avg 276 us`
  - `payload_validate avg 56 us`

- `100% write`
  - `2833.09 MiB/s`
  - `send_body avg 5 us`
  - `wait_response avg 4765 us`
  - the client is still waiting on durable target work, but it is no longer
    waiting on Tokio's generic blocking pool to stop clowning around

- `70% read / 30% write`
  - `3876.20 ops/s`
  - read throughput `2713.73 MiB/s`
  - write throughput `1162.47 MiB/s`
  - `total_errors=0`

So the client is now waiting on real target work rather than tripping over
mixed-load correctness bugs or a generic server-side blocking trampoline.

## Current Multi-Target Findings

On March 19, 2026, on `andreas-bm-kvm-nvlustre-prim`, with `12` real targets on
ports `18080..18091`, `3072` live keys (`256` per target), `24` workers,
`8` in-flight streams per worker, and a per-target initial in-flight ceiling of
`16`:

- `100% read`
  - `13250.89 MiB/s`
  - `13250.89 chunk ops/s`
  - read `p50/p95/p99 = 8192 / 16384 / 32768 us`
  - `wait_response avg 8562 us`
  - `collect_response avg 3113 us`

- `100% write`
  - `17459.15 MiB/s`
  - `17459.15 chunk ops/s`
  - write `p50/p95/p99 = 8192 / 16384 / 16384 us`
  - `wait_response avg 9331 us`

- `70% read / 30% write`
  - `15903.15 chunk ops/s`
  - read throughput `11094.46 MiB/s`
  - write throughput `4808.68 MiB/s`
  - read `p50/p95/p99 = 8192 / 16384 / 32768 us`
  - write `p50/p95/p99 = 8192 / 16384 / 16384 us`
  - `total_errors=0`

Important note:

- the first EPYC numbers were garbage because `127.0.0.1:18080` was still bound
  by an older KST process; once that stale listener was killed and replaced,
  single-target write latency dropped from roughly `1.19 s` to roughly
  `2.3 ms`, and the 12-target matrix stopped lying

## Current Packed Multi-Target Findings

On March 19, 2026, on `andreas-bm-kvm-nvlustre-prim`, with `12` real targets on
ports `18080..18091`, `3072` live keys (`256` per target), `24` workers,
`8` in-flight streams per worker, packed `16 x 1 MiB` extent transactions, and
the corrected target-advertised wire-body ceiling:

- `100% read`
  - `478.13 pack ops/s`
  - `7650.02 MiB/s`
  - read `p50/p95/p99 = 131072 / 262144 / 524288 us`
  - `wait_response avg 44212 us`
  - `collect_response avg 122013 us`
  - `payload_validate avg 17836 us`

- `100% write`
  - `481.76 pack ops/s`
  - `7708.10 MiB/s`
  - write `p50/p95/p99 = 131072 / 131072 / 262144 us`
  - `send_body avg 351 us`
  - `wait_response avg 178953 us`

- `70% read / 30% write`
  - `696.71 pack ops/s`
  - read throughput `7796.34 MiB/s`
  - write throughput `3350.99 MiB/s`
  - read `p50/p95/p99 = 131072 / 262144 / 262144 us`
  - write `p50/p95/p99 = 65536 / 131072 / 131072 us`
  - `total_errors=0`

What those numbers say:

- the packed path is now first-class enough to carry full `16 MiB` logical
  transactions without getting rejected by the target
- packed throughput is solid, but latency is still much fatter than the direct
  single-chunk path because KST and KSC are still doing too much whole-pack
  buffering and response collection work
