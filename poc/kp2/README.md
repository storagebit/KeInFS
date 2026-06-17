# KP2

KP2 is the KeInFS native data protocol for direct storage-target traffic.

It is the protocol spoken between the KeinFS Smart Client (`KSC`) and the KeinFS
Storage Target (`KST`). It is not a management protocol and it is not gRPC.

## Scope

KP2 defines:

- single-chunk read, write, head, and delete behavior over raw HTTP/2
- packed same-target write transactions
- packed same-target read transactions
- the binary framing used for packed request and response bodies
- the HTTP/2 headers that identify packed KP2 traffic
- the HTTP/2 headers and status semantics used for rate limiting and backpressure
- the observability signals that KST and KSC must expose for KP2 traffic

## File Map

- [Current I/O Lifecycle](/Users/akrause/devel/local/KeInFS/poc/IO_LIFECYCLE.md)
  Current end-to-end direct and packed I/O flow through the native stack.
- [KP2_Spec.md](/Users/akrause/devel/local/KeInFS/poc/kp2/KP2_Spec.md)
  Detailed protocol specification.

## First Principles

- The `kp2` crate is the single source of truth for KP2 framing and constants.
- KP2 is the native data plane. gRPC is management/control plane only.
- A KP2 connection always terminates at a single storage target.
- Packed KP2 transactions may contain only chunks for that single target.
- The initial packed body ceiling is `16 MiB` total payload per transaction.
- KP2 rate limiting is a protocol-visible behavior, not a target-local mystery.
- KP2 is designed around raw HTTP/2 semantics so direct push delivery can use
  `PUSH_PROMISE` later without transport rework.
- Packed-write acknowledgements use binary KP2 framing too; JSON is not part of
  the hot write-ack path anymore.
- KP2 favors explicit, machine-parseable wire framing and explicit, human-readable
  observability.

## Current Status

The current KP2 implementation is already carrying the active POC path rather
than merely serving as a paper protocol.

- direct single-chunk `1 MiB` traffic between `KSC` and `KST` runs on KP2 over
  raw HTTP/2
- packed same-target reads and writes are implemented and benchmarked
- KP2 rate-limit headers are emitted by `KST` and consumed by `KSC`
- KST and KSC both publish KP2-relevant runtime state and latency data
- KSC object reads and writes now use KMS on the control plane and KP2 on the
  fragment data plane without dragging gRPC into the wrong part of the stack

The current March 19, 2026 single-target baseline on `10.0.0.20` for direct
single-chunk `1 MiB` traffic is approximately:

- `100%` read: `4102.72 MiB/s`
- `100%` write: `2833.09 MiB/s`
- `70/30` mixed: `2713.73 MiB/s` read plus `1162.47 MiB/s` write

These are implementation results for the current POC, not a protocol limit.

## Rate-Limit Surface

KP2 treats rate limiting as protocol-visible behavior. The client should not
have to infer target distress from timeouts and vibes.

Current KP2 rate-limit behavior includes:

- `429 Too Many Requests` when target stream or execution budgets are exhausted
- explicit limit scope and class headers
- retry-after guidance in milliseconds
- target-local backoff in `KSC` rather than global client self-harm
