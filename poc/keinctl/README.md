# keinctl

`keinctl` is the KeInFS operator CLI and the shared public gRPC/type crate used
by the control plane.

It owns two things:

- the public gRPC surface and shared messages for `KMS`, `KAS`, `KSC`, and `KRS`
- the operator-facing `keinctl` binary for KeInFS inspection, status, and
  placement control

It does **not** own native fragment transport. `KP2` remains the single source
of truth for the native data path.

## First Principles

- `keinctl` is for **KeInFS itself**, not deployment substrate management
- KeInFS-owned configuration and status artifacts are **TOML-first**
- supported `keinctl` output modes are:
  - `table`
  - `json`
  - `toml`
- there is **no YAML output mode**
- YAML is used only where a third-party system requires it

## Scope

`keinctl` covers:

- `KMS`
- `KAS`
- `KRS`
- `KST`
- namespaces, buckets, EC profiles, and objects
- targets and target lifecycle
- placement tasks
- write intents
- allocator reservations
- cluster-wide health summaries

`keinctl` does **not** cover:

- Kubernetes pod or node administration
- VM lifecycle
- Teleport or SSH plumbing
- OCI management
- host package or service management

That split is intentional. KeInFS operators should not have to drag deployment
machinery into every command, and deployment tooling should not colonize the
KeInFS CLI like an invasive species.

## Contexts

Contexts live at:

- `~/.config/keinctl/contexts.toml`

Each context contains only KeInFS reachability and optional local status roots,
for example:

- `kms_endpoint`
- `kas_endpoint`
- optional runtime-tree roots for `KMS`, `KAS`, `KRS`, `KST`
- optional `KST` HTTP endpoint defaults
- optional cluster label

## Build Identity and Service Registry

KeInFS binaries now embed repo-wide build identity from:

- `BUILD.toml` (repo root)

That build identity includes:

- package name
- binary name
- semantic version
- monotonic release/build number
- git SHA
- dirty flag
- build timestamp
- build profile
- target triple

`KMS`, `KAS`, and `KRS` heartbeat their running build/config identity into the
service registry stored in `KAS`.

That registry is exposed through `keinctl service list` and
`keinctl service status ...`, so operators can see version drift and stale
instances without guessing which binary blob landed on which node.

## Command Surface

Current top-level commands:

```text
keinctl context list|show|use|validate

keinctl cluster status|topology|events|watch

keinctl service list|status|stats|watch

keinctl namespace create|create-entry|list|show|tree|resolve-path|list-children
keinctl bucket create|list|show
keinctl ec-profile create|list|show

keinctl object head|manifest|locate

keinctl target register|list|show|fail|drain|recover|retire
keinctl target rebalance-preview
keinctl target rebalance-enqueue

keinctl placement summary|list|show|watch|wait
keinctl intent summary|list|show|wait
keinctl allocator reservations|reservation-show|reserve-batch

keinctl diag runtime-list|runtime-show|last-errors
keinctl diag target-http-info|target-http-stats
```

Global flags:

- `--context`
- `--format table|json|toml`
- `--watch[=<seconds>]`
- `--timeout`
- `--verbose`
- `--fail-on healthy|degraded|unhealthy|unknown`

Mutating commands also require:

- `--confirm`

Exit codes:

- `0`: success
- `1`: command or transport failure
- `2`: requested health threshold breached via `--fail-on`

## Runtime Status Contract

KeInFS services now publish TOML-first runtime artifacts:

- `identity.toml`
- `status.toml`
- `summary.toml`
- `events.jsonl`

Legacy `summary` files remain for compatibility while the rest of the tree
catches up.

The intent is simple:

- `identity.toml`: static identity/config view
- `status.toml`: normalized health/readiness/uptime/last-error snapshot
- `summary.toml`: richer service-specific counters and latency summaries
- `events.jsonl`: append-only recent health/error transitions

## Current Public Message Families

- `NamespaceRecord`
- `NamespaceDomainEntry`
- `BucketRecord`
- `EcProfile`
- `WriteIntent`
- `FragmentPlan`
- `ObjectVersionManifest`
- `PlacementTask`
- `TargetRecord`
- `TargetPlacementStatus`
- `PlacementReservationRecord`
- `MetadataEvent`

## Current Service Split

`KMS` owns:

- namespace and hierarchy truth
- bucket definitions and immutable EC binding
- object heads and version manifests
- write intents
- fragment index records
- placement tasks and watch/event history

`KAS` owns:

- target inventory and lifecycle state
- free spans and reservations
- stripe placement
- replacement placement

This crate exists so those public types and control-plane behaviors stop
drifting apart across crates like badly managed paperwork, and so operators can
use one actual tool instead of a rotating cast of `grpcurl` incantations.
