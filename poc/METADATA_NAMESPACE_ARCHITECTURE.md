# KeInFS Metadata and Namespace Architecture

This document records the current target direction for the KeInFS metadata and
allocator plane after the FoundationDB and NATS cutover.

The short version:

- `namespace` means a tenant-scoped metadata domain
- a namespace can contain multiple projects, groups, teams, or workspaces
- those domain entries can contain multiple buckets
- buckets contain collections and objects
- `KMS` owns namespace truth
- `KAS` owns placement truth
- target inventory is partitioned into allocation shards
- one `KAS` leader owns mutations for one allocation shard at a time
- FoundationDB is the durable substrate
- NATS provides invalidation and event fan-out
- watches are a `KMS` contract, not a database feature we pretend is an API

## Canonical Hierarchy

```mermaid
flowchart TD
    MP["KeInFS Metadata Plane"]
    MP --> NS["Namespace (Tenant Domain)"]
    NS --> G1["Project / Group A"]
    NS --> G2["Project / Group B"]
    G1 --> B1["Bucket: training-data"]
    G1 --> B2["Bucket: checkpoints"]
    G2 --> B3["Bucket: model-registry"]
    B1 --> C1["Collection: 2026"]
    C1 --> O1["Object: shard-00042.parquet"]
    B2 --> O2["Object: run-019.ckpt"]
```

## Service Ownership

```mermaid
flowchart LR
    KSC["KSC / SDK / FUSE"] --> FE["KMS Frontend / Router / Watch Fanout"]
    FE --> SH["Owning KMS Shard"]
    FE --> KAS["KAS Service"]
    SH --> FDB["FoundationDB"]
    KAS --> FDB
    SH --> NATS["NATS invalidation fan-out"]
    KAS --> NATS
    KSC --> KST["KST targets over KP2"]
    KRS["KRS"] --> FE
    KRS --> KAS
    KRS --> KST
```

`KMS` owns:

- namespace records
- namespace domain hierarchy
- bucket definitions
- immutable EC profile binding
- path resolution
- object heads
- immutable manifests
- write intents
- fragment-to-target index
- rebuild task state
- metadata event log (planned; not yet persisted)
- watch replay contract

`KAS` owns:

- allocation-shard leader leases
- target inventory
- target heartbeats
- failure-domain labels
- free spans
- reservations
- rebuild replacement placement

## Scale Model

The design is not "one heroic metadata box saves the cluster."

It is:

- more `KMS` shards for more metadata capacity
- more stateless `KMS` frontend instances for more clients and watches
- shard-local hot caches and fat reservation windows for the hot path
- allocation-shard-local `KAS` leadership instead of multiple allocators
  mutating the same targets and then acting surprised
- FoundationDB for durable truth and recovery boundaries
- NATS for invalidation and event fan-out

```mermaid
flowchart TB
    subgraph Clients["Thousands to 10k+ clients"]
        C1["KSC / SDK"]
        C2["KSC / SDK"]
        CN["..."]
    end

    FE["KMS frontends / routers"]
    C1 --> FE
    C2 --> FE
    CN --> FE

    subgraph KMSS["KMS shards"]
        S1["Shard A"]
        S2["Shard B"]
        S3["Shard C"]
    end

    FE --> S1
    FE --> S2
    FE --> S3

    FDB["FoundationDB cluster"]
    NATS["NATS invalidation fan-out"]
    S1 --> FDB
    S2 --> FDB
    S3 --> FDB
    S1 --> NATS
    S2 --> NATS
    S3 --> NATS

    subgraph KASG["KAS"]
        KASS["Allocator service"]
    end

    KASS --> FDB
    KASS --> NATS
```

## Shard Rule

Current v1 ownership rule:

- one namespace maps to one `kms_shard_id`
- all metadata under that namespace lives on that shard
- hot-namespace path-range splitting is a planned extension, not a day-one lie

`ShardMapEntry` carries:

- `shard_id`
- `namespace_id`
- optional `path_prefix_start`
- optional `path_prefix_end`
- `leader_endpoint`
- `replica_endpoints`
- `revision`

## Allocation Shard Rule

Current allocator ownership rule:

- each target carries one `allocation_shard_id`
- each allocation shard has exactly one active `KAS` leader at a time
- only that leader may mutate free-span, reservation, and reservation-bin state
  for targets in that shard
- `KMS` may reserve across multiple allocation shards to assemble one stripe
- allocator shards are not the same thing as failure domains or placement
  domains

The point is not decorative taxonomy. The point is making overlapping allocator
mutation impossible by construction instead of by hope.

## Watch and Replay Model

Clients do not talk raw backend notification semantics. That would be lazy and
fragile.

The watch contract belongs to `KMS`. What exists today:

- `KMS` publishes ephemeral `MetadataInvalidationEvent` hints on NATS; the
  default invalidation subject is `keinfs.kms.events`
- `WatchEntry`, `WatchPrefix`, and `ListMetadataEvents` are defined in the proto
  and wired through the service layer, but their store-side event readers
  currently return no rows, so there is no durable backlog and revision-based
  resume is a no-op

Planned (not yet implemented):

- durable events in a `metadata_events` record family
- a monotonically increasing revision on every visible mutation
- client resume by replaying `revision > last_seen_revision`

The diagram below shows that **target** durable watch/replay flow (planned):

```mermaid
sequenceDiagram
    participant Client as "KSC / FUSE"
    participant FE as "KMS Frontend"
    participant SH as "Owning KMS Shard"
    participant DB as "FoundationDB"
    participant N as "NATS"

    Client->>FE: WatchPrefix(namespace, parent, last_revision)
    FE->>SH: attach watch
    SH->>DB: read metadata_events > last_revision
    DB-->>SH: backlog rows
    SH-->>FE: replay backlog
    FE-->>Client: stream backlog

    SH->>DB: commit mutation + metadata_events
    SH->>N: publish invalidation
    N-->>SH: wake-up hint
    SH->>DB: read events > current_revision
    SH-->>FE: event batch
    FE-->>Client: stream updates
```

## Current Record Families

### KMS

- shard-map records
- namespace records
- namespace entries
- EC profiles
- bucket contexts
- object heads
- object-version manifests
- write intents
- fragment indexes
- rebuild tasks
- metadata events (planned; not yet persisted)

### KAS

- allocation-shard leader leases
- targets (with inline heartbeat/health state)
- target free spans
- reservations
- reservation bins

The important improvement over the old lab slice is not merely the backend swap.
It is the end of the old "one metadata blob and a prayer" anti-pattern.
Record classes are explicit now, which is what lets the services scale without
pretending one giant hot value was a personality trait.

## Compatibility Path

- existing `bucket + key` APIs remain valid
- internally, a bucket is one namespace entry kind, not the whole universe
- `KMS` may keep compatibility wrappers while the fully sharded namespace model
  finishes growing up
