# KMS

`KMS` is the KeInFS Metadata Service.

Current direction:

- FoundationDB-backed hot metadata path
- NATS-driven invalidation fan-out
- default invalidation subject `keinfs.kms.events`
- tenant-scoped namespaces, not bucket-as-universe folklore
- watch RPCs (`WatchEntry` / `WatchPrefix`) exist; the durable revision log and
  replay are planned but not yet wired (the event store returns no events, and
  the derived shard-map revision is hardcoded to `1`)
- compatibility wrappers for the current `bucket + key` object path

Architecture reference:

- [Metadata and Namespace Architecture](/Users/akrause/devel/local/KeInFS/poc/METADATA_NAMESPACE_ARCHITECTURE.md)

## Responsibilities

`KMS` owns:

- namespace records
- namespace domain hierarchy
- bucket definitions
- immutable EC profile binding
- path resolution
- object heads
- immutable object-version manifests (fragment-to-target placement lives inside
  the manifest's object-version chunk records, not in a separate index family)
- write intents
- current-target-fragment secondary index records used by rebuild and recovery
  (the only target-keyed secondary index)
- rebuild task catalog and leases
- `NATS` invalidation-hint emission on write and delete (a durable metadata
  event log for watch replay is not yet implemented)

It does **not** own free-space accounting or target placement. That is `KAS`.

## Current FoundationDB Layout

Current logical record families:

- namespace records
- namespace entries
- EC profiles
- bucket contexts
- object heads
- immutable object-version manifests (object-version chunk records hold the
  fragment-to-target placement)
- write intents
- current-target-fragment indexes (the only target-keyed secondary index)
- rebuild task state

Shard mapping is not a stored record family: it is derived on the fly from the
namespace record (the namespace's `shard_id`).

That split is not aesthetics. It is the difference between bounded transactional
records and one giant metadata hairball that behaves like a personality defect.

## First-Slice Rules

- one namespace maps to one shard
- immutable `8+2` manifests on the current lab profile family
- `1 MiB` fragment size in the current lab profile
- multi-stripe objects are live on the current object path
- latest-version reads only
- no range reads yet
- Linux startup can backfill the current-target-fragment index so older metadata
  does not leave rebuild and recovery paths relearning the same truth by hand
- compatibility `bucket + key` object flow remains in place while the namespace
  hierarchy grows up

## Namespace Model

Canonical hierarchy:

- namespace
- project / team / group / workspace
- bucket
- collection
- object

A bucket is a storage root inside the namespace hierarchy. It is not the whole
namespace wearing a fake mustache.

## Watch Model

`KMS` exposes:

- `WatchEntry`
- `WatchPrefix`
- `ListChildren`
- `ResolvePath`
- `ResolveShard`

Client-facing watches do not rely on any database-specific primitive. Today
watches are driven purely by the `NATS` (or poll) invalidation hint. The durable
`metadata_events` revision log and watch replay are not yet implemented:
`read_entry_events`, `read_prefix_events`, and `list_metadata_events` currently
return no events.

The current cache-coherence rule is intentionally blunt:

- `KMS` invalidates its own local read cache synchronously on write and delete
  before publishing the `NATS` invalidation hint
- clients default to `keinfs.kms.events`
- if an operator overrides the subject, both `KMS` and the clients must be
  changed together or they will deserve the stale-cache confusion they get

## Compatibility Object Flow

Write:

1. `KSC` calls `InitiateObjectWrite`.
2. `KMS` resolves the bucket inside the namespace hierarchy.
3. `KMS` loads the immutable EC profile and asks `KAS` for placement.
4. `KMS` persists a write intent with fragment plans and expiry.
5. `KSC` writes fragments directly to `KST`.
6. `KSC` calls `CommitObjectWrite`.
7. `KMS` publishes the immutable manifest, updates the object head, records
   fragment index entries, emits metadata events, and finalizes the reservation.

Read:

1. `KSC` calls `ResolveObjectRead`.
2. `KMS` resolves the current object head and returns the manifest plus EC
   profile.
3. `KSC` reads fragments directly from `KST`.
4. `KSC` reconstructs through `KEE` if enough fragments survive.

## Runtime Tree

`KMS` publishes:

- `/run/keinfs/kms/kms-<shard-id>-<pid>/summary`

The summary reports:

- request counts by RPC kind
- phase latency summaries
- reservation-cache depth
- read-cache hit and miss behavior
- last error text
- identity fields including shard id, public endpoint, and metadata store
