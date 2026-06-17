# KAS

`KAS` is the KeInFS Allocator Service.

Current direction:

- FoundationDB-backed allocator state
- NATS-driven control-plane fan-out where notification is needed
- explicit `allocation_shard_id` ownership with one active leader per shard
- explicit target inventory, free spans, reservations, and rebuild placement
- granule-based public allocation model

Architecture reference:

- [Metadata and Namespace Architecture](/Users/akrause/devel/local/KeInFS/poc/METADATA_NAMESPACE_ARCHITECTURE.md)

## Responsibilities

`KAS` owns:

- allocation-shard leader leases
- target registration
- target heartbeat state
- failure-domain labels
- free spans expressed as granule runs
- reservation placements (selected granules recorded inside each reservation record)
- stripe placement reservations
- replacement placement for rebuild

It does **not** own namespace records, manifests, object heads, or rebuild task
catalog state. That is `KMS`.

## Current FoundationDB Layout

Current logical record families:

- target inventory (heartbeat/health is a field on the target record, not a
  separate family)
- free-span records (`PREFIX_TARGET_SPAN`, the only span family)
- reservation records (selected granules are stored inline in the reservation
  record's `placements`, not as a separate span family)
- reservation-bin state
- service-instance records
- coordination leases
- the allocator-state stamp

This is deliberate. Allocator churn is noisy enough already without pretending
it belongs in the same hot record family as namespace heads and manifests.

## First-Slice Rules

- public allocation model is `granule_index`, not `slot`
- each fragment allocation is exactly one `1 MiB` granule in the current lab
- each target belongs to exactly one `allocation_shard_id`
- only the active `KAS` leader for that shard may mutate free-span or
  reservation state for that target
- free space is still stored as spans so the model does not paint itself into a
  kindergarten corner
- `drive-domain-lab` is the explicit reason an `8+2` stripe may live on one
  `12`-drive EPYC server
- strict `node` or `rack` domain requests are rejected when the lab cannot
  satisfy them honestly

## Placement Behavior

Current allocator behavior is intentionally simple and legible:

- healthy targets are grouped by failure domain
- richer domains are preferred before starved ones
- per-domain members are sorted by remaining free capacity
- reservations record their selected granules so release paths can put them back
  without creative storytelling

That is not the last word on allocator scale. It is merely adult enough not to
turn the allocation path into a tiny transaction confessional booth.

## Runtime Tree

`KAS` publishes:

- `/run/keinfs/kas/kas-<pid>/summary`

The summary reports:

- request counts by RPC kind
- phase latency summaries
- last error text
- allocator identity fields, including the active FoundationDB cluster file

## Current Service Surface

Current gRPC RPCs:

- `UpsertServiceInstance`
- `ListServiceInstances`
- `GetServiceInstance`
- `RegisterTarget`
- `HeartbeatTarget`
- `ListTargets`
- `SetTargetState`
- `ListReservations`
- `GetReservation`
- `ReserveStripePlacement`
- `ReserveStripeBatch`
- `FinalizeReservations`
- `FinalizeReservationsBatch`
- `ReleaseReservations`
- `ReleaseReservationsBatch`
- `ReclaimTargetGranules`
- `ReserveRebuildPlacement`
- `ReserveReplacementPlacement`
