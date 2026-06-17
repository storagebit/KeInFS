# KRS

KeInFS Rebuild Service.

Current scope:
- one daemon per storage server
- leases rebuild tasks from KMS
- fans placement requests across one or more KAS endpoints
- asks KAS for rebuild-specific placement first, then falls back to generic
  replacement placement when that is the only honest answer left
- reconstructs missing fragments with KEE through the shared KSC library on
  rebuild tasks, and copies fragments directly (no reconstruction) on rebalance
  and evacuate tasks
- writes replacements directly to KST

Architecture reference:
- [Metadata and Namespace Architecture](/Users/akrause/devel/local/KeInFS/poc/METADATA_NAMESPACE_ARCHITECTURE.md)

It is intentionally a daemon, not a committee.

## Current Placement-Task Flow

`KRS` dispatches on the task kind: `Rebuild` tasks reconstruct the missing
fragment with `KEE`, while `Rebalance` and `Evacuate` tasks move an existing
fragment with a direct source-read copy and no erasure reconstruction.

### Rebuild

1. `KRS` polls `KMS` for placement leases.
2. For each leased task, it loads the manifest and EC profile carried with the
   lease.
   The leased task also carries namespace, bucket, and object-entry identity so
   rebuilds are tied back to the namespace truth instead of free-floating in
   space like lost luggage.
3. It reads surviving fragments directly from `KST` through the shared `KSC`
   target session code.
4. It reconstructs the missing fragment with `KEE`.
5. It asks `KAS` for rebuild placement, or replacement placement if rebuild
   placement is unavailable.
6. It writes the replacement fragment directly to `KST`.
7. It commits the repaired manifest through `KMS`.
8. It finalizes or releases the reservation in `KAS`.

### Rebalance / Evacuate

1. `KRS` polls `KMS` for placement leases and loads the manifest and EC profile
   as above.
2. It reads the source fragment directly from its current `KST` through the
   shared `KSC` target session code. There is no `KEE` reconstruction on this
   path — the fragment bytes are copied as-is.
3. It reserves placement from `KAS` for a destination target.
4. It writes the fragment directly to the `KAS`-chosen destination `KST`.
5. It commits the updated manifest through `KMS`.
6. It finalizes the reservation in `KAS`.
7. It deletes the original fragment from the source `KST`.

## Runtime Tree

`KRS` publishes:

- `/run/keinfs/krs/krs-<pid>/summary`

The summary records:

- lease polls
- leased tasks
- rebuilt tasks
- failed tasks
- rebuilt bytes
- active task id
- last error text

## Boundaries

Current `KRS` is:

- one daemon per storage server
- latest-manifest only
- rebuild-through-direct-KST only
- live-validated for fresh drain, recover, rebalance, and target-outage recovery
  scenarios on the current VM-backed control plane
- still in need of broader long-soak resiliency automation for larger
  multi-stripe objects

Current `KRS` is not:

- a cluster-wide scheduler
- a generic scrub daemon
- a metadata owner
