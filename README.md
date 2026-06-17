# KeInFS

KeInFS is an experimental object-storage stack for AI and HPC workloads that
keeps native data traffic on direct target connections instead of shoving object
bytes through a coordinator out of habit.

The repo contains two different things, and pretending otherwise is how people
get confused:

- `design/` is the target-state architecture and API specification
- `poc/` is the current implementation slice and the documents that describe how
  it actually behaves today

## Current Active Stack

The active prototype direction in this repo is:

- `KSC` as the native smart client
- `KFC` as the FUSE mount built on the same object path
- `KMS` as namespace and object-metadata owner
- `KAS` as allocator / placement owner
- `FoundationDB` as durable metadata substrate
- `NATS` as invalidation and event fan-out
- `KST` + `KIX` as direct storage-target path

In the current lab shape, writes and reads are validated against a
multi-target EPYC backend with a separate three-VM control plane for `KMS`,
`KAS`, `FoundationDB`, and `NATS`. Allocation is partitioned into explicit
allocation shards so one allocator leader owns a given target set at a time
instead of letting multiple allocators improvise on the same spans.

The live-validated path today is broader than the old single-stripe toy slice:

- multi-stripe object write, read, and delete through `KMS` + `KAS` + `KST`
- same-key delete + recreate without stale-manifest readback lies
- `KFC` FUSE mounts with `NATS` invalidation on both single-endpoint and
  multi-endpoint `KMS` configurations

## Performance

Measured on a single commodity x86 storage server (dual-socket, NVMe SSDs, a
~100 Gbps NIC) under load from 8 concurrent clients:

- **~9.3 GiB/s aggregate write throughput**, measured as logical
  (pre-erasure-coding) object bytes committed end to end through `KMS` + `KAS`
  + `KST`.
- Under the default **8+2 Reed–Solomon** profile that is **~11.6 GiB/s** of
  fragment traffic on the wire (1.25× parity expansion), which **saturates the
  storage host's ~100 Gbps NIC**. The ceiling is the network link, not KeInFS —
  CPU and storage still have headroom.
- Throughput stays flat as clients are added past saturation, and per-write
  latency stays low and consistent: there is no software-side contention
  collapse, because native object bytes flow **directly from client to storage
  target** and never transit a coordinator.

That is full **erasure-coded** write performance — protected data, not raw
replication — sustained at network line rate. Faster networking is the next
ceiling, not the software.

## Validation Rule

Authoritative runtime validation for the current lab happens from the remote
KVM host against the VM-backed control plane. Developer laptops are fine for
editing and compile sanity, but they are not the source of truth for cluster
behavior, FUSE behavior, or resiliency claims.

## Start Here

- [design/KeInFS_Intend_and_high_level_design.md](design/KeInFS_Intend_and_high_level_design.md)
  Target-state architecture and design specification.
- [design/KeInFS_Management_API_CLI.md](design/KeInFS_Management_API_CLI.md)
  Target-state management API and CLI contract.
- [poc/METADATA_NAMESPACE_ARCHITECTURE.md](poc/METADATA_NAMESPACE_ARCHITECTURE.md)
  Current metadata / allocator direction after the FoundationDB + NATS cutover.
- [poc/IO_LIFECYCLE.md](poc/IO_LIFECYCLE.md)
  Current end-to-end IO flow through the active prototype.
- [poc/ksc/README.md](poc/ksc/README.md)
  Native client status and object-path notes.
- [poc/kfc/README.md](poc/kfc/README.md)
  Current FUSE mount status, supported operations, and limits.

## Configure And Build

The repo now has a real root-level toolchain instead of the previous
crate-by-crate scavenger hunt.

Typical flow:

```bash
./configure --prefix /opt/keinfs \
  --sysconfdir /etc \
  --systemd-unit-dir /etc/systemd/system

make build
make render-configs render-systemd
tools/keinfs-tooling.sh render-single-host-vm-lab \
  --config-env build/config.env \
  --out-dir build/quickstart/single-host-vm-lab \
  --device /dev/nvme0n1 --host-ip 10.0.0.20
```

What that gives you:

- `make build`
  builds the current binary set from the root instead of making operators play
  cargo whack-a-mole.
- `make render-configs`
  renders baseline `KMS`, `KAS`, `KRS`, `KST`, and `keinctl` assets into
  `build/render/etc/keinfs`.
- `make render-systemd`
  renders generic `systemd` units into `build/render/systemd`.
- `tools/keinfs-tooling.sh render-single-host-vm-lab`
  generates a one-host VM-lab asset tree, including a fake-target TSV and per-
  target `KST` environment files, in `build/quickstart/single-host-vm-lab`.

If you want the installed layout staged under a temp root instead of sprayed
straight at `/`, use:

```bash
make install DESTDIR="$PWD/build/stage"
```

## Repo Shape

- `poc/keinctl`
  Control-plane protobufs and admin CLI.
- `poc/kms`
  Namespace, object, manifest, and intent service.
- `poc/kas`
  Target inventory, reservations, and allocator ownership.
- `poc/ksc`
  Native direct client and object path.
- `poc/kfc`
  FUSE client on top of the KSC object path.
- `poc/kst`
  Storage-target process speaking KP2.
- `poc/kix`
  Raw-device arena and chunk-media layout.
- `poc/kee`
  Erasure-coding engine.
- `poc/krs`
  Rebuild daemon.

## Practical Reading Rule

If a design document and a `poc/` document disagree, treat the `poc/` document
as the description of current implementation behavior and the `design/`
document as the intended direction unless the code proves otherwise.

## License

KeInFS is free software: you can redistribute it and/or modify it under the
terms of the GNU General Public License as published by the Free Software
Foundation, either version 2 of the License, or (at your option) any later
version (`GPL-2.0-or-later`). See [LICENSE](LICENSE) for the full text.

Copyright (C) 2026 Andreas Krause / storagebit
