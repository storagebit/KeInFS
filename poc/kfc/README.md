# KFC

`KFC` is the current KeinFS FUSE client prototype.

It mounts one KeInFS bucket as a POSIX filesystem and uses the same native
object path southbound that `KSC` uses, instead of inventing a second client
engine with its own bugs and personality defects.

## Current Status

As of April 9, 2026, the Linux host-bench smoke path is validated for:

- `mkdir`
- `readdir`
- `lookup`
- zero-byte file create
- file read and write
- `truncate`
- `fsync` / `flush` / `release` commit
- `unlink`
- `rmdir`
- remount persistence across daemon restart
- out-of-band create and delete visibility through `NATS` invalidation
- same-key delete + recreate coherence through the mounted view
- single-endpoint and multi-endpoint `KMS` mount configurations

The current implementation stages whole-file contents locally inside the mount
daemon and commits them as object writes on `flush`, `fsync`, `release`, or
explicit truncate-driven commit paths.

That means `KFC` is operational for compatibility and smoke coverage, but it is
not pretending to be the final low-latency POSIX client yet.

## Current Limits

Current prototype limits and omissions:

- one bucket per mount
- full-object staging, not true random-write mutation against immutable objects
- no `rename` yet
- no xattr mapping yet
- no POSIX byte-range locking
- no local NVMe read/write cache layer yet
- no read-ahead or splice path yet

If a workflow genuinely needs the highest-performance path, use `KSC` or a
future native SDK. `KFC` is the compatibility path, not the religion.

## Build

```bash
cd poc/kfc
cargo build --release --features fuse
```

## Mount

```bash
./target/release/kfc mount \
  --kms-endpoint http://192.168.130.11:50060,http://192.168.130.12:50060,http://192.168.130.13:50060 \
  --namespace-id lab-ns \
  --bucket-id lab-8p2 \
  --mountpoint /tmp/keinfs-mnt
```

Notes:

- `--bucket-id` is accepted for consistency with the rest of the CLI surface
- `--bucket` remains accepted as a shorter alias
- `--kms-endpoint` accepts a comma-delimited endpoint list
- `--metadata-notification-nats-url` enables shared `KSC` resolve/payload cache
  invalidation through `NATS`
- `--metadata-notification-subject` defaults to `keinfs.kms.events`
- the live VM control plane is aligned to that same default subject, so the
  default flags are no longer quietly wrong by configuration accident
- mount mode defaults:
  - read completion: `interrupt`
  - write completion: `hot-poll`

## Mode Benchmark

```bash
./target/release/kfc mode-bench \
  --kms-endpoint http://192.168.130.11:50060,http://192.168.130.12:50060,http://192.168.130.13:50060 \
  --bucket-id lab-8p2 \
  --input /tmp/payload.bin
```

This is a client-mode comparison tool, not a replacement for the larger KSC or
host-level workload benchmarks.

## Relationship To KSC

`KFC` is intentionally thin:

- metadata calls go to `KMS`
- object reads and writes go through `KSC` object clients
- those `KSC` clients now share one in-process object metadata cache per mount
  config instead of each pool slot forgetting on its own
- when `NATS` invalidation is enabled, mounted views track out-of-band
  create/delete and same-key rewrite events instead of waiting for TTL expiry
- data fragments still move directly between client and `KST` targets over KP2

If `KFC` drifts into a different southbound data path, that is a bug, not an
innovation.
