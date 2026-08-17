# KEE

KEE is the KeinFS Erasure Engine.

It is a small Rust library for the current POC slice:

- EC profile validation
- stripe-local `8+2` encode
- stripe-local reconstruction
- parity verification
- runtime backend inventory

## Scope

KEE currently targets the first lab shape:

- `8` data fragments
- `2` parity fragments
- `1 MiB` fragments by default; `validate_single_stripe` accepts any
  power-of-two, `4 KiB`-aligned fragment size of at least `64 KiB`. Only the
  `8+2` geometry is fixed, not the fragment size.
- one `8+2` stripe per encode/reconstruct call; higher layers iterate that
  across multi-stripe objects

## API

- `EcProfile`
  Validated EC profile with immutable codec and failure-domain metadata.
- `KeeEngine`
  Convenience wrapper that validates a profile and routes encode/reconstruct/verify through the active backend.
- `PreparedEcPlan`
  High-performance encode/reconstruct path obtained via `KeeEngine::prepared_plan()`. It builds the ISA-L encode/decode tables once (shared process-wide) and then reuses them. Exposes `allocate_output_buffers()`, `encode(object)`, `encode_into(object, shards)` for reusable output buffers, and `reconstruct(fragments)`.
- `hardware_inventory()`
  Returns the selected backend and the compile-time acceleration posture.
- `backend_inventory()`
  Alias for `hardware_inventory()` with a more explicit name.
- `encode(profile, object_bytes)`
  Encode one stripe payload into `10` fragments.
- `reconstruct(profile, fragments)`
  Fill missing fragments when at most `2` are absent.
- `verify(profile, fragments)`
  Verify that parity matches the supplied data fragments.

## Backend Policy

- Primary backend: ISA-L when the crate is compiled with `--features isa-l-backend`
- Fallback backend: software Reed-Solomon via `reed-solomon-erasure`
- Runtime backend inventory reports which backend is selected
- The ISA-L feature path requires a system `libisal` (dev artifact) at build time; if it is absent the build fails to link rather than falling back. To use the software backend, build without `--features isa-l-backend` (i.e. `./configure --disable-isa-l`)

## Notes

This crate is intentionally small and opinionated. It does not know about
KMS, KAS, KRS, buckets, manifests, or network placement. Those belong above
the engine.
