# KP2 Specification

## 1. Purpose

KP2 is the native KeInFS data-plane protocol between:

- `KSC`: KeinFS Smart Client
- `KST`: KeinFS Storage Target

KP2 exists to move chunk data directly between smart clients and storage
targets with:

- no coordinator byte relaying
- no filesystem in the storage-node data path
- direct HTTP/2 semantics
- explicit support for same-target chunk packing
- a framing model that can support future push-based reads

## 2. First-Principle Rules

### 2.1 Data Plane vs Management Plane

KP2 is the native data plane.

gRPC is reserved for:

- management
- orchestration
- namespace and metadata control operations
- cluster administration

KP2 is not used on the management plane, and gRPC is not used for native chunk
bytes.

### 2.2 One Target per Endpoint

One physical drive is one target.

One KP2 endpoint maps to one KST target identity, one direct locality domain,
one KIX arena slice set, and one chunk-media slice set.

### 2.3 Same-Target Packing Only

KP2 packed transactions may carry only chunks destined for the same target.

KSC is responsible for grouping chunks by target before packing. A packed KP2
transaction must never mix chunks for different targets.

### 2.4 Packed Size Ceiling

The initial protocol ceiling for one packed request or packed response body is
`16 MiB` of logical chunk payload.

This is a protocol rule, not a suggestion. Larger logical batches must be split
by KSC.

The logical ceiling is distinct from the larger HTTP request-body size needed
to carry KP2 framing overhead. A full `16 MiB` `KP2W` transaction is still
protocol-valid even though its wire body is `24 + 52*chunk_count + payload`.

### 2.5 Raw HTTP/2 Foundation

KP2 is defined on raw HTTP/2 semantics.

The current POC uses prior-knowledge HTTP/2 over TCP. Production transport
envelope details such as TLS, ALPN, and certificate handling are outside this
document for now. The protocol surface is intentionally written so those later
transport decisions do not change KP2 message semantics.

### 2.6 Canonical Protocol Definition

The `kp2` crate is the single source of truth for:

- protocol constants
- header names
- binary framing
- validation rules
- rate-limit header semantics

KST, KSC, and any future native component must consume KP2 behavior from that
crate instead of carrying divergent local copies.

## 3. Actors

### 3.1 KSC

KSC is the smart native client stack used by:

- `libkeinfs`
- the KeinFS FUSE client
- any future native client runtime

KSC is responsible for:

- grouping operations by target
- opening and maintaining HTTP/2 connections to KST targets
- pacing targets independently
- choosing single-chunk vs packed transaction form
- respecting protocol limits
- interpreting packed read responses
- handling future push-promised packed reads

### 3.2 KST

KST is the storage-target data service.

KST is responsible for:

- validating KP2 request semantics
- applying chunk writes to raw chunk media
- publishing location records into KIX
- reading chunk payloads from raw chunk media
- emitting per-request and per-connection observability
- later, issuing push-promised packed responses

## 4. Endpoint Surface

### 4.1 Single-Chunk Endpoints

These remain part of KP2:

- `HEAD /v1/chunk/<chunk-id-hex>`
- `GET /v1/chunk/<chunk-id-hex>`
- `PUT /v1/chunk/<chunk-id-hex>?granule=<n>&generation=<g>`
- `DELETE /v1/chunk/<chunk-id-hex>`

These are the baseline, un-packed operations.

### 4.2 Packed Endpoints

The initial packed KP2 surface is:

- `PUT /v1/kp2/chunk-pack`
- `POST /v1/kp2/chunk-pack/read`

`PUT /v1/kp2/chunk-pack` carries a packed same-target write transaction.

`POST /v1/kp2/chunk-pack/read` carries a packed read-query transaction and
returns a packed read-response body.

These endpoints are intentionally separate from single-chunk paths so the
protocol does not devolve into header-driven ambiguity soup.

## 5. HTTP/2 Headers

Packed KP2 transactions use these headers:

| Header | Direction | Required | Meaning |
| --- | --- | --- | --- |
| `x-kp2-protocol` | request/response | yes | Must be `kp2` |
| `x-kp2-transfer` | request/response | yes | Must be `packed` for packed transactions |
| `x-kp2-kind` | request/response | yes | `write`, `query`, or `read` |
| `x-kp2-chunk-count` | request/response | yes | Number of chunk entries in the body |
| `x-kp2-total-payload-bytes` | request/response | yes | Sum of logical payload bytes carried in the transaction |

`x-kp2-total-payload-bytes` carries logical payload bytes, not raw HTTP body
bytes. Wire-body sizing remains a target capability concern.

### 5.1 Rate-Limit Headers

When KST rejects or throttles a request because in-flight work has hit a target
or connection ceiling, it must report the condition explicitly with:

| Header | Direction | Required When Rate-Limited | Meaning |
| --- | --- | --- | --- |
| `x-kp2-limit-scope` | response | yes | `target` or `connection` |
| `x-kp2-limit-class` | response | yes | `all`, `read`, or `write` |
| `x-kp2-limit-current-in-flight` | response | yes | Current in-flight count at rejection time |
| `x-kp2-limit-max-in-flight` | response | yes | Configured ceiling for the rejected class |
| `x-kp2-retry-after-ms` | response | yes | Explicit retry hint in milliseconds |

Rate-limited KP2 requests must return `429 Too Many Requests`.

Header semantics:

- `x-kp2-kind=write` is used for packed write requests
- `x-kp2-kind=query` is used for packed read-query requests
- `x-kp2-kind=read` is used for packed read responses
- `x-kp2-limit-class=all` is used when a general target-wide in-flight ceiling is hit
- `x-kp2-limit-class=read` is used when the read/control lane is saturated
- `x-kp2-limit-class=write` is used when the write/delete lane is saturated

### 5.2 KSC Rate-Limit Obligations

KSC must treat KP2 rate-limit feedback as target-local state.

That means:

- `429` from one target must not stall unrelated targets
- `x-kp2-retry-after-ms` is a cooldown hint for the affected target
- `x-kp2-limit-max-in-flight` should cap the affected target's client-side
  in-flight budget until later success justifies a measured increase

KSC may choose its exact pacing algorithm, but it must not respond to
per-target pressure with one global sleep that punishes the whole data path.

## 6. Binary Framing Rules

### 6.1 Encoding

All multibyte integers are little-endian.

There is no implicit padding between fields.

Packed bodies are strictly self-delimiting through explicit count and size
fields. Parsers must reject bodies with trailing garbage or truncated payload
runs.

### 6.2 Common Header

All packed KP2 binary bodies begin with the same `24` byte common header:

| Offset | Size | Type | Name |
| --- | --- | --- | --- |
| `0` | `4` | bytes | magic |
| `4` | `2` | `u16` | version |
| `6` | `2` | `u16` | flags |
| `8` | `4` | `u32` | chunk_count |
| `12` | `4` | `u32` | total_payload_bytes |
| `16` | `4` | `u32` | entry_table_bytes |
| `20` | `4` | `u32` | reserved |

Current values:

- `version = 1`
- `flags = 0`
- `reserved = 0`

### 6.3 Magic Values

| Magic | Meaning |
| --- | --- |
| `KP2W` | packed write request |
| `KP2Q` | packed read-query request |
| `KP2R` | packed read response |
| `KP2A` | packed write acknowledgement |

## 7. Packed Write Request

### 7.1 Body Kind

Packed write requests use magic `KP2W`.

### 7.2 Entry Descriptor

Each packed write entry descriptor is `52` bytes:

| Offset | Size | Type | Name |
| --- | --- | --- | --- |
| `0` | `32` | bytes | `chunk_id` |
| `32` | `8` | `u64` | `slot_index` |
| `40` | `4` | `u32` | `generation` |
| `44` | `4` | `u32` | `payload_bytes` |
| `48` | `4` | `u32` | reserved |

`reserved` must be `0`.

### 7.3 Body Layout

The packed write body is:

1. common header
2. `chunk_count` write entry descriptors
3. chunk payload bytes concatenated in descriptor order

### 7.4 Semantics

For each entry, KST must:

1. validate that `payload_bytes` matches the configured slot/layout policy
2. validate that the declared logical payload stays within the protocol ceiling
3. validate that the encoded HTTP body fits within the target-advertised
   wire-body ceiling
4. write the payload to raw chunk media for the given slot
5. publish the resulting location record into KIX

KST applies the entries in descriptor order.

### 7.5 Atomicity

Packed write atomicity is not claimed at the pack level.

The committed unit remains the individual chunk publication. If a packed write
fails after some entries have already been published, KST must report the
per-entry result honestly. Recovery from partially published state still relies
on KIX correctness and rebuild-from-media behavior.

That is not pretty, but it is honest, and honest beats fake atomicity.

### 7.6 Response

Packed write responses use magic `KP2A`.

The response content type is `application/vnd.keinfs.kp2`.

Each packed write acknowledgement entry descriptor is `80` bytes:

| Offset | Size | Type | Name |
| --- | --- | --- | --- |
| `0` | `32` | bytes | `chunk_id` |
| `32` | `2` | `u16` | `status_code` |
| `34` | `2` | `u16` | `location_kind` |
| `36` | `2` | `u16` | `drive_id` |
| `38` | `2` | `u16` | flags |
| `40` | `8` | `u64` | `physical_offset` |
| `48` | `4` | `u32` | `logical_length` |
| `52` | `4` | `u32` | `stored_length` |
| `56` | `4` | `u32` | `generation` |
| `60` | `4` | `u32` | `checksum` |
| `64` | `8` | `u64` | `slot_index` |
| `72` | `4` | `u32` | `requested_generation` |
| `76` | `4` | `u32` | `error_bytes` |

`flags` must be `0` in version `1`.

The packed write acknowledgement body is:

1. common header
2. `chunk_count` write-ack entry descriptors
3. UTF-8 error text bytes concatenated in entry order for entries whose
   `error_bytes > 0`

Entries with `location_kind = 0` carry no location record.

Entries with `error_bytes = 0` carry no error text.

## 8. Packed Read Query

### 8.1 Body Kind

Packed read-query requests use magic `KP2Q`.

### 8.2 Entry Descriptor

Each packed read-query entry descriptor is `32` bytes:

| Offset | Size | Type | Name |
| --- | --- | --- | --- |
| `0` | `32` | bytes | `chunk_id` |

### 8.3 Body Layout

The packed read-query body is:

1. common header
2. `chunk_count` query entry descriptors

There is no payload section in `KP2Q`.

## 9. Packed Read Response

### 9.1 Body Kind

Packed read responses use magic `KP2R`.

### 9.2 Entry Descriptor

Each packed read-response entry descriptor is `80` bytes:

| Offset | Size | Type | Name |
| --- | --- | --- | --- |
| `0` | `32` | bytes | `chunk_id` |
| `32` | `2` | `u16` | `status_code` |
| `34` | `2` | `u16` | `location_kind` |
| `36` | `2` | `u16` | `drive_id` |
| `38` | `2` | `u16` | reserved |
| `40` | `8` | `u64` | `physical_offset` |
| `48` | `4` | `u32` | `logical_length` |
| `52` | `4` | `u32` | `stored_length` |
| `56` | `4` | `u32` | `generation` |
| `60` | `4` | `u32` | `checksum` |
| `64` | `8` | `u64` | `slot_index` |
| `72` | `4` | `u32` | `payload_bytes` |
| `76` | `4` | `u32` | reserved_2 |

`location_kind` values:

| Value | Meaning |
| --- | --- |
| `0` | no location |
| `1` | extent |
| `2` | packed-container |

`reserved` and `reserved_2` must be `0`.

### 9.3 Body Layout

The packed read-response body is:

1. common header
2. `chunk_count` read-response entry descriptors
3. payload bytes concatenated in entry order for entries whose `status_code = 200`

Entries that are not found, corrupt, or otherwise unavailable must carry
`payload_bytes = 0`.

### 9.4 Partial Success

Packed read responses are allowed to be partially successful.

One missing or corrupt chunk must not force the entire packed response to fail
unless the whole transaction is structurally invalid.

That keeps KSC from learning about one bad chunk by detonating an otherwise
useful response.

## 10. Push-Promise Read Delivery

KP2 is intentionally built on raw HTTP/2 so KST may later use `PUSH_PROMISE`
for grouped chunk delivery.

The intended model is:

1. KSC issues a packed read-query or read-intent request.
2. KST groups the satisfied entries for that target.
3. KST emits one or more pushed responses whose bodies use the same `KP2R`
   framing described above.

The important rule is that push delivery changes transport behavior, not payload
format. `KP2R` is the response body format whether the response is inline or
pushed.

## 11. KSC Packing Rules

KSC must:

- group chunk operations by target
- pack only same-target entries together
- keep one packed transaction at or below `16 MiB` logical payload
- keep one packed write body at or below the target-advertised wire-body
  ceiling
- preserve per-target ordering when required by the caller
- spill overflow entries into a new pack instead of violating the ceiling
- prefer packed writes for many small or medium chunks headed to the same target
- prefer packed reads when fetching many chunks from the same target
- treat KP2 `429` responses as explicit backpressure, not as a vague suggestion
- read the KP2 rate-limit headers and pace retries accordingly

KSC must not:

- mix targets inside one KP2 packed body
- exceed the protocol ceiling and hope the target will be nice about it
- assume packed write atomicity
- ignore explicit target backpressure and keep hammering the same endpoint

## 12. Error Handling

### 12.1 Structural Errors

KST must reject the whole request if:

- required KP2 headers are missing
- the magic is unknown
- the version is unsupported
- counts or sizes are inconsistent
- payload runs exceed body length
- total logical payload exceeds `16 MiB`
- encoded request-body bytes exceed the target transport ceiling

### 12.2 Per-Entry Errors

For packed reads, per-entry errors are represented inside `KP2R`.

For packed writes, per-entry errors are represented inside `KP2A`.

### 12.3 Human-Readable Diagnostics

KST error text must remain explicit and actionable. Operator-facing text should
name the violated rule directly, for example:

- missing header
- unsupported version
- target-mixed pack attempt
- payload ceiling exceeded
- slot/layout mismatch

### 12.4 Rate-Limit Errors

KST may reject a request with `429 Too Many Requests` when:

- the target-wide active in-flight ceiling is exhausted
- the target read/control lane is exhausted
- the target write/delete lane is exhausted
- a future connection-local ceiling is exhausted

The response must include the KP2 rate-limit headers defined above. Silence here
would be cheap, lazy, and operationally useless.

## 13. Observability

KST must expose KP2-specific runtime reporting at least for:

- packed write request count
- packed write chunk count
- packed write logical payload bytes
- packed read request count
- packed read chunk count
- packed read logical payload bytes
- rate-limit rejection count by class
- packed transaction failures
- packed transaction size distribution

KSC must expose at least:

- packs emitted per target
- average and peak chunks per pack
- average and peak payload bytes per pack
- flush reasons
  - size ceiling
  - timer expiry
  - explicit caller flush
- retry counts
- rate-limit responses observed by scope and class
- push-promise acceptance and rejection counts

## 14. Versioning

Protocol compatibility is controlled by:

- the HTTP/2 headers that identify a KP2 packed transaction
- the magic value
- the `version` field in the common header

Unsupported versions must be rejected explicitly. Silent downgrade behavior is
for cowards and for future incident reviews.

## 15. Open Items

The following are intentionally still open:

- authenticated capability headers on KP2 transactions
- TLS/ALPN production transport envelope
- checksum offload hints and negotiated acceleration reporting
- exact push-promise stream choreography
- whether very large same-target read groups should be split by KST into more
  than one `KP2R` response body even when KSC asks once
- exact future connection-identity model for client-scoped rate limiting beyond
  the current target-scope POC behavior
