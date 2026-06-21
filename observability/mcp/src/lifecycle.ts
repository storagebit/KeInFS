// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit
//
// The canonical end-to-end I/O lifecycle phase ordering for a WRITE and a READ
// through the KeInFS stack. Every (service, rpc, phase) tuple below is taken
// verbatim from the live phase inventory (/tmp/phase-inventory.txt) — no phase
// name is invented. The ordering reflects the real flow:
//
//   WRITE:  KSC --gRPC--> KMS initiate_object_write (reserve placement)
//           KSC --KP2 ---> KST write (admission -> media -> kix publish)
//           KSC --gRPC--> KMS commit_object_write   (finalize the head)
//
//   READ:   KSC --gRPC--> KMS resolve_object_read   (resolve placement)
//           KSC --KP2 ---> KST read  (admission -> media read -> response)
//
// `top_phases` and `io_lifecycle_latency` consume these tables.

export interface LifecyclePhase {
  /** Service the phase lives in (kms | kas | kst). */
  service: "kms" | "kas" | "kst";
  /** RPC the phase is a child of. */
  rpc: string;
  /** Phase label exactly as it appears in Prometheus. */
  phase: string;
  /** Short description of what happens in this phase. */
  description: string;
}

/**
 * WRITE lifecycle. KMS reserve phases (initiate_object_write) -> KST write
 * execution/media phases -> KMS commit phases. Phases listed in the order time
 * is actually spent within each stage.
 */
export const WRITE_LIFECYCLE: LifecyclePhase[] = [
  // --- Stage 1: KMS reserve (initiate_object_write) ---
  { service: "kms", rpc: "initiate_object_write", phase: "request_decode", description: "Decode the initiate-write request." },
  { service: "kms", rpc: "initiate_object_write", phase: "key_normalize", description: "Normalize the object key." },
  { service: "kms", rpc: "initiate_object_write", phase: "bucket_context_load", description: "Load bucket context (cache miss path)." },
  { service: "kms", rpc: "initiate_object_write", phase: "profile_select", description: "Select the EC profile for the bucket." },
  { service: "kms", rpc: "initiate_object_write", phase: "profile_validate", description: "Validate the selected EC profile." },
  { service: "kms", rpc: "initiate_object_write", phase: "reserve_route_resolve", description: "Resolve which allocation shard owns the placement (route cache)." },
  { service: "kms", rpc: "initiate_object_write", phase: "reservation_cache_acquire", description: "Acquire a reservation from the KMS reservation cache." },
  { service: "kms", rpc: "initiate_object_write", phase: "store_reserve_object_write_window_total", description: "Persist the reserved write window (FDB store)." },
  { service: "kms", rpc: "initiate_object_write", phase: "store_initiate_write_total", description: "Total store time for initiate-write." },
  // --- Stage 2: KST write execution + media ---
  { service: "kst", rpc: "write", phase: "request_decode", description: "Decode the KP2 write frame header." },
  { service: "kst", rpc: "write", phase: "ingress_queue_wait", description: "Wait in the ingress admission queue." },
  { service: "kst", rpc: "write", phase: "body_stream_receive", description: "Receive the payload body over HTTP/2." },
  { service: "kst", rpc: "write", phase: "body_collect", description: "Collect the streamed body into a buffer." },
  { service: "kst", rpc: "write", phase: "execution_queue_wait", description: "Wait for an execution-group slot." },
  { service: "kst", rpc: "write", phase: "route_execute", description: "Dispatch into the write execution group." },
  { service: "kst", rpc: "write", phase: "location_map", description: "Map the chunk to a raw-device location." },
  { service: "kst", rpc: "write", phase: "media_write_prepare", description: "Prepare the aligned direct-I/O write." },
  { service: "kst", rpc: "write", phase: "media_crc", description: "Compute the payload CRC." },
  { service: "kst", rpc: "write", phase: "media_write_io", description: "Direct-I/O write to the raw device." },
  { service: "kst", rpc: "write", phase: "media_fsync", description: "fsync/flush the media write." },
  { service: "kst", rpc: "write", phase: "kix_lookup", description: "Look up the KIX index slot." },
  { service: "kst", rpc: "write", phase: "kix_publish", description: "Publish the new chunk location into KIX." },
  { service: "kst", rpc: "write", phase: "publication_retry", description: "Retry slot publication if contended." },
  { service: "kst", rpc: "write", phase: "response_encode", description: "Encode the KP2 write response." },
  { service: "kst", rpc: "write", phase: "response_send_headers", description: "Send the response headers." },
  { service: "kst", rpc: "write", phase: "response_send_body", description: "Send the response body." },
  { service: "kst", rpc: "write", phase: "response_send", description: "Total response-send time." },
  // --- Stage 3: KMS commit (commit_object_write) ---
  { service: "kms", rpc: "commit_object_write", phase: "request_decode", description: "Decode the commit request." },
  { service: "kms", rpc: "commit_object_write", phase: "queue_finalize_reservations", description: "Queue finalize of the placement reservations." },
  { service: "kms", rpc: "commit_object_write", phase: "store_commit_object_write_total", description: "Persist the committed object head (FDB store)." },
];

/**
 * READ lifecycle. KMS resolve_object_read -> KST read execution/media phases.
 * The KST read RPC reuses the same execution/media phase vocabulary as write;
 * only the media direction differs (payload_read instead of write_io).
 */
export const READ_LIFECYCLE: LifecyclePhase[] = [
  // --- Stage 1: KMS resolve placement ---
  { service: "kms", rpc: "resolve_object_read", phase: "read_cache_invalidate_object", description: "Per-object read-cache invalidation check." },
  { service: "kms", rpc: "resolve_object_read", phase: "read_cache_invalidate_all", description: "Bulk read-cache invalidation check." },
  // --- Stage 2: KST read execution + media ---
  { service: "kst", rpc: "read", phase: "request_decode", description: "Decode the KP2 read frame header." },
  { service: "kst", rpc: "read", phase: "ingress_queue_wait", description: "Wait in the ingress admission queue." },
  { service: "kst", rpc: "read", phase: "execution_queue_wait", description: "Wait for an execution-group slot." },
  { service: "kst", rpc: "read", phase: "route_execute", description: "Dispatch into the read execution group." },
  { service: "kst", rpc: "read", phase: "kix_lookup", description: "Look up the chunk location in KIX." },
  { service: "kst", rpc: "read", phase: "location_map", description: "Map the chunk to a raw-device location." },
  { service: "kst", rpc: "read", phase: "media_payload_read", description: "Direct-I/O read of the payload from the raw device." },
  { service: "kst", rpc: "read", phase: "media_payload_copy", description: "Copy the payload into the response buffer." },
  { service: "kst", rpc: "read", phase: "media_header_validate", description: "Validate the on-media chunk header." },
  { service: "kst", rpc: "read", phase: "media_crc", description: "Verify the payload CRC." },
  { service: "kst", rpc: "read", phase: "response_encode", description: "Encode the KP2 read response." },
  { service: "kst", rpc: "read", phase: "response_send_headers", description: "Send the response headers." },
  { service: "kst", rpc: "read", phase: "response_send_body", description: "Send the payload body." },
  { service: "kst", rpc: "read", phase: "response_send", description: "Total response-send time." },
];

export type Direction = "write" | "read";

export function lifecycleFor(direction: Direction): LifecyclePhase[] {
  return direction === "write" ? WRITE_LIFECYCLE : READ_LIFECYCLE;
}
