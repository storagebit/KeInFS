// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::execution::{DirectExecutionHandle, DirectExecutionRequest, DirectExecutionSubmitError};
use crate::ingress::{IngressHandle, IngressRequest, IngressSubmitError};
use crate::stats::{RequestPhase, RpcKind, TargetIdentity, TargetLiveSnapshot, TargetRuntimeStats};
use bytes::Bytes;
use h2::server::SendResponse;
use h2::{client::SendRequest, RecvStream};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri};
use kix::{
    chunk_media_slot_index_for_record, ChunkId, ChunkMediaHandle, ChunkMediaLayoutSpec, KixClient,
    KixEngine, LocationKind, LocationRecord,
};
use kp2::{
    apply_packed_headers, apply_rate_limit_headers, decode_read_query, decode_write_request,
    encode_read_response, encode_write_reply, validate_declared_counts, validate_request_headers,
    LocationKindCode, PackedReadEntry, PackedReadLocation, PackedReadResponse, PackedWriteLocation,
    PackedWriteReply, PackedWriteReplyEntry, CONTENT_TYPE as KP2_CONTENT_TYPE, KIND_QUERY,
    KIND_READ, KIND_WRITE, LIMIT_CLASS_ALL, LIMIT_CLASS_READ, LIMIT_CLASS_WRITE,
    LIMIT_SCOPE_TARGET,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::io;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

mod connection;
#[cfg(test)]
mod tests;

pub(crate) use connection::serve_connection;

const HEADER_LOCATION_KIND: HeaderName = HeaderName::from_static("x-kst-location-kind");
const HEADER_PHYSICAL_OFFSET: HeaderName = HeaderName::from_static("x-kst-physical-offset");
const HEADER_LOGICAL_LENGTH: HeaderName = HeaderName::from_static("x-kst-logical-length");
const HEADER_STORED_LENGTH: HeaderName = HeaderName::from_static("x-kst-stored-length");
const HEADER_GENERATION: HeaderName = HeaderName::from_static("x-kst-generation");
const HEADER_CHECKSUM: HeaderName = HeaderName::from_static("x-kst-checksum");
const HEADER_GRANULE_INDEX: HeaderName = HeaderName::from_static("x-kst-granule-index");
const HEADER_SLOT_INDEX: HeaderName = HeaderName::from_static("x-kst-slot-index");
const HEADER_DRIVE_ID: HeaderName = HeaderName::from_static("x-kst-drive-id");
const RATE_LIMIT_RETRY_AFTER_MS: u64 = 25;
const PUBLICATION_RETRY_LIMIT: usize = 2;

pub(crate) struct TargetRouter {
    pub(crate) _engine: Arc<KixEngine>,
    pub(crate) client: KixClient,
    pub(crate) media: ChunkMediaHandle,
    pub(crate) stats: Arc<TargetRuntimeStats>,
    pub(crate) slot_publications: Arc<Vec<SlotPublication>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PublishedSlotOwner {
    pub(crate) chunk_id: ChunkId,
    pub(crate) record: LocationRecord,
}

/// Per-slot publication coordinator.
///
/// The slot lock is held ONLY long enough to reserve a publication lane (and to
/// later commit or roll back that reservation). The expensive media write,
/// durability barrier, and blocking KIX upsert all run with the lock released so
/// that distinct-slot and even back-to-back same-slot writes do not serialize on
/// those costs. Same-slot concurrency is bounded to one in-flight publication at
/// a time via [`SlotPublication::reserve`] so a new write always lands in the
/// lane opposite the currently readable payload (the two-lane scheme), and reads
/// continue to resolve through KIX with the existing stale-lane retry.
#[derive(Debug)]
pub(crate) struct SlotPublication {
    state: Mutex<SlotPublicationState>,
    idle: Condvar,
}

impl SlotPublication {
    fn new(state: SlotPublicationState) -> Self {
        Self {
            state: Mutex::new(state),
            idle: Condvar::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn current(&self) -> Option<PublishedSlotOwner> {
        self.state.lock().expect("slot state poisoned").current
    }

    /// Reserves the next publication lane for this slot, blocking while another
    /// publication for the same slot is in flight.
    ///
    /// `fresh_lane` chooses the lane when there is no in-flight reservation and
    /// no committed owner whose lane should be flipped; it computes the lane
    /// opposite the currently published record. The slot lock is released as
    /// soon as the reservation is taken, so the caller performs the media write,
    /// durability barrier, and KIX upsert without holding it.
    fn reserve<F>(&self, fresh_lane: F) -> Result<SlotPublicationReservation, ServiceError>
    where
        F: FnOnce(Option<LocationRecord>) -> Result<u64, ServiceError>,
    {
        let mut state = self.lock_state()?;
        while state.busy {
            state = self.idle.wait(state).map_err(|_| slot_poisoned_error())?;
        }
        let current = state.current;
        let lane = fresh_lane(current.map(|owner| owner.record))?;
        state.busy = true;
        Ok(SlotPublicationReservation { lane, current })
    }

    /// Commits a reservation after the media write + barrier + KIX upsert
    /// succeeded, installing the new owner if it wins by generation.
    ///
    /// Winner resolution mirrors recovery (`build_slot_publications`): an owner
    /// is only installed when no newer generation already occupies the slot. The
    /// slot is then marked idle and a waiter is woken.
    fn commit(&self, owner: PublishedSlotOwner) -> Result<(), ServiceError> {
        let mut state = self.lock_state()?;
        let install = match state.current {
            Some(existing) => existing.record.generation <= owner.record.generation,
            None => true,
        };
        if install {
            state.current = Some(owner);
        }
        state.busy = false;
        drop(state);
        self.idle.notify_one();
        Ok(())
    }

    /// Releases a reservation without publishing (the media write, barrier, or
    /// KIX upsert failed). The committed owner is left untouched.
    fn rollback(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.busy = false;
            drop(state);
            self.idle.notify_one();
        }
    }

    /// Marks the slot busy for a delete, blocking while a publication is in
    /// flight so the tombstone cannot race a concurrent write, and returns the
    /// current owner. The caller MUST call [`Self::finish_delete`] afterwards.
    fn begin_delete(&self) -> Result<Option<PublishedSlotOwner>, ServiceError> {
        let mut state = self.lock_state()?;
        while state.busy {
            state = self.idle.wait(state).map_err(|_| slot_poisoned_error())?;
        }
        state.busy = true;
        Ok(state.current)
    }

    /// Releases a delete reservation. When `cleared` is true the slot owner is
    /// removed (the tombstone + KIX delete succeeded); otherwise the owner is
    /// left intact (the delete failed and is reported as an error).
    fn finish_delete(&self, cleared: bool) {
        if let Ok(mut state) = self.state.lock() {
            if cleared {
                state.current = None;
            }
            state.busy = false;
            drop(state);
            self.idle.notify_one();
        }
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, SlotPublicationState>, ServiceError> {
        self.state.lock().map_err(|_| slot_poisoned_error())
    }
}

fn slot_poisoned_error() -> ServiceError {
    ServiceError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "KST could not acquire a slot publication guard because it is poisoned".to_string(),
        true,
    )
}

/// A held lane reservation for a single in-flight publication.
///
/// While a reservation is outstanding the slot is marked busy; other writers and
/// deletes for the same slot block in [`SlotPublication::reserve`] until it is
/// resolved via [`SlotPublicationReservation::commit`] or dropped (rollback).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SlotPublicationReservation {
    pub(crate) lane: u64,
    pub(crate) current: Option<PublishedSlotOwner>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SlotPublicationState {
    pub(crate) current: Option<PublishedSlotOwner>,
    /// True while a reservation handed out by `reserve` has not yet been
    /// committed or rolled back. Bounds same-slot concurrency to one in-flight
    /// publication so the non-current lane is never double-booked.
    pub(crate) busy: bool,
}

/// Per-entry state carried between the three phases of a KP2 packed write:
/// reserve+write (no barrier), single shared barrier, then publish.
enum PackEntryStage<'a> {
    /// The payload was written to its reserved lane but is not yet durable or
    /// published. The held reservation must be committed or rolled back.
    Written {
        slot_publication: &'a SlotPublication,
        reservation: SlotPublicationReservation,
        chunk_id: ChunkId,
        slot_index: u64,
        generation: u32,
        record: LocationRecord,
    },
    /// The entry already has a terminal reply (e.g. it failed media validation
    /// before any durable state was created).
    Failed(PackedWriteReplyEntry),
}

/// Rolls back every still-reserved staged write (used when a whole-request
/// error aborts phase A before the shared barrier).
fn rollback_staged(staged: &[PackEntryStage<'_>]) {
    for stage in staged {
        if let PackEntryStage::Written {
            slot_publication, ..
        } = stage
        {
            slot_publication.rollback();
        }
    }
}

pub(crate) struct TargetState {
    pub(crate) router: Arc<TargetRouter>,
    pub(crate) max_request_body_bytes: usize,
    pub(crate) max_active_streams: usize,
    pub(crate) active_stream_limit: Arc<Semaphore>,
    pub(crate) max_read_streams: usize,
    pub(crate) read_stream_limit: Arc<Semaphore>,
    pub(crate) max_write_streams: usize,
    pub(crate) write_stream_limit: Arc<Semaphore>,
    pub(crate) read_ingress: IngressHandle,
    pub(crate) write_ingress: IngressHandle,
    pub(crate) direct_read_execution: DirectExecutionHandle,
    pub(crate) direct_write_execution: DirectExecutionHandle,
    pub(crate) h2_initial_window_bytes: u32,
    pub(crate) h2_initial_connection_window_bytes: u32,
    pub(crate) h2_max_frame_bytes: u32,
    pub(crate) h2_max_header_list_bytes: u32,
    pub(crate) h2_max_concurrent_streams: u32,
    pub(crate) h2_max_send_buffer_bytes: usize,
}

impl TargetRouter {
    fn target_identity(&self) -> TargetIdentity {
        self.stats.snapshot().identity
    }

    fn live_snapshot(&self) -> TargetLiveSnapshot {
        self.stats.snapshot()
    }

    pub(crate) fn route_buffered(
        &self,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Vec<u8>,
    ) -> Result<ServiceResponse, ServiceError> {
        let path = uri.path();
        match (method, path) {
            (Method::GET, "/v1/info") => self.handle_info(body),
            (Method::GET, "/v1/stats") => self.handle_stats(body),
            (Method::PUT, "/v1/kp2/chunk-pack") => self.handle_kp2_write(&headers, body),
            (Method::POST, "/v1/kp2/chunk-pack/read") => self.handle_kp2_read(&headers, body),
            (method, path) if path.starts_with("/v1/chunk/") => {
                self.handle_chunk(method, uri, body)
            }
            (method, _) => Err(ServiceError::new(
                StatusCode::NOT_FOUND,
                format!(
                    "KST does not expose {} {}; use /v1/info, /v1/stats, /v1/chunk/<chunk-id>, /v1/kp2/chunk-pack, or /v1/kp2/chunk-pack/read",
                    method, uri
                ),
                true,
            )),
        }
    }

    fn handle_info(&self, body: Vec<u8>) -> Result<ServiceResponse, ServiceError> {
        if !body.is_empty() {
            return Err(ServiceError::new(
                StatusCode::BAD_REQUEST,
                "KST info requests must not include a request body".to_string(),
                true,
            ));
        }
        let document = self.target_identity();
        let payload = encode_json(&document).map_err(json_error)?;
        Ok(ServiceResponse::json(StatusCode::OK, payload))
    }

    fn handle_stats(&self, body: Vec<u8>) -> Result<ServiceResponse, ServiceError> {
        if !body.is_empty() {
            return Err(ServiceError::new(
                StatusCode::BAD_REQUEST,
                "KST stats requests must not include a request body".to_string(),
                true,
            ));
        }
        let payload = encode_json(&self.live_snapshot()).map_err(json_error)?;
        Ok(ServiceResponse::json(StatusCode::OK, payload))
    }

    fn handle_chunk(
        &self,
        method: Method,
        uri: Uri,
        body: Vec<u8>,
    ) -> Result<ServiceResponse, ServiceError> {
        let chunk_id = parse_chunk_id_from_path(uri.path())?;
        match method {
            Method::HEAD => self.handle_head(chunk_id, body),
            Method::GET => self.handle_read(chunk_id, body),
            Method::PUT => self.handle_write(chunk_id, uri.query(), body),
            Method::DELETE => self.handle_delete(chunk_id, body),
            other => Err(ServiceError::new(
                StatusCode::METHOD_NOT_ALLOWED,
                format!(
                    "KST does not allow {} on {}; use HEAD, GET, PUT, or DELETE",
                    other,
                    uri.path()
                ),
                true,
            )),
        }
    }

    fn handle_head(
        &self,
        chunk_id: ChunkId,
        body: Vec<u8>,
    ) -> Result<ServiceResponse, ServiceError> {
        if !body.is_empty() {
            return Err(ServiceError::new(
                StatusCode::BAD_REQUEST,
                "KST HEAD requests must not include a request body".to_string(),
                true,
            ));
        }
        let Some(record) = self.kix_lookup_timed(RpcKind::Head, chunk_id)? else {
            return Ok(ServiceResponse::empty(StatusCode::NOT_FOUND));
        };
        let Some(record) =
            self.resolve_published_record_for_head(RpcKind::Head, chunk_id, record)?
        else {
            return Ok(ServiceResponse::empty(StatusCode::NOT_FOUND));
        };
        Ok(ServiceResponse::with_location_headers(
            StatusCode::OK,
            self.location_document_timed(RpcKind::Head, record)?,
        ))
    }

    fn handle_read(
        &self,
        chunk_id: ChunkId,
        body: Vec<u8>,
    ) -> Result<ServiceResponse, ServiceError> {
        if !body.is_empty() {
            return Err(ServiceError::new(
                StatusCode::BAD_REQUEST,
                "KST read requests must not include a request body".to_string(),
                true,
            ));
        }
        let Some(record) = self.kix_lookup_timed(RpcKind::Read, chunk_id)? else {
            return Ok(ServiceResponse::empty(StatusCode::NOT_FOUND));
        };
        let Some((record, payload)) =
            self.read_published_payload(RpcKind::Read, chunk_id, record)?
        else {
            return Ok(ServiceResponse::empty(StatusCode::NOT_FOUND));
        };
        Ok(ServiceResponse::octets(
            StatusCode::OK,
            {
                self.stats.record_phase(
                    RpcKind::Read,
                    RequestPhase::MediaHeaderValidate,
                    payload.timing.header_validate,
                );
                self.stats.record_phase(
                    RpcKind::Read,
                    RequestPhase::MediaPayloadRead,
                    payload.timing.payload_read,
                );
                self.stats.record_phase(
                    RpcKind::Read,
                    RequestPhase::MediaPayloadCopy,
                    payload.timing.payload_copy,
                );
                self.stats
                    .record_phase(RpcKind::Read, RequestPhase::MediaCrc, payload.timing.crc);
                self.location_document_timed(RpcKind::Read, record)?
            },
            Bytes::from(payload.payload),
        ))
    }

    fn handle_write(
        &self,
        chunk_id: ChunkId,
        query: Option<&str>,
        body: Vec<u8>,
    ) -> Result<ServiceResponse, ServiceError> {
        let decode_started = Instant::now();
        let slot_index = parse_query_granule_index(query)?;
        let generation = parse_query_u32(query, "generation")?;
        self.stats.record_phase(
            RpcKind::Write,
            RequestPhase::RequestDecode,
            decode_started.elapsed(),
        );
        self.write_chunk_with_payload(chunk_id, slot_index, generation, body)
    }

    pub(crate) fn handle_direct_chunk_write(
        &self,
        chunk_id: ChunkId,
        slot_index: u64,
        generation: u32,
        body: Vec<u8>,
    ) -> Result<ServiceResponse, ServiceError> {
        self.write_chunk_with_payload(chunk_id, slot_index, generation, body)
    }

    pub(crate) fn handle_direct_chunk_read(
        &self,
        chunk_id: ChunkId,
    ) -> Result<ServiceResponse, ServiceError> {
        self.handle_read(chunk_id, Vec::new())
    }

    fn write_chunk_with_payload(
        &self,
        chunk_id: ChunkId,
        slot_index: u64,
        generation: u32,
        body: Vec<u8>,
    ) -> Result<ServiceResponse, ServiceError> {
        let slot_publication = self.slot_publication(slot_index)?;
        // Hold the slot lock ONLY to reserve the publication lane. The lookup,
        // media write, durability barrier, and KIX upsert below run with the
        // lock released.
        let lookup_started = Instant::now();
        let current_record = self.kix_lookup(chunk_id)?;
        self.stats.record_phase(
            RpcKind::Write,
            RequestPhase::KixLookup,
            lookup_started.elapsed(),
        );
        let reservation = slot_publication.reserve(|current_slot_record| {
            let baseline = current_slot_record.or_else(|| match current_record {
                Some(record)
                    if chunk_media_slot_index_for_record(self.media.layout(), record)
                        .ok()
                        .is_some_and(|mapped_slot| mapped_slot == slot_index) =>
                {
                    Some(record)
                }
                _ => None,
            });
            self.fresh_publication_lane(slot_index, baseline)
        })?;

        // GUARD D (allocator-safety backstop). The slot already has a committed
        // owner of a DIFFERENT identity → this write would overwrite committed
        // object data. With the durable committed-occupancy fix the allocator never
        // hands out an occupied granule, so this fires only on a safety breach:
        // refuse loudly (formerly the prior owner was silently retired + overwritten).
        // Rejecting here — before the media write and KIX upsert — leaves the
        // committed owner fully intact. See poc/kas/DESIGN_KAS_COMMITTED_OCCUPANCY.md.
        if let Some(owner) = reservation.current {
            if owner.chunk_id != chunk_id {
                slot_publication.rollback();
                self.stats.record_committed_slot_write_rejection();
                return Err(ServiceError::new(
                    StatusCode::CONFLICT,
                    format!(
                        "KST refused to write chunk {:?} to slot {}: it already holds committed \
                         chunk {:?}. Refusing to overwrite committed data (the allocator handed out \
                         an occupied granule).",
                        chunk_id, slot_index, owner.chunk_id
                    ),
                    false,
                ));
            }
        }

        // From here on the reservation must be resolved (commit or rollback)
        // before returning so the slot does not stay busy.
        match self.publish_reserved(chunk_id, slot_index, generation, &body, reservation) {
            Ok(record) => {
                slot_publication.commit(PublishedSlotOwner { chunk_id, record })?;
                let map_started = Instant::now();
                let location = LocationRecordDocument::from_record(record, slot_index);
                self.stats.record_phase(
                    RpcKind::Write,
                    RequestPhase::LocationMap,
                    map_started.elapsed(),
                );
                Ok(ServiceResponse::with_location_headers_and_accounted_bytes(
                    StatusCode::CREATED,
                    location,
                    record.stored_length as u64,
                ))
            }
            Err(err) => {
                slot_publication.rollback();
                Err(err)
            }
        }
    }

    /// Writes the payload into the reserved lane, issues a single durability
    /// barrier, then publishes the location into KIX (and retires a superseded
    /// chunk). Records phase timings. Returns the published record on success.
    ///
    /// Crash-consistency: the KIX upsert (which is what makes the chunk
    /// readable) happens ONLY after `fdatasync` has made the media write
    /// durable. The slot owner is committed by the caller after this returns.
    fn publish_reserved(
        &self,
        chunk_id: ChunkId,
        slot_index: u64,
        generation: u32,
        body: &[u8],
        reservation: SlotPublicationReservation,
    ) -> Result<LocationRecord, ServiceError> {
        let media_result = self
            .media
            .write_payload_to_lane_unsynced(
                slot_index,
                reservation.lane,
                chunk_id,
                generation,
                body,
            )
            .map_err(|err| {
                map_media_error(
                    err,
                    format!(
                        "KST write rejected slot {} for chunk {:?}; the body does not match the configured target layout",
                        slot_index, chunk_id
                    ),
                )
            })?;
        self.stats.record_phase(
            RpcKind::Write,
            RequestPhase::MediaWritePrepare,
            media_result.timing.prepare,
        );
        self.stats.record_phase(
            RpcKind::Write,
            RequestPhase::MediaWriteIo,
            media_result.timing.write_io,
        );
        let fsync_started = Instant::now();
        self.media.fdatasync().map_err(|err| {
            map_media_error(
                err,
                format!(
                    "KST wrote chunk {:?} to raw media for slot {} but could not flush the durability barrier",
                    chunk_id, slot_index
                ),
            )
        })?;
        self.stats.record_phase(
            RpcKind::Write,
            RequestPhase::MediaFsync,
            fsync_started.elapsed(),
        );
        let record = media_result.record;
        let publish_started = Instant::now();
        self.client.upsert(chunk_id, record).map_err(|err| {
            ServiceError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "KST wrote chunk {:?} to raw media but could not publish the location into KIX: {}. The chunk may be recovered by rebuild-from-media, but the target is inconsistent until that happens.",
                    chunk_id, err
                ),
                true,
            )
        })?;
        if let Some(owner) = reservation.current {
            if owner.chunk_id != chunk_id {
                self.client.delete(owner.chunk_id).map_err(|err| {
                    ServiceError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "KST published chunk {:?} into slot {} but could not retire the superseded chunk {:?} from KIX: {}. Rebuild-from-media can recover the target, but the live index is inconsistent until then.",
                            chunk_id, slot_index, owner.chunk_id, err
                        ),
                        true,
                    )
                })?;
            }
        }
        self.stats.record_phase(
            RpcKind::Write,
            RequestPhase::KixPublish,
            publish_started.elapsed(),
        );
        Ok(record)
    }

    /// Phase A of a packed write: reserve a lane and write each entry's payload
    /// without a per-entry barrier. On a whole-request error (poisoned slot,
    /// KIX lookup failure, layout error) every reservation already taken is
    /// rolled back so no slot is left marked busy.
    fn stage_packed_writes(
        &self,
        entries: Vec<kp2::PackedWriteEntry>,
        media_prepare: &mut std::time::Duration,
        media_write_io: &mut std::time::Duration,
        write_lookup: &mut std::time::Duration,
    ) -> Result<Vec<PackEntryStage<'_>>, ServiceError> {
        let mut staged: Vec<PackEntryStage> = Vec::with_capacity(entries.len());
        // A single packed write must not target the same logical slot twice. The
        // per-slot publication guard serializes same-slot writers, so two entries
        // sharing a slot would make the second reservation block on the first
        // within this same thread, deadlocking the request. Reject up front,
        // before any lane is reserved (so no slot is left marked busy).
        {
            let mut seen_slots = std::collections::HashSet::with_capacity(entries.len());
            for entry in &entries {
                if !seen_slots.insert(entry.slot_index) {
                    return Err(ServiceError::new(
                        StatusCode::BAD_REQUEST,
                        format!(
                            "KST rejected a packed write that targets slot {} more than once in one pack",
                            entry.slot_index
                        ),
                        true,
                    ));
                }
            }
        }
        for entry in entries {
            let chunk_id = ChunkId(entry.chunk_id.0);
            let slot_index = entry.slot_index;
            let generation = entry.generation;
            let result = (|| {
                let slot_publication = self.slot_publication(slot_index)?;
                let lookup_started = Instant::now();
                let current_record = self.kix_lookup(chunk_id)?;
                *write_lookup += lookup_started.elapsed();
                let reservation = slot_publication.reserve(|current_slot_record| {
                    let baseline = current_slot_record.or_else(|| match current_record {
                        Some(record)
                            if chunk_media_slot_index_for_record(self.media.layout(), record)
                                .ok()
                                .is_some_and(|mapped_slot| mapped_slot == slot_index) =>
                        {
                            Some(record)
                        }
                        _ => None,
                    });
                    self.fresh_publication_lane(slot_index, baseline)
                })?;
                Ok::<_, ServiceError>((slot_publication, reservation))
            })();
            let (slot_publication, reservation) = match result {
                Ok(reserved) => reserved,
                Err(err) => {
                    rollback_staged(&staged);
                    return Err(err);
                }
            };
            match self.media.write_payload_to_lane_unsynced(
                slot_index,
                reservation.lane,
                chunk_id,
                generation,
                &entry.payload,
            ) {
                Ok(media_result) => {
                    *media_prepare += media_result.timing.prepare;
                    *media_write_io += media_result.timing.write_io;
                    staged.push(PackEntryStage::Written {
                        slot_publication,
                        reservation,
                        chunk_id,
                        slot_index,
                        generation,
                        record: media_result.record,
                    });
                }
                Err(err) => {
                    slot_publication.rollback();
                    staged.push(PackEntryStage::Failed(PackedWriteReplyEntry {
                        chunk_id: kp2::ChunkId(chunk_id.0),
                        slot_index,
                        requested_generation: generation,
                        status_code: 422,
                        location: None,
                        error: Some(format!(
                            "KST rejected the packed entry on raw media: {}",
                            err
                        )),
                    }));
                }
            }
        }
        Ok(staged)
    }

    fn handle_kp2_write(
        &self,
        headers: &HeaderMap,
        body: Vec<u8>,
    ) -> Result<ServiceResponse, ServiceError> {
        let decode_started = Instant::now();
        validate_request_headers(headers, KIND_WRITE).map_err(kp2_request_error)?;
        let pack = decode_write_request(&body).map_err(kp2_request_error)?;
        let total_payload_bytes = pack
            .entries
            .iter()
            .map(|entry| entry.payload.len())
            .sum::<usize>();
        validate_declared_counts(headers, pack.entries.len(), total_payload_bytes)
            .map_err(kp2_request_error)?;
        self.stats.record_phase(
            RpcKind::Write,
            RequestPhase::RequestDecode,
            decode_started.elapsed(),
        );

        // Phase A: reserve a lane and write every entry's payload WITHOUT a
        // per-entry durability barrier. Nothing is published into KIX yet, so a
        // crash here leaves no readable record for any written entry. A
        // whole-request error rolls back every reservation already taken so no
        // slot is left marked busy.
        let mut media_prepare = std::time::Duration::ZERO;
        let mut media_write_io = std::time::Duration::ZERO;
        let mut write_lookup = std::time::Duration::ZERO;
        let staged = self.stage_packed_writes(
            pack.entries,
            &mut media_prepare,
            &mut media_write_io,
            &mut write_lookup,
        )?;

        // Phase B: a SINGLE durability barrier covers every staged write in the
        // pack instead of one fdatasync per entry.
        let any_written = staged
            .iter()
            .any(|stage| matches!(stage, PackEntryStage::Written { .. }));
        let fsync_started = Instant::now();
        let barrier = if any_written {
            self.media.fdatasync()
        } else {
            Ok(())
        };
        let media_fsync = fsync_started.elapsed();
        let barrier_error = barrier.err();

        // Phase C: only now publish each durable entry into KIX. If the shared
        // barrier failed, no entry is published (publish-before-sync is never
        // allowed); every staged write is rolled back and reported as 500.
        let mut entries = Vec::with_capacity(staged.len());
        let mut successes = 0_usize;
        let mut kix_publish = std::time::Duration::ZERO;
        let mut location_map = std::time::Duration::ZERO;
        for stage in staged {
            match stage {
                PackEntryStage::Failed(reply) => entries.push(reply),
                PackEntryStage::Written {
                    slot_publication,
                    reservation,
                    chunk_id,
                    slot_index,
                    generation,
                    record,
                } => {
                    if let Some(err) = &barrier_error {
                        slot_publication.rollback();
                        entries.push(PackedWriteReplyEntry {
                            chunk_id: kp2::ChunkId(chunk_id.0),
                            slot_index,
                            requested_generation: generation,
                            status_code: 500,
                            location: None,
                            error: Some(format!(
                                "KST wrote the chunk to raw media but could not flush the shared durability barrier: {}",
                                err
                            )),
                        });
                        continue;
                    }
                    // GUARD D (packed path): refuse to overwrite a slot already
                    // holding a committed chunk of a different identity. Rejected
                    // before the KIX upsert/retire, so the committed owner is intact.
                    // See poc/kas/DESIGN_KAS_COMMITTED_OCCUPANCY.md.
                    if let Some(owner) = reservation.current {
                        if owner.chunk_id != chunk_id {
                            slot_publication.rollback();
                            self.stats.record_committed_slot_write_rejection();
                            entries.push(PackedWriteReplyEntry {
                                chunk_id: kp2::ChunkId(chunk_id.0),
                                slot_index,
                                requested_generation: generation,
                                status_code: 409,
                                location: None,
                                error: Some(format!(
                                    "KST refused to write chunk {:?} to slot {}: it already holds \
                                     committed chunk {:?}; refusing to overwrite committed data",
                                    chunk_id, slot_index, owner.chunk_id
                                )),
                            });
                            continue;
                        }
                    }
                    let publish_started = Instant::now();
                    match self.client.upsert(chunk_id, record) {
                        Ok(()) => {
                            let mut publish_cost = publish_started.elapsed();
                            if let Some(owner) = reservation.current {
                                if owner.chunk_id != chunk_id {
                                    let retire_started = Instant::now();
                                    match self.client.delete(owner.chunk_id) {
                                        Ok(()) => {
                                            publish_cost += retire_started.elapsed();
                                        }
                                        Err(err) => {
                                            publish_cost += retire_started.elapsed();
                                            kix_publish += publish_cost;
                                            slot_publication.rollback();
                                            entries.push(PackedWriteReplyEntry {
                                                chunk_id: kp2::ChunkId(chunk_id.0),
                                                slot_index,
                                                requested_generation: generation,
                                                status_code: 500,
                                                location: self
                                                    .location_document(record)
                                                    .ok()
                                                    .map(Into::into),
                                                error: Some(format!(
                                                    "KST published the chunk into KIX but could not retire the superseded chunk {:?}: {}",
                                                    owner.chunk_id, err
                                                )),
                                            });
                                            continue;
                                        }
                                    }
                                }
                            }
                            kix_publish += publish_cost;
                            slot_publication.commit(PublishedSlotOwner { chunk_id, record })?;
                            let map_started = Instant::now();
                            let location = self.location_document(record).map(Into::into);
                            location_map += map_started.elapsed();
                            match location {
                                Ok(location) => {
                                    successes += 1;
                                    entries.push(PackedWriteReplyEntry {
                                        chunk_id: kp2::ChunkId(chunk_id.0),
                                        slot_index,
                                        requested_generation: generation,
                                        status_code: 201,
                                        location: Some(location),
                                        error: None,
                                    });
                                }
                                Err(err) => {
                                    // The chunk is durable and published in KIX
                                    // (it is the live slot owner), but its
                                    // record could not be mapped back to a slot
                                    // for the reply. Report a per-entry failure
                                    // without disturbing the committed owner.
                                    entries.push(PackedWriteReplyEntry {
                                        chunk_id: kp2::ChunkId(chunk_id.0),
                                        slot_index,
                                        requested_generation: generation,
                                        status_code: 500,
                                        location: None,
                                        error: Some(format!(
                                            "KST published chunk {:?} but could not map its record back to a slot for the reply: {}",
                                            chunk_id, err.public_message
                                        )),
                                    });
                                }
                            }
                        }
                        Err(err) => {
                            kix_publish += publish_started.elapsed();
                            slot_publication.rollback();
                            let map_started = Instant::now();
                            let location = self.location_document(record).ok().map(Into::into);
                            location_map += map_started.elapsed();
                            entries.push(PackedWriteReplyEntry {
                                chunk_id: kp2::ChunkId(chunk_id.0),
                                slot_index,
                                requested_generation: generation,
                                status_code: 500,
                                location,
                                error: Some(format!(
                                    "KST wrote the chunk to raw media but could not publish it into KIX: {}",
                                    err
                                )),
                            });
                        }
                    }
                }
            }
        }
        self.stats.record_phase(
            RpcKind::Write,
            RequestPhase::MediaWritePrepare,
            media_prepare,
        );
        self.stats
            .record_phase(RpcKind::Write, RequestPhase::KixLookup, write_lookup);
        self.stats
            .record_phase(RpcKind::Write, RequestPhase::MediaWriteIo, media_write_io);
        self.stats
            .record_phase(RpcKind::Write, RequestPhase::MediaFsync, media_fsync);
        self.stats
            .record_phase(RpcKind::Write, RequestPhase::KixPublish, kix_publish);
        self.stats
            .record_phase(RpcKind::Write, RequestPhase::LocationMap, location_map);
        self.stats
            .record_kp2_write(entries.len(), total_payload_bytes);

        let entry_count = entries.len();
        let status = if successes == entry_count {
            StatusCode::CREATED
        } else {
            StatusCode::from_u16(207).expect("207 Multi-Status is a valid HTTP status code")
        };
        let encode_started = Instant::now();
        let payload =
            encode_write_reply(&PackedWriteReply { entries }).map_err(kp2_internal_error)?;
        self.stats.record_phase(
            RpcKind::Write,
            RequestPhase::ResponseEncode,
            encode_started.elapsed(),
        );
        Ok(ServiceResponse::bytes_with_headers(
            status,
            KP2_CONTENT_TYPE,
            Bytes::from(payload),
            total_payload_bytes as u64,
            kp2_headers(KIND_WRITE, entry_count, total_payload_bytes)
                .map_err(kp2_internal_error)?,
        ))
    }

    fn handle_kp2_read(
        &self,
        headers: &HeaderMap,
        body: Vec<u8>,
    ) -> Result<ServiceResponse, ServiceError> {
        let decode_started = Instant::now();
        validate_request_headers(headers, KIND_QUERY).map_err(kp2_request_error)?;
        let query = decode_read_query(&body).map_err(kp2_request_error)?;
        validate_declared_counts(headers, query.chunk_ids.len(), 0).map_err(kp2_request_error)?;
        self.stats.record_phase(
            RpcKind::Read,
            RequestPhase::RequestDecode,
            decode_started.elapsed(),
        );

        let mut entries = Vec::with_capacity(query.chunk_ids.len());
        let mut total_payload_bytes = 0_usize;
        let mut kix_lookup = std::time::Duration::ZERO;
        let mut media_header_validate = std::time::Duration::ZERO;
        let mut media_payload_read = std::time::Duration::ZERO;
        let mut media_payload_copy = std::time::Duration::ZERO;
        let mut media_crc = std::time::Duration::ZERO;
        let mut location_map = std::time::Duration::ZERO;
        for (index, query_chunk_id) in query.chunk_ids.iter().enumerate() {
            let query_chunk_id = *query_chunk_id;
            let chunk_id = ChunkId(query_chunk_id.0);
            // Byte-granular: the sub-range (if any) the reader wants of this chunk.
            let chunk_range = query.ranges.as_ref().and_then(|r| r.get(index).copied());
            let lookup_started = Instant::now();
            let maybe_record = self.kix_lookup(chunk_id)?;
            kix_lookup += lookup_started.elapsed();
            let Some(record) = maybe_record else {
                entries.push(PackedReadEntry {
                    chunk_id: query_chunk_id,
                    status_code: 404,
                    location: None,
                    payload: Vec::new(),
                });
                continue;
            };
            match self.read_published_payload(RpcKind::Read, chunk_id, record) {
                Ok(Some((record, payload))) => {
                    media_header_validate += payload.timing.header_validate;
                    media_payload_read += payload.timing.payload_read;
                    media_payload_copy += payload.timing.payload_copy;
                    media_crc += payload.timing.crc;
                    // Byte-granular: serve only the requested sub-range of the
                    // chunk's logical payload. KST still read + CRC-validated the
                    // whole chunk; this trims the KP2 transfer to the asked bytes.
                    let served = match chunk_range {
                        Some(r) => {
                            let start = (r.offset as usize).min(payload.payload.len());
                            let end = start
                                .saturating_add(r.length as usize)
                                .min(payload.payload.len());
                            payload.payload[start..end].to_vec()
                        }
                        None => payload.payload,
                    };
                    total_payload_bytes += served.len();
                    let map_started = Instant::now();
                    let slot_index = chunk_media_slot_index_for_record(self.media.layout(), record)
                        .map_err(|err| {
                            ServiceError::new(
                                StatusCode::PRECONDITION_FAILED,
                                format!(
                                    "KST could not map the KIX location record at offset {} back to a chunk-media slot: {}",
                                    record.physical_offset, err
                                ),
                                true,
                            )
                        })?;
                    location_map += map_started.elapsed();
                    entries.push(PackedReadEntry {
                        chunk_id: query_chunk_id,
                        status_code: 200,
                        location: Some(PackedReadLocation {
                            drive_id: record.drive_id,
                            location_kind: match record.location_kind {
                                LocationKind::Extent => LocationKindCode::Extent,
                                LocationKind::PackedContainer => LocationKindCode::PackedContainer,
                            },
                            physical_offset: record.physical_offset,
                            logical_length: record.logical_length,
                            stored_length: record.stored_length,
                            generation: record.generation,
                            checksum: record.checksum,
                            slot_index,
                        }),
                        payload: served,
                    });
                }
                Ok(None) => entries.push(PackedReadEntry {
                    chunk_id: query_chunk_id,
                    status_code: 404,
                    location: None,
                    payload: Vec::new(),
                }),
                Err(_) => entries.push(PackedReadEntry {
                    chunk_id: query_chunk_id,
                    status_code: 500,
                    location: None,
                    payload: Vec::new(),
                }),
            }
        }
        self.stats
            .record_phase(RpcKind::Read, RequestPhase::KixLookup, kix_lookup);
        self.stats.record_phase(
            RpcKind::Read,
            RequestPhase::MediaHeaderValidate,
            media_header_validate,
        );
        self.stats.record_phase(
            RpcKind::Read,
            RequestPhase::MediaPayloadRead,
            media_payload_read,
        );
        self.stats.record_phase(
            RpcKind::Read,
            RequestPhase::MediaPayloadCopy,
            media_payload_copy,
        );
        self.stats
            .record_phase(RpcKind::Read, RequestPhase::MediaCrc, media_crc);
        self.stats
            .record_phase(RpcKind::Read, RequestPhase::LocationMap, location_map);
        let pack = PackedReadResponse { entries };
        let encode_started = Instant::now();
        let encoded = encode_read_response(&pack).map_err(kp2_internal_error)?;
        self.stats.record_phase(
            RpcKind::Read,
            RequestPhase::ResponseEncode,
            encode_started.elapsed(),
        );
        self.stats
            .record_kp2_read(pack.entries.len(), total_payload_bytes);
        Ok(ServiceResponse::bytes_with_headers(
            StatusCode::OK,
            KP2_CONTENT_TYPE,
            Bytes::from(encoded),
            total_payload_bytes as u64,
            kp2_headers(KIND_READ, pack.entries.len(), total_payload_bytes)
                .map_err(kp2_internal_error)?,
        ))
    }

    fn handle_delete(
        &self,
        chunk_id: ChunkId,
        body: Vec<u8>,
    ) -> Result<ServiceResponse, ServiceError> {
        if !body.is_empty() {
            return Err(ServiceError::new(
                StatusCode::BAD_REQUEST,
                "KST delete requests must not include a request body".to_string(),
                true,
            ));
        }
        let Some(record) = self.kix_lookup(chunk_id)? else {
            let payload = encode_json(&DeleteChunkDocument {
                deleted: false,
                tombstone_generation: 0,
            })
            .map_err(json_error)?;
            return Ok(ServiceResponse::json(StatusCode::OK, payload));
        };
        let tombstone_generation = record.generation.checked_add(1).ok_or_else(|| {
            ServiceError::new(
                StatusCode::PRECONDITION_FAILED,
                format!(
                    "KST cannot tombstone chunk {:?} because the generation {} would overflow",
                    chunk_id, record.generation
                ),
                true,
            )
        })?;
        let slot_index = chunk_media_slot_index_for_record(self.media.layout(), record).map_err(
            |err| {
                ServiceError::new(
                    StatusCode::PRECONDITION_FAILED,
                    format!(
                        "KST could not map the current KIX location record at offset {} back to a chunk-media slot before delete: {}",
                        record.physical_offset, err
                    ),
                    true,
                )
            },
        )?;
        let slot_publication = self.slot_publication(slot_index)?;
        // Mark the slot busy (waiting out any in-flight publication) so the
        // tombstone cannot race a concurrent same-slot write, but do not hold
        // the slot lock across the media tombstone write or the KIX delete.
        let current_owner = slot_publication.begin_delete()?;
        if let Some(owner) = current_owner {
            if owner.chunk_id != chunk_id {
                slot_publication.finish_delete(false);
                return Err(ServiceError::new(
                    StatusCode::CONFLICT,
                    format!(
                        "KST refused to delete chunk {:?} from slot {} because the slot is currently published to chunk {:?}",
                        chunk_id, slot_index, owner.chunk_id
                    ),
                    true,
                ));
            }
        }
        let delete_result = self.tombstone_and_retire(chunk_id, record, tombstone_generation);
        slot_publication.finish_delete(delete_result.is_ok());
        delete_result?;
        let payload = encode_json(&DeleteChunkDocument {
            deleted: true,
            tombstone_generation,
        })
        .map_err(json_error)?;
        Ok(ServiceResponse::json(StatusCode::OK, payload))
    }

    /// Writes a tombstone to raw media and removes the chunk from KIX. The KIX
    /// delete (which makes reads see the chunk as gone) happens only after the
    /// media tombstone write has been flushed by the media layer's barrier.
    fn tombstone_and_retire(
        &self,
        chunk_id: ChunkId,
        record: LocationRecord,
        tombstone_generation: u32,
    ) -> Result<(), ServiceError> {
        self.media
            .write_tombstone_against_current(chunk_id, record, tombstone_generation)
            .map_err(|err| {
                map_media_error(
                    err,
                    format!(
                        "KST could not tombstone chunk {:?} on raw media before deleting it from KIX",
                        chunk_id
                    ),
                )
            })?;
        self.client.delete(chunk_id).map_err(|err| {
            ServiceError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "KST tombstoned chunk {:?} on raw media but could not remove it from KIX: {}. Reads through KST now treat the chunk as missing, but KIX requires repair or rebuild to become clean again.",
                    chunk_id, err
                ),
                true,
            )
        })
    }

    fn kix_lookup(&self, chunk_id: ChunkId) -> Result<Option<LocationRecord>, ServiceError> {
        self.client.get(chunk_id).map_err(|err| {
            ServiceError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("KST could not resolve chunk {:?} in KIX: {}", chunk_id, err),
                true,
            )
        })
    }

    fn kix_lookup_timed(
        &self,
        rpc: RpcKind,
        chunk_id: ChunkId,
    ) -> Result<Option<LocationRecord>, ServiceError> {
        let started = Instant::now();
        let result = self.kix_lookup(chunk_id);
        self.stats
            .record_phase(rpc, RequestPhase::KixLookup, started.elapsed());
        result
    }

    fn resolve_published_record_for_head(
        &self,
        rpc: RpcKind,
        chunk_id: ChunkId,
        initial_record: LocationRecord,
    ) -> Result<Option<LocationRecord>, ServiceError> {
        let mut record = initial_record;
        for attempt in 0..=PUBLICATION_RETRY_LIMIT {
            let validate_started = Instant::now();
            match self.media.validate_live_record(chunk_id, record) {
                Ok(()) => {
                    self.stats.record_phase(
                        rpc,
                        RequestPhase::MediaHeaderValidate,
                        validate_started.elapsed(),
                    );
                    return Ok(Some(record));
                }
                Err(err) if publication_retryable(&err) && attempt < PUBLICATION_RETRY_LIMIT => {
                    self.stats.record_phase(
                        rpc,
                        RequestPhase::MediaHeaderValidate,
                        validate_started.elapsed(),
                    );
                    let retry_started = Instant::now();
                    let Some(latest) = self.kix_lookup_timed(rpc, chunk_id)? else {
                        self.stats.record_phase(
                            rpc,
                            RequestPhase::PublicationRetry,
                            retry_started.elapsed(),
                        );
                        return Ok(None);
                    };
                    self.stats.record_phase(
                        rpc,
                        RequestPhase::PublicationRetry,
                        retry_started.elapsed(),
                    );
                    if latest == record {
                        return Err(map_media_error(
                            err,
                            format!(
                                "KST head validation failed for chunk {:?}; KIX and chunk media disagree",
                                chunk_id
                            ),
                        ));
                    }
                    record = latest;
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    self.stats.record_phase(
                        rpc,
                        RequestPhase::MediaHeaderValidate,
                        validate_started.elapsed(),
                    );
                    return Ok(None);
                }
                Err(err) => {
                    self.stats.record_phase(
                        rpc,
                        RequestPhase::MediaHeaderValidate,
                        validate_started.elapsed(),
                    );
                    return Err(map_media_error(
                        err,
                        format!(
                            "KST head validation failed for chunk {:?}; KIX and chunk media disagree",
                            chunk_id
                        ),
                    ));
                }
            }
        }
        Err(ServiceError::new(
            StatusCode::CONFLICT,
            format!(
                "KST could not stabilize the published record for chunk {:?} after {} retries; the target is under publication churn",
                chunk_id, PUBLICATION_RETRY_LIMIT
            ),
            true,
        ))
    }

    fn read_published_payload(
        &self,
        rpc: RpcKind,
        chunk_id: ChunkId,
        initial_record: LocationRecord,
    ) -> Result<Option<(LocationRecord, kix::ChunkMediaReadResult)>, ServiceError> {
        let mut record = initial_record;
        for attempt in 0..=PUBLICATION_RETRY_LIMIT {
            match self.media.read_payload_timed(chunk_id, record) {
                Ok(payload) => return Ok(Some((record, payload))),
                Err(err) if publication_retryable(&err) && attempt < PUBLICATION_RETRY_LIMIT => {
                    let retry_started = Instant::now();
                    let Some(latest) = self.kix_lookup_timed(rpc, chunk_id)? else {
                        self.stats.record_phase(
                            rpc,
                            RequestPhase::PublicationRetry,
                            retry_started.elapsed(),
                        );
                        return Ok(None);
                    };
                    self.stats.record_phase(
                        rpc,
                        RequestPhase::PublicationRetry,
                        retry_started.elapsed(),
                    );
                    if latest == record {
                        return Err(map_media_error(
                            err,
                            format!(
                                "KST read failed for chunk {:?}; the media record is missing or corrupt",
                                chunk_id
                            ),
                        ));
                    }
                    record = latest;
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(err) => {
                    return Err(map_media_error(
                        err,
                        format!(
                            "KST read failed for chunk {:?}; the media record is missing or corrupt",
                            chunk_id
                        ),
                    ));
                }
            }
        }
        Err(ServiceError::new(
            StatusCode::CONFLICT,
            format!(
                "KST could not stabilize the published payload for chunk {:?} after {} retries; the target is under publication churn",
                chunk_id, PUBLICATION_RETRY_LIMIT
            ),
            true,
        ))
    }

    fn location_document(
        &self,
        record: LocationRecord,
    ) -> Result<LocationRecordDocument, ServiceError> {
        let slot_index = chunk_media_slot_index_for_record(self.media.layout(), record).map_err(|err| {
            ServiceError::new(
                StatusCode::PRECONDITION_FAILED,
                format!(
                    "KST could not map the KIX location record at offset {} back to a chunk-media slot: {}",
                    record.physical_offset, err
                ),
                true,
            )
        })?;
        Ok(LocationRecordDocument::from_record(record, slot_index))
    }

    fn location_document_timed(
        &self,
        rpc: RpcKind,
        record: LocationRecord,
    ) -> Result<LocationRecordDocument, ServiceError> {
        let started = Instant::now();
        let result = self.location_document(record);
        self.stats
            .record_phase(rpc, RequestPhase::LocationMap, started.elapsed());
        result
    }

    fn slot_publication(&self, slot_index: u64) -> Result<&SlotPublication, ServiceError> {
        self.slot_publications
            .get(slot_index as usize)
            .ok_or_else(|| {
                ServiceError::new(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "KST slot {} exceeds configured key_slots {}",
                        slot_index,
                        self.slot_publications.len()
                    ),
                    true,
                )
            })
    }

    /// Computes the publication lane a fresh write for `slot_index` should use
    /// given the currently published record. Pure layout math (no media I/O).
    fn fresh_publication_lane(
        &self,
        slot_index: u64,
        current_record: Option<LocationRecord>,
    ) -> Result<u64, ServiceError> {
        self.media
            .next_publication_lane_for_slot(slot_index, current_record)
            .map_err(|err| {
                ServiceError::new(
                    StatusCode::PRECONDITION_FAILED,
                    format!(
                        "KST could not select a publication lane for slot {}: {}",
                        slot_index, err
                    ),
                    true,
                )
            })
    }
}

pub(crate) fn build_slot_publications<I>(
    layout: &ChunkMediaLayoutSpec,
    key_slots: u64,
    entries: I,
) -> io::Result<Vec<SlotPublication>>
where
    I: IntoIterator<Item = (ChunkId, LocationRecord)>,
{
    let slot_count = key_slots.max(1) as usize;
    let mut publications = vec![SlotPublicationState::default(); slot_count];
    for (chunk_id, record) in entries {
        let slot_index = chunk_media_slot_index_for_record(layout, record).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "KST could not map recovered KIX record for chunk {:?} at offset {} back to a slot: {}",
                    chunk_id, record.physical_offset, err
                ),
            )
        })?;
        let Some(slot_state) = publications.get_mut(slot_index as usize) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "KST recovered KIX record for chunk {:?} into slot {} outside configured key_slots {}",
                    chunk_id, slot_index, key_slots
                ),
            ));
        };
        let candidate = PublishedSlotOwner { chunk_id, record };
        match slot_state.current {
            Some(existing) if existing.chunk_id != candidate.chunk_id => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "KST recovered conflicting live slot owners for slot {}: {:?} generation {} and {:?} generation {}. Run `kix check --fix` or rebuild from media before starting the target.",
                        slot_index,
                        existing.chunk_id,
                        existing.record.generation,
                        candidate.chunk_id,
                        candidate.record.generation
                    ),
                ));
            }
            Some(existing) if existing.chunk_id == candidate.chunk_id => {
                if existing.record.generation <= candidate.record.generation {
                    slot_state.current = Some(candidate);
                }
            }
            _ => slot_state.current = Some(candidate),
        }
    }
    Ok(publications.into_iter().map(SlotPublication::new).collect())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct LocationRecordDocument {
    pub drive_id: u16,
    pub location_kind: String,
    pub physical_offset: u64,
    pub logical_length: u32,
    pub stored_length: u32,
    pub generation: u32,
    pub checksum: u32,
    pub granule_index: u64,
    pub slot_index: u64,
}

impl LocationRecordDocument {
    fn from_record(record: LocationRecord, slot_index: u64) -> Self {
        Self {
            drive_id: record.drive_id,
            location_kind: match record.location_kind {
                LocationKind::Extent => "extent".to_string(),
                LocationKind::PackedContainer => "packed-container".to_string(),
            },
            physical_offset: record.physical_offset,
            logical_length: record.logical_length,
            stored_length: record.stored_length,
            generation: record.generation,
            checksum: record.checksum,
            granule_index: slot_index,
            slot_index,
        }
    }
}

impl From<LocationRecordDocument> for PackedWriteLocation {
    fn from(value: LocationRecordDocument) -> Self {
        Self {
            drive_id: value.drive_id,
            location_kind: LocationKindCode::from_name(&value.location_kind)
                .expect("LocationRecordDocument always carries a valid KP2 location kind name"),
            physical_offset: value.physical_offset,
            logical_length: value.logical_length,
            stored_length: value.stored_length,
            generation: value.generation,
            checksum: value.checksum,
            slot_index: value.granule_index,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DeleteChunkDocument {
    pub deleted: bool,
    pub tombstone_generation: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ErrorDocument {
    error: String,
}

pub(crate) struct ServiceResponse {
    status: StatusCode,
    body: Bytes,
    content_type: Option<&'static str>,
    location: Option<LocationRecordDocument>,
    extra_headers: Vec<(HeaderName, HeaderValue)>,
    accounted_payload_bytes: u64,
}

impl ServiceResponse {
    fn empty(status: StatusCode) -> Self {
        Self {
            status,
            body: Bytes::new(),
            content_type: None,
            location: None,
            extra_headers: Vec::new(),
            accounted_payload_bytes: 0,
        }
    }

    fn json(status: StatusCode, body: Vec<u8>) -> Self {
        Self {
            status,
            body: Bytes::from(body),
            content_type: Some("application/json"),
            location: None,
            extra_headers: Vec::new(),
            accounted_payload_bytes: 0,
        }
    }

    fn with_location_headers(status: StatusCode, location: LocationRecordDocument) -> Self {
        Self::with_location_headers_and_accounted_bytes(status, location, 0)
    }

    fn with_location_headers_and_accounted_bytes(
        status: StatusCode,
        location: LocationRecordDocument,
        accounted_payload_bytes: u64,
    ) -> Self {
        Self {
            status,
            body: Bytes::new(),
            content_type: None,
            location: Some(location),
            extra_headers: Vec::new(),
            accounted_payload_bytes,
        }
    }

    fn octets(status: StatusCode, location: LocationRecordDocument, body: Bytes) -> Self {
        let accounted_payload_bytes = body.len() as u64;
        Self {
            status,
            body,
            content_type: Some("application/octet-stream"),
            location: Some(location),
            extra_headers: Vec::new(),
            accounted_payload_bytes,
        }
    }

    fn bytes_with_headers(
        status: StatusCode,
        content_type: &'static str,
        body: Bytes,
        accounted_payload_bytes: u64,
        extra_headers: Vec<(HeaderName, HeaderValue)>,
    ) -> Self {
        Self {
            status,
            body,
            content_type: Some(content_type),
            location: None,
            extra_headers,
            accounted_payload_bytes,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ServiceError {
    status: StatusCode,
    public_message: String,
    count_as_error: bool,
}

impl ServiceError {
    fn new(status: StatusCode, public_message: String, count_as_error: bool) -> Self {
        Self {
            status,
            public_message,
            count_as_error,
        }
    }
}

fn apply_location_headers(
    headers: &mut http::HeaderMap,
    location: &LocationRecordDocument,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    headers.insert(
        HEADER_DRIVE_ID,
        HeaderValue::from_str(&location.drive_id.to_string())?,
    );
    headers.insert(
        HEADER_LOCATION_KIND,
        HeaderValue::from_str(&location.location_kind)?,
    );
    headers.insert(
        HEADER_PHYSICAL_OFFSET,
        HeaderValue::from_str(&location.physical_offset.to_string())?,
    );
    headers.insert(
        HEADER_LOGICAL_LENGTH,
        HeaderValue::from_str(&location.logical_length.to_string())?,
    );
    headers.insert(
        HEADER_STORED_LENGTH,
        HeaderValue::from_str(&location.stored_length.to_string())?,
    );
    headers.insert(
        HEADER_GENERATION,
        HeaderValue::from_str(&location.generation.to_string())?,
    );
    headers.insert(
        HEADER_CHECKSUM,
        HeaderValue::from_str(&location.checksum.to_string())?,
    );
    headers.insert(
        HEADER_GRANULE_INDEX,
        HeaderValue::from_str(&location.granule_index.to_string())?,
    );
    headers.insert(
        HEADER_SLOT_INDEX,
        HeaderValue::from_str(&location.slot_index.to_string())?,
    );
    Ok(())
}

fn parse_chunk_id_from_path(path: &str) -> Result<ChunkId, ServiceError> {
    let encoded = path.strip_prefix("/v1/chunk/").ok_or_else(|| {
        ServiceError::new(
            StatusCode::NOT_FOUND,
            format!("KST path {} is not a chunk endpoint", path),
            true,
        )
    })?;
    let raw = hex::decode(encoded).map_err(|err| {
        ServiceError::new(
            StatusCode::BAD_REQUEST,
            format!(
                "KST expects chunk ids as 64 hex characters in the URL path; got {}: {}",
                encoded, err
            ),
            true,
        )
    })?;
    if raw.len() != 32 {
        return Err(ServiceError::new(
            StatusCode::BAD_REQUEST,
            format!(
                "KST expects chunk ids to decode to 32 bytes; {} decoded to {} bytes",
                encoded,
                raw.len()
            ),
            true,
        ));
    }
    let mut chunk_id = [0_u8; 32];
    chunk_id.copy_from_slice(&raw);
    Ok(ChunkId(chunk_id))
}

fn parse_query_granule_index(query: Option<&str>) -> Result<u64, ServiceError> {
    parse_query_value(query, "granule")
        .or_else(|_| parse_query_value(query, "slot"))
        .and_then(|value| {
            value.parse::<u64>().map_err(|err| {
                ServiceError::new(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "KST query parameter `granule` must be an unsigned integer: {}",
                        err
                    ),
                    true,
                )
            })
        })
}

fn parse_query_u32(query: Option<&str>, key: &str) -> Result<u32, ServiceError> {
    parse_query_value(query, key)?
        .parse::<u32>()
        .map_err(|err| {
            ServiceError::new(
                StatusCode::BAD_REQUEST,
                format!("KST query parameter `{key}` must be a 32-bit unsigned integer: {err}"),
                true,
            )
        })
}

fn parse_query_value<'a>(query: Option<&'a str>, key: &str) -> Result<&'a str, ServiceError> {
    let query = query.ok_or_else(|| {
        ServiceError::new(
            StatusCode::BAD_REQUEST,
            format!("KST requires the `{key}` query parameter"),
            true,
        )
    })?;
    for pair in query.split('&') {
        let (name, value) = match pair.split_once('=') {
            Some(parts) => parts,
            None => continue,
        };
        if name == key {
            return Ok(value);
        }
    }
    Err(ServiceError::new(
        StatusCode::BAD_REQUEST,
        format!("KST requires the `{key}` query parameter"),
        true,
    ))
}

fn map_media_error(err: io::Error, context: String) -> ServiceError {
    let status = match err.kind() {
        io::ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
        io::ErrorKind::NotFound => StatusCode::NOT_FOUND,
        io::ErrorKind::InvalidData => StatusCode::PRECONDITION_FAILED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    ServiceError::new(
        status,
        format!("{context}: {err}"),
        status != StatusCode::NOT_FOUND,
    )
}

fn publication_retryable(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::InvalidData | io::ErrorKind::NotFound
    )
}

fn map_ingress_submit_error(err: IngressSubmitError) -> ServiceError {
    match err {
        IngressSubmitError::QueueFull { kind, queue_depth } => ServiceError::new(
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "KST rejected the request because the {} ingress queue hit its configured depth of {}. Retry after the target drains.",
                kind.as_str(),
                queue_depth
            ),
            true,
        ),
        IngressSubmitError::WorkerGone { kind } => ServiceError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "KST {} ingress workers stopped unexpectedly before the request could be handled",
                kind.as_str()
            ),
            true,
        ),
    }
}

fn map_direct_submit_error(err: DirectExecutionSubmitError) -> ServiceError {
    match err {
        DirectExecutionSubmitError::QueueFull { kind, queue_depth } => ServiceError::new(
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "KST rejected the request because the direct {} execution queue hit its configured depth of {}. Retry after the target drains.",
                kind.as_str(),
                queue_depth
            ),
            true,
        ),
        DirectExecutionSubmitError::WorkerGone { kind } => ServiceError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "KST direct {} execution workers stopped unexpectedly before the request could be handled",
                kind.as_str()
            ),
            true,
        ),
    }
}

fn kp2_request_error(err: io::Error) -> ServiceError {
    let status = match err.kind() {
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    ServiceError::new(
        status,
        format!("KST rejected the KP2 transaction: {}", err),
        true,
    )
}

fn kp2_internal_error(err: io::Error) -> ServiceError {
    ServiceError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("KST could not materialize the KP2 response: {}", err),
        true,
    )
}

fn kp2_headers(
    kind: &str,
    chunk_count: usize,
    total_payload_bytes: usize,
) -> io::Result<Vec<(HeaderName, HeaderValue)>> {
    let mut headers = HeaderMap::new();
    apply_packed_headers(&mut headers, kind, chunk_count, total_payload_bytes)?;
    Ok(headers
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect())
}

fn kp2_rate_limit_headers(
    scope: &str,
    class: &str,
    current_in_flight: usize,
    max_in_flight: usize,
    retry_after_ms: u64,
) -> io::Result<Vec<(HeaderName, HeaderValue)>> {
    let mut headers = HeaderMap::new();
    apply_rate_limit_headers(
        &mut headers,
        scope,
        class,
        current_in_flight,
        max_in_flight,
        retry_after_ms,
    )?;
    Ok(headers
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect())
}

fn encode_json<T: Serialize>(value: &T) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(value)
}

fn classify_rpc(method: &Method, path: &str) -> RpcKind {
    match (method, path) {
        (&Method::GET, "/v1/info") => RpcKind::TargetInfo,
        (&Method::GET, "/v1/stats") => RpcKind::Stats,
        (&Method::PUT, "/v1/kp2/chunk-pack") => RpcKind::Write,
        (&Method::POST, "/v1/kp2/chunk-pack/read") => RpcKind::Read,
        (&Method::HEAD, path) if path.starts_with("/v1/chunk/") => RpcKind::Head,
        (&Method::GET, path) if path.starts_with("/v1/chunk/") => RpcKind::Read,
        (&Method::PUT, path) if path.starts_with("/v1/chunk/") => RpcKind::Write,
        (&Method::DELETE, path) if path.starts_with("/v1/chunk/") => RpcKind::Delete,
        _ => RpcKind::Other,
    }
}

fn stream_class_name(rpc: RpcKind) -> &'static str {
    match rpc {
        RpcKind::Write | RpcKind::Delete => "write",
        RpcKind::TargetInfo | RpcKind::Head | RpcKind::Read | RpcKind::Stats | RpcKind::Other => {
            "read"
        }
    }
}

fn is_streamed_chunk_write(method: &Method, path: &str) -> bool {
    *method == Method::PUT && path.starts_with("/v1/chunk/")
}

fn is_direct_chunk_read_fast_path(method: &Method, path: &str) -> bool {
    path.starts_with("/v1/chunk/") && (*method == Method::GET || *method == Method::HEAD)
}

fn parse_content_length(headers: &HeaderMap) -> Result<Option<usize>, ServiceError> {
    let Some(value) = headers.get(CONTENT_LENGTH) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|err| {
        ServiceError::new(
            StatusCode::BAD_REQUEST,
            format!("KST content-length header is not valid ASCII: {}", err),
            true,
        )
    })?;
    let parsed = value.parse::<usize>().map_err(|err| {
        ServiceError::new(
            StatusCode::BAD_REQUEST,
            format!("KST content-length header is not a valid size: {}", err),
            true,
        )
    })?;
    Ok(Some(parsed))
}

pub(crate) async fn run_smoke(
    endpoint: &str,
    chunk_seed: u64,
    slot_index: u64,
    generation: u32,
) -> Result<(), Box<dyn Error>> {
    let uri: Uri = endpoint.parse()?;
    let authority = uri
        .authority()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "KST smoke endpoint must include host:port",
            )
        })?
        .clone();
    let host = authority.host();
    let port = authority.port_u16().unwrap_or(80);
    let socket = TcpStream::connect((host, port)).await?;
    let (mut client, connection) = h2::client::handshake(socket).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let (info_status, _, info_body) = send_h2_request(
        &mut client,
        endpoint,
        Method::GET,
        "/v1/info",
        None,
        Bytes::new(),
    )
    .await?;
    ensure_status("info", info_status, StatusCode::OK, &info_body)?;
    let info: TargetIdentity = serde_json::from_slice(&info_body)?;

    let chunk_id = ChunkId::from_seed(chunk_seed);
    let chunk_hex = hex::encode(chunk_id.0);
    let payload_len = payload_len_for_slot(&info, slot_index)?;
    let payload = smoke_payload(chunk_id, slot_index, generation, payload_len);

    let write_path = format!("/v1/chunk/{chunk_hex}?slot={slot_index}&generation={generation}");
    let (write_status, write_headers, write_body) = send_h2_request(
        &mut client,
        endpoint,
        Method::PUT,
        &write_path,
        Some("application/octet-stream"),
        Bytes::from(payload.clone()),
    )
    .await?;
    ensure_status("write", write_status, StatusCode::CREATED, &write_body)?;
    let write_location = location_from_headers(&write_headers)?;

    let chunk_path = format!("/v1/chunk/{chunk_hex}");
    let (head_status, head_headers, _) = send_h2_request(
        &mut client,
        endpoint,
        Method::HEAD,
        &chunk_path,
        None,
        Bytes::new(),
    )
    .await?;
    if head_status != StatusCode::OK {
        return Err(boxed_error(format!(
            "KST smoke HEAD returned {head_status} instead of 200"
        )));
    }
    let head_location = location_from_headers(&head_headers)?;
    if head_location != write_location {
        return Err(boxed_error(
            "KST smoke HEAD location does not match the write reply",
        ));
    }

    let (read_status, read_headers, read_body) = send_h2_request(
        &mut client,
        endpoint,
        Method::GET,
        &chunk_path,
        None,
        Bytes::new(),
    )
    .await?;
    ensure_status("read", read_status, StatusCode::OK, &read_body)?;
    let read_location = location_from_headers(&read_headers)?;
    if read_location != write_location {
        return Err(boxed_error(
            "KST smoke read location does not match the write reply",
        ));
    }
    if read_body != payload {
        return Err(boxed_error(
            "KST smoke read payload does not match the written payload",
        ));
    }

    let (stats_status, _, stats_body) = send_h2_request(
        &mut client,
        endpoint,
        Method::GET,
        "/v1/stats",
        None,
        Bytes::new(),
    )
    .await?;
    ensure_status("stats", stats_status, StatusCode::OK, &stats_body)?;
    let snapshot: TargetLiveSnapshot = serde_json::from_slice(&stats_body)?;

    let (delete_status, _, delete_body) = send_h2_request(
        &mut client,
        endpoint,
        Method::DELETE,
        &chunk_path,
        None,
        Bytes::new(),
    )
    .await?;
    ensure_status("delete", delete_status, StatusCode::OK, &delete_body)?;
    let delete_reply: DeleteChunkDocument = serde_json::from_slice(&delete_body)?;
    if !delete_reply.deleted {
        return Err(boxed_error(
            "KST smoke delete reported deleted=false after a successful write",
        ));
    }

    let (head_after_status, _, _) = send_h2_request(
        &mut client,
        endpoint,
        Method::HEAD,
        &chunk_path,
        None,
        Bytes::new(),
    )
    .await?;
    if head_after_status != StatusCode::NOT_FOUND {
        return Err(boxed_error(format!(
            "KST smoke expected HEAD after delete to return 404, got {}",
            head_after_status
        )));
    }

    println!(
        concat!(
            "kst_smoke_target_id={}\n",
            "kst_smoke_endpoint={}\n",
            "kst_smoke_chunk={}\n",
            "kst_smoke_slot_index={}\n",
            "kst_smoke_payload_bytes={}\n",
            "kst_smoke_write_generation={}\n",
            "kst_smoke_total_requests={}\n",
            "kst_smoke_total_errors={}\n",
            "kst_smoke_result=ok\n"
        ),
        info.target_id,
        endpoint,
        chunk_hex,
        slot_index,
        payload_len,
        write_location.generation,
        snapshot.stats.total_requests,
        snapshot.stats.total_errors,
    );
    Ok(())
}

async fn send_h2_request(
    client: &mut SendRequest<Bytes>,
    endpoint: &str,
    method: Method,
    path: &str,
    content_type: Option<&str>,
    body: Bytes,
) -> Result<(StatusCode, http::HeaderMap, Vec<u8>), Box<dyn Error>> {
    let uri = format!("{}{}", endpoint.trim_end_matches('/'), path);
    let mut ready = client.clone().ready().await?;
    let mut request = Request::builder().method(method).uri(uri).body(())?;
    if let Some(content_type) = content_type {
        request
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_str(content_type)?);
    }
    request.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string())?,
    );
    let end_stream = body.is_empty();
    let (response_future, mut send_stream) = ready.send_request(request, end_stream)?;
    if !end_stream {
        send_stream.send_data(body, true)?;
    }
    let response = response_future.await?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = connection::collect_body(response.into_body(), usize::MAX).await?;
    Ok((status, headers, body))
}

fn ensure_status(
    phase: &str,
    observed: StatusCode,
    expected: StatusCode,
    body: &[u8],
) -> Result<(), Box<dyn Error>> {
    if observed == expected {
        return Ok(());
    }
    if body.is_empty() {
        return Err(boxed_error(format!(
            "KST smoke {} request expected {}, got {} with an empty response body",
            phase, expected, observed
        )));
    }
    let message = serde_json::from_slice::<ErrorDocument>(body)
        .map(|doc| doc.error)
        .unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned());
    Err(boxed_error(format!(
        "KST smoke {} request expected {}, got {}: {}",
        phase, expected, observed, message
    )))
}

fn location_from_headers(
    headers: &http::HeaderMap,
) -> Result<LocationRecordDocument, Box<dyn Error>> {
    let granule_index = headers
        .get(&HEADER_GRANULE_INDEX)
        .map(|_| header_u64(headers, &HEADER_GRANULE_INDEX))
        .transpose()?
        .unwrap_or(header_u64(headers, &HEADER_SLOT_INDEX)?);
    Ok(LocationRecordDocument {
        drive_id: header_u16(headers, &HEADER_DRIVE_ID)?,
        location_kind: header_string(headers, &HEADER_LOCATION_KIND)?,
        physical_offset: header_u64(headers, &HEADER_PHYSICAL_OFFSET)?,
        logical_length: header_u32(headers, &HEADER_LOGICAL_LENGTH)?,
        stored_length: header_u32(headers, &HEADER_STORED_LENGTH)?,
        generation: header_u32(headers, &HEADER_GENERATION)?,
        checksum: header_u32(headers, &HEADER_CHECKSUM)?,
        granule_index,
        slot_index: granule_index,
    })
}

fn header_string(headers: &http::HeaderMap, name: &HeaderName) -> Result<String, Box<dyn Error>> {
    Ok(headers
        .get(name)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing KST response header {}", name),
            )
        })?
        .to_str()?
        .to_string())
}

fn header_u16(headers: &http::HeaderMap, name: &HeaderName) -> Result<u16, Box<dyn Error>> {
    Ok(header_string(headers, name)?.parse()?)
}

fn header_u32(headers: &http::HeaderMap, name: &HeaderName) -> Result<u32, Box<dyn Error>> {
    Ok(header_string(headers, name)?.parse()?)
}

fn header_u64(headers: &http::HeaderMap, name: &HeaderName) -> Result<u64, Box<dyn Error>> {
    Ok(header_string(headers, name)?.parse()?)
}

fn payload_len_for_slot(info: &TargetIdentity, slot_index: u64) -> Result<usize, Box<dyn Error>> {
    match info.layout_kind.as_str() {
        "extent-only" => Ok(info.extent_bytes as usize),
        "packed-only" => Ok(info.packed_bytes as usize),
        "mixed" => {
            if slot_index & 1 == 0 {
                Ok(info.extent_bytes as usize)
            } else {
                Ok(info.packed_bytes as usize)
            }
        }
        other => Err(boxed_error(format!(
            "unknown KST layout kind `{other}` in target info"
        ))),
    }
}

fn smoke_payload(
    chunk_id: ChunkId,
    slot_index: u64,
    generation: u32,
    payload_len: usize,
) -> Vec<u8> {
    let slot = slot_index.to_le_bytes();
    let generation = generation.to_le_bytes();
    let mut payload = vec![0_u8; payload_len];
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte = chunk_id.0[index % chunk_id.0.len()]
            ^ slot[index % slot.len()]
            ^ generation[index % generation.len()]
            ^ (index as u8).wrapping_mul(29);
    }
    payload
}

fn json_error(err: serde_json::Error) -> ServiceError {
    ServiceError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("KST could not serialize its response payload: {err}"),
        true,
    )
}

fn boxed_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::Other, message.into()))
}
