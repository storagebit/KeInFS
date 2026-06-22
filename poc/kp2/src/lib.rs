// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use http::{HeaderMap, HeaderValue};
use std::io;

pub const CONTENT_TYPE: &str = "application/vnd.keinfs.kp2";
pub const HEADER_PROTOCOL: &str = "x-kp2-protocol";
pub const HEADER_TRANSFER: &str = "x-kp2-transfer";
pub const HEADER_KIND: &str = "x-kp2-kind";
pub const HEADER_CHUNK_COUNT: &str = "x-kp2-chunk-count";
pub const HEADER_TOTAL_PAYLOAD_BYTES: &str = "x-kp2-total-payload-bytes";
pub const HEADER_LIMIT_SCOPE: &str = "x-kp2-limit-scope";
pub const HEADER_LIMIT_CLASS: &str = "x-kp2-limit-class";
pub const HEADER_LIMIT_CURRENT_IN_FLIGHT: &str = "x-kp2-limit-current-in-flight";
pub const HEADER_LIMIT_MAX_IN_FLIGHT: &str = "x-kp2-limit-max-in-flight";
pub const HEADER_RETRY_AFTER_MS: &str = "x-kp2-retry-after-ms";
pub const PROTOCOL_NAME: &str = "kp2";
pub const TRANSFER_PACKED: &str = "packed";
pub const KIND_WRITE: &str = "write";
pub const KIND_QUERY: &str = "query";
pub const KIND_READ: &str = "read";
pub const VERSION: u16 = 1;
pub const MAX_PACK_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub const LIMIT_SCOPE_TARGET: &str = "target";
pub const LIMIT_SCOPE_CONNECTION: &str = "connection";
pub const LIMIT_CLASS_ALL: &str = "all";
pub const LIMIT_CLASS_READ: &str = "read";
pub const LIMIT_CLASS_WRITE: &str = "write";
/// Common-header flag (KP2Q only): query entries carry a per-chunk
/// (offset:u64, length:u32) sub-range, so a reader fetches a slice of a chunk
/// instead of the whole chunk. Entries are `QUERY_ENTRY_RANGED_BYTES` wide.
pub const FLAG_QUERY_RANGED: u16 = 0x0001;

const MAGIC_WRITE: &[u8; 4] = b"KP2W";
const MAGIC_QUERY: &[u8; 4] = b"KP2Q";
const MAGIC_READ: &[u8; 4] = b"KP2R";
const MAGIC_ACK: &[u8; 4] = b"KP2A";
const COMMON_HEADER_BYTES: usize = 24;
const WRITE_ENTRY_BYTES: usize = 58;
const QUERY_ENTRY_BYTES: usize = 32;
/// Ranged query entry: chunk_id[32] + offset:u64 + length:u32 + reserved:u32.
const QUERY_ENTRY_RANGED_BYTES: usize = 48;
const READ_ENTRY_BYTES: usize = 80;
const ACK_ENTRY_BYTES: usize = 80;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ChunkId(pub [u8; 32]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocationKindCode {
    None = 0,
    Extent = 1,
    PackedContainer = 2,
}

impl LocationKindCode {
    pub fn from_u16(value: u16) -> io::Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Extent),
            2 => Ok(Self::PackedContainer),
            other => Err(invalid_data(format!(
                "KP2 location kind {} is unknown",
                other
            ))),
        }
    }

    pub fn from_name(value: &str) -> io::Result<Self> {
        match value {
            "extent" => Ok(Self::Extent),
            "packed-container" => Ok(Self::PackedContainer),
            "none" => Ok(Self::None),
            other => Err(invalid_data(format!(
                "KP2 location kind name `{}` is unknown",
                other
            ))),
        }
    }

    pub fn as_name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Extent => "extent",
            Self::PackedContainer => "packed-container",
        }
    }
}

/// Self-describing object identity for a fragment write (TLA/SC+ Phase 2). Carried on
/// the KP2 write request so the storage target stamps it into the on-media slot header.
/// object_id/object_version are server-minted (KP2M.BeginObject); until that lands they
/// are 0. stripe/frag are the fragment's position in the object's EC layout.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteIdentity {
    pub object_id: u32,
    pub object_version: u16,
    pub stripe: u16,
    pub frag: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedWriteEntry {
    pub chunk_id: ChunkId,
    pub slot_index: u64,
    pub generation: u32,
    pub identity: WriteIdentity,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedWriteRequest {
    pub entries: Vec<PackedWriteEntry>,
}

/// A byte sub-range within a chunk's logical payload (byte-granular reads).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkRange {
    pub offset: u64,
    pub length: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedReadQuery {
    pub chunk_ids: Vec<ChunkId>,
    /// `None` fetches each chunk whole (legacy, 32-byte entries). `Some(ranges)`
    /// (len must equal `chunk_ids`) fetches only `ranges[i]` of `chunk_ids[i]`,
    /// setting `FLAG_QUERY_RANGED` on the wire (48-byte entries).
    pub ranges: Option<Vec<ChunkRange>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedReadLocation {
    pub drive_id: u16,
    pub location_kind: LocationKindCode,
    pub physical_offset: u64,
    pub logical_length: u32,
    pub stored_length: u32,
    pub generation: u32,
    pub checksum: u32,
    pub slot_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedReadEntry {
    pub chunk_id: ChunkId,
    pub status_code: u16,
    pub location: Option<PackedReadLocation>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedReadResponse {
    pub entries: Vec<PackedReadEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedWriteLocation {
    pub drive_id: u16,
    pub location_kind: LocationKindCode,
    pub physical_offset: u64,
    pub logical_length: u32,
    pub stored_length: u32,
    pub generation: u32,
    pub checksum: u32,
    pub slot_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedWriteReplyEntry {
    pub chunk_id: ChunkId,
    pub slot_index: u64,
    pub requested_generation: u32,
    pub status_code: u16,
    pub location: Option<PackedWriteLocation>,
    pub error: Option<String>,
}

impl PackedWriteReplyEntry {
    pub fn success(&self) -> bool {
        (200..300).contains(&self.status_code)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedWriteReply {
    pub entries: Vec<PackedWriteReplyEntry>,
}

pub fn validate_packed_headers(headers: &HeaderMap, expected_kind: &str) -> io::Result<()> {
    let protocol = header_string(headers, HEADER_PROTOCOL)?;
    if protocol != PROTOCOL_NAME {
        return Err(invalid_input(format!(
            "KP2 requires {}={} and got {}",
            HEADER_PROTOCOL, PROTOCOL_NAME, protocol
        )));
    }
    let transfer = header_string(headers, HEADER_TRANSFER)?;
    if transfer != TRANSFER_PACKED {
        return Err(invalid_input(format!(
            "KP2 requires {}={} and got {}",
            HEADER_TRANSFER, TRANSFER_PACKED, transfer
        )));
    }
    let kind = header_string(headers, HEADER_KIND)?;
    if kind != expected_kind {
        return Err(invalid_input(format!(
            "KP2 requires {}={} and got {}",
            HEADER_KIND, expected_kind, kind
        )));
    }
    Ok(())
}

pub fn validate_request_headers(headers: &HeaderMap, expected_kind: &str) -> io::Result<()> {
    validate_packed_headers(headers, expected_kind)
}

pub fn validate_declared_counts(
    headers: &HeaderMap,
    chunk_count: usize,
    total_payload_bytes: usize,
) -> io::Result<()> {
    let declared_chunk_count = header_u64(headers, HEADER_CHUNK_COUNT)? as usize;
    if declared_chunk_count != chunk_count {
        return Err(invalid_input(format!(
            "KP2 declared {}={} but the body contains {} chunk entries",
            HEADER_CHUNK_COUNT, declared_chunk_count, chunk_count
        )));
    }
    let declared_payload_bytes = header_u64(headers, HEADER_TOTAL_PAYLOAD_BYTES)? as usize;
    if declared_payload_bytes != total_payload_bytes {
        return Err(invalid_input(format!(
            "KP2 declared {}={} but the body contains {} logical payload bytes",
            HEADER_TOTAL_PAYLOAD_BYTES, declared_payload_bytes, total_payload_bytes
        )));
    }
    Ok(())
}

pub fn apply_packed_headers(
    headers: &mut HeaderMap,
    kind: &str,
    chunk_count: usize,
    total_payload_bytes: usize,
) -> io::Result<()> {
    headers.insert(HEADER_PROTOCOL, HeaderValue::from_static(PROTOCOL_NAME));
    headers.insert(HEADER_TRANSFER, HeaderValue::from_static(TRANSFER_PACKED));
    headers.insert(
        HEADER_KIND,
        HeaderValue::from_str(kind)
            .map_err(|err| invalid_input(format!("KP2 kind header value is invalid: {}", err)))?,
    );
    headers.insert(
        HEADER_CHUNK_COUNT,
        HeaderValue::from_str(&chunk_count.to_string()).map_err(|err| {
            invalid_input(format!("KP2 chunk-count header value is invalid: {}", err))
        })?,
    );
    headers.insert(
        HEADER_TOTAL_PAYLOAD_BYTES,
        HeaderValue::from_str(&total_payload_bytes.to_string()).map_err(|err| {
            invalid_input(format!(
                "KP2 total-payload-bytes header value is invalid: {}",
                err
            ))
        })?,
    );
    Ok(())
}

pub fn apply_rate_limit_headers(
    headers: &mut HeaderMap,
    scope: &str,
    class: &str,
    current_in_flight: usize,
    max_in_flight: usize,
    retry_after_ms: u64,
) -> io::Result<()> {
    headers.insert(
        HEADER_LIMIT_SCOPE,
        HeaderValue::from_str(scope).map_err(|err| {
            invalid_input(format!("KP2 limit-scope header value is invalid: {}", err))
        })?,
    );
    headers.insert(
        HEADER_LIMIT_CLASS,
        HeaderValue::from_str(class).map_err(|err| {
            invalid_input(format!("KP2 limit-class header value is invalid: {}", err))
        })?,
    );
    headers.insert(
        HEADER_LIMIT_CURRENT_IN_FLIGHT,
        HeaderValue::from_str(&current_in_flight.to_string()).map_err(|err| {
            invalid_input(format!(
                "KP2 current-in-flight header value is invalid: {}",
                err
            ))
        })?,
    );
    headers.insert(
        HEADER_LIMIT_MAX_IN_FLIGHT,
        HeaderValue::from_str(&max_in_flight.to_string()).map_err(|err| {
            invalid_input(format!(
                "KP2 max-in-flight header value is invalid: {}",
                err
            ))
        })?,
    );
    headers.insert(
        HEADER_RETRY_AFTER_MS,
        HeaderValue::from_str(&retry_after_ms.to_string()).map_err(|err| {
            invalid_input(format!(
                "KP2 retry-after-ms header value is invalid: {}",
                err
            ))
        })?,
    );
    Ok(())
}

pub fn body_too_large(bytes: usize) -> io::Result<()> {
    if bytes > MAX_PACK_PAYLOAD_BYTES {
        Err(invalid_input(format!(
            "KP2 packed logical payload {} bytes exceeds the {} byte ceiling",
            bytes, MAX_PACK_PAYLOAD_BYTES
        )))
    } else {
        Ok(())
    }
}

pub fn encoded_write_request_len(
    chunk_count: usize,
    total_payload_bytes: usize,
) -> io::Result<usize> {
    body_too_large(total_payload_bytes)?;
    let entry_table_bytes = chunk_count
        .checked_mul(WRITE_ENTRY_BYTES)
        .ok_or_else(|| invalid_input("KP2 write entry table size overflow"))?;
    COMMON_HEADER_BYTES
        .checked_add(entry_table_bytes)
        .and_then(|bytes| bytes.checked_add(total_payload_bytes))
        .ok_or_else(|| invalid_input("KP2 write request size overflow"))
}

pub fn max_encoded_write_request_bytes(min_payload_bytes: usize) -> io::Result<usize> {
    if min_payload_bytes == 0 {
        return Err(invalid_input(
            "KP2 cannot derive a packed write wire ceiling from a zero-byte minimum payload",
        ));
    }
    let max_entry_count = MAX_PACK_PAYLOAD_BYTES.div_ceil(min_payload_bytes);
    encoded_write_request_len(max_entry_count, MAX_PACK_PAYLOAD_BYTES)
}

/// Encodes only the fixed-size prefix of a packed write request: the common
/// header followed by the per-entry descriptor table, but NOT the variable-length
/// payload region. This is byte-for-byte identical to the leading
/// `COMMON_HEADER_BYTES + entries.len() * WRITE_ENTRY_BYTES` bytes that
/// [`encode_write_request`] produces.
///
/// Clients can transmit this prefix followed by each entry's payload as separate
/// chunks (e.g. distinct HTTP/2 DATA frames) to avoid concatenating every payload
/// into one buffer; the receiver reassembles the identical byte stream regardless
/// of how the body was split for transport.
pub fn encode_write_request_header(entries: &[PackedWriteEntry]) -> io::Result<Vec<u8>> {
    if entries.is_empty() {
        return Err(invalid_input(
            "KP2 packed write requires at least one entry",
        ));
    }
    let total_payload_bytes = entries
        .iter()
        .map(|entry| entry.payload.len())
        .sum::<usize>();
    body_too_large(total_payload_bytes)?;
    let entry_table_bytes = entries.len() * WRITE_ENTRY_BYTES;
    let mut out = Vec::with_capacity(COMMON_HEADER_BYTES + entry_table_bytes);
    encode_common_header(
        &mut out,
        MAGIC_WRITE,
        entries.len() as u32,
        total_payload_bytes as u32,
        entry_table_bytes as u32,
    );
    for entry in entries {
        u32::try_from(entry.payload.len())
            .map_err(|_| invalid_input("KP2 write entry payload length does not fit into u32"))?;
        out.extend_from_slice(&entry.chunk_id.0);
        put_u64(&mut out, entry.slot_index);
        put_u32(&mut out, entry.generation);
        put_u32(&mut out, entry.payload.len() as u32);
        put_u32(&mut out, entry.identity.object_id);
        put_u16(&mut out, entry.identity.object_version);
        put_u16(&mut out, entry.identity.stripe);
        put_u16(&mut out, entry.identity.frag);
    }
    Ok(out)
}

pub fn encode_write_request(pack: &PackedWriteRequest) -> io::Result<Vec<u8>> {
    let total_payload_bytes = pack
        .entries
        .iter()
        .map(|entry| entry.payload.len())
        .sum::<usize>();
    let encoded_len = encoded_write_request_len(pack.entries.len(), total_payload_bytes)?;
    let mut out = encode_write_request_header(&pack.entries)?;
    out.reserve(encoded_len.saturating_sub(out.len()));
    for entry in &pack.entries {
        out.extend_from_slice(&entry.payload);
    }
    Ok(out)
}

pub fn decode_write_request(body: &[u8]) -> io::Result<PackedWriteRequest> {
    let header = decode_common_header(body, MAGIC_WRITE)?;
    let entry_table_bytes = header.entry_table_bytes as usize;
    if entry_table_bytes != header.chunk_count as usize * WRITE_ENTRY_BYTES {
        return Err(invalid_data(format!(
            "KP2 write entry table is {} bytes but {} entries require {} bytes",
            entry_table_bytes,
            header.chunk_count,
            header.chunk_count as usize * WRITE_ENTRY_BYTES
        )));
    }
    let payload_region_offset = COMMON_HEADER_BYTES + entry_table_bytes;
    if body.len() < payload_region_offset {
        return Err(invalid_data(
            "KP2 write body is shorter than its entry table",
        ));
    }
    let mut payload_cursor = payload_region_offset;
    let mut entries = Vec::with_capacity(header.chunk_count as usize);
    for index in 0..header.chunk_count as usize {
        let base = COMMON_HEADER_BYTES + index * WRITE_ENTRY_BYTES;
        let chunk_id = read_chunk_id(body, base)?;
        let slot_index = read_u64(body, base + 32)?;
        let generation = read_u32(body, base + 40)?;
        let payload_bytes = read_u32(body, base + 44)? as usize;
        let identity = WriteIdentity {
            object_id: read_u32(body, base + 48)?,
            object_version: read_u16(body, base + 52)?,
            stripe: read_u16(body, base + 54)?,
            frag: read_u16(body, base + 56)?,
        };
        let payload_end = payload_cursor
            .checked_add(payload_bytes)
            .ok_or_else(|| invalid_data("KP2 write payload cursor overflow"))?;
        if payload_end > body.len() {
            return Err(invalid_data("KP2 write payload overruns the request body"));
        }
        entries.push(PackedWriteEntry {
            chunk_id,
            slot_index,
            generation,
            identity,
            payload: body[payload_cursor..payload_end].to_vec(),
        });
        payload_cursor = payload_end;
    }
    let actual_payload_bytes = payload_cursor - payload_region_offset;
    if actual_payload_bytes != header.total_payload_bytes as usize {
        return Err(invalid_data(format!(
            "KP2 write body declared {} logical payload bytes but carried {}",
            header.total_payload_bytes, actual_payload_bytes
        )));
    }
    if payload_cursor != body.len() {
        return Err(invalid_data(
            "KP2 write body contains trailing bytes after the declared payload runs",
        ));
    }
    Ok(PackedWriteRequest { entries })
}

pub fn encode_read_query(query: &PackedReadQuery) -> io::Result<Vec<u8>> {
    if query.chunk_ids.is_empty() {
        return Err(invalid_input(
            "KP2 packed read query requires at least one chunk id",
        ));
    }
    let ranged = match &query.ranges {
        Some(ranges) => {
            if ranges.len() != query.chunk_ids.len() {
                return Err(invalid_input(
                    "KP2 ranged read query: ranges length must equal chunk_ids length",
                ));
            }
            true
        }
        None => false,
    };
    let entry_bytes = if ranged {
        QUERY_ENTRY_RANGED_BYTES
    } else {
        QUERY_ENTRY_BYTES
    };
    let entry_table_bytes = query
        .chunk_ids
        .len()
        .checked_mul(entry_bytes)
        .ok_or_else(|| invalid_input("KP2 query entry table size overflow"))?;
    let flags = if ranged { FLAG_QUERY_RANGED } else { 0 };
    let mut out = Vec::with_capacity(COMMON_HEADER_BYTES + entry_table_bytes);
    encode_common_header_with_flags(
        &mut out,
        MAGIC_QUERY,
        flags,
        query.chunk_ids.len() as u32,
        0,
        entry_table_bytes as u32,
    );
    for (index, chunk_id) in query.chunk_ids.iter().enumerate() {
        out.extend_from_slice(&chunk_id.0);
        if ranged {
            // SAFETY: `ranged` implies `query.ranges` is `Some` with matching len.
            let range = query.ranges.as_ref().unwrap()[index];
            put_u64(&mut out, range.offset);
            put_u32(&mut out, range.length);
            put_u32(&mut out, 0); // reserved
        }
    }
    Ok(out)
}

pub fn decode_read_query(body: &[u8]) -> io::Result<PackedReadQuery> {
    let header = decode_common_header_with_flags(body, MAGIC_QUERY, FLAG_QUERY_RANGED)?;
    let ranged = header.flags & FLAG_QUERY_RANGED != 0;
    let entry_bytes = if ranged {
        QUERY_ENTRY_RANGED_BYTES
    } else {
        QUERY_ENTRY_BYTES
    };
    let entry_table_bytes = header.entry_table_bytes as usize;
    if entry_table_bytes != header.chunk_count as usize * entry_bytes {
        return Err(invalid_data(format!(
            "KP2 read-query entry table is {} bytes but {} {} entries require {} bytes",
            entry_table_bytes,
            header.chunk_count,
            if ranged { "ranged" } else { "plain" },
            header.chunk_count as usize * entry_bytes
        )));
    }
    let expected_len = COMMON_HEADER_BYTES + entry_table_bytes;
    if body.len() != expected_len {
        return Err(invalid_data(format!(
            "KP2 read-query body length {} does not match the expected {} bytes",
            body.len(),
            expected_len
        )));
    }
    let mut chunk_ids = Vec::with_capacity(header.chunk_count as usize);
    let mut ranges = if ranged {
        Some(Vec::with_capacity(header.chunk_count as usize))
    } else {
        None
    };
    for index in 0..header.chunk_count as usize {
        let base = COMMON_HEADER_BYTES + index * entry_bytes;
        chunk_ids.push(read_chunk_id(body, base)?);
        if ranged {
            let offset = read_u64(body, base + 32)?;
            let length = read_u32(body, base + 40)?;
            // bytes [base+44, base+48) are reserved; ignored for forward-compat.
            ranges
                .as_mut()
                .expect("ranged implies ranges is Some")
                .push(ChunkRange { offset, length });
        }
    }
    Ok(PackedReadQuery { chunk_ids, ranges })
}

pub fn encode_read_response(pack: &PackedReadResponse) -> io::Result<Vec<u8>> {
    if pack.entries.is_empty() {
        return Err(invalid_input(
            "KP2 packed read response requires at least one entry",
        ));
    }
    let total_payload_bytes = pack
        .entries
        .iter()
        .map(|entry| entry.payload.len())
        .sum::<usize>();
    body_too_large(total_payload_bytes)?;
    let entry_table_bytes = pack
        .entries
        .len()
        .checked_mul(READ_ENTRY_BYTES)
        .ok_or_else(|| invalid_input("KP2 read response entry table size overflow"))?;
    let mut out = Vec::with_capacity(COMMON_HEADER_BYTES + entry_table_bytes + total_payload_bytes);
    encode_common_header(
        &mut out,
        MAGIC_READ,
        pack.entries.len() as u32,
        total_payload_bytes as u32,
        entry_table_bytes as u32,
    );
    for entry in &pack.entries {
        out.extend_from_slice(&entry.chunk_id.0);
        put_u16(&mut out, entry.status_code);
        let (
            location_kind,
            drive_id,
            physical_offset,
            logical_length,
            stored_length,
            generation,
            checksum,
            slot_index,
        ) = if let Some(location) = &entry.location {
            (
                location.location_kind as u16,
                location.drive_id,
                location.physical_offset,
                location.logical_length,
                location.stored_length,
                location.generation,
                location.checksum,
                location.slot_index,
            )
        } else {
            (LocationKindCode::None as u16, 0, 0, 0, 0, 0, 0, 0)
        };
        put_u16(&mut out, location_kind);
        put_u16(&mut out, drive_id);
        put_u16(&mut out, 0);
        put_u64(&mut out, physical_offset);
        put_u32(&mut out, logical_length);
        put_u32(&mut out, stored_length);
        put_u32(&mut out, generation);
        put_u32(&mut out, checksum);
        put_u64(&mut out, slot_index);
        put_u32(&mut out, entry.payload.len() as u32);
        put_u32(&mut out, 0);
    }
    for entry in &pack.entries {
        out.extend_from_slice(&entry.payload);
    }
    Ok(out)
}

pub fn decode_read_response(body: &[u8]) -> io::Result<PackedReadResponse> {
    let header = decode_common_header(body, MAGIC_READ)?;
    let entry_table_bytes = header.entry_table_bytes as usize;
    if entry_table_bytes != header.chunk_count as usize * READ_ENTRY_BYTES {
        return Err(invalid_data(format!(
            "KP2 read-response entry table is {} bytes but {} entries require {} bytes",
            entry_table_bytes,
            header.chunk_count,
            header.chunk_count as usize * READ_ENTRY_BYTES
        )));
    }
    let payload_region_offset = COMMON_HEADER_BYTES + entry_table_bytes;
    if body.len() < payload_region_offset {
        return Err(invalid_data(
            "KP2 read-response body is shorter than its entry table",
        ));
    }
    let mut payload_cursor = payload_region_offset;
    let mut entries = Vec::with_capacity(header.chunk_count as usize);
    for index in 0..header.chunk_count as usize {
        let base = COMMON_HEADER_BYTES + index * READ_ENTRY_BYTES;
        let chunk_id = read_chunk_id(body, base)?;
        let status_code = read_u16(body, base + 32)?;
        let location_kind = LocationKindCode::from_u16(read_u16(body, base + 34)?)?;
        let drive_id = read_u16(body, base + 36)?;
        let reserved = read_u16(body, base + 38)?;
        if reserved != 0 {
            return Err(invalid_data(
                "KP2 read-response descriptor reserved field must be zero",
            ));
        }
        let physical_offset = read_u64(body, base + 40)?;
        let logical_length = read_u32(body, base + 48)?;
        let stored_length = read_u32(body, base + 52)?;
        let generation = read_u32(body, base + 56)?;
        let checksum = read_u32(body, base + 60)?;
        let slot_index = read_u64(body, base + 64)?;
        let payload_bytes = read_u32(body, base + 72)? as usize;
        let reserved_2 = read_u32(body, base + 76)?;
        if reserved_2 != 0 {
            return Err(invalid_data(
                "KP2 read-response descriptor reserved_2 field must be zero",
            ));
        }
        let payload_end = payload_cursor
            .checked_add(payload_bytes)
            .ok_or_else(|| invalid_data("KP2 read-response payload cursor overflow"))?;
        if payload_end > body.len() {
            return Err(invalid_data(
                "KP2 read-response payload overruns the response body",
            ));
        }
        let location = if location_kind == LocationKindCode::None {
            None
        } else {
            Some(PackedReadLocation {
                drive_id,
                location_kind,
                physical_offset,
                logical_length,
                stored_length,
                generation,
                checksum,
                slot_index,
            })
        };
        entries.push(PackedReadEntry {
            chunk_id,
            status_code,
            location,
            payload: body[payload_cursor..payload_end].to_vec(),
        });
        payload_cursor = payload_end;
    }
    let actual_payload_bytes = payload_cursor - payload_region_offset;
    if actual_payload_bytes != header.total_payload_bytes as usize {
        return Err(invalid_data(format!(
            "KP2 read-response body declared {} logical payload bytes but carried {}",
            header.total_payload_bytes, actual_payload_bytes
        )));
    }
    if payload_cursor != body.len() {
        return Err(invalid_data(
            "KP2 read-response body contains trailing bytes after the declared payload runs",
        ));
    }
    Ok(PackedReadResponse { entries })
}

pub fn encode_write_reply(reply: &PackedWriteReply) -> io::Result<Vec<u8>> {
    if reply.entries.is_empty() {
        return Err(invalid_input(
            "KP2 packed write reply requires at least one entry",
        ));
    }
    let total_error_bytes = reply
        .entries
        .iter()
        .map(|entry| entry.error.as_ref().map_or(0, |error| error.len()))
        .sum::<usize>();
    body_too_large(total_error_bytes)?;
    let entry_table_bytes = reply
        .entries
        .len()
        .checked_mul(ACK_ENTRY_BYTES)
        .ok_or_else(|| invalid_input("KP2 write reply entry table size overflow"))?;
    let mut out = Vec::with_capacity(COMMON_HEADER_BYTES + entry_table_bytes + total_error_bytes);
    encode_common_header(
        &mut out,
        MAGIC_ACK,
        reply.entries.len() as u32,
        total_error_bytes as u32,
        entry_table_bytes as u32,
    );
    for entry in &reply.entries {
        out.extend_from_slice(&entry.chunk_id.0);
        put_u16(&mut out, entry.status_code);
        let (
            location_kind,
            drive_id,
            physical_offset,
            logical_length,
            stored_length,
            generation,
            checksum,
            slot_index,
        ) = if let Some(location) = &entry.location {
            (
                location.location_kind as u16,
                location.drive_id,
                location.physical_offset,
                location.logical_length,
                location.stored_length,
                location.generation,
                location.checksum,
                location.slot_index,
            )
        } else {
            (
                LocationKindCode::None as u16,
                0,
                0,
                0,
                0,
                0,
                0,
                entry.slot_index,
            )
        };
        put_u16(&mut out, location_kind);
        put_u16(&mut out, drive_id);
        put_u16(&mut out, 0);
        put_u64(&mut out, physical_offset);
        put_u32(&mut out, logical_length);
        put_u32(&mut out, stored_length);
        put_u32(&mut out, generation);
        put_u32(&mut out, checksum);
        put_u64(&mut out, slot_index);
        put_u32(&mut out, entry.requested_generation);
        put_u32(
            &mut out,
            entry
                .error
                .as_ref()
                .map_or(0_u32, |error| error.len() as u32),
        );
    }
    for entry in &reply.entries {
        if let Some(error) = &entry.error {
            out.extend_from_slice(error.as_bytes());
        }
    }
    Ok(out)
}

pub fn decode_write_reply(body: &[u8]) -> io::Result<PackedWriteReply> {
    let header = decode_common_header(body, MAGIC_ACK)?;
    let entry_table_bytes = header.entry_table_bytes as usize;
    if entry_table_bytes != header.chunk_count as usize * ACK_ENTRY_BYTES {
        return Err(invalid_data(format!(
            "KP2 write-reply entry table is {} bytes but {} entries require {} bytes",
            entry_table_bytes,
            header.chunk_count,
            header.chunk_count as usize * ACK_ENTRY_BYTES
        )));
    }
    let payload_region_offset = COMMON_HEADER_BYTES + entry_table_bytes;
    if body.len() < payload_region_offset {
        return Err(invalid_data(
            "KP2 write-reply body is shorter than its entry table",
        ));
    }
    let mut error_cursor = payload_region_offset;
    let mut entries = Vec::with_capacity(header.chunk_count as usize);
    for index in 0..header.chunk_count as usize {
        let base = COMMON_HEADER_BYTES + index * ACK_ENTRY_BYTES;
        let chunk_id = read_chunk_id(body, base)?;
        let status_code = read_u16(body, base + 32)?;
        let location_kind = LocationKindCode::from_u16(read_u16(body, base + 34)?)?;
        let drive_id = read_u16(body, base + 36)?;
        let flags = read_u16(body, base + 38)?;
        if flags != 0 {
            return Err(invalid_data(
                "KP2 write-reply descriptor flags field must be zero",
            ));
        }
        let physical_offset = read_u64(body, base + 40)?;
        let logical_length = read_u32(body, base + 48)?;
        let stored_length = read_u32(body, base + 52)?;
        let generation = read_u32(body, base + 56)?;
        let checksum = read_u32(body, base + 60)?;
        let slot_index = read_u64(body, base + 64)?;
        let requested_generation = read_u32(body, base + 72)?;
        let error_bytes = read_u32(body, base + 76)? as usize;
        let error_end = error_cursor
            .checked_add(error_bytes)
            .ok_or_else(|| invalid_data("KP2 write-reply error cursor overflow"))?;
        if error_end > body.len() {
            return Err(invalid_data(
                "KP2 write-reply error text overruns the response body",
            ));
        }
        let error = if error_bytes == 0 {
            None
        } else {
            Some(
                std::str::from_utf8(&body[error_cursor..error_end])
                    .map_err(|err| {
                        invalid_data(format!(
                            "KP2 write-reply error text is not valid UTF-8: {}",
                            err
                        ))
                    })?
                    .to_string(),
            )
        };
        let location = if location_kind == LocationKindCode::None {
            None
        } else {
            Some(PackedWriteLocation {
                drive_id,
                location_kind,
                physical_offset,
                logical_length,
                stored_length,
                generation,
                checksum,
                slot_index,
            })
        };
        entries.push(PackedWriteReplyEntry {
            chunk_id,
            slot_index,
            requested_generation,
            status_code,
            location,
            error,
        });
        error_cursor = error_end;
    }
    let actual_error_bytes = error_cursor - payload_region_offset;
    if actual_error_bytes != header.total_payload_bytes as usize {
        return Err(invalid_data(format!(
            "KP2 write-reply body declared {} error bytes but carried {}",
            header.total_payload_bytes, actual_error_bytes
        )));
    }
    if error_cursor != body.len() {
        return Err(invalid_data(
            "KP2 write-reply body contains trailing bytes after the declared error text runs",
        ));
    }
    Ok(PackedWriteReply { entries })
}

fn encode_common_header(
    out: &mut Vec<u8>,
    magic: &[u8; 4],
    chunk_count: u32,
    total_payload_bytes: u32,
    entry_table_bytes: u32,
) {
    encode_common_header_with_flags(
        out,
        magic,
        0,
        chunk_count,
        total_payload_bytes,
        entry_table_bytes,
    );
}

fn encode_common_header_with_flags(
    out: &mut Vec<u8>,
    magic: &[u8; 4],
    flags: u16,
    chunk_count: u32,
    total_payload_bytes: u32,
    entry_table_bytes: u32,
) {
    out.extend_from_slice(magic);
    put_u16(out, VERSION);
    put_u16(out, flags);
    put_u32(out, chunk_count);
    put_u32(out, total_payload_bytes);
    put_u32(out, entry_table_bytes);
    put_u32(out, 0);
}

struct CommonHeader {
    flags: u16,
    chunk_count: u32,
    total_payload_bytes: u32,
    entry_table_bytes: u32,
}

fn decode_common_header(body: &[u8], expected_magic: &[u8; 4]) -> io::Result<CommonHeader> {
    decode_common_header_with_flags(body, expected_magic, 0)
}

/// Like [`decode_common_header`] but tolerates the bits set in `allowed_flags`
/// (any flag outside the mask is still rejected). The accepted flags are
/// returned in `CommonHeader::flags` for the message decoder to act on.
fn decode_common_header_with_flags(
    body: &[u8],
    expected_magic: &[u8; 4],
    allowed_flags: u16,
) -> io::Result<CommonHeader> {
    if body.len() < COMMON_HEADER_BYTES {
        return Err(invalid_data(format!(
            "KP2 body is {} bytes long but the common header requires {} bytes",
            body.len(),
            COMMON_HEADER_BYTES
        )));
    }
    if &body[0..4] != expected_magic {
        return Err(invalid_data(format!(
            "KP2 magic mismatch: expected {:?}, got {:?}",
            std::str::from_utf8(expected_magic).unwrap_or("????"),
            std::str::from_utf8(&body[0..4]).unwrap_or("????")
        )));
    }
    let version = read_u16(body, 4)?;
    if version != VERSION {
        return Err(invalid_input(format!(
            "KP2 version {} is unsupported; expected {}",
            version, VERSION
        )));
    }
    let flags = read_u16(body, 6)?;
    if flags & !allowed_flags != 0 {
        return Err(invalid_data(format!(
            "KP2 flags {} set bits outside the allowed mask {} for this message",
            flags, allowed_flags
        )));
    }
    let chunk_count = read_u32(body, 8)?;
    if chunk_count == 0 {
        return Err(invalid_input("KP2 chunk_count must be > 0"));
    }
    let total_payload_bytes = read_u32(body, 12)?;
    body_too_large(total_payload_bytes as usize)?;
    let entry_table_bytes = read_u32(body, 16)?;
    let reserved = read_u32(body, 20)?;
    if reserved != 0 {
        return Err(invalid_data(
            "KP2 common header reserved field must be zero",
        ));
    }
    Ok(CommonHeader {
        flags,
        chunk_count,
        total_payload_bytes,
        entry_table_bytes,
    })
}

fn header_string(headers: &HeaderMap, name: &str) -> io::Result<String> {
    let value = headers
        .get(name)
        .ok_or_else(|| invalid_input(format!("KP2 requires the {} header", name)))?;
    value
        .to_str()
        .map(str::to_string)
        .map_err(|err| invalid_input(format!("KP2 header {} is not valid UTF-8: {}", name, err)))
}

fn header_u64(headers: &HeaderMap, name: &str) -> io::Result<u64> {
    header_string(headers, name)?.parse::<u64>().map_err(|err| {
        invalid_input(format!(
            "KP2 header {} must be an unsigned integer and got parse error: {}",
            name, err
        ))
    })
}

fn read_chunk_id(body: &[u8], offset: usize) -> io::Result<ChunkId> {
    let end = offset
        .checked_add(32)
        .ok_or_else(|| invalid_data("KP2 chunk id offset overflow"))?;
    let slice = body
        .get(offset..end)
        .ok_or_else(|| invalid_data("KP2 body ended while reading a chunk id"))?;
    let mut chunk_id = [0_u8; 32];
    chunk_id.copy_from_slice(slice);
    Ok(ChunkId(chunk_id))
}

fn read_u16(body: &[u8], offset: usize) -> io::Result<u16> {
    let end = offset
        .checked_add(2)
        .ok_or_else(|| invalid_data("KP2 u16 offset overflow"))?;
    let slice = body
        .get(offset..end)
        .ok_or_else(|| invalid_data("KP2 body ended while reading a u16"))?;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}

fn read_u32(body: &[u8], offset: usize) -> io::Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| invalid_data("KP2 u32 offset overflow"))?;
    let slice = body
        .get(offset..end)
        .ok_or_else(|| invalid_data("KP2 body ended while reading a u32"))?;
    Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn read_u64(body: &[u8], offset: usize) -> io::Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| invalid_data("KP2 u64 offset overflow"))?;
    let slice = body
        .get(offset..end)
        .ok_or_else(|| invalid_data("KP2 body ended while reading a u64"))?;
    Ok(u64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(seed: u8) -> ChunkId {
        let mut raw = [0_u8; 32];
        raw.fill(seed);
        ChunkId(raw)
    }

    #[test]
    fn write_entry_carries_self_describing_identity() {
        let pack = PackedWriteRequest {
            entries: vec![PackedWriteEntry {
                chunk_id: chunk(5),
                slot_index: 42,
                generation: 9,
                identity: WriteIdentity {
                    object_id: 0x1234_5678,
                    object_version: 3,
                    stripe: 7,
                    frag: 2,
                },
                payload: vec![9, 9, 9],
            }],
        };
        let decoded = decode_write_request(&encode_write_request(&pack).unwrap()).unwrap();
        assert_eq!(decoded.entries[0].identity, pack.entries[0].identity);
        assert_eq!(decoded, pack);
    }

    #[test]
    fn write_pack_round_trips() {
        let pack = PackedWriteRequest {
            entries: vec![
                PackedWriteEntry {
                    chunk_id: chunk(1),
                    slot_index: 7,
                    generation: 3,
                    identity: WriteIdentity::default(),
                    payload: vec![1, 2, 3, 4],
                },
                PackedWriteEntry {
                    chunk_id: chunk(2),
                    slot_index: 8,
                    generation: 4,
                    identity: WriteIdentity::default(),
                    payload: vec![5, 6],
                },
            ],
        };
        let encoded = encode_write_request(&pack).unwrap();
        let decoded = decode_write_request(&encoded).unwrap();
        assert_eq!(decoded, pack);
    }

    #[test]
    fn header_plus_payload_segments_match_whole_pack_encoding() {
        // Fix #7(b): clients stream the header/descriptor prefix followed by each
        // payload as separate frames instead of concatenating the whole pack. The
        // reassembled wire bytes must be byte-identical to encode_write_request, and
        // still decode back to the original pack.
        let pack = PackedWriteRequest {
            entries: vec![
                PackedWriteEntry {
                    chunk_id: chunk(1),
                    slot_index: 7,
                    generation: 3,
                    identity: WriteIdentity::default(),
                    payload: vec![1, 2, 3, 4],
                },
                PackedWriteEntry {
                    chunk_id: chunk(2),
                    slot_index: 8,
                    generation: 4,
                    identity: WriteIdentity::default(),
                    payload: vec![5, 6],
                },
                PackedWriteEntry {
                    chunk_id: chunk(3),
                    slot_index: 9,
                    generation: 5,
                    identity: WriteIdentity::default(),
                    payload: Vec::new(),
                },
            ],
        };

        let whole = encode_write_request(&pack).unwrap();
        let header = encode_write_request_header(&pack.entries).unwrap();
        assert_eq!(
            header.len(),
            COMMON_HEADER_BYTES + pack.entries.len() * WRITE_ENTRY_BYTES
        );

        let mut reassembled = header.clone();
        for entry in &pack.entries {
            reassembled.extend_from_slice(&entry.payload);
        }
        assert_eq!(reassembled, whole);
        // And the header is exactly the prefix of the monolithic encoding.
        assert_eq!(&whole[..header.len()], header.as_slice());
        // The segmented stream still decodes to the original request.
        assert_eq!(decode_write_request(&reassembled).unwrap(), pack);
    }

    #[test]
    fn encode_write_request_header_rejects_empty_entries() {
        assert!(encode_write_request_header(&[]).is_err());
    }

    #[test]
    fn read_query_round_trips() {
        let query = PackedReadQuery {
            chunk_ids: vec![chunk(9), chunk(10)],
            ranges: None,
        };
        let encoded = encode_read_query(&query).unwrap();
        let decoded = decode_read_query(&encoded).unwrap();
        assert_eq!(decoded, query);
    }

    #[test]
    fn ranged_read_query_round_trips() {
        let query = PackedReadQuery {
            chunk_ids: vec![chunk(9), chunk(10)],
            ranges: Some(vec![
                ChunkRange {
                    offset: 4096,
                    length: 4096,
                },
                ChunkRange {
                    offset: 0,
                    length: 1024,
                },
            ]),
        };
        let encoded = encode_read_query(&query).unwrap();
        // The ranged flag is set on the wire and entries are 48 bytes wide.
        assert_eq!(
            read_u16(&encoded, 6).unwrap() & FLAG_QUERY_RANGED,
            FLAG_QUERY_RANGED
        );
        assert_eq!(encoded.len(), COMMON_HEADER_BYTES + 2 * QUERY_ENTRY_RANGED_BYTES);
        let decoded = decode_read_query(&encoded).unwrap();
        assert_eq!(decoded, query);
    }

    #[test]
    fn ranged_query_length_mismatch_is_rejected() {
        let query = PackedReadQuery {
            chunk_ids: vec![chunk(1), chunk(2)],
            ranges: Some(vec![ChunkRange {
                offset: 0,
                length: 8,
            }]),
        };
        assert!(encode_read_query(&query).is_err());
    }

    #[test]
    fn ranged_flag_rejected_when_not_allowed() {
        // A non-query decoder (allowed_flags = 0) must reject the ranged flag.
        let query = PackedReadQuery {
            chunk_ids: vec![chunk(3)],
            ranges: Some(vec![ChunkRange {
                offset: 0,
                length: 4,
            }]),
        };
        let encoded = encode_read_query(&query).unwrap();
        assert!(decode_common_header(&encoded, MAGIC_QUERY).is_err());
    }

    #[test]
    fn read_response_round_trips() {
        let response = PackedReadResponse {
            entries: vec![
                PackedReadEntry {
                    chunk_id: chunk(7),
                    status_code: 200,
                    location: Some(PackedReadLocation {
                        drive_id: 1,
                        location_kind: LocationKindCode::Extent,
                        physical_offset: 4096,
                        logical_length: 4,
                        stored_length: 4,
                        generation: 2,
                        checksum: 99,
                        slot_index: 3,
                    }),
                    payload: vec![1, 2, 3, 4],
                },
                PackedReadEntry {
                    chunk_id: chunk(8),
                    status_code: 404,
                    location: None,
                    payload: Vec::new(),
                },
            ],
        };
        let encoded = encode_read_response(&response).unwrap();
        let decoded = decode_read_response(&encoded).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn write_reply_round_trips() {
        let reply = PackedWriteReply {
            entries: vec![
                PackedWriteReplyEntry {
                    chunk_id: chunk(3),
                    slot_index: 11,
                    requested_generation: 5,
                    status_code: 201,
                    location: Some(PackedWriteLocation {
                        drive_id: 2,
                        location_kind: LocationKindCode::PackedContainer,
                        physical_offset: 8192,
                        logical_length: 4096,
                        stored_length: 4096,
                        generation: 5,
                        checksum: 1234,
                        slot_index: 11,
                    }),
                    error: None,
                },
                PackedWriteReplyEntry {
                    chunk_id: chunk(4),
                    slot_index: 12,
                    requested_generation: 6,
                    status_code: 422,
                    location: None,
                    error: Some("slot/layout mismatch".to_string()),
                },
            ],
        };
        let encoded = encode_write_reply(&reply).unwrap();
        let decoded = decode_write_reply(&encoded).unwrap();
        assert_eq!(decoded, reply);
    }

    #[test]
    fn rate_limit_headers_round_trip() {
        let mut headers = HeaderMap::new();
        apply_rate_limit_headers(
            &mut headers,
            LIMIT_SCOPE_TARGET,
            LIMIT_CLASS_READ,
            12,
            48,
            25,
        )
        .unwrap();
        assert_eq!(
            header_string(&headers, HEADER_LIMIT_SCOPE).unwrap(),
            LIMIT_SCOPE_TARGET
        );
        assert_eq!(
            header_string(&headers, HEADER_LIMIT_CLASS).unwrap(),
            LIMIT_CLASS_READ
        );
        assert_eq!(
            header_u64(&headers, HEADER_LIMIT_CURRENT_IN_FLIGHT).unwrap(),
            12
        );
        assert_eq!(
            header_u64(&headers, HEADER_LIMIT_MAX_IN_FLIGHT).unwrap(),
            48
        );
        assert_eq!(header_u64(&headers, HEADER_RETRY_AFTER_MS).unwrap(), 25);
    }

    #[test]
    fn encoded_write_request_len_accounts_for_wire_overhead() {
        let bytes = encoded_write_request_len(16, MAX_PACK_PAYLOAD_BYTES).unwrap();
        assert_eq!(
            bytes,
            COMMON_HEADER_BYTES + (16 * WRITE_ENTRY_BYTES) + MAX_PACK_PAYLOAD_BYTES
        );
    }

    #[test]
    fn max_encoded_write_request_bytes_uses_smallest_payload_granule() {
        let bytes = max_encoded_write_request_bytes(16 * 1024).unwrap();
        let expected_entries = MAX_PACK_PAYLOAD_BYTES.div_ceil(16 * 1024);
        assert_eq!(
            bytes,
            COMMON_HEADER_BYTES + (expected_entries * WRITE_ENTRY_BYTES) + MAX_PACK_PAYLOAD_BYTES
        );
    }
}
