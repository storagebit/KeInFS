// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use bytes::Bytes;
use h2::{client::SendRequest, RecvStream};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Uri};
use kp2::{
    apply_packed_headers, decode_read_response, decode_write_reply, encode_read_query,
    encode_write_request_header, validate_declared_counts, validate_packed_headers, ChunkId,
    PackedReadQuery, PackedReadResponse, PackedWriteEntry, PackedWriteReply, PackedWriteRequest,
    WriteIdentity, CONTENT_TYPE as KP2_CONTENT_TYPE, HEADER_LIMIT_CLASS, HEADER_LIMIT_CURRENT_IN_FLIGHT,
    HEADER_LIMIT_MAX_IN_FLIGHT, HEADER_LIMIT_SCOPE, HEADER_RETRY_AFTER_MS, KIND_QUERY, KIND_READ,
    KIND_WRITE, MAX_PACK_PAYLOAD_BYTES,
};
use serde::Deserialize;
use std::fmt;
use std::future::{poll_fn, Future};
use std::io;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

const H2_INITIAL_WINDOW_BYTES: u32 = 8 * 1024 * 1024;
const H2_INITIAL_CONNECTION_WINDOW_BYTES: u32 = 512 * 1024 * 1024;
const H2_MAX_FRAME_BYTES: u32 = 1024 * 1024;
const H2_MAX_CONCURRENT_STREAMS: u32 = 512;
const H2_MAX_SEND_BUFFER_BYTES: usize = 256 * 1024 * 1024;
const H2_REQUEST_BODY_CHUNK_BYTES: usize = 1024 * 1024;
const HEADER_GRANULE_INDEX: &str = "x-kst-granule-index";
const HEADER_SLOT_INDEX: &str = "x-kst-slot-index";
const HEADER_GENERATION: &str = "x-kst-generation";

pub type DynError = Box<dyn std::error::Error + Send + Sync>;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionMode {
    Interrupt,
    HotPoll,
}

impl CompletionMode {
    #[allow(dead_code)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interrupt => "interrupt",
            Self::HotPoll => "hot-poll",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TargetSessionOptions {
    pub read_completion_mode: CompletionMode,
    pub write_completion_mode: CompletionMode,
}

impl Default for TargetSessionOptions {
    fn default() -> Self {
        Self {
            read_completion_mode: CompletionMode::Interrupt,
            write_completion_mode: CompletionMode::Interrupt,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum RequestClass {
    Info,
    Read,
    Write,
    Delete,
}

impl TargetSessionOptions {
    fn completion_mode_for(self, class: RequestClass) -> CompletionMode {
        match class {
            RequestClass::Read | RequestClass::Info => self.read_completion_mode,
            RequestClass::Write | RequestClass::Delete => self.write_completion_mode,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RequestPhaseTimes {
    pub ready_wait: Duration,
    pub request_prepare: Duration,
    pub send_headers: Duration,
    pub send_body: Duration,
    pub wait_response: Duration,
    pub collect_response: Duration,
    pub protocol_decode: Duration,
    pub payload_validate: Duration,
}

impl RequestPhaseTimes {
    pub fn add_protocol_decode(&mut self, elapsed: Duration) {
        self.protocol_decode += elapsed;
    }

    pub fn add_payload_validate(&mut self, elapsed: Duration) {
        self.payload_validate += elapsed;
    }
}

#[derive(Debug)]
pub struct TimedResponse<T> {
    pub value: T,
    pub phases: RequestPhaseTimes,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TargetInfo {
    pub target_id: String,
    pub layout_kind: String,
    pub extent_bytes: u32,
    pub packed_bytes: u32,
    #[serde(default)]
    pub max_request_body_bytes: usize,
    #[serde(default = "default_max_packed_payload_bytes")]
    pub max_packed_payload_bytes: usize,
    #[serde(default)]
    pub max_packed_write_request_bytes: usize,
}

fn default_max_packed_payload_bytes() -> usize {
    MAX_PACK_PAYLOAD_BYTES
}

impl TargetInfo {
    pub fn packed_payload_limit(&self, requested_limit: usize) -> usize {
        let advertised = if self.max_packed_payload_bytes == 0 {
            MAX_PACK_PAYLOAD_BYTES
        } else {
            self.max_packed_payload_bytes
        };
        requested_limit.min(advertised)
    }

    pub fn packed_write_body_limit(&self) -> usize {
        if self.max_packed_write_request_bytes != 0 {
            self.max_packed_write_request_bytes
        } else if self.max_request_body_bytes != 0 {
            self.max_request_body_bytes
        } else {
            usize::MAX
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReadChunkReply {
    pub payload: Vec<u8>,
    #[allow(dead_code)]
    pub granule_index: u64,
    pub slot_index: u64,
    pub generation: u32,
}

#[derive(Debug)]
pub enum ClientError {
    Transport(String),
    Http {
        action: &'static str,
        status: StatusCode,
        headers: HeaderMap,
        body: Vec<u8>,
    },
    Protocol(String),
}

impl ClientError {
    pub fn is_rate_limited(&self) -> bool {
        matches!(
            self,
            Self::Http {
                status: StatusCode::TOO_MANY_REQUESTS,
                ..
            }
        )
    }

    pub fn retry_after_ms(&self) -> Option<u64> {
        let Self::Http { headers, .. } = self else {
            return None;
        };
        headers
            .get(HEADER_RETRY_AFTER_MS)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
    }

    pub fn rate_limit_class(&self) -> Option<String> {
        let Self::Http { headers, .. } = self else {
            return None;
        };
        headers
            .get(HEADER_LIMIT_CLASS)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    }

    pub fn limit_max_inflight(&self) -> Option<usize> {
        let Self::Http { headers, .. } = self else {
            return None;
        };
        headers
            .get(HEADER_LIMIT_MAX_IN_FLIGHT)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(message) | Self::Protocol(message) => write!(f, "{message}"),
            Self::Http {
                action,
                status,
                headers,
                body,
            } => write!(
                f,
                "KSC {} expected a successful KP2 response and got {}{} with body {}",
                action,
                status,
                rate_limit_suffix(headers),
                String::from_utf8_lossy(body)
            ),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Clone)]
pub struct TargetSession {
    endpoint: String,
    client: SendRequest<Bytes>,
    options: TargetSessionOptions,
}

impl TargetSession {
    pub async fn connect(endpoint: &str) -> Result<Self, ClientError> {
        Self::connect_with_options(endpoint, TargetSessionOptions::default()).await
    }

    pub async fn connect_with_options(
        endpoint: &str,
        options: TargetSessionOptions,
    ) -> Result<Self, ClientError> {
        let uri: Uri = endpoint.parse().map_err(|err| {
            ClientError::Transport(format!("KSC endpoint URI `{endpoint}` is invalid: {err}"))
        })?;
        let authority = uri.authority().ok_or_else(|| {
            ClientError::Transport("KSC endpoint must include host:port".to_string())
        })?;
        let host = authority.host().to_string();
        let port = authority.port_u16().unwrap_or(80);
        let socket = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|err| {
                ClientError::Transport(format!(
                    "KSC could not connect to target {}:{}: {}",
                    host, port, err
                ))
            })?;
        socket.set_nodelay(true).map_err(|err| {
            ClientError::Transport(format!(
                "KSC connected to target {} but could not enable TCP_NODELAY: {}",
                endpoint, err
            ))
        })?;
        let mut builder = h2::client::Builder::new();
        builder
            .initial_window_size(H2_INITIAL_WINDOW_BYTES)
            .initial_connection_window_size(H2_INITIAL_CONNECTION_WINDOW_BYTES)
            .max_frame_size(H2_MAX_FRAME_BYTES)
            .max_concurrent_streams(H2_MAX_CONCURRENT_STREAMS)
            .max_send_buffer_size(H2_MAX_SEND_BUFFER_BYTES);
        let (client, connection) = builder.handshake(socket).await.map_err(|err| {
            ClientError::Transport(format!(
                "KSC HTTP/2 handshake with {} failed: {}",
                endpoint, err
            ))
        })?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
            options,
        })
    }

    pub async fn info(&self) -> Result<TargetInfo, ClientError> {
        let response = self
            .send_h2_request(
                Method::GET,
                "/v1/info",
                None,
                Vec::new(),
                None,
                RequestClass::Info,
            )
            .await?;
        let TimedResponse {
            value: (status, headers, body),
            ..
        } = response;
        ensure_status("info", status, StatusCode::OK, &headers, &body)?;
        serde_json::from_slice(&body).map_err(|err| {
            ClientError::Protocol(format!(
                "KSC could not decode KST target info JSON: {}",
                err
            ))
        })
    }

    pub async fn packed_write(
        &self,
        pack: PackedWriteRequest,
    ) -> Result<TimedResponse<PackedWriteReply>, ClientError> {
        // Encode only the header + descriptor table, then stream it followed by each
        // entry payload as a separate HTTP/2 DATA frame. The payload buffers move
        // (zero-copy) into `Bytes`, so the whole-pack concatenation that previously
        // copied every fragment into one buffer is eliminated. The bytes the target
        // reassembles are identical to `encode_write_request(&pack)`.
        let header = encode_write_request_header(&pack.entries).map_err(|err| {
            ClientError::Protocol(format!("KSC could not encode KP2 packed write: {}", err))
        })?;
        let entry_count = pack.entries.len();
        let payload_bytes = total_payload_bytes(&pack.entries);
        let total_body_len = header.len() + payload_bytes;
        let mut segments = Vec::with_capacity(entry_count + 1);
        segments.push(Bytes::from(header));
        for entry in pack.entries {
            if !entry.payload.is_empty() {
                segments.push(Bytes::from(entry.payload));
            }
        }
        let headers = packed_headers(KIND_WRITE, entry_count, payload_bytes).map_err(|err| {
            ClientError::Protocol(format!(
                "KSC could not construct KP2 packed write headers: {}",
                err
            ))
        })?;
        let response = self
            .send_h2_request_segments(
                Method::PUT,
                "/v1/kp2/chunk-pack",
                Some(KP2_CONTENT_TYPE),
                segments,
                total_body_len,
                Some(headers),
                RequestClass::Write,
            )
            .await?;
        let TimedResponse {
            value: (status, headers, body),
            phases,
        } = response;
        if status != StatusCode::CREATED && status.as_u16() != 207 {
            return Err(ClientError::Http {
                action: "packed-write",
                status,
                headers,
                body,
            });
        }
        let decode_started = Instant::now();
        let content_type = required_content_type(&headers)?;
        if content_type != KP2_CONTENT_TYPE {
            return Err(ClientError::Protocol(format!(
                "KSC packed write expected a binary KP2 acknowledgement and got content-type {}",
                content_type
            )));
        }
        validate_packed_headers(&headers, KIND_WRITE).map_err(|err| {
            ClientError::Protocol(format!(
                "KSC packed write response headers are invalid: {}",
                err
            ))
        })?;
        validate_declared_counts(&headers, entry_count, payload_bytes).map_err(|err| {
            ClientError::Protocol(format!(
                "KSC packed write response declared invalid counts: {}",
                err
            ))
        })?;
        let reply = decode_write_reply(&body).map_err(|err| {
            ClientError::Protocol(format!(
                "KSC could not decode KP2 packed write reply: {}",
                err
            ))
        })?;
        let mut phases = phases;
        phases.add_protocol_decode(decode_started.elapsed());
        Ok(TimedResponse {
            value: reply,
            phases,
        })
    }

    pub async fn packed_read(
        &self,
        query: &PackedReadQuery,
        _expected_payload_bytes: usize,
    ) -> Result<TimedResponse<PackedReadResponse>, ClientError> {
        let body = encode_read_query(query).map_err(|err| {
            ClientError::Protocol(format!(
                "KSC could not encode KP2 packed read query: {}",
                err
            ))
        })?;
        let headers = packed_headers(KIND_QUERY, query.chunk_ids.len(), 0).map_err(|err| {
            ClientError::Protocol(format!(
                "KSC could not construct KP2 packed read-query headers: {}",
                err
            ))
        })?;
        let response = self
            .send_h2_request(
                Method::POST,
                "/v1/kp2/chunk-pack/read",
                Some(KP2_CONTENT_TYPE),
                body,
                Some(headers),
                RequestClass::Read,
            )
            .await?;
        let TimedResponse {
            value: (status, headers, body),
            phases,
        } = response;
        ensure_status("packed-read", status, StatusCode::OK, &headers, &body)?;
        let decode_started = Instant::now();
        validate_packed_headers(&headers, KIND_READ).map_err(|err| {
            ClientError::Protocol(format!(
                "KSC packed read response headers are invalid: {}",
                err
            ))
        })?;
        let packed = decode_read_response(&body).map_err(|err| {
            ClientError::Protocol(format!(
                "KSC could not decode KP2 packed read response: {}",
                err
            ))
        })?;
        let actual_payload_bytes = packed
            .entries
            .iter()
            .map(|entry| entry.payload.len())
            .sum::<usize>();
        validate_declared_counts(&headers, packed.entries.len(), actual_payload_bytes).map_err(
            |err| {
                ClientError::Protocol(format!(
                    "KSC packed read response declared invalid counts: {}",
                    err
                ))
            },
        )?;
        if packed.entries.len() != query.chunk_ids.len() {
            return Err(ClientError::Protocol(format!(
                "KSC packed read response returned {} entries for a query of {} chunk ids",
                packed.entries.len(),
                query.chunk_ids.len()
            )));
        }
        let mut phases = phases;
        phases.add_protocol_decode(decode_started.elapsed());
        Ok(TimedResponse {
            value: packed,
            phases,
        })
    }

    pub async fn write_chunk(
        &self,
        chunk_id: ChunkId,
        granule_index: u64,
        generation: u32,
        identity: WriteIdentity,
        payload: Vec<u8>,
    ) -> Result<RequestPhaseTimes, ClientError> {
        let path = format!(
            "/v1/chunk/{chunk}?granule={granule_index}&generation={generation}\
             &object_id={object_id}&object_version={object_version}&stripe={stripe}&frag={frag}",
            chunk = hex::encode(chunk_id.0),
            object_id = identity.object_id,
            object_version = identity.object_version,
            stripe = identity.stripe,
            frag = identity.frag,
        );
        let response = self
            .send_h2_request(
                Method::PUT,
                &path,
                Some("application/octet-stream"),
                payload,
                None,
                RequestClass::Write,
            )
            .await?;
        let TimedResponse {
            value: (status, headers, body),
            phases,
        } = response;
        ensure_status("write", status, StatusCode::CREATED, &headers, &body)?;
        Ok(phases)
    }

    pub async fn read_chunk(
        &self,
        chunk_id: ChunkId,
    ) -> Result<TimedResponse<ReadChunkReply>, ClientError> {
        let path = format!("/v1/chunk/{}", hex::encode(chunk_id.0));
        let response = self
            .send_h2_request(
                Method::GET,
                &path,
                None,
                Vec::new(),
                None,
                RequestClass::Read,
            )
            .await?;
        let TimedResponse {
            value: (status, headers, body),
            phases,
        } = response;
        ensure_status("read", status, StatusCode::OK, &headers, &body)?;
        let granule_index = required_header_u64(&headers, HEADER_GRANULE_INDEX)
            .or_else(|_| required_header_u64(&headers, HEADER_SLOT_INDEX))
            .map_err(|err| {
                ClientError::Protocol(format!(
                    "KSC read response did not carry a valid granule header: {}",
                    err
                ))
            })?;
        let generation = required_header_u32(&headers, HEADER_GENERATION).map_err(|err| {
            ClientError::Protocol(format!(
                "KSC read response did not carry a valid {} header: {}",
                HEADER_GENERATION, err
            ))
        })?;
        Ok(TimedResponse {
            value: ReadChunkReply {
                payload: body,
                granule_index,
                slot_index: granule_index,
                generation,
            },
            phases,
        })
    }

    pub async fn delete_chunk(&self, chunk_id: ChunkId) -> Result<RequestPhaseTimes, ClientError> {
        let path = format!("/v1/chunk/{}", hex::encode(chunk_id.0));
        let response = self
            .send_h2_request(
                Method::DELETE,
                &path,
                None,
                Vec::new(),
                None,
                RequestClass::Delete,
            )
            .await?;
        let TimedResponse {
            value: (status, headers, body),
            phases,
        } = response;
        ensure_status("delete", status, StatusCode::OK, &headers, &body)?;
        Ok(phases)
    }

    async fn send_h2_request(
        &self,
        method: Method,
        path: &str,
        content_type: Option<&str>,
        body: Vec<u8>,
        extra_headers: Option<HeaderMap>,
        request_class: RequestClass,
    ) -> Result<TimedResponse<(StatusCode, HeaderMap, Vec<u8>)>, ClientError> {
        let total_len = body.len();
        let segments = if body.is_empty() {
            Vec::new()
        } else {
            vec![Bytes::from(body)]
        };
        self.send_h2_request_segments(
            method,
            path,
            content_type,
            segments,
            total_len,
            extra_headers,
            request_class,
        )
        .await
    }

    /// Sends a request whose body is supplied as a sequence of pre-built byte
    /// segments. Each segment (after sub-chunking to the negotiated frame size) is
    /// transmitted as its own HTTP/2 DATA frame. Splitting the body across frames
    /// does not alter the bytes the peer reassembles, so callers can stream a KP2
    /// header followed by each entry payload without first concatenating them into
    /// a single buffer.
    #[allow(clippy::too_many_arguments)]
    async fn send_h2_request_segments(
        &self,
        method: Method,
        path: &str,
        content_type: Option<&str>,
        body_segments: Vec<Bytes>,
        total_body_len: usize,
        extra_headers: Option<HeaderMap>,
        request_class: RequestClass,
    ) -> Result<TimedResponse<(StatusCode, HeaderMap, Vec<u8>)>, ClientError> {
        let uri = format!("{}{}", self.endpoint.trim_end_matches('/'), path);
        let mut phases = RequestPhaseTimes::default();
        let completion_mode = self.options.completion_mode_for(request_class);

        let ready_started = Instant::now();
        let mut ready = self.client.clone().ready().await.map_err(|err| {
            ClientError::Transport(format!(
                "KSC could not ready an HTTP/2 stream for {} {}: {}",
                method, uri, err
            ))
        })?;
        phases.ready_wait = ready_started.elapsed();

        let prepare_started = Instant::now();
        let mut request = Request::builder()
            .method(method.clone())
            .uri(uri.clone())
            .body(())
            .map_err(|err| {
                ClientError::Transport(format!(
                    "KSC could not construct {} {} request: {}",
                    method, uri, err
                ))
            })?;
        if let Some(content_type) = content_type {
            request.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_str(content_type).map_err(|err| {
                    ClientError::Transport(format!(
                        "KSC content-type header is invalid for {} {}: {}",
                        method, uri, err
                    ))
                })?,
            );
        }
        if let Some(headers) = extra_headers {
            for (name, value) in headers {
                if let Some(name) = name {
                    request.headers_mut().insert(name, value);
                }
            }
        }
        request.headers_mut().insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&total_body_len.to_string()).map_err(|err| {
                ClientError::Transport(format!(
                    "KSC content-length header is invalid for {} {}: {}",
                    method, uri, err
                ))
            })?,
        );
        phases.request_prepare = prepare_started.elapsed();
        let end_stream = total_body_len == 0;
        let send_headers_started = Instant::now();
        let (response_future, mut send_stream) =
            ready.send_request(request, end_stream).map_err(|err| {
                ClientError::Transport(format!(
                    "KSC could not send {} {} headers: {}",
                    method, uri, err
                ))
            })?;
        phases.send_headers = send_headers_started.elapsed();
        if !end_stream {
            let send_body_started = Instant::now();
            let mut remaining_bytes = total_body_len;
            for mut segment in body_segments {
                while !segment.is_empty() {
                    let chunk_len = segment.len().min(H2_REQUEST_BODY_CHUNK_BYTES);
                    let chunk = segment.split_to(chunk_len);
                    remaining_bytes -= chunk.len();
                    let end_of_stream = remaining_bytes == 0;
                    send_stream.send_data(chunk, end_of_stream).map_err(|err| {
                        ClientError::Transport(format!(
                            "KSC could not send {} {} body: {}",
                            method, uri, err
                        ))
                    })?;
                }
            }
            phases.send_body = send_body_started.elapsed();
        }
        let wait_response_started = Instant::now();
        let response = await_with_mode(completion_mode, response_future)
            .await
            .map_err(|err| {
                ClientError::Transport(format!(
                    "KSC did not receive a {} {} response: {}",
                    method, uri, err
                ))
            })?;
        phases.wait_response = wait_response_started.elapsed();
        let status = response.status();
        let headers = response.headers().clone();
        let collect_response_started = Instant::now();
        let body = collect_body_with_mode(response.into_body(), completion_mode)
            .await
            .map_err(|err| {
                ClientError::Transport(format!(
                    "KSC could not collect {} {} response body: {}",
                    method, uri, err
                ))
            })?;
        phases.collect_response = collect_response_started.elapsed();
        Ok(TimedResponse {
            value: (status, headers, body),
            phases,
        })
    }
}

pub(crate) async fn collect_body_with_mode(
    mut body: RecvStream,
    completion_mode: CompletionMode,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = await_with_mode(completion_mode, body.data()).await {
        let chunk = chunk.map_err(|err| io::Error::other(err.to_string()))?;
        out.extend_from_slice(&chunk);
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(|err| io::Error::other(err.to_string()))?;
    }
    Ok(out)
}

pub(crate) fn ensure_status(
    action: &'static str,
    actual: StatusCode,
    expected: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(), ClientError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ClientError::Http {
            action,
            status: actual,
            headers: headers.clone(),
            body: body.to_vec(),
        })
    }
}

fn required_content_type(headers: &HeaderMap) -> Result<&str, ClientError> {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            ClientError::Protocol(
                "KSC response did not carry a valid content-type header".to_string(),
            )
        })
}

pub(crate) fn packed_headers(
    kind: &str,
    chunk_count: usize,
    total_payload_bytes: usize,
) -> io::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    apply_packed_headers(&mut headers, kind, chunk_count, total_payload_bytes)?;
    Ok(headers)
}

pub(crate) fn total_payload_bytes(entries: &[PackedWriteEntry]) -> usize {
    entries.iter().map(|entry| entry.payload.len()).sum()
}

// These helpers are used by the standalone `ksc` smoke and benchmark paths.
// When other crates depend on `ksc` as a library, those binaries are not part
// of the build, so these otherwise-legitimate helpers look dead to the compiler.
#[allow(dead_code)]
pub(crate) fn payload_len_for_slot(info: &TargetInfo, slot_index: u64) -> Result<usize, DynError> {
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

#[allow(dead_code)]
pub(crate) fn chunk_id_from_seed(seed: u64) -> ChunkId {
    let mut x = seed;
    let mut out = [0_u8; 32];
    for slot in out.chunks_exact_mut(8) {
        x = splitmix64(x);
        slot.copy_from_slice(&x.to_le_bytes());
    }
    ChunkId(out)
}

#[allow(dead_code)]
pub(crate) fn synthetic_payload(
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

#[allow(dead_code)]
pub(crate) fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

#[allow(dead_code)]
pub(crate) fn boxed_error(message: impl Into<String>) -> DynError {
    Box::new(io::Error::other(message.into()))
}

async fn await_with_mode<F>(mode: CompletionMode, future: F) -> F::Output
where
    F: Future,
{
    match mode {
        CompletionMode::Interrupt => future.await,
        CompletionMode::HotPoll => {
            let mut future = std::pin::pin!(future);
            poll_fn(move |cx| match future.as_mut().poll(cx) {
                std::task::Poll::Ready(value) => std::task::Poll::Ready(value),
                std::task::Poll::Pending => {
                    cx.waker().wake_by_ref();
                    std::hint::spin_loop();
                    std::task::Poll::Pending
                }
            })
            .await
        }
    }
}

fn required_header_u64(headers: &HeaderMap, name: &str) -> io::Result<u64> {
    headers
        .get(name)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing {}", name)))?
        .to_str()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?
        .parse::<u64>()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
}

fn required_header_u32(headers: &HeaderMap, name: &str) -> io::Result<u32> {
    headers
        .get(name)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing {}", name)))?
        .to_str()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?
        .parse::<u32>()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
}

fn rate_limit_suffix(headers: &HeaderMap) -> String {
    let scope = headers
        .get(HEADER_LIMIT_SCOPE)
        .and_then(|value| value.to_str().ok());
    let class = headers
        .get(HEADER_LIMIT_CLASS)
        .and_then(|value| value.to_str().ok());
    let current = headers
        .get(HEADER_LIMIT_CURRENT_IN_FLIGHT)
        .and_then(|value| value.to_str().ok());
    let max = headers
        .get(HEADER_LIMIT_MAX_IN_FLIGHT)
        .and_then(|value| value.to_str().ok());
    let retry = headers
        .get(HEADER_RETRY_AFTER_MS)
        .and_then(|value| value.to_str().ok());
    match (scope, class, current, max, retry) {
        (Some(scope), Some(class), Some(current), Some(max), Some(retry)) => format!(
            " (kp2-rate-limit scope={} class={} current={} max={} retry_after_ms={})",
            scope, class, current, max, retry
        ),
        _ => String::new(),
    }
}
