// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use super::*;
use std::time::Duration;

pub(crate) async fn serve_connection(
    socket: TcpStream,
    state: Arc<TargetState>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let handshake_started = std::time::Instant::now();
    let mut builder = h2::server::Builder::new();
    builder
        .initial_window_size(state.h2_initial_window_bytes)
        .initial_connection_window_size(state.h2_initial_connection_window_bytes)
        .max_frame_size(state.h2_max_frame_bytes)
        .max_header_list_size(state.h2_max_header_list_bytes)
        .max_concurrent_streams(state.h2_max_concurrent_streams)
        .max_send_buffer_size(state.h2_max_send_buffer_bytes);
    let mut connection = match builder.handshake(socket).await {
        Ok(connection) => {
            state
                .router
                .stats
                .record_handshake_success(handshake_started);
            connection
        }
        Err(err) => {
            state.router.stats.record_handshake_failure(
                handshake_started,
                format!("KST HTTP/2 handshake failed: {}", err),
            );
            return Err(Box::new(err));
        }
    };
    while let Some(result) = connection.accept().await {
        let (request, respond) = result?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let request_state = Arc::clone(&state);
            if let Err(err) = handle_request(request, respond, request_state).await {
                let stats = Arc::clone(&state.router.stats);
                stats.record_background_error(format!(
                    "KST request handler failed before it could reply cleanly: {err}"
                ));
            }
        });
    }
    Ok(())
}

async fn handle_request(
    request: Request<RecvStream>,
    mut respond: SendResponse<Bytes>,
    state: Arc<TargetState>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let rpc = classify_rpc(&method, uri.path());
    let started = state.router.stats.begin(rpc);
    let stream_permit = match Arc::clone(&state.active_stream_limit).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            state.router.stats.record_stream_rejection(rpc);
            let error = ServiceError::new(
                StatusCode::TOO_MANY_REQUESTS,
                format!(
                    "KST rejected the request because active HTTP/2 streams hit the configured ceiling of {}. Retry after the target drains.",
                    state.max_active_streams
                ),
                true,
            );
            state
                .router
                .stats
                .finish(rpc, started, 0, Some(error.public_message.clone()));
            send_error_response(
                &mut respond,
                &method,
                error.status,
                &error.public_message,
                kp2_rate_limit_headers(
                    LIMIT_SCOPE_TARGET,
                    LIMIT_CLASS_ALL,
                    current_in_flight(&state.active_stream_limit, state.max_active_streams),
                    state.max_active_streams,
                    RATE_LIMIT_RETRY_AFTER_MS,
                )?,
            )?;
            return Ok(());
        }
    };
    let class_permit = match stream_class_name(rpc) {
        "read" => match Arc::clone(&state.read_stream_limit).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                state.router.stats.record_stream_rejection(rpc);
                let error = ServiceError::new(
                    StatusCode::TOO_MANY_REQUESTS,
                    format!(
                        "KST rejected the request because active read/control streams hit the configured ceiling of {}. Retry after the target drains.",
                        state.max_read_streams
                    ),
                    true,
                );
                state
                    .router
                    .stats
                    .finish(rpc, started, 0, Some(error.public_message.clone()));
                send_error_response(
                    &mut respond,
                    &method,
                    error.status,
                    &error.public_message,
                    kp2_rate_limit_headers(
                        LIMIT_SCOPE_TARGET,
                        LIMIT_CLASS_READ,
                        current_in_flight(&state.read_stream_limit, state.max_read_streams),
                        state.max_read_streams,
                        RATE_LIMIT_RETRY_AFTER_MS,
                    )?,
                )?;
                return Ok(());
            }
        },
        "write" => match Arc::clone(&state.write_stream_limit).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                state.router.stats.record_stream_rejection(rpc);
                let error = ServiceError::new(
                    StatusCode::TOO_MANY_REQUESTS,
                    format!(
                        "KST rejected the request because active write/delete streams hit the configured ceiling of {}. Retry after the target drains.",
                        state.max_write_streams
                    ),
                    true,
                );
                state
                    .router
                    .stats
                    .finish(rpc, started, 0, Some(error.public_message.clone()));
                send_error_response(
                    &mut respond,
                    &method,
                    error.status,
                    &error.public_message,
                    kp2_rate_limit_headers(
                        LIMIT_SCOPE_TARGET,
                        LIMIT_CLASS_WRITE,
                        current_in_flight(&state.write_stream_limit, state.max_write_streams),
                        state.max_write_streams,
                        RATE_LIMIT_RETRY_AFTER_MS,
                    )?,
                )?;
                return Ok(());
            }
        },
        _ => unreachable!(),
    };
    // Correlated per-fragment trace: assembled across the decode/body-collect here and the
    // queue_wait/route_execute the execution worker hands back, then emitted once after the
    // response is sent. Only the streamed-write fast path (the 10 MiB fragment path) sets it.
    let trace_enabled = frame_trace_enabled();
    let mut frag_trace: Option<KstFragmentTrace> = None;
    let response_result: Result<ServiceResponse, ServiceError> = if is_streamed_chunk_write(
        &method,
        uri.path(),
    ) {
        let decode_started = Instant::now();
        let streamed_request = (|| {
            let chunk_id = parse_chunk_id_from_path(uri.path())?;
            let slot_index = parse_query_granule_index(uri.query())?;
            let generation = parse_query_u32(uri.query(), "generation")?;
            let identity = ChunkSelfDescribingIdentity {
                object_id: parse_query_u32_opt(uri.query(), "object_id"),
                object_version: parse_query_u32_opt(uri.query(), "object_version") as u16,
                stripe: parse_query_u32_opt(uri.query(), "stripe") as u16,
                frag: parse_query_u32_opt(uri.query(), "frag") as u16,
            };
            let expected_bytes = parse_content_length(&headers)?;
            Ok::<_, ServiceError>((chunk_id, slot_index, generation, identity, expected_bytes))
        })();
        match streamed_request {
            Ok((chunk_id, slot_index, generation, identity, expected_bytes)) => {
                let decode_elapsed = decode_started.elapsed();
                state
                    .router
                    .stats
                    .record_phase(rpc, RequestPhase::RequestDecode, decode_elapsed);
                let trace_id = trace_enabled.then(|| fragment_trace_id(&identity));
                let body_receive_started = Instant::now();
                match collect_streamed_body(
                    request.into_body(),
                    state.max_request_body_bytes,
                    expected_bytes,
                    trace_id.as_deref(),
                )
                .await
                {
                    Ok(body) => {
                        let body_collect_elapsed = body_receive_started.elapsed();
                        state.router.stats.record_phase(
                            rpc,
                            RequestPhase::BodyStreamReceive,
                            body_collect_elapsed,
                        );
                        match state.direct_write_execution.submit(
                                rpc,
                                DirectExecutionRequest::Write {
                                    chunk_id,
                                    slot_index,
                                    generation,
                                    identity,
                                    body,
                                },
                            ) {
                                Ok(response_rx) => match response_rx.await {
                                    Ok(outcome) => {
                                        if let Some(trace_id) = trace_id {
                                            frag_trace = Some(KstFragmentTrace {
                                                trace_id,
                                                decode: decode_elapsed,
                                                body_collect: body_collect_elapsed,
                                                queue_wait: outcome.queue_wait,
                                                route_execute: outcome.route_execute,
                                            });
                                        }
                                        outcome.result
                                    }
                                    Err(_) => Err(ServiceError::new(
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        "KST direct chunk write execution worker stopped before it could produce a response"
                                            .to_string(),
                                        true,
                                    )),
                                },
                                Err(err) => Err(map_direct_submit_error(err)),
                            }
                    }
                    Err(err) => {
                        let status = if err.kind() == io::ErrorKind::InvalidData {
                            StatusCode::PAYLOAD_TOO_LARGE
                        } else {
                            StatusCode::BAD_REQUEST
                        };
                        Err(ServiceError::new(status, err.to_string(), true))
                    }
                }
            }
            Err(err) => Err(err),
        }
    } else if is_direct_chunk_read_fast_path(&method, uri.path()) {
        let body_collect_started = Instant::now();
        let body = collect_body(request.into_body(), state.max_request_body_bytes).await;
        state.router.stats.record_phase(
            rpc,
            RequestPhase::BodyCollect,
            body_collect_started.elapsed(),
        );
        match body {
            Ok(body) => {
                if !body.is_empty() {
                    Err(ServiceError::new(
                        StatusCode::BAD_REQUEST,
                        "KST read requests must not include a request body".to_string(),
                        true,
                    ))
                } else {
                    match parse_chunk_id_from_path(uri.path()) {
                            Ok(chunk_id) => match state
                                .direct_read_execution
                                .submit(rpc, DirectExecutionRequest::Read { chunk_id })
                            {
                                Ok(response_rx) => match response_rx.await {
                                    Ok(outcome) => outcome.result,
                                    Err(_) => Err(ServiceError::new(
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        "KST direct read execution worker stopped before it could produce a response"
                                            .to_string(),
                                        true,
                                    )),
                                },
                                Err(err) => Err(map_direct_submit_error(err)),
                            },
                            Err(err) => Err(err),
                        }
                }
            }
            Err(err) => {
                let status = if err.kind() == io::ErrorKind::InvalidData {
                    StatusCode::PAYLOAD_TOO_LARGE
                } else {
                    StatusCode::BAD_REQUEST
                };
                Err(ServiceError::new(status, err.to_string(), true))
            }
        }
    } else {
        let body_collect_started = Instant::now();
        let body = collect_body(request.into_body(), state.max_request_body_bytes).await;
        state.router.stats.record_phase(
            rpc,
            RequestPhase::BodyCollect,
            body_collect_started.elapsed(),
        );
        match body {
            Ok(body) => {
                let ingress = match stream_class_name(rpc) {
                    "read" => &state.read_ingress,
                    "write" => &state.write_ingress,
                    _ => unreachable!(),
                };
                let response_rx = ingress.submit(
                    rpc,
                    IngressRequest::Buffered {
                        method: method.clone(),
                        uri: uri.clone(),
                        headers: headers.clone(),
                        body,
                    },
                );
                match response_rx {
                    Ok(response_rx) => match response_rx.await {
                        Ok(result) => result,
                        Err(_) => Err(ServiceError::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "KST ingress worker stopped before it could produce a response"
                                .to_string(),
                            true,
                        )),
                    },
                    Err(err) => Err(map_ingress_submit_error(err)),
                }
            }
            Err(err) => {
                let status = if err.kind() == io::ErrorKind::InvalidData {
                    StatusCode::PAYLOAD_TOO_LARGE
                } else {
                    StatusCode::BAD_REQUEST
                };
                Err(ServiceError::new(status, err.to_string(), true))
            }
        }
    };
    // Assigned by both match arms (each diverges via `?` before this point only on a send
    // failure, in which case the post-match read is never reached).
    let resp_send_elapsed: Duration;
    match response_result {
        Ok(response) => {
            let payload_bytes = response.accounted_payload_bytes;
            state.router.stats.finish(rpc, started, payload_bytes, None);
            let send_started = Instant::now();
            let send_timing = send_response(&mut respond, response, method == Method::HEAD)?;
            state.router.stats.record_phase(
                rpc,
                RequestPhase::ResponseSendHeaders,
                send_timing.headers,
            );
            state
                .router
                .stats
                .record_phase(rpc, RequestPhase::ResponseSendBody, send_timing.body);
            resp_send_elapsed = send_started.elapsed();
            state
                .router
                .stats
                .record_phase(rpc, RequestPhase::ResponseSend, resp_send_elapsed);
        }
        Err(err) => {
            let error_message = err.public_message.clone();
            state.router.stats.finish(
                rpc,
                started,
                0,
                err.count_as_error.then_some(error_message.clone()),
            );
            let send_started = Instant::now();
            let send_timing = send_error_response(
                &mut respond,
                &method,
                err.status,
                &error_message,
                Vec::new(),
            )?;
            state.router.stats.record_phase(
                rpc,
                RequestPhase::ResponseSendHeaders,
                send_timing.headers,
            );
            state
                .router
                .stats
                .record_phase(rpc, RequestPhase::ResponseSendBody, send_timing.body);
            resp_send_elapsed = send_started.elapsed();
            state
                .router
                .stats
                .record_phase(rpc, RequestPhase::ResponseSend, resp_send_elapsed);
        }
    }
    if let Some(trace) = frag_trace {
        emit_kst_frame_trace(&trace, resp_send_elapsed);
    }
    drop(class_permit);
    drop(stream_permit);
    Ok(())
}

struct ResponseSendTiming {
    headers: Duration,
    body: Duration,
}

fn send_response(
    respond: &mut SendResponse<Bytes>,
    response: ServiceResponse,
    head_only: bool,
) -> Result<ResponseSendTiming, Box<dyn Error + Send + Sync>> {
    let ServiceResponse {
        status,
        body,
        content_type,
        location,
        extra_headers,
        ..
    } = response;
    let mut http_response = Response::builder().status(status).body(())?;
    if let Some(content_type) = content_type {
        http_response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    }
    if let Some(location) = location {
        apply_location_headers(http_response.headers_mut(), &location)?;
    }
    for (name, value) in extra_headers {
        http_response.headers_mut().insert(name, value);
    }
    let body_len = if head_only { 0 } else { body.len() };
    http_response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&body_len.to_string())?,
    );
    let end_stream = head_only || body.is_empty();
    let headers_started = Instant::now();
    let mut send_stream = respond.send_response(http_response, end_stream)?;
    let headers_elapsed = headers_started.elapsed();
    let mut body_elapsed = Duration::ZERO;
    if !end_stream {
        let body_started = Instant::now();
        send_stream.send_data(body, true)?;
        body_elapsed = body_started.elapsed();
    }
    Ok(ResponseSendTiming {
        headers: headers_elapsed,
        body: body_elapsed,
    })
}

fn send_error_response(
    respond: &mut SendResponse<Bytes>,
    method: &Method,
    status: StatusCode,
    message: &str,
    extra_headers: Vec<(HeaderName, HeaderValue)>,
) -> Result<ResponseSendTiming, Box<dyn Error + Send + Sync>> {
    let body = if *method == Method::HEAD {
        Bytes::new()
    } else {
        Bytes::from(encode_json(&ErrorDocument {
            error: message.to_string(),
        })?)
    };
    let mut http_response = Response::builder().status(status).body(())?;
    if !body.is_empty() {
        http_response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }
    for (name, value) in extra_headers {
        http_response.headers_mut().insert(name, value);
    }
    http_response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string())?,
    );
    let end_stream = body.is_empty();
    let headers_started = Instant::now();
    let mut send_stream = respond.send_response(http_response, end_stream)?;
    let headers_elapsed = headers_started.elapsed();
    let mut body_elapsed = Duration::ZERO;
    if !end_stream {
        let body_started = Instant::now();
        send_stream.send_data(body, true)?;
        body_elapsed = body_started.elapsed();
    }
    Ok(ResponseSendTiming {
        headers: headers_elapsed,
        body: body_elapsed,
    })
}

fn current_in_flight(limit: &Semaphore, max_in_flight: usize) -> usize {
    max_in_flight.saturating_sub(limit.available_permits())
}

/// Diagnostic: gated on `KST_FRAME_TRACE=1`. Splits the fused body-receive span into the
/// part that no per-request phase isolates — the wait for the FIRST DATA frame to become
/// deliverable (scheduling/wire) vs the inter-frame gaps while draining. A large
/// `first_frame_wait_us` at low CPU means the h2 connection task was not scheduled (the
/// busy-poll-starvation signature); large inter-frame gaps clustered at the stream-window
/// boundary instead point at flow-control stop-and-wait. Off by default — one `OnceLock`
/// env read, zero cost on the hot path when disabled.
fn frame_trace_enabled() -> bool {
    static FLAG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *FLAG.get_or_init(|| {
        std::env::var("KST_FRAME_TRACE")
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// The cross-hop correlation key for one fragment write. Derived from the same four request
/// fields the client uses (they ride the URL query string, so there are no extra wire bytes
/// and no protocol change). A fragment's `kst_frame_trace` line joins to the client's
/// `ksc_frame_trace` line on this string.
fn fragment_trace_id(identity: &ChunkSelfDescribingIdentity) -> String {
    format!(
        "{}:{}:{}:{}",
        identity.object_id, identity.object_version, identity.stripe, identity.frag
    )
}

/// One fragment's target-side timeline, assembled across the request task (decode,
/// body_collect, resp_send) and the execution worker (queue_wait, route_execute — handed back
/// over the completion channel since the worker runs on a different thread). Emitted as a
/// single correlated line so a fragment can be lined up against its `ksc_frame_trace`. The
/// media/fsync/kix split stays in the per-RPC phase stats; `route_execute` is their sum here.
struct KstFragmentTrace {
    trace_id: String,
    decode: Duration,
    body_collect: Duration,
    queue_wait: Duration,
    route_execute: Duration,
}

fn emit_kst_frame_trace(trace: &KstFragmentTrace, resp_send: Duration) {
    eprintln!(
        "kst_frame_trace trace={} decode_us={} body_collect_us={} queue_wait_us={} \
         route_execute_us={} resp_send_us={} total_us={}",
        trace.trace_id,
        trace.decode.as_micros(),
        trace.body_collect.as_micros(),
        trace.queue_wait.as_micros(),
        trace.route_execute.as_micros(),
        resp_send.as_micros(),
        (trace.decode + trace.body_collect + trace.queue_wait + trace.route_execute + resp_send)
            .as_micros(),
    );
}

pub(super) async fn collect_body(mut body: RecvStream, max_bytes: usize) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    let trace = frame_trace_enabled();
    let recv_started = Instant::now();
    let mut first_frame_wait: Option<std::time::Duration> = None;
    let mut last_frame_at = recv_started;
    let mut max_inter_frame_gap = std::time::Duration::ZERO;
    let mut frames = 0_usize;
    while let Some(chunk) = body.data().await {
        if trace {
            let now = Instant::now();
            match first_frame_wait {
                None => first_frame_wait = Some(now.duration_since(recv_started)),
                Some(_) => {
                    max_inter_frame_gap =
                        max_inter_frame_gap.max(now.duration_since(last_frame_at))
                }
            }
            last_frame_at = now;
            frames += 1;
        }
        let chunk = chunk.map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        let new_len = out.len().saturating_add(chunk.len());
        if new_len > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "KST request body exceeded the configured {} byte wire-body limit",
                    max_bytes
                ),
            ));
        }
        out.extend_from_slice(&chunk);
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
    }
    if trace {
        eprintln!(
            "kst_frame_trace path=buffered bytes={} frames={} first_frame_wait_us={} max_inter_frame_gap_us={} total_recv_us={}",
            out.len(),
            frames,
            first_frame_wait.unwrap_or_default().as_micros(),
            max_inter_frame_gap.as_micros(),
            recv_started.elapsed().as_micros(),
        );
    }
    Ok(out)
}

async fn collect_streamed_body(
    mut body: RecvStream,
    max_bytes: usize,
    expected_bytes: Option<usize>,
    trace_id: Option<&str>,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(expected_bytes.unwrap_or(0).min(max_bytes));
    let trace = frame_trace_enabled();
    let recv_started = Instant::now();
    let mut first_frame_wait: Option<std::time::Duration> = None;
    let mut last_frame_at = recv_started;
    let mut max_inter_frame_gap = std::time::Duration::ZERO;
    let mut frames = 0_usize;
    while let Some(chunk) = body.data().await {
        if trace {
            let now = Instant::now();
            match first_frame_wait {
                None => first_frame_wait = Some(now.duration_since(recv_started)),
                Some(_) => {
                    max_inter_frame_gap = max_inter_frame_gap.max(now.duration_since(last_frame_at))
                }
            }
            last_frame_at = now;
            frames += 1;
        }
        let chunk = chunk.map_err(|err| {
            io::Error::other(format!(
                "KST failed while receiving the streamed request body: {}",
                err
            ))
        })?;
        let new_len = out.len().saturating_add(chunk.len());
        if new_len > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "KST request body exceeded the configured {} byte wire-body limit",
                    max_bytes
                ),
            ));
        }
        out.extend_from_slice(&chunk);
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(|err| io::Error::other(err.to_string()))?;
    }
    if trace {
        eprintln!(
            "kst_frame_trace path=streamed trace={} bytes={} frames={} first_frame_wait_us={} max_inter_frame_gap_us={} total_recv_us={}",
            trace_id.unwrap_or("-"),
            out.len(),
            frames,
            first_frame_wait.unwrap_or_default().as_micros(),
            max_inter_frame_gap.as_micros(),
            recv_started.elapsed().as_micros(),
        );
    }
    if let Some(expected_bytes) = expected_bytes {
        if out.len() != expected_bytes {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "KST received {} bytes for the streamed chunk body but the request declared content-length {}",
                    out.len(),
                    expected_bytes
                ),
            ));
        }
    }
    Ok(out)
}
