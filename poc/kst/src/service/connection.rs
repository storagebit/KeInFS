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
    let response_result: Result<ServiceResponse, ServiceError> = if is_streamed_chunk_write(
        &method,
        uri.path(),
    ) {
        let decode_started = Instant::now();
        let streamed_request = (|| {
            let chunk_id = parse_chunk_id_from_path(uri.path())?;
            let slot_index = parse_query_granule_index(uri.query())?;
            let generation = parse_query_u32(uri.query(), "generation")?;
            let expected_bytes = parse_content_length(&headers)?;
            Ok::<_, ServiceError>((chunk_id, slot_index, generation, expected_bytes))
        })();
        match streamed_request {
            Ok((chunk_id, slot_index, generation, expected_bytes)) => {
                state.router.stats.record_phase(
                    rpc,
                    RequestPhase::RequestDecode,
                    decode_started.elapsed(),
                );
                let body_receive_started = Instant::now();
                match collect_streamed_body(
                    request.into_body(),
                    state.max_request_body_bytes,
                    expected_bytes,
                )
                .await
                {
                    Ok(body) => {
                        state.router.stats.record_phase(
                            rpc,
                            RequestPhase::BodyStreamReceive,
                            body_receive_started.elapsed(),
                        );
                        match state.direct_write_execution.submit(
                                rpc,
                                DirectExecutionRequest::Write {
                                    chunk_id,
                                    slot_index,
                                    generation,
                                    body,
                                },
                            ) {
                                Ok(response_rx) => match response_rx.await {
                                    Ok(result) => result,
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
                                    Ok(result) => result,
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
            state.router.stats.record_phase(
                rpc,
                RequestPhase::ResponseSend,
                send_started.elapsed(),
            );
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
            state.router.stats.record_phase(
                rpc,
                RequestPhase::ResponseSend,
                send_started.elapsed(),
            );
        }
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

pub(super) async fn collect_body(mut body: RecvStream, max_bytes: usize) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = body.data().await {
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
    Ok(out)
}

async fn collect_streamed_body(
    mut body: RecvStream,
    max_bytes: usize,
    expected_bytes: Option<usize>,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(expected_bytes.unwrap_or(0).min(max_bytes));
    while let Some(chunk) = body.data().await {
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
