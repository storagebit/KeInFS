// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::client::{
    chunk_id_from_seed, payload_len_for_slot, synthetic_payload, total_payload_bytes, ClientError,
    TargetSession,
};
use crate::config::SmokeConfig;
use kp2::{PackedReadQuery, PackedWriteEntry, PackedWriteReply, PackedWriteRequest};

pub(crate) async fn run_smoke(config: SmokeConfig) -> Result<(), ClientError> {
    let session = TargetSession::connect(&config.endpoint).await?;
    let info = session.info().await?;

    let mut write_entries = Vec::with_capacity(config.packed_count);
    let mut query_chunk_ids = Vec::with_capacity(config.packed_count);
    let mut expected_payloads = Vec::with_capacity(config.packed_count);
    for index in 0..config.packed_count {
        let chunk_id = chunk_id_from_seed(config.chunk_seed + index as u64);
        let slot_index = config.slot_index + index as u64;
        let generation = config.generation + index as u32;
        let payload_len = payload_len_for_slot(&info, slot_index)
            .map_err(|err| ClientError::Protocol(err.to_string()))?;
        let payload = synthetic_payload(chunk_id, slot_index, generation, payload_len);
        write_entries.push(PackedWriteEntry {
            chunk_id,
            slot_index,
            generation,
            payload: payload.clone(),
        });
        query_chunk_ids.push(chunk_id);
        expected_payloads.push((chunk_id, slot_index, generation, payload));
    }

    let write_pack = PackedWriteRequest {
        entries: write_entries,
    };
    let write_payload_bytes = total_payload_bytes(&write_pack.entries);
    let write_reply = session.packed_write(write_pack).await?;
    validate_write_reply(&write_reply.value, config.packed_count)?;

    let read_query = PackedReadQuery {
        chunk_ids: query_chunk_ids,
        ranges: None,
    };
    let expected_payload_bytes = expected_payloads
        .iter()
        .map(|(_, _, _, payload)| payload.len())
        .sum();
    let read_reply = session
        .packed_read(&read_query, expected_payload_bytes)
        .await?;
    if read_reply.value.entries.len() != expected_payloads.len() {
        return Err(ClientError::Protocol(
            "KSC packed read response entry count does not match the query".to_string(),
        ));
    }
    for (expected_chunk, expected_slot, expected_generation, expected_payload) in &expected_payloads
    {
        let Some(entry) = read_reply
            .value
            .entries
            .iter()
            .find(|entry| entry.chunk_id == *expected_chunk)
        else {
            return Err(ClientError::Protocol(
                "KSC packed read response is missing an expected chunk".to_string(),
            ));
        };
        let Some(location) = &entry.location else {
            return Err(ClientError::Protocol(
                "KSC packed read response omitted the location record for a successful entry"
                    .to_string(),
            ));
        };
        if entry.status_code != 200
            || location.slot_index != *expected_slot
            || location.generation != *expected_generation
            || entry.payload != *expected_payload
        {
            return Err(ClientError::Protocol(format!(
                "KSC packed read validation failed for chunk {}",
                hex::encode(expected_chunk.0)
            )));
        }
    }

    for chunk_id in &read_query.chunk_ids {
        session.delete_chunk(*chunk_id).await?;
    }

    println!(
        concat!(
            "ksc_smoke_target_id={}\n",
            "ksc_smoke_endpoint={}\n",
            "ksc_smoke_protocol=kp2\n",
            "ksc_smoke_transfer=packed\n",
            "ksc_smoke_chunk_count={}\n",
            "ksc_smoke_total_payload_bytes={}\n",
            "ksc_smoke_result=ok\n"
        ),
        info.target_id,
        config.endpoint,
        config.packed_count,
        write_payload_bytes,
    );
    Ok(())
}

fn validate_write_reply(
    reply: &PackedWriteReply,
    expected_count: usize,
) -> Result<(), ClientError> {
    if reply.entries.len() != expected_count {
        return Err(ClientError::Protocol(
            "KSC packed write reply did not return the expected entry count".to_string(),
        ));
    }
    for entry in &reply.entries {
        if !entry.success() {
            return Err(ClientError::Protocol(format!(
                "KSC packed write entry {} at slot {} generation {} failed: {}",
                hex::encode(entry.chunk_id.0),
                entry.slot_index,
                entry.requested_generation,
                entry.error.clone().unwrap_or_default()
            )));
        }
    }
    Ok(())
}
