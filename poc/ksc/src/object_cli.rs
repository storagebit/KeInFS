// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::config::{
    CompletionMode as CliCompletionMode, ObjectDeleteConfig, ObjectGetConfig, ObjectPutConfig,
};
use ksc::client::CompletionMode as ClientCompletionMode;
use ksc::object::{
    delete_object_with_options, get_object_range_with_options,
    get_object_single_stripe_with_options, put_object_from_path_with_options, ObjectClientOptions,
    ObjectPhaseTimes,
};

pub(crate) async fn run_put_object(
    config: ObjectPutConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let logical_length_bytes = std::fs::metadata(&config.input_path)?.len();
    let result = put_object_from_path_with_options(
        &config.kms_endpoints,
        &config.bucket_id,
        &config.key,
        &config.input_path,
        ObjectClientOptions {
            read_completion_mode: client_mode(config.write_completion_mode),
            write_completion_mode: client_mode(config.write_completion_mode),
            write_window_max_stripes: config.write_window_max_stripes,
            write_window_inflight_stripes: config.write_window_inflight_stripes,
            kms_grpc_max_message_bytes: config.kms_grpc_max_message_bytes,
            metadata_notification_nats_url: config.metadata_notification_nats_url.clone(),
            metadata_notification_subject: config.metadata_notification_subject.clone(),
            ..ObjectClientOptions::default()
        },
    )
    .await?;
    println!(
        "ksc_object_put bucket={} key={} logical_bytes={} version_id={} intent_id={} stripes={} fragments={}",
        config.bucket_id,
        config.key,
        logical_length_bytes,
        result.manifest.version_id,
        result.intent.intent_id,
        result.manifest.stripes.len(),
        result
            .manifest
            .stripes
            .iter()
            .map(|stripe| stripe.fragments.len())
            .sum::<usize>()
    );
    print_phase_line("ksc_object_put_phases_us", result.phases);
    Ok(())
}

pub(crate) async fn run_get_object(
    config: ObjectGetConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    if config.range_offset.is_some() != config.range_length.is_some() {
        return Err("ksc get-object: pass both --offset and --length, or neither".into());
    }
    if let (Some(offset), Some(length)) = (config.range_offset, config.range_length) {
        let result = get_object_range_with_options(
            &config.kms_endpoints,
            &config.bucket_id,
            &config.key,
            offset,
            length,
            ObjectClientOptions {
                read_completion_mode: client_mode(config.read_completion_mode),
                write_completion_mode: client_mode(config.read_completion_mode),
                kms_grpc_max_message_bytes: config.kms_grpc_max_message_bytes,
                metadata_notification_nats_url: config.metadata_notification_nats_url.clone(),
                metadata_notification_subject: config.metadata_notification_subject.clone(),
                ..ObjectClientOptions::default()
            },
        )
        .await?;
        std::fs::write(&config.output_path, &result.payload)?;
        println!(
            "ksc_object_get_range bucket={} key={} offset={} length={} payload_bytes={} version_id={} output={}",
            config.bucket_id,
            config.key,
            offset,
            length,
            result.payload.len(),
            result.manifest.version_id,
            config.output_path.display()
        );
        print_phase_line("ksc_object_get_phases_us", result.phases);
        return Ok(());
    }
    let result = get_object_single_stripe_with_options(
        &config.kms_endpoints,
        &config.bucket_id,
        &config.key,
        ObjectClientOptions {
            read_completion_mode: client_mode(config.read_completion_mode),
            write_completion_mode: client_mode(config.read_completion_mode),
            kms_grpc_max_message_bytes: config.kms_grpc_max_message_bytes,
            metadata_notification_nats_url: config.metadata_notification_nats_url.clone(),
            metadata_notification_subject: config.metadata_notification_subject.clone(),
            ..ObjectClientOptions::default()
        },
    )
    .await?;
    std::fs::write(&config.output_path, &result.payload)?;
    println!(
        "ksc_object_get bucket={} key={} logical_bytes={} version_id={} missing_fragments={} output={}",
        config.bucket_id,
        config.key,
        result.payload.len(),
        result.manifest.version_id,
        result.missing_fragments,
        config.output_path.display()
    );
    print_phase_line("ksc_object_get_phases_us", result.phases);
    Ok(())
}

pub(crate) async fn run_delete_object(
    config: ObjectDeleteConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = delete_object_with_options(
        &config.kms_endpoints,
        &config.bucket_id,
        &config.key,
        &config.version_ids,
        ObjectClientOptions {
            read_completion_mode: client_mode(config.write_completion_mode),
            write_completion_mode: client_mode(config.write_completion_mode),
            kms_grpc_max_message_bytes: config.kms_grpc_max_message_bytes,
            metadata_notification_nats_url: config.metadata_notification_nats_url.clone(),
            metadata_notification_subject: config.metadata_notification_subject.clone(),
            ..ObjectClientOptions::default()
        },
    )
    .await?;
    let deleted_versions = result
        .deleted_versions
        .iter()
        .map(|version| version.version_id.clone())
        .collect::<Vec<_>>();
    println!(
        "ksc_object_delete bucket={} key={} deleted_versions={} fragment_delete_attempts={} fragment_delete_successes={} reclaimed_granules={} cleanup_complete={}",
        config.bucket_id,
        config.key,
        deleted_versions.join(","),
        result.fragment_delete_attempts,
        result.fragment_delete_successes,
        result.reclaimed_granules,
        result.cleanup_complete
    );
    Ok(())
}

fn client_mode(mode: CliCompletionMode) -> ClientCompletionMode {
    match mode {
        CliCompletionMode::Interrupt => ClientCompletionMode::Interrupt,
        CliCompletionMode::HotPoll => ClientCompletionMode::HotPoll,
    }
}

pub(crate) fn print_phase_line(prefix: &str, phases: ObjectPhaseTimes) {
    println!(
        "{} kms_initiate={} kms_commit={} kms_resolve={} ec_encode={} ec_reconstruct={} target_connect={} target_write={} target_read={} target_ready_wait={} target_request_prepare={} target_send_headers={} target_send_body={} target_wait_response={} target_collect_response={} target_protocol_decode={} target_payload_validate={}",
        prefix,
        phases.kms_initiate.as_micros(),
        phases.kms_commit.as_micros(),
        phases.kms_resolve.as_micros(),
        phases.ec_encode.as_micros(),
        phases.ec_reconstruct.as_micros(),
        phases.target_connect.as_micros(),
        phases.target_write.as_micros(),
        phases.target_read.as_micros(),
        phases.target_ready_wait.as_micros(),
        phases.target_request_prepare.as_micros(),
        phases.target_send_headers.as_micros(),
        phases.target_send_body.as_micros(),
        phases.target_wait_response.as_micros(),
        phases.target_collect_response.as_micros(),
        phases.target_protocol_decode.as_micros(),
        phases.target_payload_validate.as_micros(),
    );
}
