// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

//! KMS control-plane client (gRPC via tonic).
//!
//! Ported from `poc/kfc/src/metadata.rs`. Per FIRST_PRINCIPLES §2/§10 this is
//! the *only* gRPC the client speaks, and it talks to KMS (never KAS). Object
//! bytes never travel this path.

use keinctl::proto::kms_client::KmsClient;
use keinctl::proto::{
    BucketRecord, CreateNamespaceEntryRequest, DeleteNamespaceEntryRequest, GetBucketRequest,
    ListChildrenRequest, NamespaceDomainEntry, NamespaceEntryKind, ResolveObjectReadRequest,
};
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const KMS_GRPC_INITIAL_STREAM_WINDOW_BYTES: u32 = 8 * 1024 * 1024;
const KMS_GRPC_INITIAL_CONNECTION_WINDOW_BYTES: u32 = 256 * 1024 * 1024;
const KMS_GRPC_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KMS_GRPC_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

pub type DynError = Box<dyn Error + Send + Sync>;

pub fn boxed_error(message: impl Into<String>) -> DynError {
    Box::new(std::io::Error::other(message.into()))
}

#[derive(Clone)]
pub(crate) struct MetadataClient {
    channels: Arc<Vec<Channel>>,
    next: Arc<AtomicUsize>,
}

impl MetadataClient {
    pub async fn connect(kms_endpoints: &[String]) -> Result<Self, DynError> {
        if kms_endpoints.is_empty() {
            return Err(boxed_error("KFC requires at least one KMS endpoint"));
        }
        let mut channels = Vec::with_capacity(kms_endpoints.len());
        let mut errors = Vec::new();
        for endpoint in kms_endpoints {
            let endpoint = match Endpoint::from_shared(endpoint.clone()) {
                Ok(endpoint) => endpoint
                    .initial_stream_window_size(KMS_GRPC_INITIAL_STREAM_WINDOW_BYTES)
                    .initial_connection_window_size(KMS_GRPC_INITIAL_CONNECTION_WINDOW_BYTES)
                    .http2_keep_alive_interval(KMS_GRPC_KEEPALIVE_INTERVAL)
                    .keep_alive_timeout(KMS_GRPC_KEEPALIVE_TIMEOUT)
                    .keep_alive_while_idle(true),
                Err(err) => {
                    errors.push(format!("{endpoint} invalid: {err}"));
                    continue;
                }
            };
            match endpoint.connect().await {
                Ok(channel) => channels.push(channel),
                Err(err) => errors.push(format!("connect failed: {err}")),
            }
        }
        if channels.is_empty() {
            return Err(boxed_error(format!(
                "KFC could not connect to any KMS endpoint: {}",
                errors.join(" | ")
            )));
        }
        Ok(Self {
            channels: Arc::new(channels),
            next: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn get_bucket(&self, bucket_id: &str) -> Result<BucketRecord, DynError> {
        let mut kms = self.client();
        let reply = rpc_timeout(kms.get_bucket(Request::new(GetBucketRequest {
            bucket_id: bucket_id.to_string(),
        })))
        .await?
        .into_inner();
        reply
            .bucket
            .ok_or_else(|| boxed_error(format!("KFC GetBucket returned no bucket for {bucket_id}")))
    }

    pub async fn bucket_entry_id(&self, bucket_id: &str) -> Result<String, DynError> {
        match self.get_bucket(bucket_id).await {
            Ok(bucket) if !bucket.bucket_entry_id.is_empty() => Ok(bucket.bucket_entry_id),
            Ok(_) => Ok(format!("bucket::{bucket_id}")),
            Err(err) => {
                let message = err.to_string().to_ascii_lowercase();
                if message.contains("getbucket")
                    && (message.contains("not implemented") || message.contains("not supported"))
                {
                    Ok(format!("bucket::{bucket_id}"))
                } else {
                    Err(err)
                }
            }
        }
    }

    pub async fn list_children_all(
        &self,
        namespace_id: &str,
        parent_entry_id: &str,
        limit: u32,
    ) -> Result<Vec<NamespaceDomainEntry>, DynError> {
        let mut cursor = String::new();
        let mut entries = Vec::new();
        loop {
            let mut kms = self.client();
            let reply = rpc_timeout(kms.list_children(Request::new(ListChildrenRequest {
                namespace_id: namespace_id.to_string(),
                parent_entry_id: parent_entry_id.to_string(),
                cursor: cursor.clone(),
                limit,
            })))
            .await?
            .into_inner();
            let next_cursor = reply.next_cursor.clone();
            entries.extend(reply.entries);
            if next_cursor.is_empty() {
                break;
            }
            cursor = next_cursor;
        }
        Ok(entries)
    }

    /// Create a collection (directory) namespace entry as a child of
    /// `parent_entry_id`. The recursive child listing surfaces it naturally on
    /// the next `list_children`.
    pub async fn create_collection(
        &self,
        namespace_id: &str,
        parent_entry_id: &str,
        entry_id: &str,
        name: &str,
    ) -> Result<NamespaceDomainEntry, DynError> {
        let mut kms = self.client();
        let reply = rpc_timeout(kms.create_namespace_entry(Request::new(
            CreateNamespaceEntryRequest {
                entry: Some(NamespaceDomainEntry {
                    entry_id: entry_id.to_string(),
                    namespace_id: namespace_id.to_string(),
                    parent_entry_id: parent_entry_id.to_string(),
                    name: name.to_string(),
                    kind: NamespaceEntryKind::Collection as i32,
                    path: String::new(),
                    size_bytes: 0,
                }),
            },
        )))
        .await?
        .into_inner();
        reply.entry.ok_or_else(|| {
            boxed_error(format!(
                "KFC CreateNamespaceEntry returned no entry for {namespace_id}/{parent_entry_id}/{name}"
            ))
        })
    }

    pub async fn delete_namespace_entry(
        &self,
        namespace_id: &str,
        entry_id: &str,
    ) -> Result<NamespaceDomainEntry, DynError> {
        let mut kms = self.client();
        let reply = rpc_timeout(kms.delete_namespace_entry(Request::new(
            DeleteNamespaceEntryRequest {
                namespace_id: namespace_id.to_string(),
                entry_id: entry_id.to_string(),
            },
        )))
        .await?
        .into_inner();
        reply.entry.ok_or_else(|| {
            boxed_error(format!(
                "KFC DeleteNamespaceEntry returned no entry for {namespace_id}/{entry_id}"
            ))
        })
    }

    pub async fn resolve_object_size(&self, bucket_id: &str, key: &str) -> Result<u64, DynError> {
        let mut kms = self.client();
        let reply = rpc_timeout(kms.resolve_object_read(Request::new(ResolveObjectReadRequest {
            bucket_id: bucket_id.to_string(),
            key: key.to_string(),
        })))
        .await?
        .into_inner();
        let manifest = reply.manifest.ok_or_else(|| {
            boxed_error(format!(
                "KFC ResolveObjectRead returned no manifest for {bucket_id}/{key}"
            ))
        })?;
        Ok(manifest.logical_length_bytes)
    }

    fn client(&self) -> KmsClient<Channel> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.channels.len();
        KmsClient::new(self.channels[index].clone())
    }
}

async fn rpc_timeout<T, F>(future: F) -> Result<T, DynError>
where
    F: std::future::Future<Output = Result<T, tonic::Status>>,
{
    timeout(CONTROL_RPC_TIMEOUT, future)
        .await
        .map_err(|_| boxed_error("KFC control RPC timed out"))?
        .map_err(|err| boxed_error(err.to_string()))
}
