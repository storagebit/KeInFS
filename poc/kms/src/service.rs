// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::hot_store::HotMetadataStore;
use crate::read_cache::ResolveObjectReadCache;
use crate::stats::{KmsStats, RpcKind};
use crate::store::{
    build_finalize_plans, normalize_object_key, BucketWriteContext, CommittedObjectWrite,
    CommittedObjectWriteWindow, DeletedObjectVersion as StoreDeletedObjectVersion, KmsStore,
    ReservationFinalizePlan, ReservedObjectWriteWindow, StorePhaseTiming, TimedStoreResult,
};
use crate::watch::NotificationHub;
use async_stream::try_stream;
use bytes::Bytes;
use h2::{client::SendRequest, RecvStream};
use http::header::CONTENT_LENGTH;
use http::{Method, Request as HttpRequest, StatusCode, Uri};
use keinctl::proto::kas_client::KasClient;
use keinctl::proto::kms_server::Kms;
use keinctl::proto::{
    AbortObjectWriteReply, AbortObjectWriteRequest, BeginObjectReply, BeginObjectRequest,
    CommitObjectReply, CommitObjectRequest, CommitObjectWriteReply,
    CommitObjectWriteRequest, CommitObjectWriteWindowReply, CommitObjectWriteWindowRequest,
    CommitPlacementTaskReply, CommitPlacementTaskRequest, CommitRebuildReply, CommitRebuildRequest,
    CreateBucketReply, CreateBucketRequest, CreateEcProfileReply, CreateEcProfileRequest,
    CreateNamespaceEntryReply, CreateNamespaceEntryRequest, CreateNamespaceReply,
    CreateNamespaceRequest, DeleteNamespaceEntryReply, DeleteNamespaceEntryRequest,
    DeleteObjectReply, DeleteObjectRequest, DeletedObjectVersion, DrainTargetReply,
    DrainTargetRequest, EcProfile, EnqueueTargetRebalanceReply, EnqueueTargetRebalanceRequest,
    FailPlacementTaskReply, FailPlacementTaskRequest, FinalizeReservationsBatchRequest,
    GetBucketReply, GetBucketRequest, GetNamespaceReply, GetNamespaceRequest,
    GetPlacementTaskReply, GetPlacementTaskRequest, GetTargetPlacementStatusReply,
    GetTargetPlacementStatusRequest, GetWriteIntentReply, GetWriteIntentRequest,
    InitiateObjectWriteReply, InitiateObjectWriteRequest, LeasePlacementTasksReply,
    LeasePlacementTasksRequest, LeaseRebuildTasksReply, LeaseRebuildTasksRequest, ListBucketsReply,
    ListBucketsRequest, ListChildrenReply, ListChildrenRequest, ListEcProfilesReply,
    ListEcProfilesRequest, ListMetadataEventsReply, ListMetadataEventsRequest, ListNamespacesReply,
    ListNamespacesRequest, ListPlacementTasksReply, ListPlacementTasksRequest,
    ListServiceInstancesRequest, ListTargetsRequest, ListWriteIntentsReply,
    ListWriteIntentsRequest, MetadataEventKind, MetadataInvalidationEvent, PlacementReservation,
    PlacementReservationRecord, PlacementTaskKind, PlacementTaskState, PreviewTargetRebalanceReply,
    PreviewTargetRebalanceRequest, ReclaimTargetGranulesRequest, RecoverTargetReply,
    RecoverTargetRequest, ReleaseReservationsBatchRequest, RepairObjectWriteReply,
    RepairObjectWriteRequest, ReportTargetFailureReply, ReportTargetFailureRequest,
    ReservationMutation, ReserveObjectWriteWindowReply, ReserveObjectWriteWindowRequest,
    ReserveReplacementPlacementRequest, ReserveStripeBatchRequest, ResolveObjectReadReply,
    ResolveObjectReadRequest, ResolvePathReply, ResolvePathRequest, ResolveShardReply,
    ResolveShardRequest, RetireTargetReply, RetireTargetRequest, ServiceKind,
    SetTargetStateRequest, TargetGranule, TargetLifecycleState, WatchEntryReply, WatchEntryRequest,
    WatchPrefixReply, WatchPrefixRequest, WriteIntent, WriteIntentState,
};
use keinctl::ProfileError;
use serde::Deserialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{sleep, timeout};
use tokio_stream::Stream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use uuid::Uuid;

type ReplyStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

const TARGET_DELETE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TARGET_RECOVER_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const TARGET_DELETE_MAX_FRAME_BYTES: u32 = 1024 * 1024;
const TARGET_DELETE_MAX_CONCURRENT_STREAMS: u32 = 256;
const TARGET_DELETE_INITIAL_WINDOW_BYTES: u32 = 8 * 1024 * 1024;
const TARGET_DELETE_INITIAL_CONNECTION_WINDOW_BYTES: u32 = 256 * 1024 * 1024;
const KAS_GRPC_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone)]
struct TargetDeleteSession {
    endpoint: String,
    client: SendRequest<Bytes>,
}

#[derive(Debug, Deserialize)]
struct DeleteChunkDocument {
    deleted: bool,
}

#[derive(Default)]
struct DeleteCleanupResult {
    fragment_delete_attempts: u64,
    fragment_delete_successes: u64,
    reclaimed_granules: u64,
    cleanup_complete: bool,
    granules: Vec<TargetGranule>,
}

#[derive(Clone)]
pub(crate) struct KmsService {
    pub(crate) store: KmsStore,
    pub(crate) hot_store: Arc<dyn HotMetadataStore>,
    pub(crate) notifications: NotificationHub,
    pub(crate) read_cache: ResolveObjectReadCache,
    pub(crate) kas_channels: KasEndpointBalancer,
    pub(crate) stats: Arc<KmsStats>,
    pub(crate) write_intent_ttl: Duration,
    pub(crate) reservation_finalizer_grace: Duration,
    pub(crate) large_write_initiate_gate: Arc<Semaphore>,
    pub(crate) reservation_cache: ReservationCache,
    pub(crate) route_cache: AllocationRouteCache,
    pub(crate) write_profile_max_stripes: usize,
    pub(crate) write_profile_min_fragment_bytes: u32,
    pub(crate) reservation_mutation_batch_size: usize,
    pub(crate) reservation_mutation_dispatcher: ReservationMutationDispatcher,
    pub(crate) kas_rpc_timeout: Duration,
    pub(crate) kas_reserve_attempt_timeout: Duration,
    pub(crate) bucket_write_contexts: Arc<Mutex<HashMap<String, BucketWriteContext>>>,
    pub(crate) ec_profile_catalog: Arc<Mutex<Option<Vec<EcProfile>>>>,
    pub(crate) object_parent_contexts:
        Arc<Mutex<HashMap<ObjectParentCacheKey, ObjectParentContext>>>,
}

#[derive(Clone)]
pub(crate) struct ReservationMutationDispatcher {
    sender: mpsc::UnboundedSender<ReservationMutationWork>,
}

#[derive(Clone)]
pub(crate) struct ReservationMutationWork {
    pub(crate) intent_id: Option<String>,
    pub(crate) finalize_plans: Vec<ReservationFinalizePlan>,
    pub(crate) reservation_ids: Vec<String>,
}

impl ReservationMutationDispatcher {
    pub(crate) fn new(sender: mpsc::UnboundedSender<ReservationMutationWork>) -> Self {
        Self { sender }
    }

    pub(crate) fn dispatch(
        &self,
        work: ReservationMutationWork,
    ) -> Result<(), ReservationMutationWork> {
        self.sender.send(work).map_err(|err| err.0)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReservationCacheConfig {
    pub(crate) high_watermark: usize,
    pub(crate) low_watermark: usize,
    pub(crate) refill_batch: usize,
    pub(crate) reservation_ttl: Duration,
    pub(crate) min_usable_ttl: Duration,
    pub(crate) refill_concurrency: usize,
    pub(crate) wait_timeout: Duration,
    pub(crate) stale_refill_after: Duration,
    pub(crate) small_object_max_stripes: usize,
    pub(crate) single_window_seed_batch: usize,
    pub(crate) initiate_write_window_max_stripes: usize,
}

#[derive(Clone)]
pub(crate) struct KasEndpoint {
    pub(crate) endpoint: String,
    pub(crate) channel: Channel,
}

#[derive(Clone)]
pub(crate) struct KasEndpointBalancer {
    endpoints: Arc<Vec<KasEndpoint>>,
    next: Arc<AtomicUsize>,
}

impl KasEndpointBalancer {
    pub(crate) fn new(endpoints: Vec<KasEndpoint>) -> Self {
        Self {
            endpoints: Arc::new(endpoints),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }

    pub(crate) fn ordered_endpoints(&self) -> Vec<KasEndpoint> {
        let len = self.endpoints.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % len;
        (0..len)
            .map(|offset| self.endpoints[(start + offset) % len].clone())
            .collect()
    }

    pub(crate) fn ordered_channels(&self) -> Vec<Channel> {
        self.ordered_endpoints()
            .into_iter()
            .map(|endpoint| endpoint.channel)
            .collect()
    }

    pub(crate) fn channel_for_endpoint(&self, endpoint: &str) -> Option<Channel> {
        let normalized_endpoint = normalize_service_endpoint(endpoint);
        self.endpoints
            .iter()
            .find(|entry| normalize_service_endpoint(&entry.endpoint) == normalized_endpoint)
            .map(|entry| entry.channel.clone())
    }

    pub(crate) fn client(&self) -> KasClient<Channel> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.endpoints.len();
        KasClient::new(self.endpoints[index].channel.clone())
            .max_decoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES)
    }
}

#[tonic::async_trait]
impl Kms for KmsService {
    type WatchEntryStream = ReplyStream<WatchEntryReply>;
    type WatchPrefixStream = ReplyStream<WatchPrefixReply>;

    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceReply>, Status> {
        let kind = RpcKind::CreateNamespace;
        let started = Instant::now();
        self.stats.record_request(kind);
        let phase_started = Instant::now();
        let namespace = request
            .into_inner()
            .namespace
            .ok_or_else(|| Status::invalid_argument("KMS CreateNamespace requires namespace"))?;
        self.stats
            .record_phase(kind, "request_decode", phase_started.elapsed());
        let phase_started = Instant::now();
        match self.store.create_namespace(namespace).await {
            Ok((namespace, shard_map)) => {
                self.notifications.notify(namespace_invalidation_event(
                    namespace.namespace_id.clone(),
                    MetadataEventKind::NamespaceCreated,
                ));
                self.stats
                    .record_phase(kind, "store_create_namespace", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(CreateNamespaceReply {
                    namespace: Some(namespace),
                    shard_map: Some(shard_map),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_create_namespace", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn list_namespaces(
        &self,
        request: Request<ListNamespacesRequest>,
    ) -> Result<Response<ListNamespacesReply>, Status> {
        let kind = RpcKind::ListNamespaces;
        let started = Instant::now();
        self.stats.record_request(kind);
        let tenant_id = request.into_inner().tenant_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .list_namespaces((!tenant_id.is_empty()).then_some(tenant_id.as_str()))
            .await
        {
            Ok(namespaces) => {
                self.stats
                    .record_phase(kind, "store_list_namespaces", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ListNamespacesReply { namespaces }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_list_namespaces", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn get_namespace(
        &self,
        request: Request<GetNamespaceRequest>,
    ) -> Result<Response<GetNamespaceReply>, Status> {
        let kind = RpcKind::GetNamespace;
        let started = Instant::now();
        self.stats.record_request(kind);
        let namespace_id = request.into_inner().namespace_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.store.get_namespace(namespace_id).await {
            Ok((namespace, shard_map)) => {
                self.stats
                    .record_phase(kind, "store_get_namespace", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(GetNamespaceReply {
                    namespace: Some(namespace),
                    shard_map: Some(shard_map),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_get_namespace", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn create_namespace_entry(
        &self,
        request: Request<CreateNamespaceEntryRequest>,
    ) -> Result<Response<CreateNamespaceEntryReply>, Status> {
        let kind = RpcKind::CreateNamespaceEntry;
        let started = Instant::now();
        self.stats.record_request(kind);
        let phase_started = Instant::now();
        let entry = request
            .into_inner()
            .entry
            .ok_or_else(|| Status::invalid_argument("KMS CreateNamespaceEntry requires entry"))?;
        self.stats
            .record_phase(kind, "request_decode", phase_started.elapsed());
        let phase_started = Instant::now();
        match self.store.create_namespace_entry(entry).await {
            Ok(entry) => {
                self.notifications.notify(entry_invalidation_event(
                    &entry,
                    MetadataEventKind::Unspecified,
                ));
                self.stats.record_phase(
                    kind,
                    "store_create_namespace_entry",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(CreateNamespaceEntryReply {
                    entry: Some(entry),
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_create_namespace_entry",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn delete_namespace_entry(
        &self,
        request: Request<DeleteNamespaceEntryRequest>,
    ) -> Result<Response<DeleteNamespaceEntryReply>, Status> {
        let kind = RpcKind::CreateNamespaceEntry;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .delete_namespace_entry(request.namespace_id, request.entry_id)
            .await
        {
            Ok(entry) => {
                self.notifications.notify(entry_invalidation_event(
                    &entry,
                    MetadataEventKind::EntryCreated,
                ));
                self.stats.record_phase(
                    kind,
                    "store_delete_namespace_entry",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(DeleteNamespaceEntryReply {
                    entry: Some(entry),
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_delete_namespace_entry",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn create_ec_profile(
        &self,
        request: Request<CreateEcProfileRequest>,
    ) -> Result<Response<CreateEcProfileReply>, Status> {
        let kind = RpcKind::CreateEcProfile;
        let started = Instant::now();
        self.stats.record_request(kind);
        let phase_started = Instant::now();
        let profile = match request.into_inner().profile {
            Some(profile) => profile,
            None => {
                return kms_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::invalid_argument("KMS CreateEcProfile requires profile"),
                );
            }
        };
        self.stats
            .record_phase(kind, "request_decode", phase_started.elapsed());
        let phase_started = Instant::now();
        if let Err(err) = profile
            .validate_single_stripe_lab()
            .map_err(map_profile_error)
        {
            return kms_err(&self.stats, kind, &started, err);
        }
        self.stats
            .record_phase(kind, "profile_validate", phase_started.elapsed());
        let phase_started = Instant::now();
        match self.store.create_ec_profile(profile).await {
            Ok(profile) => {
                self.stats
                    .record_phase(kind, "store_create_ec_profile", phase_started.elapsed());
                self.clear_ec_profile_catalog();
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(CreateEcProfileReply {
                    profile: Some(profile),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_create_ec_profile", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn list_ec_profiles(
        &self,
        _request: Request<ListEcProfilesRequest>,
    ) -> Result<Response<ListEcProfilesReply>, Status> {
        let kind = RpcKind::ListEcProfiles;
        let started = Instant::now();
        self.stats.record_request(kind);
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.store.list_ec_profiles().await {
            Ok(profiles) => {
                self.stats
                    .record_phase(kind, "store_list_ec_profiles", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ListEcProfilesReply { profiles }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_list_ec_profiles", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketReply>, Status> {
        let kind = RpcKind::CreateBucket;
        let started = Instant::now();
        self.stats.record_request(kind);
        let phase_started = Instant::now();
        let bucket = match request.into_inner().bucket {
            Some(bucket) => bucket,
            None => {
                return kms_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::invalid_argument("KMS CreateBucket requires bucket"),
                );
            }
        };
        self.stats
            .record_phase(kind, "request_decode", phase_started.elapsed());
        let phase_started = Instant::now();
        match self.store.create_bucket(bucket).await {
            Ok((bucket, profile)) => {
                self.stats
                    .record_phase(kind, "store_create_bucket", phase_started.elapsed());
                let phase_started = Instant::now();
                if let Err(err) = profile
                    .validate_single_stripe_lab()
                    .map_err(map_profile_error)
                {
                    return kms_err(&self.stats, kind, &started, err);
                }
                self.stats
                    .record_phase(kind, "profile_validate", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(CreateBucketReply {
                    bucket: Some(bucket),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_create_bucket", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn get_bucket(
        &self,
        request: Request<GetBucketRequest>,
    ) -> Result<Response<GetBucketReply>, Status> {
        let kind = RpcKind::GetBucket;
        let started = Instant::now();
        self.stats.record_request(kind);
        let bucket_id = request.into_inner().bucket_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.store.get_bucket(bucket_id).await {
            Ok((bucket, ec_profile)) => {
                self.stats
                    .record_phase(kind, "store_get_bucket", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(GetBucketReply {
                    bucket: Some(bucket),
                    ec_profile: Some(ec_profile),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_get_bucket", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn list_buckets(
        &self,
        request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsReply>, Status> {
        let kind = RpcKind::ListBuckets;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .list_buckets(
                (!request.namespace_id.is_empty()).then_some(request.namespace_id.as_str()),
                (!request.parent_entry_id.is_empty()).then_some(request.parent_entry_id.as_str()),
            )
            .await
        {
            Ok(buckets) => {
                self.stats
                    .record_phase(kind, "store_list_buckets", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ListBucketsReply { buckets }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_list_buckets", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn resolve_path(
        &self,
        request: Request<ResolvePathRequest>,
    ) -> Result<Response<ResolvePathReply>, Status> {
        let kind = RpcKind::ResolvePath;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .resolve_path(request.namespace_id, request.path)
            .await
        {
            Ok(reply) => {
                self.stats
                    .record_phase(kind, "store_resolve_path", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(reply))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_resolve_path", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn list_children(
        &self,
        request: Request<ListChildrenRequest>,
    ) -> Result<Response<ListChildrenReply>, Status> {
        let kind = RpcKind::ListChildren;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .list_children(
                request.namespace_id,
                request.parent_entry_id,
                request.cursor,
                request.limit,
            )
            .await
        {
            Ok((entries, next_cursor, current_revision)) => {
                self.stats
                    .record_phase(kind, "store_list_children", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ListChildrenReply {
                    entries,
                    next_cursor,
                    current_revision,
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_list_children", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn watch_entry(
        &self,
        request: Request<WatchEntryRequest>,
    ) -> Result<Response<Self::WatchEntryStream>, Status> {
        let kind = RpcKind::WatchEntry;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let entry_id = request.entry_id;
        let mut last_revision = request.start_revision;
        let notifications = self.notifications.subscribe();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        self.stats.record_success(kind, started.elapsed());
        let store = self.store.clone();
        let stats = self.stats.clone();
        let stream = try_stream! {
            let mut signal = notifications;
            loop {
                let phase_started = Instant::now();
                let events = store.read_entry_events(&entry_id, last_revision, 128).await?;
                stats.record_phase(kind, "store_watch_entry", phase_started.elapsed());
                if !events.is_empty() {
                    for event in events {
                        last_revision = event.revision;
                        yield WatchEntryReply { event: Some(event) };
                    }
                    continue;
                }
                let _ = timeout(Duration::from_secs(1), signal.changed()).await;
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn watch_prefix(
        &self,
        request: Request<WatchPrefixRequest>,
    ) -> Result<Response<Self::WatchPrefixStream>, Status> {
        let kind = RpcKind::WatchPrefix;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let namespace_id = request.namespace_id;
        let parent_entry_id = request.parent_entry_id;
        let mut last_revision = request.start_revision;
        let notifications = self.notifications.subscribe();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        self.stats.record_success(kind, started.elapsed());
        let store = self.store.clone();
        let stats = self.stats.clone();
        let stream = try_stream! {
            let mut signal = notifications;
            loop {
                let phase_started = Instant::now();
                let events = store
                    .read_prefix_events(&namespace_id, &parent_entry_id, last_revision, 128)
                    .await?;
                stats.record_phase(kind, "store_watch_prefix", phase_started.elapsed());
                if !events.is_empty() {
                    for event in events {
                        last_revision = event.revision;
                        yield WatchPrefixReply { event: Some(event) };
                    }
                    continue;
                }
                let _ = timeout(Duration::from_secs(1), signal.changed()).await;
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn resolve_shard(
        &self,
        request: Request<ResolveShardRequest>,
    ) -> Result<Response<ResolveShardReply>, Status> {
        let kind = RpcKind::ResolveShard;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.store.resolve_shard(request.namespace_id).await {
            Ok(shard_map) => {
                self.stats
                    .record_phase(kind, "store_resolve_shard", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ResolveShardReply {
                    shard_map: Some(shard_map),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_resolve_shard", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn initiate_object_write(
        &self,
        request: Request<InitiateObjectWriteRequest>,
    ) -> Result<Response<InitiateObjectWriteReply>, Status> {
        let kind = RpcKind::InitiateObjectWrite;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let phase_started = Instant::now();
        let bucket_context = match self
            .load_bucket_write_context(kind, &request.bucket_id)
            .await
        {
            Ok(values) => values,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        self.stats
            .record_phase(kind, "bucket_context_load", phase_started.elapsed());
        let BucketWriteContext {
            bucket,
            bucket_entry,
            ec_profile: default_ec_profile,
        } = bucket_context;
        let phase_started = Instant::now();
        let ec_profile = match self
            .select_write_ec_profile(kind, &default_ec_profile, request.logical_length_bytes)
            .await
        {
            Ok(profile) => profile,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        self.stats
            .record_phase(kind, "profile_select", phase_started.elapsed());
        let stripe_logical_bytes =
            u64::from(ec_profile.data_fragments) * u64::from(ec_profile.fragment_bytes);
        let stripe_count = request.logical_length_bytes.div_ceil(stripe_logical_bytes);
        let stripe_count = match usize::try_from(stripe_count) {
            Ok(value) => value,
            Err(_) => {
                return kms_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::invalid_argument(format!(
                        "logical length {} requires more stripes than this KMS process can track",
                        request.logical_length_bytes
                    )),
                );
            }
        };
        let _large_write_initiate_permit = if stripe_count
            > self
                .reservation_cache
                .config
                .initiate_write_window_max_stripes
        {
            let phase_started = Instant::now();
            let permit = self
                .large_write_initiate_gate
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Status::internal("large write initiate gate closed"))?;
            self.stats.record_phase(
                kind,
                "large_write_initiate_gate_wait",
                phase_started.elapsed(),
            );
            Some(permit)
        } else {
            None
        };
        let phase_started = Instant::now();
        let normalized_request_key = match normalize_object_key(&request.key) {
            Ok(value) => value,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        self.stats
            .record_phase(kind, "key_normalize", phase_started.elapsed());
        let phase_started = Instant::now();
        if let Err(err) = ec_profile
            .validate_single_stripe_lab()
            .map_err(map_profile_error)
        {
            return kms_err(&self.stats, kind, &started, err);
        }
        self.stats
            .record_phase(kind, "profile_validate", phase_started.elapsed());

        let parent_hint = if !normalized_request_key.contains('/') {
            self.stats
                .record_phase(kind, "object_parent_root_fast_path", Duration::ZERO);
            Some(ObjectParentContext {
                parent_entry_id: bucket.bucket_entry_id.clone(),
                parent_path: bucket_entry.path.clone(),
            })
        } else if let Some(parent) =
            self.lookup_object_parent(&bucket.bucket_id, &normalized_request_key)
        {
            self.stats
                .record_phase(kind, "object_parent_cache_hit", Duration::ZERO);
            Some(parent)
        } else {
            self.stats
                .record_phase(kind, "object_parent_cache_miss", Duration::ZERO);
            None
        };

        let intent = WriteIntent {
            intent_id: Uuid::new_v4().to_string(),
            version_id: Uuid::new_v4().to_string(),
            bucket_id: bucket.bucket_id.clone(),
            key: normalized_request_key.clone(),
            logical_length_bytes: request.logical_length_bytes,
            ec_profile_id: ec_profile.id.clone(),
            stripe_count: stripe_count as u32,
            fragment_plans: Vec::new(),
            expires_at_unix_ms: now_unix_ms()
                .saturating_add(self.write_intent_ttl.as_millis() as u64),
            state: WriteIntentState::Reserved as i32,
            reservation_id: String::new(),
            namespace_id: bucket.namespace_id.clone(),
            object_entry_id: String::new(),
            bucket_entry_id: bucket.bucket_entry_id.clone(),
            reservation_ids: Vec::new(),
            fragment_status: Vec::new(),
            reservations_finalized: false,
            parent_entry_id: String::new(),
            parent_path: String::new(),
        };
        let phase_started = Instant::now();
        match self
            .hot_store
            .prepare_and_create_write_intent(
                intent,
                bucket.bucket_entry_id.clone(),
                bucket_entry.path.clone(),
                parent_hint
                    .as_ref()
                    .map(|parent| (parent.parent_entry_id.clone(), parent.parent_path.clone())),
            )
            .await
        {
            Ok(TimedStoreResult {
                value: intent,
                phase_timings,
            }) => {
                self.stats.record_phase(
                    kind,
                    "store_initiate_write_total",
                    phase_started.elapsed(),
                );
                record_store_phase_timings(
                    &self.stats,
                    kind,
                    "store_initiate_write",
                    &phase_timings,
                );
                self.remember_object_parent(
                    &bucket.bucket_id,
                    &intent.key,
                    &intent.parent_entry_id,
                    &intent.parent_path,
                );
                let initial_window_stripe_count = capped_initial_write_window_stripe_count(
                    request.initial_window_stripe_count as usize,
                    stripe_count,
                    self.reservation_cache
                        .config
                        .initiate_write_window_max_stripes,
                );
                let initial_fragment_plans = if initial_window_stripe_count > 0 {
                    match self
                        .reserve_write_window_with_cache(
                            kind,
                            &intent,
                            &ec_profile,
                            0,
                            initial_window_stripe_count,
                        )
                        .await
                    {
                        Ok(fragment_plans) => fragment_plans,
                        Err(err) => {
                            self.abort_failed_write_intent_init(&intent.intent_id).await;
                            return kms_err(&self.stats, kind, &started, err);
                        }
                    }
                } else {
                    Vec::new()
                };
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(InitiateObjectWriteReply {
                    intent: Some(intent),
                    ec_profile: Some(ec_profile),
                    initial_fragment_plans,
                    initial_window_stripe_count: initial_window_stripe_count as u32,
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_initiate_write_total",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn begin_object(
        &self,
        request: Request<BeginObjectRequest>,
    ) -> Result<Response<BeginObjectReply>, Status> {
        let request = request.into_inner();
        // Mint the monotonic object_id + numeric version up front.
        let (object_id, version) = self
            .hot_store
            .mint_object_id(&request.bucket_id, &request.key)
            .await?;
        // EC profile is the bucket's immutable binding.
        let ec_profile = self
            .hot_store
            .get_bucket_write_context(request.bucket_id.clone())
            .await?
            .ec_profile;
        Ok(Response::new(BeginObjectReply {
            object_id,
            version,
            ec_profile: Some(ec_profile),
            topology_epoch: 0,
        }))
    }

    async fn commit_object(
        &self,
        _request: Request<CommitObjectRequest>,
    ) -> Result<Response<CommitObjectReply>, Status> {
        // Single-shot commit (manifest + per-target reverse log + CAS head flip) — not yet implemented.
        Err(Status::unimplemented("KMS CommitObject is not yet implemented"))
    }

    async fn commit_object_write(
        &self,
        request: Request<CommitObjectWriteRequest>,
    ) -> Result<Response<CommitObjectWriteReply>, Status> {
        let kind = RpcKind::CommitObjectWrite;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let intent_id = request.intent_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .hot_store
            .commit_object_write(
                intent_id,
                request.successful_fragments,
                now_unix_ms().saturating_add(self.reservation_finalizer_grace.as_millis() as u64),
            )
            .await
        {
            Ok(TimedStoreResult {
                value:
                    CommittedObjectWrite {
                        intent_id,
                        manifest,
                        reservation_ids,
                        finalize_plans,
                    },
                phase_timings,
            }) => {
                invalidate_local_object_cache(&self.read_cache, &manifest.bucket_id, &manifest.key);
                self.notifications.notify(object_invalidation_event(
                    &manifest.namespace_id,
                    &manifest.bucket_id,
                    &manifest.key,
                    MetadataEventKind::ObjectHeadUpdated,
                    &manifest.version_id,
                ));
                self.stats.record_phase(
                    kind,
                    "store_commit_object_write_total",
                    phase_started.elapsed(),
                );
                record_store_phase_timings(
                    &self.stats,
                    kind,
                    "store_commit_object_write",
                    &phase_timings,
                );
                let phase_started = Instant::now();
                dispatch_finalize_reservations(
                    &self.reservation_mutation_dispatcher,
                    self.hot_store.clone(),
                    self.kas_channels.clone(),
                    self.stats.clone(),
                    ReservationMutationWork {
                        intent_id: Some(intent_id),
                        finalize_plans,
                        reservation_ids,
                    },
                    self.reservation_mutation_batch_size,
                    self.kas_rpc_timeout.saturating_mul(3),
                );
                self.stats.record_phase(
                    kind,
                    "queue_finalize_reservations",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(CommitObjectWriteReply {
                    manifest: Some(manifest),
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_commit_object_write_total",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn reserve_object_write_window(
        &self,
        request: Request<ReserveObjectWriteWindowRequest>,
    ) -> Result<Response<ReserveObjectWriteWindowReply>, Status> {
        let kind = RpcKind::ReserveObjectWriteWindow;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        if request.stripe_count == 0 {
            return kms_err(
                &self.stats,
                kind,
                &started,
                Status::invalid_argument("ReserveObjectWriteWindow requires stripe_count > 0"),
            );
        }
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        let intent = match self
            .hot_store
            .get_write_intent(request.intent_id.clone())
            .await
        {
            Ok(Some(intent)) => intent,
            Ok(None) => {
                return kms_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::not_found(format!("unknown write intent {}", request.intent_id)),
                );
            }
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        if intent.state != WriteIntentState::Reserved as i32 {
            return kms_err(
                &self.stats,
                kind,
                &started,
                Status::failed_precondition(format!(
                    "write intent {} is not reservable in state {}",
                    intent.intent_id, intent.state
                )),
            );
        }
        let total_stripes = usize::try_from(intent.stripe_count).map_err(|_| {
            Status::internal(format!(
                "write intent {} declares unsupported stripe count {}",
                intent.intent_id, intent.stripe_count
            ))
        });
        let total_stripes = match total_stripes {
            Ok(value) => value,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        let start_stripe_index = request.start_stripe_index as usize;
        let window_stripe_count = request.stripe_count as usize;
        let window_end = start_stripe_index.saturating_add(window_stripe_count);
        if start_stripe_index >= total_stripes || window_end > total_stripes {
            return kms_err(
                &self.stats,
                kind,
                &started,
                Status::invalid_argument(format!(
                    "write window {}..{} is out of range for intent {} with {} stripes",
                    start_stripe_index, window_end, intent.intent_id, total_stripes
                )),
            );
        }
        self.stats
            .record_phase(kind, "load_write_intent", phase_started.elapsed());
        let phase_started = Instant::now();
        let ec_profile = match self
            .load_bucket_write_context(kind, &intent.bucket_id)
            .await
        {
            Ok(context) => context.ec_profile,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        self.stats
            .record_phase(kind, "load_bucket_profile", phase_started.elapsed());
        match self
            .reserve_write_window_with_cache(
                kind,
                &intent,
                &ec_profile,
                start_stripe_index,
                window_stripe_count,
            )
            .await
        {
            Ok(fragment_plans) => {
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ReserveObjectWriteWindowReply {
                    fragment_plans,
                }))
            }
            Err(err) => kms_err(&self.stats, kind, &started, err),
        }
    }

    async fn commit_object_write_window(
        &self,
        request: Request<CommitObjectWriteWindowRequest>,
    ) -> Result<Response<CommitObjectWriteWindowReply>, Status> {
        let kind = RpcKind::CommitObjectWriteWindow;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .hot_store
            .commit_object_write_window(request.intent_id, request.successful_fragments)
            .await
        {
            Ok(TimedStoreResult {
                value:
                    CommittedObjectWriteWindow {
                        intent_id: _intent_id,
                        reservation_ids,
                        finalize_plans,
                    },
                phase_timings,
            }) => {
                self.stats.record_phase(
                    kind,
                    "store_commit_object_write_window_total",
                    phase_started.elapsed(),
                );
                record_store_phase_timings(
                    &self.stats,
                    kind,
                    "store_commit_object_write_window",
                    &phase_timings,
                );
                dispatch_finalize_reservations(
                    &self.reservation_mutation_dispatcher,
                    self.hot_store.clone(),
                    self.kas_channels.clone(),
                    self.stats.clone(),
                    ReservationMutationWork {
                        intent_id: None,
                        finalize_plans,
                        reservation_ids,
                    },
                    self.reservation_mutation_batch_size,
                    self.kas_rpc_timeout.saturating_mul(3),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(CommitObjectWriteWindowReply {}))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_commit_object_write_window_total",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn abort_object_write(
        &self,
        request: Request<AbortObjectWriteRequest>,
    ) -> Result<Response<AbortObjectWriteReply>, Status> {
        let kind = RpcKind::AbortObjectWrite;
        let started = Instant::now();
        self.stats.record_request(kind);
        let intent_id = request.into_inner().intent_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .hot_store
            .abort_object_write(intent_id, WriteIntentState::Aborted)
            .await
        {
            Ok(intent) => {
                self.stats
                    .record_phase(kind, "store_abort_object_write", phase_started.elapsed());
                if !intent.reservation_ids.is_empty() {
                    let phase_started = Instant::now();
                    if let Err(err) = release_reservation_ids(
                        self.kas_channels.clone(),
                        &intent.reservation_ids,
                        self.reservation_mutation_batch_size,
                        self.kas_rpc_timeout.saturating_mul(3),
                    )
                    .await
                    {
                        return kms_err(&self.stats, kind, &started, err);
                    }
                    self.stats.record_phase(
                        kind,
                        "kas_release_reservations",
                        phase_started.elapsed(),
                    );
                }
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(AbortObjectWriteReply {
                    intent: Some(intent),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_abort_object_write", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn resolve_object_read(
        &self,
        request: Request<ResolveObjectReadRequest>,
    ) -> Result<Response<ResolveObjectReadReply>, Status> {
        let kind = RpcKind::ResolveObjectRead;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let phase_started = Instant::now();
        let normalized_key = match normalize_object_key(&request.key) {
            Ok(value) => value,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        self.stats
            .record_phase(kind, "request_decode", phase_started.elapsed());
        let phase_started = Instant::now();
        if let Some((manifest, ec_profile)) =
            self.read_cache.get(&request.bucket_id, &normalized_key)
        {
            self.stats
                .record_phase(kind, "read_cache_hit", phase_started.elapsed());
            self.stats.record_success(kind, started.elapsed());
            return Ok(Response::new(ResolveObjectReadReply {
                manifest: Some(manifest),
                ec_profile: Some(ec_profile),
            }));
        }
        self.stats
            .record_phase(kind, "read_cache_miss", phase_started.elapsed());
        let phase_started = Instant::now();
        match self
            .hot_store
            .resolve_object_read(request.bucket_id.clone(), normalized_key.clone())
            .await
        {
            Ok((manifest, ec_profile)) => {
                self.stats
                    .record_phase(kind, "store_resolve_object_read", phase_started.elapsed());
                let cache_phase_started = Instant::now();
                self.read_cache.insert(
                    request.bucket_id,
                    normalized_key,
                    manifest.namespace_id.clone(),
                    manifest.clone(),
                    ec_profile.clone(),
                );
                self.stats
                    .record_phase(kind, "read_cache_store", cache_phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ResolveObjectReadReply {
                    manifest: Some(manifest),
                    ec_profile: Some(ec_profile),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_resolve_object_read", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectReply>, Status> {
        let kind = RpcKind::DeleteObject;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);

        let phase_started = Instant::now();
        let deleted = match self
            .hot_store
            .delete_object(request.bucket_id, request.key, request.version_ids)
            .await
        {
            Ok(TimedStoreResult {
                value,
                phase_timings,
            }) => {
                self.stats
                    .record_phase(kind, "store_delete_object_total", phase_started.elapsed());
                record_store_phase_timings(
                    &self.stats,
                    kind,
                    "store_delete_object",
                    &phase_timings,
                );
                value
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_delete_object_total", phase_started.elapsed());
                return kms_err(&self.stats, kind, &started, err);
            }
        };

        invalidate_local_object_cache(&self.read_cache, &deleted.bucket_id, &deleted.key);
        let phase_started = Instant::now();
        let mut cleanup = cleanup_deleted_object_fragments(&deleted.deleted_versions).await;
        self.stats
            .record_phase(kind, "target_delete_chunks", phase_started.elapsed());

        self.notifications.notify(object_invalidation_event(
            deleted
                .deleted_versions
                .first()
                .map(|version| version.manifest.namespace_id.as_str())
                .unwrap_or_default(),
            &deleted.bucket_id,
            &deleted.key,
            MetadataEventKind::ObjectDeleted,
            "",
        ));

        if !cleanup.cleanup_complete {
            self.stats.set_last_error(format!(
                "delete cleanup incomplete for {}/{}: deleted {} of {} fragments before allocator reclaim",
                deleted.bucket_id,
                deleted.key,
                cleanup.fragment_delete_successes,
                cleanup.fragment_delete_attempts
            ));
        }

        let phase_started = Instant::now();
        if !cleanup.granules.is_empty() {
            match reclaim_target_granules(
                self.kas_channels.clone(),
                cleanup.granules.clone(),
                self.kas_rpc_timeout.saturating_mul(3),
            )
            .await
            {
                Ok(reclaimed) => {
                    cleanup.reclaimed_granules = reclaimed;
                }
                Err(err) => {
                    cleanup.cleanup_complete = false;
                    self.stats.set_last_error(format!(
                        "allocator reclaim incomplete for {}/{}: {}",
                        deleted.bucket_id, deleted.key, err
                    ));
                }
            }
        }
        self.stats
            .record_phase(kind, "kas_reclaim_target_granules", phase_started.elapsed());
        cleanup.cleanup_complete = cleanup.cleanup_complete
            && cleanup.reclaimed_granules == cleanup.fragment_delete_successes;

        self.stats.record_success(kind, started.elapsed());
        Ok(Response::new(DeleteObjectReply {
            deleted_versions: deleted
                .deleted_versions
                .iter()
                .map(proto_deleted_object_version)
                .collect(),
            fragment_delete_attempts: cleanup.fragment_delete_attempts,
            fragment_delete_successes: cleanup.fragment_delete_successes,
            reclaimed_granules: cleanup.reclaimed_granules,
            cleanup_complete: cleanup.cleanup_complete,
        }))
    }

    async fn repair_object_write(
        &self,
        request: Request<RepairObjectWriteRequest>,
    ) -> Result<Response<RepairObjectWriteReply>, Status> {
        let kind = RpcKind::RepairObjectWrite;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        if request.failed_fragments.is_empty() || request.failed_fragments.len() > 2 {
            return kms_err(
                &self.stats,
                kind,
                &started,
                Status::invalid_argument("RepairObjectWrite requires one or two failed fragments"),
            );
        }
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let intent = self
            .hot_store
            .get_write_intent(request.intent_id.clone())
            .await
            .and_then(|intent| {
                intent.ok_or_else(|| {
                    Status::not_found(format!("unknown write intent {}", request.intent_id))
                })
            });
        let intent = match intent {
            Ok(intent) => intent,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        let failed_stripe = request.failed_fragments[0].stripe_index;
        if request
            .failed_fragments
            .iter()
            .any(|fragment| fragment.stripe_index != failed_stripe)
        {
            return kms_err(
                &self.stats,
                kind,
                &started,
                Status::invalid_argument(
                    "RepairObjectWrite only supports repairing fragments from one stripe at a time",
                ),
            );
        }
        let ec_profile = match self
            .load_bucket_write_context(kind, &intent.bucket_id)
            .await
        {
            Ok(context) => context.ec_profile,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        let phase_started = Instant::now();
        let replacement = match reserve_replacement_placement(
            self.kas_channels.clone(),
            request.failed_fragments.len(),
            &ec_profile,
            intent
                .fragment_plans
                .iter()
                .filter(|plan| plan.stripe_index == failed_stripe)
                .map(|plan| plan.target_id.clone())
                .collect(),
            self.write_intent_ttl.as_millis() as u64,
            Vec::new(),
            self.kas_rpc_timeout,
        )
        .await
        {
            Ok(reservation) => reservation,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        self.stats
            .record_phase(kind, "kas_reserve_replacement", phase_started.elapsed());
        let phase_started = Instant::now();
        match self
            .hot_store
            .repair_object_write(
                request.intent_id,
                request.failed_fragments,
                replacement.clone(),
            )
            .await
        {
            Ok(intent) => {
                self.stats
                    .record_phase(kind, "store_repair_object_write", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(RepairObjectWriteReply {
                    intent: Some(intent),
                }))
            }
            Err(err) => {
                let _ = release_reservation_ids(
                    self.kas_channels.clone(),
                    std::slice::from_ref(&replacement.reservation_id),
                    self.reservation_mutation_batch_size,
                    self.kas_rpc_timeout.saturating_mul(3),
                )
                .await;
                self.stats
                    .record_phase(kind, "store_repair_object_write", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn list_write_intents(
        &self,
        request: Request<ListWriteIntentsRequest>,
    ) -> Result<Response<ListWriteIntentsReply>, Status> {
        let kind = RpcKind::ListWriteIntents;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let state =
            WriteIntentState::try_from(request.state).unwrap_or(WriteIntentState::Unspecified);
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.hot_store.list_write_intents().await {
            Ok(intents) => {
                self.stats
                    .record_phase(kind, "store_list_write_intents", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                let intents = intents
                    .into_iter()
                    .filter(|intent| {
                        (request.bucket_id.is_empty() || intent.bucket_id == request.bucket_id)
                            && (state == WriteIntentState::Unspecified
                                || intent.state == request.state)
                    })
                    .take(request.limit as usize)
                    .map(|intent| keinctl::proto::WriteIntentSummary {
                        intent_id: intent.intent_id,
                        version_id: intent.version_id,
                        bucket_id: intent.bucket_id,
                        key: intent.key,
                        logical_length_bytes: intent.logical_length_bytes,
                        ec_profile_id: intent.ec_profile_id,
                        expires_at_unix_ms: intent.expires_at_unix_ms,
                        state: intent.state,
                        namespace_id: intent.namespace_id,
                        object_entry_id: intent.object_entry_id,
                        bucket_entry_id: intent.bucket_entry_id,
                        reservation_ids: intent.reservation_ids,
                        fragment_status: intent.fragment_status,
                    })
                    .collect();
                Ok(Response::new(ListWriteIntentsReply { intents }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_list_write_intents", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn get_write_intent(
        &self,
        request: Request<GetWriteIntentRequest>,
    ) -> Result<Response<GetWriteIntentReply>, Status> {
        let kind = RpcKind::GetWriteIntent;
        let started = Instant::now();
        self.stats.record_request(kind);
        let intent_id = request.into_inner().intent_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.hot_store.get_write_intent(intent_id.clone()).await {
            Ok(Some(intent)) => {
                self.stats
                    .record_phase(kind, "store_get_write_intent", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(GetWriteIntentReply {
                    intent: Some(intent),
                }))
            }
            Ok(None) => {
                self.stats
                    .record_phase(kind, "store_get_write_intent", phase_started.elapsed());
                kms_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::not_found(format!("unknown write intent {}", intent_id)),
                )
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_get_write_intent", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn lease_rebuild_tasks(
        &self,
        request: Request<LeaseRebuildTasksRequest>,
    ) -> Result<Response<LeaseRebuildTasksReply>, Status> {
        let kind = RpcKind::LeaseRebuildTasks;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .lease_rebuild_tasks(
                request.lease_owner,
                request.max_tasks.max(1) as usize,
                request.lease_ttl_ms.max(1_000),
                now_unix_ms(),
            )
            .await
        {
            Ok(tasks) => {
                self.stats
                    .record_phase(kind, "store_lease_rebuild_tasks", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(LeaseRebuildTasksReply { tasks }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_lease_rebuild_tasks", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn lease_placement_tasks(
        &self,
        request: Request<LeasePlacementTasksRequest>,
    ) -> Result<Response<LeasePlacementTasksReply>, Status> {
        let kind = RpcKind::LeasePlacementTasks;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .lease_placement_tasks(
                request.lease_owner,
                request.max_tasks.max(1) as usize,
                request.lease_ttl_ms.max(1_000),
                now_unix_ms(),
            )
            .await
        {
            Ok(tasks) => {
                self.stats.record_phase(
                    kind,
                    "store_lease_placement_tasks",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(LeasePlacementTasksReply { tasks }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_lease_placement_tasks",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn commit_rebuild(
        &self,
        request: Request<CommitRebuildRequest>,
    ) -> Result<Response<CommitRebuildReply>, Status> {
        let kind = RpcKind::CommitRebuild;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let replacement_fragment = match request.replacement_fragment {
            Some(fragment) => fragment,
            None => {
                return kms_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::invalid_argument("CommitRebuild requires replacement_fragment"),
                );
            }
        };
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .commit_rebuild(request.task_id, replacement_fragment)
            .await
        {
            Ok(manifest) => {
                self.stats
                    .record_phase(kind, "store_commit_rebuild", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(CommitRebuildReply {
                    manifest: Some(manifest),
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_commit_rebuild", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn commit_placement_task(
        &self,
        request: Request<CommitPlacementTaskRequest>,
    ) -> Result<Response<CommitPlacementTaskReply>, Status> {
        let kind = RpcKind::CommitPlacementTask;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let replacement_fragment = match request.replacement_fragment {
            Some(fragment) => fragment,
            None => {
                return kms_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::invalid_argument("CommitPlacementTask requires replacement_fragment"),
                );
            }
        };
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .commit_placement_task(request.task_id, replacement_fragment)
            .await
        {
            Ok(manifest) => {
                self.stats.record_phase(
                    kind,
                    "store_commit_placement_task",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(CommitPlacementTaskReply {
                    manifest: Some(manifest),
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_commit_placement_task",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn fail_placement_task(
        &self,
        request: Request<FailPlacementTaskRequest>,
    ) -> Result<Response<FailPlacementTaskReply>, Status> {
        let kind = RpcKind::FailPlacementTask;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .fail_placement_task(request.task_id, request.failure_reason)
            .await
        {
            Ok(task) => {
                self.stats
                    .record_phase(kind, "store_fail_placement_task", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(FailPlacementTaskReply { task: Some(task) }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_fail_placement_task", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn list_placement_tasks(
        &self,
        request: Request<ListPlacementTasksRequest>,
    ) -> Result<Response<ListPlacementTasksReply>, Status> {
        let kind = RpcKind::ListPlacementTasks;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        let task_kind = PlacementTaskKind::try_from(request.task_kind)
            .unwrap_or(PlacementTaskKind::Unspecified);
        let state =
            PlacementTaskState::try_from(request.state).unwrap_or(PlacementTaskState::Unspecified);
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .list_placement_tasks(
                (!request.source_target_id.is_empty()).then_some(request.source_target_id.as_str()),
                (!request.object_version_ref.is_empty())
                    .then_some(request.object_version_ref.as_str()),
                (task_kind != PlacementTaskKind::Unspecified).then_some(task_kind),
                (state != PlacementTaskState::Unspecified).then_some(state),
                request.limit as usize,
            )
            .await
        {
            Ok(tasks) => {
                self.stats.record_phase(
                    kind,
                    "store_list_placement_tasks",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ListPlacementTasksReply { tasks }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_list_placement_tasks",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn get_placement_task(
        &self,
        request: Request<GetPlacementTaskRequest>,
    ) -> Result<Response<GetPlacementTaskReply>, Status> {
        let kind = RpcKind::GetPlacementTask;
        let started = Instant::now();
        self.stats.record_request(kind);
        let task_id = request.into_inner().task_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.store.get_placement_task(&task_id).await {
            Ok(Some((task, manifest, ec_profile))) => {
                self.stats
                    .record_phase(kind, "store_get_placement_task", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(GetPlacementTaskReply {
                    task: Some(task),
                    manifest: Some(manifest),
                    ec_profile: Some(ec_profile),
                }))
            }
            Ok(None) => {
                self.stats
                    .record_phase(kind, "store_get_placement_task", phase_started.elapsed());
                kms_err(
                    &self.stats,
                    kind,
                    &started,
                    Status::not_found(format!("unknown placement task {}", task_id)),
                )
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_get_placement_task", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn get_target_placement_status(
        &self,
        request: Request<GetTargetPlacementStatusRequest>,
    ) -> Result<Response<GetTargetPlacementStatusReply>, Status> {
        let kind = RpcKind::GetTargetPlacementStatus;
        let started = Instant::now();
        self.stats.record_request(kind);
        let target_id = request.into_inner().target_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self.store.get_target_placement_status(&target_id).await {
            Ok(status) => {
                self.stats.record_phase(
                    kind,
                    "store_get_target_placement_status",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(GetTargetPlacementStatusReply {
                    status: Some(status),
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_get_target_placement_status",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn list_metadata_events(
        &self,
        request: Request<ListMetadataEventsRequest>,
    ) -> Result<Response<ListMetadataEventsReply>, Status> {
        let kind = RpcKind::ListMetadataEvents;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        match self
            .store
            .list_metadata_events(
                request.namespace_id.as_str(),
                (!request.entry_id.is_empty()).then_some(request.entry_id.as_str()),
                (!request.parent_entry_id.is_empty()).then_some(request.parent_entry_id.as_str()),
                request.start_revision,
                request.limit as usize,
            )
            .await
        {
            Ok(events) => {
                self.stats.record_phase(
                    kind,
                    "store_list_metadata_events",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ListMetadataEventsReply { events }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_list_metadata_events",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn report_target_failure(
        &self,
        request: Request<ReportTargetFailureRequest>,
    ) -> Result<Response<ReportTargetFailureReply>, Status> {
        let kind = RpcKind::ReportTargetFailure;
        let started = Instant::now();
        self.stats.record_request(kind);
        let target_id = request.into_inner().target_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        if let Err(err) = set_target_state(
            self.kas_channels.clone(),
            &target_id,
            TargetLifecycleState::Unhealthy,
            self.kas_rpc_timeout.saturating_mul(3),
        )
        .await
        {
            self.stats
                .record_phase(kind, "kas_set_target_unhealthy", phase_started.elapsed());
            return kms_err(&self.stats, kind, &started, err);
        }
        self.stats
            .record_phase(kind, "kas_set_target_unhealthy", phase_started.elapsed());
        let phase_started = Instant::now();
        match self.store.report_target_failure(target_id).await {
            Ok(created_tasks) => {
                self.stats.record_phase(
                    kind,
                    "store_report_target_failure",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(ReportTargetFailureReply { created_tasks }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_report_target_failure",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn drain_target(
        &self,
        request: Request<DrainTargetRequest>,
    ) -> Result<Response<DrainTargetReply>, Status> {
        let kind = RpcKind::DrainTarget;
        let started = Instant::now();
        self.stats.record_request(kind);
        let target_id = request.into_inner().target_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        if let Err(err) = set_target_state(
            self.kas_channels.clone(),
            &target_id,
            TargetLifecycleState::Draining,
            self.kas_rpc_timeout.saturating_mul(3),
        )
        .await
        {
            self.stats
                .record_phase(kind, "kas_set_target_draining", phase_started.elapsed());
            return kms_err(&self.stats, kind, &started, err);
        }
        self.stats
            .record_phase(kind, "kas_set_target_draining", phase_started.elapsed());
        let phase_started = Instant::now();
        let active_targets = match list_active_targets(
            self.kas_channels.clone(),
            self.kas_rpc_timeout.saturating_mul(3),
        )
        .await
        {
            Ok(targets) => targets,
            Err(err) => {
                self.stats
                    .record_phase(kind, "kas_list_active_targets", phase_started.elapsed());
                return kms_err(&self.stats, kind, &started, err);
            }
        };
        self.stats
            .record_phase(kind, "kas_list_active_targets", phase_started.elapsed());
        let phase_started = Instant::now();
        match self.store.drain_target(target_id, active_targets).await {
            Ok(created_tasks) => {
                self.stats
                    .record_phase(kind, "store_drain_target", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(DrainTargetReply { created_tasks }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "store_drain_target", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn preview_target_rebalance(
        &self,
        request: Request<PreviewTargetRebalanceRequest>,
    ) -> Result<Response<PreviewTargetRebalanceReply>, Status> {
        let kind = RpcKind::PreviewTargetRebalance;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        let active_targets = match list_active_targets(
            self.kas_channels.clone(),
            self.kas_rpc_timeout.saturating_mul(3),
        )
        .await
        {
            Ok(targets) => targets,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        self.stats
            .record_phase(kind, "kas_list_targets", phase_started.elapsed());
        let phase_started = Instant::now();
        match self
            .store
            .preview_target_rebalance(
                request.source_target_ids,
                request.include_target_ids.into_iter().collect(),
                request.exclude_target_ids.into_iter().collect(),
                active_targets,
                normalize_max_tasks(request.max_tasks),
            )
            .await
        {
            Ok((candidate_tasks, live_fragments)) => {
                self.stats.record_phase(
                    kind,
                    "store_preview_target_rebalance",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(PreviewTargetRebalanceReply {
                    live_fragments,
                    candidate_tasks,
                }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_preview_target_rebalance",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn enqueue_target_rebalance(
        &self,
        request: Request<EnqueueTargetRebalanceRequest>,
    ) -> Result<Response<EnqueueTargetRebalanceReply>, Status> {
        let kind = RpcKind::EnqueueTargetRebalance;
        let started = Instant::now();
        self.stats.record_request(kind);
        let request = request.into_inner();
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        let active_targets = match list_active_targets(
            self.kas_channels.clone(),
            self.kas_rpc_timeout.saturating_mul(3),
        )
        .await
        {
            Ok(targets) => targets,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        self.stats
            .record_phase(kind, "kas_list_targets", phase_started.elapsed());
        let phase_started = Instant::now();
        match self
            .store
            .enqueue_target_rebalance(
                request.source_target_ids,
                request.include_target_ids.into_iter().collect(),
                request.exclude_target_ids.into_iter().collect(),
                active_targets,
                normalize_max_tasks(request.max_tasks),
            )
            .await
        {
            Ok(created_tasks) => {
                self.stats.record_phase(
                    kind,
                    "store_enqueue_target_rebalance",
                    phase_started.elapsed(),
                );
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(EnqueueTargetRebalanceReply { created_tasks }))
            }
            Err(err) => {
                self.stats.record_phase(
                    kind,
                    "store_enqueue_target_rebalance",
                    phase_started.elapsed(),
                );
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn recover_target(
        &self,
        request: Request<RecoverTargetRequest>,
    ) -> Result<Response<RecoverTargetReply>, Status> {
        let kind = RpcKind::RecoverTarget;
        let started = Instant::now();
        self.stats.record_request(kind);
        let target_id = request.into_inner().target_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        let target = match get_target_record(
            self.kas_channels.clone(),
            &target_id,
            self.kas_rpc_timeout.saturating_mul(3),
        )
        .await
        {
            Ok(target) => target,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        self.stats
            .record_phase(kind, "kas_get_target", phase_started.elapsed());
        let phase_started = Instant::now();
        let target_reachable = match target_endpoint_reachable(&target.endpoint).await {
            Ok(reachable) => reachable,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        self.stats
            .record_phase(kind, "target_reachability_probe", phase_started.elapsed());
        let target_lifecycle_state = TargetLifecycleState::try_from(target.lifecycle_state)
            .unwrap_or(TargetLifecycleState::Unspecified);
        let allow_reachable_state_recovery = target_reachable
            && matches!(
                target_lifecycle_state,
                TargetLifecycleState::Unhealthy
                    | TargetLifecycleState::Draining
                    | TargetLifecycleState::Active
            );
        let cancel_pending_evacuate = target_reachable
            && ((target_lifecycle_state == TargetLifecycleState::Draining)
                || (target_lifecycle_state == TargetLifecycleState::Active));
        let cancel_pending_rebuild = target_reachable;
        let cancel_pending_rebalance =
            target_reachable && target_lifecycle_state == TargetLifecycleState::Active;
        let live_fragments = if allow_reachable_state_recovery {
            // A reachable target that is merely marked unhealthy, or a draining target that the
            // operator wants to return to service, or an already-active target that simply needs
            // stale evacuation work cleared, does not need an exact multi-million-fragment census
            // before KMS flips the metadata state back to active and clears obsolete placement
            // backlog.
            self.stats
                .record_phase(kind, "store_live_fragment_count", Duration::ZERO);
            0
        } else {
            let phase_started = Instant::now();
            let count = match self
                .store
                .recover_or_retire_target_allowed(&target_id)
                .await
            {
                Ok(count) => count,
                Err(err) => return kms_err(&self.stats, kind, &started, err),
            };
            self.stats
                .record_phase(kind, "store_live_fragment_count", phase_started.elapsed());
            count
        };
        if live_fragments != 0 {
            return kms_err(
                &self.stats,
                kind,
                &started,
                Status::failed_precondition(format!(
                    "target {} still has {} live fragments and cannot be recovered",
                    target_id, live_fragments
                )),
            );
        }
        if cancel_pending_evacuate {
            let clear_started = Instant::now();
            match self.store.cancel_pending_evacuate_tasks(&target_id).await {
                Ok(_) => self.stats.record_phase(
                    kind,
                    "store_cancel_pending_evacuate",
                    clear_started.elapsed(),
                ),
                Err(err) => {
                    self.stats.record_phase(
                        kind,
                        "store_cancel_pending_evacuate",
                        clear_started.elapsed(),
                    );
                    return kms_err(&self.stats, kind, &started, err);
                }
            }
        } else {
            self.stats
                .record_phase(kind, "store_cancel_pending_evacuate", Duration::ZERO);
        }
        if cancel_pending_rebuild {
            let clear_started = Instant::now();
            match self.store.cancel_pending_rebuild_tasks(&target_id).await {
                Ok(_) => self.stats.record_phase(
                    kind,
                    "store_cancel_pending_rebuild",
                    clear_started.elapsed(),
                ),
                Err(err) => {
                    self.stats.record_phase(
                        kind,
                        "store_cancel_pending_rebuild",
                        clear_started.elapsed(),
                    );
                    return kms_err(&self.stats, kind, &started, err);
                }
            }
        } else {
            self.stats
                .record_phase(kind, "store_cancel_pending_rebuild", Duration::ZERO);
        }
        if cancel_pending_rebalance {
            let clear_started = Instant::now();
            match self.store.cancel_pending_rebalance_tasks(&target_id).await {
                Ok(_) => self.stats.record_phase(
                    kind,
                    "store_cancel_pending_rebalance",
                    clear_started.elapsed(),
                ),
                Err(err) => {
                    self.stats.record_phase(
                        kind,
                        "store_cancel_pending_rebalance",
                        clear_started.elapsed(),
                    );
                    return kms_err(&self.stats, kind, &started, err);
                }
            }
        } else {
            self.stats
                .record_phase(kind, "store_cancel_pending_rebalance", Duration::ZERO);
        }
        let phase_started = Instant::now();
        match set_target_state(
            self.kas_channels.clone(),
            &target_id,
            TargetLifecycleState::Active,
            self.kas_rpc_timeout.saturating_mul(3),
        )
        .await
        {
            Ok(target) => {
                self.stats
                    .record_phase(kind, "kas_set_target_active", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(RecoverTargetReply {
                    target: Some(target),
                    live_fragments,
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "kas_set_target_active", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }

    async fn retire_target(
        &self,
        request: Request<RetireTargetRequest>,
    ) -> Result<Response<RetireTargetReply>, Status> {
        let kind = RpcKind::RetireTarget;
        let started = Instant::now();
        self.stats.record_request(kind);
        let target_id = request.into_inner().target_id;
        self.stats
            .record_phase(kind, "request_decode", Duration::ZERO);
        let phase_started = Instant::now();
        let live_fragments = match self
            .store
            .recover_or_retire_target_allowed(&target_id)
            .await
        {
            Ok(count) => count,
            Err(err) => return kms_err(&self.stats, kind, &started, err),
        };
        self.stats
            .record_phase(kind, "store_live_fragment_count", phase_started.elapsed());
        if live_fragments != 0 {
            return kms_err(
                &self.stats,
                kind,
                &started,
                Status::failed_precondition(format!(
                    "target {} still has {} live fragments and cannot be retired",
                    target_id, live_fragments
                )),
            );
        }
        let phase_started = Instant::now();
        match set_target_state(
            self.kas_channels.clone(),
            &target_id,
            TargetLifecycleState::Retired,
            self.kas_rpc_timeout.saturating_mul(3),
        )
        .await
        {
            Ok(target) => {
                self.stats
                    .record_phase(kind, "kas_set_target_retired", phase_started.elapsed());
                self.stats.record_success(kind, started.elapsed());
                Ok(Response::new(RetireTargetReply {
                    target: Some(target),
                    live_fragments,
                }))
            }
            Err(err) => {
                self.stats
                    .record_phase(kind, "kas_set_target_retired", phase_started.elapsed());
                kms_err(&self.stats, kind, &started, err)
            }
        }
    }
}

impl KmsService {
    fn clear_ec_profile_catalog(&self) {
        *self
            .ec_profile_catalog
            .lock()
            .expect("ec_profile_catalog mutex poisoned") = None;
    }

    async fn list_ec_profiles_cached(&self, kind: RpcKind) -> Result<Vec<EcProfile>, Status> {
        if let Some(cached) = self
            .ec_profile_catalog
            .lock()
            .expect("ec_profile_catalog mutex poisoned")
            .clone()
        {
            self.stats
                .record_phase(kind, "ec_profile_catalog_cache_hit", Duration::ZERO);
            return Ok(cached);
        }
        self.stats
            .record_phase(kind, "ec_profile_catalog_cache_miss", Duration::ZERO);
        let phase_started = Instant::now();
        let profiles = self.store.list_ec_profiles().await?;
        self.stats.record_phase(
            kind,
            "store_list_ec_profiles_cached",
            phase_started.elapsed(),
        );
        *self
            .ec_profile_catalog
            .lock()
            .expect("ec_profile_catalog mutex poisoned") = Some(profiles.clone());
        Ok(profiles)
    }

    async fn select_write_ec_profile(
        &self,
        kind: RpcKind,
        default_profile: &EcProfile,
        logical_length_bytes: u64,
    ) -> Result<EcProfile, Status> {
        let max_stripes = self.write_profile_max_stripes.max(1);
        let default_stripe_count = stripe_count_for_profile(logical_length_bytes, default_profile);
        if default_profile.fragment_bytes <= self.write_profile_min_fragment_bytes
            || default_stripe_count > max_stripes
        {
            self.stats
                .record_phase(kind, "write_profile_default", Duration::ZERO);
            return Ok(default_profile.clone());
        }
        let mut candidates = self
            .list_ec_profiles_cached(kind)
            .await?
            .into_iter()
            .filter(|candidate| profile_matches_family(candidate, default_profile))
            .filter(|candidate| candidate.fragment_bytes >= self.write_profile_min_fragment_bytes)
            .filter(|candidate| candidate.fragment_bytes <= default_profile.fragment_bytes)
            .collect::<Vec<_>>();
        candidates.sort_by_key(|candidate| candidate.fragment_bytes);
        for candidate in candidates {
            if stripe_count_for_profile(logical_length_bytes, &candidate) <= max_stripes {
                if candidate.id == default_profile.id {
                    self.stats
                        .record_phase(kind, "write_profile_default", Duration::ZERO);
                } else {
                    self.stats
                        .record_phase(kind, "write_profile_alternate", Duration::ZERO);
                }
                return Ok(candidate);
            }
        }
        self.stats
            .record_phase(kind, "write_profile_default", Duration::ZERO);
        Ok(default_profile.clone())
    }

    async fn load_bucket_write_context(
        &self,
        kind: RpcKind,
        bucket_id: &str,
    ) -> Result<BucketWriteContext, Status> {
        if let Some(cached) = self
            .bucket_write_contexts
            .lock()
            .expect("bucket_write_contexts mutex poisoned")
            .get(bucket_id)
            .cloned()
        {
            self.stats
                .record_phase(kind, "bucket_context_cache_hit", Duration::ZERO);
            return Ok(cached);
        }

        self.stats
            .record_phase(kind, "bucket_context_cache_miss", Duration::ZERO);
        let loaded = self
            .hot_store
            .get_bucket_write_context(bucket_id.to_string())
            .await?;
        self.bucket_write_contexts
            .lock()
            .expect("bucket_write_contexts mutex poisoned")
            .insert(bucket_id.to_string(), loaded.clone());
        Ok(loaded)
    }

    fn lookup_object_parent(
        &self,
        bucket_id: &str,
        normalized_key: &str,
    ) -> Option<ObjectParentContext> {
        let parent_key = parent_key_from_object_key(normalized_key)?;
        self.object_parent_contexts
            .lock()
            .expect("object_parent_contexts mutex poisoned")
            .get(&ObjectParentCacheKey::new(bucket_id, &parent_key))
            .cloned()
    }

    fn remember_object_parent(
        &self,
        bucket_id: &str,
        normalized_key: &str,
        parent_entry_id: &str,
        parent_path: &str,
    ) {
        let Some(parent_key) = parent_key_from_object_key(normalized_key) else {
            return;
        };
        let mut cache = self
            .object_parent_contexts
            .lock()
            .expect("object_parent_contexts mutex poisoned");
        if cache.len() >= 8_192 {
            cache.clear();
        }
        cache.insert(
            ObjectParentCacheKey::new(bucket_id, &parent_key),
            ObjectParentContext {
                parent_entry_id: parent_entry_id.to_string(),
                parent_path: parent_path.to_string(),
            },
        );
    }

    async fn reserve_write_window_with_cache(
        &self,
        kind: RpcKind,
        intent: &WriteIntent,
        ec_profile: &EcProfile,
        start_stripe_index: usize,
        window_stripe_count: usize,
    ) -> Result<Vec<keinctl::proto::FragmentPlan>, Status> {
        if window_stripe_count == 0 {
            return Ok(Vec::new());
        }
        let total_stripes = usize::try_from(intent.stripe_count).map_err(|_| {
            Status::internal(format!(
                "write intent {} declares unsupported stripe count {}",
                intent.intent_id, intent.stripe_count
            ))
        })?;
        let window_end = start_stripe_index.saturating_add(window_stripe_count);
        if start_stripe_index >= total_stripes || window_end > total_stripes {
            return Err(Status::invalid_argument(format!(
                "write window {}..{} is out of range for intent {} with {} stripes",
                start_stripe_index, window_end, intent.intent_id, total_stripes
            )));
        }
        let remaining_after_window = total_stripes.saturating_sub(window_end);
        let configured_kas_endpoint_count = self.kas_channels.endpoint_count().max(1);
        // Shard-count comes from the TTL route cache, not a fresh
        // per-reserve discovery. The cache records its own hit/miss + RPC count.
        // We still record the wall-time spent here so the lab can see
        // route resolution drop toward ~0 once the cache warms.
        let route_phase_started = Instant::now();
        let discovered_allocation_shard_count = self
            .route_cache
            .shard_count(
                self.kas_channels.clone(),
                self.kas_rpc_timeout.min(Duration::from_secs(5)),
            )
            .await
            .max(1);
        self.stats.record_phase(
            kind,
            "reserve_route_resolve",
            route_phase_started.elapsed(),
        );
        let allocation_shard_count =
            discovered_allocation_shard_count.max(configured_kas_endpoint_count);
        let use_shared_reservation_cache = shared_reservation_cache_enabled(
            remaining_after_window,
            window_stripe_count,
            self.reservation_cache.small_object_max_stripes(),
            allocation_shard_count,
        );
        // Distinguish the pool (cache) branch from the synchronous KAS
        // bypass. On multi-shard with the pre-staged RAM pool the bypass counter
        // should fall toward zero; previously it was hit on every multi-shard reserve.
        if use_shared_reservation_cache {
            self.stats.record_reservation_cache_serve();
        } else if allocation_shard_count > 1 {
            self.stats.record_reservation_cache_shard_bypass();
            self.stats
                .record_phase(kind, "reserve_cache_shard_bypass", Duration::ZERO);
        }
        let phase_started = Instant::now();
        let reservations = if use_shared_reservation_cache {
            self.take_cached_reservations(
                kind,
                &intent.bucket_id,
                ec_profile,
                window_stripe_count,
                remaining_after_window,
            )
            .await?
        } else {
            let bypass_started = Instant::now();
            let reserved = reserve_stripe_batch(
                self.kas_channels.clone(),
                &self.route_cache,
                window_stripe_count,
                ec_profile,
                self.reservation_cache.reservation_ttl_ms(),
                self.kas_rpc_timeout,
                self.kas_reserve_attempt_timeout,
                self.reservation_mutation_batch_size,
            )
            .await?;
            // Latency of the synchronous foreground KAS reserve, so the
            // lab can attribute the bypass cost separately from the pool path.
            self.stats.record_phase(
                kind,
                "reserve_stripe_batch_sync",
                bypass_started.elapsed(),
            );
            reserved
        };
        self.stats
            .record_phase(kind, "reservation_cache_acquire", phase_started.elapsed());
        if use_shared_reservation_cache {
            self.schedule_cache_refill(
                intent.bucket_id.clone(),
                ec_profile.clone(),
                remaining_after_window
                    .min(window_stripe_count)
                    .max(window_stripe_count),
            );
        }
        let reservation_ids_for_release = collect_reservation_ids_for_release(&reservations);
        let phase_started = Instant::now();
        match self
            .hot_store
            .reserve_object_write_window(
                intent.intent_id.clone(),
                start_stripe_index as u32,
                reservations,
            )
            .await
        {
            Ok(TimedStoreResult {
                value:
                    ReservedObjectWriteWindow {
                        fragment_plans,
                        used_reservations,
                    },
                phase_timings,
            }) => {
                self.stats.record_phase(
                    kind,
                    "store_reserve_object_write_window_total",
                    phase_started.elapsed(),
                );
                record_store_phase_timings(
                    &self.stats,
                    kind,
                    "store_reserve_object_write_window",
                    &phase_timings,
                );
                if !used_reservations && !reservation_ids_for_release.is_empty() {
                    let _ = release_reservation_ids(
                        self.kas_channels.clone(),
                        &reservation_ids_for_release,
                        self.reservation_mutation_batch_size,
                        self.kas_rpc_timeout.saturating_mul(3),
                    )
                    .await;
                }
                Ok(fragment_plans)
            }
            Err(err) => {
                if !reservation_ids_for_release.is_empty() {
                    let _ = release_reservation_ids(
                        self.kas_channels.clone(),
                        &reservation_ids_for_release,
                        self.reservation_mutation_batch_size,
                        self.kas_rpc_timeout.saturating_mul(3),
                    )
                    .await;
                }
                self.stats.record_phase(
                    kind,
                    "store_reserve_object_write_window_total",
                    phase_started.elapsed(),
                );
                Err(err)
            }
        }
    }

    async fn abort_failed_write_intent_init(&self, intent_id: &str) {
        let aborted = match self
            .hot_store
            .abort_object_write(intent_id.to_string(), WriteIntentState::Aborted)
            .await
        {
            Ok(intent) => intent,
            Err(err) => {
                self.stats.set_last_error(format!(
                    "KMS could not abort failed initiate for intent {}: {}",
                    intent_id, err
                ));
                return;
            }
        };
        if aborted.reservation_ids.is_empty() {
            return;
        }
        if let Err(err) = release_reservation_ids(
            self.kas_channels.clone(),
            &aborted.reservation_ids,
            self.reservation_mutation_batch_size,
            self.kas_rpc_timeout.saturating_mul(3),
        )
        .await
        {
            self.stats.set_last_error(format!(
                "KMS could not release failed initiate reservations for intent {}: {}",
                intent_id, err
            ));
        }
    }

    async fn take_cached_reservations(
        &self,
        kind: RpcKind,
        bucket_id: &str,
        ec_profile: &EcProfile,
        count: usize,
        future_demand: usize,
    ) -> Result<Vec<PlacementReservationRecord>, Status> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let cache_key = ReservationCacheKey::new(bucket_id, ec_profile);
        let mut acquired = Vec::with_capacity(count);
        loop {
            while acquired.len() < count {
                let Some(reservation) = self.reservation_cache.take(&cache_key, &self.stats) else {
                    break;
                };
                self.stats.record_reservation_cache_hit();
                self.stats
                    .record_phase(kind, "reserve_cache_hit", Duration::ZERO);
                acquired.push(reservation);
            }
            if acquired.len() >= count {
                return Ok(acquired);
            }

            self.stats.record_reservation_cache_miss();
            self.stats
                .record_phase(kind, "reserve_cache_miss", Duration::ZERO);
            if self
                .reservation_cache
                .begin_forced_refill(&cache_key, &self.stats)
            {
                let shortage = count.saturating_sub(acquired.len());
                match self
                    .refill_cache_on_demand(
                        kind,
                        bucket_id,
                        ec_profile,
                        &cache_key,
                        shortage,
                        count,
                        future_demand,
                    )
                    .await
                {
                    Ok(mut reservations) => {
                        acquired.append(&mut reservations);
                        continue;
                    }
                    Err(err) => {
                        if !acquired.is_empty() {
                            self.reservation_cache
                                .store_batch(&cache_key, acquired, &self.stats);
                        }
                        return Err(err);
                    }
                }
            }

            let wait_started = Instant::now();
            match self
                .reservation_cache
                .wait_for_refill(&cache_key, &self.stats)
                .await
            {
                ReservationCacheWaitOutcome::Ready => {
                    self.stats.record_phase(
                        kind,
                        "reserve_cache_wait_for_refill",
                        wait_started.elapsed(),
                    );
                    self.stats
                        .record_phase(kind, "reserve_cache_wait_hit", Duration::ZERO);
                    continue;
                }
                ReservationCacheWaitOutcome::Retry => {
                    self.stats.record_phase(
                        kind,
                        "reserve_cache_wait_for_refill",
                        wait_started.elapsed(),
                    );
                    self.stats
                        .record_phase(kind, "reserve_cache_wait_retry", Duration::ZERO);
                    continue;
                }
                ReservationCacheWaitOutcome::TimedOut => {
                    self.stats.record_phase(
                        kind,
                        "reserve_cache_wait_timeout",
                        wait_started.elapsed(),
                    );
                    if !acquired.is_empty() {
                        self.reservation_cache
                            .store_batch(&cache_key, acquired, &self.stats);
                    }
                    return Err(Status::deadline_exceeded(format!(
                        "KMS reservation cache could not obtain {} placements for bucket {} profile {} within {} ms",
                        count,
                        bucket_id,
                        ec_profile.id,
                        self.reservation_cache.wait_timeout().as_millis()
                    )));
                }
            }
        }
    }

    fn schedule_cache_refill(
        &self,
        bucket_id: String,
        ec_profile: EcProfile,
        recent_demand: usize,
    ) {
        let cache_key = ReservationCacheKey::new(&bucket_id, &ec_profile);
        let Some(batch_size) =
            self.reservation_cache
                .begin_async_refill(&cache_key, recent_demand, &self.stats)
        else {
            return;
        };

        let cache = self.reservation_cache.clone();
        let kas_channels = self.kas_channels.clone();
        let route_cache = self.route_cache.clone();
        let stats = self.stats.clone();
        let kas_rpc_timeout = self.kas_rpc_timeout;
        let reservation_mutation_batch_size = self.reservation_mutation_batch_size;
        tokio::spawn(async move {
            // The BACKGROUND refill assembles full stripes via
            // the sharded reserve path (and so benefits from the concurrent
            // per-shard fan-out). This is what keeps the foreground reserve off
            // a synchronous KAS call on multi-shard.
            let result = reserve_stripe_batch(
                kas_channels,
                &route_cache,
                batch_size,
                &ec_profile,
                cache.reservation_ttl_ms(),
                kas_rpc_timeout,
                kas_rpc_timeout.min(Duration::from_secs(10)),
                reservation_mutation_batch_size,
            )
            .await;
            match result {
                Ok(reservations) => {
                    if !reservations.is_empty() {
                        stats.record_reservation_cache_refill();
                        cache.store_batch(&cache_key, reservations, &stats);
                    }
                }
                Err(err) => {
                    stats.set_last_error(format!(
                        "KMS reservation cache refill failed for bucket {} profile {}: {}",
                        bucket_id, ec_profile.id, err
                    ));
                }
            }
            cache.finish_async_refill(&cache_key, &stats);
        });
    }

    async fn refill_cache_on_demand(
        &self,
        kind: RpcKind,
        bucket_id: &str,
        ec_profile: &EcProfile,
        cache_key: &ReservationCacheKey,
        shortage: usize,
        total_demand: usize,
        future_demand: usize,
    ) -> Result<Vec<PlacementReservationRecord>, Status> {
        let batch_size = self.reservation_cache.miss_refill_batch_size(
            cache_key,
            shortage,
            total_demand,
            future_demand,
            &self.stats,
        );
        let direct_started = Instant::now();
        let reservations = reserve_stripe_batch(
            self.kas_channels.clone(),
            &self.route_cache,
            batch_size,
            ec_profile,
            self.reservation_cache.reservation_ttl_ms(),
            self.kas_rpc_timeout,
            self.kas_reserve_attempt_timeout,
            self.reservation_mutation_batch_size,
        )
        .await;
        self.stats.record_phase(
            kind,
            "reserve_cache_direct_reserve",
            direct_started.elapsed(),
        );
        self.reservation_cache
            .finish_async_refill(cache_key, &self.stats);
        let mut reservations = reservations?;
        if reservations.is_empty() {
            return Err(Status::failed_precondition(format!(
                "KMS could not reserve a placement for bucket {} profile {}",
                bucket_id, ec_profile.id
            )));
        }
        let extras = if reservations.len() > shortage {
            reservations.split_off(shortage)
        } else {
            Vec::new()
        };
        if !extras.is_empty() {
            self.stats.record_reservation_cache_refill();
            self.reservation_cache
                .store_batch(cache_key, extras, &self.stats);
        }
        Ok(reservations)
    }
}

pub(crate) async fn reap_expired_intents(
    store: KmsStore,
    kas_channels: KasEndpointBalancer,
    stats: Arc<KmsStats>,
    reservation_mutation_batch_size: usize,
) -> Result<(), Status> {
    let started = Instant::now();
    let expired = store.expire_write_intents(now_unix_ms()).await?;
    if expired.is_empty() {
        stats.record_reaper_run(started.elapsed(), 0);
        return Ok(());
    }
    stats.record_expired_write_intents(expired.len());
    let mut released = 0_usize;
    for reservation_id in expired {
        let release_started = Instant::now();
        if let Err(err) = release_reservation(
            kas_channels.clone(),
            reservation_id,
            reservation_mutation_batch_size,
            Duration::from_secs(15),
        )
        .await
        {
            stats.set_last_error(format!(
                "KMS reaper could not release expired reservation: {err}"
            ));
        } else {
            stats.record_reaper_release(release_started.elapsed());
            released += 1;
        }
    }
    stats.record_reaper_run(started.elapsed(), released);
    Ok(())
}

pub(crate) async fn reconcile_pending_reservations(
    hot_store: Arc<dyn HotMetadataStore>,
    kas_channels: KasEndpointBalancer,
    stats: Arc<KmsStats>,
    limit: usize,
    reservation_mutation_batch_size: usize,
    rpc_timeout: Duration,
    finalizer_grace: Duration,
) -> Result<usize, Status> {
    let intents = hot_store
        .list_pending_finalization_intents(
            limit,
            now_unix_ms().saturating_sub(finalizer_grace.as_millis() as u64),
        )
        .await?;
    let mut finalized = 0_usize;
    for intent in intents {
        let intent_id = intent.intent_id.clone();
        if let Err(err) = finalize_intent_reservations(
            hot_store.clone(),
            kas_channels.clone(),
            intent,
            reservation_mutation_batch_size,
            rpc_timeout,
        )
        .await
        {
            stats.set_last_error(format!(
                "KMS reservation finalizer could not finalize intent {}: {}",
                intent_id, err
            ));
            continue;
        }
        finalized += 1;
    }
    Ok(finalized)
}

pub(crate) async fn reservation_mutation_dispatch_loop(
    hot_store: Arc<dyn HotMetadataStore>,
    kas_channels: KasEndpointBalancer,
    stats: Arc<KmsStats>,
    mut receiver: mpsc::UnboundedReceiver<ReservationMutationWork>,
    dispatch_batch_size: usize,
    dispatch_flush: Duration,
    reservation_mutation_batch_size: usize,
    rpc_timeout: Duration,
) {
    let dispatch_batch_size = dispatch_batch_size.max(1);
    while let Some(first) = receiver.recv().await {
        let mut works = Vec::with_capacity(dispatch_batch_size);
        works.push(first);
        while works.len() < dispatch_batch_size {
            match receiver.try_recv() {
                Ok(work) => works.push(work),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        if works.len() < dispatch_batch_size {
            let flush_sleep = sleep(dispatch_flush);
            tokio::pin!(flush_sleep);
            while works.len() < dispatch_batch_size {
                tokio::select! {
                    _ = &mut flush_sleep => break,
                    maybe = receiver.recv() => match maybe {
                        Some(work) => works.push(work),
                        None => break,
                    }
                }
            }
        }
        process_reservation_mutation_batch(
            hot_store.clone(),
            kas_channels.clone(),
            stats.clone(),
            works,
            reservation_mutation_batch_size,
            rpc_timeout,
        )
        .await;
    }
}

fn dispatch_finalize_reservations(
    dispatcher: &ReservationMutationDispatcher,
    hot_store: Arc<dyn HotMetadataStore>,
    kas_channels: KasEndpointBalancer,
    stats: Arc<KmsStats>,
    work: ReservationMutationWork,
    reservation_mutation_batch_size: usize,
    rpc_timeout: Duration,
) {
    if let Err(work) = dispatcher.dispatch(work) {
        tokio::spawn(async move {
            if let Err(err) = process_reservation_mutation_work(
                hot_store,
                kas_channels,
                work,
                reservation_mutation_batch_size,
                rpc_timeout,
            )
            .await
            {
                stats.set_last_error(format!(
                    "KMS direct reservation finalization fallback failed: {}",
                    err
                ));
            }
        });
    }
}

async fn process_reservation_mutation_batch(
    hot_store: Arc<dyn HotMetadataStore>,
    kas_channels: KasEndpointBalancer,
    stats: Arc<KmsStats>,
    works: Vec<ReservationMutationWork>,
    reservation_mutation_batch_size: usize,
    rpc_timeout: Duration,
) {
    let mut finalize_mutations = Vec::new();
    let mut release_mutations = Vec::new();
    let mut intent_ids = Vec::new();
    for work in &works {
        let (mut finalize_for_work, mut release_for_work) =
            build_reservation_mutations(&work.finalize_plans, &work.reservation_ids);
        finalize_mutations.append(&mut finalize_for_work);
        release_mutations.append(&mut release_for_work);
        if let Some(intent_id) = &work.intent_id {
            intent_ids.push(intent_id.clone());
        }
    }
    match send_reservation_mutation_batches(
        kas_channels.clone(),
        &finalize_mutations,
        &release_mutations,
        reservation_mutation_batch_size,
        rpc_timeout,
    )
    .await
    {
        Ok(()) => {
            for intent_id in intent_ids {
                if let Err(err) = hot_store
                    .mark_write_intent_reservations_finalized(intent_id.clone())
                    .await
                {
                    stats.set_last_error(format!(
                        "KMS could not mark intent {} reservation finalization complete: {}",
                        intent_id, err
                    ));
                }
            }
        }
        Err(err) => {
            stats.set_last_error(format!(
                "KMS batched reservation finalization fell back to per-intent processing: {}",
                err
            ));
            for work in works {
                if let Err(work_err) = process_reservation_mutation_work(
                    hot_store.clone(),
                    kas_channels.clone(),
                    work,
                    reservation_mutation_batch_size,
                    rpc_timeout,
                )
                .await
                {
                    stats.set_last_error(format!(
                        "KMS background reservation finalization fallback failed: {}",
                        work_err
                    ));
                }
            }
        }
    }
}

async fn process_reservation_mutation_work(
    hot_store: Arc<dyn HotMetadataStore>,
    kas_channels: KasEndpointBalancer,
    work: ReservationMutationWork,
    reservation_mutation_batch_size: usize,
    rpc_timeout: Duration,
) -> Result<(), Status> {
    match finalize_reservations(
        kas_channels,
        work.finalize_plans,
        work.reservation_ids,
        reservation_mutation_batch_size,
        rpc_timeout,
    )
    .await
    {
        Ok(()) => {}
        Err(err) if is_idempotent_reservation_finalize_error(&err) => {}
        Err(err) => return Err(err),
    }
    if let Some(intent_id) = work.intent_id {
        hot_store
            .mark_write_intent_reservations_finalized(intent_id)
            .await?;
    }
    Ok(())
}

fn build_reservation_mutations(
    finalize_plans: &[ReservationFinalizePlan],
    reservation_ids: &[String],
) -> (Vec<ReservationMutation>, Vec<ReservationMutation>) {
    let mut known = HashSet::new();
    let mut finalize_mutations = Vec::new();
    let mut release_mutations = Vec::new();
    for plan in finalize_plans {
        known.insert(plan.reservation_id.clone());
        if plan.placement_indexes.is_empty() {
            release_mutations.push(ReservationMutation {
                reservation_id: plan.reservation_id.clone(),
                placement_indexes: Vec::new(),
            });
        } else {
            finalize_mutations.push(ReservationMutation {
                reservation_id: plan.reservation_id.clone(),
                placement_indexes: plan.placement_indexes.clone(),
            });
        }
    }
    for reservation_id in reservation_ids {
        if known.insert(reservation_id.clone()) {
            release_mutations.push(ReservationMutation {
                reservation_id: reservation_id.clone(),
                placement_indexes: Vec::new(),
            });
        }
    }
    (finalize_mutations, release_mutations)
}

async fn send_reservation_mutation_batches(
    kas_channels: KasEndpointBalancer,
    finalize_mutations: &[ReservationMutation],
    release_mutations: &[ReservationMutation],
    reservation_mutation_batch_size: usize,
    rpc_timeout: Duration,
) -> Result<(), Status> {
    if finalize_mutations.is_empty() && release_mutations.is_empty() {
        return Ok(());
    }

    let routes = resolved_allocation_shard_routes(
        kas_channels.clone(),
        rpc_timeout.min(Duration::from_secs(5)),
        None,
    )
    .await;
    if routes.is_empty() {
        let mut client = kas_channels.client();
        send_finalize_mutation_batches(
            &mut client,
            finalize_mutations,
            reservation_mutation_batch_size,
            rpc_timeout,
        )
        .await?;
        send_release_mutation_batches(
            &mut client,
            release_mutations,
            reservation_mutation_batch_size,
            rpc_timeout,
        )
        .await?;
        return Ok(());
    }

    let route_map = allocation_shard_route_map(routes);
    let finalize_groups = group_mutations_by_route(finalize_mutations, &route_map);
    let release_groups = group_mutations_by_route(release_mutations, &route_map);
    let targeted_endpoints = finalize_groups
        .keys()
        .chain(release_groups.keys())
        .cloned()
        .collect::<HashSet<_>>();

    for endpoint in targeted_endpoints {
        let Some(route) = route_map.get(&endpoint) else {
            continue;
        };
        let mut client = KasClient::new(route.channel.clone())
            .max_decoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES);
        let finalize = finalize_groups
            .get(&endpoint)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let release = release_groups
            .get(&endpoint)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        send_finalize_mutation_batches(
            &mut client,
            finalize,
            reservation_mutation_batch_size,
            rpc_timeout,
        )
        .await?;
        send_release_mutation_batches(
            &mut client,
            release,
            reservation_mutation_batch_size,
            rpc_timeout,
        )
        .await?;
    }

    let fallback_finalize = unmatched_mutations(finalize_mutations, &route_map);
    let fallback_release = unmatched_mutations(release_mutations, &route_map);
    if !fallback_finalize.is_empty() || !fallback_release.is_empty() {
        for endpoint in kas_channels.ordered_endpoints() {
            let mut client = KasClient::new(endpoint.channel.clone())
                .max_decoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES);
            send_finalize_mutation_batches(
                &mut client,
                &fallback_finalize,
                reservation_mutation_batch_size,
                rpc_timeout,
            )
            .await?;
            send_release_mutation_batches(
                &mut client,
                &fallback_release,
                reservation_mutation_batch_size,
                rpc_timeout,
            )
            .await?;
        }
    }
    Ok(())
}

async fn finalize_intent_reservations(
    hot_store: Arc<dyn HotMetadataStore>,
    kas_channels: KasEndpointBalancer,
    intent: WriteIntent,
    reservation_mutation_batch_size: usize,
    rpc_timeout: Duration,
) -> Result<(), Status> {
    let finalize_plans = build_finalize_plans(&intent)?;
    match finalize_reservations(
        kas_channels,
        finalize_plans,
        intent.reservation_ids.clone(),
        reservation_mutation_batch_size,
        rpc_timeout,
    )
    .await
    {
        Ok(()) => {}
        Err(err) if is_idempotent_reservation_finalize_error(&err) => {}
        Err(err) => return Err(err),
    }
    hot_store
        .mark_write_intent_reservations_finalized(intent.intent_id.clone())
        .await?;
    Ok(())
}

fn is_idempotent_reservation_finalize_error(status: &Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && (status.message().contains("is not releasable")
            || status.message().contains("is not finalizable"))
}

/// FIX C (DESIGN_KAS_WRITE_SCALE.md §4/§7): whether a per-shard reserve error
/// means the route is now stale (a leadership change on the target KAS shard)
/// and the [`AllocationRouteCache`] must be invalidated for immediate
/// re-discovery rather than retried for the route TTL.
///
/// With the always-on epoch fence (#3) a demoted-but-reachable old leader does
/// NOT return `Unavailable`; it returns `Aborted` (its fenced commit hit an
/// advanced epoch) or `FailedPrecondition` (not-leader/superseded). Treating
/// only `Unavailable` as a re-route signal — as the original code did — would
/// keep routing reserves to that demoted leader until the TTL elapsed.
fn route_change_should_invalidate(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::Aborted | tonic::Code::FailedPrecondition
    )
}

#[derive(Clone)]
struct AllocationShardRoute {
    shard_id: String,
    endpoint: String,
    channel: Channel,
}

/// TTL cache for allocation-shard route discovery.
///
/// Before this, `discover_allocation_shard_routes` ran *uncached* on every
/// reserve (`reserve_object_write_window` at ~2605) and *again* inside
/// `reserve_stripe_batch -> resolved_allocation_shard_routes` (~3551), so a
/// single foreground reserve issued ~6 `list_service_instances` RPCs to KAS.
/// This memoizes the merged-and-resolved route set for a configurable TTL
/// (default ~5s) so steady-state reserves read routes from RAM.
///
/// Failover safety: a per-shard reserve that comes back `UNAVAILABLE` (e.g. a
/// demoted leader after a KAS failover) calls [`AllocationRouteCache::invalidate`]
/// so the *next* resolve re-discovers rather than silently retrying the stale
/// route. (See `reserve_stripe_batch_sharded`.)
#[derive(Clone)]
pub(crate) struct AllocationRouteCache {
    inner: Arc<AllocationRouteCacheInner>,
}

struct AllocationRouteCacheInner {
    // Cached, fully-resolved (discovered ∪ configured) route set + the instant
    // it was fetched. `None` => cold / explicitly invalidated.
    cached: Mutex<Option<CachedAllocationRoutes>>,
    // Single-flight refresh guard so a thundering herd of foreground reserves
    // does not all stampede KAS with discovery RPCs on a cold/expired cache.
    refresh_lock: tokio::sync::Mutex<()>,
    ttl: Duration,
    stats: Arc<KmsStats>,
}

#[derive(Clone)]
struct CachedAllocationRoutes {
    routes: Vec<AllocationShardRoute>,
    fetched_at: Instant,
}

impl AllocationRouteCache {
    pub(crate) fn new(ttl: Duration, stats: Arc<KmsStats>) -> Self {
        Self {
            inner: Arc::new(AllocationRouteCacheInner {
                cached: Mutex::new(None),
                refresh_lock: tokio::sync::Mutex::new(()),
                ttl,
                stats,
            }),
        }
    }

    fn fresh_snapshot(&self) -> Option<Vec<AllocationShardRoute>> {
        let guard = self.inner.cached.lock().unwrap();
        guard.as_ref().and_then(|cached| {
            if cached.fetched_at.elapsed() < self.inner.ttl {
                Some(cached.routes.clone())
            } else {
                None
            }
        })
    }

    /// Force the next [`resolve`](Self::resolve) to re-discover. Called on a
    /// per-shard `UNAVAILABLE` so a leadership change is picked up immediately
    /// instead of waiting out the TTL on a demoted leader.
    pub(crate) fn invalidate(&self) {
        *self.inner.cached.lock().unwrap() = None;
    }

    /// Return the resolved allocation-shard routes, served from the TTL cache
    /// when fresh and otherwise re-discovered under a single-flight guard.
    async fn resolve(
        &self,
        kas_channels: KasEndpointBalancer,
        rpc_timeout: Duration,
    ) -> Vec<AllocationShardRoute> {
        if let Some(routes) = self.fresh_snapshot() {
            self.inner.stats.record_route_cache_hit();
            return routes;
        }
        // Cold or expired: serialize discovery so concurrent reserves do not
        // each fan out their own ~6 RPCs.
        let _refresh = self.inner.refresh_lock.lock().await;
        // Re-check under the refresh lock: another task may have just refilled
        // it while we waited for the guard.
        if let Some(routes) = self.fresh_snapshot() {
            self.inner.stats.record_route_cache_hit();
            return routes;
        }
        self.inner.stats.record_route_cache_miss();
        let routes =
            resolved_allocation_shard_routes(kas_channels, rpc_timeout, Some(&self.inner.stats))
                .await;
        if !routes.is_empty() {
            *self.inner.cached.lock().unwrap() = Some(CachedAllocationRoutes {
                routes: routes.clone(),
                fetched_at: Instant::now(),
            });
        }
        routes
    }

    /// Count of currently-cached routes (fresh only), for the foreground
    /// shard-count decision. Does *not* issue RPCs.
    async fn shard_count(
        &self,
        kas_channels: KasEndpointBalancer,
        rpc_timeout: Duration,
    ) -> usize {
        self.resolve(kas_channels, rpc_timeout).await.len()
    }
}

async fn reserve_stripe_batch(
    kas_channels: KasEndpointBalancer,
    route_cache: &AllocationRouteCache,
    batch_size: usize,
    profile: &EcProfile,
    reservation_ttl_ms: u64,
    rpc_timeout: Duration,
    attempt_timeout: Duration,
    reservation_mutation_batch_size: usize,
) -> Result<Vec<PlacementReservationRecord>, Status> {
    if batch_size == 0 {
        return Ok(Vec::new());
    }

    // Route discovery is served from the TTL cache. This is the
    // background-refill / single-shard-fallback path; the foreground reserve
    // resolves shard count via the same cache before deciding to use the pool.
    let routes = route_cache
        .resolve(kas_channels.clone(), rpc_timeout.min(Duration::from_secs(5)))
        .await;
    if routes.len() > 1 {
        return reserve_stripe_batch_sharded(
            kas_channels,
            route_cache,
            routes,
            batch_size,
            profile,
            reservation_ttl_ms,
            rpc_timeout,
            attempt_timeout,
            reservation_mutation_batch_size,
        )
        .await;
    }

    reserve_stripe_batch_via_endpoints(
        kas_channels,
        batch_size,
        profile,
        reservation_ttl_ms,
        rpc_timeout,
        attempt_timeout,
        reservation_mutation_batch_size,
        String::new(),
    )
    .await
}

async fn discover_allocation_shard_routes(
    kas_channels: KasEndpointBalancer,
    rpc_timeout: Duration,
    stats: Option<&KmsStats>,
) -> Result<Vec<AllocationShardRoute>, Status> {
    let now_ms = now_unix_ms();
    let mut last_err = None;
    let mut fresh_routes = HashMap::new();
    let mut stale_routes = HashMap::new();
    // Count the list_service_instances RPCs this single resolution
    // issued so the lab can measure route-discovery amplification per reserve.
    let mut rpc_count = 0usize;
    for endpoint in kas_channels.ordered_endpoints() {
        let mut client = KasClient::new(endpoint.channel.clone())
            .max_decoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES);
        rpc_count += 1;
        let service_instances = match timeout(
            rpc_timeout,
            client.list_service_instances(Request::new(ListServiceInstancesRequest {
                service_kind: ServiceKind::Kas as i32,
                node_id: String::new(),
                limit: 1024,
            })),
        )
        .await
        {
            Ok(Ok(reply)) => reply.into_inner().instances,
            Ok(Err(err)) => {
                last_err = Some(err);
                continue;
            }
            Err(_) => {
                last_err = Some(Status::deadline_exceeded(format!(
                    "KMS shard discovery list_service_instances timed out after {} ms",
                    rpc_timeout.as_millis()
                )));
                continue;
            }
        };
        merge_discovered_allocation_shard_routes(
            &kas_channels,
            &service_instances,
            now_ms,
            &mut fresh_routes,
            &mut stale_routes,
        );
        if fresh_routes.len() >= kas_channels.endpoint_count() {
            break;
        }
    }
    if let Some(stats) = stats {
        stats.record_route_discovery(rpc_count);
    }

    let routes = finalize_discovered_allocation_shard_routes(fresh_routes, stale_routes);
    if !routes.is_empty() {
        return Ok(routes);
    }

    Err(last_err
        .unwrap_or_else(|| Status::unavailable("KMS could not discover allocator shard routes")))
}

fn merge_discovered_allocation_shard_routes(
    kas_channels: &KasEndpointBalancer,
    service_instances: &[keinctl::proto::ServiceInstanceRecord],
    now_ms: u64,
    fresh_routes: &mut HashMap<String, AllocationShardRoute>,
    stale_routes: &mut HashMap<String, AllocationShardRoute>,
) {
    for instance in service_instances {
        let shard_id = instance.instance_label.trim();
        if shard_id.is_empty() {
            continue;
        }
        let Some(channel) = kas_channels.channel_for_endpoint(&instance.endpoint) else {
            continue;
        };
        let route = AllocationShardRoute {
            shard_id: shard_id.to_string(),
            endpoint: instance.endpoint.clone(),
            channel,
        };
        if service_instance_is_stale(instance, now_ms) {
            stale_routes
                .entry(route.shard_id.clone())
                .or_insert(route);
        } else {
            fresh_routes
                .entry(route.shard_id.clone())
                .or_insert(route);
        }
    }
}

fn finalize_discovered_allocation_shard_routes(
    mut fresh_routes: HashMap<String, AllocationShardRoute>,
    stale_routes: HashMap<String, AllocationShardRoute>,
) -> Vec<AllocationShardRoute> {
    for (shard_id, route) in stale_routes {
        fresh_routes.entry(shard_id).or_insert(route);
    }
    let mut routes = fresh_routes.into_values().collect::<Vec<_>>();
    routes.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    routes
}

fn merge_configured_allocation_shard_routes(
    discovered_routes: Vec<AllocationShardRoute>,
    configured_routes: Vec<AllocationShardRoute>,
) -> Vec<AllocationShardRoute> {
    let mut routes_by_endpoint = discovered_routes
        .into_iter()
        .map(|route| {
            (
                normalize_service_endpoint(&route.endpoint).to_string(),
                route,
            )
        })
        .collect::<HashMap<_, _>>();
    for route in configured_routes {
        routes_by_endpoint
            .entry(normalize_service_endpoint(&route.endpoint).to_string())
            .or_insert(route);
    }
    let mut routes = routes_by_endpoint.into_values().collect::<Vec<_>>();
    routes.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    routes
}

async fn resolved_allocation_shard_routes(
    kas_channels: KasEndpointBalancer,
    rpc_timeout: Duration,
    stats: Option<&KmsStats>,
) -> Vec<AllocationShardRoute> {
    let configured_routes = configured_allocation_shard_routes(&kas_channels);
    let discovered_routes = discover_allocation_shard_routes(kas_channels, rpc_timeout, stats)
        .await
        .unwrap_or_default();
    merge_configured_allocation_shard_routes(discovered_routes, configured_routes)
}

fn configured_allocation_shard_routes(
    kas_channels: &KasEndpointBalancer,
) -> Vec<AllocationShardRoute> {
    kas_channels
        .endpoints
        .iter()
        .enumerate()
        .map(|(index, endpoint)| AllocationShardRoute {
            shard_id: format!("alloc-shard-{index:02}"),
            endpoint: endpoint.endpoint.clone(),
            channel: endpoint.channel.clone(),
        })
        .collect()
}

fn service_instance_is_stale(
    instance: &keinctl::proto::ServiceInstanceRecord,
    now_ms: u64,
) -> bool {
    let heartbeat_interval_ms = instance.heartbeat_interval_ms.max(1_000);
    let heartbeat_age_ms = now_ms.saturating_sub(instance.heartbeat_at_unix_ms);
    heartbeat_age_ms > heartbeat_interval_ms.saturating_mul(3)
}

fn normalize_service_endpoint(endpoint: &str) -> &str {
    endpoint
        .trim()
        .trim_end_matches('/')
        .strip_prefix("http://")
        .or_else(|| {
            endpoint
                .trim()
                .trim_end_matches('/')
                .strip_prefix("https://")
        })
        .unwrap_or_else(|| endpoint.trim().trim_end_matches('/'))
}

fn collect_reservation_ids_for_release(reservations: &[PlacementReservationRecord]) -> Vec<String> {
    let mut collected = Vec::new();
    let mut seen = HashSet::new();
    for reservation in reservations {
        let mut saw_placement_reservation = false;
        for placement in &reservation.placements {
            let reservation_id = placement.reservation_id.trim();
            if reservation_id.is_empty() {
                continue;
            }
            saw_placement_reservation = true;
            if seen.insert(reservation_id.to_string()) {
                collected.push(reservation_id.to_string());
            }
        }
        if saw_placement_reservation {
            continue;
        }
        let reservation_id = reservation.reservation_id.trim();
        if reservation_id.is_empty() {
            continue;
        }
        if seen.insert(reservation_id.to_string()) {
            collected.push(reservation_id.to_string());
        }
    }
    collected
}

fn reservation_shard_id(reservation_id: &str) -> Option<&str> {
    let (shard_id, _) = reservation_id.split_once('/')?;
    (!shard_id.trim().is_empty()).then_some(shard_id)
}

fn allocation_shard_route_map(
    routes: Vec<AllocationShardRoute>,
) -> HashMap<String, AllocationShardRoute> {
    routes
        .into_iter()
        .map(|route| {
            (
                normalize_service_endpoint(&route.endpoint).to_string(),
                route,
            )
        })
        .collect()
}

fn group_mutations_by_route(
    mutations: &[ReservationMutation],
    route_map: &HashMap<String, AllocationShardRoute>,
) -> HashMap<String, Vec<ReservationMutation>> {
    let shard_to_endpoint = route_map
        .values()
        .map(|route| {
            (
                route.shard_id.clone(),
                normalize_service_endpoint(&route.endpoint).to_string(),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut grouped = HashMap::<String, Vec<ReservationMutation>>::new();
    for mutation in mutations {
        let Some(shard_id) = reservation_shard_id(&mutation.reservation_id) else {
            continue;
        };
        let Some(endpoint) = shard_to_endpoint.get(shard_id) else {
            continue;
        };
        grouped
            .entry(endpoint.clone())
            .or_default()
            .push(mutation.clone());
    }
    grouped
}

fn unmatched_mutations(
    mutations: &[ReservationMutation],
    route_map: &HashMap<String, AllocationShardRoute>,
) -> Vec<ReservationMutation> {
    let shard_ids = route_map
        .values()
        .map(|route| route.shard_id.as_str())
        .collect::<HashSet<_>>();
    mutations
        .iter()
        .filter(|mutation| {
            reservation_shard_id(&mutation.reservation_id)
                .map(|shard_id| !shard_ids.contains(shard_id))
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}

fn shard_fragment_distributions(
    fragment_count: usize,
    routes: &[AllocationShardRoute],
) -> Result<Vec<Vec<(AllocationShardRoute, usize)>>, Status> {
    if routes.is_empty() {
        return Err(Status::resource_exhausted(
            "no allocation shards available for reservation",
        ));
    }
    let shard_count = routes.len();
    let base = fragment_count / shard_count;
    let extras = fragment_count % shard_count;
    let extra_assignments = choose_extra_shards(shard_count, extras);
    let mut plans = Vec::with_capacity(extra_assignments.len().max(1));
    for extra_indices in extra_assignments {
        let extra_index_set = extra_indices.into_iter().collect::<HashSet<_>>();
        let plan = routes
            .iter()
            .enumerate()
            .filter_map(|(index, route)| {
                let count = base + usize::from(extra_index_set.contains(&index));
                (count > 0).then(|| (route.clone(), count))
            })
            .collect::<Vec<_>>();
        plans.push(plan);
    }
    Ok(plans)
}

fn choose_extra_shards(shard_count: usize, extras: usize) -> Vec<Vec<usize>> {
    if extras == 0 {
        return vec![Vec::new()];
    }
    let mut selected = Vec::with_capacity(extras);
    let mut combinations = Vec::new();
    choose_extra_shards_rec(0, shard_count, extras, &mut selected, &mut combinations);
    combinations
}

fn choose_extra_shards_rec(
    start_index: usize,
    shard_count: usize,
    remaining: usize,
    selected: &mut Vec<usize>,
    combinations: &mut Vec<Vec<usize>>,
) {
    if remaining == 0 {
        combinations.push(selected.clone());
        return;
    }
    let max_start = shard_count.saturating_sub(remaining);
    for index in start_index..=max_start {
        selected.push(index);
        choose_extra_shards_rec(
            index + 1,
            shard_count,
            remaining - 1,
            selected,
            combinations,
        );
        selected.pop();
    }
}

fn format_shard_plan(shard_plan: &[(AllocationShardRoute, usize)]) -> String {
    shard_plan
        .iter()
        .map(|(route, count)| format!("{}={}", route.shard_id, count))
        .collect::<Vec<_>>()
        .join(",")
}

fn merge_sharded_reservation_batches(
    fragment_count: usize,
    batch_size: usize,
    partial_batches: &[(AllocationShardRoute, Vec<PlacementReservationRecord>)],
) -> Result<Vec<PlacementReservationRecord>, Status> {
    let mut merged = Vec::with_capacity(batch_size);
    for stripe_index in 0..batch_size {
        let mut placements = Vec::with_capacity(fragment_count);
        // A merged full-stripe reservation is only usable while EVERY per-shard
        // fragment is still valid, so its cached expiry is the EARLIEST (min)
        // fragment expiry, NOT the max. Treat 0 as "no expiry" (+infinity) so it
        // does not collapse the min; if every fragment is 0 the merged record is 0.
        let mut expires_at_unix_ms: Option<u64> = None;
        for (_, reservations) in partial_batches {
            let reservation = reservations.get(stripe_index).ok_or_else(|| {
                Status::internal(format!(
                    "allocator shard reservation batch is missing stripe {}",
                    stripe_index
                ))
            })?;
            if reservation.expires_at_unix_ms != 0 {
                expires_at_unix_ms = Some(match expires_at_unix_ms {
                    Some(current) => current.min(reservation.expires_at_unix_ms),
                    None => reservation.expires_at_unix_ms,
                });
            }
            for (placement_index, placement) in reservation.placements.iter().enumerate() {
                let merged_placement = PlacementReservation {
                    target_id: placement.target_id.clone(),
                    endpoint: placement.endpoint.clone(),
                    granule_index: placement.granule_index,
                    fragment_index: placements.len() as u32,
                    reservation_id: if placement.reservation_id.is_empty() {
                        reservation.reservation_id.clone()
                    } else {
                        placement.reservation_id.clone()
                    },
                    reservation_placement_index: if placement.reservation_id.is_empty() {
                        placement_index as u32
                    } else {
                        placement.reservation_placement_index
                    },
                };
                if merged_placement.reservation_id.is_empty() {
                    return Err(Status::internal(
                        "merged reservation placement is missing reservation_id",
                    ));
                }
                placements.push(merged_placement);
            }
        }
        if placements.len() != fragment_count {
            return Err(Status::internal(format!(
                "merged reservation stripe {} produced {} placements, expected {}",
                stripe_index,
                placements.len(),
                fragment_count
            )));
        }
        merged.push(PlacementReservationRecord {
            reservation_id: format!("merged-{}", Uuid::new_v4()),
            state: keinctl::proto::ReservationState::Reserved as i32,
            placements,
            // None (all fragments had no expiry) -> 0 = "no expiry".
            expires_at_unix_ms: expires_at_unix_ms.unwrap_or(0),
        });
    }
    Ok(merged)
}

async fn reserve_stripe_batch_sharded(
    kas_channels: KasEndpointBalancer,
    route_cache: &AllocationRouteCache,
    routes: Vec<AllocationShardRoute>,
    batch_size: usize,
    profile: &EcProfile,
    reservation_ttl_ms: u64,
    rpc_timeout: Duration,
    attempt_timeout: Duration,
    reservation_mutation_batch_size: usize,
) -> Result<Vec<PlacementReservationRecord>, Status> {
    let fragment_count = profile.fragment_count() as usize;
    let shard_plans = shard_fragment_distributions(fragment_count, &routes)?;
    let mut last_error = None;
    for shard_plan in shard_plans {
        // Fan out the per-shard reserves concurrently instead of
        // awaiting each before the next. Each shard leader holds its own lease,
        // so the 3 reserves overlap rather than serialize (~3x latency cut on a
        // 3-shard cluster). Partial-failure compensation below is preserved:
        // any shard that DID succeed is released before we move to the next
        // candidate plan.
        let shard_futures = shard_plan.iter().map(|(route, shard_fragment_count)| {
            let route = route.clone();
            let shard_fragment_count = *shard_fragment_count;
            let kas_channels = kas_channels.clone();
            async move {
                let result = reserve_stripe_batch_via_channels(
                    vec![route.channel.clone()],
                    1,
                    batch_size,
                    shard_fragment_count,
                    profile.failure_domain,
                    reservation_ttl_ms,
                    rpc_timeout,
                    attempt_timeout,
                    reservation_mutation_batch_size,
                    route.shard_id.clone(),
                    kas_channels,
                )
                .await;
                (route, result)
            }
        });
        let shard_results = futures_util::future::join_all(shard_futures).await;

        // Partition the fan-out into successes and failures. Successes must be
        // released if ANY shard in the plan failed (compensating action), so we
        // collect their reservation IDs up front.
        let mut partial_batches = Vec::with_capacity(shard_plan.len());
        let mut reservation_ids_for_release = Vec::new();
        let mut seen_reservation_ids = HashSet::new();
        let mut plan_error: Option<String> = None;
        let mut saw_route_change = false;
        for (route, result) in shard_results {
            match result {
                Ok(reservations) => {
                    for reservation_id in collect_reservation_ids_for_release(&reservations) {
                        if seen_reservation_ids.insert(reservation_id.clone()) {
                            reservation_ids_for_release.push(reservation_id);
                        }
                    }
                    partial_batches.push((route, reservations));
                }
                Err(err) => {
                    // A demoted/superseded leader after a KAS failover surfaces
                    // here and we must force a route re-discovery so the next
                    // reserve does not retry the stale route for the TTL window
                    // (design §4/§7). Three signals mean "this is no longer the
                    // shard leader, re-route now":
                    //   * UNAVAILABLE   — leader-resident wrapper refused (not leader);
                    //   * ABORTED       — the always-on epoch fence aborted the commit
                    //                     (a newer leader bumped the epoch);
                    //   * FAILED_PRECONDITION — superseded/not-leader precondition.
                    // FIX C: invalidating on ABORTED/FAILED_PRECONDITION (not just
                    // UNAVAILABLE) stops KMS burning seconds retrying a reachable but
                    // demoted leader until the route TTL expires.
                    if route_change_should_invalidate(&err) {
                        saw_route_change = true;
                    }
                    if plan_error.is_none() {
                        plan_error = Some(format!(
                            "allocator shard plan {} failed on {} via {}: {}",
                            format_shard_plan(&shard_plan),
                            route.shard_id,
                            route.endpoint,
                            err
                        ));
                    }
                }
            }
        }

        if let Some(err) = plan_error {
            // Partial failure: release every shard reservation that DID succeed
            // before trying the next candidate plan.
            if !reservation_ids_for_release.is_empty() {
                let _ = release_reservation_ids(
                    kas_channels.clone(),
                    &reservation_ids_for_release,
                    reservation_mutation_batch_size,
                    rpc_timeout.saturating_mul(3),
                )
                .await;
            }
            if saw_route_change {
                route_cache.invalidate();
            }
            last_error = Some(err);
            continue;
        }

        match merge_sharded_reservation_batches(fragment_count, batch_size, &partial_batches) {
            Ok(merged) => return Ok(merged),
            Err(err) => {
                if !reservation_ids_for_release.is_empty() {
                    let _ = release_reservation_ids(
                        kas_channels.clone(),
                        &reservation_ids_for_release,
                        reservation_mutation_batch_size,
                        rpc_timeout.saturating_mul(3),
                    )
                    .await;
                }
                last_error = Some(format!(
                    "allocator shard plan {} failed to merge: {}",
                    format_shard_plan(&shard_plan),
                    err
                ));
            }
        }
    }

    Err(Status::unavailable(last_error.unwrap_or_else(|| {
        "allocator shards could not satisfy the reservation request".to_string()
    })))
}

async fn reserve_stripe_batch_via_endpoints(
    kas_channels: KasEndpointBalancer,
    batch_size: usize,
    profile: &EcProfile,
    reservation_ttl_ms: u64,
    rpc_timeout: Duration,
    attempt_timeout: Duration,
    reservation_mutation_batch_size: usize,
    allocation_shard_id: String,
) -> Result<Vec<PlacementReservationRecord>, Status> {
    let fragment_count = profile.fragment_count() as usize;
    let channels = kas_channels.ordered_channels();
    let attempts = kas_channels.endpoint_count().max(1);
    reserve_stripe_batch_via_channels(
        channels,
        attempts,
        batch_size,
        fragment_count,
        profile.failure_domain,
        reservation_ttl_ms,
        rpc_timeout,
        attempt_timeout,
        reservation_mutation_batch_size,
        allocation_shard_id,
        kas_channels,
    )
    .await
}

async fn reserve_stripe_batch_via_channels(
    channels: Vec<Channel>,
    attempts: usize,
    batch_size: usize,
    fragment_count: usize,
    failure_domain: i32,
    reservation_ttl_ms: u64,
    rpc_timeout: Duration,
    attempt_timeout: Duration,
    reservation_mutation_batch_size: usize,
    allocation_shard_id: String,
    kas_channels: KasEndpointBalancer,
) -> Result<Vec<PlacementReservationRecord>, Status> {
    let per_attempt_timeout = attempt_timeout
        .min(rpc_timeout)
        .max(Duration::from_millis(500));
    let mut acquired = Vec::with_capacity(batch_size);
    let mut acquired_ids = HashSet::with_capacity(batch_size);
    let mut last_err = None;
    let retry_deadline = Instant::now() + rpc_timeout;
    let mut retry_rounds = 0u32;

    loop {
        let mut made_progress = false;
        let mut saw_empty = false;
        let mut saw_retryable_err = false;

        for channel in channels.iter().take(attempts).cloned() {
            let remaining = batch_size.saturating_sub(acquired.len());
            if remaining == 0 {
                return Ok(acquired);
            }
            let mut client = KasClient::new(channel)
                .max_decoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(KAS_GRPC_MAX_MESSAGE_BYTES);
            match timeout(
                per_attempt_timeout,
                client.reserve_stripe_batch(ReserveStripeBatchRequest {
                    batch_size: remaining as u32,
                    fragment_count: fragment_count as u32,
                    failure_domain,
                    excluded_target_ids: Vec::new(),
                    reservation_ttl_ms,
                    allocation_shard_id: allocation_shard_id.clone(),
                }),
            )
            .await
            {
                Ok(Ok(reply)) => {
                    let reservations = reply.into_inner().reservations;
                    if !reservations.is_empty() {
                        for reservation in reservations {
                            if acquired_ids.insert(reservation.reservation_id.clone()) {
                                acquired.push(reservation);
                                made_progress = true;
                            }
                        }
                        if acquired.len() >= batch_size {
                            return Ok(acquired);
                        }
                        saw_empty = true;
                        last_err = Some(Status::unavailable(format!(
                            "KAS returned only {}/{} stripe placements for reserve batch",
                            acquired.len(),
                            batch_size
                        )));
                        continue;
                    }
                    saw_empty = true;
                    last_err = Some(Status::unavailable(
                        "KAS returned no stripe placements for reserve batch",
                    ));
                }
                Ok(Err(err)) => {
                    saw_retryable_err = saw_retryable_err || is_retryable_kas_reserve_status(&err);
                    last_err = Some(err);
                }
                Err(_) => {
                    let timeout_status = Status::deadline_exceeded(format!(
                        "KMS->KAS ReserveStripeBatch attempt timed out after {} ms",
                        per_attempt_timeout.as_millis()
                    ));
                    saw_retryable_err = true;
                    last_err = Some(timeout_status);
                }
            }
        }

        if acquired.len() >= batch_size {
            return Ok(acquired);
        }

        if made_progress {
            retry_rounds = 0;
            continue;
        }

        if (!saw_empty && !saw_retryable_err) || Instant::now() >= retry_deadline {
            break;
        }

        let base_backoff_ms: u64 = if saw_retryable_err { 50 } else { 25 };
        let backoff_ms = base_backoff_ms
            .saturating_mul(1u64 << retry_rounds.min(3))
            .min(if saw_retryable_err { 400 } else { 200 });
        sleep(Duration::from_millis(backoff_ms)).await;
        retry_rounds = retry_rounds.saturating_add(1);
    }

    if !acquired.is_empty() {
        let acquired_ids = acquired
            .iter()
            .map(|reservation| reservation.reservation_id.clone())
            .collect::<Vec<_>>();
        let _ = release_reservation_ids(
            kas_channels.clone(),
            &acquired_ids,
            reservation_mutation_batch_size,
            rpc_timeout.saturating_mul(3),
        )
        .await;
    }

    Err(last_err.unwrap_or_else(|| {
        Status::deadline_exceeded(format!(
            "KMS->KAS ReserveStripeBatch timed out after {} ms",
            rpc_timeout.as_millis()
        ))
    }))
}

fn is_retryable_kas_reserve_status(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Aborted | tonic::Code::DeadlineExceeded | tonic::Code::Unavailable
    ) || {
        let message = status.message();
        message.contains("[40001]")
            || message.contains("TransactionRetry")
            || message.contains("WriteTooOld")
            || message.contains("ABORT_REASON_PUSHER_ABORTED")
    }
}

async fn reserve_replacement_placement(
    kas_channels: KasEndpointBalancer,
    replacement_count: usize,
    profile: &EcProfile,
    excluded_target_ids: Vec<String>,
    reservation_ttl_ms: u64,
    required_target_ids: Vec<String>,
    rpc_timeout: Duration,
) -> Result<PlacementReservationRecord, Status> {
    let mut client = kas_channels.client();
    let reply = timeout(
        rpc_timeout,
        client.reserve_replacement_placement(ReserveReplacementPlacementRequest {
            reservation_id: format!("replace-{}", Uuid::new_v4()),
            replacement_count: replacement_count as u32,
            failure_domain: profile.failure_domain,
            excluded_target_ids,
            reservation_ttl_ms,
            required_target_ids,
        }),
    )
    .await
    .map_err(|_| {
        Status::deadline_exceeded(format!(
            "KMS->KAS ReserveReplacementPlacement timed out after {} ms",
            rpc_timeout.as_millis()
        ))
    })??
    .into_inner();
    reply
        .reservation
        .ok_or_else(|| Status::internal("KAS did not return replacement reservation"))
}

async fn finalize_reservations(
    kas_channels: KasEndpointBalancer,
    finalize_plans: Vec<ReservationFinalizePlan>,
    reservation_ids: Vec<String>,
    reservation_mutation_batch_size: usize,
    rpc_timeout: Duration,
) -> Result<(), Status> {
    let (finalize_mutations, release_mutations) =
        build_reservation_mutations(&finalize_plans, &reservation_ids);
    send_reservation_mutation_batches(
        kas_channels,
        &finalize_mutations,
        &release_mutations,
        reservation_mutation_batch_size,
        rpc_timeout,
    )
    .await
}

async fn release_reservation_ids(
    kas_channels: KasEndpointBalancer,
    reservation_ids: &[String],
    reservation_mutation_batch_size: usize,
    rpc_timeout: Duration,
) -> Result<(), Status> {
    let release_mutations = reservation_ids
        .iter()
        .map(|reservation_id| ReservationMutation {
            reservation_id: reservation_id.clone(),
            placement_indexes: Vec::new(),
        })
        .collect::<Vec<_>>();
    send_reservation_mutation_batches(
        kas_channels,
        &[],
        &release_mutations,
        reservation_mutation_batch_size,
        rpc_timeout,
    )
    .await
}

async fn send_finalize_mutation_batches(
    client: &mut KasClient<Channel>,
    mutations: &[ReservationMutation],
    batch_size: usize,
    rpc_timeout: Duration,
) -> Result<(), Status> {
    if mutations.is_empty() {
        return Ok(());
    }
    for chunk in mutations.chunks(batch_size.max(1)) {
        timeout(
            rpc_timeout,
            client.finalize_reservations_batch(FinalizeReservationsBatchRequest {
                mutations: chunk.to_vec(),
            }),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded(format!(
                "KMS->KAS FinalizeReservationsBatch timed out after {} ms",
                rpc_timeout.as_millis()
            ))
        })??;
    }
    Ok(())
}

async fn send_release_mutation_batches(
    client: &mut KasClient<Channel>,
    mutations: &[ReservationMutation],
    batch_size: usize,
    rpc_timeout: Duration,
) -> Result<(), Status> {
    if mutations.is_empty() {
        return Ok(());
    }
    for chunk in mutations.chunks(batch_size.max(1)) {
        timeout(
            rpc_timeout,
            client.release_reservations_batch(ReleaseReservationsBatchRequest {
                mutations: chunk.to_vec(),
            }),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded(format!(
                "KMS->KAS ReleaseReservationsBatch timed out after {} ms",
                rpc_timeout.as_millis()
            ))
        })??;
    }
    Ok(())
}

async fn reclaim_target_granules(
    kas_channels: KasEndpointBalancer,
    granules: Vec<TargetGranule>,
    rpc_timeout: Duration,
) -> Result<u64, Status> {
    // FIX A (DESIGN_KAS_WRITE_SCALE.md §3 #2) — KMS-side routing, PARTIAL.
    //
    // reclaim mutates allocator free spans, so it MUST land on the owning
    // shard's KAS leader. This still uses the round-robin `kas_channels.client()`
    // (NOT shard-aware): a `TargetGranule` carries only `target_id`, and KMS has
    // no target -> allocation_shard_id map here, so resolving the owning shard
    // would need either a target->shard lookup cache or a shard hint on the
    // granule — a larger change deferred to a follow-up.
    //
    // This is SAFE today because the KAS side now runs reclaim under the
    // allocator mutation lease and persists `EpochFence::Leased` (see
    // kas/fdb_store.rs::reclaim_target_granules): a reclaim that lands on a
    // non-leader / wrong-shard KAS deterministically ABORTS (no held epoch /
    // epoch mismatch) instead of clobbering the rightful leader's committed
    // reservations. The cost of the misroute is a failed reclaim (surfaced as a
    // cleanup error and retried), not corruption.
    //
    // TODO(write-scale): route ReclaimTargetGranules shard-aware to the owning
    // shard's KAS leader via the AllocationRouteCache once a target->shard
    // resolution exists, so reclaim succeeds first-try instead of relying on the
    // fence to reject misroutes.
    let mut client = kas_channels.client();
    let reply = timeout(
        rpc_timeout,
        client.reclaim_target_granules(ReclaimTargetGranulesRequest { granules }),
    )
    .await
    .map_err(|_| {
        Status::deadline_exceeded(format!(
            "KMS->KAS ReclaimTargetGranules timed out after {} ms",
            rpc_timeout.as_millis()
        ))
    })??
    .into_inner();
    Ok(reply.reclaimed_granules)
}

async fn cleanup_deleted_object_fragments(
    deleted_versions: &[StoreDeletedObjectVersion],
) -> DeleteCleanupResult {
    let mut result = DeleteCleanupResult {
        cleanup_complete: true,
        ..DeleteCleanupResult::default()
    };
    let mut sessions = HashMap::new();

    for deleted_version in deleted_versions {
        for stripe in &deleted_version.manifest.stripes {
            for fragment in &stripe.fragments {
                result.fragment_delete_attempts = result.fragment_delete_attempts.saturating_add(1);
                let session = match get_delete_session(&mut sessions, &fragment.endpoint).await {
                    Ok(session) => session,
                    Err(_) => {
                        result.cleanup_complete = false;
                        continue;
                    }
                };
                match session.delete_chunk(&fragment.chunk_id).await {
                    Ok(true) => {
                        result.fragment_delete_successes =
                            result.fragment_delete_successes.saturating_add(1);
                        result.granules.push(TargetGranule {
                            target_id: fragment.target_id.clone(),
                            granule_index: fragment.granule_index,
                        });
                    }
                    Ok(false) => {
                        result.cleanup_complete = false;
                    }
                    Err(_) => {
                        result.cleanup_complete = false;
                    }
                }
            }
        }
    }

    let mut seen = HashSet::new();
    result
        .granules
        .retain(|granule| seen.insert((granule.target_id.clone(), granule.granule_index)));
    result
}

async fn get_delete_session<'a>(
    sessions: &'a mut HashMap<String, TargetDeleteSession>,
    endpoint: &str,
) -> Result<&'a TargetDeleteSession, Status> {
    if !sessions.contains_key(endpoint) {
        let session = TargetDeleteSession::connect(endpoint).await?;
        sessions.insert(endpoint.to_string(), session);
    }
    sessions.get(endpoint).ok_or_else(|| {
        Status::internal(format!(
            "target delete session for endpoint {} disappeared after connect",
            endpoint
        ))
    })
}

fn proto_deleted_object_version(value: &StoreDeletedObjectVersion) -> DeletedObjectVersion {
    DeletedObjectVersion {
        version_id: value.manifest.version_id.clone(),
        logical_length_bytes: value.manifest.logical_length_bytes,
        stripe_count: value.manifest.stripes.len() as u32,
        fragment_count: value
            .manifest
            .stripes
            .iter()
            .map(|stripe| stripe.fragments.len() as u32)
            .sum(),
    }
}

async fn set_target_state(
    kas_channels: KasEndpointBalancer,
    target_id: &str,
    lifecycle_state: TargetLifecycleState,
    rpc_timeout: Duration,
) -> Result<keinctl::proto::TargetRecord, Status> {
    let mut client = kas_channels.client();
    let reply = timeout(
        rpc_timeout,
        client.set_target_state(SetTargetStateRequest {
            target_id: target_id.to_string(),
            lifecycle_state: lifecycle_state as i32,
        }),
    )
    .await
    .map_err(|_| {
        Status::deadline_exceeded(format!(
            "KMS->KAS SetTargetState timed out after {} ms",
            rpc_timeout.as_millis()
        ))
    })??
    .into_inner();
    reply
        .target
        .ok_or_else(|| Status::internal("KAS did not return target after SetTargetState"))
}

async fn list_active_targets(
    kas_channels: KasEndpointBalancer,
    rpc_timeout: Duration,
) -> Result<HashSet<String>, Status> {
    let mut client = kas_channels.client();
    let reply = timeout(rpc_timeout, client.list_targets(ListTargetsRequest {}))
        .await
        .map_err(|_| {
            Status::deadline_exceeded(format!(
                "KMS->KAS ListTargets timed out after {} ms",
                rpc_timeout.as_millis()
            ))
        })??
        .into_inner();
    Ok(reply
        .targets
        .into_iter()
        .filter(|target| {
            target.healthy && target.lifecycle_state == TargetLifecycleState::Active as i32
        })
        .map(|target| target.target_id)
        .collect())
}

async fn get_target_record(
    kas_channels: KasEndpointBalancer,
    target_id: &str,
    rpc_timeout: Duration,
) -> Result<keinctl::proto::TargetRecord, Status> {
    let mut client = kas_channels.client();
    let reply = timeout(rpc_timeout, client.list_targets(ListTargetsRequest {}))
        .await
        .map_err(|_| {
            Status::deadline_exceeded(format!(
                "KMS->KAS ListTargets timed out after {} ms",
                rpc_timeout.as_millis()
            ))
        })??
        .into_inner();
    reply
        .targets
        .into_iter()
        .find(|target| target.target_id == target_id)
        .ok_or_else(|| Status::not_found(format!("unknown target {}", target_id)))
}

async fn target_endpoint_reachable(endpoint: &str) -> Result<bool, Status> {
    let uri: Uri = endpoint.parse().map_err(|err| {
        Status::invalid_argument(format!(
            "target endpoint URI `{endpoint}` is invalid for recovery probe: {err}"
        ))
    })?;
    let authority = uri.authority().ok_or_else(|| {
        Status::invalid_argument("target endpoint must include host:port for recovery probe")
    })?;
    let host = authority.host().to_string();
    let port = authority.port_u16().unwrap_or(80);
    match timeout(
        TARGET_RECOVER_CONNECT_TIMEOUT,
        TcpStream::connect((host.as_str(), port)),
    )
    .await
    {
        Ok(Ok(_)) => Ok(true),
        Ok(Err(_)) => Ok(false),
        Err(_) => Ok(false),
    }
}

impl TargetDeleteSession {
    async fn connect(endpoint: &str) -> Result<Self, Status> {
        let uri: Uri = endpoint.parse().map_err(|err| {
            Status::invalid_argument(format!(
                "target endpoint URI `{endpoint}` is invalid for delete cleanup: {err}"
            ))
        })?;
        let authority = uri.authority().ok_or_else(|| {
            Status::invalid_argument("target endpoint must include host:port for delete cleanup")
        })?;
        let host = authority.host().to_string();
        let port = authority.port_u16().unwrap_or(80);
        let socket = timeout(
            TARGET_DELETE_CONNECT_TIMEOUT,
            TcpStream::connect((host.as_str(), port)),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded(format!(
                "KMS target delete connect to {}:{} timed out after {} ms",
                host,
                port,
                TARGET_DELETE_CONNECT_TIMEOUT.as_millis()
            ))
        })?
        .map_err(|err| {
            Status::unavailable(format!(
                "KMS could not connect to target {}:{} for delete cleanup: {}",
                host, port, err
            ))
        })?;
        socket.set_nodelay(true).map_err(|err| {
            Status::internal(format!(
                "KMS connected to target {} but could not enable TCP_NODELAY: {}",
                endpoint, err
            ))
        })?;

        let mut builder = h2::client::Builder::new();
        builder
            .initial_window_size(TARGET_DELETE_INITIAL_WINDOW_BYTES)
            .initial_connection_window_size(TARGET_DELETE_INITIAL_CONNECTION_WINDOW_BYTES)
            .max_frame_size(TARGET_DELETE_MAX_FRAME_BYTES)
            .max_concurrent_streams(TARGET_DELETE_MAX_CONCURRENT_STREAMS);
        let (client, connection) = builder.handshake(socket).await.map_err(|err| {
            Status::unavailable(format!(
                "KMS HTTP/2 handshake with target {} failed during delete cleanup: {}",
                endpoint, err
            ))
        })?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    async fn delete_chunk(&self, chunk_id: &[u8]) -> Result<bool, Status> {
        let uri = format!(
            "{}{}",
            self.endpoint.trim_end_matches('/'),
            format!("/v1/chunk/{}", hex::encode(chunk_id))
        );
        let mut ready = self.client.clone().ready().await.map_err(|err| {
            Status::unavailable(format!(
                "KMS could not ready HTTP/2 stream for target delete {}: {}",
                uri, err
            ))
        })?;
        let mut request = HttpRequest::builder()
            .method(Method::DELETE)
            .uri(uri.clone())
            .body(())
            .map_err(|err| {
                Status::internal(format!(
                    "KMS could not construct target delete request {}: {}",
                    uri, err
                ))
            })?;
        request
            .headers_mut()
            .insert(CONTENT_LENGTH, http::HeaderValue::from_static("0"));
        let (response_future, _) = ready.send_request(request, true).map_err(|err| {
            Status::unavailable(format!(
                "KMS could not send target delete request {}: {}",
                uri, err
            ))
        })?;
        let response = response_future.await.map_err(|err| {
            Status::unavailable(format!(
                "KMS did not receive target delete response {}: {}",
                uri, err
            ))
        })?;
        let status = response.status();
        let body = collect_h2_body(response.into_body()).await.map_err(|err| {
            Status::unavailable(format!(
                "KMS could not collect target delete response {}: {}",
                uri, err
            ))
        })?;
        if status != StatusCode::OK {
            return Err(Status::internal(format!(
                "KMS target delete {} returned {} with body {}",
                uri,
                status,
                String::from_utf8_lossy(&body)
            )));
        }
        let document: DeleteChunkDocument = serde_json::from_slice(&body).map_err(|err| {
            Status::internal(format!(
                "KMS could not decode target delete response {}: {}",
                uri, err
            ))
        })?;
        Ok(document.deleted)
    }
}

async fn collect_h2_body(mut body: RecvStream) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|err| io::Error::other(err.to_string()))?;
        out.extend_from_slice(&chunk);
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(|err| io::Error::other(err.to_string()))?;
    }
    Ok(out)
}

fn normalize_max_tasks(raw: u32) -> usize {
    if raw == 0 {
        usize::MAX
    } else {
        raw as usize
    }
}

async fn release_reservation(
    kas_channels: KasEndpointBalancer,
    reservation_id: String,
    reservation_mutation_batch_size: usize,
    rpc_timeout: Duration,
) -> Result<(), Status> {
    release_reservation_ids(
        kas_channels,
        &[reservation_id],
        reservation_mutation_batch_size,
        rpc_timeout,
    )
    .await?;
    Ok(())
}

fn namespace_invalidation_event(
    namespace_id: String,
    event_kind: MetadataEventKind,
) -> MetadataInvalidationEvent {
    MetadataInvalidationEvent {
        namespace_id,
        bucket_id: String::new(),
        key: String::new(),
        entry_id: String::new(),
        parent_entry_id: String::new(),
        event_kind: event_kind as i32,
        version_id: String::new(),
    }
}

fn entry_invalidation_event(
    entry: &keinctl::proto::NamespaceDomainEntry,
    event_kind: MetadataEventKind,
) -> MetadataInvalidationEvent {
    MetadataInvalidationEvent {
        namespace_id: entry.namespace_id.clone(),
        bucket_id: String::new(),
        key: String::new(),
        entry_id: entry.entry_id.clone(),
        parent_entry_id: entry.parent_entry_id.clone(),
        event_kind: event_kind as i32,
        version_id: String::new(),
    }
}

fn object_invalidation_event(
    namespace_id: &str,
    bucket_id: &str,
    key: &str,
    event_kind: MetadataEventKind,
    version_id: &str,
) -> MetadataInvalidationEvent {
    MetadataInvalidationEvent {
        namespace_id: namespace_id.to_string(),
        bucket_id: bucket_id.to_string(),
        key: key.to_string(),
        entry_id: String::new(),
        parent_entry_id: String::new(),
        event_kind: event_kind as i32,
        version_id: version_id.to_string(),
    }
}

fn invalidate_local_object_cache(read_cache: &ResolveObjectReadCache, bucket_id: &str, key: &str) {
    read_cache.invalidate_object(bucket_id, key);
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn map_profile_error(err: ProfileError) -> Status {
    Status::invalid_argument(err.to_string())
}

#[derive(Clone)]
pub(crate) struct ReservationCache {
    inner: Arc<Mutex<ReservationCacheInner>>,
    config: ReservationCacheConfig,
}

impl ReservationCache {
    pub(crate) fn new(config: ReservationCacheConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ReservationCacheInner::default())),
            config,
        }
    }

    fn take(
        &self,
        key: &ReservationCacheKey,
        stats: &KmsStats,
    ) -> Option<PlacementReservationRecord> {
        let now_ms = now_unix_ms();
        let min_usable_until = now_ms.saturating_add(self.config.min_usable_ttl.as_millis() as u64);
        let mut inner = self.inner.lock().unwrap();
        let _ = inner.maybe_clear_stale_refill(key, self.config.stale_refill_after, stats);
        inner.prune_expiring(key, min_usable_until);
        let queue = inner.queues.get_mut(key)?;
        let reservation = queue.pop_front();
        stats.set_reservation_cache_depth(inner.depth());
        reservation
    }

    fn store_batch(
        &self,
        key: &ReservationCacheKey,
        reservations: Vec<PlacementReservationRecord>,
        stats: &KmsStats,
    ) {
        let now_ms = now_unix_ms();
        let min_usable_until = now_ms.saturating_add(self.config.min_usable_ttl.as_millis() as u64);
        let mut inner = self.inner.lock().unwrap();
        // Memory bound: `high_watermark` is the GLOBAL pool budget,
        // partitioned across distinct `(bucket, ec_profile, failure_domain)`
        // keys rather than applied per-key. Multi-shard makes the cache eligible
        // for many keys, so a per-key cap would let total RAM grow as
        // `num_keys * high_watermark`. We track the running global depth and
        // refuse new entries once the whole pool reaches the budget.
        let mut global_depth = inner.depth();
        let queue = inner.queues.entry(key.clone()).or_default();
        for reservation in reservations {
            if reservation.expires_at_unix_ms != 0
                && reservation.expires_at_unix_ms < min_usable_until
            {
                continue;
            }
            // Per-key ceiling (keeps any one key from monopolizing the pool) and
            // the global budget ceiling (bounds total memory across all keys).
            if queue.len() >= self.config.high_watermark
                || global_depth >= self.config.high_watermark
            {
                break;
            }
            queue.push_back(reservation);
            global_depth += 1;
        }
        stats.set_reservation_cache_depth(inner.depth());
    }

    fn begin_async_refill(
        &self,
        key: &ReservationCacheKey,
        recent_demand: usize,
        stats: &KmsStats,
    ) -> Option<usize> {
        let mut inner = self.inner.lock().unwrap();
        let now_ms = now_unix_ms();
        let min_usable_until = now_ms.saturating_add(self.config.min_usable_ttl.as_millis() as u64);
        let _ = inner.maybe_clear_stale_refill(key, self.config.stale_refill_after, stats);
        inner.prune_expiring(key, min_usable_until);
        if inner.inflight.contains_key(key)
            || inner.active_refills >= self.config.refill_concurrency
        {
            stats.set_reservation_cache_depth(inner.depth());
            return None;
        }
        let depth = inner.queues.get(key).map_or(0, VecDeque::len);
        if depth >= self.config.low_watermark {
            stats.set_reservation_cache_depth(inner.depth());
            return None;
        }
        let target_depth = self.target_depth_for_demand(depth, recent_demand);
        let batch_size = target_depth.saturating_sub(depth);
        if batch_size == 0 {
            stats.set_reservation_cache_depth(inner.depth());
            return None;
        }
        inner.begin_refill(key.clone());
        inner.active_refills += 1;
        stats.set_reservation_cache_depth(inner.depth());
        Some(batch_size)
    }

    fn finish_async_refill(&self, key: &ReservationCacheKey, stats: &KmsStats) {
        let mut inner = self.inner.lock().unwrap();
        inner.finish_refill(key);
        stats.set_reservation_cache_depth(inner.depth());
    }

    fn begin_forced_refill(&self, key: &ReservationCacheKey, stats: &KmsStats) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let now_ms = now_unix_ms();
        let min_usable_until = now_ms.saturating_add(self.config.min_usable_ttl.as_millis() as u64);
        let _ = inner.maybe_clear_stale_refill(key, self.config.stale_refill_after, stats);
        inner.prune_expiring(key, min_usable_until);
        if !inner.can_begin_forced_refill(key, self.refill_takeover_after()) {
            stats.set_reservation_cache_depth(inner.depth());
            return false;
        }
        inner.begin_refill(key.clone());
        inner.active_refills += 1;
        stats.set_reservation_cache_depth(inner.depth());
        true
    }

    fn refill_step_size(&self, recent_demand: usize) -> usize {
        let demand = recent_demand.max(1);
        if demand <= self.config.small_object_max_stripes {
            return self
                .config
                .single_window_seed_batch
                .max(demand)
                .min(self.config.high_watermark)
                .max(1);
        }
        demand
            .saturating_mul(32)
            .max(self.config.low_watermark / 4)
            .max(256)
            .min(self.config.refill_batch.max(256))
    }

    fn cold_seed_batch_size(&self, recent_demand: usize) -> usize {
        if recent_demand.max(1) <= self.config.small_object_max_stripes {
            return self
                .config
                .single_window_seed_batch
                .max(recent_demand.max(1))
                .min(self.config.high_watermark)
                .max(1);
        }
        recent_demand
            .max(1)
            .saturating_mul(64)
            .max(self.config.low_watermark / 2)
            .max(self.config.refill_batch)
            .min(self.config.high_watermark)
    }

    fn target_depth_for_demand(&self, depth: usize, recent_demand: usize) -> usize {
        let step = self.refill_step_size(recent_demand);
        if depth == 0 {
            return self.cold_seed_batch_size(recent_demand);
        }
        let small_object = recent_demand.max(1) <= self.config.small_object_max_stripes;
        let adaptive_cap = if small_object {
            step.saturating_mul(4)
                .max(self.config.single_window_seed_batch)
        } else {
            step.saturating_mul(8).max(self.config.low_watermark)
        };
        depth
            .saturating_add(step)
            .min(adaptive_cap)
            .min(self.config.high_watermark)
    }

    fn wait_timeout(&self) -> Duration {
        self.config.wait_timeout
    }

    fn refill_takeover_after(&self) -> Duration {
        let takeover_ms = (self.config.wait_timeout.as_millis() / 20).clamp(25, 250) as u64;
        Duration::from_millis(takeover_ms).min(self.config.stale_refill_after)
    }

    fn miss_refill_batch_size(
        &self,
        key: &ReservationCacheKey,
        shortage: usize,
        total_demand: usize,
        future_demand: usize,
        stats: &KmsStats,
    ) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let now_ms = now_unix_ms();
        let min_usable_until = now_ms.saturating_add(self.config.min_usable_ttl.as_millis() as u64);
        let _ = inner.maybe_clear_stale_refill(key, self.config.stale_refill_after, stats);
        inner.prune_expiring(key, min_usable_until);
        let depth = inner.queues.get(key).map_or(0, VecDeque::len);
        if future_demand == 0 && total_demand <= self.config.small_object_max_stripes {
            return self
                .cold_seed_batch_size(total_demand)
                .max(shortage)
                .min(self.config.high_watermark)
                .max(1);
        }
        if future_demand == 0 {
            return shortage.max(1);
        }
        if depth == 0 {
            return self.cold_seed_batch_size(total_demand).max(shortage).max(1);
        }
        let target_depth = self.target_depth_for_demand(depth, total_demand);
        let desired = target_depth
            .saturating_sub(depth)
            .max(shortage)
            .max(total_demand.saturating_mul(8))
            .max(1);
        let sync_cap = self
            .config
            .refill_batch
            .saturating_mul(4)
            .max(self.config.low_watermark)
            .max(total_demand.saturating_mul(16))
            .min(self.config.high_watermark);
        desired.min(sync_cap).max(shortage).max(1)
    }

    async fn wait_for_refill(
        &self,
        key: &ReservationCacheKey,
        stats: &KmsStats,
    ) -> ReservationCacheWaitOutcome {
        let deadline = Instant::now() + self.config.wait_timeout;
        while Instant::now() < deadline {
            {
                let mut inner = self.inner.lock().unwrap();
                let _ = inner.maybe_clear_stale_refill(key, self.config.stale_refill_after, stats);
                let now_ms = now_unix_ms();
                let min_usable_until =
                    now_ms.saturating_add(self.config.min_usable_ttl.as_millis() as u64);
                inner.prune_expiring(key, min_usable_until);
                if inner.queues.get(key).is_some_and(|queue| !queue.is_empty()) {
                    stats.set_reservation_cache_depth(inner.depth());
                    return ReservationCacheWaitOutcome::Ready;
                }
                if !inner.inflight.contains_key(key) {
                    stats.set_reservation_cache_depth(inner.depth());
                    return ReservationCacheWaitOutcome::Retry;
                }
                if inner.can_begin_forced_refill(key, self.refill_takeover_after()) {
                    stats.set_reservation_cache_depth(inner.depth());
                    return ReservationCacheWaitOutcome::Retry;
                }
                stats.set_reservation_cache_depth(inner.depth());
            }
            sleep(Duration::from_millis(2)).await;
        }
        ReservationCacheWaitOutcome::TimedOut
    }

    fn reservation_ttl_ms(&self) -> u64 {
        self.config.reservation_ttl.as_millis() as u64
    }

    fn small_object_max_stripes(&self) -> usize {
        self.config.small_object_max_stripes
    }
}

#[derive(Default)]
struct ReservationCacheInner {
    queues: HashMap<ReservationCacheKey, VecDeque<PlacementReservationRecord>>,
    inflight: HashMap<ReservationCacheKey, InflightRefillState>,
    active_refills: usize,
}

impl ReservationCacheInner {
    fn depth(&self) -> usize {
        self.queues.values().map(VecDeque::len).sum()
    }

    fn prune_expiring(&mut self, key: &ReservationCacheKey, min_usable_until: u64) {
        let mut remove_key = false;
        if let Some(queue) = self.queues.get_mut(key) {
            while let Some(front) = queue.front() {
                if front.expires_at_unix_ms == 0 || front.expires_at_unix_ms >= min_usable_until {
                    break;
                }
                queue.pop_front();
            }
            remove_key = queue.is_empty();
        }
        if remove_key {
            self.queues.remove(key);
        }
    }

    fn maybe_clear_stale_refill(
        &mut self,
        key: &ReservationCacheKey,
        stale_after: Duration,
        stats: &KmsStats,
    ) -> bool {
        let Some(state) = self.inflight.get(key) else {
            return false;
        };
        if state.started_at.elapsed() < stale_after {
            return false;
        }
        let active = state.active;
        self.inflight.remove(key);
        self.active_refills = self.active_refills.saturating_sub(active);
        stats.set_last_error(format!(
            "KMS reservation cache refill for bucket {} profile {} went stale after {} ms; forcing takeover",
            key.bucket_id,
            key.ec_profile_id,
            stale_after.as_millis()
        ));
        true
    }

    fn can_begin_forced_refill(&self, key: &ReservationCacheKey, takeover_after: Duration) -> bool {
        match self.inflight.get(key) {
            None => true,
            Some(state) => state.active < 2 && state.started_at.elapsed() >= takeover_after,
        }
    }

    fn begin_refill(&mut self, key: ReservationCacheKey) {
        self.inflight
            .entry(key)
            .and_modify(|state| state.active = state.active.saturating_add(1))
            .or_insert_with(InflightRefillState::new);
    }

    fn finish_refill(&mut self, key: &ReservationCacheKey) {
        let mut remove = false;
        if let Some(state) = self.inflight.get_mut(key) {
            state.active = state.active.saturating_sub(1);
            if state.active == 0 {
                remove = true;
            } else {
                // Reset the takeover clock for the remaining in-flight refill instead of
                // letting an older completed refill keep every waiter in takeover mode.
                state.started_at = Instant::now();
            }
        }
        if remove {
            self.inflight.remove(key);
        }
        self.active_refills = self.active_refills.saturating_sub(1);
    }
}

#[derive(Clone, Copy, Debug)]
enum ReservationCacheWaitOutcome {
    Ready,
    Retry,
    TimedOut,
}

#[derive(Clone, Copy, Debug)]
struct InflightRefillState {
    started_at: Instant,
    active: usize,
}

impl InflightRefillState {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            active: 1,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ReservationCacheKey {
    bucket_id: String,
    ec_profile_id: String,
    failure_domain: i32,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ObjectParentCacheKey {
    bucket_id: String,
    parent_key: String,
}

impl ObjectParentCacheKey {
    fn new(bucket_id: &str, parent_key: &str) -> Self {
        Self {
            bucket_id: bucket_id.to_string(),
            parent_key: parent_key.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ObjectParentContext {
    parent_entry_id: String,
    parent_path: String,
}

fn parent_key_from_object_key(normalized_key: &str) -> Option<String> {
    normalized_key
        .rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
}

impl ReservationCacheKey {
    fn new(bucket_id: &str, ec_profile: &EcProfile) -> Self {
        Self {
            bucket_id: bucket_id.to_string(),
            ec_profile_id: ec_profile.id.clone(),
            failure_domain: ec_profile.failure_domain,
        }
    }
}

fn kms_err<T>(
    stats: &KmsStats,
    kind: RpcKind,
    started: &Instant,
    err: Status,
) -> Result<T, Status> {
    stats.record_error(kind, started.elapsed(), err.to_string());
    Err(err)
}

fn record_store_phase_timings(
    stats: &KmsStats,
    kind: RpcKind,
    prefix: &str,
    phases: &[StorePhaseTiming],
) {
    for phase in phases {
        stats.record_phase(kind, &format!("{prefix}.{}", phase.name), phase.elapsed);
    }
}

fn stripe_count_for_profile(logical_length_bytes: u64, profile: &EcProfile) -> usize {
    let stripe_logical_bytes =
        u64::from(profile.data_fragments.max(1)) * u64::from(profile.fragment_bytes.max(1));
    logical_length_bytes
        .max(1)
        .div_ceil(stripe_logical_bytes.max(1))
        .try_into()
        .unwrap_or(usize::MAX)
}

fn capped_initial_write_window_stripe_count(
    requested: usize,
    stripe_count: usize,
    cap: usize,
) -> usize {
    requested.min(cap).min(stripe_count)
}

fn shared_reservation_cache_enabled(
    remaining_after_window: usize,
    window_stripe_count: usize,
    small_object_max_stripes: usize,
    allocation_shard_count: usize,
) -> bool {
    // The foreground reserve drains a pre-staged RAM pool on
    // multi-shard clusters too. Previously this required
    // `allocation_shard_count <= 1`, which made the cache dead code on the
    // 3-shard prod/lab topology and forced every foreground reserve into a
    // synchronous KAS `reserve_stripe_batch`. A cached
    // `PlacementReservationRecord` is a FULL assembled stripe (its `placements`
    // already span all shards — see `merge_sharded_reservation_batches`), so the
    // existing (bucket, ec_profile, failure_domain) cache key is correct on
    // multi-shard; "shard-aware" here means the BACKGROUND refill assembles
    // stripes via the sharded reserve path. We therefore drop the shard-count
    // clause and keep the cache eligible whenever the whole object fits one
    // window of small-object stripes.
    let _ = allocation_shard_count;
    remaining_after_window == 0 && window_stripe_count <= small_object_max_stripes
}

fn profile_matches_family(candidate: &EcProfile, base: &EcProfile) -> bool {
    candidate.codec_id == base.codec_id
        && candidate.data_fragments == base.data_fragments
        && candidate.parity_fragments == base.parity_fragments
        && candidate.failure_domain == base.failure_domain
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::KmsIdentity;
    use keinctl::proto::ObjectVersionManifest;
    use tonic::transport::Endpoint;

    fn test_stats() -> Arc<KmsStats> {
        KmsStats::new(KmsIdentity {
            build: keinbuild::build_info!(),
            listen_addr: "127.0.0.1:55060".to_string(),
            kas_endpoints: "http://127.0.0.1:55061".to_string(),
            shard_id: "kms-test".to_string(),
            public_endpoint: "http://127.0.0.1:55060".to_string(),
            metadata_store: "foundationdb:/tmp/fdb.cluster".to_string(),
            pid: 1,
            stats_root: "/tmp".to_string(),
        })
    }

    fn test_cache() -> ReservationCache {
        ReservationCache::new(ReservationCacheConfig {
            high_watermark: 8_192,
            low_watermark: 2_048,
            refill_batch: 2_048,
            reservation_ttl: Duration::from_secs(30),
            min_usable_ttl: Duration::from_secs(5),
            refill_concurrency: 8,
            wait_timeout: Duration::from_secs(15),
            stale_refill_after: Duration::from_secs(5),
            small_object_max_stripes: 16,
            single_window_seed_batch: 1_024,
            initiate_write_window_max_stripes: 256,
        })
    }

    fn test_key() -> ReservationCacheKey {
        ReservationCacheKey {
            bucket_id: "lab-8p2".to_string(),
            ec_profile_id: "lab-rs-8p2-1m".to_string(),
            failure_domain: 1,
        }
    }

    #[test]
    fn small_object_miss_seeds_reservation_cache_batch() {
        let cache = test_cache();
        let stats = test_stats();
        let batch = cache.miss_refill_batch_size(&test_key(), 1, 1, 0, &stats);
        assert_eq!(batch, 1_024);
    }

    #[test]
    fn large_single_window_miss_does_not_overseed() {
        let cache = test_cache();
        let stats = test_stats();
        let batch = cache.miss_refill_batch_size(&test_key(), 1, 64, 0, &stats);
        assert_eq!(batch, 1);
    }

    #[test]
    fn initiate_window_is_capped_before_first_reserve() {
        assert_eq!(
            capped_initial_write_window_stripe_count(2_048, 2_048, 256),
            256
        );
        assert_eq!(
            capped_initial_write_window_stripe_count(128, 2_048, 256),
            128
        );
        assert_eq!(capped_initial_write_window_stripe_count(512, 128, 256), 128);
    }

    #[test]
    fn shared_reservation_cache_is_enabled_for_small_objects_on_any_shard_count() {
        // The pool is the foreground source on multi-shard too,
        // so the shard count no longer gates eligibility. What still gates it is
        // "the whole object fits one small-object window" (no remaining stripes
        // after the window, and the window is within small_object_max_stripes).
        assert!(shared_reservation_cache_enabled(0, 1, 16, 1));
        assert!(shared_reservation_cache_enabled(0, 1, 16, 3));
        assert!(shared_reservation_cache_enabled(0, 16, 16, 8));
        // Still disabled when the object spills past the first window...
        assert!(!shared_reservation_cache_enabled(1, 1, 16, 1));
        assert!(!shared_reservation_cache_enabled(1, 1, 16, 3));
        // ...or when the window itself exceeds the small-object threshold.
        assert!(!shared_reservation_cache_enabled(0, 32, 16, 1));
        assert!(!shared_reservation_cache_enabled(0, 32, 16, 3));
    }

    #[test]
    fn collect_release_ids_prefers_underlying_shard_reservations() {
        let reservations = vec![PlacementReservationRecord {
            reservation_id: "merged-123".to_string(),
            placements: vec![
                PlacementReservation {
                    reservation_id: "alloc-shard-00/reserve-a".to_string(),
                    ..PlacementReservation::default()
                },
                PlacementReservation {
                    reservation_id: "alloc-shard-01/reserve-b".to_string(),
                    ..PlacementReservation::default()
                },
                PlacementReservation {
                    reservation_id: "alloc-shard-01/reserve-b".to_string(),
                    ..PlacementReservation::default()
                },
            ],
            ..PlacementReservationRecord::default()
        }];
        assert_eq!(
            collect_reservation_ids_for_release(&reservations),
            vec![
                "alloc-shard-00/reserve-a".to_string(),
                "alloc-shard-01/reserve-b".to_string(),
            ]
        );
    }

    #[test]
    fn reservation_shard_id_extracts_prefixed_shard() {
        assert_eq!(
            reservation_shard_id("alloc-shard-02/reserve-batch-abc-0001"),
            Some("alloc-shard-02")
        );
        assert_eq!(reservation_shard_id("reserve-batch-abc-0001"), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shard_fragment_distributions_rotate_extra_shards() {
        let routes = (0..3)
            .map(|index| AllocationShardRoute {
                shard_id: format!("alloc-shard-0{index}"),
                endpoint: format!("http://127.0.0.1:{}", 55061 + index),
                channel: Endpoint::from_shared(format!("http://127.0.0.1:{}", 55061 + index))
                    .expect("valid test endpoint")
                    .connect_lazy(),
            })
            .collect::<Vec<_>>();

        let plans = shard_fragment_distributions(10, &routes).expect("plans");
        let formatted = plans
            .iter()
            .map(|plan| format_shard_plan(plan))
            .collect::<Vec<_>>();

        assert_eq!(
            formatted,
            vec![
                "alloc-shard-00=4,alloc-shard-01=3,alloc-shard-02=3".to_string(),
                "alloc-shard-00=3,alloc-shard-01=4,alloc-shard-02=3".to_string(),
                "alloc-shard-00=3,alloc-shard-01=3,alloc-shard-02=4".to_string(),
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn discovered_routes_merge_partial_registry_views() {
        let balancer = KasEndpointBalancer::new(
            (0..3)
                .map(|index| KasEndpoint {
                    endpoint: format!("http://127.0.0.1:{}", 55061 + index),
                    channel: Endpoint::from_shared(format!("http://127.0.0.1:{}", 55061 + index))
                        .expect("valid test endpoint")
                        .connect_lazy(),
                })
                .collect(),
        );
        let now_ms = 10_000;
        let mut fresh_routes = HashMap::new();
        let mut stale_routes = HashMap::new();

        merge_discovered_allocation_shard_routes(
            &balancer,
            &[
                keinctl::proto::ServiceInstanceRecord {
                    instance_id: "kas:http://127.0.0.1:55061".to_string(),
                    service_kind: ServiceKind::Kas as i32,
                    node_id: "cp-01".to_string(),
                    endpoint: "http://127.0.0.1:55061".to_string(),
                    instance_label: "alloc-shard-00".to_string(),
                    heartbeat_at_unix_ms: now_ms,
                    heartbeat_interval_ms: 30_000,
                    ..Default::default()
                },
                keinctl::proto::ServiceInstanceRecord {
                    instance_id: "kas:http://127.0.0.1:55062".to_string(),
                    service_kind: ServiceKind::Kas as i32,
                    node_id: "cp-02".to_string(),
                    endpoint: "http://127.0.0.1:55062".to_string(),
                    instance_label: "alloc-shard-01".to_string(),
                    heartbeat_at_unix_ms: now_ms,
                    heartbeat_interval_ms: 30_000,
                    ..Default::default()
                },
            ],
            now_ms,
            &mut fresh_routes,
            &mut stale_routes,
        );
        merge_discovered_allocation_shard_routes(
            &balancer,
            &[
                keinctl::proto::ServiceInstanceRecord {
                    instance_id: "kas:http://127.0.0.1:55062".to_string(),
                    service_kind: ServiceKind::Kas as i32,
                    node_id: "cp-02".to_string(),
                    endpoint: "http://127.0.0.1:55062".to_string(),
                    instance_label: "alloc-shard-01".to_string(),
                    heartbeat_at_unix_ms: now_ms,
                    heartbeat_interval_ms: 30_000,
                    ..Default::default()
                },
                keinctl::proto::ServiceInstanceRecord {
                    instance_id: "kas:http://127.0.0.1:55063".to_string(),
                    service_kind: ServiceKind::Kas as i32,
                    node_id: "cp-03".to_string(),
                    endpoint: "http://127.0.0.1:55063".to_string(),
                    instance_label: "alloc-shard-02".to_string(),
                    heartbeat_at_unix_ms: now_ms,
                    heartbeat_interval_ms: 30_000,
                    ..Default::default()
                },
            ],
            now_ms,
            &mut fresh_routes,
            &mut stale_routes,
        );

        let routes = finalize_discovered_allocation_shard_routes(fresh_routes, stale_routes);
        let shard_ids = routes
            .into_iter()
            .map(|route| route.shard_id)
            .collect::<Vec<_>>();
        assert_eq!(
            shard_ids,
            vec![
                "alloc-shard-00".to_string(),
                "alloc-shard-01".to_string(),
                "alloc-shard-02".to_string(),
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn configured_routes_fill_missing_discovered_shards() {
        let balancer = KasEndpointBalancer::new(
            (0..3)
                .map(|index| KasEndpoint {
                    endpoint: format!("http://127.0.0.1:{}", 55061 + index),
                    channel: Endpoint::from_shared(format!("http://127.0.0.1:{}", 55061 + index))
                        .expect("valid test endpoint")
                        .connect_lazy(),
                })
                .collect(),
        );
        let discovered = vec![
            AllocationShardRoute {
                shard_id: "alloc-shard-01".to_string(),
                endpoint: "http://127.0.0.1:55062".to_string(),
                channel: balancer
                    .channel_for_endpoint("http://127.0.0.1:55062")
                    .expect("route"),
            },
            AllocationShardRoute {
                shard_id: "alloc-shard-02".to_string(),
                endpoint: "http://127.0.0.1:55063".to_string(),
                channel: balancer
                    .channel_for_endpoint("http://127.0.0.1:55063")
                    .expect("route"),
            },
        ];

        let routes = merge_configured_allocation_shard_routes(
            discovered,
            configured_allocation_shard_routes(&balancer),
        );
        let shard_ids = routes
            .into_iter()
            .map(|route| route.shard_id)
            .collect::<Vec<_>>();
        assert_eq!(
            shard_ids,
            vec![
                "alloc-shard-00".to_string(),
                "alloc-shard-01".to_string(),
                "alloc-shard-02".to_string(),
            ]
        );
    }

    #[test]
    fn small_object_target_depth_scales_to_four_seed_batches() {
        let cache = ReservationCache::new(ReservationCacheConfig {
            high_watermark: 131_072,
            low_watermark: 131_072,
            refill_batch: 131_072,
            reservation_ttl: Duration::from_secs(30),
            min_usable_ttl: Duration::from_secs(5),
            refill_concurrency: 8,
            wait_timeout: Duration::from_secs(15),
            stale_refill_after: Duration::from_secs(5),
            small_object_max_stripes: 16,
            single_window_seed_batch: 4_096,
            initiate_write_window_max_stripes: 256,
        });
        assert_eq!(cache.target_depth_for_demand(4_096, 1), 8_192);
    }

    #[test]
    fn profile_family_match_requires_same_layout() {
        let base = EcProfile {
            id: "lab-rs-8p2-1m".to_string(),
            codec_id: "rs".to_string(),
            data_fragments: 8,
            parity_fragments: 2,
            fragment_bytes: 1_048_576,
            failure_domain: 1,
        };
        let sibling = EcProfile {
            id: "lab-rs-8p2-128k".to_string(),
            fragment_bytes: 131_072,
            ..base.clone()
        };
        let wrong = EcProfile {
            id: "lab-rs-6p2-128k".to_string(),
            data_fragments: 6,
            fragment_bytes: 131_072,
            ..base.clone()
        };
        assert!(profile_matches_family(&sibling, &base));
        assert!(!profile_matches_family(&wrong, &base));
    }

    #[test]
    fn stripe_count_scales_with_fragment_size() {
        let base = EcProfile {
            id: "lab-rs-8p2-1m".to_string(),
            codec_id: "rs".to_string(),
            data_fragments: 8,
            parity_fragments: 2,
            fragment_bytes: 1_048_576,
            failure_domain: 1,
        };
        let small = EcProfile {
            id: "lab-rs-8p2-128k".to_string(),
            fragment_bytes: 131_072,
            ..base.clone()
        };
        assert_eq!(stripe_count_for_profile(1_048_576, &base), 1);
        assert_eq!(stripe_count_for_profile(1_048_576, &small), 1);
        assert_eq!(stripe_count_for_profile(16 * 1_048_576, &small), 16);
    }

    #[test]
    fn local_object_invalidation_clears_read_cache_immediately() {
        let cache = ResolveObjectReadCache::new(8, Duration::from_secs(30));
        let manifest = ObjectVersionManifest {
            version_id: "v1".to_string(),
            bucket_id: "lab-8p2".to_string(),
            key: "dir/object.bin".to_string(),
            namespace_id: "ns-1".to_string(),
            object_entry_id: "obj-1".to_string(),
            bucket_entry_id: "bucket::lab-8p2".to_string(),
            logical_length_bytes: 123,
            ec_profile_id: "lab-rs-8p2-1m".to_string(),
            stripes: Vec::new(),
        };
        let profile = EcProfile {
            id: "lab-rs-8p2-1m".to_string(),
            codec_id: "rs".to_string(),
            data_fragments: 8,
            parity_fragments: 2,
            fragment_bytes: 1_048_576,
            failure_domain: 1,
        };
        cache.insert(
            manifest.bucket_id.clone(),
            manifest.key.clone(),
            manifest.namespace_id.clone(),
            manifest,
            profile,
        );
        assert!(cache.get("lab-8p2", "dir/object.bin").is_some());

        invalidate_local_object_cache(&cache, "lab-8p2", "dir/object.bin");

        assert!(cache.get("lab-8p2", "dir/object.bin").is_none());
    }

    fn seed_route(shard_id: &str) -> AllocationShardRoute {
        // Channels are not exercised by the TTL logic under test; any
        // well-formed lazy channel suffices to populate the cache.
        let channel = Endpoint::from_static("http://127.0.0.1:55061").connect_lazy();
        AllocationShardRoute {
            shard_id: shard_id.to_string(),
            endpoint: "http://127.0.0.1:55061".to_string(),
            channel,
        }
    }

    fn seed_route_cache(cache: &AllocationRouteCache, shards: usize, fetched_at: Instant) {
        let routes = (0..shards)
            .map(|index| seed_route(&format!("alloc-shard-{index:02}")))
            .collect();
        *cache.inner.cached.lock().unwrap() = Some(CachedAllocationRoutes { routes, fetched_at });
    }

    #[tokio::test(flavor = "current_thread")]
    async fn route_cache_serves_fresh_snapshot_within_ttl() {
        // A freshly-fetched route set is served from RAM (no RPC),
        // and the cached shard count is what the foreground reserve reads.
        let cache = AllocationRouteCache::new(Duration::from_secs(5), test_stats());
        seed_route_cache(&cache, 3, Instant::now());
        let snapshot = cache.fresh_snapshot().expect("fresh routes within TTL");
        assert_eq!(snapshot.len(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn route_cache_expires_after_ttl() {
        // An entry older than the TTL is not served; the next resolve must
        // re-discover. Modeled by back-dating fetched_at past the TTL.
        let cache = AllocationRouteCache::new(Duration::from_millis(50), test_stats());
        let stale_at = Instant::now()
            .checked_sub(Duration::from_millis(200))
            .expect("instant in range");
        seed_route_cache(&cache, 3, stale_at);
        assert!(
            cache.fresh_snapshot().is_none(),
            "expired routes must not be served from the TTL cache"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn route_cache_invalidate_forces_rediscovery() {
        // Failover net (design §4/§7): an UNAVAILABLE per-shard reserve calls
        // invalidate(), which drops the cached routes so the NEXT resolve picks
        // up the new leadership instead of retrying a demoted leader.
        let cache = AllocationRouteCache::new(Duration::from_secs(60), test_stats());
        seed_route_cache(&cache, 3, Instant::now());
        assert!(cache.fresh_snapshot().is_some());
        cache.invalidate();
        assert!(
            cache.fresh_snapshot().is_none(),
            "invalidate() must drop the cached routes even while within TTL"
        );
    }

    fn full_stripe_reservation(expires_at_unix_ms: u64) -> PlacementReservationRecord {
        // A cached reservation is a FULL assembled stripe: its placements span
        // every shard (see merge_sharded_reservation_batches). The shard-aware
        // pool keys on (bucket, ec_profile, failure_domain) and stores these
        // whole stripes — the test only needs the count + expiry to exercise the
        // budget bound.
        PlacementReservationRecord {
            reservation_id: format!("merged-{}", Uuid::new_v4()),
            placements: vec![
                PlacementReservation {
                    reservation_id: "alloc-shard-00/r".to_string(),
                    ..PlacementReservation::default()
                },
                PlacementReservation {
                    reservation_id: "alloc-shard-01/r".to_string(),
                    ..PlacementReservation::default()
                },
            ],
            expires_at_unix_ms,
            ..PlacementReservationRecord::default()
        }
    }

    fn small_budget_cache(high_watermark: usize) -> ReservationCache {
        ReservationCache::new(ReservationCacheConfig {
            high_watermark,
            low_watermark: high_watermark / 4,
            refill_batch: high_watermark,
            reservation_ttl: Duration::from_secs(30),
            min_usable_ttl: Duration::from_secs(5),
            refill_concurrency: 8,
            wait_timeout: Duration::from_secs(15),
            stale_refill_after: Duration::from_secs(5),
            small_object_max_stripes: 16,
            single_window_seed_batch: 1_024,
            initiate_write_window_max_stripes: 256,
        })
    }

    #[test]
    fn pool_memory_budget_is_global_not_per_key() {
        // Memory bound: high_watermark is the GLOBAL pool budget,
        // partitioned across distinct keys. Storing into many keys must not let
        // total depth exceed the budget (which a per-key cap would allow:
        // num_keys * high_watermark).
        let cache = small_budget_cache(4);
        let stats = test_stats();
        let expires = now_unix_ms().saturating_add(60_000);
        for shard in 0..8 {
            let key = ReservationCacheKey {
                bucket_id: format!("bucket-{shard}"),
                ec_profile_id: "lab-rs-8p2-1m".to_string(),
                failure_domain: 1,
            };
            let batch = (0..4).map(|_| full_stripe_reservation(expires)).collect();
            cache.store_batch(&key, batch, &stats);
        }
        let total_depth = cache.inner.lock().unwrap().depth();
        assert_eq!(
            total_depth, 4,
            "global pool depth must be capped at high_watermark across all keys"
        );
    }

    #[test]
    fn pool_take_then_store_round_trips_full_stripe() {
        // Sanity: a stored full stripe is the same one popped back out, keyed by
        // (bucket, ec_profile, failure_domain) — confirming the cache stores
        // assembled stripes, not per-shard fragments.
        let cache = small_budget_cache(8);
        let stats = test_stats();
        let key = test_key();
        let expires = now_unix_ms().saturating_add(60_000);
        let mut record = full_stripe_reservation(expires);
        record.reservation_id = "merged-fixed".to_string();
        cache.store_batch(&key, vec![record], &stats);
        let popped = cache.take(&key, &stats).expect("stored stripe is available");
        assert_eq!(popped.reservation_id, "merged-fixed");
        assert_eq!(popped.placements.len(), 2);
        assert!(cache.take(&key, &stats).is_none(), "pool drains to empty");
    }

    /// One per-shard reservation (the fragment a single shard contributes to a
    /// stripe) with a chosen expiry. `merge_sharded_reservation_batches` combines
    /// one of these per shard into a full stripe.
    fn shard_fragment_reservation(
        shard_id: &str,
        expires_at_unix_ms: u64,
    ) -> PlacementReservationRecord {
        PlacementReservationRecord {
            reservation_id: format!("{shard_id}/reserve-x"),
            state: keinctl::proto::ReservationState::Reserved as i32,
            placements: vec![PlacementReservation {
                target_id: format!("{shard_id}-target"),
                reservation_id: format!("{shard_id}/reserve-x"),
                reservation_placement_index: 0,
                ..PlacementReservation::default()
            }],
            expires_at_unix_ms,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn merge_uses_min_fragment_expiry_across_shards() {
        // A merged full-stripe reservation
        // is only usable while EVERY per-shard fragment is still valid, so its
        // cached expiry must be the EARLIEST (MIN), never the MAX. shard-0
        // expires soon, shard-1 late -> merged expiry == shard-0's (the min).
        let now = now_unix_ms();
        let shard0_expiry = now.saturating_add(2_000); // soon
        let shard1_expiry = now.saturating_add(120_000); // late
        let route0 = seed_route("alloc-shard-00");
        let route1 = seed_route("alloc-shard-01");
        let partial_batches = vec![
            (route0, vec![shard_fragment_reservation("alloc-shard-00", shard0_expiry)]),
            (route1, vec![shard_fragment_reservation("alloc-shard-01", shard1_expiry)]),
        ];

        // fragment_count = 2 (one fragment per shard), batch_size = 1 stripe.
        let merged = merge_sharded_reservation_batches(2, 1, &partial_batches)
            .expect("merge succeeds");
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].expires_at_unix_ms, shard0_expiry,
            "merged stripe expiry must be the MIN fragment expiry (shard-0), not the MAX"
        );
        assert_ne!(
            merged[0].expires_at_unix_ms, shard1_expiry,
            "merged stripe must NOT carry the later (max) shard-1 expiry"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn merge_treats_zero_expiry_as_no_expiry_not_min() {
        // A fragment with expires_at == 0 ("no expiry") must NOT collapse the min
        // to 0; the merged expiry is the min of the NON-zero fragment expiries.
        let now = now_unix_ms();
        let late = now.saturating_add(90_000);
        let route0 = seed_route("alloc-shard-00");
        let route1 = seed_route("alloc-shard-01");
        let partial_batches = vec![
            (route0, vec![shard_fragment_reservation("alloc-shard-00", 0)]),
            (route1, vec![shard_fragment_reservation("alloc-shard-01", late)]),
        ];
        let merged = merge_sharded_reservation_batches(2, 1, &partial_batches)
            .expect("merge succeeds");
        assert_eq!(
            merged[0].expires_at_unix_ms, late,
            "a 0 (no-expiry) fragment must not pull the merged expiry to 0"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pool_prunes_stripe_carrying_min_shard_expiry_near_min_usable_ttl() {
        // Companion to the merge MIN test: a stored stripe whose (merged, MIN)
        // expiry is the soon-expiring shard-0 fragment is pruned out of the pool
        // once it falls inside min_usable_ttl, so the foreground never hands out a
        // stripe that shard-0 is about to invalidate. small_budget_cache uses a
        // 5s min_usable_ttl.
        let cache = small_budget_cache(8);
        let stats = test_stats();
        let key = test_key();
        let now = now_unix_ms();
        // MIN expiry is shard-0's = now + 1s, well inside the 5s min_usable_ttl.
        let near_min = now.saturating_add(1_000);
        let merged = merge_sharded_reservation_batches(
            2,
            1,
            &[
                (seed_route("alloc-shard-00"), vec![shard_fragment_reservation("alloc-shard-00", near_min)]),
                (seed_route("alloc-shard-01"), vec![shard_fragment_reservation("alloc-shard-01", now.saturating_add(120_000))]),
            ],
        )
        .expect("merge succeeds");
        assert_eq!(merged[0].expires_at_unix_ms, near_min);

        // store_batch itself rejects entries already inside min_usable_ttl, and
        // take()/begin_async_refill prune the front — either way the pool must not
        // serve this stripe.
        cache.store_batch(&key, merged, &stats);
        assert!(
            cache.take(&key, &stats).is_none(),
            "a stripe whose MIN (shard-0) expiry is inside min_usable_ttl must be pruned, not handed out"
        );
    }

    #[test]
    fn route_change_invalidation_covers_failover_signals() {
        // FIX C (DESIGN_KAS_WRITE_SCALE.md §4/§7): with the always-on epoch fence,
        // a demoted-but-reachable leader returns Aborted (epoch mismatch) or
        // FailedPrecondition (not-leader), not just Unavailable. All three must
        // force a route-cache invalidate so KMS re-discovers immediately instead
        // of retrying the stale leader for the TTL.
        assert!(route_change_should_invalidate(&Status::unavailable("x")));
        assert!(route_change_should_invalidate(&Status::aborted("epoch fence")));
        assert!(route_change_should_invalidate(&Status::failed_precondition("not leader")));
        // Errors that are NOT a leadership-change signal must not blow the cache.
        assert!(!route_change_should_invalidate(&Status::internal("x")));
        assert!(!route_change_should_invalidate(&Status::deadline_exceeded("x")));
        assert!(!route_change_should_invalidate(&Status::resource_exhausted("x")));
    }
}
