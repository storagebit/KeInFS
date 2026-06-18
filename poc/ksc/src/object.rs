// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use crate::client::{
    ClientError, CompletionMode, RequestPhaseTimes, TargetSession, TargetSessionOptions,
};
use futures_util::StreamExt;
use kee::{EcProfile as KeeProfile, FailureDomain as KeeFailureDomain, KeeEngine, PreparedEcPlan};
use keinctl::proto::kms_client::KmsClient;
use keinctl::proto::{
    AbortObjectWriteRequest, CommitObjectWriteRequest, CommitObjectWriteWindowRequest,
    DeleteObjectRequest, DeletedObjectVersion, EcProfile, FailureDomain, FragmentPlan, FragmentRef,
    InitiateObjectWriteRequest, MetadataInvalidationEvent, ObjectVersionManifest,
    RepairObjectWriteRequest, ReserveObjectWriteWindowRequest, ResolveObjectReadRequest,
    WriteIntent,
};
use kp2::{
    ChunkId, ChunkRange, PackedReadQuery, PackedWriteEntry, PackedWriteRequest,
    MAX_PACK_PAYLOAD_BYTES,
};
use prost::Message;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fs::File;
use std::future::Future;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tonic::transport::{Channel, Endpoint};

const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(120);
const TARGET_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TARGET_IO_TIMEOUT: Duration = Duration::from_secs(10);
const TARGET_SAME_PLAN_RETRY_ATTEMPTS: usize = 2;
const TARGET_SAME_PLAN_RETRY_BACKOFF: Duration = Duration::from_millis(50);
/// Ceiling on how long a KP2 429 `Retry-After` (or computed exponential backoff)
/// may stall a same-target retry. A misbehaving or hostile target cannot park a
/// write task for longer than this.
const TARGET_RETRY_BACKOFF_CEILING: Duration = Duration::from_secs(3);
pub const DEFAULT_WRITE_WINDOW_MAX_STRIPES: usize = 4096;
pub const DEFAULT_WRITE_WINDOW_INFLIGHT_STRIPES: usize = 16;
/// Additive-increase step (in permits) applied to the adaptive write limiter
/// after a batch completes with no 429 observed, until it recovers to the
/// configured ceiling.
const ADAPTIVE_INFLIGHT_RECOVERY_STEP: usize = 1;
const KMS_GRPC_INITIAL_STREAM_WINDOW_BYTES: u32 = 8 * 1024 * 1024;
const KMS_GRPC_INITIAL_CONNECTION_WINDOW_BYTES: u32 = 256 * 1024 * 1024;
pub const DEFAULT_KMS_GRPC_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
const KMS_GRPC_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KMS_GRPC_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_READ_RESOLVE_CACHE_TTL: Duration = Duration::from_secs(1);
const DEFAULT_READ_PAYLOAD_CACHE_MAX_ENTRIES: usize = 1024;
const DEFAULT_READ_PAYLOAD_CACHE_MAX_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_READ_PAYLOAD_CACHE_MAX_OBJECT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_READ_WINDOW_MAX_STRIPES: usize = 64;
pub const DEFAULT_METADATA_NOTIFICATION_SUBJECT: &str = "keinfs.kms.events";

#[derive(Debug)]
pub enum ObjectError {
    Transport(String),
    Metadata(String),
    ControlStatus(tonic::Status),
    Data(ClientError),
    Codec(kee::KeeError),
}

impl fmt::Display for ObjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(message) | Self::Metadata(message) => f.write_str(message),
            Self::ControlStatus(status) => write!(f, "{status}"),
            Self::Data(err) => write!(f, "{err}"),
            Self::Codec(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ObjectError {}

impl ObjectError {
    /// Extract the KP2 429 backpressure signal from a data-plane error, if any.
    /// Only `ObjectError::Data` wraps a `ClientError` that can carry the
    /// `x-kp2-*` headers KST emits on a 429; every other variant yields the
    /// empty (non-rate-limited) signal.
    fn rate_limit_signal(&self) -> RateLimitSignal {
        match self {
            Self::Data(err) => RateLimitSignal::from_client_error(err),
            _ => RateLimitSignal::default(),
        }
    }
}

/// Structured KP2 429 backpressure signal pulled off a `ClientError` before it
/// is flattened to a log string. Carrying these fields (rather than the message
/// alone) lets the retry path honor `Retry-After` and lets the adaptive limiter
/// shrink toward the target's advertised `max-in-flight`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RateLimitSignal {
    rate_limited: bool,
    retry_after_ms: Option<u64>,
    limit_max_inflight: Option<usize>,
}

impl RateLimitSignal {
    fn from_client_error(err: &ClientError) -> Self {
        if !err.is_rate_limited() {
            return Self::default();
        }
        Self {
            rate_limited: true,
            retry_after_ms: err.retry_after_ms(),
            limit_max_inflight: err.limit_max_inflight(),
        }
    }
}

impl From<ClientError> for ObjectError {
    fn from(value: ClientError) -> Self {
        Self::Data(value)
    }
}

impl From<kee::KeeError> for ObjectError {
    fn from(value: kee::KeeError) -> Self {
        Self::Codec(value)
    }
}

impl From<tonic::Status> for ObjectError {
    fn from(value: tonic::Status) -> Self {
        Self::ControlStatus(value)
    }
}

#[derive(Clone, Debug)]
pub struct ObjectPutResult {
    pub intent: WriteIntent,
    pub manifest: ObjectVersionManifest,
    pub ec_profile: EcProfile,
    pub phases: ObjectPhaseTimes,
}

#[derive(Clone, Debug)]
pub struct ObjectGetResult {
    pub payload: Vec<u8>,
    pub manifest: ObjectVersionManifest,
    pub ec_profile: EcProfile,
    pub phases: ObjectPhaseTimes,
    pub missing_fragments: usize,
    pub data_fragment_reads: usize,
    pub parity_fragment_reads: usize,
    pub reconstructed: bool,
}

/// Result of a stripe-granular ranged read. `payload` covers `[offset,
/// offset+payload.len())` of the object (clamped to the object length). Built
/// by reading only the stripes the requested range touches, instead of
/// materializing the whole object.
#[derive(Clone, Debug)]
pub struct RangedGetResult {
    pub payload: Vec<u8>,
    pub offset: u64,
    pub object_length_bytes: u64,
    pub manifest: ObjectVersionManifest,
    pub ec_profile: EcProfile,
    pub phases: ObjectPhaseTimes,
    pub missing_fragments: usize,
    pub data_fragment_reads: usize,
    pub parity_fragment_reads: usize,
    pub reconstructed: bool,
}

#[derive(Clone, Debug)]
pub struct ObjectDeleteResult {
    pub deleted_versions: Vec<DeletedObjectVersion>,
    pub fragment_delete_attempts: u64,
    pub fragment_delete_successes: u64,
    pub reclaimed_granules: u64,
    pub cleanup_complete: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectPhaseTimes {
    pub kms_initiate: Duration,
    pub kms_commit: Duration,
    pub kms_resolve: Duration,
    pub ec_encode: Duration,
    pub ec_reconstruct: Duration,
    pub target_connect: Duration,
    pub target_write: Duration,
    pub target_read: Duration,
    pub target_ready_wait: Duration,
    pub target_request_prepare: Duration,
    pub target_send_headers: Duration,
    pub target_send_body: Duration,
    pub target_wait_response: Duration,
    pub target_collect_response: Duration,
    pub target_protocol_decode: Duration,
    pub target_payload_validate: Duration,
}

pub struct ObjectClient {
    kms: KmsEndpointBalancer,
    target_sessions: HashMap<String, TargetSession>,
    shared_target_sessions: Arc<tokio::sync::Mutex<HashMap<String, TargetSession>>>,
    bucket_profiles: HashMap<String, EcProfile>,
    shared_read_cache: SharedObjectMetadataCache,
    prepared_encoders: HashMap<String, PreparedEncodeWorkspace>,
    session_options: TargetSessionOptions,
    write_window_max_stripes: usize,
    write_window_inflight_stripes: usize,
    /// Adaptive concurrency gate for fragment writes (KP2 429 backpressure).
    /// Shared (`Arc`) so the per-fragment write tasks spawned inside
    /// `write_prepared_stripe_batch_with_sessions` hold permits across the await.
    write_inflight_limiter: Arc<AdaptiveWriteLimiter>,
}

#[derive(Debug)]
struct FragmentWriteFailure {
    stripe_index: u32,
    fragment_index: u32,
    message: String,
    /// True when the underlying KST replied HTTP 429 (KP2 backpressure).
    rate_limited: bool,
    /// `x-kp2-retry-after-ms` value, when the target advertised one.
    retry_after_ms: Option<u64>,
    /// `x-kp2-limit-max-in-flight` value, used to shrink the adaptive limiter.
    limit_max_inflight: Option<usize>,
}

impl FragmentWriteFailure {
    /// Build a failure carrying a structured 429 signal alongside the log
    /// message. Used at every wrap site so retry/backpressure logic can inspect
    /// the signal instead of re-parsing the flattened message.
    fn new(
        stripe_index: u32,
        fragment_index: u32,
        message: String,
        signal: RateLimitSignal,
    ) -> Self {
        Self {
            stripe_index,
            fragment_index,
            message,
            rate_limited: signal.rate_limited,
            retry_after_ms: signal.retry_after_ms,
            limit_max_inflight: signal.limit_max_inflight,
        }
    }

    /// Build a failure with no rate-limit signal (e.g. join errors, length
    /// mismatches) — the common case where there is no `ClientError` to inspect.
    fn plain(stripe_index: u32, fragment_index: u32, message: String) -> Self {
        Self::new(
            stripe_index,
            fragment_index,
            message,
            RateLimitSignal::default(),
        )
    }
}

#[derive(Default)]
struct RetryWriteResult {
    failures: Vec<FragmentWriteFailure>,
    phases: RequestPhaseTimes,
    connect_elapsed: Duration,
    write_elapsed: Duration,
}

struct PreparedStripeWrite {
    stripe_index: u32,
    plans: Vec<FragmentPlan>,
    fragments: Vec<Vec<u8>>,
}

struct PreparedStripeWriteResult {
    prepared: PreparedStripeWrite,
    failures: Vec<FragmentWriteFailure>,
}

struct PreparedStripeBatchWriteResult {
    stripe_results: Vec<PreparedStripeWriteResult>,
    phases: RequestPhaseTimes,
    write_elapsed: Duration,
}

struct BatchedTargetWritePlan {
    endpoint: String,
    target_id: String,
    stripe_index: u32,
    fragment_index: u32,
    granule_index: u64,
    generation: u32,
    chunk_id: ChunkId,
    payload: Vec<u8>,
}

struct BatchedTargetReadPlan {
    endpoint: String,
    stripe_index: u32,
    fragment_index: usize,
    chunk_id: ChunkId,
    payload_bytes: usize,
}

struct WindowStripeReadState {
    stripe_index: usize,
    needed_data_fragments: usize,
    fragments: Vec<Option<Vec<u8>>>,
}

struct StripeReadResult {
    payload: Vec<u8>,
    missing_fragments: usize,
    data_fragment_reads: usize,
    parity_fragment_reads: usize,
    reconstructed: bool,
    phases: ObjectPhaseTimes,
}

#[derive(Clone)]
struct KmsEndpointBalancer {
    channels: Arc<Vec<Channel>>,
    next: Arc<AtomicUsize>,
    grpc_max_message_bytes: usize,
}

/// Thread-safe pool of reusable EC shard buffer sets.
///
/// The encode/reconstruct work runs on `spawn_blocking` worker threads, so the
/// recycling pool has to be shareable and lockable rather than living behind the
/// `&mut self` of the async task. `plan` is cheap to clone (an `EcProfile` plus a
/// `&'static` table reference), so blocking tasks take an owned copy.
#[derive(Clone)]
struct ShardPool {
    plan: PreparedEcPlan,
    reusable_shards: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
}

impl ShardPool {
    fn new(plan: PreparedEcPlan) -> Self {
        Self {
            plan,
            reusable_shards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn take_shards(&self) -> Vec<Vec<u8>> {
        self.reusable_shards
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_else(|| self.plan.allocate_output_buffers())
    }

    fn return_shards(&self, shards: Vec<Vec<u8>>) {
        self.reusable_shards.lock().unwrap().push(shards);
    }
}

struct PreparedEncodeWorkspace {
    pool: ShardPool,
}

struct CachedResolvedRead {
    manifest: ObjectVersionManifest,
    ec_profile: EcProfile,
    inserted_at: Instant,
}

struct CachedPayloadRead {
    payload: Arc<[u8]>,
    inserted_at: Instant,
}

struct SharedObjectMetadataCacheInner {
    state: Mutex<SharedObjectMetadataCacheState>,
    read_resolve_cache_ttl: Duration,
    read_payload_cache_max_entries: usize,
    read_payload_cache_max_bytes: usize,
    read_payload_cache_max_object_bytes: usize,
}

struct SharedObjectMetadataCacheState {
    resolved_reads: HashMap<(String, String), CachedResolvedRead>,
    cached_payloads: HashMap<(String, String, String), CachedPayloadRead>,
    cached_payload_order: VecDeque<(String, String, String)>,
    cached_payload_bytes: usize,
}

#[derive(Clone)]
struct SharedObjectMetadataCache {
    inner: Arc<SharedObjectMetadataCacheInner>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SharedObjectMetadataCacheKey {
    kms_endpoints: Vec<String>,
    notification_nats_url: Option<String>,
    notification_subject: String,
    read_resolve_cache_ttl_ms: u64,
    read_payload_cache_max_entries: usize,
    read_payload_cache_max_bytes: usize,
    read_payload_cache_max_object_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct ObjectClientOptions {
    pub read_completion_mode: CompletionMode,
    pub write_completion_mode: CompletionMode,
    pub write_window_max_stripes: usize,
    pub write_window_inflight_stripes: usize,
    pub kms_grpc_max_message_bytes: usize,
    pub read_resolve_cache_ttl: Duration,
    pub read_payload_cache_max_entries: usize,
    pub read_payload_cache_max_bytes: usize,
    pub read_payload_cache_max_object_bytes: usize,
    pub metadata_notification_nats_url: Option<String>,
    pub metadata_notification_subject: String,
}

impl Default for ObjectClientOptions {
    fn default() -> Self {
        Self {
            read_completion_mode: CompletionMode::Interrupt,
            write_completion_mode: CompletionMode::Interrupt,
            write_window_max_stripes: DEFAULT_WRITE_WINDOW_MAX_STRIPES,
            write_window_inflight_stripes: DEFAULT_WRITE_WINDOW_INFLIGHT_STRIPES,
            kms_grpc_max_message_bytes: DEFAULT_KMS_GRPC_MAX_MESSAGE_BYTES,
            read_resolve_cache_ttl: DEFAULT_READ_RESOLVE_CACHE_TTL,
            read_payload_cache_max_entries: DEFAULT_READ_PAYLOAD_CACHE_MAX_ENTRIES,
            read_payload_cache_max_bytes: DEFAULT_READ_PAYLOAD_CACHE_MAX_BYTES,
            read_payload_cache_max_object_bytes: DEFAULT_READ_PAYLOAD_CACHE_MAX_OBJECT_BYTES,
            metadata_notification_nats_url: None,
            metadata_notification_subject: DEFAULT_METADATA_NOTIFICATION_SUBJECT.to_string(),
        }
    }
}

impl ObjectClientOptions {
    fn normalized(self) -> Self {
        Self {
            read_completion_mode: self.read_completion_mode,
            write_completion_mode: self.write_completion_mode,
            write_window_max_stripes: self.write_window_max_stripes.max(1),
            write_window_inflight_stripes: self.write_window_inflight_stripes.max(1),
            kms_grpc_max_message_bytes: self.kms_grpc_max_message_bytes.max(1),
            read_resolve_cache_ttl: if self.read_resolve_cache_ttl.is_zero() {
                DEFAULT_READ_RESOLVE_CACHE_TTL
            } else {
                self.read_resolve_cache_ttl
            },
            read_payload_cache_max_entries: self.read_payload_cache_max_entries.max(1),
            read_payload_cache_max_bytes: self.read_payload_cache_max_bytes.max(1),
            read_payload_cache_max_object_bytes: self
                .read_payload_cache_max_object_bytes
                .max(1)
                .min(self.read_payload_cache_max_bytes.max(1)),
            metadata_notification_nats_url: self
                .metadata_notification_nats_url
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            metadata_notification_subject: if self.metadata_notification_subject.trim().is_empty() {
                DEFAULT_METADATA_NOTIFICATION_SUBJECT.to_string()
            } else {
                self.metadata_notification_subject.trim().to_string()
            },
        }
    }
}

impl From<ObjectClientOptions> for TargetSessionOptions {
    fn from(value: ObjectClientOptions) -> Self {
        Self {
            read_completion_mode: value.read_completion_mode,
            write_completion_mode: value.write_completion_mode,
        }
    }
}

impl PreparedEncodeWorkspace {
    fn new(plan: PreparedEcPlan) -> Self {
        Self {
            pool: ShardPool::new(plan),
        }
    }
}

impl SharedObjectMetadataCache {
    fn new(options: &ObjectClientOptions) -> Self {
        Self {
            inner: Arc::new(SharedObjectMetadataCacheInner {
                state: Mutex::new(SharedObjectMetadataCacheState {
                    resolved_reads: HashMap::new(),
                    cached_payloads: HashMap::new(),
                    cached_payload_order: VecDeque::new(),
                    cached_payload_bytes: 0,
                }),
                read_resolve_cache_ttl: options.read_resolve_cache_ttl,
                read_payload_cache_max_entries: options.read_payload_cache_max_entries,
                read_payload_cache_max_bytes: options.read_payload_cache_max_bytes,
                read_payload_cache_max_object_bytes: options.read_payload_cache_max_object_bytes,
            }),
        }
    }

    fn shared_for(kms_endpoints: &[String], options: &ObjectClientOptions) -> Self {
        let key = SharedObjectMetadataCacheKey {
            kms_endpoints: canonicalize_endpoints(kms_endpoints),
            notification_nats_url: options.metadata_notification_nats_url.clone(),
            notification_subject: options.metadata_notification_subject.clone(),
            read_resolve_cache_ttl_ms: options.read_resolve_cache_ttl.as_millis() as u64,
            read_payload_cache_max_entries: options.read_payload_cache_max_entries,
            read_payload_cache_max_bytes: options.read_payload_cache_max_bytes,
            read_payload_cache_max_object_bytes: options.read_payload_cache_max_object_bytes,
        };
        let registry = global_object_metadata_caches();
        let mut registry = registry.lock().unwrap();
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Self { inner: existing };
        }
        let cache = Self::new(options);
        if let Some(nats_url) = options.metadata_notification_nats_url.clone() {
            cache.spawn_invalidator(nats_url, options.metadata_notification_subject.clone());
        }
        registry.insert(key, Arc::downgrade(&cache.inner));
        cache
    }

    fn cached_resolved_read(
        &self,
        bucket_id: &str,
        key: &str,
    ) -> Option<(ObjectVersionManifest, EcProfile)> {
        let cache_key = (bucket_id.to_string(), key.to_string());
        let mut state = self.inner.state.lock().unwrap();
        let cached = state.resolved_reads.get(&cache_key)?;
        if cached.inserted_at.elapsed() > self.inner.read_resolve_cache_ttl {
            state.resolved_reads.remove(&cache_key);
            return None;
        }
        Some((cached.manifest.clone(), cached.ec_profile.clone()))
    }

    fn cache_resolved_read(&self, manifest: ObjectVersionManifest, ec_profile: EcProfile) {
        self.inner.state.lock().unwrap().resolved_reads.insert(
            (manifest.bucket_id.clone(), manifest.key.clone()),
            CachedResolvedRead {
                manifest,
                ec_profile,
                inserted_at: Instant::now(),
            },
        );
    }

    fn cached_payload_read(&self, manifest: &ObjectVersionManifest) -> Option<Vec<u8>> {
        let cache_key = (
            manifest.bucket_id.clone(),
            manifest.key.clone(),
            manifest.version_id.clone(),
        );
        let mut state = self.inner.state.lock().unwrap();
        let cached = state.cached_payloads.get(&cache_key)?;
        if cached.inserted_at.elapsed() > self.inner.read_resolve_cache_ttl {
            Self::remove_cached_payload_locked(&mut state, &cache_key);
            return None;
        }
        Some(cached.payload.as_ref().to_vec())
    }

    fn cache_payload_read(&self, manifest: &ObjectVersionManifest, payload: &[u8]) {
        if payload.is_empty() || payload.len() > self.inner.read_payload_cache_max_object_bytes {
            return;
        }
        let cache_key = (
            manifest.bucket_id.clone(),
            manifest.key.clone(),
            manifest.version_id.clone(),
        );
        let payload_arc: Arc<[u8]> = Arc::from(payload.to_vec());
        let payload_len = payload_arc.len();
        let mut state = self.inner.state.lock().unwrap();
        Self::remove_cached_payload_locked(&mut state, &cache_key);
        while state.cached_payloads.len() >= self.inner.read_payload_cache_max_entries
            || state.cached_payload_bytes.saturating_add(payload_len)
                > self.inner.read_payload_cache_max_bytes
        {
            let Some(oldest) = state.cached_payload_order.pop_front() else {
                break;
            };
            Self::remove_cached_payload_locked(&mut state, &oldest);
        }
        state.cached_payload_bytes = state.cached_payload_bytes.saturating_add(payload_len);
        state.cached_payload_order.push_back(cache_key.clone());
        state.cached_payloads.insert(
            cache_key,
            CachedPayloadRead {
                payload: payload_arc,
                inserted_at: Instant::now(),
            },
        );
    }

    fn invalidate_key(&self, bucket_id: &str, key: &str) {
        let mut state = self.inner.state.lock().unwrap();
        state
            .resolved_reads
            .remove(&(bucket_id.to_string(), key.to_string()));
        let cache_keys = state
            .cached_payloads
            .keys()
            .filter(|(cached_bucket, cached_key, _)| {
                cached_bucket == bucket_id && cached_key == key
            })
            .cloned()
            .collect::<Vec<_>>();
        for cache_key in cache_keys {
            Self::remove_cached_payload_locked(&mut state, &cache_key);
        }
    }

    fn invalidate_namespace(&self, namespace_id: &str) {
        let mut state = self.inner.state.lock().unwrap();
        let removed_keys = state
            .resolved_reads
            .iter()
            .filter(|(_, cached)| cached.manifest.namespace_id == namespace_id)
            .map(|((bucket_id, key), _)| (bucket_id.clone(), key.clone()))
            .collect::<Vec<_>>();
        state
            .resolved_reads
            .retain(|_, cached| cached.manifest.namespace_id != namespace_id);
        let cache_keys = state
            .cached_payloads
            .keys()
            .filter(|(bucket_id, key, _)| {
                removed_keys.iter().any(|(removed_bucket, removed_key)| {
                    removed_bucket == bucket_id && removed_key == key
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        for cache_key in cache_keys {
            Self::remove_cached_payload_locked(&mut state, &cache_key);
        }
    }

    fn invalidate_all(&self) {
        let mut state = self.inner.state.lock().unwrap();
        state.resolved_reads.clear();
        state.cached_payloads.clear();
        state.cached_payload_order.clear();
        state.cached_payload_bytes = 0;
    }

    fn remove_cached_payload_locked(
        state: &mut SharedObjectMetadataCacheState,
        cache_key: &(String, String, String),
    ) {
        if let Some(cached) = state.cached_payloads.remove(cache_key) {
            state.cached_payload_bytes = state
                .cached_payload_bytes
                .saturating_sub(cached.payload.len());
        }
        if let Some(position) = state
            .cached_payload_order
            .iter()
            .position(|candidate| candidate == cache_key)
        {
            state.cached_payload_order.remove(position);
        }
    }

    fn spawn_invalidator(&self, nats_url: String, subject: String) {
        let cache = self.clone();
        tokio::spawn(async move {
            cache.nats_invalidation_loop(nats_url, subject).await;
        });
    }

    async fn nats_invalidation_loop(self, nats_url: String, subject: String) {
        loop {
            match async_nats::connect(&nats_url).await {
                Ok(client) => match client.subscribe(subject.clone()).await {
                    Ok(mut subscriber) => {
                        while let Some(message) = subscriber.next().await {
                            let event =
                                decode_metadata_invalidation_event(message.payload.as_ref());
                            if event.namespace_id.is_empty() {
                                self.invalidate_all();
                            } else if !event.bucket_id.is_empty() && !event.key.is_empty() {
                                self.invalidate_key(&event.bucket_id, &event.key);
                            } else {
                                self.invalidate_namespace(&event.namespace_id);
                            }
                        }
                    }
                    Err(_) => {}
                },
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

fn global_object_metadata_caches(
) -> &'static Mutex<HashMap<SharedObjectMetadataCacheKey, Weak<SharedObjectMetadataCacheInner>>> {
    static CACHES: OnceLock<
        Mutex<HashMap<SharedObjectMetadataCacheKey, Weak<SharedObjectMetadataCacheInner>>>,
    > = OnceLock::new();
    CACHES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn canonicalize_endpoints(endpoints: &[String]) -> Vec<String> {
    let mut canonical = endpoints
        .iter()
        .map(|endpoint| endpoint.trim().to_string())
        .filter(|endpoint| !endpoint.is_empty())
        .collect::<Vec<_>>();
    canonical.sort();
    canonical.dedup();
    canonical
}

fn decode_metadata_invalidation_event(payload: &[u8]) -> MetadataInvalidationEvent {
    MetadataInvalidationEvent::decode(payload).unwrap_or_else(|_| MetadataInvalidationEvent {
        namespace_id: std::str::from_utf8(payload)
            .map(str::trim)
            .unwrap_or_default()
            .to_string(),
        bucket_id: String::new(),
        key: String::new(),
        entry_id: String::new(),
        parent_entry_id: String::new(),
        event_kind: 0,
        version_id: String::new(),
    })
}

impl ObjectClient {
    pub async fn connect(kms_endpoints: &[String]) -> Result<Self, ObjectError> {
        Self::connect_with_options(kms_endpoints, ObjectClientOptions::default()).await
    }

    pub async fn connect_with_options(
        kms_endpoints: &[String],
        options: ObjectClientOptions,
    ) -> Result<Self, ObjectError> {
        let options = options.normalized();
        let session_options = TargetSessionOptions::from(options.clone());
        let shared_read_cache = SharedObjectMetadataCache::shared_for(kms_endpoints, &options);
        Ok(Self {
            kms: KmsEndpointBalancer::connect(kms_endpoints, options.kms_grpc_max_message_bytes)
                .await?,
            target_sessions: HashMap::new(),
            shared_target_sessions: global_target_sessions(),
            bucket_profiles: HashMap::new(),
            shared_read_cache,
            prepared_encoders: HashMap::new(),
            session_options,
            write_window_max_stripes: options.write_window_max_stripes,
            write_window_inflight_stripes: options.write_window_inflight_stripes,
            write_inflight_limiter: Arc::new(AdaptiveWriteLimiter::new(
                options.write_window_inflight_stripes,
            )),
        })
    }

    pub async fn put_object_single_stripe(
        &mut self,
        bucket_id: &str,
        key: &str,
        payload: &[u8],
    ) -> Result<ObjectPutResult, ObjectError> {
        self.invalidate_resolved_read(bucket_id, key);
        let result = self
            .put_object_stream(bucket_id, key, payload.len() as u64, |offset, len| {
                let start = usize::try_from(offset).map_err(|_| {
                    ObjectError::Metadata(format!(
                        "object write offset {} overflowed payload indexing for {}/{}",
                        offset, bucket_id, key
                    ))
                })?;
                let end = start.saturating_add(len);
                let chunk = payload.get(start..end).ok_or_else(|| {
                    ObjectError::Metadata(format!(
                        "object write chunk {}..{} is out of range for {}/{} payload {}",
                        start,
                        end,
                        bucket_id,
                        key,
                        payload.len()
                    ))
                })?;
                Ok(chunk.to_vec())
            })
            .await?;
        self.cache_payload_read(&result.manifest, payload);
        Ok(result)
    }

    pub async fn put_object_from_path(
        &mut self,
        bucket_id: &str,
        key: &str,
        path: &Path,
    ) -> Result<ObjectPutResult, ObjectError> {
        self.invalidate_resolved_read(bucket_id, key);
        let metadata = std::fs::metadata(path).map_err(|err| {
            ObjectError::Metadata(format!(
                "failed to stat object payload path {}: {err}",
                path.display()
            ))
        })?;
        let logical_length_bytes = metadata.len();
        let mut file = File::open(path).map_err(|err| {
            ObjectError::Metadata(format!(
                "failed to open object payload path {}: {err}",
                path.display()
            ))
        })?;
        self.put_object_stream(bucket_id, key, logical_length_bytes, move |offset, len| {
            file.seek(SeekFrom::Start(offset)).map_err(|err| {
                ObjectError::Metadata(format!(
                    "failed to seek object payload path {} to {}: {err}",
                    path.display(),
                    offset
                ))
            })?;
            let mut chunk = vec![0u8; len];
            file.read_exact(&mut chunk).map_err(|err| {
                ObjectError::Metadata(format!(
                    "failed to read {} bytes from object payload path {} at {}: {err}",
                    len,
                    path.display(),
                    offset
                ))
            })?;
            Ok(chunk)
        })
        .await
    }

    async fn put_object_stream<F>(
        &mut self,
        bucket_id: &str,
        key: &str,
        logical_length_bytes: u64,
        mut read_range: F,
    ) -> Result<ObjectPutResult, ObjectError>
    where
        F: FnMut(u64, usize) -> Result<Vec<u8>, ObjectError>,
    {
        let mut phases = ObjectPhaseTimes::default();
        let initiated_started = Instant::now();
        let mut kms = self.kms.client();
        let initiated = control_rpc(
            "KMS InitiateObjectWrite",
            kms.initiate_object_write(InitiateObjectWriteRequest {
                bucket_id: bucket_id.to_string(),
                key: key.to_string(),
                logical_length_bytes,
                initial_window_stripe_count: self.write_window_max_stripes as u32,
            }),
        )
        .await?
        .into_inner();
        phases.kms_initiate = initiated_started.elapsed();
        let mut intent = initiated.intent.ok_or_else(|| {
            ObjectError::Metadata("KMS InitiateObjectWrite did not return intent".to_string())
        })?;
        let ec_profile = initiated.ec_profile.ok_or_else(|| {
            ObjectError::Metadata("KMS InitiateObjectWrite did not return ec_profile".to_string())
        })?;
        self.bucket_profiles
            .insert(bucket_id.to_string(), ec_profile.clone());
        let profile_id = ec_profile.id.clone();
        let stripe_logical_bytes = stripe_logical_bytes(&ec_profile)?;
        let stripe_count = usize::try_from(intent.stripe_count).map_err(|_| {
            ObjectError::Metadata(format!(
                "write intent {} declares invalid stripe count {}",
                intent.intent_id, intent.stripe_count
            ))
        })?;
        let mut initial_window_plans = initiated.initial_fragment_plans;
        let mut initial_window_stripe_count = initiated.initial_window_stripe_count as usize;
        if initial_window_stripe_count == 0 {
            initial_window_stripe_count = fragment_window_stripe_count(&initial_window_plans);
        }

        let result = async {
            let mut window_start = 0usize;
            while window_start < stripe_count {
                let default_window_stripe_count = (stripe_count - window_start)
                    .min(self.write_window_max_stripes);
                let (window_stripe_count, window_plans) = if window_start == 0
                    && initial_window_stripe_count > 0
                    && !initial_window_plans.is_empty()
                {
                    (
                        initial_window_stripe_count.min(default_window_stripe_count),
                        std::mem::take(&mut initial_window_plans),
                    )
                } else {
                    let phase_started = Instant::now();
                    let reserved = control_rpc(
                        "KMS ReserveObjectWriteWindow",
                        kms.reserve_object_write_window(ReserveObjectWriteWindowRequest {
                            intent_id: intent.intent_id.clone(),
                            start_stripe_index: window_start as u32,
                            stripe_count: default_window_stripe_count as u32,
                        }),
                    )
                    .await?
                    .into_inner();
                    phases.kms_initiate += phase_started.elapsed();
                    (default_window_stripe_count, reserved.fragment_plans)
                };
                // Producer/consumer pipeline: the CPU-bound EC encode of the next
                // inflight batch (off the tokio runtime via spawn_blocking) overlaps
                // the network fragment writes of the current batch, instead of the
                // old behaviour of encoding the whole inflight batch before issuing
                // any I/O. `prefetched` holds the already-encoded next batch.
                let pool = self.prepared_shard_pool(&ec_profile)?;
                let mut prefetched: Option<EncodedStripeBatch> = None;
                let mut stripe_offset = 0usize;
                while stripe_offset < window_stripe_count {
                    let stripe_batch_len = (window_stripe_count - stripe_offset)
                        .min(self.write_window_inflight_stripes);

                    let EncodedStripeBatch {
                        prepared_batch,
                        batch_plans,
                        ec_encode: _,
                    } = match prefetched.take() {
                        Some(batch) => batch,
                        None => {
                            let batch = encode_stripe_batch(
                                pool.clone(),
                                &window_plans,
                                window_start,
                                stripe_offset,
                                stripe_batch_len,
                                logical_length_bytes as usize,
                                stripe_logical_bytes,
                                &mut read_range,
                            )
                            .await?;
                            phases.ec_encode += batch.ec_encode;
                            batch
                        }
                    };

                    let phase_started = Instant::now();
                    self.ensure_target_sessions(&batch_plans).await?;
                    phases.target_connect += phase_started.elapsed();
                    let session_snapshot = Arc::new(snapshot_target_sessions(
                        &self.target_sessions,
                        &batch_plans,
                        &intent.intent_id,
                    )?);

                    // Determine whether a subsequent inflight batch exists; if so,
                    // encode it concurrently with the current batch's network writes.
                    let next_offset = stripe_offset + stripe_batch_len;
                    let next_batch_len = (window_stripe_count.saturating_sub(next_offset))
                        .min(self.write_window_inflight_stripes);

                    let intent_id = intent.intent_id.clone();
                    let write_future = write_prepared_stripe_batch_with_sessions(
                        session_snapshot,
                        &intent_id,
                        prepared_batch,
                        Arc::clone(&self.write_inflight_limiter),
                    );

                    let batch_write = if next_batch_len > 0 {
                        let encode_future = encode_stripe_batch(
                            pool.clone(),
                            &window_plans,
                            window_start,
                            next_offset,
                            next_batch_len,
                            logical_length_bytes as usize,
                            stripe_logical_bytes,
                            &mut read_range,
                        );
                        let (batch_write, next_batch) = tokio::join!(write_future, encode_future);
                        let next_batch = next_batch?;
                        phases.ec_encode += next_batch.ec_encode;
                        prefetched = Some(next_batch);
                        batch_write?
                    } else {
                        write_future.await?
                    };
                    accumulate_target_request_phases(&mut phases, &batch_write.phases);
                    phases.target_write += batch_write.write_elapsed;

                    for stripe_result in batch_write.stripe_results {
                        let stripe_index = stripe_result.prepared.stripe_index;
                        let mut write_failures = stripe_result.failures;

                        if !write_failures.is_empty() {
                            let retry_result = self
                                .retry_fragment_failures_same_target(
                                    &intent.intent_id,
                                    &stripe_result.prepared.plans,
                                    &stripe_result.prepared.fragments,
                                    std::mem::take(&mut write_failures),
                                )
                                .await?;
                            accumulate_target_request_phases(&mut phases, &retry_result.phases);
                            phases.target_connect += retry_result.connect_elapsed;
                            phases.target_write += retry_result.write_elapsed;
                            write_failures = retry_result.failures;
                        }

                        if !write_failures.is_empty() {
                            if write_failures.len() > ec_profile.parity_fragments as usize {
                                let _ = kms
                                    .abort_object_write(AbortObjectWriteRequest {
                                        intent_id: intent.intent_id.clone(),
                                    })
                                    .await;
                                self.return_prepared_shards(
                                    &profile_id,
                                    stripe_result.prepared.fragments,
                                );
                                return Err(ObjectError::Transport(format!(
                                    "KSC object write to {}/{} failed on stripe {} before commit: {}",
                                    bucket_id,
                                    key,
                                    stripe_index,
                                    join_fragment_failures(&write_failures)
                                )));
                            }

                            let failed_fragments = write_failures
                                .iter()
                                .map(|failure| FragmentRef {
                                    stripe_index: failure.stripe_index,
                                    fragment_index: failure.fragment_index,
                                })
                                .collect::<Vec<_>>();
                            let repaired = control_rpc(
                                "KMS RepairObjectWrite",
                                kms.repair_object_write(RepairObjectWriteRequest {
                                    intent_id: intent.intent_id.clone(),
                                    failed_fragments: failed_fragments.clone(),
                                }),
                            )
                            .await?
                            .into_inner()
                            .intent
                            .ok_or_else(|| {
                                ObjectError::Metadata(
                                    "KMS RepairObjectWrite did not return repaired intent".to_string(),
                                )
                            })?;
                            let retry_plans = failed_fragments
                                .iter()
                                .map(|fragment_ref| {
                                    repaired
                                        .fragment_plans
                                        .iter()
                                        .find(|plan| {
                                            plan.stripe_index == fragment_ref.stripe_index
                                                && plan.fragment_index == fragment_ref.fragment_index
                                        })
                                        .cloned()
                                        .ok_or_else(|| {
                                            ObjectError::Metadata(format!(
                                                "repaired intent {} is missing stripe {} fragment plan {}",
                                                repaired.intent_id,
                                                fragment_ref.stripe_index,
                                                fragment_ref.fragment_index
                                            ))
                                        })
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                            let phase_started = Instant::now();
                            self.ensure_target_sessions(&retry_plans).await?;
                            phases.target_connect += phase_started.elapsed();
                            let phase_started = Instant::now();
                            let (retry_write_failures, retry_rpc_phases) = self
                                .write_fragment_plans(
                                    &repaired.intent_id,
                                    &retry_plans,
                                    &stripe_result.prepared.fragments,
                                )
                                .await?;
                            write_failures = retry_write_failures;
                            accumulate_target_request_phases(&mut phases, &retry_rpc_phases);
                            phases.target_write += phase_started.elapsed();

                            if !write_failures.is_empty() {
                                let retry_result = self
                                    .retry_fragment_failures_same_target(
                                        &repaired.intent_id,
                                        &retry_plans,
                                        &stripe_result.prepared.fragments,
                                        std::mem::take(&mut write_failures),
                                    )
                                    .await?;
                                accumulate_target_request_phases(&mut phases, &retry_result.phases);
                                phases.target_connect += retry_result.connect_elapsed;
                                phases.target_write += retry_result.write_elapsed;
                                write_failures = retry_result.failures;
                            }

                            if !write_failures.is_empty() {
                                let _ = kms
                                    .abort_object_write(AbortObjectWriteRequest {
                                        intent_id: repaired.intent_id.clone(),
                                    })
                                    .await;
                                self.return_prepared_shards(
                                    &profile_id,
                                    stripe_result.prepared.fragments,
                                );
                                return Err(ObjectError::Transport(format!(
                                    "KSC object write to {}/{} failed on stripe {} after repair: {}",
                                    bucket_id,
                                    key,
                                    stripe_index,
                                    join_fragment_failures(&write_failures)
                                )));
                            }
                            intent = repaired;
                        }

                        self.return_prepared_shards(&profile_id, stripe_result.prepared.fragments);
                    }

                    stripe_offset += stripe_batch_len;
                }

                let phase_started = Instant::now();
                control_rpc(
                    "KMS CommitObjectWriteWindow",
                    kms.commit_object_write_window(CommitObjectWriteWindowRequest {
                        intent_id: intent.intent_id.clone(),
                        successful_fragments: window_plans
                            .iter()
                            .map(|plan| FragmentRef {
                                stripe_index: plan.stripe_index,
                                fragment_index: plan.fragment_index,
                            })
                            .collect(),
                    }),
                )
                .await?;
                phases.kms_commit += phase_started.elapsed();
                window_start += window_stripe_count;
            }

            let phase_started = Instant::now();
            let committed = control_rpc(
                "KMS CommitObjectWrite",
                kms.commit_object_write(CommitObjectWriteRequest {
                    intent_id: intent.intent_id.clone(),
                    successful_fragments: Vec::new(),
                }),
            )
            .await?
            .into_inner();
            phases.kms_commit += phase_started.elapsed();
            let manifest = committed.manifest.ok_or_else(|| {
                ObjectError::Metadata("KMS CommitObjectWrite did not return manifest".to_string())
            })?;
            self.cache_resolved_read(manifest.clone(), ec_profile.clone());
            Ok(ObjectPutResult {
                intent,
                manifest,
                ec_profile,
                phases,
            })
        }
        .await;
        result
    }

    /// Stripe-granular ranged read: returns the object bytes in `[offset,
    /// offset+len)` (clamped to the object length) by reading only the stripes
    /// the range touches, rather than materializing the whole object. Built on
    /// `read_single_stripe` + the uniform stripe geometry; no protocol change.
    /// Byte-granular fast path for a clamped window `[start, end)`: when the
    /// window lies entirely inside ONE present data fragment, fetch just those
    /// bytes with a single ranged KP2 packed read (one chunk, one sub-range)
    /// instead of reading the whole stripe. Returns `Ok(None)` — so the caller
    /// falls back to the full stripe loop (with reconstruction) — when the
    /// window spans fragments/stripes, lands in the parity-padded tail, or the
    /// fragment read does not return exactly the requested bytes (e.g. missing).
    async fn try_byte_granular_read(
        &mut self,
        manifest: &ObjectVersionManifest,
        ec_profile: &EcProfile,
        object_len: u64,
        start: u64,
        end: u64,
        phases: &mut ObjectPhaseTimes,
    ) -> Result<Option<RangedGetResult>, ObjectError> {
        let fragment_bytes = ec_profile.fragment_bytes as u64;
        let data_fragments = ec_profile.data_fragments as usize;
        if fragment_bytes == 0 || data_fragments == 0 || end <= start {
            return Ok(None);
        }
        let stripe_width = fragment_bytes * data_fragments as u64;
        // Single stripe?
        if start / stripe_width != (end - 1) / stripe_width {
            return Ok(None);
        }
        let stripe_index = (start / stripe_width) as usize;
        let stripe_local_start = start - stripe_index as u64 * stripe_width;
        let stripe_local_end = end - stripe_index as u64 * stripe_width;
        // Single data fragment?
        if stripe_local_start / fragment_bytes != (stripe_local_end - 1) / fragment_bytes {
            return Ok(None);
        }
        let fragment_index = (stripe_local_start / fragment_bytes) as usize;
        // Must be a real data fragment for this (possibly partial) stripe.
        let stripe_logical_bytes =
            stripe_logical_length_bytes(manifest, ec_profile, stripe_index as u32);
        if fragment_index >= needed_data_fragment_count(ec_profile, stripe_logical_bytes) {
            return Ok(None);
        }
        let Some(stripe) = manifest.stripes.get(stripe_index) else {
            return Ok(None);
        };
        let Some(plan) = stripe.fragments.get(fragment_index) else {
            return Ok(None);
        };
        let chunk_id = chunk_id_from_proto(&plan.chunk_id)?;
        let fragment_offset = stripe_local_start - fragment_index as u64 * fragment_bytes;
        let fragment_len = (end - start) as u32;

        let plans = std::slice::from_ref(plan);
        let (connect_elapsed, _connect_failures) = self.ensure_read_target_sessions(plans).await;
        phases.target_connect += connect_elapsed;
        let Some(session) = self.target_sessions.get(&plan.endpoint).cloned() else {
            return Ok(None);
        };

        let query = PackedReadQuery {
            chunk_ids: vec![chunk_id],
            ranges: Some(vec![ChunkRange {
                offset: fragment_offset,
                length: fragment_len,
            }]),
        };
        let read_started = Instant::now();
        let reply = match data_rpc(
            "target ranged read",
            TARGET_IO_TIMEOUT,
            session.packed_read(&query, fragment_len as usize),
        )
        .await
        {
            Ok(reply) => reply,
            // Any RPC failure: fall back to the proven full-stripe path.
            Err(_) => return Ok(None),
        };
        phases.target_read += read_started.elapsed();
        accumulate_target_request_phases(phases, &reply.phases);
        let Some(entry) = reply.value.entries.into_iter().next() else {
            return Ok(None);
        };
        // 200 with exactly the requested bytes, or fall back (missing fragment,
        // short read, or anything unexpected).
        if entry.status_code != 200 || entry.payload.len() != fragment_len as usize {
            return Ok(None);
        }
        Ok(Some(RangedGetResult {
            payload: entry.payload,
            offset: start,
            object_length_bytes: object_len,
            manifest: manifest.clone(),
            ec_profile: ec_profile.clone(),
            phases: *phases,
            missing_fragments: 0,
            data_fragment_reads: 1,
            parity_fragment_reads: 0,
            reconstructed: false,
        }))
    }

    pub async fn get_object_range(
        &mut self,
        bucket_id: &str,
        key: &str,
        offset: u64,
        len: u64,
    ) -> Result<RangedGetResult, ObjectError> {
        let mut phases = ObjectPhaseTimes::default();
        let (manifest, ec_profile) = if let Some(cached) = self.cached_resolved_read(bucket_id, key)
        {
            cached
        } else {
            let phase_started = Instant::now();
            let mut kms = self.kms.client();
            let resolved = control_rpc(
                "KMS ResolveObjectRead",
                kms.resolve_object_read(ResolveObjectReadRequest {
                    bucket_id: bucket_id.to_string(),
                    key: key.to_string(),
                }),
            )
            .await?
            .into_inner();
            phases.kms_resolve = phase_started.elapsed();
            let manifest = resolved.manifest.ok_or_else(|| {
                ObjectError::Metadata("KMS ResolveObjectRead did not return manifest".to_string())
            })?;
            let ec_profile = resolved.ec_profile.ok_or_else(|| {
                ObjectError::Metadata("KMS ResolveObjectRead did not return ec_profile".to_string())
            })?;
            self.cache_resolved_read(manifest.clone(), ec_profile.clone());
            (manifest, ec_profile)
        };

        // Clamp the requested window to the object's logical length.
        let object_len = manifest.logical_length_bytes;
        let start = offset.min(object_len);
        let end = offset.saturating_add(len).min(object_len);
        let empty = |manifest: ObjectVersionManifest, ec_profile: EcProfile, phases| RangedGetResult {
            payload: Vec::new(),
            offset: start,
            object_length_bytes: object_len,
            manifest,
            ec_profile,
            phases,
            missing_fragments: 0,
            data_fragment_reads: 0,
            parity_fragment_reads: 0,
            reconstructed: false,
        };
        if end <= start {
            return Ok(empty(manifest, ec_profile, phases));
        }

        // Whole-object payload-cache fast path: slice the window out of a cached
        // full payload with zero target I/O. Never write a partial slice back —
        // the payload cache is keyed whole-object.
        if let Some(full) = self.cached_payload_read(&manifest) {
            let lo = (start as usize).min(full.len());
            let hi = (end as usize).min(full.len());
            let mut result = empty(manifest, ec_profile, phases);
            if lo < hi {
                result.payload = full[lo..hi].to_vec();
            }
            return Ok(result);
        }

        if manifest.stripes.is_empty() {
            return Err(ObjectError::Metadata(
                "manifest has no stripes for a non-empty ranged read".to_string(),
            ));
        }

        // Byte-granular fast path: a window inside one present data fragment is
        // served by a single ranged KP2 packed read (~the asked bytes), not the
        // whole stripe. Falls through to the full stripe loop on any miss.
        if let Some(result) = self
            .try_byte_granular_read(&manifest, &ec_profile, object_len, start, end, &mut phases)
            .await?
        {
            return Ok(result);
        }

        // read_single_stripe's reconstruct path needs the encoder prepared.
        self.ensure_prepared_encoder(&ec_profile)?;

        // Uniform stripe width W = data_fragments * fragment_bytes. Every stripe
        // but the last is exactly W; read_single_stripe truncates each returned
        // payload to that stripe's logical length, so we slice directly into it.
        let stripe_width = stripe_logical_bytes(&ec_profile)? as u64;
        if stripe_width == 0 {
            return Err(ObjectError::Metadata(
                "EC profile has a zero-width stripe geometry".to_string(),
            ));
        }
        let (first_stripe, last_stripe) = range_to_stripe_indices(start, end, stripe_width);
        if last_stripe >= manifest.stripes.len() {
            return Err(ObjectError::Metadata(format!(
                "ranged read [{start}, {end}) maps to stripe {last_stripe} but manifest carries {} stripes",
                manifest.stripes.len()
            )));
        }

        let mut out = Vec::with_capacity((end - start) as usize);
        let mut missing_fragments = 0_usize;
        let mut data_fragment_reads = 0_usize;
        let mut parity_fragment_reads = 0_usize;
        let mut reconstructed_any = false;

        for stripe_index in first_stripe..=last_stripe {
            let stripe = self
                .read_single_stripe(bucket_id, key, &manifest, &ec_profile, stripe_index)
                .await?;
            add_object_phase_times(&mut phases, &stripe.phases);
            missing_fragments += stripe.missing_fragments;
            data_fragment_reads += stripe.data_fragment_reads;
            parity_fragment_reads += stripe.parity_fragment_reads;
            reconstructed_any |= stripe.reconstructed;

            let (slice_start, slice_end) =
                stripe_slice_bounds(start, end, stripe_index, stripe_width, stripe.payload.len());
            if slice_start < slice_end {
                out.extend_from_slice(&stripe.payload[slice_start..slice_end]);
            }
        }

        Ok(RangedGetResult {
            payload: out,
            offset: start,
            object_length_bytes: object_len,
            manifest,
            ec_profile,
            phases,
            missing_fragments,
            data_fragment_reads,
            parity_fragment_reads,
            reconstructed: reconstructed_any,
        })
    }

    pub async fn get_object_single_stripe(
        &mut self,
        bucket_id: &str,
        key: &str,
    ) -> Result<ObjectGetResult, ObjectError> {
        let mut phases = ObjectPhaseTimes::default();
        let (manifest, ec_profile) = if let Some(cached) = self.cached_resolved_read(bucket_id, key)
        {
            cached
        } else {
            let phase_started = Instant::now();
            let mut kms = self.kms.client();
            let resolved = control_rpc(
                "KMS ResolveObjectRead",
                kms.resolve_object_read(ResolveObjectReadRequest {
                    bucket_id: bucket_id.to_string(),
                    key: key.to_string(),
                }),
            )
            .await?
            .into_inner();
            phases.kms_resolve = phase_started.elapsed();
            let manifest = resolved.manifest.ok_or_else(|| {
                ObjectError::Metadata("KMS ResolveObjectRead did not return manifest".to_string())
            })?;
            let ec_profile = resolved.ec_profile.ok_or_else(|| {
                ObjectError::Metadata("KMS ResolveObjectRead did not return ec_profile".to_string())
            })?;
            self.cache_resolved_read(manifest.clone(), ec_profile.clone());
            (manifest, ec_profile)
        };
        if let Some(payload) = self.cached_payload_read(&manifest) {
            return Ok(ObjectGetResult {
                payload,
                manifest,
                ec_profile,
                phases,
                missing_fragments: 0,
                data_fragment_reads: 0,
                parity_fragment_reads: 0,
                reconstructed: false,
            });
        }
        self.ensure_prepared_encoder(&ec_profile)?;
        let data_fragments = ec_profile.data_fragments as usize;
        if manifest.stripes.is_empty() {
            if manifest.logical_length_bytes == 0 {
                return Ok(ObjectGetResult {
                    payload: Vec::new(),
                    manifest,
                    ec_profile,
                    phases,
                    missing_fragments: 0,
                    data_fragment_reads: 0,
                    parity_fragment_reads: 0,
                    reconstructed: false,
                });
            }
            return Err(ObjectError::Metadata("manifest has no stripes".to_string()));
        }
        let payload_capacity = usize::try_from(manifest.logical_length_bytes).unwrap_or(usize::MAX);
        let mut out = Vec::with_capacity(payload_capacity);
        let mut missing_fragments = 0_usize;
        let mut data_fragment_reads = 0_usize;
        let mut parity_fragment_reads = 0_usize;
        let mut reconstructed_any = false;

        for window_start in (0..manifest.stripes.len()).step_by(DEFAULT_READ_WINDOW_MAX_STRIPES) {
            let window_end =
                (window_start + DEFAULT_READ_WINDOW_MAX_STRIPES).min(manifest.stripes.len());
            let mut window_states = Vec::with_capacity(window_end - window_start);
            let mut window_plans = Vec::new();
            for stripe_index in window_start..window_end {
                let stripe = &manifest.stripes[stripe_index];
                if stripe.fragments.len() < data_fragments {
                    return Err(ObjectError::Metadata(format!(
                        "manifest stripe only carries {} fragments but profile {} needs {} data fragments",
                        stripe.fragments.len(),
                        ec_profile.id,
                        data_fragments
                    )));
                }
                let stripe_logical_bytes =
                    stripe_logical_length_bytes(&manifest, &ec_profile, stripe_index as u32);
                let needed_data_fragments =
                    needed_data_fragment_count(&ec_profile, stripe_logical_bytes);
                for fragment_index in 0..needed_data_fragments {
                    let plan = stripe.fragments[fragment_index].clone();
                    window_plans.push(BatchedTargetReadPlan {
                        endpoint: plan.endpoint.clone(),
                        stripe_index: stripe_index as u32,
                        fragment_index,
                        chunk_id: chunk_id_from_proto(&plan.chunk_id)?,
                        payload_bytes: data_fragment_payload_bytes(
                            &ec_profile,
                            stripe_logical_bytes,
                            fragment_index,
                        ),
                    });
                }
                window_states.push(WindowStripeReadState {
                    stripe_index,
                    needed_data_fragments,
                    fragments: vec![None; needed_data_fragments],
                });
            }

            let needed_connect_plans = window_states
                .iter()
                .flat_map(|state| {
                    manifest.stripes[state.stripe_index]
                        .fragments
                        .iter()
                        .take(state.needed_data_fragments)
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let (connect_elapsed, _) = self
                .ensure_read_target_sessions(&needed_connect_plans)
                .await;
            phases.target_connect += connect_elapsed;

            let batched_read = read_plans_batched_with_sessions(
                Arc::new(self.target_sessions.clone()),
                window_start,
                &mut window_states,
                window_plans,
            )
            .await?;
            accumulate_target_request_phases(&mut phases, &batched_read.phases);
            phases.target_read += batched_read.read_elapsed;
            data_fragment_reads += needed_connect_plans.len();

            for state in window_states {
                if state.fragments.iter().all(Option::is_some) {
                    for fragment in state.fragments {
                        out.extend_from_slice(fragment.as_ref().ok_or_else(|| {
                            ObjectError::Metadata(
                                "healthy batched read path lost a data fragment after validation"
                                    .to_string(),
                            )
                        })?);
                    }
                    continue;
                }

                let stripe_result = self
                    .read_single_stripe(bucket_id, key, &manifest, &ec_profile, state.stripe_index)
                    .await?;
                out.extend_from_slice(&stripe_result.payload);
                missing_fragments += stripe_result.missing_fragments;
                data_fragment_reads += stripe_result.data_fragment_reads;
                parity_fragment_reads += stripe_result.parity_fragment_reads;
                reconstructed_any |= stripe_result.reconstructed;
                add_object_phase_times(&mut phases, &stripe_result.phases);
            }
        }

        out.truncate(manifest.logical_length_bytes as usize);
        self.cache_payload_read(&manifest, &out);
        Ok(ObjectGetResult {
            payload: out,
            manifest,
            ec_profile,
            phases,
            missing_fragments,
            data_fragment_reads,
            parity_fragment_reads,
            reconstructed: reconstructed_any,
        })
    }

    pub async fn delete_object(
        &mut self,
        bucket_id: &str,
        key: &str,
        version_ids: &[String],
    ) -> Result<ObjectDeleteResult, ObjectError> {
        let mut kms = self.kms.client();
        let deleted = control_rpc(
            "KMS DeleteObject",
            kms.delete_object(DeleteObjectRequest {
                bucket_id: bucket_id.to_string(),
                key: key.to_string(),
                version_ids: version_ids.to_vec(),
            }),
        )
        .await?
        .into_inner();
        self.invalidate_resolved_read(bucket_id, key);
        Ok(ObjectDeleteResult {
            deleted_versions: deleted.deleted_versions,
            fragment_delete_attempts: deleted.fragment_delete_attempts,
            fragment_delete_successes: deleted.fragment_delete_successes,
            reclaimed_granules: deleted.reclaimed_granules,
            cleanup_complete: deleted.cleanup_complete,
        })
    }

    async fn read_plans(
        &self,
        plans: &[FragmentPlan],
        fragments: &mut [Option<Vec<u8>>],
        version_id: &str,
    ) -> Result<(Vec<String>, RequestPhaseTimes), ObjectError> {
        let mut reads = JoinSet::new();
        let mut failures = Vec::new();
        for plan in plans {
            let Some(session) = self.target_sessions.get(&plan.endpoint).cloned() else {
                failures.push(format!(
                    "fragment {} target {} endpoint {} has no live target session in read manifest {}",
                    plan.fragment_index, plan.target_id, plan.endpoint, version_id
                ));
                continue;
            };
            let chunk_id = chunk_id_from_proto(&plan.chunk_id)?;
            let endpoint = plan.endpoint.clone();
            let target_id = plan.target_id.clone();
            let fragment_index = plan.fragment_index as usize;
            reads.spawn(async move {
                match data_rpc(
                    "target read",
                    TARGET_IO_TIMEOUT,
                    session.read_chunk(chunk_id),
                )
                .await
                {
                    Ok(reply) => Ok((fragment_index, reply.value.payload, reply.phases)),
                    Err(err) => Err(format!(
                        "fragment {} target {} endpoint {} failed: {}",
                        fragment_index, target_id, endpoint, err
                    )),
                }
            });
        }
        let mut phase_totals = RequestPhaseTimes::default();
        while let Some(result) = reads.join_next().await {
            match result {
                Ok(Ok((fragment_index, payload, phases))) => {
                    fragments[fragment_index] = Some(payload);
                    add_request_phase_times(&mut phase_totals, &phases);
                }
                Ok(Err(err)) => failures.push(err),
                Err(err) => failures.push(format!(
                    "object read worker task failed before completing a fragment read: {}",
                    err
                )),
            }
        }
        Ok((failures, phase_totals))
    }

    async fn read_single_stripe(
        &mut self,
        bucket_id: &str,
        key: &str,
        manifest: &ObjectVersionManifest,
        ec_profile: &EcProfile,
        stripe_index: usize,
    ) -> Result<StripeReadResult, ObjectError> {
        let mut phases = ObjectPhaseTimes::default();
        let stripe = &manifest.stripes[stripe_index];
        let data_fragments = ec_profile.data_fragments as usize;
        if stripe.fragments.len() < data_fragments {
            return Err(ObjectError::Metadata(format!(
                "manifest stripe only carries {} fragments but profile {} needs {} data fragments",
                stripe.fragments.len(),
                ec_profile.id,
                data_fragments
            )));
        }
        let stripe_logical_bytes =
            stripe_logical_length_bytes(manifest, ec_profile, stripe_index as u32);
        let needed_data_fragments = needed_data_fragment_count(ec_profile, stripe_logical_bytes);
        let needed_data_plans = stripe
            .fragments
            .iter()
            .take(needed_data_fragments)
            .cloned()
            .collect::<Vec<_>>();

        let (connect_elapsed, mut connect_failures) =
            self.ensure_read_target_sessions(&needed_data_plans).await;
        phases.target_connect += connect_elapsed;

        let phase_started = Instant::now();
        let mut fragments = vec![None; stripe.fragments.len()];
        let (read_failures, read_rpc_phases) = self
            .read_plans(&needed_data_plans, &mut fragments, &manifest.version_id)
            .await?;
        let mut failures = Vec::new();
        failures.append(&mut connect_failures);
        failures.extend(read_failures);
        let mut data_fragment_reads = needed_data_plans.len();
        accumulate_target_request_phases(&mut phases, &read_rpc_phases);
        phases.target_read += phase_started.elapsed();

        let missing_data = fragments
            .iter()
            .take(needed_data_fragments)
            .filter(|slot| slot.is_none())
            .count();
        if missing_data == 0 {
            let mut payload = Vec::with_capacity(stripe_logical_bytes as usize);
            for fragment in fragments.iter().take(needed_data_fragments) {
                payload.extend_from_slice(fragment.as_ref().ok_or_else(|| {
                    ObjectError::Metadata(
                        "healthy read fallback path lost a data fragment after validation"
                            .to_string(),
                    )
                })?);
            }
            payload.truncate(stripe_logical_bytes as usize);
            return Ok(StripeReadResult {
                payload,
                missing_fragments: failures.len(),
                data_fragment_reads,
                parity_fragment_reads: 0,
                reconstructed: false,
                phases,
            });
        }
        if missing_data > ec_profile.parity_fragments as usize {
            return Err(ObjectError::Transport(format!(
                "KSC object read for {}/{} lost too many fragments in one stripe: {}",
                bucket_id,
                key,
                failures.join(" | ")
            )));
        }
        let remaining_data_plans = stripe
            .fragments
            .iter()
            .skip(needed_data_fragments)
            .take(data_fragments.saturating_sub(needed_data_fragments))
            .cloned()
            .collect::<Vec<_>>();
        if !remaining_data_plans.is_empty() {
            let (connect_elapsed, mut connect_failures) = self
                .ensure_read_target_sessions(&remaining_data_plans)
                .await;
            phases.target_connect += connect_elapsed;

            let phase_started = Instant::now();
            let (remaining_failures, remaining_rpc_phases) = self
                .read_plans(&remaining_data_plans, &mut fragments, &manifest.version_id)
                .await?;
            accumulate_target_request_phases(&mut phases, &remaining_rpc_phases);
            phases.target_read += phase_started.elapsed();
            data_fragment_reads += remaining_data_plans.len();
            failures.append(&mut connect_failures);
            failures.extend(remaining_failures);
        }
        let all_parity_plans = stripe
            .fragments
            .iter()
            .skip(data_fragments)
            .cloned()
            .collect::<Vec<_>>();
        let mut parity_fragment_reads = 0_usize;
        let mut parity_offset = 0_usize;
        while fragments
            .iter()
            .take(data_fragments)
            .filter(|slot| slot.is_some())
            .count()
            < data_fragments
            && parity_offset < all_parity_plans.len()
        {
            let still_needed = data_fragments.saturating_sub(
                fragments
                    .iter()
                    .take(data_fragments)
                    .filter(|slot| slot.is_some())
                    .count(),
            );
            let batch_len = still_needed.min(all_parity_plans.len().saturating_sub(parity_offset));
            let parity_plans = all_parity_plans[parity_offset..parity_offset + batch_len].to_vec();
            let (connect_elapsed, mut connect_failures) =
                self.ensure_read_target_sessions(&parity_plans).await;
            phases.target_connect += connect_elapsed;

            let phase_started = Instant::now();
            let (parity_read_failures, parity_rpc_phases) = self
                .read_plans(&parity_plans, &mut fragments, &manifest.version_id)
                .await?;
            accumulate_target_request_phases(&mut phases, &parity_rpc_phases);
            phases.target_read += phase_started.elapsed();
            parity_fragment_reads += parity_plans.len();
            failures.append(&mut connect_failures);
            failures.extend(parity_read_failures);
            parity_offset += batch_len;
        }
        if failures.len() > ec_profile.parity_fragments as usize {
            return Err(ObjectError::Transport(format!(
                "KSC object read for {}/{} lost too many fragments: {}",
                bucket_id,
                key,
                failures.join(" | ")
            )));
        }
        if fragments.iter().filter(|slot| slot.is_some()).count() < data_fragments {
            return Err(ObjectError::Transport(format!(
                "KSC object read for {}/{} could not gather enough fragments to reconstruct: {}",
                bucket_id,
                key,
                failures.join(" | ")
            )));
        }
        let phase_started = Instant::now();
        // The plan is a cheap clone (an EcProfile plus a &'static table); move it and
        // the owned fragment buffers onto a blocking worker so the CPU-bound Reed-Solomon
        // reconstruct never stalls a tokio runtime thread.
        let plan = self
            .prepared_encoders
            .get(&ec_profile.id)
            .ok_or_else(|| {
                ObjectError::Metadata(format!(
                    "prepared EC workspace for profile {} disappeared before reconstruct",
                    ec_profile.id
                ))
            })?
            .pool
            .plan
            .clone();
        let restored = tokio::task::spawn_blocking(move || {
            plan.reconstruct(&mut fragments).map_err(ObjectError::from)
        })
        .await
        .map_err(|err| {
            ObjectError::Metadata(format!("EC reconstruct worker task failed: {err}"))
        })??;
        phases.ec_reconstruct += phase_started.elapsed();
        let mut payload = Vec::with_capacity(stripe_logical_bytes as usize);
        for fragment in restored.iter().take(data_fragments) {
            payload.extend_from_slice(fragment);
        }
        payload.truncate(stripe_logical_bytes as usize);
        Ok(StripeReadResult {
            payload,
            missing_fragments: failures.len(),
            data_fragment_reads,
            parity_fragment_reads,
            reconstructed: true,
            phases,
        })
    }

    fn cached_resolved_read(
        &mut self,
        bucket_id: &str,
        key: &str,
    ) -> Option<(ObjectVersionManifest, EcProfile)> {
        self.shared_read_cache.cached_resolved_read(bucket_id, key)
    }

    fn cache_resolved_read(&mut self, manifest: ObjectVersionManifest, ec_profile: EcProfile) {
        self.shared_read_cache
            .cache_resolved_read(manifest, ec_profile);
    }

    fn invalidate_resolved_read(&mut self, bucket_id: &str, key: &str) {
        self.shared_read_cache.invalidate_key(bucket_id, key);
    }

    fn cached_payload_read(&mut self, manifest: &ObjectVersionManifest) -> Option<Vec<u8>> {
        self.shared_read_cache.cached_payload_read(manifest)
    }

    fn cache_payload_read(&mut self, manifest: &ObjectVersionManifest, payload: &[u8]) {
        self.shared_read_cache.cache_payload_read(manifest, payload);
    }

    async fn ensure_read_target_sessions(
        &mut self,
        plans: &[FragmentPlan],
    ) -> (Duration, Vec<String>) {
        let phase_started = Instant::now();
        let mut pending = Vec::new();
        for plan in plans {
            if !self.target_sessions.contains_key(&plan.endpoint)
                && !pending.iter().any(|endpoint| endpoint == &plan.endpoint)
            {
                pending.push(plan.endpoint.clone());
            }
        }
        if pending.is_empty() {
            return (phase_started.elapsed(), Vec::new());
        }

        {
            let shared = self.shared_target_sessions.lock().await;
            for endpoint in &pending {
                if let Some(session) = shared.get(endpoint) {
                    self.target_sessions
                        .entry(endpoint.clone())
                        .or_insert_with(|| session.clone());
                }
            }
        }
        pending.retain(|endpoint| !self.target_sessions.contains_key(endpoint));
        if pending.is_empty() {
            return (phase_started.elapsed(), Vec::new());
        }

        let mut connects = JoinSet::new();
        let session_options = self.session_options;
        for endpoint in pending {
            connects.spawn(async move {
                let session = data_rpc(
                    "target connect",
                    TARGET_CONNECT_TIMEOUT,
                    TargetSession::connect_with_options(&endpoint, session_options),
                )
                .await;
                (endpoint, session)
            });
        }
        let mut failures = Vec::new();
        while let Some(result) = connects.join_next().await {
            match result {
                Ok((endpoint, Ok(session))) => {
                    {
                        let mut shared = self.shared_target_sessions.lock().await;
                        shared
                            .entry(endpoint.clone())
                            .or_insert_with(|| session.clone());
                    }
                    self.target_sessions.insert(endpoint, session);
                }
                Ok((endpoint, Err(err))) => failures.push(format!(
                    "target session endpoint {} could not connect for degraded read: {}",
                    endpoint, err
                )),
                Err(err) => failures.push(format!(
                    "KSC target-session connect task failed before completing a degraded read connection: {}",
                    err
                )),
            }
        }
        (phase_started.elapsed(), failures)
    }

    async fn ensure_target_sessions(&mut self, plans: &[FragmentPlan]) -> Result<(), ObjectError> {
        let mut pending = Vec::new();
        for plan in plans {
            if !self.target_sessions.contains_key(&plan.endpoint)
                && !pending.iter().any(|endpoint| endpoint == &plan.endpoint)
            {
                pending.push(plan.endpoint.clone());
            }
        }
        if pending.is_empty() {
            return Ok(());
        }

        {
            let shared = self.shared_target_sessions.lock().await;
            for endpoint in &pending {
                if let Some(session) = shared.get(endpoint) {
                    self.target_sessions
                        .entry(endpoint.clone())
                        .or_insert_with(|| session.clone());
                }
            }
        }
        pending.retain(|endpoint| !self.target_sessions.contains_key(endpoint));
        if pending.is_empty() {
            return Ok(());
        }

        let mut connects = JoinSet::new();
        let session_options = self.session_options;
        for endpoint in pending {
            connects.spawn(async move {
                let session = data_rpc(
                    "target connect",
                    TARGET_CONNECT_TIMEOUT,
                    TargetSession::connect_with_options(&endpoint, session_options),
                )
                .await;
                (endpoint, session)
            });
        }
        while let Some(result) = connects.join_next().await {
            let (endpoint, session) = result.map_err(|err| {
                ObjectError::Transport(format!(
                    "KSC target-session connect task failed before completing: {}",
                    err
                ))
            })?;
            let session = session?;
            {
                let mut shared = self.shared_target_sessions.lock().await;
                shared
                    .entry(endpoint.clone())
                    .or_insert_with(|| session.clone());
            }
            self.target_sessions.insert(endpoint, session);
        }
        Ok(())
    }

    async fn reconnect_target_sessions(&mut self, endpoints: &[String]) -> Result<(), ObjectError> {
        let mut pending = Vec::new();
        for endpoint in endpoints {
            if !pending.iter().any(|value| value == endpoint) {
                pending.push(endpoint.clone());
            }
        }
        if pending.is_empty() {
            return Ok(());
        }

        let mut connects = JoinSet::new();
        let session_options = self.session_options;
        for endpoint in pending {
            connects.spawn(async move {
                let session = data_rpc(
                    "target reconnect",
                    TARGET_CONNECT_TIMEOUT,
                    TargetSession::connect_with_options(&endpoint, session_options),
                )
                .await;
                (endpoint, session)
            });
        }
        while let Some(result) = connects.join_next().await {
            let (endpoint, session) = result.map_err(|err| {
                ObjectError::Transport(format!(
                    "KSC target-session reconnect task failed before completing: {}",
                    err
                ))
            })?;
            let session = session?;
            {
                let mut shared = self.shared_target_sessions.lock().await;
                shared.insert(endpoint.clone(), session.clone());
            }
            self.target_sessions.insert(endpoint, session);
        }
        Ok(())
    }

    async fn write_fragment_plans(
        &self,
        intent_id: &str,
        plans: &[FragmentPlan],
        fragments: &[Vec<u8>],
    ) -> Result<(Vec<FragmentWriteFailure>, RequestPhaseTimes), ObjectError> {
        write_fragment_plans_with_sessions(
            Arc::new(snapshot_target_sessions(
                &self.target_sessions,
                plans,
                intent_id,
            )?),
            intent_id,
            plans,
            fragments,
            Arc::clone(&self.write_inflight_limiter),
        )
        .await
    }

    async fn retry_fragment_failures_same_target(
        &mut self,
        intent_id: &str,
        plans: &[FragmentPlan],
        fragments: &[Vec<u8>],
        failures: Vec<FragmentWriteFailure>,
    ) -> Result<RetryWriteResult, ObjectError> {
        let mut pending_failures = failures;
        let mut result = RetryWriteResult::default();
        for attempt in 0..TARGET_SAME_PLAN_RETRY_ATTEMPTS {
            if pending_failures.is_empty() {
                break;
            }
            let retry_plans = retry_plans_for_failures(plans, &pending_failures)?;
            if retry_plans.is_empty() {
                break;
            }
            let retry_endpoints = retry_plans
                .iter()
                .map(|plan| plan.endpoint.clone())
                .collect::<Vec<_>>();
            let connect_started = Instant::now();
            self.reconnect_target_sessions(&retry_endpoints).await?;
            result.connect_elapsed += connect_started.elapsed();

            let write_started = Instant::now();
            let (retry_failures, retry_phases) = self
                .write_fragment_plans(intent_id, &retry_plans, fragments)
                .await?;
            result.write_elapsed += write_started.elapsed();
            add_request_phase_times(&mut result.phases, &retry_phases);
            pending_failures = retry_failures;

            if !pending_failures.is_empty() && attempt + 1 < TARGET_SAME_PLAN_RETRY_ATTEMPTS {
                // Honor the MAX 429 Retry-After across this batch's rate-limited
                // failures (clamped); fall back to a capped exponential backoff
                // when no target asked for a specific pause.
                //
                // Note: with TARGET_SAME_PLAN_RETRY_ATTEMPTS == 2 only one backoff
                // sleep fires (attempt == 0), so the `<< attempt` exponential
                // growth is dormant today; it is retained (and unit-tested) so the
                // backoff scales correctly if the attempt count is raised.
                let backoff = compute_retry_backoff(
                    &pending_failures,
                    attempt as u32,
                    TARGET_SAME_PLAN_RETRY_BACKOFF,
                    TARGET_RETRY_BACKOFF_CEILING,
                );
                sleep(backoff).await;
            }
        }
        result.failures = pending_failures;
        Ok(result)
    }

    /// Returns a cloneable handle to the (thread-safe) shard pool for `profile`,
    /// ensuring the prepared encoder exists first. The handle can be moved into a
    /// `spawn_blocking` task to run the CPU-bound encode/reconstruct off the async
    /// runtime while still recycling buffers back into the shared pool.
    fn prepared_shard_pool(&mut self, profile: &EcProfile) -> Result<ShardPool, ObjectError> {
        self.ensure_prepared_encoder(profile)?;
        self.prepared_encoders
            .get(&profile.id)
            .map(|workspace| workspace.pool.clone())
            .ok_or_else(|| {
                ObjectError::Metadata(format!(
                    "prepared EC workspace for profile {} disappeared mid-write",
                    profile.id
                ))
            })
    }

    fn return_prepared_shards(&mut self, profile_id: &str, shards: Vec<Vec<u8>>) {
        if let Some(workspace) = self.prepared_encoders.get(profile_id) {
            workspace.pool.return_shards(shards);
        }
    }

    fn ensure_prepared_encoder(&mut self, profile: &EcProfile) -> Result<(), ObjectError> {
        if self.prepared_encoders.contains_key(&profile.id) {
            return Ok(());
        }
        let kee_profile = kee_profile_from_control(profile)?;
        let engine = KeeEngine::new(kee_profile)?;
        let prepared = engine.prepared_plan()?;
        self.prepared_encoders
            .insert(profile.id.clone(), PreparedEncodeWorkspace::new(prepared));
        Ok(())
    }
}

/// One inflight batch of stripes whose payloads have already been EC-encoded.
struct EncodedStripeBatch {
    prepared_batch: Vec<PreparedStripeWrite>,
    batch_plans: Vec<FragmentPlan>,
    ec_encode: Duration,
}

/// Produces (reads + EC-encodes) one inflight batch of stripes.
///
/// Reads run on the async task because the supplied `read_range` closure is a
/// synchronous, non-`Send` `FnMut`. The CPU-bound EC encode for every stripe is
/// dispatched to `tokio::task::spawn_blocking` so it never blocks a tokio worker,
/// and the per-stripe encodes run concurrently with one another. Encoded shard
/// buffers are drawn from and (on this happy path) stay owned by the caller, who
/// recycles them back into the shared pool after the network writes complete.
///
/// The returned `prepared_batch` is ordered by ascending stripe index, matching
/// the pre-pipelining behaviour so downstream batching/commit logic is unchanged.
async fn encode_stripe_batch<F>(
    pool: ShardPool,
    window_plans: &[FragmentPlan],
    window_start: usize,
    stripe_offset: usize,
    stripe_batch_len: usize,
    logical_length_bytes: usize,
    stripe_logical_bytes: usize,
    read_range: &mut F,
) -> Result<EncodedStripeBatch, ObjectError>
where
    F: FnMut(u64, usize) -> Result<Vec<u8>, ObjectError>,
{
    let mut batch_plans = Vec::new();
    let mut encodes: JoinSet<Result<(usize, Vec<FragmentPlan>, u32, Vec<Vec<u8>>), ObjectError>> =
        JoinSet::new();
    for batch_offset in 0..stripe_batch_len {
        let stripe_index = (window_start + stripe_offset + batch_offset) as u32;
        let stripe_plans = fragment_plans_for_stripe(window_plans, stripe_index)?;
        batch_plans.extend(stripe_plans.iter().cloned());
        let (stripe_start, stripe_end) =
            stripe_payload_range(logical_length_bytes, stripe_index as usize, stripe_logical_bytes)?;
        // Read on the async task (closure is sync + !Send), then hand the owned
        // payload + shard buffers to a blocking worker for the CPU-bound encode.
        let stripe_payload = read_range(stripe_start as u64, stripe_end - stripe_start)?;
        let pool = pool.clone();
        let mut shards = pool.take_shards();
        encodes.spawn_blocking(move || {
            pool.plan.encode_into(&stripe_payload, &mut shards)?;
            Ok((batch_offset, stripe_plans, stripe_index, shards))
        });
    }

    let encode_started = Instant::now();
    let mut encoded_slots: Vec<Option<PreparedStripeWrite>> =
        (0..stripe_batch_len).map(|_| None).collect();
    while let Some(joined) = encodes.join_next().await {
        let (batch_offset, plans, stripe_index, fragments) = joined.map_err(|err| {
            ObjectError::Metadata(format!("EC encode worker task failed: {err}"))
        })??;
        encoded_slots[batch_offset] = Some(PreparedStripeWrite {
            stripe_index,
            plans,
            fragments,
        });
    }
    let ec_encode = encode_started.elapsed();
    let prepared_batch = encoded_slots
        .into_iter()
        .map(|slot| {
            slot.ok_or_else(|| {
                ObjectError::Metadata(
                    "EC encode pipeline dropped a stripe before completing".to_string(),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(EncodedStripeBatch {
        prepared_batch,
        batch_plans,
        ec_encode,
    })
}

fn snapshot_target_sessions(
    target_sessions: &HashMap<String, TargetSession>,
    plans: &[FragmentPlan],
    intent_id: &str,
) -> Result<HashMap<String, TargetSession>, ObjectError> {
    let mut snapshot = HashMap::new();
    for plan in plans {
        if snapshot.contains_key(&plan.endpoint) {
            continue;
        }
        let session = target_sessions.get(&plan.endpoint).ok_or_else(|| {
            ObjectError::Metadata(format!(
                "KSC has no cached target session for endpoint {} in write intent {}",
                plan.endpoint, intent_id
            ))
        })?;
        snapshot.insert(plan.endpoint.clone(), session.clone());
    }
    Ok(snapshot)
}

async fn write_fragment_plans_with_sessions(
    target_sessions: Arc<HashMap<String, TargetSession>>,
    intent_id: &str,
    plans: &[FragmentPlan],
    fragments: &[Vec<u8>],
    write_inflight_limiter: Arc<AdaptiveWriteLimiter>,
) -> Result<(Vec<FragmentWriteFailure>, RequestPhaseTimes), ObjectError> {
    let mut writes = JoinSet::new();
    for plan in plans {
        let session = target_sessions
            .get(&plan.endpoint)
            .ok_or_else(|| {
                ObjectError::Metadata(format!(
                    "KSC has no cached target session for endpoint {} in write intent {}",
                    plan.endpoint, intent_id
                ))
            })?
            .clone();
        let chunk_id = chunk_id_from_proto(&plan.chunk_id)?;
        let fragment = fragments
            .get(plan.fragment_index as usize)
            .ok_or_else(|| {
                ObjectError::Metadata(format!(
                    "fragment index {} is out of range for write intent {}",
                    plan.fragment_index, intent_id
                ))
            })?
            .clone();
        let endpoint = plan.endpoint.clone();
        let target_id = plan.target_id.clone();
        let fragment_index = plan.fragment_index;
        let stripe_index = plan.stripe_index;
        let granule_index = plan.granule_index;
        let generation = plan.generation;
        // Honor the shared adaptive gate on the same-target RETRY path too: a 429
        // that shrank the gate to 1 must still bound this fan-out. Acquire a
        // permit BEFORE spawning; it moves into the task and is released on
        // completion (success or error).
        let permit = write_inflight_limiter.acquire().await;
        writes.spawn(async move {
            let _permit = permit;
            data_rpc(
                "target write",
                TARGET_IO_TIMEOUT,
                session.write_chunk(chunk_id, granule_index, generation, fragment),
            )
            .await
            .map_err(|err| {
                let signal = err.rate_limit_signal();
                FragmentWriteFailure::new(
                    stripe_index,
                    fragment_index,
                    format!(
                        "fragment {} target {} endpoint {} failed: {}",
                        fragment_index, target_id, endpoint, err
                    ),
                    signal,
                )
            })
        });
    }
    let mut failures = Vec::new();
    let mut phase_totals = RequestPhaseTimes::default();
    while let Some(result) = writes.join_next().await {
        match result {
            Ok(Ok(phases)) => add_request_phase_times(&mut phase_totals, &phases),
            Ok(Err(err)) => failures.push(err),
            Err(err) => failures.push(FragmentWriteFailure::plain(
                u32::MAX,
                u32::MAX,
                format!(
                    "object write worker task failed before completing a fragment write: {}",
                    err
                ),
            )),
        }
    }
    // Feed the shared gate from the retry path as well so 429s observed during
    // retries shrink the client-wide inflight ceiling (and clean retries let it
    // recover). Mirrors the primary batched path: only an all-clean batch grows
    // the gate; a 429 shrinks it; a non-429 failure is a no-op (neither grow nor
    // shrink) so a failing-but-not-rate-limited target is not nudged wider.
    feed_inflight_limiter(&write_inflight_limiter, failures.iter());
    Ok((failures, phase_totals))
}

/// Drive the adaptive write gate from a completed batch's failures.
///
/// - Any 429 shrinks the gate toward the smallest advertised max-in-flight.
/// - A fully clean batch (no failures of any kind) additively recovers it.
/// - A batch with only non-429 failures is a no-op: a target failing for
///   transport/join/length reasons is not healthy, so growing concurrency at it
///   is counterproductive, and it asked for no specific backpressure.
fn feed_inflight_limiter<'a, I>(limiter: &AdaptiveWriteLimiter, failures: I)
where
    I: Iterator<Item = &'a FragmentWriteFailure>,
{
    let mut observed_rate_limit = false;
    let mut any_failure = false;
    let mut advertised_inflight: Option<usize> = None;
    for failure in failures {
        any_failure = true;
        if failure.rate_limited {
            observed_rate_limit = true;
            if let Some(limit) = failure.limit_max_inflight {
                advertised_inflight = Some(match advertised_inflight {
                    Some(existing) => existing.min(limit),
                    None => limit,
                });
            }
        }
    }
    if observed_rate_limit {
        limiter.note_rate_limited(advertised_inflight);
    } else if !any_failure {
        limiter.note_success();
    }
}

async fn write_prepared_stripe_batch_with_sessions(
    target_sessions: Arc<HashMap<String, TargetSession>>,
    intent_id: &str,
    prepared_batch: Vec<PreparedStripeWrite>,
    write_inflight_limiter: Arc<AdaptiveWriteLimiter>,
) -> Result<PreparedStripeBatchWriteResult, ObjectError> {
    let endpoint_batches = build_endpoint_write_batches(intent_id, &prepared_batch)?;
    let mut writes = JoinSet::new();
    for batch in endpoint_batches {
        let endpoint = batch
            .first()
            .map(|item| item.endpoint.clone())
            .ok_or_else(|| {
                ObjectError::Metadata(format!(
                    "KSC built an empty target batch for write intent {}",
                    intent_id
                ))
            })?;
        let session = target_sessions
            .get(&endpoint)
            .ok_or_else(|| {
                ObjectError::Metadata(format!(
                    "KSC has no cached target session for endpoint {} in write intent {}",
                    endpoint, intent_id
                ))
            })?
            .clone();
        // Acquire a permit BEFORE spawning so the fan-out is bounded by the
        // adaptive gate. The permit moves into the task and is dropped (released)
        // when the task finishes, on both the success and error paths.
        let permit = write_inflight_limiter.acquire().await;
        writes.spawn(async move {
            let _permit = permit;
            let write_started = Instant::now();
            let outcome = if batch.len() == 1 {
                let mut item = batch.into_iter().next().expect("single-item batch");
                // Move the fragment payload into the request rather than cloning it; the
                // result only needs `item`'s routing/index fields afterwards, so the now
                // empty `payload` is never read again on the single-write path.
                let payload = std::mem::take(&mut item.payload);
                match data_rpc(
                    "target write",
                    TARGET_IO_TIMEOUT,
                    session.write_chunk(
                        item.chunk_id,
                        item.granule_index,
                        item.generation,
                        payload,
                    ),
                )
                .await
                {
                    Ok(phases) => EndpointWriteBatchResult::Single {
                        item,
                        phases,
                        write_elapsed: write_started.elapsed(),
                        error: None,
                    },
                    Err(err) => {
                        let signal = err.rate_limit_signal();
                        EndpointWriteBatchResult::Single {
                            item,
                            phases: RequestPhaseTimes::default(),
                            write_elapsed: write_started.elapsed(),
                            error: Some((err.to_string(), signal)),
                        }
                    }
                }
            } else {
                let mut batch = batch;
                // Move each fragment payload out of the batch into the packed request
                // (zero extra copy) instead of cloning. The batch is still returned in
                // the result for failure reporting; only its routing/index fields are
                // read after the write, so the emptied payloads are never used again.
                let pack = PackedWriteRequest {
                    entries: batch
                        .iter_mut()
                        .map(|item| PackedWriteEntry {
                            chunk_id: item.chunk_id,
                            slot_index: item.granule_index,
                            generation: item.generation,
                            payload: std::mem::take(&mut item.payload),
                        })
                        .collect(),
                };
                match data_rpc(
                    "target packed write",
                    TARGET_IO_TIMEOUT,
                    session.packed_write(pack),
                )
                .await
                {
                    Ok(reply) => EndpointWriteBatchResult::Packed {
                        items: batch,
                        reply: Ok(reply.value),
                        phases: reply.phases,
                        write_elapsed: write_started.elapsed(),
                    },
                    Err(err) => {
                        let signal = err.rate_limit_signal();
                        EndpointWriteBatchResult::Packed {
                            items: batch,
                            reply: Err((err.to_string(), signal)),
                            phases: RequestPhaseTimes::default(),
                            write_elapsed: write_started.elapsed(),
                        }
                    }
                }
            };
            Ok::<EndpointWriteBatchResult, ObjectError>(outcome)
        });
    }

    let mut failures_by_stripe = HashMap::<u32, Vec<FragmentWriteFailure>>::new();
    let mut phase_totals = RequestPhaseTimes::default();
    let mut write_elapsed = Duration::ZERO;
    while let Some(result) = writes.join_next().await {
        match result {
            Ok(Ok(batch_result)) => match batch_result {
                EndpointWriteBatchResult::Single {
                    item,
                    phases,
                    write_elapsed: batch_elapsed,
                    error,
                } => {
                    add_request_phase_times(&mut phase_totals, &phases);
                    write_elapsed += batch_elapsed;
                    if let Some((message, signal)) = error {
                        record_batched_write_failure(
                            &mut failures_by_stripe,
                            FragmentWriteFailure::new(
                                item.stripe_index,
                                item.fragment_index,
                                format!(
                                    "fragment {} target {} endpoint {} failed: {}",
                                    item.fragment_index, item.target_id, item.endpoint, message
                                ),
                                signal,
                            ),
                        );
                    }
                }
                EndpointWriteBatchResult::Packed {
                    items,
                    reply,
                    phases,
                    write_elapsed: batch_elapsed,
                } => {
                    add_request_phase_times(&mut phase_totals, &phases);
                    write_elapsed += batch_elapsed;
                    match reply {
                        Ok(reply) => {
                            let item_count = items.len();
                            if reply.entries.len() != item_count {
                                for item in items {
                                    record_batched_write_failure(
                                        &mut failures_by_stripe,
                                        FragmentWriteFailure::plain(
                                            item.stripe_index,
                                            item.fragment_index,
                                            format!(
                                                "fragment {} target {} endpoint {} failed: packed write reply length {} did not match request length {}",
                                                item.fragment_index,
                                                item.target_id,
                                                item.endpoint,
                                                reply.entries.len(),
                                                item_count
                                            ),
                                        ),
                                    );
                                }
                                continue;
                            }
                            for (item, entry) in items.into_iter().zip(reply.entries) {
                                if entry.success() {
                                    continue;
                                }
                                let detail = entry.error.unwrap_or_else(|| {
                                    format!("target returned status {}", entry.status_code)
                                });
                                record_batched_write_failure(
                                    &mut failures_by_stripe,
                                    FragmentWriteFailure::plain(
                                        item.stripe_index,
                                        item.fragment_index,
                                        format!(
                                            "fragment {} target {} endpoint {} failed: {}",
                                            item.fragment_index,
                                            item.target_id,
                                            item.endpoint,
                                            detail
                                        ),
                                    ),
                                );
                            }
                        }
                        Err((message, signal)) => {
                            for item in items {
                                record_batched_write_failure(
                                    &mut failures_by_stripe,
                                    FragmentWriteFailure::new(
                                        item.stripe_index,
                                        item.fragment_index,
                                        format!(
                                            "fragment {} target {} endpoint {} failed: {}",
                                            item.fragment_index,
                                            item.target_id,
                                            item.endpoint,
                                            message
                                        ),
                                        signal,
                                    ),
                                );
                            }
                        }
                    }
                }
            },
            Ok(Err(err)) => {
                return Err(err);
            }
            Err(err) => {
                record_batched_write_failure(
                    &mut failures_by_stripe,
                    FragmentWriteFailure::plain(
                        u32::MAX,
                        u32::MAX,
                        format!(
                            "object write worker task failed before completing a batched target write: {}",
                            err
                        ),
                    ),
                );
            }
        }
    }

    // Feed the adaptive gate: if any fragment in this batch hit a 429, shrink
    // toward the smallest advertised max-in-flight; if the batch was fully clean,
    // additively recover; if it failed for non-429 reasons, leave the gate alone
    // (don't grow concurrency at an unhealthy target).
    feed_inflight_limiter(&write_inflight_limiter, failures_by_stripe.values().flatten());

    let stripe_results = prepared_batch
        .into_iter()
        .map(|prepared| PreparedStripeWriteResult {
            failures: failures_by_stripe
                .remove(&prepared.stripe_index)
                .unwrap_or_default(),
            prepared,
        })
        .collect();
    Ok(PreparedStripeBatchWriteResult {
        stripe_results,
        phases: phase_totals,
        write_elapsed,
    })
}

enum EndpointWriteBatchResult {
    Single {
        item: BatchedTargetWritePlan,
        phases: RequestPhaseTimes,
        write_elapsed: Duration,
        // (message, 429 signal) so the consumer can build a structured
        // FragmentWriteFailure instead of just a string.
        error: Option<(String, RateLimitSignal)>,
    },
    Packed {
        items: Vec<BatchedTargetWritePlan>,
        reply: Result<kp2::PackedWriteReply, (String, RateLimitSignal)>,
        phases: RequestPhaseTimes,
        write_elapsed: Duration,
    },
}

enum EndpointReadBatchResult {
    Single {
        item: BatchedTargetReadPlan,
        payload: Option<Vec<u8>>,
        phases: RequestPhaseTimes,
        read_elapsed: Duration,
    },
    Packed {
        items: Vec<BatchedTargetReadPlan>,
        reply: Option<kp2::PackedReadResponse>,
        phases: RequestPhaseTimes,
        read_elapsed: Duration,
    },
}

struct BatchedTargetReadResult {
    phases: RequestPhaseTimes,
    read_elapsed: Duration,
}

fn build_endpoint_write_batches(
    intent_id: &str,
    prepared_batch: &[PreparedStripeWrite],
) -> Result<Vec<Vec<BatchedTargetWritePlan>>, ObjectError> {
    let mut by_endpoint = HashMap::<String, Vec<BatchedTargetWritePlan>>::new();
    for prepared in prepared_batch {
        for plan in &prepared.plans {
            let chunk_id = chunk_id_from_proto(&plan.chunk_id)?;
            let payload = prepared
                .fragments
                .get(plan.fragment_index as usize)
                .ok_or_else(|| {
                    ObjectError::Metadata(format!(
                        "fragment index {} is out of range for write intent {}",
                        plan.fragment_index, intent_id
                    ))
                })?
                .clone();
            by_endpoint
                .entry(plan.endpoint.clone())
                .or_default()
                .push(BatchedTargetWritePlan {
                    endpoint: plan.endpoint.clone(),
                    target_id: plan.target_id.clone(),
                    stripe_index: plan.stripe_index,
                    fragment_index: plan.fragment_index,
                    granule_index: plan.granule_index,
                    generation: plan.generation,
                    chunk_id,
                    payload,
                });
        }
    }

    let mut batches = Vec::new();
    for (_, items) in by_endpoint {
        let mut current = Vec::new();
        let mut current_payload_bytes = 0usize;
        for item in items {
            if !current.is_empty()
                && current_payload_bytes.saturating_add(item.payload.len()) > MAX_PACK_PAYLOAD_BYTES
            {
                batches.push(current);
                current = Vec::new();
                current_payload_bytes = 0;
            }
            current_payload_bytes = current_payload_bytes.saturating_add(item.payload.len());
            current.push(item);
        }
        if !current.is_empty() {
            batches.push(current);
        }
    }
    Ok(batches)
}

fn build_endpoint_read_batches(
    read_plans: Vec<BatchedTargetReadPlan>,
) -> Vec<Vec<BatchedTargetReadPlan>> {
    let mut by_endpoint = HashMap::<String, Vec<BatchedTargetReadPlan>>::new();
    for item in read_plans {
        by_endpoint
            .entry(item.endpoint.clone())
            .or_default()
            .push(item);
    }

    let mut batches = Vec::new();
    for (_, items) in by_endpoint {
        let mut current = Vec::new();
        let mut current_payload_bytes = 0usize;
        for item in items {
            if !current.is_empty()
                && current_payload_bytes.saturating_add(item.payload_bytes) > MAX_PACK_PAYLOAD_BYTES
            {
                batches.push(current);
                current = Vec::new();
                current_payload_bytes = 0;
            }
            current_payload_bytes = current_payload_bytes.saturating_add(item.payload_bytes);
            current.push(item);
        }
        if !current.is_empty() {
            batches.push(current);
        }
    }
    batches
}

async fn read_plans_batched_with_sessions(
    target_sessions: Arc<HashMap<String, TargetSession>>,
    window_start: usize,
    window_states: &mut [WindowStripeReadState],
    read_plans: Vec<BatchedTargetReadPlan>,
) -> Result<BatchedTargetReadResult, ObjectError> {
    let endpoint_batches = build_endpoint_read_batches(read_plans);
    let mut reads = JoinSet::new();
    for batch in endpoint_batches {
        let endpoint = batch
            .first()
            .map(|item| item.endpoint.clone())
            .ok_or_else(|| {
                ObjectError::Metadata("KSC built an empty target batch for object read".to_string())
            })?;
        let Some(session) = target_sessions.get(&endpoint).cloned() else {
            let degraded = if batch.len() == 1 {
                EndpointReadBatchResult::Single {
                    item: batch.into_iter().next().expect("single-item read batch"),
                    payload: None,
                    phases: RequestPhaseTimes::default(),
                    read_elapsed: Duration::ZERO,
                }
            } else {
                EndpointReadBatchResult::Packed {
                    items: batch,
                    reply: None,
                    phases: RequestPhaseTimes::default(),
                    read_elapsed: Duration::ZERO,
                }
            };
            reads.spawn(async move { Ok::<EndpointReadBatchResult, ObjectError>(degraded) });
            continue;
        };
        reads.spawn(async move {
            let read_started = Instant::now();
            let outcome = if batch.len() == 1 {
                let item = batch.into_iter().next().expect("single-item read batch");
                match data_rpc(
                    "target read",
                    TARGET_IO_TIMEOUT,
                    session.read_chunk(item.chunk_id),
                )
                .await
                {
                    Ok(reply) => EndpointReadBatchResult::Single {
                        item,
                        payload: Some(reply.value.payload),
                        phases: reply.phases,
                        read_elapsed: read_started.elapsed(),
                    },
                    Err(_) => EndpointReadBatchResult::Single {
                        item,
                        payload: None,
                        phases: RequestPhaseTimes::default(),
                        read_elapsed: read_started.elapsed(),
                    },
                }
            } else {
                let query = PackedReadQuery {
                    chunk_ids: batch.iter().map(|item| item.chunk_id).collect(),
                    ranges: None,
                };
                match data_rpc(
                    "target packed read",
                    TARGET_IO_TIMEOUT,
                    session.packed_read(&query, batch.iter().map(|item| item.payload_bytes).sum()),
                )
                .await
                {
                    Ok(reply) => EndpointReadBatchResult::Packed {
                        items: batch,
                        reply: Some(reply.value),
                        phases: reply.phases,
                        read_elapsed: read_started.elapsed(),
                    },
                    Err(_) => EndpointReadBatchResult::Packed {
                        items: batch,
                        reply: None,
                        phases: RequestPhaseTimes::default(),
                        read_elapsed: read_started.elapsed(),
                    },
                }
            };
            Ok::<EndpointReadBatchResult, ObjectError>(outcome)
        });
    }

    let mut phase_totals = RequestPhaseTimes::default();
    let mut read_elapsed = Duration::ZERO;
    while let Some(result) = reads.join_next().await {
        match result {
            Ok(Ok(batch_result)) => match batch_result {
                EndpointReadBatchResult::Single {
                    item,
                    payload,
                    phases,
                    read_elapsed: batch_elapsed,
                } => {
                    add_request_phase_times(&mut phase_totals, &phases);
                    read_elapsed += batch_elapsed;
                    if let Some(payload) = payload {
                        let state_index = item.stripe_index as usize - window_start;
                        if let Some(state) = window_states.get_mut(state_index) {
                            if item.fragment_index < state.fragments.len() {
                                state.fragments[item.fragment_index] = Some(payload);
                            }
                        }
                    }
                }
                EndpointReadBatchResult::Packed {
                    items,
                    reply,
                    phases,
                    read_elapsed: batch_elapsed,
                } => {
                    add_request_phase_times(&mut phase_totals, &phases);
                    read_elapsed += batch_elapsed;
                    let Some(reply) = reply else {
                        continue;
                    };
                    if reply.entries.len() != items.len() {
                        continue;
                    }
                    let mut by_chunk = items
                        .into_iter()
                        .map(|item| (item.chunk_id, item))
                        .collect::<HashMap<_, _>>();
                    for entry in reply.entries {
                        if entry.status_code != 200 {
                            continue;
                        }
                        let Some(item) = by_chunk.remove(&entry.chunk_id) else {
                            continue;
                        };
                        let state_index = item.stripe_index as usize - window_start;
                        if let Some(state) = window_states.get_mut(state_index) {
                            if item.fragment_index < state.fragments.len() {
                                state.fragments[item.fragment_index] = Some(entry.payload);
                            }
                        }
                    }
                }
            },
            Ok(Err(err)) => return Err(err),
            Err(err) => {
                return Err(ObjectError::Transport(format!(
                    "object read worker task failed before completing a batched target read: {}",
                    err
                )));
            }
        }
    }

    Ok(BatchedTargetReadResult {
        phases: phase_totals,
        read_elapsed,
    })
}

/// Compute how long to sleep before the next same-target retry attempt.
///
/// Pure function so the backpressure math is unit-testable in isolation:
/// - If any failure in `failures` carried a KP2 429 `Retry-After`, honor the
///   MAX advertised delay across them (a target that asked for the longest
///   pause gets it), clamped to `ceiling`.
/// - Otherwise fall back to an exponential backoff seeded at `fallback`
///   (`fallback << attempt`), also clamped to `ceiling`, so repeated transient
///   failures back off instead of hammering at a fixed interval.
///
/// `attempt` is zero-based (0 for the first retry pause).
fn compute_retry_backoff(
    failures: &[FragmentWriteFailure],
    attempt: u32,
    fallback: Duration,
    ceiling: Duration,
) -> Duration {
    let max_retry_after_ms = failures
        .iter()
        .filter(|failure| failure.rate_limited)
        .filter_map(|failure| failure.retry_after_ms)
        .max();
    let backoff = match max_retry_after_ms {
        Some(ms) => Duration::from_millis(ms),
        None => {
            let shift = attempt.min(16);
            fallback
                .checked_mul(1u32 << shift)
                .unwrap_or(ceiling)
        }
    };
    backoff.min(ceiling)
}

/// Pure adaptive-limit transition for the write inflight gate (AIMD-style).
///
/// `current` and `max` are the live and configured ceilings; the result is
/// always clamped to `[1, max]` so the gate never deadlocks (>= 1) and never
/// exceeds the operator-configured limit.
fn adaptive_inflight_after_429(current: usize, max: usize, advertised: Option<usize>) -> usize {
    let target = match advertised {
        // Honor the target's advertised ceiling, but never grow past it here
        // and never below 1.
        Some(advertised) => advertised.min(current),
        // No advertised ceiling: multiplicative decrease (halve).
        None => current / 2,
    };
    target.clamp(1, max.max(1))
}

/// Additive-increase recovery for the write inflight gate after a clean batch.
fn adaptive_inflight_after_success(current: usize, max: usize, step: usize) -> usize {
    current.saturating_add(step).clamp(1, max.max(1))
}

/// Client-wide adaptive concurrency gate for fragment writes.
///
/// Scope: this is a single per-`ObjectClient` limiter, not per-target-endpoint.
/// `write_prepared_stripe_batch_with_sessions` is a free function that fans out
/// one task per *endpoint batch*, so a per-target gate would mean threading a
/// keyed map of semaphores through every spawn site and the retry path. The spec
/// permits a client-wide limiter; it is chosen here for minimal blast radius. A
/// 429 from any target shrinks the shared gate, which is conservative (it also
/// throttles healthy targets briefly) but safe and deadlock-free. Both the
/// primary batched path and the same-target retry path
/// (`write_fragment_plans_with_sessions`) acquire permits from and report 429s
/// to this shared gate.
///
/// Mechanics: a `tokio::sync::Semaphore` carries the permit budget. tokio has no
/// atomic "resize" primitive, so the limiter maintains a target ceiling plus a
/// *forget debt* under a mutex:
/// - Shrinking lowers `target` and forgets as many *available* permits as it can
///   immediately (`try_acquire_many` + `forget`); permits that are checked out by
///   in-flight tasks can't be forgotten yet, so the shortfall is recorded as
///   `forget_debt`.
/// - Every `acquire` first reconciles: it pays down the debt by forgetting freshly
///   available permits before taking one for itself. This is what keeps a
///   returned in-flight permit from overshooting a shrunken ceiling — the next
///   acquirer absorbs it.
/// - Growing raises `target`, first cancelling outstanding debt, then
///   `add_permits` for any real surplus.
///
/// Invariants: `target` is always clamped to `[1, max]` (never deadlocks, never
/// exceeds the configured ceiling); the limiter never forgets more permits than
/// are available at the moment, so in-flight work is never starved. Permits are
/// acquired before a task spawns and released (dropped) on completion — success
/// or error — on BOTH the primary batched fan-out
/// (`write_prepared_stripe_batch_with_sessions`) and the same-target retry
/// fan-out (`write_fragment_plans_with_sessions`), and both paths feed the gate
/// via `feed_inflight_limiter`.
///
/// Ceiling semantics: `max` is the only HARD ceiling — total issued permits is
/// always `target + forget_debt <= max`, so in-flight work never exceeds `max`.
/// The live shrunk `target` is a SOFT ceiling honored at acquire boundaries:
/// after a shrink-with-debt where in-flight tasks then return their permits
/// before any intervening acquire, `available_permits()` can transiently sit
/// above `target` until the next `acquire()` (or a `note_success()`, which now
/// pays down debt eagerly) reconciles it.
struct AdaptiveWriteLimiter {
    semaphore: Arc<tokio::sync::Semaphore>,
    max: usize,
    state: Mutex<AdaptiveLimiterState>,
}

struct AdaptiveLimiterState {
    /// Desired live ceiling (`[1, max]`).
    target: usize,
    /// Permits we still owe forgetting because they were checked out when a
    /// shrink happened; paid down opportunistically as permits free up.
    forget_debt: usize,
}

impl AdaptiveWriteLimiter {
    fn new(initial: usize) -> Self {
        let initial = initial.max(1);
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(initial)),
            max: initial,
            state: Mutex::new(AdaptiveLimiterState {
                target: initial,
                forget_debt: 0,
            }),
        }
    }

    #[cfg(test)]
    fn current_limit(&self) -> usize {
        self.state.lock().unwrap().target
    }

    /// Forget as many currently-available permits as the outstanding debt
    /// allows. Caller holds `state`. Returns nothing; debt is decremented by the
    /// number actually forgotten.
    fn pay_forget_debt(&self, state: &mut AdaptiveLimiterState) {
        if state.forget_debt == 0 {
            return;
        }
        let available = self.semaphore.available_permits();
        // Bound the request to both what is available and the configured ceiling
        // so the `as u32` cast is provably in range regardless of future callers
        // (`forget_debt` is bounded by `max` today, but make that explicit).
        let forgettable = state.forget_debt.min(available).min(self.max);
        debug_assert!(forgettable <= u32::MAX as usize);
        if forgettable > 0 {
            if let Ok(permits) = self.semaphore.try_acquire_many(forgettable as u32) {
                permits.forget();
                state.forget_debt -= forgettable;
            }
        }
    }

    /// Acquire one permit, held by the returned guard until it is dropped (on
    /// task completion). Reconciles any pending shrink debt first so a permit
    /// returned by a finished task cannot overshoot a shrunken ceiling. Never
    /// fails unless the semaphore is closed, which this limiter never does.
    async fn acquire(&self) -> tokio::sync::OwnedSemaphorePermit {
        {
            let mut state = self.state.lock().unwrap();
            self.pay_forget_debt(&mut state);
        }
        Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("write inflight semaphore is never closed")
    }

    /// Shrink the live ceiling toward a 429's advertised max-in-flight (or
    /// halve when none was advertised). Forgets available permits immediately and
    /// records the rest as debt; never forgets more than are available, so
    /// in-flight work is never wedged.
    fn note_rate_limited(&self, advertised: Option<usize>) {
        let mut state = self.state.lock().unwrap();
        let next = adaptive_inflight_after_429(state.target, self.max, advertised);
        if next < state.target {
            state.forget_debt += state.target - next;
            state.target = next;
            self.pay_forget_debt(&mut state);
        }
    }

    /// Additively recover the live ceiling toward the configured max after a
    /// clean batch. Cancels outstanding shrink debt first (cheapest way to
    /// re-grant capacity), then adds real permits for any remaining growth.
    fn note_success(&self) {
        let mut state = self.state.lock().unwrap();
        // Reconcile any outstanding shrink debt first so permits returned by
        // finished in-flight tasks (which can sit above the shrunken `target`
        // until the next acquire) are reclaimed eagerly here rather than only at
        // the next acquire(). This keeps the live ceiling closer to `target`
        // between batches instead of relying solely on an acquire to reconcile.
        self.pay_forget_debt(&mut state);
        let next =
            adaptive_inflight_after_success(state.target, self.max, ADAPTIVE_INFLIGHT_RECOVERY_STEP);
        if next > state.target {
            let mut grow = next - state.target;
            let debt_cancelled = grow.min(state.forget_debt);
            state.forget_debt -= debt_cancelled;
            grow -= debt_cancelled;
            if grow > 0 {
                self.semaphore.add_permits(grow);
            }
            state.target = next;
        }
    }
}

fn record_batched_write_failure(
    failures_by_stripe: &mut HashMap<u32, Vec<FragmentWriteFailure>>,
    failure: FragmentWriteFailure,
) {
    failures_by_stripe
        .entry(failure.stripe_index)
        .or_default()
        .push(failure);
}

fn join_fragment_failures(failures: &[FragmentWriteFailure]) -> String {
    failures
        .iter()
        .map(|failure| failure.message.clone())
        .collect::<Vec<_>>()
        .join(" | ")
}

fn add_object_phase_times(into: &mut ObjectPhaseTimes, from: &ObjectPhaseTimes) {
    into.kms_initiate += from.kms_initiate;
    into.kms_commit += from.kms_commit;
    into.kms_resolve += from.kms_resolve;
    into.ec_encode += from.ec_encode;
    into.ec_reconstruct += from.ec_reconstruct;
    into.target_connect += from.target_connect;
    into.target_write += from.target_write;
    into.target_read += from.target_read;
    into.target_ready_wait += from.target_ready_wait;
    into.target_request_prepare += from.target_request_prepare;
    into.target_send_headers += from.target_send_headers;
    into.target_send_body += from.target_send_body;
    into.target_wait_response += from.target_wait_response;
    into.target_collect_response += from.target_collect_response;
    into.target_protocol_decode += from.target_protocol_decode;
    into.target_payload_validate += from.target_payload_validate;
}

fn stripe_logical_bytes(profile: &EcProfile) -> Result<usize, ObjectError> {
    (profile.data_fragments as usize)
        .checked_mul(profile.fragment_bytes as usize)
        .ok_or_else(|| {
            ObjectError::Metadata(format!(
                "EC profile {} has an unsupported stripe geometry",
                profile.id
            ))
        })
}

/// Map a non-empty, length-clamped object byte range `[start, end)` to the
/// inclusive span of stripe indices it touches, given the uniform stripe width.
/// Caller guarantees `end > start` and `stripe_width > 0`.
fn range_to_stripe_indices(start: u64, end: u64, stripe_width: u64) -> (usize, usize) {
    let first = (start / stripe_width) as usize;
    let last = ((end - 1) / stripe_width) as usize;
    (first, last)
}

/// For one covered stripe, the `[lo, hi)` slice of its payload (already
/// truncated to the stripe's logical length) that falls inside the requested
/// object range `[start, end)`. Returns an empty slice if the range does not
/// overlap this stripe's payload.
fn stripe_slice_bounds(
    start: u64,
    end: u64,
    stripe_index: usize,
    stripe_width: u64,
    payload_len: usize,
) -> (usize, usize) {
    let stripe_object_start = (stripe_index as u64) * stripe_width;
    let payload_len = payload_len as u64;
    let lo = start.saturating_sub(stripe_object_start).min(payload_len) as usize;
    let hi = end.saturating_sub(stripe_object_start).min(payload_len) as usize;
    (lo, hi)
}

fn stripe_payload_range(
    payload_len: usize,
    stripe_index: usize,
    stripe_logical_bytes: usize,
) -> Result<(usize, usize), ObjectError> {
    let start = stripe_index
        .checked_mul(stripe_logical_bytes)
        .ok_or_else(|| {
            ObjectError::Metadata(format!(
                "stripe {} overflowed payload indexing for {} bytes",
                stripe_index, payload_len
            ))
        })?;
    if start >= payload_len {
        return Err(ObjectError::Metadata(format!(
            "stripe {} starts past payload end {}",
            stripe_index, payload_len
        )));
    }
    Ok((
        start,
        payload_len.min(start.saturating_add(stripe_logical_bytes)),
    ))
}

fn fragment_plans_for_stripe(
    plans: &[FragmentPlan],
    stripe_index: u32,
) -> Result<Vec<FragmentPlan>, ObjectError> {
    let mut stripe_plans = plans
        .iter()
        .filter(|plan| plan.stripe_index == stripe_index)
        .cloned()
        .collect::<Vec<_>>();
    if stripe_plans.is_empty() {
        return Err(ObjectError::Metadata(format!(
            "write intent is missing fragment plans for stripe {}",
            stripe_index
        )));
    }
    stripe_plans.sort_unstable_by_key(|plan| plan.fragment_index);
    Ok(stripe_plans)
}

fn fragment_window_stripe_count(plans: &[FragmentPlan]) -> usize {
    plans
        .iter()
        .map(|plan| plan.stripe_index as usize)
        .max()
        .map(|index| index.saturating_add(1))
        .unwrap_or(0)
}

fn retry_plans_for_failures(
    plans: &[FragmentPlan],
    failures: &[FragmentWriteFailure],
) -> Result<Vec<FragmentPlan>, ObjectError> {
    let mut retry_plans = Vec::with_capacity(failures.len());
    for failure in failures {
        let Some(plan) = plans.iter().find(|plan| {
            plan.stripe_index == failure.stripe_index
                && plan.fragment_index == failure.fragment_index
        }) else {
            return Err(ObjectError::Metadata(format!(
                "KSC could not map a failed fragment retry for stripe {} fragment {} back to a fragment plan",
                failure.stripe_index, failure.fragment_index
            )));
        };
        retry_plans.push(plan.clone());
    }
    Ok(retry_plans)
}

pub async fn put_object_single_stripe(
    kms_endpoints: &[String],
    bucket_id: &str,
    key: &str,
    payload: &[u8],
) -> Result<ObjectPutResult, ObjectError> {
    put_object_single_stripe_with_options(
        kms_endpoints,
        bucket_id,
        key,
        payload,
        ObjectClientOptions::default(),
    )
    .await
}

pub async fn put_object_single_stripe_with_options(
    kms_endpoints: &[String],
    bucket_id: &str,
    key: &str,
    payload: &[u8],
    options: ObjectClientOptions,
) -> Result<ObjectPutResult, ObjectError> {
    let mut client = ObjectClient::connect_with_options(kms_endpoints, options).await?;
    client
        .put_object_single_stripe(bucket_id, key, payload)
        .await
}

pub async fn put_object_from_path(
    kms_endpoints: &[String],
    bucket_id: &str,
    key: &str,
    path: &Path,
) -> Result<ObjectPutResult, ObjectError> {
    put_object_from_path_with_options(
        kms_endpoints,
        bucket_id,
        key,
        path,
        ObjectClientOptions::default(),
    )
    .await
}

pub async fn put_object_from_path_with_options(
    kms_endpoints: &[String],
    bucket_id: &str,
    key: &str,
    path: &Path,
    options: ObjectClientOptions,
) -> Result<ObjectPutResult, ObjectError> {
    let mut client = ObjectClient::connect_with_options(kms_endpoints, options).await?;
    client.put_object_from_path(bucket_id, key, path).await
}

pub async fn get_object_single_stripe(
    kms_endpoints: &[String],
    bucket_id: &str,
    key: &str,
) -> Result<ObjectGetResult, ObjectError> {
    get_object_single_stripe_with_options(
        kms_endpoints,
        bucket_id,
        key,
        ObjectClientOptions::default(),
    )
    .await
}

pub async fn get_object_single_stripe_with_options(
    kms_endpoints: &[String],
    bucket_id: &str,
    key: &str,
    options: ObjectClientOptions,
) -> Result<ObjectGetResult, ObjectError> {
    let mut client = ObjectClient::connect_with_options(kms_endpoints, options).await?;
    client.get_object_single_stripe(bucket_id, key).await
}

pub async fn get_object_range(
    kms_endpoints: &[String],
    bucket_id: &str,
    key: &str,
    offset: u64,
    len: u64,
) -> Result<RangedGetResult, ObjectError> {
    get_object_range_with_options(
        kms_endpoints,
        bucket_id,
        key,
        offset,
        len,
        ObjectClientOptions::default(),
    )
    .await
}

pub async fn get_object_range_with_options(
    kms_endpoints: &[String],
    bucket_id: &str,
    key: &str,
    offset: u64,
    len: u64,
    options: ObjectClientOptions,
) -> Result<RangedGetResult, ObjectError> {
    let mut client = ObjectClient::connect_with_options(kms_endpoints, options).await?;
    client.get_object_range(bucket_id, key, offset, len).await
}

pub async fn delete_object(
    kms_endpoints: &[String],
    bucket_id: &str,
    key: &str,
    version_ids: &[String],
) -> Result<ObjectDeleteResult, ObjectError> {
    delete_object_with_options(
        kms_endpoints,
        bucket_id,
        key,
        version_ids,
        ObjectClientOptions::default(),
    )
    .await
}

pub async fn delete_object_with_options(
    kms_endpoints: &[String],
    bucket_id: &str,
    key: &str,
    version_ids: &[String],
    options: ObjectClientOptions,
) -> Result<ObjectDeleteResult, ObjectError> {
    let mut client = ObjectClient::connect_with_options(kms_endpoints, options).await?;
    client.delete_object(bucket_id, key, version_ids).await
}

pub fn kee_profile_from_control(profile: &EcProfile) -> Result<KeeProfile, ObjectError> {
    let failure_domain = match FailureDomain::try_from(profile.failure_domain)
        .unwrap_or(FailureDomain::Unspecified)
    {
        FailureDomain::DriveDomainLab => KeeFailureDomain::DriveDomainLab,
        FailureDomain::Node => KeeFailureDomain::Node,
        FailureDomain::Rack => KeeFailureDomain::Rack,
        FailureDomain::Unspecified => {
            return Err(ObjectError::Metadata(format!(
                "control-plane EC profile {} does not specify a valid failure domain",
                profile.id
            )));
        }
    };
    Ok(KeeProfile {
        id: profile.id.clone(),
        codec_id: profile.codec_id.clone(),
        data_fragments: profile.data_fragments as usize,
        parity_fragments: profile.parity_fragments as usize,
        fragment_bytes: profile.fragment_bytes as usize,
        failure_domain,
    })
}

pub fn chunk_id_from_proto(bytes: &[u8]) -> Result<ChunkId, ObjectError> {
    if bytes.len() != 32 {
        return Err(ObjectError::Metadata(format!(
            "fragment chunk id must be 32 bytes, got {}",
            bytes.len()
        )));
    }
    let mut raw = [0_u8; 32];
    raw.copy_from_slice(bytes);
    Ok(ChunkId(raw))
}

fn stripe_logical_length_bytes(
    manifest: &ObjectVersionManifest,
    ec_profile: &EcProfile,
    stripe_index: u32,
) -> u64 {
    let stripe_width_bytes =
        u64::from(ec_profile.data_fragments) * u64::from(ec_profile.fragment_bytes);
    let stripe_start = u64::from(stripe_index).saturating_mul(stripe_width_bytes);
    manifest
        .logical_length_bytes
        .saturating_sub(stripe_start)
        .min(stripe_width_bytes)
}

fn data_fragment_payload_bytes(
    ec_profile: &EcProfile,
    stripe_logical_bytes: u64,
    fragment_index: usize,
) -> usize {
    if stripe_logical_bytes == 0 {
        return 0;
    }
    let fragment_bytes = u64::from(ec_profile.fragment_bytes.max(1));
    let fragment_start = (fragment_index as u64).saturating_mul(fragment_bytes);
    stripe_logical_bytes
        .saturating_sub(fragment_start)
        .min(fragment_bytes) as usize
}

fn needed_data_fragment_count(ec_profile: &EcProfile, stripe_logical_bytes: u64) -> usize {
    if stripe_logical_bytes == 0 {
        return 0;
    }
    let fragment_bytes = u64::from(ec_profile.fragment_bytes.max(1));
    let needed = stripe_logical_bytes.div_ceil(fragment_bytes);
    needed
        .min(u64::from(ec_profile.data_fragments))
        .try_into()
        .unwrap_or(ec_profile.data_fragments as usize)
}

static NEXT_KMS_ENDPOINT: AtomicUsize = AtomicUsize::new(0);

fn initial_kms_endpoint_offset(endpoint_count: usize) -> usize {
    if endpoint_count == 0 {
        return 0;
    }
    let process_bias = std::process::id() as usize;
    let time_bias = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos() as usize)
        .unwrap_or(0);
    let global_bias = NEXT_KMS_ENDPOINT.fetch_add(1, Ordering::Relaxed);
    process_bias
        .wrapping_mul(1_103_515_245)
        .wrapping_add(time_bias)
        .wrapping_add(global_bias)
        % endpoint_count
}

impl KmsEndpointBalancer {
    async fn connect(
        kms_endpoints: &[String],
        grpc_max_message_bytes: usize,
    ) -> Result<Self, ObjectError> {
        if kms_endpoints.is_empty() {
            return Err(ObjectError::Transport(
                "KSC has no KMS endpoints configured".to_string(),
            ));
        }
        let start = initial_kms_endpoint_offset(kms_endpoints.len());
        let mut errors = Vec::with_capacity(kms_endpoints.len());
        let mut channels = Vec::with_capacity(kms_endpoints.len());
        for offset in 0..kms_endpoints.len() {
            let kms_endpoint = &kms_endpoints[(start + offset) % kms_endpoints.len()];
            let endpoint = match Endpoint::from_shared(kms_endpoint.to_string()) {
                Ok(endpoint) => endpoint
                    .initial_stream_window_size(KMS_GRPC_INITIAL_STREAM_WINDOW_BYTES)
                    .initial_connection_window_size(KMS_GRPC_INITIAL_CONNECTION_WINDOW_BYTES)
                    .http2_keep_alive_interval(KMS_GRPC_KEEPALIVE_INTERVAL)
                    .keep_alive_timeout(KMS_GRPC_KEEPALIVE_TIMEOUT)
                    .keep_alive_while_idle(true),
                Err(err) => {
                    errors.push(format!("{} invalid: {}", kms_endpoint, err));
                    continue;
                }
            };
            match endpoint.connect().await {
                Ok(channel) => channels.push(channel),
                Err(err) => errors.push(format!("{} connect failed: {}", kms_endpoint, err)),
            }
        }
        if channels.is_empty() {
            return Err(ObjectError::Transport(format!(
                "KSC could not connect to any KMS endpoint: {}",
                errors.join(" | ")
            )));
        }
        Ok(Self {
            channels: Arc::new(channels),
            next: Arc::new(AtomicUsize::new(start)),
            grpc_max_message_bytes,
        })
    }

    fn client(&self) -> KmsClient<Channel> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.channels.len();
        KmsClient::new(self.channels[index].clone())
            .max_decoding_message_size(self.grpc_max_message_bytes)
            .max_encoding_message_size(self.grpc_max_message_bytes)
    }
}

fn global_target_sessions() -> Arc<tokio::sync::Mutex<HashMap<String, TargetSession>>> {
    static SHARED: OnceLock<Arc<tokio::sync::Mutex<HashMap<String, TargetSession>>>> =
        OnceLock::new();
    SHARED
        .get_or_init(|| Arc::new(tokio::sync::Mutex::new(HashMap::new())))
        .clone()
}

async fn control_rpc<T, F>(label: &str, future: F) -> Result<T, ObjectError>
where
    F: Future<Output = Result<T, tonic::Status>>,
{
    timeout(CONTROL_RPC_TIMEOUT, future)
        .await
        .map_err(|_| {
            ObjectError::Transport(format!(
                "{} timed out after {} ms",
                label,
                CONTROL_RPC_TIMEOUT.as_millis()
            ))
        })?
        .map_err(ObjectError::from)
}

async fn data_rpc<T, F>(
    label: &str,
    timeout_duration: Duration,
    future: F,
) -> Result<T, ObjectError>
where
    F: Future<Output = Result<T, ClientError>>,
{
    timeout(timeout_duration, future)
        .await
        .map_err(|_| {
            ObjectError::Transport(format!(
                "{} timed out after {} ms",
                label,
                timeout_duration.as_millis()
            ))
        })?
        .map_err(ObjectError::from)
}

fn add_request_phase_times(into: &mut RequestPhaseTimes, phases: &RequestPhaseTimes) {
    into.ready_wait += phases.ready_wait;
    into.request_prepare += phases.request_prepare;
    into.send_headers += phases.send_headers;
    into.send_body += phases.send_body;
    into.wait_response += phases.wait_response;
    into.collect_response += phases.collect_response;
    into.protocol_decode += phases.protocol_decode;
    into.payload_validate += phases.payload_validate;
}

fn accumulate_target_request_phases(into: &mut ObjectPhaseTimes, request: &RequestPhaseTimes) {
    into.target_ready_wait += request.ready_wait;
    into.target_request_prepare += request.request_prepare;
    into.target_send_headers += request.send_headers;
    into.target_send_body += request.send_body;
    into.target_wait_response += request.wait_response;
    into.target_collect_response += request.collect_response;
    into.target_protocol_decode += request.protocol_decode;
    into.target_payload_validate += request.payload_validate;
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- ranged-read stripe arithmetic (Phase 2 / get_object_range) ----

    #[test]
    fn range_to_stripe_indices_spans_boundaries() {
        let w = 100u64;
        // Whole first stripe.
        assert_eq!(range_to_stripe_indices(0, 100, w), (0, 0));
        // Range entirely inside stripe 1.
        assert_eq!(range_to_stripe_indices(100, 200, w), (1, 1));
        // Range crossing the 0/1 boundary.
        assert_eq!(range_to_stripe_indices(50, 150, w), (0, 1));
        // End exactly on a boundary stays in the lower stripe (exclusive end).
        assert_eq!(range_to_stripe_indices(0, 100, w), (0, 0));
        assert_eq!(range_to_stripe_indices(99, 100, w), (0, 0));
        assert_eq!(range_to_stripe_indices(100, 101, w), (1, 1));
        // Multi-stripe span.
        assert_eq!(range_to_stripe_indices(50, 350, w), (0, 3));
        // Single byte at the very start of stripe 2.
        assert_eq!(range_to_stripe_indices(200, 201, w), (2, 2));
    }

    #[test]
    fn stripe_slice_bounds_clamps_to_payload_and_range() {
        let w = 100u64;
        // First covered stripe (index 0): range starts mid-stripe.
        assert_eq!(stripe_slice_bounds(50, 150, 0, w, 100), (50, 100));
        // Interior/last covered stripe (index 1): range ends mid-stripe.
        assert_eq!(stripe_slice_bounds(50, 150, 1, w, 100), (0, 50));
        // Range fully inside a single interior stripe (index 1).
        assert_eq!(stripe_slice_bounds(120, 180, 1, w, 100), (20, 80));
        // Short last stripe: payload shorter than stripe width clamps hi.
        assert_eq!(stripe_slice_bounds(300, 340, 3, w, 25), (0, 25));
        // Stripe not overlapping the range yields an empty slice.
        assert_eq!(stripe_slice_bounds(0, 50, 2, w, 100), (0, 0));
    }

    #[test]
    fn stripe_slice_bounds_reassembles_a_full_object() {
        // Object of 250 bytes, stripe width 100 => stripes [100,100,50].
        let w = 100u64;
        let stripes = [vec![1u8; 100], vec![2u8; 100], vec![3u8; 50]];
        // Read [80, 230): stripe0[80..100] + stripe1[0..100] + stripe2[0..30].
        let (start, end) = (80u64, 230u64);
        let (first, last) = range_to_stripe_indices(start, end, w);
        assert_eq!((first, last), (0, 2));
        let mut out = Vec::new();
        for s in first..=last {
            let (lo, hi) = stripe_slice_bounds(start, end, s, w, stripes[s].len());
            out.extend_from_slice(&stripes[s][lo..hi]);
        }
        assert_eq!(out.len(), 150);
        assert_eq!(&out[..20], &[1u8; 20]);
        assert_eq!(&out[20..120], &[2u8; 100]);
        assert_eq!(&out[120..150], &[3u8; 30]);
    }

    fn test_client_with_cache(shared_read_cache: SharedObjectMetadataCache) -> ObjectClient {
        ObjectClient {
            kms: KmsEndpointBalancer {
                channels: Arc::new(Vec::new()),
                next: Arc::new(AtomicUsize::new(0)),
                grpc_max_message_bytes: DEFAULT_KMS_GRPC_MAX_MESSAGE_BYTES,
            },
            target_sessions: HashMap::new(),
            shared_target_sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            bucket_profiles: HashMap::new(),
            shared_read_cache,
            prepared_encoders: HashMap::new(),
            session_options: TargetSessionOptions::default(),
            write_window_max_stripes: 1,
            write_window_inflight_stripes: 1,
            write_inflight_limiter: Arc::new(AdaptiveWriteLimiter::new(1)),
        }
    }

    fn test_client() -> ObjectClient {
        let options = ObjectClientOptions {
            read_resolve_cache_ttl: Duration::from_secs(300),
            read_payload_cache_max_entries: 4,
            read_payload_cache_max_bytes: 8 * 1024 * 1024,
            read_payload_cache_max_object_bytes: 2 * 1024 * 1024,
            ..ObjectClientOptions::default()
        };
        test_client_with_cache(SharedObjectMetadataCache::new(&options))
    }

    fn test_manifest(version_id: &str, key: &str) -> ObjectVersionManifest {
        ObjectVersionManifest {
            version_id: version_id.to_string(),
            namespace_id: "lab-ns".to_string(),
            object_entry_id: format!("obj::{version_id}"),
            bucket_entry_id: "bucket::lab-8p2".to_string(),
            bucket_id: "lab-8p2".to_string(),
            key: key.to_string(),
            logical_length_bytes: 1024,
            ec_profile_id: "lab-rs-8p2-1m".to_string(),
            stripes: Vec::new(),
        }
    }

    fn test_fragment_plan(endpoint: &str, stripe_index: u32, fragment_index: u32) -> FragmentPlan {
        FragmentPlan {
            fragment_index,
            chunk_id: vec![fragment_index as u8; 32],
            target_id: format!("target-{fragment_index}"),
            endpoint: endpoint.to_string(),
            granule_index: u64::from(fragment_index),
            generation: 1,
            stripe_index,
        }
    }

    fn test_profile() -> EcProfile {
        EcProfile {
            id: "lab-rs-8p2-1m".to_string(),
            codec_id: "rs".to_string(),
            data_fragments: 8,
            parity_fragments: 2,
            fragment_bytes: 1024 * 1024,
            failure_domain: FailureDomain::Node as i32,
        }
    }

    #[test]
    fn payload_cache_round_trips_small_object() {
        let mut client = test_client();
        let manifest = test_manifest("v1", "bench/object.bin");
        let payload = vec![7_u8; 1024];
        client.cache_payload_read(&manifest, &payload);
        let cached = client.cached_payload_read(&manifest).expect("cache hit");
        assert_eq!(cached, payload);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn batched_read_missing_session_is_treated_as_missing_fragment() {
        let target_sessions = Arc::new(HashMap::new());
        let mut window_states = vec![WindowStripeReadState {
            stripe_index: 0,
            needed_data_fragments: 1,
            fragments: vec![None],
        }];
        let read_plans = vec![BatchedTargetReadPlan {
            endpoint: "http://127.0.0.1:18082".to_string(),
            stripe_index: 0,
            fragment_index: 0,
            chunk_id: ChunkId([7; 32]),
            payload_bytes: 1024,
        }];

        let result =
            read_plans_batched_with_sessions(target_sessions, 0, &mut window_states, read_plans)
                .await
                .expect("missing target session should degrade, not fail");

        assert_eq!(result.read_elapsed, Duration::ZERO);
        assert!(window_states[0].fragments[0].is_none());
    }

    #[test]
    fn payload_cache_invalidation_drops_all_versions_for_key() {
        let mut client = test_client();
        let payload = vec![9_u8; 1024];
        let manifest_v1 = test_manifest("v1", "bench/object.bin");
        let manifest_v2 = test_manifest("v2", "bench/object.bin");
        client.cache_payload_read(&manifest_v1, &payload);
        client.cache_payload_read(&manifest_v2, &payload);
        client.invalidate_resolved_read("lab-8p2", "bench/object.bin");
        assert!(client.cached_payload_read(&manifest_v1).is_none());
        assert!(client.cached_payload_read(&manifest_v2).is_none());
    }

    #[test]
    fn shared_resolve_cache_invalidation_reaches_other_clients() {
        let options = ObjectClientOptions {
            read_resolve_cache_ttl: Duration::from_secs(300),
            ..ObjectClientOptions::default()
        };
        let shared_cache = SharedObjectMetadataCache::new(&options);
        let mut writer = test_client_with_cache(shared_cache.clone());
        let mut reader = test_client_with_cache(shared_cache);
        let manifest = test_manifest("v1", "bench/object.bin");
        writer.cache_resolved_read(manifest.clone(), test_profile());

        assert!(reader
            .cached_resolved_read("lab-8p2", "bench/object.bin")
            .is_some());

        writer.invalidate_resolved_read("lab-8p2", "bench/object.bin");

        assert!(reader
            .cached_resolved_read("lab-8p2", "bench/object.bin")
            .is_none());
    }

    #[test]
    fn endpoint_write_batches_split_at_packed_payload_ceiling() {
        let prepared_batch = vec![PreparedStripeWrite {
            stripe_index: 0,
            plans: (0..17)
                .map(|fragment_index| test_fragment_plan("http://t0", 0, fragment_index))
                .collect(),
            fragments: (0..17).map(|_| vec![7_u8; 1024 * 1024]).collect(),
        }];

        let batches = build_endpoint_write_batches("intent-1", &prepared_batch).unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 16);
        assert_eq!(batches[1].len(), 1);
        assert!(batches
            .iter()
            .all(|batch| batch.iter().all(|entry| entry.endpoint == "http://t0")));
    }

    #[test]
    fn endpoint_write_batches_do_not_mix_targets() {
        let prepared_batch = vec![PreparedStripeWrite {
            stripe_index: 0,
            plans: vec![
                test_fragment_plan("http://t0", 0, 0),
                test_fragment_plan("http://t1", 0, 1),
            ],
            fragments: vec![vec![1_u8; 1024], vec![2_u8; 1024]],
        }];

        let mut batches = build_endpoint_write_batches("intent-2", &prepared_batch).unwrap();
        batches.sort_by(|left, right| left[0].endpoint.cmp(&right[0].endpoint));

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[1].len(), 1);
        assert_ne!(batches[0][0].endpoint, batches[1][0].endpoint);
    }

    #[test]
    fn endpoint_read_batches_split_at_packed_payload_ceiling() {
        let plans = (0..17)
            .map(|fragment_index| BatchedTargetReadPlan {
                endpoint: "http://t0".to_string(),
                stripe_index: 0,
                fragment_index,
                chunk_id: ChunkId([fragment_index as u8; 32]),
                payload_bytes: 1024 * 1024,
            })
            .collect::<Vec<_>>();

        let batches = build_endpoint_read_batches(plans);

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 16);
        assert_eq!(batches[1].len(), 1);
        assert!(batches
            .iter()
            .all(|batch| batch.iter().all(|entry| entry.endpoint == "http://t0")));
    }

    #[test]
    fn endpoint_read_batches_do_not_mix_targets() {
        let mut batches = build_endpoint_read_batches(vec![
            BatchedTargetReadPlan {
                endpoint: "http://t0".to_string(),
                stripe_index: 0,
                fragment_index: 0,
                chunk_id: ChunkId([0_u8; 32]),
                payload_bytes: 1024,
            },
            BatchedTargetReadPlan {
                endpoint: "http://t1".to_string(),
                stripe_index: 0,
                fragment_index: 1,
                chunk_id: ChunkId([1_u8; 32]),
                payload_bytes: 1024,
            },
        ]);
        batches.sort_by(|left, right| left[0].endpoint.cmp(&right[0].endpoint));

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[1].len(), 1);
        assert_ne!(batches[0][0].endpoint, batches[1][0].endpoint);
    }

    fn test_shard_pool() -> ShardPool {
        let kee_profile = kee_profile_from_control(&test_profile()).expect("kee profile");
        let engine = KeeEngine::new(kee_profile).expect("kee engine");
        let plan = engine.prepared_plan().expect("prepared plan");
        ShardPool::new(plan)
    }

    #[test]
    fn shard_pool_recycles_returned_buffers() {
        let pool = test_shard_pool();
        // Empty pool allocates fresh buffers.
        let shards = pool.take_shards();
        assert_eq!(shards.len(), 10); // 8 data + 2 parity fragment slots
        assert_eq!(pool.reusable_shards.lock().unwrap().len(), 0);
        pool.return_shards(shards);
        assert_eq!(pool.reusable_shards.lock().unwrap().len(), 1);
        // The next take pops the recycled set rather than allocating another.
        let _recycled = pool.take_shards();
        assert_eq!(pool.reusable_shards.lock().unwrap().len(), 0);
    }

    #[test]
    fn shard_pool_is_shared_across_clones_and_threads() {
        let pool = test_shard_pool();
        // Pre-seed two recyclable buffer sets (take both first, then return both, so
        // the second take cannot just pop the set the first one returned).
        let first = pool.take_shards();
        let second = pool.take_shards();
        pool.return_shards(first);
        pool.return_shards(second);
        assert_eq!(pool.reusable_shards.lock().unwrap().len(), 2);

        // Clones observe the same backing pool (Arc<Mutex<..>>); concurrent takes and
        // returns from worker threads keep the recycling accounting consistent.
        let handles = (0..8)
            .map(|_| {
                let pool = pool.clone();
                std::thread::spawn(move || {
                    for _ in 0..64 {
                        let shards = pool.take_shards();
                        pool.return_shards(shards);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().expect("worker thread");
        }
        // No buffers leaked or duplicated: at least the two seeded sets remain, and the
        // pool never panicked on a poisoned lock.
        assert!(pool.reusable_shards.lock().unwrap().len() >= 2);
    }

    fn encode_test_plans(stripe_count: u32, endpoint: &str) -> Vec<FragmentPlan> {
        (0..stripe_count)
            .flat_map(|stripe_index| {
                (0..10).map(move |fragment_index| {
                    test_fragment_plan(endpoint, stripe_index, fragment_index)
                })
            })
            .collect()
    }

    #[tokio::test]
    async fn encode_stripe_batch_preserves_stripe_order_and_recycles_via_pool() {
        let pool = test_shard_pool();
        let profile = test_profile();
        let stripe_logical = stripe_logical_bytes(&profile).expect("stripe logical bytes");
        let stripe_count = 4_u32;
        let window_plans = encode_test_plans(stripe_count, "http://t0");
        let logical_length = stripe_logical * stripe_count as usize;

        // The read closure stamps each stripe's payload with its index so we can verify
        // the encoded batch comes back ordered by ascending stripe index regardless of
        // the order the blocking encode workers complete in.
        let mut read_calls = Vec::new();
        let mut read_range = |offset: u64, len: usize| -> Result<Vec<u8>, ObjectError> {
            let stripe_index = (offset as usize / stripe_logical) as u8;
            read_calls.push(stripe_index);
            Ok(vec![stripe_index; len])
        };

        let encoded = encode_stripe_batch(
            pool.clone(),
            &window_plans,
            0,
            0,
            stripe_count as usize,
            logical_length,
            stripe_logical,
            &mut read_range,
        )
        .await
        .expect("encode batch");

        assert_eq!(encoded.prepared_batch.len(), stripe_count as usize);
        let returned_order: Vec<u32> = encoded
            .prepared_batch
            .iter()
            .map(|stripe| stripe.stripe_index)
            .collect();
        assert_eq!(returned_order, vec![0, 1, 2, 3]);
        assert_eq!(read_calls, vec![0, 1, 2, 3]);
        for stripe in &encoded.prepared_batch {
            assert_eq!(stripe.fragments.len(), 10);
            assert!(stripe.plans.iter().all(|plan| plan.stripe_index == stripe.stripe_index));
        }

        // Recycle the encoded shard buffers back into the shared pool, as the write
        // loop does after the network writes complete, and confirm they are reusable.
        let before = pool.reusable_shards.lock().unwrap().len();
        for stripe in encoded.prepared_batch {
            pool.return_shards(stripe.fragments);
        }
        assert_eq!(
            pool.reusable_shards.lock().unwrap().len(),
            before + stripe_count as usize
        );
    }

    #[tokio::test]
    async fn encode_stripe_batch_matches_direct_encode_bytes() {
        // The pipelined off-runtime encode must produce byte-identical fragments to a
        // direct in-line encode of the same payload.
        let pool = test_shard_pool();
        let profile = test_profile();
        let stripe_logical = stripe_logical_bytes(&profile).expect("stripe logical bytes");
        let window_plans = encode_test_plans(1, "http://t0");

        let payload: Vec<u8> = (0..stripe_logical).map(|i| (i % 251) as u8).collect();
        let mut read_range = {
            let payload = payload.clone();
            move |_offset: u64, len: usize| -> Result<Vec<u8>, ObjectError> {
                Ok(payload[..len].to_vec())
            }
        };

        let encoded = encode_stripe_batch(
            pool.clone(),
            &window_plans,
            0,
            0,
            1,
            stripe_logical,
            stripe_logical,
            &mut read_range,
        )
        .await
        .expect("encode batch");

        let mut expected = pool.plan.allocate_output_buffers();
        pool.plan
            .encode_into(&payload, &mut expected)
            .expect("direct encode");
        assert_eq!(encoded.prepared_batch[0].fragments, expected);
    }

    // ---- KP2 429 backpressure: backoff + adaptive-limit math (Phase 3) ----

    fn rate_limited_failure(retry_after_ms: Option<u64>) -> FragmentWriteFailure {
        FragmentWriteFailure::new(
            0,
            0,
            "rate limited".to_string(),
            RateLimitSignal {
                rate_limited: true,
                retry_after_ms,
                limit_max_inflight: None,
            },
        )
    }

    #[test]
    fn retry_backoff_falls_back_to_exponential_when_no_429() {
        let fallback = Duration::from_millis(50);
        let ceiling = Duration::from_secs(3);
        // Plain (non-429) failure: backoff is fallback << attempt.
        let plain = vec![FragmentWriteFailure::plain(0, 0, "boom".to_string())];
        assert_eq!(
            compute_retry_backoff(&plain, 0, fallback, ceiling),
            Duration::from_millis(50)
        );
        assert_eq!(
            compute_retry_backoff(&plain, 1, fallback, ceiling),
            Duration::from_millis(100)
        );
        assert_eq!(
            compute_retry_backoff(&plain, 2, fallback, ceiling),
            Duration::from_millis(200)
        );
        // Exponential growth saturates at the ceiling, never beyond.
        assert_eq!(
            compute_retry_backoff(&plain, 30, fallback, ceiling),
            ceiling
        );
        // Empty batch behaves like the no-429 fallback path.
        assert_eq!(
            compute_retry_backoff(&[], 0, fallback, ceiling),
            Duration::from_millis(50)
        );
    }

    #[test]
    fn retry_backoff_honors_max_retry_after_clamped() {
        let fallback = Duration::from_millis(50);
        let ceiling = Duration::from_secs(3);
        // Takes the MAX retry-after across rate-limited failures (250 > 100).
        let failures = vec![
            rate_limited_failure(Some(100)),
            rate_limited_failure(Some(250)),
            // A plain failure in the mix is ignored by the 429 path.
            FragmentWriteFailure::plain(0, 0, "boom".to_string()),
        ];
        assert_eq!(
            compute_retry_backoff(&failures, 0, fallback, ceiling),
            Duration::from_millis(250)
        );
        // A huge retry-after is clamped to the ceiling.
        let absurd = vec![rate_limited_failure(Some(60_000))];
        assert_eq!(compute_retry_backoff(&absurd, 0, fallback, ceiling), ceiling);
        // 429 without a retry-after header falls through to the fallback (not 0).
        let no_header = vec![rate_limited_failure(None)];
        assert_eq!(
            compute_retry_backoff(&no_header, 0, fallback, ceiling),
            Duration::from_millis(50)
        );
    }

    #[test]
    fn adaptive_inflight_shrinks_on_429() {
        // Advertised ceiling pulls the limit down to it.
        assert_eq!(adaptive_inflight_after_429(16, 16, Some(4)), 4);
        // Advertised >= current does not grow the live limit here.
        assert_eq!(adaptive_inflight_after_429(8, 16, Some(12)), 8);
        // No advertised ceiling => multiplicative halving.
        assert_eq!(adaptive_inflight_after_429(16, 16, None), 8);
        assert_eq!(adaptive_inflight_after_429(8, 16, None), 4);
        // Floor is always >= 1, never 0.
        assert_eq!(adaptive_inflight_after_429(1, 16, None), 1);
        assert_eq!(adaptive_inflight_after_429(2, 16, Some(0)), 1);
        assert_eq!(adaptive_inflight_after_429(1, 16, Some(0)), 1);
    }

    #[test]
    fn adaptive_inflight_recovers_additively_to_max() {
        // Additive increase by `step`, capped at the configured max.
        assert_eq!(adaptive_inflight_after_success(4, 16, 1), 5);
        assert_eq!(adaptive_inflight_after_success(15, 16, 1), 16);
        assert_eq!(adaptive_inflight_after_success(16, 16, 1), 16);
        // Larger step is clamped at max, never overshoots.
        assert_eq!(adaptive_inflight_after_success(14, 16, 4), 16);
        // Floor invariant holds even with a zero-ish max.
        assert_eq!(adaptive_inflight_after_success(1, 1, 1), 1);
    }

    #[test]
    fn adaptive_limiter_shrinks_then_recovers_without_deadlock() {
        let limiter = AdaptiveWriteLimiter::new(8);
        assert_eq!(limiter.current_limit(), 8);
        assert_eq!(limiter.semaphore.available_permits(), 8);

        // A 429 advertising max-in-flight 2 shrinks the gate to 2.
        limiter.note_rate_limited(Some(2));
        assert_eq!(limiter.current_limit(), 2);
        assert_eq!(limiter.semaphore.available_permits(), 2);

        // Shrinking never forgets more permits than are available: even an
        // aggressive shrink leaves at least one permit so the gate cannot wedge.
        limiter.note_rate_limited(Some(1));
        assert_eq!(limiter.current_limit(), 1);
        assert_eq!(limiter.semaphore.available_permits(), 1);

        // Sustained success additively recovers toward the configured max (8).
        for _ in 0..100 {
            limiter.note_success();
        }
        assert_eq!(limiter.current_limit(), 8);
        assert_eq!(limiter.semaphore.available_permits(), 8);
    }

    #[tokio::test]
    async fn adaptive_limiter_shrink_does_not_lose_held_permits() {
        let limiter = Arc::new(AdaptiveWriteLimiter::new(4));
        // Hold two permits (as in-flight tasks would).
        let held_a = limiter.acquire().await;
        let held_b = limiter.acquire().await;
        assert_eq!(limiter.semaphore.available_permits(), 2);

        // Shrink toward 1 while two permits are checked out: only the 2 available
        // permits can be forgotten immediately, so 1 unit of shrink becomes debt.
        limiter.note_rate_limited(Some(1));
        assert_eq!(limiter.current_limit(), 1);
        assert_eq!(limiter.semaphore.available_permits(), 0);

        // Releasing held permits returns them to the pool (held permits are never
        // lost), temporarily overshooting the shrunken ceiling.
        drop(held_a);
        drop(held_b);
        assert_eq!(limiter.semaphore.available_permits(), 2);

        // The next acquire reconciles: it pays down the 1-unit debt before taking
        // its own permit, so the effective ceiling settles back at the target (1).
        let reconciled = limiter.acquire().await;
        // One forgotten for the debt, one held by `reconciled` => 0 available.
        assert_eq!(limiter.semaphore.available_permits(), 0);
        drop(reconciled);
        // Now exactly `target` (1) permit is available — ceiling honored, no deadlock.
        assert_eq!(limiter.semaphore.available_permits(), 1);
        assert_eq!(limiter.current_limit(), 1);
    }
}
