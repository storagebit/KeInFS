// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

#[cfg(target_os = "linux")]
mod imp {
    use crate::allocator_store::AllocatorStore;
    use crate::fdb_schema::{
        allocator_mutation_lease_name, allocator_state_stamp_key, coordination_lease_key,
        prefix_range_end, reservation_bin_member_key, reservation_bin_prefix, reservation_key,
        reservation_prefix, service_instance_key, service_instance_prefix, target_key,
        target_prefix, target_span_all_prefix, target_span_chunk_key, target_span_prefix,
    };
    use crate::store::{
        ReservationBinKey, ReservationMutationSpec, StorePhaseTiming, TimedStoreResult,
    };
    use foundationdb::{api::NetworkAutoStop, Database, FdbBindingError, RangeOption};
    use futures::StreamExt;
    use keinctl::proto::{
        FailureDomain, PlacementReservation, PlacementReservationRecord, ReservationState,
        ServiceInstanceRecord, ServiceKind, TargetGranule, TargetLifecycleState, TargetRecord,
    };
    use serde::{de::DeserializeOwned, Deserialize, Serialize};
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::error::Error;
    use std::fmt::{Display, Formatter};
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;
    use tonic::Status;
    use uuid::Uuid;

    #[derive(Clone)]
    pub(crate) struct FdbKasStore {
        db: Arc<Database>,
        state: Arc<tokio::sync::Mutex<AllocatorState>>,
        // Per-shard in-process mutation locks. A single instance only ever
        // serves one `allocation_shard_id`, but keying the guard by shard id
        // keeps disjoint shards from serializing against each other and mirrors
        // the per-shard lease/stamp sharding below.
        mutation_guards: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
        owner_id: Arc<String>,
        allocation_shard_id: Arc<Option<String>>,
        // Phase-2 (DESIGN_KAS_WRITE_SCALE.md §3 change #2/#4). When set, this
        // instance holds the per-shard mutation lease for the whole leadership
        // term instead of acquiring/releasing it per op, drops the per-op stamp
        // read, and relies on the epoch fence (#3) for split-brain safety. The
        // flag gates ONLY that behavior change; the epoch fence write/assert is
        // always on (it is strictly stronger than the per-op stamp reload).
        leader_resident_lease: bool,
        // In-memory leadership term: the epoch this instance acquired and the
        // wall-clock expiry of the held lease. `Some` only while we believe we
        // are the shard leader under the leader-resident model; the background
        // renewer keeps `expires_at_unix_ms` ahead of now and the foreground
        // fence asserts `epoch` in every mutating txn. Cleared on step-down /
        // lost renewal / observed epoch bump, which makes the instance stop
        // serving mutations until it re-acquires.
        leadership: Arc<tokio::sync::Mutex<Option<LeadershipTerm>>>,
        // Phase-3 (change #7) deferred durable bin-member deletes. The lock-light
        // claim pops the in-memory bin (authoritative) and defers the durable
        // `reservation_bin_member_key` cleanup here instead of committing it per
        // claim. These entries are write-only acceleration never read back into
        // `state.bins` (every refresh clears bins), so deferral cannot affect
        // claim correctness; the flush is opportunistic and best-effort.
        deferred_bin_member_deletes: Arc<tokio::sync::Mutex<Vec<(String, String)>>>,
        // Optional handle to the runtime stats tree for fence/renewal
        // observability (fenced-commit aborts, leader renew failures). `None` in
        // unit construction; wired by `main` so the counters land under
        // /run/keinfs/kas/<id>/.
        stats: Option<Arc<crate::stats::KasStats>>,
    }

    /// One leadership term under the leader-resident lease model. `epoch` is the
    /// fencing token this instance acquired; the foreground asserts it in every
    /// mutating txn (the epoch fence). `lease_expires_at_unix_ms` is the durable
    /// lease expiry we are renewing toward.
    #[derive(Clone, Copy, Debug)]
    struct LeadershipTerm {
        epoch: u64,
        lease_expires_at_unix_ms: u64,
    }

    #[derive(Default)]
    struct AllocatorState {
        targets: HashMap<String, TargetAllocatorState>,
        reservations: HashMap<String, PlacementReservationRecord>,
        service_instances: HashMap<String, ServiceInstanceRecord>,
        bins: HashMap<String, VecDeque<String>>,
        allocator_state_stamp: Option<String>,
        target_state_loaded: bool,
        reservation_state_loaded: bool,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TargetAllocatorState {
        target: TargetRecord,
        spans: Vec<GranuleSpan>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct CoordinationLeaseRecord {
        owner_id: String,
        expires_at_unix_ms: u64,
        /// Monotonic per-lease fencing token (DESIGN_KAS_WRITE_SCALE.md §3/§4
        /// change #3, the epoch fence). Bumped ONLY on a *new* grant (an
        /// absent/expired lease, or one taken from another owner) — never on a
        /// self-renewal. A new leader that wins a stale lease increments it, so a
        /// superseded old leader still carries the prior (now lower) epoch.
        ///
        /// `#[serde(default)]` so records written before this field existed
        /// decode to epoch 0; the first new acquisition after upgrade bumps to 1,
        /// which is strictly greater, so the fence never spuriously passes for a
        /// stale leader.
        #[serde(default)]
        epoch: u64,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct AllocatorStateStampRecord {
        stamp_id: String,
        updated_at_unix_ms: u64,
        owner_id: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct GranuleSpan {
        start: u64,
        len: u64,
    }

    /// Number of free spans stored per FDB chunk value. A `GranuleSpan` is two
    /// u64s (~28 bytes JSON), so 1024 spans is ~30 KB -- comfortably under the
    /// 100 KB FDB value limit even with serialization overhead. A target's full
    /// free-span list is split into `ceil(len / SPANS_PER_CHUNK)` such chunks.
    const SPANS_PER_CHUNK: usize = 1024;

    /// Upper bound on how many reservations one refill persist commits in a
    /// single FDB transaction. The refiller loops in batches of this size so a
    /// large `top_up_chunk` can never build a transaction above the 10 MB FDB
    /// limit (FdbError 2101), and releases/re-acquires the mutation lease between
    /// batches so foreground reserves are not starved.
    const REFILL_PERSIST_BATCH_MAX: usize = 1024;

    /// One persisted chunk of a target's free-span list. The `target_id` and
    /// `chunk_index` are carried in the value so the bulk loader can group and
    /// order chunks without parsing keys.
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TargetSpanChunk {
        target_id: String,
        chunk_index: usize,
        spans: Vec<GranuleSpan>,
    }

    /// Split a target's free-span list into bounded chunks for FDB storage.
    /// Returns at least one (possibly empty) chunk so an empty span list still
    /// rewrites cleanly. Pure and unit-tested independently of FoundationDB.
    fn split_spans_into_chunks(target_id: &str, spans: &[GranuleSpan]) -> Vec<TargetSpanChunk> {
        if spans.is_empty() {
            return vec![TargetSpanChunk {
                target_id: target_id.to_string(),
                chunk_index: 0,
                spans: Vec::new(),
            }];
        }
        spans
            .chunks(SPANS_PER_CHUNK)
            .enumerate()
            .map(|(chunk_index, slice)| TargetSpanChunk {
                target_id: target_id.to_string(),
                chunk_index,
                spans: slice.to_vec(),
            })
            .collect()
    }

    /// Reassemble per-target free-span lists from loaded chunks, ordered by
    /// `chunk_index` so the in-memory span order matches what was written.
    fn assemble_spans_from_chunks(
        mut chunks: Vec<TargetSpanChunk>,
    ) -> HashMap<String, Vec<GranuleSpan>> {
        chunks.sort_by(|a, b| {
            a.target_id
                .cmp(&b.target_id)
                .then(a.chunk_index.cmp(&b.chunk_index))
        });
        let mut by_target: HashMap<String, Vec<GranuleSpan>> = HashMap::new();
        for chunk in chunks {
            by_target
                .entry(chunk.target_id)
                .or_default()
                .extend(chunk.spans);
        }
        by_target
    }

    /// Pre-encoded FDB writes for one target: the (small) target record plus the
    /// chunked free-span values, and the key range whose stale chunks must be
    /// cleared before the new chunks are written. Built outside the transaction
    /// so JSON encoding errors surface early and the retryable closure only does
    /// cheap byte copies.
    #[derive(Clone)]
    struct TargetWrite {
        record_key: Vec<u8>,
        record_value: Vec<u8>,
        // `Some` only when the caller asked to rewrite this target's free spans
        // (`persist_state(rewrite_spans = true)`). When `None` the target's span
        // chunks are left entirely untouched and only the metadata record is set,
        // so a span-neutral control-plane write can never clobber a leader's
        // committed spans (FIX B).
        span_rewrite: Option<TargetSpanRewrite>,
    }

    #[derive(Clone)]
    struct TargetSpanRewrite {
        clear_begin: Vec<u8>,
        clear_end: Vec<u8>,
        chunk_writes: Vec<(Vec<u8>, Vec<u8>)>,
    }

    /// A single FoundationDB mutation. `persist_state` groups these into
    /// byte-bounded batches so no transaction exceeds the ~10 MB FDB limit.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum WriteOp {
        ClearRange(Vec<u8>, Vec<u8>),
        Set(Vec<u8>, Vec<u8>),
        Clear(Vec<u8>),
    }

    #[derive(Clone)]
    struct CandidateTarget {
        target: TargetRecord,
        spans: Vec<GranuleSpan>,
    }

    #[derive(Clone)]
    struct CandidateDomain {
        key: String,
        members: Vec<CandidateTarget>,
    }

    struct PlannedReservationBatch {
        reservations: Vec<PlacementReservationRecord>,
        dirty_targets: HashSet<String>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ReservationMutationKind {
        Finalize,
        Release,
    }

    /// Whether a `persist_state` commit is fenced by the per-shard allocator
    /// mutation epoch (DESIGN_KAS_WRITE_SCALE.md §3/§4 change #3).
    ///
    /// `Leased` is used by every split-brain-sensitive allocator-state mutation
    /// (reserve / claim / finalize / release / expire / refill) — those run under
    /// the per-shard mutation lease, so each of their commits reads the lease
    /// epoch in the SAME FDB txn and aborts if a newer leader has bumped it.
    ///
    /// `Unfenced` preserves today's behavior for control-plane writes
    /// (register_target, heartbeat_target, set_target_state, upsert_service_instance,
    /// reclaim_target_granules, reset) that were never taken under the mutation
    /// lease and are not part of the double-allocated-span hazard the fence guards.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum EpochFence {
        Leased,
        Unfenced,
    }

    #[derive(Debug)]
    struct StatusCarrier(Status);

    impl Display for StatusCarrier {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0.message())
        }
    }

    impl Error for StatusCarrier {}

    pub(crate) struct FdbNetworkGuard {
        _inner: NetworkAutoStop,
    }

    pub(crate) fn maybe_boot_network() -> Result<Option<FdbNetworkGuard>, Box<dyn Error>> {
        let inner = unsafe { foundationdb::boot() };
        Ok(Some(FdbNetworkGuard { _inner: inner }))
    }

    impl FdbKasStore {
        // Lease TTL = 30s, renew at TTL/2 = 15s (DESIGN_KAS_WRITE_SCALE.md §4
        // decision: keep TTL modest so clock skew degrades to "stale writes
        // abort," not corruption; the KMS pool masks the short failover gap a
        // hung-but-not-crashed leader leaves). The per-op model also uses this
        // TTL, so the change is backward compatible for the default path.
        const ALLOCATOR_MUTATION_LEASE_TTL_MS: u64 = 30_000;
        const ALLOCATOR_MUTATION_RETRY_MS: u64 = 10;
        /// Renew the leader-resident lease once its remaining life drops to this
        /// fraction of the TTL. TTL/2 (15s at a 30s TTL) leaves a full renew
        /// interval of slack before expiry even under one missed renewal.
        const ALLOCATOR_MUTATION_RENEW_FRACTION: u64 = 2;
        /// Background renewer cadence for the leader-resident lease. Well below
        /// TTL/2 so several renew attempts fall inside the renew window; a single
        /// failed tick still leaves slack before expiry.
        const ALLOCATOR_MUTATION_RENEW_TICK_MS: u64 = 5_000;

        /// Coordination lease name for this instance's allocation shard. Disjoint
        /// shards derive distinct names so they never contend on a single
        /// cluster-wide lease.
        fn allocator_mutation_lease_name(&self) -> String {
            allocator_mutation_lease_name(self.allocation_shard_id.as_deref())
        }

        /// Allocator state stamp key for this instance's allocation shard. A
        /// mutation in one shard only bumps that shard's stamp, so it cannot
        /// invalidate cached state (including read-only paths) for other shards.
        fn allocator_state_stamp_key(&self) -> Vec<u8> {
            allocator_state_stamp_key(self.allocation_shard_id.as_deref())
        }

        /// Returns the in-process mutation guard for this instance's shard,
        /// creating it on first use. Keyed by shard id so disjoint shards do not
        /// serialize against one another.
        async fn mutation_guard(&self) -> Arc<tokio::sync::Mutex<()>> {
            let shard_key = self
                .allocation_shard_id
                .as_deref()
                .unwrap_or("")
                .to_string();
            let mut guards = self.mutation_guards.lock().await;
            guards.entry(shard_key).or_default().clone()
        }

        pub(crate) fn connect(
            cluster_file: &str,
            allocation_shard_id: Option<String>,
            leader_resident_lease: bool,
            stats: Option<Arc<crate::stats::KasStats>>,
        ) -> Result<Self, Box<dyn Error>> {
            let db = if cluster_file.trim().is_empty() {
                Database::default()?
            } else {
                Database::from_path(cluster_file)?
            };
            Ok(Self {
                db: Arc::new(db),
                state: Arc::new(tokio::sync::Mutex::new(AllocatorState::default())),
                mutation_guards: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
                owner_id: Arc::new(format!("kas-fdb-{}", Uuid::new_v4())),
                allocation_shard_id: Arc::new(
                    allocation_shard_id
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty()),
                ),
                leader_resident_lease,
                leadership: Arc::new(tokio::sync::Mutex::new(None)),
                deferred_bin_member_deletes: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                stats,
            })
        }

        /// Start the leader-resident lease renewer (DESIGN_KAS_WRITE_SCALE.md §3
        /// #2). No-op unless the `leader_resident_lease` flag is set, so the
        /// default build spawns nothing and keeps the per-op model. Must be called
        /// from within a tokio runtime (it is, from `main`). The renewer:
        ///   * acquires the per-shard lease on first tick (becoming leader),
        ///   * renews at TTL/2 while held,
        ///   * steps down (drops in-memory state, stops serving) on any renewal
        ///     failure / observed epoch bump.
        ///
        /// Renewal errors are surfaced into the store (step-down bumps the
        /// `leader_renew_failures` counter and clears in-memory state) AND made
        /// visible here (logged + recorded as last_error) instead of being
        /// silently dropped — the renewer is a detached spawn, so a silent failure
        /// would otherwise be invisible. The next tick re-attempts acquisition and
        /// the epoch fence keeps a superseded leader's writes from committing in
        /// the meantime. The loop never exits on a renew error (only the tokio
        /// runtime shutting down ends it), so the renewer cannot die silently.
        pub(crate) fn start_leader_resident_renewer(&self) {
            if !self.leader_resident_lease {
                return;
            }
            let renewer = self.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(
                    Self::ALLOCATOR_MUTATION_RENEW_TICK_MS,
                ));
                loop {
                    ticker.tick().await;
                    // `renew_leader_resident_lease` (re)acquires when not held and
                    // renews when near expiry; on failure it has already stepped us
                    // down (which bumps `leader_renew_failures`). Log + record the
                    // error so the detached renewer's loss of leadership is visible,
                    // then continue — a renew error must not kill the renewer.
                    if let Err(err) = renewer.renew_leader_resident_lease().await {
                        let message = format!(
                            "KAS leader-resident lease renew failed (stepped down): {err}"
                        );
                        eprintln!("{message}");
                        if let Some(stats) = renewer.stats.as_ref() {
                            stats.set_last_error(message);
                        }
                    }
                }
            });
        }

        async fn refresh_target_state_from_fdb(&self) -> Result<(), Status> {
            let targets = self.load_all_targets().await?;
            let service_instances = self.load_service_instances_from_fdb().await?;
            let allocator_state_stamp = self.load_allocator_state_stamp().await?;
            let mut state = self.state.lock().await;
            state.targets = targets
                .into_iter()
                .map(|target| (target.target.target_id.clone(), target))
                .collect();
            state.service_instances = service_instances
                .into_iter()
                .map(|instance| (instance.instance_id.clone(), instance))
                .collect();
            state.reservations.clear();
            state.bins.clear();
            state.allocator_state_stamp = allocator_state_stamp;
            state.target_state_loaded = true;
            state.reservation_state_loaded = false;
            Ok(())
        }

        async fn refresh_full_state_from_fdb(&self) -> Result<(), Status> {
            let targets = self.load_all_targets().await?;
            let (reservations, delete_reservations) = self.load_active_reservations().await?;
            let service_instances = self.load_service_instances_from_fdb().await?;
            if !delete_reservations.is_empty() {
                // Read-path cleanup of already-non-Reserved (finalized/released)
                // records. Runs from un-leased read paths too, so it must not be
                // epoch-fenced; it is benign (only deletes records no longer
                // Reserved) and not part of the double-allocation hazard.
                self.persist_state(
                    &[],
                    &[],
                    &delete_reservations,
                    &[],
                    &[],
                    &[],
                    // No targets passed -> rewrite_spans is moot; false keeps the
                    // span keyspace untouched.
                    false,
                    EpochFence::Unfenced,
                )
                .await?;
            }
            let allocator_state_stamp = self.load_allocator_state_stamp().await?;
            let mut state = self.state.lock().await;
            state.targets = targets
                .into_iter()
                .map(|target| (target.target.target_id.clone(), target))
                .collect();
            state.reservations = reservations
                .into_iter()
                .map(|reservation| (reservation.reservation_id.clone(), reservation))
                .collect();
            state.service_instances = service_instances
                .into_iter()
                .map(|instance| (instance.instance_id.clone(), instance))
                .collect();
            // Reservation bins are purely in-memory acceleration. Once we refresh from FDB
            // we discard them so replicas cannot keep serving stale reservation ids.
            state.bins.clear();
            state.allocator_state_stamp = allocator_state_stamp;
            state.target_state_loaded = true;
            state.reservation_state_loaded = true;
            Ok(())
        }

        async fn load_service_instances_from_fdb(
            &self,
        ) -> Result<Vec<ServiceInstanceRecord>, Status> {
            self.load_all::<ServiceInstanceRecord>(
                service_instance_prefix(),
                prefix_range_end(&service_instance_prefix()),
            )
            .await
        }

        async fn refresh_service_instances_from_fdb(&self) -> Result<(), Status> {
            let service_instances = self.load_service_instances_from_fdb().await?;
            let mut state = self.state.lock().await;
            state.service_instances = service_instances
                .into_iter()
                .map(|instance| (instance.instance_id.clone(), instance))
                .collect();
            Ok(())
        }

        async fn load_active_reservations(
            &self,
        ) -> Result<(Vec<PlacementReservationRecord>, Vec<String>), Status> {
            let begin = reservation_prefix();
            let end = prefix_range_end(&begin);
            // Paginated read: the reservation set can be many thousands of
            // ~2.5 KB records; scanning the whole prefix in one transaction
            // would charge >10 MB of reads against the FDB transaction budget
            // (FdbError 2101). scan_range_values pages across bounded txns.
            let values = self.scan_range_values(begin, end).await?;
            let mut reservations = Vec::with_capacity(values.len());
            let mut delete_reservations = Vec::new();
            for bytes in values {
                let record = decode_json::<PlacementReservationRecord>(&bytes)?;
                if record.state == ReservationState::Reserved as i32 {
                    reservations.push(record);
                } else {
                    delete_reservations.push(record.reservation_id);
                }
            }
            Ok((reservations, delete_reservations))
        }

        async fn load_allocator_state_stamp(&self) -> Result<Option<String>, Status> {
            let key = self.allocator_state_stamp_key();
            let value = self
                .db
                .run(move |trx, _| {
                    let key = key.clone();
                    async move {
                        let value = trx
                            .get(&key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| bytes.as_ref().to_vec());
                        Ok::<Option<Vec<u8>>, FdbBindingError>(value)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            value
                .map(|bytes| {
                    decode_json::<AllocatorStateStampRecord>(&bytes).map(|record| record.stamp_id)
                })
                .transpose()
        }

        /// Read an entire key range and return the values, paging across
        /// multiple bounded transactions. FoundationDB charges bytes *read*
        /// against the ~10 MB per-transaction limit, so scanning a large prefix
        /// (the full target-span, reservation, or target-record set) under one
        /// read version trips FdbError 2101. Reads in separate transactions do
        /// not share that budget, so each page stays small while the assembled
        /// result is complete. Value order within the range is preserved.
        async fn scan_range_values(
            &self,
            begin: Vec<u8>,
            end: Vec<u8>,
        ) -> Result<Vec<Vec<u8>>, Status> {
            const MAX_ROWS_PER_TXN: usize = 5_000;
            const MAX_BYTES_PER_TXN: usize = 4 * 1024 * 1024;
            let mut values: Vec<Vec<u8>> = Vec::new();
            let mut cursor = begin;
            loop {
                let scan_begin = cursor.clone();
                let scan_end = end.clone();
                let (mut batch, last_key, more) = self
                    .db
                    .run(move |trx, _| {
                        let scan_begin = scan_begin.clone();
                        let scan_end = scan_end.clone();
                        async move {
                            let mut opt = RangeOption::from((scan_begin, scan_end));
                            opt.limit = Some(MAX_ROWS_PER_TXN);
                            let mut stream = trx.get_ranges_keyvalues(opt, false);
                            let mut batch: Vec<Vec<u8>> = Vec::new();
                            let mut last_key: Option<Vec<u8>> = None;
                            let mut bytes = 0usize;
                            let mut more = false;
                            while let Some(next) = stream.next().await {
                                let kv = next?;
                                bytes += kv.key().len() + kv.value().len();
                                last_key = Some(kv.key().to_vec());
                                batch.push(kv.value().to_vec());
                                if batch.len() >= MAX_ROWS_PER_TXN || bytes >= MAX_BYTES_PER_TXN {
                                    more = true;
                                    break;
                                }
                            }
                            Ok::<(Vec<Vec<u8>>, Option<Vec<u8>>, bool), FdbBindingError>((
                                batch, last_key, more,
                            ))
                        }
                    })
                    .await
                    .map_err(map_fdb_binding_error)?;
                values.append(&mut batch);
                match (more, last_key) {
                    // Resume strictly after the last key read (key + 0x00 is its
                    // immediate successor in unbounded byte-string space).
                    (true, Some(mut k)) => {
                        k.push(0x00);
                        cursor = k;
                    }
                    _ => break,
                }
            }
            Ok(values)
        }

        async fn load_all<T>(&self, begin: Vec<u8>, end: Vec<u8>) -> Result<Vec<T>, Status>
        where
            T: DeserializeOwned,
        {
            self.scan_range_values(begin, end)
                .await?
                .into_iter()
                .map(|bytes| decode_json::<T>(&bytes))
                .collect()
        }

        /// Load all targets, reassembling each target's free-span list from its
        /// span chunks. The mirror of `persist_state`'s chunked target writes:
        /// the target metadata lives under `PREFIX_TARGET`, the spans under
        /// `PREFIX_TARGET_SPAN`.
        async fn load_all_targets(&self) -> Result<Vec<TargetAllocatorState>, Status> {
            let records = self
                .load_all::<TargetRecord>(target_prefix(), prefix_range_end(&target_prefix()))
                .await?;
            let chunks = self
                .load_all::<TargetSpanChunk>(
                    target_span_all_prefix(),
                    prefix_range_end(&target_span_all_prefix()),
                )
                .await?;
            let mut spans_by_target = assemble_spans_from_chunks(chunks);
            Ok(records
                .into_iter()
                .map(|target| {
                    let spans = spans_by_target.remove(&target.target_id).unwrap_or_default();
                    TargetAllocatorState { target, spans }
                })
                .collect())
        }

        /// `rewrite_spans` (DESIGN_KAS_WRITE_SCALE.md §3 #2/#3 FIX B). When `true`
        /// each passed target's free-span chunk set is fully rewritten:
        /// `ClearRange(target_span_prefix)` + a blind re-write of `entry.spans`.
        /// That blind global rewrite is the split-brain surface — span keys are
        /// keyed by `target_id` ONLY (not shard-namespaced, see `fdb_schema.rs`),
        /// so ANY writer that passes a target here clobbers that target's spans
        /// for every shard from its (possibly stale) in-memory copy.
        ///
        /// Control-plane metadata writes (register/heartbeat/set-state) only mean
        /// to change the target RECORD, never its spans, so they pass `false`: the
        /// per-target `ClearRange` + chunk writes are skipped entirely and only the
        /// `target_key` record is set. Paths that genuinely mutate spans
        /// (reserve / reclaim / top_up / reset / release) pass `true`.
        async fn persist_state(
            &self,
            targets: &[TargetAllocatorState],
            reservations: &[PlacementReservationRecord],
            delete_reservations: &[String],
            service_instances: &[ServiceInstanceRecord],
            delete_bin_members: &[(String, String)],
            add_bin_members: &[(String, String)],
            rewrite_spans: bool,
            fence: EpochFence,
        ) -> Result<(), Status> {
            // Resolve the epoch to assert in each committed batch. `Leased`
            // mutations (the split-brain-sensitive allocator-state writes) read
            // the epoch held for the current critical section: the per-op path and
            // the leader-resident path both publish the granted epoch into
            // `leadership` while they hold the lease, so this is `Some` for any
            // legitimately-leased commit. If it is `None` we are not (any longer)
            // the leader and MUST refuse to mutate rather than silently commit
            // unfenced — fail closed. `Unfenced` control-plane writes commit
            // without the epoch check, exactly as today.
            let fence_epoch = match fence {
                EpochFence::Leased => Some(self.current_mutation_epoch().await.ok_or_else(|| {
                    Status::aborted(
                        "allocator mutation epoch fence: leadership not held at commit; refusing \
                         to persist allocator state",
                    )
                })?),
                EpochFence::Unfenced => None,
            };
            let targets = targets.to_vec();
            let reservations = reservations.to_vec();
            let delete_reservations = delete_reservations.to_vec();
            let service_instances = service_instances.to_vec();
            let delete_bin_members = delete_bin_members.to_vec();
            let add_bin_members = add_bin_members.to_vec();
            // Pre-encode each target's metadata record and chunked free spans
            // outside the transaction. Spans become many small chunk values
            // (never one value above FDB's 100 KB limit), and a target's stale
            // chunks are cleared before the new ones are written.
            let mut target_writes: Vec<TargetWrite> = Vec::with_capacity(targets.len());
            for target in &targets {
                let id = target.target.target_id.as_str();
                // FIX B: only build the span clear-range + chunk writes when this
                // caller actually changed spans. A span-neutral metadata write
                // (rewrite_spans = false) leaves the target's existing span chunks
                // untouched so it cannot clobber a leader's committed free spans.
                let span_rewrite = if rewrite_spans {
                    let span_prefix = target_span_prefix(id);
                    let span_end = prefix_range_end(&span_prefix);
                    let mut chunk_writes = Vec::new();
                    for chunk in split_spans_into_chunks(id, &target.spans) {
                        chunk_writes.push((
                            target_span_chunk_key(id, chunk.chunk_index),
                            encode_json(&chunk)?,
                        ));
                    }
                    Some(TargetSpanRewrite {
                        clear_begin: span_prefix,
                        clear_end: span_end,
                        chunk_writes,
                    })
                } else {
                    None
                };
                target_writes.push(TargetWrite {
                    record_key: target_key(id),
                    record_value: encode_json(&target.target)?,
                    span_rewrite,
                });
            }
            let update_allocator_state = !targets.is_empty()
                || !reservations.is_empty()
                || !delete_reservations.is_empty()
                || !delete_bin_members.is_empty()
                || !add_bin_members.is_empty();
            let allocator_state_stamp = update_allocator_state.then(|| AllocatorStateStampRecord {
                stamp_id: Uuid::new_v4().to_string(),
                updated_at_unix_ms: now_unix_ms(),
                owner_id: self.owner_id.as_ref().clone(),
            });
            let allocator_state_stamp_key = self.allocator_state_stamp_key();

            // Build write units. Each target's (clear-range + record + span
            // chunks) is one indivisible unit; every other write is a single
            // op. Units are flushed in byte-bounded batches so a fleet of
            // fragmented targets or a large reservation batch can never build a
            // transaction above FDB's ~10 MB limit (FdbError 2101). The stamp is
            // the final unit so it commits in the last batch. Cross-batch
            // atomicity is not required in POC state (FIRST_PRINCIPLES #17): a
            // crash mid-persist is recoverable by reset/reformat.
            let mut units: Vec<(Vec<WriteOp>, usize)> = Vec::new();
            for tw in target_writes {
                units.push(build_target_write_unit(tw));
            }
            for reservation in &reservations {
                let key = reservation_key(&reservation.reservation_id);
                let value = encode_json(reservation)?;
                let bytes = key.len() + value.len();
                units.push((vec![WriteOp::Set(key, value)], bytes));
            }
            for reservation_id in &delete_reservations {
                let key = reservation_key(reservation_id);
                let bytes = key.len();
                units.push((vec![WriteOp::Clear(key)], bytes));
            }
            for instance in &service_instances {
                let key = service_instance_key(&instance.instance_id);
                let value = encode_json(instance)?;
                let bytes = key.len() + value.len();
                units.push((vec![WriteOp::Set(key, value)], bytes));
            }
            for (bin_key, reservation_id) in &delete_bin_members {
                let key = reservation_bin_member_key(bin_key, reservation_id);
                let bytes = key.len();
                units.push((vec![WriteOp::Clear(key)], bytes));
            }
            for (bin_key, reservation_id) in &add_bin_members {
                let key = reservation_bin_member_key(bin_key, reservation_id);
                let value = reservation_id.clone().into_bytes();
                let bytes = key.len() + value.len();
                units.push((vec![WriteOp::Set(key, value)], bytes));
            }
            if let Some(stamp) = &allocator_state_stamp {
                let value = encode_json(stamp)?;
                units.push((vec![WriteOp::Set(allocator_state_stamp_key, value)], 0));
            }

            const MAX_TXN_BYTES: usize = 4 * 1024 * 1024;
            let mut batch: Vec<WriteOp> = Vec::new();
            let mut batch_bytes = 0usize;
            for (ops, bytes) in units {
                if !batch.is_empty() && batch_bytes.saturating_add(bytes) > MAX_TXN_BYTES {
                    self.commit_write_ops(std::mem::take(&mut batch), fence_epoch)
                        .await?;
                    batch_bytes = 0;
                }
                batch.extend(ops);
                batch_bytes = batch_bytes.saturating_add(bytes);
            }
            if !batch.is_empty() {
                self.commit_write_ops(batch, fence_epoch).await?;
            }

            if let Some(stamp) = allocator_state_stamp {
                let mut state = self.state.lock().await;
                state.allocator_state_stamp = Some(stamp.stamp_id);
                state.target_state_loaded = true;
            }
            Ok(())
        }

        /// Apply a batch of mutations in one FoundationDB transaction. Callers
        /// (currently `persist_state`) size batches under the ~10 MB limit.
        ///
        /// `fence_epoch`, when `Some`, is the per-shard allocator mutation epoch
        /// this writer holds. The transaction then reads the mutation lease key
        /// and aborts (without writing anything) if the stored epoch differs —
        /// the epoch fence (DESIGN_KAS_WRITE_SCALE.md §3/§4). Reading the lease
        /// key inside the SAME txn turns it into an FDB conflict key: a superseded
        /// leader's commit fails deterministically under serializable isolation
        /// rather than corrupting state. We deliberately read only this one
        /// already-hot key (smallest possible conflict footprint, §8 open
        /// question 3) instead of a separate epoch key.
        ///
        /// When `None` the commit is unfenced (control-plane writes that never
        /// took the mutation lease), preserving today's behavior.
        async fn commit_write_ops(
            &self,
            ops: Vec<WriteOp>,
            fence_epoch: Option<u64>,
        ) -> Result<(), Status> {
            let fence_key = fence_epoch.map(|_| {
                coordination_lease_key(&self.allocator_mutation_lease_name())
            });
            self.db
                .run(move |trx, _| {
                    let ops = ops.clone();
                    let fence_key = fence_key.clone();
                    async move {
                        // Epoch fence: read the lease epoch in this txn and abort
                        // if we are no longer the epoch holder. The read makes the
                        // lease key a conflict key, so a concurrent new leader's
                        // bump deterministically fails this commit.
                        if let (Some(expected), Some(key)) = (fence_epoch, fence_key.as_ref()) {
                            let current = trx
                                .get(key, false)
                                .await
                                .map_err(FdbBindingError::from)?
                                .map(|bytes| bytes.as_ref().to_vec());
                            let actual = match current {
                                Some(bytes) => {
                                    decode_json::<CoordinationLeaseRecord>(&bytes)
                                        .map_err(status_to_fdb)?
                                        .epoch
                                }
                                // Lease record gone (cleared/expired-and-reaped):
                                // treat as epoch 0, which never matches a held
                                // epoch >= 1, so the commit aborts.
                                None => 0,
                            };
                            if actual != expected {
                                return Err(status_to_fdb(Status::aborted(format!(
                                    "allocator mutation epoch fence: held {expected}, found \
                                     {actual}; a newer leader has taken the shard"
                                ))));
                            }
                        }
                        for op in &ops {
                            match op {
                                WriteOp::ClearRange(begin, end) => trx.clear_range(begin, end),
                                WriteOp::Set(key, value) => trx.set(key, value),
                                WriteOp::Clear(key) => trx.clear(key),
                            }
                        }
                        Ok::<(), FdbBindingError>(())
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
                .inspect_err(|err| {
                    // Surface a fenced-commit abort into the stats tree so failover
                    // fault-injection can confirm a superseded leader's write was
                    // rejected (not committed). Only the epoch fence raises an
                    // Aborted from this txn, so the code is a precise signal.
                    if fence_epoch.is_some() && err.code() == tonic::Code::Aborted {
                        if let Some(stats) = self.stats.as_ref() {
                            stats.record_fenced_commit_abort();
                        }
                    }
                })
        }

        /// Phase-3 lock-light claim persist (change #7): write ONLY the claimed
        /// reservations' (TTL-bumped) records, fenced by the held epoch, WITHOUT
        /// bumping the allocator-state stamp. Reaping correctness needs the TTL
        /// bump durable; the stamp is intentionally not touched (the sole fenced
        /// writer no longer relies on it — the epoch fence is the safety net).
        /// Reuses the byte-bounded batching of `commit_write_ops` so a large claim
        /// can never build an over-limit transaction.
        async fn persist_claim_ttl_bumps_lock_light(
            &self,
            claimed: &[PlacementReservationRecord],
        ) -> Result<(), Status> {
            // Fail closed: a lock-light claim must hold leadership, else its fenced
            // commit would have nothing to assert. (The wrapper already ensured it.)
            let fence_epoch = Some(self.current_mutation_epoch().await.ok_or_else(|| {
                Status::aborted(
                    "lock-light claim: leadership not held at commit; refusing to persist",
                )
            })?);
            const MAX_TXN_BYTES: usize = 4 * 1024 * 1024;
            let mut batch: Vec<WriteOp> = Vec::new();
            let mut batch_bytes = 0usize;
            for reservation in claimed {
                let key = reservation_key(&reservation.reservation_id);
                let value = encode_json(reservation)?;
                let bytes = key.len() + value.len();
                if !batch.is_empty() && batch_bytes.saturating_add(bytes) > MAX_TXN_BYTES {
                    self.commit_write_ops(std::mem::take(&mut batch), fence_epoch)
                        .await?;
                    batch_bytes = 0;
                }
                batch.push(WriteOp::Set(key, value));
                batch_bytes = batch_bytes.saturating_add(bytes);
            }
            if !batch.is_empty() {
                self.commit_write_ops(batch, fence_epoch).await?;
            }
            Ok(())
        }

        /// Flush deferred durable bin-member deletes accumulated by lock-light
        /// claims (change #7). Best-effort: these entries are write-only and never
        /// read back, so on failure they are simply left for a later flush / a
        /// `reset`'s prefix clear. Fenced like any other leased write.
        async fn flush_deferred_bin_member_deletes(&self) -> Result<(), Status> {
            let pending = {
                let mut deferred = self.deferred_bin_member_deletes.lock().await;
                if deferred.is_empty() {
                    return Ok(());
                }
                std::mem::take(&mut *deferred)
            };
            // Only the leader may emit fenced deletes. If we are not leader, drop
            // them: a new leader's refresh discards `state.bins` anyway, so the
            // orphaned durable members are harmless until the next `reset`.
            let Some(fence_epoch) = self.current_mutation_epoch().await else {
                return Ok(());
            };
            const MAX_TXN_BYTES: usize = 4 * 1024 * 1024;
            let mut batch: Vec<WriteOp> = Vec::new();
            let mut batch_bytes = 0usize;
            for (bin_key, reservation_id) in &pending {
                let key = reservation_bin_member_key(bin_key, reservation_id);
                let bytes = key.len();
                if !batch.is_empty() && batch_bytes.saturating_add(bytes) > MAX_TXN_BYTES {
                    self.commit_write_ops(std::mem::take(&mut batch), Some(fence_epoch))
                        .await?;
                    batch_bytes = 0;
                }
                batch.push(WriteOp::Clear(key));
                batch_bytes = batch_bytes.saturating_add(bytes);
            }
            if !batch.is_empty() {
                self.commit_write_ops(batch, Some(fence_epoch)).await?;
            }
            Ok(())
        }

        async fn clear_all_reservations_and_bins(&self) -> Result<(), Status> {
            let reservation_begin = reservation_prefix();
            let reservation_end = prefix_range_end(&reservation_begin);
            let bin_begin = reservation_bin_prefix("");
            let bin_end = prefix_range_end(&bin_begin);
            let allocator_state_stamp = AllocatorStateStampRecord {
                stamp_id: Uuid::new_v4().to_string(),
                updated_at_unix_ms: now_unix_ms(),
                owner_id: self.owner_id.as_ref().clone(),
            };
            let persisted_allocator_state_stamp = allocator_state_stamp.clone();
            let allocator_state_stamp_key = self.allocator_state_stamp_key();
            self.db
                .run(move |trx, _| {
                    let reservation_begin = reservation_begin.clone();
                    let reservation_end = reservation_end.clone();
                    let bin_begin = bin_begin.clone();
                    let bin_end = bin_end.clone();
                    let allocator_state_stamp = persisted_allocator_state_stamp.clone();
                    let allocator_state_stamp_key = allocator_state_stamp_key.clone();
                    async move {
                        trx.clear_range(&reservation_begin, &reservation_end);
                        trx.clear_range(&bin_begin, &bin_end);
                        trx.set(
                            &allocator_state_stamp_key,
                            &encode_json(&allocator_state_stamp).map_err(status_to_fdb)?,
                        );
                        Ok(())
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            let mut state = self.state.lock().await;
            state.allocator_state_stamp = Some(allocator_state_stamp.stamp_id);
            state.target_state_loaded = true;
            state.reservation_state_loaded = true;
            Ok(())
        }

        async fn ensure_target_state_current(&self) -> Result<(), Status> {
            self.ensure_target_state_current_inner(false).await
        }

        /// `skip_stamp_read` implements change #4: while the leader holds the
        /// per-shard lease, the per-op `load_allocator_state_stamp` GET is
        /// dropped. It is safe ONLY because the epoch fence (#3) now detects an
        /// out-of-band writer at commit time instead. A reload is still forced on
        /// (re)acquisition / epoch change, which `acquire_leader_resident_lease`
        /// signals by clearing `target_state_loaded`, so the `needs_refresh`
        /// branch below still picks it up.
        async fn ensure_target_state_current_inner(
            &self,
            skip_stamp_read: bool,
        ) -> Result<(), Status> {
            let needs_refresh = {
                let state = self.state.lock().await;
                !state.target_state_loaded
            };
            if needs_refresh {
                return self.refresh_target_state_from_fdb().await;
            }
            if skip_stamp_read {
                return Ok(());
            }
            let remote_stamp = self.load_allocator_state_stamp().await?;
            let local_stamp = {
                let state = self.state.lock().await;
                state.allocator_state_stamp.clone()
            };
            if remote_stamp != local_stamp {
                self.refresh_target_state_from_fdb().await?;
            }
            Ok(())
        }

        async fn ensure_full_state_current(&self) -> Result<(), Status> {
            self.ensure_full_state_current_inner(false).await
        }

        /// See `ensure_target_state_current_inner`: `skip_stamp_read` drops the
        /// per-op stamp GET under a held leader-resident lease (change #4).
        async fn ensure_full_state_current_inner(
            &self,
            skip_stamp_read: bool,
        ) -> Result<(), Status> {
            let needs_refresh = {
                let state = self.state.lock().await;
                !state.target_state_loaded || !state.reservation_state_loaded
            };
            if needs_refresh {
                return self.refresh_full_state_from_fdb().await;
            }
            if skip_stamp_read {
                return Ok(());
            }
            let remote_stamp = self.load_allocator_state_stamp().await?;
            let local_stamp = {
                let state = self.state.lock().await;
                state.allocator_state_stamp.clone()
            };
            if remote_stamp != local_stamp {
                self.refresh_full_state_from_fdb().await?;
            }
            Ok(())
        }

        /// Per-op (default-path) acquire. Loops until it holds the per-shard
        /// mutation lease, then publishes the granted epoch + expiry into
        /// `leadership` so the always-on epoch fence in `persist_state(Leased)`
        /// can assert it. The matching `release_allocator_mutation_lease` clears
        /// `leadership` and the durable record, restoring the historical
        /// sub-millisecond hold window.
        async fn acquire_allocator_mutation_lease(&self) -> Result<(), Status> {
            let lease_name = self.allocator_mutation_lease_name();
            loop {
                // The epoch-aware acquirer already self-renews (same owner, live)
                // vs new-grant (absent/expired) and returns the epoch we now hold.
                if let Some(epoch) = self
                    .try_acquire_coordination_lease_epoch(
                        &lease_name,
                        self.owner_id.as_str(),
                        Self::ALLOCATOR_MUTATION_LEASE_TTL_MS,
                    )
                    .await?
                {
                    let expires_at_unix_ms =
                        now_unix_ms().saturating_add(Self::ALLOCATOR_MUTATION_LEASE_TTL_MS);
                    let mut leadership = self.leadership.lock().await;
                    *leadership = Some(LeadershipTerm {
                        epoch,
                        lease_expires_at_unix_ms: expires_at_unix_ms,
                    });
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(Self::ALLOCATOR_MUTATION_RETRY_MS)).await;
            }
        }

        async fn release_allocator_mutation_lease(&self) -> Result<(), Status> {
            {
                let mut leadership = self.leadership.lock().await;
                *leadership = None;
            }
            self.release_coordination_lease(
                &self.allocator_mutation_lease_name(),
                self.owner_id.as_str(),
            )
            .await
        }

        async fn release_coordination_lease(
            &self,
            lease_name: &str,
            owner_id: &str,
        ) -> Result<(), Status> {
            let key = coordination_lease_key(lease_name);
            let owner_id = owner_id.to_string();
            self.db
                .run(move |trx, _| {
                    let key = key.clone();
                    let owner_id = owner_id.clone();
                    async move {
                        let value = trx
                            .get(&key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| bytes.as_ref().to_vec());
                        let Some(bytes) = value else {
                            return Ok::<(), FdbBindingError>(());
                        };
                        let lease = decode_json::<CoordinationLeaseRecord>(&bytes)
                            .map_err(status_to_fdb)?;
                        if lease.owner_id == owner_id {
                            trx.clear(&key);
                        }
                        Ok::<(), FdbBindingError>(())
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            Ok(())
        }

        // ---- Epoch fence + leader-resident lease (DESIGN_KAS_WRITE_SCALE.md §3/§4) ----

        /// Acquire (or self-renew) a coordination lease and return the epoch the
        /// caller now holds, or `None` if the lease is held by a live other owner.
        ///
        /// The epoch is the fencing token for change #3. In one serializable FDB
        /// transaction we read the current record and decide availability exactly
        /// as before (`owner_id == us || expired`). The epoch is then:
        ///   * carried unchanged on a **self-renew** (same owner, still live) — a
        ///     renewal must never advance the fence or it would invalidate our own
        ///     in-flight writes;
        ///   * **bumped** (`existing.epoch + 1`) on every *new* grant — an
        ///     absent/expired lease, or one stolen from a different (dead) owner.
        ///     A fresh record starts at epoch 1 (0 is reserved for "never granted"
        ///     / legacy records that decode with `#[serde(default)]`).
        ///
        /// Because FDB linearizes the read+set on the lease key, two racing new
        /// acquisitions cannot both win the same epoch: one commits, the other
        /// conflicts and retries against the now-advanced record. That is what
        /// makes pure lease-race leadership safe (§8 open question 4).
        async fn try_acquire_coordination_lease_epoch(
            &self,
            lease_name: &str,
            owner_id: &str,
            lease_ttl_ms: u64,
        ) -> Result<Option<u64>, Status> {
            let now_ms = now_unix_ms();
            let expires_at_unix_ms = now_ms.saturating_add(lease_ttl_ms.max(1_000));
            let key = coordination_lease_key(lease_name);
            let owner_id = owner_id.to_string();
            self.db
                .run(move |trx, _| {
                    let key = key.clone();
                    let owner_id = owner_id.clone();
                    async move {
                        let current = trx
                            .get(&key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| bytes.as_ref().to_vec());
                        let existing = match current {
                            Some(bytes) => Some(
                                decode_json::<CoordinationLeaseRecord>(&bytes)
                                    .map_err(status_to_fdb)?,
                            ),
                            None => None,
                        };
                        let (available, epoch) = decide_lease_grant(
                            existing.as_ref().map(|record| {
                                (record.owner_id.as_str(), record.expires_at_unix_ms, record.epoch)
                            }),
                            &owner_id,
                            now_ms,
                        );
                        if available {
                            let record = CoordinationLeaseRecord {
                                owner_id: owner_id.clone(),
                                expires_at_unix_ms,
                                epoch,
                            };
                            let value = encode_json(&record).map_err(status_to_fdb)?;
                            trx.set(&key, &value);
                        }
                        Ok::<Option<u64>, FdbBindingError>(available.then_some(epoch))
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        /// The epoch this instance must assert in fenced commits, or `None` if it
        /// is not currently a leader under the leader-resident model.
        ///
        /// On the leader-resident path this is the held term's epoch. On the
        /// default (per-op) path the per-op acquire records the freshly granted
        /// epoch into `leadership` for the duration of the critical section, so a
        /// `Leased` commit reads the right value here too.
        async fn current_mutation_epoch(&self) -> Option<u64> {
            self.leadership.lock().await.map(|term| term.epoch)
        }

        /// Become (or refresh) the shard leader under the leader-resident lease
        /// model: acquire the per-shard mutation lease, record the granted epoch
        /// and expiry in `leadership`. Returns `Ok(true)` if we hold leadership
        /// afterward, `Ok(false)` if another live owner holds the lease (we are
        /// not leader and must not mutate).
        ///
        /// If the granted epoch differs from a previously-held one we drop our
        /// in-memory allocator state and force a refresh on the next op, because
        /// an epoch change means a different leader may have mutated the shard
        /// while we were not leader.
        async fn acquire_leader_resident_lease(&self) -> Result<bool, Status> {
            let lease_name = self.allocator_mutation_lease_name();
            let granted = self
                .try_acquire_coordination_lease_epoch(
                    &lease_name,
                    self.owner_id.as_str(),
                    Self::ALLOCATOR_MUTATION_LEASE_TTL_MS,
                )
                .await?;
            let Some(epoch) = granted else {
                // Lost the lease to a live other owner: step down hard.
                self.step_down_leader_resident().await;
                return Ok(false);
            };
            let expires_at_unix_ms =
                now_unix_ms().saturating_add(Self::ALLOCATOR_MUTATION_LEASE_TTL_MS);
            let epoch_changed = {
                let mut leadership = self.leadership.lock().await;
                let changed = leadership.map(|term| term.epoch) != Some(epoch);
                *leadership = Some(LeadershipTerm {
                    epoch,
                    lease_expires_at_unix_ms: expires_at_unix_ms,
                });
                changed
            };
            if epoch_changed {
                // A new term (or first acquisition): our cached allocator state
                // may be stale relative to whatever the previous leader wrote.
                // Mark it for reload; the fence guards against acting on stale
                // spans in the meantime.
                let mut state = self.state.lock().await;
                state.target_state_loaded = false;
                state.reservation_state_loaded = false;
            }
            Ok(true)
        }

        /// Renew the held leader-resident lease if it is near expiry. A renewal
        /// never advances the epoch. On any renewal failure or observed loss of
        /// ownership we step down (drop in-memory state, stop serving) rather than
        /// blindly retry — renewal failure == immediate loss of leadership (§4).
        async fn renew_leader_resident_lease(&self) -> Result<(), Status> {
            let held = *self.leadership.lock().await;
            let Some(term) = held else {
                // Not currently leader: (re)attempt acquisition.
                let _ = self.acquire_leader_resident_lease().await?;
                return Ok(());
            };
            let now_ms = now_unix_ms();
            let renew_before_ms =
                Self::ALLOCATOR_MUTATION_LEASE_TTL_MS / Self::ALLOCATOR_MUTATION_RENEW_FRACTION;
            if term.lease_expires_at_unix_ms.saturating_sub(now_ms) > renew_before_ms {
                return Ok(());
            }
            let lease_name = self.allocator_mutation_lease_name();
            match self
                .try_acquire_coordination_lease_epoch(
                    &lease_name,
                    self.owner_id.as_str(),
                    Self::ALLOCATOR_MUTATION_LEASE_TTL_MS,
                )
                .await
            {
                Ok(Some(epoch)) if epoch == term.epoch => {
                    // Clean self-renew: epoch unchanged, push out expiry.
                    let mut leadership = self.leadership.lock().await;
                    if let Some(current) = leadership.as_mut() {
                        if current.epoch == term.epoch {
                            current.lease_expires_at_unix_ms = now_unix_ms()
                                .saturating_add(Self::ALLOCATOR_MUTATION_LEASE_TTL_MS);
                        }
                    }
                    Ok(())
                }
                Ok(Some(_epoch)) => {
                    // We re-acquired but the epoch advanced under us: someone else
                    // held the shard in between. Step down — our cached state and
                    // any in-flight assumptions are no longer valid.
                    self.step_down_leader_resident().await;
                    Err(Status::aborted(
                        "allocator mutation lease epoch advanced during renewal; stepped down",
                    ))
                }
                Ok(None) => {
                    self.step_down_leader_resident().await;
                    Err(Status::aborted(
                        "allocator mutation lease lost to another owner; stepped down",
                    ))
                }
                Err(err) => {
                    // Renewal RPC failed: we can no longer prove leadership. Drop
                    // it; the fence would abort our writes anyway once the lease
                    // expires and another leader bumps the epoch.
                    self.step_down_leader_resident().await;
                    Err(err)
                }
            }
        }

        /// Drop leadership: clear the held term and mark in-memory allocator state
        /// stale so the next (re)acquisition reloads from FDB. Does NOT clear the
        /// durable lease record — a partitioned ex-leader can no longer reach FDB
        /// to release, and the epoch fence makes an unreleased-but-superseded
        /// lease harmless (its writes abort).
        async fn step_down_leader_resident(&self) {
            let was_leader = {
                let mut leadership = self.leadership.lock().await;
                leadership.take().is_some()
            };
            // Count only a genuine loss of held leadership (renew failure / lost
            // lease / epoch advanced under us), not a cold "never had it" tick, so
            // the counter measures real step-downs the detached renewer would
            // otherwise hide.
            if was_leader {
                if let Some(stats) = self.stats.as_ref() {
                    stats.record_leader_renew_failure();
                }
            }
            let mut state = self.state.lock().await;
            state.target_state_loaded = false;
            state.reservation_state_loaded = false;
        }

        async fn reserve_batch_common_locked(
            &self,
            batch_size: usize,
            fragment_count: usize,
            failure_domain: FailureDomain,
            excluded_target_ids: Vec<String>,
            banned_target_id: Option<String>,
            reservation_ttl_ms: u64,
            required_target_ids: Vec<String>,
            explicit_reservation_id: Option<String>,
        ) -> Result<TimedStoreResult<Vec<PlacementReservationRecord>>, Status> {
            if batch_size == 0 {
                return Ok(TimedStoreResult {
                    value: Vec::new(),
                    phase_timings: Vec::new(),
                });
            }
            if fragment_count == 0 {
                return Err(Status::invalid_argument(
                    "fragment_count must be greater than zero",
                ));
            }

            let started = Instant::now();
            let excluded_target_ids = excluded_target_ids.into_iter().collect::<HashSet<_>>();
            let required_target_ids = required_target_ids.into_iter().collect::<HashSet<_>>();
            let expires_at_unix_ms = if reservation_ttl_ms == 0 {
                0
            } else {
                now_unix_ms().saturating_add(reservation_ttl_ms)
            };
            let (dirty_targets, reservations) = {
                let mut state = self.state.lock().await;
                let mut domains = candidate_domains_from_state(
                    &state.targets,
                    failure_domain,
                    &excluded_target_ids,
                    banned_target_id.as_deref(),
                    &required_target_ids,
                    self.allocation_shard_id.as_deref(),
                )?;
                let planned = plan_reservation_batch(
                    &mut domains,
                    batch_size,
                    fragment_count,
                    explicit_reservation_id.as_deref(),
                    self.allocation_shard_id.as_deref(),
                    expires_at_unix_ms,
                )?;

                let mut dirty_targets = Vec::new();
                for target_id in &planned.dirty_targets {
                    let Some(domain_member) = domains
                        .iter()
                        .flat_map(|domain| domain.members.iter())
                        .find(|candidate| candidate.target.target_id == *target_id)
                    else {
                        continue;
                    };
                    if let Some(entry) = state.targets.get_mut(target_id) {
                        entry.target.free_granules = domain_member.target.free_granules;
                        entry.spans = domain_member.spans.clone();
                        dirty_targets.push(entry.clone());
                    }
                }
                for reservation in &planned.reservations {
                    state
                        .reservations
                        .insert(reservation.reservation_id.clone(), reservation.clone());
                }
                (dirty_targets, planned.reservations)
            };
            let persist_started = Instant::now();
            self.persist_state(
                &dirty_targets,
                &reservations,
                &[],
                &[],
                &[],
                &[],
                // Reserve consumes free spans from the dirty targets -> rewrite.
                true,
                EpochFence::Leased,
            )
            .await?;
            Ok(TimedStoreResult {
                value: reservations,
                phase_timings: vec![
                    StorePhaseTiming {
                        name: "plan_in_memory",
                        elapsed: persist_started.saturating_duration_since(started),
                    },
                    StorePhaseTiming {
                        name: "persist_fdb",
                        elapsed: Instant::now().saturating_duration_since(persist_started),
                    },
                ],
            })
        }

        fn claim_ready_bin_records(
            state: &mut AllocatorState,
            bin_key: &str,
            batch_size: usize,
            reservation_ttl_ms: u64,
        ) -> (Vec<PlacementReservationRecord>, Vec<(String, String)>) {
            let now_ms = now_unix_ms();
            let expires_at = if reservation_ttl_ms == 0 {
                0
            } else {
                now_ms.saturating_add(reservation_ttl_ms)
            };
            let mut claimed = Vec::new();
            let mut deleted = Vec::new();
            let Some(queue) = state.bins.get_mut(bin_key) else {
                return (claimed, deleted);
            };
            while claimed.len() < batch_size {
                let Some(reservation_id) = queue.pop_front() else {
                    break;
                };
                let Some(record) = state.reservations.get_mut(&reservation_id) else {
                    deleted.push((bin_key.to_string(), reservation_id));
                    continue;
                };
                if record.state != ReservationState::Reserved as i32 {
                    deleted.push((bin_key.to_string(), reservation_id));
                    continue;
                }
                if record.expires_at_unix_ms > 0 && record.expires_at_unix_ms <= now_ms {
                    deleted.push((bin_key.to_string(), reservation_id));
                    continue;
                }
                if expires_at > 0 {
                    record.expires_at_unix_ms = expires_at;
                }
                claimed.push(record.clone());
                deleted.push((bin_key.to_string(), reservation_id));
            }
            if queue.is_empty() {
                state.bins.remove(bin_key);
            }
            (claimed, deleted)
        }

        async fn try_finalize_reservations_metadata_only(
            &self,
            mutations: Vec<ReservationMutationSpec>,
        ) -> Result<Option<Vec<PlacementReservationRecord>>, Status> {
            if mutations.is_empty() {
                return Ok(Some(Vec::new()));
            }
            let (results, delete_reservations, delete_bin_members) = {
                let mut state = self.state.lock().await;
                let mut unique = HashSet::new();
                let mut results = Vec::with_capacity(mutations.len());
                let mut delete_reservations = Vec::new();
                let mut delete_bin_members = Vec::new();

                for mutation in &mutations {
                    if !unique.insert(mutation.reservation_id.clone()) {
                        return Err(Status::invalid_argument(format!(
                            "duplicate reservation mutation for {}",
                            mutation.reservation_id
                        )));
                    }
                    let Some(record) = state.reservations.get(&mutation.reservation_id).cloned()
                    else {
                        continue;
                    };
                    if record.state != ReservationState::Reserved as i32 {
                        continue;
                    }
                    let keep = normalize_subset_indexes(
                        record.placements.len(),
                        &mutation.placement_indexes,
                    )?;
                    if keep.len() != record.placements.len() {
                        return Ok(None);
                    }
                }

                for mutation in mutations {
                    let Some(mut record) =
                        state.reservations.get(&mutation.reservation_id).cloned()
                    else {
                        results.push(synthetic_reservation_result(
                            &mutation.reservation_id,
                            ReservationState::Finalized,
                        ));
                        continue;
                    };
                    if record.state == ReservationState::Reserved as i32 {
                        record.state = ReservationState::Finalized as i32;
                        remove_reservation_from_bins(
                            &mut state.bins,
                            &record.reservation_id,
                            &mut delete_bin_members,
                        );
                        state.reservations.remove(&record.reservation_id);
                        delete_reservations.push(record.reservation_id.clone());
                    }
                    results.push(record);
                }
                (results, delete_reservations, delete_bin_members)
            };
            self.persist_state(
                &[],
                &[],
                &delete_reservations,
                &[],
                &delete_bin_members,
                &[],
                // Metadata-only finalize: no targets, no span change.
                false,
                EpochFence::Leased,
            )
            .await?;
            Ok(Some(results))
        }

        async fn apply_reservation_mutations_locked(
            &self,
            mutations: Vec<ReservationMutationSpec>,
            kind: ReservationMutationKind,
        ) -> Result<Vec<PlacementReservationRecord>, Status> {
            if mutations.is_empty() {
                return Ok(Vec::new());
            }
            let mut unique = HashSet::new();
            for mutation in &mutations {
                if !unique.insert(mutation.reservation_id.clone()) {
                    return Err(Status::invalid_argument(format!(
                        "duplicate reservation mutation for {}",
                        mutation.reservation_id
                    )));
                }
            }

            let (
                results,
                dirty_targets,
                dirty_reservations,
                delete_reservations,
                delete_bin_members,
            ) = {
                let mut state = self.state.lock().await;
                let mut results = Vec::with_capacity(mutations.len());
                let mut dirty_targets = HashSet::new();
                let mut dirty_reservations = Vec::new();
                let mut delete_reservations = Vec::new();
                let mut delete_bin_members = Vec::new();

                for mutation in mutations {
                    let Some(mut record) =
                        state.reservations.get(&mutation.reservation_id).cloned()
                    else {
                        let state = match kind {
                            ReservationMutationKind::Finalize => ReservationState::Finalized,
                            ReservationMutationKind::Release => ReservationState::Released,
                        };
                        results.push(synthetic_reservation_result(
                            &mutation.reservation_id,
                            state,
                        ));
                        continue;
                    };

                    match kind {
                        ReservationMutationKind::Finalize => {
                            if record.state == ReservationState::Finalized as i32
                                || record.state == ReservationState::Released as i32
                            {
                                results.push(record);
                                continue;
                            }
                            if record.state != ReservationState::Reserved as i32 {
                                results.push(record);
                                continue;
                            }
                            let keep = normalize_subset_indexes(
                                record.placements.len(),
                                &mutation.placement_indexes,
                            )?;
                            record.placements = keep
                                .iter()
                                .map(|index| record.placements[*index].clone())
                                .collect();
                            record.state = ReservationState::Finalized as i32;
                        }
                        ReservationMutationKind::Release => {
                            if record.state == ReservationState::Released as i32
                                || record.state == ReservationState::Finalized as i32
                            {
                                results.push(record);
                                continue;
                            }
                            if record.state != ReservationState::Reserved as i32 {
                                results.push(record);
                                continue;
                            }
                            let release = normalize_subset_indexes(
                                record.placements.len(),
                                &mutation.placement_indexes,
                            )?;
                            let full_release = release.len() == record.placements.len();
                            release_record_placements(
                                &mut state,
                                &record,
                                &release,
                                &mut dirty_targets,
                            )?;
                            if full_release {
                                record.placements.clear();
                                record.state = ReservationState::Released as i32;
                            } else {
                                let released = release.into_iter().collect::<HashSet<_>>();
                                record.placements = record
                                    .placements
                                    .iter()
                                    .enumerate()
                                    .filter_map(|(index, placement)| {
                                        (!released.contains(&index)).then_some(placement.clone())
                                    })
                                    .collect();
                            }
                        }
                    }

                    remove_reservation_from_bins(
                        &mut state.bins,
                        &record.reservation_id,
                        &mut delete_bin_members,
                    );
                    if kind == ReservationMutationKind::Finalize
                        || (kind == ReservationMutationKind::Release
                            && record.state == ReservationState::Released as i32)
                    {
                        state.reservations.remove(&record.reservation_id);
                        delete_reservations.push(record.reservation_id.clone());
                    } else {
                        state
                            .reservations
                            .insert(record.reservation_id.clone(), record.clone());
                        dirty_reservations.push(record.clone());
                    }
                    results.push(record);
                }

                let dirty_targets = dirty_targets
                    .into_iter()
                    .filter_map(|target_id| state.targets.get(&target_id).cloned())
                    .collect::<Vec<_>>();
                (
                    results,
                    dirty_targets,
                    dirty_reservations,
                    delete_reservations,
                    delete_bin_members,
                )
            };
            self.persist_state(
                &dirty_targets,
                &dirty_reservations,
                &delete_reservations,
                &[],
                &delete_bin_members,
                &[],
                // Release returns spans to the dirty targets -> rewrite.
                true,
                EpochFence::Leased,
            )
            .await?;
            Ok(results)
        }

        async fn with_allocator_mutation_lease<T, F>(&self, op: F) -> Result<T, Status>
        where
            F: std::future::Future<Output = Result<T, Status>>,
        {
            self.with_allocator_mutation_lease_refreshed(false, op)
                .await
        }

        async fn with_allocator_mutation_lease_full_state<T, F>(&self, op: F) -> Result<T, Status>
        where
            F: std::future::Future<Output = Result<T, Status>>,
        {
            self.with_allocator_mutation_lease_refreshed(true, op).await
        }

        async fn with_allocator_mutation_lease_refreshed<T, F>(
            &self,
            refresh_full_state: bool,
            op: F,
        ) -> Result<T, Status>
        where
            F: std::future::Future<Output = Result<T, Status>>,
        {
            if self.leader_resident_lease {
                self.with_leader_resident_lease(refresh_full_state, op)
                    .await
            } else {
                self.with_per_op_lease(refresh_full_state, op).await
            }
        }

        /// Default path (flag off): the historical behavior, now also publishing
        /// the held epoch into `leadership` so the always-on fence asserts it. The
        /// in-process guard serializes writers; the durable lease is acquired and
        /// released around each op (sub-millisecond hold); the per-op stamp read
        /// still runs.
        async fn with_per_op_lease<T, F>(
            &self,
            refresh_full_state: bool,
            op: F,
        ) -> Result<T, Status>
        where
            F: std::future::Future<Output = Result<T, Status>>,
        {
            let guard = self.mutation_guard().await;
            let _guard = guard.lock().await;
            self.acquire_allocator_mutation_lease().await?;
            let refresh_result = if refresh_full_state {
                self.ensure_full_state_current().await
            } else {
                self.ensure_target_state_current().await
            };
            let result = match refresh_result {
                Ok(()) => op.await,
                Err(err) => Err(err),
            };
            // Release clears `leadership` and the durable lease record.
            let release_result = self.release_allocator_mutation_lease().await;
            match (result, release_result) {
                (Ok(value), Ok(())) => Ok(value),
                (Ok(_), Err(err)) => Err(err),
                (Err(err), Ok(())) => Err(err),
                (Err(err), Err(_)) => Err(err),
            }
        }

        /// Leader-resident path (flag on, DESIGN_KAS_WRITE_SCALE.md §3 #2/#4):
        /// the lease is held for the whole leadership term, NOT acquired/released
        /// per op. Per op we only:
        ///   1. ensure leadership (acquire on first use, opportunistically renew
        ///      at TTL/2) — a step-down here means we are no longer leader and
        ///      must not serve, so we return UNAVAILABLE and the KMS pool (#1)
        ///      masks the gap;
        ///   2. refresh in-memory state WITHOUT the per-op stamp GET (#4) — the
        ///      epoch fence detects any out-of-band writer at commit time;
        ///   3. run the op. No per-op durable release.
        ///
        /// The in-process `mutation_guard` mutex still serializes writers within
        /// this process; the lease+epoch handle the cross-process invariant.
        async fn with_leader_resident_lease<T, F>(
            &self,
            refresh_full_state: bool,
            op: F,
        ) -> Result<T, Status>
        where
            F: std::future::Future<Output = Result<T, Status>>,
        {
            let guard = self.mutation_guard().await;
            let _guard = guard.lock().await;
            // Ensure we hold leadership; renew if near expiry. A renewal failure /
            // observed epoch bump steps us down and surfaces an error.
            self.renew_leader_resident_lease().await?;
            if self.current_mutation_epoch().await.is_none() {
                // Not (any longer) the leader. Refuse to mutate; the route cache /
                // KMS pool is expected to surface UNAVAILABLE and refresh routes
                // (design §7: a demoted leader must surface UNAVAILABLE, not retry).
                return Err(Status::unavailable(
                    "KAS is not the current allocation-shard leader; retry routing",
                ));
            }
            // Skip the per-op stamp GET; the fence supersedes it.
            let refresh_result = if refresh_full_state {
                self.ensure_full_state_current_inner(true).await
            } else {
                self.ensure_target_state_current_inner(true).await
            };
            match refresh_result {
                Ok(()) => op.await,
                Err(err) => Err(err),
            }
            // No release: the lease is leadership-resident.
        }
    }

    #[tonic::async_trait]
    impl AllocatorStore for FdbKasStore {
        async fn init(&self) -> Result<(), Status> {
            self.refresh_target_state_from_fdb().await
        }

        async fn reset_allocator_state(&self) -> Result<(), Status> {
            let dirty_targets = {
                let mut state = self.state.lock().await;
                let mut dirty_targets = Vec::with_capacity(state.targets.len());
                for target in state.targets.values_mut() {
                    target.spans = vec![GranuleSpan {
                        start: 0,
                        len: target.target.granule_count,
                    }];
                    target.target.free_granules = target.target.granule_count;
                    dirty_targets.push(target.clone());
                }
                state.reservations.clear();
                state.bins.clear();
                dirty_targets
            };
            self.clear_all_reservations_and_bins().await?;
            // Reset rewrites every target's spans back to one full span -> rewrite.
            self.persist_state(
                &dirty_targets,
                &[],
                &[],
                &[],
                &[],
                &[],
                true,
                EpochFence::Unfenced,
            )
            .await
        }

        async fn register_target(&self, mut target: TargetRecord) -> Result<TargetRecord, Status> {
            if target.allocation_shard_id.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "target allocation_shard_id must not be empty",
                ));
            }
            if target.lifecycle_state == TargetLifecycleState::Unspecified as i32 {
                target.lifecycle_state = TargetLifecycleState::Active as i32;
            }
            let (stored, is_new_target) = {
                let mut state = self.state.lock().await;
                let existing_spans = state
                    .targets
                    .get(&target.target_id)
                    .map(|existing| existing.spans.clone());
                // A first-time registration must durably seed the initial full
                // span; a re-registration is a metadata refresh that must NOT
                // rewrite spans (FIX B: that blind rewrite from in-memory could
                // clobber a leader's committed reservations cross-shard).
                let is_new_target = existing_spans.is_none();
                let spans = existing_spans.unwrap_or_else(|| {
                    vec![GranuleSpan {
                        start: 0,
                        len: target.granule_count,
                    }]
                });
                target.free_granules = span_free_count(&spans);
                let stored = TargetAllocatorState {
                    target: target.clone(),
                    spans,
                };
                state
                    .targets
                    .insert(target.target_id.clone(), stored.clone());
                (stored, is_new_target)
            };
            // Only seed spans on a genuinely-new target; re-registrations are
            // span-neutral metadata writes.
            self.persist_state(
                &[stored],
                &[],
                &[],
                &[],
                &[],
                &[],
                is_new_target,
                EpochFence::Unfenced,
            )
            .await?;
            Ok(target)
        }

        async fn heartbeat_target(
            &self,
            target_id: String,
            healthy: bool,
            observed_unix_ms: u64,
        ) -> Result<TargetRecord, Status> {
            let (reply, entry) = {
                let mut state = self.state.lock().await;
                let entry = state.targets.get_mut(&target_id).ok_or_else(|| {
                    Status::not_found(format!("unknown target for heartbeat: {target_id}"))
                })?;
                entry.target.healthy = healthy;
                entry.target.last_heartbeat_unix_ms = observed_unix_ms;
                (entry.target.clone(), entry.clone())
            };
            // Heartbeat only touches target metadata; never rewrite spans (FIX B).
            self.persist_state(
                &[entry],
                &[],
                &[],
                &[],
                &[],
                &[],
                false,
                EpochFence::Unfenced,
            )
            .await?;
            Ok(reply)
        }

        async fn upsert_service_instance(
            &self,
            instance: ServiceInstanceRecord,
        ) -> Result<ServiceInstanceRecord, Status> {
            validate_service_instance(&instance)?;
            {
                let mut state = self.state.lock().await;
                state
                    .service_instances
                    .insert(instance.instance_id.clone(), instance.clone());
            }
            self.persist_state(
                &[],
                &[],
                &[],
                &[instance.clone()],
                &[],
                &[],
                // Service-instance write only; no targets, no span change.
                false,
                EpochFence::Unfenced,
            )
            .await?;
            Ok(instance)
        }

        async fn try_acquire_coordination_lease(
            &self,
            lease_name: &str,
            owner_id: &str,
            lease_ttl_ms: u64,
        ) -> Result<bool, Status> {
            // Delegate to the epoch-aware acquirer and discard the granted epoch.
            // Non-mutation coordination leases (reaper, bin-refill leader
            // election) only need the boolean. The epoch is still bumped on the
            // durable record on a new grant — harmless for those leases and the
            // single source of truth for the allocator mutation lease's fence.
            Ok(self
                .try_acquire_coordination_lease_epoch(lease_name, owner_id, lease_ttl_ms)
                .await?
                .is_some())
        }

        async fn list_service_instances(
            &self,
            service_kind: Option<ServiceKind>,
            node_id: Option<&str>,
            limit: usize,
        ) -> Result<Vec<ServiceInstanceRecord>, Status> {
            self.refresh_service_instances_from_fdb().await?;
            let state = self.state.lock().await;
            let mut instances = state
                .service_instances
                .values()
                .filter(|instance| {
                    service_kind
                        .map(|kind| instance.service_kind == kind as i32)
                        .unwrap_or(true)
                        && node_id.map(|node| instance.node_id == node).unwrap_or(true)
                })
                .cloned()
                .collect::<Vec<_>>();
            instances.sort_by(|left, right| {
                left.service_kind
                    .cmp(&right.service_kind)
                    .then_with(|| left.node_id.cmp(&right.node_id))
                    .then_with(|| left.instance_id.cmp(&right.instance_id))
            });
            instances.truncate(limit.max(1));
            Ok(instances)
        }

        async fn get_service_instance(
            &self,
            instance_id: &str,
        ) -> Result<Option<ServiceInstanceRecord>, Status> {
            self.refresh_service_instances_from_fdb().await?;
            let state = self.state.lock().await;
            Ok(state.service_instances.get(instance_id).cloned())
        }

        async fn list_targets(&self) -> Result<Vec<TargetRecord>, Status> {
            let state = self.state.lock().await;
            let mut targets = state
                .targets
                .values()
                .map(|entry| entry.target.clone())
                .collect::<Vec<_>>();
            targets.sort_by(|left, right| left.target_id.cmp(&right.target_id));
            Ok(targets)
        }

        async fn set_target_state(
            &self,
            target_id: String,
            lifecycle_state: TargetLifecycleState,
        ) -> Result<TargetRecord, Status> {
            if lifecycle_state == TargetLifecycleState::Unspecified {
                return Err(Status::invalid_argument(
                    "target lifecycle state must be specified",
                ));
            }
            let (target, entry) = {
                let mut state = self.state.lock().await;
                let entry = state.targets.get_mut(&target_id).ok_or_else(|| {
                    Status::not_found(format!("unknown target for lifecycle update: {target_id}"))
                })?;
                entry.target.lifecycle_state = lifecycle_state as i32;
                let observed_unix_ms = now_unix_ms();
                match lifecycle_state {
                    TargetLifecycleState::Active => {
                        entry.target.healthy = true;
                        entry.target.last_heartbeat_unix_ms = observed_unix_ms;
                    }
                    TargetLifecycleState::Unhealthy => {
                        entry.target.healthy = false;
                        entry.target.last_heartbeat_unix_ms = observed_unix_ms;
                    }
                    TargetLifecycleState::Draining | TargetLifecycleState::Retired => {}
                    TargetLifecycleState::Unspecified => {}
                }
                (entry.target.clone(), entry.clone())
            };
            // Lifecycle transition only touches metadata; never rewrite spans (FIX B).
            self.persist_state(
                &[entry],
                &[],
                &[],
                &[],
                &[],
                &[],
                false,
                EpochFence::Unfenced,
            )
            .await?;
            Ok(target)
        }

        async fn list_reservations(
            &self,
            state_filter: Option<ReservationState>,
            target_id: Option<&str>,
            limit: usize,
        ) -> Result<Vec<PlacementReservationRecord>, Status> {
            self.ensure_full_state_current().await?;
            let state = self.state.lock().await;
            let mut reservations = state
                .reservations
                .values()
                .filter(|reservation| {
                    state_filter
                        .filter(|value| *value != ReservationState::Unspecified)
                        .map(|value| reservation.state == value as i32)
                        .unwrap_or(true)
                        && target_id
                            .map(|target_id| {
                                reservation
                                    .placements
                                    .iter()
                                    .any(|placement| placement.target_id == target_id)
                            })
                            .unwrap_or(true)
                })
                .cloned()
                .collect::<Vec<_>>();
            reservations.sort_by(|left, right| {
                right
                    .expires_at_unix_ms
                    .cmp(&left.expires_at_unix_ms)
                    .then_with(|| left.reservation_id.cmp(&right.reservation_id))
            });
            reservations.truncate(limit.max(1));
            Ok(reservations)
        }

        async fn get_reservation(
            &self,
            reservation_id: &str,
        ) -> Result<Option<PlacementReservationRecord>, Status> {
            self.ensure_full_state_current().await?;
            let state = self.state.lock().await;
            Ok(state.reservations.get(reservation_id).cloned())
        }

        async fn reserve_stripe_placement(
            &self,
            reservation_id: String,
            fragment_count: usize,
            failure_domain: FailureDomain,
            excluded_target_ids: Vec<String>,
            reservation_ttl_ms: u64,
        ) -> Result<TimedStoreResult<PlacementReservationRecord>, Status> {
            let mut records = self
                .with_allocator_mutation_lease(self.reserve_batch_common_locked(
                    1,
                    fragment_count,
                    failure_domain,
                    excluded_target_ids,
                    None,
                    reservation_ttl_ms,
                    Vec::new(),
                    Some(reservation_id),
                ))
                .await?;
            records
                .value
                .pop()
                .map(|value| TimedStoreResult {
                    value,
                    phase_timings: records.phase_timings,
                })
                .ok_or_else(|| Status::resource_exhausted("allocator could not reserve placement"))
        }

        async fn reserve_stripe_batch(
            &self,
            batch_size: usize,
            fragment_count: usize,
            failure_domain: FailureDomain,
            excluded_target_ids: Vec<String>,
            reservation_ttl_ms: u64,
        ) -> Result<TimedStoreResult<Vec<PlacementReservationRecord>>, Status> {
            self.with_allocator_mutation_lease(self.reserve_batch_common_locked(
                batch_size,
                fragment_count,
                failure_domain,
                excluded_target_ids,
                None,
                reservation_ttl_ms,
                Vec::new(),
                None,
            ))
            .await
        }

        async fn claim_reservation_bin_batch(
            &self,
            batch_size: usize,
            fragment_count: usize,
            failure_domain: FailureDomain,
            reservation_ttl_ms: u64,
        ) -> Result<TimedStoreResult<Vec<PlacementReservationRecord>>, Status> {
            let bin_key = format!("fc:{}:fd:{}", fragment_count, failure_domain as i32);
            self.with_allocator_mutation_lease(async move {
                // On the leader-resident path the wrapper already dropped the per-op
                // acquire/release + stamp read; this op skips the per-op stamp GET
                // too (`_inner(true)`). On the default path it behaves as before.
                if self.leader_resident_lease {
                    self.ensure_full_state_current_inner(true).await?;
                } else {
                    self.ensure_full_state_current().await?;
                }
                let started = Instant::now();
                let (claimed, delete_bin_members) = {
                    let mut state = self.state.lock().await;
                    Self::claim_ready_bin_records(
                        &mut state,
                        &bin_key,
                        batch_size,
                        reservation_ttl_ms,
                    )
                };
                if self.leader_resident_lease {
                    // Phase 3 / change #7 — lock-light claim. The in-memory pop
                    // (under `state.lock()` + the in-process mutation guard) is the
                    // authoritative hand-out: a popped reservation_id is gone from
                    // the in-memory queue so it cannot be re-claimed in-process, and
                    // `refresh_full_state_from_fdb` always *clears* `state.bins` (the
                    // durable bin members are write-only acceleration that is never
                    // read back), so the durable `delete_bin_members` cleanup is pure
                    // bookkeeping with no claim-correctness role. We therefore:
                    //   * persist ONLY the claimed reservations' TTL bumps, fenced —
                    //     required so the reaper (`release_expired_reservations`) sees
                    //     the extended expiry and does not reap a just-handed-out
                    //     reservation;
                    //   * SKIP the stamp bump (sole fenced writer; the stamp is no
                    //     longer the safety net — the epoch fence is);
                    //   * DEFER the durable bin-member deletes — accumulated and
                    //     flushed opportunistically off the hot path.
                    if !claimed.is_empty() {
                        self.persist_claim_ttl_bumps_lock_light(&claimed).await?;
                    }
                    if !delete_bin_members.is_empty() {
                        let mut deferred = self.deferred_bin_member_deletes.lock().await;
                        deferred.extend(delete_bin_members);
                    }
                } else {
                    // Default path: unchanged — fenced persist of TTL bumps +
                    // synchronous durable bin-member deletes, stamp bumped.
                    self.persist_state(
                        &[],
                        &claimed,
                        &[],
                        &[],
                        &delete_bin_members,
                        &[],
                        // Claim bumps reservation TTLs + bin members only; no spans.
                        false,
                        EpochFence::Leased,
                    )
                    .await?;
                }
                Ok(TimedStoreResult {
                    value: claimed,
                    phase_timings: vec![StorePhaseTiming {
                        name: "claim_bin_in_memory",
                        elapsed: Instant::now().saturating_duration_since(started),
                    }],
                })
            })
            .await
        }

        async fn top_up_reservation_bin(
            &self,
            bin_key: &ReservationBinKey,
            reservation_ttl_ms: u64,
            low_watermark: usize,
            high_watermark: usize,
            top_up_chunk: usize,
        ) -> Result<TimedStoreResult<usize>, Status> {
            if high_watermark == 0 || high_watermark <= low_watermark {
                return Ok(TimedStoreResult {
                    value: 0,
                    phase_timings: Vec::new(),
                });
            }
            // Opportunistically drain the lock-light claim's deferred durable
            // bin-member deletes (change #7) off the refill cadence — never on the
            // foreground claim path. Best-effort; failures are swallowed (these
            // entries are write-only and discarded on any refresh).
            if self.leader_resident_lease {
                let _ = self.flush_deferred_bin_member_deletes().await;
            }
            let storage_key = format!(
                "fc:{}:fd:{}",
                bin_key.fragment_count(),
                bin_key.failure_domain_raw()
            );
            let current = {
                let state = self.state.lock().await;
                state.bins.get(&storage_key).map(VecDeque::len).unwrap_or(0)
            };
            if current >= high_watermark {
                return Ok(TimedStoreResult {
                    value: 0,
                    phase_timings: Vec::new(),
                });
            }
            // Total stripes to add this cycle, bounded by `top_up_chunk`. We
            // commit them in `REFILL_PERSIST_BATCH_MAX`-sized atomic persists so
            // a large `top_up_chunk` can never build an over-limit FDB
            // transaction (FdbError 2101), and we drop the mutation lease between
            // batches so foreground reserves are not starved (the refiller no
            // longer holds the lease for the whole top-up).
            let cycle_target = high_watermark
                .saturating_sub(current)
                .min(top_up_chunk.max(1));
            let mut added_total = 0usize;
            let mut phase_timings = Vec::new();
            while added_total < cycle_target {
                let want = (cycle_target - added_total).min(REFILL_PERSIST_BATCH_MAX);
                let storage_key = storage_key.clone();
                let batch = self
                    .with_allocator_mutation_lease(async move {
                        self.ensure_full_state_current().await?;
                        let records = self
                            .reserve_batch_common_locked(
                                want,
                                bin_key.fragment_count(),
                                bin_key.failure_domain()?,
                                Vec::new(),
                                None,
                                reservation_ttl_ms,
                                Vec::new(),
                                None,
                            )
                            .await?;
                        let add_bin_members = {
                            let mut state = self.state.lock().await;
                            let queue = state.bins.entry(storage_key.clone()).or_default();
                            records
                                .value
                                .iter()
                                .map(|record| {
                                    queue.push_back(record.reservation_id.clone());
                                    (storage_key.clone(), record.reservation_id.clone())
                                })
                                .collect::<Vec<_>>()
                        };
                        self.persist_state(
                            &[],
                            &[],
                            &[],
                            &[],
                            &[],
                            &add_bin_members,
                            // The span change for these reservations was already
                            // persisted (rewrite) inside reserve_batch_common_locked;
                            // this commit only adds bin members. No span rewrite.
                            false,
                            EpochFence::Leased,
                        )
                        .await?;
                        Ok(TimedStoreResult {
                            value: add_bin_members.len(),
                            phase_timings: records.phase_timings,
                        })
                    })
                    .await?;
                phase_timings.extend(batch.phase_timings);
                added_total += batch.value;
                // Fewer placements than requested means the domain ran dry; stop
                // rather than spin acquiring the lease for empty batches.
                if batch.value < want {
                    break;
                }
            }
            Ok(TimedStoreResult {
                value: added_total,
                phase_timings,
            })
        }

        async fn reserve_rebuild_placement(
            &self,
            reservation_id: String,
            failed_target_id: String,
            failure_domain: FailureDomain,
            occupied_target_ids: Vec<String>,
        ) -> Result<TimedStoreResult<PlacementReservationRecord>, Status> {
            let mut records = self
                .with_allocator_mutation_lease(self.reserve_batch_common_locked(
                    1,
                    1,
                    failure_domain,
                    occupied_target_ids,
                    Some(failed_target_id),
                    0,
                    Vec::new(),
                    Some(reservation_id),
                ))
                .await?;
            records
                .value
                .pop()
                .map(|value| TimedStoreResult {
                    value,
                    phase_timings: records.phase_timings,
                })
                .ok_or_else(|| {
                    Status::resource_exhausted("allocator could not reserve rebuild placement")
                })
        }

        async fn reserve_replacement_placement(
            &self,
            reservation_id: String,
            replacement_count: usize,
            failure_domain: FailureDomain,
            excluded_target_ids: Vec<String>,
            reservation_ttl_ms: u64,
            required_target_ids: Vec<String>,
        ) -> Result<TimedStoreResult<PlacementReservationRecord>, Status> {
            let mut records = self
                .with_allocator_mutation_lease(self.reserve_batch_common_locked(
                    1,
                    replacement_count,
                    failure_domain,
                    excluded_target_ids,
                    None,
                    reservation_ttl_ms,
                    required_target_ids,
                    Some(reservation_id),
                ))
                .await?;
            records
                .value
                .pop()
                .map(|value| TimedStoreResult {
                    value,
                    phase_timings: records.phase_timings,
                })
                .ok_or_else(|| {
                    Status::resource_exhausted("allocator could not reserve replacement placement")
                })
        }

        async fn finalize_reservations(
            &self,
            reservation_id: String,
            placement_indexes: Vec<u32>,
        ) -> Result<PlacementReservationRecord, Status> {
            let mut values = self
                .finalize_reservations_batch(vec![ReservationMutationSpec {
                    reservation_id,
                    placement_indexes,
                }])
                .await?;
            values.pop().ok_or_else(|| {
                Status::internal("FdbKasStore finalize_reservations_batch returned no records")
            })
        }

        async fn finalize_reservations_batch(
            &self,
            mutations: Vec<ReservationMutationSpec>,
        ) -> Result<Vec<PlacementReservationRecord>, Status> {
            if mutations.is_empty() {
                return Ok(Vec::new());
            }
            self.with_allocator_mutation_lease_full_state(async move {
                if let Some(results) = self
                    .try_finalize_reservations_metadata_only(mutations.clone())
                    .await?
                {
                    return Ok(results);
                }
                self.apply_reservation_mutations_locked(
                    mutations,
                    ReservationMutationKind::Finalize,
                )
                .await
            })
            .await
        }

        async fn release_reservations(
            &self,
            reservation_id: String,
            placement_indexes: Vec<u32>,
        ) -> Result<PlacementReservationRecord, Status> {
            let mut values = self
                .release_reservations_batch(vec![ReservationMutationSpec {
                    reservation_id,
                    placement_indexes,
                }])
                .await?;
            values.pop().ok_or_else(|| {
                Status::internal("FdbKasStore release_reservations_batch returned no records")
            })
        }

        async fn release_reservations_batch(
            &self,
            mutations: Vec<ReservationMutationSpec>,
        ) -> Result<Vec<PlacementReservationRecord>, Status> {
            if mutations.is_empty() {
                return Ok(Vec::new());
            }
            self.with_allocator_mutation_lease_full_state(async move {
                self.apply_reservation_mutations_locked(mutations, ReservationMutationKind::Release)
                    .await
            })
            .await
        }

        async fn reclaim_target_granules(
            &self,
            granules: Vec<TargetGranule>,
        ) -> Result<u64, Status> {
            if granules.is_empty() {
                return Ok(0);
            }
            // FIX A (DESIGN_KAS_WRITE_SCALE.md §3 #2). reclaim genuinely mutates a
            // target's free spans (release_granule), and its `persist_state`
            // ALWAYS rewrote that target's ENTIRE span chunk set from in-memory.
            // Run it UNDER the allocator mutation lease (like reserve/release) so:
            //   * on the per-op (flag-off) path it acquires+holds the epoch and the
            //     state is freshly re-read before we mutate spans, then the persist
            //     commits `Leased` (epoch-fenced);
            //   * on the leader-resident path a non-leader returns UNAVAILABLE (the
            //     wrapper refuses to mutate) instead of clobbering the rightful
            //     leader's committed reservations from stale memory.
            // The `full_state` refresh re-reads spans + reservations inside the
            // lease so the release operates on current state, not a stale snapshot.
            //
            // KMS-side routing TODO: KMS currently routes ReclaimTargetGranules
            // round-robin (kas_channels.client()), NOT shard-aware, so a reclaim can
            // land on the wrong shard's KAS. With this `Leased` fence that misroute
            // ABORTS (no held epoch / epoch mismatch -> commit fails) instead of
            // corrupting state — SAFE but not yet correctly routed. See the
            // matching TODO in kms/service.rs::reclaim_target_granules.
            self.with_allocator_mutation_lease_full_state(async move {
                let (reclaimed, dirty_targets) = {
                    let mut state = self.state.lock().await;
                    let mut by_target: HashMap<String, Vec<u64>> = HashMap::new();
                    for granule in granules {
                        by_target
                            .entry(granule.target_id)
                            .or_default()
                            .push(granule.granule_index);
                    }
                    let mut reclaimed = 0u64;
                    let mut dirty_targets = HashSet::new();
                    for (target_id, indexes) in by_target {
                        let entry = state.targets.get_mut(&target_id).ok_or_else(|| {
                            Status::not_found(format!("unknown target {}", target_id))
                        })?;
                        for granule_index in indexes {
                            if granule_index >= entry.target.granule_count {
                                return Err(Status::invalid_argument(format!(
                                    "granule {} is out of range for target {} capacity {}",
                                    granule_index, target_id, entry.target.granule_count
                                )));
                            }
                            if granule_is_free(&entry.spans, granule_index) {
                                continue;
                            }
                            release_granule(&mut entry.spans, granule_index);
                            entry.target.free_granules = span_free_count(&entry.spans);
                            reclaimed = reclaimed.saturating_add(1);
                            dirty_targets.insert(target_id.clone());
                        }
                    }
                    let dirty_targets = dirty_targets
                        .into_iter()
                        .filter_map(|target_id| state.targets.get(&target_id).cloned())
                        .collect::<Vec<_>>();
                    (reclaimed, dirty_targets)
                };
                // Spans changed -> rewrite_spans = true; fenced by the held epoch.
                self.persist_state(
                    &dirty_targets,
                    &[],
                    &[],
                    &[],
                    &[],
                    &[],
                    true,
                    EpochFence::Leased,
                )
                .await?;
                Ok(reclaimed)
            })
            .await
        }

        async fn release_expired_reservations(
            &self,
            now_ms: u64,
            limit: usize,
        ) -> Result<usize, Status> {
            self.with_allocator_mutation_lease_full_state(async move {
                let mutations = {
                    let state = self.state.lock().await;
                    let mut expired = state
                        .reservations
                        .values()
                        .filter(|reservation| {
                            reservation.state == ReservationState::Reserved as i32
                                && reservation.expires_at_unix_ms > 0
                                && reservation.expires_at_unix_ms <= now_ms
                        })
                        .map(|reservation| reservation.reservation_id.clone())
                        .collect::<Vec<_>>();
                    expired.sort();
                    expired.truncate(limit);
                    expired
                        .into_iter()
                        .map(|reservation_id| ReservationMutationSpec {
                            reservation_id,
                            placement_indexes: Vec::new(),
                        })
                        .collect::<Vec<_>>()
                };
                if mutations.is_empty() {
                    return Ok(0);
                }
                let released = self
                    .apply_reservation_mutations_locked(mutations, ReservationMutationKind::Release)
                    .await?;
                Ok(released.len())
            })
            .await
        }
    }

    fn candidate_domains_from_state(
        targets: &HashMap<String, TargetAllocatorState>,
        failure_domain: FailureDomain,
        excluded: &HashSet<String>,
        banned_target_id: Option<&str>,
        required: &HashSet<String>,
        allocation_shard_id: Option<&str>,
    ) -> Result<Vec<CandidateDomain>, Status> {
        let candidates = targets
            .values()
            .filter(|entry| entry.target.healthy)
            .filter(|entry| entry.target.lifecycle_state == TargetLifecycleState::Active as i32)
            .filter(|entry| !entry.spans.is_empty() && entry.target.free_granules > 0)
            .filter(|entry| {
                allocation_shard_id
                    .map(|shard| entry.target.allocation_shard_id == shard)
                    .unwrap_or(true)
            })
            .filter(|entry| !excluded.contains(&entry.target.target_id))
            .filter(|entry| {
                banned_target_id
                    .map(|id| entry.target.target_id != id)
                    .unwrap_or(true)
            })
            .filter(|entry| required.is_empty() || required.contains(&entry.target.target_id))
            .map(|entry| CandidateTarget {
                target: entry.target.clone(),
                spans: entry.spans.clone(),
            })
            .collect::<Vec<_>>();
        candidate_domains_from_targets(failure_domain, candidates)
    }

    fn candidate_domains_from_targets(
        failure_domain: FailureDomain,
        candidates: Vec<CandidateTarget>,
    ) -> Result<Vec<CandidateDomain>, Status> {
        let mut grouped = HashMap::<String, Vec<CandidateTarget>>::new();
        for candidate in candidates {
            let domain_key = failure_domain_key(failure_domain, &candidate.target)?;
            grouped.entry(domain_key).or_default().push(candidate);
        }
        let mut domains = grouped
            .into_iter()
            .map(|(key, mut members)| {
                sort_domain_members(&mut members);
                CandidateDomain { key, members }
            })
            .collect::<Vec<_>>();
        domains.sort_by(|left, right| {
            domain_head_free(right)
                .cmp(&domain_head_free(left))
                .then_with(|| left.key.cmp(&right.key))
        });
        Ok(domains)
    }

    fn plan_reservation_batch(
        domains: &mut [CandidateDomain],
        batch_size: usize,
        fragment_count: usize,
        explicit_reservation_id: Option<&str>,
        reservation_id_prefix: Option<&str>,
        expires_at_unix_ms: u64,
    ) -> Result<PlannedReservationBatch, Status> {
        let mut reservations = Vec::with_capacity(batch_size);
        let mut dirty_targets = HashSet::new();
        for batch_index in 0..batch_size {
            let reservation_id = explicit_reservation_id
                .map(str::to_string)
                .unwrap_or_else(|| {
                    let generated = format!("reserve-batch-{}-{batch_index:04}", Uuid::new_v4());
                    reservation_id_prefix
                        .filter(|value| !value.trim().is_empty())
                        .map(|prefix| format!("{prefix}/{generated}"))
                        .unwrap_or(generated)
                });
            sort_candidate_domains(domains, &reservation_id);
            let selected = domains
                .iter()
                .enumerate()
                .filter(|(_, domain)| domain_head_free(domain) > 0)
                .take(fragment_count)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if selected.len() != fragment_count {
                break;
            }
            let mut placements = Vec::with_capacity(fragment_count);
            for domain_index in selected {
                let domain = &mut domains[domain_index];
                let member_index = domain_head_index(domain).ok_or_else(|| {
                    Status::internal(format!(
                        "allocator lost a viable candidate while reserving {}",
                        reservation_id
                    ))
                })?;
                let candidate = &mut domain.members[member_index];
                let granule_index = allocate_one(&mut candidate.spans).ok_or_else(|| {
                    Status::internal(format!(
                        "allocator picked an empty span set for target {} while reserving {}",
                        candidate.target.target_id, reservation_id
                    ))
                })?;
                candidate.target.free_granules = span_free_count(&candidate.spans);
                dirty_targets.insert(candidate.target.target_id.clone());
                placements.push(PlacementReservation {
                    target_id: candidate.target.target_id.clone(),
                    endpoint: candidate.target.endpoint.clone(),
                    granule_index,
                    fragment_index: placements.len() as u32,
                    reservation_id: reservation_id.clone(),
                    reservation_placement_index: placements.len() as u32,
                });
                sort_domain_members(&mut domain.members);
            }
            reservations.push(PlacementReservationRecord {
                reservation_id: reservation_id.clone(),
                state: ReservationState::Reserved as i32,
                placements,
                expires_at_unix_ms: if expires_at_unix_ms == 0 {
                    0
                } else {
                    expires_at_unix_ms.saturating_add(batch_index as u64)
                },
            });
        }
        Ok(PlannedReservationBatch {
            reservations,
            dirty_targets,
        })
    }

    fn sort_candidate_domains(domains: &mut [CandidateDomain], reservation_id: &str) {
        domains.sort_by(|left, right| {
            domain_head_free(right)
                .cmp(&domain_head_free(left))
                .then_with(|| {
                    domain_order_bias(reservation_id, &left.key)
                        .cmp(&domain_order_bias(reservation_id, &right.key))
                })
                .then_with(|| left.key.cmp(&right.key))
        });
    }

    fn sort_domain_members(members: &mut [CandidateTarget]) {
        members.sort_by(|left, right| {
            right
                .target
                .free_granules
                .cmp(&left.target.free_granules)
                .then_with(|| left.target.target_id.cmp(&right.target.target_id))
        });
    }

    fn domain_head_index(domain: &CandidateDomain) -> Option<usize> {
        domain
            .members
            .iter()
            .position(|candidate| candidate.target.free_granules > 0 && !candidate.spans.is_empty())
    }

    fn domain_head_free(domain: &CandidateDomain) -> u64 {
        domain_head_index(domain)
            .map(|index| domain.members[index].target.free_granules)
            .unwrap_or(0)
    }

    fn domain_order_bias(reservation_id: &str, domain_key: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        reservation_id.hash(&mut hasher);
        domain_key.hash(&mut hasher);
        hasher.finish()
    }

    fn failure_domain_key(
        failure_domain: FailureDomain,
        target: &TargetRecord,
    ) -> Result<String, Status> {
        match failure_domain {
            FailureDomain::DriveDomainLab => Ok(format!("target:{}", target.target_id)),
            FailureDomain::Node => Ok(format!("node:{}", target.server_id)),
            FailureDomain::Rack => Ok(format!("rack:{}", target.rack_id)),
            FailureDomain::Unspecified => {
                Err(Status::invalid_argument("failure domain must be specified"))
            }
        }
    }

    fn span_free_count(spans: &[GranuleSpan]) -> u64 {
        spans.iter().map(|span| span.len).sum()
    }

    fn allocate_one(spans: &mut Vec<GranuleSpan>) -> Option<u64> {
        let first = spans.first_mut()?;
        let granule = first.start;
        first.start += 1;
        first.len -= 1;
        if first.len == 0 {
            spans.remove(0);
        }
        Some(granule)
    }

    fn release_granule(spans: &mut Vec<GranuleSpan>, granule_index: u64) {
        spans.push(GranuleSpan {
            start: granule_index,
            len: 1,
        });
        spans.sort_by(|a, b| a.start.cmp(&b.start));
        let mut merged: Vec<GranuleSpan> = Vec::with_capacity(spans.len());
        for span in spans.drain(..) {
            if let Some(last) = merged.last_mut() {
                if last.start + last.len == span.start {
                    last.len += span.len;
                    continue;
                }
            }
            merged.push(span);
        }
        *spans = merged;
    }

    fn granule_is_free(spans: &[GranuleSpan], granule_index: u64) -> bool {
        spans.iter().any(|span| {
            granule_index >= span.start && granule_index < span.start.saturating_add(span.len)
        })
    }

    fn normalize_subset_indexes(total: usize, raw_indexes: &[u32]) -> Result<Vec<usize>, Status> {
        if total == 0 {
            return Ok(Vec::new());
        }
        if raw_indexes.is_empty() {
            return Ok((0..total).collect());
        }
        let mut indexes = raw_indexes
            .iter()
            .map(|index| {
                usize::try_from(*index).map_err(|_| {
                    Status::invalid_argument(format!(
                        "placement index {} exceeds usize::MAX",
                        index
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        indexes.sort_unstable();
        indexes.dedup();
        if let Some(out_of_range) = indexes.iter().find(|index| **index >= total) {
            return Err(Status::invalid_argument(format!(
                "placement index {} is out of range for {} placements",
                out_of_range, total
            )));
        }
        Ok(indexes)
    }

    fn release_record_placements(
        state: &mut AllocatorState,
        record: &PlacementReservationRecord,
        indexes: &[usize],
        dirty_targets: &mut HashSet<String>,
    ) -> Result<(), Status> {
        let released = indexes
            .iter()
            .filter_map(|index| record.placements.get(*index))
            .cloned()
            .collect::<Vec<_>>();
        for placement in released {
            let entry = state.targets.get_mut(&placement.target_id).ok_or_else(|| {
                Status::not_found(format!("unknown target {}", placement.target_id))
            })?;
            if granule_is_free(&entry.spans, placement.granule_index) {
                continue;
            }
            release_granule(&mut entry.spans, placement.granule_index);
            entry.target.free_granules = span_free_count(&entry.spans);
            dirty_targets.insert(entry.target.target_id.clone());
        }
        Ok(())
    }

    fn remove_reservation_from_bins(
        bins: &mut HashMap<String, VecDeque<String>>,
        reservation_id: &str,
        deleted: &mut Vec<(String, String)>,
    ) {
        let keys = bins.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            if let Some(queue) = bins.get_mut(&key) {
                let before = queue.len();
                queue.retain(|value| value != reservation_id);
                if queue.len() != before {
                    deleted.push((key.clone(), reservation_id.to_string()));
                }
                if queue.is_empty() {
                    bins.remove(&key);
                }
            }
        }
    }

    fn validate_service_instance(instance: &ServiceInstanceRecord) -> Result<(), Status> {
        if instance.instance_id.is_empty() {
            return Err(Status::invalid_argument(
                "service instance_id must not be empty",
            ));
        }
        if matches!(
            ServiceKind::try_from(instance.service_kind),
            Ok(ServiceKind::Unspecified) | Err(_)
        ) {
            return Err(Status::invalid_argument(
                "service_kind must be a known non-unspecified value",
            ));
        }
        if instance.node_id.is_empty() {
            return Err(Status::invalid_argument(
                "service node_id must not be empty",
            ));
        }
        if instance.package_name.is_empty() {
            return Err(Status::invalid_argument(
                "service package_name must not be empty",
            ));
        }
        let build = instance
            .build
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("service build info is required"))?;
        if build.version.is_empty() {
            return Err(Status::invalid_argument(
                "service build version must not be empty",
            ));
        }
        Ok(())
    }

    fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>, Status> {
        serde_json::to_vec(value)
            .map_err(|err| Status::internal(format!("failed to encode FoundationDB value: {err}")))
    }

    fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Status> {
        serde_json::from_slice(bytes)
            .map_err(|err| Status::internal(format!("failed to decode FoundationDB value: {err}")))
    }

    fn map_fdb_binding_error(err: FdbBindingError) -> Status {
        Status::internal(format!("KAS FoundationDB failure: {err}"))
    }

    fn status_to_fdb(status: Status) -> FdbBindingError {
        FdbBindingError::new_custom_error(Box::new(StatusCarrier(status)))
    }

    fn now_unix_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Pure epoch-fence grant decision (DESIGN_KAS_WRITE_SCALE.md §3/§4 change #3),
    /// factored out of the FDB transaction so the bump rule is unit-testable.
    ///
    /// `existing` is `(owner_id, expires_at_unix_ms, epoch)` of the current lease
    /// record, or `None` if no record exists. Returns `(available, epoch)` where
    /// `available` is whether `owner_id` may take/keep the lease and `epoch` is the
    /// fencing token it would hold:
    ///   * absent record        -> grant, epoch 1 (first term);
    ///   * same owner, still live -> self-renew, epoch UNCHANGED (never advance on
    ///     renewal — that would invalidate the renewing leader's own writes);
    ///   * expired (any owner)  -> NEW grant, epoch = old + 1 (advance the fence so
    ///     a superseded leader's later commit aborts);
    ///   * live, different owner -> not available, epoch 0 (caller is not leader).
    /// Build the FDB write unit for one target (FIX B). The metadata record is
    /// ALWAYS written. The span clear-range + chunk Sets are emitted ONLY when
    /// `tw.span_rewrite` is `Some` (the caller passed `rewrite_spans = true`).
    /// A span-neutral metadata write therefore produces exactly one op — the
    /// record Set — and touches nothing under `PREFIX_TARGET_SPAN`, so it can
    /// never clobber a leader's committed free spans. Returns `(ops, byte_size)`
    /// for the byte-bounded batcher. Extracted as a free function so the
    /// span-neutral invariant is directly unit-testable.
    fn build_target_write_unit(tw: TargetWrite) -> (Vec<WriteOp>, usize) {
        let chunk_count = tw
            .span_rewrite
            .as_ref()
            .map(|rewrite| rewrite.chunk_writes.len())
            .unwrap_or(0);
        let mut ops = Vec::with_capacity(2 + chunk_count);
        let mut bytes = tw.record_key.len() + tw.record_value.len();
        ops.push(WriteOp::Set(tw.record_key, tw.record_value));
        if let Some(rewrite) = tw.span_rewrite {
            ops.push(WriteOp::ClearRange(rewrite.clear_begin, rewrite.clear_end));
            for (key, value) in rewrite.chunk_writes {
                bytes += key.len() + value.len();
                ops.push(WriteOp::Set(key, value));
            }
        }
        (ops, bytes)
    }

    fn decide_lease_grant(
        existing: Option<(&str, u64, u64)>,
        owner_id: &str,
        now_ms: u64,
    ) -> (bool, u64) {
        match existing {
            None => (true, 1),
            Some((existing_owner, expires_at_unix_ms, existing_epoch)) => {
                let same_owner = existing_owner == owner_id;
                let live = expires_at_unix_ms > now_ms;
                if same_owner && live {
                    // Self-renew: keep epoch.
                    (true, existing_epoch)
                } else if !live {
                    // Expired (ours or theirs): new grant, advance the fence.
                    (true, existing_epoch.saturating_add(1))
                } else {
                    // Live and held by a different owner: not available.
                    (false, 0)
                }
            }
        }
    }

    fn synthetic_reservation_result(
        reservation_id: &str,
        state: ReservationState,
    ) -> PlacementReservationRecord {
        PlacementReservationRecord {
            reservation_id: reservation_id.to_string(),
            state: state as i32,
            ..PlacementReservationRecord::default()
        }
    }

    #[cfg(test)]
    mod epoch_fence_tests {
        use super::*;

        const ME: &str = "owner-a";
        const OTHER: &str = "owner-b";
        const NOW: u64 = 1_000_000;

        #[test]
        fn first_grant_starts_at_epoch_one() {
            // No record: first term is epoch 1 (0 is reserved for legacy /
            // never-granted so a held epoch is always strictly greater).
            assert_eq!(decide_lease_grant(None, ME, NOW), (true, 1));
        }

        #[test]
        fn self_renew_keeps_epoch() {
            // Same owner, still live -> renew, epoch UNCHANGED. Advancing here
            // would fence out the renewing leader's own in-flight writes.
            let existing = Some((ME, NOW + 5_000, 7));
            assert_eq!(decide_lease_grant(existing, ME, NOW), (true, 7));
        }

        #[test]
        fn new_grant_from_expired_other_owner_bumps_epoch() {
            // A dead leader's lease expired; we take it and ADVANCE the fence so
            // the old leader's resumed commit aborts (held epoch 5 < new 6).
            let existing = Some((OTHER, NOW - 1, 5));
            assert_eq!(decide_lease_grant(existing, ME, NOW), (true, 6));
        }

        #[test]
        fn reacquiring_our_own_expired_lease_bumps_epoch() {
            // Even our own expired lease is a NEW term: bump so any write that was
            // in flight under the lapsed term is fenced out.
            let existing = Some((ME, NOW - 1, 3));
            assert_eq!(decide_lease_grant(existing, ME, NOW), (true, 4));
        }

        #[test]
        fn live_lease_held_by_other_is_unavailable() {
            // A different owner holds a live lease: we are not leader.
            let existing = Some((OTHER, NOW + 5_000, 9));
            assert_eq!(decide_lease_grant(existing, ME, NOW), (false, 0));
        }

        #[test]
        fn expiry_boundary_is_exclusive_grant() {
            // expires_at == now means expired (the `> now` liveness test fails),
            // so it is a new grant that advances the fence.
            let existing = Some((OTHER, NOW, 2));
            assert_eq!(decide_lease_grant(existing, ME, NOW), (true, 3));
        }

        #[test]
        fn epoch_is_monotonic_across_failovers() {
            // Simulate A -> (expire) -> B -> (expire) -> A: epoch only ever rises.
            let (_, e1) = decide_lease_grant(None, "A", NOW); // 1
            let (_, e2) = decide_lease_grant(Some(("A", NOW - 1, e1)), "B", NOW); // 2
            let (_, e3) = decide_lease_grant(Some(("B", NOW - 1, e2)), "A", NOW); // 3
            assert!(e1 < e2 && e2 < e3, "epoch must be strictly monotonic");
            assert_eq!((e1, e2, e3), (1, 2, 3));
        }
    }

    #[cfg(test)]
    mod span_neutral_write_tests {
        use super::*;

        fn record_only_target_write() -> TargetWrite {
            TargetWrite {
                record_key: target_key("epyc-target-07"),
                record_value: b"{\"record\":true}".to_vec(),
                span_rewrite: None,
            }
        }

        #[test]
        fn span_neutral_write_emits_only_the_record_set() {
            // FIX B: a metadata-only write (rewrite_spans = false -> span_rewrite
            // None) must produce EXACTLY the record Set and NOTHING under the span
            // keyspace — no ClearRange, no chunk Sets. This is what stops a
            // control-plane write from clobbering a leader's committed spans.
            let (ops, _bytes) = build_target_write_unit(record_only_target_write());
            assert_eq!(ops.len(), 1, "span-neutral write must emit one op");
            match &ops[0] {
                WriteOp::Set(key, _) => {
                    assert_eq!(key, &target_key("epyc-target-07"));
                    // The single op must NOT be under the span prefix.
                    assert!(!key.starts_with(&target_span_all_prefix()));
                }
                other => panic!("expected a record Set, got {other:?}"),
            }
            // No op may touch the target's span keyspace.
            let span_prefix = target_span_prefix("epyc-target-07");
            for op in &ops {
                match op {
                    WriteOp::ClearRange(begin, _) => {
                        assert!(!begin.starts_with(&span_prefix), "must not clear spans");
                    }
                    WriteOp::Set(key, _) | WriteOp::Clear(key) => {
                        assert!(!key.starts_with(&span_prefix), "must not write span chunks");
                    }
                }
            }
        }

        #[test]
        fn span_rewriting_write_clears_then_writes_chunks() {
            // The reserve/reclaim/reset path (rewrite_spans = true) DOES rewrite:
            // the unit clears the target's span prefix and writes its chunk(s).
            let id = "epyc-target-07";
            let span_prefix = target_span_prefix(id);
            let span_end = prefix_range_end(&span_prefix);
            let chunk_key = target_span_chunk_key(id, 0);
            let tw = TargetWrite {
                record_key: target_key(id),
                record_value: b"{\"record\":true}".to_vec(),
                span_rewrite: Some(TargetSpanRewrite {
                    clear_begin: span_prefix.clone(),
                    clear_end: span_end.clone(),
                    chunk_writes: vec![(chunk_key.clone(), b"{\"spans\":[]}".to_vec())],
                }),
            };
            let (ops, _bytes) = build_target_write_unit(tw);
            // record Set, then ClearRange(span), then one chunk Set.
            assert_eq!(ops.len(), 3);
            assert_eq!(ops[0], WriteOp::Set(target_key(id), b"{\"record\":true}".to_vec()));
            assert_eq!(ops[1], WriteOp::ClearRange(span_prefix, span_end));
            assert_eq!(ops[2], WriteOp::Set(chunk_key, b"{\"spans\":[]}".to_vec()));
        }
    }

    #[cfg(test)]
    mod span_chunk_tests {
        use super::*;

        fn span(start: u64, len: u64) -> GranuleSpan {
            GranuleSpan { start, len }
        }

        #[test]
        fn split_then_assemble_round_trips_spans_in_order() {
            // More than two chunks' worth so cross-chunk ordering is exercised.
            let spans: Vec<GranuleSpan> = (0..(SPANS_PER_CHUNK * 2 + 7) as u64)
                .map(|i| span(i * 2, 1))
                .collect();
            let chunks = split_spans_into_chunks("t0", &spans);
            assert_eq!(chunks.len(), 3);
            assert!(chunks.iter().all(|c| c.spans.len() <= SPANS_PER_CHUNK));
            assert!(chunks.iter().all(|c| c.target_id == "t0"));

            let assembled = assemble_spans_from_chunks(chunks);
            let restored = assembled.get("t0").expect("target present");
            assert_eq!(restored.len(), spans.len());
            // Order must be preserved so allocate_one() behaves identically.
            for (orig, got) in spans.iter().zip(restored.iter()) {
                assert_eq!(orig.start, got.start);
                assert_eq!(orig.len, got.len);
            }
        }

        #[test]
        fn empty_spans_round_trip_to_empty() {
            let chunks = split_spans_into_chunks("t0", &[]);
            assert_eq!(chunks.len(), 1, "empty span list still rewrites one chunk");
            let assembled = assemble_spans_from_chunks(chunks);
            assert!(assembled.get("t0").map(|s| s.is_empty()).unwrap_or(true));
        }

        #[test]
        fn assemble_groups_multiple_targets_independently() {
            let mut chunks = split_spans_into_chunks("t0", &[span(0, 4), span(10, 2)]);
            chunks.extend(split_spans_into_chunks("t1", &[span(100, 1)]));
            let assembled = assemble_spans_from_chunks(chunks);
            assert_eq!(assembled.get("t0").unwrap().len(), 2);
            assert_eq!(assembled.get("t1").unwrap().len(), 1);
            assert_eq!(assembled.get("t1").unwrap()[0].start, 100);
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use crate::allocator_store::AllocatorStore;
    use crate::store::{ReservationBinKey, ReservationMutationSpec, TimedStoreResult};
    use keinctl::proto::{
        FailureDomain, PlacementReservationRecord, ReservationState, ServiceInstanceRecord,
        ServiceKind, TargetGranule, TargetLifecycleState, TargetRecord,
    };
    use std::error::Error;
    use tonic::Status;

    #[derive(Clone)]
    pub(crate) struct FdbKasStore;

    pub(crate) struct FdbNetworkGuard;

    pub(crate) fn maybe_boot_network() -> Result<Option<FdbNetworkGuard>, Box<dyn Error>> {
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "FoundationDB-backed KAS is only supported on Linux",
        )))
    }

    impl FdbKasStore {
        pub(crate) fn connect(
            _cluster_file: &str,
            _allocation_shard_id: Option<String>,
            _leader_resident_lease: bool,
            _stats: Option<std::sync::Arc<crate::stats::KasStats>>,
        ) -> Result<Self, Box<dyn Error>> {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "FoundationDB-backed KAS is only supported on Linux",
            )))
        }

        /// Signature parity with the Linux impl; the non-Linux store never
        /// constructs successfully, so this is unreachable in practice.
        pub(crate) fn start_leader_resident_renewer(&self) {}
    }

    #[tonic::async_trait]
    impl AllocatorStore for FdbKasStore {
        async fn init(&self) -> Result<(), Status> {
            unsupported()
        }
        async fn reset_allocator_state(&self) -> Result<(), Status> {
            unsupported()
        }
        async fn register_target(&self, _target: TargetRecord) -> Result<TargetRecord, Status> {
            unsupported()
        }
        async fn heartbeat_target(
            &self,
            _target_id: String,
            _healthy: bool,
            _observed_unix_ms: u64,
        ) -> Result<TargetRecord, Status> {
            unsupported()
        }
        async fn upsert_service_instance(
            &self,
            _instance: ServiceInstanceRecord,
        ) -> Result<ServiceInstanceRecord, Status> {
            unsupported()
        }
        async fn try_acquire_coordination_lease(
            &self,
            _lease_name: &str,
            _owner_id: &str,
            _lease_ttl_ms: u64,
        ) -> Result<bool, Status> {
            unsupported()
        }
        async fn list_service_instances(
            &self,
            _service_kind: Option<ServiceKind>,
            _node_id: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<ServiceInstanceRecord>, Status> {
            unsupported()
        }
        async fn get_service_instance(
            &self,
            _instance_id: &str,
        ) -> Result<Option<ServiceInstanceRecord>, Status> {
            unsupported()
        }
        async fn list_targets(&self) -> Result<Vec<TargetRecord>, Status> {
            unsupported()
        }
        async fn set_target_state(
            &self,
            _target_id: String,
            _lifecycle_state: TargetLifecycleState,
        ) -> Result<TargetRecord, Status> {
            unsupported()
        }
        async fn list_reservations(
            &self,
            _state: Option<ReservationState>,
            _target_id: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<PlacementReservationRecord>, Status> {
            unsupported()
        }
        async fn get_reservation(
            &self,
            _reservation_id: &str,
        ) -> Result<Option<PlacementReservationRecord>, Status> {
            unsupported()
        }
        async fn reserve_stripe_placement(
            &self,
            _reservation_id: String,
            _fragment_count: usize,
            _failure_domain: FailureDomain,
            _excluded_target_ids: Vec<String>,
            _reservation_ttl_ms: u64,
        ) -> Result<TimedStoreResult<PlacementReservationRecord>, Status> {
            unsupported()
        }
        async fn reserve_stripe_batch(
            &self,
            _batch_size: usize,
            _fragment_count: usize,
            _failure_domain: FailureDomain,
            _excluded_target_ids: Vec<String>,
            _reservation_ttl_ms: u64,
        ) -> Result<TimedStoreResult<Vec<PlacementReservationRecord>>, Status> {
            unsupported()
        }
        async fn claim_reservation_bin_batch(
            &self,
            _batch_size: usize,
            _fragment_count: usize,
            _failure_domain: FailureDomain,
            _reservation_ttl_ms: u64,
        ) -> Result<TimedStoreResult<Vec<PlacementReservationRecord>>, Status> {
            unsupported()
        }
        async fn top_up_reservation_bin(
            &self,
            _bin_key: &ReservationBinKey,
            _reservation_ttl_ms: u64,
            _low_watermark: usize,
            _high_watermark: usize,
            _top_up_chunk: usize,
        ) -> Result<TimedStoreResult<usize>, Status> {
            unsupported()
        }
        async fn reserve_rebuild_placement(
            &self,
            _reservation_id: String,
            _failed_target_id: String,
            _failure_domain: FailureDomain,
            _occupied_target_ids: Vec<String>,
        ) -> Result<TimedStoreResult<PlacementReservationRecord>, Status> {
            unsupported()
        }
        async fn reserve_replacement_placement(
            &self,
            _reservation_id: String,
            _replacement_count: usize,
            _failure_domain: FailureDomain,
            _excluded_target_ids: Vec<String>,
            _reservation_ttl_ms: u64,
            _required_target_ids: Vec<String>,
        ) -> Result<TimedStoreResult<PlacementReservationRecord>, Status> {
            unsupported()
        }
        async fn finalize_reservations(
            &self,
            _reservation_id: String,
            _placement_indexes: Vec<u32>,
        ) -> Result<PlacementReservationRecord, Status> {
            unsupported()
        }
        async fn finalize_reservations_batch(
            &self,
            _mutations: Vec<ReservationMutationSpec>,
        ) -> Result<Vec<PlacementReservationRecord>, Status> {
            unsupported()
        }
        async fn release_reservations(
            &self,
            _reservation_id: String,
            _placement_indexes: Vec<u32>,
        ) -> Result<PlacementReservationRecord, Status> {
            unsupported()
        }
        async fn release_reservations_batch(
            &self,
            _mutations: Vec<ReservationMutationSpec>,
        ) -> Result<Vec<PlacementReservationRecord>, Status> {
            unsupported()
        }
        async fn reclaim_target_granules(
            &self,
            _granules: Vec<TargetGranule>,
        ) -> Result<u64, Status> {
            unsupported()
        }
        async fn release_expired_reservations(
            &self,
            _now_ms: u64,
            _limit: usize,
        ) -> Result<usize, Status> {
            unsupported()
        }
    }

    fn unsupported<T>() -> Result<T, Status> {
        Err(Status::failed_precondition(
            "FoundationDB-backed KAS is only supported on Linux",
        ))
    }
}

pub(crate) use imp::*;
