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
        span_clear_begin: Vec<u8>,
        span_clear_end: Vec<u8>,
        chunk_writes: Vec<(Vec<u8>, Vec<u8>)>,
    }

    /// A single FoundationDB mutation. `persist_state` groups these into
    /// byte-bounded batches so no transaction exceeds the ~10 MB FDB limit.
    #[derive(Clone)]
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
        const ALLOCATOR_MUTATION_LEASE_TTL_MS: u64 = 120_000;
        const ALLOCATOR_MUTATION_RETRY_MS: u64 = 10;

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
            })
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
                self.persist_state(&[], &[], &delete_reservations, &[], &[], &[])
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

        async fn persist_state(
            &self,
            targets: &[TargetAllocatorState],
            reservations: &[PlacementReservationRecord],
            delete_reservations: &[String],
            service_instances: &[ServiceInstanceRecord],
            delete_bin_members: &[(String, String)],
            add_bin_members: &[(String, String)],
        ) -> Result<(), Status> {
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
                let span_prefix = target_span_prefix(id);
                let span_end = prefix_range_end(&span_prefix);
                let mut chunk_writes = Vec::new();
                for chunk in split_spans_into_chunks(id, &target.spans) {
                    chunk_writes
                        .push((target_span_chunk_key(id, chunk.chunk_index), encode_json(&chunk)?));
                }
                target_writes.push(TargetWrite {
                    record_key: target_key(id),
                    record_value: encode_json(&target.target)?,
                    span_clear_begin: span_prefix,
                    span_clear_end: span_end,
                    chunk_writes,
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
                let mut ops = Vec::with_capacity(2 + tw.chunk_writes.len());
                let mut bytes = tw.record_key.len() + tw.record_value.len();
                ops.push(WriteOp::ClearRange(tw.span_clear_begin, tw.span_clear_end));
                ops.push(WriteOp::Set(tw.record_key, tw.record_value));
                for (key, value) in tw.chunk_writes {
                    bytes += key.len() + value.len();
                    ops.push(WriteOp::Set(key, value));
                }
                units.push((ops, bytes));
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
                    self.commit_write_ops(std::mem::take(&mut batch)).await?;
                    batch_bytes = 0;
                }
                batch.extend(ops);
                batch_bytes = batch_bytes.saturating_add(bytes);
            }
            if !batch.is_empty() {
                self.commit_write_ops(batch).await?;
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
        async fn commit_write_ops(&self, ops: Vec<WriteOp>) -> Result<(), Status> {
            self.db
                .run(move |trx, _| {
                    let ops = ops.clone();
                    async move {
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

        async fn load_lease(
            &self,
            lease_name: &str,
        ) -> Result<Option<CoordinationLeaseRecord>, Status> {
            let key = coordination_lease_key(lease_name);
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
                .map(|bytes| decode_json::<CoordinationLeaseRecord>(&bytes))
                .transpose()
        }

        async fn ensure_target_state_current(&self) -> Result<(), Status> {
            let needs_refresh = {
                let state = self.state.lock().await;
                !state.target_state_loaded
            };
            if needs_refresh {
                return self.refresh_target_state_from_fdb().await;
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
            let needs_refresh = {
                let state = self.state.lock().await;
                !state.target_state_loaded || !state.reservation_state_loaded
            };
            if needs_refresh {
                return self.refresh_full_state_from_fdb().await;
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

        async fn acquire_allocator_mutation_lease(&self) -> Result<(), Status> {
            let lease_name = self.allocator_mutation_lease_name();
            loop {
                let now_ms = now_unix_ms();
                if let Some(existing) = self.load_lease(&lease_name).await? {
                    if existing.owner_id == self.owner_id.as_str()
                        && existing.expires_at_unix_ms > now_ms
                    {
                        let renew_before_ms = Self::ALLOCATOR_MUTATION_LEASE_TTL_MS / 2;
                        if existing.expires_at_unix_ms.saturating_sub(now_ms) <= renew_before_ms {
                            self.try_acquire_coordination_lease(
                                &lease_name,
                                self.owner_id.as_str(),
                                Self::ALLOCATOR_MUTATION_LEASE_TTL_MS,
                            )
                            .await?;
                        }
                        return Ok(());
                    }
                }
                if self
                    .try_acquire_coordination_lease(
                        &lease_name,
                        self.owner_id.as_str(),
                        Self::ALLOCATOR_MUTATION_LEASE_TTL_MS,
                    )
                    .await?
                {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(Self::ALLOCATOR_MUTATION_RETRY_MS)).await;
            }
        }

        async fn release_allocator_mutation_lease(&self) -> Result<(), Status> {
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
            self.persist_state(&dirty_targets, &reservations, &[], &[], &[], &[])
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
            let release_result = self.release_allocator_mutation_lease().await;
            match (result, release_result) {
                (Ok(value), Ok(())) => Ok(value),
                (Ok(_), Err(err)) => Err(err),
                (Err(err), Ok(())) => Err(err),
                (Err(err), Err(_)) => Err(err),
            }
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
            self.persist_state(&dirty_targets, &[], &[], &[], &[], &[])
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
            let stored = {
                let mut state = self.state.lock().await;
                let spans = state
                    .targets
                    .get(&target.target_id)
                    .map(|existing| existing.spans.clone())
                    .unwrap_or_else(|| {
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
                stored
            };
            self.persist_state(&[stored], &[], &[], &[], &[], &[])
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
            self.persist_state(&[entry], &[], &[], &[], &[], &[])
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
            self.persist_state(&[], &[], &[], &[instance.clone()], &[], &[])
                .await?;
            Ok(instance)
        }

        async fn try_acquire_coordination_lease(
            &self,
            lease_name: &str,
            owner_id: &str,
            lease_ttl_ms: u64,
        ) -> Result<bool, Status> {
            let now_ms = now_unix_ms();
            let expires_at_unix_ms = now_ms.saturating_add(lease_ttl_ms.max(1_000));
            let key = coordination_lease_key(lease_name);
            let owner_id = owner_id.to_string();
            let record = CoordinationLeaseRecord {
                owner_id: owner_id.clone(),
                expires_at_unix_ms,
            };
            let value = encode_json(&record)?;
            self.db
                .run(move |trx, _| {
                    let key = key.clone();
                    let owner_id = owner_id.clone();
                    let value = value.clone();
                    async move {
                        let current = trx
                            .get(&key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| bytes.as_ref().to_vec());
                        let available = match current {
                            Some(bytes) => {
                                let existing = decode_json::<CoordinationLeaseRecord>(&bytes)
                                    .map_err(status_to_fdb)?;
                                existing.owner_id == owner_id
                                    || existing.expires_at_unix_ms <= now_ms
                            }
                            None => true,
                        };
                        if available {
                            trx.set(&key, &value);
                        }
                        Ok::<bool, FdbBindingError>(available)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
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
            self.persist_state(&[entry], &[], &[], &[], &[], &[])
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
                self.ensure_full_state_current().await?;
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
                self.persist_state(&[], &claimed, &[], &[], &delete_bin_members, &[])
                    .await?;
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
                        self.persist_state(&[], &[], &[], &[], &[], &add_bin_members)
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
            self.persist_state(&dirty_targets, &[], &[], &[], &[], &[])
                .await?;
            Ok(reclaimed)
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
        ) -> Result<Self, Box<dyn Error>> {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "FoundationDB-backed KAS is only supported on Linux",
            )))
        }
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
