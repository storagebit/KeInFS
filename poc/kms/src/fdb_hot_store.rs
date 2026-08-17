// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

#[cfg(target_os = "linux")]
mod imp {
    use crate::fdb_schema::{
        bucket_context_key, cluster_salt_key, decode_pending_version_key_parts, decode_segments,
        ec_profile_key, namespace_entry_key, namespace_path_key, object_head_key,
        object_id_counter_key, object_lease_key, object_lease_range, object_version_chunk_key,
        object_version_chunk_prefix, object_version_key, occupancy_key_for_reverse_log_key,
        decode_reverse_log_key_version_id, decode_version_reclaim_fence_key_parts,
        pending_version_key, pending_version_object_range, pending_version_scan_range,
        pending_version_subspace_range, pending_version_target_key,
        target_current_fragment_key, target_current_fragment_version_range,
        target_inventory_key, target_inventory_range, target_reverse_log_key,
        target_reverse_log_scan_range, target_reverse_log_version_range,
        version_reclaim_fence_key, version_reclaim_fence_scan_range, write_intent_chunk_key,
        write_intent_chunk_prefix, write_intent_key, write_intent_range,
    };
    use crate::hot_store::{
        HotMetadataStore, OrphanReclaimDecision, OrphanTargetResume, OrphanVersionCandidate,
        OrphanVersionClaim, OrphanVersionCleanPage,
    };
    use crate::store::{
        apply_fragment_repair, auto_create_levels, build_finalize_plans,
        clear_target_current_fragment_index, decode_manifest_bytes,
        decode_pending_version_value, decode_write_intent_bytes, decode_write_lease_expiry,
        encode_manifest, encode_pending_version_value, encode_reverse_log_value,
        encode_write_intent, encode_write_lease_value, expected_fragment_count,
        classify_reverse_log_value, fragment_plans_for_window, is_reverse_log_reclaim_tombstone,
        join_path, mark_successful_fragments, normalize_object_key, normalize_write_intent,
        parse_version_id_parts, pending_version_claim_eligible, random_chunk_id, random_salt,
        write_target_current_fragment_index, AutoCreateLevel, BucketWriteContext,
        CommittedObjectWrite, CommittedObjectWriteWindow, DeletedObject, DeletedObjectVersion,
        PendingVersion, ReservedObjectWriteWindow, ReverseLogRowValue, StoredBucketWriteContext,
        TimedStoreResult, PENDING_VERSION_STATE_RECLAIMING, PENDING_VERSION_STATE_WRITING,
        REVERSE_LOG_RECLAIM_TOMBSTONE,
    };
    use foundationdb::{
        api::NetworkAutoStop, Database, FdbBindingError, RangeOption, RetryableTransaction,
    };
    use futures_util::StreamExt;
    use keinctl::proto::{
        EcProfile, FragmentPlan, FragmentRef, FragmentWriteState, FragmentWriteStatus,
        NamespaceDomainEntry, NamespaceEntryKind, ObjectHead, ObjectVersionManifest,
        PlacementReservationRecord, StripeManifest, TargetLifecycleState, TargetRecord,
        WriteIntent, WriteIntentState,
    };
    use prost::Message;
    use std::error::Error;
    use std::fmt::{Display, Formatter};
    use std::sync::Arc;
    use tonic::Status;
    use uuid::Uuid;

    #[derive(Clone)]
    pub(crate) struct FdbHotStore {
        db: DatabasePool,
        // Process-local block allocator for object_ids. BeginObject previously minted the id
        // with a read-modify-write on a single global counter key; that `get` put the hot key
        // in every transaction's read-conflict set, so concurrent BeginObjects conflicted and
        // FDB serialized them with backoff retries — the dominant per-object latency under
        // fan-out (measured ~280 ms avg / multi-second tail at 64 concurrent writers). Reserving
        // a block of ids per refill and handing them out from memory makes the hot-key
        // transaction ~OBJECT_ID_BLOCK_SIZE-rarer. Ids stay unique (blocks are disjoint) and
        // continue the existing global sequence (no collision with already-issued ids); a
        // process restart simply skips the unused tail of its block. Global monotonicity is not
        // required by any consumer — only uniqueness (the on-media identity and reverse-log key
        // just need a distinct object_id). `Arc` so cloned store handles share ONE block (the
        // allocator is process-wide); a non-shared clone would still be correct (disjoint
        // refills) but would waste a block per clone.
        object_id_block: Arc<tokio::sync::Mutex<ObjectIdBlock>>,
    }

    /// A reserved, half-open range `[next, end)` of object_ids the process hands out without
    /// touching FDB. `next == end` (the default) forces a refill on the next allocation.
    #[derive(Debug, Default)]
    struct ObjectIdBlock {
        next: u32,
        end: u32,
    }

    /// How many object_ids each refill reserves from the global counter in one transaction.
    /// Large enough that the hot-key refill transaction is rare (≈ one per this many writes
    /// per KMS process), small enough that an unused-block-on-restart gap is negligible.
    const OBJECT_ID_BLOCK_SIZE: u32 = 1024;

    #[derive(Debug)]
    struct StatusCarrier(Status);

    const CHUNKED_BLOB_META_MAGIC: &[u8; 8] = b"KFBLOB01";
    const MAX_FDB_BLOB_CHUNK_BYTES: usize = 80_000;
    // Conservative ceiling on the total bytes a single-shot commit writes in one FDB
    // transaction. FoundationDB's hard limit is 10 MB; staying under 9 MB leaves
    // headroom for key encoding and the transaction's own bookkeeping. Objects whose
    // commit would exceed this need the append-then-seal segmented manifest path.
    const MAX_SINGLE_SHOT_TXN_BYTES: usize = 9 * 1024 * 1024;
    // Estimated per-fragment cost of the writes the commit makes ON TOP OF the
    // manifest blob: a reverse-log key+value, the secondary-index/occupancy keys, and
    // the committed-occupancy value (each embedding the version_id/target_id strings).
    // Deliberately generous so the fail-fast guard trips before the real transaction
    // would breach the FDB limit. Recalibrated from 512: measured a 12 GiB object
    // (15360 fragments) at ~512 B/frag estimate ~7.9 MB (under the old 9 MB budget) PASS
    // the guard, then get rejected by FDB with error 2101 (txn > 10 MB) — so 512 was far
    // too low. At 1280 a 5 GiB object (6400 frags, the largest that empirically commits)
    // still passes (~8 MB) while 10/12 GiB trip the guard with a clean precondition error
    // instead of a cryptic FDB 2101. (Objects past this should use the segmented commit.)
    const SINGLE_SHOT_PER_FRAGMENT_TXN_BYTES: usize = 1280;
    // Estimated transaction cost of the pending-version marker rewrite every segment
    // append carries (key + value with head coordinates + conflict-range overhead).
    const PENDING_VERSION_MARKER_TXN_BYTES: usize = 512;
    // Estimated transaction cost of one per-target presence row (key + empty value +
    // overhead).
    const PRESENCE_ROW_TXN_BYTES: usize = 96;

    impl Display for StatusCarrier {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0.message())
        }
    }

    impl Error for StatusCarrier {}

    pub(crate) struct FdbNetworkGuard {
        _inner: NetworkAutoStop,
    }

    pub(crate) fn maybe_boot_network(
        client_threads: usize,
        external_client_dir: &str,
    ) -> Result<Option<FdbNetworkGuard>, Box<dyn Error>> {
        let inner = if client_threads > 1 {
            // The stock client funnels every operation of the whole process through
            // ONE libfdb network thread, which saturates at a few thousand
            // transactions per second — measured as the binding constraint on a busy
            // KMS while the cluster itself was idle. client_threads_per_version runs
            // this many client threads instead, each loading its own copy of the
            // client library from the external directory; libfdb assigns database
            // handles to threads round-robin at creation, so the store opens one
            // handle per thread and spreads transactions across all of them.
            use foundationdb::api::FdbApiBuilder;
            use foundationdb::options::NetworkOption;
            let network = FdbApiBuilder::default()
                .build()?
                .set_option(NetworkOption::ExternalClientDirectory(
                    external_client_dir.to_string(),
                ))?
                .set_option(NetworkOption::ClientThreadsPerVersion(
                    i32::try_from(client_threads).map_err(|_| "fdb client threads exceed i32")?,
                ))?;
            unsafe { network.boot()? }
        } else {
            unsafe { foundationdb::boot() }
        };
        Ok(Some(FdbNetworkGuard { _inner: inner }))
    }

    /// Round-robins transactions across one database handle per FDB client thread.
    /// libfdb assigns each created database to a client thread at creation, so N
    /// handles on an N-thread client give N independent network event loops. With
    /// the stock single-threaded client this holds exactly one handle and behaves
    /// like a plain `Database`.
    #[derive(Clone)]
    pub(crate) struct DatabasePool {
        handles: Arc<Vec<Database>>,
        cursor: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl DatabasePool {
        fn connect(cluster_file: &str, handles: usize) -> Result<Self, Box<dyn Error>> {
            let count = handles.max(1);
            let mut list = Vec::with_capacity(count);
            for _ in 0..count {
                list.push(if cluster_file.trim().is_empty() {
                    Database::default()?
                } else {
                    Database::from_path(cluster_file)?
                });
            }
            Ok(Self {
                handles: Arc::new(list),
                cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            })
        }

        pub(crate) async fn run<F, Fut, T>(&self, closure: F) -> Result<T, FdbBindingError>
        where
            F: Fn(RetryableTransaction, foundationdb::MaybeCommitted) -> Fut,
            Fut: std::future::Future<Output = Result<T, FdbBindingError>>,
        {
            let index = self
                .cursor
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.handles[index % self.handles.len()].run(closure).await
        }
    }

    impl FdbHotStore {
        pub(crate) fn connect(
            cluster_file: &str,
            client_threads: usize,
        ) -> Result<Self, Box<dyn Error>> {
            Ok(Self {
                db: DatabasePool::connect(cluster_file, client_threads)?,
                object_id_block: Arc::new(tokio::sync::Mutex::new(ObjectIdBlock::default())),
            })
        }

        /// Hands out a unique object_id from the process-local reserved block, refilling it
        /// from the global counter only once per `OBJECT_ID_BLOCK_SIZE` ids. The refill is the
        /// ONLY transaction that touches the hot counter key (read-modify-write); amortized one
        /// per block, it no longer serializes concurrent BeginObjects. Ids continue the global
        /// sequence, so they never collide with previously issued ids.
        async fn allocate_object_id(&self) -> Result<u32, Status> {
            let mut block = self.object_id_block.lock().await;
            if block.next >= block.end {
                let counter_key = object_id_counter_key();
                // Reserve [last+1, last+BLOCK]; the counter keeps recording the last issued id,
                // exactly as the prior per-id read-modify-write did, so the on-disk sequence is
                // continuous across the change.
                let last_issued = self
                    .db
                    .run(move |trx, _| {
                        let counter_key = counter_key.clone();
                        async move {
                            let current = trx
                                .get(&counter_key, false)
                                .await
                                .map_err(FdbBindingError::from)?
                                .map(|bytes| {
                                    let mut raw = [0u8; 4];
                                    let src = bytes.as_ref();
                                    let n = src.len().min(4);
                                    raw[..n].copy_from_slice(&src[..n]);
                                    u32::from_le_bytes(raw)
                                })
                                .unwrap_or(0);
                            // Exhaustion is a hard stop, never silent reuse: everything
                            // downstream (blind marker/lease mints, version_id keyspaces,
                            // reclaiming-is-terminal) rests on ids never repeating.
                            if current >= u32::MAX - OBJECT_ID_BLOCK_SIZE {
                                return Err(status_to_fdb(Status::resource_exhausted(
                                    "the u32 object_id space is exhausted; widening the id is a proto migration",
                                )));
                            }
                            trx.set(
                                &counter_key,
                                &(current + OBJECT_ID_BLOCK_SIZE).to_le_bytes(),
                            );
                            Ok(current)
                        }
                    })
                    .await
                    .map_err(map_fdb_binding_error)?;
                block.next = last_issued + 1;
                block.end = last_issued + 1 + OBJECT_ID_BLOCK_SIZE;
            }
            let id = block.next;
            block.next = block.next.saturating_add(1);
            Ok(id)
        }
        async fn load_bucket_context(
            &self,
            bucket_id: &str,
        ) -> Result<Option<BucketWriteContext>, Status> {
            let key = bucket_context_key(bucket_id);
            let value = self
                .db
                .run(move |trx, _| {
                    let key = key.clone();
                    async move {
                        let value = trx.get(&key, false).await.map_err(FdbBindingError::from)?;
                        Ok(value.map(|bytes| bytes.as_ref().to_vec()))
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            value
                .map(|bytes| {
                    serde_json::from_slice::<StoredBucketWriteContext>(&bytes)
                        .map(BucketWriteContext::from)
                        .map_err(|err| {
                            Status::internal(format!(
                                "failed to decode FoundationDB bucket context for {bucket_id}: {err}"
                            ))
                        })
                })
                .transpose()
        }

        async fn load_write_intents(&self) -> Result<Vec<WriteIntent>, Status> {
            let (begin, end) = write_intent_range();
            let encoded = self
                .db
                .run(move |trx, _| {
                    let begin = begin.clone();
                    let end = end.clone();
                    async move {
                        let mut stream = trx.get_ranges_keyvalues((begin, end).into(), false);
                        let mut keys = Vec::new();
                        while let Some(next) = stream.next().await {
                            let kv = next?;
                            keys.push(kv.key().to_vec());
                        }
                        Ok::<Vec<Vec<u8>>, FdbBindingError>(keys)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            let mut intents = Vec::with_capacity(encoded.len());
            for key in encoded {
                let (_, segments) = decode_segments(&key).map_err(|err| {
                    Status::internal(format!(
                        "failed to decode FoundationDB write intent key: {err}"
                    ))
                })?;
                let intent_id = segments
                    .first()
                    .cloned()
                    .ok_or_else(|| Status::internal("write intent key is missing intent id"))?;
                let Some(bytes) = self
                    .db
                    .run(move |trx, _| {
                        let key = key.clone();
                        let intent_id = intent_id.clone();
                        async move {
                            load_blob(&trx, &key, |chunk_index| {
                                write_intent_chunk_key(&intent_id, chunk_index)
                            })
                            .await
                        }
                    })
                    .await
                    .map_err(map_fdb_binding_error)?
                else {
                    continue;
                };
                let mut intent = decode_write_intent_bytes(&bytes)?;
                normalize_write_intent(&mut intent)?;
                intents.push(intent);
            }
            Ok(intents)
        }
    }

    // ── Shared single-shot commit machinery (used by both the single and batched paths) ──
    // One object's commit is split into three phases so its logic is identical whether it
    // commits alone or inside a MultiCommit batch:
    //   • validate — pre-transaction checks (no FDB): key normalization + the u16 on-media
    //     identity-coordinate bounds the lease-fenced GC depends on.
    //   • pass1 — every read + CAS + the reverse-log reclaim-fence reads + prepare, emitting
    //     NO writes, so a batch runs pass1 for ALL objects before any write and the
    //     read-conflict set stays complete (the HARD INVARIANT the GC fence relies on).
    //   • pass2 — the writes (manifest blob, reverse log, indexes, head flip, lease clear).
    // A logical rejection (CAS miss, create-only clash, reclaimed granule, oversized txn) is
    // a `Rejected` value, NOT a transaction error: the single path turns it into an `Err`,
    // the batch records it per-object and still commits the survivors in the one txn.
    #[derive(Clone)]
    struct ValidatedSingleShotCommit {
        manifest: ObjectVersionManifest,
        head_key: Vec<u8>,
        object_id: u32,
        object_version: u32,
        expected_prior_version: u32,
        parent_entry_id: String,
        parent_path: String,
        topology_epoch: u64,
        omit_manifest: bool,
        seals_segments: bool,
        // The pending-version marker must be live-writing (seal rule extended to
        // unary decentralized commits that ran BeginObject).
        marker_required: bool,
        // Replace the per-reverse-log-row tombstone reads with the per-version
        // reclaim-fence read. Only honored when the marker is required: the fence
        // pair (marker live-writing + fence absent) is what makes skipping the
        // per-row reads sound.
        marker_fenced: bool,
    }

    struct SingleShotWritePlan {
        head_key: Vec<u8>,
        head: ObjectHead,
        manifest: ObjectVersionManifest,
        manifest_bytes: Vec<u8>,
        object_entry_json: Vec<u8>,
        object_entry_path: String,
        object_id: u32,
        object_version: u32,
        marker_present: bool,
        omit_manifest: bool,
    }

    enum SingleShotPass1 {
        WriteReady(Box<SingleShotWritePlan>),
        Idempotent(ObjectHead),
        Rejected(Status),
    }

    fn validate_single_shot_commit(
        commit: crate::hot_store::SingleShotCommit,
    ) -> Result<ValidatedSingleShotCommit, Status> {
        let mut manifest = commit.manifest;
        manifest.key = normalize_object_key(&manifest.key)?;
        let head_key = object_head_key(&manifest.bucket_id, &manifest.key);
        let (object_id, object_version) = parse_version_id_parts(&manifest.version_id)?;
        const IDENTITY_COORD_MAX: u32 = u16::MAX as u32;
        if object_version > IDENTITY_COORD_MAX {
            return Err(Status::failed_precondition(format!(
                "single-shot commit object_version {object_version} for {}/{} exceeds the {IDENTITY_COORD_MAX} on-media identity limit",
                manifest.bucket_id, manifest.key
            )));
        }
        for stripe in &manifest.stripes {
            for fragment in &stripe.fragments {
                if fragment.stripe_index > IDENTITY_COORD_MAX
                    || fragment.fragment_index > IDENTITY_COORD_MAX
                {
                    return Err(Status::failed_precondition(format!(
                        "single-shot commit fragment identity (stripe {}, fragment {}) for {}/{} exceeds the {IDENTITY_COORD_MAX} on-media identity limit",
                        fragment.stripe_index, fragment.fragment_index, manifest.bucket_id, manifest.key
                    )));
                }
            }
        }
        let marker_required = commit.seals_segments || commit.require_pending_version_marker;
        Ok(ValidatedSingleShotCommit {
            manifest,
            head_key,
            object_id,
            object_version,
            expected_prior_version: commit.expected_prior_version,
            parent_entry_id: commit.parent_entry_id,
            parent_path: commit.parent_path,
            topology_epoch: commit.topology_epoch,
            omit_manifest: commit.omit_manifest,
            seals_segments: commit.seals_segments,
            marker_required,
            marker_fenced: commit.marker_fenced && marker_required,
        })
    }

    async fn single_shot_pass1(
        trx: &RetryableTransaction,
        vc: &ValidatedSingleShotCommit,
    ) -> Result<SingleShotPass1, FdbBindingError> {
        let manifest = &vc.manifest;
        // Point reads BEFORE any write (HARD INVARIANT). The head, marker, and — in
        // fenced mode — reclaim-fence gets are ISSUED concurrently (the FDB client
        // pipelines them, one round trip) but EVALUATED strictly in the order below:
        // head idempotency/CAS first, then the marker requirement, then the fence.
        // A retried commit whose version already won must return success before any
        // marker rule can reject it (its marker died with its own head flip). The
        // extra keys join the read-conflict set on every outcome; short-circuit
        // outcomes (idempotent, rejected) write nothing, so a read-only transaction
        // cannot conflict-abort and the added reads change no semantics.
        let marker_key = pending_version_key(vc.object_id, vc.object_version);
        let (prior_bytes, marker_bytes, fence_bytes) = if vc.marker_fenced {
            let fence_key = version_reclaim_fence_key(vc.object_id, vc.object_version);
            let (prior, marker, fence) = futures_util::future::try_join3(
                trx.get(&vc.head_key, false),
                trx.get(&marker_key, false),
                trx.get(&fence_key, false),
            )
            .await
            .map_err(FdbBindingError::from)?;
            (prior, marker, fence)
        } else {
            let (prior, marker) = futures_util::future::try_join(
                trx.get(&vc.head_key, false),
                trx.get(&marker_key, false),
            )
            .await
            .map_err(FdbBindingError::from)?;
            (prior, marker, None)
        };
        let prior = prior_bytes
            .map(|bytes| decode_object_head(bytes.as_ref()))
            .transpose()
            .map_err(status_to_fdb)?;
        // Idempotent success: a retried commit whose own version already won.
        if let Some(existing) = &prior {
            if existing.current_version_id == manifest.version_id {
                return Ok(SingleShotPass1::Idempotent(existing.clone()));
            }
        }
        let witnessed = prior.as_ref().map(|head| head.version).unwrap_or(0);
        if witnessed != vc.expected_prior_version {
            return Ok(SingleShotPass1::Rejected(Status::failed_precondition(format!(
                "single-shot commit version mismatch for {}/{}: live head version {} != expected {}",
                manifest.bucket_id, manifest.key, witnessed, vc.expected_prior_version
            ))));
        }
        // Create-only: overwriting needs supersede + GC of the prior version's granules.
        if prior.is_some() {
            return Ok(SingleShotPass1::Rejected(Status::failed_precondition(format!(
                "single-shot commit is create-only and {}/{} already exists",
                manifest.bucket_id, manifest.key
            ))));
        }
        // Pending-version marker fence. For a segmented SEAL — and for any commit with
        // the marker required (the decentralized path, which always ran BeginObject) —
        // the marker is the one key this transaction and the orphan-version reaper's
        // claim both touch: the claim rewrites it, this read conflicts, FDB lets exactly
        // one win — without it a seal and a reap of the same version could BOTH commit
        // (the seal's stripes are empty, so the reverse-log rendezvous below is a no-op)
        // and the GC would later free a live head's granules. A unary commit WITHOUT the
        // marker requirement tolerates an absent marker (tools may commit without
        // BeginObject, and a post-reap stale unary commit is independently safe: it
        // reads every reverse-log row it writes, so it either dies on a reclaim
        // tombstone or atomically re-establishes the rows the GC serializes against) —
        // but never commits while a claim is mid-clean.
        let marker_state = match marker_bytes {
            Some(bytes) => match decode_pending_version_value(bytes.as_ref()) {
                Ok(pending) => Some(pending.state),
                Err(status) => return Ok(SingleShotPass1::Rejected(status)),
            },
            None => None,
        };
        let reclaimed = Status::failed_precondition(format!(
            "commit for {}/{} version {} cannot proceed: the version was (or is being) \
             reclaimed after its write lease lapsed{}; the object must be rewritten under \
             a fresh write",
            manifest.bucket_id,
            manifest.key,
            manifest.version_id,
            if vc.marker_required {
                " (or was never begun)"
            } else {
                ""
            }
        ));
        match marker_state {
            Some(state) if state != PENDING_VERSION_STATE_WRITING => {
                return Ok(SingleShotPass1::Rejected(reclaimed));
            }
            None if vc.marker_required => {
                return Ok(SingleShotPass1::Rejected(reclaimed));
            }
            _ => {}
        }
        let marker_present = marker_state.is_some();
        if vc.marker_fenced {
            // Per-version reclaim fence (required ABSENT): the granule GC's authorize
            // path stamps this key in the same transaction as any reclaim tombstone it
            // writes for the version, so fence-present means at least one of this
            // version's granules is gone and the version can never become a live head.
            // Reading it here makes the O(1) rendezvous two-sided: authorize-first is
            // seen directly; commit-first read-conflicts a racing authorize. This
            // replaces the O(fragments) per-row reads below — a blind pass2 write can
            // never bury a tombstone, because the tombstone's transaction set the fence.
            // No expiry arithmetic: the commit rejects only because GC ACTED, never
            // because time passed, so a slow-but-alive writer still commits.
            if fence_bytes.is_some() {
                return Ok(SingleShotPass1::Rejected(Status::failed_precondition(format!(
                    "commit for {}/{} version {} cannot proceed: a granule of this version \
                     was reclaimed by GC after its write lease lapsed; the object must be \
                     rewritten under a fresh write",
                    manifest.bucket_id, manifest.key, manifest.version_id
                ))));
            }
        } else {
            // Lease-fenced GC rendezvous (READ before any write): read every reverse-log
            // row this commit will write, adding each to the read-conflict set so a
            // concurrent reclaiming GC forces exactly one of the two to abort. The gets
            // are issued CONCURRENTLY — the FDB client pipelines them over one
            // connection, collapsing O(fragments) sequential round-trips into ~one;
            // every key still joins the read-conflict set identically, so the fence is
            // byte-for-byte the same.
            let fence_reads = futures_util::future::try_join_all(
                manifest.stripes.iter().flat_map(|stripe| {
                    stripe.fragments.iter().map(|fragment| {
                        let rl_key = target_reverse_log_key(
                            &fragment.target_id,
                            &manifest.version_id,
                            fragment.stripe_index,
                            fragment.fragment_index,
                        );
                        async move {
                            let value =
                                trx.get(&rl_key, false).await.map_err(FdbBindingError::from)?;
                            Ok::<_, FdbBindingError>((fragment, value))
                        }
                    })
                }),
            )
            .await?;
            for (fragment, value) in fence_reads {
                if let Some(existing) = value {
                    if is_reverse_log_reclaim_tombstone(existing.as_ref()) {
                        return Ok(SingleShotPass1::Rejected(Status::failed_precondition(format!(
                            "single-shot commit for {}/{} cannot proceed: the granule for stripe {} fragment {} on target {} was reclaimed by GC; the fragment bytes are gone and the object must be rewritten",
                            manifest.bucket_id, manifest.key, fragment.stripe_index, fragment.fragment_index, fragment.target_id
                        ))));
                    }
                }
            }
        }
        // Prepare the writes (still NO writes issued).
        let mut manifest = manifest.clone();
        if manifest.object_entry_id.is_empty() {
            manifest.object_entry_id = Uuid::new_v4().to_string();
        }
        let manifest_bytes = if vc.omit_manifest {
            Vec::new()
        } else {
            encode_manifest(&manifest)
        };
        let fragment_count: usize = manifest.stripes.iter().map(|s| s.fragments.len()).sum();
        let estimated_txn_bytes = manifest_bytes
            .len()
            .saturating_add(fragment_count.saturating_mul(SINGLE_SHOT_PER_FRAGMENT_TXN_BYTES));
        if estimated_txn_bytes > MAX_SINGLE_SHOT_TXN_BYTES {
            return Ok(SingleShotPass1::Rejected(Status::failed_precondition(format!(
                "single-shot commit for {}/{} would write ~{} bytes across {} fragments, over the {}-byte transaction budget",
                manifest.bucket_id, manifest.key, estimated_txn_bytes, fragment_count, MAX_SINGLE_SHOT_TXN_BYTES
            ))));
        }
        let object_name = manifest
            .key
            .rsplit('/')
            .next()
            .unwrap_or(manifest.key.as_str())
            .to_string();
        let object_path = join_path(&vc.parent_path, &object_name);
        let object_entry = NamespaceDomainEntry {
            entry_id: manifest.object_entry_id.clone(),
            namespace_id: manifest.namespace_id.clone(),
            parent_entry_id: vc.parent_entry_id.clone(),
            name: object_name,
            kind: NamespaceEntryKind::Object as i32,
            path: object_path.clone(),
            size_bytes: manifest.logical_length_bytes,
        };
        let object_entry_json = serde_json::to_vec(&object_entry).map_err(|err| {
            status_to_fdb(Status::internal(format!(
                "failed to encode namespace entry JSON payload: {err}"
            )))
        })?;
        let head = ObjectHead {
            object_entry_id: manifest.object_entry_id.clone(),
            current_version_id: manifest.version_id.clone(),
            revision: 0,
            version: vc.expected_prior_version + 1,
            logical_length_bytes: manifest.logical_length_bytes,
            ec_profile_id: manifest.ec_profile_id.clone(),
            topology_epoch: vc.topology_epoch,
        };
        Ok(SingleShotPass1::WriteReady(Box::new(SingleShotWritePlan {
            head_key: vc.head_key.clone(),
            head,
            manifest,
            manifest_bytes,
            object_entry_json,
            object_entry_path: object_path,
            object_id: vc.object_id,
            object_version: vc.object_version,
            marker_present,
            omit_manifest: vc.omit_manifest,
        })))
    }

    /// Resolves — auto-creating any missing levels of — the parent collection for
    /// `normalized_key`, inside the caller's transaction, so the resolved parent and
    /// the caller's own writes commit atomically. A caller-supplied hint is re-validated
    /// against the path index IN THIS TRANSACTION before use: the hint comes from an
    /// in-memory service cache that is NOT invalidated when a parent collection is
    /// deleted, and trusting it blindly would commit the object under a dangling parent
    /// id. On mismatch the hint is dropped and the walk re-materializes the parent.
    ///
    /// Slashed keys (e.g. "a/b/c.txt") walk every path segment from the bucket root
    /// down and auto-create any missing collection entry, shallow -> deep, so parents
    /// exist before their children. Each level resolves via the namespace path index
    /// with a single point get (minimal read-conflict footprint; the index is maintained
    /// in the same transactions that mutate namespace entries). A level that resolves to
    /// an existing OBJECT entry is rejected — the path index is shared across bucket,
    /// collection and object entries, and reusing a file as a directory would nest an
    /// object under another object.
    async fn resolve_or_create_object_parent(
        trx: &RetryableTransaction,
        namespace_id: &str,
        normalized_key: &str,
        bucket_entry_id: &str,
        bucket_path: &str,
        parent_hint: Option<(String, String)>,
    ) -> Result<(String, String), FdbBindingError> {
        let parent_hint = if let Some((parent_entry_id, parent_path)) = parent_hint {
            let index_id = trx
                .get(&namespace_path_key(namespace_id, &parent_path), false)
                .await
                .map_err(FdbBindingError::from)?
                .map(|bytes| {
                    String::from_utf8(bytes.as_ref().to_vec()).map_err(|err| {
                        status_to_fdb(Status::internal(format!(
                            "namespace path index value is not valid utf-8: {err}"
                        )))
                    })
                })
                .transpose()?;
            if index_id.as_deref() == Some(parent_entry_id.as_str()) {
                Some((parent_entry_id, parent_path))
            } else {
                None
            }
        } else {
            None
        };
        if let Some((parent_entry_id, parent_path)) = parent_hint {
            return Ok((parent_entry_id, parent_path));
        }
        let Some((parent_key, _)) = normalized_key.rsplit_once('/') else {
            return Ok((bucket_entry_id.to_string(), bucket_path.to_string()));
        };
        let mut parent_id = bucket_entry_id.to_string();
        let mut parent_path = bucket_path.to_string();
        // Pure level synthesis (prefix accumulation, level_path, deterministic id)
        // lives in store::auto_create_levels and is unit-tested; the FDB reads and
        // writes per level stay here.
        for level in auto_create_levels(bucket_entry_id, bucket_path, parent_key) {
            let AutoCreateLevel {
                segment,
                prefix: _,
                level_path,
                deterministic_id,
            } = level;
            let level_index_key = namespace_path_key(namespace_id, &level_path);
            // Idempotency: reuse an existing collection id if the path index already
            // maps this level; never overwrite an established entry.
            let existing_id = trx
                .get(&level_index_key, false)
                .await
                .map_err(FdbBindingError::from)?
                .map(|bytes| {
                    String::from_utf8(bytes.as_ref().to_vec()).map_err(|err| {
                        status_to_fdb(Status::internal(format!(
                            "namespace path index value is not valid utf-8: {err}"
                        )))
                    })
                })
                .transpose()?;
            let level_id = if let Some(id) = existing_id {
                let entry_bytes = trx
                    .get(&namespace_entry_key(namespace_id, &id), false)
                    .await
                    .map_err(FdbBindingError::from)?;
                if let Some(entry_bytes) = entry_bytes {
                    let entry =
                        serde_json::from_slice::<NamespaceDomainEntry>(entry_bytes.as_ref())
                            .map_err(|err| {
                                status_to_fdb(Status::internal(format!(
                                    "failed to decode namespace entry JSON payload: {err}"
                                )))
                            })?;
                    if entry.kind == NamespaceEntryKind::Object as i32 {
                        return Err(status_to_fdb(Status::failed_precondition(format!(
                            "cannot create object under non-directory path component {level_path}"
                        ))));
                    }
                }
                id
            } else {
                let id = deterministic_id;
                let collection = NamespaceDomainEntry {
                    entry_id: id.clone(),
                    namespace_id: namespace_id.to_string(),
                    parent_entry_id: parent_id.clone(),
                    name: segment,
                    // Collection is the directory-kind entry; kfc maps any non-Object
                    // kind to a directory.
                    kind: NamespaceEntryKind::Collection as i32,
                    path: level_path.clone(),
                    size_bytes: 0,
                };
                trx.set(
                    &namespace_entry_key(namespace_id, &id),
                    &serde_json::to_vec(&collection).map_err(|err| {
                        status_to_fdb(Status::internal(format!(
                            "failed to encode namespace entry JSON payload: {err}"
                        )))
                    })?,
                );
                // Maintain the path -> entry_id index in the same transaction.
                trx.set(&level_index_key, id.as_bytes());
                id
            };
            parent_id = level_id;
            parent_path = level_path;
        }
        Ok((parent_id, parent_path))
    }

    fn single_shot_pass2(trx: &RetryableTransaction, plan: &SingleShotWritePlan) {
        let manifest = &plan.manifest;
        if !plan.omit_manifest {
            store_blob(
                trx,
                &object_version_key(&manifest.version_id),
                &object_version_chunk_prefix(&manifest.version_id),
                |chunk_index| object_version_chunk_key(&manifest.version_id, chunk_index),
                &plan.manifest_bytes,
            );
        }
        for stripe in &manifest.stripes {
            for fragment in &stripe.fragments {
                trx.set(
                    &target_reverse_log_key(
                        &fragment.target_id,
                        &manifest.version_id,
                        fragment.stripe_index,
                        fragment.fragment_index,
                    ),
                    &encode_reverse_log_value(fragment.generation, fragment.granule_index),
                );
            }
        }
        write_target_current_fragment_index(trx, manifest);
        trx.set(
            &namespace_entry_key(&manifest.namespace_id, &manifest.object_entry_id),
            &plan.object_entry_json,
        );
        trx.set(
            &namespace_path_key(&manifest.namespace_id, &plan.object_entry_path),
            manifest.object_entry_id.as_bytes(),
        );
        // CAS head-flip last: the object becomes resolvable only once every fragment, the
        // reverse log, and the entry are durable. Clear the lease in the SAME txn so the
        // granules pass straight from lease-protected to head-protected with no gap.
        trx.set(&plan.head_key, &encode_object_head(&plan.head));
        trx.clear(&object_lease_key(plan.object_id));
        // The pending-version marker (and its presence rows) dies with the head flip:
        // a live head's version never has a marker. Versions committed without a
        // BeginObject have no marker and mutate this keyspace not at all.
        if plan.marker_present {
            let (marker_begin, marker_end) =
                pending_version_subspace_range(plan.object_id, plan.object_version);
            trx.clear_range(&marker_begin, &marker_end);
        }
    }

    #[tonic::async_trait]
    impl HotMetadataStore for FdbHotStore {
        async fn get_bucket_write_context(
            &self,
            bucket_id: String,
        ) -> Result<BucketWriteContext, Status> {
            self.load_bucket_context(&bucket_id)
                .await?
                .ok_or_else(|| Status::not_found(format!("unknown bucket {}", bucket_id)))
        }

        async fn mint_object_id(
            &self,
            bucket_id: &str,
            key: &str,
        ) -> Result<(u32, u32), Status> {
            let normalized_key = normalize_object_key(key)?;
            // object_id comes from the process-local block allocator — conflict-free, unlike the
            // old per-id read-modify-write on the shared counter that serialized BeginObject.
            // It is independent of the version below (the version is per-(bucket,key) and never
            // collides across objects), so allocating it outside the version transaction loses
            // no invariant; a version-read retry simply re-reads the head, it does not re-mint
            // the id.
            let object_id = self.allocate_object_id().await?;
            let head_key = object_head_key(bucket_id, &normalized_key);
            let version = self
                .db
                .run(move |trx, _| {
                    let head_key = head_key.clone();
                    async move {
                        // version = prior head revision + 1 (1 if the object is new).
                        let version = trx
                            .get(&head_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| decode_object_head(bytes.as_ref()))
                            .transpose()
                            .map_err(status_to_fdb)?
                            .map(|head| head.version.saturating_add(1))
                            .unwrap_or(1);
                        Ok(version)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            Ok((object_id, version))
        }

        async fn prepare_and_create_write_intent(
            &self,
            mut intent: WriteIntent,
            bucket_entry_id: String,
            bucket_path: String,
            parent_hint: Option<(String, String)>,
        ) -> Result<TimedStoreResult<WriteIntent>, Status> {
            intent.key = normalize_object_key(&intent.key)?;
            normalize_write_intent(&mut intent)?;
            let normalized_key = intent.key.clone();
            let intent_key = write_intent_key(&intent.intent_id);
            let object_head_key = object_head_key(&intent.bucket_id, &normalized_key);

            let created = self
                .db
                .run(move |trx, _| {
                    let mut intent = intent.clone();
                    let normalized_key = normalized_key.clone();
                    let intent_key = intent_key.clone();
                    let object_head_key = object_head_key.clone();
                    let bucket_entry_id = bucket_entry_id.clone();
                    let bucket_path = bucket_path.clone();
                    let parent_hint = parent_hint.clone();
                    let intent_id = intent.intent_id.clone();
                    async move {
                        if let Some(existing) = load_blob(&trx, &intent_key, |chunk_index| {
                            write_intent_chunk_key(&intent_id, chunk_index)
                        })
                        .await?
                        {
                            let mut existing =
                                decode_write_intent_bytes(&existing).map_err(status_to_fdb)?;
                            normalize_write_intent(&mut existing).map_err(status_to_fdb)?;
                            return Ok(existing);
                        }

                        let object_name = normalized_key.rsplit('/').next().ok_or_else(|| {
                            status_to_fdb(Status::invalid_argument("object key must not be empty"))
                        })?;
                        let parent_context = resolve_or_create_object_parent(
                            &trx,
                            &intent.namespace_id,
                            &normalized_key,
                            &bucket_entry_id,
                            &bucket_path,
                            parent_hint,
                        )
                        .await?;

                        let existing_head = trx
                            .get(&object_head_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| decode_object_head(bytes.as_ref()).map_err(status_to_fdb))
                            .transpose()?;
                        intent.object_entry_id = existing_head
                            .map(|head| head.object_entry_id)
                            .unwrap_or_else(|| Uuid::new_v4().to_string());
                        intent.parent_entry_id = parent_context.0;
                        intent.parent_path = parent_context.1;
                        let intent_bytes = encode_write_intent(&intent);
                        store_blob(
                            &trx,
                            &intent_key,
                            &write_intent_chunk_prefix(&intent.intent_id),
                            |chunk_index| write_intent_chunk_key(&intent.intent_id, chunk_index),
                            &intent_bytes,
                        );
                        let _ = object_name;
                        Ok(intent)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;

            Ok(TimedStoreResult {
                value: created,
                phase_timings: Vec::new(),
            })
        }

        async fn list_write_intents(&self) -> Result<Vec<WriteIntent>, Status> {
            self.load_write_intents().await
        }

        async fn get_write_intent(&self, intent_id: String) -> Result<Option<WriteIntent>, Status> {
            let key = write_intent_key(&intent_id);
            let value = self
                .db
                .run(move |trx, _| {
                    let key = key.clone();
                    let intent_id = intent_id.clone();
                    async move {
                        load_blob(&trx, &key, |chunk_index| {
                            write_intent_chunk_key(&intent_id, chunk_index)
                        })
                        .await
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            value
                .map(|bytes| {
                    let mut intent = decode_write_intent_bytes(&bytes)?;
                    normalize_write_intent(&mut intent)?;
                    Ok(intent)
                })
                .transpose()
        }

        async fn reserve_object_write_window(
            &self,
            intent_id: String,
            start_stripe_index: u32,
            reservations: Vec<PlacementReservationRecord>,
        ) -> Result<TimedStoreResult<ReservedObjectWriteWindow>, Status> {
            let key = write_intent_key(&intent_id);
            let value = self
                .db
                .run(move |trx, _| {
                    let key = key.clone();
                    let reservations = reservations.clone();
                    let intent_id = intent_id.clone();
                    async move {
                        let bytes = load_blob(&trx, &key, |chunk_index| {
                            write_intent_chunk_key(&intent_id, chunk_index)
                        })
                        .await?
                        .ok_or_else(|| {
                            status_to_fdb(Status::not_found(format!(
                                "unknown write intent {}",
                                intent_id
                            )))
                        })?;
                        let mut intent =
                            decode_write_intent_bytes(&bytes).map_err(status_to_fdb)?;
                        normalize_write_intent(&mut intent).map_err(status_to_fdb)?;
                        if intent.state != WriteIntentState::Reserved as i32 {
                            return Err(status_to_fdb(Status::failed_precondition(format!(
                                "write intent {} is not reservable in state {}",
                                intent.intent_id, intent.state
                            ))));
                        }
                        if reservations.is_empty() {
                            return Err(status_to_fdb(Status::invalid_argument(
                                "ReserveObjectWriteWindow requires at least one reservation",
                            )));
                        }
                        let fragment_count = reservations[0].placements.len();
                        if fragment_count == 0 {
                            return Err(status_to_fdb(Status::invalid_argument(
                                "write window reservations must contain at least one placement",
                            )));
                        }
                        for reservation in &reservations {
                            if reservation.placements.len() != fragment_count {
                                return Err(status_to_fdb(Status::invalid_argument(format!(
                                    "write window reservations must all have {} placements",
                                    fragment_count
                                ))));
                            }
                        }
                        let start = start_stripe_index as usize;
                        let end = start.saturating_add(reservations.len());
                        let total_stripes = usize::try_from(intent.stripe_count).map_err(|_| {
                            status_to_fdb(Status::internal(format!(
                                "write intent {} declares an unsupported stripe count {}",
                                intent.intent_id, intent.stripe_count
                            )))
                        })?;
                        if start >= total_stripes || end > total_stripes {
                            return Err(status_to_fdb(Status::invalid_argument(format!(
                                "write window {}..{} is out of range for intent {} with {} stripes",
                                start, end, intent.intent_id, total_stripes
                            ))));
                        }

                        let existing = fragment_plans_for_window(
                            &intent,
                            start_stripe_index,
                            reservations.len(),
                        );
                        if !existing.is_empty() {
                            let expected = reservations.len().saturating_mul(fragment_count);
                            if existing.len() != expected {
                                return Err(status_to_fdb(Status::failed_precondition(format!(
                                    "write window {}..{} for intent {} is partially planned",
                                    start, end, intent.intent_id
                                ))));
                            }
                            return Ok(ReservedObjectWriteWindow {
                                fragment_plans: existing,
                                used_reservations: false,
                            });
                        }

                        let mut window_plans =
                            Vec::with_capacity(reservations.len().saturating_mul(fragment_count));
                        for (stripe_offset, reservation) in reservations.iter().enumerate() {
                            let stripe_index = start_stripe_index + stripe_offset as u32;
                            for (placement_index, placement) in
                                reservation.placements.iter().enumerate()
                            {
                                let reservation_id = if placement.reservation_id.is_empty() {
                                    reservation.reservation_id.clone()
                                } else {
                                    placement.reservation_id.clone()
                                };
                                let reservation_placement_index =
                                    if placement.reservation_id.is_empty() {
                                        placement_index as u32
                                    } else {
                                        placement.reservation_placement_index
                                    };
                                let fragment_index =
                                    if placement.fragment_index == 0 && placement_index > 0 {
                                        placement_index as u32
                                    } else {
                                        placement.fragment_index
                                    };
                                if !reservation_id.is_empty()
                                    && !intent
                                        .reservation_ids
                                        .iter()
                                        .any(|id| id == &reservation_id)
                                {
                                    intent.reservation_ids.push(reservation_id.clone());
                                }
                                if intent.reservation_id.is_empty() && !reservation_id.is_empty() {
                                    intent.reservation_id = reservation_id.clone();
                                }
                                let plan = FragmentPlan {
                                    fragment_index,
                                    chunk_id: random_chunk_id(),
                                    target_id: placement.target_id.clone(),
                                    endpoint: placement.endpoint.clone(),
                                    granule_index: placement.granule_index,
                                    generation: 1,
                                    stripe_index,
                                };
                                intent.fragment_status.push(FragmentWriteStatus {
                                    fragment_index,
                                    state: FragmentWriteState::Planned as i32,
                                    reservation_id,
                                    reservation_placement_index,
                                    stripe_index,
                                });
                                intent.fragment_plans.push(plan.clone());
                                window_plans.push(plan);
                            }
                        }
                        let intent_bytes = encode_write_intent(&intent);
                        store_blob(
                            &trx,
                            &key,
                            &write_intent_chunk_prefix(&intent.intent_id),
                            |chunk_index| write_intent_chunk_key(&intent.intent_id, chunk_index),
                            &intent_bytes,
                        );
                        Ok(ReservedObjectWriteWindow {
                            fragment_plans: window_plans,
                            used_reservations: true,
                        })
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;

            Ok(TimedStoreResult {
                value,
                phase_timings: Vec::new(),
            })
        }

        async fn commit_object_write_window(
            &self,
            intent_id: String,
            successful_fragments: Vec<FragmentRef>,
        ) -> Result<TimedStoreResult<CommittedObjectWriteWindow>, Status> {
            let key = write_intent_key(&intent_id);
            let value = self
                .db
                .run(move |trx, _| {
                    let key = key.clone();
                    let successful_fragments = successful_fragments.clone();
                    let intent_id = intent_id.clone();
                    async move {
                        let bytes = load_blob(&trx, &key, |chunk_index| {
                            write_intent_chunk_key(&intent_id, chunk_index)
                        })
                        .await?
                        .ok_or_else(|| {
                            status_to_fdb(Status::not_found(format!(
                                "unknown write intent {}",
                                intent_id
                            )))
                        })?;
                        let mut intent =
                            decode_write_intent_bytes(&bytes).map_err(status_to_fdb)?;
                        normalize_write_intent(&mut intent).map_err(status_to_fdb)?;
                        if intent.state != WriteIntentState::Reserved as i32 {
                            return Err(status_to_fdb(Status::failed_precondition(format!(
                                "write intent {} is not window-committable in state {}",
                                intent.intent_id, intent.state
                            ))));
                        }
                        if successful_fragments.is_empty() {
                            return Err(status_to_fdb(Status::invalid_argument(
                                "CommitObjectWriteWindow requires at least one successful fragment",
                            )));
                        }
                        mark_successful_fragments(&mut intent, &successful_fragments)
                            .map_err(status_to_fdb)?;
                        let intent_bytes = encode_write_intent(&intent);
                        store_blob(
                            &trx,
                            &key,
                            &write_intent_chunk_prefix(&intent.intent_id),
                            |chunk_index| write_intent_chunk_key(&intent.intent_id, chunk_index),
                            &intent_bytes,
                        );
                        Ok(CommittedObjectWriteWindow {
                            intent_id: intent.intent_id.clone(),
                            reservation_ids: Vec::new(),
                            finalize_plans: Vec::new(),
                        })
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;

            Ok(TimedStoreResult {
                value,
                phase_timings: Vec::new(),
            })
        }

        async fn commit_object_write(
            &self,
            intent_id: String,
            successful_fragments: Vec<FragmentRef>,
            finalization_sweep_after_ms: u64,
        ) -> Result<TimedStoreResult<CommittedObjectWrite>, Status> {
            let intent_key = write_intent_key(&intent_id);
            let value = self
                .db
                .run(move |trx, _| {
                    let intent_key = intent_key.clone();
                    let successful_fragments = successful_fragments.clone();
                    let intent_id = intent_id.clone();
                    async move {
                        let bytes = load_blob(&trx, &intent_key, |chunk_index| {
                            write_intent_chunk_key(&intent_id, chunk_index)
                        })
                        .await?
                            .ok_or_else(|| {
                                status_to_fdb(Status::not_found(format!(
                                    "unknown write intent {}",
                                    intent_id
                                )))
                            })?;
                        let mut intent = decode_write_intent_bytes(&bytes).map_err(status_to_fdb)?;
                        normalize_write_intent(&mut intent).map_err(status_to_fdb)?;
                        let version_key = object_version_key(&intent.version_id);
                        if intent.state == WriteIntentState::Committed as i32 {
                            let manifest_bytes = load_blob(&trx, &version_key, |chunk_index| {
                                object_version_chunk_key(&intent.version_id, chunk_index)
                            })
                            .await?
                                .ok_or_else(|| {
                                    status_to_fdb(Status::internal(
                                        "committed write intent is missing manifest",
                                    ))
                                })?;
                            let manifest = decode_manifest_bytes(&manifest_bytes).map_err(status_to_fdb)?;
                            let finalize_plans =
                                build_finalize_plans(&intent).map_err(status_to_fdb)?;
                            return Ok(CommittedObjectWrite {
                                intent_id: intent.intent_id.clone(),
                                manifest,
                                reservation_ids: intent.reservation_ids.clone(),
                                finalize_plans,
                            });
                        }
                        if intent.state != WriteIntentState::Reserved as i32 {
                            return Err(status_to_fdb(Status::failed_precondition(format!(
                                "write intent {} is in state {} and cannot be committed",
                                intent.intent_id, intent.state
                            ))));
                        }

                        mark_successful_fragments(&mut intent, &successful_fragments)
                            .map_err(status_to_fdb)?;
                        let incomplete = intent
                            .fragment_status
                            .iter()
                            .filter(|status| status.state != FragmentWriteState::Written as i32)
                            .map(|status| format!("{}:{}", status.stripe_index, status.fragment_index))
                            .collect::<Vec<_>>();
                        if !incomplete.is_empty() {
                            return Err(status_to_fdb(Status::failed_precondition(format!(
                                "write intent {} still has non-written fragments: {:?}",
                                intent.intent_id, incomplete
                            ))));
                        }
                        let expected_fragment_count =
                            expected_fragment_count(&intent).map_err(status_to_fdb)?;
                        if intent.fragment_plans.len() != expected_fragment_count
                            || intent.fragment_status.len() != expected_fragment_count
                        {
                            return Err(status_to_fdb(Status::failed_precondition(format!(
                                "write intent {} is incomplete: have {} planned fragments and {} status entries, need {}",
                                intent.intent_id,
                                intent.fragment_plans.len(),
                                intent.fragment_status.len(),
                                expected_fragment_count
                            ))));
                        }

                        let stripe_count = usize::try_from(intent.stripe_count).map_err(|_| {
                            status_to_fdb(Status::internal(format!(
                                "write intent {} declares an unsupported stripe count {}",
                                intent.intent_id, intent.stripe_count
                            )))
                        })?;
                        let mut stripes = (0..stripe_count)
                            .map(|_| StripeManifest {
                                fragments: Vec::new(),
                            })
                            .collect::<Vec<_>>();
                        for plan in &intent.fragment_plans {
                            let stripe = stripes
                                .get_mut(plan.stripe_index as usize)
                                .ok_or_else(|| {
                                    status_to_fdb(Status::internal(format!(
                                        "write intent {} references missing stripe {}",
                                        intent.intent_id, plan.stripe_index
                                    )))
                                })?;
                            stripe.fragments.push(plan.clone());
                        }
                        for (stripe_index, stripe) in stripes.iter_mut().enumerate() {
                            if stripe.fragments.is_empty() {
                                return Err(status_to_fdb(Status::internal(format!(
                                    "write intent {} is missing fragment plans for stripe {}",
                                    intent.intent_id, stripe_index
                                ))));
                            }
                            stripe.fragments.sort_unstable_by_key(|plan| plan.fragment_index);
                        }

                        let manifest = ObjectVersionManifest {
                            version_id: intent.version_id.clone(),
                            bucket_id: intent.bucket_id.clone(),
                            key: intent.key.clone(),
                            logical_length_bytes: intent.logical_length_bytes,
                            ec_profile_id: intent.ec_profile_id.clone(),
                            stripes,
                            namespace_id: intent.namespace_id.clone(),
                            object_entry_id: intent.object_entry_id.clone(),
                            bucket_entry_id: intent.bucket_entry_id.clone(),
                        };
                        let head_key = object_head_key(&manifest.bucket_id, &manifest.key);
                        let previous_head = trx
                            .get(&head_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| decode_object_head(bytes.as_ref()))
                            .transpose()
                            .map_err(status_to_fdb)?;
                        let prior_version = previous_head.as_ref().map(|h| h.version).unwrap_or(0);
                        if let Some(previous_head) = previous_head {
                            if let Some(previous_manifest_bytes) = load_blob(
                                &trx,
                                &object_version_key(&previous_head.current_version_id),
                                |chunk_index| {
                                    object_version_chunk_key(
                                        &previous_head.current_version_id,
                                        chunk_index,
                                    )
                                },
                            )
                            .await?
                            {
                                let previous_manifest = decode_manifest_bytes(
                                    &previous_manifest_bytes,
                                )
                                .map_err(status_to_fdb)?;
                                clear_target_current_fragment_index(&trx, &previous_manifest);
                            }
                        }
                        let head = ObjectHead {
                            object_entry_id: manifest.object_entry_id.clone(),
                            current_version_id: manifest.version_id.clone(),
                            revision: finalization_sweep_after_ms,
                            version: prior_version + 1,
                            logical_length_bytes: manifest.logical_length_bytes,
                            ec_profile_id: manifest.ec_profile_id.clone(),
                            // The window/commit write path does not compute placement,
                            // so it records no topology epoch.
                            topology_epoch: 0,
                        };
                        let object_name = manifest
                            .key
                            .rsplit('/')
                            .next()
                            .unwrap_or(manifest.key.as_str())
                            .to_string();
                        let object_entry = NamespaceDomainEntry {
                            entry_id: manifest.object_entry_id.clone(),
                            namespace_id: manifest.namespace_id.clone(),
                            parent_entry_id: intent.parent_entry_id.clone(),
                            name: object_name.clone(),
                            kind: NamespaceEntryKind::Object as i32,
                            path: join_path(&intent.parent_path, &object_name),
                            // Denormalize at commit: the entry is persisted as
                            // JSON and range-read by list_children, so the size
                            // round-trips with no resolve-at-list. Overwrite (a
                            // new committed version) re-runs this commit and
                            // re-sets namespace_entry_key, refreshing size_bytes.
                            size_bytes: manifest.logical_length_bytes,
                        };
                        let manifest_bytes = encode_manifest(&manifest);
                        store_blob(
                            &trx,
                            &version_key,
                            &object_version_chunk_prefix(&manifest.version_id),
                            |chunk_index| object_version_chunk_key(&manifest.version_id, chunk_index),
                            &manifest_bytes,
                        );
                        trx.set(&head_key, &encode_object_head(&head));
                        write_target_current_fragment_index(&trx, &manifest);
                        trx.set(
                            &namespace_entry_key(&manifest.namespace_id, &manifest.object_entry_id),
                            &serde_json::to_vec(&object_entry).map_err(|err| {
                                status_to_fdb(Status::internal(format!(
                                    "failed to encode namespace entry JSON payload: {err}"
                                )))
                            })?,
                        );
                        // Maintain the path -> entry_id index in the same
                        // transaction so the write-intent parent lookup can
                        // resolve this object's directory with a point get.
                        trx.set(
                            &namespace_path_key(&object_entry.namespace_id, &object_entry.path),
                            object_entry.entry_id.as_bytes(),
                        );
                        intent.state = WriteIntentState::Committed as i32;
                        intent.reservations_finalized = false;
                        intent.expires_at_unix_ms = finalization_sweep_after_ms;
                        let intent_bytes = encode_write_intent(&intent);
                        store_blob(
                            &trx,
                            &intent_key,
                            &write_intent_chunk_prefix(&intent.intent_id),
                            |chunk_index| write_intent_chunk_key(&intent.intent_id, chunk_index),
                            &intent_bytes,
                        );
                        let finalize_plans = build_finalize_plans(&intent).map_err(status_to_fdb)?;
                        Ok(CommittedObjectWrite {
                            intent_id: intent.intent_id.clone(),
                            manifest,
                            reservation_ids: intent.reservation_ids.clone(),
                            finalize_plans,
                        })
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;

            Ok(TimedStoreResult {
                value,
                phase_timings: Vec::new(),
            })
        }

        async fn commit_object_single_shot(
            &self,
            commit: crate::hot_store::SingleShotCommit,
        ) -> Result<ObjectHead, Status> {
            let vc = validate_single_shot_commit(commit)?;
            self.db
                .run(move |trx, _| {
                    let vc = vc.clone();
                    async move {
                        match single_shot_pass1(&trx, &vc).await? {
                            SingleShotPass1::WriteReady(plan) => {
                                single_shot_pass2(&trx, &plan);
                                Ok(plan.head)
                            }
                            SingleShotPass1::Idempotent(head) => Ok(head),
                            // A logical rejection (CAS miss, create-only clash, reclaimed
                            // granule, oversized) aborts the single commit with its Status.
                            SingleShotPass1::Rejected(status) => Err(status_to_fdb(status)),
                        }
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn commit_objects_single_shot_batch(
            &self,
            commits: Vec<crate::hot_store::SingleShotCommit>,
        ) -> Result<Vec<Result<ObjectHead, Status>>, Status> {
            use std::collections::HashSet;
            // Pre-transaction: validate each object and reject any duplicate (bucket, key)
            // inside the batch up front. Because the transaction reads every head before any
            // write, two same-key commits would both pass the create-only CAS and the second
            // would silently clobber the first; deduping here keeps the in-txn invariants
            // sound for the surviving distinct-key set.
            let mut slots: Vec<Result<ValidatedSingleShotCommit, Status>> =
                Vec::with_capacity(commits.len());
            let mut seen: HashSet<(String, String)> = HashSet::new();
            for commit in commits {
                match validate_single_shot_commit(commit) {
                    Ok(vc) => {
                        let dedup_key = (vc.manifest.bucket_id.clone(), vc.manifest.key.clone());
                        if seen.insert(dedup_key) {
                            slots.push(Ok(vc));
                        } else {
                            slots.push(Err(Status::failed_precondition(format!(
                                "object {}/{} appears more than once in one MultiCommit batch; commit duplicates separately",
                                vc.manifest.bucket_id, vc.manifest.key
                            ))));
                        }
                    }
                    Err(status) => slots.push(Err(status)),
                }
            }
            let attempts: Vec<ValidatedSingleShotCommit> = slots
                .iter()
                .filter_map(|slot| slot.as_ref().ok().cloned())
                .collect();
            let txn_results: Vec<Result<ObjectHead, Status>> = if attempts.is_empty() {
                Vec::new()
            } else {
                self.db
                    .run(move |trx, _| {
                        let attempts = attempts.clone();
                        async move {
                            // Pass 1: every object's reads + validation, NO writes — this
                            // completes the read-conflict set (incl. the reverse-log reclaim
                            // fence) before anything mutates.
                            let mut outcomes: Vec<SingleShotPass1> =
                                Vec::with_capacity(attempts.len());
                            for vc in &attempts {
                                outcomes.push(single_shot_pass1(&trx, vc).await?);
                            }
                            // Pass 2: writes only for the objects that passed pass 1.
                            for outcome in &outcomes {
                                if let SingleShotPass1::WriteReady(plan) = outcome {
                                    single_shot_pass2(&trx, plan);
                                }
                            }
                            Ok(outcomes
                                .into_iter()
                                .map(|outcome| match outcome {
                                    SingleShotPass1::WriteReady(plan) => Ok(plan.head),
                                    SingleShotPass1::Idempotent(head) => Ok(head),
                                    SingleShotPass1::Rejected(status) => Err(status),
                                })
                                .collect::<Vec<Result<ObjectHead, Status>>>())
                        }
                    })
                    .await
                    .map_err(map_fdb_binding_error)?
            };
            // Re-thread the per-object transaction results into the index-aligned slots:
            // pre-validation rejects keep their Err; each attempted object takes the next
            // transaction result in order.
            let mut txn_iter = txn_results.into_iter();
            let merged = slots
                .into_iter()
                .map(|slot| match slot {
                    Err(status) => Err(status),
                    Ok(_) => txn_iter
                        .next()
                        .expect("one transaction result per attempted commit"),
                })
                .collect();
            Ok(merged)
        }

        async fn append_manifest_segment(
            &self,
            version_id: String,
            stripes: Vec<StripeManifest>,
            marker_expires_at_unix_ms: u64,
            marker_fenced: bool,
        ) -> Result<(), Status> {
            let (object_id, object_version) = parse_version_id_parts(&version_id)?;
            let marker_key = pending_version_key(object_id, object_version);
            let fence_key = version_reclaim_fence_key(object_id, object_version);
            // One presence row per distinct target in this segment, computed up front so
            // the size guard accounts for them.
            let mut presence_targets: Vec<&str> = stripes
                .iter()
                .flat_map(|stripe| {
                    stripe
                        .fragments
                        .iter()
                        .map(|fragment| fragment.target_id.as_str())
                })
                .collect();
            presence_targets.sort_unstable();
            presence_targets.dedup();
            let presence_keys: Vec<Vec<u8>> = presence_targets
                .iter()
                .map(|target_id| pending_version_target_key(object_id, object_version, target_id))
                .collect();
            // Bound this segment's transaction server-side rather than trusting the client's
            // stripes-per-segment: same per-fragment estimate the single-shot path uses, plus
            // the marker rewrite and presence rows this transaction adds, so an oversized
            // segment is rejected with a clean precondition error instead of an opaque FDB 2101.
            let fragment_count: usize = stripes.iter().map(|s| s.fragments.len()).sum();
            let estimated_txn_bytes = fragment_count
                .saturating_mul(SINGLE_SHOT_PER_FRAGMENT_TXN_BYTES)
                .saturating_add(PENDING_VERSION_MARKER_TXN_BYTES)
                .saturating_add(presence_keys.len().saturating_mul(PRESENCE_ROW_TXN_BYTES));
            if estimated_txn_bytes > MAX_SINGLE_SHOT_TXN_BYTES {
                return Err(Status::failed_precondition(format!(
                    "segmented commit for version {} sent a {}-fragment segment (~{} bytes), over the {}-byte per-transaction budget; the client must send fewer stripes per segment",
                    version_id, fragment_count, estimated_txn_bytes, MAX_SINGLE_SHOT_TXN_BYTES
                )));
            }
            self.db
                .run(move |trx, _| {
                    let version_id = version_id.clone();
                    let stripes = stripes.clone();
                    let marker_key = marker_key.clone();
                    let fence_key = fence_key.clone();
                    let presence_keys = presence_keys.clone();
                    async move {
                        // Pending-version fence (READ before any write, the HARD INVARIANT):
                        // the version's marker must still be in the writing state. Absent or
                        // reclaiming means the write forfeited — its lease lapsed and the
                        // orphan-version reaper claimed (or already cleaned) the version — so
                        // the append fails instead of resurrecting reclaimed rows. Appends
                        // never MINT the marker, so nothing a zombie stream sends can bring a
                        // reclaimed version back. Reading the marker also puts it in the
                        // read-conflict set: this append and a concurrent claim (which
                        // rewrites the marker) can never both win. In fenced mode the
                        // per-version reclaim-fence get rides the same round trip.
                        let forfeited = |version_id: &str| {
                            status_to_fdb(Status::failed_precondition(format!(
                                "segmented commit for version {version_id} cannot proceed: the \
                                 write lease lapsed and the version was (or is being) reclaimed; \
                                 the object must be rewritten under a fresh write"
                            )))
                        };
                        let (marker_bytes, fence_bytes) = if marker_fenced {
                            futures_util::future::try_join(
                                trx.get(&marker_key, false),
                                trx.get(&fence_key, false),
                            )
                            .await
                            .map_err(FdbBindingError::from)?
                        } else {
                            (
                                trx.get(&marker_key, false)
                                    .await
                                    .map_err(FdbBindingError::from)?,
                                None,
                            )
                        };
                        let marker_bytes = marker_bytes.ok_or_else(|| forfeited(&version_id))?;
                        let pending = decode_pending_version_value(marker_bytes.as_ref())
                            .map_err(status_to_fdb)?;
                        if pending.state != PENDING_VERSION_STATE_WRITING {
                            return Err(forfeited(&version_id));
                        }
                        if marker_fenced {
                            // Per-version reclaim fence (required ABSENT): the granule GC
                            // stamps it in the same transaction as any reclaim tombstone
                            // for this version, so one point read replaces the per-row
                            // rendezvous below — a blind row write can never bury a
                            // tombstone this append failed to see.
                            if fence_bytes.is_some() {
                                return Err(status_to_fdb(Status::failed_precondition(format!(
                                    "segmented commit for version {version_id} cannot proceed: a \
                                     granule of this version was reclaimed by GC after its write \
                                     lease lapsed; the object must be rewritten under a fresh write"
                                ))));
                            }
                        } else {
                            // Lease-fenced GC rendezvous (still reads, before any write): read
                            // every reverse-log row this segment will write, adding each to the
                            // read-conflict set so a concurrent reclaiming GC forces exactly one of
                            // the two to abort. A reclaim tombstone on any row means the granule is
                            // gone. Issued concurrently — the FDB client pipelines the gets, so a
                            // 512-stripe segment pays ~one read round-trip instead of thousands;
                            // the read-conflict set is identical.
                            let fence_reads = futures_util::future::try_join_all(
                                stripes.iter().flat_map(|stripe| {
                                    stripe.fragments.iter().map(|fragment| {
                                        let rl_key = target_reverse_log_key(
                                            &fragment.target_id,
                                            &version_id,
                                            fragment.stripe_index,
                                            fragment.fragment_index,
                                        );
                                        let trx = &trx;
                                        async move {
                                            let value = trx
                                                .get(&rl_key, false)
                                                .await
                                                .map_err(FdbBindingError::from)?;
                                            Ok::<_, FdbBindingError>((fragment, value))
                                        }
                                    })
                                }),
                            )
                            .await?;
                            for (fragment, value) in fence_reads {
                                if let Some(existing) = value {
                                    if is_reverse_log_reclaim_tombstone(existing.as_ref()) {
                                        return Err(status_to_fdb(Status::failed_precondition(format!(
                                            "segmented commit for version {} cannot proceed: the granule for stripe {} fragment {} on target {} was reclaimed by GC; the fragment bytes are gone and the object must be rewritten",
                                            version_id, fragment.stripe_index, fragment.fragment_index, fragment.target_id
                                        ))));
                                    }
                                }
                            }
                        }
                        // Writes. Marker first: push its protection forward — every landed
                        // segment proves the writer is alive, so a healthy stream never
                        // becomes claim-eligible mid-commit.
                        let renewed = PendingVersion {
                            expires_at_unix_ms: pending
                                .expires_at_unix_ms
                                .max(marker_expires_at_unix_ms),
                            ..pending
                        };
                        trx.set(&marker_key, &encode_pending_version_value(&renewed));
                        // Per-fragment reverse-log rows. NO head, NO lease clear — the
                        // object stays invisible and lease-protected until the seal.
                        for stripe in &stripes {
                            for fragment in &stripe.fragments {
                                trx.set(
                                    &target_reverse_log_key(
                                        &fragment.target_id,
                                        &version_id,
                                        fragment.stripe_index,
                                        fragment.fragment_index,
                                    ),
                                    &encode_reverse_log_value(
                                        fragment.generation,
                                        fragment.granule_index,
                                    ),
                                );
                            }
                        }
                        // Presence rows ride the same transaction as their targets' rows, so
                        // the reaper's target enumeration can never under-cover them.
                        for presence_key in &presence_keys {
                            trx.set(presence_key, &[]);
                        }
                        // Occupancy index + committed-granule markers for this segment's stripes
                        // (a minimal manifest carrying just version_id + stripes is all
                        // write_target_current_fragment_index reads).
                        let segment_manifest = ObjectVersionManifest {
                            version_id: version_id.clone(),
                            stripes,
                            ..Default::default()
                        };
                        write_target_current_fragment_index(&trx, &segment_manifest);
                        Ok(())
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn get_or_init_cluster_salt(&self) -> Result<Vec<u8>, Status> {
            let key = cluster_salt_key();
            self.db
                .run(move |trx, _| {
                    let key = key.clone();
                    async move {
                        if let Some(existing) =
                            trx.get(&key, false).await.map_err(FdbBindingError::from)?
                        {
                            return Ok(existing.as_ref().to_vec());
                        }
                        // First use mints the salt. FDB serializability makes the first
                        // committer win; a concurrent loser conflicts on the read, retries,
                        // finds the committed salt, and returns it — so the cluster keeps
                        // exactly one salt for its lifetime.
                        let salt = random_salt();
                        trx.set(&key, &salt);
                        Ok(salt)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn get_object_head(
            &self,
            bucket_id: String,
            key_path: String,
        ) -> Result<Option<ObjectHead>, Status> {
            let normalized_key = normalize_object_key(&key_path)?;
            let head_key = object_head_key(&bucket_id, &normalized_key);
            self.db
                .run(move |trx, _| {
                    let head_key = head_key.clone();
                    async move {
                        trx.get(&head_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| decode_object_head(bytes.as_ref()))
                            .transpose()
                            .map_err(status_to_fdb)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn begin_object_write(
            &self,
            object_id: u32,
            object_version: u32,
            bucket_id: &str,
            key: &str,
            expires_at_unix_ms: u64,
        ) -> Result<(), Status> {
            let normalized_key = normalize_object_key(key)?;
            let lease_key = object_lease_key(object_id);
            let marker_key = pending_version_key(object_id, object_version);
            let marker_value = encode_pending_version_value(&PendingVersion {
                expires_at_unix_ms,
                state: PENDING_VERSION_STATE_WRITING,
                bucket_id: bucket_id.to_string(),
                key: normalized_key,
            });
            self.db
                .run(move |trx, _| {
                    let lease_key = lease_key.clone();
                    let marker_key = marker_key.clone();
                    let marker_value = marker_value.clone();
                    async move {
                        // Blind sets: the object_id is minted fresh per write attempt
                        // from the monotonic block allocator and never reused, so
                        // neither key can pre-exist.
                        trx.set(&lease_key, &encode_write_lease_value(expires_at_unix_ms));
                        trx.set(&marker_key, &marker_value);
                        Ok(())
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn renew_object_write(
            &self,
            object_id: u32,
            new_expires_at_unix_ms: u64,
        ) -> Result<(), Status> {
            let (range_begin, range_end) = pending_version_object_range(object_id);
            let lease_key = object_lease_key(object_id);
            self.db
                .run(move |trx, _| {
                    let range_begin = range_begin.clone();
                    let range_end = range_end.clone();
                    let lease_key = lease_key.clone();
                    async move {
                        // Locate this object's marker. One object_id maps to exactly one
                        // minted version, and a marker sorts before its presence rows,
                        // so the first three-segment row in the range is it. Reading it
                        // puts the marker in the read-conflict set: a concurrent claim
                        // (which rewrites it to reclaiming) serializes against this
                        // renewal — exactly one wins.
                        let mut stream = trx
                            .get_ranges_keyvalues((range_begin, range_end).into(), false);
                        let mut marker: Option<(Vec<u8>, PendingVersion)> = None;
                        while let Some(next) = stream.next().await {
                            let kv = next?;
                            let is_marker = decode_pending_version_key_parts(kv.key())
                                .map(|(_, _, target)| target.is_none())
                                .unwrap_or(false);
                            if is_marker {
                                let decoded = decode_pending_version_value(kv.value())
                                    .map_err(status_to_fdb)?;
                                marker = Some((kv.key().to_vec(), decoded));
                                break;
                            }
                        }
                        let refuse = || {
                            status_to_fdb(Status::failed_precondition(format!(
                                "object {object_id}'s write has forfeited (its version was \
                                 or is being reclaimed after the lease lapsed); the object \
                                 must be rewritten under a fresh write"
                            )))
                        };
                        let Some((marker_key, pending)) = marker else {
                            // No marker: reaped (or never begun). Write NOTHING — a
                            // zombie heartbeat must not resurrect the lease and stall
                            // reclamation.
                            return Err(refuse());
                        };
                        if pending.state == PENDING_VERSION_STATE_RECLAIMING {
                            return Err(refuse());
                        }
                        // Renew both writer liveness (lease) and version liveness
                        // (marker): the heartbeat is what protects a slow write during
                        // its fragment-upload phase, before any segment append has
                        // pushed the marker's expiry forward.
                        let renewed = PendingVersion {
                            expires_at_unix_ms: pending
                                .expires_at_unix_ms
                                .max(new_expires_at_unix_ms),
                            ..pending
                        };
                        trx.set(
                            &lease_key,
                            &encode_write_lease_value(new_expires_at_unix_ms),
                        );
                        trx.set(&marker_key, &encode_pending_version_value(&renewed));
                        Ok(())
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }


        async fn begin_object_write_resolved(
            &self,
            bucket_id: &str,
            key: &str,
            namespace_id: &str,
            bucket_entry_id: &str,
            bucket_path: &str,
            parent_hint: Option<(String, String)>,
            expires_at_unix_ms: u64,
        ) -> Result<(u32, u32, String, String), Status> {
            let normalized_key = normalize_object_key(key)?;
            let object_id = self.allocate_object_id().await?;
            let head_key = object_head_key(bucket_id, &normalized_key);
            let bucket_id = bucket_id.to_string();
            let namespace_id = namespace_id.to_string();
            let bucket_entry_id = bucket_entry_id.to_string();
            let bucket_path = bucket_path.to_string();
            let (version, parent_entry_id, parent_path) = self
                .db
                .run(move |trx, _| {
                    let head_key = head_key.clone();
                    let normalized_key = normalized_key.clone();
                    let bucket_id = bucket_id.clone();
                    let namespace_id = namespace_id.clone();
                    let bucket_entry_id = bucket_entry_id.clone();
                    let bucket_path = bucket_path.clone();
                    let parent_hint = parent_hint.clone();
                    async move {
                        // Version from the prior head (1 if new) — the same rule the
                        // separate mint transaction used; deriving it here keeps the
                        // version and the marker mint in one atomic transaction.
                        let version = trx
                            .get(&head_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| decode_object_head(bytes.as_ref()))
                            .transpose()
                            .map_err(status_to_fdb)?
                            .map(|head| head.version.saturating_add(1))
                            .unwrap_or(1);
                        let (parent_entry_id, parent_path) = resolve_or_create_object_parent(
                            &trx,
                            &namespace_id,
                            &normalized_key,
                            &bucket_entry_id,
                            &bucket_path,
                            parent_hint,
                        )
                        .await?;
                        // Marker + lease, blind sets: the object_id is minted fresh
                        // per write attempt and never reused, so neither key can
                        // pre-exist.
                        trx.set(
                            &pending_version_key(object_id, version),
                            &encode_pending_version_value(&PendingVersion {
                                expires_at_unix_ms,
                                state: PENDING_VERSION_STATE_WRITING,
                                bucket_id: bucket_id.clone(),
                                key: normalized_key.clone(),
                            }),
                        );
                        trx.set(
                            &object_lease_key(object_id),
                            &encode_write_lease_value(expires_at_unix_ms),
                        );
                        Ok((version, parent_entry_id, parent_path))
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            Ok((object_id, version, parent_entry_id, parent_path))
        }

        async fn forfeit_object_write(
            &self,
            object_id: u32,
            object_version: u32,
        ) -> Result<(), Status> {
            let marker_key = pending_version_key(object_id, object_version);
            let lease_key = object_lease_key(object_id);
            self.db
                .run(move |trx, _| {
                    let marker_key = marker_key.clone();
                    let lease_key = lease_key.clone();
                    async move {
                        // Reading the marker serializes a forfeit against a racing
                        // commit or reaper claim (each also touches it): exactly one
                        // side wins, and the losers retry into idempotent success.
                        let Some(bytes) = trx
                            .get(&marker_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                        else {
                            // Committed, or already reaped — nothing to abandon.
                            return Ok(());
                        };
                        let pending = decode_pending_version_value(bytes.as_ref())
                            .map_err(status_to_fdb)?;
                        if pending.state == PENDING_VERSION_STATE_WRITING {
                            // Reclaiming is terminal; the reaper's next scan yields
                            // every reclaiming marker and cleans the write's rows
                            // without waiting out the lease TTL.
                            trx.set(
                                &marker_key,
                                &encode_pending_version_value(&PendingVersion {
                                    expires_at_unix_ms: 0,
                                    state: PENDING_VERSION_STATE_RECLAIMING,
                                    ..pending
                                }),
                            );
                        }
                        trx.clear(&lease_key);
                        Ok(())
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn reap_expired_leases(
            &self,
            now_unix_ms: u64,
            grace_ms: u64,
            limit: usize,
        ) -> Result<usize, Status> {
            let (begin, end) = object_lease_range();
            self.db
                .run(move |trx, _| {
                    let begin = begin.clone();
                    let end = end.clone();
                    async move {
                        // Scan the lease keyspace, clearing rows whose expiry plus the
                        // reclaim grace has passed. The grace matches the GC's eligibility
                        // grace, so a lease persists long enough to gate reclaim instead of
                        // vanishing the instant it expires. Lease volume is bounded by
                        // in-flight writes, so a single scan-and-clear transaction suffices.
                        let mut stream = trx.get_ranges_keyvalues((begin, end).into(), false);
                        let mut reaped = 0usize;
                        while let Some(next) = stream.next().await {
                            if reaped >= limit {
                                break;
                            }
                            let kv = next?;
                            let expiry =
                                decode_write_lease_expiry(kv.value()).map_err(status_to_fdb)?;
                            if expiry.saturating_add(grace_ms) <= now_unix_ms {
                                trx.clear(kv.key());
                                reaped += 1;
                            }
                        }
                        Ok(reaped)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn authorize_orphan_reclaim(
            &self,
            target_id: String,
            object_id: u32,
            object_version: u32,
            stripe_index: u32,
            fragment_index: u32,
            now_unix_ms: u64,
            lease_grace_ms: u64,
        ) -> Result<OrphanReclaimDecision, Status> {
            let version_id = format!("{object_id}:{object_version}");
            let rl_key =
                target_reverse_log_key(&target_id, &version_id, stripe_index, fragment_index);
            // The committed-occupancy index (prefix 12) is written by BOTH commit paths —
            // the legacy window commit AND the single-shot commit — whereas the reverse log
            // (prefix 16) is written only by the single-shot path. So a committed legacy
            // object has NO reverse-log row; the secondary index is the universal "this
            // fragment is committed and live" signal and MUST be checked, or the GC would
            // free a committed legacy fragment's granule. (Revisit when the decentralized
            // path stops writing the secondary index — the reverse log carries it then.)
            let occupancy_key =
                target_current_fragment_key(&target_id, &version_id, stripe_index, fragment_index);
            let marker_key = pending_version_key(object_id, object_version);
            let lease_key = object_lease_key(object_id);
            self.db
                .run(move |trx, _| {
                    let rl_key = rl_key.clone();
                    let occupancy_key = occupancy_key.clone();
                    let marker_key = marker_key.clone();
                    let lease_key = lease_key.clone();
                    async move {
                        // Read the reverse-log row FIRST so it joins this transaction's
                        // read-conflict set: a concurrent single-shot commit (which also
                        // reads then writes this row) can then never both-win with this
                        // reclaim.
                        if let Some(existing) =
                            trx.get(&rl_key, false).await.map_err(FdbBindingError::from)?
                        {
                            if is_reverse_log_reclaim_tombstone(existing.as_ref()) {
                                // A prior sweep already authorized the free; the physical
                                // delete may still be pending, so let the caller re-issue it.
                                // Re-stamp the version's reclaim fence on this path too:
                                // the prior tombstone may have been stamped by a binary
                                // that predates the fence key (self-healing while the
                                // granule is still being revisited).
                                trx.set(
                                    &version_reclaim_fence_key(object_id, object_version),
                                    &now_unix_ms.to_be_bytes(),
                                );
                                return Ok(OrphanReclaimDecision::AlreadyReclaimed);
                            }
                            // A committed value: the fragment is live. Do not reclaim.
                            return Ok(OrphanReclaimDecision::Committed);
                        }

                        // No reverse-log row. It may still be a committed legacy fragment, or
                        // a single-shot fragment whose commit is racing this reclaim — the
                        // committed-occupancy index covers both. Reading it conflicts a
                        // commit that writes it, so the two cannot both win.
                        if trx
                            .get(&occupancy_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .is_some()
                        {
                            return Ok(OrphanReclaimDecision::Committed);
                        }

                        // Version-granular writer shield: a live pending-version marker in
                        // the writing state protects this write's granules even after the
                        // lease row was reaped (or overwritten by a later write attempt on
                        // the same object) — the marker's expiry is what segment appends
                        // and heartbeats renew. Reclaiming, expired, or absent falls
                        // through to the lease check below.
                        if let Some(marker) = trx
                            .get(&marker_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                        {
                            if let Ok(pending) = decode_pending_version_value(marker.as_ref()) {
                                if pending.state == PENDING_VERSION_STATE_WRITING
                                    && pending
                                        .expires_at_unix_ms
                                        .saturating_add(lease_grace_ms)
                                        > now_unix_ms
                                {
                                    return Ok(OrphanReclaimDecision::LeaseActive);
                                }
                            }
                        }

                        // Uncommitted. The granule is reclaimable only once the write lease
                        // is gone or expired past the grace (a still-valid lease means a
                        // write may be in flight). Reading the lease conflicts a racing
                        // BeginObject that re-issues it.
                        if let Some(lease) =
                            trx.get(&lease_key, false).await.map_err(FdbBindingError::from)?
                        {
                            let expiry =
                                decode_write_lease_expiry(lease.as_ref()).map_err(status_to_fdb)?;
                            if expiry.saturating_add(lease_grace_ms) > now_unix_ms {
                                return Ok(OrphanReclaimDecision::LeaseActive);
                            }
                        }

                        // Authorize: stamp the reclaim tombstone. A commit that reads this
                        // row after this transaction commits sees the tombstone and aborts;
                        // one that read it absent earlier read-conflicts and retries.
                        trx.set(&rl_key, REVERSE_LOG_RECLAIM_TOMBSTONE);
                        // Per-version reclaim fence, in the SAME transaction as the
                        // tombstone: marker-fenced commits and appends read this key
                        // instead of every reverse-log row, and a marker-only fence
                        // would be unsound — this very path tombstones expired-WRITING
                        // versions without ever touching the marker. Idempotent blind
                        // set; authorizing another granule of the same version re-stamps
                        // it. Cleared by finish_orphan_version or the fence janitor.
                        trx.set(
                            &version_reclaim_fence_key(object_id, object_version),
                            &now_unix_ms.to_be_bytes(),
                        );
                        Ok(OrphanReclaimDecision::Authorized)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn scan_pending_versions(
            &self,
            cursor: Option<Vec<u8>>,
            row_limit: usize,
            now_unix_ms: u64,
            claim_grace_ms: u64,
        ) -> Result<(Vec<OrphanVersionCandidate>, Option<Vec<u8>>, u64), Status> {
            if row_limit == 0 {
                return Ok((Vec::new(), None, 0));
            }
            let (scan_begin, scan_end) = pending_version_scan_range();
            let begin = match cursor {
                Some(mut key) => {
                    // Resume exclusively after the last raw key: its immediate
                    // successor is the key plus one zero byte.
                    key.push(0x00);
                    key
                }
                None => scan_begin,
            };
            self.db
                .run(move |trx, _| {
                    let begin = begin.clone();
                    let scan_end = scan_end.clone();
                    async move {
                        let mut range = RangeOption::from((begin, scan_end));
                        range.limit = Some(row_limit);
                        // Snapshot reads: discovery must not read-conflict the marker
                        // renewals riding every segment append and heartbeat.
                        let mut stream = trx.get_ranges_keyvalues(range, true);
                        let mut candidates = Vec::new();
                        let mut rows_seen = 0usize;
                        let mut undecodable = 0u64;
                        let mut last_key: Option<Vec<u8>> = None;
                        while let Some(next) = stream.next().await {
                            let kv = next?;
                            rows_seen += 1;
                            last_key = Some(kv.key().to_vec());
                            // Undecodable rows are counted but never abort the scan.
                            let Ok((object_id, object_version, target)) =
                                decode_pending_version_key_parts(kv.key())
                            else {
                                undecodable += 1;
                                continue;
                            };
                            if target.is_some() {
                                // Presence row; the claim re-reads targets itself.
                                continue;
                            }
                            let Ok(pending) = decode_pending_version_value(kv.value()) else {
                                undecodable += 1;
                                continue;
                            };
                            let eligible = pending.state == PENDING_VERSION_STATE_RECLAIMING
                                || (pending.state == PENDING_VERSION_STATE_WRITING
                                    && pending_version_claim_eligible(
                                        &pending,
                                        now_unix_ms,
                                        claim_grace_ms,
                                    ));
                            if eligible {
                                candidates.push(OrphanVersionCandidate {
                                    object_id,
                                    object_version,
                                    state: pending.state,
                                    expires_at_unix_ms: pending.expires_at_unix_ms,
                                });
                            }
                        }
                        // A short page means the scan wrapped; restart from the range
                        // start next sweep.
                        let next_cursor = if rows_seen >= row_limit { last_key } else { None };
                        Ok((candidates, next_cursor, undecodable))
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn claim_orphan_version(
            &self,
            object_id: u32,
            object_version: u32,
            now_unix_ms: u64,
            claim_grace_ms: u64,
            lease_grace_ms: u64,
        ) -> Result<OrphanVersionClaim, Status> {
            let marker_key = pending_version_key(object_id, object_version);
            let lease_key = object_lease_key(object_id);
            let (subspace_begin, subspace_end) =
                pending_version_subspace_range(object_id, object_version);
            let version_id = format!("{object_id}:{object_version}");
            self.db
                .run(move |trx, _| {
                    let marker_key = marker_key.clone();
                    let lease_key = lease_key.clone();
                    let subspace_begin = subspace_begin.clone();
                    let subspace_end = subspace_end.clone();
                    let version_id = version_id.clone();
                    async move {
                        // Marker first: this read plus the rewrite below is the conflict
                        // pair against every seal, append, and renewal of the version.
                        let Some(marker_bytes) = trx
                            .get(&marker_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                        else {
                            return Ok(OrphanVersionClaim::Gone);
                        };
                        let pending = decode_pending_version_value(marker_bytes.as_ref())
                            .map_err(status_to_fdb)?;
                        let resuming = pending.state == PENDING_VERSION_STATE_RECLAIMING;
                        if !resuming {
                            // Renewed since discovery (an append or heartbeat landed)?
                            if !pending_version_claim_eligible(
                                &pending,
                                now_unix_ms,
                                claim_grace_ms,
                            ) {
                                return Ok(OrphanVersionClaim::SkippedWriterAlive);
                            }
                            // Heartbeat veto: an alive lease shields the write even if
                            // the marker's own expiry lapsed.
                            if let Some(lease) = trx
                                .get(&lease_key, false)
                                .await
                                .map_err(FdbBindingError::from)?
                            {
                                let expiry = decode_write_lease_expiry(lease.as_ref())
                                    .map_err(status_to_fdb)?;
                                if expiry.saturating_add(lease_grace_ms) > now_unix_ms {
                                    return Ok(OrphanVersionClaim::SkippedWriterAlive);
                                }
                            }
                            // Head-liveness backstop: a marker naming the LIVE head means
                            // some commit flipped the head without clearing the marker
                            // (possible only via a binary predating the marker protocol).
                            // Neutralize the marker; never touch the version's rows.
                            if !pending.bucket_id.is_empty() && !pending.key.is_empty() {
                                let head_key =
                                    object_head_key(&pending.bucket_id, &pending.key);
                                if let Some(head_bytes) = trx
                                    .get(&head_key, false)
                                    .await
                                    .map_err(FdbBindingError::from)?
                                {
                                    let head = decode_object_head(head_bytes.as_ref())
                                        .map_err(status_to_fdb)?;
                                    if head.current_version_id == version_id {
                                        trx.clear_range(&subspace_begin, &subspace_end);
                                        return Ok(OrphanVersionClaim::SkippedHeadLive);
                                    }
                                }
                            }
                        }
                        // Presence rows name exactly the targets still holding rows; a
                        // non-empty value is the durable resume cursor a prior partial
                        // clean of that target recorded.
                        let mut targets = Vec::new();
                        let mut stream = trx.get_ranges_keyvalues(
                            (subspace_begin.clone(), subspace_end.clone()).into(),
                            false,
                        );
                        while let Some(next) = stream.next().await {
                            let kv = next?;
                            if let Ok((_, _, Some(target_id))) =
                                decode_pending_version_key_parts(kv.key())
                            {
                                let cursor = (!kv.value().is_empty())
                                    .then(|| kv.value().to_vec());
                                targets.push(OrphanTargetResume { target_id, cursor });
                            }
                        }
                        if resuming {
                            return Ok(OrphanVersionClaim::Resumed { targets });
                        }
                        // Claim. Reclaiming is terminal: nothing transitions it back to
                        // writing, and the version pair is never re-minted.
                        trx.set(
                            &marker_key,
                            &encode_pending_version_value(&PendingVersion {
                                expires_at_unix_ms: now_unix_ms,
                                state: PENDING_VERSION_STATE_RECLAIMING,
                                bucket_id: pending.bucket_id.clone(),
                                key: pending.key.clone(),
                            }),
                        );
                        Ok(OrphanVersionClaim::Claimed { targets })
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn clean_orphan_version_target_page(
            &self,
            object_id: u32,
            object_version: u32,
            target_id: String,
            cursor: Option<Vec<u8>>,
            row_limit: usize,
        ) -> Result<OrphanVersionCleanPage, Status> {
            let marker_key = pending_version_key(object_id, object_version);
            let version_id = format!("{object_id}:{object_version}");
            let (range_begin, range_end) =
                target_reverse_log_version_range(&target_id, &version_id);
            let begin = match cursor {
                Some(mut key) => {
                    key.push(0x00);
                    key
                }
                None => range_begin,
            };
            let (occupancy_begin, occupancy_end) =
                target_current_fragment_version_range(&target_id, &version_id);
            let presence_key =
                pending_version_target_key(object_id, object_version, &target_id);
            self.db
                .run(move |trx, _| {
                    let marker_key = marker_key.clone();
                    let begin = begin.clone();
                    let range_end = range_end.clone();
                    let occupancy_begin = occupancy_begin.clone();
                    let occupancy_end = occupancy_end.clone();
                    let presence_key = presence_key.clone();
                    let target_id = target_id.clone();
                    async move {
                        // The marker must still say reclaiming: absence means another
                        // instance finished the version (nothing left to do), and
                        // writing is unreachable by the state machine — refuse loudly
                        // rather than clean a live write's rows.
                        let Some(marker_bytes) = trx
                            .get(&marker_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                        else {
                            return Ok(OrphanVersionCleanPage {
                                target_done: true,
                                ..Default::default()
                            });
                        };
                        let pending = decode_pending_version_value(marker_bytes.as_ref())
                            .map_err(status_to_fdb)?;
                        if pending.state != PENDING_VERSION_STATE_RECLAIMING {
                            return Err(status_to_fdb(Status::internal(format!(
                                "orphan-version clean found version {object_id}:{object_version} back in the writing state; refusing to touch its rows"
                            ))));
                        }
                        // Collect the page first (bounded), then clear. The granule
                        // index for the paired KCO1 clear is recoverable only from the
                        // row value read in this same transaction — never blind-clear.
                        let mut range = RangeOption::from((begin, range_end));
                        range.limit = Some(row_limit);
                        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                        let mut stream = trx.get_ranges_keyvalues(range, false);
                        while let Some(next) = stream.next().await {
                            let kv = next?;
                            rows.push((kv.key().to_vec(), kv.value().to_vec()));
                        }
                        drop(stream);
                        let exhausted = rows.len() < row_limit;
                        let mut page = OrphanVersionCleanPage::default();
                        for (rl_key, value) in &rows {
                            match classify_reverse_log_value(value) {
                                ReverseLogRowValue::Committed { granule_index, .. } => {
                                    trx.clear(rl_key);
                                    trx.clear(&occupancy_key_for_reverse_log_key(rl_key));
                                    trx.clear(
                                        &keinctl::committed_occupancy::committed_granule_key(
                                            &target_id,
                                            granule_index,
                                        ),
                                    );
                                    page.rows_cleared += 1;
                                    page.kco1_cleared += 1;
                                }
                                // Reclaim tombstones are the granule GC's free-retry
                                // sentinel and the permanent fence against any later
                                // commit of this version: preserve them.
                                ReverseLogRowValue::ReclaimTombstone => {
                                    page.tombstones_skipped += 1;
                                }
                                ReverseLogRowValue::Unrecognized => {
                                    page.unrecognized_skipped += 1;
                                }
                            }
                        }
                        if exhausted {
                            // Stray-occupancy belt (no commit of this version can land
                            // while the marker is reclaiming; window-path occupancy
                            // lives under UUID version_ids, disjoint by construction)
                            // plus durable per-target completion for crash resume.
                            trx.clear_range(&occupancy_begin, &occupancy_end);
                            trx.clear(&presence_key);
                            page.target_done = true;
                        } else {
                            page.next_cursor = rows.last().map(|(key, _)| key.clone());
                            // Record the cursor durably in the presence row so a later
                            // resume continues here instead of re-paging the prefix the
                            // granule GC refills with tombstones behind the cleaner.
                            if let Some(cursor) = &page.next_cursor {
                                trx.set(&presence_key, cursor);
                            }
                        }
                        Ok(page)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn finish_orphan_version(
            &self,
            object_id: u32,
            object_version: u32,
        ) -> Result<bool, Status> {
            let marker_key = pending_version_key(object_id, object_version);
            let (subspace_begin, subspace_end) =
                pending_version_subspace_range(object_id, object_version);
            self.db
                .run(move |trx, _| {
                    let marker_key = marker_key.clone();
                    let subspace_begin = subspace_begin.clone();
                    let subspace_end = subspace_end.clone();
                    async move {
                        let Some(marker_bytes) = trx
                            .get(&marker_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                        else {
                            // Another instance already finished the version.
                            return Ok(true);
                        };
                        let pending = decode_pending_version_value(marker_bytes.as_ref())
                            .map_err(status_to_fdb)?;
                        if pending.state != PENDING_VERSION_STATE_RECLAIMING {
                            return Err(status_to_fdb(Status::internal(format!(
                                "orphan-version finish found version {object_id}:{object_version} back in the writing state; refusing to delete its marker"
                            ))));
                        }
                        // Any row beyond the marker itself is a presence row whose
                        // target still holds rows: not finished.
                        let mut range =
                            RangeOption::from((subspace_begin.clone(), subspace_end.clone()));
                        range.limit = Some(2);
                        let mut stream = trx.get_ranges_keyvalues(range, false);
                        let mut rows = 0usize;
                        while let Some(next) = stream.next().await {
                            next?;
                            rows += 1;
                        }
                        drop(stream);
                        if rows > 1 {
                            return Ok(false);
                        }
                        trx.clear_range(&subspace_begin, &subspace_end);
                        // The version is fully retired: its reclaim fence (stamped by the
                        // granule GC's authorize path, if any granule was tombstoned) has
                        // nothing left to fence. Same transaction as the marker clear so
                        // the fence never outlives a finished version.
                        trx.clear(&version_reclaim_fence_key(object_id, object_version));
                        Ok(true)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn sweep_reclaim_fences(
            &self,
            now_unix_ms: u64,
            older_than_ms: u64,
            row_limit: usize,
            cursor: Option<Vec<u8>>,
        ) -> Result<(u64, Option<Vec<u8>>), Status> {
            if row_limit == 0 {
                return Ok((0, None));
            }
            let (scan_begin, scan_end) = version_reclaim_fence_scan_range();
            let begin = match cursor {
                Some(mut key) => {
                    // Resume exclusively after the last scanned key.
                    key.push(0x00);
                    key
                }
                None => scan_begin,
            };
            self.db
                .run(move |trx, _| {
                    let begin = begin.clone();
                    let scan_end = scan_end.clone();
                    async move {
                        // Snapshot scan: discovery must not read-conflict the GC's
                        // fence stamps. Each candidate is then re-checked with plain
                        // reads so a racing BeginObject re-mint (same object_id can
                        // never recur, but a live marker/lease of an unclaimed orphan
                        // can) conflicts this clear instead of losing its fence.
                        let mut range = RangeOption::from((begin, scan_end));
                        range.limit = Some(row_limit);
                        let mut stream = trx.get_ranges_keyvalues(range, true);
                        let mut candidates = Vec::new();
                        let mut rows_seen = 0usize;
                        let mut last_key: Option<Vec<u8>> = None;
                        while let Some(next) = stream.next().await {
                            let kv = next?;
                            rows_seen += 1;
                            last_key = Some(kv.key().to_vec());
                            let Ok((object_id, object_version)) =
                                decode_version_reclaim_fence_key_parts(kv.key())
                            else {
                                continue;
                            };
                            let stamped_at = match kv.value().try_into() {
                                Ok(bytes) => u64::from_be_bytes(bytes),
                                Err(_) => continue,
                            };
                            if stamped_at.saturating_add(older_than_ms) <= now_unix_ms {
                                candidates.push((kv.key().to_vec(), object_id, object_version));
                            }
                        }
                        drop(stream);
                        // Every candidate's marker + lease re-check rides one pipelined
                        // round trip; a serial await per row could push a full page past
                        // the FDB transaction time limit on a degraded cluster.
                        let rechecks = futures_util::future::try_join_all(
                            candidates
                                .iter()
                                .map(|(_, object_id, object_version)| {
                                    let marker_key =
                                        pending_version_key(*object_id, *object_version);
                                    let lease_key = object_lease_key(*object_id);
                                    let trx = &trx;
                                    async move {
                                        futures_util::future::try_join(
                                            trx.get(&marker_key, false),
                                            trx.get(&lease_key, false),
                                        )
                                        .await
                                    }
                                }),
                        )
                        .await
                        .map_err(FdbBindingError::from)?;
                        let mut cleared = 0u64;
                        for ((fence_key, _, _), (marker, lease)) in
                            candidates.iter().zip(rechecks)
                        {
                            // A live marker or lease means the version (or a later
                            // write attempt on the same object) is still in play; its
                            // fence must keep rejecting late commits. Skip it — the
                            // orphan-version reaper's finish clears such fences.
                            if marker.is_none() && lease.is_none() {
                                trx.clear(fence_key);
                                cleared += 1;
                            }
                        }
                        // A short page means the scan wrapped; restart from the range
                        // start next sweep so skipped rows never block the tail.
                        let next_cursor = if rows_seen >= row_limit { last_key } else { None };
                        Ok((cleared, next_cursor))
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn backfill_reclaim_fences(&self, now_unix_ms: u64) -> Result<u64, Status> {
            const BACKFILL_ROWS_PER_TXN: usize = 8_192;
            let (scan_begin, scan_end) = target_reverse_log_scan_range();
            let mut cursor = scan_begin;
            let mut stamped_total = 0u64;
            loop {
                let begin = cursor.clone();
                let end = scan_end.clone();
                let (stamped, next_cursor) = self
                    .db
                    .run(move |trx, _| {
                        let begin = begin.clone();
                        let end = end.clone();
                        async move {
                            // Snapshot page over the reverse log; tombstones are the
                            // 1-byte sentinel value, committed rows are 12 bytes, so
                            // the value alone distinguishes them. Fence stamps are
                            // blind, idempotent sets.
                            let mut range = RangeOption::from((begin, end));
                            range.limit = Some(BACKFILL_ROWS_PER_TXN);
                            let mut stream = trx.get_ranges_keyvalues(range, true);
                            let mut stamped = 0u64;
                            let mut rows_seen = 0usize;
                            let mut last_key: Option<Vec<u8>> = None;
                            let mut seen_versions: std::collections::HashSet<(u32, u32)> =
                                std::collections::HashSet::new();
                            while let Some(next) = stream.next().await {
                                let kv = next?;
                                rows_seen += 1;
                                last_key = Some(kv.key().to_vec());
                                if !is_reverse_log_reclaim_tombstone(kv.value()) {
                                    continue;
                                }
                                let Ok(version_id) =
                                    decode_reverse_log_key_version_id(kv.key())
                                else {
                                    continue;
                                };
                                let Ok((object_id, object_version)) =
                                    parse_version_id_parts(&version_id).map_err(|_| ())
                                else {
                                    continue;
                                };
                                if seen_versions.insert((object_id, object_version)) {
                                    trx.set(
                                        &version_reclaim_fence_key(object_id, object_version),
                                        &now_unix_ms.to_be_bytes(),
                                    );
                                    stamped += 1;
                                }
                            }
                            drop(stream);
                            let next_cursor = if rows_seen >= BACKFILL_ROWS_PER_TXN {
                                last_key
                            } else {
                                None
                            };
                            Ok((stamped, next_cursor))
                        }
                    })
                    .await
                    .map_err(map_fdb_binding_error)?;
                stamped_total += stamped;
                match next_cursor {
                    Some(mut key) => {
                        key.push(0x00);
                        cursor = key;
                    }
                    None => break,
                }
            }
            Ok(stamped_total)
        }

        async fn register_target(&self, mut target: TargetRecord) -> Result<TargetRecord, Status> {
            if target.target_id.is_empty() {
                return Err(Status::invalid_argument(
                    "register_target requires a non-empty target_id",
                ));
            }
            // Default an unspecified lifecycle to Active; (re)registration marks healthy
            // (heartbeat / set_target_lifecycle refine it). Without the allocator,
            // free_granules is informational and just mirrors granule_count if unset.
            if target.lifecycle_state == TargetLifecycleState::Unspecified as i32 {
                target.lifecycle_state = TargetLifecycleState::Active as i32;
            }
            target.healthy = true;
            if target.free_granules == 0 {
                target.free_granules = target.granule_count;
            }
            let key = target_inventory_key(&target.target_id);
            let value = encode_target_record(&target);
            self.db
                .run(move |trx, _| {
                    let key = key.clone();
                    let value = value.clone();
                    async move {
                        trx.set(&key, &value);
                        Ok(())
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            Ok(target)
        }

        async fn heartbeat_target(
            &self,
            target_id: String,
            healthy: bool,
            observed_unix_ms: u64,
        ) -> Result<TargetRecord, Status> {
            let key = target_inventory_key(&target_id);
            self.db
                .run(move |trx, _| {
                    let key = key.clone();
                    let target_id = target_id.clone();
                    async move {
                        let mut record = trx
                            .get(&key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| decode_target_record(bytes.as_ref()))
                            .transpose()
                            .map_err(status_to_fdb)?
                            .ok_or_else(|| {
                                status_to_fdb(Status::not_found(format!(
                                    "heartbeat for unknown target {target_id}"
                                )))
                            })?;
                        record.healthy = healthy;
                        record.last_heartbeat_unix_ms = observed_unix_ms;
                        trx.set(&key, &encode_target_record(&record));
                        Ok(record)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn set_target_lifecycle(
            &self,
            target_id: String,
            lifecycle_state: i32,
            now_unix_ms: u64,
        ) -> Result<TargetRecord, Status> {
            if lifecycle_state == TargetLifecycleState::Unspecified as i32 {
                return Err(Status::invalid_argument(
                    "set_target_lifecycle requires a concrete lifecycle state",
                ));
            }
            let key = target_inventory_key(&target_id);
            self.db
                .run(move |trx, _| {
                    let key = key.clone();
                    let target_id = target_id.clone();
                    async move {
                        let mut record = trx
                            .get(&key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| decode_target_record(bytes.as_ref()))
                            .transpose()
                            .map_err(status_to_fdb)?
                            .ok_or_else(|| {
                                status_to_fdb(Status::not_found(format!(
                                    "set state for unknown target {target_id}"
                                )))
                            })?;
                        record.lifecycle_state = lifecycle_state;
                        if lifecycle_state == TargetLifecycleState::Active as i32 {
                            record.healthy = true;
                            record.last_heartbeat_unix_ms = now_unix_ms;
                        } else if lifecycle_state == TargetLifecycleState::Unhealthy as i32 {
                            record.healthy = false;
                            record.last_heartbeat_unix_ms = now_unix_ms;
                        }
                        trx.set(&key, &encode_target_record(&record));
                        Ok(record)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn list_targets(&self) -> Result<Vec<TargetRecord>, Status> {
            let (begin, end) = target_inventory_range();
            self.db
                .run(move |trx, _| {
                    let begin = begin.clone();
                    let end = end.clone();
                    async move {
                        let mut stream = trx.get_ranges_keyvalues((begin, end).into(), false);
                        let mut targets = Vec::new();
                        while let Some(next) = stream.next().await {
                            let kv = next?;
                            targets.push(
                                decode_target_record(kv.value()).map_err(status_to_fdb)?,
                            );
                        }
                        targets.sort_by(|left, right| left.target_id.cmp(&right.target_id));
                        Ok(targets)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn abort_object_write(
            &self,
            intent_id: String,
            next_state: WriteIntentState,
        ) -> Result<WriteIntent, Status> {
            let key = write_intent_key(&intent_id);
            self.db
                .run(move |trx, _| {
                    let key = key.clone();
                    let intent_id = intent_id.clone();
                    async move {
                        let bytes = load_blob(&trx, &key, |chunk_index| {
                            write_intent_chunk_key(&intent_id, chunk_index)
                        })
                        .await?
                        .ok_or_else(|| {
                            status_to_fdb(Status::not_found(format!(
                                "unknown write intent {}",
                                intent_id
                            )))
                        })?;
                        let mut intent =
                            decode_write_intent_bytes(&bytes).map_err(status_to_fdb)?;
                        normalize_write_intent(&mut intent).map_err(status_to_fdb)?;
                        if intent.state == WriteIntentState::Committed as i32 {
                            return Err(status_to_fdb(Status::failed_precondition(format!(
                                "write intent {} is already committed and cannot be aborted",
                                intent.intent_id
                            ))));
                        }
                        if intent.state != next_state as i32 {
                            intent.state = next_state as i32;
                            let intent_bytes = encode_write_intent(&intent);
                            store_blob(
                                &trx,
                                &key,
                                &write_intent_chunk_prefix(&intent.intent_id),
                                |chunk_index| {
                                    write_intent_chunk_key(&intent.intent_id, chunk_index)
                                },
                                &intent_bytes,
                            );
                        }
                        Ok(intent)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn repair_object_write(
            &self,
            intent_id: String,
            failed_fragments: Vec<FragmentRef>,
            replacement_reservation: PlacementReservationRecord,
        ) -> Result<WriteIntent, Status> {
            let key = write_intent_key(&intent_id);
            self.db
                .run(move |trx, _| {
                    let key = key.clone();
                    let intent_id = intent_id.clone();
                    let failed_fragments = failed_fragments.clone();
                    let replacement_reservation = replacement_reservation.clone();
                    async move {
                        let bytes = load_blob(&trx, &key, |chunk_index| {
                            write_intent_chunk_key(&intent_id, chunk_index)
                        })
                        .await?
                        .ok_or_else(|| {
                            status_to_fdb(Status::not_found(format!(
                                "unknown write intent {}",
                                intent_id
                            )))
                        })?;
                        let mut intent =
                            decode_write_intent_bytes(&bytes).map_err(status_to_fdb)?;
                        normalize_write_intent(&mut intent).map_err(status_to_fdb)?;
                        if intent.state != WriteIntentState::Reserved as i32 {
                            return Err(status_to_fdb(Status::failed_precondition(format!(
                                "write intent {} is not repairable in state {}",
                                intent.intent_id, intent.state
                            ))));
                        }
                        apply_fragment_repair(
                            &mut intent,
                            &failed_fragments,
                            &replacement_reservation,
                        )
                        .map_err(status_to_fdb)?;
                        let intent_bytes = encode_write_intent(&intent);
                        store_blob(
                            &trx,
                            &key,
                            &write_intent_chunk_prefix(&intent.intent_id),
                            |chunk_index| write_intent_chunk_key(&intent.intent_id, chunk_index),
                            &intent_bytes,
                        );
                        Ok(intent)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn mark_write_intent_reservations_finalized(
            &self,
            intent_id: String,
        ) -> Result<(), Status> {
            let key = write_intent_key(&intent_id);
            self.db
                .run(move |trx, _| {
                    let key = key.clone();
                    let intent_id = intent_id.clone();
                    async move {
                        let Some(bytes) = load_blob(&trx, &key, |chunk_index| {
                            write_intent_chunk_key(&intent_id, chunk_index)
                        })
                        .await?
                        else {
                            return Ok(());
                        };
                        let mut intent =
                            decode_write_intent_bytes(&bytes).map_err(status_to_fdb)?;
                        normalize_write_intent(&mut intent).map_err(status_to_fdb)?;
                        if !intent.reservations_finalized || !intent.reservation_ids.is_empty() {
                            intent.reservations_finalized = true;
                            intent.reservation_ids.clear();
                            let intent_bytes = encode_write_intent(&intent);
                            store_blob(
                                &trx,
                                &key,
                                &write_intent_chunk_prefix(&intent.intent_id),
                                |chunk_index| {
                                    write_intent_chunk_key(&intent.intent_id, chunk_index)
                                },
                                &intent_bytes,
                            );
                        }
                        Ok(())
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn list_pending_finalization_intents(
            &self,
            limit: usize,
            now_ms: u64,
        ) -> Result<Vec<WriteIntent>, Status> {
            let mut intents = self.list_write_intents().await?;
            intents.retain(|intent| {
                intent.state == WriteIntentState::Committed as i32
                    && !intent.reservations_finalized
                    && intent.expires_at_unix_ms > 0
                    && intent.expires_at_unix_ms <= now_ms
            });
            intents.sort_by(|left, right| {
                left.expires_at_unix_ms
                    .cmp(&right.expires_at_unix_ms)
                    .then_with(|| left.intent_id.cmp(&right.intent_id))
            });
            intents.truncate(limit.max(1));
            Ok(intents)
        }

        async fn resolve_object_read(
            &self,
            bucket_id: String,
            key_path: String,
        ) -> Result<(ObjectVersionManifest, EcProfile), Status> {
            let key_path = normalize_object_key(&key_path)?;
            let head_key = object_head_key(&bucket_id, &key_path);
            self.db
                .run(move |trx, _| {
                    let head_key = head_key.clone();
                    let bucket_id = bucket_id.clone();
                    let key_path = key_path.clone();
                    async move {
                        let head_bytes = trx
                            .get(&head_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .ok_or_else(|| {
                                status_to_fdb(Status::not_found(format!(
                                    "object {}/{} has no committed current version",
                                    bucket_id, key_path
                                )))
                            })?;
                        let head =
                            decode_object_head(head_bytes.as_ref()).map_err(status_to_fdb)?;
                        let manifest_bytes = load_blob(
                            &trx,
                            &object_version_key(&head.current_version_id),
                            |chunk_index| {
                                object_version_chunk_key(&head.current_version_id, chunk_index)
                            },
                        )
                        .await?
                        .ok_or_else(|| {
                            status_to_fdb(Status::not_found(format!(
                                "version {} for {}/{} is missing",
                                head.current_version_id, bucket_id, key_path
                            )))
                        })?;
                        let manifest =
                            decode_manifest_bytes(&manifest_bytes).map_err(status_to_fdb)?;
                        let profile_bytes = trx
                            .get(&ec_profile_key(&manifest.ec_profile_id), false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .ok_or_else(|| {
                                status_to_fdb(Status::not_found(format!(
                                    "ec profile {} is missing",
                                    manifest.ec_profile_id
                                )))
                            })?;
                        let profile = serde_json::from_slice::<EcProfile>(profile_bytes.as_ref())
                            .map_err(|err| {
                            status_to_fdb(Status::internal(format!(
                                "failed to decode ec profile {}: {err}",
                                manifest.ec_profile_id
                            )))
                        })?;
                        Ok((manifest, profile))
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
        }

        async fn delete_object(
            &self,
            bucket_id: String,
            key_path: String,
            version_ids: Vec<String>,
        ) -> Result<TimedStoreResult<DeletedObject>, Status> {
            let key_path = normalize_object_key(&key_path)?;
            let head_key = object_head_key(&bucket_id, &key_path);
            // The roster, loaded up front: a manifest-free (decentralized) version has
            // no stored fragment list, so its reverse-log/occupancy rows are cleared by
            // per-(target, version) RANGE clears across every inventoried target — O(1)
            // transaction cost per target regardless of object size, no row reads. A
            // target registered after this list cannot hold rows for an already
            // committed version. (When commits later record their target set, this
            // sweep narrows to exactly the targets that hold rows.)
            let roster_target_ids: Vec<String> = self
                .list_targets()
                .await?
                .into_iter()
                .map(|target| target.target_id)
                .collect();
            // The bucket's namespace binding is immutable, and a manifest-free version
            // has no stored namespace_id — resolve it from the bucket so the entry and
            // path rows clear under the right namespace.
            let bucket_namespace_id = self
                .get_bucket_write_context(bucket_id.clone())
                .await?
                .bucket
                .namespace_id;
            let value = self
                .db
                .run(move |trx, _| {
                    let head_key = head_key.clone();
                    let bucket_id = bucket_id.clone();
                    let key_path = key_path.clone();
                    let version_ids = version_ids.clone();
                    let roster_target_ids = roster_target_ids.clone();
                    let bucket_namespace_id = bucket_namespace_id.clone();
                    async move {
                        let Some(head_bytes) = trx
                            .get(&head_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                        else {
                            return Err(status_to_fdb(Status::not_found(format!(
                                "object {bucket_id}/{key_path} has no committed current version"
                            ))));
                        };
                        let head =
                            decode_object_head(head_bytes.as_ref()).map_err(status_to_fdb)?;
                        if !version_ids.is_empty()
                            && !version_ids
                                .iter()
                                .any(|version_id| version_id == &head.current_version_id)
                        {
                            return Err(status_to_fdb(Status::not_found(format!(
                                "object {bucket_id}/{key_path} has no requested versions to delete"
                            ))));
                        }

                        let version_key = object_version_key(&head.current_version_id);
                        let manifest_bytes = load_blob(&trx, &version_key, |chunk_index| {
                            object_version_chunk_key(&head.current_version_id, chunk_index)
                        })
                        .await?;
                        let manifest = match manifest_bytes {
                            Some(bytes) => {
                                // Manifest-backed version: the stored fragment list
                                // names every occupancy row to clear precisely.
                                let manifest =
                                    decode_manifest_bytes(&bytes).map_err(status_to_fdb)?;
                                clear_target_current_fragment_index(&trx, &manifest);
                                manifest
                            }
                            None => {
                                // Manifest-free (decentralized) version: the per-target
                                // reverse log IS the fragment record. Clear this
                                // version's reverse-log + occupancy ranges on every
                                // roster target. After this transaction the version is
                                // row-absent, marker-absent, and lease-absent, so the
                                // granule GC's decision table authorizes and frees each
                                // physically occupied granule through the existing
                                // tombstone protocol. Committed-granule markers are
                                // granule-keyed (no per-version range) and self-heal
                                // when a freed granule is next committed. Reads racing
                                // the delete stay safe: they address chunks by computed
                                // id through KIX, and a freed-then-reused granule binds
                                // a new chunk id, so a stale read fails "not found"
                                // rather than returning foreign bytes.
                                parse_version_id_parts(&head.current_version_id).map_err(|_| {
                                    status_to_fdb(Status::failed_precondition(format!(
                                        "version {} for {}/{} has no stored manifest and no canonical version id; it cannot be reclaimed",
                                        head.current_version_id, bucket_id, key_path
                                    )))
                                })?;
                                for target_id in &roster_target_ids {
                                    let (rl_begin, rl_end) = target_reverse_log_version_range(
                                        target_id,
                                        &head.current_version_id,
                                    );
                                    trx.clear_range(&rl_begin, &rl_end);
                                    let (occ_begin, occ_end) =
                                        target_current_fragment_version_range(
                                            target_id,
                                            &head.current_version_id,
                                        );
                                    trx.clear_range(&occ_begin, &occ_end);
                                }
                                // A minimal manifest for the reply: decentralized
                                // versions never stored one.
                                ObjectVersionManifest {
                                    version_id: head.current_version_id.clone(),
                                    bucket_id: bucket_id.clone(),
                                    key: key_path.clone(),
                                    logical_length_bytes: head.logical_length_bytes,
                                    ec_profile_id: head.ec_profile_id.clone(),
                                    object_entry_id: head.object_entry_id.clone(),
                                    namespace_id: bucket_namespace_id.clone(),
                                    ..Default::default()
                                }
                            }
                        };

                        trx.clear(&head_key);
                        trx.clear(&version_key);
                        let chunk_prefix = object_version_chunk_prefix(&head.current_version_id);
                        trx.clear_range(&chunk_prefix, &prefix_range_end(&chunk_prefix));
                        let entry_key = namespace_entry_key(
                            &manifest.namespace_id,
                            &head.object_entry_id,
                        );
                        // Clear the path index in the same transaction. Read the
                        // owning entry for its authoritative path so the index
                        // never drifts from the namespace entry it mirrors.
                        if let Some(entry_bytes) = trx
                            .get(&entry_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                        {
                            if let Ok(entry) = serde_json::from_slice::<NamespaceDomainEntry>(
                                entry_bytes.as_ref(),
                            ) {
                                trx.clear(&namespace_path_key(
                                    &entry.namespace_id,
                                    &entry.path,
                                ));
                            }
                        }
                        trx.clear(&entry_key);

                        Ok(DeletedObject {
                            bucket_id,
                            key: key_path,
                            deleted_versions: vec![DeletedObjectVersion { manifest }],
                        })
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            Ok(TimedStoreResult {
                value,
                phase_timings: Vec::new(),
            })
        }
    }

    async fn load_blob<F>(
        trx: &RetryableTransaction,
        meta_key: &[u8],
        chunk_key: F,
    ) -> Result<Option<Vec<u8>>, FdbBindingError>
    where
        F: Fn(u32) -> Vec<u8>,
    {
        let Some(meta_or_value) = trx
            .get(meta_key, false)
            .await
            .map_err(FdbBindingError::from)?
        else {
            return Ok(None);
        };
        if let Some((chunk_count, total_len)) = decode_blob_meta(meta_or_value.as_ref()) {
            let mut bytes = Vec::with_capacity(total_len);
            for chunk_index in 0..chunk_count {
                let chunk = trx
                    .get(&chunk_key(chunk_index), false)
                    .await
                    .map_err(FdbBindingError::from)?
                    .ok_or_else(|| {
                        status_to_fdb(Status::internal(format!(
                            "FoundationDB blob is missing chunk {}",
                            chunk_index
                        )))
                    })?;
                bytes.extend_from_slice(chunk.as_ref());
            }
            bytes.truncate(total_len);
            Ok(Some(bytes))
        } else {
            Ok(Some(meta_or_value.as_ref().to_vec()))
        }
    }

    fn store_blob<F>(
        trx: &RetryableTransaction,
        meta_key: &[u8],
        chunk_prefix: &[u8],
        chunk_key: F,
        bytes: &[u8],
    ) where
        F: Fn(u32) -> Vec<u8>,
    {
        trx.clear_range(chunk_prefix, &prefix_range_end(chunk_prefix));
        if bytes.len() <= MAX_FDB_BLOB_CHUNK_BYTES {
            trx.set(meta_key, bytes);
            return;
        }
        let chunk_count = bytes.chunks(MAX_FDB_BLOB_CHUNK_BYTES).len() as u32;
        trx.set(meta_key, &encode_blob_meta(chunk_count, bytes.len()));
        for (chunk_index, chunk) in bytes.chunks(MAX_FDB_BLOB_CHUNK_BYTES).enumerate() {
            trx.set(&chunk_key(chunk_index as u32), chunk);
        }
    }

    fn encode_blob_meta(chunk_count: u32, total_len: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(CHUNKED_BLOB_META_MAGIC.len() + 12);
        bytes.extend_from_slice(CHUNKED_BLOB_META_MAGIC);
        bytes.extend_from_slice(&chunk_count.to_be_bytes());
        bytes.extend_from_slice(&(total_len as u64).to_be_bytes());
        bytes
    }

    fn decode_blob_meta(bytes: &[u8]) -> Option<(u32, usize)> {
        if bytes.len() != CHUNKED_BLOB_META_MAGIC.len() + 12
            || !bytes.starts_with(CHUNKED_BLOB_META_MAGIC)
        {
            return None;
        }
        let chunk_count = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let total_len = u64::from_be_bytes([
            bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19],
        ]);
        usize::try_from(total_len)
            .ok()
            .map(|len| (chunk_count, len))
    }

    fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
        let mut end = prefix.to_vec();
        for index in (0..end.len()).rev() {
            if end[index] != u8::MAX {
                end[index] += 1;
                end.truncate(index + 1);
                return end;
            }
        }
        let mut end = prefix.to_vec();
        end.push(0);
        end
    }

    fn encode_object_head(head: &ObjectHead) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(head.encoded_len());
        prost::Message::encode(head, &mut bytes)
            .expect("prost message encoding to Vec<u8> should not fail");
        bytes
    }

    fn decode_object_head(bytes: &[u8]) -> Result<ObjectHead, Status> {
        prost::Message::decode(bytes).map_err(|err| {
            Status::internal(format!(
                "failed to decode object head protobuf payload: {err}"
            ))
        })
    }

    fn encode_target_record(target: &TargetRecord) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(target.encoded_len());
        prost::Message::encode(target, &mut bytes)
            .expect("prost message encoding to Vec<u8> should not fail");
        bytes
    }

    fn decode_target_record(bytes: &[u8]) -> Result<TargetRecord, Status> {
        prost::Message::decode(bytes).map_err(|err| {
            Status::internal(format!(
                "failed to decode target inventory protobuf payload: {err}"
            ))
        })
    }

    fn status_to_fdb(status: Status) -> FdbBindingError {
        FdbBindingError::new_custom_error(Box::new(StatusCarrier(status)))
    }

    fn map_fdb_binding_error(err: FdbBindingError) -> Status {
        match err {
            FdbBindingError::CustomError(error) => {
                if let Some(status) = error.downcast_ref::<StatusCarrier>() {
                    status.0.clone()
                } else {
                    Status::internal(format!("FoundationDB custom error: {error}"))
                }
            }
            other => {
                if let Some(fdb_error) = other.get_fdb_error() {
                    Status::internal(format!(
                        "FoundationDB error [{}]: {}",
                        fdb_error.code(),
                        fdb_error.message()
                    ))
                } else {
                    Status::internal(format!("FoundationDB binding error: {other}"))
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use crate::hot_store::{
        HotMetadataStore, OrphanReclaimDecision, OrphanVersionCandidate, OrphanVersionClaim,
        OrphanVersionCleanPage,
    };
    use crate::store::{
        BucketWriteContext, CommittedObjectWrite, CommittedObjectWriteWindow, DeletedObject,
        ReservedObjectWriteWindow, TimedStoreResult,
    };
    use keinctl::proto::{
        EcProfile, FragmentRef, ObjectHead, ObjectVersionManifest, PlacementReservationRecord,
        StripeManifest, TargetRecord, WriteIntent, WriteIntentState,
    };
    use std::error::Error;
    use tonic::Status;

    #[derive(Clone)]
    pub(crate) struct FdbHotStore;

    pub(crate) struct FdbNetworkGuard;

    pub(crate) fn maybe_boot_network(
        _client_threads: usize,
        _external_client_dir: &str,
    ) -> Result<Option<FdbNetworkGuard>, Box<dyn Error>> {
        Err(Box::<dyn Error>::from(
            "FoundationDB metadata backend is supported only on Linux",
        ))
    }

    impl FdbHotStore {
        pub(crate) fn connect(
            _cluster_file: &str,
            _client_threads: usize,
        ) -> Result<Self, Box<dyn Error>> {
            Err(Box::<dyn Error>::from(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }
    }

    #[tonic::async_trait]
    impl HotMetadataStore for FdbHotStore {
        async fn get_bucket_write_context(
            &self,
            _bucket_id: String,
        ) -> Result<BucketWriteContext, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn mint_object_id(
            &self,
            _bucket_id: &str,
            _key: &str,
        ) -> Result<(u32, u32), Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn prepare_and_create_write_intent(
            &self,
            _intent: WriteIntent,
            _bucket_entry_id: String,
            _bucket_path: String,
            _parent_hint: Option<(String, String)>,
        ) -> Result<TimedStoreResult<WriteIntent>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn list_write_intents(&self) -> Result<Vec<WriteIntent>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn get_write_intent(
            &self,
            _intent_id: String,
        ) -> Result<Option<WriteIntent>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn reserve_object_write_window(
            &self,
            _intent_id: String,
            _start_stripe_index: u32,
            _reservations: Vec<PlacementReservationRecord>,
        ) -> Result<TimedStoreResult<ReservedObjectWriteWindow>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn commit_object_write_window(
            &self,
            _intent_id: String,
            _successful_fragments: Vec<FragmentRef>,
        ) -> Result<TimedStoreResult<CommittedObjectWriteWindow>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn commit_object_write(
            &self,
            _intent_id: String,
            _successful_fragments: Vec<FragmentRef>,
            _finalization_sweep_after_ms: u64,
        ) -> Result<TimedStoreResult<CommittedObjectWrite>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn commit_object_single_shot(
            &self,
            _commit: crate::hot_store::SingleShotCommit,
        ) -> Result<ObjectHead, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn commit_objects_single_shot_batch(
            &self,
            _commits: Vec<crate::hot_store::SingleShotCommit>,
        ) -> Result<Vec<Result<ObjectHead, Status>>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn append_manifest_segment(
            &self,
            _version_id: String,
            _stripes: Vec<StripeManifest>,
            _marker_expires_at_unix_ms: u64,
            _marker_fenced: bool,
        ) -> Result<(), Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn sweep_reclaim_fences(
            &self,
            _now_unix_ms: u64,
            _older_than_ms: u64,
            _row_limit: usize,
            _cursor: Option<Vec<u8>>,
        ) -> Result<(u64, Option<Vec<u8>>), Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn backfill_reclaim_fences(&self, _now_unix_ms: u64) -> Result<u64, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn get_or_init_cluster_salt(&self) -> Result<Vec<u8>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn get_object_head(
            &self,
            _bucket_id: String,
            _key_path: String,
        ) -> Result<Option<ObjectHead>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn begin_object_write(
            &self,
            _object_id: u32,
            _object_version: u32,
            _bucket_id: &str,
            _key: &str,
            _expires_at_unix_ms: u64,
        ) -> Result<(), Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn renew_object_write(
            &self,
            _object_id: u32,
            _new_expires_at_unix_ms: u64,
        ) -> Result<(), Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn begin_object_write_resolved(
            &self,
            _bucket_id: &str,
            _key: &str,
            _namespace_id: &str,
            _bucket_entry_id: &str,
            _bucket_path: &str,
            _parent_hint: Option<(String, String)>,
            _expires_at_unix_ms: u64,
        ) -> Result<(u32, u32, String, String), Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn forfeit_object_write(
            &self,
            _object_id: u32,
            _object_version: u32,
        ) -> Result<(), Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn authorize_orphan_reclaim(
            &self,
            _target_id: String,
            _object_id: u32,
            _object_version: u32,
            _stripe_index: u32,
            _fragment_index: u32,
            _now_unix_ms: u64,
            _lease_grace_ms: u64,
        ) -> Result<OrphanReclaimDecision, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn scan_pending_versions(
            &self,
            _cursor: Option<Vec<u8>>,
            _row_limit: usize,
            _now_unix_ms: u64,
            _claim_grace_ms: u64,
        ) -> Result<(Vec<OrphanVersionCandidate>, Option<Vec<u8>>, u64), Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn claim_orphan_version(
            &self,
            _object_id: u32,
            _object_version: u32,
            _now_unix_ms: u64,
            _claim_grace_ms: u64,
            _lease_grace_ms: u64,
        ) -> Result<OrphanVersionClaim, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn clean_orphan_version_target_page(
            &self,
            _object_id: u32,
            _object_version: u32,
            _target_id: String,
            _cursor: Option<Vec<u8>>,
            _row_limit: usize,
        ) -> Result<OrphanVersionCleanPage, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn finish_orphan_version(
            &self,
            _object_id: u32,
            _object_version: u32,
        ) -> Result<bool, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn register_target(&self, _target: TargetRecord) -> Result<TargetRecord, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn heartbeat_target(
            &self,
            _target_id: String,
            _healthy: bool,
            _observed_unix_ms: u64,
        ) -> Result<TargetRecord, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn set_target_lifecycle(
            &self,
            _target_id: String,
            _lifecycle_state: i32,
            _now_unix_ms: u64,
        ) -> Result<TargetRecord, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn list_targets(&self) -> Result<Vec<TargetRecord>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn reap_expired_leases(
            &self,
            _now_unix_ms: u64,
            _grace_ms: u64,
            _limit: usize,
        ) -> Result<usize, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn abort_object_write(
            &self,
            _intent_id: String,
            _next_state: WriteIntentState,
        ) -> Result<WriteIntent, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn repair_object_write(
            &self,
            _intent_id: String,
            _failed_fragments: Vec<FragmentRef>,
            _replacement_reservation: PlacementReservationRecord,
        ) -> Result<WriteIntent, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn mark_write_intent_reservations_finalized(
            &self,
            _intent_id: String,
        ) -> Result<(), Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn list_pending_finalization_intents(
            &self,
            _limit: usize,
            _now_ms: u64,
        ) -> Result<Vec<WriteIntent>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn resolve_object_read(
            &self,
            _bucket_id: String,
            _key_path: String,
        ) -> Result<(ObjectVersionManifest, EcProfile), Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }

        async fn delete_object(
            &self,
            _bucket_id: String,
            _key_path: String,
            _version_ids: Vec<String>,
        ) -> Result<TimedStoreResult<DeletedObject>, Status> {
            Err(Status::unimplemented(
                "FoundationDB metadata backend is supported only on Linux",
            ))
        }
    }
}

pub(crate) use imp::{maybe_boot_network, FdbHotStore};
