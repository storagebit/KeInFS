// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

#[cfg(target_os = "linux")]
mod imp {
    use crate::fdb_schema::{
        bucket_context_key, cluster_salt_key, decode_segments, ec_profile_key, namespace_entry_key,
        namespace_path_key, object_head_key, object_id_counter_key, object_version_chunk_key,
        object_version_chunk_prefix, object_version_key, target_reverse_log_key,
        write_intent_chunk_key, write_intent_chunk_prefix, write_intent_key, write_intent_range,
    };
    use crate::hot_store::HotMetadataStore;
    use crate::store::{
        apply_fragment_repair, auto_create_levels, build_finalize_plans,
        clear_target_current_fragment_index, decode_manifest_bytes, decode_write_intent_bytes,
        encode_manifest, encode_reverse_log_value, encode_write_intent, expected_fragment_count,
        fragment_plans_for_window, join_path, mark_successful_fragments, normalize_object_key,
        normalize_write_intent, random_chunk_id, random_salt,
        write_target_current_fragment_index, AutoCreateLevel, BucketWriteContext,
        CommittedObjectWrite, CommittedObjectWriteWindow, DeletedObject, DeletedObjectVersion,
        ReservedObjectWriteWindow, StoredBucketWriteContext, TimedStoreResult,
    };
    use foundationdb::{api::NetworkAutoStop, Database, FdbBindingError, RetryableTransaction};
    use futures_util::StreamExt;
    use keinctl::proto::{
        EcProfile, FragmentPlan, FragmentRef, FragmentWriteState, FragmentWriteStatus,
        NamespaceDomainEntry, NamespaceEntryKind, ObjectHead, ObjectVersionManifest,
        PlacementReservationRecord, StripeManifest, WriteIntent, WriteIntentState,
    };
    use prost::Message;
    use std::error::Error;
    use std::fmt::{Display, Formatter};
    use std::sync::Arc;
    use tonic::Status;
    use uuid::Uuid;

    #[derive(Clone)]
    pub(crate) struct FdbHotStore {
        db: Arc<Database>,
    }

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
    // would breach the FDB limit.
    const SINGLE_SHOT_PER_FRAGMENT_TXN_BYTES: usize = 512;

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

    impl FdbHotStore {
        pub(crate) fn connect(cluster_file: &str) -> Result<Self, Box<dyn Error>> {
            let db = if cluster_file.trim().is_empty() {
                Database::default()?
            } else {
                Database::from_path(cluster_file)?
            };
            Ok(Self { db: Arc::new(db) })
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
            let counter_key = object_id_counter_key();
            let head_key = object_head_key(bucket_id, &normalized_key);
            self.db
                .run(move |trx, _| {
                    let counter_key = counter_key.clone();
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
                        // Globally-monotonic object_id via read-modify-write; FDB
                        // serializability keeps it monotonic under conflict retry.
                        let next_id = trx
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
                            .unwrap_or(0)
                            .saturating_add(1);
                        trx.set(&counter_key, &next_id.to_le_bytes());
                        Ok((next_id, version))
                    }
                })
                .await
                .map_err(map_fdb_binding_error)
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
                        // Self-heal a stale parent hint. The hint is sourced
                        // from an in-memory cache in the service layer that is
                        // NOT invalidated when the parent collection is deleted.
                        // Trusting it blindly would commit the object under a
                        // dangling parent id (the very orphaning the auto-create
                        // walk exists to prevent). Re-validate IN THIS
                        // TRANSACTION that the hinted parent's path still maps to
                        // the hinted id via the path index (one point get); if it
                        // does not, drop the hint and fall through to the
                        // auto-create walk so the parent is re-materialized.
                        let parent_hint = if let Some((parent_entry_id, parent_path)) = parent_hint {
                            let index_id = trx
                                .get(
                                    &namespace_path_key(&intent.namespace_id, &parent_path),
                                    false,
                                )
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
                        let parent_context =
                            if let Some((parent_entry_id, parent_path)) = parent_hint {
                                (parent_entry_id, parent_path)
                            } else if let Some((parent_key, _)) = normalized_key.rsplit_once('/') {
                                // Slashed key (e.g. "a/b/c.txt"): the parent is a
                                // collection "a/b" that may not exist yet. Walk
                                // every path segment from the bucket root down and
                                // auto-create (mkdir -p) any missing collection
                                // entry IN THIS SAME TRANSACTION, so the object is
                                // never orphaned under a phantom parent id. The
                                // deepest collection becomes the parent.
                                //
                                // Each level is resolved via the namespace path
                                // index with a single point get() (no range scan,
                                // minimal read-conflict footprint; the index is
                                // maintained in the same transactions that mutate
                                // namespace entries — see namespace_path_key in
                                // fdb_schema.rs). Walk shallow->deep so parents
                                // exist before their children.
                                let mut parent_id = bucket_entry_id.clone();
                                let mut parent_path = bucket_path.clone();
                                // Pure level synthesis (prefix accumulation,
                                // level_path, deterministic id) lives in
                                // store::auto_create_levels and is unit-tested;
                                // the FDB reads/writes per level stay here.
                                for level in
                                    auto_create_levels(&bucket_entry_id, &bucket_path, parent_key)
                                {
                                    let AutoCreateLevel {
                                        segment,
                                        prefix: _,
                                        level_path,
                                        deterministic_id,
                                    } = level;
                                    // `segment` is the bare directory-component
                                    // name used below for the collection entry.
                                    let level_index_key =
                                        namespace_path_key(&intent.namespace_id, &level_path);
                                    // Idempotency: reuse an existing collection id
                                    // if the path index already maps this level;
                                    // never overwrite an established entry.
                                    let existing_id = trx
                                        .get(&level_index_key, false)
                                        .await
                                        .map_err(FdbBindingError::from)?
                                        .map(|bytes| {
                                            String::from_utf8(bytes.as_ref().to_vec()).map_err(
                                                |err| {
                                                    status_to_fdb(Status::internal(format!(
                                                "namespace path index value is not valid utf-8: {err}"
                                            )))
                                                },
                                            )
                                        })
                                        .transpose()?;
                                    let level_id = if let Some(id) = existing_id {
                                        // Idempotent reuse is only safe if the
                                        // resolved entry is directory-like. The
                                        // path index is shared across bucket,
                                        // collection AND object entries, so a
                                        // prior object committed at this exact
                                        // path (a file named like a directory
                                        // component) would otherwise be reused
                                        // as a parent, nesting an object under
                                        // another object. Load the owning entry
                                        // (one extra point get per reused level,
                                        // still bounded by path depth) and
                                        // reject if it is an Object.
                                        let entry_bytes = trx
                                            .get(
                                                &namespace_entry_key(&intent.namespace_id, &id),
                                                false,
                                            )
                                            .await
                                            .map_err(FdbBindingError::from)?;
                                        if let Some(entry_bytes) = entry_bytes {
                                            let entry = serde_json::from_slice::<NamespaceDomainEntry>(
                                                entry_bytes.as_ref(),
                                            )
                                            .map_err(|err| {
                                                status_to_fdb(Status::internal(format!(
                                                    "failed to decode namespace entry JSON payload: {err}"
                                                )))
                                            })?;
                                            if entry.kind == NamespaceEntryKind::Object as i32 {
                                                return Err(status_to_fdb(
                                                    Status::failed_precondition(format!(
                                                        "cannot create object under non-directory path component {level_path}"
                                                    )),
                                                ));
                                            }
                                        }
                                        id
                                    } else {
                                        // Deterministic id consistent with the
                                        // previous synthesis: {bucket}::<prefix>.
                                        let id = deterministic_id;
                                        let collection = NamespaceDomainEntry {
                                            entry_id: id.clone(),
                                            namespace_id: intent.namespace_id.clone(),
                                            parent_entry_id: parent_id.clone(),
                                            name: segment,
                                            // Collection is the directory-kind
                                            // entry; kfc maps any non-Object kind
                                            // to a directory.
                                            kind: NamespaceEntryKind::Collection as i32,
                                            path: level_path.clone(),
                                            size_bytes: 0,
                                        };
                                        trx.set(
                                            &namespace_entry_key(&intent.namespace_id, &id),
                                            &serde_json::to_vec(&collection).map_err(|err| {
                                                status_to_fdb(Status::internal(format!(
                                                    "failed to encode namespace entry JSON payload: {err}"
                                                )))
                                            })?,
                                        );
                                        // Maintain the path -> entry_id index in
                                        // the same transaction (mirrors the object
                                        // entry persistence below).
                                        trx.set(&level_index_key, id.as_bytes());
                                        id
                                    };
                                    parent_id = level_id;
                                    parent_path = level_path;
                                }
                                (parent_id, parent_path)
                            } else {
                                (bucket_entry_id.clone(), bucket_path.clone())
                            };

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
            expected_prior_version: u32,
            manifest: ObjectVersionManifest,
            parent_entry_id: String,
            parent_path: String,
            topology_epoch: u64,
            omit_manifest: bool,
        ) -> Result<ObjectHead, Status> {
            let mut manifest = manifest;
            manifest.key = normalize_object_key(&manifest.key)?;
            let head_key = object_head_key(&manifest.bucket_id, &manifest.key);
            self.db
                .run(move |trx, _| {
                    let mut manifest = manifest.clone();
                    let head_key = head_key.clone();
                    let parent_entry_id = parent_entry_id.clone();
                    let parent_path = parent_path.clone();
                    async move {
                        // The single transaction reads + validates the head BEFORE
                        // issuing any write, so a CAS loser or oversized manifest
                        // leaves no partial state (HARD INVARIANT).
                        let prior = trx
                            .get(&head_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| decode_object_head(bytes.as_ref()))
                            .transpose()
                            .map_err(status_to_fdb)?;

                        // Idempotent success: a retried commit whose own version
                        // already won finds the live head pointing at its version_id.
                        // Return it OK regardless of the CAS witness, which the client
                        // may have refreshed between attempts.
                        if let Some(existing) = &prior {
                            if existing.current_version_id == manifest.version_id {
                                return Ok(existing.clone());
                            }
                        }

                        // Compare-and-swap on the live version.
                        let witnessed = prior.as_ref().map(|head| head.version).unwrap_or(0);
                        if witnessed != expected_prior_version {
                            return Err(status_to_fdb(Status::failed_precondition(format!(
                                "single-shot commit version mismatch for {}/{}: live head version {} != expected {}",
                                manifest.bucket_id, manifest.key, witnessed, expected_prior_version
                            ))));
                        }

                        // Create-only: overwriting requires a per-object write lease
                        // and lease-fenced GC, which do not exist yet; without them an
                        // overwrite would orphan the superseded version's granules, so
                        // an existing head is refused rather than replaced.
                        if prior.is_some() {
                            return Err(status_to_fdb(Status::failed_precondition(format!(
                                "single-shot commit is create-only and {}/{} already exists",
                                manifest.bucket_id, manifest.key
                            ))));
                        }

                        // From here the commit will write. Finalize the object entry
                        // id (the write-intent path mints it and carries it on the
                        // manifest; mint a fresh one only if absent), then encode the
                        // manifest once and reject anything too large for one txn. The
                        // budget bounds the WHOLE transaction (manifest blob + the
                        // per-fragment reverse-log/index/occupancy writes), not just the
                        // manifest, so the commit fails fast instead of mid-transaction.
                        if manifest.object_entry_id.is_empty() {
                            manifest.object_entry_id = Uuid::new_v4().to_string();
                        }
                        // The decentralized path persists NO manifest blob (the per-target
                        // reverse log is the per-fragment record and reads recompute the
                        // layout), so skip encoding it; otherwise encode once for the blob.
                        let manifest_bytes = if omit_manifest {
                            Vec::new()
                        } else {
                            encode_manifest(&manifest)
                        };
                        let fragment_count: usize =
                            manifest.stripes.iter().map(|s| s.fragments.len()).sum();
                        let estimated_txn_bytes = manifest_bytes.len().saturating_add(
                            fragment_count.saturating_mul(SINGLE_SHOT_PER_FRAGMENT_TXN_BYTES),
                        );
                        if estimated_txn_bytes > MAX_SINGLE_SHOT_TXN_BYTES {
                            return Err(status_to_fdb(Status::failed_precondition(format!(
                                "single-shot commit for {}/{} would write ~{} bytes across {} fragments, over the {}-byte transaction budget",
                                manifest.bucket_id,
                                manifest.key,
                                estimated_txn_bytes,
                                fragment_count,
                                MAX_SINGLE_SHOT_TXN_BYTES
                            ))));
                        }

                        let object_name = manifest
                            .key
                            .rsplit('/')
                            .next()
                            .unwrap_or(manifest.key.as_str())
                            .to_string();
                        let object_entry = NamespaceDomainEntry {
                            entry_id: manifest.object_entry_id.clone(),
                            namespace_id: manifest.namespace_id.clone(),
                            parent_entry_id: parent_entry_id.clone(),
                            name: object_name.clone(),
                            kind: NamespaceEntryKind::Object as i32,
                            // Denormalize the size at commit so listings carry it
                            // without a per-object resolve (mirrors commit_object_write).
                            path: join_path(&parent_path, &object_name),
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
                            version: expected_prior_version + 1,
                            logical_length_bytes: manifest.logical_length_bytes,
                            ec_profile_id: manifest.ec_profile_id.clone(),
                            // The placement topology the client computed against; 0 when
                            // the committer does not compute placement.
                            topology_epoch,
                        };

                        // --- Writes (all reads/validation above are complete) ---
                        // The manifest blob is the thing the decentralized design drops:
                        // persist it only when the committer wants a manifest-readable
                        // object (the reverse log below is the per-fragment record either way).
                        if !omit_manifest {
                            store_blob(
                                &trx,
                                &object_version_key(&manifest.version_id),
                                &object_version_chunk_prefix(&manifest.version_id),
                                |chunk_index| {
                                    object_version_chunk_key(&manifest.version_id, chunk_index)
                                },
                                &manifest_bytes,
                            );
                        }
                        // Per-target reverse log: the durable target->object map that
                        // rebuild/GC range-scan by target_id. Keyed by version_id so it
                        // is idempotent under FDB transaction retry.
                        for stripe in &manifest.stripes {
                            for fragment in &stripe.fragments {
                                trx.set(
                                    &target_reverse_log_key(
                                        &fragment.target_id,
                                        &manifest.version_id,
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
                        // Retain the committed-occupancy markers + legacy secondary
                        // index: while reservations remain on the write path, the
                        // allocator/reaper relies on them to know a granule is taken.
                        write_target_current_fragment_index(&trx, &manifest);
                        // Namespace entry + path index, mirroring commit_object_write.
                        trx.set(
                            &namespace_entry_key(
                                &manifest.namespace_id,
                                &manifest.object_entry_id,
                            ),
                            &object_entry_json,
                        );
                        trx.set(
                            &namespace_path_key(&manifest.namespace_id, &object_entry.path),
                            manifest.object_entry_id.as_bytes(),
                        );
                        // CAS head-flip last: the object becomes resolvable only once
                        // every fragment, the reverse log, and the entry are durable.
                        trx.set(&head_key, &encode_object_head(&head));
                        Ok(head)
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
            let value = self
                .db
                .run(move |trx, _| {
                    let head_key = head_key.clone();
                    let bucket_id = bucket_id.clone();
                    let key_path = key_path.clone();
                    let version_ids = version_ids.clone();
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
                        .await?
                        .ok_or_else(|| {
                            status_to_fdb(Status::not_found(format!(
                                "version {} for {}/{} is missing",
                                head.current_version_id, bucket_id, key_path
                            )))
                        })?;
                        let manifest =
                            decode_manifest_bytes(&manifest_bytes).map_err(status_to_fdb)?;

                        clear_target_current_fragment_index(&trx, &manifest);
                        trx.clear(&head_key);
                        trx.clear(&version_key);
                        let chunk_prefix = object_version_chunk_prefix(&head.current_version_id);
                        trx.clear_range(&chunk_prefix, &prefix_range_end(&chunk_prefix));
                        let entry_key = namespace_entry_key(
                            &manifest.namespace_id,
                            &manifest.object_entry_id,
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
    use crate::hot_store::HotMetadataStore;
    use crate::store::{
        BucketWriteContext, CommittedObjectWrite, CommittedObjectWriteWindow, DeletedObject,
        ReservedObjectWriteWindow, TimedStoreResult,
    };
    use keinctl::proto::{
        EcProfile, FragmentRef, ObjectHead, ObjectVersionManifest, PlacementReservationRecord,
        WriteIntent, WriteIntentState,
    };
    use std::error::Error;
    use tonic::Status;

    #[derive(Clone)]
    pub(crate) struct FdbHotStore;

    pub(crate) struct FdbNetworkGuard;

    pub(crate) fn maybe_boot_network() -> Result<Option<FdbNetworkGuard>, Box<dyn Error>> {
        Err(Box::<dyn Error>::from(
            "FoundationDB metadata backend is supported only on Linux",
        ))
    }

    impl FdbHotStore {
        pub(crate) fn connect(_cluster_file: &str) -> Result<Self, Box<dyn Error>> {
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
            _expected_prior_version: u32,
            _manifest: ObjectVersionManifest,
            _parent_entry_id: String,
            _parent_path: String,
            _topology_epoch: u64,
            _omit_manifest: bool,
        ) -> Result<ObjectHead, Status> {
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
