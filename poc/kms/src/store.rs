// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

#![cfg_attr(any(test, not(target_os = "linux")), allow(dead_code))]

#[cfg(target_os = "linux")]
use crate::fdb_schema::{
    bucket_context_key, bucket_record_key, decode_segments, ec_profile_key, ec_profile_range,
    maintenance_marker_key, namespace_entry_key, namespace_entry_prefix, namespace_key,
    namespace_path_key,
    object_head_key, object_head_range, object_version_chunk_key, object_version_chunk_prefix,
    object_version_key, placement_task_key, placement_task_range, prefix_end,
    target_current_fragment_key, target_current_fragment_range,
};
use keinctl::proto::{
    BucketRecord, EcProfile, FragmentPlan, FragmentRef, FragmentWriteState, FragmentWriteStatus,
    LeasedPlacementTask, LeasedRebuildTask, MetadataEvent, NamespaceDomainEntry, NamespaceRecord,
    ObjectVersionManifest, PlacementReservationRecord, PlacementTask, PlacementTaskKind,
    PlacementTaskState, PlacementTaskSummary, ResolvePathReply, ShardMapEntry,
    TargetPlacementStatus, WriteIntent,
};
#[cfg(target_os = "linux")]
use keinctl::proto::{NamespaceEntryKind, ObjectHead, RebuildTask};
use prost::Message;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
#[cfg(target_os = "linux")]
use std::fmt::{Display, Formatter};
#[cfg(target_os = "linux")]
use std::sync::Arc;
use std::time::Duration;
use tonic::Status;

#[cfg(target_os = "linux")]
use foundationdb::{Database, FdbBindingError, RangeOption};
#[cfg(target_os = "linux")]
use tokio_stream::StreamExt;

#[derive(Clone, Debug)]
pub(crate) struct BucketWriteContext {
    pub(crate) bucket: BucketRecord,
    pub(crate) bucket_entry: NamespaceDomainEntry,
    pub(crate) ec_profile: EcProfile,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StoredBucketWriteContext {
    pub(crate) bucket: BucketRecord,
    pub(crate) bucket_entry: NamespaceDomainEntry,
    pub(crate) ec_profile: EcProfile,
}

impl From<BucketWriteContext> for StoredBucketWriteContext {
    fn from(value: BucketWriteContext) -> Self {
        Self {
            bucket: value.bucket,
            bucket_entry: value.bucket_entry,
            ec_profile: value.ec_profile,
        }
    }
}

impl From<StoredBucketWriteContext> for BucketWriteContext {
    fn from(value: StoredBucketWriteContext) -> Self {
        Self {
            bucket: value.bucket,
            bucket_entry: value.bucket_entry,
            ec_profile: value.ec_profile,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CommittedObjectWrite {
    pub(crate) intent_id: String,
    pub(crate) manifest: ObjectVersionManifest,
    pub(crate) reservation_ids: Vec<String>,
    pub(crate) finalize_plans: Vec<ReservationFinalizePlan>,
}

#[derive(Clone, Debug)]
pub(crate) struct ReservedObjectWriteWindow {
    pub(crate) fragment_plans: Vec<FragmentPlan>,
    pub(crate) used_reservations: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct CommittedObjectWriteWindow {
    pub(crate) intent_id: String,
    pub(crate) reservation_ids: Vec<String>,
    pub(crate) finalize_plans: Vec<ReservationFinalizePlan>,
}

#[derive(Clone, Debug)]
pub(crate) struct DeletedObjectVersion {
    pub(crate) manifest: ObjectVersionManifest,
}

#[derive(Clone, Debug)]
pub(crate) struct DeletedObject {
    pub(crate) bucket_id: String,
    pub(crate) key: String,
    pub(crate) deleted_versions: Vec<DeletedObjectVersion>,
}

#[derive(Clone, Debug)]
pub(crate) struct StorePhaseTiming {
    pub(crate) name: &'static str,
    pub(crate) elapsed: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct TimedStoreResult<T> {
    pub(crate) value: T,
    pub(crate) phase_timings: Vec<StorePhaseTiming>,
}

#[derive(Clone, Debug)]
pub(crate) struct ReservationFinalizePlan {
    pub(crate) reservation_id: String,
    pub(crate) placement_indexes: Vec<u32>,
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
pub(crate) struct KmsStore {
    db: Arc<Database>,
}

#[cfg(target_os = "linux")]
const CHUNKED_BLOB_META_MAGIC: &[u8; 8] = b"KFBLOB01";

#[cfg(target_os = "linux")]
const MAX_FDB_BLOB_CHUNK_BYTES: usize = 80_000;

#[cfg(target_os = "linux")]
const TARGET_CURRENT_FRAGMENT_BACKFILL_MARKER: &str = "target-current-fragment-backfill-v1";

#[cfg(target_os = "linux")]
const TARGET_MAINTENANCE_BATCH_VERSIONS: usize = 256;
#[cfg(target_os = "linux")]
const TARGET_MAINTENANCE_BATCH_TASKS: u32 = 2_048;

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct CurrentManifestRecord {
    manifest: ObjectVersionManifest,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct PlacementTaskRecord {
    task: PlacementTask,
    manifest: ObjectVersionManifest,
    ec_profile: EcProfile,
}

#[cfg(not(target_os = "linux"))]
#[derive(Clone, Default)]
pub(crate) struct KmsStore;

impl KmsStore {
    #[cfg(target_os = "linux")]
    pub(crate) async fn connect(cluster_file: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let db = if cluster_file.trim().is_empty() {
            Database::default()?
        } else {
            Database::from_path(cluster_file)?
        };
        Ok(Self { db: Arc::new(db) })
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn connect(_cluster_file: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self)
    }

    pub(crate) async fn init(&self) -> Result<(), Status> {
        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn load_all<T>(&self, begin: Vec<u8>, end: Vec<u8>) -> Result<Vec<T>, Status>
    where
        T: for<'de> Deserialize<'de>,
    {
        let values = self
            .db
            .run(move |trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut stream =
                        trx.get_ranges_keyvalues(RangeOption::from((begin, end)), false);
                    let mut values = Vec::new();
                    while let Some(next) = stream.next().await {
                        let kv = next?;
                        values.push(kv.value().to_vec());
                    }
                    Ok::<Vec<Vec<u8>>, FdbBindingError>(values)
                }
            })
            .await
            .map_err(map_fdb_binding_error)?;
        values
            .into_iter()
            .map(|bytes| decode_json::<T>(&bytes, "range value"))
            .collect()
    }

    #[cfg(target_os = "linux")]
    async fn load_manifest_for_version(
        &self,
        version_id: &str,
    ) -> Result<Option<ObjectVersionManifest>, Status> {
        let version_key = object_version_key(version_id);
        let version_id = version_id.to_string();
        let bytes = self
            .db
            .run(move |trx, _| {
                let version_key = version_key.clone();
                let version_id = version_id.clone();
                async move {
                    load_blob(&trx, &version_key, |chunk_index| {
                        object_version_chunk_key(&version_id, chunk_index)
                    })
                    .await
                }
            })
            .await
            .map_err(map_fdb_binding_error)?;
        bytes.map(|value| decode_manifest_bytes(&value)).transpose()
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn backfill_target_current_fragment_index(&self) -> Result<(), Status> {
        if self.target_current_fragment_backfill_complete().await? {
            return Ok(());
        }
        let (begin, end) = object_head_range();
        let head_bytes = self
            .db
            .run(move |trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut stream =
                        trx.get_ranges_keyvalues(RangeOption::from((begin, end)), false);
                    let mut values = Vec::new();
                    while let Some(next) = stream.next().await {
                        let kv = next?;
                        values.push(kv.value().to_vec());
                    }
                    Ok::<Vec<Vec<u8>>, FdbBindingError>(values)
                }
            })
            .await
            .map_err(map_fdb_binding_error)?;
        let version_ids = head_bytes
            .into_iter()
            .map(|bytes| {
                decode_proto_message::<ObjectHead>(&bytes, "object head")
                    .map(|head| head.current_version_id)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let total = version_ids.len();
        let mut processed = 0usize;
        for version_id in version_ids {
            self.db
                .run(move |trx, _| {
                    let version_id = version_id.clone();
                    async move {
                        let Some(manifest_bytes) =
                            load_blob(&trx, &object_version_key(&version_id), |chunk_index| {
                                object_version_chunk_key(&version_id, chunk_index)
                            })
                            .await?
                        else {
                            return Ok::<(), FdbBindingError>(());
                        };
                        let manifest =
                            decode_manifest_bytes(&manifest_bytes).map_err(status_to_fdb)?;
                        write_target_current_fragment_index(&trx, &manifest);
                        Ok::<(), FdbBindingError>(())
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            processed = processed.saturating_add(1);
            if processed == total || processed % 256 == 0 {
                eprintln!("kms: target-current-fragment backfill progress {processed}/{total}");
            }
            if processed % 32 == 0 {
                tokio::task::yield_now().await;
            }
        }
        let marker_key = maintenance_marker_key(TARGET_CURRENT_FRAGMENT_BACKFILL_MARKER);
        self.db
            .run(move |trx, _| {
                let marker_key = marker_key.clone();
                async move {
                    trx.set(&marker_key, b"complete");
                    Ok::<(), FdbBindingError>(())
                }
            })
            .await
            .map_err(map_fdb_binding_error)?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn target_current_fragment_backfill_complete(&self) -> Result<bool, Status> {
        let marker_key = maintenance_marker_key(TARGET_CURRENT_FRAGMENT_BACKFILL_MARKER);
        self.db
            .run(move |trx, _| {
                let marker_key = marker_key.clone();
                async move {
                    let value = trx
                        .get(&marker_key, false)
                        .await
                        .map_err(FdbBindingError::from)?;
                    Ok::<bool, FdbBindingError>(value.is_some())
                }
            })
            .await
            .map_err(map_fdb_binding_error)
    }

    #[cfg(target_os = "linux")]
    async fn load_current_manifest_records_for_versions(
        &self,
        mut version_ids: Vec<String>,
    ) -> Result<Vec<CurrentManifestRecord>, Status> {
        version_ids.sort();
        version_ids.dedup();
        let mut records = Vec::with_capacity(version_ids.len());
        for batch in version_ids.chunks(8) {
            let batch = batch.to_vec();
            let manifests = self
                .db
                .run(move |trx, _| {
                    let batch = batch.clone();
                    async move {
                        let mut manifests = Vec::with_capacity(batch.len());
                        for version_id in &batch {
                            let Some(manifest_bytes) =
                                load_blob(&trx, &object_version_key(version_id), |chunk_index| {
                                    object_version_chunk_key(version_id, chunk_index)
                                })
                                .await?
                            else {
                                continue;
                            };
                            let manifest =
                                decode_manifest_bytes(&manifest_bytes).map_err(status_to_fdb)?;
                            let Some(head_bytes) = trx
                                .get(&object_head_key(&manifest.bucket_id, &manifest.key), false)
                                .await
                                .map_err(FdbBindingError::from)?
                            else {
                                continue;
                            };
                            let head: ObjectHead =
                                decode_proto_message(head_bytes.as_ref(), "object head")
                                    .map_err(status_to_fdb)?;
                            if head.current_version_id == manifest.version_id {
                                manifests.push(manifest);
                            }
                        }
                        Ok::<Vec<ObjectVersionManifest>, FdbBindingError>(manifests)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            records.extend(
                manifests
                    .into_iter()
                    .map(|manifest| CurrentManifestRecord { manifest }),
            );
        }
        Ok(records)
    }

    #[cfg(target_os = "linux")]
    async fn count_target_current_fragments(&self, target_id: &str) -> Result<u64, Status> {
        let (begin, end) = target_current_fragment_range(target_id);
        self.db
            .run(move |trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut stream =
                        trx.get_ranges_keyvalues(RangeOption::from((begin, end)), false);
                    let mut count = 0u64;
                    while let Some(next) = stream.next().await {
                        next?;
                        count = count.saturating_add(1);
                    }
                    Ok::<u64, FdbBindingError>(count)
                }
            })
            .await
            .map_err(map_fdb_binding_error)
    }

    #[cfg(target_os = "linux")]
    async fn list_target_current_version_ids_limited(
        &self,
        target_id: &str,
        max_versions: usize,
    ) -> Result<Vec<String>, Status> {
        if max_versions == 0 {
            return Ok(Vec::new());
        }
        let (begin, end) = target_current_fragment_range(target_id);
        self.db
            .run(move |trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut stream =
                        trx.get_ranges_keyvalues(RangeOption::from((begin, end)), false);
                    let mut version_ids = Vec::new();
                    let mut previous_version_id: Option<String> = None;
                    while let Some(next) = stream.next().await {
                        let kv = next?;
                        let version_id = decode_target_current_fragment_version_id(kv.key())
                            .map_err(status_to_fdb)?;
                        if previous_version_id.as_ref() != Some(&version_id) {
                            previous_version_id = Some(version_id.clone());
                            version_ids.push(version_id);
                            if version_ids.len() >= max_versions {
                                break;
                            }
                        }
                    }
                    Ok::<Vec<String>, FdbBindingError>(version_ids)
                }
            })
            .await
            .map_err(map_fdb_binding_error)
    }

    #[cfg(target_os = "linux")]
    async fn list_all_placement_tasks(&self) -> Result<Vec<PlacementTask>, Status> {
        let (begin, end) = placement_task_range();
        let values = self
            .db
            .run(move |trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut stream =
                        trx.get_ranges_keyvalues(RangeOption::from((begin, end)), false);
                    let mut values = Vec::new();
                    while let Some(next) = stream.next().await {
                        let kv = next?;
                        values.push(kv.value().to_vec());
                    }
                    Ok::<Vec<Vec<u8>>, FdbBindingError>(values)
                }
            })
            .await
            .map_err(map_fdb_binding_error)?;
        values
            .into_iter()
            .map(|bytes| decode_proto_message(&bytes, "placement task"))
            .collect()
    }

    #[cfg(target_os = "linux")]
    async fn load_placement_task_record(
        &self,
        task_id: &str,
    ) -> Result<Option<PlacementTaskRecord>, Status> {
        let key = placement_task_key(task_id);
        let bytes = self
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
        let Some(task_bytes) = bytes else {
            return Ok(None);
        };
        let task: PlacementTask = decode_proto_message(&task_bytes, "placement task")?;
        let Some(manifest) = self
            .load_manifest_for_version(&task.object_version_ref)
            .await?
        else {
            return Err(Status::not_found(format!(
                "manifest {} for placement task {} is missing",
                task.object_version_ref, task.task_id
            )));
        };
        let (_, ec_profile) = self.get_bucket(task.bucket_id.clone()).await?;
        Ok(Some(PlacementTaskRecord {
            task,
            manifest,
            ec_profile,
        }))
    }

    #[cfg(target_os = "linux")]
    async fn upsert_placement_task(&self, task: PlacementTask) -> Result<(), Status> {
        let key = placement_task_key(&task.task_id);
        let bytes = encode_proto_message(&task);
        self.db
            .run(move |trx, _| {
                let key = key.clone();
                let bytes = bytes.clone();
                async move {
                    trx.set(&key, &bytes);
                    Ok(())
                }
            })
            .await
            .map_err(map_fdb_binding_error)?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn create_namespace(
        &self,
        mut namespace: NamespaceRecord,
    ) -> Result<(NamespaceRecord, ShardMapEntry), Status> {
        if namespace.namespace_id.trim().is_empty() {
            return Err(Status::invalid_argument("namespace_id must not be empty"));
        }
        if namespace.tenant_id.trim().is_empty() {
            return Err(Status::invalid_argument("tenant_id must not be empty"));
        }
        if namespace.display_name.trim().is_empty() {
            namespace.display_name = namespace.namespace_id.clone();
        }
        if namespace.state == 0 {
            namespace.state = 1;
        }
        if namespace.shard_id.trim().is_empty() {
            namespace.shard_id = format!("fdb:{}", namespace.namespace_id);
        }
        let key = namespace_key(&namespace.namespace_id);
        let namespace = self
            .db
            .run(move |trx, _| {
                let key = key.clone();
                let namespace = namespace.clone();
                async move {
                    if let Some(existing) =
                        trx.get(&key, false).await.map_err(FdbBindingError::from)?
                    {
                        return decode_json(existing.as_ref(), "namespace").map_err(status_to_fdb);
                    }
                    trx.set(&key, &encode_json(&namespace).map_err(status_to_fdb)?);
                    Ok(namespace)
                }
            })
            .await
            .map_err(map_fdb_binding_error)?;
        Ok((namespace.clone(), shard_map_for_namespace(&namespace)))
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn create_namespace(
        &self,
        _namespace: NamespaceRecord,
    ) -> Result<(NamespaceRecord, ShardMapEntry), Status> {
        Err(cold_path_unimplemented("CreateNamespace"))
    }

    pub(crate) async fn get_namespace(
        &self,
        _namespace_id: String,
    ) -> Result<(NamespaceRecord, ShardMapEntry), Status> {
        Err(cold_path_unimplemented("GetNamespace"))
    }

    pub(crate) async fn list_namespaces(
        &self,
        _tenant_id: Option<&str>,
    ) -> Result<Vec<NamespaceRecord>, Status> {
        Err(cold_path_unimplemented("ListNamespaces"))
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn create_namespace_entry(
        &self,
        entry: NamespaceDomainEntry,
    ) -> Result<NamespaceDomainEntry, Status> {
        if entry.namespace_id.trim().is_empty() {
            return Err(Status::invalid_argument("namespace_id must not be empty"));
        }
        if entry.entry_id.trim().is_empty() {
            return Err(Status::invalid_argument("entry_id must not be empty"));
        }
        validate_entry_name(&entry.name)?;
        let namespace_record_key = namespace_key(&entry.namespace_id);
        let entry_key = namespace_entry_key(&entry.namespace_id, &entry.entry_id);
        let entry = self
            .db
            .run(move |trx, _| {
                let namespace_record_key = namespace_record_key.clone();
                let entry_key = entry_key.clone();
                let mut entry = entry.clone();
                async move {
                    let namespace_exists = trx
                        .get(&namespace_record_key, false)
                        .await
                        .map_err(FdbBindingError::from)?
                        .is_some();
                    if !namespace_exists {
                        return Err(status_to_fdb(Status::not_found(format!(
                            "namespace {} not found",
                            entry.namespace_id
                        ))));
                    }
                    if let Some(existing) = trx
                        .get(&entry_key, false)
                        .await
                        .map_err(FdbBindingError::from)?
                    {
                        return decode_json(existing.as_ref(), "namespace entry")
                            .map_err(status_to_fdb);
                    }
                    entry.path = if entry.parent_entry_id.is_empty() {
                        normalize_hierarchy_path(&entry.name).map_err(status_to_fdb)?
                    } else {
                        let parent_key =
                            namespace_entry_key(&entry.namespace_id, &entry.parent_entry_id);
                        let parent_bytes = trx
                            .get(&parent_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .ok_or_else(|| {
                                status_to_fdb(Status::not_found(format!(
                                    "parent namespace entry {} not found",
                                    entry.parent_entry_id
                                )))
                            })?;
                        let parent: NamespaceDomainEntry =
                            decode_json(parent_bytes.as_ref(), "parent namespace entry")
                                .map_err(status_to_fdb)?;
                        join_path(&parent.path, &entry.name)
                    };
                    trx.set(&entry_key, &encode_json(&entry).map_err(status_to_fdb)?);
                    // Maintain the path -> entry_id index in the same
                    // transaction (see namespace_path_key in fdb_schema.rs).
                    trx.set(
                        &namespace_path_key(&entry.namespace_id, &entry.path),
                        entry.entry_id.as_bytes(),
                    );
                    Ok(entry)
                }
            })
            .await
            .map_err(map_fdb_binding_error)?;
        Ok(entry)
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn create_namespace_entry(
        &self,
        _entry: NamespaceDomainEntry,
    ) -> Result<NamespaceDomainEntry, Status> {
        Err(cold_path_unimplemented("CreateNamespaceEntry"))
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn delete_namespace_entry(
        &self,
        namespace_id: String,
        entry_id: String,
    ) -> Result<NamespaceDomainEntry, Status> {
        if namespace_id.trim().is_empty() {
            return Err(Status::invalid_argument("namespace_id must not be empty"));
        }
        if entry_id.trim().is_empty() {
            return Err(Status::invalid_argument("entry_id must not be empty"));
        }
        let entry_key = namespace_entry_key(&namespace_id, &entry_id);
        let begin = namespace_entry_prefix(&namespace_id);
        let end = prefix_end(&begin);
        self.db
            .run(move |trx, _| {
                let entry_key = entry_key.clone();
                let begin = begin.clone();
                let end = end.clone();
                let namespace_id = namespace_id.clone();
                let entry_id = entry_id.clone();
                async move {
                    let entry_bytes = trx
                        .get(&entry_key, false)
                        .await
                        .map_err(FdbBindingError::from)?
                        .ok_or_else(|| {
                            status_to_fdb(Status::not_found(format!(
                                "namespace entry {namespace_id}/{entry_id} not found"
                            )))
                        })?;
                    let entry: NamespaceDomainEntry =
                        decode_json(entry_bytes.as_ref(), "namespace entry")
                            .map_err(status_to_fdb)?;
                    if entry.kind == NamespaceEntryKind::Bucket as i32 {
                        return Err(status_to_fdb(Status::failed_precondition(format!(
                            "bucket namespace entry {} cannot be deleted",
                            entry.entry_id
                        ))));
                    }

                    let mut range = trx.get_ranges_keyvalues(
                        RangeOption::from((begin.clone(), end.clone())),
                        false,
                    );
                    while let Some(kv_result) = range.next().await {
                        let kv = kv_result.map_err(FdbBindingError::from)?;
                        if kv.key() == entry_key.as_slice() {
                            continue;
                        }
                        let child: NamespaceDomainEntry =
                            decode_json(kv.value().as_ref(), "namespace entry")
                                .map_err(status_to_fdb)?;
                        if child.parent_entry_id == entry.entry_id {
                            return Err(status_to_fdb(Status::failed_precondition(format!(
                                "namespace entry {} is not empty",
                                entry.entry_id
                            ))));
                        }
                    }

                    trx.clear(&entry_key);
                    // Clear the path index in the same transaction so it never
                    // drifts from the namespace entry it mirrors.
                    trx.clear(&namespace_path_key(&entry.namespace_id, &entry.path));
                    Ok(entry)
                }
            })
            .await
            .map_err(map_fdb_binding_error)
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn delete_namespace_entry(
        &self,
        _namespace_id: String,
        _entry_id: String,
    ) -> Result<NamespaceDomainEntry, Status> {
        Err(cold_path_unimplemented("DeleteNamespaceEntry"))
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn create_ec_profile(&self, _profile: EcProfile) -> Result<EcProfile, Status> {
        let mut profile = _profile;
        if profile.id.trim().is_empty() {
            return Err(Status::invalid_argument("ec profile id must not be empty"));
        }
        if profile.codec_id.trim().is_empty() {
            profile.codec_id = "rs".to_string();
        }
        let key = ec_profile_key(&profile.id);
        let profile = self
            .db
            .run(move |trx, _| {
                let key = key.clone();
                let profile = profile.clone();
                async move {
                    if let Some(existing) =
                        trx.get(&key, false).await.map_err(FdbBindingError::from)?
                    {
                        return decode_json(existing.as_ref(), "ec profile").map_err(status_to_fdb);
                    }
                    trx.set(&key, &encode_json(&profile).map_err(status_to_fdb)?);
                    Ok(profile)
                }
            })
            .await
            .map_err(map_fdb_binding_error)?;
        Ok(profile)
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn create_ec_profile(&self, _profile: EcProfile) -> Result<EcProfile, Status> {
        Err(cold_path_unimplemented("CreateEcProfile"))
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn list_ec_profiles(&self) -> Result<Vec<EcProfile>, Status> {
        let (begin, end) = ec_profile_range();
        self.load_all(begin, end).await
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn list_ec_profiles(&self) -> Result<Vec<EcProfile>, Status> {
        Err(cold_path_unimplemented("ListEcProfiles"))
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn create_bucket(
        &self,
        mut bucket: BucketRecord,
    ) -> Result<(BucketRecord, EcProfile), Status> {
        if bucket.bucket_id.trim().is_empty() {
            return Err(Status::invalid_argument("bucket_id must not be empty"));
        }
        if bucket.namespace_id.trim().is_empty() {
            return Err(Status::invalid_argument("namespace_id must not be empty"));
        }
        if bucket.parent_entry_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "parent_entry_id must not be empty",
            ));
        }
        validate_entry_name(&bucket.bucket_id)?;
        if bucket.bucket_entry_id.trim().is_empty() {
            bucket.bucket_entry_id = format!("bucket::{}", bucket.bucket_id);
        }
        let bucket_key = bucket_record_key(&bucket.bucket_id);
        let profile_key = ec_profile_key(&bucket.ec_profile_id);
        let namespace_record_key = namespace_key(&bucket.namespace_id);
        let parent_entry_key = namespace_entry_key(&bucket.namespace_id, &bucket.parent_entry_id);
        let bucket_entry_key = namespace_entry_key(&bucket.namespace_id, &bucket.bucket_entry_id);
        let bucket_context_record_key = bucket_context_key(&bucket.bucket_id);
        let (bucket, profile) = self
            .db
            .run(move |trx, _| {
                let bucket_key = bucket_key.clone();
                let profile_key = profile_key.clone();
                let namespace_record_key = namespace_record_key.clone();
                let parent_entry_key = parent_entry_key.clone();
                let bucket_entry_key = bucket_entry_key.clone();
                let bucket_context_record_key = bucket_context_record_key.clone();
                let bucket = bucket.clone();
                async move {
                    let namespace_exists = trx
                        .get(&namespace_record_key, false)
                        .await
                        .map_err(FdbBindingError::from)?
                        .is_some();
                    if !namespace_exists {
                        return Err(status_to_fdb(Status::not_found(format!(
                            "namespace {} not found",
                            bucket.namespace_id
                        ))));
                    }
                    let profile_bytes = trx
                        .get(&profile_key, false)
                        .await
                        .map_err(FdbBindingError::from)?
                        .ok_or_else(|| {
                            status_to_fdb(Status::not_found(format!(
                                "ec profile {} not found",
                                bucket.ec_profile_id
                            )))
                        })?;
                    let profile: EcProfile =
                        decode_json(profile_bytes.as_ref(), "ec profile").map_err(status_to_fdb)?;

                    if let Some(existing_bucket) = trx
                        .get(&bucket_key, false)
                        .await
                        .map_err(FdbBindingError::from)?
                    {
                        let existing: BucketRecord =
                            decode_json(existing_bucket.as_ref(), "bucket")
                                .map_err(status_to_fdb)?;
                        return Ok((existing, profile));
                    }

                    let parent_bytes = trx
                        .get(&parent_entry_key, false)
                        .await
                        .map_err(FdbBindingError::from)?
                        .ok_or_else(|| {
                            status_to_fdb(Status::not_found(format!(
                                "parent namespace entry {} not found",
                                bucket.parent_entry_id
                            )))
                        })?;
                    let parent_entry: NamespaceDomainEntry =
                        decode_json(parent_bytes.as_ref(), "parent namespace entry")
                            .map_err(status_to_fdb)?;
                    let bucket_entry = NamespaceDomainEntry {
                        entry_id: bucket.bucket_entry_id.clone(),
                        namespace_id: bucket.namespace_id.clone(),
                        parent_entry_id: bucket.parent_entry_id.clone(),
                        name: bucket.bucket_id.clone(),
                        kind: keinctl::proto::NamespaceEntryKind::Bucket as i32,
                        path: join_path(&parent_entry.path, &bucket.bucket_id),
                        // Bucket is a directory-kind entry; size_bytes is unused.
                        size_bytes: 0,
                    };
                    let bucket_context = StoredBucketWriteContext {
                        bucket: bucket.clone(),
                        bucket_entry: bucket_entry.clone(),
                        ec_profile: profile.clone(),
                    };
                    trx.set(&bucket_key, &encode_json(&bucket).map_err(status_to_fdb)?);
                    trx.set(
                        &bucket_entry_key,
                        &encode_json(&bucket_entry).map_err(status_to_fdb)?,
                    );
                    // Index the bucket entry's path so objects written directly
                    // under the bucket root can resolve their parent via a point
                    // get (see namespace_path_key in fdb_schema.rs).
                    trx.set(
                        &namespace_path_key(&bucket_entry.namespace_id, &bucket_entry.path),
                        bucket_entry.entry_id.as_bytes(),
                    );
                    trx.set(
                        &bucket_context_record_key,
                        &encode_json(&bucket_context).map_err(status_to_fdb)?,
                    );
                    Ok((bucket, profile))
                }
            })
            .await
            .map_err(map_fdb_binding_error)?;
        Ok((bucket, profile))
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn create_bucket(
        &self,
        _bucket: BucketRecord,
    ) -> Result<(BucketRecord, EcProfile), Status> {
        Err(cold_path_unimplemented("CreateBucket"))
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn get_bucket(
        &self,
        bucket_id: String,
    ) -> Result<(BucketRecord, EcProfile), Status> {
        let key = bucket_context_key(&bucket_id);
        let context = self
            .db
            .run(move |trx, _| {
                let key = key.clone();
                async move {
                    let value = trx.get(&key, false).await.map_err(FdbBindingError::from)?;
                    Ok(value.map(|bytes| bytes.as_ref().to_vec()))
                }
            })
            .await
            .map_err(map_fdb_binding_error)?
            .ok_or_else(|| Status::not_found(format!("bucket {} not found", bucket_id)))?;
        let context: StoredBucketWriteContext = decode_json(&context, "bucket context")?;
        Ok((context.bucket, context.ec_profile))
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn get_bucket(
        &self,
        _bucket_id: String,
    ) -> Result<(BucketRecord, EcProfile), Status> {
        Err(cold_path_unimplemented("GetBucket"))
    }

    pub(crate) async fn list_buckets(
        &self,
        _namespace_id: Option<&str>,
        _parent_entry_id: Option<&str>,
    ) -> Result<Vec<BucketRecord>, Status> {
        Err(cold_path_unimplemented("ListBuckets"))
    }

    pub(crate) async fn resolve_path(
        &self,
        _namespace_id: String,
        _path: String,
    ) -> Result<ResolvePathReply, Status> {
        Err(cold_path_unimplemented("ResolvePath"))
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn list_children(
        &self,
        namespace_id: String,
        parent_entry_id: String,
        cursor: String,
        limit: u32,
    ) -> Result<(Vec<NamespaceDomainEntry>, String, u64), Status> {
        if namespace_id.trim().is_empty() {
            return Err(Status::invalid_argument("namespace_id must not be empty"));
        }
        if parent_entry_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "parent_entry_id must not be empty",
            ));
        }
        let begin = namespace_entry_prefix(&namespace_id);
        let end = prefix_end(&begin);
        let entries = self
            .load_all::<NamespaceDomainEntry>(begin, end)
            .await?
            .into_iter()
            .filter(|entry| entry.parent_entry_id == parent_entry_id)
            .collect::<Vec<_>>();
        let mut entries = entries;
        entries.sort_by(|left, right| left.name.cmp(&right.name));

        let start = if cursor.trim().is_empty() {
            0
        } else {
            cursor.parse::<usize>().map_err(|_| {
                Status::invalid_argument(format!(
                    "invalid ListChildren cursor `{cursor}`; expected numeric offset"
                ))
            })?
        };
        if start >= entries.len() {
            return Ok((Vec::new(), String::new(), 0));
        }
        let limit = if limit == 0 {
            entries.len().saturating_sub(start)
        } else {
            limit as usize
        };
        let end_index = start.saturating_add(limit).min(entries.len());
        let next_cursor = if end_index < entries.len() {
            end_index.to_string()
        } else {
            String::new()
        };
        Ok((entries[start..end_index].to_vec(), next_cursor, 0))
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn list_children(
        &self,
        _namespace_id: String,
        _parent_entry_id: String,
        _cursor: String,
        _limit: u32,
    ) -> Result<(Vec<NamespaceDomainEntry>, String, u64), Status> {
        Err(cold_path_unimplemented("ListChildren"))
    }

    pub(crate) async fn resolve_shard(
        &self,
        _namespace_id: String,
    ) -> Result<ShardMapEntry, Status> {
        Err(cold_path_unimplemented("ResolveShard"))
    }

    pub(crate) async fn report_target_failure(&self, target_id: String) -> Result<u32, Status> {
        #[cfg(target_os = "linux")]
        {
            let version_ids = self
                .list_target_current_version_ids_limited(
                    &target_id,
                    TARGET_MAINTENANCE_BATCH_VERSIONS,
                )
                .await?;
            let records = self
                .load_current_manifest_records_for_versions(version_ids)
                .await?;
            let mut created_tasks = 0u32;
            'records: for record in records {
                for task in build_rebuild_tasks(&record.manifest, &target_id) {
                    self.upsert_placement_task(task).await?;
                    created_tasks = created_tasks.saturating_add(1);
                    if created_tasks >= TARGET_MAINTENANCE_BATCH_TASKS {
                        break 'records;
                    }
                }
            }
            return Ok(created_tasks);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = target_id;
            Err(cold_path_unimplemented("ReportTargetFailure"))
        }
    }

    pub(crate) async fn drain_target(
        &self,
        target_id: String,
        active_target_ids: HashSet<String>,
    ) -> Result<u32, Status> {
        #[cfg(target_os = "linux")]
        {
            let version_ids = self
                .list_target_current_version_ids_limited(
                    &target_id,
                    TARGET_MAINTENANCE_BATCH_VERSIONS,
                )
                .await?;
            let records = self
                .load_current_manifest_records_for_versions(version_ids)
                .await?;
            let mut created_tasks = 0u32;
            'records: for record in records {
                for task in build_evacuate_tasks(&record.manifest, &target_id, &active_target_ids) {
                    self.upsert_placement_task(task).await?;
                    created_tasks = created_tasks.saturating_add(1);
                    if created_tasks >= TARGET_MAINTENANCE_BATCH_TASKS {
                        break 'records;
                    }
                }
            }
            return Ok(created_tasks);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (target_id, active_target_ids);
            Err(cold_path_unimplemented("DrainTarget"))
        }
    }

    pub(crate) async fn preview_target_rebalance(
        &self,
        source_target_ids: Vec<String>,
        include_target_ids: HashSet<String>,
        exclude_target_ids: HashSet<String>,
        active_target_ids: HashSet<String>,
        max_tasks: usize,
    ) -> Result<(u32, u64), Status> {
        #[cfg(target_os = "linux")]
        {
            let source_target_ids = source_target_ids.into_iter().collect::<HashSet<_>>();
            let mut referenced_versions = HashSet::new();
            let mut live_fragments = 0u64;
            for source_target_id in &source_target_ids {
                live_fragments = live_fragments.saturating_add(
                    self.count_target_current_fragments(source_target_id)
                        .await?,
                );
                let remaining = max_tasks.saturating_sub(referenced_versions.len());
                if remaining == 0 {
                    continue;
                }
                referenced_versions.extend(
                    self.list_target_current_version_ids_limited(source_target_id, remaining)
                        .await?,
                );
            }
            let records = self
                .load_current_manifest_records_for_versions(
                    referenced_versions.into_iter().collect(),
                )
                .await?;
            let mut candidate_tasks = 0u32;
            for record in records {
                for _ in build_rebalance_tasks(
                    &record.manifest,
                    &source_target_ids,
                    &include_target_ids,
                    &exclude_target_ids,
                    &active_target_ids,
                    max_tasks.saturating_sub(candidate_tasks as usize),
                ) {
                    candidate_tasks = candidate_tasks.saturating_add(1);
                    if candidate_tasks as usize >= max_tasks {
                        break;
                    }
                }
                if candidate_tasks as usize >= max_tasks {
                    break;
                }
            }
            return Ok((candidate_tasks, live_fragments));
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (
                source_target_ids,
                include_target_ids,
                exclude_target_ids,
                active_target_ids,
                max_tasks,
            );
            Err(cold_path_unimplemented("PreviewTargetRebalance"))
        }
    }

    pub(crate) async fn enqueue_target_rebalance(
        &self,
        source_target_ids: Vec<String>,
        include_target_ids: HashSet<String>,
        exclude_target_ids: HashSet<String>,
        active_target_ids: HashSet<String>,
        max_tasks: usize,
    ) -> Result<u32, Status> {
        #[cfg(target_os = "linux")]
        {
            let source_target_ids = source_target_ids.into_iter().collect::<HashSet<_>>();
            let mut referenced_versions = HashSet::new();
            for source_target_id in &source_target_ids {
                let remaining = max_tasks.saturating_sub(referenced_versions.len());
                if remaining == 0 {
                    break;
                }
                referenced_versions.extend(
                    self.list_target_current_version_ids_limited(source_target_id, remaining)
                        .await?,
                );
            }
            let records = self
                .load_current_manifest_records_for_versions(
                    referenced_versions.into_iter().collect(),
                )
                .await?;
            let mut created_tasks = 0u32;
            for record in records {
                for task in build_rebalance_tasks(
                    &record.manifest,
                    &source_target_ids,
                    &include_target_ids,
                    &exclude_target_ids,
                    &active_target_ids,
                    max_tasks.saturating_sub(created_tasks as usize),
                ) {
                    self.upsert_placement_task(task).await?;
                    created_tasks = created_tasks.saturating_add(1);
                    if created_tasks as usize >= max_tasks {
                        break;
                    }
                }
                if created_tasks as usize >= max_tasks {
                    break;
                }
            }
            return Ok(created_tasks);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (
                source_target_ids,
                include_target_ids,
                exclude_target_ids,
                active_target_ids,
                max_tasks,
            );
            Err(cold_path_unimplemented("EnqueueTargetRebalance"))
        }
    }

    pub(crate) async fn list_placement_tasks(
        &self,
        source_target_id: Option<&str>,
        object_version_ref: Option<&str>,
        task_kind: Option<PlacementTaskKind>,
        state: Option<PlacementTaskState>,
        limit: usize,
    ) -> Result<Vec<PlacementTaskSummary>, Status> {
        #[cfg(target_os = "linux")]
        {
            let mut tasks = self.list_all_placement_tasks().await?;
            tasks.retain(|task| {
                placement_task_matches_filters(
                    task,
                    source_target_id,
                    object_version_ref,
                    task_kind,
                    state,
                )
            });
            tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
            if limit > 0 && tasks.len() > limit {
                tasks.truncate(limit);
            }
            return Ok(tasks.into_iter().map(placement_task_summary).collect());
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (
                source_target_id,
                object_version_ref,
                task_kind,
                state,
                limit,
            );
            Ok(Vec::new())
        }
    }

    pub(crate) async fn get_placement_task(
        &self,
        task_id: &str,
    ) -> Result<Option<(PlacementTask, ObjectVersionManifest, EcProfile)>, Status> {
        #[cfg(target_os = "linux")]
        {
            return self
                .load_placement_task_record(task_id)
                .await
                .map(|record| {
                    record.map(|record| (record.task, record.manifest, record.ec_profile))
                });
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = task_id;
            Ok(None)
        }
    }

    pub(crate) async fn get_target_placement_status(
        &self,
        target_id: &str,
    ) -> Result<TargetPlacementStatus, Status> {
        #[cfg(target_os = "linux")]
        {
            let tasks = self.list_all_placement_tasks().await?;
            let mut status = TargetPlacementStatus {
                target_id: target_id.to_string(),
                live_fragments: self.live_fragment_count_for_target(target_id).await?,
                ..TargetPlacementStatus::default()
            };
            for task in tasks {
                if task.source_target_id != target_id {
                    continue;
                }
                match placement_task_state(task.state) {
                    PlacementTaskState::Pending => match placement_task_kind(task.task_kind) {
                        PlacementTaskKind::Rebuild => {
                            status.pending_rebuild_tasks =
                                status.pending_rebuild_tasks.saturating_add(1)
                        }
                        PlacementTaskKind::Rebalance => {
                            status.pending_rebalance_tasks =
                                status.pending_rebalance_tasks.saturating_add(1)
                        }
                        PlacementTaskKind::Evacuate => {
                            status.pending_evacuate_tasks =
                                status.pending_evacuate_tasks.saturating_add(1)
                        }
                        PlacementTaskKind::Unspecified => {}
                    },
                    PlacementTaskState::Leased => {
                        status.leased_tasks = status.leased_tasks.saturating_add(1)
                    }
                    PlacementTaskState::Failed => {
                        status.failed_tasks = status.failed_tasks.saturating_add(1)
                    }
                    PlacementTaskState::Completed | PlacementTaskState::Unspecified => {}
                }
            }
            return Ok(status);
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(TargetPlacementStatus {
                target_id: target_id.to_string(),
                ..TargetPlacementStatus::default()
            })
        }
    }

    pub(crate) async fn lease_placement_tasks(
        &self,
        lease_owner: String,
        max_tasks: usize,
        lease_ttl_ms: u64,
        now_ms: u64,
    ) -> Result<Vec<LeasedPlacementTask>, Status> {
        #[cfg(target_os = "linux")]
        {
            let mut tasks = self.list_all_placement_tasks().await?;
            tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
            let mut leased = Vec::new();
            for task in tasks {
                if leased.len() >= max_tasks {
                    break;
                }
                let state = placement_task_state(task.state);
                let lease_expired =
                    task.lease_expires_at_unix_ms == 0 || task.lease_expires_at_unix_ms <= now_ms;
                if !matches!(state, PlacementTaskState::Pending)
                    && !(matches!(state, PlacementTaskState::Leased) && lease_expired)
                {
                    continue;
                }
                let key = placement_task_key(&task.task_id);
                let lease_owner = lease_owner.clone();
                let updated = self
                    .db
                    .run(move |trx, _| {
                        let key = key.clone();
                        let lease_owner = lease_owner.clone();
                        async move {
                            let Some(bytes) =
                                trx.get(&key, false).await.map_err(FdbBindingError::from)?
                            else {
                                return Ok::<Option<PlacementTask>, FdbBindingError>(None);
                            };
                            let mut task: PlacementTask =
                                decode_proto_message(bytes.as_ref(), "placement task")
                                    .map_err(status_to_fdb)?;
                            let state = placement_task_state(task.state);
                            let lease_expired = task.lease_expires_at_unix_ms == 0
                                || task.lease_expires_at_unix_ms <= now_ms;
                            if !matches!(state, PlacementTaskState::Pending)
                                && !(matches!(state, PlacementTaskState::Leased) && lease_expired)
                            {
                                return Ok(None);
                            }
                            task.state = PlacementTaskState::Leased as i32;
                            task.lease_owner = lease_owner;
                            task.lease_expires_at_unix_ms =
                                now_ms.saturating_add(lease_ttl_ms.max(1_000));
                            trx.set(&key, &encode_proto_message(&task));
                            Ok(Some(task))
                        }
                    })
                    .await
                    .map_err(map_fdb_binding_error)?;
                let Some(task) = updated else {
                    continue;
                };
                if let Some(record) = self.load_placement_task_record(&task.task_id).await? {
                    leased.push(LeasedPlacementTask {
                        task: Some(record.task),
                        manifest: Some(record.manifest),
                        ec_profile: Some(record.ec_profile),
                    });
                }
            }
            return Ok(leased);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (lease_owner, max_tasks, lease_ttl_ms, now_ms);
            Ok(Vec::new())
        }
    }

    pub(crate) async fn lease_rebuild_tasks(
        &self,
        lease_owner: String,
        max_tasks: usize,
        lease_ttl_ms: u64,
        now_ms: u64,
    ) -> Result<Vec<LeasedRebuildTask>, Status> {
        #[cfg(target_os = "linux")]
        {
            let leased = self
                .lease_placement_tasks(lease_owner, max_tasks, lease_ttl_ms, now_ms)
                .await?;
            let mut rebuild_tasks = Vec::new();
            for leased_task in leased {
                let Some(task) = leased_task.task else {
                    continue;
                };
                if placement_task_kind(task.task_kind) != PlacementTaskKind::Rebuild {
                    continue;
                }
                rebuild_tasks.push(LeasedRebuildTask {
                    task: Some(RebuildTask {
                        task_id: task.task_id,
                        failed_target_id: task.source_target_id,
                        object_version_ref: task.object_version_ref,
                        stripe_index: task.stripe_index,
                        fragment_index: task.fragment_index,
                        replacement_target_id: task.destination_target_id,
                        replacement_granule_index: task.destination_granule_index,
                        lease_owner: task.lease_owner,
                        lease_expires_at_unix_ms: task.lease_expires_at_unix_ms,
                        state: match placement_task_state(task.state) {
                            PlacementTaskState::Pending => 1,
                            PlacementTaskState::Leased => 2,
                            PlacementTaskState::Completed => 3,
                            PlacementTaskState::Failed => 4,
                            PlacementTaskState::Unspecified => 0,
                        },
                        namespace_id: task.namespace_id,
                        bucket_id: task.bucket_id,
                        object_entry_id: task.object_entry_id,
                    }),
                    manifest: leased_task.manifest,
                    ec_profile: leased_task.ec_profile,
                });
            }
            return Ok(rebuild_tasks);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (lease_owner, max_tasks, lease_ttl_ms, now_ms);
            Ok(Vec::new())
        }
    }

    pub(crate) async fn cancel_pending_evacuate_tasks(
        &self,
        target_id: &str,
    ) -> Result<u32, Status> {
        self.cancel_source_tasks(
            target_id,
            PlacementTaskKind::Evacuate,
            &[
                PlacementTaskState::Pending,
                PlacementTaskState::Leased,
                PlacementTaskState::Failed,
            ],
        )
        .await
    }

    pub(crate) async fn cancel_pending_rebuild_tasks(
        &self,
        target_id: &str,
    ) -> Result<u32, Status> {
        self.cancel_source_tasks(
            target_id,
            PlacementTaskKind::Rebuild,
            &[
                PlacementTaskState::Pending,
                PlacementTaskState::Leased,
                PlacementTaskState::Failed,
            ],
        )
        .await
    }

    pub(crate) async fn cancel_pending_rebalance_tasks(
        &self,
        target_id: &str,
    ) -> Result<u32, Status> {
        self.cancel_source_tasks(
            target_id,
            PlacementTaskKind::Rebalance,
            &[
                PlacementTaskState::Pending,
                PlacementTaskState::Leased,
                PlacementTaskState::Failed,
            ],
        )
        .await
    }

    async fn cancel_source_tasks(
        &self,
        target_id: &str,
        kind: PlacementTaskKind,
        states: &[PlacementTaskState],
    ) -> Result<u32, Status> {
        #[cfg(target_os = "linux")]
        {
            let task_ids = self
                .list_all_placement_tasks()
                .await?
                .into_iter()
                .filter(|task| {
                    task.source_target_id == target_id
                        && placement_task_kind(task.task_kind) == kind
                        && states.contains(&placement_task_state(task.state))
                })
                .map(|task| task.task_id)
                .collect::<Vec<_>>();
            if task_ids.is_empty() {
                return Ok(0);
            }
            let cleared = task_ids.len() as u32;
            self.db
                .run(move |trx, _| {
                    let task_ids = task_ids.clone();
                    async move {
                        for task_id in task_ids {
                            trx.clear(&placement_task_key(&task_id));
                        }
                        Ok::<(), FdbBindingError>(())
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            return Ok(cleared);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (target_id, kind, states);
            Ok(0)
        }
    }

    pub(crate) async fn commit_placement_task(
        &self,
        task_id: String,
        replacement_fragment: FragmentPlan,
    ) -> Result<ObjectVersionManifest, Status> {
        #[cfg(target_os = "linux")]
        {
            let task_key = placement_task_key(&task_id);
            let manifest = self
                .db
                .run(move |trx, _| {
                    let task_key = task_key.clone();
                    let task_id = task_id.clone();
                    let replacement_fragment = replacement_fragment.clone();
                    async move {
                        let task_bytes = trx
                            .get(&task_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .ok_or_else(|| {
                                status_to_fdb(Status::not_found(format!(
                                    "unknown placement task {}",
                                    task_id
                                )))
                            })?;
                        let mut task: PlacementTask =
                            decode_proto_message(task_bytes.as_ref(), "placement task")
                                .map_err(status_to_fdb)?;
                        let version_key = object_version_key(&task.object_version_ref);
                        let manifest_bytes = load_blob(&trx, &version_key, |chunk_index| {
                            object_version_chunk_key(&task.object_version_ref, chunk_index)
                        })
                        .await?
                        .ok_or_else(|| {
                            status_to_fdb(Status::not_found(format!(
                                "manifest {} for placement task {} is missing",
                                task.object_version_ref, task.task_id
                            )))
                        })?;
                        let mut manifest =
                            decode_manifest_bytes(&manifest_bytes).map_err(status_to_fdb)?;
                        let head_key = object_head_key(&manifest.bucket_id, &manifest.key);
                        let current_head = trx
                            .get(&head_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .map(|bytes| {
                                decode_proto_message::<ObjectHead>(bytes.as_ref(), "object head")
                            })
                            .transpose()
                            .map_err(status_to_fdb)?;
                        let manifest_is_current = current_head
                            .as_ref()
                            .is_some_and(|head| head.current_version_id == manifest.version_id);
                        if manifest_is_current {
                            clear_target_current_fragment_index(&trx, &manifest);
                        }
                        let stripe = manifest
                            .stripes
                            .get_mut(task.stripe_index as usize)
                            .ok_or_else(|| {
                                status_to_fdb(Status::failed_precondition(format!(
                                    "manifest {} has no stripe {}",
                                    manifest.version_id, task.stripe_index
                                )))
                            })?;
                        let fragment = stripe
                            .fragments
                            .iter_mut()
                            .find(|fragment| {
                                fragment.fragment_index == task.fragment_index
                                    && fragment.target_id == task.source_target_id
                            })
                            .ok_or_else(|| {
                                status_to_fdb(Status::failed_precondition(format!(
                                    "manifest {} does not contain fragment {} on target {}",
                                    manifest.version_id, task.fragment_index, task.source_target_id
                                )))
                            })?;
                        *fragment = replacement_fragment.clone();
                        task.destination_target_id = replacement_fragment.target_id.clone();
                        task.destination_granule_index = replacement_fragment.granule_index;
                        task.lease_owner.clear();
                        task.lease_expires_at_unix_ms = 0;
                        task.state = PlacementTaskState::Completed as i32;
                        task.reason.clear();
                        store_blob(
                            &trx,
                            &version_key,
                            &object_version_chunk_prefix(&manifest.version_id),
                            |chunk_index| {
                                object_version_chunk_key(&manifest.version_id, chunk_index)
                            },
                            &encode_manifest(&manifest),
                        );
                        if manifest_is_current {
                            write_target_current_fragment_index(&trx, &manifest);
                        }
                        trx.set(&task_key, &encode_proto_message(&task));
                        Ok(manifest)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            return Ok(manifest);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (task_id, replacement_fragment);
            Err(cold_path_unimplemented("CommitPlacementTask"))
        }
    }

    pub(crate) async fn fail_placement_task(
        &self,
        task_id: String,
        failure_reason: String,
    ) -> Result<PlacementTask, Status> {
        #[cfg(target_os = "linux")]
        {
            let task_key = placement_task_key(&task_id);
            let task = self
                .db
                .run(move |trx, _| {
                    let task_key = task_key.clone();
                    let task_id = task_id.clone();
                    let failure_reason = failure_reason.clone();
                    async move {
                        let task_bytes = trx
                            .get(&task_key, false)
                            .await
                            .map_err(FdbBindingError::from)?
                            .ok_or_else(|| {
                                status_to_fdb(Status::not_found(format!(
                                    "unknown placement task {}",
                                    task_id
                                )))
                            })?;
                        let mut task: PlacementTask =
                            decode_proto_message(task_bytes.as_ref(), "placement task")
                                .map_err(status_to_fdb)?;
                        task.state = PlacementTaskState::Failed as i32;
                        task.reason = failure_reason;
                        task.lease_owner.clear();
                        task.lease_expires_at_unix_ms = 0;
                        trx.set(&task_key, &encode_proto_message(&task));
                        Ok(task)
                    }
                })
                .await
                .map_err(map_fdb_binding_error)?;
            return Ok(task);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (task_id, failure_reason);
            Err(cold_path_unimplemented("FailPlacementTask"))
        }
    }

    pub(crate) async fn commit_rebuild(
        &self,
        task_id: String,
        replacement_fragment: FragmentPlan,
    ) -> Result<ObjectVersionManifest, Status> {
        self.commit_placement_task(task_id, replacement_fragment)
            .await
    }

    pub(crate) async fn live_fragment_count_for_target(
        &self,
        target_id: &str,
    ) -> Result<u64, Status> {
        #[cfg(target_os = "linux")]
        {
            return self.count_target_current_fragments(target_id).await;
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = target_id;
            Ok(0)
        }
    }

    pub(crate) async fn recover_or_retire_target_allowed(
        &self,
        target_id: &str,
    ) -> Result<u64, Status> {
        self.live_fragment_count_for_target(target_id).await
    }

    pub(crate) async fn expire_write_intents(&self, _now_ms: u64) -> Result<Vec<String>, Status> {
        Ok(Vec::new())
    }

    pub(crate) async fn read_entry_events(
        &self,
        _entry_id: &str,
        _after_revision: u64,
        _limit: usize,
    ) -> Result<Vec<MetadataEvent>, Status> {
        Ok(Vec::new())
    }

    pub(crate) async fn read_prefix_events(
        &self,
        _namespace_id: &str,
        _parent_entry_id: &str,
        _after_revision: u64,
        _limit: usize,
    ) -> Result<Vec<MetadataEvent>, Status> {
        Ok(Vec::new())
    }

    pub(crate) async fn list_metadata_events(
        &self,
        _namespace_id: &str,
        _entry_id: Option<&str>,
        _parent_entry_id: Option<&str>,
        _start_revision: u64,
        _limit: usize,
    ) -> Result<Vec<MetadataEvent>, Status> {
        Ok(Vec::new())
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct StatusCarrier(Status);

#[cfg(target_os = "linux")]
impl Display for StatusCarrier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.message())
    }
}

#[cfg(target_os = "linux")]
impl std::error::Error for StatusCarrier {}

#[cfg(target_os = "linux")]
fn encode_json<T>(value: &T) -> Result<Vec<u8>, Status>
where
    T: Serialize,
{
    serde_json::to_vec(value).map_err(|err| {
        Status::internal(format!("failed to encode FoundationDB JSON payload: {err}"))
    })
}

#[cfg(target_os = "linux")]
fn decode_json<T>(bytes: &[u8], what: &str) -> Result<T, Status>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_slice(bytes).map_err(|err| {
        Status::internal(format!(
            "failed to decode FoundationDB {what} JSON payload: {err}"
        ))
    })
}

#[cfg(target_os = "linux")]
fn status_to_fdb(status: Status) -> FdbBindingError {
    FdbBindingError::new_custom_error(Box::new(StatusCarrier(status)))
}

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
async fn load_blob<F>(
    trx: &foundationdb::RetryableTransaction,
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

#[cfg(target_os = "linux")]
fn store_blob<F>(
    trx: &foundationdb::RetryableTransaction,
    meta_key: &[u8],
    chunk_prefix: &[u8],
    chunk_key: F,
    bytes: &[u8],
) where
    F: Fn(u32) -> Vec<u8>,
{
    trx.clear_range(chunk_prefix, &prefix_end(chunk_prefix));
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

#[cfg(target_os = "linux")]
fn encode_blob_meta(chunk_count: u32, total_len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(CHUNKED_BLOB_META_MAGIC.len() + 12);
    bytes.extend_from_slice(CHUNKED_BLOB_META_MAGIC);
    bytes.extend_from_slice(&chunk_count.to_be_bytes());
    bytes.extend_from_slice(&(total_len as u64).to_be_bytes());
    bytes
}

#[cfg(target_os = "linux")]
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

fn placement_task_kind(value: i32) -> PlacementTaskKind {
    PlacementTaskKind::try_from(value).unwrap_or(PlacementTaskKind::Unspecified)
}

fn placement_task_state(value: i32) -> PlacementTaskState {
    PlacementTaskState::try_from(value).unwrap_or(PlacementTaskState::Unspecified)
}

fn placement_task_kind_label(kind: PlacementTaskKind) -> &'static str {
    match kind {
        PlacementTaskKind::Rebuild => "rebuild",
        PlacementTaskKind::Rebalance => "rebalance",
        PlacementTaskKind::Evacuate => "evacuate",
        PlacementTaskKind::Unspecified => "unspecified",
    }
}

fn placement_task_id(
    kind: PlacementTaskKind,
    source_target_id: &str,
    version_id: &str,
    stripe_index: u32,
    fragment_index: u32,
) -> String {
    format!(
        "placement:{}:{}:{}:{}:{}",
        placement_task_kind_label(kind),
        source_target_id,
        version_id,
        stripe_index,
        fragment_index
    )
}

fn placement_task_summary(task: PlacementTask) -> PlacementTaskSummary {
    PlacementTaskSummary {
        task_id: task.task_id,
        task_kind: task.task_kind,
        source_target_id: task.source_target_id,
        object_version_ref: task.object_version_ref,
        stripe_index: task.stripe_index,
        fragment_index: task.fragment_index,
        destination_target_id: task.destination_target_id,
        destination_granule_index: task.destination_granule_index,
        lease_owner: task.lease_owner,
        lease_expires_at_unix_ms: task.lease_expires_at_unix_ms,
        state: task.state,
        namespace_id: task.namespace_id,
        bucket_id: task.bucket_id,
        object_entry_id: task.object_entry_id,
        reason: task.reason,
    }
}

fn placement_task_matches_filters(
    task: &PlacementTask,
    source_target_id: Option<&str>,
    object_version_ref: Option<&str>,
    task_kind: Option<PlacementTaskKind>,
    state: Option<PlacementTaskState>,
) -> bool {
    if let Some(value) = source_target_id {
        if task.source_target_id != value {
            return false;
        }
    }
    if let Some(value) = object_version_ref {
        if task.object_version_ref != value {
            return false;
        }
    }
    if let Some(value) = task_kind {
        if placement_task_kind(task.task_kind) != value {
            return false;
        }
    }
    if let Some(value) = state {
        if placement_task_state(task.state) != value {
            return false;
        }
    }
    true
}

#[cfg(target_os = "linux")]
pub(crate) fn clear_target_current_fragment_index(
    trx: &foundationdb::RetryableTransaction,
    manifest: &ObjectVersionManifest,
) {
    for stripe in &manifest.stripes {
        for fragment in &stripe.fragments {
            trx.clear(&target_current_fragment_key(
                &fragment.target_id,
                &manifest.version_id,
                fragment.stripe_index,
                fragment.fragment_index,
            ));
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn write_target_current_fragment_index(
    trx: &foundationdb::RetryableTransaction,
    manifest: &ObjectVersionManifest,
) {
    for stripe in &manifest.stripes {
        for fragment in &stripe.fragments {
            trx.set(
                &target_current_fragment_key(
                    &fragment.target_id,
                    &manifest.version_id,
                    fragment.stripe_index,
                    fragment.fragment_index,
                ),
                &[],
            );
        }
    }
}

#[cfg(target_os = "linux")]
fn decode_target_current_fragment_version_id(key: &[u8]) -> Result<String, Status> {
    let (_, segments) = decode_segments(key).map_err(|err| {
        Status::internal(format!(
            "failed to decode target-current-fragment key: {err}"
        ))
    })?;
    if segments.len() != 4 {
        return Err(Status::internal(format!(
            "target-current-fragment key has {} segments, expected 4",
            segments.len()
        )));
    }
    Ok(segments[1].clone())
}

fn candidate_destination_target_ids(
    occupied_target_ids: &HashSet<String>,
    active_target_ids: &HashSet<String>,
    include_target_ids: &HashSet<String>,
    exclude_target_ids: &HashSet<String>,
) -> Vec<String> {
    let mut candidates = active_target_ids
        .iter()
        .filter(|target_id| !occupied_target_ids.contains(*target_id))
        .filter(|target_id| !exclude_target_ids.contains(*target_id))
        .filter(|target_id| {
            include_target_ids.is_empty() || include_target_ids.contains(*target_id)
        })
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort();
    candidates
}

fn build_placement_task(
    kind: PlacementTaskKind,
    manifest: &ObjectVersionManifest,
    fragment: &FragmentPlan,
    reason: &str,
    destination_target_id: Option<String>,
) -> PlacementTask {
    PlacementTask {
        task_id: placement_task_id(
            kind,
            &fragment.target_id,
            &manifest.version_id,
            fragment.stripe_index,
            fragment.fragment_index,
        ),
        task_kind: kind as i32,
        source_target_id: fragment.target_id.clone(),
        object_version_ref: manifest.version_id.clone(),
        stripe_index: fragment.stripe_index,
        fragment_index: fragment.fragment_index,
        destination_target_id: destination_target_id.unwrap_or_default(),
        destination_granule_index: 0,
        lease_owner: String::new(),
        lease_expires_at_unix_ms: 0,
        state: PlacementTaskState::Pending as i32,
        namespace_id: manifest.namespace_id.clone(),
        bucket_id: manifest.bucket_id.clone(),
        object_entry_id: manifest.object_entry_id.clone(),
        reason: reason.to_string(),
    }
}

fn build_rebuild_tasks(manifest: &ObjectVersionManifest, target_id: &str) -> Vec<PlacementTask> {
    let mut tasks = Vec::new();
    for stripe in &manifest.stripes {
        for fragment in &stripe.fragments {
            if fragment.target_id == target_id {
                tasks.push(build_placement_task(
                    PlacementTaskKind::Rebuild,
                    manifest,
                    fragment,
                    "target failure",
                    None,
                ));
            }
        }
    }
    tasks
}

fn build_evacuate_tasks(
    manifest: &ObjectVersionManifest,
    target_id: &str,
    active_target_ids: &HashSet<String>,
) -> Vec<PlacementTask> {
    let mut tasks = Vec::new();
    for stripe in &manifest.stripes {
        let occupied_target_ids = stripe
            .fragments
            .iter()
            .map(|fragment| fragment.target_id.clone())
            .collect::<HashSet<_>>();
        let candidates = candidate_destination_target_ids(
            &occupied_target_ids,
            active_target_ids,
            &HashSet::new(),
            &HashSet::new(),
        );
        if candidates.is_empty() {
            continue;
        }
        for fragment in &stripe.fragments {
            if fragment.target_id == target_id {
                tasks.push(build_placement_task(
                    PlacementTaskKind::Evacuate,
                    manifest,
                    fragment,
                    "target drain",
                    None,
                ));
            }
        }
    }
    tasks
}

fn build_rebalance_tasks(
    manifest: &ObjectVersionManifest,
    source_target_ids: &HashSet<String>,
    include_target_ids: &HashSet<String>,
    exclude_target_ids: &HashSet<String>,
    active_target_ids: &HashSet<String>,
    max_tasks: usize,
) -> Vec<PlacementTask> {
    let mut tasks = Vec::new();
    if max_tasks == 0 {
        return tasks;
    }
    for stripe in &manifest.stripes {
        let occupied_target_ids = stripe
            .fragments
            .iter()
            .map(|fragment| fragment.target_id.clone())
            .collect::<HashSet<_>>();
        let candidates = candidate_destination_target_ids(
            &occupied_target_ids,
            active_target_ids,
            include_target_ids,
            exclude_target_ids,
        );
        if candidates.is_empty() {
            continue;
        }
        let destination_hint = if candidates.len() == 1 {
            Some(candidates[0].clone())
        } else {
            None
        };
        for fragment in &stripe.fragments {
            if !source_target_ids.contains(&fragment.target_id) {
                continue;
            }
            tasks.push(build_placement_task(
                PlacementTaskKind::Rebalance,
                manifest,
                fragment,
                "manual rebalance",
                destination_hint.clone(),
            ));
            if tasks.len() >= max_tasks {
                return tasks;
            }
        }
    }
    tasks
}

fn shard_map_for_namespace(namespace: &NamespaceRecord) -> ShardMapEntry {
    ShardMapEntry {
        shard_id: namespace.shard_id.clone(),
        namespace_id: namespace.namespace_id.clone(),
        path_prefix_start: String::new(),
        path_prefix_end: String::new(),
        leader_endpoint: String::new(),
        replica_endpoints: Vec::new(),
        revision: 1,
    }
}

fn cold_path_unimplemented(operation: &str) -> Status {
    Status::unimplemented(format!(
        "{operation} is not implemented in the FoundationDB-only KMS build"
    ))
}

fn encode_proto_message<M>(message: &M) -> Vec<u8>
where
    M: Message,
{
    message.encode_to_vec()
}

fn decode_proto_message<M>(bytes: &[u8], what: &str) -> Result<M, Status>
where
    M: Message + Default,
{
    M::decode(bytes)
        .map_err(|err| Status::internal(format!("failed to decode {what} protobuf payload: {err}")))
}

pub(crate) fn encode_write_intent(intent: &WriteIntent) -> Vec<u8> {
    encode_proto_message(intent)
}

pub(crate) fn decode_write_intent_bytes(bytes: &[u8]) -> Result<WriteIntent, Status> {
    decode_proto_message(bytes, "write intent")
}

pub(crate) fn encode_manifest(manifest: &ObjectVersionManifest) -> Vec<u8> {
    encode_proto_message(manifest)
}

pub(crate) fn decode_manifest_bytes(bytes: &[u8]) -> Result<ObjectVersionManifest, Status> {
    decode_proto_message(bytes, "object manifest")
}

pub(crate) fn normalize_write_intent(intent: &mut WriteIntent) -> Result<(), Status> {
    if intent.reservation_ids.is_empty() && !intent.reservation_id.is_empty() {
        intent.reservation_ids.push(intent.reservation_id.clone());
    }
    if intent.fragment_status.is_empty() {
        let reservation_id = intent.reservation_ids.first().cloned().unwrap_or_default();
        intent.fragment_status = intent
            .fragment_plans
            .iter()
            .enumerate()
            .map(|(placement_index, plan)| FragmentWriteStatus {
                fragment_index: plan.fragment_index,
                state: FragmentWriteState::Planned as i32,
                reservation_id: reservation_id.clone(),
                reservation_placement_index: placement_index as u32,
                stripe_index: plan.stripe_index,
            })
            .collect();
    }
    if intent.fragment_status.len() != intent.fragment_plans.len() {
        return Err(Status::internal(format!(
            "write intent {} has {} fragment plans but {} fragment status entries",
            intent.intent_id,
            intent.fragment_plans.len(),
            intent.fragment_status.len()
        )));
    }
    if intent.reservation_id.is_empty() {
        intent.reservation_id = intent.reservation_ids.first().cloned().unwrap_or_default();
    }
    Ok(())
}

pub(crate) fn mark_successful_fragments(
    intent: &mut WriteIntent,
    successful_fragments: &[FragmentRef],
) -> Result<(), Status> {
    if successful_fragments.is_empty() {
        return Ok(());
    }
    let mut seen = HashSet::new();
    for fragment in successful_fragments {
        let fragment_index = locate_fragment_entry(intent, fragment)?;
        if !seen.insert(fragment_index) {
            continue;
        }
        let status = intent
            .fragment_status
            .get_mut(fragment_index)
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "successful stripe {} fragment {} is out of range for intent {}",
                    fragment.stripe_index, fragment.fragment_index, intent.intent_id
                ))
            })?;
        status.state = FragmentWriteState::Written as i32;
    }
    Ok(())
}

fn locate_fragment_entry(intent: &WriteIntent, fragment: &FragmentRef) -> Result<usize, Status> {
    intent
        .fragment_status
        .iter()
        .position(|status| {
            status.stripe_index == fragment.stripe_index
                && status.fragment_index == fragment.fragment_index
        })
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "stripe {} fragment {} is out of range for intent {}",
                fragment.stripe_index, fragment.fragment_index, intent.intent_id
            ))
        })
}

pub(crate) fn expected_fragment_count(intent: &WriteIntent) -> Result<usize, Status> {
    let stripe_count = usize::try_from(intent.stripe_count).map_err(|_| {
        Status::internal(format!(
            "write intent {} declares an unsupported stripe count {}",
            intent.intent_id, intent.stripe_count
        ))
    })?;
    if stripe_count == 0 {
        return Ok(0);
    }
    let per_stripe = intent
        .fragment_plans
        .iter()
        .map(|plan| plan.fragment_index)
        .max()
        .map(|value| value as usize + 1)
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "write intent {} has no fragment plans to infer stripe width",
                intent.intent_id
            ))
        })?;
    stripe_count
        .checked_mul(per_stripe)
        .ok_or_else(|| Status::internal("write intent fragment count overflowed"))
}

pub(crate) fn fragment_plans_for_window(
    intent: &WriteIntent,
    start_stripe_index: u32,
    stripe_count: usize,
) -> Vec<FragmentPlan> {
    let end_stripe_index = start_stripe_index.saturating_add(stripe_count as u32);
    let mut plans = intent
        .fragment_plans
        .iter()
        .filter(|plan| {
            plan.stripe_index >= start_stripe_index && plan.stripe_index < end_stripe_index
        })
        .cloned()
        .collect::<Vec<_>>();
    plans.sort_unstable_by_key(|plan| (plan.stripe_index, plan.fragment_index));
    plans
}

pub(crate) fn build_finalize_plans(
    intent: &WriteIntent,
) -> Result<Vec<ReservationFinalizePlan>, Status> {
    let mut by_reservation: HashMap<String, Vec<u32>> = HashMap::new();
    for status in &intent.fragment_status {
        if status.reservation_id.is_empty() {
            continue;
        }
        by_reservation
            .entry(status.reservation_id.clone())
            .or_default()
            .push(status.reservation_placement_index);
    }
    let mut plans = intent
        .reservation_ids
        .iter()
        .map(|reservation_id| ReservationFinalizePlan {
            reservation_id: reservation_id.clone(),
            placement_indexes: by_reservation.remove(reservation_id).unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    for (reservation_id, placement_indexes) in by_reservation {
        plans.push(ReservationFinalizePlan {
            reservation_id,
            placement_indexes,
        });
    }
    Ok(plans)
}

#[cfg(test)]
pub(crate) fn build_finalize_plans_for_reservations(
    intent: &WriteIntent,
    reservation_ids: &[String],
) -> Result<Vec<ReservationFinalizePlan>, Status> {
    let mut by_reservation: HashMap<String, Vec<&FragmentWriteStatus>> = HashMap::new();
    for status in &intent.fragment_status {
        by_reservation
            .entry(status.reservation_id.clone())
            .or_default()
            .push(status);
    }
    let mut plans = Vec::new();
    for reservation_id in reservation_ids {
        let Some(statuses) = by_reservation.get(reservation_id) else {
            continue;
        };
        if statuses
            .iter()
            .all(|status| status.state == FragmentWriteState::Written as i32)
        {
            plans.push(ReservationFinalizePlan {
                reservation_id: reservation_id.clone(),
                placement_indexes: statuses
                    .iter()
                    .map(|status| status.reservation_placement_index)
                    .collect(),
            });
        }
    }
    Ok(plans)
}

pub(crate) fn apply_fragment_repair(
    intent: &mut WriteIntent,
    failed_fragments: &[FragmentRef],
    replacement_reservation: &PlacementReservationRecord,
) -> Result<(), Status> {
    if failed_fragments.is_empty() {
        return Err(Status::invalid_argument(
            "RepairObjectWrite requires at least one failed fragment",
        ));
    }
    if replacement_reservation.placements.len() != failed_fragments.len() {
        return Err(Status::invalid_argument(format!(
            "replacement reservation {} has {} placements for {} failed fragments",
            replacement_reservation.reservation_id,
            replacement_reservation.placements.len(),
            failed_fragments.len()
        )));
    }

    let mut seen = HashSet::new();
    for (replacement_index, fragment) in failed_fragments.iter().enumerate() {
        let status_index = locate_fragment_entry(intent, fragment)?;
        if !seen.insert(status_index) {
            continue;
        }
        let plan = intent.fragment_plans.get_mut(status_index).ok_or_else(|| {
            Status::internal(format!(
                "write intent {} is missing fragment plan for stripe {} fragment {}",
                intent.intent_id, fragment.stripe_index, fragment.fragment_index
            ))
        })?;
        let replacement = replacement_reservation
            .placements
            .get(replacement_index)
            .ok_or_else(|| {
                Status::internal(format!(
                    "replacement reservation {} is missing placement {}",
                    replacement_reservation.reservation_id, replacement_index
                ))
            })?;
        plan.target_id = replacement.target_id.clone();
        plan.endpoint = replacement.endpoint.clone();
        plan.granule_index = replacement.granule_index;
        plan.chunk_id = random_chunk_id();
        plan.generation = plan.generation.saturating_add(1);

        let status = intent
            .fragment_status
            .get_mut(status_index)
            .ok_or_else(|| {
                Status::internal(format!(
                    "write intent {} is missing fragment status for stripe {} fragment {}",
                    intent.intent_id, fragment.stripe_index, fragment.fragment_index
                ))
            })?;
        status.state = FragmentWriteState::Planned as i32;
        status.reservation_id = if replacement.reservation_id.is_empty() {
            replacement_reservation.reservation_id.clone()
        } else {
            replacement.reservation_id.clone()
        };
        status.reservation_placement_index = if replacement.reservation_id.is_empty() {
            replacement_index as u32
        } else {
            replacement.reservation_placement_index
        };
    }

    let replacement_reservation_id = replacement_reservation
        .placements
        .first()
        .and_then(|placement| {
            (!placement.reservation_id.is_empty()).then_some(placement.reservation_id.clone())
        })
        .unwrap_or_else(|| replacement_reservation.reservation_id.clone());
    if !intent
        .reservation_ids
        .iter()
        .any(|reservation_id| reservation_id == &replacement_reservation_id)
    {
        intent
            .reservation_ids
            .push(replacement_reservation_id.clone());
    }
    if intent.reservation_id.is_empty() {
        intent.reservation_id = replacement_reservation_id;
    }
    Ok(())
}

pub(crate) fn random_chunk_id() -> Vec<u8> {
    let mut bytes = vec![0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    bytes
}

fn validate_entry_name(name: &str) -> Result<(), Status> {
    if name.is_empty() {
        return Err(Status::invalid_argument("entry name must not be empty"));
    }
    if name.contains('/') {
        return Err(Status::invalid_argument(
            "entry names are single path components and may not contain '/'",
        ));
    }
    if name == "." || name == ".." {
        return Err(Status::invalid_argument(
            "entry names may not be '.' or '..'",
        ));
    }
    Ok(())
}

fn normalize_hierarchy_path(path: &str) -> Result<String, Status> {
    if path.trim().is_empty() {
        return Ok(String::new());
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return Err(Status::invalid_argument(
                "path traversal with '..' is not allowed",
            ));
        }
        validate_entry_name(part)?;
        parts.push(part);
    }
    Ok(parts.join("/"))
}

pub(crate) fn normalize_object_key(path: &str) -> Result<String, Status> {
    normalize_hierarchy_path(path)
}

pub(crate) fn join_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}

/// One level of the auto-create (mkdir -p) walk for a slashed object key.
///
/// `segment` is the bare path component; `prefix` is the accumulated
/// bucket-relative prefix shallow->deep (e.g. "a", then "a/b"); `level_path`
/// is the absolute namespace path of the collection at this level
/// (`join_path(bucket_path, prefix)`); `deterministic_id` is the id synthesized
/// when the level does not already exist in the path index
/// (`{bucket_entry_id}::<prefix>`). The actual id used at commit time is the
/// existing path-index id when present, else `deterministic_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AutoCreateLevel {
    pub segment: String,
    pub prefix: String,
    pub level_path: String,
    pub deterministic_id: String,
}

/// Pure synthesis of the auto-create walk levels for `parent_key` (the object
/// key with its trailing object name stripped), ordered shallow->deep so that
/// each parent is materialized before its children. Empty segments (from
/// leading/duplicate slashes) are skipped, matching the FDB walk in
/// `FdbHotStore::prepare_and_create_write_intent`.
pub(crate) fn auto_create_levels(
    bucket_entry_id: &str,
    bucket_path: &str,
    parent_key: &str,
) -> Vec<AutoCreateLevel> {
    let mut levels = Vec::new();
    let mut prefix = String::new();
    for segment in parent_key.split('/') {
        if segment.is_empty() {
            continue;
        }
        if prefix.is_empty() {
            prefix = segment.to_string();
        } else {
            prefix = format!("{prefix}/{segment}");
        }
        let level_path = join_path(bucket_path, &prefix);
        let deterministic_id = format!("{bucket_entry_id}::{prefix}");
        levels.push(AutoCreateLevel {
            segment: segment.to_string(),
            prefix: prefix.clone(),
            level_path,
            deterministic_id,
        });
    }
    levels
}

#[cfg(test)]
mod tests {
    use super::{apply_fragment_repair, build_finalize_plans_for_reservations};
    use keinctl::proto::{
        FragmentPlan, FragmentRef, FragmentWriteState, FragmentWriteStatus, PlacementReservation,
        PlacementReservationRecord, ReservationState, WriteIntent,
    };

    fn sample_intent() -> WriteIntent {
        WriteIntent {
            intent_id: "intent-1".to_string(),
            version_id: "version-1".to_string(),
            bucket_id: "bucket-1".to_string(),
            key: "bench/object.bin".to_string(),
            logical_length_bytes: 1_048_576,
            ec_profile_id: "ec".to_string(),
            stripe_count: 1,
            fragment_plans: vec![
                FragmentPlan {
                    fragment_index: 0,
                    chunk_id: vec![0; 32],
                    target_id: "t0".to_string(),
                    endpoint: "http://t0".to_string(),
                    granule_index: 10,
                    generation: 1,
                    stripe_index: 0,
                },
                FragmentPlan {
                    fragment_index: 1,
                    chunk_id: vec![1; 32],
                    target_id: "t1".to_string(),
                    endpoint: "http://t1".to_string(),
                    granule_index: 11,
                    generation: 1,
                    stripe_index: 0,
                },
                FragmentPlan {
                    fragment_index: 2,
                    chunk_id: vec![2; 32],
                    target_id: "t2".to_string(),
                    endpoint: "http://t2".to_string(),
                    granule_index: 12,
                    generation: 1,
                    stripe_index: 0,
                },
            ],
            expires_at_unix_ms: 0,
            state: 1,
            reservation_id: "res-old".to_string(),
            namespace_id: "ns".to_string(),
            object_entry_id: "obj".to_string(),
            bucket_entry_id: "bucket-entry".to_string(),
            reservation_ids: vec!["res-old".to_string()],
            fragment_status: vec![
                FragmentWriteStatus {
                    fragment_index: 0,
                    state: FragmentWriteState::Written as i32,
                    reservation_id: "res-old".to_string(),
                    reservation_placement_index: 0,
                    stripe_index: 0,
                },
                FragmentWriteStatus {
                    fragment_index: 1,
                    state: FragmentWriteState::Planned as i32,
                    reservation_id: "res-old".to_string(),
                    reservation_placement_index: 1,
                    stripe_index: 0,
                },
                FragmentWriteStatus {
                    fragment_index: 2,
                    state: FragmentWriteState::Planned as i32,
                    reservation_id: "res-old".to_string(),
                    reservation_placement_index: 2,
                    stripe_index: 0,
                },
            ],
            reservations_finalized: false,
            parent_entry_id: "parent".to_string(),
            parent_path: "bench".to_string(),
        }
    }

    #[test]
    fn apply_fragment_repair_retargets_failed_fragments_only() {
        let mut intent = sample_intent();
        let replacement = PlacementReservationRecord {
            reservation_id: "res-new".to_string(),
            state: ReservationState::Reserved as i32,
            placements: vec![
                PlacementReservation {
                    target_id: "t9".to_string(),
                    endpoint: "http://t9".to_string(),
                    granule_index: 99,
                    fragment_index: 0,
                    reservation_id: "res-new".to_string(),
                    reservation_placement_index: 0,
                },
                PlacementReservation {
                    target_id: "t8".to_string(),
                    endpoint: "http://t8".to_string(),
                    granule_index: 98,
                    fragment_index: 1,
                    reservation_id: "res-new".to_string(),
                    reservation_placement_index: 1,
                },
            ],
            expires_at_unix_ms: 0,
        };

        apply_fragment_repair(
            &mut intent,
            &[
                FragmentRef {
                    stripe_index: 0,
                    fragment_index: 1,
                },
                FragmentRef {
                    stripe_index: 0,
                    fragment_index: 2,
                },
            ],
            &replacement,
        )
        .expect("repair should succeed");

        assert_eq!(intent.fragment_plans[0].target_id, "t0");
        assert_eq!(intent.fragment_plans[1].target_id, "t9");
        assert_eq!(intent.fragment_plans[2].target_id, "t8");
        assert_eq!(
            intent.fragment_status[1].reservation_id,
            replacement.reservation_id
        );
        assert_eq!(
            intent.fragment_status[2].reservation_id,
            replacement.reservation_id
        );
        assert!(intent
            .reservation_ids
            .iter()
            .any(|reservation_id| reservation_id == "res-old"));
        assert!(intent
            .reservation_ids
            .iter()
            .any(|reservation_id| reservation_id == "res-new"));

        let old_finalize = build_finalize_plans_for_reservations(&intent, &["res-old".to_string()])
            .expect("old reservation finalize plan");
        assert_eq!(old_finalize.len(), 1);
        assert_eq!(old_finalize[0].placement_indexes, vec![0]);

        let new_finalize = build_finalize_plans_for_reservations(&intent, &["res-new".to_string()])
            .expect("new reservation finalize plan");
        assert!(new_finalize.is_empty());
    }

    #[test]
    fn auto_create_levels_threads_prefix_path_and_deterministic_id_shallow_to_deep() {
        // Object key "a/b/c.txt" under bucket "buck" whose entry id is
        // "buck-eid" and whose namespace path is "tenant/buck". The parent key
        // (object name stripped) is "a/b".
        let levels = super::auto_create_levels("buck-eid", "tenant/buck", "a/b");
        let triples: Vec<(String, String, String)> = levels
            .iter()
            .map(|l| {
                (
                    l.prefix.clone(),
                    l.level_path.clone(),
                    l.deterministic_id.clone(),
                )
            })
            .collect();
        assert_eq!(
            triples,
            vec![
                (
                    "a".to_string(),
                    "tenant/buck/a".to_string(),
                    "buck-eid::a".to_string(),
                ),
                (
                    "a/b".to_string(),
                    "tenant/buck/a/b".to_string(),
                    "buck-eid::a/b".to_string(),
                ),
            ],
        );
        // Segments preserved bare for the entry name.
        assert_eq!(levels[0].segment, "a");
        assert_eq!(levels[1].segment, "b");
    }

    #[test]
    fn auto_create_levels_root_bucket_path_has_no_leading_slash() {
        // Bucket sitting at the namespace root (empty bucket_path): level_path
        // must be the bare prefix, not "/a".
        let levels = super::auto_create_levels("buck-eid", "", "a/b");
        assert_eq!(levels[0].level_path, "a");
        assert_eq!(levels[1].level_path, "a/b");
    }

    #[test]
    fn auto_create_levels_skips_empty_segments() {
        // Leading and duplicate slashes must not synthesize empty levels,
        // matching the FDB walk's `segment.is_empty()` skip.
        let levels = super::auto_create_levels("buck-eid", "buck", "/a//b/");
        let prefixes: Vec<String> = levels.iter().map(|l| l.prefix.clone()).collect();
        assert_eq!(prefixes, vec!["a".to_string(), "a/b".to_string()]);
    }

    #[test]
    fn auto_create_levels_single_level() {
        let levels = super::auto_create_levels("buck-eid", "buck", "a");
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].prefix, "a");
        assert_eq!(levels[0].level_path, "buck/a");
        assert_eq!(levels[0].deterministic_id, "buck-eid::a");
    }
}
