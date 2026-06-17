// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit

use clap::{Args, Parser, Subcommand, ValueEnum};
use http::{header::CONTENT_LENGTH, Method, Request as HttpRequest, StatusCode, Uri};
use keinctl::proto;
use proto::kas_client::KasClient;
use proto::kms_client::KmsClient;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

type DynError = Box<dyn Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(name = "keinctl")]
#[command(about = "KeInFS operator CLI")]
struct Cli {
    #[arg(long)]
    context: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
    #[arg(long, num_args = 0..=1, default_missing_value = "2")]
    watch: Option<u64>,
    #[arg(long, default_value_t = 10)]
    timeout: u64,
    #[arg(long)]
    verbose: bool,
    #[arg(long)]
    confirm: bool,
    #[arg(long, value_enum)]
    fail_on: Option<HealthState>,
    #[command(subcommand)]
    command: TopCommand,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum OutputFormat {
    Table,
    Json,
    Toml,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

impl HealthState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
            Self::Unknown => "unknown",
        }
    }

    fn from_text(value: &str) -> Self {
        match value {
            "healthy" => Self::Healthy,
            "degraded" => Self::Degraded,
            "unhealthy" => Self::Unhealthy,
            _ => Self::Unknown,
        }
    }

    fn max(self, other: Self) -> Self {
        if other > self {
            other
        } else {
            self
        }
    }
}

#[derive(Subcommand, Debug)]
enum TopCommand {
    Context {
        #[command(subcommand)]
        command: ContextCommand,
    },
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    Namespace {
        #[command(subcommand)]
        command: NamespaceCommand,
    },
    Bucket {
        #[command(subcommand)]
        command: BucketCommand,
    },
    EcProfile {
        #[command(subcommand)]
        command: EcProfileCommand,
    },
    Object {
        #[command(subcommand)]
        command: ObjectCommand,
    },
    Target {
        #[command(subcommand)]
        command: TargetCommand,
    },
    Placement {
        #[command(subcommand)]
        command: PlacementCommand,
    },
    Intent {
        #[command(subcommand)]
        command: IntentCommand,
    },
    Allocator {
        #[command(subcommand)]
        command: AllocatorCommand,
    },
    Diag {
        #[command(subcommand)]
        command: DiagCommand,
    },
}

#[derive(Subcommand, Debug)]
enum ContextCommand {
    List,
    Show(ContextSelect),
    Use(ContextSelect),
    Validate(ContextSelect),
}

#[derive(Args, Debug)]
struct ContextSelect {
    name: Option<String>,
}

#[derive(Subcommand, Debug)]
enum ClusterCommand {
    Status,
    Topology,
    Events(ClusterEventsArgs),
    Watch,
}

#[derive(Args, Debug)]
struct ClusterEventsArgs {
    #[arg(long)]
    namespace_id: String,
    #[arg(long)]
    entry_id: Option<String>,
    #[arg(long)]
    parent_entry_id: Option<String>,
    #[arg(long, default_value_t = 0)]
    start_revision: u64,
    #[arg(long, default_value_t = 256)]
    limit: u32,
}

#[derive(Subcommand, Debug)]
enum ServiceCommand {
    List,
    Status(ServiceArgs),
    Stats(ServiceArgs),
    Watch(ServiceArgs),
}

#[derive(Args, Debug, Clone)]
struct ServiceArgs {
    service: ServiceKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum ServiceKind {
    Kms,
    Kas,
    Krs,
    Kst,
}

#[derive(Subcommand, Debug)]
enum NamespaceCommand {
    Create(CreateNamespaceArgs),
    CreateEntry(CreateNamespaceEntryArgs),
    List(NamespaceListArgs),
    Show(NamespaceShowArgs),
    Tree(NamespaceShowArgs),
    ResolvePath(NamespaceResolvePathArgs),
    ListChildren(NamespaceListChildrenArgs),
}

#[derive(Args, Debug)]
struct CreateNamespaceArgs {
    #[arg(long)]
    namespace_id: String,
    #[arg(long)]
    tenant_id: String,
    #[arg(long)]
    display_name: String,
    #[arg(long, value_enum, default_value_t = NamespaceStateArg::Active)]
    state: NamespaceStateArg,
}

#[derive(Args, Debug)]
struct CreateNamespaceEntryArgs {
    #[arg(long)]
    entry_id: String,
    #[arg(long)]
    namespace_id: String,
    #[arg(long, default_value = "")]
    parent_entry_id: String,
    #[arg(long)]
    name: String,
    #[arg(long, value_enum)]
    kind: NamespaceEntryKindArg,
}

#[derive(Args, Debug)]
struct NamespaceListArgs {
    #[arg(long)]
    tenant_id: Option<String>,
}

#[derive(Args, Debug)]
struct NamespaceShowArgs {
    namespace_id: String,
}

#[derive(Args, Debug)]
struct NamespaceResolvePathArgs {
    namespace_id: String,
    path: String,
}

#[derive(Args, Debug)]
struct NamespaceListChildrenArgs {
    namespace_id: String,
    #[arg(long, default_value = "")]
    parent_entry_id: String,
    #[arg(long, default_value_t = 256)]
    limit: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum NamespaceStateArg {
    Active,
    Disabled,
    Deleting,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum NamespaceEntryKindArg {
    Project,
    Team,
    Group,
    Workspace,
    Bucket,
    Collection,
    Object,
}

#[derive(Subcommand, Debug)]
enum BucketCommand {
    Create(CreateBucketArgs),
    List(ListBucketsArgs),
    Show(ShowBucketArgs),
}

#[derive(Args, Debug)]
struct CreateBucketArgs {
    #[arg(long)]
    bucket_id: String,
    #[arg(long)]
    namespace_id: String,
    #[arg(long)]
    parent_entry_id: String,
    #[arg(long)]
    ec_profile_id: String,
}

#[derive(Args, Debug)]
struct ListBucketsArgs {
    #[arg(long)]
    namespace_id: Option<String>,
    #[arg(long)]
    parent_entry_id: Option<String>,
}

#[derive(Args, Debug)]
struct ShowBucketArgs {
    bucket_id: String,
}

#[derive(Subcommand, Debug)]
enum EcProfileCommand {
    Create(CreateEcProfileArgs),
    List,
    Show(ShowEcProfileArgs),
}

#[derive(Args, Debug)]
struct CreateEcProfileArgs {
    #[arg(long)]
    id: String,
    #[arg(long, default_value = "rs")]
    codec_id: String,
    #[arg(long)]
    data_fragments: u32,
    #[arg(long)]
    parity_fragments: u32,
    #[arg(long)]
    fragment_bytes: u32,
    #[arg(long, value_enum, default_value_t = FailureDomainArg::DriveDomainLab)]
    failure_domain: FailureDomainArg,
}

#[derive(Args, Debug)]
struct ShowEcProfileArgs {
    id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum FailureDomainArg {
    DriveDomainLab,
    Node,
    Rack,
}

#[derive(Subcommand, Debug)]
enum ObjectCommand {
    Head(ObjectKeyArgs),
    Manifest(ObjectKeyArgs),
    Locate(ObjectKeyArgs),
}

#[derive(Args, Debug)]
struct ObjectKeyArgs {
    #[arg(long)]
    bucket_id: String,
    #[arg(long)]
    key: String,
}

#[derive(Subcommand, Debug)]
enum TargetCommand {
    Register(RegisterTargetArgs),
    List,
    Show(TargetIdArgs),
    Fail(TargetIdArgs),
    Drain(TargetIdArgs),
    Recover(TargetIdArgs),
    Retire(TargetIdArgs),
    RebalancePreview(RebalanceArgs),
    RebalanceEnqueue(RebalanceArgs),
}

#[derive(Args, Debug)]
struct TargetIdArgs {
    target_id: String,
}

#[derive(Args, Debug)]
struct RegisterTargetArgs {
    #[arg(long)]
    target_id: String,
    #[arg(long)]
    endpoint: String,
    #[arg(long)]
    server_id: String,
    #[arg(long)]
    rack_id: String,
    #[arg(long)]
    allocation_shard_id: String,
    #[arg(long, value_enum, default_value_t = FailureDomainArg::DriveDomainLab)]
    failure_domain: FailureDomainArg,
    #[arg(long)]
    granule_count: u64,
    #[arg(long)]
    free_granules: Option<u64>,
    #[arg(long, default_value_t = true)]
    healthy: bool,
    #[arg(long, default_value_t = 0)]
    last_heartbeat_unix_ms: u64,
    #[arg(long, value_enum, default_value_t = TargetLifecycleStateArg::Active)]
    lifecycle_state: TargetLifecycleStateArg,
}

#[derive(Args, Debug)]
struct RebalanceArgs {
    #[arg(long, value_delimiter = ',')]
    source_target_ids: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    include_target_ids: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    exclude_target_ids: Vec<String>,
    #[arg(long, default_value_t = 256)]
    max_tasks: u32,
}

#[derive(Subcommand, Debug)]
enum PlacementCommand {
    Summary(PlacementListArgs),
    List(PlacementListArgs),
    Show(PlacementShowArgs),
    Watch(PlacementListArgs),
    Wait(PlacementWaitArgs),
}

#[derive(Args, Debug, Clone)]
struct PlacementListArgs {
    #[arg(long)]
    source_target_id: Option<String>,
    #[arg(long)]
    object_version_ref: Option<String>,
    #[arg(long, value_enum)]
    task_kind: Option<PlacementTaskKindArg>,
    #[arg(long, value_enum)]
    state: Option<PlacementTaskStateArg>,
    #[arg(long, default_value_t = 256)]
    limit: u32,
}

#[derive(Args, Debug, Clone)]
struct PlacementWaitArgs {
    #[command(flatten)]
    filters: PlacementListArgs,
    #[arg(long, default_value_t = 300)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 2)]
    poll_secs: u64,
    #[arg(long, default_value_t = 1)]
    settle_polls: u32,
    #[arg(long, default_value_t = false)]
    allow_failed: bool,
}

#[derive(Args, Debug)]
struct PlacementShowArgs {
    task_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum PlacementTaskKindArg {
    Rebuild,
    Rebalance,
    Evacuate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum PlacementTaskStateArg {
    Pending,
    Leased,
    Completed,
    Failed,
}

#[derive(Subcommand, Debug)]
enum IntentCommand {
    Summary(IntentListArgs),
    List(IntentListArgs),
    Show(IntentShowArgs),
    Wait(IntentWaitArgs),
}

#[derive(Args, Debug, Clone)]
struct IntentListArgs {
    #[arg(long)]
    bucket_id: Option<String>,
    #[arg(long, value_enum)]
    state: Option<WriteIntentStateArg>,
    #[arg(long, default_value_t = 256)]
    limit: u32,
}

#[derive(Args, Debug, Clone)]
struct IntentWaitArgs {
    #[command(flatten)]
    filters: IntentListArgs,
    #[arg(long, default_value_t = 300)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 2)]
    poll_secs: u64,
    #[arg(long, default_value_t = 1)]
    settle_polls: u32,
}

#[derive(Args, Debug)]
struct IntentShowArgs {
    intent_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum WriteIntentStateArg {
    Pending,
    Reserved,
    Committed,
    Aborted,
    Expired,
}

#[derive(Subcommand, Debug)]
enum AllocatorCommand {
    Reservations(ReservationListArgs),
    ReservationShow(ReservationShowArgs),
    ReserveBatch(ReservationReserveBatchArgs),
}

#[derive(Args, Debug)]
struct ReservationListArgs {
    #[arg(long, value_enum)]
    state: Option<ReservationStateArg>,
    #[arg(long)]
    target_id: Option<String>,
    #[arg(long, default_value_t = 256)]
    limit: u32,
}

#[derive(Args, Debug)]
struct ReservationShowArgs {
    reservation_id: String,
}

#[derive(Args, Debug)]
struct ReservationReserveBatchArgs {
    #[arg(long)]
    batch_size: u32,
    #[arg(long)]
    fragment_count: u32,
    #[arg(long, value_enum, default_value_t = FailureDomainArg::DriveDomainLab)]
    failure_domain: FailureDomainArg,
    #[arg(long, default_value_t = 30_000)]
    reservation_ttl_ms: u64,
    #[arg(long, value_delimiter = ',')]
    excluded_target_ids: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum ReservationStateArg {
    Reserved,
    Finalized,
    Released,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum TargetLifecycleStateArg {
    Active,
    Draining,
    Unhealthy,
    Retired,
}

#[derive(Subcommand, Debug)]
enum DiagCommand {
    RuntimeList,
    RuntimeShow(RuntimeShowArgs),
    LastErrors,
    TargetHttpInfo(TargetHttpArgs),
    TargetHttpStats(TargetHttpArgs),
}

#[derive(Args, Debug)]
struct RuntimeShowArgs {
    service: ServiceKind,
    #[arg(long)]
    runtime_dir: Option<String>,
}

#[derive(Args, Debug)]
struct TargetHttpArgs {
    #[arg(long, value_delimiter = ',')]
    endpoints: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContextConfig {
    #[serde(default)]
    label: Option<String>,
    #[serde(default = "default_kms_endpoint")]
    kms_endpoint: String,
    #[serde(default = "default_kas_endpoint")]
    kas_endpoint: String,
    #[serde(default)]
    kms_runtime_root: Option<String>,
    #[serde(default)]
    kas_runtime_root: Option<String>,
    #[serde(default)]
    krs_runtime_root: Option<String>,
    #[serde(default)]
    kst_runtime_root: Option<String>,
    #[serde(default)]
    kst_http_endpoints: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContextFile {
    current_context: String,
    contexts: BTreeMap<String, ContextConfig>,
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            label: Some("default".to_string()),
            kms_endpoint: default_kms_endpoint(),
            kas_endpoint: default_kas_endpoint(),
            kms_runtime_root: Some("/run/keinfs/kms".to_string()),
            kas_runtime_root: Some("/run/keinfs/kas".to_string()),
            krs_runtime_root: Some("/run/keinfs/krs".to_string()),
            kst_runtime_root: Some("/run/keinfs/kst".to_string()),
            kst_http_endpoints: Vec::new(),
        }
    }
}

impl Default for ContextFile {
    fn default() -> Self {
        let mut contexts = BTreeMap::new();
        contexts.insert("default".to_string(), ContextConfig::default());
        Self {
            current_context: "default".to_string(),
            contexts,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct RuntimeStatusDoc {
    service: String,
    health: String,
    ready: bool,
    uptime_ms: u64,
    started_unix_s: u64,
    pid: u32,
    #[serde(default)]
    last_error: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, toml::Value>,
}

#[derive(Clone, Debug, Serialize)]
struct ServiceStatusReport {
    service: String,
    health: HealthState,
    reachable: bool,
    endpoint: Option<String>,
    runtime_dir: Option<String>,
    last_error: Option<String>,
    details: BTreeMap<String, String>,
    instances: Vec<ServiceInstanceStatus>,
}

#[derive(Clone, Debug, Serialize)]
struct ServiceInstanceStatus {
    instance_id: String,
    service: String,
    node_id: String,
    endpoint: String,
    package_name: String,
    version: String,
    release: u64,
    git_sha: String,
    git_dirty: bool,
    instance_label: String,
    config_hash: String,
    heartbeat_age_ms: u64,
    heartbeat_interval_ms: u64,
    stale: bool,
}

#[derive(Clone, Debug, Serialize)]
struct ClusterStatusReport {
    health: HealthState,
    reasons: Vec<String>,
    services: Vec<ServiceStatusReport>,
    targets: TargetCountReport,
    placement: PlacementCountReport,
    intents: IntentCountReport,
}

#[derive(Clone, Debug, Serialize, Default)]
struct TargetCountReport {
    total: usize,
    active: usize,
    draining: usize,
    unhealthy: usize,
    retired: usize,
    unhealthy_heartbeat: usize,
}

#[derive(Clone, Debug, Serialize, Default)]
struct PlacementCountReport {
    total: usize,
    pending_rebuild: usize,
    pending_rebalance: usize,
    pending_evacuate: usize,
    leased: usize,
    failed: usize,
}

#[derive(Clone, Debug, Serialize, Default)]
struct IntentCountReport {
    total: usize,
    pending: usize,
    reserved: usize,
    committed: usize,
    aborted: usize,
    expired: usize,
}

#[derive(Clone, Debug, Serialize)]
struct TargetReport {
    target: proto::TargetRecord,
    placement: proto::TargetPlacementStatus,
    health: HealthState,
    reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct PlacementSummaryReport {
    total: usize,
    by_kind_state: Vec<PlacementKindStateCount>,
}

#[derive(Clone, Debug, Serialize)]
struct PlacementKindStateCount {
    task_kind: String,
    state: String,
    count: usize,
}

#[derive(Clone, Debug, Serialize)]
struct RuntimeListEntry {
    service: String,
    runtime_dir: String,
    status_path: Option<String>,
    summary_path: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct RuntimeShowReport {
    service: String,
    runtime_dir: String,
    status: Option<RuntimeStatusDoc>,
    identity_toml: Option<String>,
    summary_toml: Option<String>,
    summary_json: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct TargetHttpReport<T: Serialize> {
    endpoint: String,
    value: T,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let code = match run().await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err}");
            1
        }
    };
    std::process::exit(code);
}

async fn run() -> Result<i32, DynError> {
    let cli = Cli::parse();
    let timeout = Duration::from_secs(cli.timeout.max(1));
    let result_health = match &cli.command {
        TopCommand::Context { command } => {
            run_context_command(command, &cli).await?;
            None
        }
        _ => {
            let (context_name, context) = resolve_context(cli.context.as_deref())?;
            dispatch_with_context(&cli, &context_name, &context, timeout).await?
        }
    };

    if let (Some(actual), Some(threshold)) = (result_health, cli.fail_on) {
        if actual >= threshold {
            return Ok(2);
        }
    }
    Ok(0)
}

async fn dispatch_with_context(
    cli: &Cli,
    context_name: &str,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<Option<HealthState>, DynError> {
    match &cli.command {
        TopCommand::Cluster { command } => {
            run_cluster_command(command, cli, context_name, context, timeout).await
        }
        TopCommand::Service { command } => {
            run_service_command(command, cli, context, timeout).await
        }
        TopCommand::Namespace { command } => {
            run_namespace_command(command, cli, context, timeout).await?;
            Ok(None)
        }
        TopCommand::Bucket { command } => {
            run_bucket_command(command, cli, context, timeout).await?;
            Ok(None)
        }
        TopCommand::EcProfile { command } => {
            run_ec_profile_command(command, cli, context, timeout).await?;
            Ok(None)
        }
        TopCommand::Object { command } => {
            run_object_command(command, cli, context, timeout).await?;
            Ok(None)
        }
        TopCommand::Target { command } => run_target_command(command, cli, context, timeout).await,
        TopCommand::Placement { command } => {
            run_placement_command(command, cli, context, timeout).await?;
            Ok(None)
        }
        TopCommand::Intent { command } => {
            run_intent_command(command, cli, context, timeout).await?;
            Ok(None)
        }
        TopCommand::Allocator { command } => {
            run_allocator_command(command, cli, context, timeout).await?;
            Ok(None)
        }
        TopCommand::Diag { command } => {
            run_diag_command(command, cli, context, timeout).await?;
            Ok(None)
        }
        TopCommand::Context { .. } => unreachable!(),
    }
}

async fn run_context_command(command: &ContextCommand, cli: &Cli) -> Result<(), DynError> {
    let mut file = load_context_file()?;
    match command {
        ContextCommand::List => {
            let rows = file
                .contexts
                .iter()
                .map(|(name, context)| {
                    BTreeMap::from([
                        ("name".to_string(), name.clone()),
                        (
                            "current".to_string(),
                            (file.current_context == *name).to_string(),
                        ),
                        ("kms_endpoint".to_string(), context.kms_endpoint.clone()),
                        ("kas_endpoint".to_string(), context.kas_endpoint.clone()),
                    ])
                })
                .collect::<Vec<_>>();
            print_rows(
                cli.format,
                &rows,
                &["name", "current", "kms_endpoint", "kas_endpoint"],
            )?;
        }
        ContextCommand::Show(select) => {
            let name = select
                .name
                .as_deref()
                .unwrap_or(file.current_context.as_str())
                .to_string();
            let context = file
                .contexts
                .get(&name)
                .cloned()
                .ok_or_else(|| boxed_error(format!("unknown context {}", name)))?;
            let report = ContextShowReport {
                name: name.clone(),
                current: file.current_context == name,
                context: context.clone(),
            };
            print_structured(cli.format, &report, || {
                Ok(render_context_show(
                    &file.current_context,
                    &report.name,
                    &report.context,
                ))
            })?;
        }
        ContextCommand::Use(select) => {
            let name = select
                .name
                .as_deref()
                .unwrap_or(file.current_context.as_str())
                .to_string();
            if !file.contexts.contains_key(&name) {
                return Err(boxed_error(format!("unknown context {}", name)));
            }
            file.current_context = name.clone();
            save_context_file(&file)?;
            print_text(cli.format, &format!("current_context = \"{}\"\n", name))?;
        }
        ContextCommand::Validate(select) => {
            let name = select
                .name
                .as_deref()
                .unwrap_or(file.current_context.as_str())
                .to_string();
            let context = file
                .contexts
                .get(&name)
                .cloned()
                .ok_or_else(|| boxed_error(format!("unknown context {}", name)))?;
            let timeout = Duration::from_secs(cli.timeout.max(1));
            let kms_ok = connect_kms(&context, timeout).await.is_ok();
            let kas_ok = connect_kas(&context, timeout).await.is_ok();
            let report = ContextValidateReport {
                name,
                kms_reachable: kms_ok,
                kas_reachable: kas_ok,
                kms_runtime_root_exists: context_root_exists(context.kms_runtime_root.as_deref()),
                kas_runtime_root_exists: context_root_exists(context.kas_runtime_root.as_deref()),
                krs_runtime_root_exists: context_root_exists(context.krs_runtime_root.as_deref()),
                kst_runtime_root_exists: context_root_exists(context.kst_runtime_root.as_deref()),
            };
            print_structured(cli.format, &report, || Ok(render_context_validate(&report)))?;
        }
    }
    Ok(())
}

async fn run_cluster_command(
    command: &ClusterCommand,
    cli: &Cli,
    context_name: &str,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<Option<HealthState>, DynError> {
    match command {
        ClusterCommand::Status => {
            let report = collect_cluster_status(context_name, context, timeout).await?;
            print_structured(cli.format, &report, || Ok(render_cluster_status(&report)))?;
            Ok(Some(report.health))
        }
        ClusterCommand::Topology => {
            let report = collect_cluster_topology(context_name, context, timeout).await?;
            print_structured(cli.format, &report, || Ok(render_cluster_topology(&report)))?;
            Ok(None)
        }
        ClusterCommand::Events(args) => {
            let report = collect_cluster_events(context, timeout, args).await?;
            print_structured(cli.format, &report, || Ok(render_metadata_events(&report)))?;
            Ok(None)
        }
        ClusterCommand::Watch => {
            let interval = cli.watch.unwrap_or(2);
            loop {
                let report = collect_cluster_status(context_name, context, timeout).await?;
                print_structured(cli.format, &report, || Ok(render_cluster_status(&report)))?;
                tokio::time::sleep(Duration::from_secs(interval.max(1))).await;
            }
        }
    }
}

async fn run_service_command(
    command: &ServiceCommand,
    cli: &Cli,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<Option<HealthState>, DynError> {
    match command {
        ServiceCommand::List => {
            let rows = collect_service_list_rows(context, timeout).await?;
            print_rows(
                cli.format,
                &rows,
                &[
                    "service", "node_id", "endpoint", "version", "release", "git_sha", "stale",
                ],
            )?;
            Ok(None)
        }
        ServiceCommand::Status(args) => {
            let report = collect_service_status(context, timeout, &args.service).await?;
            print_structured(cli.format, &report, || Ok(render_service_status(&report)))?;
            Ok(Some(report.health))
        }
        ServiceCommand::Stats(args) => {
            let report = collect_service_stats(context, &args.service).await?;
            print_structured(cli.format, &report, || Ok(render_runtime_show(&report)))?;
            Ok(None)
        }
        ServiceCommand::Watch(args) => {
            let interval = cli.watch.unwrap_or(2);
            loop {
                let report = collect_service_status(context, timeout, &args.service).await?;
                print_structured(cli.format, &report, || Ok(render_service_status(&report)))?;
                tokio::time::sleep(Duration::from_secs(interval.max(1))).await;
            }
        }
    }
}

async fn run_namespace_command(
    command: &NamespaceCommand,
    cli: &Cli,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<(), DynError> {
    let mut kms = connect_kms(context, timeout).await?;
    match command {
        NamespaceCommand::Create(args) => {
            require_confirm(cli)?;
            let reply = rpc_timeout(
                timeout,
                kms.create_namespace(Request::new(proto::CreateNamespaceRequest {
                    namespace: Some(proto::NamespaceRecord {
                        namespace_id: args.namespace_id.clone(),
                        tenant_id: args.tenant_id.clone(),
                        display_name: args.display_name.clone(),
                        state: namespace_state_from_arg(args.state.clone()) as i32,
                        shard_id: String::new(),
                    }),
                })),
            )
            .await?
            .into_inner();
            let namespace = reply
                .namespace
                .ok_or_else(|| boxed_error("KMS returned no namespace"))?;
            print_structured(cli.format, &namespace, || {
                Ok(render_namespace_list(&[namespace.clone()]))
            })?;
        }
        NamespaceCommand::CreateEntry(args) => {
            require_confirm(cli)?;
            let reply = rpc_timeout(
                timeout,
                kms.create_namespace_entry(Request::new(proto::CreateNamespaceEntryRequest {
                    entry: Some(proto::NamespaceDomainEntry {
                        entry_id: args.entry_id.clone(),
                        namespace_id: args.namespace_id.clone(),
                        parent_entry_id: args.parent_entry_id.clone(),
                        name: args.name.clone(),
                        kind: namespace_entry_kind_from_arg(args.kind.clone()) as i32,
                        path: String::new(),
                    }),
                })),
            )
            .await?
            .into_inner();
            let entry = reply
                .entry
                .ok_or_else(|| boxed_error("KMS returned no namespace entry"))?;
            print_structured(cli.format, &entry, || {
                Ok(render_list_children(&[entry.clone()]))
            })?;
        }
        NamespaceCommand::List(args) => {
            let reply = rpc_timeout(
                timeout,
                kms.list_namespaces(Request::new(proto::ListNamespacesRequest {
                    tenant_id: args.tenant_id.clone().unwrap_or_default(),
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply.namespaces, || {
                Ok(render_namespace_list(&reply.namespaces))
            })?;
        }
        NamespaceCommand::Show(args) => {
            let reply = rpc_timeout(
                timeout,
                kms.get_namespace(Request::new(proto::GetNamespaceRequest {
                    namespace_id: args.namespace_id.clone(),
                })),
            )
            .await?
            .into_inner();
            let namespace = reply
                .namespace
                .ok_or_else(|| boxed_error("KMS returned no namespace"))?;
            let report = NamespaceShowReport {
                namespace,
                shard_map: reply.shard_map,
            };
            print_structured(cli.format, &report, || {
                Ok(render_namespace_show(
                    &report.namespace,
                    report.shard_map.as_ref(),
                ))
            })?;
        }
        NamespaceCommand::Tree(args) => {
            let tree = build_namespace_tree(&mut kms, timeout, &args.namespace_id, "").await?;
            print_structured(cli.format, &tree, || Ok(render_namespace_tree(&tree, 0)))?;
        }
        NamespaceCommand::ResolvePath(args) => {
            let reply = rpc_timeout(
                timeout,
                kms.resolve_path(Request::new(proto::ResolvePathRequest {
                    namespace_id: args.namespace_id.clone(),
                    path: args.path.clone(),
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply, || Ok(render_resolve_path(&reply)))?;
        }
        NamespaceCommand::ListChildren(args) => {
            let entries = list_children_all(
                &mut kms,
                timeout,
                &args.namespace_id,
                &args.parent_entry_id,
                args.limit,
            )
            .await?;
            print_structured(cli.format, &entries, || Ok(render_list_children(&entries)))?;
        }
    }
    Ok(())
}

async fn run_bucket_command(
    command: &BucketCommand,
    cli: &Cli,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<(), DynError> {
    let mut kms = connect_kms(context, timeout).await?;
    match command {
        BucketCommand::Create(args) => {
            require_confirm(cli)?;
            let reply = rpc_timeout(
                timeout,
                kms.create_bucket(Request::new(proto::CreateBucketRequest {
                    bucket: Some(proto::BucketRecord {
                        bucket_id: args.bucket_id.clone(),
                        ec_profile_id: args.ec_profile_id.clone(),
                        namespace_id: args.namespace_id.clone(),
                        parent_entry_id: args.parent_entry_id.clone(),
                        bucket_entry_id: String::new(),
                    }),
                })),
            )
            .await?
            .into_inner();
            let bucket = reply
                .bucket
                .ok_or_else(|| boxed_error("KMS returned no bucket"))?;
            print_structured(cli.format, &bucket, || {
                Ok(render_bucket_list(&[bucket.clone()]))
            })?;
        }
        BucketCommand::List(args) => {
            let reply = rpc_timeout(
                timeout,
                kms.list_buckets(Request::new(proto::ListBucketsRequest {
                    namespace_id: args.namespace_id.clone().unwrap_or_default(),
                    parent_entry_id: args.parent_entry_id.clone().unwrap_or_default(),
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply.buckets, || {
                Ok(render_bucket_list(&reply.buckets))
            })?;
        }
        BucketCommand::Show(args) => {
            let reply = rpc_timeout(
                timeout,
                kms.get_bucket(Request::new(proto::GetBucketRequest {
                    bucket_id: args.bucket_id.clone(),
                })),
            )
            .await?
            .into_inner();
            let bucket = reply
                .bucket
                .ok_or_else(|| boxed_error("KMS returned no bucket"))?;
            let report = BucketShowReport {
                bucket,
                ec_profile: reply.ec_profile,
            };
            print_structured(cli.format, &report, || {
                Ok(render_bucket_show(
                    &report.bucket,
                    report.ec_profile.as_ref(),
                ))
            })?;
        }
    }
    Ok(())
}

async fn run_ec_profile_command(
    command: &EcProfileCommand,
    cli: &Cli,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<(), DynError> {
    let mut kms = connect_kms(context, timeout).await?;
    match command {
        EcProfileCommand::Create(args) => {
            require_confirm(cli)?;
            let reply = rpc_timeout(
                timeout,
                kms.create_ec_profile(Request::new(proto::CreateEcProfileRequest {
                    profile: Some(proto::EcProfile {
                        id: args.id.clone(),
                        codec_id: args.codec_id.clone(),
                        data_fragments: args.data_fragments,
                        parity_fragments: args.parity_fragments,
                        fragment_bytes: args.fragment_bytes,
                        failure_domain: failure_domain_from_arg(args.failure_domain.clone()) as i32,
                    }),
                })),
            )
            .await?
            .into_inner();
            let profile = reply
                .profile
                .ok_or_else(|| boxed_error("KMS returned no profile"))?;
            print_structured(cli.format, &profile, || {
                Ok(render_ec_profiles(&[profile.clone()]))
            })?;
        }
        EcProfileCommand::List => {
            let reply = rpc_timeout(
                timeout,
                kms.list_ec_profiles(Request::new(proto::ListEcProfilesRequest {})),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply.profiles, || {
                Ok(render_ec_profiles(&reply.profiles))
            })?;
        }
        EcProfileCommand::Show(args) => {
            let reply = rpc_timeout(
                timeout,
                kms.list_ec_profiles(Request::new(proto::ListEcProfilesRequest {})),
            )
            .await?
            .into_inner();
            let profile = reply
                .profiles
                .into_iter()
                .find(|profile| profile.id == args.id)
                .ok_or_else(|| boxed_error(format!("unknown EC profile {}", args.id)))?;
            print_structured(cli.format, &profile, || {
                Ok(render_ec_profiles(&[profile.clone()]))
            })?;
        }
    }
    Ok(())
}

async fn run_object_command(
    command: &ObjectCommand,
    cli: &Cli,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<(), DynError> {
    let mut kms = connect_kms(context, timeout).await?;
    let args = match command {
        ObjectCommand::Head(args) | ObjectCommand::Manifest(args) | ObjectCommand::Locate(args) => {
            args
        }
    };
    let reply = rpc_timeout(
        timeout,
        kms.resolve_object_read(Request::new(proto::ResolveObjectReadRequest {
            bucket_id: args.bucket_id.clone(),
            key: args.key.clone(),
        })),
    )
    .await?
    .into_inner();
    let manifest = reply
        .manifest
        .ok_or_else(|| boxed_error("KMS returned no manifest"))?;
    match command {
        ObjectCommand::Head(_) => {
            let report = ObjectHeadReport {
                bucket_id: manifest.bucket_id.clone(),
                key: manifest.key.clone(),
                version_id: manifest.version_id.clone(),
                logical_length_bytes: manifest.logical_length_bytes,
                ec_profile_id: manifest.ec_profile_id.clone(),
                stripe_count: manifest.stripes.len() as u32,
            };
            print_structured(cli.format, &report, || Ok(render_object_head(&report)))?;
        }
        ObjectCommand::Manifest(_) => {
            print_structured(cli.format, &manifest, || {
                Ok(render_object_manifest(&manifest))
            })?;
        }
        ObjectCommand::Locate(_) => {
            let report = build_object_locate_report(&manifest);
            print_structured(cli.format, &report, || Ok(render_object_locate(&report)))?;
        }
    }
    Ok(())
}

async fn run_target_command(
    command: &TargetCommand,
    cli: &Cli,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<Option<HealthState>, DynError> {
    match command {
        TargetCommand::Register(args) => {
            require_confirm(cli)?;
            let mut kas = connect_kas(context, timeout).await?;
            let free_granules = args.free_granules.unwrap_or(args.granule_count);
            let reply = rpc_timeout(
                timeout,
                kas.register_target(Request::new(proto::RegisterTargetRequest {
                    target: Some(proto::TargetRecord {
                        target_id: args.target_id.clone(),
                        endpoint: args.endpoint.clone(),
                        server_id: args.server_id.clone(),
                        rack_id: args.rack_id.clone(),
                        allocation_shard_id: args.allocation_shard_id.clone(),
                        failure_domain: failure_domain_from_arg(args.failure_domain.clone()) as i32,
                        granule_count: args.granule_count,
                        free_granules,
                        healthy: args.healthy,
                        last_heartbeat_unix_ms: args.last_heartbeat_unix_ms,
                        lifecycle_state: target_lifecycle_from_arg(args.lifecycle_state.clone())
                            as i32,
                    }),
                })),
            )
            .await?
            .into_inner();
            let target = reply
                .target
                .ok_or_else(|| boxed_error("KAS returned no target"))?;
            let row = BTreeMap::from([
                ("target_id".to_string(), target.target_id),
                ("endpoint".to_string(), target.endpoint),
                ("server_id".to_string(), target.server_id),
                ("rack_id".to_string(), target.rack_id),
                (
                    "allocation_shard_id".to_string(),
                    target.allocation_shard_id,
                ),
                (
                    "failure_domain".to_string(),
                    failure_domain_name(target.failure_domain).to_string(),
                ),
                (
                    "granule_count".to_string(),
                    target.granule_count.to_string(),
                ),
                (
                    "free_granules".to_string(),
                    target.free_granules.to_string(),
                ),
                ("healthy".to_string(), target.healthy.to_string()),
                (
                    "lifecycle_state".to_string(),
                    target_lifecycle_name(target.lifecycle_state).to_string(),
                ),
            ]);
            print_rows(
                cli.format,
                &[row],
                &[
                    "target_id",
                    "endpoint",
                    "server_id",
                    "rack_id",
                    "failure_domain",
                    "granule_count",
                    "free_granules",
                    "healthy",
                    "lifecycle_state",
                ],
            )?;
            Ok(None)
        }
        TargetCommand::List => {
            let reports = collect_target_reports(context, timeout).await?;
            print_structured(cli.format, &reports, || Ok(render_target_reports(&reports)))?;
            Ok(Some(aggregate_health(
                reports.iter().map(|report| report.health),
            )))
        }
        TargetCommand::Show(args) => {
            let report = collect_target_report(context, timeout, &args.target_id).await?;
            print_structured(cli.format, &report, || Ok(render_target_report(&report)))?;
            Ok(Some(report.health))
        }
        TargetCommand::Fail(args) => {
            require_confirm(cli)?;
            let mut kms = connect_kms(context, timeout).await?;
            let reply = rpc_timeout(
                timeout,
                kms.report_target_failure(Request::new(proto::ReportTargetFailureRequest {
                    target_id: args.target_id.clone(),
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply, || {
                Ok(format!(
                    "target_id={}\ncreated_tasks={}\n",
                    args.target_id, reply.created_tasks
                ))
            })?;
            Ok(None)
        }
        TargetCommand::Drain(args) => {
            require_confirm(cli)?;
            let mut kms = connect_kms(context, timeout).await?;
            let reply = rpc_timeout(
                timeout,
                kms.drain_target(Request::new(proto::DrainTargetRequest {
                    target_id: args.target_id.clone(),
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply, || {
                Ok(format!(
                    "target_id={}\ncreated_tasks={}\n",
                    args.target_id, reply.created_tasks
                ))
            })?;
            Ok(None)
        }
        TargetCommand::Recover(args) => {
            require_confirm(cli)?;
            let mut kms = connect_kms(context, timeout).await?;
            let reply = rpc_timeout(
                timeout,
                kms.recover_target(Request::new(proto::RecoverTargetRequest {
                    target_id: args.target_id.clone(),
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply, || {
                Ok(render_recover_retire(
                    &args.target_id,
                    &reply.target,
                    reply.live_fragments,
                ))
            })?;
            Ok(None)
        }
        TargetCommand::Retire(args) => {
            require_confirm(cli)?;
            let mut kms = connect_kms(context, timeout).await?;
            let reply = rpc_timeout(
                timeout,
                kms.retire_target(Request::new(proto::RetireTargetRequest {
                    target_id: args.target_id.clone(),
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply, || {
                Ok(render_recover_retire(
                    &args.target_id,
                    &reply.target,
                    reply.live_fragments,
                ))
            })?;
            Ok(None)
        }
        TargetCommand::RebalancePreview(args) => {
            let mut kms = connect_kms(context, timeout).await?;
            let reply = rpc_timeout(
                timeout,
                kms.preview_target_rebalance(Request::new(proto::PreviewTargetRebalanceRequest {
                    source_target_ids: args.source_target_ids.clone(),
                    include_target_ids: args.include_target_ids.clone(),
                    exclude_target_ids: args.exclude_target_ids.clone(),
                    max_tasks: args.max_tasks,
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply, || {
                Ok(render_rebalance_reply("preview", &reply))
            })?;
            Ok(None)
        }
        TargetCommand::RebalanceEnqueue(args) => {
            require_confirm(cli)?;
            let mut kms = connect_kms(context, timeout).await?;
            let reply = rpc_timeout(
                timeout,
                kms.enqueue_target_rebalance(Request::new(proto::EnqueueTargetRebalanceRequest {
                    source_target_ids: args.source_target_ids.clone(),
                    include_target_ids: args.include_target_ids.clone(),
                    exclude_target_ids: args.exclude_target_ids.clone(),
                    max_tasks: args.max_tasks,
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply, || {
                Ok(format!("created_tasks={}\n", reply.created_tasks))
            })?;
            Ok(None)
        }
    }
}

async fn run_placement_command(
    command: &PlacementCommand,
    cli: &Cli,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<(), DynError> {
    match command {
        PlacementCommand::Summary(args) => {
            let tasks = list_placement_tasks(context, timeout, args).await?;
            let summary = summarize_placement_tasks(&tasks);
            print_structured(cli.format, &summary, || {
                Ok(render_placement_summary(&summary))
            })?;
        }
        PlacementCommand::List(args) => {
            let tasks = list_placement_tasks(context, timeout, args).await?;
            print_structured(cli.format, &tasks, || Ok(render_placement_tasks(&tasks)))?;
        }
        PlacementCommand::Show(args) => {
            let mut kms = connect_kms(context, timeout).await?;
            let reply = rpc_timeout(
                timeout,
                kms.get_placement_task(Request::new(proto::GetPlacementTaskRequest {
                    task_id: args.task_id.clone(),
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply, || {
                Ok(render_placement_task_show(&reply))
            })?;
        }
        PlacementCommand::Watch(args) => {
            let interval = cli.watch.unwrap_or(2);
            loop {
                let tasks = list_placement_tasks(context, timeout, args).await?;
                let summary = summarize_placement_tasks(&tasks);
                print_structured(cli.format, &summary, || {
                    Ok(render_placement_summary(&summary))
                })?;
                tokio::time::sleep(Duration::from_secs(interval.max(1))).await;
            }
        }
        PlacementCommand::Wait(args) => {
            let report = wait_for_placement_quiescence(context, timeout, args).await?;
            print_structured(cli.format, &report, || Ok(render_placement_counts(&report)))?;
        }
    }
    Ok(())
}

async fn run_intent_command(
    command: &IntentCommand,
    cli: &Cli,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<(), DynError> {
    match command {
        IntentCommand::Summary(args) => {
            let intents = list_write_intents(context, timeout, args).await?;
            let report = summarize_intents(&intents);
            print_structured(cli.format, &report, || Ok(render_intent_counts(&report)))?;
        }
        IntentCommand::List(args) => {
            let intents = list_write_intents(context, timeout, args).await?;
            print_structured(cli.format, &intents, || Ok(render_write_intents(&intents)))?;
        }
        IntentCommand::Show(args) => {
            let mut kms = connect_kms(context, timeout).await?;
            let reply = rpc_timeout(
                timeout,
                kms.get_write_intent(Request::new(proto::GetWriteIntentRequest {
                    intent_id: args.intent_id.clone(),
                })),
            )
            .await?
            .into_inner();
            let intent = reply
                .intent
                .ok_or_else(|| boxed_error("KMS returned no intent"))?;
            print_structured(cli.format, &intent, || {
                Ok(render_write_intent_show(&intent))
            })?;
        }
        IntentCommand::Wait(args) => {
            let report = wait_for_clean_intents(context, timeout, args).await?;
            print_structured(cli.format, &report, || Ok(render_intent_counts(&report)))?;
        }
    }
    Ok(())
}

async fn run_allocator_command(
    command: &AllocatorCommand,
    cli: &Cli,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<(), DynError> {
    let mut kas = connect_kas(context, timeout).await?;
    match command {
        AllocatorCommand::Reservations(args) => {
            let state = args
                .state
                .as_ref()
                .map(|value| reservation_state_from_arg(value.clone()) as i32)
                .unwrap_or_default();
            let reply = rpc_timeout(
                timeout,
                kas.list_reservations(Request::new(proto::ListReservationsRequest {
                    state,
                    target_id: args.target_id.clone().unwrap_or_default(),
                    limit: args.limit,
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply.reservations, || {
                Ok(render_reservations(&reply.reservations))
            })?;
        }
        AllocatorCommand::ReservationShow(args) => {
            let reply = rpc_timeout(
                timeout,
                kas.get_reservation(Request::new(proto::GetReservationRequest {
                    reservation_id: args.reservation_id.clone(),
                })),
            )
            .await?
            .into_inner();
            let reservation = reply
                .reservation
                .ok_or_else(|| boxed_error("KAS returned no reservation"))?;
            print_structured(cli.format, &reservation, || {
                Ok(render_reservations(&[reservation.clone()]))
            })?;
        }
        AllocatorCommand::ReserveBatch(args) => {
            require_confirm(cli)?;
            let reply = rpc_timeout(
                timeout,
                kas.reserve_stripe_batch(Request::new(proto::ReserveStripeBatchRequest {
                    batch_size: args.batch_size,
                    fragment_count: args.fragment_count,
                    failure_domain: failure_domain_from_arg(args.failure_domain.clone()) as i32,
                    excluded_target_ids: args.excluded_target_ids.clone(),
                    reservation_ttl_ms: args.reservation_ttl_ms,
                    allocation_shard_id: String::new(),
                })),
            )
            .await?
            .into_inner();
            print_structured(cli.format, &reply.reservations, || {
                Ok(render_reservations(&reply.reservations))
            })?;
        }
    }
    Ok(())
}

async fn run_diag_command(
    command: &DiagCommand,
    cli: &Cli,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<(), DynError> {
    match command {
        DiagCommand::RuntimeList => {
            let runtimes = collect_runtime_list(context)?;
            print_structured(cli.format, &runtimes, || Ok(render_runtime_list(&runtimes)))?;
        }
        DiagCommand::RuntimeShow(args) => {
            let report = collect_runtime_show(context, &args.service, args.runtime_dir.as_deref())?;
            print_structured(cli.format, &report, || Ok(render_runtime_show(&report)))?;
        }
        DiagCommand::LastErrors => {
            let report = collect_last_errors(context)?;
            print_structured(cli.format, &report, || Ok(render_last_errors(&report)))?;
        }
        DiagCommand::TargetHttpInfo(args) => {
            let endpoints = resolve_target_endpoints(context, timeout, &args.endpoints).await?;
            let mut reports = Vec::new();
            for endpoint in endpoints {
                reports.push(TargetHttpReport {
                    endpoint: endpoint.clone(),
                    value: fetch_kst_info(&endpoint).await?,
                });
            }
            print_structured(cli.format, &reports, || {
                Ok(render_target_http_info(&reports))
            })?;
        }
        DiagCommand::TargetHttpStats(args) => {
            let endpoints = resolve_target_endpoints(context, timeout, &args.endpoints).await?;
            let mut reports = Vec::new();
            for endpoint in endpoints {
                reports.push(TargetHttpReport {
                    endpoint: endpoint.clone(),
                    value: fetch_kst_stats(&endpoint).await?,
                });
            }
            print_structured(cli.format, &reports, || {
                Ok(render_target_http_stats(&reports))
            })?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize)]
struct ContextShowReport {
    name: String,
    current: bool,
    context: ContextConfig,
}

#[derive(Clone, Debug, Serialize)]
struct ContextValidateReport {
    name: String,
    kms_reachable: bool,
    kas_reachable: bool,
    kms_runtime_root_exists: bool,
    kas_runtime_root_exists: bool,
    krs_runtime_root_exists: bool,
    kst_runtime_root_exists: bool,
}

#[derive(Clone, Debug, Serialize)]
struct NamespaceShowReport {
    namespace: proto::NamespaceRecord,
    shard_map: Option<proto::ShardMapEntry>,
}

#[derive(Clone, Debug, Serialize)]
struct BucketShowReport {
    bucket: proto::BucketRecord,
    ec_profile: Option<proto::EcProfile>,
}

#[derive(Clone, Debug, Serialize)]
struct ObjectHeadReport {
    bucket_id: String,
    key: String,
    version_id: String,
    logical_length_bytes: u64,
    ec_profile_id: String,
    stripe_count: u32,
}

#[derive(Clone, Debug, Serialize)]
struct ObjectLocateReport {
    version_id: String,
    fragments: Vec<ObjectFragmentLocation>,
}

#[derive(Clone, Debug, Serialize)]
struct ObjectFragmentLocation {
    stripe_index: u32,
    fragment_index: u32,
    target_id: String,
    endpoint: String,
    granule_index: u64,
    generation: u32,
}

#[derive(Clone, Debug, Serialize)]
struct ClusterTopologyReport {
    context: String,
    kms_endpoint: String,
    kas_endpoint: String,
    runtime_roots: BTreeMap<String, Option<String>>,
    namespace_count: usize,
    bucket_count: usize,
    target_count: usize,
}

async fn collect_cluster_status(
    context_name: &str,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<ClusterStatusReport, DynError> {
    let kms_status = collect_service_status(context, timeout, &ServiceKind::Kms).await?;
    let kas_status = collect_service_status(context, timeout, &ServiceKind::Kas).await?;
    let krs_status = collect_service_status(context, timeout, &ServiceKind::Krs).await?;
    let mut kas = connect_kas(context, timeout).await?;
    let targets = rpc_timeout(
        timeout,
        kas.list_targets(Request::new(proto::ListTargetsRequest {})),
    )
    .await?
    .into_inner()
    .targets;
    let target_counts = summarize_targets(&targets);
    let placement_tasks = list_placement_tasks(
        context,
        timeout,
        &PlacementListArgs {
            source_target_id: None,
            object_version_ref: None,
            task_kind: None,
            state: None,
            limit: 10_000,
        },
    )
    .await?;
    let placement = summarize_placement_counts(&placement_tasks);
    let intents = list_write_intents(
        context,
        timeout,
        &IntentListArgs {
            bucket_id: None,
            state: None,
            limit: 10_000,
        },
    )
    .await?;
    let intent_counts = summarize_intents(&intents);

    let mut health = HealthState::Healthy;
    let mut reasons = Vec::new();
    for service in [&kms_status, &kas_status, &krs_status] {
        health = health.max(service.health);
        if service.health != HealthState::Healthy {
            reasons.push(format!(
                "{} is {}",
                service.service,
                service.health.as_str()
            ));
        }
    }
    if target_counts.unhealthy > 0 || target_counts.unhealthy_heartbeat > 0 {
        health = health.max(HealthState::Unhealthy);
        reasons.push("one or more targets are unhealthy".to_string());
    } else if target_counts.draining > 0 || target_counts.retired > 0 {
        health = health.max(HealthState::Degraded);
        reasons.push("one or more targets are draining or retired".to_string());
    }
    if placement.pending_rebuild > 0 || placement.failed > 0 {
        health = health.max(HealthState::Degraded);
        reasons.push("placement backlog exists".to_string());
    }
    if intent_counts.pending > 0 || intent_counts.reserved > 0 {
        health = health.max(HealthState::Degraded);
        reasons.push("write intents remain in-flight".to_string());
    }
    if reasons.is_empty() {
        reasons.push(format!("cluster {} looks healthy", context_name));
    }

    Ok(ClusterStatusReport {
        health,
        reasons,
        services: vec![kms_status, kas_status, krs_status],
        targets: target_counts,
        placement,
        intents: intent_counts,
    })
}

async fn collect_cluster_topology(
    context_name: &str,
    context: &ContextConfig,
    timeout: Duration,
) -> Result<ClusterTopologyReport, DynError> {
    let mut kms = connect_kms(context, timeout).await?;
    let mut kas = connect_kas(context, timeout).await?;
    let namespaces = rpc_timeout(
        timeout,
        kms.list_namespaces(Request::new(proto::ListNamespacesRequest {
            tenant_id: String::new(),
        })),
    )
    .await?
    .into_inner()
    .namespaces;
    let buckets = rpc_timeout(
        timeout,
        kms.list_buckets(Request::new(proto::ListBucketsRequest {
            namespace_id: String::new(),
            parent_entry_id: String::new(),
        })),
    )
    .await?
    .into_inner()
    .buckets;
    let targets = rpc_timeout(
        timeout,
        kas.list_targets(Request::new(proto::ListTargetsRequest {})),
    )
    .await?
    .into_inner()
    .targets;
    Ok(ClusterTopologyReport {
        context: context_name.to_string(),
        kms_endpoint: context.kms_endpoint.clone(),
        kas_endpoint: context.kas_endpoint.clone(),
        runtime_roots: BTreeMap::from([
            ("kms".to_string(), context.kms_runtime_root.clone()),
            ("kas".to_string(), context.kas_runtime_root.clone()),
            ("krs".to_string(), context.krs_runtime_root.clone()),
            ("kst".to_string(), context.kst_runtime_root.clone()),
        ]),
        namespace_count: namespaces.len(),
        bucket_count: buckets.len(),
        target_count: targets.len(),
    })
}

async fn collect_cluster_events(
    context: &ContextConfig,
    timeout: Duration,
    args: &ClusterEventsArgs,
) -> Result<Vec<proto::MetadataEvent>, DynError> {
    let mut kms = connect_kms(context, timeout).await?;
    let reply = rpc_timeout(
        timeout,
        kms.list_metadata_events(Request::new(proto::ListMetadataEventsRequest {
            namespace_id: args.namespace_id.clone(),
            entry_id: args.entry_id.clone().unwrap_or_default(),
            parent_entry_id: args.parent_entry_id.clone().unwrap_or_default(),
            start_revision: args.start_revision,
            limit: args.limit,
        })),
    )
    .await?
    .into_inner();
    Ok(reply.events)
}

async fn collect_service_status(
    context: &ContextConfig,
    timeout: Duration,
    service: &ServiceKind,
) -> Result<ServiceStatusReport, DynError> {
    match service {
        ServiceKind::Kms => {
            let runtime = read_latest_runtime_status(context.kms_runtime_root.as_deref(), "kms");
            let reachable = connect_kms(context, timeout).await.is_ok();
            let instances =
                collect_registered_instances(context, timeout, proto_service_kind(service)).await?;
            let mut details = BTreeMap::new();
            if let Some(status) = &runtime {
                details.insert("pid".to_string(), status.status.pid.to_string());
                details.insert("uptime_ms".to_string(), status.status.uptime_ms.to_string());
            }
            Ok(build_service_status(
                "kms",
                Some(context.kms_endpoint.clone()),
                runtime,
                reachable,
                details,
                instances,
            ))
        }
        ServiceKind::Kas => {
            let runtime = read_latest_runtime_status(context.kas_runtime_root.as_deref(), "kas");
            let reachable = connect_kas(context, timeout).await.is_ok();
            let instances =
                collect_registered_instances(context, timeout, proto_service_kind(service)).await?;
            let mut details = BTreeMap::new();
            if let Some(status) = &runtime {
                details.insert("pid".to_string(), status.status.pid.to_string());
                details.insert("uptime_ms".to_string(), status.status.uptime_ms.to_string());
            }
            Ok(build_service_status(
                "kas",
                Some(context.kas_endpoint.clone()),
                runtime,
                reachable,
                details,
                instances,
            ))
        }
        ServiceKind::Krs => {
            let runtime = read_latest_runtime_status(context.krs_runtime_root.as_deref(), "krs");
            let reachable = runtime.is_some();
            let instances =
                collect_registered_instances(context, timeout, proto_service_kind(service)).await?;
            let mut details = BTreeMap::new();
            if let Some(status) = &runtime {
                details.insert("pid".to_string(), status.status.pid.to_string());
                details.insert("uptime_ms".to_string(), status.status.uptime_ms.to_string());
                if let Some(active) = status
                    .status
                    .extra
                    .get("active_task")
                    .and_then(toml_value_to_string)
                {
                    details.insert("active_task".to_string(), active);
                }
            }
            Ok(build_service_status(
                "krs", None, runtime, reachable, details, instances,
            ))
        }
        ServiceKind::Kst => {
            let endpoints = resolve_target_endpoints(context, timeout, &[]).await?;
            let mut successes = 0usize;
            let mut rebuild_required = 0usize;
            let mut details = BTreeMap::new();
            for endpoint in &endpoints {
                if let Ok(info) = fetch_kst_info(endpoint).await {
                    successes += 1;
                    if info.rebuild_required {
                        rebuild_required += 1;
                    }
                }
            }
            details.insert("targets_seen".to_string(), endpoints.len().to_string());
            details.insert("targets_reachable".to_string(), successes.to_string());
            details.insert(
                "rebuild_required_targets".to_string(),
                rebuild_required.to_string(),
            );
            let health = if endpoints.is_empty() {
                HealthState::Unknown
            } else if successes == 0 {
                HealthState::Unhealthy
            } else if rebuild_required > 0 || successes < endpoints.len() {
                HealthState::Degraded
            } else {
                HealthState::Healthy
            };
            Ok(ServiceStatusReport {
                service: "kst".to_string(),
                health,
                reachable: successes > 0,
                endpoint: None,
                runtime_dir: None,
                last_error: (successes < endpoints.len())
                    .then_some("one or more targets did not answer /v1/info".to_string()),
                details,
                instances: Vec::new(),
            })
        }
    }
}

fn build_service_status(
    service: &str,
    endpoint: Option<String>,
    runtime: Option<RuntimeLocated>,
    reachable: bool,
    details: BTreeMap<String, String>,
    instances: Vec<ServiceInstanceStatus>,
) -> ServiceStatusReport {
    let runtime_health = runtime
        .as_ref()
        .map(|located| HealthState::from_text(&located.status.health))
        .unwrap_or(HealthState::Unknown);
    let mut details = details;
    details.insert(
        "registered_instances".to_string(),
        instances.len().to_string(),
    );
    let stale_instances = instances.iter().filter(|instance| instance.stale).count();
    if stale_instances > 0 {
        details.insert("stale_instances".to_string(), stale_instances.to_string());
    }
    let version_set = instances
        .iter()
        .map(|instance| {
            format!(
                "{}-{}@{}",
                instance.version, instance.release, instance.git_sha
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    if !version_set.is_empty() {
        details.insert(
            "versions".to_string(),
            version_set.iter().cloned().collect::<Vec<_>>().join(","),
        );
    }
    let mut health = if runtime.is_some() {
        if !reachable && endpoint.is_some() {
            HealthState::Unhealthy
        } else {
            runtime_health
        }
    } else if reachable {
        HealthState::Healthy
    } else if endpoint.is_some() {
        HealthState::Unhealthy
    } else {
        HealthState::Unknown
    };
    let mut last_error = runtime
        .as_ref()
        .and_then(|located| located.status.last_error.clone());
    if instances.is_empty() && matches!(service, "kms" | "kas" | "krs") {
        health = health.max(HealthState::Degraded);
        last_error.get_or_insert_with(|| "service has no registry heartbeat".to_string());
    }
    if stale_instances > 0 {
        health = health.max(HealthState::Degraded);
        last_error.get_or_insert_with(|| "one or more registered instances are stale".to_string());
    }
    if version_set.len() > 1 {
        health = health.max(HealthState::Degraded);
        last_error
            .get_or_insert_with(|| "registered instances disagree on build version".to_string());
        details.insert("version_drift".to_string(), "true".to_string());
    }
    ServiceStatusReport {
        service: service.to_string(),
        health,
        reachable,
        endpoint,
        runtime_dir: runtime
            .as_ref()
            .map(|located| located.runtime_dir.display().to_string()),
        last_error,
        details,
        instances,
    }
}

async fn collect_service_stats(
    context: &ContextConfig,
    service: &ServiceKind,
) -> Result<RuntimeShowReport, DynError> {
    collect_runtime_show(context, service, None)
}

async fn collect_target_reports(
    context: &ContextConfig,
    timeout: Duration,
) -> Result<Vec<TargetReport>, DynError> {
    let mut kas = connect_kas(context, timeout).await?;
    let targets = rpc_timeout(
        timeout,
        kas.list_targets(Request::new(proto::ListTargetsRequest {})),
    )
    .await?
    .into_inner()
    .targets;
    let mut reports = Vec::with_capacity(targets.len());
    for target in targets {
        reports.push(collect_target_report_for_record(context, timeout, target).await?);
    }
    Ok(reports)
}

async fn collect_target_report(
    context: &ContextConfig,
    timeout: Duration,
    target_id: &str,
) -> Result<TargetReport, DynError> {
    let mut kas = connect_kas(context, timeout).await?;
    let target = rpc_timeout(
        timeout,
        kas.list_targets(Request::new(proto::ListTargetsRequest {})),
    )
    .await?
    .into_inner()
    .targets
    .into_iter()
    .find(|target| target.target_id == target_id)
    .ok_or_else(|| boxed_error(format!("unknown target {}", target_id)))?;
    collect_target_report_for_record(context, timeout, target).await
}

async fn collect_target_report_for_record(
    context: &ContextConfig,
    timeout: Duration,
    target: proto::TargetRecord,
) -> Result<TargetReport, DynError> {
    let mut kms = connect_kms(context, timeout).await?;
    let placement = match rpc_timeout(
        timeout,
        kms.get_target_placement_status(Request::new(proto::GetTargetPlacementStatusRequest {
            target_id: target.target_id.clone(),
        })),
    )
    .await
    {
        Ok(reply) => reply
            .into_inner()
            .status
            .ok_or_else(|| boxed_error("KMS returned no target placement status"))?,
        Err(err) => return Err(err),
    };
    let mut health = HealthState::Healthy;
    let mut reasons = Vec::new();
    if !target.healthy {
        health = HealthState::Unhealthy;
        reasons.push("allocator heartbeat marked target unhealthy".to_string());
    }
    match proto::TargetLifecycleState::try_from(target.lifecycle_state)
        .unwrap_or(proto::TargetLifecycleState::Unspecified)
    {
        proto::TargetLifecycleState::Active => {}
        proto::TargetLifecycleState::Draining => {
            health = health.max(HealthState::Degraded);
            reasons.push("target is draining".to_string());
        }
        proto::TargetLifecycleState::Unhealthy => {
            health = HealthState::Unhealthy;
            reasons.push("target lifecycle is unhealthy".to_string());
        }
        proto::TargetLifecycleState::Retired => {
            health = health.max(HealthState::Degraded);
            reasons.push("target is retired".to_string());
        }
        proto::TargetLifecycleState::Unspecified => {
            health = health.max(HealthState::Unknown);
            reasons.push("target lifecycle is unspecified".to_string());
        }
    }
    if placement.pending_rebuild_tasks > 0 || placement.failed_tasks > 0 {
        health = health.max(HealthState::Degraded);
        reasons.push("placement work is pending for this target".to_string());
    }
    if reasons.is_empty() {
        reasons.push("target looks healthy".to_string());
    }
    Ok(TargetReport {
        target,
        placement,
        health,
        reasons,
    })
}

async fn list_placement_tasks(
    context: &ContextConfig,
    timeout: Duration,
    args: &PlacementListArgs,
) -> Result<Vec<proto::PlacementTaskSummary>, DynError> {
    let mut kms = connect_kms(context, timeout).await?;
    let reply = rpc_timeout(
        timeout,
        kms.list_placement_tasks(Request::new(proto::ListPlacementTasksRequest {
            source_target_id: args.source_target_id.clone().unwrap_or_default(),
            object_version_ref: args.object_version_ref.clone().unwrap_or_default(),
            task_kind: args
                .task_kind
                .as_ref()
                .map(|value| placement_task_kind_from_arg(value.clone()) as i32)
                .unwrap_or_default(),
            state: args
                .state
                .as_ref()
                .map(|value| placement_task_state_from_arg(value.clone()) as i32)
                .unwrap_or_default(),
            limit: args.limit,
        })),
    )
    .await?
    .into_inner();
    Ok(reply.tasks)
}

async fn wait_for_placement_quiescence(
    context: &ContextConfig,
    timeout: Duration,
    args: &PlacementWaitArgs,
) -> Result<PlacementCountReport, DynError> {
    let poll = Duration::from_secs(args.poll_secs.max(1));
    let deadline = Instant::now() + Duration::from_secs(args.timeout_secs.max(1));
    let settle_needed = args.settle_polls.max(1);
    let mut settled = 0_u32;
    loop {
        let tasks = list_placement_tasks(context, timeout, &args.filters).await?;
        let counts = summarize_placement_counts(&tasks);
        let quiesced = counts.pending_rebuild == 0
            && counts.pending_rebalance == 0
            && counts.pending_evacuate == 0
            && counts.leased == 0
            && (args.allow_failed || counts.failed == 0);
        if quiesced {
            settled = settled.saturating_add(1);
            if settled >= settle_needed {
                return Ok(counts);
            }
        } else {
            settled = 0;
        }
        if Instant::now() >= deadline {
            return Err(boxed_error(format!(
                "placement work did not quiesce within {}s (pending_rebuild={}, pending_rebalance={}, pending_evacuate={}, leased={}, failed={})",
                args.timeout_secs.max(1),
                counts.pending_rebuild,
                counts.pending_rebalance,
                counts.pending_evacuate,
                counts.leased,
                counts.failed,
            )));
        }
        tokio::time::sleep(poll).await;
    }
}

async fn list_write_intents(
    context: &ContextConfig,
    timeout: Duration,
    args: &IntentListArgs,
) -> Result<Vec<proto::WriteIntentSummary>, DynError> {
    let mut kms = connect_kms(context, timeout).await?;
    let reply = rpc_timeout(
        timeout,
        kms.list_write_intents(Request::new(proto::ListWriteIntentsRequest {
            bucket_id: args.bucket_id.clone().unwrap_or_default(),
            state: args
                .state
                .as_ref()
                .map(|value| write_intent_state_from_arg(value.clone()) as i32)
                .unwrap_or_default(),
            limit: args.limit,
        })),
    )
    .await?
    .into_inner();
    Ok(reply.intents)
}

async fn wait_for_clean_intents(
    context: &ContextConfig,
    timeout: Duration,
    args: &IntentWaitArgs,
) -> Result<IntentCountReport, DynError> {
    let poll = Duration::from_secs(args.poll_secs.max(1));
    let deadline = Instant::now() + Duration::from_secs(args.timeout_secs.max(1));
    let settle_needed = args.settle_polls.max(1);
    let mut settled = 0_u32;
    loop {
        let intents = list_write_intents(context, timeout, &args.filters).await?;
        let counts = summarize_intents(&intents);
        let clean = counts.pending == 0 && counts.reserved == 0;
        if clean {
            settled = settled.saturating_add(1);
            if settled >= settle_needed {
                return Ok(counts);
            }
        } else {
            settled = 0;
        }
        if Instant::now() >= deadline {
            return Err(boxed_error(format!(
                "write intents did not quiesce within {}s (pending={}, reserved={}, committed={}, aborted={}, expired={})",
                args.timeout_secs.max(1),
                counts.pending,
                counts.reserved,
                counts.committed,
                counts.aborted,
                counts.expired,
            )));
        }
        tokio::time::sleep(poll).await;
    }
}

fn summarize_placement_tasks(tasks: &[proto::PlacementTaskSummary]) -> PlacementSummaryReport {
    let mut counts: BTreeMap<(String, String), usize> = BTreeMap::new();
    for task in tasks {
        *counts
            .entry((
                placement_task_kind_name(task.task_kind).to_string(),
                placement_task_state_name(task.state).to_string(),
            ))
            .or_default() += 1;
    }
    PlacementSummaryReport {
        total: tasks.len(),
        by_kind_state: counts
            .into_iter()
            .map(|((task_kind, state), count)| PlacementKindStateCount {
                task_kind,
                state,
                count,
            })
            .collect(),
    }
}

fn summarize_placement_counts(tasks: &[proto::PlacementTaskSummary]) -> PlacementCountReport {
    let mut report = PlacementCountReport::default();
    report.total = tasks.len();
    for task in tasks {
        let state = proto::PlacementTaskState::try_from(task.state)
            .unwrap_or(proto::PlacementTaskState::Unspecified);
        let kind = proto::PlacementTaskKind::try_from(task.task_kind)
            .unwrap_or(proto::PlacementTaskKind::Unspecified);
        match state {
            proto::PlacementTaskState::Pending => match kind {
                proto::PlacementTaskKind::Rebuild => report.pending_rebuild += 1,
                proto::PlacementTaskKind::Rebalance => report.pending_rebalance += 1,
                proto::PlacementTaskKind::Evacuate => report.pending_evacuate += 1,
                proto::PlacementTaskKind::Unspecified => {}
            },
            proto::PlacementTaskState::Leased => report.leased += 1,
            proto::PlacementTaskState::Failed => report.failed += 1,
            proto::PlacementTaskState::Completed | proto::PlacementTaskState::Unspecified => {}
        }
    }
    report
}

fn summarize_targets(targets: &[proto::TargetRecord]) -> TargetCountReport {
    let mut report = TargetCountReport::default();
    report.total = targets.len();
    for target in targets {
        match proto::TargetLifecycleState::try_from(target.lifecycle_state)
            .unwrap_or(proto::TargetLifecycleState::Unspecified)
        {
            proto::TargetLifecycleState::Active => report.active += 1,
            proto::TargetLifecycleState::Draining => report.draining += 1,
            proto::TargetLifecycleState::Unhealthy => report.unhealthy += 1,
            proto::TargetLifecycleState::Retired => report.retired += 1,
            proto::TargetLifecycleState::Unspecified => {}
        }
        if !target.healthy {
            report.unhealthy_heartbeat += 1;
        }
    }
    report
}

fn summarize_intents(intents: &[proto::WriteIntentSummary]) -> IntentCountReport {
    let mut report = IntentCountReport::default();
    report.total = intents.len();
    for intent in intents {
        match proto::WriteIntentState::try_from(intent.state)
            .unwrap_or(proto::WriteIntentState::Unspecified)
        {
            proto::WriteIntentState::Pending => report.pending += 1,
            proto::WriteIntentState::Reserved => report.reserved += 1,
            proto::WriteIntentState::Committed => report.committed += 1,
            proto::WriteIntentState::Aborted => report.aborted += 1,
            proto::WriteIntentState::Expired => report.expired += 1,
            proto::WriteIntentState::Unspecified => {}
        }
    }
    report
}

fn build_object_locate_report(manifest: &proto::ObjectVersionManifest) -> ObjectLocateReport {
    let mut fragments = Vec::new();
    for (stripe_index, stripe) in manifest.stripes.iter().enumerate() {
        for fragment in &stripe.fragments {
            fragments.push(ObjectFragmentLocation {
                stripe_index: stripe_index as u32,
                fragment_index: fragment.fragment_index,
                target_id: fragment.target_id.clone(),
                endpoint: fragment.endpoint.clone(),
                granule_index: fragment.granule_index,
                generation: fragment.generation,
            });
        }
    }
    ObjectLocateReport {
        version_id: manifest.version_id.clone(),
        fragments,
    }
}

fn build_namespace_tree<'a>(
    kms: &'a mut KmsClient<Channel>,
    timeout: Duration,
    namespace_id: &'a str,
    parent_entry_id: &'a str,
) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<TreeNode>, DynError>> + Send + 'a>> {
    Box::pin(async move {
        let entries = list_children_all(kms, timeout, namespace_id, parent_entry_id, 256).await?;
        let mut nodes = Vec::new();
        for entry in entries {
            let children =
                build_namespace_tree(kms, timeout, namespace_id, &entry.entry_id).await?;
            nodes.push(TreeNode { entry, children });
        }
        Ok(nodes)
    })
}

#[derive(Clone, Debug, Serialize)]
struct TreeNode {
    entry: proto::NamespaceDomainEntry,
    children: Vec<TreeNode>,
}

async fn list_children_all(
    kms: &mut KmsClient<Channel>,
    timeout: Duration,
    namespace_id: &str,
    parent_entry_id: &str,
    limit: u32,
) -> Result<Vec<proto::NamespaceDomainEntry>, DynError> {
    let mut cursor = String::new();
    let mut entries = Vec::new();
    loop {
        let reply = rpc_timeout(
            timeout,
            kms.list_children(Request::new(proto::ListChildrenRequest {
                namespace_id: namespace_id.to_string(),
                parent_entry_id: parent_entry_id.to_string(),
                cursor: cursor.clone(),
                limit,
            })),
        )
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

fn resolve_context(name: Option<&str>) -> Result<(String, ContextConfig), DynError> {
    let file = load_context_file()?;
    let selected = name.unwrap_or(file.current_context.as_str()).to_string();
    let context = file
        .contexts
        .get(&selected)
        .cloned()
        .ok_or_else(|| boxed_error(format!("unknown context {}", selected)))?;
    Ok((selected, context))
}

fn load_context_file() -> Result<ContextFile, DynError> {
    let path = context_file_path()?;
    if !path.exists() {
        return Ok(ContextFile::default());
    }
    let raw = fs::read_to_string(path)?;
    let file: ContextFile = toml::from_str(&raw)?;
    Ok(file)
}

fn save_context_file(file: &ContextFile) -> Result<(), DynError> {
    let path = context_file_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, toml::to_string_pretty(file)?)?;
    Ok(())
}

fn context_file_path() -> Result<PathBuf, DynError> {
    let base = dirs::config_dir()
        .or_else(dirs::home_dir)
        .ok_or_else(|| boxed_error("could not determine config directory"))?;
    Ok(base.join("keinctl").join("contexts.toml"))
}

fn context_root_exists(root: Option<&str>) -> bool {
    root.map(Path::new).is_some_and(Path::exists)
}

async fn connect_kms(
    context: &ContextConfig,
    timeout: Duration,
) -> Result<KmsClient<Channel>, DynError> {
    const GRPC_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
    let endpoint = Endpoint::from_shared(context.kms_endpoint.clone())?;
    let channel = tokio::time::timeout(timeout, endpoint.connect()).await??;
    Ok(KmsClient::new(channel)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES))
}

async fn connect_kas(
    context: &ContextConfig,
    timeout: Duration,
) -> Result<KasClient<Channel>, DynError> {
    const GRPC_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
    let endpoint = Endpoint::from_shared(context.kas_endpoint.clone())?;
    let channel = tokio::time::timeout(timeout, endpoint.connect()).await??;
    Ok(KasClient::new(channel)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES))
}

async fn rpc_timeout<F, T>(timeout: Duration, fut: F) -> Result<T, DynError>
where
    F: std::future::Future<Output = Result<T, tonic::Status>>,
{
    Ok(tokio::time::timeout(timeout, fut).await??)
}

async fn collect_registered_instances(
    context: &ContextConfig,
    timeout: Duration,
    service_kind: proto::ServiceKind,
) -> Result<Vec<ServiceInstanceStatus>, DynError> {
    if matches!(service_kind, proto::ServiceKind::Unspecified) {
        return Ok(Vec::new());
    }
    let mut kas = match connect_kas(context, timeout).await {
        Ok(kas) => kas,
        Err(_) => return Ok(Vec::new()),
    };
    let reply = match rpc_timeout(
        timeout,
        kas.list_service_instances(Request::new(proto::ListServiceInstancesRequest {
            service_kind: service_kind as i32,
            node_id: String::new(),
            limit: 1_024,
        })),
    )
    .await
    {
        Ok(reply) => reply.into_inner(),
        Err(_) => return Ok(Vec::new()),
    };
    let now = now_unix_ms();
    reply
        .instances
        .into_iter()
        .map(|instance| service_instance_status_from_proto(instance, now))
        .collect()
}

async fn collect_service_list_rows(
    context: &ContextConfig,
    timeout: Duration,
) -> Result<Vec<BTreeMap<String, String>>, DynError> {
    let mut kas = match connect_kas(context, timeout).await {
        Ok(kas) => kas,
        Err(_) => {
            return Ok([
                ("kms", context.kms_endpoint.clone()),
                ("kas", context.kas_endpoint.clone()),
                ("krs", context.krs_runtime_root.clone().unwrap_or_default()),
                ("kst", context.kst_runtime_root.clone().unwrap_or_default()),
            ]
            .into_iter()
            .map(|(service, endpoint)| {
                BTreeMap::from([
                    ("service".to_string(), service.to_string()),
                    ("node_id".to_string(), String::new()),
                    ("endpoint".to_string(), endpoint),
                    ("version".to_string(), String::new()),
                    ("release".to_string(), String::new()),
                    ("git_sha".to_string(), String::new()),
                    ("stale".to_string(), String::new()),
                ])
            })
            .collect())
        }
    };
    let reply = match rpc_timeout(
        timeout,
        kas.list_service_instances(Request::new(proto::ListServiceInstancesRequest {
            service_kind: proto::ServiceKind::Unspecified as i32,
            node_id: String::new(),
            limit: 4_096,
        })),
    )
    .await
    {
        Ok(reply) => reply.into_inner(),
        Err(_) => {
            return Ok([
                ("kms", context.kms_endpoint.clone()),
                ("kas", context.kas_endpoint.clone()),
                ("krs", context.krs_runtime_root.clone().unwrap_or_default()),
                ("kst", context.kst_runtime_root.clone().unwrap_or_default()),
            ]
            .into_iter()
            .map(|(service, endpoint)| {
                BTreeMap::from([
                    ("service".to_string(), service.to_string()),
                    ("node_id".to_string(), String::new()),
                    ("endpoint".to_string(), endpoint),
                    ("version".to_string(), String::new()),
                    ("release".to_string(), String::new()),
                    ("git_sha".to_string(), String::new()),
                    ("stale".to_string(), String::new()),
                ])
            })
            .collect())
        }
    };
    let now = now_unix_ms();
    Ok(reply
        .instances
        .into_iter()
        .map(|instance| service_instance_status_from_proto(instance, now))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|instance| {
            BTreeMap::from([
                ("service".to_string(), instance.service),
                ("node_id".to_string(), instance.node_id),
                ("endpoint".to_string(), instance.endpoint),
                ("version".to_string(), instance.version),
                ("release".to_string(), instance.release.to_string()),
                ("git_sha".to_string(), instance.git_sha),
                ("stale".to_string(), instance.stale.to_string()),
            ])
        })
        .collect())
}

fn proto_service_kind(service: &ServiceKind) -> proto::ServiceKind {
    match service {
        ServiceKind::Kms => proto::ServiceKind::Kms,
        ServiceKind::Kas => proto::ServiceKind::Kas,
        ServiceKind::Krs => proto::ServiceKind::Krs,
        ServiceKind::Kst => proto::ServiceKind::Kst,
    }
}

fn service_instance_status_from_proto(
    instance: proto::ServiceInstanceRecord,
    now_unix_ms: u64,
) -> Result<ServiceInstanceStatus, DynError> {
    let build = instance.build.ok_or_else(|| {
        boxed_error(format!(
            "service instance {} has no build info",
            instance.instance_id
        ))
    })?;
    let service_kind = proto::ServiceKind::try_from(instance.service_kind)
        .unwrap_or(proto::ServiceKind::Unspecified);
    let heartbeat_age_ms = now_unix_ms.saturating_sub(instance.heartbeat_at_unix_ms);
    let stale = service_instance_is_stale(heartbeat_age_ms, instance.heartbeat_interval_ms);
    Ok(ServiceInstanceStatus {
        instance_id: instance.instance_id,
        service: service_kind_name(service_kind).to_string(),
        node_id: instance.node_id,
        endpoint: instance.endpoint,
        package_name: instance.package_name,
        version: build.version,
        release: build.release,
        git_sha: build.git_sha,
        git_dirty: build.git_dirty,
        instance_label: instance.instance_label,
        config_hash: instance.config_hash,
        heartbeat_age_ms,
        heartbeat_interval_ms: instance.heartbeat_interval_ms,
        stale,
    })
}

fn service_instance_is_stale(heartbeat_age_ms: u64, heartbeat_interval_ms: u64) -> bool {
    let grace = heartbeat_interval_ms.max(5_000).saturating_mul(3);
    heartbeat_age_ms > grace
}

#[derive(Clone, Debug)]
struct RuntimeLocated {
    runtime_dir: PathBuf,
    status: RuntimeStatusDoc,
}

fn read_latest_runtime_status(root: Option<&str>, prefix: &str) -> Option<RuntimeLocated> {
    let runtime_dir = discover_latest_runtime_dir(root, prefix)?;
    let status = read_runtime_status(&runtime_dir)?;
    Some(RuntimeLocated {
        runtime_dir,
        status,
    })
}

fn discover_latest_runtime_dir(root: Option<&str>, prefix: &str) -> Option<PathBuf> {
    let root = Path::new(root?);
    let mut entries = fs::read_dir(root)
        .ok()?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().ok().is_some_and(|ty| ty.is_dir()))
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(prefix))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.metadata().and_then(|meta| meta.modified()).ok());
    entries.last().map(|entry| entry.path())
}

fn read_runtime_status(runtime_dir: &Path) -> Option<RuntimeStatusDoc> {
    let path = runtime_dir.join("status.toml");
    if path.exists() {
        toml::from_str(&fs::read_to_string(path).ok()?).ok()
    } else {
        read_legacy_runtime_status(runtime_dir)
    }
}

fn read_legacy_runtime_status(runtime_dir: &Path) -> Option<RuntimeStatusDoc> {
    let summary = runtime_dir.join("summary");
    let raw = fs::read_to_string(summary).ok()?;
    let json: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let service = runtime_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .split('-')
        .next()
        .unwrap_or("unknown")
        .to_string();
    Some(RuntimeStatusDoc {
        service,
        health: if json
            .get("last_error")
            .and_then(|value| value.as_str())
            .is_some()
        {
            "degraded".to_string()
        } else {
            "healthy".to_string()
        },
        ready: true,
        uptime_ms: json
            .get("uptime_ms")
            .and_then(|value| value.as_u64())
            .unwrap_or_default(),
        started_unix_s: json
            .get("started_unix_s")
            .and_then(|value| value.as_u64())
            .unwrap_or_default(),
        pid: json
            .get("identity")
            .and_then(|value| value.get("pid"))
            .and_then(|value| value.as_u64())
            .unwrap_or_default() as u32,
        last_error: json
            .get("last_error")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        extra: BTreeMap::new(),
    })
}

fn collect_runtime_list(context: &ContextConfig) -> Result<Vec<RuntimeListEntry>, DynError> {
    let mut entries = Vec::new();
    for (service, root, prefix) in [
        ("kms", context.kms_runtime_root.as_deref(), "kms"),
        ("kas", context.kas_runtime_root.as_deref(), "kas"),
        ("krs", context.krs_runtime_root.as_deref(), "krs"),
        ("kst", context.kst_runtime_root.as_deref(), ""),
    ] {
        let Some(root) = root else {
            continue;
        };
        let path = Path::new(root);
        if !path.exists() {
            continue;
        }
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if !prefix.is_empty() && !name.starts_with(prefix) {
                continue;
            }
            let runtime_dir = entry.path();
            entries.push(RuntimeListEntry {
                service: service.to_string(),
                runtime_dir: runtime_dir.display().to_string(),
                status_path: existing_path(&runtime_dir.join("status.toml")),
                summary_path: existing_path(&runtime_dir.join("summary.toml"))
                    .or_else(|| existing_path(&runtime_dir.join("summary"))),
            });
        }
    }
    entries.sort_by(|a, b| a.runtime_dir.cmp(&b.runtime_dir));
    Ok(entries)
}

fn collect_runtime_show(
    context: &ContextConfig,
    service: &ServiceKind,
    explicit_runtime_dir: Option<&str>,
) -> Result<RuntimeShowReport, DynError> {
    let runtime_dir = if let Some(explicit_runtime_dir) = explicit_runtime_dir {
        PathBuf::from(explicit_runtime_dir)
    } else {
        discover_latest_runtime_dir(service_root(context, service), service_prefix(service))
            .ok_or_else(|| {
                boxed_error(format!(
                    "no runtime directory found for {}",
                    service_name(service)
                ))
            })?
    };
    Ok(RuntimeShowReport {
        service: service_name(service).to_string(),
        runtime_dir: runtime_dir.display().to_string(),
        status: read_runtime_status(&runtime_dir),
        identity_toml: read_optional_string(&runtime_dir.join("identity.toml")),
        summary_toml: read_optional_string(&runtime_dir.join("summary.toml")),
        summary_json: read_optional_string(&runtime_dir.join("summary")),
    })
}

fn collect_last_errors(context: &ContextConfig) -> Result<Vec<ServiceStatusReport>, DynError> {
    let mut reports = Vec::new();
    for service in [ServiceKind::Kms, ServiceKind::Kas, ServiceKind::Krs] {
        let runtime =
            read_latest_runtime_status(service_root(context, &service), service_prefix(&service));
        if let Some(runtime) = runtime {
            reports.push(ServiceStatusReport {
                service: service_name(&service).to_string(),
                health: HealthState::from_text(&runtime.status.health),
                reachable: true,
                endpoint: None,
                runtime_dir: Some(runtime.runtime_dir.display().to_string()),
                last_error: runtime.status.last_error,
                details: BTreeMap::new(),
                instances: Vec::new(),
            });
        }
    }
    Ok(reports)
}

fn service_root<'a>(context: &'a ContextConfig, service: &ServiceKind) -> Option<&'a str> {
    match service {
        ServiceKind::Kms => context.kms_runtime_root.as_deref(),
        ServiceKind::Kas => context.kas_runtime_root.as_deref(),
        ServiceKind::Krs => context.krs_runtime_root.as_deref(),
        ServiceKind::Kst => context.kst_runtime_root.as_deref(),
    }
}

fn service_prefix(service: &ServiceKind) -> &'static str {
    match service {
        ServiceKind::Kms => "kms",
        ServiceKind::Kas => "kas",
        ServiceKind::Krs => "krs",
        ServiceKind::Kst => "",
    }
}

fn service_name(service: &ServiceKind) -> &'static str {
    match service {
        ServiceKind::Kms => "kms",
        ServiceKind::Kas => "kas",
        ServiceKind::Krs => "krs",
        ServiceKind::Kst => "kst",
    }
}

fn existing_path(path: &Path) -> Option<String> {
    path.exists().then(|| path.display().to_string())
}

fn read_optional_string(path: &Path) -> Option<String> {
    path.exists()
        .then(|| fs::read_to_string(path).ok())
        .flatten()
}

async fn resolve_target_endpoints(
    context: &ContextConfig,
    timeout: Duration,
    explicit: &[String],
) -> Result<Vec<String>, DynError> {
    if !explicit.is_empty() {
        return Ok(explicit.to_vec());
    }
    if !context.kst_http_endpoints.is_empty() {
        return Ok(context.kst_http_endpoints.clone());
    }
    let mut kas = connect_kas(context, timeout).await?;
    let reply = rpc_timeout(
        timeout,
        kas.list_targets(Request::new(proto::ListTargetsRequest {})),
    )
    .await?
    .into_inner();
    Ok(reply
        .targets
        .into_iter()
        .map(|target| target.endpoint)
        .collect())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct KstTargetInfo {
    target_id: String,
    listen_addr: String,
    pid: u32,
    rebuild_required: bool,
}

async fn fetch_kst_info(endpoint: &str) -> Result<KstTargetInfo, DynError> {
    fetch_kst_json(endpoint, "/v1/info").await
}

async fn fetch_kst_stats(endpoint: &str) -> Result<serde_json::Value, DynError> {
    fetch_kst_json(endpoint, "/v1/stats").await
}

async fn fetch_kst_json<T: DeserializeOwned>(endpoint: &str, path: &str) -> Result<T, DynError> {
    let uri: Uri = endpoint.parse()?;
    let authority = uri
        .authority()
        .ok_or_else(|| boxed_error("KST endpoint must include host:port"))?;
    let socket = TcpStream::connect((authority.host(), authority.port_u16().unwrap_or(80))).await?;
    let (mut client, connection) = h2::client::handshake(socket).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let request = HttpRequest::builder()
        .method(Method::GET)
        .uri(path)
        .header(CONTENT_LENGTH, "0")
        .body(())?;
    let (response_future, _) = client.send_request(request, true)?;
    let response = response_future.await?;
    let status = response.status();
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    while let Some(frame) = body.data().await {
        bytes.extend_from_slice(&frame?);
    }
    if status != StatusCode::OK {
        return Err(boxed_error(format!(
            "KST {} {} returned {}: {}",
            endpoint,
            path,
            status,
            String::from_utf8_lossy(&bytes)
        )));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn require_confirm(cli: &Cli) -> Result<(), DynError> {
    if cli.confirm {
        Ok(())
    } else {
        Err(boxed_error("mutating commands require --confirm"))
    }
}

fn default_kms_endpoint() -> String {
    "http://127.0.0.1:50060".to_string()
}

fn default_kas_endpoint() -> String {
    "http://127.0.0.1:50061".to_string()
}

fn namespace_state_from_arg(value: NamespaceStateArg) -> proto::NamespaceState {
    match value {
        NamespaceStateArg::Active => proto::NamespaceState::Active,
        NamespaceStateArg::Disabled => proto::NamespaceState::Disabled,
        NamespaceStateArg::Deleting => proto::NamespaceState::Deleting,
    }
}

fn namespace_entry_kind_from_arg(value: NamespaceEntryKindArg) -> proto::NamespaceEntryKind {
    match value {
        NamespaceEntryKindArg::Project => proto::NamespaceEntryKind::Project,
        NamespaceEntryKindArg::Team => proto::NamespaceEntryKind::Team,
        NamespaceEntryKindArg::Group => proto::NamespaceEntryKind::Group,
        NamespaceEntryKindArg::Workspace => proto::NamespaceEntryKind::Workspace,
        NamespaceEntryKindArg::Bucket => proto::NamespaceEntryKind::Bucket,
        NamespaceEntryKindArg::Collection => proto::NamespaceEntryKind::Collection,
        NamespaceEntryKindArg::Object => proto::NamespaceEntryKind::Object,
    }
}

fn failure_domain_from_arg(value: FailureDomainArg) -> proto::FailureDomain {
    match value {
        FailureDomainArg::DriveDomainLab => proto::FailureDomain::DriveDomainLab,
        FailureDomainArg::Node => proto::FailureDomain::Node,
        FailureDomainArg::Rack => proto::FailureDomain::Rack,
    }
}

fn placement_task_kind_from_arg(value: PlacementTaskKindArg) -> proto::PlacementTaskKind {
    match value {
        PlacementTaskKindArg::Rebuild => proto::PlacementTaskKind::Rebuild,
        PlacementTaskKindArg::Rebalance => proto::PlacementTaskKind::Rebalance,
        PlacementTaskKindArg::Evacuate => proto::PlacementTaskKind::Evacuate,
    }
}

fn placement_task_state_from_arg(value: PlacementTaskStateArg) -> proto::PlacementTaskState {
    match value {
        PlacementTaskStateArg::Pending => proto::PlacementTaskState::Pending,
        PlacementTaskStateArg::Leased => proto::PlacementTaskState::Leased,
        PlacementTaskStateArg::Completed => proto::PlacementTaskState::Completed,
        PlacementTaskStateArg::Failed => proto::PlacementTaskState::Failed,
    }
}

fn write_intent_state_from_arg(value: WriteIntentStateArg) -> proto::WriteIntentState {
    match value {
        WriteIntentStateArg::Pending => proto::WriteIntentState::Pending,
        WriteIntentStateArg::Reserved => proto::WriteIntentState::Reserved,
        WriteIntentStateArg::Committed => proto::WriteIntentState::Committed,
        WriteIntentStateArg::Aborted => proto::WriteIntentState::Aborted,
        WriteIntentStateArg::Expired => proto::WriteIntentState::Expired,
    }
}

fn reservation_state_from_arg(value: ReservationStateArg) -> proto::ReservationState {
    match value {
        ReservationStateArg::Reserved => proto::ReservationState::Reserved,
        ReservationStateArg::Finalized => proto::ReservationState::Finalized,
        ReservationStateArg::Released => proto::ReservationState::Released,
    }
}

fn target_lifecycle_from_arg(value: TargetLifecycleStateArg) -> proto::TargetLifecycleState {
    match value {
        TargetLifecycleStateArg::Active => proto::TargetLifecycleState::Active,
        TargetLifecycleStateArg::Draining => proto::TargetLifecycleState::Draining,
        TargetLifecycleStateArg::Unhealthy => proto::TargetLifecycleState::Unhealthy,
        TargetLifecycleStateArg::Retired => proto::TargetLifecycleState::Retired,
    }
}

fn aggregate_health<I>(values: I) -> HealthState
where
    I: IntoIterator<Item = HealthState>,
{
    values
        .into_iter()
        .fold(HealthState::Healthy, HealthState::max)
}

fn toml_value_to_string(value: &toml::Value) -> Option<String> {
    match value {
        toml::Value::String(value) => Some(value.clone()),
        toml::Value::Integer(value) => Some(value.to_string()),
        toml::Value::Boolean(value) => Some(value.to_string()),
        _ => None,
    }
}

fn print_structured<T, F>(format: OutputFormat, value: &T, table: F) -> Result<(), DynError>
where
    T: Serialize,
    F: FnOnce() -> Result<String, DynError>,
{
    match format {
        OutputFormat::Table => {
            println!("{}", table()?);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(value)?);
        }
        OutputFormat::Toml => {
            println!("{}", toml::to_string_pretty(value)?);
        }
    }
    Ok(())
}

fn print_rows(
    format: OutputFormat,
    rows: &[BTreeMap<String, String>],
    columns: &[&str],
) -> Result<(), DynError> {
    match format {
        OutputFormat::Table => {
            println!("{}", render_rows(rows, columns));
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(rows)?),
        OutputFormat::Toml => println!("{}", toml::to_string_pretty(rows)?),
    }
    Ok(())
}

fn print_text(format: OutputFormat, text: &str) -> Result<(), DynError> {
    match format {
        OutputFormat::Table => print!("{text}"),
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&BTreeMap::from([("text", text)]))?
        ),
        OutputFormat::Toml => println!(
            "{}",
            toml::to_string_pretty(&BTreeMap::from([("text", text)]))?
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn clap_command_graph_builds() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_target_rebalance_enqueue_command() {
        let cli = Cli::try_parse_from([
            "keinctl",
            "--format",
            "toml",
            "--confirm",
            "target",
            "rebalance-enqueue",
            "--source-target-ids",
            "target-a,target-b",
            "--include-target-ids",
            "target-c,target-d",
            "--exclude-target-ids",
            "target-e",
            "--max-tasks",
            "64",
        ])
        .expect("CLI should parse");

        assert_eq!(cli.format, OutputFormat::Toml);
        assert!(cli.confirm);
        match cli.command {
            TopCommand::Target {
                command: TargetCommand::RebalanceEnqueue(args),
            } => {
                assert_eq!(args.source_target_ids, vec!["target-a", "target-b"]);
                assert_eq!(args.include_target_ids, vec!["target-c", "target-d"]);
                assert_eq!(args.exclude_target_ids, vec!["target-e"]);
                assert_eq!(args.max_tasks, 64);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parses_placement_wait_command() {
        let cli = Cli::try_parse_from([
            "keinctl",
            "placement",
            "wait",
            "--source-target-id",
            "target-a",
            "--timeout-secs",
            "90",
            "--poll-secs",
            "3",
            "--settle-polls",
            "2",
        ])
        .expect("CLI should parse");

        match cli.command {
            TopCommand::Placement {
                command: PlacementCommand::Wait(args),
            } => {
                assert_eq!(args.filters.source_target_id.as_deref(), Some("target-a"));
                assert_eq!(args.timeout_secs, 90);
                assert_eq!(args.poll_secs, 3);
                assert_eq!(args.settle_polls, 2);
                assert!(!args.allow_failed);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parses_target_register_command() {
        let cli = Cli::try_parse_from([
            "keinctl",
            "--confirm",
            "target",
            "register",
            "--target-id",
            "epyc-target-00",
            "--endpoint",
            "http://192.168.131.1:18080",
            "--server-id",
            "epyc-host",
            "--rack-id",
            "lab-rack-01",
            "--granule-count",
            "4096",
        ])
        .expect("CLI should parse");

        match cli.command {
            TopCommand::Target {
                command: TargetCommand::Register(args),
            } => {
                assert_eq!(args.target_id, "epyc-target-00");
                assert_eq!(args.endpoint, "http://192.168.131.1:18080");
                assert_eq!(args.server_id, "epyc-host");
                assert_eq!(args.rack_id, "lab-rack-01");
                assert_eq!(args.granule_count, 4096);
                assert_eq!(args.free_granules, None);
                assert!(args.healthy);
                assert_eq!(args.lifecycle_state, TargetLifecycleStateArg::Active);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parses_namespace_create_entry_command() {
        let cli = Cli::try_parse_from([
            "keinctl",
            "--confirm",
            "namespace",
            "create-entry",
            "--entry-id",
            "project-ml-platform",
            "--namespace-id",
            "tenant-acme",
            "--parent-entry-id",
            "",
            "--name",
            "ml-platform",
            "--kind",
            "project",
        ])
        .expect("CLI should parse");

        match cli.command {
            TopCommand::Namespace {
                command: NamespaceCommand::CreateEntry(args),
            } => {
                assert_eq!(args.entry_id, "project-ml-platform");
                assert_eq!(args.namespace_id, "tenant-acme");
                assert_eq!(args.parent_entry_id, "");
                assert_eq!(args.name, "ml-platform");
                assert_eq!(args.kind, NamespaceEntryKindArg::Project);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn yaml_output_mode_is_rejected() {
        let err = Cli::try_parse_from(["keinctl", "--format", "yaml", "context", "list"])
            .expect_err("yaml should not be accepted");
        let rendered = err.to_string();
        assert!(rendered.contains("possible values"));
        assert!(rendered.contains("table"));
        assert!(rendered.contains("json"));
        assert!(rendered.contains("toml"));
    }

    #[test]
    fn context_file_round_trips_as_toml() {
        let file = ContextFile::default();
        let encoded = toml::to_string_pretty(&file).expect("context file serializes");
        assert!(encoded.contains("current_context"));
        assert!(encoded.contains("kms_endpoint"));
        assert!(encoded.contains("kas_endpoint"));
        let decoded: ContextFile = toml::from_str(&encoded).expect("context file deserializes");
        assert_eq!(decoded.current_context, "default");
        assert!(decoded.contexts.contains_key("default"));
    }

    #[test]
    fn parses_intent_wait_command() {
        let cli = Cli::try_parse_from([
            "keinctl",
            "intent",
            "wait",
            "--bucket-id",
            "bucket-a",
            "--timeout-secs",
            "120",
            "--poll-secs",
            "5",
        ])
        .expect("CLI should parse");

        match cli.command {
            TopCommand::Intent {
                command: IntentCommand::Wait(args),
            } => {
                assert_eq!(args.filters.bucket_id.as_deref(), Some("bucket-a"));
                assert_eq!(args.timeout_secs, 120);
                assert_eq!(args.poll_secs, 5);
                assert_eq!(args.settle_polls, 1);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn aggregate_health_uses_worst_state() {
        let health = aggregate_health([
            HealthState::Healthy,
            HealthState::Degraded,
            HealthState::Unknown,
            HealthState::Unhealthy,
        ]);
        assert_eq!(health, HealthState::Unknown);
    }

    #[test]
    fn service_with_live_endpoint_and_no_registry_is_degraded() {
        let report = build_service_status(
            "kms",
            Some("http://192.168.130.11:50060".to_string()),
            None,
            true,
            BTreeMap::new(),
            Vec::new(),
        );
        assert_eq!(report.health, HealthState::Degraded);
    }

    #[test]
    fn krs_without_runtime_tree_is_unknown_not_unhealthy() {
        let report = build_service_status("krs", None, None, false, BTreeMap::new(), Vec::new());
        assert_eq!(report.health, HealthState::Unknown);
    }
}

fn render_rows(rows: &[BTreeMap<String, String>], columns: &[&str]) -> String {
    let mut widths = columns.iter().map(|name| name.len()).collect::<Vec<_>>();
    for row in rows {
        for (index, column) in columns.iter().enumerate() {
            widths[index] = widths[index].max(
                row.get(*column)
                    .map(|value| value.len())
                    .unwrap_or_default(),
            );
        }
    }
    let mut out = String::new();
    for (index, column) in columns.iter().enumerate() {
        out.push_str(&format!("{:width$}", column, width = widths[index]));
        if index + 1 != columns.len() {
            out.push_str("  ");
        }
    }
    out.push('\n');
    for width in &widths {
        out.push_str(&"-".repeat(*width));
        out.push_str("  ");
    }
    out.push('\n');
    for row in rows {
        for (index, column) in columns.iter().enumerate() {
            out.push_str(&format!(
                "{:width$}",
                row.get(*column).cloned().unwrap_or_default(),
                width = widths[index]
            ));
            if index + 1 != columns.len() {
                out.push_str("  ");
            }
        }
        out.push('\n');
    }
    out
}

fn render_context_show(current: &str, name: &str, context: &ContextConfig) -> String {
    format!(
        concat!(
            "name={}\n",
            "current={}\n",
            "label={}\n",
            "kms_endpoint={}\n",
            "kas_endpoint={}\n",
            "kms_runtime_root={}\n",
            "kas_runtime_root={}\n",
            "krs_runtime_root={}\n",
            "kst_runtime_root={}\n",
            "kst_http_endpoints={}\n"
        ),
        name,
        current == name,
        context.label.clone().unwrap_or_default(),
        context.kms_endpoint,
        context.kas_endpoint,
        context.kms_runtime_root.clone().unwrap_or_default(),
        context.kas_runtime_root.clone().unwrap_or_default(),
        context.krs_runtime_root.clone().unwrap_or_default(),
        context.kst_runtime_root.clone().unwrap_or_default(),
        context.kst_http_endpoints.join(","),
    )
}

fn render_context_validate(report: &ContextValidateReport) -> String {
    format!(
        concat!(
            "name={}\n",
            "kms_reachable={}\n",
            "kas_reachable={}\n",
            "kms_runtime_root_exists={}\n",
            "kas_runtime_root_exists={}\n",
            "krs_runtime_root_exists={}\n",
            "kst_runtime_root_exists={}\n"
        ),
        report.name,
        report.kms_reachable,
        report.kas_reachable,
        report.kms_runtime_root_exists,
        report.kas_runtime_root_exists,
        report.krs_runtime_root_exists,
        report.kst_runtime_root_exists,
    )
}

fn render_cluster_status(report: &ClusterStatusReport) -> String {
    let mut out = format!("health={}\n", report.health.as_str());
    out.push_str("reasons=\n");
    for reason in &report.reasons {
        out.push_str(&format!("  - {}\n", reason));
    }
    out.push_str("services=\n");
    for service in &report.services {
        out.push_str(&format!(
            "  {} health={} reachable={} registered_instances={} last_error={}\n",
            service.service,
            service.health.as_str(),
            service.reachable,
            service.instances.len(),
            service.last_error.clone().unwrap_or_default()
        ));
    }
    out.push_str(&format!(
        "targets total={} active={} draining={} unhealthy={} retired={} unhealthy_heartbeat={}\n",
        report.targets.total,
        report.targets.active,
        report.targets.draining,
        report.targets.unhealthy,
        report.targets.retired,
        report.targets.unhealthy_heartbeat
    ));
    out.push_str(&format!(
        "placement total={} pending_rebuild={} pending_rebalance={} pending_evacuate={} leased={} failed={}\n",
        report.placement.total,
        report.placement.pending_rebuild,
        report.placement.pending_rebalance,
        report.placement.pending_evacuate,
        report.placement.leased,
        report.placement.failed
    ));
    out.push_str(&format!(
        "intents total={} pending={} reserved={} committed={} aborted={} expired={}\n",
        report.intents.total,
        report.intents.pending,
        report.intents.reserved,
        report.intents.committed,
        report.intents.aborted,
        report.intents.expired
    ));
    out
}

fn render_cluster_topology(report: &ClusterTopologyReport) -> String {
    format!(
        concat!(
            "context={}\n",
            "kms_endpoint={}\n",
            "kas_endpoint={}\n",
            "namespace_count={}\n",
            "bucket_count={}\n",
            "target_count={}\n",
            "kms_runtime_root={}\n",
            "kas_runtime_root={}\n",
            "krs_runtime_root={}\n",
            "kst_runtime_root={}\n"
        ),
        report.context,
        report.kms_endpoint,
        report.kas_endpoint,
        report.namespace_count,
        report.bucket_count,
        report.target_count,
        report
            .runtime_roots
            .get("kms")
            .cloned()
            .flatten()
            .unwrap_or_default(),
        report
            .runtime_roots
            .get("kas")
            .cloned()
            .flatten()
            .unwrap_or_default(),
        report
            .runtime_roots
            .get("krs")
            .cloned()
            .flatten()
            .unwrap_or_default(),
        report
            .runtime_roots
            .get("kst")
            .cloned()
            .flatten()
            .unwrap_or_default(),
    )
}

fn render_service_status(report: &ServiceStatusReport) -> String {
    let mut out = format!(
        concat!(
            "service={}\n",
            "health={}\n",
            "reachable={}\n",
            "endpoint={}\n",
            "runtime_dir={}\n",
            "last_error={}\n",
            "registered_instances={}\n"
        ),
        report.service,
        report.health.as_str(),
        report.reachable,
        report.endpoint.clone().unwrap_or_default(),
        report.runtime_dir.clone().unwrap_or_default(),
        report.last_error.clone().unwrap_or_default(),
        report.instances.len(),
    );
    for (key, value) in &report.details {
        out.push_str(&format!("{}={}\n", key, value));
    }
    if !report.instances.is_empty() {
        out.push_str("instances=\n");
        for instance in &report.instances {
            out.push_str(&format!(
                "  {} node={} endpoint={} version={}-{} git_sha={} stale={} label={}\n",
                instance.instance_id,
                instance.node_id,
                instance.endpoint,
                instance.version,
                instance.release,
                instance.git_sha,
                instance.stale,
                instance.instance_label,
            ));
        }
    }
    out
}

fn render_namespace_list(namespaces: &[proto::NamespaceRecord]) -> String {
    let rows = namespaces
        .iter()
        .map(|namespace| {
            BTreeMap::from([
                ("namespace_id".to_string(), namespace.namespace_id.clone()),
                ("tenant_id".to_string(), namespace.tenant_id.clone()),
                ("display_name".to_string(), namespace.display_name.clone()),
                (
                    "state".to_string(),
                    namespace_state_name(namespace.state).to_string(),
                ),
                ("shard_id".to_string(), namespace.shard_id.clone()),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(
        &rows,
        &[
            "namespace_id",
            "tenant_id",
            "display_name",
            "state",
            "shard_id",
        ],
    )
}

fn render_namespace_show(
    namespace: &proto::NamespaceRecord,
    shard_map: Option<&proto::ShardMapEntry>,
) -> String {
    format!(
        concat!(
            "namespace_id={}\n",
            "tenant_id={}\n",
            "display_name={}\n",
            "state={}\n",
            "shard_id={}\n",
            "leader_endpoint={}\n",
            "replica_endpoints={}\n",
            "revision={}\n"
        ),
        namespace.namespace_id,
        namespace.tenant_id,
        namespace.display_name,
        namespace_state_name(namespace.state),
        namespace.shard_id,
        shard_map
            .map(|value| value.leader_endpoint.clone())
            .unwrap_or_default(),
        shard_map
            .map(|value| value.replica_endpoints.join(","))
            .unwrap_or_default(),
        shard_map.map(|value| value.revision).unwrap_or_default(),
    )
}

fn render_namespace_tree(nodes: &[TreeNode], depth: usize) -> String {
    let mut out = String::new();
    for node in nodes {
        out.push_str(&format!(
            "{}{} [{}]\n",
            "  ".repeat(depth),
            node.entry.name,
            namespace_entry_kind_name(node.entry.kind)
        ));
        out.push_str(&render_namespace_tree(&node.children, depth + 1));
    }
    out
}

fn render_resolve_path(reply: &proto::ResolvePathReply) -> String {
    let mut out = String::new();
    if let Some(namespace) = &reply.namespace {
        out.push_str(&format!("namespace_id={}\n", namespace.namespace_id));
    }
    if let Some(bucket) = &reply.bucket {
        out.push_str(&format!("bucket_id={}\n", bucket.bucket_id));
    }
    if let Some(entry) = &reply.final_entry {
        out.push_str(&format!(
            "final_entry={} [{}]\n",
            entry.path,
            namespace_entry_kind_name(entry.kind)
        ));
    }
    if let Some(head) = &reply.object_head {
        out.push_str(&format!(
            "object_head object_entry_id={} current_version_id={} revision={}\n",
            head.object_entry_id, head.current_version_id, head.revision
        ));
    }
    if !reply.chain.is_empty() {
        out.push_str("chain=\n");
        for entry in &reply.chain {
            out.push_str(&format!(
                "  {} [{}]\n",
                entry.path,
                namespace_entry_kind_name(entry.kind)
            ));
        }
    }
    out
}

fn render_list_children(entries: &[proto::NamespaceDomainEntry]) -> String {
    let rows = entries
        .iter()
        .map(|entry| {
            BTreeMap::from([
                ("entry_id".to_string(), entry.entry_id.clone()),
                ("name".to_string(), entry.name.clone()),
                (
                    "kind".to_string(),
                    namespace_entry_kind_name(entry.kind).to_string(),
                ),
                ("path".to_string(), entry.path.clone()),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(&rows, &["entry_id", "name", "kind", "path"])
}

fn render_bucket_list(buckets: &[proto::BucketRecord]) -> String {
    let rows = buckets
        .iter()
        .map(|bucket| {
            BTreeMap::from([
                ("bucket_id".to_string(), bucket.bucket_id.clone()),
                ("namespace_id".to_string(), bucket.namespace_id.clone()),
                (
                    "parent_entry_id".to_string(),
                    bucket.parent_entry_id.clone(),
                ),
                ("ec_profile_id".to_string(), bucket.ec_profile_id.clone()),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(
        &rows,
        &[
            "bucket_id",
            "namespace_id",
            "parent_entry_id",
            "ec_profile_id",
        ],
    )
}

fn render_bucket_show(
    bucket: &proto::BucketRecord,
    ec_profile: Option<&proto::EcProfile>,
) -> String {
    format!(
        concat!(
            "bucket_id={}\n",
            "namespace_id={}\n",
            "parent_entry_id={}\n",
            "bucket_entry_id={}\n",
            "ec_profile_id={}\n",
            "data_fragments={}\n",
            "parity_fragments={}\n",
            "fragment_bytes={}\n"
        ),
        bucket.bucket_id,
        bucket.namespace_id,
        bucket.parent_entry_id,
        bucket.bucket_entry_id,
        bucket.ec_profile_id,
        ec_profile
            .map(|value| value.data_fragments)
            .unwrap_or_default(),
        ec_profile
            .map(|value| value.parity_fragments)
            .unwrap_or_default(),
        ec_profile
            .map(|value| value.fragment_bytes)
            .unwrap_or_default(),
    )
}

fn render_ec_profiles(profiles: &[proto::EcProfile]) -> String {
    let rows = profiles
        .iter()
        .map(|profile| {
            BTreeMap::from([
                ("id".to_string(), profile.id.clone()),
                ("codec_id".to_string(), profile.codec_id.clone()),
                ("data".to_string(), profile.data_fragments.to_string()),
                ("parity".to_string(), profile.parity_fragments.to_string()),
                (
                    "fragment_bytes".to_string(),
                    profile.fragment_bytes.to_string(),
                ),
                (
                    "failure_domain".to_string(),
                    failure_domain_name(profile.failure_domain).to_string(),
                ),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(
        &rows,
        &[
            "id",
            "codec_id",
            "data",
            "parity",
            "fragment_bytes",
            "failure_domain",
        ],
    )
}

fn render_object_head(report: &ObjectHeadReport) -> String {
    format!(
        concat!(
            "bucket_id={}\n",
            "key={}\n",
            "version_id={}\n",
            "logical_length_bytes={}\n",
            "ec_profile_id={}\n",
            "stripe_count={}\n"
        ),
        report.bucket_id,
        report.key,
        report.version_id,
        report.logical_length_bytes,
        report.ec_profile_id,
        report.stripe_count,
    )
}

fn render_object_manifest(manifest: &proto::ObjectVersionManifest) -> String {
    let mut out = format!(
        concat!(
            "version_id={}\n",
            "bucket_id={}\n",
            "key={}\n",
            "logical_length_bytes={}\n",
            "ec_profile_id={}\n",
            "stripes={}\n"
        ),
        manifest.version_id,
        manifest.bucket_id,
        manifest.key,
        manifest.logical_length_bytes,
        manifest.ec_profile_id,
        manifest.stripes.len(),
    );
    for (stripe_index, stripe) in manifest.stripes.iter().enumerate() {
        out.push_str(&format!("stripe[{}]=\n", stripe_index));
        for fragment in &stripe.fragments {
            out.push_str(&format!(
                "  fragment={} target_id={} endpoint={} granule={} generation={}\n",
                fragment.fragment_index,
                fragment.target_id,
                fragment.endpoint,
                fragment.granule_index,
                fragment.generation
            ));
        }
    }
    out
}

fn render_object_locate(report: &ObjectLocateReport) -> String {
    let rows = report
        .fragments
        .iter()
        .map(|fragment| {
            BTreeMap::from([
                ("stripe".to_string(), fragment.stripe_index.to_string()),
                ("fragment".to_string(), fragment.fragment_index.to_string()),
                ("target_id".to_string(), fragment.target_id.clone()),
                ("endpoint".to_string(), fragment.endpoint.clone()),
                ("granule".to_string(), fragment.granule_index.to_string()),
                ("generation".to_string(), fragment.generation.to_string()),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(
        &rows,
        &[
            "stripe",
            "fragment",
            "target_id",
            "endpoint",
            "granule",
            "generation",
        ],
    )
}

fn render_target_reports(reports: &[TargetReport]) -> String {
    let rows = reports
        .iter()
        .map(|report| {
            BTreeMap::from([
                ("target_id".to_string(), report.target.target_id.clone()),
                ("health".to_string(), report.health.as_str().to_string()),
                (
                    "lifecycle".to_string(),
                    target_lifecycle_name(report.target.lifecycle_state).to_string(),
                ),
                ("heartbeat".to_string(), report.target.healthy.to_string()),
                (
                    "free_granules".to_string(),
                    report.target.free_granules.to_string(),
                ),
                (
                    "live_fragments".to_string(),
                    report.placement.live_fragments.to_string(),
                ),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(
        &rows,
        &[
            "target_id",
            "health",
            "lifecycle",
            "heartbeat",
            "free_granules",
            "live_fragments",
        ],
    )
}

fn render_target_report(report: &TargetReport) -> String {
    let mut out = format!(
        concat!(
            "target_id={}\n",
            "endpoint={}\n",
            "server_id={}\n",
            "rack_id={}\n",
            "failure_domain={}\n",
            "health={}\n",
            "lifecycle_state={}\n",
            "heartbeat_healthy={}\n",
            "granule_count={}\n",
            "free_granules={}\n",
            "live_fragments={}\n",
            "pending_rebuild_tasks={}\n",
            "pending_rebalance_tasks={}\n",
            "pending_evacuate_tasks={}\n",
            "leased_tasks={}\n",
            "failed_tasks={}\n"
        ),
        report.target.target_id,
        report.target.endpoint,
        report.target.server_id,
        report.target.rack_id,
        failure_domain_name(report.target.failure_domain),
        report.health.as_str(),
        target_lifecycle_name(report.target.lifecycle_state),
        report.target.healthy,
        report.target.granule_count,
        report.target.free_granules,
        report.placement.live_fragments,
        report.placement.pending_rebuild_tasks,
        report.placement.pending_rebalance_tasks,
        report.placement.pending_evacuate_tasks,
        report.placement.leased_tasks,
        report.placement.failed_tasks,
    );
    out.push_str("reasons=\n");
    for reason in &report.reasons {
        out.push_str(&format!("  - {}\n", reason));
    }
    out
}

fn render_recover_retire(
    target_id: &str,
    target: &Option<proto::TargetRecord>,
    live_fragments: u64,
) -> String {
    format!(
        concat!(
            "target_id={}\n",
            "lifecycle_state={}\n",
            "live_fragments={}\n"
        ),
        target_id,
        target
            .as_ref()
            .map(|value| target_lifecycle_name(value.lifecycle_state))
            .unwrap_or(""),
        live_fragments,
    )
}

fn render_rebalance_reply(mode: &str, reply: &proto::PreviewTargetRebalanceReply) -> String {
    format!(
        "mode={}\nlive_fragments={}\ncandidate_tasks={}\n",
        mode, reply.live_fragments, reply.candidate_tasks
    )
}

fn render_placement_summary(report: &PlacementSummaryReport) -> String {
    let rows = report
        .by_kind_state
        .iter()
        .map(|entry| {
            BTreeMap::from([
                ("task_kind".to_string(), entry.task_kind.clone()),
                ("state".to_string(), entry.state.clone()),
                ("count".to_string(), entry.count.to_string()),
            ])
        })
        .collect::<Vec<_>>();
    format!(
        "total={}\n{}",
        report.total,
        render_rows(&rows, &["task_kind", "state", "count"])
    )
}

fn render_placement_counts(report: &PlacementCountReport) -> String {
    format!(
        concat!(
            "total={}\n",
            "pending_rebuild={}\n",
            "pending_rebalance={}\n",
            "pending_evacuate={}\n",
            "leased={}\n",
            "failed={}\n"
        ),
        report.total,
        report.pending_rebuild,
        report.pending_rebalance,
        report.pending_evacuate,
        report.leased,
        report.failed,
    )
}

fn render_placement_tasks(tasks: &[proto::PlacementTaskSummary]) -> String {
    let rows = tasks
        .iter()
        .map(|task| {
            BTreeMap::from([
                ("task_id".to_string(), task.task_id.clone()),
                (
                    "kind".to_string(),
                    placement_task_kind_name(task.task_kind).to_string(),
                ),
                (
                    "state".to_string(),
                    placement_task_state_name(task.state).to_string(),
                ),
                (
                    "source_target_id".to_string(),
                    task.source_target_id.clone(),
                ),
                (
                    "destination_target_id".to_string(),
                    task.destination_target_id.clone(),
                ),
                (
                    "object_version_ref".to_string(),
                    task.object_version_ref.clone(),
                ),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(
        &rows,
        &[
            "task_id",
            "kind",
            "state",
            "source_target_id",
            "destination_target_id",
            "object_version_ref",
        ],
    )
}

fn render_placement_task_show(reply: &proto::GetPlacementTaskReply) -> String {
    let Some(task) = &reply.task else {
        return "task=\n".to_string();
    };
    format!(
        concat!(
            "task_id={}\n",
            "task_kind={}\n",
            "state={}\n",
            "source_target_id={}\n",
            "destination_target_id={}\n",
            "destination_granule_index={}\n",
            "object_version_ref={}\n",
            "stripe_index={}\n",
            "fragment_index={}\n",
            "lease_owner={}\n",
            "lease_expires_at_unix_ms={}\n",
            "namespace_id={}\n",
            "bucket_id={}\n",
            "object_entry_id={}\n",
            "reason={}\n"
        ),
        task.task_id,
        placement_task_kind_name(task.task_kind),
        placement_task_state_name(task.state),
        task.source_target_id,
        task.destination_target_id,
        task.destination_granule_index,
        task.object_version_ref,
        task.stripe_index,
        task.fragment_index,
        task.lease_owner,
        task.lease_expires_at_unix_ms,
        task.namespace_id,
        task.bucket_id,
        task.object_entry_id,
        task.reason,
    )
}

fn render_write_intents(intents: &[proto::WriteIntentSummary]) -> String {
    let rows = intents
        .iter()
        .map(|intent| {
            BTreeMap::from([
                ("intent_id".to_string(), intent.intent_id.clone()),
                ("bucket_id".to_string(), intent.bucket_id.clone()),
                ("key".to_string(), intent.key.clone()),
                (
                    "state".to_string(),
                    write_intent_state_name(intent.state).to_string(),
                ),
                (
                    "expires_at_unix_ms".to_string(),
                    intent.expires_at_unix_ms.to_string(),
                ),
                (
                    "reservation_ids".to_string(),
                    intent.reservation_ids.join(","),
                ),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(
        &rows,
        &[
            "intent_id",
            "bucket_id",
            "key",
            "state",
            "expires_at_unix_ms",
            "reservation_ids",
        ],
    )
}

fn render_intent_counts(report: &IntentCountReport) -> String {
    format!(
        concat!(
            "total={}\n",
            "pending={}\n",
            "reserved={}\n",
            "committed={}\n",
            "aborted={}\n",
            "expired={}\n"
        ),
        report.total,
        report.pending,
        report.reserved,
        report.committed,
        report.aborted,
        report.expired,
    )
}

fn render_write_intent_show(intent: &proto::WriteIntent) -> String {
    let mut out = format!(
        concat!(
            "intent_id={}\n",
            "version_id={}\n",
            "bucket_id={}\n",
            "key={}\n",
            "state={}\n",
            "expires_at_unix_ms={}\n",
            "reservation_ids={}\n"
        ),
        intent.intent_id,
        intent.version_id,
        intent.bucket_id,
        intent.key,
        write_intent_state_name(intent.state),
        intent.expires_at_unix_ms,
        intent.reservation_ids.join(","),
    );
    out.push_str("fragment_status=\n");
    for status in &intent.fragment_status {
        out.push_str(&format!(
            "  fragment={} state={} reservation_id={} reservation_placement_index={}\n",
            status.fragment_index,
            fragment_write_state_name(status.state),
            status.reservation_id,
            status.reservation_placement_index
        ));
    }
    out
}

fn render_reservations(reservations: &[proto::PlacementReservationRecord]) -> String {
    let rows = reservations
        .iter()
        .map(|reservation| {
            BTreeMap::from([
                (
                    "reservation_id".to_string(),
                    reservation.reservation_id.clone(),
                ),
                (
                    "state".to_string(),
                    reservation_state_name(reservation.state).to_string(),
                ),
                (
                    "placements".to_string(),
                    reservation.placements.len().to_string(),
                ),
                (
                    "expires_at_unix_ms".to_string(),
                    reservation.expires_at_unix_ms.to_string(),
                ),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(
        &rows,
        &[
            "reservation_id",
            "state",
            "placements",
            "expires_at_unix_ms",
        ],
    )
}

fn render_runtime_list(entries: &[RuntimeListEntry]) -> String {
    let rows = entries
        .iter()
        .map(|entry| {
            BTreeMap::from([
                ("service".to_string(), entry.service.clone()),
                ("runtime_dir".to_string(), entry.runtime_dir.clone()),
                (
                    "status_path".to_string(),
                    entry.status_path.clone().unwrap_or_default(),
                ),
                (
                    "summary_path".to_string(),
                    entry.summary_path.clone().unwrap_or_default(),
                ),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(
        &rows,
        &["service", "runtime_dir", "status_path", "summary_path"],
    )
}

fn render_runtime_show(report: &RuntimeShowReport) -> String {
    let mut out = format!(
        "service={}\nruntime_dir={}\n",
        report.service, report.runtime_dir
    );
    if let Some(status) = &report.status {
        out.push_str(&format!(
            "health={}\nready={}\nuptime_ms={}\nlast_error={}\n",
            status.health,
            status.ready,
            status.uptime_ms,
            status.last_error.clone().unwrap_or_default()
        ));
    }
    if let Some(identity) = &report.identity_toml {
        out.push_str("identity_toml=\n");
        out.push_str(identity);
        if !identity.ends_with('\n') {
            out.push('\n');
        }
    }
    if let Some(summary) = &report.summary_toml {
        out.push_str("summary_toml=\n");
        out.push_str(summary);
        if !summary.ends_with('\n') {
            out.push('\n');
        }
    } else if let Some(summary) = &report.summary_json {
        out.push_str("summary_json=\n");
        out.push_str(summary);
        if !summary.ends_with('\n') {
            out.push('\n');
        }
    }
    out
}

fn render_last_errors(reports: &[ServiceStatusReport]) -> String {
    let rows = reports
        .iter()
        .map(|report| {
            BTreeMap::from([
                ("service".to_string(), report.service.clone()),
                ("health".to_string(), report.health.as_str().to_string()),
                (
                    "runtime_dir".to_string(),
                    report.runtime_dir.clone().unwrap_or_default(),
                ),
                (
                    "last_error".to_string(),
                    report.last_error.clone().unwrap_or_default(),
                ),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(&rows, &["service", "health", "runtime_dir", "last_error"])
}

fn render_target_http_info(reports: &[TargetHttpReport<KstTargetInfo>]) -> String {
    let rows = reports
        .iter()
        .map(|report| {
            BTreeMap::from([
                ("endpoint".to_string(), report.endpoint.clone()),
                ("target_id".to_string(), report.value.target_id.clone()),
                ("listen_addr".to_string(), report.value.listen_addr.clone()),
                ("pid".to_string(), report.value.pid.to_string()),
                (
                    "rebuild_required".to_string(),
                    report.value.rebuild_required.to_string(),
                ),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(
        &rows,
        &[
            "endpoint",
            "target_id",
            "listen_addr",
            "pid",
            "rebuild_required",
        ],
    )
}

fn render_target_http_stats(reports: &[TargetHttpReport<serde_json::Value>]) -> String {
    let rows = reports
        .iter()
        .map(|report| {
            let stats = report.value.get("stats").cloned().unwrap_or_default();
            BTreeMap::from([
                ("endpoint".to_string(), report.endpoint.clone()),
                (
                    "target_id".to_string(),
                    report
                        .value
                        .get("identity")
                        .and_then(|v| v.get("target_id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                ),
                (
                    "total_requests".to_string(),
                    stats
                        .get("total_requests")
                        .and_then(|v| v.as_u64())
                        .unwrap_or_default()
                        .to_string(),
                ),
                (
                    "total_errors".to_string(),
                    stats
                        .get("total_errors")
                        .and_then(|v| v.as_u64())
                        .unwrap_or_default()
                        .to_string(),
                ),
                (
                    "inflight_requests".to_string(),
                    stats
                        .get("inflight_requests")
                        .and_then(|v| v.as_u64())
                        .unwrap_or_default()
                        .to_string(),
                ),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(
        &rows,
        &[
            "endpoint",
            "target_id",
            "total_requests",
            "total_errors",
            "inflight_requests",
        ],
    )
}

fn render_metadata_events(events: &[proto::MetadataEvent]) -> String {
    let rows = events
        .iter()
        .map(|event| {
            BTreeMap::from([
                ("revision".to_string(), event.revision.to_string()),
                (
                    "event_kind".to_string(),
                    metadata_event_kind_name(event.event_kind).to_string(),
                ),
                ("entry_path".to_string(), event.entry_path.clone()),
                ("summary".to_string(), event.summary.clone()),
            ])
        })
        .collect::<Vec<_>>();
    render_rows(&rows, &["revision", "event_kind", "entry_path", "summary"])
}

fn boxed_error(message: impl Into<String>) -> DynError {
    Box::new(io::Error::new(io::ErrorKind::Other, message.into()))
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn namespace_state_name(value: i32) -> &'static str {
    match proto::NamespaceState::try_from(value).unwrap_or(proto::NamespaceState::Unspecified) {
        proto::NamespaceState::Active => "active",
        proto::NamespaceState::Disabled => "disabled",
        proto::NamespaceState::Deleting => "deleting",
        proto::NamespaceState::Unspecified => "unspecified",
    }
}

fn namespace_entry_kind_name(value: i32) -> &'static str {
    match proto::NamespaceEntryKind::try_from(value)
        .unwrap_or(proto::NamespaceEntryKind::Unspecified)
    {
        proto::NamespaceEntryKind::Project => "project",
        proto::NamespaceEntryKind::Team => "team",
        proto::NamespaceEntryKind::Group => "group",
        proto::NamespaceEntryKind::Workspace => "workspace",
        proto::NamespaceEntryKind::Bucket => "bucket",
        proto::NamespaceEntryKind::Collection => "collection",
        proto::NamespaceEntryKind::Object => "object",
        proto::NamespaceEntryKind::Unspecified => "unspecified",
    }
}

fn failure_domain_name(value: i32) -> &'static str {
    match proto::FailureDomain::try_from(value).unwrap_or(proto::FailureDomain::Unspecified) {
        proto::FailureDomain::DriveDomainLab => "drive_domain_lab",
        proto::FailureDomain::Node => "node",
        proto::FailureDomain::Rack => "rack",
        proto::FailureDomain::Unspecified => "unspecified",
    }
}

fn service_kind_name(value: proto::ServiceKind) -> &'static str {
    match value {
        proto::ServiceKind::Kms => "kms",
        proto::ServiceKind::Kas => "kas",
        proto::ServiceKind::Krs => "krs",
        proto::ServiceKind::Kst => "kst",
        proto::ServiceKind::Ksc => "ksc",
        proto::ServiceKind::Keinctl => "keinctl",
        proto::ServiceKind::Unspecified => "unspecified",
    }
}

fn write_intent_state_name(value: i32) -> &'static str {
    match proto::WriteIntentState::try_from(value).unwrap_or(proto::WriteIntentState::Unspecified) {
        proto::WriteIntentState::Pending => "pending",
        proto::WriteIntentState::Reserved => "reserved",
        proto::WriteIntentState::Committed => "committed",
        proto::WriteIntentState::Aborted => "aborted",
        proto::WriteIntentState::Expired => "expired",
        proto::WriteIntentState::Unspecified => "unspecified",
    }
}

fn fragment_write_state_name(value: i32) -> &'static str {
    match proto::FragmentWriteState::try_from(value)
        .unwrap_or(proto::FragmentWriteState::Unspecified)
    {
        proto::FragmentWriteState::Planned => "planned",
        proto::FragmentWriteState::Written => "written",
        proto::FragmentWriteState::Failed => "failed",
        proto::FragmentWriteState::Unspecified => "unspecified",
    }
}

fn reservation_state_name(value: i32) -> &'static str {
    match proto::ReservationState::try_from(value).unwrap_or(proto::ReservationState::Unspecified) {
        proto::ReservationState::Reserved => "reserved",
        proto::ReservationState::Finalized => "finalized",
        proto::ReservationState::Released => "released",
        proto::ReservationState::Unspecified => "unspecified",
    }
}

fn placement_task_kind_name(value: i32) -> &'static str {
    match proto::PlacementTaskKind::try_from(value).unwrap_or(proto::PlacementTaskKind::Unspecified)
    {
        proto::PlacementTaskKind::Rebuild => "rebuild",
        proto::PlacementTaskKind::Rebalance => "rebalance",
        proto::PlacementTaskKind::Evacuate => "evacuate",
        proto::PlacementTaskKind::Unspecified => "unspecified",
    }
}

fn placement_task_state_name(value: i32) -> &'static str {
    match proto::PlacementTaskState::try_from(value)
        .unwrap_or(proto::PlacementTaskState::Unspecified)
    {
        proto::PlacementTaskState::Pending => "pending",
        proto::PlacementTaskState::Leased => "leased",
        proto::PlacementTaskState::Completed => "completed",
        proto::PlacementTaskState::Failed => "failed",
        proto::PlacementTaskState::Unspecified => "unspecified",
    }
}

fn target_lifecycle_name(value: i32) -> &'static str {
    match proto::TargetLifecycleState::try_from(value)
        .unwrap_or(proto::TargetLifecycleState::Unspecified)
    {
        proto::TargetLifecycleState::Active => "active",
        proto::TargetLifecycleState::Draining => "draining",
        proto::TargetLifecycleState::Unhealthy => "unhealthy",
        proto::TargetLifecycleState::Retired => "retired",
        proto::TargetLifecycleState::Unspecified => "unspecified",
    }
}

fn metadata_event_kind_name(value: i32) -> &'static str {
    match proto::MetadataEventKind::try_from(value).unwrap_or(proto::MetadataEventKind::Unspecified)
    {
        proto::MetadataEventKind::NamespaceCreated => "namespace_created",
        proto::MetadataEventKind::EntryCreated => "entry_created",
        proto::MetadataEventKind::BucketCreated => "bucket_created",
        proto::MetadataEventKind::ObjectHeadUpdated => "object_head_updated",
        proto::MetadataEventKind::WriteIntentCreated => "write_intent_created",
        proto::MetadataEventKind::WriteIntentAborted => "write_intent_aborted",
        proto::MetadataEventKind::WriteIntentExpired => "write_intent_expired",
        proto::MetadataEventKind::RebuildTaskCreated => "rebuild_task_created",
        proto::MetadataEventKind::RebuildCommitted => "rebuild_committed",
        proto::MetadataEventKind::ShardMapUpdated => "shard_map_updated",
        proto::MetadataEventKind::WriteIntentRepaired => "write_intent_repaired",
        proto::MetadataEventKind::PlacementTaskCreated => "placement_task_created",
        proto::MetadataEventKind::PlacementTaskCommitted => "placement_task_committed",
        proto::MetadataEventKind::TargetStateUpdated => "target_state_updated",
        proto::MetadataEventKind::ObjectDeleted => "object_deleted",
        proto::MetadataEventKind::ObjectVersionsDeleted => "object_versions_deleted",
        proto::MetadataEventKind::Unspecified => "unspecified",
    }
}
