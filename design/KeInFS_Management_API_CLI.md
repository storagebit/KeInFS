# KeInFS Management API & CLI Reference

### Administration, Provisioning, and Operations Interface Specification

---

**Andreas Krause**
*Storage Engineering & High-Performance Computing Infrastructure*

**Document Version:** 0.1-draft | **Date:** February 2026 | **Classification:** API & CLI Reference Specification

**Companion to:** *KeInFS: Erasure-Coded Parallel Object Storage on Raw Block Devices — A Design Specification for High-Performance AI Storage Infrastructure*

---

> Note
> This document specifies the intended management contract.
> For current prototype behavior, active control-plane direction, and the
> current client surface, start with [README.md](../README.md),
> [poc/METADATA_NAMESPACE_ARCHITECTURE.md](../poc/METADATA_NAMESPACE_ARCHITECTURE.md),
> [poc/ksc/README.md](../poc/ksc/README.md), and
> [poc/kfc/README.md](../poc/kfc/README.md).

## Abstract

This document specifies the KeInFS Management API and the `keinctl` command-line interface for cluster administration, provisioning, multi-tenant resource governance, and operational observability. The Management API is the single authoritative interface for all administrative operations on a KeInFS cluster. The `keinctl` CLI is a stateless client that translates command-line invocations into Management API HTTP/2 calls — it contains no business logic, no local state, and no privileged access paths that bypass the API. Every operation available in `keinctl` is available in the API, and every operation available in the API is available in `keinctl`. Third-party tooling, infrastructure-as-code pipelines (Terraform, Pulumi, Ansible), Kubernetes operators, and self-service provisioning portals are all first-class API consumers with identical capabilities and identical security enforcement.

The Management API runs on KeInFS coordinators, which own authentication, metadata mutation, quota policy, and control-path orchestration. Administrative endpoints are distinguished by the `/mgmt/v1/` path prefix and are subject to their own role-based access control (RBAC) policy, separate from bucket-level data access permissions. Native data-plane bytes do not traverse these coordinators; S3 proxy ingress runs on storage nodes because S3 insists on being proxied and there is no reason to infect the native path with that limitation. All management operations are recorded in the KeInFS audit log with full attribution context, providing a tamper-evident record of who changed what, when, and from where.

---

## Table of Contents

1. Architecture and Design Principles
2. Security Model
3. API Conventions
4. CLI Global Configuration
5. Cluster Management
6. Node Management
7. Drive Management
8. Bucket Management
9. Storage Classes and EC Profile Management
10. Tenant, Team, and Project Management
11. Quota Management
12. Job Monitoring and Diagnostics
13. Usage and Chargeback Reporting
14. Rebuild, Scrub, and Garbage Collection
15. Observability: Tracing and Metrics
16. Audit Log Access
17. Authentication and Token Management
18. Attribution Context Management
19. Appendix A — Error Catalogue
20. Appendix B — RBAC Permission Matrix
21. Appendix C — CLI Quick Reference

---

## 1. Architecture and Design Principles

### 1.1 Everything Is the API

The KeInFS management architecture follows a single, non-negotiable principle: **every administrative action — without exception — is an HTTP/2 API call to a coordinator endpoint.** There are no side-channel SSH sessions that modify the metadata store directly, no out-of-band configuration files that change cluster behavior without an API call, and no privileged local commands that bypass authentication and authorization. The coordinator is the policy-enforcement point for the management plane.

This principle exists for two fundamental reasons.

**Reason 1 — One functional standard to maintain.** When the API is the only way to interact with the cluster, there is exactly one code path for every operation. The validation logic for "create a bucket" exists in one place — the API handler. The authorization check for "set a quota" exists in one place — the API's RBAC evaluator. The audit logging for "evict a drive" exists in one place — the API's audit middleware. There is no second implementation in the CLI, no third implementation in a Terraform provider, no fourth implementation in a Kubernetes operator — because the CLI, the Terraform provider, and the Kubernetes operator are all HTTP/2 clients that call the same API endpoint. When a bug is fixed in the API's bucket creation validation, every consumer gets the fix simultaneously, because every consumer goes through the same gate. When a new authorization check is added to the quota endpoint, every consumer is subject to it immediately, because there is no alternative path. The alternative — maintaining functional parity between a CLI that talks directly to the metadata store, an API that talks directly to the metadata store, and an SDK that talks directly to the metadata store — is the architecture that produces the security vulnerabilities, behavioral inconsistencies, and maintenance burden that KeInFS is designed to eliminate.

**Reason 2 — Threat surface reduction.** The API is the security perimeter. Every request that reaches the cluster's internal components — the metadata store, storage nodes, background services — has been authenticated, authorized, validated, rate-limited, and audit-logged by the coordinator. A vulnerability in the CLI — a buffer overflow in argument parsing, an injection flaw in how it constructs requests, a credential leak in how it reads configuration files — cannot compromise the cluster, because the CLI has no privileged access path. It is an HTTP client. Its requests are subject to the same authentication, authorization, and validation as a `curl` command. The worst a compromised CLI can do is send malformed HTTP/2 requests, which the coordinator rejects. This is a deliberate architectural choice: the CLI is untrusted code running on an untrusted machine, and the API is the gatekeeper that decides whether the request is legitimate. Moving any business logic, validation, or privileged access into the CLI would expand the attack surface from "the coordinator process on a hardened server" to "every machine where an administrator has installed `keinctl`" — which is exactly the threat model that KeInFS rejects.

These two reasons produce four practical consequences that inform the design of every endpoint and CLI command in this document.

First, the `keinctl` CLI is a thin, stateless HTTP/2 client. It reads its configuration (coordinator endpoint, credentials, output format preferences) from a local configuration file or environment variables, constructs an HTTP/2 request, sends it to a coordinator, and formats the response for human consumption. It performs no local computation, caches no cluster state, and makes no decisions that the API does not make. If `keinctl` is unavailable, any HTTP client (`curl`, `httpie`, Python `requests`, a Terraform provider) can perform the same operation with the same request. The CLI is a convenience, not a dependency.

Second, all administrative operations are subject to the same authentication, authorization, rate limiting, and audit logging as data-plane operations. An administrator setting a quota is authenticated via the same mechanisms (HMAC-signed requests, bearer tokens, or mTLS client certificates) as a training job writing a checkpoint. Their action is authorized against the RBAC policy stored in the cluster's metadata. Their action is rate-limited by the same per-identity rate limiter. Their action is recorded in the same audit log. There is no separate "admin channel" with different security properties.

Third, the Management API is suitable for automation. Every endpoint accepts and returns structured JSON. Every endpoint returns machine-parseable error codes and messages. Every mutating operation is idempotent (or explicitly documented as non-idempotent where this is unavoidable). Every long-running operation returns immediately with a status URL for polling. The API is designed to be driven by Terraform providers, Ansible modules, Kubernetes operators, and CI/CD pipelines with the same fidelity and reliability as interactive `keinctl` usage.

Fourth, error messages are written for the operator, not the developer. Error codes and messages describe what went wrong in terms of the operation the user attempted, not in terms of the internal component that failed. A namespace lookup failure says "namespace unavailable"; it does not expose backend-specific internal state. A quota violation says which quota, at which hierarchy level, for which project; it does not expose internal counter key paths. The API is an abstraction boundary: the operator interacts with clusters, nodes, drives, buckets, tenants, quotas, and jobs. The internal implementation is behind that boundary.

### 1.2 API Endpoint Location

The Management API is served by KeInFS coordinators. All management endpoints are rooted under the `/mgmt/v1/` path prefix. Native KeInFS/2 control-plane endpoints (`initiate`, `resolve`, `commit`, list, head) also terminate on coordinators. S3-compatible requests terminate on storage-node S3 ingress endpoints. The topology is deliberate: management and native control live on coordinators; proxied S3 ingress lives on storage nodes; native data bytes live nowhere near either of those abstractions unless someone has made a mistake.

```
Native control plane:   https://coord.keinfs.local:8443/v1/{bucket}/{key}
S3 proxy ingress:       https://s3.keinfs.local:8443/{bucket}/{key}
Management API:         https://coord.keinfs.local:8443/mgmt/v1/...
```

Because coordinators are stateless, any coordinator can serve any management request. There is no "primary" management coordinator — the concept does not exist. The smart client library (`libkeinfs`) maintains its own list of available coordinators and handles selection, failover, and retry internally — the load-balancing logic lives in the client, not in an intermediary. S3-compatible clients (`boto3`, `aws s3`, `curl`) require a load balancer or DNS round-robin in front of the storage-node S3 ingress fleet because they cannot participate in coordinator discovery or native direct I/O. For the native path, no data-plane load balancer is needed.

For the native data plane, the management model is explicitly target-oriented. One physical drive corresponds to one storage target identity and one native endpoint. A multi-drive storage server therefore presents a set of targets, not a single pooled data endpoint. Management APIs that enumerate drives, targets, or node topology must preserve that distinction because placement, pacing, failure-domain policy, and observability all operate at target granularity.

#### 1.2.1 Internal Control-Plane Services in the Current Laboratory Slice

The current proof-of-concept introduces two internal gRPC services behind the broader management and coordination model.

**KMS** (KeInFS Metadata Service) owns EC profiles, immutable bucket-to-profile bindings, write intents, object manifests, current pointers, fragment secondary indexes, and rebuild task state.

**KAS** (KeInFS Allocator Service) owns target registration, target heartbeats, failure-domain labels, free-span accounting, stripe reservations, and rebuild replacement placement.

These services are control-plane components, not native data-plane endpoints. `KSC` talks to `KMS` for the first object write and read slice. `KSC` does not call `KAS` directly. Human and automation administration still belong at the coordinator-facing management API layer described in this document; the gRPC services exist so the prototype can exercise a real metadata and allocation boundary instead of cramming everything into one process and calling it architecture.

For the current lab deployment, `KMS` and `KAS` run as separate control-plane services over a FoundationDB durability layer with NATS-based invalidation fan-out, while the storage server presents `12` physical-drive `KST` targets and one `KRS` rebuild daemon. The first end-to-end object slice uses one immutable `8+2` EC profile and an explicit `drive-domain-lab` placement mode so the stripe can be spread across `10` distinct targets on one EPYC host with `2` spare targets left for rebuild testing.

### 1.3 Relationship Between CLI and API

Every `keinctl` command maps to exactly one Management API endpoint. The mapping is mechanical and documented in every section of this reference. Where the CLI provides convenience features — such as `keinctl job diagnose` which internally issues multiple API calls and correlates their results — the individual API calls are documented separately so that automation consumers can replicate the same logic.

The CLI output format is controlled by global flags:

| Flag | Format | Use Case |
|------|--------|----------|
| (default) | Human-readable table/text | Interactive terminal use |
| `--json` | JSON | Programmatic parsing, piping to `jq` |
| `--toml` | TOML | Configuration management integration |
| `--quiet` or `-q` | Minimal output (exit code only) | Scripts and CI/CD |

---

## 2. Security Model

### 2.1 Transport Security

All Management API communication uses TLS 1.3. There are no plaintext management endpoints. The mandatory cipher suites are `TLS_AES_256_GCM_SHA384`, `TLS_CHACHA20_POLY1305_SHA256`, and `TLS_AES_128_GCM_SHA256`. ALPN negotiation for `h2` (HTTP/2) is required; connections that fail ALPN negotiation are terminated. HTTP/1.1 is not supported on any KeInFS endpoint, including management endpoints.

### 2.2 Authentication Methods

The Management API supports the same three authentication mechanisms as the data-plane API, applied identically.

**HMAC-SHA256 Signed Requests** follow a request signing scheme analogous to AWS Signature V4. Each request is signed with a secret key, and the signature covers the HTTP method, path, query string, selected headers (including `x-keinfs-date`), and a SHA-256 hash of the request body. This is the recommended authentication method for `keinctl` and for automation clients that store long-lived credentials securely.

**Bearer Tokens** are short-lived opaque tokens issued by the `/mgmt/v1/auth/token` endpoint or by an external identity provider via OIDC federation. They are suitable for interactive sessions, CI/CD pipelines, and integration with existing enterprise identity infrastructure (Okta, Azure AD, Google Workspace, Keycloak). Tokens are passed in the `Authorization: Bearer <token>` header.

**mTLS Client Certificates** provide certificate-based authentication for environments with existing PKI infrastructure. The client's X.509 certificate is validated against the cluster CA or a configured external CA. The certificate's Subject or SAN field is mapped to a KeInFS identity for RBAC evaluation. This method is particularly suitable for machine-to-machine administration (e.g., a Terraform provider running in a CI pipeline with a service certificate).

### 2.3 Role-Based Access Control (RBAC)

The Management API enforces role-based access control on every request. RBAC policies are stored in the cluster's distributed metadata store and evaluated at the coordinator on every management API call. The RBAC model is hierarchical, following the same tenant → team → project hierarchy used for quota enforcement and usage attribution.

#### 2.3.1 Built-in Roles

KeInFS defines a set of built-in roles that cover the most common administrative personas. Custom roles can be created by combining individual permissions (see Appendix B for the full permission matrix).

**`cluster-admin`** has unrestricted access to all management operations across the entire cluster. This role is intended for the infrastructure team that owns the KeInFS deployment. It can create and delete tenants, modify global cluster configuration, manage storage nodes and drives, and override any tenant-level or team-level setting. There should be very few principals with this role. Analogous to Kubernetes `cluster-admin`.

**`tenant-admin`** has full administrative access within a single tenant's scope. This role can create and manage teams and projects within the tenant, set quotas at the team and project level (up to the tenant's own limits), manage bucket configurations within the tenant, view usage and audit logs for the tenant, and manage credentials for identities within the tenant. It cannot create or delete tenants, modify cluster-level configuration, or manage storage nodes. This is the role for the platform engineering lead or storage administrator responsible for a single organization on a shared cluster.

**`team-admin`** has administrative access within a single team's scope. This role can create and manage projects within the team, set quotas at the project level (up to the team's limits), manage buckets assigned to the team, and view usage and audit logs for the team. It cannot modify team-level quotas, create or delete teams, or see other teams' data.

**`project-admin`** has administrative access within a single project's scope. This role can manage buckets assigned to the project, view usage and job diagnostics for the project, and manage credentials for project-scoped service accounts. It cannot modify project-level quotas or see other projects' data.

**`viewer`** has read-only access to the scoped resource (cluster, tenant, team, or project). Viewers can read status, configuration, usage, and diagnostics but cannot make any changes. This role is suitable for dashboards, monitoring integrations, and on-call engineers who need visibility without modification capability.

**`auditor`** has read-only access to audit logs within the scoped resource. This role is separated from `viewer` because audit log access may be governed by different compliance requirements than operational visibility.

#### 2.3.2 RBAC Policy Structure

RBAC bindings associate an identity (a user, a service account, or a group from an external identity provider) with a role at a specific scope.

```json
{
    "bindings": [
        {
            "identity": "user:alice@acme.com",
            "role": "tenant-admin",
            "scope": { "tenant": "acme" }
        },
        {
            "identity": "group:ml-platform-team@acme.com",
            "role": "team-admin",
            "scope": { "tenant": "acme", "team": "ml-platform" }
        },
        {
            "identity": "serviceaccount:terraform-ci",
            "role": "cluster-admin",
            "scope": { "cluster": "*" }
        },
        {
            "identity": "user:bob@acme.com",
            "role": "viewer",
            "scope": { "tenant": "acme", "team": "ml-research" }
        }
    ]
}
```

The scope field determines the boundary of the role's permissions. A `team-admin` scoped to `{"tenant": "acme", "team": "ml-research"}` can manage projects and buckets within the `ml-research` team of the `acme` tenant, but has no access to other teams or tenants. A `cluster-admin` scoped to `{"cluster": "*"}` has unrestricted access.

### 2.4 Audit of Administrative Operations

Every Management API call is recorded in the KeInFS audit log, including the full request path and method, the authenticated identity and their resolved RBAC role, the request body (for mutating operations), the response status code, the source IP address and TLS client certificate (if mTLS), and a timestamp with microsecond precision. Audit log entries for management operations are tagged with `"category": "management"` to distinguish them from data-plane audit entries. See Section 16 for audit log query endpoints.

---

## 3. API Conventions

### 3.1 Request Format

All requests use HTTP/2 over TLS 1.3. Request bodies for `POST`, `PUT`, and `PATCH` operations use `Content-Type: application/json`. Query parameters are used for filtering, pagination, and optional modifiers.

Every request must include an authentication header (one of `Authorization: Bearer <token>`, `Authorization: KEINFS-HMAC-SHA256 ...`, or mTLS client certificate) and the standard KeInFS date header `x-keinfs-date` in ISO 8601 format. The optional `x-keinfs-request-id` header allows clients to attach an idempotency key for mutating operations — the coordinator will deduplicate requests with the same idempotency key within a 24-hour window.

### 3.2 Response Format

All responses use `Content-Type: application/json`. Successful responses return HTTP 2xx status codes. The response body for single-resource endpoints is the resource object directly. The response body for collection endpoints follows a pagination envelope:

```json
{
    "items": [ ... ],
    "pagination": {
        "total": 142,
        "limit": 50,
        "offset": 0,
        "next": "/mgmt/v1/nodes?offset=50&limit=50"
    }
}
```

### 3.3 Pagination

Collection endpoints support `offset` and `limit` query parameters. The default page size is 50. The maximum page size is 1000. The `pagination` object in the response includes a `next` URL for convenience. For very large collections (e.g., millions of objects in audit logs), cursor-based pagination is available via the `cursor` parameter, which is an opaque token returned in the `pagination.cursor` field.

### 3.4 Filtering and Sorting

Collection endpoints support filtering via query parameters specific to each resource type (documented per-endpoint). Sorting is controlled by the `sort` query parameter, which accepts a comma-separated list of fields with optional `-` prefix for descending order (e.g., `sort=-created_at,name`).

### 3.5 Long-Running Operations

Operations that may take more than a few seconds to complete (e.g., node drain, cluster-wide format, drive eviction) return immediately with HTTP `202 Accepted` and a JSON body containing an operation resource:

```json
{
    "operation_id": "op-01HXYZ...",
    "status": "running",
    "type": "node.drain",
    "target": "sn-042",
    "progress": {
        "chunks_migrated": 0,
        "chunks_total": 145832,
        "percent": 0.0
    },
    "created_at": "2026-02-19T10:30:05Z",
    "status_url": "/mgmt/v1/operations/op-01HXYZ..."
}
```

The client polls the `status_url` to monitor progress. The operation resource is retained in the cluster's metadata store for 7 days after completion for post-hoc review.

### 3.6 Error Responses

Error responses use standard HTTP status codes and include a structured JSON body:

```json
{
    "error": {
        "code": "QUOTA_EXCEEDED",
        "message": "Project 'llm-v3' capacity quota of 50 TB would be exceeded.",
        "details": {
            "dimension": "capacity",
            "level": "project",
            "project": "llm-v3",
            "limit_bytes": 54975581388800,
            "current_bytes": 53687091200000,
            "requested_bytes": 1073741824
        },
        "request_id": "req-01HABCD...",
        "trace_id": "trace-01HEFGH..."
    }
}
```

The `code` field is a machine-readable error identifier from the KeInFS Error Catalogue (Appendix A). The `message` field is a human-readable explanation. The `details` field contains error-specific structured data. The `request_id` and `trace_id` enable correlation with the audit log and distributed tracing system.

### 3.7 Idempotency

All `PUT` operations are inherently idempotent (setting a resource to a desired state). `POST` operations that create resources accept an `x-keinfs-request-id` header as an idempotency key. If a coordinator receives a `POST` with an idempotency key it has seen within the last 24 hours, it returns the cached response from the original request rather than creating a duplicate resource.

`DELETE` operations are idempotent: deleting an already-deleted resource returns `204 No Content` (not `404 Not Found`).

---

## 4. CLI Global Configuration

### 4.1 Configuration File

`keinctl` reads its configuration from `$HOME/.keinfs/config.toml` (or the path specified by `--config`). The configuration file specifies the coordinator endpoint list, default credentials, and output preferences.

```toml
# ~/.keinfs/config.toml

# Coordinator endpoints — the CLI tries these in order, failing over automatically.
# A single coordinator is valid; multiple coordinators provide resilience.
coordinators = [
    "https://coord-001.keinfs.local:8443",
    "https://coord-002.keinfs.local:8443",
    "https://coord-003.keinfs.local:8443",
]

# Authentication — choose one method
[auth]
method = "hmac"                                # "hmac", "bearer", or "mtls"
access_key = "AKID-01HXYZ..."                  # For HMAC method
secret_key = "sk-..."                          # For HMAC method
# token = "eyJ..."                             # For bearer method
# client_cert = "/path/to/client.crt"          # For mTLS method
# client_key = "/path/to/client.key"           # For mTLS method

# TLS
[tls]
ca_cert = "/path/to/ca.crt"                   # Cluster CA certificate (for self-signed CAs)
# insecure_skip_verify = false                 # Never use in production

# Defaults
[defaults]
tenant = "acme"                                # Default --tenant value
team = "ml-research"                           # Default --team value
output = "table"                               # "table", "json", "toml"
```

### 4.2 Environment Variables

Every configuration field can be overridden by an environment variable. Environment variables take precedence over the configuration file, and CLI flags take precedence over both.

| Environment Variable | Configuration Field | CLI Flag |
|---|---|---|
| `KEINFS_COORDINATORS` | `coordinators` | `--coordinators` |
| `KEINFS_ACCESS_KEY` | `auth.access_key` | `--access-key` |
| `KEINFS_SECRET_KEY` | `auth.secret_key` | `--secret-key` |
| `KEINFS_TOKEN` | `auth.token` | `--token` |
| `KEINFS_CA_CERT` | `tls.ca_cert` | `--ca-cert` |
| `KEINFS_TENANT` | `defaults.tenant` | `--tenant` |
| `KEINFS_TEAM` | `defaults.team` | `--team` |
| `KEINFS_OUTPUT` | `defaults.output` | `--output` or `-o` |

The `KEINFS_COORDINATORS` environment variable accepts a comma-separated list of coordinator URLs (e.g., `https://coord-001:8443,https://coord-002:8443`). A single URL is valid.

### 4.3 Global CLI Flags

These flags are available on every `keinctl` command:

| Flag | Description |
|---|---|
| `--coordinators <url,...>` | Comma-separated list of coordinator endpoint URLs |
| `--access-key <key>` | HMAC access key ID |
| `--secret-key <key>` | HMAC secret key |
| `--token <token>` | Bearer token |
| `--ca-cert <path>` | CA certificate for TLS verification |
| `--config <path>` | Configuration file path |
| `--output <format>` or `-o` | Output format: `table`, `json`, `toml` |
| `--quiet` or `-q` | Suppress output; exit code only |
| `--verbose` or `-v` | Show HTTP request/response details (for debugging) |
| `--dry-run` | Show the API request that would be sent without executing it |
| `--confirm` | Skip interactive confirmation prompts (for automation) |
| `--timeout <duration>` | Request timeout (default: `30s`, format: `10s`, `2m`, `1h`) |

### 4.4 Credential Resolution Order

When `keinctl` needs credentials, it resolves them in the following order, using the first available source: CLI flags, then environment variables, then configuration file, then (on supported platforms) the OS keychain. If no credentials are found, the command fails with a clear error message identifying which authentication methods are available.

---

## 5. Cluster Management

Cluster management endpoints provide global visibility into the KeInFS cluster's health, topology, and configuration. These operations require the `cluster-admin` or `viewer` role at cluster scope.

### 5.1 Get Cluster Status

Returns a summary of cluster health, including node counts, aggregate capacity, active rebuild operations, and any degraded objects.

**API:**

```
GET /mgmt/v1/cluster/status
```

**Response (200 OK):**

```json
{
    "cluster_id": "prod-us-east",
    "status": "healthy",
    "nodes": {
        "total": 120,
        "healthy": 118,
        "degraded": 1,
        "dead": 1,
        "draining": 0
    },
    "drives": {
        "total": 960,
        "healthy": 951,
        "failed": 5,
        "draining": 4
    },
    "capacity": {
        "total_bytes": 3686400000000000,
        "used_bytes": 2209920000000000,
        "available_bytes": 1476480000000000,
        "utilization_percent": 59.9
    },
    "objects": {
        "total": 48293841,
        "total_versions": 52841293,
        "degraded": 142,
        "rebuilding": 89
    },
    "rebuild": {
        "active": true,
        "chunks_remaining": 12483,
        "estimated_completion": "2026-02-19T11:15:00Z",
        "bandwidth_bytes_per_sec": 524288000
    },
    "namespace": {
        "nodes": 5,
        "healthy": 5,
        "leader_regions": 48,
        "storage_used_bytes": 42949672960
    },
    "coordinators": {
        "total": 8,
        "healthy": 8,
        "active_connections": 32481
    },
    "s3_ingress": {
        "nodes_enabled": 96,
        "healthy": 96,
        "active_connections": 4412
    },
    "version": {
        "coordinator": "0.1.0",
        "storage_node": "0.1.0",
        "format_version": 1
    },
    "uptime_seconds": 2592000
}
```

**CLI:**

```bash
keinctl cluster status
```

```
Cluster: prod-us-east                          Status: HEALTHY

Nodes:      120 total    118 healthy    1 degraded    1 dead
Drives:     960 total    951 healthy    5 failed      4 draining
Coordinators: 8 total      8 healthy    32,481 connections
S3 ingress:  96 nodes     96 healthy     4,412 connections

Capacity:   3.69 PB total    2.21 PB used    1.48 PB free    59.9% utilized

Objects:    48,293,841 total    142 degraded    89 rebuilding
Rebuild:    ACTIVE — 12,483 chunks remaining — ETA 11:15 UTC (500 MB/s)

Namespace:  5/5 nodes healthy    43 GB metadata
Version:    coordinator 0.1.0    storage-node 0.1.0    format v1
Uptime:     30 days
```

### 5.2 Get Cluster Topology

Returns the full topology of the cluster, including all nodes, their drives, failure domains (rack, power zone), and the placement ring.

**API:**

```
GET /mgmt/v1/cluster/topology
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `depth` | string | Level of detail: `nodes` (default), `drives`, or `full` |
| `failure_domain` | string | Filter by failure domain (e.g., `rack-A1`) |
| `status` | string | Filter by node status: `healthy`, `degraded`, `dead`, `draining` |

**Response (200 OK):**

```json
{
    "failure_domains": [
        {
            "type": "rack",
            "name": "rack-A1",
            "power_zone": "pz-1",
            "nodes": [
                {
                    "node_id": "sn-001",
                    "status": "healthy",
                    "drives": 8,
                    "capacity_bytes": 30720000000000,
                    "used_bytes": 18432000000000
                }
            ]
        }
    ],
    "placement_ring": {
        "algorithm": "rendezvous_hrw",
        "nodes_in_ring": 118,
        "last_rebalance": "2026-02-18T04:00:00Z"
    }
}
```

**CLI:**

```bash
keinctl cluster topology
keinctl cluster topology --depth drives
keinctl cluster topology --failure-domain rack-A1
```

### 5.3 Get Cluster Configuration

Returns the current global cluster configuration.

**API:**

```
GET /mgmt/v1/cluster/config
```

**Response (200 OK):**

```json
{
    "cluster_id": "prod-us-east",
    "default_ec_profile": "resilient",
    "rebuild": {
        "max_bandwidth_per_node_bytes": 524288000,
        "max_concurrent_operations": 50,
        "detection_timeout_seconds": 30,
        "io_priority": "low"
    },
    "scrub": {
        "enabled": true,
        "interval_days": 14,
        "max_bandwidth_per_node_bytes": 104857600
    },
    "gc": {
        "enabled": true,
        "orphan_ttl_hours": 24,
        "sweep_interval_minutes": 60
    },
    "tracing": {
        "default_sample_rate": 0.01,
        "error_sample_rate": 1.0,
        "slow_request_sample_rate": 1.0,
        "slow_request_threshold_ms": 200
    },
    "audit": {
        "enabled": true,
        "retention_days": 90,
        "log_reads": false,
        "external_sink": null
    },
    "rate_limiting": {
        "connection_rate_per_ip": 100,
        "max_concurrent_streams": 256,
        "idle_timeout_seconds": 300
    },
    "latency_profiles": {
        "default": "balanced",
        "hot-core": {
            "busy_poll": true,
            "dedicated_poll_core": true
        }
    },
    "native_path": {
        "default_read_mode": "pull",
        "require_direct_storage_reachability": true
    },
    "heartbeat_interval_seconds": 5,
    "placement": {
        "failure_domain_constraint": "rack",
        "rebalance_threshold_percent": 15
    }
}
```

**CLI:**

```bash
keinctl cluster config show
keinctl cluster config show --json
```

### 5.4 Update Cluster Configuration

Updates one or more fields in the global cluster configuration. Only specified fields are modified; unspecified fields retain their current values (merge semantics).

**API:**

```
PATCH /mgmt/v1/cluster/config
```

**Request body:**

```json
{
    "rebuild": {
        "max_bandwidth_per_node_bytes": 209715200
    },
    "tracing": {
        "default_sample_rate": 0.05
    }
}
```

**Response (200 OK):** Returns the complete updated configuration (same schema as GET).

**CLI:**

```bash
keinctl cluster config set rebuild.max_bandwidth_per_node_bytes 209715200
keinctl cluster config set tracing.default_sample_rate 0.05
keinctl cluster config set --json '{"rebuild": {"max_bandwidth_per_node_bytes": 209715200}}'
```

**Required role:** `cluster-admin`

### 5.5 Bulk Format Cluster Devices

Initiates formatting of all unformatted raw block devices across all storage nodes (or a specified subset). This is a long-running operation.

**API:**

```
POST /mgmt/v1/cluster/format
```

**Request body:**

```json
{
    "nodes": ["sn-001", "sn-002", "sn-003"],
    "granule_size_bytes": 1048576,
    "allocator_policy": "large-object",
    "small_object_threshold_bytes": 262144,
    "packed_container_size_bytes": 8388608,
    "dry_run": false,
    "confirm": true
}
```

If `nodes` is omitted or empty, all registered storage nodes are targeted. If allocator parameters are omitted, the cluster defaults are used. `allocator_policy` selects the placement bias (`large-object`, `mixed`, or `small-object`) that buckets and storage classes inherit unless overridden at a higher policy layer. The `confirm` field must be `true` for the operation to proceed — this prevents accidental cluster-wide formatting from an errant API call.

**Response (202 Accepted):** Returns an operation resource (Section 3.5).

**CLI:**

```bash
# Format all unformatted devices across the cluster
keinctl cluster format --confirm

# Format specific nodes
keinctl cluster format --nodes sn-001,sn-002,sn-003 --confirm

# Explicit allocator profile
keinctl cluster format --confirm \
    --granule-size 1MiB \
    --allocator-policy large-object \
    --small-object-threshold 256KiB \
    --packed-container-size 8MiB

# Dry run — show what would happen
keinctl cluster format --dry-run
```

**Required role:** `cluster-admin`

---

## 6. Node Management

Node management endpoints control the lifecycle of storage nodes in the cluster. Storage nodes are the machines that run the `kst` storage-target daemon and host raw block devices containing KeInFS chunk data.

### 6.1 List Nodes

Returns all storage nodes registered in the cluster.

**API:**

```
GET /mgmt/v1/nodes
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `status` | string | Filter: `healthy`, `degraded`, `dead`, `draining` |
| `failure_domain` | string | Filter by failure domain name |
| `sort` | string | Sort field (default: `node_id`) |
| `limit` | integer | Page size (default: 50) |
| `offset` | integer | Page offset |

**Response (200 OK):**

```json
{
    "items": [
        {
            "node_id": "sn-001",
            "status": "healthy",
            "failure_domain": { "rack": "rack-A1", "power_zone": "pz-1" },
            "drives": {
                "total": 8,
                "healthy": 8,
                "failed": 0
            },
            "capacity": {
                "total_bytes": 30720000000000,
                "used_bytes": 18432000000000,
                "utilization_percent": 60.0
            },
            "network": {
                "bandwidth_available_bps": 100000000000,
                "connections_active": 847
            },
            "last_heartbeat": "2026-02-19T10:30:05Z",
            "registered_at": "2026-01-15T08:00:00Z",
            "version": "0.1.0"
        }
    ],
    "pagination": { "total": 120, "limit": 50, "offset": 0 }
}
```

**CLI:**

```bash
keinctl node list
keinctl node list --status healthy
keinctl node list --failure-domain rack-A1
```

```
NODE       STATUS     DRIVES    CAPACITY      USED      UTIL%   LAST HEARTBEAT
sn-001     healthy      8/8     30.72 TB    18.43 TB    60.0%   5s ago
sn-002     healthy      8/8     30.72 TB    19.01 TB    61.9%   3s ago
sn-003     degraded     7/8     26.88 TB    17.20 TB    64.0%   4s ago
sn-004     dead         0/8       —           —           —     12m ago ⚠
...
```

### 6.2 Get Node Status

Returns detailed status for a single storage node, including per-drive health, SMART data, I/O statistics, allocator utilization, native chunk-service state, and S3 ingress state where enabled.

**API:**

```
GET /mgmt/v1/nodes/{node_id}
```

**Response (200 OK):**

```json
{
    "node_id": "sn-042",
    "status": "healthy",
    "failure_domain": { "rack": "rack-C2", "power_zone": "pz-3" },
    "hostname": "storage-042.keinfs.local",
    "ip_addresses": ["10.0.42.1"],
    "registered_at": "2026-01-15T08:00:00Z",
    "version": "0.1.0",
    "os": "Ubuntu 24.04 LTS",
    "kernel": "6.8.0-45-generic",
    "cpu": {
        "model": "Intel Xeon w9-3595X",
        "cores": 64,
        "simd": "AVX-512 + GFNI"
    },
    "memory_bytes": 549755813888,
    "drives": [
        {
            "drive_id": "nvme0",
            "device": "/dev/nvme0n1",
            "uuid": "a1b2c3d4-e5f6-7890-abcd-ef0123456789",
            "status": "healthy",
            "model": "Samsung PM1733a 3.84TB",
            "firmware": "2.1.0",
            "capacity_bytes": 3840000000000,
            "used_bytes": 2304000000000,
            "utilization_percent": 60.0,
            "smart": {
                "healthy": true,
                "temperature_celsius": 42,
                "wear_leveling_percent": 8,
                "power_on_hours": 8760,
                "unsafe_shutdowns": 0,
                "media_errors": 0
            },
            "io_stats": {
                "queue_depth": 12,
                "read_bytes_per_sec": 1073741824,
                "write_bytes_per_sec": 536870912,
                "read_iops": 45000,
                "write_iops": 22000,
                "read_latency_p50_us": 85,
                "read_latency_p99_us": 420,
                "write_latency_p50_us": 110,
                "write_latency_p99_us": 850
            },
            "allocator": {
                "granule_size_bytes": 1048576,
                "policy": "large-object",
                "small_object_threshold_bytes": 262144,
                "packed_containers": {
                    "total": 18342,
                    "used": 11103,
                    "utilization_percent": 60.5
                },
                "extent_pool": {
                    "allocated_clusters": 562500000,
                    "free_clusters": 375000000,
                    "utilization_percent": 60.0
                }
            },
            "chunk_count": 4094838,
            "formatted_at": "2026-01-15T08:05:12Z"
        }
    ],
    "network": {
        "interfaces": [
            {
                "name": "eth0",
                "speed_bps": 100000000000,
                "rx_bytes_per_sec": 5368709120,
                "tx_bytes_per_sec": 3221225472
            }
        ],
        "connections_active": 847,
        "latency_profile": "hot-core-busy-poll"
    },
    "services": {
        "chunk_service": { "status": "healthy", "busy_poll": true },
        "s3_ingress": { "enabled": true, "status": "healthy", "active_connections": 119 }
    },
    "last_heartbeat": "2026-02-19T10:30:05Z"
}
```

**CLI:**

```bash
keinctl node status sn-042
```

### 6.3 Drain Node

Initiates a graceful drain of a storage node, migrating all chunks to other healthy nodes before the node is taken offline for maintenance. This is a long-running operation.

**API:**

```
POST /mgmt/v1/nodes/{node_id}/drain
```

**Request body (optional):**

```json
{
    "max_bandwidth_bytes_per_sec": 1073741824,
    "reason": "Scheduled firmware update"
}
```

**Response (202 Accepted):** Returns an operation resource.

**CLI:**

```bash
keinctl node drain sn-042
keinctl node drain sn-042 --max-bandwidth 1GB/s --reason "Firmware update"
```

**Required role:** `cluster-admin`

### 6.4 Rejoin Node

Re-enables a drained node, allowing new chunks to be placed on it. The rebalancer will gradually fill the node with chunks from overloaded nodes.

**API:**

```
POST /mgmt/v1/nodes/{node_id}/rejoin
```

**Response (200 OK):**

```json
{
    "node_id": "sn-042",
    "status": "healthy",
    "message": "Node re-enabled for chunk placement."
}
```

**CLI:**

```bash
keinctl node rejoin sn-042
```

**Required role:** `cluster-admin`

---

## 7. Drive Management

Drive management endpoints control the lifecycle of individual raw block devices within storage nodes — from formatting through eviction and destruction. These operations map to the drive UUID lifecycle defined in the design specification (active → draining → evicted → removed).

### 7.1 List Drives

Returns all drives across the cluster, or drives for a specific node.

**API:**

```
GET /mgmt/v1/drives
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `node_id` | string | Filter by storage node |
| `status` | string | Filter: `healthy`, `failed`, `draining`, `evicted` |
| `sort` | string | Sort field (default: `node_id,drive_id`) |
| `limit` | integer | Page size |
| `offset` | integer | Page offset |

**CLI:**

```bash
keinctl drive list
keinctl drive list --node sn-042
keinctl drive list --status failed
```

### 7.2 Get Drive Status

Returns detailed status for a single drive, identified by its UUID or by node and drive ID.

**API:**

```
GET /mgmt/v1/drives/{drive_uuid}
GET /mgmt/v1/nodes/{node_id}/drives/{drive_id}
```

Both paths return the same drive object (the schema shown in the `drives` array of Section 6.2). The UUID path is useful when you have a UUID from a log or alert but do not know which node the drive belongs to.

**CLI:**

```bash
keinctl drive status sn-042 nvme0
keinctl drive status --uuid a1b2c3d4-e5f6-7890-abcd-ef0123456789
```

### 7.3 Format Drive

Formats a raw block device on a storage node, writing the KeInFS superblock, allocator metadata, and the raw KIX arena header. The device is formatted as a single raw KeInFS span with no filesystem partition. This is a node-local operation executed by the `kst` storage-target daemon on the target node; the Management API authorizes it, records it, and orchestrates it from the coordinator.

**API:**

```
POST /mgmt/v1/nodes/{node_id}/drives/format
```

**Request body:**

```json
{
    "device": "/dev/nvme0n1",
    "granule_size_bytes": 1048576,
    "allocator_policy": "large-object",
    "small_object_threshold_bytes": 262144,
    "packed_container_size_bytes": 8388608,
    "force": false,
    "secure_erase": false,
    "dry_run": false
}
```

If `force` is `false` (the default), the operation refuses to format a device that already has a KeInFS superblock or a recognized filesystem signature. If `secure_erase` is `true`, an NVMe Secure Erase command is issued before formatting.

**Response (200 OK for immediate completion, 202 Accepted if secure erase is in progress):**

```json
{
    "drive_uuid": "a1b2c3d4-e5f6-7890-abcd-ef0123456789",
    "node_id": "sn-042",
    "device": "/dev/nvme0n1",
    "capacity_bytes": 3840000000000,
    "format_version": 1,
    "allocator": {
        "granule_size_bytes": 1048576,
        "policy": "large-object",
        "small_object_threshold_bytes": 262144,
        "packed_container_size_bytes": 8388608,
        "extent_pool_bytes": 3827000000000
    },
    "metadata_overhead_bytes": 2097152,
    "format_duration_ms": 312,
    "registered": true
}
```

**CLI:**

```bash
# Format a single device on a node
keinctl drive format sn-042 /dev/nvme0n1

# Explicit allocator profile
keinctl drive format sn-042 /dev/nvme0n1 \
    --granule-size 1MiB \
    --allocator-policy large-object \
    --small-object-threshold 256KiB \
    --packed-container-size 8MiB

# Dry run
keinctl drive format sn-042 /dev/nvme0n1 --dry-run

# Force re-format
keinctl drive format sn-042 /dev/nvme0n1 --force

# Secure erase + format
keinctl drive format sn-042 /dev/nvme0n1 --force --secure-erase
```

**Required role:** `cluster-admin`

### 7.4 Inspect Drive

Returns the on-disk superblock and allocator layout of a formatted drive without modifying it. This is useful for verifying a format operation or examining a drive's configuration.

**API:**

```
GET /mgmt/v1/nodes/{node_id}/drives/{drive_id}/inspect
```

**Response (200 OK):** Returns the superblock fields (magic, format version, UUID, device size, creation timestamp, granule size, allocator policy, packed-container configuration, KIX arena summary, CRC32C integrity) and the current allocator summary.

**CLI:**

```bash
keinctl drive inspect sn-042 nvme0
```

### 7.5 Evict Drive (Graceful)

Initiates graceful eviction of a drive, draining all chunks to other drives before decommissioning it. The drive continues to serve reads during the drain but accepts no new chunk writes.

**API:**

```
POST /mgmt/v1/drives/{drive_uuid}/evict
```

**Request body:**

```json
{
    "mode": "drain",
    "reason": "Approaching wear-out threshold"
}
```

**Response (202 Accepted):** Returns an operation resource tracking the drain progress.

**CLI:**

```bash
keinctl drive evict a1b2c3d4-... --drain
keinctl drive evict a1b2c3d4-... --drain --reason "Wear leveling at 92%"
```

### 7.6 Evict Drive (Forced)

Immediately evicts a failed drive that cannot participate in a drain. Triggers EC rebuild for all chunks that had copies on the evicted drive.

**API:**

```
POST /mgmt/v1/drives/{drive_uuid}/evict
```

**Request body:**

```json
{
    "mode": "force",
    "reason": "Drive failed — SMART reports media errors"
}
```

**Response (202 Accepted):** Returns an operation resource tracking the rebuild.

**CLI:**

```bash
keinctl drive evict a1b2c3d4-... --force
```

**Required role:** `cluster-admin`

### 7.7 Destroy Drive

Destroys the superblock, bitmaps, and partition table of an evicted drive, rendering it inert. The drive must be in the `evicted` state in the cluster's drive registry before destruction is allowed (unless `--force` is used, which requires interactive confirmation or the `--confirm` flag).

**API:**

```
POST /mgmt/v1/drives/{drive_uuid}/destroy
```

**Request body:**

```json
{
    "confirm_uuid": "a1b2c3d4-e5f6-7890-abcd-ef0123456789",
    "force": false
}
```

The `confirm_uuid` field must match the drive's UUID — this is a safety check analogous to the interactive "type the UUID to confirm" prompt in the CLI. If `force` is `true`, the eviction state check is bypassed (dangerous — can destroy a live drive).

**Response (200 OK):**

```json
{
    "drive_uuid": "a1b2c3d4-e5f6-7890-abcd-ef0123456789",
    "status": "destroyed",
    "message": "Superblock, bitmaps, and partition table zeroed. NVMe Secure Erase issued. Drive UUID removed from cluster registry."
}
```

**CLI:**

```bash
keinctl drive destroy a1b2c3d4-...
# Interactive prompt: "Type the drive UUID to confirm destruction: "

keinctl drive destroy a1b2c3d4-... --confirm
# Skips the interactive prompt (for automation)
```

**Required role:** `cluster-admin`

---

## 8. Bucket Management

Buckets are storage roots inside the namespace hierarchy, not the whole namespace. A tenant-scoped namespace can contain multiple projects, teams, or groups, and those entries can contain multiple buckets. Each bucket is associated with a namespace, a parent domain entry, an EC profile (storage class), and optional access policies. Bucket operations require `tenant-admin` (or higher) at the appropriate scope.

### 8.1 Create Bucket

**API:**

```
POST /mgmt/v1/buckets
```

**Request body:**

```json
{
    "name": "training-data",
    "tenant": "acme",
    "team": "ml-research",
    "project": "llm-v3",
    "ec_profile": "resilient",
    "allocator_policy": "large-object",
    "native_read_mode": "pull",
    "versioning": true,
    "audit_reads": false,
    "tags": {
        "environment": "production",
        "cost-center": "ai-research"
    }
}
```

The `ec_profile` field references a named EC profile defined in the cluster configuration (see Section 9). If omitted, the cluster default EC profile is used. `allocator_policy` selects the storage-layout bias for new objects in the bucket (`large-object`, `mixed`, or `small-object`). `native_read_mode` sets the default native read hint (`pull` by default, `push` when explicitly selected). `versioning` controls whether previous versions of objects are retained on overwrite (default: `true`). `audit_reads` controls whether GET operations to this bucket are recorded in the audit log (default: `false`, since training data reads at high throughput would generate excessive audit volume).

The `team` and `project` fields are optional. If specified, the bucket is owned by that team or project, and access is governed by the RBAC policies at that scope. If only `tenant` is specified, the bucket is tenant-wide.

**Response (201 Created):**

```json
{
    "name": "training-data",
    "tenant": "acme",
    "team": "ml-research",
    "project": "llm-v3",
    "ec_profile": "resilient",
    "allocator_policy": "large-object",
    "native_read_mode": "pull",
    "versioning": true,
    "audit_reads": false,
    "created_at": "2026-02-19T10:30:05Z",
    "created_by": "user:alice@acme.com",
    "object_count": 0,
    "size_bytes": 0,
    "tags": {
        "environment": "production",
        "cost-center": "ai-research"
    }
}
```

**CLI:**

```bash
keinctl bucket create training-data \
    --tenant acme \
    --team ml-research \
    --project llm-v3 \
    --ec-profile resilient \
    --allocator-policy large-object \
    --native-read-mode pull \
    --versioning \
    --tag environment=production \
    --tag cost-center=ai-research
```

### 8.2 List Buckets

**API:**

```
GET /mgmt/v1/buckets
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `tenant` | string | Filter by tenant (required unless `cluster-admin`) |
| `team` | string | Filter by team |
| `project` | string | Filter by project |
| `sort` | string | Sort field (default: `name`) |

**CLI:**

```bash
keinctl bucket list --tenant acme
keinctl bucket list --tenant acme --team ml-research
```

### 8.3 Get Bucket

**API:**

```
GET /mgmt/v1/buckets/{bucket_name}
```

**Response (200 OK):** Returns the full bucket configuration (same schema as the create response, plus current usage statistics).

**CLI:**

```bash
keinctl bucket show training-data
```

### 8.4 Update Bucket Configuration

**API:**

```
PATCH /mgmt/v1/buckets/{bucket_name}
```

**Request body:** Only specified fields are updated.

```json
{
    "allocator_policy": "mixed",
    "native_read_mode": "push",
    "audit_reads": true,
    "tags": {
        "environment": "production",
        "cost-center": "ai-research",
        "team-lead": "alice"
    }
}
```

The `ec_profile` of an existing bucket cannot be changed (doing so would require re-encoding all existing objects). `allocator_policy` and `native_read_mode` can be changed for future writes and future native read hints, but they do not retroactively rewrite existing objects already placed on disk. To change EC profiles, create a new bucket with the desired profile and migrate objects.

**CLI:**

```bash
keinctl bucket update training-data --allocator-policy mixed --native-read-mode push
keinctl bucket update training-data --audit-reads true
keinctl bucket update training-data --tag team-lead=alice
```

### 8.5 Delete Bucket

Deletes a bucket. The bucket must be empty (contain zero objects) unless `--force` is specified, in which case all objects and versions are deleted first.

**API:**

```
DELETE /mgmt/v1/buckets/{bucket_name}
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `force` | boolean | Delete all objects before deleting the bucket |

**Response (204 No Content)** on success. Returns `409 Conflict` if the bucket is not empty and `force` is not set.

**CLI:**

```bash
keinctl bucket delete training-data
keinctl bucket delete training-data --force --confirm
```

---

## 9. Storage Classes and EC Profile Management

EC profiles define the erasure coding parameters and placement constraints for buckets. Each profile specifies the number of data chunks (k), parity chunks (m), the stripe size, and failure domain constraints. Profiles are cluster-wide named configurations stored in the cluster metadata.

### 9.1 List EC Profiles

**API:**

```
GET /mgmt/v1/ec-profiles
```

**Response (200 OK):**

```json
{
    "items": [
        {
            "name": "kamikaze",
            "description": "Maximum throughput, no parity protection. Use only for reproducible data.",
            "data_chunks": 8,
            "parity_chunks": 0,
            "stripe_size_bytes": 4194304,
            "failure_domain": "node",
            "min_nodes": 8,
            "storage_overhead": 1.0
        },
        {
            "name": "efficient",
            "description": "Balanced storage efficiency for large-scale training data.",
            "data_chunks": 12,
            "parity_chunks": 3,
            "stripe_size_bytes": 4194304,
            "failure_domain": "rack",
            "min_nodes": 15,
            "storage_overhead": 1.25
        },
        {
            "name": "resilient",
            "description": "Higher parity for critical data such as model checkpoints.",
            "data_chunks": 8,
            "parity_chunks": 4,
            "stripe_size_bytes": 4194304,
            "failure_domain": "rack",
            "min_nodes": 12,
            "storage_overhead": 1.5
        },
        {
            "name": "replicated",
            "description": "Triple replication for metadata-heavy workloads or small files.",
            "data_chunks": 1,
            "parity_chunks": 2,
            "stripe_size_bytes": 1048576,
            "failure_domain": "rack",
            "min_nodes": 3,
            "storage_overhead": 3.0
        }
    ]
}
```

**CLI:**

```bash
keinctl ec-profile list
```

```
PROFILE       K    M    STRIPE    FAILURE DOMAIN    OVERHEAD    MIN NODES
kamikaze      8    0    4 MiB     node              1.00x         8
efficient    12    3    4 MiB     rack              1.25x        15
resilient     8    4    4 MiB     rack              1.50x        12
replicated    1    2    1 MiB     rack              3.00x         3
```

### 9.2 Create EC Profile

**API:**

```
POST /mgmt/v1/ec-profiles
```

**Request body:**

```json
{
    "name": "archive",
    "description": "High-density archival with minimal parity.",
    "data_chunks": 16,
    "parity_chunks": 2,
    "stripe_size_bytes": 4194304,
    "failure_domain": "rack"
}
```

**CLI:**

```bash
keinctl ec-profile create archive \
    --data-chunks 16 \
    --parity-chunks 2 \
    --stripe-size 4MiB \
    --failure-domain rack \
    --description "High-density archival with minimal parity."
```

**Required role:** `cluster-admin`

### 9.3 Get EC Profile

**API:**

```
GET /mgmt/v1/ec-profiles/{name}
```

**CLI:**

```bash
keinctl ec-profile show resilient
```

### 9.4 Delete EC Profile

An EC profile can only be deleted if no buckets reference it.

**API:**

```
DELETE /mgmt/v1/ec-profiles/{name}
```

**Response:** `204 No Content` on success, `409 Conflict` if buckets still reference the profile.

**CLI:**

```bash
keinctl ec-profile delete archive
```

**Required role:** `cluster-admin`

---

## 10. Tenant, Team, and Project Management

The KeInFS multi-tenant hierarchy follows the design specification's four-level model: tenant → team → project → job. The first three levels are provisioned and managed via the Management API. Jobs are transient and identified by their attribution context at runtime (see Section 18).

### 10.1 Tenant Management

Tenants are the top-level organizational boundary, representing a company, division, or major business unit on a shared KeInFS cluster.

#### Create Tenant

**API:**

```
POST /mgmt/v1/tenants
```

**Request body:**

```json
{
    "name": "acme",
    "display_name": "Acme Corporation",
    "admin_contact": "storage-admin@acme.com",
    "quota": {
        "capacity_bytes": 549755813888000,
        "object_count": 100000000,
        "bandwidth_bytes_per_sec": 25000000000,
        "request_rate_per_sec": 100000,
        "concurrent_connections": 10000
    },
    "tags": {
        "billing-id": "ACME-2026-01",
        "tier": "enterprise"
    }
}
```

The quota block specifies the maximum resource consumption for the entire tenant across all teams, projects, and jobs. All five quota dimensions are optional at creation time and can be set or adjusted later (see Section 11).

**Response (201 Created):** Returns the tenant object with `created_at` and a generated `tenant_id`.

**CLI:**

```bash
keinctl tenant create acme \
    --display-name "Acme Corporation" \
    --admin-contact storage-admin@acme.com \
    --capacity 500TB \
    --bandwidth 200Gbps \
    --tag billing-id=ACME-2026-01
```

**Required role:** `cluster-admin`

#### List Tenants

**API:**

```
GET /mgmt/v1/tenants
```

**CLI:**

```bash
keinctl tenant list
```

#### Get Tenant

**API:**

```
GET /mgmt/v1/tenants/{tenant_name}
```

**CLI:**

```bash
keinctl tenant show acme
```

#### Update Tenant

**API:**

```
PATCH /mgmt/v1/tenants/{tenant_name}
```

**CLI:**

```bash
keinctl tenant update acme --display-name "Acme Corp (Merged)"
```

#### Delete Tenant

A tenant can only be deleted if it contains no teams, no buckets, and no active credentials. This is an intentionally high-friction operation to prevent accidental data loss.

**API:**

```
DELETE /mgmt/v1/tenants/{tenant_name}
```

**CLI:**

```bash
keinctl tenant delete acme --confirm
```

**Required role:** `cluster-admin`

### 10.2 Team Management

Teams exist within a tenant and represent a functional group (e.g., "ml-research", "ml-platform", "data-engineering").

#### Create Team

**API:**

```
POST /mgmt/v1/tenants/{tenant_name}/teams
```

**Request body:**

```json
{
    "name": "ml-research",
    "display_name": "Machine Learning Research",
    "admin_contact": "ml-leads@acme.com",
    "quota": {
        "capacity_bytes": 219902325555200,
        "bandwidth_bytes_per_sec": 12500000000
    }
}
```

Team quotas cannot exceed the parent tenant's quota. The coordinator validates this constraint at creation time and on every quota update.

**CLI:**

```bash
keinctl team create ml-research \
    --tenant acme \
    --display-name "Machine Learning Research" \
    --capacity 200TB \
    --bandwidth 100Gbps
```

**Required role:** `tenant-admin` or higher for the specified tenant.

#### List Teams

**API:**

```
GET /mgmt/v1/tenants/{tenant_name}/teams
```

**CLI:**

```bash
keinctl team list --tenant acme
```

#### Get, Update, Delete Team

Follow the same pattern as tenant operations, scoped to `/mgmt/v1/tenants/{tenant}/teams/{team}`.

### 10.3 Project Management

Projects exist within a team and represent a specific initiative, experiment, or service (e.g., "llm-v3", "image-classifier-prod", "data-pipeline-q1").

#### Create Project

**API:**

```
POST /mgmt/v1/tenants/{tenant}/teams/{team}/projects
```

**Request body:**

```json
{
    "name": "llm-v3",
    "display_name": "Large Language Model v3 Training",
    "quota": {
        "capacity_bytes": 54975581388800,
        "bandwidth_bytes_per_sec": 6250000000
    }
}
```

**CLI:**

```bash
keinctl project create llm-v3 \
    --tenant acme \
    --team ml-research \
    --display-name "Large Language Model v3 Training" \
    --capacity 50TB \
    --bandwidth 50Gbps
```

**Required role:** `team-admin` or higher for the specified team.

#### List, Get, Update, Delete Project

Follow the same pattern, scoped to `/mgmt/v1/tenants/{tenant}/teams/{team}/projects/{project}`.

---

## 11. Quota Management

Quotas control resource consumption at every level of the tenant → team → project hierarchy. They are enforced in real time at the coordinators, as specified in the design document. The Management API provides endpoints for setting, viewing, and alerting on quota thresholds.

### 11.1 Set Quota

Sets or updates the quota limits for a specific scope (tenant, team, or project). Only specified dimensions are modified; unspecified dimensions retain their current values.

**API:**

```
PUT /mgmt/v1/quotas
```

**Request body:**

```json
{
    "scope": {
        "tenant": "acme",
        "team": "ml-research",
        "project": "llm-v3"
    },
    "limits": {
        "capacity_bytes": 54975581388800,
        "object_count": 10000000,
        "bandwidth_bytes_per_sec": 6250000000,
        "request_rate_per_sec": 50000,
        "concurrent_connections": 2000
    }
}
```

The coordinator validates that the quota at each level does not exceed the parent level. Setting a project quota higher than its team's quota is rejected with a `QUOTA_HIERARCHY_VIOLATION` error.

**Response (200 OK):** Returns the updated quota state including current usage.

**CLI:**

```bash
# Set tenant-level quota
keinctl quota set --tenant acme --capacity 500TB --bandwidth 200Gbps

# Set team-level quota
keinctl quota set --tenant acme --team ml-research --capacity 200TB

# Set project-level quota
keinctl quota set --tenant acme --team ml-research --project llm-v3 \
    --capacity 50TB --bandwidth 50Gbps --request-rate 50000 --connections 2000

# Set individual dimension
keinctl quota set --tenant acme --team ml-research --object-count 50000000
```

**Required role:** `tenant-admin` to set team/project quotas within their tenant; `cluster-admin` to set tenant quotas.

### 11.2 Show Quota

Returns the current quota limits and real-time usage for a specific scope. The `--recursive` flag shows the complete hierarchy below the specified scope.

**API:**

```
GET /mgmt/v1/quotas
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `tenant` | string | Tenant name (required) |
| `team` | string | Team name (optional) |
| `project` | string | Project name (optional) |
| `recursive` | boolean | Include child scopes (default: `false`) |

**Response (200 OK):**

```json
{
    "scope": { "tenant": "acme", "team": "ml-research" },
    "limits": {
        "capacity_bytes": 219902325555200,
        "object_count": 50000000,
        "bandwidth_bytes_per_sec": 12500000000,
        "request_rate_per_sec": 100000,
        "concurrent_connections": 5000
    },
    "usage": {
        "capacity_bytes": 156170924064768,
        "object_count": 12483921,
        "bandwidth_bytes_per_sec_current": 8053063680,
        "request_rate_per_sec_current": 42381,
        "concurrent_connections_current": 1847
    },
    "utilization_percent": {
        "capacity": 71.0,
        "object_count": 25.0,
        "bandwidth": 64.4,
        "request_rate": 42.4,
        "concurrent_connections": 36.9
    },
    "children": [
        {
            "scope": { "tenant": "acme", "team": "ml-research", "project": "llm-v3" },
            "limits": { "capacity_bytes": 54975581388800 },
            "usage": { "capacity_bytes": 42949672960000 },
            "utilization_percent": { "capacity": 78.1 }
        }
    ]
}
```

**CLI:**

```bash
keinctl quota show --tenant acme --recursive
keinctl quota show --tenant acme --team ml-research
keinctl quota show --tenant acme --team ml-research --project llm-v3
```

```
Scope: acme / ml-research

DIMENSION              LIMIT           CURRENT        UTIL%
Capacity              200.00 TB       142.00 TB       71.0%
Object count       50,000,000      12,483,921       25.0%
Bandwidth            100 Gbps        64.4 Gbps       64.4%
Request rate      100,000 ops/s    42,381 ops/s      42.4%
Connections            5,000           1,847          36.9%

Projects:
  llm-v3             50 TB / 42.9 TB used (78.1%)
  image-cls          30 TB / 18.2 TB used (60.7%)
  data-pipeline      80 TB / 52.1 TB used (65.1%)
```

### 11.3 Configure Quota Alerts

Sets alert thresholds for quota dimensions. When usage crosses a threshold, the alert is recorded in the audit log and can optionally trigger a webhook notification.

**API:**

```
PUT /mgmt/v1/quotas/alerts
```

**Request body:**

```json
{
    "scope": {
        "tenant": "acme",
        "team": "ml-research"
    },
    "alerts": [
        {
            "dimension": "capacity",
            "threshold_percent": 80,
            "webhook_url": "https://hooks.slack.com/services/T.../B.../..."
        },
        {
            "dimension": "capacity",
            "threshold_percent": 95,
            "webhook_url": "https://hooks.slack.com/services/T.../B.../..."
        },
        {
            "dimension": "bandwidth",
            "threshold_percent": 90,
            "webhook_url": null
        }
    ]
}
```

**CLI:**

```bash
keinctl quota alert --tenant acme --team ml-research \
    --dimension capacity --threshold 80 \
    --webhook https://hooks.slack.com/services/T.../B.../...

keinctl quota alert --tenant acme --team ml-research \
    --dimension capacity --threshold 95
```

---

## 12. Job Monitoring and Diagnostics

Job monitoring endpoints provide real-time and historical visibility into the I/O behavior of individual jobs running on the cluster. Jobs are identified by their attribution context (see Section 18) and are transient — they exist in the monitoring system for as long as they are active, plus a configurable retention period (default: 7 days) for historical analysis.

### 12.1 List Active Jobs

**API:**

```
GET /mgmt/v1/jobs
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `tenant` | string | Filter by tenant |
| `team` | string | Filter by team |
| `project` | string | Filter by project |
| `status` | string | Filter: `active`, `completed`, `all` (default: `active`) |
| `sort` | string | Sort field (default: `-bandwidth_current`) |

**Response (200 OK):**

```json
{
    "items": [
        {
            "job_id": "train-run-42",
            "project": "llm-v3",
            "team": "ml-research",
            "tenant": "acme",
            "status": "active",
            "started_at": "2026-02-19T08:00:00Z",
            "duration_seconds": 9005,
            "ranks": 16384,
            "current_io": {
                "read_bandwidth_bytes_per_sec": 3078000000000,
                "write_bandwidth_bytes_per_sec": 133143986176,
                "active_streams": 32768
            },
            "lifetime_totals": {
                "read_bytes": 26492528640000000,
                "write_bytes": 1209462790553600,
                "objects_read": 148000000,
                "objects_written": 2840
            },
            "buckets_accessed": ["training-data", "checkpoints"]
        }
    ]
}
```

**CLI:**

```bash
keinctl job list
keinctl job list --tenant acme --project llm-v3
```

### 12.2 Get Job Statistics

Returns real-time and lifetime I/O statistics for a specific job.

**API:**

```
GET /mgmt/v1/jobs/{job_id}
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `tenant` | string | Tenant context (required for disambiguation if job IDs are not globally unique) |
| `project` | string | Project context |

**CLI:**

```bash
keinctl job stats train-run-42
keinctl job stats train-run-42 --tenant acme --project llm-v3
```

Output matches the design specification's example in Section 10.5.

### 12.3 Diagnose Job

Performs an automated bottleneck analysis for a specific job. This is a composite operation: the coordinator queries real-time metrics for all ranks of the job, identifies outlier ranks by comparing their p99 latency and throughput against the job-wide distribution, cross-references outlier ranks with storage node health and I/O metrics, and returns a structured diagnosis with actionable recommendations.

**API:**

```
GET /mgmt/v1/jobs/{job_id}/diagnose
```

**Response (200 OK):**

```json
{
    "job_id": "train-run-42",
    "diagnosis": {
        "status": "bottleneck_detected",
        "findings": [
            {
                "severity": "warning",
                "type": "rank_latency_outlier",
                "rank": "142/16384",
                "metric": "p99_read_latency_ms",
                "value": 850,
                "cluster_average": 45,
                "cause": {
                    "storage_node": "sn-009",
                    "drive": "nvme2",
                    "issue": "drive_queue_saturated",
                    "detail": "Queue depth 128 (max). Concurrent rebuild of 12,000 chunks from sn-007 failure."
                },
                "recommendations": [
                    {
                        "action": "Reduce rebuild bandwidth to decrease I/O contention",
                        "command": "keinctl rebuild throttle --max-bandwidth 200MB/s",
                        "api": "PATCH /mgmt/v1/cluster/config {\"rebuild\": {\"max_bandwidth_per_node_bytes\": 209715200}}"
                    },
                    {
                        "action": "Wait for rebuild to complete (~8 minutes remaining)",
                        "command": "keinctl rebuild status",
                        "api": "GET /mgmt/v1/rebuild/status"
                    }
                ]
            }
        ]
    }
}
```

**CLI:**

```bash
keinctl job diagnose train-run-42
```

Output matches the design specification's diagnostic example in Section 10.5, with color-coded severity indicators in terminal output.

### 12.4 Get Job History

Returns the lifetime I/O summary for a completed job.

**API:**

```
GET /mgmt/v1/jobs/{job_id}/history
```

**CLI:**

```bash
keinctl job history train-run-42
```

---

## 13. Usage and Chargeback Reporting

Usage endpoints provide aggregated resource consumption data for billing, chargeback, and capacity planning. The usage aggregator background service rolls up per-request counters into hourly and daily summaries.

### 13.1 Query Usage

**API:**

```
GET /mgmt/v1/usage
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `group_by` | string | Aggregation level: `tenant`, `team`, `project`, `job` |
| `tenant` | string | Filter by tenant |
| `team` | string | Filter by team |
| `project` | string | Filter by project |
| `period` | string | Time range: `1h`, `24h`, `7d`, `30d`, or custom `start,end` in ISO 8601 |
| `sort` | string | Sort field (default: `-read_bytes`) |

**Response (200 OK):**

```json
{
    "period": {
        "start": "2026-02-18T10:30:05Z",
        "end": "2026-02-19T10:30:05Z"
    },
    "group_by": "team",
    "items": [
        {
            "tenant": "acme",
            "team": "ml-research",
            "read_bytes": 1319413953536000,
            "write_bytes": 97893588992000,
            "capacity_bytes_avg": 156170924064768,
            "objects_read": 14000000,
            "objects_written": 48000,
            "requests_total": 14048000,
            "quota_utilization_avg_percent": {
                "capacity": 71.0,
                "bandwidth": 64.4
            }
        },
        {
            "tenant": "acme",
            "team": "ml-platform",
            "read_bytes": 373834188800000,
            "write_bytes": 13194139535360,
            "capacity_bytes_avg": 41804782362624,
            "objects_read": 3200000,
            "objects_written": 12000,
            "requests_total": 3212000,
            "quota_utilization_avg_percent": {
                "capacity": 19.0,
                "bandwidth": 22.1
            }
        }
    ]
}
```

**CLI:**

```bash
keinctl usage --by team --period 24h
keinctl usage --by project --tenant acme --team ml-research --period 7d
keinctl usage --by job --project llm-v3 --period 1h
```

```
Period: last 24 hours

TEAM             READ       WRITE     CAPACITY   QUOTA%
ml-research      1.2 PB     89 TB     142 TB     71%
ml-platform      340 TB     12 TB      38 TB     19%
data-eng          89 TB     45 TB      95 TB     48%
inference-prod   890 TB      2 TB      12 TB     60%
```

### 13.2 Export Usage Report

Generates a downloadable usage report in CSV or JSON format for a specified time range, suitable for import into billing or chargeback systems.

**API:**

```
POST /mgmt/v1/usage/export
```

**Request body:**

```json
{
    "period": {
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-02-01T00:00:00Z"
    },
    "group_by": "project",
    "tenant": "acme",
    "format": "csv",
    "granularity": "daily"
}
```

**Response (202 Accepted):** Returns an operation resource with a download URL upon completion.

**CLI:**

```bash
keinctl usage export --tenant acme --period 2026-01-01,2026-02-01 \
    --group-by project --format csv --granularity daily \
    --output usage-jan-2026.csv
```

---

## 14. Rebuild, Scrub, and Garbage Collection

These endpoints provide visibility into and control over the background maintenance services described in the design specification.

### 14.1 Rebuild Status

**API:**

```
GET /mgmt/v1/rebuild/status
```

**Response (200 OK):**

```json
{
    "active": true,
    "reason": "Drive failure on sn-009 (nvme2)",
    "started_at": "2026-02-19T10:00:00Z",
    "progress": {
        "chunks_total": 48000,
        "chunks_completed": 35517,
        "chunks_remaining": 12483,
        "percent": 74.0
    },
    "bandwidth": {
        "current_bytes_per_sec": 524288000,
        "max_bytes_per_sec": 524288000
    },
    "estimated_completion": "2026-02-19T11:15:00Z",
    "affected_objects": {
        "total": 4200,
        "fully_protected": 3100,
        "degraded": 1100
    }
}
```

**CLI:**

```bash
keinctl rebuild status
```

### 14.2 Adjust Rebuild Throttle

**API:**

```
PATCH /mgmt/v1/rebuild/config
```

**Request body:**

```json
{
    "max_bandwidth_per_node_bytes": 209715200,
    "max_concurrent_operations": 25,
    "io_priority": "low"
}
```

**CLI:**

```bash
keinctl rebuild throttle --max-bandwidth 200MB/s
keinctl rebuild throttle --max-concurrent 25
keinctl rebuild throttle --pause       # Pause rebuild entirely
keinctl rebuild throttle --resume      # Resume rebuild
```

**Required role:** `cluster-admin`

### 14.3 Scrub Status

**API:**

```
GET /mgmt/v1/scrub/status
```

**Response (200 OK):**

```json
{
    "enabled": true,
    "current_cycle": {
        "started_at": "2026-02-10T00:00:00Z",
        "progress_percent": 42.3,
        "chunks_verified": 84293000,
        "chunks_total": 199284000,
        "errors_found": 3,
        "errors_repaired": 3
    },
    "last_complete_cycle": {
        "completed_at": "2026-01-27T14:30:00Z",
        "duration_days": 14,
        "chunks_verified": 192481000,
        "errors_found": 7,
        "errors_repaired": 7
    }
}
```

**CLI:**

```bash
keinctl scrub status
```

### 14.4 Garbage Collection Status

**API:**

```
GET /mgmt/v1/gc/status
```

**Response (200 OK):**

```json
{
    "enabled": true,
    "last_sweep": {
        "completed_at": "2026-02-19T09:00:00Z",
        "duration_seconds": 342,
        "orphan_chunks_found": 1482,
        "orphan_chunks_deleted": 1482,
        "space_reclaimed_bytes": 6174015488
    },
    "pending_orphans": {
        "abandoned_uploads": 12,
        "stale_chunks": 0,
        "total_bytes": 51539607552
    },
    "next_sweep": "2026-02-19T10:00:00Z"
}
```

**CLI:**

```bash
keinctl gc status
```

---

## 15. Observability: Tracing and Metrics

### 15.1 Query Trace

Retrieves a distributed trace by its trace ID, showing the full span hierarchy from client through coordinator, metadata layer, and storage nodes.

**API:**

```
GET /mgmt/v1/traces/{trace_id}
```

**Response (200 OK):** Returns the trace in OpenTelemetry-compatible JSON format, including all spans, their durations, status codes, and tags (including the full attribution context).

**CLI:**

```bash
keinctl trace 01HABCD-EFGH-...
```

### 15.2 Find Slow Requests

**API:**

```
GET /mgmt/v1/traces/slow
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `period` | string | Time range (default: `1h`) |
| `threshold_ms` | integer | Minimum duration to include (default: from cluster config) |
| `tenant` | string | Filter by tenant |
| `project` | string | Filter by project |
| `job` | string | Filter by job |
| `operation` | string | Filter by operation: `GET`, `PUT`, `DELETE`, `LIST` |
| `limit` | integer | Max results (default: 50) |

**CLI:**

```bash
keinctl trace slow --last 1h
keinctl trace slow --job train-run-42 --threshold 500
keinctl trace slow --project llm-v3 --operation GET --last 4h
```

### 15.3 Search Traces

**API:**

```
GET /mgmt/v1/traces/search
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `job` | string | Filter by job ID |
| `tenant` | string | Filter by tenant |
| `project` | string | Filter by project |
| `bucket` | string | Filter by bucket |
| `key_prefix` | string | Filter by object key prefix |
| `status` | string | Filter: `ok`, `error` |
| `period` | string | Time range |
| `limit` | integer | Max results |

**CLI:**

```bash
keinctl trace search --job train-run-42 --last 1h
keinctl trace search --bucket training-data --status error --last 24h
```

### 15.4 Metrics Endpoints

Every KeInFS component (coordinator, storage node, background service) exposes a Prometheus/OpenMetrics endpoint at `/metrics` on its own management port. The Management API provides a convenience endpoint for discovering these endpoints.

**API:**

```
GET /mgmt/v1/metrics/targets
```

**Response (200 OK):**

```json
{
    "targets": [
        { "component": "coordinator", "instance": "coord-001", "endpoint": "https://coord-001.keinfs.local:8444/metrics" },
        { "component": "coordinator", "instance": "coord-002", "endpoint": "https://coord-002.keinfs.local:8444/metrics" },
        { "component": "storage_node", "instance": "sn-001", "endpoint": "https://sn-001.keinfs.local:9444/metrics" },
        { "component": "rebuilder", "instance": "rebuilder-leader", "endpoint": "https://rebuilder.keinfs.local:9544/metrics" }
    ]
}
```

This endpoint is designed for integration with Prometheus service discovery (`http_sd_configs`).

**CLI:**

```bash
keinctl metrics targets
```

---

## 16. Audit Log Access

The audit log provides a tamper-evident record of all control-plane and (optionally) data-plane operations. Audit entries are stored in the cluster's metadata store with configurable retention.

### 16.1 Query Audit Log

**API:**

```
GET /mgmt/v1/audit
```

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `tenant` | string | Filter by tenant |
| `team` | string | Filter by team |
| `project` | string | Filter by project |
| `identity` | string | Filter by authenticated identity |
| `operation` | string | Filter by operation type (e.g., `PutObject`, `CreateBucket`, `SetQuota`) |
| `category` | string | Filter: `data`, `management`, `auth` |
| `result` | string | Filter: `success`, `failure` |
| `period` | string | Time range |
| `bucket` | string | Filter by bucket |
| `sort` | string | Sort field (default: `-timestamp`) |
| `limit` | integer | Page size |
| `cursor` | string | Cursor for pagination (recommended for large result sets) |

**Response (200 OK):**

```json
{
    "items": [
        {
            "timestamp": "2026-02-19T10:30:05.123456Z",
            "request_id": "req-01HXYZ...",
            "trace_id": "trace-01HABCD...",
            "category": "management",
            "operation": "SetQuota",
            "identity": "user:alice@acme.com",
            "role": "tenant-admin",
            "scope": { "tenant": "acme", "team": "ml-research", "project": "llm-v3" },
            "request_body": {
                "limits": { "capacity_bytes": 54975581388800 }
            },
            "result": "success",
            "client_ip": "10.0.1.42",
            "coordinator": "coord-003",
            "duration_ms": 12
        }
    ],
    "pagination": { "total": 48293, "cursor": "eyJ..." }
}
```

**CLI:**

```bash
keinctl audit --tenant acme --period 24h
keinctl audit --category management --identity user:alice@acme.com --period 7d
keinctl audit --operation CreateBucket --result success --period 30d
keinctl audit --bucket training-data --operation PutObject --period 1h
```

**Required role:** `auditor` or `tenant-admin` (or higher) at the appropriate scope.

### 16.2 Export Audit Log

Exports audit log entries to a file (JSON or CSV) or streams them to an external sink.

**API:**

```
POST /mgmt/v1/audit/export
```

**Request body:**

```json
{
    "period": {
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-02-01T00:00:00Z"
    },
    "tenant": "acme",
    "format": "json",
    "sink": {
        "type": "s3",
        "endpoint": "https://audit-archive.s3.amazonaws.com",
        "bucket": "keinfs-audit-logs",
        "prefix": "acme/2026-01/"
    }
}
```

Supported sink types are `s3` (any S3-compatible endpoint, including KeInFS itself), `kafka`, and `syslog`.

**CLI:**

```bash
keinctl audit export --tenant acme --period 2026-01-01,2026-02-01 --format json \
    --output audit-jan-2026.json

keinctl audit export --tenant acme --period 2026-01-01,2026-02-01 \
    --sink s3 --sink-endpoint https://audit-archive.s3.amazonaws.com \
    --sink-bucket keinfs-audit-logs --sink-prefix acme/2026-01/
```

---

## 17. Authentication and Token Management

These endpoints manage the credentials and identities used to authenticate to the KeInFS cluster (both data-plane and management-plane).

### 17.1 Create Access Key

Creates an HMAC access key pair for a specified identity.

**API:**

```
POST /mgmt/v1/auth/access-keys
```

**Request body:**

```json
{
    "identity": "serviceaccount:training-pipeline",
    "scope": {
        "tenant": "acme",
        "team": "ml-research",
        "project": "llm-v3"
    },
    "description": "CI/CD pipeline for training data ingestion",
    "expires_at": "2027-02-19T00:00:00Z"
}
```

**Response (201 Created):**

```json
{
    "access_key_id": "AKID-01HXYZ...",
    "secret_key": "sk-...",
    "identity": "serviceaccount:training-pipeline",
    "scope": { "tenant": "acme", "team": "ml-research", "project": "llm-v3" },
    "created_at": "2026-02-19T10:30:05Z",
    "expires_at": "2027-02-19T00:00:00Z"
}
```

The `secret_key` is returned only in this response and is never stored or retrievable again. If lost, the key must be deleted and a new one created.

**CLI:**

```bash
keinctl auth create-access-key \
    --identity serviceaccount:training-pipeline \
    --tenant acme --team ml-research --project llm-v3 \
    --description "CI/CD pipeline for training data ingestion" \
    --expires 2027-02-19
```

### 17.2 List Access Keys

**API:**

```
GET /mgmt/v1/auth/access-keys
```

**Query parameters:** `identity`, `tenant`, `team`, `project`, `status` (`active`, `expired`, `revoked`).

Returns key metadata (not the secret key).

**CLI:**

```bash
keinctl auth list-access-keys --tenant acme
```

### 17.3 Revoke Access Key

**API:**

```
DELETE /mgmt/v1/auth/access-keys/{access_key_id}
```

**CLI:**

```bash
keinctl auth revoke-access-key AKID-01HXYZ...
```

### 17.4 Issue Bearer Token

Issues a short-lived bearer token from an existing credential (HMAC key or mTLS certificate). This is the recommended flow for interactive sessions: authenticate once with a long-lived credential, receive a short-lived token, and use the token for subsequent requests.

**API:**

```
POST /mgmt/v1/auth/token
```

**Request body:**

```json
{
    "ttl_seconds": 3600,
    "scope": {
        "tenant": "acme",
        "team": "ml-research"
    }
}
```

The request must be authenticated with a valid HMAC key or mTLS certificate. The issued token's scope cannot exceed the authenticating credential's scope.

**Response (200 OK):**

```json
{
    "token": "eyJ...",
    "expires_at": "2026-02-19T11:30:05Z",
    "identity": "user:alice@acme.com",
    "scope": { "tenant": "acme", "team": "ml-research" }
}
```

**CLI:**

```bash
keinctl auth login
# Authenticates using configured HMAC key, stores bearer token in
# $HOME/.keinfs/token (permission 0600) for subsequent commands.

keinctl auth login --ttl 8h
```

### 17.5 RBAC Policy Management

#### List Role Bindings

**API:**

```
GET /mgmt/v1/auth/rbac/bindings
```

**Query parameters:** `tenant`, `team`, `project`, `identity`, `role`.

**CLI:**

```bash
keinctl auth rbac list --tenant acme
```

#### Create Role Binding

**API:**

```
POST /mgmt/v1/auth/rbac/bindings
```

**Request body:**

```json
{
    "identity": "user:bob@acme.com",
    "role": "team-admin",
    "scope": {
        "tenant": "acme",
        "team": "ml-research"
    }
}
```

**CLI:**

```bash
keinctl auth rbac grant user:bob@acme.com team-admin \
    --tenant acme --team ml-research
```

#### Delete Role Binding

**API:**

```
DELETE /mgmt/v1/auth/rbac/bindings/{binding_id}
```

**CLI:**

```bash
keinctl auth rbac revoke user:bob@acme.com team-admin \
    --tenant acme --team ml-research
```

---

## 18. Attribution Context Management

Attribution contexts are signed tokens that identify the tenant, team, project, job, and distributed training rank associated with a workload. They are embedded in every KeInFS/2 request via the `x-keinfs-context` header and are the foundation of per-job observability and quota enforcement.

### 18.1 Create Attribution Context

Generates a signed attribution context token for use by workloads. The context is typically created at job launch time by the scheduler integration (Slurm plugin, Kubernetes admission webhook) or manually by the operator.

**API:**

```
POST /mgmt/v1/contexts
```

**Request body:**

```json
{
    "tenant": "acme",
    "team": "ml-research",
    "project": "llm-v3",
    "job": "train-run-42",
    "scheduler": "slurm",
    "scheduler_job_id": "889421",
    "ttl_seconds": 86400
}
```

**Response (201 Created):**

```json
{
    "context_token": "eyJ0ZW5hbnQiOiJhY21lIi...",
    "expires_at": "2026-02-20T10:30:05Z",
    "claims": {
        "tenant": "acme",
        "team": "ml-research",
        "project": "llm-v3",
        "job": "train-run-42",
        "scheduler": "slurm",
        "scheduler_job_id": "889421",
        "iat": 1708340805,
        "exp": 1708427205
    }
}
```

The `context_token` is a coordinator-signed context token that `libkeinfs` attaches to every request. Workloads consume it via the `KEINFS_CONTEXT` environment variable.

**CLI:**

```bash
keinctl context create \
    --tenant acme \
    --team ml-research \
    --project llm-v3 \
    --job train-run-42 \
    --scheduler slurm \
    --scheduler-job-id 889421 \
    --ttl 24h
```

The CLI outputs only the token value (suitable for `export KEINFS_CONTEXT=$(...)`):

```bash
# Typical usage in a Slurm prolog or job script:
export KEINFS_CONTEXT=$(keinctl context create \
    --team ml-research \
    --project llm-v3 \
    --job $SLURM_JOB_NAME \
    --scheduler slurm \
    --scheduler-job-id $SLURM_JOB_ID)
```

### 18.2 Validate Attribution Context

Validates a context token without executing any operation. Useful for debugging context propagation issues.

**API:**

```
POST /mgmt/v1/contexts/validate
```

**Request body:**

```json
{
    "context_token": "eyJ0ZW5hbnQiOiJhY21lIi..."
}
```

**Response (200 OK):**

```json
{
    "valid": true,
    "claims": {
        "tenant": "acme",
        "team": "ml-research",
        "project": "llm-v3",
        "job": "train-run-42",
        "iat": 1708340805,
        "exp": 1708427205
    },
    "expires_in_seconds": 72342
}
```

**CLI:**

```bash
keinctl context validate $KEINFS_CONTEXT
```

---

## 19. Operations

Long-running operations (node drain, cluster format, drive eviction, usage export) are tracked as operation resources.

### 19.1 List Operations

**API:**

```
GET /mgmt/v1/operations
```

**Query parameters:** `type`, `status` (`running`, `completed`, `failed`), `period`.

**CLI:**

```bash
keinctl operation list
keinctl operation list --status running
```

### 19.2 Get Operation Status

**API:**

```
GET /mgmt/v1/operations/{operation_id}
```

**CLI:**

```bash
keinctl operation status op-01HXYZ...
```

### 19.3 Cancel Operation

Some long-running operations can be cancelled (e.g., a drain can be aborted, reverting the node to active status).

**API:**

```
POST /mgmt/v1/operations/{operation_id}/cancel
```

**CLI:**

```bash
keinctl operation cancel op-01HXYZ...
```

---

## Appendix A — Error Catalogue

All error codes returned by the Management API are listed below. The `code` field in error responses is always one of these values. Error codes and messages are written for the operator — they describe what went wrong in terms of the operation attempted, not in terms of internal components. Implementation details (database engines, internal service names, internal key paths) are never exposed in error responses.

### Authentication and Authorization Errors

| Code | HTTP Status | Description |
|---|---|---|
| `AUTHENTICATION_REQUIRED` | 401 | The request did not include any credentials. Include an `Authorization` header (Bearer token or HMAC signature) or present an mTLS client certificate. |
| `AUTHENTICATION_FAILED` | 401 | The provided credentials are invalid, expired, or revoked. For HMAC: the signature does not match (check access key, secret key, and request signing). For Bearer: the token has expired or been revoked. For mTLS: the client certificate is not trusted by the cluster CA. |
| `AUTHORIZATION_DENIED` | 403 | The authenticated identity does not have the required role for this operation at the requested scope. The response body includes the identity, the required role, and the scope, so the operator can request the appropriate RBAC binding. |

### Resource Lifecycle Errors

| Code | HTTP Status | Description |
|---|---|---|
| `RESOURCE_NOT_FOUND` | 404 | The requested resource (node, drive, bucket, tenant, team, project, EC profile, or operation) does not exist. The response body includes the resource type and identifier. |
| `RESOURCE_ALREADY_EXISTS` | 409 | A resource with the specified name already exists at the same scope. Bucket names are globally unique; tenant, team, and project names are unique within their parent scope. |
| `RESOURCE_NOT_EMPTY` | 409 | Cannot delete a resource that still contains child resources. For example: a bucket that still contains objects, a tenant that still has teams, or a team that still has projects. The response body includes the count of remaining children. Empty the resource first, or use `--force` where supported to cascade-delete. |
| `RESOURCE_IN_USE` | 409 | Cannot delete a resource that is referenced by other resources. For example: an EC profile that is still assigned to one or more buckets. The response body lists the referencing resources. |

### Drive and Device Errors

| Code | HTTP Status | Description |
|---|---|---|
| `DRIVE_NOT_EVICTED` | 409 | The drive must be fully evicted before it can be destroyed. Run `keinctl drive evict <uuid> --drain` (or `--force` for a failed drive) and wait for the eviction to complete before retrying destruction. The response body includes the drive's current lifecycle state. |
| `DRIVE_HAS_FILESYSTEM` | 409 | The target device contains a recognized filesystem signature (ext4, XFS, etc.). To prevent accidental destruction of non-KeInFS data, formatting is refused. Use `--force` to override this check if you are certain the device should be formatted. The response body identifies the detected filesystem type. |
| `DRIVE_HAS_SUPERBLOCK` | 409 | The target device already contains a KeInFS superblock. To prevent accidental re-formatting of a drive that may be actively serving data, formatting is refused. Use `--force` to override. The response body includes the existing superblock's UUID and creation timestamp so you can verify you are formatting the intended device. |
| `CLUSTER_ID_MISMATCH` | 409 | The device's superblock contains a cluster ID that does not match this cluster. This typically means the drive was formatted for a different KeInFS cluster and has been physically moved. To prevent cross-cluster data contamination, the drive is rejected. Reformat the drive with `--force` to assign it to this cluster, or return it to its original cluster. The response body includes both the expected and actual cluster IDs. |
| `NODE_UNREACHABLE` | 503 | The storage node could not be contacted to execute the requested operation. The node may be offline, the network path may be interrupted, or the node daemon may not be running. The response body includes the node ID and the last successful heartbeat timestamp. |
| `NODE_NOT_DRAINABLE` | 409 | The node cannot be drained because it is already in a drain operation or has been declared dead. A node that is already draining must complete or be cancelled before a new drain can start. A dead node must be force-evicted at the drive level. The response body includes the node's current status. |

### Quota Errors

| Code | HTTP Status | Description |
|---|---|---|
| `QUOTA_EXCEEDED` | 429 or 507 | A quota limit has been reached. HTTP 429 is returned for rate-based dimensions (bandwidth, request rate, concurrent connections), with a `Retry-After` header indicating when to retry. HTTP 507 is returned for capacity-based dimensions (storage capacity, object count). The response body identifies the specific dimension, the hierarchy level (tenant, team, or project), the current limit, and the current usage, so the operator can determine whether to increase the quota or reduce consumption. |
| `QUOTA_HIERARCHY_VIOLATION` | 400 | The requested quota value violates the hierarchy constraint: a child scope's quota cannot exceed its parent's quota. For example, setting a project capacity quota of 100 TB when the parent team's quota is 80 TB is rejected. The response body includes both the requested value and the parent's limit. |

### Rate Limiting Errors

| Code | HTTP Status | Description |
|---|---|---|
| `RATE_LIMITED` | 429 | The per-identity or per-IP request rate limit has been exceeded. This applies to the management API itself, independent of data-plane quota enforcement. The response includes a `Retry-After` header. If this occurs during normal automation, consider reducing the polling frequency or batching operations. |

### Validation Errors

| Code | HTTP Status | Description |
|---|---|---|
| `INVALID_REQUEST` | 400 | The request body or query parameters are malformed, missing required fields, or contain values outside acceptable ranges. The response body includes a list of specific validation errors, each identifying the field, the constraint violated, and the acceptable range or format. |
| `INVALID_STATE` | 409 | The requested operation is not valid given the resource's current lifecycle state. For example: attempting to rejoin a node that is not in a drained state, or attempting to format a device on a node that is currently draining. The response body includes the resource's current state and lists the states from which the requested operation is permitted. |

### Operational Errors

| Code | HTTP Status | Description |
|---|---|---|
| `OPERATION_CANCELLED` | 409 | The long-running operation was cancelled by an operator before it completed. Partial effects may have occurred (e.g., some chunks may have been migrated during a drain). The response body includes a summary of what was completed before cancellation. |
| `OPERATION_FAILED` | 500 | The long-running operation encountered an unrecoverable error and has stopped. The response body includes a detailed error description, the operation's progress at the time of failure, and a suggestion for recovery (e.g., "retry the operation" or "contact cluster administrator"). |

### System Availability Errors

| Code | HTTP Status | Description |
|---|---|---|
| `NAMESPACE_UNAVAILABLE` | 503 | The cluster's namespace service is temporarily unavailable. The coordinator cannot read or write cluster metadata, which means it cannot process management operations, resolve object locations, or enforce quotas. This typically indicates that the metadata cluster is undergoing leader election (usually resolves within seconds) or that a quorum of metadata nodes is unreachable (requires operator investigation). The response includes a `Retry-After` header. If this error persists beyond 30 seconds, check the health of the metadata cluster nodes. |
| `COORDINATOR_OVERLOADED` | 503 | The coordinator is temporarily unable to accept new requests due to resource exhaustion (memory, connection pool, or CPU). The response includes a `Retry-After` header. Smart clients (`libkeinfs`, `keinctl`) automatically retry on a different coordinator from their configured coordinator list. If this occurs frequently across multiple coordinators, add more coordinator instances to the cluster and update the coordinator list in client configurations. For S3-compatible clients behind a load balancer, verify that the load balancer's health checks are removing unhealthy storage-node S3 ingress instances from rotation. |
| `INTERNAL_ERROR` | 500 | An unexpected error occurred in the coordinator while processing the request. The response body includes a `request_id` and `trace_id` for correlation with the audit log and distributed tracing system. Report this error along with these IDs to the cluster administrator. |

---

## Appendix B — RBAC Permission Matrix

This matrix shows which Management API operations each built-in role can perform. "Scoped" means the role can perform the operation only within its assigned scope (tenant, team, or project).

| Operation | `cluster-admin` | `tenant-admin` | `team-admin` | `project-admin` | `viewer` | `auditor` |
|---|---|---|---|---|---|---|
| Cluster status/topology/config | Read/Write | Read | Read | — | Read | — |
| Node list/status | Yes | Read | Read | — | Read | — |
| Node drain/rejoin | Yes | — | — | — | — | — |
| Drive format/evict/destroy | Yes | — | — | — | — | — |
| Tenant create/delete | Yes | — | — | — | — | — |
| Tenant update | Yes | Scoped | — | — | — | — |
| Team create/delete | Yes | Scoped | — | — | — | — |
| Team update | Yes | Scoped | Scoped | — | — | — |
| Project create/delete | Yes | Scoped | Scoped | — | — | — |
| Project update | Yes | Scoped | Scoped | Scoped | — | — |
| Bucket create/delete | Yes | Scoped | Scoped | Scoped | — | — |
| Bucket update | Yes | Scoped | Scoped | Scoped | — | — |
| EC profile manage | Yes | — | — | — | — | — |
| Quota set (tenant) | Yes | — | — | — | — | — |
| Quota set (team) | Yes | Scoped | — | — | — | — |
| Quota set (project) | Yes | Scoped | Scoped | — | — | — |
| Quota show | Yes | Scoped | Scoped | Scoped | Scoped | — |
| Job list/stats/diagnose | Yes | Scoped | Scoped | Scoped | Scoped | — |
| Usage query/export | Yes | Scoped | Scoped | Scoped | Scoped | — |
| Rebuild/scrub/GC control | Yes | — | — | — | — | — |
| Rebuild/scrub/GC status | Yes | Read | Read | — | Read | — |
| Trace query/search | Yes | Scoped | Scoped | Scoped | Scoped | — |
| Audit log query | Yes | Scoped | — | — | — | Scoped |
| Audit log export | Yes | Scoped | — | — | — | Scoped |
| Access key manage | Yes | Scoped | Scoped | Scoped | — | — |
| RBAC binding manage | Yes | Scoped | — | — | — | — |
| Context create/validate | Yes | Scoped | Scoped | Scoped | — | — |
| Metrics targets | Yes | Read | Read | Read | Read | — |

---

## Appendix C — CLI Quick Reference

```
CLUSTER
  keinctl cluster status                 Cluster health summary
  keinctl cluster topology               Node, drive, and failure domain map
  keinctl cluster config show            Current cluster configuration
  keinctl cluster config set <k> <v>     Update cluster configuration
  keinctl cluster format [--confirm]     Format all unformatted devices

NODES
  keinctl node list                      List all storage nodes
  keinctl node status <node>             Detailed node status
  keinctl node drain <node>              Initiate graceful drain
  keinctl node rejoin <node>             Re-enable after maintenance

DRIVES
  keinctl drive list                     List all drives
  keinctl drive status <node> <drive>    Detailed drive status
  keinctl drive format <node> <device>   Format a raw block device
  keinctl drive inspect <node> <drive>   Read superblock and allocator layout
  keinctl drive evict <uuid> --drain     Graceful drive eviction
  keinctl drive evict <uuid> --force     Immediate drive eviction
  keinctl drive destroy <uuid>           Destroy superblock (evicted drives only)

BUCKETS
  keinctl bucket create <name>           Create a bucket
  keinctl bucket list                    List buckets
  keinctl bucket show <name>             Bucket details and usage
  keinctl bucket update <name>           Update bucket configuration
  keinctl bucket delete <name>           Delete a bucket

EC PROFILES
  keinctl ec-profile list                List erasure coding profiles
  keinctl ec-profile create <name>       Create a new EC profile
  keinctl ec-profile show <name>         EC profile details
  keinctl ec-profile delete <name>       Delete an EC profile

TENANTS, TEAMS, PROJECTS
  keinctl tenant create|list|show|update|delete <name>
  keinctl team create|list|show|update|delete <name> --tenant <t>
  keinctl project create|list|show|update|delete <name> --tenant <t> --team <tm>

QUOTAS
  keinctl quota set --tenant <t> [--team <tm>] [--project <p>] --<dimension> <value>
  keinctl quota show --tenant <t> [--recursive]
  keinctl quota alert --tenant <t> --dimension <dim> --threshold <pct>

JOBS
  keinctl job list                       Active jobs with I/O summary
  keinctl job stats <job>                Real-time job I/O statistics
  keinctl job diagnose <job>             Automated bottleneck analysis
  keinctl job history <job>              Lifetime I/O summary

USAGE
  keinctl usage --by <level> --period <p>        Aggregated usage
  keinctl usage export --tenant <t> --period <p>  Export usage report

MAINTENANCE
  keinctl rebuild status                 Current rebuild progress
  keinctl rebuild throttle [flags]       Adjust rebuild bandwidth
  keinctl scrub status                   Scrub progress and findings
  keinctl gc status                      Garbage collection status

TRACING
  keinctl trace <trace-id>               Show full request trace
  keinctl trace slow --last <period>     Find slow requests
  keinctl trace search --job <j>         Find traces for a job

AUDIT
  keinctl audit [--tenant <t>] [flags]   Query audit log
  keinctl audit export [flags]           Export audit log

AUTH
  keinctl auth login                     Obtain bearer token
  keinctl auth create-access-key         Create HMAC key pair
  keinctl auth list-access-keys          List keys
  keinctl auth revoke-access-key <id>    Revoke a key
  keinctl auth rbac list                 List role bindings
  keinctl auth rbac grant <id> <role>    Grant role to identity
  keinctl auth rbac revoke <id> <role>   Revoke role from identity

CONTEXT
  keinctl context create [flags]         Create signed attribution context
  keinctl context validate <token>       Validate a context token

OPERATIONS
  keinctl operation list                 List long-running operations
  keinctl operation status <id>          Check operation progress
  keinctl operation cancel <id>          Cancel an operation

GLOBAL FLAGS
  --coordinators <url,...>  Coordinator endpoints (comma-separated)
  --output <format>     Output: table, json, toml
  --quiet               Suppress output
  --verbose             Show HTTP details
  --dry-run             Show request without executing
  --confirm             Skip confirmation prompts
  --timeout <duration>  Request timeout
  --config <path>       Configuration file path
```

---

*Document revision history: v0.1-draft, February 2026. Initial Management API and CLI reference specification.*
