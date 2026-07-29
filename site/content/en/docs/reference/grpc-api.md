---
title: "gRPC API"
linkTitle: "gRPC API"
weight: 2
description: >
  The InferenceCache service, RPC by RPC.
---

Service **`InferenceCache`**, package **`inferencecache.v1alpha1`**, proto
`proto/inferencecache/v1alpha1/inferencecache.proto`. Served on `:9090` (plaintext by
default; [TLS overlay]({{< relref "/docs/administration/grpc-tls/" >}}) available). See
[The gRPC contract]({{< relref "/docs/concepts/grpc-contract/" >}}) for the guarantees; this page is the
per-RPC reference.

## Consumer RPCs

### `LookupRoute(LookupRouteRequest) → LookupRouteResponse`

The core cache-aware routing hint. Unary and fail-open. Lookups are side-effect-free apart
from metrics, except that a delivered prefix hit under an LFU policy credits the matched
entries' eviction access counters.

- **Request:** `model_id`, `tenant_id`, `hash_scheme`, and a prefix identified (in precedence
  order) by `prefix_hash` + `block_hashes` chain, or `token_ids`, or `prompt_text`; plus
  optional `slo` and `adapter_id`. The adapter ID selects the LoRA index partition and must
  match the producer's value.
- **Response:** `replica_scores[]` (best-first), `reason_code` (string), `lookup_latency_us`,
  `adapter_id` (the partition consulted), and `token_ids` only when the server tokenized
  `prompt_text`. Caller-supplied `token_ids` are not echoed.
- **Reason codes:** `PREFIX_MATCH`, `TENANT_HOT`, `AFFINITY_HINT`, `NO_HINT`,
  `POLICY_REQUIRES_CHAIN`, `TIMEOUT`, `UNKNOWN_TENANT`, `UNKNOWN_MODEL`,
  `UNKNOWN_HASH_SCHEME`. See [reason codes]({{< relref "/docs/reference/reason-codes/" >}}).

### `RenderTemplate(RenderTemplateRequest) → RenderTemplateResponse`

Deterministic mutable-slot prompt render. Response carries `rendered_prompt`,
`stable_prefix_hash`, `tenant_namespace`, `template_revision`, and a `reason_code`
(`OK` | `TEMPLATE_NOT_FOUND` | `RENDER_ERROR`). A fail-open stub today (returns `OK`).

### `LookupPDRoute(LookupPDRouteRequest) → LookupPDRouteResponse`

Prefill/decode split routing. Response: `prefill_replica_id`, `decode_replica_id`,
`transport_hint` (`Mooncake` | `NIXL` | `Direct`), `reason_code`. Phase-2 stub (returns no
hint).

### `GetCacheState(GetCacheStateRequest) → GetCacheStateResponse`

The `(tenant, model)` aggregate — replica stats plus a cache summary.

## Producer RPCs

### `ReportCacheState(stream CacheStateUpdate) → Ack`

Client-stream. The authoritative, **additive** ingest of prefix/replica state, idempotent per
`(replica, hash_scheme, adapter_id, prefix_hash)`. `CacheStateUpdate` carries `replica_id`,
`model_id`, `tenant_id`, `hash_scheme`, `timestamp_us`, `PrefixEntry[]`, `ReplicaStats`, and
an update-level `adapter_id` fallback. A `PrefixEntry` may select its own adapter partition.
Absence of a prefix in a later update does **not** remove it.

### `PublishEvent(CacheEvent) → Ack`

Scheme-safe deltas. `CacheEvent.Type` ∈ `PREFIX_ADDED` (no-op),
`PREFIX_EVICTED`, `REPLICA_UPDATED`, `ALL_CLEARED`.

## Observer RPCs

### `StreamCacheEvents(StreamEventsRequest) → stream CacheEvent`

Live cache events. Stub today.

### `StreamMetrics(StreamMetricsRequest) → stream Metric`

Live metrics. Stub today.

## Core messages

```proto
enum CacheTier {
  CACHE_TIER_UNSPECIFIED = 0;
  CACHE_TIER_T1          = 1;
  CACHE_TIER_T2          = 2;
  CACHE_TIER_T3          = 3;
}

message ReplicaScore {
  string replica_id               = 1;
  float  score                    = 2;
  int32  matched_tokens           = 3;
  float  estimated_cache_hit_prob = 4;
  CacheTier tier                  = 5;
}

message PrefixEntry {                    // metadata only
  bytes  prefix_hash                = 1;
  int32  token_count                = 2;
  repeated bytes block_hashes       = 3;
  repeated int32 block_token_counts = 4;
  CacheTier tier                    = 5;
  string adapter_id                 = 6;
}

message ReplicaStats {
  string replica_id         = 1;
  int64  cache_memory_bytes = 2;         // engine total across all tenants
  float  hit_rate           = 3;
  float  pressure           = 4;
  string client_version     = 5;         // opaque; no semver on the wire
  int64  t2_hit_tokens      = 6;
  int64  t2_query_tokens    = 7;
}

message SLO { int32 ttft_ms = 1; int32 tbt_ms = 2; }
message Ack { bool accepted = 1; string reason_code = 2; }
```

`Ack.reason_code` carries no codes today (always `accepted: true`).

## Health

The server also serves the standard `grpc.health.v1.Health` service on `:9090`.
