---
title: "The gRPC contract"
linkTitle: "The gRPC contract"
weight: 9
description: >
  The InferenceCache service — its RPCs, message shapes, and the guarantees clients depend
  on.
---

## The InferenceCache service

The `InferenceCache` gRPC service is the data-plane API that gateways and engines integrate
against. It is defined in `proto/inferencecache/v1alpha1/inferencecache.proto`, package
`inferencecache.v1alpha1`, and served on `:9090` (plaintext by default; TLS is an opt-in
overlay — see [gRPC TLS](/docs/administration/grpc-tls/)).

## Eight RPCs, three roles

**Consumer side (gateways) — fail-open, side-effect-free apart from metrics:**

| RPC | Kind | Purpose |
|---|---|---|
| `LookupRoute` | unary | The core cache-aware routing hint. See [LookupRoute & ranking](/docs/concepts/lookuproute/). |
| `RenderTemplate` | unary | Deterministic mutable-slot prompt render. Fail-open stub today. |
| `LookupPDRoute` | unary | Prefill/decode split routing. Phase-2 stub. |
| `GetCacheState` | unary | The `(tenant, model)` aggregate (replica stats + summary). |

**Producer side (engine adapters):**

| RPC | Kind | Purpose |
|---|---|---|
| `ReportCacheState` | client-stream | Authoritative **additive** ingest of prefix/replica state. |
| `PublishEvent` | unary | Scheme-safe deltas (`PREFIX_EVICTED`, `REPLICA_UPDATED`, `ALL_CLEARED`). |

**Observer side (dashboards / debug) — server-stream stubs:**

| RPC | Kind | Purpose |
|---|---|---|
| `StreamCacheEvents` | server-stream | Live cache events. |
| `StreamMetrics` | server-stream | Live metrics. |

## Key messages

```proto
message LookupRouteRequest {
  string model_id             = 1;
  string tenant_id            = 2;
  bytes  prefix_hash          = 3;
  int32  prefix_token_count   = 4;
  string hash_scheme          = 5;
  SLO    slo                  = 6;
  repeated bytes block_hashes = 7;   // ordered block-hash chain
  repeated int32 block_token_counts = 8;
  repeated uint32 token_ids   = 9;   // dual-input: server fingerprints
  string prompt_text          = 10;  // dual-input: server tokenizes + fingerprints
}

message LookupRouteResponse {
  repeated ReplicaScore replica_scores = 1;
  string reason_code                   = 2;
  int64  lookup_latency_us             = 3;
  repeated uint32 token_ids            = 4;  // echoed on the token_ids/prompt_text path
}

message ReplicaScore {
  string replica_id               = 1;
  float  score                    = 2;
  int32  matched_tokens           = 3;
  float  estimated_cache_hit_prob = 4;
}

message PrefixEntry {              // metadata only — never KV tensors or prompt text
  bytes  prefix_hash        = 1;
  int32  token_count        = 2;
  repeated bytes block_hashes       = 3;
  repeated int32 block_token_counts = 4;
}

message ReplicaStats {
  string replica_id        = 1;
  int64  cache_memory_bytes = 2;  // engine total across all tenants
  float  hit_rate          = 3;
  float  pressure          = 4;
  string client_version    = 5;
  int64  t2_hit_tokens     = 6;   // from vLLM external_prefix_cache_hits_total
  int64  t2_query_tokens   = 7;   // from vLLM external_prefix_cache_queries_total
}
```

`Ack { bool accepted; string reason_code; }` and `SLO { int32 ttft_ms; int32 tbt_ms; }`
round out the core shapes.

## The guarantees

These are the contract properties clients are allowed to depend on:

- **`reason_code` is a `string`, not a proto enum.** New codes can be added without breaking
  old clients — a client that does not recognize a code must treat it as the no-hint default.
- **Fail-open.** An empty `replica_scores` list is always valid. `TIMEOUT` is a fail-open
  outcome, not an error. The hot path never returns a gRPC error for a cache miss.
- **Side-effect-free lookups** (apart from metrics). The one narrow exception: an `LFU`
  namespace credits per-entry access counters on a *delivered* prefix-match hit.
- **Engine-opaque hashes, scheme-scoped.** `prefix_hash` / `block_hashes` are matched only
  within a matching `hash_scheme`; an empty scheme is not a valid domain.
- **Metadata only.** Messages carry hashes and statistics — **never** KV tensors or prompt
  text. (The one exception is the optional `prompt_text` *input* to `LookupRoute`, which
  strengthens the case for enabling TLS on `:9090`.)
- **`CacheStateUpdate` is an additive delta, not a snapshot.** A prefix's absence from a
  later update does *not* remove it. Removals come only via `CacheEvent`
  (`PREFIX_EVICTED` / `ALL_CLEARED`) or TTL. This matches the engine's own KV-event model,
  and it is why a stale entry degrades to a cache miss, never a wrong answer.
- **Reserved tenant.** `tenant_id = "inferencecache.io/probe"` is reserved for the server's
  functional self-test — excluded from `/snapshot` and from public reads and writes.

## The producer path

Engine adapters feed the index through the observation sidecar:

- `ReportCacheState` is the authoritative, idempotent ingest keyed on
  `(replica, hash_scheme, prefix_hash)`. It carries `PrefixEntry` metadata plus
  `ReplicaStats`.
- `PublishEvent` carries scheme-safe deltas. `PREFIX_ADDED` is a no-op (events carry no
  `hash_scheme`, and `ReportCacheState` is authoritative for adds); `REPLICA_UPDATED`
  refreshes replica stats; `PREFIX_EVICTED` / `ALL_CLEARED` remove.

## Keeping the contract honest

Because there are external consumers of this contract (gateway clients in other languages),
the proto stays **backward-compatible** — fields are not renumbered or removed. Any change to
`proto/` is required to update the contract design doc in the same change, and generated code
is verified drift-free in CI.

## Related pages

- [Reason codes](/docs/reference/reason-codes/) — the full string-code vocabulary.
- [gRPC API reference](/docs/reference/grpc-api/) — RPC-by-RPC detail.
- [LookupRoute & ranking](/docs/concepts/lookuproute/) — how the hint is computed.
