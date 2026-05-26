# Design: gRPC contract (`InferenceCache` service)

Status: proposed · Implements: B4 · Tracks: InferenceCache tech spec §4.2–4.4

This is the public API gateways and engines integrate against — the load-bearing contract that unblocks the cache index (B6), engine KV-event hook (C1), and gateway clients (E1). Get the signature right early; the bytes behind it are filled in by later modules.

## Identity

| | Value |
|---|---|
| proto file | `proto/inferencecache/v1alpha1/inferencecache.proto` |
| package | `inferencecache.v1alpha1` |
| service | `InferenceCache` |
| Go package | `github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1` |

Versioned `v1alpha1` → `v1beta1` → `v1`. No vendor tokens in the package or service (see CONTRIBUTING.md).

## Service

```protobuf
service InferenceCache {
  // Consumer side (gateways) — side-effect-free, fail-open.
  rpc RenderTemplate (RenderTemplateRequest) returns (RenderTemplateResponse);
  rpc LookupRoute    (LookupRouteRequest)    returns (LookupRouteResponse);
  rpc LookupPDRoute  (LookupPDRouteRequest)  returns (LookupPDRouteResponse);
  rpc GetCacheState  (GetCacheStateRequest)  returns (GetCacheStateResponse);

  // Producer side (engine adapters).
  rpc ReportCacheState (stream CacheStateUpdate) returns (Ack);   // client-stream
  rpc PublishEvent     (CacheEvent)              returns (Ack);

  // Observer side (dashboards, debug tooling).
  rpc StreamCacheEvents (StreamEventsRequest)  returns (stream CacheEvent);
  rpc StreamMetrics     (StreamMetricsRequest) returns (stream Metric);
}
```

## Messages (field numbers finalized in implementation)

- **RenderTemplateRequest** `{ string template_ref; map<string,string> variables; string tenant_id; }`
- **RenderTemplateResponse** `{ bytes rendered_prompt; bytes stable_prefix_hash; bytes tenant_namespace; string reason_code; string template_revision; }`
  reason ∈ `OK | TEMPLATE_NOT_FOUND | RENDER_ERROR`
- **LookupRouteRequest** `{ string model_id; string tenant_id; bytes prefix_hash; int32 prefix_token_count; string hash_scheme; SLO slo; }`
- **LookupRouteResponse** `{ repeated ReplicaScore replica_scores; string reason_code; int64 lookup_latency_us; }`
  reason ∈ `PREFIX_MATCH | TENANT_HOT | NO_HINT | TIMEOUT`
- **LookupPDRouteRequest** `{ string model_id; string tenant_id; bytes prefix_hash; int32 prefix_token_count; string pd_topology_ref; }`
- **LookupPDRouteResponse** `{ string prefill_replica_id; string decode_replica_id; string transport_hint; string reason_code; }`
  transport_hint ∈ `Mooncake | NIXL | Direct`
- **GetCacheStateRequest** `{ string model_id; string tenant_id; }` / **GetCacheStateResponse** `{ repeated ReplicaStats replicas; CacheSummary summary; }`
- **ReplicaScore** `{ string replica_id; float score; int32 matched_tokens; float estimated_cache_hit_prob; }`
- **CacheStateUpdate** `{ string replica_id; string model_id; string tenant_id; string hash_scheme; int64 timestamp_us; repeated PrefixEntry prefixes; ReplicaStats stats; }`
- **PrefixEntry** `{ bytes prefix_hash; int32 token_count; }` — **metadata only**
- **ReplicaStats** `{ string replica_id; int64 cache_memory_bytes; float hit_rate; float pressure; }`
- **CacheEvent** `{ Type type; string replica_id; string model_id; string tenant_id; bytes prefix_hash; int64 timestamp_us; }`
  Type ∈ `PREFIX_ADDED | PREFIX_EVICTED | REPLICA_UPDATED | ALL_CLEARED`
- **Metric** `{ string name; string type; map<string,string> labels; double value; int64 timestamp_us; }` (Prometheus `inferencecache_*`, tech spec §4.3)
- **StreamEventsRequest** `{ string model_id; string tenant_id; repeated CacheEvent.Type types; }`
- **StreamMetricsRequest** `{ repeated string names; }`
- **Ack** `{ bool accepted; string reason_code; }`
- **SLO** `{ int32 ttft_ms; int32 tbt_ms; }`

## Contract guarantees

- **Side-effect-free** lookups (`RenderTemplate`, `LookupRoute`, `LookupPDRoute`, `GetCacheState`) apart from emitting metrics — safe on the gateway hot path.
- **Fail-open**: empty `replica_scores` + `NO_HINT` is valid; the client treats it as a no-op. The server should answer within `slo` / the client's lookup timeout; otherwise the client proceeds without a hint.
- **Engine-opaque `prefix_hash`**: the server matches bytes only *within a matching `hash_scheme`* and never interprets them — vLLM and SGLang hashing stay disjoint, no cross-engine false hits.
- **Deterministic `RenderTemplate`** for a fixed `(template_ref, variables, template_revision)`.
- **Metadata only**: `CacheStateUpdate` / `PrefixEntry` carry hashes + stats, **never KV tensors or prompt text**.

## Scope of B4 (this contract)

Lands: the proto, generated Go stubs, and the `InferenceCache` service registered on the server with **fail-open stub handlers** (`LookupRoute`→`NO_HINT`; `RenderTemplate`→passthrough; `LookupPDRoute`→empty; `GetCacheState`→empty; `ReportCacheState`/`PublishEvent`→drain + `Ack`; `StreamCacheEvents`/`StreamMetrics`→close immediately). Removes the `Ping` placeholder; keeps `grpc.health.v1`.

Out of scope (later modules): real logic behind the RPCs — index-backed `LookupRoute` (B6), template rendering (D-series), PD routing (Phase 2), real metrics/events (M10). Java stubs are generated when the gateway client (E1) needs them.
