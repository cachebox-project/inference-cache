# Design: gRPC contract (`InferenceCache` service)

Status: implemented · Implements: B4 (contract + fail-open stubs), B6 (index-backed `LookupRoute` / `ReportCacheState` / `PublishEvent` / `GetCacheState`) · Tracks: InferenceCache tech spec §4.2–4.4

This is the public API gateways and engines integrate against — the load-bearing contract that unblocks the cache index (B6), engine KV-event hook (C1), and gateway clients (E1). Get the signature right early; the bytes behind it are filled in by later modules.

**Transport:** the policy server serves `:9090` **plaintext by default** — including in `config/default` — because today's gRPC clients (the in-cluster `kvevent-subscriber` producer and the external gateway client) are not yet TLS-ready. One-sided **Service TLS via cert-manager is an opt-in overlay** (`config/overlays/server-tls`) that flips the server on; see [`grpc-tls.md`](grpc-tls.md). When enabled, a verifying client dials the server's Service FQDN and the server presents a cert-manager-minted cert for that name. mTLS (client-cert verification) is deferred to Phase 2.

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
- **LookupRouteRequest** `{ string model_id = 1; string tenant_id = 2; bytes prefix_hash = 3; int32 prefix_token_count = 4; string hash_scheme = 5; SLO slo = 6; repeated bytes block_hashes = 7; repeated int32 block_token_counts = 8; }`
- **LookupRouteResponse** `{ repeated ReplicaScore replica_scores; string reason_code; int64 lookup_latency_us; }`
  reason ∈ `PREFIX_MATCH | TENANT_HOT | NO_HINT | TIMEOUT`
- **LookupPDRouteRequest** `{ string model_id; string tenant_id; bytes prefix_hash; int32 prefix_token_count; string pd_topology_ref; }`
- **LookupPDRouteResponse** `{ string prefill_replica_id; string decode_replica_id; string transport_hint; string reason_code; }`
  transport_hint ∈ `Mooncake | NIXL | Direct`
- **GetCacheStateRequest** `{ string model_id; string tenant_id; }` / **GetCacheStateResponse** `{ repeated ReplicaStats replicas; CacheSummary summary; }`
- **ReplicaScore** `{ string replica_id; float score; int32 matched_tokens; float estimated_cache_hit_prob; }`
- **CacheStateUpdate** `{ string replica_id; string model_id; string tenant_id; string hash_scheme; int64 timestamp_us; repeated PrefixEntry prefixes; ReplicaStats stats; }`
- **PrefixEntry** `{ bytes prefix_hash = 1; int32 token_count = 2; repeated bytes block_hashes = 3; repeated int32 block_token_counts = 4; }` — **metadata only**
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
- **Fail-open**: empty `replica_scores` with `NO_HINT` (or `TIMEOUT` — see below) is valid; the client treats it as a no-op. The server should answer within `slo` / the client's lookup timeout; otherwise the client proceeds without a hint.
- **`TIMEOUT` is fail-open too.** When a `CachePolicy.spec.lookupTimeoutMs` is configured for the tenant and the lookup exceeds it (or the caller's ctx is already past its deadline on arrival), `LookupRoute` returns empty `replica_scores` with `reason_code: TIMEOUT` rather than an error. Clients treat it like `NO_HINT`. See [reason-codes.md](../reference/reason-codes.md).
- **Engine-opaque `prefix_hash` / `block_hashes`**: the server matches bytes only *within a matching `hash_scheme`* and never interprets them — vLLM and SGLang hashing stay disjoint, no cross-engine false hits. An empty/unspecified `hash_scheme` is **not** a valid domain: such ingest entries are dropped and such lookups return `NO_HINT` (fail open), so a missing tag can never collapse engines into one compatibility domain. The same rule applies to chain-bearing ingests and lookups.
- **Deterministic `RenderTemplate`** for a fixed `(template_ref, variables, template_revision)`.
- **Metadata only**: `CacheStateUpdate` / `PrefixEntry` carry hashes + stats, **never KV tensors or prompt text**.
- **Additive `CacheStateUpdate`**: updates are **incremental deltas (adds/refreshes), not full snapshots** — a replica's prefixes are *not* pruned by their absence from a later update. Removals arrive as `CacheEvent` (`PREFIX_EVICTED` / `ALL_CLEARED`) or expire via TTL. This matches the engine KV-event model (vLLM `BlockStored` / `BlockRemoved`); a stale entry yields a cache miss, never a wrong answer (soft state).

## Scope of B4 (this contract)

Lands: the proto, generated Go stubs, and the `InferenceCache` service registered on the server with **fail-open stub handlers** (`LookupRoute`→`NO_HINT`; `RenderTemplate`→passthrough; `LookupPDRoute`→empty; `GetCacheState`→empty; `ReportCacheState`/`PublishEvent`→drain + `Ack`; `StreamCacheEvents`/`StreamMetrics`→close immediately). Removes the `Ping` placeholder; keeps `grpc.health.v1`.

What B4 originally landed (now partly superseded by B6, see below): fail-open stub handlers for every RPC and the empty `GetCacheState`.

Still out of scope (later modules): template rendering (D-series), PD routing (Phase 2), and the event/metric **streams** `StreamCacheEvents` / `StreamMetrics` (M10). Java stubs are generated when the gateway client (E1) needs them.

**Update — B6 (cache index):** `LookupRoute`, `ReportCacheState`, `PublishEvent`, and `GetCacheState` are now backed by the in-memory `CacheIndex` (`pkg/index`): `ReportCacheState` ingests additive deltas; `PublishEvent` applies scheme-safe deltas only — `PREFIX_EVICTED` / `ALL_CLEARED` (removals) and `REPLICA_UPDATED` (replica liveness), while `PREFIX_ADDED` is a no-op (events carry no `hash_scheme`, so additions/refreshes come via `ReportCacheState`); `LookupRoute` returns ranked replicas, `NO_HINT` (no match or below the `CachePolicy.spec.minimumPrefixTokens` gate), or `TIMEOUT` (`CachePolicy.spec.lookupTimeoutMs` budget breach — still fail-open); and `GetCacheState` returns the `(tenant, model)` aggregate. The lookup/index metrics (`inferencecache_index_entries`, `inferencecache_lookup_route_*`) are emitted on `/metrics`. `RenderTemplate`, `LookupPDRoute`, and the streams remain fail-open stubs.

**Update — B6 (CacheIndex status surface):** the cluster-wide aggregate is now exposed two ways: an internal HTTP `/snapshot` endpoint on the server (JSON; metadata only — replica/tenant stats + prefix counts, never KV/prompt data), and a cluster-scoped, status-only `CacheIndex` CRD (`kubectl get cacheindex`) that the controller maintains by scraping `/snapshot`. This is outside the gRPC contract (no proto change); see the `CacheIndex` type in `api/v1alpha1` and the `CacheIndexPoller` in `internal/controller`.

**Update — B6 follow-up (`LookupRoute` ranking v2):** the `LookupRoute` ranker now layers three additive strategies on top of the original `matched_tokens × freshness` baseline, **without any proto change** (all inputs were already on the contract). Score becomes `matched_tokens × freshness × pressure_factor × slo_bias` where `pressure_factor = max(0, 1 - PressureWeight × ReplicaStats.pressure)` and `slo_bias = 1 + freshness × SLOTightBias` when `SLO.ttft_ms` is below a configurable threshold (otherwise 1). On a prefix miss, the server falls back to **`TENANT_HOT`**: ranked replicas that are warm for the request's `(tenant, model, hash_scheme)` — i.e. the replica has at least one prefix entry in the requested engine domain AND its latest stats are recent (within a configurable window, default 5m) with `hit_rate` above a floor (default 0.1). `TENANT_HOT` responses carry `matched_tokens=0` because there is no prefix overlap; the gateway must rely on `reason_code`, not `matched_tokens`, to recognize the soft hint. Every knob is tunable per binary and the formula collapses back to the baseline whenever its supporting input is absent (no stats → pressure=0 → factor 1; no SLO hint → bias 0; `TenantHotMaxAge=0` disables the fallback entirely). See [`lookuproute-ranking.md`](./lookuproute-ranking.md) and [`reason-codes.md`](../reference/reason-codes.md) for the full knob table.

## Longest-prefix (block-level) matching

The contract supports expressing a prefix as an **ordered, engine-aligned chain of block hashes** alongside the legacy single `prefix_hash`. This unlocks **longest-common-prefix** ranking: requests that share the first N KV blocks of a prefix (common with shared system prompts + per-request RAG) get a `PREFIX_MATCH` reflecting the partial run instead of `NO_HINT` from exact-full-hash mismatch. See [`lookuproute-ranking.md` §2.5](./lookuproute-ranking.md) for the walk-through and worked example.

**Shape (additive, v1alpha1-compatible).**
- `PrefixEntry` gains `repeated bytes block_hashes` + parallel `repeated int32 block_token_counts` — ordered, engine block-boundary aligned, same `hash_scheme` semantics as `prefix_hash`. Engines that hash per block (vLLM, SGLang) can report the chain in a single entry; the server expands it into per-block index entries on ingest. Legacy entries that only carry `prefix_hash` + `token_count` continue to work as exact-match. A chain lookup can still hit a legacy single-blob entry when the request's first block hash (`block_hashes[0]`) matches the legacy `prefix_hash` exactly; deeper blocks of the request can never match a single-blob entry (so the partial run against a legacy entry is at most one block), and the leading-run rule means a legacy entry never contributes to a multi-block partial match — for that the producer must either report a chain `PrefixEntry` or emit one legacy `PrefixEntry` per block (as the existing vLLM subscriber does, populating the per-block index keys end-to-end).
- `LookupRouteRequest` gains the same parallel `block_hashes` / `block_token_counts` fields. When the request carries a non-empty chain (parallel lengths must match), the server walks block-by-block; otherwise it falls back to exact-match on `prefix_hash` (the old path is unchanged).

**Matching semantics.** For each replica, the server finds the **longest leading run** `[block_hashes[0]..block_hashes[k]]` such that the replica holds every block in that run (within the request's `hash_scheme`). `matched_tokens` on the returned `ReplicaScore` is the sum of the request's `block_token_counts[0..k]` — i.e. the token count of the partial prefix the replica already has cached. `reason_code` remains `PREFIX_MATCH` for any non-empty match (one block or many) and `NO_HINT` when no replica matches the first block. The freshness signal used for ranking is the **oldest** `lastSeen` across the matched blocks (weakest link).

**Precedence and fail-soft.** When a `PrefixEntry` or `LookupRouteRequest` carries both the chain (`block_hashes` + `block_token_counts`) and the legacy single-blob fields, the chain takes precedence and the legacy fields are ignored — except that chain ingest also writes the legacy single-blob key when `prefix_hash` is set, so unmigrated `LookupRoute` callers using exact `prefix_hash` still hit. When the chain is set but the two parallel arrays disagree in length the message is malformed: the server **drops the entry on ingest and returns `NO_HINT` on lookup** — it does not silently downgrade to the legacy single-blob path. A stale hint is acceptable in this soft-state index; a wrong hint is not.

**Block-hash position assumption.** The index matches block hashes by exact byte equality without tracking the position in which a block was originally reported. The longest-leading-run rank therefore only describes a *leading* prefix when the engine's block hashes are parent-chained (vLLM, SGLang both are) — i.e. a block hash uniquely identifies the prefix that ends in that block. Engines that ever emit position-blind block hashes (same bytes for a "middle" block as for a "leading" block) would violate this assumption and should not be ingested with the chain form.

**Engine-opaque + metadata-only guarantees still hold.** Block hashes are engine-defined opaque bytes; matching is byte-equality within a `hash_scheme`, and an empty `hash_scheme` still fails open (drop on ingest / `NO_HINT` on lookup) — the rule extends to chain-bearing ingests and lookups. Block hashes + per-block token counts are metadata only — never KV tensors or prompt text. Cross-tenant isolation is unchanged (the chain is scoped by tenant + model just like the legacy key).
