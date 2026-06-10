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
  // Consumer side (gateways) — fail-open; side-effect-free apart from metrics
  // (and the narrow LFU access-counter credit on delivered LookupRoute hits —
  // see Contract guarantees).
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
- **LookupRouteRequest** `{ string model_id = 1; string tenant_id = 2; bytes prefix_hash = 3; int32 prefix_token_count = 4; string hash_scheme = 5; SLO slo = 6; repeated bytes block_hashes = 7; repeated int32 block_token_counts = 8; repeated uint32 token_ids = 9; string prompt_text = 10; }`
- **LookupRouteResponse** `{ repeated ReplicaScore replica_scores; string reason_code; int64 lookup_latency_us; repeated uint32 token_ids = 4; }`
  reason ∈ `PREFIX_MATCH | TENANT_HOT | NO_HINT | TIMEOUT | UNKNOWN_TENANT | UNKNOWN_MODEL | UNKNOWN_HASH_SCHEME`
- **LookupPDRouteRequest** `{ string model_id; string tenant_id; bytes prefix_hash; int32 prefix_token_count; string pd_topology_ref; }`
- **LookupPDRouteResponse** `{ string prefill_replica_id; string decode_replica_id; string transport_hint; string reason_code; }`
  transport_hint ∈ `Mooncake | NIXL | Direct`
- **GetCacheStateRequest** `{ string model_id; string tenant_id; }` / **GetCacheStateResponse** `{ repeated ReplicaStats replicas; CacheSummary summary; }`
- **ReplicaScore** `{ string replica_id; float score; int32 matched_tokens; float estimated_cache_hit_prob; }`
- **CacheStateUpdate** `{ string replica_id; string model_id; string tenant_id; string hash_scheme; int64 timestamp_us; repeated PrefixEntry prefixes; ReplicaStats stats; }`
- **PrefixEntry** `{ bytes prefix_hash = 1; int32 token_count = 2; repeated bytes block_hashes = 3; repeated int32 block_token_counts = 4; }` — **metadata only**
- **ReplicaStats** `{ string replica_id = 1; int64 cache_memory_bytes = 2; float hit_rate = 3; float pressure = 4; string client_version = 5; }`
  `client_version` is an opaque version string identifying the client-side cache library the reporting replica is linked against (e.g. an LMCache client release). It carries no semver semantics on the wire — producers populate it, the server accepts it, no semver parsing or ordering is performed at this layer. Empty / unset is allowed and means "unknown"; older producers that don't fill the field MUST keep being accepted (additive, v1alpha1-compatible). The field reserves wire space for the producer half of an end-to-end client/server version-skew detection surface; the consumer half (server-side storage, per-CacheBackend exposure, the operator-visible status condition that closes the silent client/server mismatch class of bug) lands in a follow-up change and is intentionally out of scope for this contract update.
- **CacheEvent** `{ Type type; string replica_id; string model_id; string tenant_id; bytes prefix_hash; int64 timestamp_us; }`
  Type ∈ `PREFIX_ADDED | PREFIX_EVICTED | REPLICA_UPDATED | ALL_CLEARED`
- **Metric** `{ string name; string type; map<string,string> labels; double value; int64 timestamp_us; }` (Prometheus `inferencecache_*`, tech spec §4.3)
- **StreamEventsRequest** `{ string model_id; string tenant_id; repeated CacheEvent.Type types; }`
- **StreamMetricsRequest** `{ repeated string names; }`
- **Ack** `{ bool accepted; string reason_code; }`
- **SLO** `{ int32 ttft_ms; int32 tbt_ms; }`

## Contract guarantees

- **Side-effect-free** lookups (`RenderTemplate`, `LookupRoute`, `LookupPDRoute`, `GetCacheState`) apart from emitting metrics — safe on the gateway hot path. **One narrow exception:** in a namespace whose `CachePolicy.spec.eviction` is `LFU`, a `LookupRoute` call that *delivers* a prefix-match hint credits the matched entries' per-entry access counters (soft, lock-free eviction-ordering state). It never changes the response, never errors, and a `TIMEOUT`'d lookup credits nothing; the counter only influences which entries the cap-based sweep evicts later (see [policy-propagation.md](policy-propagation.md)). LRU namespaces and non-hit responses stay strictly side-effect-free apart from metrics.
- **Fail-open**: an empty `replica_scores` response is always valid and the client treats it as a no-op, regardless of the accompanying `reason_code` (`NO_HINT`, `TIMEOUT`, or the diagnostic `UNKNOWN_TENANT` / `UNKNOWN_MODEL` / `UNKNOWN_HASH_SCHEME` codes — see "Diagnostic reason codes" below). The server should answer within `slo` / the client's lookup timeout; otherwise the client proceeds without a hint.
- **`TIMEOUT` is fail-open too.** When a `CachePolicy.spec.lookupTimeoutMs` is configured for the tenant and the lookup exceeds it (or the caller's ctx is already past its deadline on arrival), `LookupRoute` returns empty `replica_scores` with `reason_code: TIMEOUT` rather than an error. Clients treat it like `NO_HINT`. See [reason-codes.md](../reference/reason-codes.md).
- **Engine-opaque `prefix_hash` / `block_hashes`**: the server matches bytes only *within a matching `hash_scheme`* and never interprets them — vLLM and SGLang hashing stay disjoint, no cross-engine false hits. An empty/unspecified `hash_scheme` is **not** a valid domain: such ingest entries are dropped and such lookups return `NO_HINT` (fail open), so a missing tag can never collapse engines into one compatibility domain. The same rule applies to chain-bearing ingests and lookups.
- **Deterministic `RenderTemplate`** for a fixed `(template_ref, variables, template_revision)`.
- **Metadata only**: `CacheStateUpdate` / `PrefixEntry` carry hashes + stats, **never KV tensors or prompt text**.
- **Additive `CacheStateUpdate`**: updates are **incremental deltas (adds/refreshes), not full snapshots** — a replica's prefixes are *not* pruned by their absence from a later update. Removals arrive as `CacheEvent` (`PREFIX_EVICTED` / `ALL_CLEARED`) or expire via TTL. This matches the engine KV-event model (vLLM `BlockStored` / `BlockRemoved`); a stale entry yields a cache miss, never a wrong answer (soft state).
- **Reserved `tenant_id` namespace.** `tenant_id = "inferencecache.io/probe"` is reserved for the server's functional self-test and is treated specially across the contract: `LookupRoute` returns empty `replica_scores` with `reason_code: NO_HINT`; `GetCacheState` returns the empty aggregate; `ReportCacheState` / `PublishEvent` silently drop messages targeting it; `/snapshot` excludes its replicas, prefixes, and tenant row from the cluster aggregate. External callers cannot read or write the reserved scope through the public contract, and the `CacheTenant` admission webhook rejects CRs that newly claim `spec.tenantID = inferencecache.io/probe`. See [`policy-propagation.md` §`/probe` wire contract](./policy-propagation.md) for the in-process probe path that bypasses the gRPC handlers.

## Scope of B4 (this contract)

Lands: the proto, generated Go stubs, and the `InferenceCache` service registered on the server with **fail-open stub handlers** (`LookupRoute`→`NO_HINT`; `RenderTemplate`→passthrough; `LookupPDRoute`→empty; `GetCacheState`→empty; `ReportCacheState`/`PublishEvent`→drain + `Ack`; `StreamCacheEvents`/`StreamMetrics`→close immediately). Removes the `Ping` placeholder; keeps `grpc.health.v1`.

What B4 originally landed (now partly superseded by B6, see below): fail-open stub handlers for every RPC and the empty `GetCacheState`.

Still out of scope (later modules): template rendering (D-series), PD routing (Phase 2), and the event/metric **streams** `StreamCacheEvents` / `StreamMetrics` (M10). Java stubs are generated when the gateway client (E1) needs them.

**Update — B6 (cache index):** `LookupRoute`, `ReportCacheState`, `PublishEvent`, and `GetCacheState` are now backed by the in-memory `CacheIndex` (`pkg/index`): `ReportCacheState` ingests additive deltas; `PublishEvent` applies scheme-safe deltas only — `PREFIX_EVICTED` / `ALL_CLEARED` (removals) and `REPLICA_UPDATED` (replica liveness), while `PREFIX_ADDED` is a no-op (events carry no `hash_scheme`, so additions/refreshes come via `ReportCacheState`); `LookupRoute` returns ranked replicas (`PREFIX_MATCH` / `TENANT_HOT`), a fail-open miss (`NO_HINT` — no match, no warm-tenant fallback, below the `CachePolicy.spec.minimumPrefixTokens` request-side gate, every candidate replica matched fewer tokens than the `CachePolicy.spec.minimumMatchedTokens` result-side floor — see "Matched-tokens floor" below — or the top per-replica score fell below the `CachePolicy.spec.routingFloorScore` post-score floor on the distinguishing-power-aware ranker — see [`lookuproute-ranking.md` §2.7](./lookuproute-ranking.md#27-the-replica-distinguishing-power-factor)), a deadline breach (`TIMEOUT` — `CachePolicy.spec.lookupTimeoutMs`, still fail-open), or one of the diagnostic codes (`UNKNOWN_TENANT` / `UNKNOWN_MODEL` / `UNKNOWN_HASH_SCHEME` — set-but-wrong contract key, see "Diagnostic reason codes" below); and `GetCacheState` returns the `(tenant, model)` aggregate. The lookup/index metrics (`inferencecache_index_entries`, `inferencecache_lookup_route_*`) are emitted on `/metrics`. `RenderTemplate`, `LookupPDRoute`, and the streams remain fail-open stubs.

#### Matched-tokens floor

`PREFIX_MATCH` requires the realized per-replica overlap to clear the per-namespace
`CachePolicy.spec.minimumMatchedTokens` floor, applied AFTER the index lookup returns.
Replicas whose matched-tokens count falls below the floor are filtered from the
response; when no replica survives, the reason code downgrades to `NO_HINT` and the
gateway round-robins honestly instead of being credited with a trivial 1-block
chat-template-only match. The server applies a default of `64` (4 KV blocks at the
typical 16-token block size) to any tenant with no `CachePolicy` installed; an
explicit `minimumMatchedTokens: 0` disables the floor for that namespace. Distinct
from the pre-lookup `minimumPrefixTokens` request-side gate — see the policy field
docs in [`policy-crds.md`](./policy-crds.md) and the operator guide at
[`docs/concepts/cachepolicy-tuning.md`](../concepts/cachepolicy-tuning.md). The
filtering happens on the server before the response is built, so the wire shape is
unchanged and old clients continue to fail open on a downgrade.

**Update — B6 (CacheIndex status surface):** the cluster-wide aggregate is now exposed two ways: an internal HTTP `/snapshot` endpoint on the server (JSON; metadata only — replica/tenant stats + prefix counts, never KV/prompt data), and a cluster-scoped, status-only `CacheIndex` CRD (`kubectl get cacheindex`) that the controller maintains by scraping `/snapshot`. This is outside the gRPC contract (no proto change); see the `CacheIndex` type in `api/v1alpha1` and the `CacheIndexPoller` in `internal/controller`.

**Update — B6 follow-up (`LookupRoute` ranking v2):** the `LookupRoute` ranker layers additive strategies on top of the original `matched_tokens × freshness` baseline, **without any proto change** (all inputs were already on the contract). Today the full score is `matched_tokens × freshness × pressure_factor × slo_bias × distinguishing_power` where `pressure_factor = max(0, 1 - PressureWeight × ReplicaStats.pressure)`, `slo_bias = 1 + freshness × SLOTightBias` when `SLO.ttft_ms` is below a configurable threshold (otherwise 1), and `distinguishing_power = 1 − num_matching_replicas / total_replicas` (1.0 when `total_replicas ≤ 1`; see [`lookuproute-ranking.md` §2.7](./lookuproute-ranking.md#27-the-replica-distinguishing-power-factor)). On a prefix miss, the server falls back to **`TENANT_HOT`**: ranked replicas that are warm for the request's `(tenant, model, hash_scheme)` — i.e. the replica has at least one prefix entry in the requested engine domain AND its latest stats are recent (within a configurable window, default 5m) with `hit_rate` above a floor (default 0.1). `TENANT_HOT` responses carry `matched_tokens=0` because there is no prefix overlap; the gateway must rely on `reason_code`, not `matched_tokens`, to recognize the soft hint. The pressure/SLO factors collapse to 1 when their supporting input is absent (no stats → pressure_factor = 1; no SLO hint → slo_bias = 1; `TenantHotMaxAge=0` disables the TENANT_HOT fallback entirely); the distinguishing-power factor collapses to 1 only for single-replica deployments — multi-replica deployments always see a cardinality-adjusted score. See [`lookuproute-ranking.md`](./lookuproute-ranking.md) and [`reason-codes.md`](../reference/reason-codes.md) for the full knob table.

## Diagnostic reason codes

`LookupRoute` distinguishes a *novel prefix* (the cache plane has the requested `(tenant_id, model_id, hash_scheme)` populated but not this particular prefix) from a *contract-key mismatch* (the caller asked with a key the cache plane does not recognize at all). The latter is almost always a misconfigured gateway/SDK; surfacing it as a specific `reason_code` is what lets operators debug "100% `NO_HINT`" without re-deriving the layering from packet captures. Closes the wrong-`hash_scheme` and wrong-`tenant_id` silent-miss patterns observed against real clusters.

> **Rule.** Every contract key that can mismatch returns a specific `reason_code` on key-level no-data — not the catch-all `NO_HINT`.

For `LookupRoute` that gives three additive codes on top of the existing vocabulary:

| Code | Emitted when |
|---|---|
| `UNKNOWN_TENANT` | The request supplied a non-empty `tenant_id`, the index is **not globally empty**, and that `tenant_id` has **zero prefix entries** across every model and hash scheme. |
| `UNKNOWN_MODEL` | The tenant is known but the `(tenant_id, model_id)` pair has **zero entries**. |
| `UNKNOWN_HASH_SCHEME` | `(tenant_id, model_id)` has entries, but **none under the request's `hash_scheme`** (e.g. ingest under `vllm`, lookup under `vllm-v1`). |

Diagnostic classification runs after the prefix-match miss, and after the `TENANT_HOT` fallback miss for non-chain requests; chain-bearing requests skip `TENANT_HOT` entirely (by design — a soft locality nudge is the wrong answer to a longest-prefix question) and go straight to the classifier. The miss-classification is identical for both paths. A **cold-start carve-out** keeps a globally empty index on `NO_HINT` — without it a freshly-started server would surface `UNKNOWN_TENANT` for every query until the first `ReportCacheState` lands, flooding gateways with configuration-error signals during normal operation. The diagnostic resumes the moment any replica reports state, which is the asymmetric configuration-drift case the codes are targeted at.

Codes are emitted in **outer-to-inner scope order** — tenant first, then model within tenant, then scheme within (tenant, model) — so the caller always learns the **outermost** mismatched key (the one that has to be fixed first regardless of whether the deeper-scoped keys are right). An empty `tenant_id`, `model_id`, or `hash_scheme` is a contract violation (not a mismatch) and continues to surface as `NO_HINT`; the diagnostic codes diagnose set-but-wrong keys only. `TIMEOUT`, `PREFIX_MATCH`, and `TENANT_HOT` semantics are unchanged.

Old clients degrade `UNKNOWN_*` to their no-hint default per the forward-compatibility rule in [`../reference/reason-codes.md`](../reference/reason-codes.md), so the change is backward-compatible at v1alpha1. Gateway-SDKs that are updated should treat `UNKNOWN_*` like a configuration error (surface to a log/metric, **still fail open** — the cache plane is hint-only, never blocking); `NO_HINT` continues to mean "route normally". See [`lookuproute-diagnostics.md`](./lookuproute-diagnostics.md) for the full design (including SDK-author guidance on the `tenant_id = $(POD_NAMESPACE)` convention that producers use).

## Longest-prefix (block-level) matching

The contract supports expressing a prefix as an **ordered, engine-aligned chain of block hashes** alongside the legacy single `prefix_hash`. This unlocks **longest-common-prefix** ranking: requests that share the first N KV blocks of a prefix (common with shared system prompts + per-request RAG) get a `PREFIX_MATCH` reflecting the partial run instead of `NO_HINT` from exact-full-hash mismatch. See [`lookuproute-ranking.md` §2.5](./lookuproute-ranking.md) for the walk-through and worked example.

**Shape (additive, v1alpha1-compatible).**
- `PrefixEntry` gains `repeated bytes block_hashes` + parallel `repeated int32 block_token_counts` — ordered, engine block-boundary aligned, same `hash_scheme` semantics as `prefix_hash`. Engines that hash per block (vLLM, SGLang) can report the chain in a single entry; the server expands it into per-block index entries on ingest. Legacy entries that only carry `prefix_hash` + `token_count` continue to work as exact-match. A chain lookup can still hit a legacy single-blob entry when the request's first block hash (`block_hashes[0]`) matches the legacy `prefix_hash` exactly; deeper blocks of the request can never match a single-blob entry (so the partial run against a legacy entry is at most one block), and the leading-run rule means a legacy entry never contributes to a multi-block partial match — for that the producer must either report a chain `PrefixEntry` or emit one legacy `PrefixEntry` per block (as the existing vLLM subscriber does, populating the per-block index keys end-to-end).
- `LookupRouteRequest` gains the same parallel `block_hashes` / `block_token_counts` fields. When the request carries a non-empty chain (parallel lengths must match), the server walks block-by-block; otherwise it falls back to exact-match on `prefix_hash` (the old path is unchanged).

**Matching semantics.** For each replica, the server finds the **longest leading run** `[block_hashes[0]..block_hashes[k]]` such that the replica holds every block in that run (within the request's `hash_scheme`). `matched_tokens` on the returned `ReplicaScore` is the sum of the request's `block_token_counts[0..k]` — i.e. the token count of the partial prefix the replica already has cached. `reason_code` is `PREFIX_MATCH` when at least one replica's `matched_tokens` clears the per-namespace `CachePolicy.spec.minimumMatchedTokens` floor — see "Matched-tokens floor" above — AND the top surviving replica's score (after the distinguishing-power factor multiplies in) clears the per-namespace `CachePolicy.spec.routingFloorScore` floor — see [`lookuproute-ranking.md` §2.7](./lookuproute-ranking.md#27-the-replica-distinguishing-power-factor). A chain match that produces only sub-floor per-replica overlaps (e.g. a 1-block run under the default 64-token floor) downgrades to `NO_HINT` via the matched-tokens filter; a chain match that clears the per-replica filter but whose top score still falls below the routing floor (e.g. every replica reached the same depth — distinguishing-power = 0 → score 0) downgrades to `NO_HINT` via the routing-floor gate. When no replica matches the first block the request lands on the same miss-classifier as the exact-match path (see "Diagnostic reason codes" above): same-key chain misses surface as `NO_HINT` (genuinely novel prefix); chain misses with a wrong contract key surface as the matching `UNKNOWN_*` code. Chain misses never fall through to `TENANT_HOT` — a chain caller is asking specifically for longest-prefix matching, and a softer locality hint would be a wrong answer to that specific question. The freshness signal used for ranking is the **oldest** `lastSeen` across the matched blocks (weakest link).

**Precedence and fail-soft.** When a `PrefixEntry` or `LookupRouteRequest` carries both the chain (`block_hashes` + `block_token_counts`) and the legacy single-blob fields, the chain takes precedence and the legacy fields are ignored — except that chain ingest also writes the legacy single-blob key when `prefix_hash` is set, so unmigrated `LookupRoute` callers using exact `prefix_hash` still hit. When the chain is set but the two parallel arrays disagree in length the message is malformed: the server **drops the entry on ingest and returns `NO_HINT` on lookup** — it does not silently downgrade to the legacy single-blob path. A stale hint is acceptable in this soft-state index; a wrong hint is not.

**Block-hash position assumption.** The index matches block hashes by exact byte equality without tracking the position in which a block was originally reported. The longest-leading-run rank therefore only describes a *leading* prefix when the engine's block hashes are parent-chained (vLLM, SGLang both are) — i.e. a block hash uniquely identifies the prefix that ends in that block. Engines that ever emit position-blind block hashes (same bytes for a "middle" block as for a "leading" block) would violate this assumption and should not be ingested with the chain form.

**Engine-opaque + metadata-only guarantees still hold.** Block hashes are engine-defined opaque bytes; matching is byte-equality within a `hash_scheme`, and an empty `hash_scheme` still fails open (drop on ingest / `NO_HINT` on lookup) — the rule extends to chain-bearing ingests and lookups. Block hashes + per-block token counts are metadata only — never KV tensors or prompt text. Cross-tenant isolation is unchanged (the chain is scoped by tenant + model just like the legacy key).

## Content-fingerprint routing key

**Update — content-fingerprint routing key.** The engine's *own* KV-block hash is seeded by a per-process-random value (vLLM's `NONE_HASH = os.urandom(32)` when `PYTHONHASHSEED` is unset), so it is **not reproducible** across replicas or by an external consumer — a gateway can never recompute it, and every `LookupRoute` would miss. The `vllm` `hash_scheme` therefore keys the index on a **deterministic content fingerprint** derived from token content, not the engine hash. The server still treats `prefix_hash` / `block_hashes` as opaque bytes (matching is byte-equality within a `hash_scheme`); this section only specifies how a producer/consumer computes them so that ingest and lookup agree.

Construction (XXH3-64, matching SMG `event_tree.rs` so the two interoperate on one index):

- `SEED = 1337`.
- Per-block `content_hash = XXH3_64(seed=SEED)` over the little-endian `uint32` encoding of each token id in the block.
- Rolling positional prefix hash: `prefix_hash[0] = content_hash[0]`; `prefix_hash[i] = XXH3_64(seed=SEED)` over `prefix_hash[i-1].to_le_bytes(8) ++ content_hash[i].to_le_bytes(8)`.
- Wire encoding: each `prefix_hash` is the **8-byte big-endian** form of that `uint64`.

The fingerprint is positional (block `i`'s key identifies the whole prefix `0..i`), satisfying the parent-chained block-hash assumption above. Both ends must agree: the `kvevent-subscriber` computes it in-pod from the engine event's `token_ids` (the tokens never leave the pod — only the hash does); a gateway recomputes it from the prompt's tokens. Reference implementation: `pkg/fingerprint`. **Caveat:** the fingerprint is token-content-only, so prefixes that differ only by LoRA adapter (identical tokens) share a key unless partitioned by `model_id` — tracked as a follow-up.

**Update — dual-input `LookupRoute` (server-side tokenization).** `LookupRouteRequest` adds `token_ids` (9) and `prompt_text` (10) so a gateway that carries **no tokenizer** can let the server compute the fingerprint. The server resolves exactly one input by precedence:

1. An explicit `prefix_hash` / `block_hashes` chain (a gateway that already fingerprinted) — used as-is, unchanged from the existing path.
2. `token_ids` — the server derives the block-hash chain directly via `pkg/fingerprint` (`Chain(token_ids, block_size)` → the same per-block positional hashes the subscriber ingests). No tokenizer needed, so this works in every server build.
3. `prompt_text` — the server wraps the text as a **single `user` chat message**, applies `model_id`'s chat template **with the assistant generation prompt appended**, tokenizes (the server-owned tokenizer), then fingerprints as in (2). This single-turn wrapping is the only supported shape today; multi-turn message arrays and already-rendered / completion-style prompts are not (a structured-messages field is a future addition), and `token_ids` is the escape hatch for callers needing exact control. This path requires a server built with the tokenizer (build tag `smgcgo`) **and** an explicit, vetted `--tokenizer-models-dir`; without either it **fails open to `NO_HINT`** rather than erroring. Tokenizers are **loaded eagerly at startup** from that directory (one subdir per model) and served from memory — the lookup path itself performs **no per-request I/O or downloads** and a request `model_id` is never joined onto a filesystem path, so `LookupRoute` stays side-effect-free apart from metrics (the model_id is confined to the pre-loaded set; an unknown model fails open). Tokenization is still bounded by the tenant's `lookupTimeoutMs`, or a default safety timeout when none is set. The canonical `token_ids` the server produced are echoed in `LookupRouteResponse.token_ids` (4) **only on this path**, so the caller forwards exactly those tokens to the engine (e.g. OpenAI `/v1/completions` with a token-id `prompt`) — the engine then caches the same tokens the lookup was fingerprinted over, so routing key and engine cache key match **by construction**, with no tokenizer-parity dependence between gateway and engine.

`model_id` / `tenant_id` / `hash_scheme` are still supplied by the caller on every path — they are engine/routing config, not tokenizer knowledge. The block size used to fingerprint token_ids / prompt_text MUST match the engine's KV block size and the kvevent-subscriber's; it defaults to 16 (vLLM's default) and is set with the server's `--engine-block-size` flag. All input paths funnel into the identical block-chain matching semantics (the longest-leading-run rule above), so `reason_code`, the matched-tokens / routing-floor gates, and fail-open behavior are unchanged. A `token_ids` / `prompt_text` input too short to fill one block yields an empty chain → fail-open `NO_HINT`. `LookupRoute` stays side-effect-free apart from metrics (tokenization is transient: the server sees prompt text only to hash it, never stores it).
