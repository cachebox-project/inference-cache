# Design: Policy CRDs

Status: implemented · API group: `inferencecache.io/v1alpha1`

This document tracks the policy-side CRDs that sit beside `CacheBackend`. These resources are the declarative policy and observability surface; routing and serving decisions remain in the gRPC server and runtime adapters.

## CachePolicy

`CachePolicy` is namespaced. It controls cache lookup and eviction behavior.

| Field | Type | Purpose |
|---|---|---|
| `spec.eviction` | enum | Index eviction algorithm applied when the index exceeds its entry cap. `LRU` (default) evicts the oldest-by-`lastSeen` entry first; `LFU` evicts the lowest-access-count entry first, breaking ties on the oldest `lastSeen`. Access counts do not age — the `evictionTTL` sweep removes stale entries regardless of algorithm, so `LFU` does not pin hot-but-stale entries. The controller lower-cases this and propagates it on `ResolvedPolicy`. The index reads it on the cap-based sweep (to order victims) and, in `LFU` namespaces, on the lookup path (to record which entries a *delivered* `LookupRoute` hint credits — a timed-out lookup credits nothing); it never changes a lookup result, and the TTL sweep is algorithm-independent. |
| `spec.evictionTTL` | duration | Maximum usable lifetime for cache entries. |
| `spec.minimumPrefixTokens` | integer | Minimum *requested* prefix token count gated BEFORE the index lookup runs. A shorter request short-circuits to `NO_HINT` without touching the index. Minimum `0`. |
| `spec.minimumMatchedTokens` | integer | Minimum *matched* prefix token count required AFTER the index lookup runs for `PREFIX_MATCH` to surface. Replicas whose matched overlap falls below this floor are filtered; if none survive, the response is downgraded to `NO_HINT`. The CRD field has a `+kubebuilder:default=64` marker, so the apiserver materializes `64` (4 KV blocks at the typical 16-token block size) on any CR that omits the field. The server independently applies the same value as `DefaultMinimumMatchedTokens` when a tenant has *no* `CachePolicy` at all — so trivial 1-block chat-template overlaps are filtered in both shapes. Set to `0` on the CR to disable enforcement for that namespace (e.g. raw-recall benchmarking); the server-side `DefaultMinimumMatchedTokens` is the fallback ONLY for tenants without a CR. Distinct from `minimumPrefixTokens` — that field is a request-side gate; this is a result-side floor. Minimum `0`. |
| `spec.routingFloorScore` | stringified float (e.g. `"0.1"`, `"5"`, `"0"`) | Per-replica *score* below which a `PREFIX_MATCH` response downgrades to `NO_HINT`. Applied AFTER the [distinguishing-power-aware ranker](lookuproute-ranking.md) computes scores. Overlaps held by every replica (chat-template framing, RAG corpus headers, custom system prompts) see `distinguishing_power = 0`, score = 0, and this floor catches them. The CRD has a `+kubebuilder:default="0.1"` marker so the apiserver materializes `"0.1"` on any CR that omits the field; the server independently applies the same value (as `DefaultRoutingFloorScore`) when a tenant has *no* `CachePolicy` at all. Set to `"0"` on the CR to disable enforcement for that namespace (raw-recall benchmarking). Composes with `minimumMatchedTokens` — the matched-tokens floor is applied first per-replica, then this score floor gates the top survivor's score. Both floors can downgrade independently; an operator can disable either by setting it to its opt-out value. Distinct from `minimumPrefixTokens` — that's a request-side gate; this is a result-side floor on the realized score. Pattern-validated at admission. |
| `spec.lookupTimeoutMs` | integer | Lookup latency budget in milliseconds. Minimum `0`. |
| `spec.strategy.enableChainMatching` | boolean | Enables the block-hash chain matcher for `LookupRoute` requests that carry `block_hashes` + `block_token_counts`. Default `true` preserves the longest-common-leading-run behavior. When `false`, the handler strips chain fields before lookup and uses the legacy exact `prefix_hash` path. |
| `spec.strategy.requireChain` | boolean | Requires callers to provide a valid block-hash chain before the index is touched. Default `false` keeps legacy exact-prefix clients working. When `true` and a request has no chain, the server returns empty scores with `reason_code: POLICY_REQUIRES_CHAIN` (fail-open to normal gateway routing). Admission rejects `requireChain: true` with `enableChainMatching: false`. |
| `spec.strategy.enableTenantHot` | boolean | Allows the `TENANT_HOT` soft locality fallback. Default `true` preserves current behavior. When `false`, a tenant-hot result is downgraded to `NO_HINT` while prefix matches and diagnostic misses still behave normally. |

`status.conditions` and `status.observedGeneration` are reserved for controller observations.

Runtime propagation (controller → server `/policy`) is described in [policy-propagation.md](policy-propagation.md): `evictionTTL` drives per-tenant index eviction; `minimumPrefixTokens` and `lookupTimeoutMs` are enforced on the `LookupRoute` path before and around the index call respectively; `minimumMatchedTokens` is enforced on the `LookupRoute` path after the index returns (downgrades sub-floor matches to `NO_HINT`); `routingFloorScore` is enforced on the `LookupRoute` path after the distinguishing-power-aware ranker scores each candidate (downgrades whole responses whose top score falls below the floor); `strategy` gates which lookup strategies may surface; and `eviction` selects the per-namespace cap-based eviction algorithm. The two eviction knobs are orthogonal: `evictionTTL` removes stale entries on the freshness sweep regardless of algorithm, while `eviction` only decides which entries the cap sweep drops when the index is over its entry cap. The lookup-filtering knobs are orthogonal too: `minimumPrefixTokens` bounds the request, `minimumMatchedTokens` bounds the realized matched-tokens count per replica, `routingFloorScore` bounds the realized per-replica score, and `strategy` selects the admissible matching/fallback families.

## CacheTenant

`CacheTenant` is namespaced. It defines tenant identity and quota.

| Field | Type | Purpose |
|---|---|---|
| `spec.tenantID` | string | Required non-empty external tenant identifier used by gateway and engine traffic. The string `inferencecache.io/probe` is **reserved** for the server's functional self-test and is rejected by the validating admission webhook on CREATE (always) and on UPDATE that newly introduces the value (via `filterIntroducedErrors`). Unchanged legacy CRs predating this rule continue to admit on unrelated edits — the v1alpha1 tightening seam. |
| `spec.quota.maxIndexEntries` | integer | Maximum distinct index prefixes attributed to the tenant. Minimum `0`. Enforced at ingest (see [policy-propagation.md](policy-propagation.md)). |
| `spec.isolationMode` | enum | `Fairness` in the current phase. |
| `spec.crypto` | object | Reserved for future cryptographic isolation settings. |

`status.indexEntries`, `status.conditions`, and `status.observedGeneration` expose observed tenant state.

There is deliberately **no** `spec.quota.maxMemoryBytes` or `status.memoryUsed`. The cache plane only surfaces a `max*` quota for a resource it authoritatively owns — the index entry table. Engine KV memory is a shared, tenant-unaware pool (vLLM/LMCache key by block hash, not tenant), so the control plane can neither enforce a per-tenant byte budget nor honestly attribute bytes per tenant (`ReplicaStats.cache_memory_bytes` is the engine total, double-counted across tenants sharing an engine). Per-tenant byte isolation is an engine/runtime concern (separate engine Deployments + pod memory limits).

## PromptTemplate

`PromptTemplate` is namespaced. It gives the rendering layer a template body plus cache-relevant slot declarations.

| Field | Type | Purpose |
|---|---|---|
| `spec.body` | string | Required non-empty template text. |
| `spec.slots[]` | list | Slot declarations keyed by `name`. |
| `spec.slots[].name` | string | Required non-empty slot identifier used by the template body. |
| `spec.slots[].type` | enum | Required `Stable` or `Mutable`. Stable slots participate in prefix stability; mutable slots do not. |
| `spec.slots[].required` | boolean | Whether callers must provide the slot. |
| `spec.slots[].description` | string | Human-readable slot notes. |

`status.templateRevision` is a stable revision identifier for cache invalidation.

## PDTopology

`PDTopology` is namespaced. It declares prefill/decode pools and accelerator classes for phase-disaggregated serving. The reconciler is future work; the type is shipped now so clients can reference the contract.

| Field | Type | Purpose |
|---|---|---|
| `spec.prefillPools[]` | list | Prefill pools keyed by `name`. |
| `spec.decodePools[]` | list | Decode pools keyed by `name`. |
| `spec.acceleratorTypes[]` | list | Accelerator classes keyed by `name`. |
| `*.name` | string | Required non-empty identifier for each pool or accelerator type. |
| `prefillPools[].matchLabels`, `decodePools[].matchLabels` | map | Selects pods or nodes for the pool. |
| `prefillPools[].replicas`, `decodePools[].replicas` | integer | Desired pool size. Minimum `0`. |
| `prefillPools[].acceleratorType`, `decodePools[].acceleratorType` | string | References `spec.acceleratorTypes[].name`. |
| `acceleratorTypes[].vendor`, `acceleratorTypes[].model` | string | Descriptive accelerator metadata. |
| `acceleratorTypes[].matchLabels` | map | Labels identifying matching nodes or pods. |

`status.conditions` and `status.observedGeneration` are reserved for future topology reconciliation.

## CacheIndex

`CacheIndex` is cluster-scoped and status-only. It reflects the server's in-memory cache aggregate for observability (`kubectl get cacheindex`); it is not a routing substrate.

- The CRD has no user-configurable spec. For v1alpha1 compatibility, it accepts an omitted spec or the legacy empty `spec: {}` shape, but it does not define writable spec fields.
- The controller owns the singleton object and writes only the status subresource.
- Status carries replica, tenant, and prefix summaries. It never stores KV tensors or prompt text.
- `status.replicas[]` is a map-list keyed on `id` (the v1alpha1 surface; unchanged for backward compatibility). Each row carries an optional `tenant` field for source disambiguation; the controller publishes only replicas that have reported stats, so the `id` key is unique in practice. If two stats-reporting replicas sharing a pod name across namespaces ever collide on `id` in a single tick, the controller picks the lexicographically-later `tenant` deterministically and the `tenant` field on the published row identifies which one was chosen.
- The status is populated by the controller scraping the server's internal `/snapshot` endpoint. `/snapshot.replicas[]` is keyed internally by `(tenant, replicaId)` (so prefix-only replicas with no stats remain attributable per-namespace) and carries per-replica `prefixCount` and `lastEventAt`, which the controller projects into [`CacheBackend.status.indexParticipation`](cachebackend-api.md#index-participation). Prefix-only replicas are deliberately omitted from `CacheIndex.status.replicas[]` — that surface is for replicas with reported stats only.
