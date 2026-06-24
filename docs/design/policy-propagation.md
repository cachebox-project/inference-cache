# Design: CachePolicy propagation

Status: implemented · Owners: controller (push) + server (apply)

`CachePolicy` is a namespaced CRD reconciled by the controller, but its
enforcement (eviction TTL, eviction algorithm, request-side prefix-length
gate, result-side matched-tokens floor, result-side routing-floor score,
lookup deadline) lives in the policy server. This document describes how the controller PROPAGATES the
declarative CRs into the server's runtime so the configuration surface
actually changes server behavior.

## Direction

The controller PUSHES; the server APPLIES.

Mirror image of the `CacheIndex` status path: the server publishes
in-memory cache aggregate at `GET /snapshot` and the controller polls it,
because the server owns the data. Here, the controller owns the data (the
set of `CachePolicy` CRs), so it publishes and the server consumes.

| Channel | Direction | Endpoint | Trigger |
|---|---|---|---|
| `/snapshot` | controller ← server | `GET` | controller tick |
| `/policy`   | controller → server | `POST` (`PUT` also accepted) | watch event + tick |
| `/probe`    | controller → server | `POST` | CacheBackend reconcile, rate-limited to ~once per backend per 30s |

All three of `/snapshot`, `/policy`, and `/probe` sit on the server's
internal `:8081` listener with **three independent gates**, each meant to
catch a failure mode the others can't. `/healthz`, `/readyz`, and
`/metrics` stay on the open `:8080` listener — kubelet probes and
Prometheus scrapes can't present a SA bearer.

`/probe` is the functional self-test the CacheBackend reconciler
drives per managed CacheBackend at reconcile time (composed on top of
the KV-event readiness gate; rate-limited to ~once per backend per 30s,
see [docs/design/cachebackend-api.md#functional-probe-gate](cachebackend-api.md#functional-probe-gate)).
The server synthesizes a deterministic round-trip — in-process
index-ingest → routing → tier-2 — under a reserved tenant id
(`inferencecache.io/probe`) and replica id (`__probe-<backend>`),
returning per-stage outcomes; the controller translates those into the
`FunctionalProbeOK` condition (and downgrades `Ready` on a stage
failure). Stage A's wire field name is `ingest` —
matching the path the probe physically traverses: it writes through
in-process `index.Ingest` rather than the gRPC `ReportCacheState`
handler the real subscriber uses (the handler now drops messages with
`tenant_id = inferencecache.io/probe` by design). A Stage A pass
therefore proves the index ingest path is accepting writes; it does
not, on its own, prove the full subscriber wire is healthy end-to-end.
A Stage A fail still definitively means the index ingest path is broken.

`/probe` shares the controller-auth identity with `/snapshot` and
`/policy` because all three endpoints serve one caller identity (the
controller SA). `/snapshot` and `/probe` use the controller audience;
`/policy` uses a separate write-side policy audience. The probe entries
auto-clean on each Run via an
`ALL_CLEARED` event against the reserved replica, so the synthesized
state never leaks into a real LookupRoute.

#### `/probe` wire contract

Request — JSON body, `POST /probe`. All fields are case-sensitive.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `backend` | string | yes | Stable, globally-unique CacheBackend identifier. Canonical form is `<namespace>/<name>` (the CacheBackend reconciler always sends this), but the handler accepts any non-empty string. Interpolated into the reserved replica id (`__probe-<backend>`) and the probe hash, so two CacheBackends with the same `backend` value share a replica id and contention slot. |
| `model` | string | yes | Model identifier the probe synthesizes state under. Must match the model the controller is checking; the reserved tenant id is server-owned and cannot be passed in. |
| `hashScheme` | string | yes | Engine domain (`vllm` / `sglang` / etc). Pins the engine the probe's synthetic block lives under so a probe for vllm cannot collide with a probe for sglang on the same backend. |
| `backendType` | string | no | The CacheBackend's `spec.type`. `LMCache` (or empty, matching the CRD default) runs Stage C; `Memory` / `External` skip it. Unknown values fall through to skip. |

Response — JSON body, HTTP `200 OK` on every well-formed request (per-stage failures are surfaced INSIDE the body, not via HTTP status — the call itself succeeded):

```json
{
  "backend": "<backend echoed back>",
  "ingest":  "ok|failed|skipped",
  "routing": "ok|failed|skipped",
  "t2":      "ok|failed|skipped",
  "errors": [
    { "stage": "ingest|routing|t2", "message": "..." }
  ]
}
```

Stage values:
- `ok` — the stage passed.
- `failed` — the stage failed; `errors` carries a stage-keyed diagnostic message.
- `skipped` — the stage was not run. T2 is skipped on non-LMCache backends and when no T2Prober is wired. Downstream stages are skipped when an upstream stage failed (cascade prevention).

Stage names:
- `ingest` — Stage A. Verifies the in-process index ingest path accepts writes. NOTE: this stage writes via `index.Ingest` directly, NOT through the gRPC `ReportCacheState` handler the real subscriber uses (the handler drops messages with `tenant_id = inferencecache.io/probe` by design). A pass proves the index ingest path is healthy; a fail definitively means it's broken. Neither alone proves the wire subscriber path is healthy end-to-end.
- `routing` — Stage B. Verifies the in-process `index.LookupRoute` (the orchestrated ranking entrypoint that the gRPC handler delegates to) returns `PREFIX_MATCH` for the probe-synthesized hash against the just-ingested entry. NOTE: this stage calls `index.LookupRoute` directly, NOT the gRPC `inferenceCacheService.LookupRoute` handler. The handler short-circuits `tenant_id = inferencecache.io/probe` to `NO_HINT` by design (defense against external lookups against the reserved scope), so the probe cannot route through it. Handler-level concerns — policy gating (`minimumPrefixTokens`), `lookupTimeoutMs` deadline, proto→domain translation — are not covered by this stage and have their own unit tests under `pkg/server`.
- `t2` — Stage C. Verifies a tier-2 put/get round trip via the supplied `T2Prober` (LMCache backends; skipped otherwise).

Status codes:
- `200` — body carries the per-stage result; `errors` is empty when all stages passed.
- `400` — body decode failure (invalid JSON, trailing content past the first value, missing required field, unknown field).
- `401` / `403` — auth-middleware rejection (TokenReview failed or wrong SA / wrong audience).
- `405` — wrong method; only `POST` is allowed (the handler echoes `Allow: POST`).

Isolation + cleanup guarantees:
- The synthesized state lives under tenant id `inferencecache.io/probe` and replica id `__probe-<backend>`. Both prefixes are reserved by the project's canonical namespace and the `__` Pod-name escape; a real workload cannot collide.
- The reserved tenant is excluded from the index's global `maxEntries` cap accounting AND its cap-sweep victim candidate set, so a concurrent real-workload `Ingest` cannot evict a real entry to make room for a transient probe entry.
- Each `Run` calls `ApplyEvent(EventAllCleared)` against the reserved replica via `defer`, leaving the index empty of probe entries on return (even on panic or early-return from a failed Stage A).
- Reserved-tenant entries are still subject to the TTL sweep as defense-in-depth if the deferred cleanup somehow fails to run.
- The `CacheTenant` admission webhook rejects CRs that newly claim `spec.tenantID = inferencecache.io/probe` — both `ValidateCreate` (unconditionally) and `ValidateUpdate` (only when the change newly introduces the id, via `filterIntroducedErrors`). Pre-existing CRs already holding the reserved id are not trapped on unrelated edits (the v1alpha1 tightening seam). The `ReportCacheState` / `PublishEvent` gRPC handlers silently drop messages carrying the same id — so an external client cannot fake state into the reserved scope through the public gRPC contract.

1. **L3/L4** — a `NetworkPolicy` restricts ingress to pods matching the
   controller's selector.
2. **L7 identity** — TokenReview-backed bearer middleware rejects every
   request whose token does not resolve to the configured controller
   `ServiceAccount` (`--allowed-controller-sa`).
3. **L7 audience** — the controller mounts two audience-bound projected
   SA tokens: `inferencecache.io/controller` at
   `/var/run/secrets/inferencecache.io/controller-token/token` for
   `/snapshot` + `/probe`, and `inferencecache.io/policy` at
   `/var/run/secrets/inferencecache.io/policy-token/token` for `/policy`.
   The server passes `TokenReviewSpec.Audiences=[--controller-audience]`
   for `/snapshot` + `/probe`, and `[--policy-audience]` for `/policy`, so
   a leaked default-audience apiserver token from the same SA is rejected,
   a leaked controller-audience token cannot push policy, and a leaked
   audience-bound bridge token is useless against the apiserver **under the
   default apiserver audience configuration**. If the cluster has been
   explicitly configured to also accept either inference-cache audience as an
   apiserver audience the cross-surface defense degrades; keep these
   audiences distinct from any audience the apiserver accepts.

All three internal endpoints share one ServiceAccount identity profile.
`/snapshot` is the *read* side
(CacheIndex poll, info leak if exposed), `/policy` is the *write* side
(CachePolicy push, active tampering if exposed), and `/probe` is the
controller-driven *functional-self-test* side (per-CacheBackend
synthesis; silent Ready-gate degradation if a regression skipped it).
Write is the most dangerous of the three because replace-on-write
semantics mean a rogue POST to `/policy` overrides every namespace's
policy state cluster-wide with no audit trail. The read-side hardening
landed first; the write side joined it on the same gate; `/probe`
joined the same shared gate (audience-bound + bearer-validated +
NetworkPolicy-restricted) with the audience layer hardening all three
endpoints uniformly.

`inferencecache_snapshot_auth_total`, `inferencecache_policy_auth_total`,
and `inferencecache_probe_auth_total` are the per-endpoint observability
surfaces for the two L7 layers (identity + audience); audience-mismatch
denials surface in the `unauth` bucket of each counter, with the
apiserver's diagnostic visible at WARN in the server log (e.g.
`token audiences [...] is invalid for the target audiences [...]`).
NetworkPolicy drops happen at the CNI before the listener and are
observed via kube state metrics + CNI flow logs, not these counters.

## Snapshot semantics

The controller always sends a FULL snapshot: one resolved policy per namespace
(see "Multiple CachePolicies in one namespace" below) PLUS one resolved tenant
entry per quota-bearing `CacheTenant` (keyed by `tenantID`). The
server adopts **replace-on-write**: the snapshot becomes the new state,
and any namespace not present reverts to server defaults. A CR delete
therefore propagates as "next snapshot omits this namespace."

The server's policy store is purely in-memory (soft state, like the cache
index). If the server restarts and loses everything, the controller's
periodic re-push (default 30s) brings it back into sync without operator
intervention.

## Wire schema (v6)

```json
{
  "version": 6,
  "policies": [
    {
      "namespace": "team-a",
      "evictionTTL": 900000000000,
      "minimumPrefixTokens": 32,
      "minimumMatchedTokens": 128,
      "routingFloorScore": 5,
      "lookupTimeoutMs": 25,
      "eviction": "lfu",
      "strategy": {
        "enableChainMatching": true,
        "requireChain": false,
        "enableTenantHot": true
      }
    },
    {
      "namespace": "team-b",
      "evictionTTL": 3600000000000,
      "minimumMatchedTokens": 64,
      "routingFloorScore": 0.1
    }
  ],
  "tenants": [
    {
      "tenantID": "team-a",
      "maxIndexEntries": 100000,
      "isolationMode": "Fairness"
    }
  ]
}
```

- `version` — schema version. Bumped on every schema change so version
  skew is observable; **whether the bump is rejected at decode is set
  separately** by `PolicyMinimumAcceptedVersion` (today `3`) — see the
  Rollout asymmetry note in §Versioning and forward-compat below. v4 and
  v5 and v6 are additive and defaultable, so the v6 server still accepts
  v3, v4, and v5 bodies; a hypothetical breaking change would bump
  `PolicyMinimumAcceptedVersion` in lockstep. The server rejects any
  value outside `[PolicyMinimumAcceptedVersion, PolicyPropagationVersion]`
  (HTTP 400). Currently `6` (bumped from `1` to `2` when `tenants` was
  added, to `3` when `policies[].eviction` was added, to `4` when
  `policies[].minimumMatchedTokens` was added, and to `5` when
  `policies[].routingFloorScore` was added, and to `6` when
  `policies[].strategy` was added).
- `policies[]` — full snapshot of all `CachePolicy` CRs in the cluster.
  Sorted by `namespace` for deterministic bodies (and for easier diffing
  in tests).
- `policies[].namespace` — the CR's namespace, used by the server as the
  tenant key (see *Tenant mapping* below).
- `policies[].evictionTTL` — Go `time.Duration` (nanoseconds, JSON
  number). Optional. `<=0` ⇒ "use server default" (`DefaultTTL = 30m`).
  Note: the CachePolicy validating webhook now rejects a non-positive
  `spec.evictionTTL` *at admission* when the field is set, so a conformant CR
  never emits `<=0` here. The wire-level `<=0` fallback is retained as a
  defensive default for an unset field and for any legacy/raced data that
  predates the webhook.
- `policies[].minimumPrefixTokens` — int32. Optional. `<=0` ⇒ "no
  threshold". A request-side gate against `LookupRouteRequest`'s claimed
  prefix length BEFORE the index is touched.
- `policies[].minimumMatchedTokens` — int32. Optional. `<=0` ⇒ "no floor for
  this namespace" (the explicit opt-out). A namespace that does NOT have a
  `CachePolicy` at all instead falls back to `DefaultMinimumMatchedTokens`
  (= 64) — the server-wide safety floor. Distinct from
  `minimumPrefixTokens`: this is a result-side filter applied AFTER the
  index returns, against each replica's realized matched-token overlap.
  Replicas whose match falls below the floor are filtered from the
  response; when none survive, the reason code downgrades to `NO_HINT`.
  See [`lookuproute-ranking.md`](./lookuproute-ranking.md).
- `policies[].routingFloorScore` — float32 pointer (omitempty). Optional.
  A nil/missing field is normalized to `DefaultRoutingFloorScore` (`0.1`)
  for v3 and v4 bodies (see Rollout asymmetry below). v5 bodies are NOT
  normalized; the v5 wire field can take three shapes:
  - **Normal CRD-defaulted shape (the common case).** The CRD has a
    `+kubebuilder:default="0.1"` marker, so an admitted CachePolicy
    always carries a non-nil `spec.routingFloorScore`. The controller
    flattens that to a non-nil `*float32` (e.g. `&0.1`) and sends it on
    the wire. v5 bodies in production traffic look like this.
  - **Explicit opt-out.** An operator setting
    `spec.routingFloorScore: "0"` reaches the wire as `&0` (omitempty
    on float32 *pointer* preserves `&0`; the zero-value case it
    suppresses is the nil pointer). The store records `0` byte-for-byte
    and the resolver returns `0`, disabling the floor.
  - **Bare/manual body.** A hand-crafted /policy POST, a legacy CR that
    predates the field, or any path that omits the field altogether
    reaches the store as nil. The resolver `PolicyStore.RoutingFloorScore`
    treats nil as the safety-default case and returns
    `DefaultRoutingFloorScore`. This is the same effective behavior as
    no CachePolicy at all, just routed through the
    "CachePolicy-installed-but-field-absent" branch.

  A namespace that does NOT have a `CachePolicy` at all falls back to
  `DefaultRoutingFloorScore`.
  Distinct from `minimumMatchedTokens`: that's a result-side filter on
  realized *matched_tokens*; this is a result-side floor on the realized
  per-replica *score* (matched_tokens × freshness × pressure_factor ×
  slo_bias × distinguishing_power) AFTER the distinguishing-power-aware
  ranker runs. When the top score falls below the floor, the whole
  response downgrades to `NO_HINT`. The two floors compose: matched-tokens
  filter runs first (per-replica), then the routing-floor-score check
  runs on the top survivor's score. See
  [`lookuproute-ranking.md`](./lookuproute-ranking.md).
- `policies[].lookupTimeoutMs` — int32 milliseconds. Optional. `<=0` ⇒
  "no deadline".
- `policies[].eviction` — lower-cased cap-eviction algorithm (`"lru"` /
  `"lfu"`). Optional. `""` ⇒ "use server default" (`LRU`). The controller
  lower-cases the CRD's upper-case enum; the index normalizes any
  unrecognized value back to `LRU`.
- `policies[].strategy` — object pointer (omitempty). Optional. Missing
  strategy or missing nested booleans resolve to the defaults:
  `enableChainMatching=true`, `requireChain=false`, `enableTenantHot=true`.
  `enableChainMatching=false` strips block-hash chain fields before lookup and
  uses the legacy exact `prefix_hash` path; `requireChain=true` returns
  `POLICY_REQUIRES_CHAIN` before the index lookup when a request has no valid
  chain; `enableTenantHot=false` downgrades tenant-hot fallbacks to `NO_HINT`.
  The CachePolicy validating webhook and CRD CEL reject
  `enableChainMatching=false` with `requireChain=true`.
- `tenants[]` — full snapshot of the `CacheTenant` CRs that carry an
  enforceable quota, keyed by `tenantID` (a different axis from the
  namespace-keyed `policies[]`). A `CacheTenant` without
  `quota.maxIndexEntries` is omitted (fail-open / unbounded). Optional and
  may be absent entirely.
- `tenants[].tenantID` — the CR's `spec.tenantID` (the value an ingest
  carries in `CacheStateUpdate.tenant_id`), **not** the CR name.
- `tenants[].maxIndexEntries` — int64 distinct-prefix budget. `0` is a
  valid enforceable cap (admit nothing), distinct from "no quota".
- `tenants[].isolationMode` — carried for forward-compat; only `Fairness`
  is implemented.

**Duplicate `tenantID` tie-break.** Two `CacheTenant` CRs may declare the same
`spec.tenantID`. A validating webhook hard-rejects this **within a namespace** —
at CREATE, and at UPDATE when a tenant's `tenantID` is changed onto a
same-namespace sibling's value (an unambiguous operator mistake) — but
`tenantID` identity is
namespace-blind — the index keys tenants by the bare `tenantID` string — so the
webhook intentionally **permits** the same `tenantID` across DIFFERENT
namespaces (it can be deliberate, e.g. a migration). Those cross-namespace
duplicates still reach the reconciler, which deduplicates deterministically:
among the quota-bearing CRs for a tenant ID, the first by `(namespace, name)`
ascending wins and is the single `tenants[]` entry emitted; the rest are dropped
from the snapshot. The CacheIndex status writer resolves the same winner, so a
shadowed duplicate's `status` reports `Ready=False` / `DuplicateTenantID` (it is
not the effective owner) rather than advertising a budget that isn't enforced.
The within-namespace admission check is best-effort (it can be raced by
concurrent CREATEs); the deterministic tie-break remains the authoritative
resolution that makes any surviving conflict deterministic and visible.

The server's `policyHandler` decodes with `DisallowUnknownFields` so an
unknown field surfaces as HTTP 400 rather than silently dropping. Request
body is capped at 1 MiB.

Successful PUSH returns `HTTP 204 No Content` with an empty body.

## Multiple CachePolicies in one namespace

A validating admission webhook rejects a **second** `CachePolicy` in a
namespace at CREATE (UPDATE on the single CR is unaffected), so the common
case never reaches the controller with more than one CR. That check is
**best-effort**, not a hard guarantee: it lists-then-admits, so two concurrent
CREATEs can both observe an empty namespace before either persists, and CRs
created before the webhook shipped may already coexist.

Because of that, the controller still resolves multiple CRs deterministically
as the **authoritative** backstop: when it observes more than one CachePolicy
in a single namespace it sorts by `(namespace, name)` ascending and the FIRST
entry per namespace wins (i.e. the lexicographically smallest `metadata.name`).
The losing policies do not appear in the wire snapshot.

This split:

- Gives operators immediate `kubectl apply` feedback on the ordinary mistake
  (admission), instead of a silently-dropped policy.
- Keeps the effective policy independent of apiserver list ordering even when
  the best-effort admission check is bypassed (controller dedup).
- Stays observable from `kubectl get cachepolicies`, so an operator can always
  predict which CR is in effect.

## Tenant mapping (phase-1)

A `CachePolicy` lives in a namespace; a `LookupRoute` carries a
`tenant_id`. Phase-1 treats them as equivalent: a policy in namespace
`team-a` applies to lookups with `tenant_id = "team-a"`.

`CacheTenant` introduces explicit tenant identifiers (`spec.tenantID`)
separate from Kubernetes namespaces. Tenant **quotas** are propagated on
the same `/policy` snapshot but keyed by `tenantID` (the value an ingest
carries in `CacheStateUpdate.tenant_id`), a different axis from the
namespace key `CachePolicy` uses — see the tenant-quota row below.

## Enforcement (what the server does with each field)

| Field | Where it lands |
|---|---|
| `evictionTTL` | `pkg/index` `TTLResolver` — per-tenant `freshness()` decay + `evictExpired()` cutoff. |
| `eviction` | `pkg/index` `EvictionResolver` — selects the per-namespace cap-based eviction algorithm. `lru` evicts oldest-by-`lastSeen`; `lfu` evicts the lowest per-entry access count, tie-broken on oldest `lastSeen`. The cap sweep (over `MaxEntries`) consults it to order victims. In `lfu` namespaces the lookup path also reads it to record which entries a *delivered* `LookupRoute` hint credits — the bump is lock-free and applied only when the response is actually returned (a `TIMEOUT`'d lookup credits nothing) and never changes a lookup result. The TTL sweep is algorithm-independent. Emitted as `inferencecache_index_evictions_total{algorithm,reason}`. |
| `minimumPrefixTokens` | Pre-lookup gate on `LookupRouteRequest`'s effective prefix token count: chain-bearing requests use `sum(block_token_counts)` and fall back to `prefix_token_count` only when the chain is empty (`effectivePrefixTokens` in the handler). A request shorter than the threshold short-circuits to `NO_HINT` without touching the index. Matches the CRD's "minimum prefix token count before lookup" semantics — gateway authors should size the gate against the chain budget they actually send. |
| `minimumMatchedTokens` | Post-lookup floor on each replica's realized `matched_tokens`. The handler resolves the per-tenant floor via `PolicyStore.MinimumMatchedTokens`, which falls back to `DefaultMinimumMatchedTokens` (= 64) for tenants with no `CachePolicy`. Replicas whose `matched_tokens` falls below the floor are filtered from the scored result; if none survive, the response downgrades to `reason_code: NO_HINT` with empty scores. The downgrade runs **before** the LFU `CreditHits` step so a non-delivered hint never bumps the per-entry access counter. See [`lookuproute-ranking.md`](./lookuproute-ranking.md). |
| `routingFloorScore` | Post-score floor on the per-replica score from the distinguishing-power-aware ranker. The handler resolves the per-tenant floor via `PolicyStore.RoutingFloorScore`, which falls back to `DefaultRoutingFloorScore` (`0.1`) for tenants with no `CachePolicy`. When the top surviving replica's score falls below the floor, the response downgrades to `reason_code: NO_HINT` with empty scores. Composes with `minimumMatchedTokens` — the matched-tokens floor runs first (per-replica filter), then this score floor checks the top survivor. Both downgrades run **before** the LFU `CreditHits` step so a non-delivered hint never bumps an LFU counter. See [`lookuproute-ranking.md`](./lookuproute-ranking.md). |
| `lookupTimeoutMs` | `LookupRoute` derives a `context.WithTimeout`. A breach yields `reason_code: TIMEOUT` (still fail-open: empty scores). |
| `strategy.enableChainMatching` / `strategy.requireChain` / `strategy.enableTenantHot` | Handler-side strategy gates. Chain matching disabled strips request block-hash fields before the index call; chain required rejects non-chain requests with `reason_code: POLICY_REQUIRES_CHAIN` before touching the index; tenant-hot disabled downgrades tenant-hot results to `NO_HINT`. The index remains policy-agnostic. |
| `CacheTenant.spec.quota.maxIndexEntries` | `pkg/index` `TenantQuotaResolver`. Pushed as a `ResolvedTenant{tenantID, maxIndexEntries, isolationMode}` slice alongside the policies. At ingest, if the tenant's distinct-prefix count exceeds the budget, the index evicts that tenant's oldest prefixes (Fairness) down to budget. Fail-open when no `CacheTenant` matches the ingest's `tenant_id`. |

The server is fail-open by construction on the hot path (no error
returned to the gateway); a `CachePolicy`-level fail-open knob is not
part of the propagated wire format.

The `/policy` snapshot carries both `[]ResolvedPolicy` (keyed by namespace)
and `[]ResolvedTenant` (keyed by `tenantID`). A single controller reconciler
watches **both** `CachePolicy` and `CacheTenant` and pushes one combined
snapshot — two reconcilers would race on the replace-on-write store. A
`CacheTenant` that disappears between snapshots reverts that tenant to
unbounded (no enforcement).

## Failure modes

- **Server unreachable.** Controller logs and returns a reconcile error;
  controller-runtime backs off. The periodic tick keeps retrying until
  the server is back.
- **Server returns non-2xx.** Same — reconcile error + retry.
- **CRD not installed.** `list CachePolicies` returns `IsNotFound`; the
  controller treats it as "nothing to push" rather than logging an error.
  This keeps the initial startup quiet in a half-installed cluster.
- **Server restart.** The server starts with an empty store (server
  defaults everywhere). The next periodic tick re-pushes the full
  snapshot; in steady state this is ≤ 30s.

## Versioning and forward-compat

The wire schema's `version` is integer-valued and explicit. New fields
that the server can ignore safely (additive, non-load-bearing) ship at
the same `version`; load-bearing or semantically breaking changes bump
`version` and gate decode on the new value. The controller pushes the
constant in `pkg/server.PolicyPropagationVersion` on every request.

`version` is `6`: it was bumped from `1` to `2` when the `tenants` slice
was added, to `3` when `policies[].eviction` (the per-namespace cap-eviction
algorithm) was added, to `4` when `policies[].minimumMatchedTokens`
(the result-side matched-tokens floor) was added, and to `5` when
`policies[].routingFloorScore` (the per-namespace post-score floor for
the distinguishing-power-aware ranker) was added, and to `6` when
`policies[].strategy` (per-namespace LookupRoute strategy gates) was added.
The server decodes with
`DisallowUnknownFields`, so an older server receiving a newer body still
fails loud on the unknown field even before its version check fires.

**Rollout asymmetry (additive-defaultable carve-out).** The version check is
deliberately asymmetric so a server-first rollout (newer server, older
controller still pushing the prior schema) does NOT drop existing policy
state mid-upgrade:

- A v6 server accepts any body whose `version` is in
  `[PolicyMinimumAcceptedVersion, PolicyPropagationVersion]` — today
  `[3, 6]`. Bodies outside the band are rejected with
  `unsupported policy snapshot version`.
- For accepted older bodies, each new field is *normalized* before reaching
  the store. v3 has neither `minimumMatchedTokens`, `routingFloorScore`, nor
  `strategy`; v4 has the first but not the latter two; v5 has both floors but
  not `strategy`. JSON decodes missing fields as their zero values, which
  would be indistinguishable from explicit opt-outs and silently change policy
  during the rollout. The server fills in `DefaultMinimumMatchedTokens` (`64`)
  for v3 bodies, `DefaultRoutingFloorScore` (`0.1`) for v3 and v4 bodies, and
  strategy defaults (`true` / `false` / `true`) for v3, v4, and v5 bodies. Every
  other knob (TTL, prefix gate, timeout, eviction, tenant quota) reaches the
  store byte-for-byte.
- v6 bodies are NOT normalized — an operator's explicit `routingFloorScore: 0`,
  `minimumMatchedTokens: 0`, or `strategy.enableTenantHot: false` reaches the
  store as written. Each normalization fires only when the version says the
  corresponding new field could not have been present.

The carve-out only applies to **additive, defaultable** schema changes: a
new field whose absence has a safe well-defined synthesized value. A
schema change that is load-bearing (removes a field, changes meaning, makes
an existing field required) MUST be paired with a `PolicyMinimumAcceptedVersion`
bump so old bodies under that change are rejected rather than silently
mis-interpreted.

## Out of scope

- General CRD **field-level** validation (structural / enum / range markers
  and the CacheBackend admission rules) — covered by the CRD admission webhook
  work, not here. The cross-CR admission rules that bear directly on
  propagation — one `CachePolicy` per namespace and same-namespace `tenantID`
  uniqueness — ARE documented above, since they shape what reaches the snapshot.
- Per-tenant **memory** budgets — out of scope by design (engine KV
  memory is tenant-unaware; see [policy-crds.md](policy-crds.md)). Only
  the index entry-count quota (`maxIndexEntries`) is enforced.
- `LookupRoute` ranking v2 (pressure / SLO scoring, `TENANT_HOT`
  fallback) — that strategy work consumes the same policy store but
  layers on top of the threshold/deadline enforcement shipped here.
- mTLS for `/snapshot`, `/policy`, and `/probe` — the current shape
  ships TokenReview-backed bearer auth + per-endpoint audience binding + a
  NetworkPolicy gate; mTLS is a separate hardening step tracked under the
  gRPC TLS posture decision, and applies uniformly across the
  controller-facing bridge.
