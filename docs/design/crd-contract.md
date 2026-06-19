# Design: CRD contract

Status: implemented · API group: `inferencecache.io/v1alpha1`

This document is the contributor-facing rationale for the CRD surface as a whole — why the API is split the way it is, the conventions every type follows, and the invariants a reviewer should enforce on any new field. The per-field semantics live in [policy-crds.md](policy-crds.md) and the per-CRD concept docs (linked at the bottom); this doc covers the *shape* of the contract and the reasoning the concept docs omit. It is the CRD-side companion to [grpc-contract.md](grpc-contract.md).

## Why 6 CRDs (not 1, not 10)

The active types are `CacheBackend`, `CachePolicy`, `CacheTenant`, `CacheIndex`, `PromptTemplate`, and `PDTopology` (see `api/v1alpha1/*_types.go`). The split axis is **distinct scope + lifecycle + writer** — each CRD answers to a different actor and changes on a different clock:

| CRD | Scope | Owns | Primary writer |
|---|---|---|---|
| `CacheBackend` | namespaced | engine binding, per backend | operator (spec); reconciler + index poller (status) |
| `CachePolicy` | namespaced | per-namespace lookup/eviction tuning | operator (spec) |
| `CacheTenant` | namespaced | tenant identity + index-entry quota | operator (spec); tenant status writer (status) |
| `CacheIndex` | **cluster** | read-only cache aggregate | controller snapshot poller (status only) |
| `PromptTemplate` | namespaced | render-layer template + slot declarations | operator (spec) |
| `PDTopology` | namespaced | prefill/decode topology (reconciler is future work) | operator (spec) |

**Not one god-CRD.** Folding these into a single object would conflate independent lifecycles (a tenant quota change and an engine rebind are unrelated edits), force one RBAC grant to cover binding *and* tenancy *and* read-only observability, and put multiple writers on one object's spec. The split lets a namespace own its `CachePolicy` without touching cluster-scoped index state, and lets the controller own `CacheIndex.status` without any operator write path.

**Not ten CRDs.** The opposite failure is fragmenting one concern across many objects (a separate CR per engine override, per slot, per pool). Those belong as fields/lists *inside* the owning CRD — `EngineOverrides` on `CacheBackend`, `slots[]` on `PromptTemplate`, `prefillPools[]`/`decodePools[]` on `PDTopology` — because they share that object's lifecycle and writer. A CRD boundary is justified only by a genuinely distinct scope/lifecycle/writer triple.

## Naming and API-group conventions

| Convention | Value |
|---|---|
| API group / CRD group | `inferencecache.io` |
| version | `v1alpha1` |
| short names | `cb`, `cpol`, `ct`, `ci`, `pt`, `pdt` |

The group is vendor-neutral identity (see `CONTRIBUTING.md`) — no cloud-vendor token appears in any group, kind, or short name. Five of the six CRDs are **namespaced**; `CacheIndex` is the only **cluster-scoped** type and is a controller-maintained **singleton** (`cluster-default`), because the cache aggregate it reflects is cluster-wide and there is nothing namespace-local to scope it to.

## Status-surface invariants

Every **spec-reconciled** CRD that carries a status follows the same three rules:

- **Conditions are the authoritative health surface.** Status health is expressed as a `[]metav1.Condition` array (`status.conditions`, `+listType=map` keyed on `type`) plus `status.observedGeneration`. Conditions are the idiomatic Kubernetes signal — `kubectl wait --for=condition=Ready`, dashboards, and GitOps tooling all understand them — so they are the *only* authoritative health signal the contract exposes.
- **No single-enum `Health` field.** A former status enum was removed (the Health-field removal) precisely because a one-word enum that no dashboard or controller reads is worse than Conditions: it looks authoritative, drifts silently, and duplicates state Conditions already carry. New status surfaces MUST use Conditions, not a bespoke health enum.
- **Status is write-only-on-change.** Every writer gates on an equality check and skips the write when nothing changed, keeping `resourceVersion` stable and watch traffic quiet. The write *mechanism* tracks contention. `CacheBackend.status` is the one genuinely **multi-writer** status (its reconciler and the snapshot poller both write it), so each writer uses field-scoped `Status().Patch` to touch only its own fields and never clobber the other. `CacheTenant.status` is **single-writer** (the snapshot poller) but uses the same field-scoped `Patch` for uniform discipline. The single-writer `CacheIndex` singleton uses a full-object `Status().Update`. The shared rule is "don't churn"; field-scoped `Patch` is how the contended status avoids clobbering, not a universal requirement.

**Exception — `CacheIndex`.** Its status is a pure server-reflection, not the outcome of reconciling a user-authored spec, so it carries **neither `conditions` nor `observedGeneration`**: there is no spec generation to observe and no health to assert beyond freshness, which `status.lastUpdated` (also write-only-on-change) conveys directly. The Conditions/`observedGeneration` rule above applies to the spec-reconciled CRDs; `CacheIndex` is the deliberate carve-out.

## Reconciler patterns — who writes what

`CacheBackend.status` is written by more than one component (its reconciler plus the snapshot poller). Coexistence works because each writer uses field-scoped `Status().Patch` (write-only-on-change), so it touches only its own fields and never clobbers a co-writer's. The other statuses are single-writer — `CacheTenant.status` still uses `Patch` for uniformity, and the `CacheIndex` singleton uses a full-object `Status().Update`.

| Status | Field(s) | Writer |
|---|---|---|
| `CacheBackend.status` | `matchedEnginePods`, `engineSelectorMessage`, `conditions`, `firstKVEventObservedAt`, `firstAvailableAt`, `observedServerInstance`, `endpoint`, `failOpen`, `capacity`, `observedGeneration` | CacheBackend reconciler |
| `CacheBackend.status` | `indexParticipation` | CacheIndex snapshot poller |
| `CachePolicy.status` | `conditions`, `observedGeneration` (reserved) | — (see note) |
| `CacheTenant.status` | `indexEntries`, `conditions`, `observedGeneration` | CacheIndex snapshot poller (per-tenant projection of `/snapshot`) |
| `CacheIndex.status` | `replicas[]`, `tenants[]`, `prefixes.summary`, `observedServer`, `lastUpdated` | CacheIndex snapshot poller (singleton) |

**`CachePolicy.status` is reserved.** The policy reconciler PUSHES the flattened policy set to the server's `/policy` endpoint (see [policy-propagation.md](policy-propagation.md)); it deliberately does **not** write `CachePolicy.status`. The status fields exist for a future propagation-health writer, but no component writes them today — so `kubectl get cachepolicy` shows configuration, not propagation state.

## The controller ↔ server bridge

The controller and server binaries exchange state over **two HTTP endpoints in opposite directions**, both on the server's internal `:8081` listener, both authenticated as the same controller ServiceAccount identity, both carrying soft (recoverable) state:

| Direction | Endpoint | Owner of the data | Semantics |
|---|---|---|---|
| PULL (controller ← server) | `GET /snapshot` | server (in-memory cache aggregate) | one poller projects the scrape onto three surfaces: `CacheIndex.status`, `CacheBackend.status.indexParticipation`, and `CacheTenant.status` |
| PUSH (controller → server) | `POST /policy` | controller (`CachePolicy`/`CacheTenant` intent) | full snapshot, replace-on-write into server runtime |

The bidirectionality is the point: pulled state reflects what the server has heard from the substrate, pushed state is the operator's declarative intent flowing the other way. Operators reason at the `CacheBackend.status` + `CachePolicy.spec` layer; the bridge is the plumbing that connects the two. Any future "controller orchestrates server runtime state" surface should reuse this shape. The wire schema, versioning, and auth profile (TokenReview-backed bearer + audience binding + a `NetworkPolicy` gate) are specified in [policy-propagation.md](policy-propagation.md).

## The enforcement-boundary principle

**We describe cache state; we do not control engine memory.** A `max*` or quota field is added to a CRD *only* when the cache plane authoritatively owns the resource being limited:

- The index entry table **is ours** — it is the control plane's own data structure — so `CacheTenant.spec.quota.maxIndexEntries` is genuinely enforceable (over-budget evicts the tenant's oldest prefixes under `Fairness`).
- Engine KV memory **is not ours** — vLLM's KV cache and lmcache-server storage are a single shared pool keyed by block hash, with no tenant awareness. A per-tenant byte budget on a shared engine is therefore neither enforceable (we have no server→engine eviction channel) nor honestly observable (`ReplicaStats.cache_memory_bytes` is the engine total, double-counted across tenants sharing the engine).

So `CacheTenant` has `spec.quota.maxIndexEntries` and `status.indexEntries`, but deliberately **no** `maxMemoryBytes` and **no** `status.memoryUsed`. The honest path to per-tenant byte isolation is operational, not a CRD field: run separate engine Deployments per tenant and let `Pod.spec.containers[].resources.limits.memory` enforce at the cgroup layer. See [../concepts/cachetenant-identity-and-quota.md](../concepts/cachetenant-identity-and-quota.md). The generalizable test for any new limit field: *do we authoritatively control the resource?* If not, omit the field or surface the data as a metric, not a quota.

## Forward invariant for new fields

Every new `v1alpha1` spec/status field ships in exactly **one of three states**:

1. **Wired at merge.** A runtime consumer in `pkg/server/`, `pkg/index/`, `pkg/adapters/`, or `internal/controller/` changes observable behavior on the field's value — verified by grep in the same PR that adds the field.
2. **Intentionally declarative.** The field scaffolds a not-yet-built controller; the type godoc must say so explicitly *and* name the tracking work. This requirement binds **new** fields at merge. Three fields predate the invariant and belong in this state but do **not** yet fully satisfy the godoc bar — `CacheTenantCryptoSpec` (godoc'd only "reserved for future cryptographic isolation"), `PromptTemplate.slots`, and `PDTopology`'s prefill/decode pools — each is scaffolded ahead of its render / disaggregation controller but names no tracking effort in godoc. That is a known pre-existing gap, tracked as a follow-up; closing it (making each godoc name the work) brings them into compliance. The doc states the bar; these three are the outstanding exceptions, not compliant examples.
3. **Tracked for wiring.** A follow-up effort exists, the field's godoc names it, and the field comment says "inert until \<that work\>" (e.g. `CacheBackend.spec.storage.pvc.*` and `status.capacity`, which name the storage wire-up).

**Inert with no tracking and no godoc carve-out is not permitted at `v1alpha1`.** A field that nothing reads and that no one is on the hook to wire is exactly the failure mode the inert-field audit closed; reviewers (and the automated review pass) hard-block any new field that fails the three-state test. This invariant **relaxes at the `v1beta1` promotion**, when adding a field becomes the more expensive operation and the review bar shifts from "is this field wired?" to "is this the right field?".

## See also — per-CRD concept docs

- [../concepts/cachebackend-engine-binding.md](../concepts/cachebackend-engine-binding.md) — CacheBackend ↔ engine-pod selector binding
- [../concepts/cachebackend-engine-overrides.md](../concepts/cachebackend-engine-overrides.md) — `spec.integration.engineOverrides`
- [../concepts/cachepolicy-tuning.md](../concepts/cachepolicy-tuning.md) — per-namespace lookup/eviction tuning
- [../concepts/cachetenant-identity-and-quota.md](../concepts/cachetenant-identity-and-quota.md) — tenant identity + index-entry quota
- [../concepts/cacheindex-cluster-aggregate.md](../concepts/cacheindex-cluster-aggregate.md) — the cluster-scoped read-only aggregate
- [../concepts/README.md](../concepts/README.md) — concept-doc index
