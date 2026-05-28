# Design: Policy CRDs

Status: implemented · API group: `inferencecache.io/v1alpha1`

This document tracks the policy-side CRDs that sit beside `CacheBackend`. These resources are the declarative policy and observability surface; routing and serving decisions remain in the gRPC server and runtime adapters.

## CachePolicy

`CachePolicy` is namespaced. It controls cache lookup and eviction behavior.

| Field | Type | Purpose |
|---|---|---|
| `spec.eviction` | enum | `LRU` or `LFU`. |
| `spec.ttl` | duration | Maximum usable lifetime for cache entries. |
| `spec.minimumPrefixTokens` | integer | Minimum prefix token count before lookup. Minimum `0`. |
| `spec.lookupTimeoutMs` | integer | Lookup latency budget in milliseconds. Minimum `0`. |
| `spec.failOpen` | boolean | Defaults to `true`; keeps inference serving when cache lookup is unavailable. |
| `spec.tenantScope` | object | Selects the tenants affected by the policy. Supports `type`, `tenantRef`, `tenantID`, and `matchLabels`. |

`status.conditions` and `status.observedGeneration` are reserved for controller observations.

## CacheTenant

`CacheTenant` is namespaced. It defines tenant identity and quota.

| Field | Type | Purpose |
|---|---|---|
| `spec.tenantID` | string | Required non-empty external tenant identifier used by gateway and engine traffic. |
| `spec.quota.maxMemoryBytes` | integer | Maximum cache memory attributed to the tenant. Minimum `0`. |
| `spec.quota.maxIndexEntries` | integer | Maximum index entries attributed to the tenant. Minimum `0`. |
| `spec.isolationMode` | enum | `Fairness` in the current phase. |
| `spec.crypto` | object | Reserved for future cryptographic isolation settings. |

`status.memoryUsed`, `status.indexEntries`, `status.conditions`, and `status.observedGeneration` expose observed tenant state.

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
