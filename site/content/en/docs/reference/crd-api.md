---
title: "CRD API"
linkTitle: "CRD API"
weight: 1
description: >
  The six custom resources — groups, scopes, short names, and key fields.
---

All CRDs are in the API group **`inferencecache.io`**, version **`v1alpha1`**.

| Kind | Scope | Short name | Reconciled? | Purpose |
|---|---|---|---|---|
| `CacheBackend` | Namespaced | `cb` | Yes | Bind engine pods to a KV-cache backend; provision the managed cache server. |
| `CachePolicy` | Namespaced | `cpol` | Declarative (pushed to server) | Per-namespace lookup and eviction tuning. |
| `CacheTenant` | Namespaced | `ct` | Declarative (pushed to server) | Tenant identity + entry-count quota. |
| `CacheIndex` | **Cluster** | `ci` | Yes (status-only) | Cluster-wide mirror of the server aggregate. |
| `PromptTemplate` | Namespaced | `pt` | Declarative | Cache-aware prompt template + stable/mutable slots. |
| `PDTopology` | Namespaced | `pdt` | Declarative | Prefill/decode topology for disaggregated serving. |

{{% alert title="v1alpha1 compatibility" color="info" %}}
Although the API is still evolving, existing `v1alpha1` objects must remain valid. Schema
changes are additive or otherwise backward-compatible; removals and incompatible validation
changes require a new version and migration path. The gRPC/proto contract follows the same
backward-compatibility rule for its external consumers.
{{% /alert %}}

## CacheBackend

**Key `spec` fields:**

| Field | Type / values | Default | Notes |
|---|---|---|---|
| `type` | `LMCache`, `Mooncake`, `External`, … | `LMCache` | Backing implementation. |
| `deploymentKind` | `Deployment`, `StatefulSet` | `Deployment` | `StatefulSet` reserved/no-op. |
| `replicas` | int32 | `1` | Min 0. |
| `autoscaling` | object | — | `minReplicas`, `maxReplicas` (required), `targetCPUUtilizationPercent` (default 80). |
| `integration.engine` | `vllm`, `sglang` | `vllm` | Runtime ID, not the adapter name. |
| `integration.mode` | `Offload`, `EventsOnly` | `Offload` | Events-only = routing only. |
| `integration.role` | `ReadOnly`, `WriteOnly`, `ReadWrite` | `ReadWrite` | Maps to LMCache `kv_role`. |
| `integration.failOpen` | bool | `true` | `false` fails closed. |
| `integration.firstEventTimeout` | duration | `5m` | KV-event readiness window. |
| `integration.engineOverrides` | object | — | `args` / `suppressArgs` / `env` / `suppressEnv`. |
| `integration.engineHostNetwork` | bool | `false` | Opt-in for Mooncake engine pods. |
| `engineSelector.matchLabels` | map | — | Equality selector over engine pod labels. |
| `backendConfig` | map[string]string | — | e.g. `model`, plus backend tunables. |
| `template` | object | — | Narrow pod-level overrides (no containers). |
| `resources` | ResourceRequirements | `requests.memory 4Gi` / `limits.memory 8Gi` | For the managed cache-server container. |
| `endpoint` | string | — | Required for `External`, rejected otherwise. |
| `allowCrossNamespace` | bool | `false` | Opt-in cross-namespace endpoints. |

**Key `status` fields:** `endpoint`, `matchedEnginePods` (`*int32`),
`firstKVEventObservedAt` (`*Time`), `indexParticipation`
(`prefixCount`, `lastEventAt`, `hitRate *string`, `t2HitRate *string`), `failOpen`,
`observedGeneration`, `conditions`.

Full page: [CacheBackend]({{< relref "/docs/concepts/cachebackend/" >}}).

## CachePolicy

| Field | Type / values | Default |
|---|---|---|
| `eviction` | `LRU`, `LFU` | `LRU` |
| `evictionTTL` | duration | server 30m (must be > 0 when set) |
| `minimumPrefixTokens` | int32 | unset (no gate) |
| `minimumMatchedTokens` | int32 | `64` (`0` opts out) |
| `routingFloorScore` | string (float) | `"0.1"` (`"0"` opts out) |
| `lookupTimeoutMs` | int32 | unset (`0`/≤0 = unbounded) |
| `strategy.enableChainMatching` | bool | `true` |
| `strategy.requireChain` | bool | `false` |
| `strategy.enableTenantHot` | bool | `true` |
| `affinityRouting` | `Enabled`, `Disabled` | `Enabled` |

`status` is reserved (not written today). Full page:
[CachePolicy]({{< relref "/docs/concepts/cachepolicy/" >}}).

## CacheTenant

| Field | Type / values | Default |
|---|---|---|
| `tenantID` | string (required, min length 1) | — |
| `quota.maxIndexEntries` | int64 | unset (unbounded) |
| `isolationMode` | `Fairness` | `Fairness` |
| `crypto` | object | reserved (empty) |

`status`: `indexEntries` (`*int64`), `conditions`, `observedGeneration`. There is
deliberately **no** `maxMemoryBytes` / `memoryUsed`. Full page:
[CacheTenant]({{< relref "/docs/concepts/cachetenant/" >}}).

## CacheIndex

`spec` is empty. `status`: `replicas[]`
(`id`, `tenant`, `cacheMemoryBytes`, `hitRate *string`, `pressure`, `lastUpdate`),
`tenants[]` (`id`, `indexEntries *int64`, `hitRate *string`, `memoryUsed` — deprecated,
always 0), `prefixes.summary` (`total`, `hot`=0), `observedServer`, `lastUpdated`. Full page:
[CacheIndex]({{< relref "/docs/concepts/cacheindex/" >}}).

## PromptTemplate

`spec`: `body` (required), `slots[]` (`name`, `type` = `Stable`|`Mutable`, `required`,
`description`). `status`: `templateRevision`, `conditions`, `observedGeneration`. Full page:
[PromptTemplate]({{< relref "/docs/concepts/prompttemplate/" >}}).

## PDTopology

`spec`: `prefillPools[]`, `decodePools[]` (`name`, `matchLabels`, `replicas`,
`acceleratorType`), `acceleratorTypes[]` (`name`, `vendor`, `model`, `matchLabels`).
`status`: `conditions`, `observedGeneration`. Full page:
[PDTopology]({{< relref "/docs/concepts/pdtopology/" >}}).
