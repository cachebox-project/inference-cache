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

{{% alert title="v1alpha1 stability" color="info" %}}
`v1alpha1` is not compatibility-frozen before the first production deployment. The CRD may
make breaking schema corrections while the API shape is being finalized. The gRPC/proto
contract follows its own compatibility policy for external consumers.
{{% /alert %}}

## CacheBackend

**Key `spec` fields:**

| Field | Type / values | Default | Notes |
|---|---|---|---|
| `runtime` | `VLLM`, `SGLang` | — | Inference runtime identity. |
| `type` | `LMCache`, `SGLangHiCache` | `LMCache` | Engine-side cache implementation. |
| `lmCache` | object | — | Engine-side LMCache configuration. |
| `remoteStorage` | object | — | Optional provider (`Redis`, `LMCacheServer`, `Mooncake`), ownership (`Managed`, `External`), and external endpoint. |
| `observation` | object | — | Model identity and first-event timeout. |
| `deploymentKind` | `Deployment`, `StatefulSet` | `Deployment` | `StatefulSet` reserved/no-op. |
| `replicas` | int32 | `1` | Min 0. |
| `autoscaling` | object | — | `minReplicas`, `maxReplicas` (required), `targetCPUUtilizationPercent` (default 80). |
| `integration.mode` | `Offload`, `EventsOnly` | `Offload` | Events-only = routing only. |
| `integration.role` | `ReadOnly`, `WriteOnly`, `ReadWrite` | `ReadWrite` | Maps to LMCache `kv_role`. |
| `integration.failOpen` | bool | `true` | `false` fails closed. |
| `integration.engineOverrides` | object | — | `args` / `suppressArgs` / `env` / `suppressEnv`. |
| `integration.engineHostNetwork` | bool | `false` | Opt-in for Mooncake engine pods. |
| `engineSelector.matchLabels` | map | — | Equality selector over engine pod labels. |
| `template` | object | — | Narrow pod-level overrides (no containers). |
| `remoteStorage.<provider>.resources` | ResourceRequirements | renderer default: `requests.memory 4Gi` / `limits.memory 8Gi` | Resources for the selected managed provider container. |
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
