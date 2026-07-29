---
title: "CacheBackend"
linkTitle: "CacheBackend"
weight: 2
description: >
  The primary resource: bind engine pods to a KV-cache backend and make their KV cache
  reusable across requests.
---

## What is a CacheBackend?

A `CacheBackend` is the primary CRD an operator writes. It describes a shared KV-cache
backend and the engine-integration policy that uses it. Applying one:

1. **Provisions** a managed cache-server workload (for backend types that need one) and a
   `ClusterIP` Service.
2. **Binds** to inference-engine pods by label (`spec.engineSelector`). The mutating Pod
   webhook injects the KV-connector configuration and an observation sidecar into matching
   pods.
3. **Makes the engine's KV cache reusable** — offloaded to the backend (tier 2) and
   surfaced to routing (tier 1) so a warm prefix skips prefill.

`CacheBackend` is namespaced. Group `inferencecache.io`, version `v1alpha1`, short name
`cb`.

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: llama3-cache
  namespace: serving
spec:
  type: LMCache
  integration:
    engine: vllm          # runtime ID, not the adapter name
    mode: Offload
    role: ReadWrite
  engineSelector:
    matchLabels:
      app: llama3-vllm
  backendConfig:
    model: meta-llama/Llama-3.1-8B-Instruct
  resources:
    requests:
      memory: 4Gi
    limits:
      memory: 8Gi
```

## Backend types (`spec.type`)

`spec.type` selects the backing implementation. The default is `LMCache`.

| Type | What it is |
|---|---|
| **`LMCache`** (default) | An in-memory `lm://` LMCache server, provisioned by the controller as a Deployment + ClusterIP Service. The simple, node-agnostic default. Not durable. |
| **`Mooncake`** | A durable, shared, peer-to-peer transfer-engine store. Requires host networking (see below). |
| **`External`** | You provide `spec.endpoint`; the controller skips all provisioning and only wires the engine side. |
| `SGLangHiCache`, `AIBrix`, `NIXL` | Reserved for future adapters. |

The runtime + backend **pair** selects the adapter — `(vllm, LMCache)`, `(vllm, Mooncake)`,
`(vllm, External)`, `(sglang, LMCache)`. Admission rejects unsupported pairs.

{{% alert title="LMCache durability" color="info" %}}
The `lm://` LMCache server is **in-memory only.** Durability is a *backend choice*, not a
generic volume knob — there is no per-`CacheBackend` PVC field. If you need a durable or
shared store, use `type: Mooncake`.
{{% /alert %}}

### Mooncake needs host networking

Mooncake is a peer-to-peer transfer-engine mesh: its master returns a *directory pointer*
("block X is on node B, port P"), and the engine then connects directly to that node's real
IP on a dynamically chosen port to move the KV bytes. A single ClusterIP cannot route that.

Consequently, a Mooncake backend renders the master with `hostNetwork: true` behind a
headless Service, and the **engine pods also need host networking** — opt in with
`spec.integration.engineHostNetwork: true`. The namespace must permit `hostNetwork`, the
backend becomes a singleton (`replicas > 1` and autoscaling are rejected), and one engine
per node per port applies. TCP transport is sufficient for correctness; RDMA/RoCE only
affects bandwidth.

## Engine integration (`spec.integration`)

| Field | Values | Meaning |
|---|---|---|
| `engine` | `vllm` (default), `sglang` | The **runtime ID** — not the adapter package name. Writing the adapter name (e.g. `vllm-lmcache`) is rejected. |
| `mode` | `Offload` (default), `EventsOnly` | `Offload` = routing + tier-2 offload + a provisioned server. `EventsOnly` = routing only, no server, no KV connector. |
| `role` | `ReadOnly`, `WriteOnly`, `ReadWrite` (default) | Maps to the LMCache `kv_role` (`kv_consumer` / `kv_producer` / `kv_both`). |
| `failOpen` | `true` (default) | The engine falls back to local prefill when the cache is unreachable. `false` fails closed (and emits a Warning Event). |
| `firstEventTimeout` | `5m` (default) | How long readiness waits for the first KV event before reporting degraded. |
| `engineOverrides` | — | Fine-grained control over injected args/env (see below). |
| `engineHostNetwork` | `false` (default) | Opt-in host networking for Mooncake engine pods. |

### Events-only mode

`mode: EventsOnly` provisions **no** cache server — routing tier only. It exists for
**hybrid-attention models** (for example gated-DeltaNet, Mamba/Jamba, Falcon-H,
Granite-hybrid families) that cannot take a vLLM KV connector, because vLLM disables its
hybrid KV-cache manager when any connector loads. The subscriber sidecar is still injected
so routing hints stay live; evictions are forwarded as `PREFIX_EVICTED`. Events-only
requires `type: LMCache`, forbids `autoscaling`, and is incompatible with `External`.

### Engine-injection overrides

`spec.integration.engineOverrides` exposes four primitives that merge on top of the
adapter's canonical injection:

- `args` — extra engine args to add.
- `suppressArgs` — canonical args to remove.
- `env` — extra environment variables to add.
- `suppressEnv` — canonical env vars to remove.

Each adapter declares **reserved** args and env that carry correctness guarantees.
Overriding a reserved value is **hard-rejected at admission**, not merely warned — a warning
would be ignored and the engine would crash later with no breadcrumb. See
[Bind an engine]({{< relref "/docs/tasks/bind-an-engine/" >}}) for the reserved lists per runtime.

## Engine binding (`spec.engineSelector`)

`spec.engineSelector.matchLabels` is an equality selector over engine **pod** labels
(`matchExpressions` is not available in v1alpha1). Any pod whose labels match, in the
`CacheBackend`'s namespace, is a target for the mutating Pod webhook. `status.matchedEnginePods`
reports how many pods currently match.

A pod carrying the annotation `inferencecache.io/skip-inject: "true"` is skipped entirely —
the all-or-nothing escape hatch.

The **replica identity convention** ties it together: the subscriber sets
`replica_id = <pod-name>`; the pod name is prefixed by its Deployment name, which equals the
`CacheBackend` name. Consumers map a replica back to its backend by prefix match.

## Readiness

`CacheBackend.Ready` composes three gates, in order:

1. **Managed-readiness baseline** — the provisioned workload is available (or scaled to
   zero, or rolling).
2. **KV-event gate** — Ready stays `False` (`AwaitingFirstKVEvent`) until a *real* engine
   pod has published at least one KV event for this backend. This proves the publisher
   works, not just that the pod IP is reachable. After `firstEventTimeout` with no events it
   reports `NoKVEventsObserved` / `Degraded`. Opt out per-CR with
   `inferencecache.io/require-kv-events: "false"`.
3. **Functional-probe gate** — the controller drives a synthetic round-trip through the
   server's `/probe` endpoint (ingest → routing → tier-2). `FunctionalProbeOK` appears only
   after this clears. Opt out with `inferencecache.io/skip-functional-probe: "true"`.

`External` backends are exempt from the KV-event and probe gates — only endpoint acceptance
and Ready are checked.

## Status

Selected `status` fields:

| Field | Meaning |
|---|---|
| `endpoint` | The resolved backend endpoint. |
| `matchedEnginePods` | Pointer — `nil` (not yet observed) vs a real count. |
| `failOpen` | The effective fail-open posture. |
| `firstKVEventObservedAt` | Write-once latch — the first time any KV event was seen. |
| `indexParticipation` | `prefixCount`, `lastEventAt`, `hitRate`, `t2HitRate` — this backend's slice of the index. |
| `conditions` | The authoritative health surface (see below). |
| `observedGeneration` | Standard reconcile bookkeeping. |

Printer columns: `Type`, `Ready`, `Matched`, `Endpoint`, `Prefixes`, `LastEvent`, `Age`.

### Conditions

Conditions are the authoritative health surface (there is no single `Health` enum). An
Offload-managed backend can publish up to seven: `Ready`, `Degraded`, `Progressing`,
`FunctionalProbeOK`, `EngineKernelsHealthy`, `T2Degraded`, `EngineCompatibility`.
Events-only publishes three (`Ready`/`Degraded`/`Progressing`); `External` publishes
`Ready` + `Progressing`.

Two advisory conditions worth calling out:

- **`T2Degraded`** — sourced from `status.indexParticipation.t2HitRate`. `True/T2ZeroHitRate`
  means the tier-2 offload was queried but served zero reloads — a silently-degraded tier.
  It never gates `Ready`. (Because it is lifetime-cumulative, a *mid-life* regression is
  caught by the `LMCacheT2NoHits` alert instead.)
- **`EngineCompatibility`** — `False/InjectedEngineCrashLooping` flags an engine that is
  crash-looping after injection (often a hybrid-attention model that cannot take a
  connector). The remedy is to switch that backend to events-only mode, **not** to
  `skip-inject`.

## Related pages

- [Bind an engine]({{< relref "/docs/tasks/bind-an-engine/" >}}) — the injection contract, reserved
  args/env, and the skip annotation.
- [CachePolicy]({{< relref "/docs/concepts/cachepolicy/" >}}) — per-namespace lookup and eviction tuning.
- [CRD API reference]({{< relref "/docs/reference/crd-api/" >}}) — every field.
