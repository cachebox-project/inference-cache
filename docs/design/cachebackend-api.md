# Design: CacheBackend API

Status: implemented · Tracks: InferenceCache tech spec §4.1 · API group: `inferencecache.io/v1alpha1`

`CacheBackend` is the namespaced CRD that describes a shared KV-cache backend and the engine integration policy that should use it. The API is vendor-neutral by contract: provider-specific behavior belongs in optional adapters, not in the core CRD.

## Identity

| | Value |
|---|---|
| group | `inferencecache.io` |
| version | `v1alpha1` |
| kind | `CacheBackend` |
| plural | `cachebackends` |
| short name | `cb` |

The `v1alpha1` contract must remain backward-compatible where possible. New fields should be additive, and tightening validation for existing fields requires an explicit migration/versioning path.

## Spec

| Field | Type | Purpose |
|---|---|---|
| `type` | string | Backend implementation identifier. Known constants are `LMCache`, `SGLangHiCache`, `AIBrix`, `Mooncake`, `NIXL`, and `External`; validation intentionally stays open for v1alpha1 compatibility. |
| `deploymentKind` | enum | Managed workload kind: `Deployment` or `StatefulSet`. |
| `replicas` | integer | Desired managed backend replicas. Minimum `0`. |
| `storage.pvc.size` | quantity | Requested PVC capacity when persistent storage is used. Required only when `storage.pvc` is present. The requested size is mirrored to `status.capacity`; wiring the PVC into the standalone LMCache server's data directory is deferred to a follow-up module (the C6 cutover moved LMCache rendering into the runtime adapter, which doesn't yet declare a data-volume contract). |
| `storage.pvc.storageClassName` | string | Optional StorageClass for PVC-backed cache storage. See `storage.pvc.size` for the wire-up status. |
| `autoscaling.minReplicas` | integer | Lower bound for HPA replica count. Defaults to `1` when unset. Minimum `1`. |
| `autoscaling.maxReplicas` | integer | Upper bound for HPA replica count. Required when `autoscaling` is set. Minimum `1`. Cross-field validation: `minReplicas <= maxReplicas`. |
| `autoscaling.targetCPUUtilizationPercent` | integer | Target average per-pod CPU utilization for the HPA. Defaults to `80` when unset. Range `[1, 100]`. |
| `integration.engine` | string | Engine integration target, such as SGLang or vLLM. |
| `integration.role` | enum | Engine participation mode: `ReadOnly`, `WriteOnly`, or `ReadWrite`. |
| `integration.lookupTimeoutMs` | integer | Lookup latency budget in milliseconds. Minimum `0`. Lookup callers must still fail open. |
| `integration.minimumPrefixTokens` | integer | Minimum prefix token count before attempting cache lookup. Minimum `0`. |
| `integration.failOpen` | boolean | Default `true`. When `true`, engine pods fall back to local prefill on cache unreachability — the cache is an optimization, never a serving dependency. Setting it to `false` is an advanced opt-in to fail-closed serving (the cache becomes a serving dependency); the controller surfaces this as a Warning Kubernetes Event on the owning `CacheBackend`. |
| `engineSelector.matchLabels` | map | Labels used to select engine pods or runtimes. |
| `backendConfig` | map | Backend-specific string settings. |
| `template` | object | Optional pod-level overrides for managed backend pods. This is a narrow override surface, not a full `PodSpec`; backend containers come from controller defaults. |
| `endpoint` | string | Optional endpoint for an existing external backend. The controller mirrors this into `status.endpoint`. |

### Template Overrides

`spec.template` supports partial pod-level overrides that can be merged with managed backend defaults:

- `nodeSelector`
- `affinity`
- `tolerations`
- `topologySpreadConstraints`
- `imagePullSecrets`
- `serviceAccountName`
- `securityContext`
- `priorityClassName`
- `schedulerName`
- `runtimeClassName`
- `terminationGracePeriodSeconds`

It intentionally does not expose `containers`; requiring users to provide containers would conflict with managed backend defaults and would make simple scheduling overrides unnecessarily large.

### backendConfig keys (managed LMCache)

`spec.backendConfig` is a free-form string map; the managed LMCache adapter (`pkg/adapters/runtime/vllm_lmcache.go`) recognizes overrides for the **standalone lmcache-server** workload it renders, and for the **engine-side env** a future mutating admission webhook will inject into vLLM pods. Defaults are overridable until they are promoted to first-class spec fields.

Server-side (consumed by `ResolveCacheServer` when rendering the cache-server pod):

| Key | Default | Purpose |
|---|---|---|
| `serverImage` | `lmcache/standalone:latest` | Container image for the standalone lmcache-server. Pin to a digest for non-local runs. Deliberately distinct from a bare `image` key (which previously addressed the all-in-one vLLM+LMCache container the prior reconciler rendered): an existing CR carrying `backendConfig.image: vllm/vllm-openai:…` is therefore silently ignored rather than rendering an lmcache-server pod with the wrong image. |
| `serverCommand` | `lmcache_server 0.0.0.0 65432 cpu` | Server command line. Override to switch to the newer `python3 -m lmcache.v1.multiprocess.server` form once it stabilises. The default targets the older `lmcache_server <host> <port> <storage>` form because it has a documented port (65432, the canonical `lm://` port) and arg layout. |

Engine-side (consumed by `InjectEngineConfig` when the webhook wires a vLLM pod to the cache):

| Key | Default | Purpose |
|---|---|---|
| `chunkSize` | `256` | `LMCACHE_CHUNK_SIZE` on the engine container. |
| `remoteSerde` | `naive` | `LMCACHE_REMOTE_SERDE` on the engine container. CPU-safe default; `cachegen` is faster but pulls in CUDA-only codepaths and should be opted into via this key on GPU. |
| `localCPU` | `False` | `LMCACHE_LOCAL_CPU` on the engine container. Defaults to `False` (remote-only); `True` enables a hybrid local+remote mode. |
| `maxLocalCPU` | `20` | `LMCACHE_MAX_LOCAL_CPU_SIZE` (GiB) on the engine container; only meaningful when `localCPU=True`. |

The webhook also injects the flags every vLLM+LMCache engine needs:

- `--kv-transfer-config '{"kv_connector":"LMCacheConnectorV1","kv_role":"<role>"}'` — `<role>` is derived from `spec.integration.role`: `ReadOnly → kv_consumer`, `WriteOnly → kv_producer`, `ReadWrite → kv_both` (also the default when `integration` is unset).
- `LMCACHE_REMOTE_URL=lm://<status.endpoint>` — the resolved cache endpoint, with the `lm://` scheme prefix added by the adapter (`status.endpoint` itself stays an engine-agnostic `host:port`).
- `VLLM_USE_V1=1`.
- `INFERENCECACHE_FAIL_OPEN=<true|false>` — mirrors `spec.integration.failOpen` onto the engine pod (defaults to `true` when the field is unset). The LMCache connector is fail-open by default at runtime regardless of this value; surfacing the bit lets the engine layer enforce fail-closed semantics when that work lands, and keeps the adapter aligned with the contract that this flag is plumbed by the engine adapter.

These are not user-overridable via `backendConfig`.

The retired colocated-rendering keys (`image`, `profile`, `model`, `hfTokenSecret`) were specific to a previous all-in-one vLLM+LMCache workload the reconciler templated. The new architecture splits the cache server from the engine: the engine is user-owned (its image/model/HF-token Secret live on the engine's own Deployment), the cache-server is engine-agnostic. CRs carrying any of those legacy keys keep validating against the unchanged CRD schema (`backendConfig` is a free-form string map) but the values are silently ignored — operators upgrading from the colocated rendering should drop them, or move them to the engine Deployment they own.

## Status

| Field | Type | Purpose |
|---|---|---|
| `endpoint` | string | Observed endpoint clients should use. For external backends this is mirrored from `spec.endpoint`; for managed backends it is populated by the controller that creates the serving resource. |
| `health` | enum | Summary state: `Pending`, `Ready`, `Degraded`, or `Failed`. |
| `capacity` | string | Human-readable summary of the backend's provisioned capacity (e.g. the requested PVC size when persistent storage is configured). Informational; clients must not parse it. |
| `indexEntries` | integer | Observed cache index entry count. Represented as a pointer in Go so an explicit `0` is serialized. |
| `failOpen` | boolean | Observed echo of the effective `spec.integration.failOpen`. Represented as a pointer in Go so an explicit `false` is serialized and operators can read the current mode from status alone. |
| `observedGeneration` | integer | The `.metadata.generation` last reconciled by the controller. Lets clients tell whether the observed status reflects the current spec. |
| `conditions` | array | Kubernetes conditions keyed by `type`. See [Conditions](#conditions). |

### Conditions

Two condition types are published on managed backends:

| Type | Meaning |
|---|---|
| `Ready` | True once the backend Deployment has rolled out its current generation and has enough updated + available replicas to serve traffic. |
| `Progressing` | True while the controller is still driving the live state toward the desired state (rollout in flight, first apply). Transitions to False once the Deployment converges (`Synced`) or stalls (`Degraded`). The pair (`Ready=False`, `Progressing=True`) means "still converging"; (`Ready=False`, `Progressing=False`) means "stuck/degraded". |

When the desired replica count is owned by an HPA (`spec.autoscaling` set) the controller compares health against the HPA-written Deployment `spec.replicas` rather than the user-set `spec.replicas`.

`kubectl get cachebackend` displays the observed `status.endpoint` so managed backends show the endpoint once reconciliation has created it.

## Contract Notes

- Lookup paths fail open by default. `spec.integration.failOpen` defaults to `true` and the engine adapter MUST fall back to local prefill on cache unreachability — the cache is an optimization, never a serving dependency. Operators may opt into fail-closed serving by setting `failOpen: false`, which is loud and visible: the controller emits a Warning `FailClosedEnabled` Event on the `CacheBackend` to make it explicit that the cache has been promoted to a serving dependency.
- The controller emits transition-only Events on the `CacheBackend`. `BackendDegraded` (Warning) on entering `Degraded`, `BackendRecovered` (Normal) on returning to `Ready` from `Degraded`, plus the `FailClosedEnabled` / `FailOpenRestored` pair above. Steady-state reconciles do not emit events.
- Optional nested specs are pointer fields in Go so omitted objects stay absent in JSON and server-side apply does not claim empty nested objects.
- `status.indexEntries` is a pointer in Go so `0` is distinguishable from unset.
