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
| `storage.pvc.size` | quantity | Requested PVC capacity when persistent storage is used. Required only when `storage.pvc` is present. |
| `storage.pvc.storageClassName` | string | Optional StorageClass for PVC-backed cache storage. |
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

## Status

| Field | Type | Purpose |
|---|---|---|
| `endpoint` | string | Observed endpoint clients should use. For external backends this is mirrored from `spec.endpoint`; for managed backends it is populated by the controller that creates the serving resource. |
| `health` | enum | Summary state: `Pending`, `Ready`, `Degraded`, or `Failed`. |
| `indexEntries` | integer | Observed cache index entry count. Represented as a pointer in Go so an explicit `0` is serialized. |
| `failOpen` | boolean | Observed echo of the effective `spec.integration.failOpen`. Represented as a pointer in Go so an explicit `false` is serialized and operators can read the current mode from status alone. |
| `observedGeneration` | integer | The `.metadata.generation` last reconciled by the controller. Lets clients tell whether the observed status reflects the current spec. |
| `conditions` | array | Kubernetes conditions keyed by `type`. |

`kubectl get cachebackend` displays the observed `status.endpoint` so managed backends show the endpoint once reconciliation has created it.

## Contract Notes

- Lookup paths fail open by default. `spec.integration.failOpen` defaults to `true` and the engine adapter MUST fall back to local prefill on cache unreachability — the cache is an optimization, never a serving dependency. Operators may opt into fail-closed serving by setting `failOpen: false`, which is loud and visible: the controller emits a Warning `FailClosedEnabled` Event on the `CacheBackend` to make it explicit that the cache has been promoted to a serving dependency.
- The controller emits transition-only Events on the `CacheBackend`. `BackendDegraded` (Warning) on entering `Degraded`, `BackendRecovered` (Normal) on returning to `Ready` from `Degraded`, plus the `FailClosedEnabled` / `FailOpenRestored` pair above. Steady-state reconciles do not emit events.
- Optional nested specs are pointer fields in Go so omitted objects stay absent in JSON and server-side apply does not claim empty nested objects.
- `status.indexEntries` is a pointer in Go so `0` is distinguishable from unset.
