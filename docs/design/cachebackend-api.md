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
| `autoscaling.minReplicas` | integer | Lower bound for HPA replica count. Defaults to `1` when unset. Minimum `1`. |
| `autoscaling.maxReplicas` | integer | Upper bound for HPA replica count. Required when `autoscaling` is set. Minimum `1`. Cross-field validation: `minReplicas <= maxReplicas`. |
| `autoscaling.targetCPUUtilizationPercent` | integer | Target average per-pod CPU utilization for the HPA. Defaults to `80` when unset. Range `[1, 100]`. |
| `integration.engine` | string | Engine integration target, such as SGLang or vLLM. |
| `integration.role` | enum | Engine participation mode: `ReadOnly`, `WriteOnly`, or `ReadWrite`. |
| `integration.lookupTimeoutMs` | integer | Lookup latency budget in milliseconds. Minimum `0`. Lookup callers must still fail open. |
| `integration.minimumPrefixTokens` | integer | Minimum prefix token count before attempting cache lookup. Minimum `0`. |
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
| `capacity` | string | Human-readable summary of the backend's provisioned capacity (e.g. the requested PVC size when persistent storage is configured). Informational; clients must not parse it. |
| `indexEntries` | integer | Observed cache index entry count. Represented as a pointer in Go so an explicit `0` is serialized. |
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

- Lookup paths fail open unconditionally. The CRD does not expose a `failOpen` switch because fail-closed lookup behavior is not part of the core contract.
- Optional nested specs are pointer fields in Go so omitted objects stay absent in JSON and server-side apply does not claim empty nested objects.
- `status.indexEntries` is a pointer in Go so `0` is distinguishable from unset.
