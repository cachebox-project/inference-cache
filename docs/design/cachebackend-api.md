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
| `storage.pvc.size` | quantity | Requested PVC capacity when persistent storage is used. Required only when `storage.pvc` is present. Accepted for forward-compat; wiring the PVC into the standalone LMCache server's data directory is deferred to a follow-up — the runtime-adapter interface doesn't yet declare a data-volume contract, so the controller has no place to attach it. Until then this field is inert. |
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
| `allowCrossNamespace` | boolean | Opt-in flag that allows `spec.endpoint` to resolve to a Kubernetes Service in a different namespace from the CacheBackend itself. Without it, admission rejects cross-namespace Service-DNS endpoints. External hostnames and IPs are unaffected. Defaults to `false`. |

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

`spec.backendConfig` is a free-form string map; the managed LMCache adapter (`pkg/adapters/runtime/vllm_lmcache.go`) recognizes overrides for the **standalone lmcache-server** workload it renders, and for the **engine-side env** that the [mutating Pod admission webhook](#mutating-pod-webhook-engine-wiring) injects into vLLM pods. Defaults are overridable until they are promoted to first-class spec fields.

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

The retired colocated-rendering keys (`image`, `profile`, `hfTokenSecret`) were specific to a previous all-in-one vLLM+LMCache workload the reconciler templated. The new architecture splits the cache server from the engine: the engine is user-owned (its image/HF-token Secret live on the engine's own Deployment), the cache-server is engine-agnostic. CRs carrying those legacy keys keep validating against the unchanged CRD schema (`backendConfig` is a free-form string map) but the values are silently ignored — operators upgrading from the colocated rendering should drop them, or move them to the engine Deployment they own.

`model` is **not** retired: the pod-mutating webhook reads `backendConfig.model` as the served model id for the `kvevent-subscriber` sidecar's `--model-id` flag. When the key is unset, the adapter skips appending the sidecar (the subscriber binary requires the flag); the next pod admission after the operator sets the key picks it up. Set this to the served model identifier the engine container is loaded with (the value that ends up in the engine's `served_model_name`).

The auto-attach itself is opt-in: the controller's `--kvevent-subscriber-image` flag defaults to empty, in which case the adapter returns no sidecar regardless of `backendConfig.model`. Operators turn auto-attach on by passing a real (digest-pinned in production) image. This default protects an unconfigured install from `ImagePullBackOff` on a nonexistent sidecar image, which would otherwise block engine pod readiness.

## Status

| Field | Type | Purpose |
|---|---|---|
| `endpoint` | string | Observed endpoint clients should use. For external backends this is mirrored from `spec.endpoint`; for managed backends it is populated by the controller that creates the serving resource. |
| `health` | enum | Summary state: `Pending`, `Ready`, `Degraded`, or `Failed`. |
| `capacity` | string | Human-readable summary of the backend's provisioned capacity. Informational; clients must not parse it. Intentionally left empty today — the runtime adapter doesn't yet declare a data volume the controller can attach a PVC to, so populating capacity from the requested PVC size would mislead operators. Populated by the storage wire-up follow-up. |
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
- Optional nested specs are pointer fields in Go so omitted objects stay absent in JSON and server-side apply does not claim empty nested objects. The defaulting webhook is the exception: it materialises `spec.integration` with the Phase-1 defaults (`lookupTimeoutMs`, `minimumPrefixTokens`) so downstream code does not need to nil-check them. The fields are owned by the apiserver field manager, not the operator's SSA apply, so SSA semantics for operator-set fields are unaffected.
- `status.indexEntries` is a pointer in Go so `0` is distinguishable from unset.

## Admission

The controller serves two webhooks for CacheBackend, both registered as `failurePolicy: Fail` with `sideEffects: None` on CREATE and UPDATE. Webhook serving requires cert-manager (see README "Cluster Prerequisites").

### Defaulting (mutating)

Stamps Phase-1 defaults onto every admitted CacheBackend, only where the operator has not specified a value. Operator-set values are never clobbered.

| Field | Default |
|---|---|
| `spec.replicas` | `1` |
| `spec.integration.lookupTimeoutMs` | `50` |
| `spec.integration.minimumPrefixTokens` | `256` |

### Validating

Rejects structurally-broken specs that the reconciler cannot do anything useful with, with field-scoped error messages. Multiple violations on a single spec are aggregated into one `Invalid` status so kubectl prints them together.

| Rule | Rejects |
|---|---|
| `External` requires `spec.endpoint` | `spec.type=External` with no endpoint — the external backend has no address to mirror to `status.endpoint`. |
| Memory-only backends cannot declare PVC storage | `spec.storage.pvc` set when `spec.type` is in the Phase-1 memory-only set (`AIBrix`, `NIXL`). These backends have no persistent tier; a PVC would never mount. |
| Cross-namespace endpoint requires opt-in | `spec.endpoint` resolves to a Service in a namespace other than the CacheBackend's, and `spec.allowCrossNamespace` is `false`. Crossing the namespace is a tenancy boundary the operator should acknowledge. Bare hostnames, IPs, and unqualified names pass through (no namespace to compare against). |
| Runtime/backend pair must be supported by an installed adapter | Effective `(engine, spec.type)` pair has no registered runtime adapter, so the reconciler would fall back to unmanaged. The effective engine is resolved with the same helper the reconciler and pod webhook use: `spec.integration.engine` lower-cased, defaulting to `vllm` when unset (Phase-1 default — the only engine the shipping adapters target). Bypassed for `spec.type=External` (mirrored, not managed) and for an empty `spec.type` (required-field rejection wins). The rejection message names both sides of the offending pair and lists the supported pairs the controller's registered adapters expose, e.g. `no runtime adapter supports the (engine="vllm", backend="Mooncake") pair; supported pairs in this build: vllm/LMCache`. |

The structural rules are an ordered, pluggable list (`CacheBackendValidator.Rules`); the runtime/backend compatibility check runs separately because it needs to consult the shared `adapterruntime.Registry` rather than just the spec.

### Migration

The validating rules tighten what `v1alpha1` accepts, so they ship together with the admission webhook itself (a previously-uninstalled webhook). Tightening applies at write time only:

- Existing stored CacheBackends that were applied before the webhook is installed remain in etcd and are unaffected until they are next created or mutated.
- An operator with a now-invalid CR can read it (`kubectl get`), but a write (create or update) fails until the spec satisfies the new rules.
- The cluster-wide rollout knob is the webhook's `failurePolicy`; future tightenings that need a softer rollout can switch to `Ignore` for one release before flipping to `Fail`.

### Mutating Pod webhook (engine wiring)

A separate mutating admission webhook on `corev1/v1.Pod` (`name: mpod.inferencecache.io`) auto-wires user-supplied inference engine pods to the matching managed `CacheBackend` — operators never have to hand-edit `LMCACHE_*` env vars or the LMCache connector arg onto their pod templates. The handler lives in `internal/webhook/pod` and runs on every Pod CREATE.

| Aspect | Behavior |
|---|---|
| Selection | Lists `CacheBackend`s in the pod's namespace via the manager's **APIReader** (uncached live client; an informer-cache miss on a freshly-Ready backend would leave the pod permanently unwired since pod CREATE is a one-shot), then matches `pod.Labels` against each `Spec.EngineSelector.MatchLabels`. The first matching `CacheBackend` wins; one with a nil or empty `EngineSelector` is skipped (a "match-everything" selector would silently claim every pod in the namespace). |
| Injection | Resolves the runtime adapter via `runtime.Registry.Select(runtimeID, cache)` and calls `adapter.InjectEngineConfig(pod.Spec, cache.Status.Endpoint, cache)`. The adapter merges: existing args/env on the engine container are preserved; repeat injections are idempotent. |
| Annotations | Stamps `inferencecache.io/injected-by: <namespace>/<name>` on every mutated pod for observability (`kubectl describe pod`). Reads `inferencecache.io/skip-inject: <truthy>` as an opt-out — the handler returns Allowed without mutation when set. |
| Idempotency | The handler calls the adapter unconditionally on every admission and trusts the adapter to converge the full injected contract (env + `--kv-transfer-config` arg). The adapter's merge primitives (`upsertEnv` / `upsertArgPair`) are no-ops when the desired value is already present, so a re-admission of a fully-injected pod produces an empty JSON-patch set and there is no apiserver round-trip cost. Trusting the adapter rather than a handler-side env-presence shortcut avoids the trap where a partially-injected pod (e.g. only `LMCACHE_REMOTE_URL` set by hand) is admitted permanently missing the rest of the contract. |
| Fail-open | Every error path (decode failure, list error, no matching backend, missing `status.endpoint`, no registered adapter, adapter rejection, re-encode failure) returns `admission.Allowed(...)` with a reason — webhook errors MUST NOT block engine admission. `MutatingWebhookConfiguration.failurePolicy` is also pinned to `Ignore` as a belt-and-suspenders second layer. |
| Verbs | `CREATE` only. UPDATE re-admissions to a running pod don't re-inject (and the engine container can't pick up env changes without a restart anyway); UPDATEs to engine pods are rare in this fleet. |
