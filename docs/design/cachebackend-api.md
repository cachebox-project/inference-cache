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
| `integration.firstEventTimeout` | duration | Default `5m`. How long a managed backend may sit `Ready=False` (`Pending`, reason `AwaitingFirstKVEvent`) after its engine becomes HTTP-Ready before the controller flips it to `Degraded` (reason `NoKVEventsObserved`). See [KV-event readiness gate](#kv-event-readiness-gate). |
| `integration.engineOverrides` | object | Optional engine-injection overrides applied to the args/env the pod-mutating webhook would otherwise inject into the engine container. See [Engine-injection overrides](#engine-injection-overrides-specintegrationengineoverrides). |
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
| `indexParticipation` | object | Per-backend slice of the cluster-wide cache index, projected from the server's `/snapshot` by grouping replicas by owning `CacheBackend`. Populated by the CacheIndex poller (status-only). Object is unset until the poller has observed a successful scrape that names the backend's replicas (see [Index Participation](#index-participation)). |
| `observedGeneration` | integer | The `.metadata.generation` last reconciled by the controller. Lets clients tell whether the observed status reflects the current spec. |
| `conditions` | array | Kubernetes conditions keyed by `type`. See [Conditions](#conditions). |

### Conditions

Three condition types are published on managed backends:

| Type | Meaning |
|---|---|
| `Ready` | True once the backend Deployment has rolled out its current generation, has enough updated + available replicas to serve traffic, **and** — when the [KV-event readiness gate](#kv-event-readiness-gate) applies — at least one KV event has been observed for the backend (reason `KVEventsObserved`). HTTP-Ready but no event yet is `Ready=False`, reason `AwaitingFirstKVEvent`. |
| `Degraded` | True when the backend is in a terminal unhealthy state: rolled out but replicas unavailable (reason `ReplicasUnavailable`), or HTTP-Ready but no KV event observed within `firstEventTimeout` (reason `NoKVEventsObserved`). Tracks `status.health == Degraded`. |
| `Progressing` | True while the controller is still driving the live state toward the desired state (rollout in flight, first apply, awaiting first KV event). Transitions to False once the Deployment converges (`Synced`) or stalls (`Degraded`). The pair (`Ready=False`, `Progressing=True`) means "still converging"; (`Ready=False`, `Progressing=False`) means "stuck/degraded". |

When the desired replica count is owned by an HPA (`spec.autoscaling` set) the controller compares health against the HPA-written Deployment `spec.replicas` rather than the user-set `spec.replicas`.

### KV-event readiness gate

`Ready` means "the engine is up **and** the cache plane is actually receiving its state" — not merely "the pod's HTTP port answers". The engine's HTTP readiness probe (`GET /health`) proves the inference API is up but says nothing about whether the engine's ZMQ KV-event publisher is publishing. A pod can be HTTP-Ready while its publisher is silent (mis-configured `--kv-events-config`, a ZMQ bind failure, or an in-process publisher crash), in which case `LookupRoute` keeps returning `NO_HINT` for that backend's prefixes while the CR claims everything is fine. The gate makes that silent degradation loud.

The signal source is `status.indexParticipation.lastEventAt` (written by the CacheIndex poller); the reconciler only reads it. Once the engine becomes HTTP-Ready:

- **no event yet, within `firstEventTimeout`** → `Health=Pending`, `Ready=False` (`AwaitingFirstKVEvent`). The timeout clock starts when the engine Deployment first reports `Available`.
- **at least one event ever observed** → `Health=Ready`, `Ready=True` (`KVEventsObserved`). An event already present on the first reconcile counts — there is no required transition through `Pending`.
- **no event by `firstEventTimeout`** → `Health=Degraded`, `Degraded=True` (`NoKVEventsObserved`). Once Degraded it stays Degraded until an event arrives, then transitions to Ready.

The gate is **on by default** and opt-out per CR with the annotation `inferencecache.io/require-kv-events: "false"` (alpha soft-rollout knob; an annotation rather than a spec field so it can be retired once the gate is trusted). `spec.type: External` backends are **always exempt** — the control plane never subscribes to an external cache, so `lastEventAt` never populates; their readiness is determined by admission accepting `spec.endpoint`, as before.

**Operator note.** If a backend is stuck at `Pending` / `AwaitingFirstKVEvent` (and then flips to `Degraded` / `NoKVEventsObserved` after `firstEventTimeout`), the engine's KV-event publisher is almost certainly mis-configured — check the engine's `--kv-events-config` and that its ZMQ socket bound. The reason is visible in `kubectl get cachebackend` (the `Health` column) and `kubectl describe` surfaces the `NoKVEventsObserved` Warning Event that names the failure mode.

`kubectl get cachebackend` displays the observed `status.endpoint`, `status.indexParticipation.prefixCount` (as `PREFIXES`), and `status.indexParticipation.lastEventAt` (as `LASTEVENT`) so managed backends show endpoint + live participation once reconciliation has created them and the poller has observed a `/snapshot` tick.

### Index Participation

| Field | Type | Purpose |
|---|---|---|
| `indexParticipation.prefixCount` | integer | Sum of distinct prefix entries currently attributed to this backend's replicas. `0` is a valid observed value (the backend is up but holds no warm prefixes yet); always serialized. |
| `indexParticipation.lastEventAt` | time | Most recent KV-event timestamp observed for any of this backend's replicas. Unset until the first event arrives; readiness gates must treat the absent value as "not yet observed" rather than epoch. |
| `indexParticipation.hitRate` | string | Prefix-count-weighted cache hit rate across this backend's replicas, formatted as a decimal in `[0,1]`. Unset until the snapshot carries an explicit per-replica presence bit (planned with the stats-reporter follow-up); a missing value MUST NOT be interpreted as `0`. |

The poller attributes each `/snapshot.replicas[]` entry to a single owning `CacheBackend` by resolving the engine pod it came from. The subscriber sidecar runs inside the engine pod and reports `replica_id = <pod-name>`, `tenant_id = <pod-namespace>`. For each replica the poller:

1. Looks up the engine pod by `(tenant, replicaID)`.
2. If the pod carries the webhook's `inferencecache.io/injected-by` annotation (stamped as `<namespace>/<name>`), resolves the owning CacheBackend directly. This is the authoritative wiring signal — the engine container was wired to exactly that backend's endpoint.
3. Otherwise, iterates that namespace's CacheBackends sorted by `metadata.name` and picks the first whose `spec.engineSelector.matchLabels` is non-empty and is a subset of the pod's labels. This mirrors the pod webhook's first-match rule for pods that bypassed the webhook (manual sidecar attachment, opt-out).

Only ONE CacheBackend ever claims a given replica — overlapping selectors must agree on which backend owns the pod, otherwise status would disagree with what the engine was actually wired to. A CacheBackend without an EngineSelector (or with empty `MatchLabels`) is excluded from the selector fallback — otherwise a misconfigured backend would silently claim every replica in its namespace by vacuous truth — but a pod can still be attributed to it via the `injected-by` annotation. A replica whose pod can no longer be found (drained between events and now) is skipped; its data still appears in the cluster-wide `CacheIndex`. A failing scrape preserves existing state (soft-state); a successful scrape that finds no matching replicas resets `prefixCount` to `0` so stale positive values do not survive a drain.

## Contract Notes

- Lookup paths fail open by default. `spec.integration.failOpen` defaults to `true` and the engine adapter MUST fall back to local prefill on cache unreachability — the cache is an optimization, never a serving dependency. Operators may opt into fail-closed serving by setting `failOpen: false`, which is loud and visible: the controller emits a Warning `FailClosedEnabled` Event on the `CacheBackend` to make it explicit that the cache has been promoted to a serving dependency.
- The controller emits transition-only Events on the `CacheBackend`. `BackendDegraded` (Warning) on entering `Degraded` and `BackendRecovered` (Normal) on returning to `Ready` from `Degraded` (both suppressed for the KV-event-gate flavors, which carry their own events); the `FailClosedEnabled` / `FailOpenRestored` pair above; and the KV-event readiness gate's `AwaitingFirstKVEvent` (Normal), `KVEventsObserved` (Normal), and `NoKVEventsObserved` (Warning). Steady-state reconciles do not emit events.
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
| `spec.integration.firstEventTimeout` | `5m` (CRD-schema default when `spec.integration` is present; the webhook also stamps it when it materializes `spec.integration`) |

### Validating

Rejects structurally-broken specs that the reconciler cannot do anything useful with, with field-scoped error messages. Multiple violations on a single spec are aggregated into one `Invalid` status so kubectl prints them together.

| Rule | Rejects |
|---|---|
| `External` requires `spec.endpoint` | `spec.type=External` with no endpoint — the external backend has no address to mirror to `status.endpoint`. |
| Memory-only backends cannot declare PVC storage | `spec.storage.pvc` set when `spec.type` is in the Phase-1 memory-only set (`AIBrix`, `NIXL`). These backends have no persistent tier; a PVC would never mount. |
| Cross-namespace endpoint requires opt-in | `spec.endpoint` resolves to a Service in a namespace other than the CacheBackend's, and `spec.allowCrossNamespace` is `false`. Crossing the namespace is a tenancy boundary the operator should acknowledge. Bare hostnames, IPs, and unqualified names pass through (no namespace to compare against). |
| `spec.integration.engineOverrides` cannot touch reserved args/env | An entry in `engineOverrides.args` / `engineOverrides.suppressArgs` matches a leading flag token the adapter declares as `ReservedArgs()`, or an entry in `engineOverrides.env` / `engineOverrides.suppressEnv` matches a name in `ReservedEnv()`. The rejection names both the offending flag/env and the adapter so the operator can fix the spec rather than wait for the engine to crash. The reserved set is per-adapter (the vLLM+LMCache adapter reserves `--kv-transfer-config`, `VLLM_USE_V1`, `LMCACHE_REMOTE_URL`, `INFERENCECACHE_FAIL_OPEN`). |
| Runtime/backend pair must be supported by an installed adapter | Effective `(engine, spec.type)` pair has no registered runtime adapter, so the reconciler would fall back to unmanaged. The effective engine is resolved with the same helper the reconciler and pod webhook use: `spec.integration.engine` lower-cased, defaulting to `vllm` when unset (Phase-1 default — the only engine the shipping adapters target). Bypassed for `spec.type=External` (mirrored, not managed) and for an empty `spec.type` (required-field rejection wins). The rejection message names both sides of the offending pair and lists the supported pairs the controller's registered adapters expose, e.g. `no runtime adapter supports the (engine="vllm", backend="Mooncake") pair; supported pairs in this build: vllm/LMCache`. |

The structural rules are an ordered, pluggable list (`CacheBackendValidator.Rules`); the runtime/backend compatibility check runs separately because it needs to consult the shared `adapterruntime.Registry` rather than just the spec.

### Migration

The validating rules tighten what `v1alpha1` accepts, so they ship together with the admission webhook itself (a previously-uninstalled webhook). Tightening applies at write time only:

- Existing stored CacheBackends that were applied before the webhook is installed remain in etcd and are unaffected until they are next created or mutated.
- An operator with a now-invalid CR can read it (`kubectl get`), but a write (create or update) fails until the spec satisfies the new rules.
- The cluster-wide rollout knob is the webhook's `failurePolicy`; future tightenings that need a softer rollout can switch to `Ignore` for one release before flipping to `Fail`.

### Engine-injection overrides (`spec.integration.engineOverrides`)

`spec.integration.engineOverrides` lets the operator amend the non-reserved args/env the pod-mutating webhook injects into the engine container — without forking an adapter. It is the user-facing seam that today's CPU-vLLM-with-LMCache use case and future engines (SGLang, Mooncake) reach to tune adapter-injected knobs (chunk size, max model length, serdes) that the canonical injection would otherwise hard-code. The reserved set (per locked decision #5/#6 below) makes this surface unsuitable for turning the integration *off*: operators who need to skip injection entirely on a pod should use the `inferencecache.io/skip-inject` annotation instead.

Shape, in `corev1` vocabulary:

| Field | Type | Behavior |
|---|---|---|
| `args` | `[]string` | Args added to the engine container, scoped to adapter-owned flags. An entry whose leading flag token matches an adapter-owned canonical arg replaces it; an entry whose token is in neither the adapter-owned set nor the user pod-template is appended; an entry colliding with a user-template flag the adapter did not touch is a silent no-op. Order preserved. |
| `suppressArgs` | `[]string` | Leading flag names the adapter MUST NOT inject. Restricted to the adapter-owned set: a suppress entry that names a user-template flag the adapter did not inject is a silent no-op. |
| `env` | `[]corev1.EnvVar` | Env upserted by `Name`, scoped to adapter-owned canonical entries. An override of an adapter-owned name wins; a new name (not on the user template) is appended; a name colliding with a user-owned env the adapter did not touch is a silent no-op. |
| `suppressEnv` | `[]string` | Env var Names the adapter MUST NOT inject. Restricted to adapter-owned entries; user-owned env is protected. |

The "adapter-owned" set is derived by the webhook at admission time by diffing the engine container's args/env immediately before and after `InjectEngineConfig` runs. A flag/env is adapter-owned if the adapter added it OR modified an existing value. User pod-template entries the adapter does not touch are protected from CR-driven mutation — the CR can amend the engine integration, but not silently rewrite the engine pod owner's own template.

No `command` override (the entrypoint stays user-owned). No `resources` override here (engine-pod resources are user-owned via the engine's own pod template, not this CR). No override on the C2-managed lmcache-server pod in v1alpha1 — that surface stays adapter-owned until a managed component grows a knob that demands it.

The CRD field default is byte-identical to the prior behavior: a CacheBackend with no `engineOverrides` block renders the same injected patch as before.

#### Reserved declarations and admission hard-reject

Each `KVCacheRuntimeAdapter` declares two methods:

- `ReservedArgs() []string` — leading flag tokens the user MUST NOT override or suppress.
- `ReservedEnv()  []string` — env var names the user MUST NOT override or suppress.

The validating webhook iterates the adapter's reserved lists (resolved from `spec.integration.engine`) and **hard-rejects** any `engineOverrides.{args,suppressArgs}` entry that overlaps `ReservedArgs()` and any `engineOverrides.{env,suppressEnv}` entry that overlaps `ReservedEnv()`. The rejection names the offending flag/env and the adapter. Warning-only would let a user silently un-wire the integration and discover it via a crashed engine; the hard-reject keeps the breadcrumb at admission time.

The vLLM+LMCache adapter (`pkg/adapters/runtime/vllm_lmcache.go`) reserves the args/env the integration cannot function without:

- `ReservedArgs()`: `--kv-transfer-config` (the LMCache connector wiring).
- `ReservedEnv()`: `VLLM_USE_V1` (selects the engine codepath the connector targets), `LMCACHE_REMOTE_URL` (the resolved cache endpoint), `INFERENCECACHE_FAIL_OPEN` (mirror of `spec.integration.failOpen` — overriding it would silently desync the pod from the CR contract).

`LMCACHE_CHUNK_SIZE`, `LMCACHE_REMOTE_SERDE`, `LMCACHE_LOCAL_CPU`, `LMCACHE_MAX_LOCAL_CPU_SIZE` are deliberately NOT reserved — they are perf/mode tunables the operator may legitimately want to change. (`spec.backendConfig` already exposes them; `engineOverrides.env` is the engine-agnostic seam future engines without a per-key map will reach for.)

#### Shape rationale (A vs. B)

Two shapes were on the table:

- **A — typed K8s vocabulary** (`[]string` args, `[]corev1.EnvVar` env, plus suppression). Chosen.
- **B — backendConfig magic keys** (`cpuMode: "true"`, `gpuLimit: "0"`, `extraArgs: "..."`). Rejected.

A is more general: SGLang, Mooncake, and future engines plug in with no per-adapter `backendConfig` schema churn. It keeps the CRD disciplined (no permanent v1alpha1 legacy keys). B is faster to ship but bakes engine-specific knobs into the CRD, which is the trap an "engine-agnostic backend" surface is meant to avoid.

#### Residual risk

A user can still set non-reserved values that break the engine in subtle ways the validator can't catch — e.g. `--max-model-len 999999999` OOMing the engine, or env that subtly changes vLLM behavior. Mitigations shipped with this surface:

- Field godoc carries a "known-fragile" callout.
- `ReservedEnv()` mirrors `ReservedArgs()` for the worst offenders, so the canonical wiring can't be silently un-wired.
- Default samples in `config/samples/` exercise the no-override path so a future drift in the adapter's canonical injection breaks them loudly.

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
