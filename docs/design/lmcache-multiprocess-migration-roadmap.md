# Design Roadmap: LMCache Multiprocess Migration

Status: **engineering-validated; Phase 0 complete (2026-08-09)** · Scope:
deprecate and remove this project's LMCache in-process data plane, converge vLLM
and SGLang on LMCache multiprocess (MP) mode, and model
Pod-local and node-local MP server placement without conflating either with
optional remote storage.

This roadmap is the tracking document for the migration. It supersedes the
long-term migration policy in
[`sglang-lmcache-mp-mode.md`](sglang-lmcache-mp-mode.md), while retaining that
document as implementation history and GPU-validation evidence for the current
SGLang prototype. It also drives the LMCache-related revisions to
[`cachebackend-api.md`](cachebackend-api.md).

## Executive recommendation

Do not delete the in-process (IP) implementation before a production-credible MP
replacement exists. Use a strangler migration:

1. freeze the IP path and lock the target API;
2. introduce an MP-only `CacheBackend` surface;
3. extract and harden the common MP server infrastructure using SGLang;
4. add vLLM MP in explicit opt-in mode;
5. migrate all repository-owned samples, tests, and manifests;
6. confirm that the Phase 0 no-consumer assumption still holds, then remove the
   IP adapter, `lm://` provider, and legacy lifecycle code; and
7. add node-local shared MP servers after the Pod-local path is stable.

The final API does **not** preserve `InProcess` as a first-class long-term mode.
For `spec.type: LMCache`, the canonical data plane is MP. IP exists only as a
temporary implementation transition path, not an external compatibility
commitment under the Phase 0 finding.

## Assumptions

- `inferencecache.io/v1alpha1` remains pre-launch and eligible for the documented
  alpha removal carve-out. The project owner confirmed on 2026-08-09 that there
  are no external `CacheBackend` consumers and no installed legacy objects that
  require migration. The selected policy is therefore an in-place alpha cleanup.
  If that fact changes before physical removal, the conditional compatibility
  stage in this roadmap becomes mandatory.
- The first complete MP milestone uses **PodLocal** placement. **NodeLocal** is a
  designed topology but does not block removal of IP when cross-Pod sharing is
  supplied by a supported remote L3.
- The initial remote L3 scope is RESP/Redis. MP + Mooncake Store, S3, NIXL, and
  other adapters are separate follow-ups; they must not be implied by accepting
  an inert provider declaration.
- Native sidecars require a supported Kubernetes version. The exact minimum
  Kubernetes version is locked in Phase 0 and enforced/documented before the MP
  path becomes the default.
- The inference-workload owner supplies and pins the engine image; CacheBackend
  never rewrites it. CacheBackend pins the cache components it injects or
  manages, and the selected runtime adapter validates the connector/server
  compatibility profile. Mixed MP client/server versions are not assumed
  wire-compatible.

## Terminology and tier model

LMCache upstream calls MP server CPU memory **L1** and secondary storage **L2**.
This project describes the complete serving hierarchy beginning with GPU KV, so
the corresponding project-level names are:

| Project tier | Owner | LMCache upstream name |
|---|---|---|
| GPU KV / L1 | inference engine | engine GPU cache |
| Host-memory / L2 | IP connector or MP server | local CPU / MP L1 |
| Remote / L3 | optional storage adapter | remote backend / MP L2 |

This roadmap uses the project-level names unless quoting an upstream flag or
type. In particular:

- **MP server** is the out-of-process LMCache service reached by an engine
  connector. Existing code and documents sometimes call it an MP worker.
- **PodLocal** means one MP server native sidecar per engine Pod, reached over
  loopback and sharing that Pod's `/dev/shm`.
- **NodeLocal** means one MP server Pod per node, normally a DaemonSet member,
  shared by engine Pods on that node.
- **Remote storage** means only the optional L3 behind the IP connector or MP
  server. An MP server is never declared as `remoteStorage`.
- **Legacy LMCacheServer** means the legacy IP centralized-sharing service
  reached through `remote_url: lm://...`. It is not an MP L3 adapter.
- **MP + Mooncake Store L3** means an MP server configured with
  `--l2-adapter '{"type":"mooncake_store", ...}'`. It is distinct from the
  legacy engine-side `mooncakestore://` connector.

## Locked architectural decisions

The following decisions are the baseline for implementation. Changing one
requires updating this roadmap and recording the replacement decision before
code lands.

| ID | Decision | Rationale |
|---|---|---|
| D1 | The final LMCache data plane is MP-only. | Upstream v0.5.3 recommends MP but still documents IP. Deprecating IP is this project's decision; keeping both indefinitely doubles adapter, lifecycle, and test complexity. |
| D2 | `remoteStorage` is optional L3 only. | Local CPU capacity and MP server placement are engine-integration concerns, not remote-provider selection. |
| D3 | `LMCacheServer` is removed from the canonical `remoteStorage.provider` set. | `lm://` is a legacy IP remote connector and is absent from the MP L3 adapter catalog. |
| D4 | PodLocal is the first production candidate and migration target. | It has the smallest scheduling and ownership surface and builds on the existing SGLang proof. |
| D5 | NodeLocal means a per-`CacheBackend` DaemonSet in its first implementation. | Multiple engine Pods of one backend may share it; cross-`CacheBackend` sharing introduces unresolved config, tenancy, port, and deletion ownership. |
| D6 | A generic Deployment behind a load-balanced Service is not a valid CUDA MP topology. | CUDA IPC and shared memory require the engine to reach the MP server on its own node. |
| D7 | Connector endpoints are not published in the generic `status.endpoint`. | PodLocal uses loopback; NodeLocal is node-dependent. Only remote L3 has a globally meaningful provider endpoint. |
| D8 | Unsupported combinations are rejected at admission. | An accepted but inert cache field commonly produces silent zero-hit behavior. |
| D9 | Fail-open is rendered into runtime-native behavior and tested. | A custom environment variable without a known consumer is not an enforceable serving contract. |
| D10 | Provider restart recovery is capability-specific. | Legacy `lm://` socket recovery, MP server recovery, and remote L3 recovery have different semantics and blast radii. |
| D11 | Each supported vLLM profile explicitly identifies its MP connector implementation; the initial reference profile uses the LMCache-shipped connector. | With vLLM 0.20 or newer, `LMCacheMPConnector` without a module path selects vLLM's built-in implementation. The initial profile uses `kv_connector_module_path: lmcache.integration.vllm.lmcache_mp_connector` so the tested client tracks the pinned LMCache server protocol; a future profile may validate a different implementation explicitly. |
| D12 | CacheBackend never owns or rewrites the inference engine image. Engine images in validation matrices are reproducible fixtures only; CacheBackend digest-pins only cache components it injects or manages. | The inference system owns its runtime lifecycle. Runtime adapters must declare and validate the connector capabilities they require without turning a tested engine image into an API allowlist or mutation default. |

## Current state

| Area | Current behavior | Gap to target |
|---|---|---|
| SGLang engine wire | Implicit MP; injects a Pod-local native sidecar, config file, loopback endpoint, and shared `/dev/shm`; the sidecar image defaults to the engine image. | Renderer is SGLang-private; cache-component ownership is coupled to the workload image; legacy ZMQ-only server entry point; incomplete worker health/recovery and parallelism coverage. |
| vLLM engine wire | `LMCacheConnectorV1` with optional host CPU, `lm://`, or `mooncakestore://`. | No `LMCacheMPConnector`; IP is still the only vLLM LMCache implementation. |
| CR API | MP mode is inferred from runtime. `hostMemory`, `workerImage`, `workerPort`, and `remoteSerde` are flat sibling fields. | No explicit MP topology; mode-specific fields can be accepted and ignored. |
| Remote storage | `Redis`, `LMCacheServer`, and `Mooncake` share one provider abstraction. | `LMCacheServer` is a legacy connector service, not a general MP L3; Mooncake needs a different MP binding shape. |
| Lifecycle | Every managed provider participates in the cache-server restart cascade. | Redis L3 restarts can roll engine fleets even though the engine connects to a local MP server. |
| Status | Provider readiness and engine-container crash loops are observed. | Native-sidecar health and node coverage are not represented; `status.endpoint` is ambiguous. |
| Tests | Strong Go unit coverage; SGLang single-GPU evidence; sample admission checks. | No default-install engine-Pod injection smoke; no vLLM MP; no automated GPU fault/parallelism matrix. |

### Connector ownership

The engine-side connector and the LMCache MP server are separate components,
and both engines require code from LMCache:

| Runtime | Engine-owned integration surface | LMCache-owned dependency |
|---|---|---|
| vLLM | Generic `KVConnectorFactory` plus built-in `LMCacheConnectorV1` and `LMCacheMPConnector` registrations. An external module path can replace the registered implementation. | The built-in connectors still import the `lmcache` package; LMCache also ships its own vLLM connector implementation and the MP server. |
| SGLang | LMCache-specific `LMCRadixCache`, `--enable-lmcache`, and `--lmcache-config-file` integration. It is not selected through vLLM's generic connector registry. | SGLang imports `LMCacheMPConnector` and related adapters from `lmcache.integration.sglang`; LMCache also supplies the MP server. |

Therefore neither engine image is self-sufficient merely because it exposes an
LMCache flag or connector class. A runtime adapter must verify that the engine
image contains the required LMCache client package/API, then CacheBackend injects
and manages a compatible MP server without replacing that engine image.

Source support is not the same as image support. The upstream vLLM Dockerfile
defaults `INSTALL_KV_CONNECTORS=false`, so its connector source may be present
while the `lmcache` runtime dependency is absent. SGLang likewise documents
installing `lmcache` separately; its integration raises an error when that import
is unavailable. CacheBackend cannot fix a missing Python package by injecting
flags or a server sidecar. A supported runtime profile must therefore establish
that the workload image already contains the required LMCache client and expose
enough version/capability metadata for the adapter to select a compatible server.

## Target architecture

### PodLocal

```text
engine Pod
  +---------------------+       optional network       +------------------+
  | vLLM or SGLang      |                              | remote L3        |
  | MP connector        |                              | Redis initially  |
  |        |            |                              +---------^--------+
  |  loopback + IPC     |                                        |
  |        v            |                                        |
  | LMCache MP server   +----------------------------------------+
  | CPU L2 / MP L1      |
  +---------------------+
```

Properties:

- one MP server per engine Pod;
- engine endpoint is `127.0.0.1:<port>`;
- L2 capacity is per engine Pod;
- engine and server share a Pod network namespace and `/dev/shm`;
- no `hostNetwork` is required;
- cross-Pod sharing requires a configured remote L3;
- lifecycle is coupled, but mid-flight server restart behavior must still be
  defined and tested.

### NodeLocal

```text
GPU node
  +-------------------+  +-------------------+
  | engine Pod A      |  | engine Pod B      |
  | MP connector      |  | MP connector      |
  +---------+---------+  +---------+---------+
            | node-local endpoint   |
            +-----------+-----------+
                        v
              +--------------------+
              | LMCache MP server  |   one DaemonSet Pod per node
              | shared CPU L2      |
              +---------+----------+
                        |
                        v
                 optional remote L3
```

Properties:

- one MP server per eligible node per `CacheBackend`;
- multiple selected engine Pods on that node share CPU cache capacity;
- engines derive the endpoint from their node identity, not a load-balanced
  Service endpoint;
- L2 capacity is per node;
- server scheduling, host port, host shared-memory arrangement, GPU visibility,
  and node coverage become controller-owned concerns;
- cross-`CacheBackend` and cross-tenant sharing are out of scope for the first
  implementation.

## Target API direction

The exact Go type names are finalized in Phase 1. The intended operator shape is:

### PodLocal example

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: vllm-lmcache
spec:
  runtime: VLLM
  type: LMCache
  lmCache:
    topology: PodLocal
    podLocal:
      server:
        image: registry.example/lmcache-vllm@sha256:...
        port: 6555
        l1Capacity: 32Gi
        maxWorkers: 1
        resources:
          requests:
            cpu: "2"
            memory: 33Gi
          limits:
            memory: 33Gi
  remoteStorage:
    provider: Redis
    ownership: External
    endpoint: redis.example:6379
  integration:
    role: ReadWrite
    failOpen: true
  engineSelector:
    matchLabels:
      app.kubernetes.io/name: vllm
```

Omit `remoteStorage` for host-only MP operation.

### NodeLocal example

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: vllm-lmcache-node-local
spec:
  runtime: VLLM
  type: LMCache
  lmCache:
    topology: NodeLocal
    nodeLocal:
      server:
        image: registry.example/lmcache-standalone@sha256:...
        port: 6555
        l1Capacity: 128Gi
        maxGPUWorkers: 8
        maxCPUWorkers: 8
        resources:
          requests:
            cpu: "8"
            memory: 132Gi
          limits:
            memory: 132Gi
      scheduling:
        nodeSelector:
          inferencecache.io/lmcache-mp: "true"
        tolerations: []
  remoteStorage:
    provider: Redis
    ownership: External
    endpoint: redis.example:6379
  engineSelector:
    matchLabels:
      app.kubernetes.io/name: vllm
```

### Final provider matrix

| Engine | MP topology | No remote L3 | Redis/RESP | Mooncake Store | Legacy LMCacheServer |
|---|---|---:|---:|---:|---:|
| SGLang | PodLocal | required MVP | required MVP | future | rejected |
| vLLM | PodLocal | required MVP | required MVP | future | rejected |
| SGLang | NodeLocal | planned | planned | future | rejected |
| vLLM | NodeLocal | planned | planned | future | rejected |

“Required MVP” means the combination must be implemented and validated, not that
the remote L3 field itself is required.

## Status direction

The final field names are part of Phase 1 API review. Status must distinguish
connector health from remote-provider health and must not compress a node-local
endpoint set into one string.

```yaml
status:
  connector:
    mode: Multiprocess
    topology: NodeLocal
    matchedEnginePods: 8
    readyEnginePods: 8
    desiredServers: 4
    readyServers: 4
    coveredEnginePods: 8
    uncoveredEnginePods: 0
  remoteStorage:
    provider: Redis
    endpoint: redis.example:6379
    ready: "True"
  conditions:
    - type: ConnectorReady
      status: "True"
    - type: RemoteStorageReady
      status: "True"
    - type: Ready
      status: "True"
```

Required semantics:

- PodLocal `desiredServers` equals the selected engine Pod count.
- NodeLocal `desiredServers` equals the number of distinct nodes hosting selected
  engine Pods, or the explicitly managed eligible-node count when the DaemonSet
  is intentionally prewarmed.
- `coveredEnginePods` counts selected engine Pods whose required MP server is
  healthy and reachable.
- PodLocal loopback and NodeLocal node-derived connector addresses are not
  published as `status.remoteStorage.endpoint`.
- A configured but unavailable remote L3 produces an explicit degraded/remote
  condition. Whether it changes overall `Ready` depends on the effective,
  runtime-native fail-open policy.

Condition and Event contract:

| Signal | Semantics |
|---|---|
| `ConnectorReady` | `Unknown/ConnectorCapabilityUnverified` until the runtime declaration and Pod shape are verified; `False` for a known incompatibility or unhealthy required MP server; `True` only when the selected engines are covered by healthy MP servers. |
| `RemoteStorageReady` | Omitted when no L3 is configured; otherwise `Unknown/RemoteStoragePending`, `False/RemoteStorageUnavailable`, or `True/RemoteStorageReady`, independently of connector health. |
| `LegacyInProcessDeprecated` | Conditional compatibility signal only if a legacy consumer appears before physical removal: condition `True` plus a Warning Event with the same reason and a migration instruction. It is never set for typed MP objects. |

Events are emitted on signal transitions or a changed observed generation, not
on every reconcile. The Phase 2 status writer implements the MP health signals;
the legacy deprecation writer is only implemented if Phase 6 is activated.

## Delivery overview

| Phase | Outcome | Depends on | Status |
|---|---|---|---|
| 0 | Design freeze, consumer audit, version/Kubernetes baseline | none | complete |
| 1 | MP-only API and admission/status contracts | Phase 0 | complete |
| 2 | Engine-neutral PodLocal MP server renderer | Phase 1 | complete |
| 3 | Production-credible SGLang PodLocal MP baseline | Phase 2 | in progress — non-GPU baseline complete; GPU matrix pending |
| 4 | vLLM PodLocal MP, host-only and Redis | Phase 3 | not started |
| 5 | Repository consumer migration; migration tooling only if needed | Phase 4 | not started |
| 6 | Conditional compatibility gate if legacy consumers appear | Phase 5 | not required by Phase 0 finding |
| 7 | Remove IP, `lm://`, and LMCacheServer provider | Phase 5; Phase 6 only when applicable | not started |
| 8 | NodeLocal shared MP server topology | Phases 3–4; does not block Phase 7 | not started |

## Phase 0 — design freeze and compatibility baseline

### Goal

Stop the target from moving while implementation begins and determine whether
the alpha removal carve-out is safe to use.

### Deliverables

- [x] Complete the engineering review and obtain project-owner approval for
      D1–D12.
- [x] Inventory all repository manifests and confirm the external/installed
      population of `CacheBackend` objects using:
  - vLLM IP host-only;
  - `remoteStorage.provider: LMCacheServer`;
  - existing IP-only `remoteStorage.provider: Mooncake`;
  - SGLang MP flat worker fields.
- [x] Decide the API migration strategy:
  - **selected:** in-place `v1alpha1` cleanup because there are zero external
    consumers and zero installed legacy objects; or
  - served compatibility period if that fact changes before removal.
- [x] Pin and record the Phase 3/4 target tuple for each engine:
  - reference engine image/digest, used only as a validation fixture;
  - CacheBackend-injected server image/digest;
  - LMCache version;
  - CUDA/runtime version;
  - Kubernetes version;
  - target model and parallelism validation modes.
- [x] Freeze new feature work on `LMCacheConnectorV1`, `lm://`, and the managed
      legacy LMCache server.
- [x] Resolve whether top-level managed-provider fields (`replicas`,
      `autoscaling`, `deploymentKind`, `template`) move below `remoteStorage` in
      the same alpha API cleanup. They must not accidentally configure an MP
      server with a different lifecycle.

  **Selected:** retain them only as legacy provider-workload inputs during the
  repository migration, then relocate any still-needed fields below a typed
  `remoteStorage` managed-workload block in Phase 7. They never configure the
  PodLocal/NodeLocal MP server; that lifecycle lives exclusively under
  `lmCache.podLocal.server` or `lmCache.nodeLocal.server`.

The locked Phase 3/4 validation targets are reference environments, not engine
image requirements or admission allowlists:

| Component/profile | Reference validation environment | Ownership | Required validation |
|---|---|---|---|
| LMCache MP server | `lmcache/standalone@sha256:b813bf0bb616d1012b6a6edcbd4a44f1576dbbdaa857962e56d48b9f7c127d13`; LMCache 0.5.3; linux/amd64; CUDA 13.0.1 | CacheBackend-injected and digest-pinned | Client/server compatibility with both reference runtimes, probes, restart, and recovery |
| vLLM connector profile | `lmcache/vllm-openai@sha256:dca0afdda6ad1bb02e63619d366fcd18975b334d7274739ed6f2025035865781`; LMCache 0.5.3; explicit `lmcache.integration.vllm.lmcache_mp_connector`; linux/amd64; CUDA 13.0.1 | Inference-owner image; test fixture only | Llama 3.1 8B; TP=1/2, plus TP=4 before common multi-GPU recommendation |
| SGLang connector profile | linux/amd64 from `lmsysorg/sglang@sha256:1c64fde976bdf0d56474a30bccbcfc19667e5b3ab34c826a534c9d6aaca41212` (`v0.5.13.post1-cu130`) with exactly `lmcache==0.5.3`; CUDA 13.0 | Inference-owner image; derived test fixture only | `--enable-lmcache`/config-file capability; Llama 3 8B; TP=1/2 |
| Redis L3 | `redis@sha256:e7723ff73d963f5cc6d9c4643ea3d989527a402a319239054e9472a7fb9219a2` (`7.4.10-alpine`) | CacheBackend-managed when not external | Both engines: cross-Pod reuse, credentials/TLS, outage, and recovery |

Kubernetes 1.29 with `SidecarContainers` enabled is the minimum; 1.33 or newer
is recommended. All four published registry digests resolved on 2026-08-09, and
the SGLang and Redis indexes contain linux/amd64 manifests. The SGLang derived
test-fixture digest is produced in Phase 3. The vLLM reference digest pins its
contents, but its upstream release build did not constrain the vLLM package
version; Phase 4 preflight must record `vllm.__version__` and reject that
reference profile if incompatible. None of these engine-image choices authorize
CacheBackend to mutate a workload's image.

### Validation

- [x] Repository search and sample inventory are attached to the design PR.
- [x] Current Go tests and sample verification pass before behavior changes.
- [x] Every unsupported or unverified combination has an explicit disposition.

The repository manifest inventory is:

| Migration class | Objects | Disposition |
|---|---:|---|
| vLLM IP + `LMCacheServer` | 13 | Migrate repository samples and references after vLLM MP passes Phase 4. |
| vLLM IP + existing engine-side Mooncake provider | 1 | Do not reinterpret as MP `mooncake_store`; migrate explicitly or defer until that adapter is supported. |
| vLLM IP host-only | 1 | Intentionally invalid test fixture; update with the admission tests. |
| SGLang MP with flat fields + Redis | 1 | Move to the typed PodLocal block in Phase 5. |
| SGLang MP with flat fields, host-only | 1 | Move to the typed PodLocal block in Phase 5. |
| EventsOnly, no LMCache data plane | 1 | No data-plane migration. |

Direct repository consumers also include the reference-stack manifest and Helm
values, default-install checks, C2/C6 scripts, and their fixtures. The
`SGLangHiCache` sample is outside this LMCache migration. The project owner
confirmed that there are no external `CacheBackend` consumers and no installed
legacy objects requiring migration, so live-cluster inventory is not required.

Baseline validation at commit `083e916` passed `go test ./...` and
`make verify-samples` (21 admitted, 2 explicit skips, 0 failures). Naming and
internal-reference checks also passed. These are API/data-plane baselines, not
GPU or kubelet-native-sidecar evidence; those gates remain in Phases 2–4.

### Exit criteria

- The project owner confirmed that no external or installed legacy consumer can
  be broken by the chosen in-place alpha cleanup.
- The immutable candidate images, software versions, Kubernetes baseline, and
  Phase 3/4 validation matrix are published.
- The target CR and migration policy are approved.

## Phase 1 — MP-only API, admission, and status contract

### Goal

Add the final MP shape before changing the data plane, while retaining only the
minimum compatibility surface needed to migrate existing IP objects.

### API work

- [x] Add typed MP-only `spec.lmCache` configuration. Because MP is the only
      canonical LMCache data plane, do not add a redundant `multiprocess`
      nesting level.
- [x] Add the `PodLocal` topology and its typed server configuration.
- [x] Design the `NodeLocal` block now, but reject it until Phase 8 is
      implemented. Do not accept inert NodeLocal objects.
- [x] Make host-only MP explicit by allowing `remoteStorage` to be absent.
- [x] Remove `LMCacheServer` from the canonical MP provider matrix while
      retaining the legacy enum for topology-less repository objects until
      Phase 7.
- [x] Define structured remote-provider bindings that can grow beyond
      `Binding{Protocol, Endpoint}` to carry credentials, TLS, and adapter
      parameters without stringly typed engine overrides. Typed Redis
      credential/TLS/database fields initially reject unsupported use, so they
      cannot be accepted and silently ignored; Phase 2 enables Secret-backed
      SGLang RESP authentication while retaining explicit TLS/database
      rejections for the pinned adapter.
- [x] Define connector and remote-storage status separately.
- [x] Define how a workload declares or exposes its engine and LMCache-client
      capability/version without allowing CacheBackend to rewrite the engine
      image or making the admission webhook pull arbitrary registry content.
- [x] Define migration/deprecation conditions and Events.

### Compatibility work

- [x] Mark the old flat fields as legacy inputs:
  - `lmCache.hostMemory`;
  - `lmCache.workerImage`;
  - `lmCache.workerPort`;
  - `lmCache.remoteSerde`.
- [x] Prevent new objects from mixing old flat fields with the typed MP
      topology.
- [x] Preserve the current runtime-derived behavior for existing objects until
      Phase 6; do not silently switch existing vLLM IP objects to MP.
- [x] Define old-to-new field mappings in the migration table below.

The runtime owner, not CacheBackend, selects and pins the inference image. Its
validated Pod template declares
`inferencecache.io/lmcache-connector-profile` and
`inferencecache.io/lmcache-client-version`. The image build pipeline must probe
the required connector import/entry point and record the package version before
publishing that declaration. At Pod admission, the selected typed MP adapter
compares the declaration with its required profile and validates observable
engine args/resources; admission never pulls the image or contacts a registry.
Successful mutation also stamps the CacheBackend generation rendered into the
immutable Pod. `ConnectorReady` treats an older generation as unverified until
the inference owner recreates or rolls that Pod; a CacheBackend spec update
cannot retroactively rewrite a running engine Pod.
An absent/mismatched declaration, an adapter that has not implemented the typed
MP contract, or an unclassifiable engine topology is admitted fail-open without
cache mutation and with an actionable diagnostic. CacheBackend never rewrites
the engine image. The concrete profile probes and supported version tuples land
with the Phase 2 renderer and Phase 3/4 runtime adapters.

| Legacy field | Typed MP disposition |
|---|---|
| `lmCache.hostMemory.capacity` | Copy to `lmCache.podLocal.server.l1Capacity`; separately choose explicit server resources with memory headroom. |
| `lmCache.workerImage` | Copy only after pinning it by digest to `lmCache.podLocal.server.image`. |
| `lmCache.workerPort` | Copy to `lmCache.podLocal.server.port` after collision validation. |
| `lmCache.remoteSerde` | No automatic mapping; remove it unless a future typed L3 adapter exposes and validates equivalent semantics. |
| `lmCache.chunkSizeTokens` | Remains `lmCache.chunkSizeTokens`; it is common connector configuration, not topology nesting. |
| `remoteStorage.provider: LMCacheServer` | No automatic provider mapping; explicitly select host-only or a supported L3 such as Redis. |

`lmCache.podLocal.server.maxWorkers` and `resources` are new required choices;
legacy objects do not contain enough information to derive production-safe
values.

### Admission invariants

- [x] Exactly one topology-specific block matches `topology`.
- [x] `PodLocal` rejects `nodeLocal`; `NodeLocal` rejects `podLocal`.
- [x] MP rejects `remoteStorage.provider: LMCacheServer`.
- [x] SGLang and vLLM reject remote providers their selected MP adapter cannot
      render.
- [x] L1 capacity is positive and has a schedulable memory budget with explicit
      headroom.
- [x] Ports are valid and do not collide with known operator-owned MP/event
      ports.
- [x] Version-sensitive or unvalidated parallelism combinations fail loudly at
      the boundary where the topology is observable: CR admission for declared
      fields, and engine-Pod admission for engine args/resources. A combination
      that cannot be classified is not silently injected.
- [x] `remoteSerde` cannot be supplied to MP.
- [x] EventsOnly cannot carry MP or remote-storage configuration that will not
      be used.

### Tests

- [x] CRD schema/defaulting unit tests.
- [x] Validating webhook table tests for every topology/provider combination.
- [x] Envtest CREATE/UPDATE compatibility tests.
- [x] Round-trip/deep-copy tests for all new typed fields.
- [x] Status serialization tests. Condition transitions land with the Phase 2
      status writer because Phase 1 intentionally changes no data plane.

### Exit criteria

- New PodLocal MP objects admit with or without Redis.
- Every impossible combination is rejected at admission.
- Existing IP objects still reconcile unchanged during the compatibility
  period.
- Every MP field has an identified renderer/status consumer, and typed MP
  objects cannot fall through to a legacy runtime adapter while those Phase 2-4
  consumers are landing.

## Phase 2 — engine-neutral PodLocal MP server renderer

### Goal

Turn the existing SGLang-specific spike into shared infrastructure before vLLM
depends on it.

### Refactoring work

- [x] Introduce an engine-neutral internal MP server configuration model.
- [x] Extract native-sidecar, config-volume, `/dev/shm`, resources, probes,
      security context, and L3 adapter rendering from
      `sglang_lmcache_wire.go`.
- [x] Keep engine launch surfaces separate:
  - SGLang config file and `--enable-lmcache`;
  - vLLM `LMCacheMPConnector` JSON and deterministic hash settings.
- [x] Preserve atomic and idempotent Pod mutation.
- [x] Preserve reserved-name and mount-collision checks.

### Runtime work

- [x] Replace `python3 -m lmcache.v1.multiprocess.server` with the supported
      `lmcache server` entry point for the pinned LMCache version.
- [x] Add HTTP startup, readiness, and liveness probes.
- [x] Expose/scrape Prometheus metrics.
- [x] Add typed worker-pool sizing (`maxWorkers` initially; split GPU/CPU pools
      when required by the pinned version and test matrix).
- [x] Add explicit CPU, memory, and optional ephemeral-storage resources.
- [x] Stop defaulting the MP sidecar to the engine image. Select the
      CacheBackend-owned standalone server image by digest without modifying the
      engine container image.
- [x] Let each runtime adapter declare its required engine-side connector
      capability and supported client/server profiles. Surface an explicit
      warning/condition when the observed runtime cannot be verified.
- [x] Render the Redis features supported by the pinned RESP adapter through
      structured binding: Secret-backed authentication is wired on both ends;
      unsupported TLS/database fields remain rejected instead of being silently
      ignored.

The exact LMCache 0.5.3 source constrains these runtime capability boundaries:

- its `resp` adapter supports username/password (rendered from `SecretKeyRef`;
  managed Redis supports the default user plus password), but it does not
  support TLS or logical database selection. Admission therefore keeps
  TLS/database rejected instead of accepting inert configuration. A future
  validated Valkey adapter/image profile is required before those fields can be
  used;
- `lmcache server` disables the separate Prometheus listener because its
  FastAPI HTTP frontend already registers `/metrics` on `--http-port` (8080).
  The renderer exposes the named `lmcache-http` port, successful typed PodLocal
  injection stamps a stable metrics label, and the optional observability
  overlay ships a cross-namespace `PodMonitor` for that label and route.

### Lifecycle work

- [x] Add MP native-sidecar health observation from
      `status.initContainerStatuses`.
- [x] Stop treating every managed provider restart as an engine-restart event.
- [x] Introduce capability-specific restart behavior for:
  - MP server restart;
  - Redis L3 restart;
  - legacy `lm://` restart during the compatibility window.
- [x] Define the Phase 2 recovery boundary: report a native-sidecar outage
      through `ConnectorReady` and rely on kubelet liveness restart. Phase 3
      GPU-validates whether the pinned SGLang connector re-registers without an
      engine restart.

### Tests

- [x] Renderer unit tests independent of SGLang.
- [x] Golden Pod tests for resources, probes, security, mounts, and L3 args.
- [x] Re-injection/idempotence tests.
- [x] Foreign volume/container collision tests.
- [x] Kubernetes 1.31 envtest admission smoke for native-sidecar fields.
- [x] Connector/remote-storage status condition-transition tests.
- [x] Pinned LMCache 0.5.3 standalone-image smoke: the exact Phase 0 digest
      starts `lmcache server` through its CPU fallback, `/healthcheck` returns
      healthy, and `/metrics` returns Prometheus text on HTTP port 8080.

### Exit criteria

- SGLang uses the common renderer with no data-plane regression.
- The server exposes a real health endpoint and metrics.
- The controller can distinguish MP server failure from engine failure and
  remote-L3 failure.
- No Redis restart causes an unconditional engine-fleet rollout.

## Phase 3 — SGLang PodLocal MP production baseline

### Goal

Use the already working SGLang path to validate the common MP server under
parallelism and failure before adding vLLM.

Current state: the non-GPU control-plane and connector compatibility baseline
is complete. No Phase 3 GPU data-path or runtime-failure result is claimed yet.

### Completed non-GPU validation

- [x] Add typed host-only, managed-Redis, and external-Redis SGLang PodLocal
      samples; all three pass CRD defaulting and admission.
- [x] Add the connector-ready SGLang fixture Dockerfile under
      `test/fixtures/sglang-lmcache`, based on the pinned SGLang digest with
      exactly `lmcache==0.5.3`.
- [x] Build the fixture locally for linux/amd64 and verify SGLang
      `0.5.13.post1`, CUDA 13.0.1, LMCache 0.5.3, `LMCacheMPConnector` import,
      and CLI parsing for LMCache, explicit page size, and TP=2 flags. This is
      compatibility preflight only; the image has not run inference.
- [x] Require an explicit SGLang `--page-size` and reject missing, malformed,
      duplicate, non-positive, and declared chunk-incompatible values before
      rendering the MP wire. LMCache retains its authoritative runtime check
      against the effective page size.
- [x] Verify the Pod webhook renders the common MP server atomically and admits
      an incompatible engine unchanged with an actionable fail-open diagnostic.
- [x] Persist a typed SGLang Pod through an envtest kube-apiserver/etcd and
      verify the native-sidecar schema/defaulting surface.
- [x] Install the controller and webhooks in a Kubernetes 1.32 kind cluster,
      create a matching SGLang Pod through the live mutating webhook, read the
      persisted injected Pod back from the API server, and verify image,
      restart policy, probes, resources, MP arguments, engine arguments,
      annotations, labels, and shared mounts. The Pod was deliberately left
      unscheduled, so no engine or sidecar container ran.
- [x] Pass the repository regression gates: full Go tests, focused envtest,
      sample verification (24 pass, 2 intentional skips), default-install
      smoke, Go vet, Prometheus rules, docs sync, REUSE, and DCO.

### GPU/runtime functional scope

- [ ] SGLang + PodLocal + no remote L3. Typed sample and admission wire are
      complete; GPU KV execution is pending.
- [ ] SGLang + PodLocal + managed Redis development profile. Typed sample,
      managed workload rendering, and admission pass; GPU KV execution is
      pending.
- [ ] SGLang + PodLocal + external Redis production profile. Typed sample and
      credential binding admission pass; GPU KV execution is pending.
- [x] ReadWrite role; reject unsupported role splits.
- [ ] Pinned SGLang/LMCache/CUDA image tuple. Local build and compatibility
      preflight pass; registry digest and GPU execution are pending.

### Correctness work

- [ ] Exercise LMCache's runtime chunk-size check against the effective SGLang
      page size on the pinned GPU tuple. The explicit-value admission guard and
      its edge-case tests are complete.
- [ ] Validate TP=1 and TP=2 at minimum.
- [ ] Prove store → engine-GPU flush → retrieve from MP L1.
- [ ] Prove cross-Pod store/retrieve through Redis with fresh engine and MP L1.
- [ ] Verify event hash-domain separation and routing behavior remain correct.
- [ ] Verify cache eviction cannot create an indefinitely silent stale-affinity
      signal without an observable metric/condition.

### Failure work

- [ ] Kill the MP server process and verify the selected recovery policy.
- [ ] Hang the MP server and verify liveness recovery.
- [ ] Restart the Pod-local native sidecar without replacing the engine process.
- [ ] Stop, restart, and replace Redis.
- [ ] Exhaust or nearly exhaust MP L1 memory and verify bounded eviction rather
      than node OOM.
- [ ] Verify fail-open behavior with runtime-native evidence.

### Operability work

- [ ] `ConnectorReady` reflects MP server health. Condition-transition tests
      pass; live SGLang failure evidence is pending.
- [ ] `RemoteStorageReady` reflects Redis independently. Condition-transition
      tests pass; live Redis failure evidence is pending.
- [ ] Metrics prove lookup/store/retrieve/hit behavior. Metrics discovery and
      Pod labeling pass; real KV traffic evidence is pending.
- [ ] Logs identify engine Pod, backend, model, MP instance, and L3 adapter
      without exposing credentials.
- [x] Default-install smoke creates a matching SGLang engine Pod through the
      live webhook and inspects the actual injected wire.

### Exit criteria

- All required SGLang GPU and failure tests pass on the pinned tuple.
- A restarted or unhealthy MP server cannot leave a Ready engine silently
  caching nothing indefinitely.
- Redis loss degrades to the documented local behavior without unnecessary
  engine rollout.
- The SGLang sample and design document match the implementation.

## Phase 4 — vLLM PodLocal MP

### Goal

Provide the complete replacement for the current vLLM IP path before any IP
consumer is forced to migrate.

### Engine wire

- [ ] Add a dedicated vLLM MP adapter; do not mutate the legacy adapter in place.
- [ ] Render `LMCacheMPConnector` with:
  - `kv_connector_module_path` selecting the pinned implementation required by
    D11;
  - `kv_role` derived from `integration.role`;
  - `lmcache.mp.host=127.0.0.1`;
  - the configured MP port;
  - validated MQ timeout/heartbeat settings when exposed;
  - runtime-native load-failure recompute/fail-open behavior.
- [ ] Preserve `PYTHONHASHSEED=0` across scheduler and worker processes.
- [ ] Reserve only correctness-critical args/env owned by the adapter.
- [ ] Reject hybrid/parallelism combinations not supported by the pinned
      vLLM/LMCache tuple.

### Functional scope

- [ ] vLLM + PodLocal + no remote L3.
- [ ] vLLM + PodLocal + managed Redis development profile.
- [ ] vLLM + PodLocal + external Redis production profile.
- [ ] ReadOnly, WriteOnly, and ReadWrite roles.
- [ ] TP=1 and TP=2; TP=4 before recommending the topology for common multi-GPU
      production workloads.
- [ ] Multi-server, DP + multi-server, and unsupported PP/MLA combinations are
      rejected, not silently attempted.

### Correctness and failure tests

- [ ] Store → GPU flush → retrieve from Pod-local MP L1.
- [ ] Cross-Pod retrieve through Redis.
- [ ] TP hash determinism with a negative test showing the zero-hit failure when
      `PYTHONHASHSEED` is not pinned.
- [ ] MP server crash/restart/re-registration.
- [ ] MP server hang/liveness recovery.
- [ ] Redis loss and recovery.
- [ ] Engine rollout while MP/L3 data remains available as designed.
- [ ] Version-skew negative test.

### Exit criteria

- vLLM MP passes the required GPU matrix below.
- The replacement provides a documented migration path for host-only IP and
  centralized `lm://` users.
- MP becomes the recommended path in samples and operator docs. Legacy IP stays
  implementation-only until repository migration and is then removed directly;
  Phase 6 applies only if a legacy consumer appears before removal.

## Phase 5 — migration tooling and consumer migration

### Goal

Make every legacy object's semantic change explicit. No automated migration may
silently remove cross-Pod cache sharing or select a different remote L3.

### Conditional tooling

Phase 0 found no external consumers or installed legacy objects, so migration
tooling is not a default deliverable. Build the following only if that fact
changes before removal:

- [ ] Add a read-only inventory/doctor command that classifies every legacy
      `CacheBackend` and prints its migration class.
- [ ] Add a dry-run manifest migration command or documented deterministic
      transformation.
- [ ] Report fields that cannot be mapped automatically.
- [ ] Emit `LegacyInProcessDeprecated` status/Events for remaining legacy
      objects.
- [ ] Provide rollback instructions during the compatibility window.

### Migration classes

| Existing object | Automatic portion | Required operator choice |
|---|---|---|
| SGLang MP with flat worker fields | Move image, port, and host-memory capacity into `lmCache.podLocal.server` and set `lmCache.topology: PodLocal`. | Confirm pinned image/resources and supported Kubernetes version. |
| vLLM IP host-only | Move host-memory capacity to PodLocal MP L1; select vLLM MP wire. | Confirm sidecar resources and accept the process/topology change. |
| vLLM IP + managed/external LMCacheServer | Preserve local capacity; remove `lm://`. | Select no L3 and lose cross-Pod sharing, or explicitly select a supported Redis/other L3. Never choose automatically. |
| vLLM IP + existing engine-side Mooncake provider | Preserve local intent only. | Wait for MP + Mooncake Store L3 support or migrate explicitly to Redis; URL config is not equivalent to MP adapter config. |
| Any IP object with `remoteSerde` | None. | Remove it or map it to a future typed L3 serde only when that adapter supports and validates the same semantics. |

### Repository migration

- [ ] Convert every canonical sample to MP.
- [ ] Convert reference-stack manifests.
- [ ] Replace IP documentation and screenshots.
- [ ] Replace IP unit/integration fixtures where they are not explicitly testing
      the transition or a conditional Phase 6 compatibility window.
- [ ] Update support tables and CLI output.
- [ ] Remove language that calls the legacy LMCache server a CPU profile or the
      default LMCache backend.

### Exit criteria

- Every repository-owned LMCache workload uses MP.
- If any external legacy object appears, it has an owner and migration
  disposition.
- If migration tooling becomes necessary, it reports zero unknown/unclassified
  legacy shapes.
- No migration silently changes cross-Pod sharing behavior.

## Phase 6 — reject new IP objects

**Conditional:** Phase 0 found no external consumers or installed legacy
objects. Skip this phase and proceed from Phase 5 to Phase 7 if that remains true.
Activate it in full if a legacy consumer or object appears before removal.

### Goal

Stop growth of the legacy population while allowing controlled migration or
deletion of existing objects.

### Admission policy

- [ ] Reject creation of vLLM LMCache objects without the MP block.
- [ ] Reject creation of `remoteStorage.provider: LMCacheServer`.
- [ ] Reject reintroduction of removed legacy fields.
- [ ] Grandfather existing IP objects only for:
  - read/status;
  - deletion;
  - updates required to migrate to MP.
- [ ] Reject updates that scale out, materially retune, or otherwise extend the
      lifetime/scope of a legacy IP deployment.

### Operational gates

- [ ] CLI/doctor reports remaining legacy object count.
- [ ] Release notes state the physical-removal target release.
- [ ] Warning Events link to migration documentation.
- [ ] A defined observation window passes with zero newly created IP objects.

### Exit criteria

- No supported API path can create a new IP data plane.
- Remaining legacy objects are zero, or each has an approved time-bounded
  exception.
- MP error rate, hit behavior, and recovery behavior meet the agreed production
  baseline.

## Phase 7 — remove IP and the legacy LMCache server

### Goal

Delete the project-deprecated data plane and all code that exists solely to
operate it.

### Code removal

- [ ] Remove the vLLM legacy LMCache adapter.
- [ ] Remove `LMCacheConnectorV1` rendering.
- [ ] Remove `LMCACHE_REMOTE_URL`, `LMCACHE_REMOTE_SERDE`, and other IP-only
      injected settings.
- [ ] Remove `ProtocolLMCache` and the `lm://` endpoint parser/binding.
- [ ] Remove the managed and external `LMCacheServer` provider surface.
- [ ] Remove the standalone LMCache-server workload renderer.
- [ ] Remove the server-instance restart cascade if no remaining provider needs
      it; otherwise narrow and rename it to the actual capability.
- [ ] Remove IP-only status fields, metrics, Events, samples, and tests.
- [ ] Remove compatibility defaulting/validation and migration-only code after
      the supported migration window closes.

### API cleanup

- [ ] Remove legacy flat LMCache fields after their replacement is complete.
- [ ] Remove `LMCacheServer` from CRD enums and provider-specific schema.
- [ ] Remove or relocate top-level managed-provider workload fields according to
      the Phase 0 decision.
- [ ] Regenerate CRDs, deepcopy code, examples, and reference documentation.

### Verification

- [ ] `go test ./...` passes.
- [ ] `make verify-samples` passes.
- [ ] Default-install and upgrade smoke pass.
- [ ] Repository search finds no production-code references to:
  - `LMCacheConnectorV1`;
  - `LMCACHE_REMOTE_URL`;
  - `ProtocolLMCache`;
  - `lm://`;
  - the managed `LMCacheServer` provider.
- [ ] Migration documentation may retain historical references clearly marked as
      removed.

### Exit criteria

- Only MP adapters can be selected for `spec.type: LMCache`.
- No controller workload or engine wire implements IP.
- No new or stored object requires the legacy schema to reconcile.

## Phase 8 — NodeLocal shared MP servers

### Goal

Add the higher-efficiency topology in which multiple engine Pods of one
`CacheBackend` share a node-local MP server without weakening placement,
isolation, or status correctness.

### Controller work

- [ ] Reconcile one DaemonSet per NodeLocal `CacheBackend`.
- [ ] Restrict it to intended GPU/engine nodes through typed scheduling fields.
- [ ] Configure host networking and host shared memory according to the pinned
      upstream deployment contract.
- [ ] Declare host ports so Kubernetes scheduling exposes conflicts.
- [ ] Authenticate ownership by CacheBackend name and UID.
- [ ] Compute desired/ready servers and engine-node coverage.
- [ ] Handle engine scheduling before the node-local server is ready without
      starting an engine against a missing required MP endpoint.

### Engine injection

- [ ] Derive the node-local address from the engine Pod's node/host IP through a
      Downward API field or another deterministic node-scoped mechanism.
- [ ] Do not use a load-balanced ClusterIP as the CUDA MP endpoint.
- [ ] Keep SGLang and vLLM launch surfaces engine-specific.
- [ ] Validate the server's global chunk size and version against every selected
      engine Pod.

### Isolation and resource work

- [ ] Define port-conflict behavior for multiple NodeLocal CacheBackends on one
      node.
- [ ] Restrict the first implementation to one trust/tenant domain per
      CacheBackend server pool.
- [ ] Document that L1 capacity is per node and shared by selected engine Pods.
- [ ] Size `maxGPUWorkers` for the number of engine instances sharing a server.
- [ ] Add NetworkPolicy/firewall guidance where host networking permits it.
- [ ] Assess the security impact of exposing all node GPUs to the MP server.

### Validation

- [ ] One engine Pod on one node.
- [ ] Multiple engine Pods sharing one node-local server.
- [ ] Engines spread across multiple nodes, each using only its local server.
- [ ] DaemonSet rollout and single-node server restart.
- [ ] Node drain and engine rescheduling.
- [ ] Host-port conflict negative test.
- [ ] Redis outage/recovery with multiple node-local servers.
- [ ] No cross-node attempt to use CUDA IPC.

### Exit criteria

- Every selected engine Pod is covered by exactly one healthy local MP server.
- No generic Service load balancing can route an engine to another node's MP
  server.
- Shared L1 behavior, resource accounting, and failure blast radius are measured
  and documented.
- Cross-`CacheBackend` pool sharing remains rejected until a separate resource
  and tenancy model is approved.

## Required GPU validation matrix

The matrix grows by phase. A cell is complete only when it proves a cache hit
after clearing or replacing the engine GPU cache; successful process startup is
not sufficient.

| Runtime | Topology | Remote L3 | Parallelism | Required by |
|---|---|---|---|---|
| SGLang | PodLocal | none | TP=1 | Phase 3 |
| SGLang | PodLocal | Redis | TP=1 | Phase 3 |
| SGLang | PodLocal | Redis | TP=2 | Phase 3 |
| vLLM | PodLocal | none | TP=1 | Phase 4 |
| vLLM | PodLocal | Redis | TP=1 | Phase 4 |
| vLLM | PodLocal | Redis | TP=2 | Phase 4 |
| vLLM | PodLocal | Redis | TP=4 | Before production recommendation for common multi-GPU workloads |
| SGLang | NodeLocal | Redis | multiple engine Pods | Phase 8 |
| vLLM | NodeLocal | Redis | multiple engine Pods | Phase 8 |

Every required data test records:

- exact image digests and LMCache version;
- Kubernetes, driver, CUDA, and GPU model;
- engine args and effective MP server config;
- first-request store evidence;
- GPU-cache clear or fresh-engine proof;
- second-request retrieve/hit evidence;
- MP and L3 metrics before and after;
- failure/recovery timestamps where applicable.

## Test pyramid

| Layer | Required evidence |
|---|---|
| API/unit | schema, defaulting, validation, provider matrix, deep copy, status transitions |
| Renderer/unit | exact args/env/config, resources, probes, security, volumes, idempotence, collision rejection |
| Envtest | real CREATE/UPDATE admission and status persistence; legacy grandfathering only if Phase 6 activates |
| Kubernetes smoke | live webhook injection into matching engine Pods, native-sidecar schema support, controller-owned workload shape |
| GPU functional | store/flush/retrieve and cross-Pod L3 reuse |
| GPU fault | MP crash/hang/restart, Redis loss/recovery, engine rollout, node drain for NodeLocal |
| Upgrade/migration | Repository manifest conversion by default; old-object inventory, dry-run conversion, grandfather rules, and rollback only if Phase 6 activates |

## Security, reliability, scalability, and cost gates

### Security

- [ ] No production managed Redis profile is exposed without an explicit network
      isolation and credential/TLS posture.
- [ ] Secrets are referenced, not embedded in CR status, Pod args visible to all
      readers, logs, or Events.
- [ ] PodLocal and NodeLocal GPU visibility is documented and reviewed for the
      target tenancy model.
- [ ] NodeLocal host networking/IPC is an explicit operator choice.
- [ ] Cross-namespace remote endpoints retain explicit opt-in validation.

### Reliability

- [ ] MP server health affects connector status.
- [ ] A hung process is detected, not just an exited process.
- [ ] Recovery cannot leave the engine Ready with permanently disabled caching.
- [ ] Remote L3 loss follows tested fail-open/fail-closed behavior.
- [ ] Restart actions are scoped to the failing component's capability.

### Scalability and latency

- [ ] PodLocal memory cost is reported per engine Pod.
- [ ] NodeLocal memory cost is reported per node.
- [ ] Worker pool sizing is tested under the expected engine count and TP shape.
- [ ] Remote L3 concurrency and connection limits are bounded.
- [ ] Routing/index signals can be correlated with actual LMCache hit metrics.

### Operability

- [ ] Status distinguishes connector, MP server, engine, and remote L3 health.
- [ ] Metrics expose server availability, L1/L3 store/retrieve/hit, capacity,
      eviction, and recovery.
- [ ] Events contain an actionable recovery or migration instruction.
- [ ] Samples never depend on an implicit runtime-selected connector mode.

## Risk register

| Risk | Impact | Mitigation / gate |
|---|---|---|
| A legacy consumer appears after Phase 0 | Breaking removal | Reconfirm before removal; activate Phase 6 and a grandfather period when non-zero. |
| MP client/server version skew | Permanent unhealthy or protocol failure | Same-image default for PodLocal, pinned matrix, skew-negative tests. |
| Worker restart does not re-register engine state | Ready engine silently misses forever | Runtime-specific recovery test and condition before Phase 3/4 exit. |
| Redis restart rolls all engines | Availability blast radius | Capability-specific restart policy in Phase 2. |
| `failOpen` is only a custom env | Contract not enforced | Render native runtime policy and fault-test it. |
| Sidecar sees all node GPUs | Isolation exposure | Document/review tenant model; prefer dedicated nodes where required. |
| PodLocal duplicates CPU L2 | Memory cost per replica | Explicit per-Pod capacity; NodeLocal follow-up. |
| NodeLocal port collision | DaemonSet Pods fail or bind incorrectly | Declared host port, typed port, controller condition, negative tests. |
| NodeLocal engine reaches remote node | CUDA IPC failure | Node-derived endpoint; reject load-balanced service topology. |
| Existing engine-side Mooncake config is treated as MP-equivalent | Admission succeeds but adapter cannot start | No automatic migration; separate MP + Mooncake Store implementation. |
| Index says warm while MP/L3 evicted data | Routing quality degrades silently | Correlate cache events with LMCache metrics/health; define stale-entry behavior. |

## Phase tracking template

Each implementation PR updates the delivery table and its phase checklist in the
same change. A phase is not marked complete merely because code merged.

```markdown
### Phase N status update — YYYY-MM-DD

- Status: not started | in progress | blocked | complete
- Owner:
- Tracking issue:
- PRs:
- Validation artifacts:
- Remaining exit criteria:
- Decision changes:
```

## Overall definition of done

The migration is complete only when all of the following are true:

- [ ] `spec.type: LMCache` selects only MP implementations.
- [ ] Both SGLang and vLLM pass the required PodLocal GPU matrix.
- [ ] Host-only and Redis-backed MP are supported for both engines.
- [ ] MP server health, failure, and recovery are observable and tested.
- [ ] `remoteStorage` is optional L3 and no longer contains LMCacheServer.
- [ ] No production code injects `LMCacheConnectorV1`, `lm://`, or
      `LMCACHE_REMOTE_URL`.
- [ ] No generic managed-provider restart automatically rolls MP engines.
- [ ] Every old IP object has been migrated or intentionally deleted.
- [ ] Canonical samples, reference manifests, CLI output, and design documents
      describe only the implemented MP behavior.
- [ ] NodeLocal, if enabled, guarantees same-node server selection and accurate
      engine coverage; otherwise it remains rejected rather than partially
      accepted.

## Upstream references

- [LMCache v0.5.3 release](https://github.com/LMCache/LMCache/releases/tag/v0.5.3)
- [LMCache MP overview](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/mp/index.rst)
- [LMCache MP deployment guide](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/mp/deployment.rst)
- [LMCache MP configuration](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/mp/configuration.rst)
- [LMCache MP supported storage adapters](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/mp/l2_storage/supported_storages.rst)
- [LMCache MP Mooncake Store adapter](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/mp/l2_storage/mooncake_store.rst)
- [Legacy IP centralized sharing](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/getting_started/quickstart/share_kv_cache.rst)
- [Official vLLM MP Kubernetes example](https://github.com/LMCache/LMCache/blob/v0.5.3/examples/multi_process/vllm-deployment.yaml)
- [MP worker liveness design](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/design/v1/multiprocess/worker_liveness.md)
- [LMCache standalone image](https://github.com/LMCache/LMCache/blob/v0.5.3/docker/README.md#2-dockerfilestandalone---lmcache-only)
- [LMCache v0.5.3 vLLM MP connector](https://github.com/LMCache/LMCache/blob/v0.5.3/lmcache/integration/vllm/lmcache_mp_connector.py)
- [LMCache v0.5.3 SGLang MP adapter](https://github.com/LMCache/LMCache/blob/v0.5.3/lmcache/integration/sglang/multi_process_adapter.py)
- [vLLM v0.26.0 connector registry](https://github.com/vllm-project/vllm/blob/v0.26.0/vllm/distributed/kv_transfer/kv_connector/factory.py)
- [vLLM v0.26.0 built-in LMCache MP connector](https://github.com/vllm-project/vllm/blob/v0.26.0/vllm/distributed/kv_transfer/kv_connector/v1/lmcache_mp_connector.py)
- [vLLM v0.26.0 Docker connector-dependency switch](https://github.com/vllm-project/vllm/blob/v0.26.0/docker/Dockerfile)
- [SGLang v0.5.13.post1 LMCache integration](https://github.com/sgl-project/sglang/blob/v0.5.13.post1/python/sglang/srt/mem_cache/storage/lmcache/README.md)
- [SGLang v0.5.13.post1 LMCache cache implementation](https://github.com/sgl-project/sglang/blob/v0.5.13.post1/python/sglang/srt/mem_cache/storage/lmcache/lmc_radix_cache.py)
