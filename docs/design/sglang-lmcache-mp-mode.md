# Design: LMCache MP mode — the converged worker model (SGLang now, vLLM migration)

Status: design accepted; implementation pending (Phase 2–3, SGLang). Validated live facts vs. open Phase-2 spikes are marked inline. · Supersedes the "mirror the vLLM+LMCache adapter" model in [cachebackend-api.md](cachebackend-api.md) SGLang section · Adapters: `pkg/adapters/runtime/sglang`, `pkg/adapters/runtime` (vLLM)

**LMCache upstream now recommends multiprocess (MP) mode for *both* vLLM and
SGLang** (its quickstart: MP is *"recommended"* for vLLM via `LMCacheMPConnector`,
and *"the SGLang integration now defaults to MP mode"*). MP mode is a **node-local
`lmcache` worker** the engine attaches to over ZMQ + shared memory — not the
`lm://` standalone-remote-server model. This doc adopts MP as the **converged
worker model for both engines** and specifies the shared infrastructure (a
node-local worker + config-file wire + a shared L2 store) that carries it.

The shipped `(sglang, LMCache)` adapter was built by analogy to the vLLM `lm://`
model, and **live GPU validation showed that is wrong for SGLang** — SGLang has no
`lm://` path at all, only MP. So SGLang is the first concrete implementation of the
converged model and the driver for this design; the vLLM migration reuses the same
infrastructure and is future work (see [Support matrix](#converged-foundation-mp-for-both-engines)).

An advisory admission warning already ships for the SGLang pair (the validator's
`sglangLMCacheDataPlaneWarning`) so operators are told the shipped data plane is
non-functional (verified — it caches nothing); this design is what replaces the
warning with a working data plane.

## TL;DR

- Both engines can drive LMCache in **multiprocess (MP) mode** (upstream-
  recommended): the engine attaches to a **node-local `lmcache` worker** over ZMQ
  (`mp_host`/`mp_port`) + a shared-memory data path. SGLang configures it via a
  **`--lmcache-config-file`** (the injected remote-connection/tuning `LMCACHE_*`
  env is ignored; `LMCACHE_USE_EXPERIMENTAL` gates the connector); vLLM via
  `LMCacheMPConnector` + `kv_connector_extra_config`. **SGLang is MP-only** (no
  `lm://` path exists); vLLM keeps its `lm://` path too — see the support matrix
  below.
- Cross-node KV sharing is a **networked L2 store behind the MP worker**
  (`--l2-adapter` = `resp`/Redis, `s3`, `mooncake_store`, or `p2p`) — **not** the
  `lm://` server, which is not even a valid MP `--l2-adapter` type.
- The design fits the existing `KVCacheRuntimeAdapter` interface with **no new
  methods**: `ResolveCacheServer` renders the **shared L2 (Redis)**;
  `InjectEngineConfig` adds a **config-file init container + a node-local MP worker
  + the engine wire** to the engine pod. `mp_host=127.0.0.1` (worker co-located in
  the pod), so — unlike Mooncake — the engine needs **no `hostNetwork`**. One
  packaging detail is still open (a **GPU-less sidecar** vs. the proven
  **single-container** form) pending a Phase-2 spike; the interface mapping and the
  L2/config-file decisions hold either way.

## Converged foundation: MP for both engines

MP mode is the model both engines share; this design builds that foundation once,
and each engine attaches through its own launch surface.

| Engine | LMCache modes | Recommended (operator docs) | This design implements |
|---|---|---|---|
| **SGLang** | **MP only** — `LMCacheMPConnector` via `--lmcache-config-file`; no `lm://` client exists | MP (the only option) | **Yes — Phases 1–3** |
| **vLLM** | `lm://` (shipped: `LMCacheConnectorV1` + `LMCACHE_REMOTE_URL`) **and** MP (`LMCacheMPConnector` + `mp.host`/`mp.port`) | **MP** | vLLM MP is a **future migration** reusing this infra; the `lm://` adapter stays supported |

Policy this locks in:

- **SGLang: MP-only.** No `lm://` client exists for SGLang, so the adapter supports
  MP exclusively (this design).
- **vLLM: both, MP recommended.** The existing `lm://` vLLM+LMCache adapter stays
  supported (validated and shipped — operators on it are not broken). A future vLLM
  MP adapter reuses the *same* worker + config/extra-config wire + shared-L2 store
  this design builds; operator-facing docs **recommend MP** for vLLM once it lands,
  matching upstream.
- **Shared, engine-agnostic infrastructure.** The node-local worker, the
  config-file / `kv_connector_extra_config` wire, the shared L2
  (Redis / `--l2-adapter`), and the `/dev/shm` + fail-open handling are not
  SGLang-specific — SGLang is just the first consumer. Keeping them engine-neutral
  is what makes the vLLM migration a wiring change, not a rebuild.

Everything below specifies the SGLang implementation concretely; the engine-agnostic
pieces are flagged so the vLLM migration inherits them.

## Background: how SGLang+LMCache actually works

Validated live on a GPU cluster (H100/A100, `lmsysorg/sglang:nightly-dev-cu13` +
in-pod `pip install lmcache`→0.5.1, Llama-3.1-8B). The load-bearing facts, with
the observed engine/worker log signals reproduced inline:

1. **MP mode is hardcoded.** `lmc_radix_cache.py.__init__` sets
   `self._mode = LMCacheMode.MP`; the in-process (`IP`) path that would read a
   remote URL directly is reachable only by editing SGLang source, so it is not
   operator-usable. `--enable-lmcache` ⇒ MP mode.
2. **Config comes from a file, not env.** MP mode requires
   `--lmcache-config-file <yaml>`; with no file the engine aborts at startup
   (`ValueError: MP mode requires ... mp_host / mp_port`). The `LMCACHE_*` env the
   current adapter injects (`LMCACHE_REMOTE_URL`, serde, chunk size, local-CPU) is
   **not read** on this path — `LMCACHE_USE_EXPERIMENTAL=True` is the one env that
   still matters (it gates the connector).
3. **The engine dials a separate node-local worker.** The config yaml carries
   `mp_host`/`mp_port` (ZMQ, default `:5555`); the engine connects to an
   already-running `python -m lmcache.v1.multiprocess.server` process — it does
   **not** spawn it. KV bytes move over the shared-memory/CUDA-IPC data path, not
   over that ZMQ socket (which is control-only).
4. **The worker owns the tiers.** L1 is the worker's local (CPU/host) cache; the
   cross-node/shared tier is the worker's **`--l2-adapter`**. Pointing SGLang at a
   bare `lm://` URL does not offload anywhere useful — `lm://` is not an
   `--l2-adapter` type.

### Live proof — node-local caching

Single pod, MP worker + engine co-located **in one GPU-bearing container** (this
validates MP-mode caching over the CUDA-IPC data path — not yet the separate
GPU-less sidecar packaging; see the open question below): request → worker
`Stored 3584 tokens`; `POST /flush_cache` (clears the engine's GPU radix cache) →
re-request → worker `Retrieved 3584 tokens`, engine `#cached-token: 3584`. Full
store→load cycle, engine serves (no hang, no abort).

### Live proof — cross-node sharing (the shared L2)

Two **independent** engine pods, each with its own MP worker and its own L1, both
with `--l2-adapter '{"type":"resp","host":"redis","port":6379}'` pointed at one
shared Redis over the cluster network:

| Step | Observed |
|---|---|
| Engine A stores (3760-token prompt) | worker-A `Stored 3584 tokens`; Redis `DBSIZE` 0→14 **immediately** (write-through, 14 chunks × 256) |
| Engine B, same prompt, **fresh L1 + fresh GPU cache** | worker-B `Prefetch (L1+L2): 14/14 retained keys (0 L1, 14 L2)` → `Retrieved 3584 tokens`; engine B `#new-token: 176, #cached-token: 3584` |

`0 L1, 14 L2` is decisive: engine B's local L1 held nothing, so every chunk was
served from the shared Redis L2 over the network — cross-instance KV reuse works.
No `PYTHONHASHSEED` pinning was needed (SGLang keys prefixes with `hashlib.sha256`
over token-id bytes, deterministic across processes — unlike vLLM's randomized
builtin-`hash()` seed, the reason the vLLM/Mooncake path pins `PYTHONHASHSEED=0`).

**`--l2-adapter` supported types** (MP server `0.5.1`): `aerospike`, `dax`, `fs`,
`fs_native`, `hfbucket`, `mock`, `mooncake_store`, `nixl_store`,
`nixl_store_dynamic`, `p2p`, `raw_block`, `resp`, `s3` (plus `fault_inject`,
`native_plugin`, `plugin`). **No `lm://`/`redis`/`lmcache` type** — the shared
tier is `resp` (Redis; simplest, proven), `s3`, `mooncake_store` (the RDMA path),
or `p2p` (peer discovery). `resp` config schema (`RESPL2AdapterConfig`):
`{"type":"resp","host":<str>,"port":<int>,"num_workers":8,"username":"","password":""}`.

## The three pieces, and how they map onto the interface

A working `(sglang, LMCache)` deployment needs three things:

1. **Engine wire** — `--enable-lmcache`, `LMCACHE_USE_EXPERIMENTAL=True`,
   `--lmcache-config-file <path>`; the file carries `mp_host`/`mp_port`/`chunk_size`.
2. **A node-local MP worker** reachable at `mp_host:mp_port`, co-located with the
   engine (shared-memory data path), configured with the shared `--l2-adapter`.
3. **A shared L2 store** (Redis) the worker offloads to, reachable cluster-wide.

The `KVCacheRuntimeAdapter` interface already accommodates all three **without a
new method**:

### `ResolveCacheServer` → the shared L2 (Redis)

The reconciler wraps the returned `(*PodSpec, *Service)` into one Deployment +
Service owned by the CR. For SGLang, that workload becomes the **shared Redis L2**,
with three constraints the design must honor:

- **Pinned image** — a digest/tag tracked in `VERSIONS.md`, consistent with the
  lmcache-server image-pin policy, never a floating `redis` tag.
- **Single replica (enforced).** A plain Redis is not clustered; multiple pods
  behind one Service would shard requests across independent key spaces and
  silently partition the L2. So this backend is **clamped to one replica** and HPA
  is not attached — the same single-instance constraint the `lm://` lmcache-server
  already carries (a multi-replica `CacheBackend` for this type is rejected at
  admission). A genuinely clustered/HA Redis is an operator-provided or future
  option, out of scope for the managed default.
- **Bounded memory.** `--maxmemory-policy allkeys-lru` only evicts once
  `--maxmemory` is set; without it Redis grows until the container is OOM-killed.
  The render derives `--maxmemory` from the pod's memory limit (with headroom),
  falling back to an explicit bounded default — so LRU eviction, not the OOM
  killer, reclaims space.

It listens on ClusterIP `:6379` and `status.endpoint` becomes the Redis Service
DNS. This replaces the `lm://` lmcache-server render (`ResolveLMCacheServer`) for
the SGLang pair only — vLLM keeps `lm://`. Redis is a shared, network-addressable
store that fits the one-Service, engines-anywhere model exactly (unlike Mooncake's
mesh), so **no `hostNetwork` is required for the L2**.

### `InjectEngineConfig` → config-file init container + MP-worker sidecar + engine wire

The mutating Pod webhook already adds volumes, init containers, and sidecar
containers to engine pods. For SGLang it adds, to the engine pod:

- **initContainer `lmcache-config`** — writes `/config/lmcache.yaml`
  (`chunk_size`, `mp_host: 127.0.0.1`, `mp_port: 5555`) into a shared `emptyDir`.
  Runs to completion before the engine, so `--lmcache-config-file` is never read
  before it exists. No ConfigMap needed (the webhook cannot create cluster
  resources; the values are static and small).
- **native sidecar `lmcache-mp-worker`** — runs the upstream-documented worker CLI
  `lmcache server --host 127.0.0.1 --port 5555 --chunk-size <n> --l1-size-gb <n>
  --l2-adapter '{"type":"resp","host":<endpoint-host>,"port":<endpoint-port>}'`
  (the `lmcache server` subcommand is the documented entrypoint; it is the same MP
  server as `python -m lmcache.v1.multiprocess.server`, which validation used).
  `<endpoint>` is the Redis address passed to `InjectEngineConfig`. Its
  **image is pinned and version-aligned with the engine's LMCache connector** —
  the two speak the LMCache MP wire (ZMQ + shared memory), so a version skew
  between worker and engine is a correctness hazard; the tuple is tracked in
  `VERSIONS.md` alongside the engine image (validation used the engine's own
  `pip install lmcache`→0.5.1, so the simplest pin is the same image and `lmcache`
  version for both). Mounts the shared `/dev/shm`
  (`emptyDir{medium: Memory, sizeLimit ≥ l1-size}`) and `/config`. **Startup
  ordering matters** — the engine dials the worker at launch, and K8s does not
  order ordinary containers within a pod. So this is a **native sidecar** (a
  `restartPolicy: Always` entry in `initContainers` — beta and on-by-default since
  K8s 1.29, stable since 1.33) with a `startupProbe`. The ZMQ port binds
  `127.0.0.1`, which a pod-IP-targeted `tcpSocket`/`httpGet` probe cannot reach, so
  the probe is either an **`exec`** loopback check (runs inside the container) or —
  cleaner — an **`httpGet` on the worker's HTTP management endpoint (`:8080`)**,
  which `lmcache server` exposes and which can bind the pod interface; Phase 2
  picks whichever the pinned build supports. Native sidecars start and gate ready
  **before** the main engine container, so the worker is listening when the engine
  connects. (An ordinary sidecar would race the engine.) The adapter's minimum is
  K8s ≥ 1.29. Fail-open interaction is resolved below.
- **engine container** — add `--enable-lmcache`, `--lmcache-config-file
  /config/lmcache.yaml`, `LMCACHE_USE_EXPERIMENTAL=True`,
  `INFERENCECACHE_FAIL_OPEN`; mount the shared `/dev/shm` + `/config`. **Drop** the
  MP-ignored `LMCACHE_REMOTE_URL` / `LMCACHE_REMOTE_SERDE` / `LMCACHE_LOCAL_CPU` /
  `LMCACHE_MAX_LOCAL_CPU_SIZE` env.

`mp_host=127.0.0.1` works because the worker is a **same-pod sidecar** — it shares
the engine's network namespace, so ZMQ over loopback reaches it and the shared
`/dev/shm` `emptyDir` gives the data path. This is the key divergence from
Mooncake: Mooncake needs `hostNetwork` on the engine (its mesh dials real host
IPs on dynamic ports); SGLang's MP worker is in-pod, so the engine stays on the
pod network.

### Fail-open semantics (resolving the startup-gate tension)

`spec.integration.failOpen: true` (the default) requires that a cache outage never
takes down serving — engine-local prefill must proceed when the cache is
unavailable. The native-sidecar gate seems to conflict with this (it makes the
worker a prerequisite for engine startup), but the tension resolves once the two
parts are named for what they are:

- **The shared L2 (Redis) is the "cache" fail-open protects — a remote
  dependency.** So the worker MUST start and keep serving **L1-only when Redis is
  unreachable**, retrying L2 in the background — it must never abort on an
  unreachable `--l2-adapter`, and its `startupProbe` gates on **local readiness
  only** (listening on `mp_port`), never on L2 connectivity. A Redis outage then
  degrades the cache (cross-node reuse pauses; L1 + local prefill continue) but
  never blocks engine startup or serving. This is exactly the fail-open contract,
  honored at the tier that can actually be "unavailable" — and it is the direct
  analog of vLLM+LMCache serving through an unreachable `lm://` server.
- **The MP worker itself is a required, co-scheduled component of the serving
  stack**, not a remote dependency — the out-of-process analog of vLLM's
  *in-process* LMCache connector. It is co-located, auto-restarted
  (`restartPolicy: Always`), and its liveness is part of the engine pod's own
  health. A worker that cannot start at all is a pod-health / `CacheBackend`
  `Degraded` condition, exactly as a broken engine connector would be — the same
  way vLLM does not "fail open" around a connector it failed to load.

> **ACCEPTED CONTRACT DECISION (endorsed by the project owner) — worker failure is a
> documented fail-open boundary of the `(sglang, LMCache)` pair, not a contract
> violation.** Because SGLang's MP worker is a *separate process* (unlike vLLM's
> compiled-in connector), it is a failure mode the vLLM pair does not have: a
> persistently-dead worker takes down engine serving even though the model is
> healthy. This is a **deliberate, bounded trade-off**, acceptable because (a) it is
> *inherent* to MP mode — SGLang has no cacheless fallback while `--enable-lmcache`
> is on, so a strict "serve without the worker" guarantee would need upstream SGLang
> support; (b) the *common* cache failure — the shared/remote L2 (Redis) — still
> fails open, matching the documented "the LMCache connector is fail-open at
> runtime"; and (c) it does not violate an *enforced* invariant (hard `failOpen`
> enforcement at the engine layer is future work per the CRD contract). **Known
> cost:** an MP-mode engine has a strictly worse worst-case availability than the
> *legacy `lm://`* path — the worker is a new in-pod single point of failure, and L1
> mis-sizing (`/dev/shm` OOM) now has an availability consequence, not only a perf
> one. This is a property of **MP mode**, not of SGLang specifically: it is exactly
> what the vLLM MP path would inherit too, and MP is the *upstream-recommended*
> posture — so accepting it aligns with the converged direction rather than taking
> on a SGLang-only wart. **Containment (all in Phase 2):** native sidecar + `restartPolicy:
> Always` self-heals transient worker crashes; an engine **liveness probe** turns a
> persistently-wedged engine into a whole-pod restart (self-healing) rather than a
> silent hang; the `CacheBackend` `Degraded` condition surfaces worker unhealth; and
> operator docs state plainly that for this pair the cache worker is a serving
> component. Not a one-way door — if upstream SGLang adds a cacheless fallback, the
> guarantee upgrades with no redesign.

Net: engine-local prefill proceeds whenever the *shared* cache is unavailable —
which is what the contract requires. This is a deliberate **contract
interpretation** ("cache unavailable" = the remote/shared tier, not the local
serving component), and Phase 2 must *validate* rather than assume it:

- **Load-bearing L2 assumption.** That the lmcache MP server starts and serves
  L1-only when its `--l2-adapter` target is unreachable (retrying L2), rather than
  aborting. If it does **not** support that — nor reconfiguring/attaching L2 later
  — the worker entrypoint supervises it (a restart/backoff loop re-attempting L2);
  and if even that cannot preserve L1-only serving, "L2 required at startup"
  becomes a **documented limitation of the pair**, not a silent breakage. The
  viable mechanism is a Phase-2 finding, not claimed here.
- **Worker crash / restart.** Whether the engine survives a mid-flight worker
  restart (`restartPolicy: Always`) — recomputing during the gap, resuming cache
  use after — is a Phase-2 acceptance test. If SGLang cannot tolerate worker loss
  at all, that is the pair's documented fail-open boundary (the worker is a
  required serving component; not every engine/backend pair offers identical
  guarantees), surfaced via the `CacheBackend` `Degraded` condition.
- **`failOpen: false`.** The operator has promoted the cache to a serving
  dependency, so the conservative behavior is intended: the startup gate is not
  relaxed, and an L2 outage may be treated as a hard error rather than a silent
  degrade — trading availability for the guarantee that served requests used the
  cache.

`INFERENCECACHE_FAIL_OPEN` is injected as today; it is the routing-layer signal.

### Why not a per-node DaemonSet worker?

Upstream's documented MP deploy is a per-node DaemonSet worker (`hostNetwork` +
host `/dev/shm` + `hostIPC`) shared by all engines on the node. That is a heavier
privilege posture and does not fit the per-CacheBackend, engines-anywhere model:
the shared-memory data path (CUDA-IPC / POSIX `/dev/shm`) requires the engine and
worker to share an IPC namespace + `/dev/shm`, which across separate pods means
`hostIPC` + a host-path `/dev/shm` mount on every engine pod. The same-pod
sidecar avoids all of that — one worker per engine pod, isolated, no host
namespaces — at the cost of not sharing an L1 across co-located engines (they
share instead through the L2, which is the cross-node path anyway). The DaemonSet
topology stays available as an operator-provided option for dense multi-engine
nodes, but is not what the managed adapter renders.

## Open question (Phase-2 implementation spike): GPU-less sidecar vs. same container

The one unresolved detail. The MP worker's data path is either **CUDA-IPC**
(the worker maps the engine's GPU KV directly — needs GPU visibility) or **POSIX
`/dev/shm`** (`non_gpu` transfer via `--shm-name` — the engine stages KV to shared
host memory, the worker needs **no GPU**). The node-local proof above ran worker +
engine in **one container** sharing the one GPU allocation, so it exercised the
CUDA-IPC path.

- **If the `non_gpu` `/dev/shm` path works** for a co-located but separate sidecar
  (no GPU request on the sidecar, shared `emptyDir` `/dev/shm`): the sidecar design
  above is clean and cheap — no second GPU, standard pod.
- **If the worker must share the GPU** (CUDA-IPC only): a separate sidecar cannot
  get the engine's specific GPU (device-plugin allocation is per-container), so the
  fallback is worker + engine in **one container** (an entrypoint that launches the
  worker, then `exec`s the engine) — proven, but it wraps the operator's engine
  command and is less K8s-idiomatic.

This is settled empirically before Phase 2 renders the sidecar (live cluster; two
containers in one pod, GPU on the engine only, shared `/dev/shm`, worker in
`non_gpu` mode → does the store→load cycle still pass?). The design is written for
the sidecar outcome; the fallback is a localized change (fold the worker into the
engine container's command) that does not affect the L2 or config-file decisions.

## Comparison to the Mooncake hostNetwork resolution

Same shape of bug (adapter modeled on vLLM's `lm://`; real backend has a different
data plane), different resolution because the data planes differ:

| | Mooncake | SGLang+LMCache (this doc) |
|---|---|---|
| Real data plane | P2P transfer-engine mesh (master returns a directory pointer; engine dials node IPs on dynamic ports) | Node-local MP worker + shared L2 store |
| Engine `hostNetwork` | **Required** (mesh needs real host IP + all ports) | **Not required** (worker is a same-pod sidecar; loopback + shared `/dev/shm`) |
| Shared/cross-node tier | The mesh itself | Networked L2 (`resp`/Redis, `s3`, `mooncake_store`, `p2p`) |
| `ResolveCacheServer` renders | Mooncake master (hostNetwork, node-IP endpoint) | Shared Redis L2 (ClusterIP) |
| Determinism knob | `PYTHONHASHSEED=0` (vLLM builtin-hash) | none needed (SGLang sha256) |

## Phased delivery

- **Phase 1 (this doc).** Record the design, and — **comment-only, no behavior
  change** — resolve the stale `TODO(wire-test before production)` in
  `enginewire.go` and align its godoc to the validated MP reality. The engine wire
  is unchanged: dropping the MP-ignored `LMCACHE_*` env is deferred to Phase 2,
  where `InjectSGLangLMCache` is rewritten wholesale (so it is edited once, not
  twice). The advisory admission warning stays (no working data plane yet), so no
  regression.
- **Phase 2.** The working data plane: `ResolveCacheServer` → Redis L2;
  `InjectSGLangLMCache` rewritten → config-file init container + MP-worker native
  sidecar + engine config-file wire + shared volumes, **dropping the inert
  `LMCACHE_*` env**; resolve the GPU-less-sidecar spike first. The pinned,
  version-aligned worker/engine/Redis image tuple lands in **`VERSIONS.md` in this
  phase** (it is a Phase-2 correctness requirement, not a Phase-3 doc polish). Flip
  the advisory warning off once validated end-to-end.
- **Phase 3.** Operator surface: `config/samples/cachebackend-sglang.yaml`, the
  `docs/reference-stack/manifests/sglang-lmcache/` reference leg, the
  default-install smoke assertions, and fully rewriting the
  [cachebackend-api.md](cachebackend-api.md) SGLang section from the `lm://` model
  to the MP-mode design (Phase 1 only flags it superseded — see below). Operator
  docs **recommend MP mode** for LMCache and state that **SGLang is MP-only**.
- **Future (out of scope here): vLLM → MP migration.** A separate effort adds a
  vLLM MP path (`LMCacheMPConnector` + `kv_connector_extra_config`) reusing the
  worker + shared-L2 infrastructure this design builds, and switches the
  operator-recommended vLLM posture to MP. The existing `lm://` vLLM adapter stays
  supported for backward compatibility; nothing here breaks it.

## Risks / notes

- **Fail-open** is resolved in [Fail-open semantics](#fail-open-semantics-resolving-the-startup-gate-tension)
  above; the one load-bearing check (the MP worker tolerates an unreachable L2 at
  startup) is the first Phase-2 validation.
- **`chunk_size` must match** between the engine config-file and the MP worker, and
  the same-scheme (`sglang`) hash tag keeps the index disjoint from vLLM (unchanged
  from today).
- **Config surface is version-sensitive.** Live validation (pinned `lmcache`
  0.5.1) needed the `--lmcache-config-file` *flag* + `mp_host`/`mp_port` for the
  separate-server path we require; the upstream quickstart shows a simpler
  `LMCACHE_CONFIG_FILE` env + `local_cpu: true` (an embedded, L1-only sub-mode that
  gives no cross-node sharing). These are different valid MP sub-configs, and the
  exact surface moves between LMCache versions — Phase 2 re-confirms the config
  wire against the version actually pinned in `VERSIONS.md`, not the quickstart.
- **`/dev/shm` sizing** — the L1 lives in `/dev/shm`; too small (default 64 MB)
  silently falls back to slow pickle serialization. The shared `emptyDir` must be
  `medium: Memory` and sized ≥ the L1.
- **L2 durability/HA** — a single managed Redis is a simple default, not an HA
  store. A planned `backendConfig` knob (not yet implemented) will let operators
  who need durability select an `s3` or `mooncake_store` `--l2-adapter` instead,
  mirroring the LMCache-vs-Mooncake durability-is-a-backend-choice decision.
- **Bleeding edge** — SGLang's LMCache integration is new (early 2026); the working
  image/version tuple is pinned by the reference stack, not assumed stable.
