# Design: SGLang + LMCache (MP mode)

Status: proposed · Supersedes the "mirror the vLLM+LMCache adapter" model in [cachebackend-api.md](cachebackend-api.md) SGLang section · Adapter: `pkg/adapters/runtime/sglang`

The shipped `(sglang, LMCache)` adapter was built by analogy to `(vllm, LMCache)`:
render a standalone `lm://` LMCache server, then inject `LMCACHE_REMOTE_URL` +
`--enable-lmcache` on the engine so it dials that server. **Live validation on a
GPU cluster showed this is wrong for SGLang** — the same class of mismatch the
Mooncake hostNetwork work surfaced for a different backend. This doc records how
SGLang+LMCache actually works, the design that fits it, and the phased path there.

An advisory admission warning already ships for this pair (the validator's
`sglangLMCacheDataPlaneWarning`) so operators are told the wiring is unverified;
this design is what replaces the warning with a working data plane.

## TL;DR

- SGLang drives LMCache in **multiprocess (MP) mode**, not the `lm://`
  remote-server client model vLLM uses. The engine talks to a **node-local MP
  worker** over ZMQ (`mp_host`/`mp_port`) + a shared-memory data path, configured
  by a **`--lmcache-config-file`** (the injected `LMCACHE_*` env is ignored).
- Cross-node KV sharing is a **networked L2 store behind the MP worker**
  (`--l2-adapter` = `resp`/Redis, `s3`, `mooncake_store`, or `p2p`) — **not** the
  `lm://` server, which is not even a valid MP `--l2-adapter` type.
- The design fits the existing `KVCacheRuntimeAdapter` interface with **no new
  methods**: `ResolveCacheServer` renders the **shared L2 (Redis)**;
  `InjectEngineConfig` adds a **config-file init container + an MP-worker sidecar
  + the engine wire** to the engine pod. `mp_host=127.0.0.1` (same-pod sidecar),
  so — unlike Mooncake — the engine needs **no `hostNetwork`**.

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
Service owned by the CR. For SGLang, that workload becomes the **shared Redis L2**
(a **pinned** Redis image — a digest/tag tracked in `VERSIONS.md`, consistent with
the lmcache-server image-pin policy, never a floating `redis` tag —
`--maxmemory-policy allkeys-lru`, ClusterIP `:6379`), and
`status.endpoint` becomes the Redis Service DNS. This replaces the `lm://`
lmcache-server render (`ResolveLMCacheServer`) for the SGLang pair only — vLLM
keeps `lm://`. Redis is a shared, network-addressable store that fits the
one-Service, engines-anywhere model exactly (unlike Mooncake's mesh), so **no
`hostNetwork` is required for the L2**.

### `InjectEngineConfig` → config-file init container + MP-worker sidecar + engine wire

The mutating Pod webhook already adds volumes, init containers, and sidecar
containers to engine pods. For SGLang it adds, to the engine pod:

- **initContainer `lmcache-config`** — writes `/config/lmcache.yaml`
  (`chunk_size`, `mp_host: 127.0.0.1`, `mp_port: 5555`) into a shared `emptyDir`.
  Runs to completion before the engine, so `--lmcache-config-file` is never read
  before it exists. No ConfigMap needed (the webhook cannot create cluster
  resources; the values are static and small).
- **native sidecar `lmcache-mp-worker`** — `python -m
  lmcache.v1.multiprocess.server --host 127.0.0.1 --port 5555 --chunk-size <n>
  --l1-size-gb <n>
  --l2-adapter '{"type":"resp","host":<endpoint-host>,"port":<endpoint-port>}'`,
  where `<endpoint>` is the Redis address passed to `InjectEngineConfig`. Mounts
  the shared `/dev/shm` (`emptyDir{medium: Memory, sizeLimit ≥ l1-size}`) and
  `/config`. **Startup ordering matters** — the engine dials the worker at launch,
  and K8s does not order ordinary containers within a pod. So this is a **native
  sidecar** (a `restartPolicy: Always` entry in `initContainers`, GA since K8s
  1.29) with a `startupProbe` on the ZMQ port: native sidecars start and gate
  ready **before** the main engine container, so the worker is always listening
  when the engine connects. (An ordinary sidecar container would race the engine.)
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
  `LMCACHE_*` env**; resolve the GPU-less-sidecar spike first. Flip the advisory
  warning off once validated end-to-end.
- **Phase 3.** Operator surface: `config/samples/cachebackend-sglang.yaml`, the
  `docs/reference-stack/manifests/sglang-lmcache/` reference leg, `VERSIONS.md`,
  the default-install smoke assertions, and correcting the
  [cachebackend-api.md](cachebackend-api.md) SGLang section from the `lm://` model
  to the MP-mode design.

## Risks / notes

- **`chunk_size` must match** between the engine config-file and the MP worker, and
  the same-scheme (`sglang`) hash tag keeps the index disjoint from vLLM (unchanged
  from today).
- **`/dev/shm` sizing** — the L1 lives in `/dev/shm`; too small (default 64 MB)
  silently falls back to slow pickle serialization. The shared `emptyDir` must be
  `medium: Memory` and sized ≥ the L1.
- **L2 durability/HA** — a single managed Redis is a simple default, not an HA
  store. A planned `backendConfig` knob (not yet implemented) will let operators
  who need durability select an `s3` or `mooncake_store` `--l2-adapter` instead,
  mirroring the LMCache-vs-Mooncake durability-is-a-backend-choice decision.
- **Bleeding edge** — SGLang's LMCache integration is new (early 2026); the working
  image/version tuple is pinned by the reference stack, not assumed stable.
