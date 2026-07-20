# Pinned versions — vLLM (+ SGLang) + LMCache reference substrate

Everything the reference stack depends on, pinned. Bump here first, re-validate
on a GPU host, then propagate to any automation that templates these manifests.

> **SGLang runs LMCache in MP mode (implemented, GPU-validated).** SGLang does not
> use the `lm://` lmcache-server model at all — it drives LMCache in **multiprocess
> (MP) mode**: config via the `--lmcache-config-file` flag, a **node-local MP worker**
> over `mp_host`/`mp_port`, and a shared **Redis L2** behind that worker (`lm://` is
> not a valid MP `--l2-adapter` type, so it cannot be reused here). The adapter renders
> exactly that. So the SGLang rows below pin **the engine/worker image tuple + Redis**,
> NOT an `lm://` server. Authoritative design + evidence:
> [`sglang-lmcache-mp-mode.md`](../design/sglang-lmcache-mp-mode.md). The vLLM rows are
> unaffected — `lm://` remains vLLM's shipped, supported path.

| Component | Pin | Where | Notes |
|---|---|---|---|
| vLLM + LMCache image | `lmcache/vllm-openai@sha256:<pin>` | `manifests/deployment.yaml`, `helm/values-reference.yaml` | Upstream ships LMCache pre-installed. Requires the vLLM **v1** engine (`VLLM_USE_V1=1`). The manifests ship a **non-applyable placeholder digest** — substitute a real one (below) before the GPU run. |
| Model | `meta-llama/Llama-3.1-8B-Instruct` | `manifests/deployment.yaml` | Gated on HF; needs `HF_TOKEN`. Small enough for a single A10/L40S-class GPU. Swap freely. |
| SGLang engine + MP worker | base `docker.io/lmsysorg/sglang:nightly-dev-cu13-20260711-7de33ce8` + **lmcache 0.5.1** → derive + `@sha256:<pin>` | `manifests/sglang-lmcache/deployment.yaml`; the controller-rendered worker defaults to the **engine's own image** (`backendConfig.workerImage` overrides) | **GPU-validated tuple** (store→flush→retrieve reuses KV). The engine and the MP worker MUST run the same lmcache version — they speak the MP wire to each other — which is why the worker defaults to the engine image; overriding `workerImage` makes that alignment yours to maintain. Base `lmsysorg/sglang` does **not** bundle lmcache: derive an image (`pip install lmcache==0.5.1`) and pin its digest. **cu13 is load-bearing**: lmcache 0.5.1 needs `libcudart.so.13`, so a cu12 base fails to align, and stock `v0.5.1.post2-cu126` is too old to have `--enable-lmcache`. See [SGLang derived image reproducibility](#sglang-derived-image-reproducibility). GPU-only. |
| lmcache-server image | `lmcache/standalone:v0.4.7` → `@sha256:<pin>` | `manifests/deployment.yaml` (vLLM) | **vLLM only.** The `lm://` standalone server, correct and in use for the **vLLM+LMCache** managed-backend default. **Not part of the SGLang wire in any form**: SGLang is MP-only, and `lm://` is not a valid MP `--l2-adapter` type, so it is not reachable behind the MP worker either — the SGLang shared tier is the Redis L2 row below. (An earlier revision of this file predicted the MP fix would reuse this server behind a per-node worker; GPU validation disproved that, and the prediction is retired rather than left to mislead.) |
| Redis L2 store (SGLang) | `docker.io/library/redis:7.4-alpine` → `@sha256:<pin>` | `pkg/adapters/runtime/redis_l2.go` (`defaultRedisImage`), overridable per-CR via `backendConfig.redisImage` | The shared **L2** the SGLang LMCache **MP worker** offloads to (its `resp` `--l2-adapter`) — the SGLang analogue of the `lm://` lmcache-server, which SGLang cannot use (`lm://` is not a valid MP `--l2-adapter` type). Rendered by the controller, not a reference manifest, so this is a **code default** rather than a manifest pin. `7.4-alpine` is a minor-version tag: stable in wire protocol and config surface, but **mutable within its patch line**. Per the image-pin policy in [`sglang-lmcache-mp-mode.md`](../design/sglang-lmcache-mp-mode.md), production **must** override `backendConfig.redisImage` with an exact release or `@sha256:` digest — the default is a convenience for dev/smoke, not a reproducible pin. Redis itself needs no lmcache version alignment (the MP worker speaks RESP), so this pin is independent of the engine/lmcache tuple above. |
| SGLang model | `meta-llama/Meta-Llama-3-8B-Instruct` | `manifests/sglang-lmcache/deployment.yaml` | Served model for the SGLang reference, kept equal to `config/samples/cachebackend-sglang.yaml`'s `backendConfig.model` so the managed-path docs line up. Gated on HF; needs `HF_TOKEN`. Swap freely, but keep the engine `--model-path`, the CacheBackend `backendConfig.model`, and request `model` identical. |
| CPU image | `vllm/vllm-openai-cpu:latest-{x86_64,arm64}` | `manifests/cpu-local/deployment.yaml` | vLLM's dedicated CPU build (arch-tagged). Runs the v1 engine on CPU (vLLM >= ~0.21), incl. the KV-event publisher. Verified on `0.21.0` (arm64): prefix-cache hit + real ZMQ events. Needs adequate RAM (CPU baseline ~5 GiB + KV). |
| CPU model | `Qwen/Qwen2.5-0.5B-Instruct` | `manifests/cpu-local/deployment.yaml` | Ungated, tiny, CPU-runnable. |
| kind | `>= v0.23` | local | `brew install kind`. Node image `kindest/node:v1.31.x`. |
| vLLM Production Stack chart | `vllm/vllm-stack` (chart `>= 0.1.6`) | `helm/values-reference.yaml` | Upstream "reference Helm chart". Pin the chart version at `helm install --version`. |
| NVIDIA k8s-device-plugin | `>= 0.15` | GPU host only | Only for the GPU-on-kind path. |

## Digest-pin the GPU images before the GPU run

`latest` is fine for a local CPU check but should not be used for a real GPU
deployment — it is not reproducible, and it is exactly the value any automation
templating these manifests would hard-code. Before
the GPU test/dev run:

```bash
docker pull lmcache/vllm-openai:latest
docker inspect --format='{{index .RepoDigests 0}}' lmcache/vllm-openai:latest
# -> lmcache/vllm-openai@sha256:...   put THIS in deployment.yaml + VERSIONS.md
```

The SGLang reference pins **two** images: (a) the **derived engine image** (which
the MP worker also runs by default), and (b) the **Redis L2** store. It does **not**
pin an lmcache-server — SGLang never dials one.

(a) The **derived** SGLang engine image with the lmcache client baked in (the
base `lmsysorg/sglang` does not bundle it). This image is used **twice**: as the
engine, and — by default — as the MP worker (`backendConfig.workerImage` overrides
it), which is what keeps the two on the same lmcache version:

```bash
# Build a derived SGLang image with a version-aligned lmcache client. The
# lmcache version must satisfy BOTH alignment constraints: (1) ENGINE <-> MP WORKER
# -- the two speak the LMCache MP wire to each other, so they must run the same
# lmcache (trivially satisfied when the worker defaults to this same image), AND
# (2) its native CUDA kernels match the SGLang base image's CUDA runtime -- a
# CUDA-mismatched wheel loads but silently falls back to a slow non-native path,
# and the standalone reference has no lmcache-kernel-check init container to catch
# it (see "LMCache client kernels <-> engine-image CUDA / vLLM alignment" in
# docs/design/cachebackend-api.md). GPU-validated tuple:
# lmsysorg/sglang:nightly-dev-cu13-20260711-7de33ce8 + lmcache 0.5.1 -- cu13 is
# load-bearing (0.5.1 links libcudart.so.13). NOTE: there is no lmcache-SERVER
# alignment constraint here; the shared L2 is Redis, which speaks RESP.
cat > Dockerfile.sglang-lmcache <<'EOF'
# Digest-pin the base too — this file is the pinning authority, and a moving tag
# would silently change the derived image's inputs on rebuild. Resolve the digest
# with: docker pull lmsysorg/sglang:<tag> &&
#   docker inspect --format='{{index .RepoDigests 0}}' lmsysorg/sglang:<tag>
# The GPU-validated tuple is the nightly-dev-cu13-20260711-7de33ce8 base +
# lmcache 0.5.1; resolve <pinned-base-digest> from that tag with the command above.
FROM lmsysorg/sglang@sha256:<pinned-base-digest>
RUN pip install --no-cache-dir lmcache==0.5.1
EOF
docker build -f Dockerfile.sglang-lmcache -t myrepo/sglang-lmcache:pinned .
docker push myrepo/sglang-lmcache:pinned
# Read the pushed digest from the push output ("... digest: sha256:..."), or
# query the registry (local `docker inspect .RepoDigests` is often empty until
# the image is pulled back):
docker buildx imagetools inspect myrepo/sglang-lmcache:pinned --format '{{.Manifest.Digest}}'
# -> sha256:...   use myrepo/sglang-lmcache@<that digest> in manifests/sglang-lmcache/deployment.yaml
```

## SGLang derived image reproducibility

The SGLang engine row above now names a **GPU-validated `(sglang-tag,
lmcache-version)` tuple**; what is still yours to supply is the **digest of the
derived image you build from it**. (This section previously said no validated tuple
existed — true when it was written, before a GPU was available; the tuple below
replaces that placeholder.) Concretely:

- **Pin the DERIVED image, not the upstream base.** `lmsysorg/sglang` does **not**
  bundle the lmcache client; bake `pip install lmcache` into your own image — see the
  build steps above. The **reference manifest** still ships a non-applyable
  placeholder digest under an `example.invalid/sglang-lmcache` name (all-zero
  `@sha256:`) — substitute your derived image's real digest before the GPU run.
  (Updating that manifest to the validated tuple is part of the reference-stack
  increment; the controller-rendered managed path does not read it.)
- **RESOLVED — the concrete `(sglang-tag, lmcache-version)` tuple is
  `(lmsysorg/sglang:nightly-dev-cu13-20260711-7de33ce8, lmcache 0.5.1)`**, validated
  end-to-end on an A100: the MP worker registers the engine's KV cache over CUDA-IPC
  and a flushed prompt is served back out of LMCache. Two constraints that tuple
  encodes, both learned the hard way: **cu13 is load-bearing** (lmcache 0.5.1 links
  `libcudart.so.13`, so a cu12 base mis-aligns at runtime), and the **stock
  `v0.5.1.post2-cu126` tag is too old** — it predates `--enable-lmcache` entirely.
  The validation installed lmcache with `pip` at pod start; a real deployment should
  bake it into a derived image and pin THAT digest here (a runtime `pip install` is
  not reproducible). The `@sha256:` digest is still yours to fill from your own
  build. **The alignment that matters is engine ↔ MP worker (same lmcache, since they
  speak the MP wire) plus lmcache ↔ CUDA runtime — NOT lmcache ↔ lmcache-server**: the
  SGLang shared tier is Redis, which speaks RESP and carries no lmcache-version
  constraint. Both constraints the tuple must
  satisfy (engine ↔ worker lmcache parity, and lmcache kernels ↔ the SGLang base
  image's CUDA runtime) are spelled out in the build steps above.
- **What IS validated without a GPU:** SGLang's exact event wire is covered by the
  Go `pkg/adapters/engine` SGLang test; the Python synthetic publisher covers only
  the shared decode/redaction. See
  [`manifests/sglang-lmcache/README.md`](manifests/sglang-lmcache/README.md).

## Why this image / engine combo

**vLLM path:**

- vLLM + LMCache via their reference manifests. Upstream packages both in
  `lmcache/vllm-openai`, so a single container runs the engine and the LMCache
  connector in-process, not a sidecar.
- **vLLM v1 is required**: the KV-event publisher (`BlockStored` / `BlockRemoved`
  / `AllBlocksCleared`) and the `LMCacheConnectorV1` connector both live on the
  v1 engine. The image's `latest` tag assumes v1.

**SGLang path:**

- SGLang loads the LMCache client in-process too, but turns it on with
  `--enable-lmcache` + `LMCACHE_USE_EXPERIMENTAL=True` (no `--kv-transfer-config`,
  no `VLLM_USE_V1`). There is no single upstream image bundling both, so the
  engine image is a **derived** `lmsysorg/sglang` + `pip install lmcache` (above).
  The KV-event publisher is SGLang's own `--kv-events-config` ZMQ scheme, wire-
  compatible with vLLM's (see the SGLang manifest README).
