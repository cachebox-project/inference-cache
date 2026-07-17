# Pinned versions — vLLM (+ SGLang) + LMCache reference substrate

Everything the reference stack depends on, pinned. Bump here first, re-validate
on a GPU host, then propagate to any automation that templates these manifests.

> **KNOWN LIMITATION (GPU-validated 2026-07):** the SGLang rows describe an `lm://`
> lmcache-server the SGLang engine "offloads to." GPU validation showed SGLang does
> **not** use LMCache that way — it uses **multiprocess (MP) mode** (config via the
> `--lmcache-config-file` flag, a node-local worker over `mp_host`/`mp_port`, not a
> cluster-reachable `lm://` server). As shipped (env only, no `--lmcache-config-file`)
> the engine **refuses to start** (`MP mode requires --lmcache-config-file`); a
> `--lmcache-config-file` carrying `remote_url: lm://…` instead hangs. The SGLang
> `lmcache-server` / `LMCACHE_REMOTE_URL` pins below are the
> shipped (incorrect) wiring, pending the MP-mode fix. See the SGLang README's KNOWN
> LIMITATION note and `docs/design/cachebackend-api.md` (SGLang engine support). The
> vLLM rows are unaffected.

| Component | Pin | Where | Notes |
|---|---|---|---|
| vLLM + LMCache image | `lmcache/vllm-openai@sha256:<pin>` | `manifests/deployment.yaml`, `helm/values-reference.yaml` | Upstream ships LMCache pre-installed. Requires the vLLM **v1** engine (`VLLM_USE_V1=1`). The manifests ship a **non-applyable placeholder digest** — substitute a real one (below) before the GPU run. |
| Model | `meta-llama/Llama-3.1-8B-Instruct` | `manifests/deployment.yaml` | Gated on HF; needs `HF_TOKEN`. Small enough for a single A10/L40S-class GPU. Swap freely. |
| SGLang image (derived) | `example.invalid/sglang-lmcache@sha256:<pin>` *(derived: base `lmsysorg/sglang` + lmcache)* | `manifests/sglang-lmcache/deployment.yaml` | Second-engine reference. **Not pinned to a runnable tuple** (no GPU build at authoring time) — see [SGLang derived image reproducibility](#sglang-derived-image-reproducibility) below for the build steps, alignment constraints, and the TODO to fill the concrete `(sglang-tag, lmcache-version)`. GPU-only. |
| lmcache-server image | `lmcache/standalone:v0.4.7` → `@sha256:<pin>` | `manifests/sglang-lmcache/deployment.yaml` | **HISTORICAL / non-runnable for SGLang** — see the note above. This `lm://` server is what the shipped wiring *tries* to point SGLang at, but SGLang uses LMCache in MP mode and never dials it, so pinning a digest for a SGLang GPU run offloads nothing. Kept only to document the (broken) shipped topology; the MP-mode fix will reuse this image behind a per-node worker. The `lm://` server itself is correct and in-use for **vLLM+LMCache** (the managed-backend default), just not for SGLang. |
| Redis L2 store (SGLang) | `docker.io/library/redis:7.4-alpine` → `@sha256:<pin>` | `pkg/adapters/runtime/redis_l2.go` (`defaultRedisImage`); will be overridable per-CR via `backendConfig.redisImage` once wired | **Staged — the renderer (`ResolveRedisL2Server`) exists but nothing calls it yet; `ResolveCacheServer` wires it in the SGLang MP-mode engine increment (follow-up PR), at which point the controller renders it and `redisImage` takes effect.** The shared **L2** the SGLang LMCache **MP worker** offloads to (its `resp` `--l2-adapter`) — the SGLang analogue of the `lm://` lmcache-server, which SGLang cannot use (`lm://` is not a valid MP `--l2-adapter` type). Rendered by the controller, not a reference manifest, so this is a **code default** rather than a manifest pin. `7.4-alpine` is a minor-version tag: stable in wire protocol and config surface, but **mutable within its patch line**. Per the image-pin policy in [`sglang-lmcache-mp-mode.md`](../design/sglang-lmcache-mp-mode.md), production **must** override `backendConfig.redisImage` with an exact release or `@sha256:` digest — the default is a convenience for dev/smoke, not a reproducible pin. Redis itself needs no lmcache version alignment (the MP worker speaks RESP), so this pin is independent of the engine/lmcache tuple above. |
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

The SGLang reference pins **two** images. (a) The standalone lmcache-server
(`lmcache/standalone:v0.4.7`) — pull + inspect it directly (it is NOT the vLLM
path's `lmcache/vllm-openai` image):

```bash
docker pull lmcache/standalone:v0.4.7
docker inspect --format='{{index .RepoDigests 0}}' lmcache/standalone:v0.4.7
# -> lmcache/standalone@sha256:...   put THIS in manifests/sglang-lmcache/deployment.yaml
```

(b) The **derived** SGLang engine image with the lmcache client baked in (the
base `lmsysorg/sglang` does not bundle it):

```bash
# Build a derived SGLang image with a version-aligned lmcache client. The
# lmcache version must satisfy BOTH alignment constraints: (1) wire-compatible
# with the lmcache-server tag, AND (2) its native CUDA kernels match the SGLang
# base image's CUDA runtime — a CUDA-mismatched wheel loads but silently falls
# back to a slow non-native path, and the standalone reference has no
# lmcache-kernel-check init container to catch it (see "LMCache client kernels ↔
# engine-image CUDA / vLLM alignment" in docs/design/cachebackend-api.md).
cat > Dockerfile.sglang-lmcache <<'EOF'
# Digest-pin the base too — this file is the pinning authority, and a moving tag
# would silently change the derived image's inputs on rebuild. Resolve the digest
# with: docker pull lmsysorg/sglang:<tag> &&
#   docker inspect --format='{{index .RepoDigests 0}}' lmsysorg/sglang:<tag>
FROM lmsysorg/sglang@sha256:<pinned-base-digest>
RUN pip install --no-cache-dir lmcache==<wire- and CUDA-aligned version>
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

The SGLang engine row above is **deliberately not pinned to a concrete, runnable
tuple** — no GPU was available at authoring time to build and validate one, and
inventing a plausible-looking digest/version would be worse than an honest
placeholder. Concretely:

- **Pin the DERIVED image, not the upstream base.** `lmsysorg/sglang` does **not**
  bundle the lmcache client; bake `pip install lmcache` (version-aligned with the
  lmcache-server tag) into your own image — see the build steps above. The
  manifest ships a non-applyable placeholder digest under an
  `example.invalid/sglang-lmcache` name (all-zero `@sha256:`); substitute your
  derived image's real digest before the GPU run.
- **TODO — fill the concrete `(sglang-tag, lmcache-version)` tuple** from your
  first successful derived-image build, and record the resolved digest here. Both
  stay as placeholders (`<pin>` / `<wire- and CUDA-aligned version>`) until then.
  This is tracked, not an omission; the alignment constraints the tuple must
  satisfy (lmcache ↔ lmcache-server wire compat, and lmcache kernels ↔ the SGLang
  base image's CUDA runtime) are in the build steps above.
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
