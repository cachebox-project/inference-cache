# Typed LMCache MP reference versions and evidence

The reference manifests use only the typed PodLocal multiprocess data plane.
Engine and server images are independently owned: this repository injects the
connector configuration and native sidecar, but does not replace the inference
engine image or infer compatibility from an allowlist or annotation. A normal
engine startup is the authoritative package/connector compatibility check.

## Manifest pins

| Component | Reference value | Notes |
|---|---|---|
| LMCache MP-server sidecar | `docker.io/lmcache/standalone@sha256:b813bf0bb616d1012b6a6edcbd4a44f1576dbbdaa857962e56d48b9f7c127d13` | Pinned by the typed `CacheBackend`; runs `lmcache server` as the PodLocal native sidecar. This exact reference digest is structurally tested, while the GPU evidence below used the separately recorded validation digest. |
| vLLM engine | non-pullable all-zero placeholder | Replace with a digest-pinned image containing the LMCache MP connector/package. The repository deliberately supplies no default engine image. |
| SGLang engine | non-pullable all-zero placeholder | Replace with a digest-pinned SGLang image containing a compatible LMCache client. The repository deliberately supplies no default engine image. |
| Redis | `docker.io/library/redis:7.4-alpine` | Used only by the SGLang reference as an explicit external L3 choice. Digest-pin it for production. |
| vLLM model | `meta-llama/Llama-3.1-8B-Instruct` | Gated; keep the served model and `observation.modelID` aligned. |
| SGLang model | `meta-llama/Meta-Llama-3-8B-Instruct` | Gated; keep the served model, request model, and `observation.modelID` aligned. |
| CPU-only engine | `vllm/vllm-openai-cpu:latest-{x86_64,arm64}` | Event/prefix-cache check only; no LMCache MP data plane. Mutable development tag, not a production pin. |

The MP-server reference digest and the GPU-validation digest differ. Do not
interpret structural manifest coverage as a claim that this exact engine/server
tuple has completed the live GPU matrix.

## Recorded live validation

Phase 3 and Phase 4 ran on 2026-08-10/11 in the SJC development environment:

| Path | Engine evidence | LMCache evidence | Result |
|---|---|---|---|
| SGLang TP=1 | `docker.io/lmsysorg/sglang@sha256:920df39109c60429b0a23eaacfd2786fcf1595c12f3ca4fc6e153b2abe34865f` (`0.5.13.post1-cu129`) | Client wheel 0.5.3 CUDA 12.9 via a test-only runtime-owner overlay; standalone `sha256:0df30fc70a7d689e1f12823789208a0ee8ef31537316eba6a4c2fa83b0abe61b` | Host-only store/retrieve, bounded L1 eviction, events/status, and managed-Redis replacement-Pod retrieval passed. |
| vLLM TP=1/2 | `us-sanjose-1.ocir.io/idqj093njucb/vllm-openai@sha256:f72dd35b1efd50fd7646ebce708f173a4040fddf3f2363759c67ad732d912d0a` (`0.25.1`) | Client wheel 0.5.3 CUDA 12.9 via a test-only runtime-owner overlay; same validation standalone digest | Host-only TP=1/2 retrieval, events/status, shared-memory budget, native extension checks, and supplemental Redis replacement-Pod retrieval passed. |

The runtime-owner wheel overlays were validation scaffolding, not authority for
`CacheBackend` to mutate an engine image. These records prove the controller
path with those exact test inputs; they are not universal image endorsements.

## Operator compatibility rule

Before production rollout:

1. Build or select the engine image in the inference-system release process.
2. Pin both engine and MP-server sidecar images by digest.
3. Create the typed `CacheBackend` and let the webhook inject MP wiring.
4. Treat engine startup/readiness as the compatibility verdict, then run a
   store, local-cache reset, and retrieve test on the target GPU/CUDA stack.

Do not map removed `LMCacheServer` or legacy IP-wired Mooncake objects to Redis automatically.
The operator must explicitly choose host-only MP or a supported L3 because the
choice changes cross-Pod sharing and persistence semantics. Mooncake support
returns only through a separately validated typed MP L2 adapter.
