# Pinned versions — vLLM + LMCache reference substrate

Everything the reference stack depends on, pinned. Bump here first, re-validate
on a GPU host, then propagate to any automation that templates these manifests.

| Component | Pin | Where | Notes |
|---|---|---|---|
| vLLM + LMCache image | `lmcache/vllm-openai@sha256:<pin>` | `manifests/deployment.yaml`, `helm/values-reference.yaml` | Upstream ships LMCache pre-installed. Requires the vLLM **v1** engine (`VLLM_USE_V1=1`). The manifests ship a **non-applyable placeholder digest** — substitute a real one (below) before the GPU run. |
| Model | `meta-llama/Llama-3.1-8B-Instruct` | `manifests/deployment.yaml` | Gated on HF; needs `HF_TOKEN`. Small enough for a single A10/L40S-class GPU. Swap freely. |
| CPU demo image | `vllm/vllm-openai-cpu:latest-{x86_64,arm64}` | `manifests/cpu-local/deployment.yaml` | vLLM's dedicated CPU build (arch-tagged). Runs on **v0** (`VLLM_USE_V1=0`) for a prefix-cache demo only — **not** the KV-event path (events require v1, which is GPU-only). |
| CPU demo model | `Qwen/Qwen2.5-0.5B-Instruct` | `manifests/cpu-local/deployment.yaml` | Ungated, tiny, CPU-runnable. |
| kind | `>= v0.23` | local | `brew install kind`. Node image `kindest/node:v1.31.x`. |
| vLLM Production Stack chart | `vllm/vllm-stack` (chart `>= 0.1.6`) | `helm/values-reference.yaml` | Upstream "reference Helm chart". Pin the chart version at `helm install --version`. |
| NVIDIA k8s-device-plugin | `>= 0.15` | GPU host only | Only for the GPU-on-kind path. |

## Re-pin `latest` to a digest before the GPU run

`latest` is fine for a local CPU check but should not be used for a real GPU
deployment — it is not reproducible, and it is exactly the value any automation
templating these manifests would hard-code. Before
the OCI test/dev run:

```bash
docker pull lmcache/vllm-openai:latest
docker inspect --format='{{index .RepoDigests 0}}' lmcache/vllm-openai:latest
# -> lmcache/vllm-openai@sha256:...   put THIS in deployment.yaml + VERSIONS.md
```

## Why this image / engine combo

- The ticket asks for **vLLM + LMCache via their reference manifests**. Upstream
  packages both in `lmcache/vllm-openai`, so a single container runs the engine
  and the LMCache connector in one container — the engine config is in-process,
  not a sidecar.
- **vLLM v1 is required**: the KV-event publisher (`BlockStored` / `BlockRemoved`
  / `AllBlocksCleared`) and the `LMCacheConnectorV1` connector both live on the
  v1 engine. The image's `latest` tag assumes v1.
