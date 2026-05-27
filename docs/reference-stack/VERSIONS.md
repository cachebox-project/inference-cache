# Pinned versions — vLLM + LMCache reference substrate

Everything the reference stack depends on, pinned. The M5 (`CacheBackend`)
reconciler should generate Deployments that match these pins; bump here first,
re-validate on the GPU fleet, then update the reconciler defaults.

| Component | Pin | Where | Notes |
|---|---|---|---|
| vLLM + LMCache image | `lmcache/vllm-openai@sha256:<pin>` | `manifests/deployment.yaml`, `helm/values-reference.yaml` | Upstream ships LMCache pre-installed. Requires the vLLM **v1** engine (`VLLM_USE_V1=1`). The manifests ship a **non-applyable placeholder digest** — substitute a real one (below) before the GPU run. |
| Model | `meta-llama/Llama-3.1-8B-Instruct` | `manifests/deployment.yaml` | Gated on HF; needs `HF_TOKEN`. Small enough for a single A10/L40S-class GPU. Swap freely. |
| CPU sanity image | `vllm/vllm-openai:latest` (linux/amd64) | `manifests/cpu-local/deployment.yaml` | Stock vLLM, no LMCache. CPU backend only. Used to validate the **ZMQ KV-event wiring + prefix-cache hit**, not LMCache. |
| CPU sanity model | `Qwen/Qwen2.5-0.5B-Instruct` | `manifests/cpu-local/deployment.yaml` | Ungated, tiny, CPU-runnable. |
| kind | `>= v0.23` | local | `brew install kind`. Node image `kindest/node:v1.31.x`. |
| vLLM Production Stack chart | `vllm/vllm-stack` (chart `>= 0.1.6`) | `helm/values-reference.yaml` | Upstream "reference Helm chart". Pin the chart version at `helm install --version`. |
| NVIDIA k8s-device-plugin | `>= 0.15` | GPU host only | Only for the GPU-on-kind path. |

## Re-pin `latest` to a digest before the GPU run

`latest` is fine for local CPU sanity but must not reach the GPU fleet — it is
not reproducible and is exactly what the M5 reconciler will hard-code. Before
the OCI test/dev run:

```bash
docker pull lmcache/vllm-openai:latest
docker inspect --format='{{index .RepoDigests 0}}' lmcache/vllm-openai:latest
# -> lmcache/vllm-openai@sha256:...   put THIS in deployment.yaml + VERSIONS.md
```

## Why this image / engine combo

- The ticket asks for **vLLM + LMCache via their reference manifests**. Upstream
  packages both in `lmcache/vllm-openai`, so a single container runs the engine
  and the LMCache connector — no sidecar (matches tech-spec §3.4: in-process
  engine config, not a sidecar).
- **vLLM v1 is required**: the KV-event publisher (`BlockStored` / `BlockRemoved`
  / `AllBlocksCleared`) and the `LMCacheConnectorV1` connector both live on the
  v1 engine. The image's `latest` tag assumes v1.
