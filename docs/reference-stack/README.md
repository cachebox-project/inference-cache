# vLLM + LMCache reference stack

A reproducible reference deployment of **vLLM** serving a model with **LMCache**
as its KV-cache backend, with **KV-cache events published over ZMQ**. Use it to
verify cache-aware behaviour end to end:

- a **prefix-cache hit** on a repeated long prompt prefix (lower latency, prefill
  skipped), and
- a live **KV-cache event stream** (`BlockStored` / `BlockRemoved` /
  `AllBlocksCleared`) that a cache-aware router or controller can consume.

The manifests here are intentionally minimal and explicit so they can serve as a
starting template for your own automation (an operator, a Helm release, or plain
`kubectl apply`).

> **You need an NVIDIA GPU** for the full stack — vLLM loads weights on CUDA and
> LMCache offloads KV from GPU memory. See [`GPU-RUNBOOK.md`](GPU-RUNBOOK.md) for
> how to size GPU memory and pick a card. If you only want to validate the event
> wiring and prefix-cache behaviour without a GPU, use the
> [CPU-only path](#cpu-only-local-check).

## Layout

| Path | What |
|---|---|
| [`VERSIONS.md`](VERSIONS.md) | Pinned images / models / chart. **Read first.** |
| [`GPU-RUNBOOK.md`](GPU-RUNBOOK.md) | GPU sizing (VRAM math), shape/card table, multi-card tensor-parallelism. |
| [`kind/cluster.yaml`](kind/cluster.yaml) | Local kind cluster (NodePorts for the API + ZMQ). |
| [`manifests/`](manifests/) | GPU reference Deployment + Service. |
| [`manifests/cpu-local/`](manifests/cpu-local/) | CPU variant (no LMCache): prefix-cache hit + KV events. |
| [`helm/values-reference.yaml`](helm/values-reference.yaml) | Upstream vLLM Production-Stack chart path (alternative to the raw manifests). |
| [`scripts/`](scripts/) | ZMQ event subscriber, prefix-cache-hit test, synthetic publisher, tests. |
| `captures/` | Where you save your event-stream sample and a cache-hit screenshot. |

## Prerequisites

```bash
brew install kind            # or your platform's installer; kubectl + helm also required
kind --version               # >= v0.23
```

For the GPU path you additionally need an NVIDIA GPU host (or a managed GPU
cluster), the NVIDIA Container Toolkit / device plugin, and — for gated models —
a Hugging Face token.

---

## Deploy and test on a GPU

> Size the GPU first with [`GPU-RUNBOOK.md`](GPU-RUNBOOK.md). The 8B reference
> model fits on a single 24 GB card.

```bash
# 1. Cluster + GPU. On the GPU host, install the NVIDIA Container Toolkit and set
#    the nvidia runtime as Docker's default FIRST, then create the cluster:
kind create cluster --name inference-cache-substrate --config kind/cluster.yaml
helm repo add nvdp https://nvidia.github.io/k8s-device-plugin
helm install nvdp nvdp/nvidia-device-plugin -n kube-system
kubectl get nodes -o json | jq '.items[].status.allocatable["nvidia.com/gpu"]'   # expect "1"
#    (Or use any managed GPU cluster that advertises nvidia.com/gpu — the manifests
#     are identical; only the cluster differs.)

# 2. Pin the image to a real digest (the manifest ships a non-applyable placeholder
#    on purpose — see VERSIONS.md), then create the HF token secret:
kubectl create namespace cache-substrate
kubectl -n cache-substrate create secret generic hf-token --from-literal=token="$HF_TOKEN"

# 3. Deploy.
kubectl apply -f manifests/namespace.yaml -f manifests/deployment.yaml -f manifests/service.yaml
kubectl -n cache-substrate rollout status deploy/vllm-lmcache-llama-8b --timeout=20m

# 4. Subscribe to the KV-cache event stream and save a sample.
pip install -r scripts/requirements.txt
python scripts/kv_events_subscriber.py --endpoint tcp://localhost:30557 \
    --topic kv-events --max 200 --json | tee captures/kv-events-sample.jsonl

# 5. Demonstrate the prefix-cache hit (run while the subscriber is watching).
./scripts/prefix_cache_hit_test.sh           # save the output to captures/
```

### What success looks like

- **Prefix-cache hit:** request 2 (same long prefix) is faster than request 1, and
  vLLM's `prefix_cache_hits` counter increases.
- **Event stream:** the subscriber prints `BlockStored` events (with block hashes)
  during request 1; the saved sample contains metadata only — hashes and counts,
  never prompt text or token content.

### Alternative: upstream Helm chart

[`helm/values-reference.yaml`](helm/values-reference.yaml) deploys the same stack
via the vLLM Production-Stack chart, if you prefer Helm over raw manifests. It
disables the chart's built-in router (this reference is about cache state and
events, not routing).

---

## CPU-only local check

You can exercise the **whole engine-config path without a GPU** — both a
prefix-cache hit and the KV-cache event stream. vLLM's v1 engine runs on CPU
(vLLM >= ~0.21) and the event publisher works there too; it is just slower and
has no LMCache offload. Uses a tiny model on vLLM's CPU build.

> **Verified** (vLLM 0.21.0 CPU image, arm64): cold request ~31s, warm
> same-prefix request ~1.4s, `vllm:prefix_cache_hits` incremented, and real
> `BlockStored` events were captured over ZMQ with token content redacted. It
> needs enough RAM — see the memory note in
> [`manifests/cpu-local/deployment.yaml`](manifests/cpu-local/deployment.yaml).

> **Match the image to your host arch.** `manifests/cpu-local/deployment.yaml`
> defaults to the **`-arm64`** image tag. On x86_64 hosts, change it to
> `vllm/vllm-openai-cpu:latest-x86_64` first (the tags are arch-specific):
> `sed -i 's/latest-arm64/latest-x86_64/' manifests/cpu-local/deployment.yaml`.

```bash
kind create cluster --name inference-cache-substrate --config kind/cluster.yaml
kubectl apply -f manifests/namespace.yaml -f manifests/cpu-local/deployment.yaml
kubectl -n cache-substrate rollout status deploy/vllm-cpu-sanity --timeout=30m

pip install -r scripts/requirements.txt
python scripts/kv_events_subscriber.py --endpoint tcp://localhost:30557 --topic kv-events &
MODEL=Qwen/Qwen2.5-0.5B-Instruct ./scripts/prefix_cache_hit_test.sh
```

### No image pull / no cluster? Validate the consumer with the synthetic publisher

If you can't pull the image or run a cluster, you can still confirm the
event-decode + token-redaction path with the synthetic publisher — it emits
vLLM-shaped frames, no image required:

```bash
pip install -r scripts/requirements.txt
python scripts/kv_events_synthetic_publisher.py --bind 'tcp://*:5557' &
python scripts/kv_events_subscriber.py --endpoint tcp://localhost:5557 --max 4
python scripts/test_kv_events.py        # asserts token_ids never surfaces; token_count kept
```

`test_kv_events.py` is the regression check for the decode + token-redaction
logic. It is run manually (the repo's CI is Go-only and has no Python step), so
run it after changing the subscriber.

---

## CacheBackend reconciler canary (CPU)

[`scripts/canary_c2_reconcile.sh`](scripts/canary_c2_reconcile.sh) is a GPU-free,
on-demand canary for the **C2 reconciler**: it brings up a kind cluster, runs the
controller, applies a `CacheBackend` with `backendConfig.profile: cpu`, and asserts
the controller stands up a healthy serving backend (`status.health=Ready`, endpoint
published), an engine prefix-cache hit through the Service, and owner-ref garbage
collection when the CR is deleted. It exercises the reconciler against real pods —
the gap the envtest unit tests can't cover.

```bash
docs/reference-stack/scripts/canary_c2_reconcile.sh
```

Like the full-chain canary it is **on-demand**, not a blocking gate: it needs
Docker + kind, pulls the vLLM CPU image, and wants ~10+ GiB of Docker VM RAM. The
`cpu` profile runs a GPU-free vLLM engine (prefix caching + KV events, no LMCache
offload); real LMCache offload still needs a GPU (the default `gpu` profile).

---

## Teardown

```bash
kind delete cluster --name inference-cache-substrate
```
