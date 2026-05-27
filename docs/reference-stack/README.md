# vLLM + LMCache reference substrate

Hand-deployed reference for the Phase-1 cache backend: **vLLM** serving a model
with **LMCache** as the KV-cache backend, emitting **KV-cache events over ZMQ**
(`BlockStored` / `BlockRemoved` / `AllBlocksCleared`).

This is module **A2 / CAC-13**. Two reasons it exists:

1. **The manifests here are the template the M5 (`CacheBackend`) reconciler
   generates** — M5/C2 is automation of [`manifests/`](manifests/), not greenfield
   design.
2. The ZMQ event stream is **the signal C1 subscribes to**. The
   [`scripts/kv_events_subscriber.py`](scripts/kv_events_subscriber.py) here is
   the by-hand stand-in for that subscriber.

> **Why two image paths?** Everything real (the cache-hit demo, captured ZMQ
> events) needs an **NVIDIA GPU** — vLLM loads weights on CUDA and LMCache
> offloads from GPU memory. The GPU run targets the **OCI test/dev GPU fleet**.
> A laptop without an NVIDIA GPU can still validate the *config* (ZMQ wiring +
> prefix-cache behaviour) via the [CPU-only path](#cpu-only-local-sanity).

## Layout

| Path | What |
|---|---|
| [`VERSIONS.md`](VERSIONS.md) | Pinned images / models / chart. **Read first.** |
| [`GPU-RUNBOOK.md`](GPU-RUNBOOK.md) | GPU sizing (VRAM math), shape/card table, multi-card TP, fleet rollout. |
| [`kind/cluster.yaml`](kind/cluster.yaml) | Local kind cluster (NodePorts for API + ZMQ). |
| [`manifests/`](manifests/) | **GPU reference** Deployment + Service — the M5 template. |
| [`manifests/cpu-local/`](manifests/cpu-local/) | CPU-only sanity variant (no LMCache). |
| [`helm/values-reference.yaml`](helm/values-reference.yaml) | Upstream Production-Stack Helm path (cross-check). |
| [`scripts/`](scripts/) | ZMQ subscriber, prefix-cache-hit test, synthetic publisher. |
| `captures/` | Committed sample of the ZMQ stream + the cache-hit screenshot (DoD). |

## Prerequisites

```bash
brew install kind            # kubectl + helm assumed present
kind --version               # >= v0.23
```

For the GPU path you also need an NVIDIA GPU host (or OKE GPU node pool) and a
Hugging Face token for the gated Llama model.

---

## GPU run (the real DoD — OCI test/dev fleet)

> **Sizing first:** see [`GPU-RUNBOOK.md`](GPU-RUNBOOK.md) for how much VRAM /
> how many cards a given model needs and which shape to pick. For the 8B
> reference, **1× 24 GB card (e.g. OCI `VM.GPU.A10.1`)** is enough.

```bash
# 1. Cluster. On the GPU host, install the NVIDIA Container Toolkit + set the
#    nvidia Docker runtime as default FIRST, then:
kind create cluster --name inference-cache-substrate --config kind/cluster.yaml
#    Install the device plugin so pods can request nvidia.com/gpu:
helm repo add nvdp https://nvidia.github.io/k8s-device-plugin
helm install nvdp nvdp/nvidia-device-plugin -n kube-system
kubectl get nodes -o json | jq '.items[].status.allocatable["nvidia.com/gpu"]'   # expect "1"
#    (Or skip kind and use an OKE GPU node pool — the manifests are identical.)

# 2. HF token secret (gated model).
kubectl create namespace cache-substrate
kubectl -n cache-substrate create secret generic hf-token --from-literal=token="$HF_TOKEN"

# 3. Deploy the reference backend.
kubectl apply -f manifests/namespace.yaml
kubectl apply -f manifests/deployment.yaml
kubectl apply -f manifests/service.yaml
kubectl -n cache-substrate rollout status deploy/vllm-lmcache-llama-8b --timeout=20m

# 4. Subscribe to the ZMQ KV-event stream and capture a sample.
pip install -r scripts/requirements.txt
python scripts/kv_events_subscriber.py --endpoint tcp://localhost:30557 \
    --topic kv-events --max 200 --json | tee captures/kv-events-sample.jsonl

# 5. Demonstrate the prefix-cache hit (run while the subscriber is watching).
./scripts/prefix_cache_hit_test.sh           # screenshot the output -> captures/
```

**Expected:** request 2 is faster than request 1, `prefix_cache_hits` delta > 0,
and the subscriber prints `BlockStored` events with block hashes during request 1.

### Upstream Helm cross-check (optional)

`helm/values-reference.yaml` deploys the same stack via the vLLM Production
Stack chart — useful to confirm our hand-written manifests match upstream. We
disable the chart's router (the gateway owns routing; we only describe cache
state).

---

## CPU-only local sanity

Validates the **ZMQ event wiring** and a **prefix-cache hit** without a GPU and
without LMCache. Uses a tiny ungated model on stock vLLM.

```bash
kind create cluster --name inference-cache-substrate --config kind/cluster.yaml
kubectl apply -f manifests/namespace.yaml
kubectl apply -f manifests/cpu-local/deployment.yaml
kubectl -n cache-substrate rollout status deploy/vllm-cpu-sanity --timeout=30m
python scripts/kv_events_subscriber.py --endpoint tcp://localhost:30557 --topic kv-events &
MODEL=Qwen/Qwen2.5-0.5B-Instruct ./scripts/prefix_cache_hit_test.sh
```

> **Apple Silicon caveat.** Published vLLM images are `linux/amd64`; on an ARM
> Mac they run under emulation — correct but very slow, and model load can time
> out. If the engine won't start, validate just the event plumbing + subscriber
> decode with the synthetic publisher:
>
> ```bash
> python scripts/kv_events_synthetic_publisher.py --bind 'tcp://*:5557' &
> python scripts/kv_events_subscriber.py --endpoint tcp://localhost:5557
> ```
>
> This proves the ZMQ framing and the C1-facing decode path; it does **not**
> capture real engine events (do that on the GPU fleet).

---

## Definition of Done (CAC-13)

- [ ] vLLM+LMCache runs on kind from `manifests/` — **GPU fleet run**.
- [ ] KV-cache events observed over ZMQ — `captures/kv-events-sample.jsonl`.
- [ ] Cache hit demonstrated + screenshotted — `captures/`.
- [ ] Manifests committed here (input for C2/M5). ✅ (this directory)

## Teardown

```bash
kind delete cluster --name inference-cache-substrate
```
