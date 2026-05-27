# GPU deployment runbook — sizing & rollout

How to deploy the [`manifests/`](manifests/) reference stack once a GPU host is
available, and how to size it: **GPU memory, card count, tensor-parallelism, and
host resources**.

The stack is cloud-neutral — it needs an NVIDIA GPU advertising `nvidia.com/gpu`,
nothing more. §4 gives a concrete OCI-shape mapping as the worked example for the
test/dev fleet; any equivalent NVIDIA card works.

---

## 1. How to size a deployment

Three things consume GPU memory. Budget them against one card's VRAM (or the sum
across cards under tensor-parallelism):

```
GPU memory needed ≈ weights + KV-cache pool + framework overhead

  weights        = params × bytes/param        (BF16 = 2 B; FP8/INT8 ≈ 1 B; INT4 ≈ 0.5 B)
  KV per token   = 2 × layers × kv_heads × head_dim × bytes/elem
  KV pool        = (concurrent_seqs × avg_context_tokens) × KV-per-token
  overhead       ≈ 2–4 GB (activations, CUDA graphs, fragmentation)
```

vLLM reserves `gpu_memory_utilization` (default **0.90**) of each card, loads the
weights, and turns whatever is left into the KV-block pool. So the usable KV pool
is roughly `0.90 × VRAM − weights`. If that is negative, the model **won't load** —
you need a bigger card or more cards (tensor-parallel).

### Worked example — the reference model (`meta-llama/Llama-3.1-8B-Instruct`, BF16)

- **Weights:** 8.03B × 2 B ≈ **16.1 GB**.
- **KV per token:** 2 × 32 layers × 8 KV-heads × 128 head_dim × 2 B = **128 KiB/token**
  (so 16K tokens of context ≈ 2 GB for a single sequence).
- **On a 24 GB A10:** 0.90 × 24 − 16.1 ≈ **5.5 GB** KV pool ≈ ~45K cached tokens.
  Runs the PoC fine; tight if you push 16K context at high concurrency.
- **On a 48 GB L40S:** ~27 GB KV pool — comfortable, and leaves host headroom for
  LMCache offload. **This is the recommended PoC card.**

> **LMCache changes the GPU math only indirectly.** LMCache offloads KV blocks
> off the GPU into **host RAM / disk**, so it relieves GPU KV pressure but adds a
> **host-memory** requirement (see §3). It does not reduce the weights footprint.

---

## 2. Recommended GPU per model size

Generic NVIDIA guidance (precision = BF16 unless noted). "Cards" = minimum for a
healthy KV pool, not the absolute floor.

| Model size | Min VRAM (weights+headroom) | Cards | Tensor-parallel | Example card(s) |
|---|---|---|---|---|
| ~8B (reference) | 24 GB | 1 | `--tensor-parallel-size 1` | 1× A10 24G / **L40S 48G** |
| ~13–34B | 48–80 GB | 1 | 1 | 1× L40S 48G / A100 80G |
| ~70B BF16 | ~160–200 GB | 2–4 | `2`–`4` | 2× H200 141G **or** 4× A100 80G |
| ~70B FP8/INT4 | 48–80 GB | 1–2 | `1`–`2` | 1× H100/H200 / 2× L40S |
| 100B+ / MoE | 320 GB+ | 8 | `8` | 8× H100/H200 (single BM node) |

Rules of thumb:
- **Card count** is driven first by *weights fitting* (must hold weights/TP across
  cards), then by *KV pool size* (more cards = bigger pool = more concurrency).
- **Multi-card needs NVLink/NVSwitch** for good tensor-parallel throughput — use a
  **bare-metal** shape, not a multi-GPU VM, for TP ≥ 2.
- `--tensor-parallel-size` **must equal the number of GPUs** in the pod, and the
  model's attention-head count must be divisible by it.

---

## 3. Non-GPU node requirements

| Resource | Reference (8B) | Why |
|---|---|---|
| Host RAM | ≥ 32 GB free | `LMCACHE_MAX_LOCAL_CPU_SIZE=20` GiB CPU offload tier + OS/engine. Scale with the offload buffer. |
| `/dev/shm` | ≥ 8 GiB (set in `manifests/deployment.yaml`) | vLLM uses shared memory for tensor/IPC; small `/dev/shm` causes cryptic NCCL/loader hangs. |
| Local disk | model size × 1.5 + LMCache disk tier | HF weight cache + optional LMCache disk offload. ~30 GB for 8B; size up for 70B. |
| Network | 100 Gb+ RDMA for multi-node | Only if you later shard across nodes; single-node TP uses NVLink. |
| Driver/runtime | NVIDIA driver + Container Toolkit; `nvidia` default Docker runtime | So kind/OKE pods can request `nvidia.com/gpu`. |

---

## 4. Worked example — OCI test/dev GPU fleet

Concrete shapes for the fleet (any equivalent NVIDIA card elsewhere works the
same). GPU memory **per card**: A10 = 24 GB, L40S = 48 GB, A100 = 80 GB (also a
40 GB variant), H100 = 80 GB, H200 = 141 GB.

| Target | OCI shape | Cards × VRAM | Good for |
|---|---|---|---|
| **PoC / this reference (recommended)** | `VM.GPU.A10.1` | 1 × 24 GB | 8B, single card, cheapest path to green |
| PoC with headroom | `BM.GPU.L40S.4` (use 1 GPU) | 4 × 48 GB | 8B–34B comfortably; room for LMCache |
| Single bigger model | `VM.GPU.A100.1` / `VM.GPU.H100.1` | 1 × 80 GB | up to ~34B, or 70B quantized |
| 70B BF16 (TP) | `BM.GPU4.8` / `BM.GPU.A100-v2.8` | 8 × 40/80 GB (use 4, NVLink) | `--tensor-parallel-size 4` |
| Largest / fastest | `BM.GPU.H100.8` / `BM.GPU.H200.8` | 8 × 80/141 GB | 70B–100B+, full-node TP |

For the **8B reference**, `VM.GPU.A10.1` is the cheapest way to satisfy the
CAC-13 DoD. Pick a bare-metal multi-GPU shape only when you need TP ≥ 2.

---

## 5. Deploy (once the GPU node is up)

Builds on [`README.md`](README.md) "GPU run". Summary:

```bash
# 0. Prereqs on the GPU host: NVIDIA driver + Container Toolkit, `nvidia` as the
#    default Docker runtime. Then a cluster (kind per kind/cluster.yaml, or OKE
#    GPU node pool) and the device plugin:
helm repo add nvdp https://nvidia.github.io/k8s-device-plugin
helm install nvdp nvdp/nvidia-device-plugin -n kube-system
kubectl get nodes -o json | jq '.items[].status.allocatable["nvidia.com/gpu"]'   # must be >= 1

# 1. Pin the image to a digest (VERSIONS.md), set the HF token secret:
kubectl create namespace cache-substrate
kubectl -n cache-substrate create secret generic hf-token --from-literal=token="$HF_TOKEN"

# 2. Apply. For multi-card, bump replicas/GPU + add --tensor-parallel-size (see below).
kubectl apply -f manifests/namespace.yaml -f manifests/deployment.yaml -f manifests/service.yaml
kubectl -n cache-substrate rollout status deploy/vllm-lmcache-llama-8b --timeout=20m
```

**Multi-card (TP ≥ 2):** the single-card reference manifest requests 1 GPU. For
tensor-parallelism, edit `manifests/deployment.yaml`:

```yaml
        args:
          - "--tensor-parallel-size=4"   # = number of GPUs in the pod
          # ... existing args ...
        resources:
          limits:
            nvidia.com/gpu: "4"           # match tensor-parallel-size
```

Then run the [§4 verification in README](README.md): subscribe with
`scripts/kv_events_subscriber.py` and fire `scripts/prefix_cache_hit_test.sh`.

## 6. Sizing-related failure cheatsheet

| Symptom | Likely cause | Fix |
|---|---|---|
| Pod OOMs / `No available memory for the cache blocks` on load | Weights + overhead > usable VRAM | bigger card, or add cards + `--tensor-parallel-size` |
| Loads but low throughput / frequent recompute | KV pool too small | bigger card, raise `gpu_memory_utilization`, or lean on LMCache offload |
| `tensor-parallel-size` mismatch / hang at startup | TP ≠ GPU count, or heads not divisible | set TP = `nvidia.com/gpu`; check head count divisibility |
| NCCL / loader hang on multi-GPU | small `/dev/shm`, or no NVLink (multi-GPU VM) | raise `/dev/shm`; use a bare-metal NVLink shape for TP |
| Host OOM with LMCache enabled | `LMCACHE_MAX_LOCAL_CPU_SIZE` > free host RAM | lower the buffer or pick a higher-RAM shape |
