# SGLang + LMCache (MP mode) reference stack

The second-engine sibling of the top-level [vLLM + LMCache reference](../../README.md):
a **SGLang** deployment that publishes **KV-cache events over ZMQ** and **offloads
KV to a shared Redis L2** via LMCache **multiprocess (MP) mode**. It is the hand-built
reference the `(sglang, LMCache)` runtime adapter (`pkg/adapters/runtime/sglang`)
mirrors: [`deployment.yaml`](deployment.yaml) stands up the same shape the adapter
auto-injects — a Redis L2 store, the engine with `--enable-lmcache` +
`--lmcache-config-file`, and a **node-local MP-worker native sidecar** that offloads
to that Redis. The engine image / `--model-path` / resources / `--kv-events-config`
are operator-owned scaffolding the adapter assumes is present, so the file as a whole
is **not** byte-for-byte adapter output.

> **Validation status.** This manifest is **derived from the GPU-validated adapter
> render** (`sglang_mp.go` + `redis_l2.go`; the controller-rendered managed path was
> validated store→flush→retrieve end-to-end in the MP-mode increment) and is
> **structurally checked** (`kubectl apply --dry-run`). It has **not** been
> independently re-run end-to-end on a GPU in this exact hand shape — run it on a GPU
> host (below) before treating it as a golden reference. All pins live in
> [`../../VERSIONS.md`](../../VERSIONS.md).

> **Kubernetes ≥ 1.29 required.** The MP worker is a **native sidecar** (an
> `initContainers` entry with `restartPolicy: Always`), which older apiservers do not
> understand.

## Why this exists (and what's already validated)

SGLang adopted vLLM's KV-event wire wholesale: `--kv-events-config` drives a ZMQ
`ZmqEventPublisher` emitting the **same** msgspec `BlockStored` / `BlockRemoved` /
`AllBlocksCleared` event structs vLLM does (the batch envelope adds a trailing
`attn_dp_rank` the decoder ignores). Two consequences:

1. **The event-decode path is engine-agnostic and already covered by tests.** The
   shipped `kvevent-subscriber` decodes SGLang's stream unchanged; the only difference
   is the `--hash-scheme=sglang` tag. The Go decoder is exercised against a synthetic
   SGLang-shaped frame in `pkg/adapters/engine/sglang_wire_test.go`, and the
   cross-engine isolation (`hash_scheme` keeps SGLang and vLLM prefixes disjoint) in
   `pkg/index` (`TestNoCrossEngineFalseHitVLLMvsSGLang`).
2. **You can validate the wire off-GPU** (below): the Go test covers SGLang's exact
   wire shape; the Python synthetic tooling covers the shared decode/redaction logic.

What this reference adds on top of those tests is the **real engine → ZMQ events +
LMCache offload** path on a GPU: a live SGLang pod publishing real
`BlockStored`/`BlockRemoved` frames while the MP worker offloads evicted KV to Redis.
Extending that to **index + `LookupRoute`** additionally requires the cache plane
installed and a `CacheBackend` (`engine: sglang`, `type: LMCache`) whose
`engineSelector` matches these pods and whose `backendConfig.model` is set, so the
controller auto-attaches the `kvevent-subscriber` sidecar (see
`config/samples/cachebackend-sglang.yaml` and the install docs). The standalone
manifest ships no event consumer, so on its own it shows the engine serving + the
publisher started + KV offloading to Redis, **not** a populated index.

## How the MP data plane fits together

```
          ┌─────────────────────── engine pod ───────────────────────┐
          │  [initContainers]                                          │
          │   lmcache-mp-worker  (native sidecar, restartPolicy:Always)│
          │     writes /etc/lmcache/config.yaml (mp_host/mp_port)      │
          │     runs the LMCache MP server on 127.0.0.1:5555           │──resp──▶ redis-l2
          │       ▲ CUDA-IPC + /dev/shm (L1)                           │        (ClusterIP,
          │  [containers]                                              │         shared L2)
          │   sglang  --enable-lmcache --lmcache-config-file <path>    │
          │     --kv-events-config (ZMQ :5557) ────────────────────────┼──▶ kvevent-subscriber
          └────────────────────────────────────────────────────────────┘     (managed path)
```

The engine dials the **local** worker over `mp_host`/`mp_port` (never the Redis
endpoint directly); the worker holds the L1 in `/dev/shm` and offloads its shared
tier to Redis over the `resp` `--l2-adapter`. `lm://` is **not** a valid MP
`--l2-adapter` type, which is why the shared tier is Redis, not the standalone
`lmcache-server` the vLLM path uses.

## Engine-side differences from the vLLM reference

| | vLLM | SGLang |
|---|---|---|
| LMCache on | `--kv-transfer-config '{"kv_connector":"LMCacheConnectorV1",…}'` | `--enable-lmcache` (bare flag) + `LMCACHE_USE_EXPERIMENTAL=True` |
| LMCache config source | `LMCACHE_*` env (`LMCACHE_REMOTE_URL`, …) | **`--lmcache-config-file`** (written by the MP worker); the `LMCACHE_*` env is ignored |
| Shared tier | standalone `lm://` `lmcache-server` | **Redis L2** behind a node-local **MP worker** (`resp` `--l2-adapter`) |
| vLLM-only env | `VLLM_USE_V1=1`, `PYTHONHASHSEED=0` | *(neither — no v1 codepath; SGLang sha256-hashes, independent of `PYTHONHASHSEED`)* |
| Default HTTP port | 8000 | 30000 |
| KV-event wire | ZMQ `BlockStored`/`BlockRemoved`/`AllBlocksCleared` | same event structs; batch envelope adds a trailing `attn_dp_rank` the decoder ignores |

## Deploy and test on a GPU

> Needs an NVIDIA GPU host (or a managed GPU cluster advertising `nvidia.com/gpu`)
> and a Hugging Face token for the gated reference model. Size the GPU the same way
> as the vLLM path — see [`../../GPU-RUNBOOK.md`](../../GPU-RUNBOOK.md); the 8B
> reference model fits on a single 24 GB card.

This manifest is a **standalone** reference showing the hand-built shape the adapter
mirrors, with **no controller or `CacheBackend` in the loop** — the engine reads its
MP config from the worker's `--lmcache-config-file`, and the worker offloads to the
in-namespace `redis-l2` Service.

Run the commands below from this directory
(`docs/reference-stack/manifests/sglang-lmcache/`) — the relative paths
(`../../kind/cluster.yaml`, `deployment.yaml`) assume it.

```bash
# 1. Cluster + GPU device plugin (identical to the vLLM path).
kind create cluster --name inference-cache-substrate --config ../../kind/cluster.yaml
helm repo add nvdp https://nvidia.github.io/k8s-device-plugin
helm install nvdp nvdp/nvidia-device-plugin -n kube-system

# 2. Fix the SGLang image in deployment.yaml: replace the ENTIRE placeholder
#    reference `example.invalid/sglang-lmcache@sha256:0000...` (in BOTH the
#    lmcache-mp-worker init container and the sglang engine container) with your
#    real DERIVED image (repo AND digest) — the base sglang image does not bundle
#    the lmcache client, so you must build one (`pip install lmcache==0.5.1` onto the
#    GPU-validated cu13 base). See the deployment.yaml header + VERSIONS.md. Redis is
#    a normal pullable tag (digest-pin for production). Then create the namespace +
#    HF token secret (idempotent so the runbook re-runs cleanly).
kubectl create namespace cache-substrate --dry-run=client -o yaml | kubectl apply -f -
kubectl -n cache-substrate create secret generic hf-token --from-literal=token="$HF_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Deploy the Redis L2 + the SGLang engine (with its MP-worker sidecar).
#    Wait for BOTH rollouts. The engine rollout only goes green once the MP worker's
#    startupProbe passes (the ZMQ server is listening on 127.0.0.1:5555) — SGLang has
#    no cacheless fallback while --enable-lmcache is on, so the worker is a serving
#    prerequisite, not a soft dependency.
kubectl apply -f deployment.yaml
kubectl -n cache-substrate rollout status deploy/redis-l2 --timeout=5m
kubectl -n cache-substrate rollout status deploy/sglang-lmcache-llama-8b --timeout=20m

# 4. Drive the SAME long-prefix prompt twice (first warms, second reuses). This
#    exercises the engine, triggers its KV-event publisher on :5557, and — because
#    MP mode is write-through — offloads stored KV to Redis immediately (not only on
#    HBM eviction; see the DBSIZE check below). Swap the `prefix = ...` line
#    for a REAL prompt long enough to span several KV blocks (>> the engine's
#    --page-size in tokens); a short literal won't reliably produce BlockStored /
#    prefix reuse.
BODY="$(python3 - <<'PY'
import json
prefix = "You are a helpful assistant. " * 200   # ~1k+ tokens of shared prefix
print(json.dumps({"model": "meta-llama/Meta-Llama-3-8B-Instruct",
                  "messages": [{"role": "user", "content": prefix}],
                  "max_tokens": 16, "temperature": 0}))
PY
)"
kubectl -n cache-substrate port-forward svc/sglang-lmcache-llama-8b 30000:30000 &
pf=$!; trap 'kill "$pf" 2>/dev/null' EXIT   # clean up the port-forward (frees :30000)
ready=""
for _ in $(seq 60); do
  kill -0 "$pf" 2>/dev/null || { echo "FAIL: port-forward exited early (is the pod running?)"; exit 1; }
  curl -sf localhost:30000/health >/dev/null 2>&1 && { ready=1; break; }
  sleep 2
done
[ -n "$ready" ] || { echo "FAIL: engine /health not ready after ~120s"; exit 1; }
rc=0
for i in 1 2; do
  # -sfS: fail (non-zero) on HTTP 4xx/5xx so a rejected request doesn't look served.
  curl -sfS localhost:30000/v1/chat/completions -H 'content-type: application/json' -d "$BODY" >/dev/null \
    || { echo "FAIL: request $i was not served (HTTP error)"; rc=1; break; }
done
kill "$pf" 2>/dev/null; trap - EXIT
[ "$rc" -eq 0 ] || exit 1
```

### What success looks like (standalone)

- **The engine serves both requests AND its KV-event publisher started.** Assert the
  publisher concretely from the engine logs (so a mis-configured `--kv-events-config`
  fails the check rather than silently passing):

  ```bash
  # Match a publisher-STARTUP line, not a config/arg echo: SGLang echoes
  # "--kv-events-config"/"kv-events" at boot, so grepping for "kv-events" would
  # false-pass on the echo alone. Adjust the pattern to your SGLang build's phrasing
  # (confirm against a real run's logs the first time).
  kubectl -n cache-substrate logs deploy/sglang-lmcache-llama-8b -c sglang \
    | grep -iE 'zmq.*publish|publisher thread|start(ing|ed).*publisher' \
    || { echo "FAIL: KV-event publisher did not start (check --kv-events-config)"; exit 1; }
  ```

- **KV offloaded to Redis.** LMCache MP mode is **write-through**: a stored prefix
  lands in the L2 immediately, not only on HBM eviction (GPU-validated — a 3760-token
  prompt took Redis `DBSIZE` 0→14, i.e. 14 chunks of the 256-token `chunk_size`). So
  after driving the prompts above, assert the L2 keyspace is non-empty:

  ```bash
  # The MP worker offloads to redis-l2 over `resp`. dbsize > 0 after requests is the
  # standalone signal that offload is actually happening.
  n=$(kubectl -n cache-substrate exec deploy/redis-l2 -c redis-l2 -- redis-cli dbsize | tr -dc '0-9')
  [ "${n:-0}" -gt 0 ] || { echo "FAIL: Redis L2 empty after requests — offload not happening"; exit 1; }
  echo "Redis L2 holds $n KV chunks"
  ```

  (Full frame→index→`LookupRoute` verification needs the **managed path** below; the
  standalone manifest ships no event consumer, so it never populates the index.)

- **Privacy boundary — the RAW ZMQ frames carry `token_ids`.** Both vLLM's and
  SGLang's `BlockStored` wire includes the block's token ids; the "metadata-only,
  never token content" guarantee is about what the IC `kvevent-subscriber` *reports to
  the policy server* — it hashes the token_ids in-pod into the content fingerprint and
  forwards only hashes + counts (over `127.0.0.1`). That report-level guarantee holds
  regardless of how the publisher binds. The **raw ZMQ frames** are a separate matter:
  the manifest binds `tcp://*:5557` (the operator contract in
  [`docs/design/cachebackend-api.md`](../../../design/cachebackend-api.md), kept
  identical to the vLLM reference), so the raw, token-bearing frames **are** reachable
  by any in-cluster pod that knows this pod's IP. Two things bound that exposure: the
  Service deliberately exposes **only** the HTTP API (never `:5557`), and — if
  in-cluster raw-frame access is a concern — you can add a **NetworkPolicy** restricting
  `:5557` to this pod (the in-pod subscriber reaches the publisher over `127.0.0.1`, so
  it is unaffected). If you must inspect the raw stream during dev, `kubectl
  port-forward` `:5557` yourself rather than adding it to the Service.

- **Why the two `scripts/` helpers don't verify SGLang here:**
  `scripts/kv_events_subscriber.py` decodes only vLLM's 2-tuple synthetic frames (it
  would print `UNDECODED` on SGLang's 3-tuple), and `scripts/prefix_cache_hit_test.sh`
  reads vLLM's `vllm:prefix_cache_hits` counter SGLang doesn't emit (use it only as a
  request driver). The shipped live SGLang consumer is the managed Go sidecar below.

### The managed path (what the adapter automates)

In a real install you do **not** hand-write this manifest. You create a `CacheBackend`
(`engine: sglang`, `type: LMCache`; see
[`config/samples/cachebackend-sglang.yaml`](../../../../config/samples/cachebackend-sglang.yaml))
whose `engineSelector` matches your SGLang pods, and the controller renders the **Redis
L2** store, injects the **MP-worker sidecar + `--enable-lmcache` + `--lmcache-config-file`**
onto the engine pod, and — with `--kvevent-subscriber-image` set — auto-attaches the Go
`kvevent-subscriber` sidecar (tagged `--hash-scheme=sglang`) that reports to the index,
enabling `LookupRoute` to return SGLang replicas (never a vLLM replica on the same
prefix bytes — the `hash_scheme` tag keeps them disjoint). Two caveats the manual
manifest sidesteps but the managed path must honor: the CacheBackend must live in the
**engine pods' namespace** (the Pod webhook matches per-namespace), and because
injection is **create-time only**, the CacheBackend's `status.endpoint` (the Redis L2
address) must be **published** before the engine pod is created, or the pod admits
unwired and must be recreated. (The precondition is specifically `status.endpoint`,
**not** `Ready` — managed `Ready` is gated on the first KV event observed *from these
very pods*, so waiting for `Ready` first would be circular.) The served model, the
CacheBackend's `backendConfig.model`, and the request's `model` must all agree, or the
index keys per-model and `LookupRoute` silently misses. **Block-size caveat:** for
raw-`token_ids`/`prompt_text` lookups the server fingerprints at its single global
`--engine-block-size` (default 16, vLLM's); SGLang's page size (e.g. 64) must match it,
or gateways must send pre-computed `prefix_hash`/`block_hashes` — otherwise
`LookupRoute` silently misses even with events flowing (see the "Block-size alignment"
note in [`docs/design/cachebackend-api.md`](../../../design/cachebackend-api.md)). This
managed wiring is exercised by the **controller/webhook envtests** — the SGLang
pod-injection, reserved-override, and admission tests — not reproduced step-by-step
here. (The install-smoke gate additionally admits the
`config/samples/cachebackend-sglang.yaml` shape against a real-cluster webhook via its
all-samples backstop, though it does not yet drive the SGLang reconcile or the full
inject→index→`LookupRoute` flow.)

## Validate the event wire WITHOUT a GPU

Two complementary off-GPU checks, with an important scope distinction:

1. **The shared decode + token-redaction path** (Python, no image/cluster). The
   `kv_events_synthetic_publisher.py` emits the **2-field** `[ts, events]` EventBatch
   envelope (vLLM's shape), so it exercises the decode + token-redaction logic common
   to both engines — not SGLang's exact envelope:

   ```bash
   pip install -r ../../scripts/requirements.txt
   python ../../scripts/kv_events_synthetic_publisher.py --bind 'tcp://*:5557' &
   pub=$!; trap 'kill "$pub" 2>/dev/null' EXIT   # background publisher; freed on exit even if a step below fails
   python ../../scripts/kv_events_subscriber.py --endpoint tcp://localhost:5557 --max 4
   python ../../scripts/test_kv_events.py   # asserts token_ids never surfaces; token_count kept
   kill "$pub" 2>/dev/null; trap - EXIT
   ```

2. **SGLang's exact wire shape** (Go, run from the repo root). SGLang's real envelope
   is the **3-tuple** `[ts, events, attn_dp_rank]` with a 6-field `BlockStored`; that
   shape — and that the subscriber's decoder tolerates the trailing `attn_dp_rank` and
   tags reports `hash_scheme=sglang` — is asserted by the Go fixture the shipped
   subscriber actually uses (from the repo root):
   `go test ./pkg/adapters/engine/ -run SGLang`
   (`TestDecodeSGLangEventBatch` + `TestReporterTagsSGLangScheme`). The Python synthetic
   path above does **not** cover the 3-tuple; rely on the Go test for the
   SGLang-specific envelope.

## LMCache MP mode (how the offload works)

SGLang drives LMCache in **MP (multiprocess) mode**, not the `lm://` remote-server
model the vLLM path uses. It ignores the `LMCACHE_*` env and reads config only from the
`--lmcache-config-file` flag, which points at the file the **MP worker** writes
(`mp_host: 127.0.0.1`, `mp_port: 5555`). The worker runs the LMCache MP server on
loopback, holds the L1 in `/dev/shm`, and offloads its shared tier to the Redis L2 over
the `resp` `--l2-adapter` (`lm://` is not a valid MP `--l2-adapter` type). The engine
attaches to the local worker over CUDA-IPC + shared memory; because they speak the MP
wire to each other, the worker defaults to the **same image** as the engine (same
lmcache version) — `backendConfig.workerImage` overrides it, at which point keeping the
two lmcache versions aligned is yours. Authoritative design + GPU-validation evidence:
[`docs/design/sglang-lmcache-mp-mode.md`](../../../design/sglang-lmcache-mp-mode.md);
key/field reference: [`docs/design/cachebackend-api.md`](../../../design/cachebackend-api.md)
(SGLang engine support).

## Teardown

```bash
kind delete cluster --name inference-cache-substrate
```
