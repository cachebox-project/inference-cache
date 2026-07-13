# SGLang + LMCache reference stack

The second-engine sibling of the top-level [vLLM + LMCache reference](../../README.md):
a **SGLang** deployment publishing **KV-cache events over ZMQ** (the working
reference leg). Its **LMCache offload** leg is a **non-working historical
template** — GPU validation (2026-07) found it wired to the wrong LMCache mode
(see the KNOWN LIMITATION below); treat it as a record of the shipped topology,
not a serving cache. It is the hand-built
reference the `(sglang, LMCache)` runtime adapter
(`pkg/adapters/runtime/sglang`) was templated from: in [`deployment.yaml`](deployment.yaml)
the SGLang container's **LMCache wiring** (`--enable-lmcache` + the `LMCACHE_*` /
`INFERENCECACHE_FAIL_OPEN` env) mirrors what the adapter auto-injects. The rest
of the manifest — image, resources, the lmcache-server, `--kv-events-config` —
is operator-owned scaffolding the adapter assumes is already present, so the
file as a whole is **not** byte-for-byte adapter output.

> **KNOWN LIMITATION — the `lm://` LMCache offload wiring here is wrong for SGLang (GPU-validated 2026-07).** The `LMCACHE_REMOTE_URL` / standalone `lmcache-server` wiring below mirrors vLLM+LMCache, but SGLang drives LMCache in **multiprocess (MP) mode**: config from the **`--lmcache-config-file` flag** (the `LMCACHE_*` env is ignored) and a **node-local** worker addressed by `mp_host`/`mp_port` over shared memory — not a cluster-reachable `lm://` server. Applied as written, the SGLang engine **hangs at startup** (a `remote_url: lm://…` config never finishes init) or refuses to start without `--lmcache-config-file`. Use this manifest to understand the KV-event path and the injection surface, **not** as a working LMCache offload. The correct MP-mode wiring (`--lmcache-config-file` + a per-node worker) is a pending fix; see `docs/design/cachebackend-api.md` (SGLang engine support). The "Status" note below predates this validation.

> **Status.** The **LMCache-offload leg** HAS since been GPU-validated (2026-07) and
> found **broken** — see the KNOWN LIMITATION above. The **KV-event leg** below
> remains an **unvalidated template**: no GPU run has exercised it end-to-end.
> Treat that leg as a **template**, not a known-good run. Applied, it is
> *designed to* stand up the real SGLang → ZMQ **KV-event** path, and the
> standalone flow's intended check is only that the publisher **starts** (from
> engine logs) — it does **not** consume or assert `BlockStored` frames (there is
> no standalone consumer). The one thing that **is** actually executed (off-GPU)
> is SGLang's exact wire *shape*, via the Go test
> (`go test ./pkg/adapters/engine/ -run SGLang`). The **LMCache offload** leg
> (`LMCACHE_REMOTE_URL`) was GPU-validated and found **broken** — SGLang uses
> LMCache MP mode, not this `lm://` env wiring (see
> [Wire-test caveat — resolved](#wire-test-caveat-resolved)).

## Why this exists (and what's already validated)

SGLang adopted vLLM's KV-event wire wholesale: `--kv-events-config` drives a ZMQ
`ZmqEventPublisher` emitting the **same** msgspec `BlockStored` / `BlockRemoved`
/ `AllBlocksCleared` event structs vLLM does (the batch envelope adds a trailing
`attn_dp_rank` the decoder ignores). Two consequences:

1. **The event-decode path is engine-agnostic and already covered by tests.** The
   shipped `kvevent-subscriber` decodes SGLang's stream unchanged; the only
   difference is the `--hash-scheme=sglang` tag. The Go decoder is exercised
   against a synthetic SGLang-shaped frame in
   `pkg/adapters/engine/sglang_wire_test.go`, and the cross-engine isolation
   (`hash_scheme` keeps SGLang and vLLM prefixes disjoint) in
   `pkg/index` (`TestNoCrossEngineFalseHitVLLMvsSGLang`).
2. **You can validate off-GPU** (below): the Go test covers SGLang's exact wire
   shape; the Python synthetic tooling covers the shared decode/redaction logic.

What this reference stack adds on top of those tests is the **real engine →
ZMQ events** path on a GPU — a live SGLang+LMCache pod publishing real
`BlockStored`/`BlockRemoved` frames a subscriber can read. Extending that all
the way to **index + `LookupRoute`** additionally requires the cache plane
installed and a `CacheBackend` (`engine: sglang`, `type: LMCache`) whose
`engineSelector` matches these pods and whose `backendConfig.model` is set, so
the controller auto-attaches the `kvevent-subscriber` sidecar (see
`config/samples/cachebackend-sglang.yaml` and the install docs). The raw
`kubectl apply` below stands up only the engine + lmcache-server, so from these
steps alone the verifiable outcome is the engine serving traffic with its
**KV-event publisher started** (asserted from the engine logs below) — not the
consumed event stream (the standalone manifest ships no consumer) and not the
populated index; the index/`LookupRoute` criterion below assumes the controller
+ that `CacheBackend` are also present.

## Engine-side differences from the vLLM reference

| | vLLM | SGLang |
|---|---|---|
| LMCache on | `--kv-transfer-config '{"kv_connector":"LMCacheConnectorV1",…}'` | `--enable-lmcache` (bare flag) + `LMCACHE_USE_EXPERIMENTAL=True` |
| vLLM-only env | `VLLM_USE_V1=1`, `PYTHONHASHSEED=0` | *(neither — no v1 codepath; SGLang sha256-hashes, independent of `PYTHONHASHSEED`)* |
| Default HTTP port | 8000 | 30000 |
| KV-event wire | ZMQ `BlockStored`/`BlockRemoved`/`AllBlocksCleared` | same event structs; batch envelope adds a trailing `attn_dp_rank` the decoder ignores |
| LMCACHE_* tunables | same | same |

## Deploy and test on a GPU

> Needs an NVIDIA GPU host (or a managed GPU cluster advertising `nvidia.com/gpu`)
> and a Hugging Face token for the gated reference model. Size the GPU the same
> way as the vLLM path — see [`../../GPU-RUNBOOK.md`](../../GPU-RUNBOOK.md); the
> 8B reference model fits on a single 24 GB card.

This manifest is a **standalone** reference: the SGLang engine is wired to the
**bundled `lmcache-server`** by the manifest itself (the `LMCACHE_REMOTE_URL` env
points at the in-namespace Service), with **no controller or `CacheBackend` in
the loop**. It stands up the real engine and emits real ZMQ KV events — the
hand-built shape the adapter was templated from.

Run the commands below from this directory
(`docs/reference-stack/manifests/sglang-lmcache/`) — the relative paths
(`../../kind/cluster.yaml`, `deployment.yaml`) assume it.

```bash
# 1. Cluster + GPU device plugin (identical to the vLLM path).
kind create cluster --name inference-cache-substrate --config ../../kind/cluster.yaml
helm repo add nvdp https://nvidia.github.io/k8s-device-plugin
helm install nvdp nvdp/nvidia-device-plugin -n kube-system

# 2. Fix BOTH placeholder images in deployment.yaml: replace the ENTIRE SGLang
#    engine image reference `example.invalid/sglang-lmcache@sha256:0000...` with
#    your real derived image (repo AND digest), and swap the lmcache-server's
#    all-zero digest for its real one — see the deployment.yaml header +
#    VERSIONS.md. Then create the namespace + HF token secret (idempotent so the
#    runbook re-runs cleanly).
kubectl create namespace cache-substrate --dry-run=client -o yaml | kubectl apply -f -
kubectl -n cache-substrate create secret generic hf-token --from-literal=token="$HF_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Deploy the lmcache-server + SGLang engine (the manifest wires them together).
#    Wait for BOTH rollouts: fail-open means the engine can serve traffic even if
#    lmcache-server is unready or its image is bad, so a green engine rollout
#    alone would hide a broken cache server (offload silently never happens).
kubectl apply -f deployment.yaml
kubectl -n cache-substrate rollout status deploy/lmcache-server --timeout=5m
kubectl -n cache-substrate rollout status deploy/sglang-lmcache-llama-8b --timeout=20m

# 4. Drive the SAME long-prefix prompt twice (first warms, second reuses). This
#    exercises the engine and triggers its KV-event publisher on :5557;
#    CONSUMING/verifying the frames is the managed path below — there is no
#    standalone printer for SGLang's 3-tuple stream in this repo. Swap the
#    `prefix = ...` line below for a REAL prompt long enough to span several KV
#    blocks (>> the engine's --page-size in tokens); a short literal won't
#    reliably produce BlockStored / prefix reuse.
# Build the request body with python3 (no jq dependency); the long shared
# prefix is what triggers block storage + reuse.
BODY="$(python3 - <<'PY'
import json
prefix = "You are a helpful assistant. " * 200   # ~1k+ tokens of shared prefix
# Tiny, greedy generation — the point is prefix reuse (block storage), not the
# output; a short deterministic decode keeps the smoke fast and reproducible.
print(json.dumps({"model": "meta-llama/Meta-Llama-3-8B-Instruct",
                  "messages": [{"role": "user", "content": prefix}],
                  "max_tokens": 16, "temperature": 0}))
PY
)"
kubectl -n cache-substrate port-forward svc/sglang-lmcache-llama-8b 30000:30000 &
pf=$!; trap 'kill "$pf" 2>/dev/null' EXIT   # clean up the port-forward (frees :30000)
# Wait for the port-forward to serve /health, but don't spin forever if it dies
# (e.g. the pod never became ready) — bound the wait and bail if $pf has exited.
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
  # Capture that failure — otherwise the trailing (exit-0) kill masks a rejected
  # request and the whole block still "passes".
  curl -sfS localhost:30000/v1/chat/completions -H 'content-type: application/json' -d "$BODY" >/dev/null \
    || { echo "FAIL: request $i was not served (HTTP error)"; rc=1; break; }
done
kill "$pf" 2>/dev/null; trap - EXIT
[ "$rc" -eq 0 ] || exit 1
```

### What success looks like (standalone) — publisher-start smoke only

- **The verifiable standalone outcome is: the engine serves both requests AND
  its KV-event publisher started.** Assert the publisher concretely from the
  engine logs (so a mis-configured `--kv-events-config` fails the check rather
  than silently passing):

  ```bash
  # Match a publisher-STARTUP line, not a config/arg echo: SGLang echoes
  # "--kv-events-config"/"kv-events" at boot, so grepping for "kv-events" would
  # false-pass on the echo alone. If your SGLang build's startup log phrases the
  # publisher differently, adjust the pattern below (confirm it against a real
  # run's logs the first time).
  kubectl -n cache-substrate logs deploy/sglang-lmcache-llama-8b -c sglang \
    | grep -iE 'zmq.*publish|publisher thread|start(ing|ed).*publisher' \
    || { echo "FAIL: KV-event publisher did not start (check --kv-events-config)"; exit 1; }
  ```

  Full stream verification stops there standalone: this flow does **not** consume
  or assert the `BlockStored` frames themselves, **nor does it prove LMCache
  offload works** — and in fact SGLang does **not** store KV into `lmcache-server`
  over `LMCACHE_REMOTE_URL` at all: that offload wiring is broken (SGLang uses
  LMCache MP mode; see [Wire-test caveat — resolved](#wire-test-caveat-resolved)),
  so do not expect stores to arrive there. Observing the actual event stream needs a
  consumer (the managed Go sidecar below; this repo ships no standalone 3-tuple
  printer), and the decoder's correctness against SGLang's exact wire is covered
  offline by the Go test. End-to-end frame→index→`LookupRoute`
  verification requires the **managed controller/sidecar path** (see below), not
  this standalone deployment — even on a GPU, the standalone manifest has no
  consumer, so it never populates the index. This is an inherent limit of the
  consumer-less standalone manifest, not a gap to paper over.
- **Privacy boundary — the RAW ZMQ frames carry `token_ids`.** Both vLLM's and
  SGLang's `BlockStored` wire includes the block's token ids; the
  "metadata-only, never token content" guarantee is about what the IC
  `kvevent-subscriber` *reports to the policy server* — it hashes the token_ids
  in-pod into the content fingerprint and forwards only hashes + counts (over
  `127.0.0.1`). That report-level guarantee holds regardless of how the publisher
  binds. The **raw ZMQ frames** are a separate matter: the manifest binds
  `tcp://*:5557` (the operator contract in
  [`docs/design/cachebackend-api.md`](../../../design/cachebackend-api.md), kept
  identical to the vLLM reference), so the raw, token-bearing frames **are**
  reachable by any in-cluster pod that knows this pod's IP. Two things bound that
  exposure: the Service deliberately exposes **only** the HTTP API (never
  `:5557`), and — if in-cluster raw-frame access is a concern — you can add a
  **NetworkPolicy** restricting `:5557` to this pod (the in-pod subscriber reaches
  the publisher over `127.0.0.1`, so it is unaffected). If you must inspect the
  raw stream during dev, `kubectl port-forward` `:5557` yourself rather than
  adding it to the Service.
- **Why the two `scripts/` helpers don't verify SGLang here:**
  `scripts/kv_events_subscriber.py` decodes only vLLM's 2-tuple synthetic frames
  (it would print `UNDECODED` on SGLang's 3-tuple), and
  `scripts/prefix_cache_hit_test.sh` reads vLLM's `vllm:prefix_cache_hits`
  counter SGLang doesn't emit (use it only as a request driver). The shipped
  live SGLang consumer is the managed Go sidecar below.

### The managed path (what the adapter automates)

In a real install you do **not** hand-write this manifest. You create a
`CacheBackend` (`engine: sglang`, `type: LMCache`; see
[`config/samples/cachebackend-sglang.yaml`](../../../../config/samples/cachebackend-sglang.yaml))
whose `engineSelector` matches your SGLang pods, and the controller renders its
own lmcache-server, injects the engine's LMCache env (overwriting the manual
`LMCACHE_REMOTE_URL` with the managed endpoint), and — with
`--kvevent-subscriber-image` set — auto-attaches the Go `kvevent-subscriber`
sidecar (tagged `--hash-scheme=sglang`) that reports to the index, enabling
`LookupRoute` to return SGLang replicas (never a vLLM replica on the same prefix
bytes — the `hash_scheme` tag keeps them disjoint). Two caveats the manual
manifest sidesteps but the managed path must honor: the CacheBackend must live
in the **engine pods' namespace** (the Pod webhook matches per-namespace), and
because injection is **create-time only**, the CacheBackend's
`status.endpoint` must be **published** before the engine pod is created, or the
pod admits unwired and must be recreated. (The precondition is specifically
`status.endpoint`, **not** `Ready` — managed `Ready` is gated on the first KV
event observed *from these very pods*, so waiting for `Ready` first would be
circular.) The served model, the CacheBackend's
`backendConfig.model`, and the request's `model` must all agree, or the index
keys per-model and `LookupRoute` silently misses. **Block-size caveat:** for
raw-`token_ids`/`prompt_text` lookups the server fingerprints at its single
global `--engine-block-size` (default 16, vLLM's); SGLang's page size (e.g. 64)
must match it, or gateways must send pre-computed `prefix_hash`/`block_hashes` —
otherwise `LookupRoute` silently misses even with events flowing (see the
"Block-size alignment" note in [`docs/design/cachebackend-api.md`](../../../design/cachebackend-api.md)).
This managed wiring is
exercised by the **controller/webhook envtests** — the SGLang pod-injection,
reserved-override, and admission tests — not reproduced step-by-step here. (The
install-smoke gate's all-samples backstop additionally admits the
`config/samples/cachebackend-sglang.yaml` shape against a real-cluster webhook,
but it does not drive the full inject→index→`LookupRoute` flow.)

## Validate the event wire WITHOUT a GPU

Two complementary off-GPU checks, with an important scope distinction:

1. **The shared decode + token-redaction path** (Python, no image/cluster). The
   `kv_events_synthetic_publisher.py` emits the **2-field** `[ts, events]`
   EventBatch envelope (vLLM's shape), so it exercises the decode +
   token-redaction logic common to both engines — not SGLang's exact envelope:

   ```bash
   pip install -r ../../scripts/requirements.txt
   python ../../scripts/kv_events_synthetic_publisher.py --bind 'tcp://*:5557' &
   pub=$!; trap 'kill "$pub" 2>/dev/null' EXIT   # background publisher; freed on exit even if a step below fails
   python ../../scripts/kv_events_subscriber.py --endpoint tcp://localhost:5557 --max 4
   python ../../scripts/test_kv_events.py   # asserts token_ids never surfaces; token_count kept
   kill "$pub" 2>/dev/null; trap - EXIT
   ```

2. **SGLang's exact wire shape** (Go, run from the repo root). SGLang's real
   envelope is the **3-tuple** `[ts, events, attn_dp_rank]` with a 6-field
   `BlockStored`; that shape — and that the subscriber's decoder tolerates the
   trailing `attn_dp_rank` and tags reports `hash_scheme=sglang` — is asserted
   by the Go fixture the shipped subscriber actually uses (from the repo root):
   `go test ./pkg/adapters/engine/ -run SGLang`
   (`TestDecodeSGLangEventBatch` + `TestReporterTagsSGLangScheme`). The Python
   synthetic path above does **not** cover the 3-tuple; rely on the Go test for
   the SGLang-specific envelope.

## Wire-test caveat (resolved)

GPU validation (2026-07) answered this: the `LMCACHE_REMOTE_URL` / remote
`lmcache-server` topology this manifest wires is **wrong for SGLang, and must not
be applied as a working config.** SGLang ignores the `LMCACHE_*` env, reads config
only from the `--lmcache-config-file` flag, and drives LMCache in **MP
(multiprocess) mode** — a node-local worker reached over `mp_host`/`mp_port`
(default `5555`) + shared memory, not a cluster-reachable `lm://` server. A config
with `remote_url: lm://…` does **not** offload — it **hangs the engine at
startup**; with no config file the engine refuses to start
(`MP mode requires --lmcache-config-file`).

**Do not** mount a `remote_url: lm://…` config expecting it to work. The correct
wiring — a `--lmcache-config-file` carrying `mp_host`/`mp_port` plus a **per-node
LMCache MP worker** (which may itself carry a `remote_url` for a shared tier) — is
the tracked adapter follow-up. See `docs/design/cachebackend-api.md` (SGLang
engine support).

## Teardown

```bash
kind delete cluster --name inference-cache-substrate
```
