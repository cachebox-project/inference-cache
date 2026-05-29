# kvevent-subscriber wiring — shape decision

**Status:** Accepted · **Module:** C5 follow-up · **Owners:** controller + runtime adapters

## Problem

The substrate has two declarative halves:

* **Engine ↔ cache backend** — the C6 mutating Pod webhook injects `--kv-transfer-config`
  and `LMCACHE_*` env vars onto the engine container when a `CacheBackend` claims the pod
  (`internal/webhook/pod/podinjector.go`).
* **Engine ↔ policy server** — the engine publishes KV-cache events over ZMQ; a
  `kvevent-subscriber` process (`cmd/kvevent-subscriber`) consumes them and reports cache
  state to the policy server.

Today **nothing in the controller stands up the second half.** End-to-end runs (demos,
local kind canaries) bring up the subscriber out of band:

```
kubectl port-forward svc/cachebackend-cpu 5557:5557 &
./bin/kvevent-subscriber --engine-endpoint tcp://127.0.0.1:5557 \
  --replica-id ... --model-id ... --hash-scheme vllm ...
```

A backend pod that stands up in production currently advertises an endpoint and emits
KV events to nobody. Closing that gap is what this ADR is about.

## Options considered

| # | Shape | Pros | Cons |
|---|---|---|---|
| 1 | **Sidecar on the engine pod**, injected by the C6 mutating Pod webhook. | Identity unambiguous (sidecar shares the engine pod's network namespace; flags derived from the same CR the webhook already reads); one webhook, no new controller; lifecycle tied to the engine pod is *correct* (when the engine dies its KV events stop). | Adds one small container per engine pod. |
| 2 | **DaemonSet** watching `CacheBackend` CRs; multiplexes subscriptions across all matching pods on the node. | One subscriber per node; survives engine pod restarts independently. | Identity discovery (which engine pods on this node? what's their CacheBackend? what's the engine port?) is real work, with nothing won at Phase-1 scale; pod-IP churn during rollouts; new controller surface. |
| 3 | **C5 owns it.** Add `EnsureObservation(pod)` to `KVCacheRuntimeAdapter`; each adapter decides how to subscribe. | Cleanest seam; SGLang / Mooncake adapters can override naturally. | The seam alone doesn't say *where* the subscription runs — option 1 or 2 still has to be picked underneath. |

## Decision

**A unification of #1 and #3: subscriber-as-sidecar, injected by the existing C6 Pod
webhook, gated by a new method on the C5 `KVCacheRuntimeAdapter` interface.**

Concretely:

* `KVCacheRuntimeAdapter` gains `ObservationSidecar(cb, pod) (*corev1.Container, error)`.
  vLLM/LMCache returns the `kvevent-subscriber` container spec; the reference adapter and
  any adapter for `type: External` return `(nil, nil)`.
* The Pod webhook (`internal/webhook/pod/podinjector.go`) calls `ObservationSidecar` right
  after `InjectEngineConfig`. A non-nil container is appended to `pod.Spec.Containers`
  (idempotent — skipped if a container by the well-known name is already present). Errors
  fail open, matching the rest of the webhook.
* Sidecar identity flags are derived from the CR + pod: `--replica-id` ← `pod.Name`
  (via the downward API so `generateName` pods work), `--tenant-id` ← `pod.Namespace`
  (downward API likewise), `--model-id` ← `spec.backendConfig.model` (single source;
  when unset, the adapter returns no sidecar — the binary requires the flag, the next
  admission picks it up once the operator sets the key), `--hash-scheme` ← the
  adapter's runtime convention (`"vllm"` here), `--server` ← the policy-server
  in-cluster Service DNS (operator-configurable via a controller flag),
  `--engine-endpoint` ← `tcp://127.0.0.1:<engine ZMQ port>`. The stats-path flags
  (`--engine-metrics-url`, `--stats-interval`, etc.) are added by the adapter when the
  shipped subscriber binary learns to scrape and emit `ReplicaStats`; passing flags the
  binary doesn't recognise would crash the sidecar at startup. No operator-supplied
  `--replica-id` / `--model-id` on the demo path.

### Why this combination

* The Pod webhook already does the lookup work — `selectCacheBackend(pod)` returns the
  matching `CacheBackend`, the registry returns the right adapter, the same `cache.Spec`
  the engine config injection consumes. One mutation step does both injections.
* The adapter seam keeps engine-specific decisions where the project already lives them.
  A future SGLang adapter can return a different sidecar (e.g. a different ZMQ port or a
  completely different observation mechanism); a Mooncake adapter can return `nil` if its
  backend exposes the same data a different way. **DaemonSet remains an option for any
  future adapter** that wants it — it just isn't this PR.
* `External` backends explicitly return `nil` — we don't manage that backend's lifecycle,
  per the ticket test plan.
* Subscriber lifecycle tied to the engine pod is correctness, not a regression: when the
  engine dies its KV events stop; pairing the subscriber with the engine matches that.

## Non-decisions (deferred)

* **DaemonSet variant** — not built. The adapter seam means a later ticket can switch any
  one engine family to a DaemonSet without touching this one.
* **`CacheBackend.spec` knobs for subscriber tuning** (`--stats-interval`, concurrency
  ceilings, cache-size hint) — not added in this PR. The sidecar passes the
  `kvevent-subscriber` binary's flag defaults and can be enriched in a follow-up when an
  operator needs the knobs.
* **TLS subscriber → policy-server** — separate ticket.
* **HA / multi-server policy-server target** — separate ticket.
* **Readiness gating on first KV event observed** — natural follow-up once this lands.

## What this unblocks

* The runbook / demo path no longer needs `port-forward` + a hand-launched
  `kvevent-subscriber`: a `CacheBackend` whose `engineSelector` claims an engine pod
  causes the engine pod to come up with both the LMCache wiring **and** the subscriber
  attached.
* The kind reconciler canary can assert `inferencecache_index_entries{model=…} > 0`
  shortly after pod Ready without any out-of-band binary start.
