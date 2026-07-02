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
webhook, gated by a new method on the C5 `KVCacheRuntimeAdapter` interface. Auto-attach
is opt-in by default — the operator passes the subscriber image via a controller flag
to turn it on.**

Concretely:

* `KVCacheRuntimeAdapter` gains `ObservationSidecar(cb, pod) (*corev1.Container, error)`.
  The vLLM/LMCache, vLLM/Mooncake, and SGLang/LMCache adapters return the `kvevent-subscriber`
  container spec (via the shared `RenderSubscriberSidecar` — the KV-event stream is the
  engine's own ZMQ publisher, independent of the L2 store; each adapter pins its engine's
  `--hash-scheme` tag + ZMQ port); the reference adapter and any adapter for
  `type: External` return `(nil, nil)`.
* The Pod webhook (`internal/webhook/pod/podinjector.go`) calls `ObservationSidecar` right
  after `InjectEngineConfig`. A non-nil container is appended to `pod.Spec.Containers`
  (idempotent — skipped if a container by the well-known name is already present). Errors
  fail open, matching the rest of the webhook.
* **The vLLM/LMCache, vLLM/Mooncake, and SGLang/LMCache adapters return nil unless the
  controller's `--kvevent-subscriber-image` flag is set** (all go through the same shared
  `RenderSubscriberSidecar`, so the opt-in behaviour is identical). An unconfigured image would put the sidecar
  container into `ImagePullBackOff`, which keeps the engine pod from going Ready — the
  exact "cache becomes a serving dependency" failure mode the fail-open posture exists
  to prevent. Defaulting auto-attach off lets the controller install cleanly into any
  cluster; operators turn it on when they're ready to ship a subscriber image alongside.
* Sidecar identity flags are derived from the CR + pod: `--replica-id` ← `pod.Name`
  (via the downward API so `generateName` pods work), `--tenant-id` ← `pod.Namespace`
  (downward API likewise), `--model-id` ← `spec.backendConfig.model` (single source;
  when unset, the adapter returns no sidecar — the binary requires the flag, the next
  admission picks it up once the operator sets the key), `--hash-scheme` ← the
  adapter's runtime convention (`"vllm"` or `"sglang"`), `--server` ← the policy-server
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
  The SGLang adapter reuses the same shared subscriber, only its `--hash-scheme` tag
  differs (SGLang adopted vLLM's ZMQ KV-event wire); the seam is what would let a genuinely
  different future engine return a different sidecar (e.g. a different ZMQ port or a
  completely different observation mechanism). The shipped Mooncake adapter, by contrast,
  returns the *same* vLLM kvevent-subscriber the LMCache adapter does — Mooncake integrates
  as an LMCache remote backend, so the engine is still vLLM and its KV events still come
  from vLLM's ZMQ publisher (scheme-tagged `vllm`); only the backend store differs. A
  future backend that fronts a non-vLLM engine, or exposes observation data some other way,
  could still return `nil` or a different container here. **DaemonSet remains an option for
  any future adapter** that wants it — it just isn't this PR.
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

## L2 cache tier semantics — `--ignore-block-removed`

vLLM's KV-event publisher emits `BlockRemoved` on every block eviction from the
GPU pool. When the engine sits *alone* (no second-tier cache), that eviction is
the prefix becoming unreachable and the cache plane must drop the routing hint
promptly — the subscriber's default behavior, forwarding `BlockRemoved` as
`PREFIX_EVICTED`, is exactly right.

When the engine is paired with a separate **L2 cache tier** (e.g. LMCache via
`--kv-transfer-config '{"kv_connector":"LMCacheConnectorV1",...}'`) the
semantics invert. LMCache retains the block after the engine offloads it from
GPU, so the replica can still serve the prefix cheaply from the L2 tier. The
vLLM-emitted `BlockRemoved` no longer means "the prefix is gone"; it means
"the prefix moved tiers." Forwarding it as `PREFIX_EVICTED` would drop a
routing hint the replica can still satisfy — the gateway then routes
elsewhere and the L2 cache hit is wasted. The cache plane should keep the
entry until its freshness TTL expires; a stale entry yields a cache miss
(soft state), while a missing entry mis-routes warm traffic away.

The subscriber binary exposes `--ignore-block-removed` (default off, for
backward compatibility with single-tier deployments). When set the reporter
drops `BlockRemoved` events without forwarding them; `AllBlocksCleared` and
`BlockStored` still flow normally. The shared `RenderSubscriberSidecar` helper
(`pkg/adapters/runtime/kvevent_subscriber.go`) — which both the vLLM/LMCache and
vLLM/Mooncake adapters call — sets the flag **per integration mode**, because
the L2 tier is present only in one of them:

- **`Offload` (default):** the adapter wires the LMCache KV connector (pointed at
  an `lm://` LMCache server or a `mooncakestore://` Mooncake store), so an L2
  tier retains the block after the engine offloads it — `BlockRemoved` means
  "moved tiers," not "gone." The helper sets `--ignore-block-removed=true` so
  the hint ages out on its freshness TTL instead of being dropped.
- **`EventsOnly`:** no KV connector is injected, so there is **no** L2 tier
  holding the block. `BlockRemoved` genuinely means the prefix is gone and the
  hint MUST be pruned, so the helper **omits** the flag (subscriber default off,
  forwarding the eviction as `PREFIX_EVICTED`). EventsOnly is restricted to
  `spec.type=LMCache` at admission (a managed Mooncake backend always provisions
  its store, so `Mooncake` + `EventsOnly` is rejected), so a Mooncake backend
  always takes the Offload branch and sets the flag.

Other adapters (e.g. plain vLLM, or future runtimes with no L2 tier) leave
the flag off for the same reason as `EventsOnly`.

## What this unblocks

* The runbook / demo path no longer needs `port-forward` + a hand-launched
  `kvevent-subscriber`: a `CacheBackend` whose `engineSelector` claims an engine pod
  causes the engine pod to come up with both the LMCache wiring **and** the subscriber
  attached.
* The kind reconciler canary can assert `inferencecache_index_entries{model=…} > 0`
  shortly after pod Ready without any out-of-band binary start.
