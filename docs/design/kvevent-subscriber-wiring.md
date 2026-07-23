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

## L2 cache tier semantics — `--ignore-block-removed` and T1/T2 tier tagging

vLLM's KV-event publisher emits `BlockRemoved` on every block eviction from the
HBM pool. When the engine sits *alone* (no second-tier cache), that eviction is
the prefix becoming unreachable and the cache plane must drop the routing hint
promptly — the subscriber's default behavior, forwarding `BlockRemoved` as
`PREFIX_EVICTED`, is exactly right.

When the engine is paired with a separate **L2 cache tier** (e.g. LMCache via
`--kv-transfer-config '{"kv_connector":"LMCacheConnectorV1",...}'`) the
semantics invert. LMCache retains the block after the engine offloads it from
HBM, so the replica can still serve the prefix cheaply from the L2 tier. The
vLLM-emitted `BlockRemoved` no longer means "the prefix is gone"; it means
"the prefix moved tiers." Forwarding it as `PREFIX_EVICTED` would drop a
routing hint the replica can still satisfy — the gateway then routes
elsewhere and the L2 cache hit is wasted.

### Tier detection from the block lifecycle (T1 / T2)

The subscriber tags each reported prefix with a **cache tier** (`PrefixEntry.tier`,
see `grpc-contract.md`) derived from the engine's block lifecycle. The available
signals are only `BlockStored` / `BlockRemoved` / `AllBlocksCleared`: **vLLM's
KV-event channel announces the T1 (HBM) lifecycle but emits nothing when
LMCache offloads a block to L2** — the `LMCacheConnectorV1` offload is invisible
to the KV-event surface. So T2 is not *directly observable*; it is *inferred*
from a T1 eviction combined with knowledge that an L2 tier is configured:

- **`BlockStored` → T1.** The block is resident in the engine KV cache (HBM).
- **`BlockRemoved`, L2 tier present → T2 (downgrade).** The block left HBM but the
  paired L2 store still holds it, so the subscriber **re-reports the same
  content-fingerprint key at T2** (reload-able from L2) rather than deleting it.
  The index applies last-write-wins on tier, moving the entry T1→T2; a later
  `BlockStored` of the same content re-reports it at T1 (upgrade). The re-report
  goes on the same additive `ReportCacheState` path as adds, but is flushed at the
  eviction boundary as its **own** update carrying the eviction timestamp — a
  `CacheStateUpdate` has a single `timestamp_us` for all its prefixes, so batching
  the downgrade with a later store in the same debounce window would refresh the
  T2 entry's freshness away from the eviction. Sending it separately keeps T2
  freshness anchored at the eviction time (when reload-ability was last confirmed)
  and preserves store→evict order.
- **`BlockRemoved`, no L2 tier → delete.** Forward `PREFIX_EVICTED` (the prefix
  is genuinely gone).
- **T3 (remote / disaggregated) is out of scope** — no engine signal
  distinguishes it today; a future ticket can add it if a signal appears.

**A T2 tag is a *prediction of reload-ability*, not confirmation the block is
actually resident in LMCache.** An offload can silently fail (the block was
evicted from HBM but never made it to L2), in which case the T2 hint is wrong —
but routing stays fail-open, so a wrong T2 hint just makes the engine recompute
the prefix (a cache miss), never a wrong answer. Detecting a silently-degraded
offload tier is a **separate health concern** (surfaced via the tier-2 reload
counters `t2_hit_tokens` / `t2_query_tokens` on `ReplicaStats`, see
`grpc-contract.md`), not this path. This is the deliberate "combine cold
observation (`BlockRemoved`) with warm prediction (L2 retains it)" model: the
cache plane keeps a *usable* hint through the HBM→L2 transition instead of going
blind the moment a warm engine stops emitting `BlockStored`.

### How the subscriber knows it has an L2 tier — `--ignore-block-removed`

The subscriber binary exposes `--ignore-block-removed` (default off). It is the
existing, least-invasive signal for "this replica has an L2 tier": the flag name
predates the T2 downgrade (earlier this path simply suppressed the eviction and
left the entry stale at T1) and is kept for backward compatibility, but the
signal it carries is unchanged. When set, a `BlockRemoved` becomes a T2 downgrade;
when unset, it forwards `PREFIX_EVICTED`. `AllBlocksCleared` and `BlockStored`
flow normally in both modes. The shared `RenderSubscriberSidecar` helper
(`pkg/adapters/runtime/kvevent_subscriber.go`) — which the vLLM/LMCache,
vLLM/Mooncake, and SGLang/LMCache adapters all call — sets the flag **per
integration mode**, because the L2 tier is present only in one of them:

- **`Offload` (default):** the adapter wires the LMCache KV connector (pointed at
  an `lm://` LMCache server or a `mooncakestore://` Mooncake store), so an L2
  tier retains the block after the engine offloads it — `BlockRemoved` means
  "moved tiers," not "gone." The helper sets `--ignore-block-removed=true` so the
  hint is re-reported at T2 and ages out on its freshness TTL.
- **`EventsOnly`:** no KV connector is injected, so there is **no** L2 tier
  holding the block. `BlockRemoved` genuinely means the prefix is gone and the
  hint MUST be pruned, so the helper **omits** the flag (subscriber default off,
  forwarding the eviction as `PREFIX_EVICTED`). EventsOnly is restricted to
  `spec.type=LMCache` at admission (a managed Mooncake backend always provisions
  its store, so `Mooncake` + `EventsOnly` is rejected), so a Mooncake backend
  always takes the Offload branch and sets the flag.

Other adapters (e.g. plain vLLM, or future runtimes with no L2 tier) leave
the flag off for the same reason as `EventsOnly` — their stored prefixes stay
T1 and a `BlockRemoved` deletes them.

## LoRA adapter identity — `--lora-adapter-names`

The routing key is a **content fingerprint over token IDs only**, so two prompts
with identical tokens under different LoRA adapters hash identically. The
subscriber therefore reads `lora_id` off each `BlockStored` event and stamps the
resolved adapter identity on every reported `PrefixEntry`, so the server can put
them in disjoint **index partitions** rather than one aliased entry (see
[`grpc-contract.md`](grpc-contract.md) "Update — adapter (LoRA) index
partition"). The adapter never enters the hash.

`lora_id` is vLLM's **internal load-order integer**, not a name: the same integer
can mean different adapters on two replicas whose `--lora-modules` order differs,
and the index is shared across replicas. `--lora-adapter-names` maps it to the
stable identity the gateway sends as `LookupRouteRequest.adapter_id`:

```
--lora-adapter-names "1=sql-lora,2=chat-lora"
```

- **Unmapped id** → `lora:<id>`. Exact within a replica, and consistent across
  replicas that share an adapter load order (the homogeneous-Deployment case).
  Supply the map for adapters **known at startup** when replicas can diverge
  (rolling updates, heterogeneous load order), and make the gateway send the
  matching `adapter_id`.
- **No LoRA at all** (the default): every event carries a nil `lora_id`, so
  every entry and eviction lands in the default (`""`) partition — byte-for-byte
  the pre-adapter behavior. Nothing to configure.

### Limitations of the static flag

`--lora-adapter-names` is resolved once at **startup**, which leaves two gaps —
both a tracked follow-up, and neither a regression (an unmapped id still
partitions by `lora:<id>`, strictly fewer collisions than the pre-adapter
single-partition behavior):

- **Runtime loads.** An adapter loaded or reloaded *after* the subscriber starts
  whose id isn't already in the map falls back to `lora:<id>` — a static list
  can't name it. Covering arbitrary runtime loads needs the subscriber to
  resolve adapter identity **dynamically** from the engine's adapter registry.
- **Managed injection.** On the `CacheBackend`-driven path the subscriber
  sidecar is **injected**, so operators cannot pass the flag on a container they
  do not define; the explicit-set guidance below applies only to a
  **self-managed** subscriber. Configuring the mapping through the managed path
  needs a `CacheBackend` field the webhook forwards.

For a self-managed subscriber, set `--lora-adapter-names` on the container
directly: the controller has no view of the engine's `--lora-modules` ordering,
so multi-adapter engines must supply the map explicitly.

## What this unblocks

* The runbook / demo path no longer needs `port-forward` + a hand-launched
  `kvevent-subscriber`: a `CacheBackend` whose `engineSelector` claims an engine pod
  causes the engine pod to come up with both the LMCache wiring **and** the subscriber
  attached.
* The kind reconciler canary can assert `inferencecache_index_entries{model=…} > 0`
  shortly after pod Ready without any out-of-band binary start.
