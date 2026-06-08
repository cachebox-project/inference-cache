# CachePolicy — per-namespace tuning

`CachePolicy` is the opt-in knob for tuning cache lookup and eviction behavior in a
single namespace. The policy server ships with sane defaults (30m TTL, LRU eviction,
no request-side prefix-length gate, a **result-side matched-tokens floor of 64
(4 KV blocks) so trivial chat-template-only overlaps do not surface as
`PREFIX_MATCH`** — see [the matched-tokens floor](#three-lookup-filter-knobs-one-role-each)
below for the rationale and the explicit opt-out, no lookup deadline). So **most
namespaces need no CachePolicy at all** — you reach for one only when a specific
namespace has a measured reason to deviate (a hot-prefix workload, a latency SLO,
an unusually short or long prefix distribution, or a benchmark that wants to see
every overlap counted).

`CachePolicy` is **namespaced** (shortName `cpol`), and **effectively one per namespace**: a
second one is rejected at admission — but that check is *best-effort* (it lists-then-admits,
so concurrent creates or CRs predating the webhook can slip through), so the controller's
deterministic dedup is the authoritative backstop. When more than one ever coexists, the
lexicographically-smallest by `metadata.name` wins and the rest are dropped from the pushed
snapshot. The CR is purely declarative: the controller pushes its resolved fields to the
server, which is where enforcement actually happens (see
[Propagation](#propagation-controller--server) below).

## The six spec knobs

| Field | Type | Default | When to tune |
|---|---|---|---|
| `eviction` | enum `LRU` \| `LFU` | `LRU` | Cap-based eviction algorithm — which entries get dropped when the index exceeds its entry cap. `LRU` drops oldest-by-`lastSeen`; `LFU` drops the lowest access-count entry (ties broken on oldest `lastSeen`). Switch to `LFU` when a few prefixes are hot and you want them to survive cap pressure. Access counts do **not** age. |
| `evictionTTL` | duration | server default `30m` | Maximum usable lifetime of a cache entry. The freshness sweep removes entries older than this regardless of eviction algorithm. Must be strictly positive when set (admission rejects `0`/negative). |
| `minimumPrefixTokens` | int32 (min `0`) | unset = no threshold | Minimum *requested* prefix token count before a lookup is attempted. A request shorter than this short-circuits to `NO_HINT` **without touching the index** — saves work when short prompts can't usefully hit the cache. Applied BEFORE the lookup runs, against the *request's* claimed prefix length. |
| `minimumMatchedTokens` | int32 (min `0`) | `64` (4 KV blocks) | Minimum *realized* matched-token count for a response to surface as `PREFIX_MATCH`. Applied AFTER the lookup runs, against the *actual* per-replica overlap. Replicas whose match falls below the floor are filtered; if none survive, the reason code downgrades to `NO_HINT` so the gateway round-robins honestly instead of being credited with a trivial chat-template-only match. Set to `0` to disable entirely (e.g. raw-recall benchmarking). Distinct from `minimumPrefixTokens` — see [Three lookup-filter knobs, one role each](#three-lookup-filter-knobs-one-role-each) below. |
| `routingFloorScore` | stringified float, e.g. `"0.1"`, `"5"`, `"0"` | `"0.1"` | Per-replica *score* floor below which a `PREFIX_MATCH` response downgrades to `NO_HINT`. Applied AFTER the lookup runs, against the per-replica score from the distinguishing-power-aware ranker (`matched_tokens × freshness × pressure_factor × slo_bias × distinguishing_power`). Overlaps held by every replica (chat-template framing, RAG corpus headers, custom system prompts shared across the deployment) produce `distinguishing_power = 0` and score = 0 — this floor catches them. Composes with `minimumMatchedTokens` — the matched-tokens floor runs first (per-replica), then this score floor gates the top survivor. Set to `"0"` to disable entirely (raw-recall benchmarking / debug). |
| `lookupTimeoutMs` | int32 (min `0`) | unset = no deadline | Per-lookup latency budget in milliseconds. A breach returns reason code `TIMEOUT` (still fail-open — empty result, never an error to the gateway). See the foot-gun in [Gotchas](#two-gotchas). |

`status` carries only `observedGeneration` + `conditions`, and both are **reserved** — the
reconciler does not write `CachePolicy.status` today.

Printer columns (`kubectl get cpol`): `Eviction` (`.spec.eviction`) and `Age`.

## Example

A namespace with a hot-prefix workload (LFU) and a real latency SLO (20ms deadline),
declining lookups on prefixes shorter than 32 tokens:

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CachePolicy
metadata:
  name: cachepolicy-sample
spec:
  eviction: LFU             # keep frequently-used prefixes under cap pressure
  evictionTTL: 30m          # drop entries older than 30m since last REPORT
  minimumPrefixTokens: 32   # short prompts short-circuit to NO_HINT (request-side gate)
  minimumMatchedTokens: 64  # downgrade trivial overlaps to NO_HINT (result-side floor, default 64)
  lookupTimeoutMs: 20       # positive => a real 20ms deadline (NOT 0 — see Gotchas)
```

A copy-pasteable starting point ships at
[`config/samples/cache_v1alpha1_cachepolicy.yaml`](../../config/samples/cache_v1alpha1_cachepolicy.yaml)
(it omits `eviction` to exercise the `LRU` default).

## Three lookup-filter knobs, one role each

The policy carries **three** lookup-filter knobs and they enforce at
different stages of the lookup path. They're orthogonal — a single
misconfigured value on any one of them can silently inflate `PREFIX_MATCH`
rates without an operator-visible signal.

| Knob | Stage | Compared against | Default | Sub-floor outcome |
|---|---|---|---|---|
| `minimumPrefixTokens` | BEFORE the index lookup | the *request's* claimed prefix token count (chain sum wins over the legacy single-blob count) | unset = no threshold | Short-circuit to `NO_HINT` — the index is never touched. Saves work on requests that wouldn't usefully hit the cache anyway. |
| `minimumMatchedTokens` | AFTER the index lookup | each replica's *realized* matched-token overlap | `64` (4 KV blocks at the typical 16-token block size) | Filter that replica from the response. If no replica survives, the reason code downgrades to `NO_HINT`. Stops trivial chat-template-only matches from being counted as routing hits. |
| `routingFloorScore` | AFTER the index lookup, after `minimumMatchedTokens` | the *top* surviving replica's score from the distinguishing-power-aware ranker (`matched_tokens × freshness × pressure_factor × slo_bias × distinguishing_power`) | `"0.1"` | Downgrade the entire response to `NO_HINT` (drop all surviving rows). Catches the "every replica has the prefix" shape that `minimumMatchedTokens` can't catch when the shared prefix is long (RAG headers, custom system prompts, few-shot examples). |

**Why all three?**

- **`minimumPrefixTokens` is per-request.** A 5000-token request can still
  produce a 16-token match (only the chat-template framing overlaps).
  `minimumPrefixTokens` can't catch that — the *request* is long enough;
  it's the *overlap* that's trivial.
- **`minimumMatchedTokens` is per-replica matched-tokens.** It catches
  Llama-style chat-template overlaps (~16 tokens) cleanly. It does NOT
  generalize to long shared prefixes — a RAG corpus header of 1500
  tokens or a custom system prompt of 250 tokens easily clears any
  reasonable fixed floor, even though every replica holds them
  identically.
- **`routingFloorScore` is per-response score.** The score includes the
  distinguishing-power factor `1 − num_matching_replicas / total_replicas`,
  which collapses to 0 for every-replica-has-it overlaps *regardless of
  length*. Catches RAG headers and custom system prompts; the
  `routingFloorScore` floor (default `0.1`) is what downgrades that
  score=0 case to `NO_HINT`. See
  [`docs/design/lookuproute-ranking.md` §2.7](../design/lookuproute-ranking.md#27-the-replica-distinguishing-power-factor).

**Composition order:**
1. `minimumPrefixTokens` runs BEFORE the index lookup. Below threshold →
   short-circuit `NO_HINT`, the index is never touched.
2. `minimumMatchedTokens` runs AFTER the index returns. Per-replica
   filter via `LookupResult.RetainReplicas`. If no replica survives →
   `NO_HINT`.
3. `routingFloorScore` runs LAST. Whole-response gate: if the top
   surviving replica's score is below the floor → downgrade to
   `NO_HINT` with empty scores.

**Opt-out values.** Setting `minimumMatchedTokens: 0` AND
`routingFloorScore: "0"` simultaneously reproduces the pre-floor
"every non-zero match is `PREFIX_MATCH`" baseline — useful for
raw-recall benchmarking and ranker debugging. Either floor can be
disabled independently; the other still fires. With a `CachePolicy`
installed each field value wins as-is (zero included); with no
`CachePolicy` installed the server applies both safety defaults.

## Two gotchas

These two behaviors are the high-value content — both are easy to get backwards.

### 1. `lookupTimeoutMs: 0` means UNBOUNDED, not "fail instantly"

Any value `<= 0` (including `0`) is treated by the server as **no deadline** — lookups run
without a latency budget. It does **not** mean "zero milliseconds, time out immediately."
If you set `lookupTimeoutMs: 0` expecting an instant timeout, you get the opposite: no
bound at all.

To actually bound lookup latency, set a **positive** value (the sample uses `20`). Leave
the field unset if you have no measured SLO — that is also "no deadline," and it is the
honest way to express it.

### 2. `evictionTTL` ages from `lastSeen`, and lookups do NOT refresh `lastSeen`

`evictionTTL` measures age from an entry's `lastSeen` timestamp, and **only ingest**
(`ReportCacheState` from an engine subscriber) advances `lastSeen`. Serving a hint does
not. A prefix that is looked up constantly but is no longer re-reported by any engine will
still expire at `evictionTTL` after its **last REPORT** — heavy lookup traffic does not
keep it warm in the index.

The only write the lookup path makes is an `LFU` access-count bump on a *delivered* hint
(a `TIMEOUT`'d lookup credits nothing). That bump feeds cap-eviction ordering only; it
never touches `lastSeen` and never affects TTL. It is also the single documented exception
to otherwise side-effect-free lookups — the contract note lives in
[`docs/design/grpc-contract.md`](../design/grpc-contract.md).

## Propagation (controller → server)

The controller watches every `CachePolicy` cluster-wide and **pushes** a full snapshot to
the server's internal `POST /policy` endpoint (on the `:8081` listener) on every reconcile
plus a periodic tick (~30s). The server adopts **replace-on-write**: deleting a `CachePolicy` reverts its
namespace to server defaults on the next snapshot. The server's policy store is in-memory
soft state, so the periodic re-push re-syncs a restarted server without operator action.

For the wire schema, auth posture, and the one-policy-per-namespace dedup backstop, see
[`docs/design/policy-propagation.md`](../design/policy-propagation.md).

## When NOT to use it

- **Empty or default-happy cluster.** Server defaults (TTL 30m, LRU, **no request-side
  prefix threshold**, **result-side `minimumMatchedTokens` floor of 64**,
  **result-side `routingFloorScore` floor of `0.1`**, no lookup deadline) are
  deliberately sane — a fresh cluster needs no `CachePolicy`. Note that BOTH
  result-side floors are enabled by default (they filter trivial chat-template-only
  matches AND every-replica-has-it overlaps even without a CR), so "no CachePolicy"
  is not the same as "no enforcement at all" — only the request-side gate is unset.
  Install a CR with `minimumMatchedTokens: 0` AND `routingFloorScore: "0"` if you
  specifically want the pre-floor behavior (raw-recall benchmarking, ranker debugging).
- **No measured SLO.** Don't set `lookupTimeoutMs` unless you have a real latency budget
  — and never set it to `0` expecting a timeout (see [Gotcha 1](#1-lookuptimeoutms-0-means-unbounded-not-fail-instantly)).
- **Per-tenant quotas.** `CachePolicy` tunes lookup/eviction behavior per namespace; it
  does not carry tenant index-entry budgets — that's `CacheTenant`'s
  `spec.quota.maxIndexEntries`.

## See also

- [`docs/design/crd-contract.md`](../design/crd-contract.md) — CRD design rationale and
  the cross-CRD invariants (status surface, reconciler ownership, enforcement boundary).
- [`docs/design/policy-propagation.md`](../design/policy-propagation.md) — how the
  controller pushes the snapshot and how the server applies each field.
- [`docs/design/policy-crds.md`](../design/policy-crds.md) — design rationale for the
  policy CRD shape and the enforcement boundary.
