# CachePolicy — per-namespace tuning

`CachePolicy` is the opt-in knob for tuning cache lookup and eviction behavior in a
single namespace. The policy server ships with sane defaults (30m TTL, LRU eviction,
no request-side prefix-length gate, a **result-side matched-tokens floor of 64
(4 KV blocks) so trivial chat-template-only overlaps do not surface as
`PREFIX_MATCH`**, a **result-side routing-floor-score of `0.1` so
every-replica-holds-it overlaps that the matched-tokens floor can't catch (long
RAG headers, custom system prompts) also downgrade off the `PREFIX_MATCH` path
to `AFFINITY_HINT` under the default-enabled `affinityRouting` (or `NO_HINT`
when an operator opts out)** — see
[the three lookup-filter knobs](#three-lookup-filter-knobs-one-role-each)
below for the rationale and the explicit opt-outs, no lookup deadline). So **most
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

## The seven spec knobs

| Field | Type | Default | When to tune |
|---|---|---|---|
| `eviction` | enum `LRU` \| `LFU` | `LRU` | Cap-based eviction algorithm — which entries get dropped when the index exceeds its entry cap. `LRU` drops oldest-by-`lastSeen`; `LFU` drops the lowest access-count entry (ties broken on oldest `lastSeen`). Switch to `LFU` when a few prefixes are hot and you want them to survive cap pressure. Access counts do **not** age. |
| `evictionTTL` | duration | server default `30m` | Maximum usable lifetime of a cache entry. The freshness sweep removes entries older than this regardless of eviction algorithm. Must be strictly positive when set (admission rejects `0`/negative). |
| `minimumPrefixTokens` | int32 (min `0`) | unset = no threshold | Minimum *requested* prefix token count before a `PREFIX_MATCH` / `TENANT_HOT` result is surfaced. With `affinityRouting: Disabled` a request shorter than this short-circuits to `NO_HINT` **without touching the index** — the cheap historical path. With `affinityRouting: Enabled` (the default) the request still runs the full lookup so the index can classify `UNKNOWN_*` diagnostics, then the handler downgrades any `PREFIX_MATCH` / `TENANT_HOT` result to `StrategyNone`; the affinity fallback then returns `AFFINITY_HINT` with a stable replica pick. Either path enforces the operator intent ("tiny prompts don't surface `PREFIX_MATCH` or `TENANT_HOT` — i.e. don't surface a positive *cache-evidence* hint"); the affinity fallback's stable replica pick carries no cache-evidence claim and is considered acceptable for tiny prompts because the gate's purpose is to bound the noisy prefix-match path, not to block stable replica pinning. |
| `minimumMatchedTokens` | int32 (min `0`) | `64` (4 KV blocks) | Minimum *realized* matched-token count for a response to surface as `PREFIX_MATCH`. Applied AFTER the lookup runs, against the *actual* per-replica overlap. Replicas whose match falls below the floor are filtered; if none survive, the response is downgraded to `StrategyNone`, which surfaces as `AFFINITY_HINT` under `affinityRouting: Enabled` (the default) with a usable seed + serving replica or as `NO_HINT` under `affinityRouting: Disabled`. Set to `0` to disable enforcement entirely (e.g. raw-recall benchmarking). Distinct from `minimumPrefixTokens` — see [Three lookup-filter knobs, one role each](#three-lookup-filter-knobs-one-role-each) below. |
| `routingFloorScore` | stringified float, e.g. `"0.1"`, `"5"`, `"0"` | `"0.1"` | Per-replica *score* floor below which a `PREFIX_MATCH` response is downgraded off the prefix-match path. Applied AFTER the lookup runs, against the per-replica score from the distinguishing-power-aware ranker (`matched_tokens × freshness × pressure_factor × slo_bias × distinguishing_power`). Overlaps held by every replica (chat-template framing, RAG corpus headers, custom system prompts shared across the deployment) produce `distinguishing_power = 0` and score = 0 — this floor catches them. The downgrade lands on `StrategyNone`, which surfaces as `AFFINITY_HINT` (default-enabled affinity) or `NO_HINT` (affinity disabled). Composes with `minimumMatchedTokens` — the matched-tokens floor runs first (per-replica), then this score floor gates the top survivor. Set to `"0"` to disable entirely (raw-recall benchmarking / debug). |
| `affinityRouting` | enum `Enabled` \| `Disabled` | `Enabled` | Toggles the consistent-hash fallback on the `StrategyNone` branch. With `Enabled` (the default), any `StrategyNone` result with a usable seed + at least one replica in `servingByScope[(tenant, model, hash_scheme)]` surfaces as `AFFINITY_HINT` with a stable single-replica pick (`SHA-256(canonical_seed) mod len(sorted servingByScope)`) — repeat prompts pin to the same replica and warm T1 on diffuse single-turn workloads. With `Disabled`, the same response stays on `NO_HINT` and the gateway round-robins; useful for raw-recall benchmarking and ranker debugging. Diagnostic codes (`UNKNOWN_*`) and `TIMEOUT` keep precedence over `AFFINITY_HINT`. |
| `lookupTimeoutMs` | int32 (min `0`) | unset = no deadline | Per-lookup latency budget in milliseconds. A breach returns reason code `TIMEOUT` (still fail-open — empty result, never an error to the gateway). See the foot-gun in [Gotchas](#two-gotchas). |
| `strategy` | object | chain matching on, chain not required, tenant-hot on | Per-namespace LookupRoute strategy gates. Use `enableChainMatching: false` to force exact `prefix_hash` matching, `requireChain: true` to reject non-chain callers with `POLICY_REQUIRES_CHAIN`, or `enableTenantHot: false` to suppress soft tenant-hot hints. |

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
  eviction: LFU              # keep frequently-used prefixes under cap pressure
  evictionTTL: 30m           # drop entries older than 30m since last REPORT
  minimumPrefixTokens: 32    # tiny prompts can't surface a positive hint (request-side; under affinityRouting: Enabled — the default — the request runs the full lookup so UNKNOWN_* diagnostics still surface; under Disabled it short-circuits to NO_HINT without touching the index)
  minimumMatchedTokens: 64   # filter sub-floor replicas per-replica; if all drop the response leaves the PREFIX_MATCH path (downgrades to StrategyNone) and surfaces as AFFINITY_HINT under default-enabled affinity or NO_HINT when disabled (result-side matched-tokens floor, default 64)
  routingFloorScore: "0.1"   # downgrade off PREFIX_MATCH when top score is below the floor — surfaces as AFFINITY_HINT under default-enabled affinity or NO_HINT when disabled (result-side score floor, default "0.1")
  affinityRouting: Enabled   # consistent-hash fallback on the StrategyNone branch; set Disabled for raw-recall benchmarking / debug
  lookupTimeoutMs: 20        # positive => a real 20ms deadline (NOT 0 — see Gotchas)
  strategy:
    enableChainMatching: true # default: use block-hash longest-prefix matching when callers send chains
    requireChain: false       # default: legacy exact prefix_hash callers still work
    enableTenantHot: true     # default: allow soft TENANT_HOT locality hints
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
| `minimumPrefixTokens` | BEFORE the index lookup (under `affinityRouting: Disabled`) or AFTER the lookup as a `PREFIX_MATCH`/`TENANT_HOT` → `StrategyNone` downgrade (under `affinityRouting: Enabled` — the default) | the *request's* claimed prefix token count (chain sum wins over the legacy single-blob count) | unset = no threshold | Under affinity Disabled: short-circuit to `NO_HINT` — the index is never touched. Under affinity Enabled: full lookup runs (so the index can classify `UNKNOWN_*` diagnostics), then a positive-hint result is downgraded; the affinity fallback then surfaces `AFFINITY_HINT`. Either way: tiny prompts don't surface a positive *cache-evidence* hint (`PREFIX_MATCH` or `TENANT_HOT`). `AFFINITY_HINT` is a positive *routing* assertion but carries no cache-evidence claim, which is why it's acceptable for tiny prompts. |
| `minimumMatchedTokens` | AFTER the index lookup | each replica's *realized* matched-token overlap | `64` (4 KV blocks at the typical 16-token block size) | Filter that replica from the response. If no replica survives, the response is downgraded to `StrategyNone`, which surfaces as `AFFINITY_HINT` under default-enabled affinity or `NO_HINT` when disabled. Stops trivial chat-template-only matches from being counted as `PREFIX_MATCH` routing hits. |
| `routingFloorScore` | AFTER the index lookup, after `minimumMatchedTokens` | the *top* surviving replica's score from the distinguishing-power-aware ranker (`matched_tokens × freshness × pressure_factor × slo_bias × distinguishing_power`) | `"0.1"` | Downgrade the entire response to `StrategyNone` (drop all surviving rows), which surfaces as `AFFINITY_HINT` or `NO_HINT` per the affinity toggle. Catches the "every replica has the prefix" shape that `minimumMatchedTokens` can't catch when the shared prefix is long (RAG headers, custom system prompts, few-shot examples). |

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
  score=0 case off the `PREFIX_MATCH` path. With `affinityRouting:
  Enabled` (the default) the downgrade surfaces as `AFFINITY_HINT`
  with a stable replica pick; with `affinityRouting: Disabled` it
  stays on `NO_HINT`. See
  [`docs/design/lookuproute-ranking.md` §2.7](../design/lookuproute-ranking.md#27-the-replica-distinguishing-power-factor).

**Composition order:**
1. `minimumPrefixTokens` runs BEFORE the index lookup under
   `affinityRouting: Disabled` — below threshold → short-circuit
   `NO_HINT`, the index is never touched. Under `affinityRouting:
   Enabled` (the default) the request runs the full lookup so
   `UNKNOWN_*` diagnostics still surface, and the handler instead
   downgrades any positive-hint result (PREFIX_MATCH or TENANT_HOT)
   to `StrategyNone` as a result-side filter.
2. `minimumMatchedTokens` runs AFTER the index returns. Per-replica
   filter via `LookupResult.RetainReplicas`. If no replica survives →
   `StrategyNone`, which surfaces as `AFFINITY_HINT` (default-enabled
   affinity) or `NO_HINT` (disabled).
3. `routingFloorScore` runs LAST. Whole-response gate: if the top
   surviving replica's score is below the floor → downgrade to
   `StrategyNone` (drop all scores), which surfaces as `AFFINITY_HINT`
   or `NO_HINT` per the affinity toggle.
4. `affinityRouting` decides the final wire reason code on every
   `StrategyNone` branch reached above: with a usable seed + at least
   one replica in `servingByScope[(tenant, model, hash_scheme)]`,
   `Enabled` returns `AFFINITY_HINT` with a single stable replica;
   `Disabled` (or no seed / no serving replica) returns `NO_HINT`.

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
  **result-side `routingFloorScore` floor of `0.1`**, **consistent-hash
  fallback (`affinityRouting`) enabled**, no lookup deadline) are
  deliberately sane — a fresh cluster needs no `CachePolicy`. Note that BOTH
  result-side floors are enabled by default (they filter trivial chat-template-only
  matches AND every-replica-has-it overlaps even without a CR) AND the
  consistent-hash fallback is enabled (so a `StrategyNone` response with a
  usable seed + serving replica surfaces as `AFFINITY_HINT` rather than
  `NO_HINT`), so "no CachePolicy"
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
