# LookupRoute ranking strategies

How the cache plane answers the gateway's "where should I send this request?"
question — what the score formula means, which strategies layer on top, and
when each one fires. This is the *design* explanation; the
[`reason-codes.md`](../reference/reason-codes.md) reference is the wire-level
"what each code means today" table.

## 1. What `LookupRoute` is for

`LookupRoute` is the single gRPC the gateway calls on the hot path of every
inference request. It carries the prompt's prefix hash (or an ordered chain
of per-block hashes — see §2.5), the tenant/model the request is for, the
engine `hash_scheme`, and an optional `SLO` (TTFT / TBT budget). The response
is a list of ranked replica hints plus a `reason_code` that names which
ranking strategy produced them.

The cache plane never routes. It only describes cache state:

> **We hint, the gateway decides.**

The gateway is always free to use a hint or ignore it. Empty hints
(`NO_HINT`) are valid; they tell the gateway "fall back to your default
policy, the cache plane is invisible to this request." No `LookupRoute`
response is ever an error on the hot path. Fail open.

## 2. The baseline strategy — `PREFIX_MATCH`

The index keys cached prefixes by `(tenant, model, hash_scheme, adapter, prefix_hash)`
→ the set of replicas that hold it, with per-replica `token_count` and a
`last_seen` timestamp. `adapter` is the resolved LoRA identity (`""` for
base-model / non-LoRA traffic); it partitions the content, never the fingerprint,
so identical tokens under different adapters share a hash but not an entry. The
`(tenant, model, hash_scheme)` *scope* used by the fallbacks below deliberately
stays adapter-blind. A baseline lookup:

1. Look up the request's `(tenant, model, hash_scheme, adapter, prefix_hash)`.
2. For each replica that holds it, compute

   ```
   score = matched_tokens × freshness
   ```

   where `freshness = max(0, 1 − age / TTL)` — a linear decay from 1 (just
   reported) to 0 (older than the TTL).
3. Filter the scored replicas through the **matched-tokens floor** (§2.6)
   so a 1-block chat-template-only overlap is not reported as a routing
   hit. If at least one replica clears the floor, return them ranked
   best-first (ties broken by replica ID) with `reason_code: PREFIX_MATCH`.
   If none clear the floor, the response downgrades to `StrategyNone`,
   which surfaces as `reason_code: AFFINITY_HINT` under default-enabled
   `CachePolicy.spec.affinityRouting` with a usable seed + serving
   replica or as `reason_code: NO_HINT` with empty scores under
   `affinityRouting: Disabled`. (The handler returns every qualifying
   candidate — there is no top-K limit today; the gateway typically
   uses the top entry.)

The intuition: more matched tokens → bigger TTFT win from the prefix-cache
hit, and a fresher report is stronger evidence the replica still holds the
state. If no replica holds the prefix, the result is empty; the
accompanying `reason_code` is `NO_HINT` when the miss is a same-key novel
prefix (or when the cold-start / empty-key carve-outs apply), or one of
the diagnostic codes (`UNKNOWN_TENANT` / `UNKNOWN_MODEL` /
`UNKNOWN_HASH_SCHEME`) when the miss-classifier identifies a
contract-key mismatch — see
[`lookuproute-diagnostics.md`](./lookuproute-diagnostics.md). Either way
the gateway falls back to its default routing policy.

This is the cache plane's job at its simplest. Everything else in this doc
*layers on top of this formula* — it never replaces it. When the new
strategies' inputs are absent, every factor reduces to 1 and the score
collapses back to `matched_tokens × freshness`.

## 2.5 Longest-prefix block-level matching — richer `PREFIX_MATCH`

### Why the exact-full-hash path leaves hits on the table

§2 looks up the request's `(tenant, model, hash_scheme, adapter, prefix_hash)` as
**one opaque blob**. Two requests that share the first N KV blocks of a
prefix but diverge after — common with a shared system prompt plus
per-request RAG context — hash to different `prefix_hash` values and miss
each other entirely. The replica that already holds 80% of the prefix
gets `NO_HINT` and the gateway routes blind.

vLLM and SGLang already chain their KV blocks: each block's hash is
computed from its parent's hash plus the block's tokens, so holding
`h_i` implies the engine computed `h_0..h_{i-1}` too. Expressing the
prefix as that **ordered chain of block hashes** lets the cache plane
match the longest common leading run instead of insisting on full
equality.

This is still `reason_code: PREFIX_MATCH` — same response shape, same
ranker factors (pressure, SLO, freshness) compose on top unchanged. The
only difference is *how* `matched_tokens` is computed.

### Contract shape (additive on `v1alpha1`)

Two new fields on `PrefixEntry` and `LookupRouteRequest` — paired
parallel arrays, same length, same order:

```
PrefixEntry {
  // ... existing legacy fields kept verbatim
  repeated bytes block_hashes = 3;          // one engine block-hash per element
  repeated int32 block_token_counts = 4;    // tokens covered by each block
}

LookupRouteRequest {
  // ... existing fields including the legacy prefix_hash
  repeated bytes block_hashes = 7;
  repeated int32 block_token_counts = 8;
}
```

Legacy `prefix_hash` / `token_count` stay in place at their original
field numbers; old engines and old gateway clients keep working
unchanged. `buf breaking --against main` is part of CI to catch any
slip into a non-additive change.

### How the lookup walks the chain

For each block `block_hashes[i]` the request carries, the index already
holds a `(tenant, model, hash_scheme, adapter, block_hash)` → `{replicaID →
{tokenCount, lastSeen}}` entry — populated either from a single chain
`PrefixEntry` (the index *expands* the chain into N per-block entries)
or from N legacy single-blob `PrefixEntry` reports (one per block, the
shape the vLLM subscriber already emits). The chain walk is one
intersection per block:

1. **At block 0**, seed a running set with every replica that holds
   `block_hashes[0]` (filtered against TTL — stale blocks are skipped).
   Each entry records `matched_tokens = block_token_counts[0]` and
   `oldest_last_seen = entry.lastSeen`.
2. **At block i > 0**, intersect the running set with the holders of
   `block_hashes[i]`. Replicas that fall out had their match end at
   block i−1 — freeze their score into a finalized map. Replicas that
   stay update `matched_tokens += block_token_counts[i]` and pull
   `oldest_last_seen` down to `min(running, entry.lastSeen)`.
3. When the running set is empty, stop walking; the rest of the chain
   would yield no further matches.
4. Replicas still running at the end of the chain matched the full
   request chain.
5. Score each finalized replica:

   ```
   freshness        = freshnessAt(now, oldest_last_seen, ttl_for_tenant)
   score (chain)    = matched_tokens × freshness × pressure_factor × slo_bias
   ```

   The pressure and SLO factors from §3–§5 layer on identically — the
   chain walk only changes how the per-replica `matched_tokens` and
   `freshness` inputs get derived.

A few details that matter for correctness:

- **Freshness is the weakest link.** A replica fresh on blocks 0–3 but
  stale on block 4 cannot ride its early blocks' freshness; the run's
  `oldest_last_seen` reflects the staler block. Stops a partially-aged
  hold from looking better than it really is.
- **Dropping out is final.** Once a replica fails to hold `block_hashes[i]`,
  it can't re-enter at block i+1 — leading-run semantics, not "any
  contiguous run of matches."
- **Seeding is at block 0 only.** A replica that holds `block_hashes[5]`
  of some unrelated chain (because the bytes happen to collide) doesn't
  show up as a partial match against this request. The leading-run rule
  is enforced by where we initialize, not by separate position tracking.
- **`reason_code` is unchanged on the match side, subject to the §2.6
  floor.** Any non-empty match — one block or the whole chain — is
  `PREFIX_MATCH` *provided* at least one replica's `matched_tokens`
  clears the per-namespace `minimumMatchedTokens` floor. A chain match
  that produces only sub-floor per-replica overlaps (e.g. a 1-block run
  under the default 64-token floor) is downgraded to `StrategyNone`
  (which then surfaces as `AFFINITY_HINT` under default-enabled
  affinity or `NO_HINT` when disabled) exactly the way an exact-match
  result does — the floor is a §2-and-§2.5 wrapping filter, not a
  separate strategy. On a miss, the request goes through
  the same miss-classifier as the exact-match path: a same-key
  first-block miss yields `StrategyNone` (which then surfaces as
  `AFFINITY_HINT` under default-enabled affinity with a usable seed +
  serving replica, or as `NO_HINT` under `affinityRouting: Disabled`);
  a contract-key mismatch yields the matching `UNKNOWN_*` code (see
  [`lookuproute-diagnostics.md`](./lookuproute-diagnostics.md)) — and
  the `UNKNOWN_*` codes keep precedence over `AFFINITY_HINT`. The
  engine-opaque guards (empty `hash_scheme`, mismatched parallel-array
  lengths) and the cold-start carve-out continue to surface as
  `NO_HINT` regardless of the affinity toggle (structurally malformed
  input and a globally-empty index are not affinity-eligible).

### Worked example — 5 blocks, 3 deep

Request asks for the chain `[h1, h2, h3, h4, h5]` with per-block counts
`[64, 64, 64, 64, 64]`. Index state for the requested
`(tenant, model, scheme)`:

Block sizes are 64 tokens each so every per-replica run clears the
default §2.6 matched-tokens floor; the §2.5 walk illustrates chain
ranking in isolation from the floor. (A 16-token-per-block example
under the default `minimumMatchedTokens: 64` would have *every* replica
in this table filtered out and the response downgraded off the
`PREFIX_MATCH` path to `StrategyNone` — which then surfaces as
`AFFINITY_HINT` under default-enabled affinity with a usable seed +
serving replica, or as `NO_HINT` under `affinityRouting: Disabled` —
correct, but it would shadow the longest-prefix mechanics the section
is teaching.)

| Block hash | Holders → `{tokenCount, lastSeen age}`                                   |
|------------|---------------------------------------------------------------------------|
| `h1`       | `replica-A: {64, 1m}, replica-B: {64, 2m}, replica-C: {64, 4m}`           |
| `h2`       | `replica-A: {128, 1m}, replica-B: {128, 2m}`                              |
| `h3`       | `replica-A: {192, 1m}`                                                    |
| `h4`       | *(nobody)*                                                                |
| `h5`       | *(nobody)*                                                                |

Walking the chain:

| Step      | Running set after this block                          | Finalized                                  |
|-----------|-------------------------------------------------------|--------------------------------------------|
| seed `h1` | `A: {tok=64, oldest=1m}, B: {tok=64, oldest=2m}, C: {tok=64, oldest=4m}` | —                                          |
| at `h2`   | `A: {tok=128, oldest=1m}, B: {tok=128, oldest=2m}`    | `C: {tok=64, oldest=4m}` *(dropped at h2)* |
| at `h3`   | `A: {tok=192, oldest=1m}`                             | `+ B: {tok=128, oldest=2m}` *(dropped at h3)*|
| at `h4`   | *(empty — A doesn't hold h4)* → stop walking          | `+ A: {tok=192, oldest=1m}`                |

Scoring each finalized replica (no pressure/SLO, TTL = 30 min, so
`freshness = 1 − age/30m`). The §2.7 depth-aware distinguishing-power
factor also multiplies in: `num_matching_at_R's_depth` counts replicas
whose `matched_tokens >= R.matched_tokens`. A is the only replica at
192 tokens (factor `1 − 1/3 = 2/3`); B shares its 128-token depth with
A (factor `1 − 2/3 = 1/3`); C's 64-token depth is reached by all three
(factor `1 − 3/3 = 0`).

| Replica | matched_tokens | freshness | distinguishing_power | score  | rank |
|---------|----------------|-----------|----------------------|--------|------|
| `A`     | 192            | 0.97      | 2/3 ≈ 0.667          | 124.1  | 1    |
| `B`     | 128            | 0.93      | 1/3 ≈ 0.333          | 39.7   | 2    |
| `C`     | 64             | 0.87      | 0                    | 0      | 3    |

Response: `reason_code: PREFIX_MATCH`, scores `[A (124), B (40), C (0)]`
— the gateway routes to A for the deepest unique-hit, with B as a hot
backup if A is overloaded. C's score is zero because every replica in
the cluster matched at C's depth (the shared head block) — the
distinguishing-power factor correctly identifies that C's contribution
is uninformative for routing.

Under the default §2.6 matched-tokens floor of 64, all three clear (A
and B by a comfortable margin, C exactly at the boundary), so the §2.6
filter doesn't drop anyone here. Under the default §2.7 routing-floor
of `"0.1"`, A's top score 124 clears comfortably so the response ships
as `PREFIX_MATCH`. Raising `minimumMatchedTokens: 128` would filter C
out via the per-replica §2.6 filter (leaving `[A, B]`); raising
`routingFloorScore: "200"` would downgrade the whole response to
`StrategyNone` via the §2.7 whole-response gate (A's 124 falls below
200), which then surfaces as `AFFINITY_HINT` under default-enabled
affinity with a usable seed + serving replica or as `NO_HINT` under
`affinityRouting: Disabled`.

### Backward-compat — what an unmigrated producer or client sees

- **Legacy producer, chain-aware client.** A producer that still emits
  one `PrefixEntry` per block (the existing vLLM subscriber) populates
  the per-block index keys directly. The chain lookup walks them
  end-to-end without any engine-side change. *Switching the producer
  to a single chain `PrefixEntry` is a future optimization, not a
  requirement for the value to land.*
- **Chain producer, legacy client.** A producer that sets both the
  chain and the legacy `prefix_hash` on the same `PrefixEntry` gets
  **both representations indexed** — the chain enables longest-prefix
  matching for new clients; the legacy single-blob key keeps
  unmigrated `LookupRoute` callers hitting via exact-match on
  `prefix_hash`. The chain wins precedence for chain-aware lookups;
  legacy is only added when the producer explicitly sets it alongside.
- **Chain producer, chain-aware client, missing per-block counts.**
  Mismatched parallel-array lengths (including the one-sided cases —
  hashes without counts or counts without hashes) are treated as
  malformed: dropped on ingest, `NO_HINT` on lookup. The handler does
  not silently downgrade to legacy exact-match — a stale hint is fine,
  a wrong hint is not.
- **Chain-only or chain-plus-legacy request with the `minimumPrefixTokens`
  gate active.** The handler uses `effectivePrefixTokens(req)`, which
  gives the chain precedence: when `block_token_counts` is set the
  gate uses `sum(block_token_counts)`; only when the chain is empty
  does it fall back to the legacy `prefix_token_count`. A chain-bearing
  request is therefore gated on what the chain actually reports — a
  co-set stale legacy count cannot let it through nor zero it out.

### Engine-opaque + parent-chain assumption

Block hashes are still engine-defined opaque bytes; the cache plane
matches them by exact byte equality within a `hash_scheme` and never
interprets them. The longest-leading-run rank is **only meaningful when
the engine's block hashes are parent-chained** — i.e. holding `h_i`
implies having computed `h_0..h_{i-1}`. vLLM and SGLang both satisfy
this. A future engine that emits position-blind block hashes (same
bytes for a "middle" block as for a "leading" block) would violate the
assumption and should not be ingested with the chain form. The contract
doesn't enforce that — it's a producer-side discipline.

## 2.6 The matched-tokens floor — a Phase-1 stopgap

**Scope caveat.** This floor is a quick win for Llama-style chat-template
workloads where the shared prefix is short (~16-32 tokens). It is **not**
the production-grade ranking answer: workloads with long shared prefixes
(RAG corpus headers, custom system prompts, few-shot examples) routinely
match well above any reasonable fixed token floor on content every replica
has — so the floor lets them through unchanged and the inflated-`PREFIX_MATCH`
signal returns. The production-grade fix is the replica-distinguishing-power
factor shipped in [§2.7](#27-the-replica-distinguishing-power-factor) below,
which weighs an overlap by how rare it is across replicas rather than how
long it is in tokens. The matched-tokens floor stays as a complementary
per-replica filter and a redundant safety net.

The baseline returned `PREFIX_MATCH` for *any* non-zero overlap, which gave
operators an inflated routing signal: the cache-stress harness benchmarks
showed ~70% of `PREFIX_MATCH` responses were 1-block (16-token) matches —
the Llama-3 chat-template framing every replica had identically. Trivial
matches deterministically route to whichever replica's chat-template hash
arrived first; they are not useful routing decisions, but they were being
counted as if they were.

The matched-tokens floor adds a per-namespace **result-side** filter on top
of the score from §2 (and §2.5 longest-prefix matching). Pseudocode:

```
effective_floor = CachePolicy.spec.minimumMatchedTokens   // per namespace
                  ?? server_default                       // = 64 (4 blocks)

scored = ranker(request)                                  // §2 / §2.5
kept   = [s for s in scored if s.matched_tokens >= effective_floor]

if len(kept) > 0:
    reply(PREFIX_MATCH, kept)
else:
    reply(NO_HINT, [])
```

Key properties:

- **Server-side default.** A namespace with no `CachePolicy` installed still
  gets the safety floor (`DefaultMinimumMatchedTokens = 64`), so the
  inflated-metric bug doesn't reappear for unconfigured tenants — which is
  the common case.
- **Per-namespace override.** A `CachePolicy.spec.minimumMatchedTokens` value
  overrides the default for that namespace exactly as written.
- **`= 0` is the explicit opt-out.** Useful for raw-recall benchmarking and
  pre-regression tests; reverts a namespace to the
  every-match-is-`PREFIX_MATCH` behavior.
- **Per-replica filter, not response-level.** A long-prefix replica still
  surfaces as `PREFIX_MATCH` even when a sibling replica only matched the
  trivial chat-template head — the floor drops the sibling, keeps the long
  match. Only when *no* replica clears the floor does the response
  downgrade.
- **Applied BEFORE `CreditHits` runs.** A sub-floor match that is downgraded
  to `NO_HINT` does not bump the LFU access counter — the contract guarantee
  that a non-delivered hint never credits remains intact. Filtered-out
  replicas' entries are pruned from the hits map in lockstep with their
  Scores, so the no-credit-on-non-delivery rule holds on the partial-keep
  path too (one replica survives, a sibling falls below the floor).
- **Distinct from `minimumPrefixTokens`.** That field is a request-side
  pre-lookup gate against the *request's* claimed prefix length (skips the
  index entirely on a too-short request); this is a result-side post-lookup
  floor against the *realized* per-replica overlap (filters what the index
  produced).

The default is 64 = four KV blocks at the typical 16-token block size — well
above the 16-token framing tokens every replica has identically, well below
any useful real-prompt prefix overlap. Tune up (`minimumMatchedTokens: 256`)
when a deployment runs especially long system prompts and you want only
substantial overlaps to count as routing wins; tune down to `0` when
debugging the ranker or measuring raw recall.

## 2.7 The replica-distinguishing-power factor

### Why the matched-tokens-alone score over-credits trivial overlaps

The baseline score (`matched_tokens × freshness`) silently makes a strong
assumption: that an overlap *distinguishes* one replica from its siblings.
For Llama-style chat-template framing — every replica identically holds a
~16-token system header — that assumption breaks. Every replica matches the
prefix; the response says `PREFIX_MATCH` on a match that any replica could
equally serve; the routing layer gets credited with a decision it didn't
make. Production logs from a cache-stress benchmark showed roughly ~70%
of `PREFIX_MATCH` responses were this 1-block trivial-overlap shape.

The §2.6 fixed-token floor solves it for the chat-template framing case
but **does not generalize** to long shared prefixes — a RAG corpus header
of 1500 tokens or a custom system prompt of 250 tokens easily clears any
reasonable fixed floor, even though every replica holds them identically.
The right signal is not "how many tokens matched" but "does this match
distinguish replicas, or do they all have it?"

### The factor

For each scored replica `R`, the ranker multiplies the score by

```
distinguishing_power_R = 1 - num_matching_at_R's_depth / total_replicas
```

- **`total_replicas`** is the count of replicas with **at least one
  prefix entry observed** in the request's engine domain (`tenant`,
  `model`, `hash_scheme`), captured from the per-scope serving counter
  under the index read lock so it stays consistent with the prefixes
  view the lookup just observed. **Important definition caveat:** a
  replica that's running in the cluster but has not (yet) reported any
  prefix in this scope — just started, just cleared its cache, or
  serves a different scope — is invisible to the index and therefore
  absent from this denominator. Consequence: a 2-of-3 partial-diffusion
  case where two replicas hold the prefix and the third has reported no
  prefix in scope is scored as 2-of-2 (factor `0`) and downgrades; the
  cache plane has no evidence the third replica is a peer. This matches
  the rest of the index — `TENANT_HOT` warmth, `servingByScope` scope
  checks, and the `UNKNOWN_*` miss classifier all use the same
  "observed via reported state" definition. Production engines (the C1
  KV-event subscriber reports prefix and stats together on
  `ReportCacheState`) appear in the denominator within one TTL cycle;
  the visible-only edge case matters only briefly at cold start and
  cache reset.
- **`num_matching_at_R's_depth`** is per-replica:
  - For exact-match (single-blob path), every scored replica matched the
    same prefix hash so the value is `len(scores)` and the factor is
    uniform across the response.
  - For chain lookups (§2.5), it's the count of replicas whose
    `matched_tokens` is at least R's `matched_tokens` — R itself plus
    every replica that went at least as deep. A uniquely-deepest replica
    sees the strongest factor `(1 − 1/N)`; a shallow-only sibling sees a
    much weaker one (or `0` when every replica reached the same depth).
- **`total_replicas ≤ 1` degrades the factor to `1.0`** so a
  single-replica deployment preserves the baseline ranking exactly —
  there's nothing to distinguish among; the factor would otherwise be
  definitionally `0` and downgrade every hint off the `PREFIX_MATCH`
  path (the final reason code on the downgrade is affinity-toggle-
  dependent: `AFFINITY_HINT` under default-enabled affinity with a
  usable seed + serving replica, or `NO_HINT` under
  `affinityRouting: Disabled`).

The full ranker formula composing all factors today:

```
score = matched_tokens × freshness × pressure_factor × slo_bias × distinguishing_power
```

The other factors compose unchanged; this is one more multiplicative term,
the same way `pressure_factor` and `slo_bias` were added on top of the §2
baseline.

### Workload-shape behavior

| Workload shape (3-replica deployment) | num_matching | total | factor | Behavior |
|---|---|---|---|---|
| Chat-template framing — every replica holds it | 3 | 3 | 0 | Score=0 → caught by post-score floor → `StrategyNone` → `AFFINITY_HINT` (default-enabled affinity) or `NO_HINT` (disabled) |
| RAG corpus header — every replica holds it | 3 | 3 | 0 | Score=0 → caught by post-score floor → `StrategyNone` → `AFFINITY_HINT` or `NO_HINT` per the affinity toggle |
| Custom shared system prompt — every replica holds it | 3 | 3 | 0 | Score=0 → caught by post-score floor → `StrategyNone` → `AFFINITY_HINT` or `NO_HINT` per the affinity toggle |
| Specific RAG context — only 1 replica holds it | 1 | 3 | 0.67 | Strong score → `PREFIX_MATCH` |
| Partial-diffusion overlap (2 of 3) | 2 | 3 | 0.33 | Weaker score, still `PREFIX_MATCH` if it clears the floor |
| Uniquely-deep chain match (chain §2.5) | 1 at depth 4 | 3 | 0.67 at depth 4, 0 at depth 1 | Deep matcher dominates ranking |
| Single-replica deployment | 1 | 1 | 1.0 (degraded) | Distinguishing-power factor preserved at baseline; the matched-tokens (§2.6) and routing-floor (§2.7) floors still run, so a single-replica match below `minimumMatchedTokens` (default 64) still downgrades off the `PREFIX_MATCH` path (to `AFFINITY_HINT` or `NO_HINT` per the affinity toggle). Set both floors to their opt-outs to reproduce pre-floor every-match-surfaces behavior. |

### The post-score floor — `CachePolicy.spec.routingFloorScore`

The factor zeroes the score for trivial overlaps; the **routing floor
score** decides how much "near-zero score" still counts as a useful
routing hint. A `PREFIX_MATCH` whose top score falls below the
per-namespace `routingFloorScore` is downgraded to `StrategyNone`,
which surfaces as `AFFINITY_HINT` under default-enabled affinity (with
a usable seed + serving replica) or as `NO_HINT` when disabled — so
the gateway round-robins honestly under opt-out or pin-stable under
the default. Applied in the service layer (after the
index ranks), BEFORE the LFU `CreditHits` step, so a non-delivered hint
never bumps an LFU counter.

- Default value: `0.1` (server-wide `DefaultRoutingFloorScore`, effective
  for tenants with no `CachePolicy`). A near-zero floor: catches the
  score=0 trivial-overlap case AND the next slice of near-zero scores
  produced by heavy diffusion × small matched_tokens, high pressure
  (pressure_factor near 0), and near-expired freshness.
- Operator tunes via `CachePolicy.spec.routingFloorScore` (stringified
  float; CRD pattern validation enforces the format). Raise (e.g. `"5"`)
  for stricter routing-signal hygiene; `"0"` disables the floor entirely.
- A namespace WITH a `CachePolicy` returns its configured value as-is,
  including an explicit `"0"` (the operator opt-out).
- Negative values clamp to 0 in the resolver — defensive against a
  hand-crafted `/policy` POST that bypassed the CRD validator.

### Composition with the §2.6 matched-tokens floor

The §2.6 matched-tokens floor and the §2.7 score floor compose
deliberately, applied in order in the service handler:

1. **§2.6 first — per-replica filter.** Replicas whose `matched_tokens`
   falls below `minimumMatchedTokens` are dropped from the scored set
   via `LookupResult.RetainReplicas` (LFU hits pruned in lockstep). A
   surviving long-prefix replica still ships; only sub-floor siblings
   are dropped. If no replica survives, the whole response downgrades
   to `StrategyNone`, which surfaces as `AFFINITY_HINT` under
   default-enabled affinity with a usable seed + serving replica or as
   `NO_HINT` under `affinityRouting: Disabled`.
2. **§2.7 second — whole-response gate.** If at least one replica
   survived §2.6, the top score (post-distinguishing-power) is
   compared to `routingFloorScore`. Below the floor → downgrade the
   whole response to `StrategyNone` and ship empty scores; the
   `StrategyNone` then surfaces as `AFFINITY_HINT` or `NO_HINT` per
   the affinity toggle.

Both downgrades run **before** `CreditHits`, so neither pathway bumps an
LFU counter for an undelivered hint. The two floors are orthogonal: an
operator can disable either via its opt-out value (`0` / `"0"`) and the
other still fires. A namespace with `minimumMatchedTokens: 0` and
`routingFloorScore: "0"` reproduces the pre-floor every-non-zero-match-
is-`PREFIX_MATCH` behavior — useful for raw-recall benchmarking and
debugging the ranker.

### Worked example — chain match against three replicas at different depths

Index state, all replicas in `(tenant=t, model=m, scheme=vllm)`:

| Replica | Holds blocks (in order) | Matched tokens |
|---|---|---|
| `r0`    | `b1, b2, b3, b4`        | 256 |
| `r1`    | `b1, b2`                | 128 |
| `r2`    | `b1`                    |  64 |

Request: chain `[b1, b2, b3, b4]`, freshness ≈ 1, no pressure / no SLO.
`total_replicas = 3`.

Per-replica factor (depth-aware):
- `r0` is the only replica at depth 4 → `1 − 1/3 = 0.667`
- `r1` shares its depth (128 tokens) with `r0` (whose depth covers it)
  → 2 replicas at depth-2-or-deeper → `1 − 2/3 = 0.333`
- `r2`'s depth (64 tokens) is reached by all three → `1 − 3/3 = 0`

Final scores: `r0 = 256 × 0.667 ≈ 170.7`, `r1 = 128 × 0.333 ≈ 42.7`,
`r2 = 64 × 0 = 0`.

With default thresholds (`minimumMatchedTokens: 64`,
`routingFloorScore: "0.1"`):

- **§2.6 pass:** `r0` (256), `r1` (128), `r2` (64) all clear 64 — no
  replicas dropped.
- **§2.7 pass:** top score is `r0` at 170.7, clears 0.1 → ship the full
  ranked list `[r0, r1, r2]` with `reason_code = PREFIX_MATCH`.

If the operator raises `routingFloorScore: "200"`, `r0`'s 170.7 falls
below; the top score doesn't clear, the response downgrades to
`StrategyNone` (whole-response gate, not per-replica filtering — even
`r0`'s scored row is dropped) and surfaces as `AFFINITY_HINT` under
default-enabled affinity with a usable seed + serving replica, or as
`NO_HINT` under `affinityRouting: Disabled`.

If `r0` had not held the deeper chain — i.e. all three replicas only
matched `[b1]` — every score would collapse to 0 (all-three-at-depth-1
→ factor 0) and the response downgrades to `StrategyNone` entirely
(same `AFFINITY_HINT` / `NO_HINT` split per the affinity toggle).

### Scope caveat — what this factor does NOT handle

- **Single-replica deployments.** The factor degrades to 1.0 so the
  cluster shape preserves baseline ranking. There's nothing to
  distinguish among; a separate signal would be needed to dampen
  trivial matches in this shape, and is not in scope.
- **Cross-engine domain comparisons.** `total_replicas` is scoped to
  `(tenant, model, hash_scheme)` — replicas serving a different engine
  domain don't enter the denominator. This matches §2.5's chain-walk
  scope and the existing `TENANT_HOT` engine-domain guard.
- **Time-varying cardinality.** Each lookup uses the cardinality at
  read time. A replica that joined the scope a millisecond after the
  lookup ran isn't counted. Soft-state semantics: a transient miscount
  is a missed hint at worst, never a wrong answer. The TTL-sweep
  window between an entry going stale and the eviction loop sweeping
  it can briefly inflate the denominator; the resulting factor stays
  slightly above 0 instead of exactly 0 for an overlap all currently-
  fresh replicas share, which the post-score floor still catches on
  the next sweep tick.

## 3. Pressure-aware scoring — locality vs. load

Replicas also report `ReplicaStats.pressure`, a 0–1 saturation signal
(higher = more loaded). The baseline ignores it, so a token-rich but
saturated replica blindly wins over a fresher, idle peer. The hint then
just piles more traffic onto an already-overloaded server.

The pressure-aware factor demotes the score by load:

```
pressure_factor = max(0, 1 − PressureWeight × pressure)
score          = matched_tokens × freshness × pressure_factor
```

Worked example with `PressureWeight = 1`:

| Replica       | tokens | freshness | pressure | baseline score | new score |
|---|---|---|---|---|---|
| `big-but-hot` | 100    | 1.0       | 0.9      | 100            | **10**    |
| `small-cool`  |  50    | 1.0       | 0.0      |  50            | **50**    |

Under the baseline `big-but-hot` wins; with pressure folded in, the smaller
but idle peer overtakes it. **The bias is against load**, not toward it —
high pressure *lowers* the score.

`pressure = 0` (no stats reported, or genuinely idle) ⇒ factor `1.0` ⇒
baseline behavior. `PressureWeight = 0` disables the penalty entirely
(useful kill switch).

Two correctness rules apply to *which* pressure value the ranker reads:

- **Stats must come with a fresh payload.** A `REPLICA_UPDATED` liveness
  event refreshes "I'm alive" without supplying new stat numbers. The
  ranker tracks a separate `statsReported` timestamp that *only* a real
  `ReportCacheState` ingest with stats payloads updates, so a stale
  high-pressure reading kept artificially "alive" by liveness events can't
  keep demoting a perfectly fresh prefix score.
- **Stats must be within the global TTL.** Otherwise we'd be applying a
  pressure reading from before the sweeper had a chance to clear it.

## 4. `TENANT_HOT` fallback — soft locality on a prefix miss

The baseline gives up with `NO_HINT` whenever no replica has the *exact*
prefix cached. But that's often pessimistic. Even without an exact
overlap, the gateway can often still benefit from sending the request to
a replica that's already serving cache hits for this tenant: its KV
allocator is warm, its tokenizer is loaded, its block manager already
holds related state, and its tenant context is hot.

When the prefix-match path returns empty, the ranker runs a second
strategy:

1. Find replicas under `(tenant, model)` whose stats are recent
   (`statsReported` within `TenantHotMaxAge`, default 5 min) AND whose
   `hit_rate` is at least a floor (default `0.1`). These are "warm."
2. Restrict to replicas that *actually serve* the requested
   `hash_scheme` — i.e. they hold at least one prefix entry in the
   request's engine domain. Without this guard, a stats-only update with
   an unrelated `hash_scheme` could leak into a hint for a different
   engine.
3. Score them as

   ```
   score = hit_rate × recency × pressure_factor × slo_bias
   ```

   where `recency = 1 − age / TenantHotMaxAge`. There's no prefix overlap,
   so `MatchedTokens = 0` on each returned `ReplicaScore`. **The gateway
   must rely on `reason_code`, not `matched_tokens`, to recognize this
   branch.**
4. Return them with `reason_code: TENANT_HOT`. If nothing qualifies, the
   miss-classifier runs (see [`lookuproute-diagnostics.md`](./lookuproute-diagnostics.md)):
   a same-key novel-prefix miss falls through to `NO_HINT`, but a
   mismatched contract key (`tenant_id`, `model_id`, or `hash_scheme`)
   surfaces as the matching `UNKNOWN_*` code.

This is a deliberately *softer* hint than `PREFIX_MATCH`. The gateway is
free to honor it or fall back to its default policy. Setting
`TenantHotMaxAge = 0` disables the fallback entirely.

A few subtleties worth knowing:

- The "warm" check on a candidate replica uses an O(1) secondary index
  keyed by `(tenant, model, hash_scheme)` — no full scan of the
  cache-state map per lookup, even when the index is large.
- A prefix entry that's been swept (passed the global TTL) decrements
  that secondary index in lockstep, so a long-stale replica naturally
  drops out of the `TENANT_HOT` candidate set.

## 5. SLO-aware bias — freshness matters more when latency is tight

### SLO is a field on `LookupRouteRequest`, not a separate RPC

Worth pinning before the formulas: **there is no "SLO" RPC.** `SLO` is a
nested message tucked into the existing `LookupRouteRequest`:

```
LookupRouteRequest {
  string model_id;
  string tenant_id;
  bytes  prefix_hash;
  int32  prefix_token_count;
  string hash_scheme;
  SLO    slo;            // ← TTFT / TBT budget the gateway is targeting
}

SLO {
  int32 ttft_ms;         // target time-to-first-token (ms)
  int32 tbt_ms;          // target time-between-tokens (ms)
}
```

It has been on the wire since the contract was first defined; what changes
in this layer is that the server now **reads** the field instead of
ignoring it. There is still one RPC, one response envelope, and the same
reason-code vocabulary. SLO is a knob that reshapes the existing
`PREFIX_MATCH` and `TENANT_HOT` ranking — not a new strategy.

It is also **not enforcement**: the cache plane does not refuse to answer
when SLO is tight, does not time the response out against `ttft_ms`, and
does not promise to meet the SLO. It uses the budget purely as a hint
about what the gateway cares about. "We hint, the gateway decides" still
holds.

### The bias

Under a tight TTFT budget, the cost of routing to a stale replica (it
still has to rebuild context) is higher relative to the cost of slightly
less prefix overlap. So when TTFT is tight, freshness should weigh more
than matched-token count.

The bias is a single multiplicative factor on top of everything else:

```
slo_bias = 1 + freshness × SLOTightBias    if ttft_ms < SLOTightTTFTMs
         = 1                                otherwise
```

For example with `SLOTightBias = 1`, a perfectly fresh candidate
(`freshness = 1`) gets a 2× boost; a stale one (`freshness = 0.1`) only
1.1×. Effect: under tight SLO, fresher candidates pull ahead of
token-richer-but-staler ones more aggressively.

The same factor composes into both shipped strategies — it multiplies
into the `PREFIX_MATCH` score and into the `TENANT_HOT` fallback score
identically. There is no "SLO strategy" or "SLO reason code"; a tight-SLO
response still comes back as `PREFIX_MATCH` (or `TENANT_HOT`, or
`NO_HINT`), just with a different internal ranking.

`SLOTightBias = 0` disables the bias; an unset (zero) TTFT budget skips
it; a TTFT budget above the threshold skips it. So baseline behavior is
preserved when no SLO is supplied.

### `tbt_ms` — plumbed but not yet used

`SLO.tbt_ms` (time-between-tokens budget) is threaded all the way through
from the proto request into the index's `LookupRequest.TBTBudgetMs`, but
the current scoring formula does not reference it. It is a placeholder
for a future tuning hook — e.g. a tight TBT budget might bias toward
replicas with low pressure so the decode loop isn't queued behind other
work. Wiring is in place; the factor is currently a no-op.

### Where the SLO budget comes from

`SLO` is **set by the gateway, per request** — there is no `SLO` field
on any cache-plane CRD. The cache plane consumes whatever budget the
gateway sends in `LookupRouteRequest.slo` and uses it to rank the
response. How the gateway derives that budget is its own decision:

- From the inference request itself (e.g. a header or parameter on the
  inbound API call).
- From the gateway's own per-tenant or per-model routing policy.
- From a global deployment configuration.

A request with no SLO message (or `ttft_ms = 0`) is treated as
"unspecified" and the SLO bias factor collapses to `1` — the cache plane
never invents a budget on its own.

> **Not the same as `CachePolicy.lookupTimeoutMs`.** That field (in the
> `CachePolicy` CRD, set per tenant by a cluster operator) is the budget
> the **server** spends on its own lookup before giving up — "if I can't
> rank in N milliseconds, answer `TIMEOUT`." It governs the server's
> internal time. The `SLO.ttft_ms` on `LookupRouteRequest` is the
> **gateway's end-to-end TTFT target for the inference request itself**
> and is used purely as a *ranking signal*, never as enforcement. Two
> different things that both sound like "SLO."

## 6. Putting the factors together

A single score expression covers both shipped scoring paths:

```
score (PREFIX_MATCH path) = matched_tokens × freshness × pressure_factor × slo_bias × distinguishing_power
score (TENANT_HOT  path) = hit_rate       × recency    × pressure_factor × slo_bias
```

(The TENANT_HOT path carries `matched_tokens = 0` — no prefix overlap — so
the distinguishing-power factor would always multiply by `1.0` there and is
omitted from the formula for clarity.)

with

```
pressure_factor      = max(0, 1 − PressureWeight × pressure)
slo_bias             = 1 + decay × SLOTightBias                  (when TTFT is tight)
                     = 1                                         (otherwise)
distinguishing_power = 1 − num_matching_at_depth / total_replicas (when total_replicas ≥ 2)
                     = 1                                         (otherwise — single-replica)

where `decay` is:
  - `freshness` in the PREFIX_MATCH path (prefix entry's lastSeen vs. TTL)
  - `recency`   in the TENANT_HOT  path (stats entry's statsReported vs. TenantHotMaxAge)
```

The distinguishing-power factor applies only on the PREFIX_MATCH path
(see §2.7) and is per-replica depth-aware for chain matches.

And the strategies compose into a single orchestrator:

```
LookupRoute(req):
   if hash_scheme is empty            → NO_HINT             (engine domain unknown — fail open)
   if there is an exact or chain match:
       drop replicas whose matched_tokens < §2.6 floor      (per-namespace minimumMatchedTokens; default 64)
       if no replica survives          → StrategyNone        (downgrade — see fallback step below)
       else if top survivor's score < §2.7 floor → StrategyNone  (per-namespace routingFloorScore; default 0.1)
       else                            → PREFIX_MATCH        (ranked by the full score over the survivors)
   else if any tenant-warm replicas   → TENANT_HOT          (ranked by the full score, soft hint)
   else (miss-classifier runs):
       if index is globally empty     → StrategyNone        (cold start — downgrade — see fallback step below)
       if tenant_id not in index      → UNKNOWN_TENANT      (configuration drift; keeps precedence over AFFINITY_HINT)
       if (tenant, model) not in index → UNKNOWN_MODEL       (configuration drift; keeps precedence over AFFINITY_HINT)
       if scope not in index          → UNKNOWN_HASH_SCHEME (configuration drift; keeps precedence over AFFINITY_HINT)
       else                            → StrategyNone        (genuine novel prefix — downgrade — see fallback step below)

   # Result-side gates run in the handler (buildLookupResponse):
   if request.effectivePrefixTokens < minimumPrefixTokens AND result ∈ {PREFIX_MATCH, TENANT_HOT}:
       result ← StrategyNone                                 (operator intent: tiny prompts don't surface a positive hint;
                                                              pre-lookup short-circuit only fires under affinityRouting: Disabled)

   # Final affinity fallback for the StrategyNone branch:
   if result == StrategyNone:
       if affinityRouting: Enabled (kubebuilder default) AND request is structurally well-formed
          (non-empty hash_scheme, chain arrays agree in length) AND request has a usable seed
          (non-empty block_hashes OR non-empty prefix_hash) AND ≥1 replica in
          servingByScope[(tenant, model, hash_scheme)]:
              → AFFINITY_HINT  (single stable replica from SHA-256(seed) % len(sorted servingByScope))
       else:
              → NO_HINT        (fail open; gateway round-robins)
```

See [`lookuproute-diagnostics.md`](./lookuproute-diagnostics.md) for the
full design behind the `UNKNOWN_*` codes.

Every factor and threshold is tunable through a `RankerConfig` (in-binary
defaults) or a `CachePolicy` CR (per-namespace overrides). Defaults are
set so that:

- A deployment with **no stats reported** sees pressure factor 1 and no
  `TENANT_HOT` candidates qualify — those two strategies collapse to the
  pre-PR baseline. The §2.6 matched-tokens floor and the §2.7
  routing-floor-score still apply on top (defaults 64 and `"0.1"`); set
  both to their opt-outs to reproduce the strict pre-floor
  every-overlap-is-`PREFIX_MATCH` ranker behavior end-to-end.
- A request with **no SLO hint** sees `slo_bias = 1` (no freshness boost).
- A **single-replica deployment** sees `distinguishing_power = 1.0` so the
  factor never demotes a hint on the simplest cluster shape.
- Setting `PressureWeight = 0`, `SLOTightBias = 0`,
  `TenantHotMaxAge = 0`, AND `routingFloorScore: "0"`,
  `minimumMatchedTokens: 0` simultaneously collapses the pressure / SLO /
  TENANT_HOT layers AND both result-side floors — but NOT the
  cardinality factor. For single-replica deployments this *is* the
  original B6 `matched_tokens × freshness` ranker (distinguishing-power
  degenerates to 1.0 with one replica); for multi-replica deployments
  it is "pre-floor raw recall with cardinality-adjusted scores."

## 7. The reason-code summary

| Code                  | When it fires                                                                                                                                                                                                                       | What the gateway treats it as           |
|---|---|---|
| `PREFIX_MATCH`        | At least one replica holds the exact prefix — or the leading block-hash run (§2.5) — in the requested engine domain AND its `matched_tokens` clears the per-namespace `minimumMatchedTokens` floor (§2.6; default 64) AND the top surviving replica's score (after the §2.7 distinguishing-power factor multiplies in) clears the per-namespace `routingFloorScore` floor (default `"0.1"`). Sub-floor matched-tokens are filtered per-replica; sub-floor score downgrades the whole response to `StrategyNone` (which then surfaces as `AFFINITY_HINT` or `NO_HINT` per the affinity toggle — see below). | Strongest hint — route to top-ranked    |
| `TENANT_HOT`          | Prefix miss, but the tenant has recently warm replicas serving this `hash_scheme`. NB: a tiny request below the per-namespace `minimumPrefixTokens` is also downgraded out of TENANT_HOT to `StrategyNone` (the same operator intent that downgrades sub-gate `PREFIX_MATCH`). | Softer hint — use or fall back          |
| `AFFINITY_HINT`       | The prefix-match path would otherwise return `NO_HINT` (no match, OR matched-tokens / routing-floor downgrade, OR minimumPrefixTokens downgrade) AND `CachePolicy.spec.affinityRouting` is `Enabled` (the kubebuilder default) AND the request is structurally well-formed (non-empty `hash_scheme`, balanced chain arrays) AND the request has a usable seed AND `servingByScope[(tenant, model, hash_scheme)]` is non-empty. The server returns a single stable replica picked by `SHA-256(canonical_seed) % len(sorted servingByScope)`. Diagnostic codes (`UNKNOWN_*`) and `TIMEOUT` keep precedence over `AFFINITY_HINT`. | Stable-pin hint — route to the single returned replica; treat as PREFIX_MATCH for routing, distinct code for metrics |
| `NO_HINT`             | Empty `hash_scheme`, malformed chain, no prefix match with no diagnosable contract-key mismatch (a genuine novel prefix or a cold-start window where the index holds no data yet), every replica's `matched_tokens` falls below the per-namespace §2.6 `minimumMatchedTokens` floor (default 64), the top per-replica score falls below the per-namespace §2.7 `routingFloorScore` floor (default `"0.1"`) — the every-replica-has-it case that drives distinguishing_power to 0 — or any other unspecified outcome. With `affinityRouting: Enabled` (the default) any downgrade case that has a usable seed + serving replica surfaces as `AFFINITY_HINT` instead; the `NO_HINT` cases below default-enabled affinity are the ones where no usable seed or no serving replica is available. | Default routing; cache plane invisible  |
| `UNKNOWN_TENANT`      | Prefix miss + `TENANT_HOT` miss + the supplied `tenant_id` has zero prefix entries while some other tenant in the index does                                                                                                         | Likely SDK/producer mismatch — fail-open; surface as configuration error |
| `UNKNOWN_MODEL`       | Prefix miss + `TENANT_HOT` miss + the tenant has entries but the requested `(tenant, model)` does not                                                                                                                                | Same — fail-open; configuration error   |
| `UNKNOWN_HASH_SCHEME` | Prefix miss + `TENANT_HOT` miss + `(tenant, model)` has entries but the requested `hash_scheme` is absent for it                                                                                                                     | Same — fail-open; configuration error   |

`TIMEOUT` is reserved in the contract vocabulary for a per-tenant lookup
deadline breach and is handled by the policy-server propagation path — not
by this ranking layer. See
[`lookuproute-diagnostics.md`](./lookuproute-diagnostics.md) for the rule
behind the `UNKNOWN_*` codes (including the empty-key and cold-start
carve-outs that keep them on `NO_HINT`).

## 8. Worked examples

Six concrete scenarios that exercise the strategies in §2–5 end-to-end.
Each shows the relevant index state, the request, the score
computation, and the response. All examples use the default
`RankerConfig`: `PressureWeight = 1`, `SLOTightTTFTMs = 200 ms`,
`SLOTightBias = 1`, `TenantHotMaxAge = 5 min`,
`TenantHotMinHitRate = 0.1`.

### 8.1. Baseline: one replica holds the prefix

Index state — tenant `team-a`, model `m`, scheme `vllm`:

| Replica | Prefix | Tokens | Freshness | Pressure |
|---|---|---|---|---|
| `r1`    | `p`    | 100    | 1.0       | 0.0      |

Request: `{tenant=team-a, model=m, hash_scheme=vllm, prefix_hash=p}`, no SLO.

Computation: `100 × 1.0 × (1 − 1 × 0.0) × 1 = 100`.

Response: `reason_code=PREFIX_MATCH`, scores `[{r1, score=100, matched_tokens=100}]`.

This is the exact pre-PR baseline: with no stats and no SLO, every new
factor collapses to 1 and the score is `matched_tokens × freshness`.

### 8.2. Pressure breaks a tie the baseline didn't see

Index state:

| Replica       | Prefix | Tokens | Freshness | Pressure |
|---|---|---|---|---|
| `big-but-hot` | `p`    | 100    | 1.0       | 0.9      |
| `small-cool`  | `p`    |  50    | 1.0       | 0.0      |

Request: same as §8.1, no SLO.

Computation:
- `big-but-hot`: `100 × 1.0 × (1 − 1 × 0.9) × 1 = 100 × 0.1 = 10`
- `small-cool`:  `50 × 1.0 × (1 − 1 × 0.0) × 1 = 50`

Response: `PREFIX_MATCH`, ranked `[small-cool (50), big-but-hot (10)]`.

The pure baseline (`tokens × freshness`) would have given `big-but-hot`
a score of `100` vs `small-cool`'s `50` and routed traffic to the
already-saturated replica. The pressure factor flips it: locality
weighed against load.

### 8.3. Tight TTFT bias reshapes ordering

Index state:

| Replica       | Prefix | Tokens | Freshness | Pressure |
|---|---|---|---|---|
| `big-old`     | `p`    | 100    | 0.6       | 0.0      |
| `small-fresh` | `p`    |  50    | 1.0       | 0.0      |

**Request A — no SLO**:
- `big-old`:     `100 × 0.6 × 1 × 1 = 60`
- `small-fresh`: `50 × 1.0 × 1 × 1 = 50`
- Response: `PREFIX_MATCH`, `[big-old (60), small-fresh (50)]`.

**Request B — `slo.ttft_ms = 50`** (below the 200 ms threshold → tight):
- `big-old`:     `100 × 0.6 × 1 × (1 + 0.6 × 1) = 100 × 0.6 × 1.6 = 96`
- `small-fresh`: `50 × 1.0 × 1 × (1 + 1.0 × 1) = 50 × 1.0 × 2.0 = 100`
- Response: `PREFIX_MATCH`, `[small-fresh (100), big-old (96)]`.

Under loose latency the token-rich older replica wins; under a tight
TTFT budget the fresher one does. Both responses still say
`PREFIX_MATCH` — the shape doesn't change, the internal ranking does.

### 8.4. Prefix miss + warm tenant replica → `TENANT_HOT`

Index state (no replica holds the *requested* prefix; one replica is
"warm" for the tenant in the requested engine domain):

| Replica  | Scheme | Prefix held | Hit rate | Pressure | Stats age |
|---|---|---|---|---|---|
| `r-warm` | vllm   | `other`     | 0.8      | 0.1      | 30 s      |

Request: `{tenant=team-a, model=m, hash_scheme=vllm, prefix_hash=novel}`,
no SLO.

Prefix-match path: empty (no replica holds `novel`).

Tenant-hot fallback (defaults `TenantHotMaxAge = 5 min`,
`TenantHotMinHitRate = 0.1`):
- `r-warm` reported 30 s ago (well under 5 min), `hit_rate = 0.8 ≥ 0.1`,
  and it holds at least one prefix in `vllm` (`other`) — qualifies.
- `recency = 1 − 30 s / 5 min = 0.9`
- `pressure_factor = 1 − 1 × 0.1 = 0.9`
- `slo_bias = 1`
- `score = 0.8 × 0.9 × 0.9 × 1 = 0.648`

Response: `TENANT_HOT`, scores `[{r-warm, score=0.648, matched_tokens=0}]`.

`matched_tokens` is `0` because there's no prefix overlap — the gateway
must rely on `reason_code` (not `matched_tokens`) to tell `TENANT_HOT`
apart from a real prefix hit.

### 8.5. Engine-domain guard rejects a warm replica from a different scheme

Index state — both replicas warm, but for different engines:

| Replica   | Scheme | Prefix held | Hit rate | Pressure | Stats age |
|---|---|---|---|---|---|
| `r-vllm`  | vllm   | `p1`        | 0.7      | 0.0      | 1 min     |
| `r-sgl`   | sglang | `p2`        | 0.9      | 0.0      | 1 min     |

Request: `{model=m, hash_scheme=vllm, prefix_hash=novel}`, no SLO.

Prefix-match path: empty.

Tenant-hot fallback:
- `r-vllm`: warm AND holds at least one prefix under `vllm` → qualifies.
- `r-sgl`:  warm AND has a higher hit rate, but its only prefix entry
  is in `sglang` — it does not serve the requested engine domain.
  **Rejected**: hinting to it would route across engines, which the
  contract forbids.

Response: `TENANT_HOT`, scores only `[{r-vllm, ...}]`. `r-sgl` does not
appear.

This is the bug the engine-domain guard prevents: stats are
scheme-independent in the index (a `ReplicaStats` applies across
engines), so without this check a stats-only update — or an update
under a different scheme — could leak into a hint for the wrong engine.

### 8.6. Empty `hash_scheme` → unconditional `NO_HINT`

Index state: any.

Request: `{model=m, hash_scheme="", prefix_hash=<any bytes>}`.

The cache plane can't safely match an opaque `prefix_hash` without
knowing which engine domain it belongs to (a vLLM hash and an SGLang
hash of the same bytes are unrelated). Both strategies short-circuit
on the empty `hash_scheme` and the response is unconditionally
`NO_HINT` with empty scores — fail open.

This is also the response shape whenever the prefix-match path is empty
AND no replica qualifies for `TENANT_HOT` AND no usable affinity
fallback fires (`affinityRouting: Disabled` on the per-namespace
policy, OR no replica known to serve the `(tenant, model, hash_scheme)`
engine domain, OR no usable seed). When affinity IS Enabled (the
kubebuilder default) and the request has a usable seed plus at least
one serving replica, the response surfaces as `AFFINITY_HINT` instead
— with a single stable replica pick — and the gateway routes to it.
The gateway treats `NO_HINT` and the diagnostic codes identically
(route per its default policy), while `AFFINITY_HINT` is treated as a
positive routing assertion.

## 9. Where the code lives

- Scoring + strategy orchestration: [`pkg/index/index.go`](../../pkg/index/index.go)
  — see `Lookup`, `lookupExact`, `lookupChain` (§2.5 chain walk),
  `LookupRoute`, `tenantHotCandidates`, `RankerConfig`.
- Handler glue (proto ↔ index, `Strategy` → `reason_code`):
  [`pkg/server/inferencecache_service.go`](../../pkg/server/inferencecache_service.go).
- Tests covering each strategy and the baseline-preservation invariant:
  [`pkg/index/index_test.go`](../../pkg/index/index_test.go) and
  [`pkg/server/server_test.go`](../../pkg/server/server_test.go).
