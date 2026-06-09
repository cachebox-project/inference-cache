# LookupRoute contract diagnostics

How `LookupRoute` distinguishes a *novel prefix* (the cache plane has the
requested `(tenant, model, hash_scheme)` populated but not this particular
prefix) from a *contract-key mismatch* (the caller asked with a key the cache
plane does not recognize at all). The latter is almost always a misconfigured
gateway/SDK, and surfacing it as a specific `reason_code` is what lets
operators debug "100% `NO_HINT`" without re-deriving the layering from
captured packets.

This is a **wire-level addition only** — no proto schema or field-number
change. The `proto/` edit that introduced the codes only widened the inline
comment on the `reason_code` field; the regenerated stubs picked up the
comment, with no binary-format change. `reason_code` is a `string`, and old
clients degrade `UNKNOWN_*` to their `NO_HINT` default per the
forward-compatibility rule in
[`../reference/reason-codes.md`](../reference/reason-codes.md).

## 1. The silent-failure pattern this closes

A `LookupRoute` request asks the server for hints under a triple of contract
keys: `tenant_id`, `model_id`, and `hash_scheme`. The prefix lookup itself is
the **leaf** of that triple — what the request *really* wants is "do any
replicas hold *this* prefix under the *(tenant, model, hash_scheme)* I'm
asking about?".

Today every miss — whether the keys are right and the prefix is genuinely
novel, or the keys are wrong and the data is sitting one key away — collapses
to a single `reason_code: NO_HINT`. The server can't tell the gateway "you
asked the wrong question" because it has no vocabulary for it.

Two production silent-failure patterns observed end-to-end on a real cluster
share this shape:

- **Wrong `hash_scheme`.** Ingest under `hash_scheme="vllm"`; lookup under
  `hash_scheme="vllm-v1"`. The server has the entry; the wrong scheme makes
  the prefix key (which is opaque bytes per the contract) un-findable.
  Returns `NO_HINT`.
- **Wrong `tenant_id`.** Ingest under `tenant_id="ic-smoke"` (the
  kvevent-subscriber sidecar Helm chart sets `--tenant-id=$(POD_NAMESPACE)`).
  Lookup under `tenant_id="default"` (the OpenAI-API-shaped SDK default that
  a naive gateway client picks up). Same prefix hash. Empirically:
  `tenant_id="ic-smoke"` → `PREFIX_MATCH` with
  `estimatedCacheHitProb=0.9997`; `tenant_id="default"` → `NO_HINT`. Looks
  the same from outside.

Both shaped identically: the right state exists in the index under a
different value of one contract key. There is currently no way for the caller
to tell that from a "this is genuinely the first time we've seen this prompt"
miss without out-of-band inspection.

The fix in this design is the smallest one that closes both: add specific
`reason_code` values for each contract-key mismatch, emit them on the miss
path, document them so SDK authors and operators can react.

## 2. The rule

> Every contract key that can mismatch returns a specific `reason_code` on
> key-level no-data — not the catch-all `NO_HINT`.

`LookupRoute` has three such keys: `tenant_id`, `model_id`, `hash_scheme`.
This design adds one reason code per key. Future contract surfaces with
mismatchable keys (e.g. `LookupPDRoute` and `pd_topology_ref`) follow the
same rule when they ship; "diagnose key-mismatches" becomes part of the
contract-diagnostics pattern.

The rule does **not** apply to:

- **Keys the caller failed to supply.** An empty `tenant_id`, `model_id`,
  or `hash_scheme` is a contract violation (the request is missing a
  required scoping field), not a mismatch. Empty-key cases continue to
  surface as `NO_HINT` per the existing fail-open semantics. The
  `UNKNOWN_*` codes specifically diagnose "you supplied a value but it
  doesn't match anything we have" — emitting them for a missing field
  would be misleading guidance ("change your value" when the actual fix
  is "supply the field").
- **A globally empty index (cold-start carve-out).** When the server
  holds zero prefix entries — fresh start before any
  `ReportCacheState` lands, a fully drained cluster — every tenant
  query would otherwise classify as `UNKNOWN_TENANT`, flooding
  gateways with configuration-error signals during normal operation.
  The classifier short-circuits the globally-empty case to `NO_HINT`;
  the diagnostic resumes the moment any replica reports, which is the
  asymmetric case the SDK guidance is targeted at (one tenant
  populated, the gateway pointing at another).
- **Policy-gate misses.** The `CachePolicy.spec.minimumPrefixTokens` short-
  circuit returns `NO_HINT` because the lookup never touched the index;
  there's no key-level mismatch to surface.
- **`TIMEOUT` paths.** Deadline-breach lookups never run the classification
  step; they return `TIMEOUT` directly.

## 3. The three new reason codes

| Code | Emitted when | What the caller learns |
|---|---|---|
| `UNKNOWN_TENANT` | The request supplied a non-empty `tenant_id` and the index has **zero prefix entries for that tenant** across every model and hash scheme. | "The `tenant_id` I queried with does not match anything the cache plane has heard about." Almost always a configuration error: the producer (engine sidecar) and consumer (gateway client) disagree on the `tenant_id` convention. |
| `UNKNOWN_MODEL` | The tenant is known (has entries somewhere) but the `(tenant, model_id)` pair has **zero entries**. | "Right tenant, wrong `model_id` — there's no cache state for this model in this tenant." Either the model has never served traffic in this tenant or the model identifier disagrees between the producer and consumer. |
| `UNKNOWN_HASH_SCHEME` | The `(tenant, model_id)` pair has entries, but **none under the request's `hash_scheme`**. | "Right tenant and model, wrong `hash_scheme`. The engine domain you asked about is empty; another engine's entries are there." Almost always a string-value typo or a vLLM-version-bump mismatch (e.g. `vllm` vs `vllm-v1`). |

These are emitted in **outer-to-inner scope order** — tenant first, then
model within tenant, then scheme within (tenant, model). A request whose
tenant is wrong gets `UNKNOWN_TENANT` and stops there; a request whose
tenant matches but whose model doesn't gets `UNKNOWN_MODEL`; and so on. The
classifier never reports a finer key as the mismatched one when a wider
key (above it in scope) already failed — the wider failure subsumes the
question. So the caller always sees the **outermost** mismatched key, which
is the one that has to be fixed first regardless of whether the
deeper-scoped keys are right.

## 4. Where the classification fits

On a `LookupRoute` request the server runs (in order):

1. **Pre-lookup policy gate** — `CachePolicy.spec.minimumPrefixTokens`. Below
   the threshold → `NO_HINT`. Untouched by this design.
2. **Deadline / timeout** — if the lookup exceeds the policy budget or the
   caller's context is already past its deadline → `TIMEOUT`. Untouched.
3. **Prefix-match lookup** — the existing `lookupExact` / `lookupChain`
   ranker. On a hit → `PREFIX_MATCH`. Untouched.
4. **`TENANT_HOT` fallback** — only for non-chain requests, and only when the
   `(tenant, model, hash_scheme)` has at least one prefix entry. On a hit →
   `TENANT_HOT`. Untouched.
5. **Contract-key classification (new)** — runs *only* if step 3 (and step 4
   for non-chain requests) found no candidates and the request supplied a
   non-empty `hash_scheme`. Short-circuits the cold-start case (globally
   empty index → `NO_HINT`), then walks the key triple outer-to-inner
   (widest scope first) and emits the first level that has no data:
   `UNKNOWN_TENANT` → `UNKNOWN_MODEL` → `UNKNOWN_HASH_SCHEME`. If every level
   is populated, the miss is a genuinely novel prefix → `NO_HINT` (the
   existing fail-open default).

The classification runs only on a miss, so it never adds work to the hot path
of a healthy gateway — `PREFIX_MATCH` and `TENANT_HOT` short-circuit before
it. The three lookups it does are all O(1) against secondary indexes
maintained in lockstep with the prefix map (`prefixesByTenant`,
`prefixesByTenantModel`, and per-scope `servingByScope`), so the miss path
stays cheap even when a sustained misconfigured client puts the diagnostic
path under load.

Chain-bearing requests run the same classification as the exact path on a
prefix miss (they do not fall through to `TENANT_HOT` by design — see
[`grpc-contract.md`](grpc-contract.md) "Longest-prefix (block-level)
matching"). The diagnostic codes apply identically — the chain caller's
contract keys can mismatch in the same three ways.

## 5. Gateway-SDK guidance

A new code surfaces wrong-configuration to callers that previously had no way
to see it. Two reactions are correct depending on the caller:

- **`UNKNOWN_*` codes are configuration errors.** A well-behaved gateway-SDK
  treats them like an HTTP 4xx — surface to a log line / metric, emit a
  warning event, and **route as if `NO_HINT` was returned** (fail open — the
  cache plane is still hint-only, never blocking). Do not retry under a
  different key value; the cache plane will not have changed between calls.
- **`NO_HINT` continues to mean "route normally, fail open."** Existing
  callers that bucket every miss into `NO_HINT` keep working unchanged — by
  the contract's forward-compatibility rule, an unrecognized code degrades to
  `NO_HINT`.

### `tenant_id` convention (the source of the wrong-tenant pattern)

The in-cluster `kvevent-subscriber` sidecar always sends
`--tenant-id=$(POD_NAMESPACE)`. A gateway-SDK querying `LookupRoute` therefore
needs `tenant_id` to be the **namespace of the engine pod it is asking
about**, not the OpenAI-API-shaped `"default"` it may have inherited from an
upstream client library.

When a gateway-SDK sees `UNKNOWN_TENANT`, the operator playbook is:

1. Confirm the gateway-SDK is passing the engine namespace as `tenant_id`,
   not a synthetic API-level tenant.
2. Confirm the subscriber sidecar's `POD_NAMESPACE` matches what the gateway
   is sending.

A future kvevent-subscriber change to emit additional tenant aliases (e.g.
also the engine pod's owning Deployment's namespace) is tracked
separately.

## 6. Metrics

`inferencecache_lookup_route_calls_total{reason_code=…}` already exists with
`reason_code` as a label, so the three new values appear as new label values
automatically — no metric-schema change. Dashboards filtering on
`reason_code=NO_HINT` continue to work; dashboards wanting to surface
configuration drift can split out the `UNKNOWN_*` values directly.

A useful operator query is the ratio of `UNKNOWN_*` to all misses, per model
— a sudden spike isolates a misdeploy of a producer or a consumer.

## 7. Backward compatibility

Three reasons the change is safe at v1alpha1:

1. **No proto change.** `reason_code` is a `string`; new values are an
   additive server-side change.
2. **Old clients degrade safely.** The contract's forward-compatibility rule
   says "clients MUST treat any unrecognized code as the no-hint default" —
   so a gateway built against the pre-diagnostics contract sees
   `UNKNOWN_TENANT` and treats it exactly like `NO_HINT`. No new-codes-only
   test failure surface in any existing integration.
3. **Existing emission paths are unchanged.** `PREFIX_MATCH`, `TENANT_HOT`,
   `TIMEOUT`, and the policy-gated / empty-`hash_scheme` `NO_HINT` paths all
   keep their existing behavior. The new codes only narrow the previously
   ambiguous "miss with populated keys vs miss with mismatched keys" case.

## 8. Cross-references

- [`grpc-contract.md`](grpc-contract.md) — `reason_code` vocabulary on the
  `LookupRouteResponse` envelope.
- [`../reference/reason-codes.md`](../reference/reason-codes.md) — the
  reference table of every emitted code, updated in lockstep with this
  change.
- [`lookuproute-ranking.md`](lookuproute-ranking.md) — the ranking strategies
  that produce `PREFIX_MATCH` and `TENANT_HOT`; this design layers diagnostics
  underneath them on the miss path.
