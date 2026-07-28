---
title: "Reason codes"
linkTitle: "Reason codes"
weight: 4
description: >
  The string reason_code vocabulary and when each is emitted.
---

`reason_code` is a **string, not a proto enum** — new codes can be added without breaking old
clients. **Clients must treat any unrecognized code as the no-hint default** (fail open,
round-robin).

## LookupRoute / LookupPDRoute

| Code | When emitted |
|---|---|
| **`PREFIX_MATCH`** | A replica holds the exact prefix or a leading block-run; the ranker returns a non-empty list; at least one replica clears `minimumMatchedTokens` (default 64) **and** the top score clears `routingFloorScore` (default 0.1). |
| **`NO_HINT`** | The fail-open default: a novel prefix with no affinity fallback, an empty/unspecified key, cold start (globally empty index), a request gated below `minimumPrefixTokens`, a sub-floor matched-tokens/score result (with affinity disabled), or a disabled index. |
| **`TENANT_HOT`** | Prefix miss, `strategy.enableTenantHot ≠ false`, and a warm replica exists (recent stats, `hit_rate` above the floor, serving the requested scheme). `matched_tokens = 0`. |
| **`AFFINITY_HINT`** | Would-be `NO_HINT`, `affinityRouting: Enabled`, a usable fingerprint, and ≥1 serving replica. A single stable replica; `score` / `matched_tokens` / `estimated_cache_hit_prob` are all `0`. |
| **`POLICY_REQUIRES_CHAIN`** | `strategy.requireChain: true` and the request has no valid block-hash chain. Returned before touching the index. |
| **`TIMEOUT`** | The lookup deadline expired (context deadline, or `lookupTimeoutMs` elapsed). Fail-open. Clients may also synthesize this locally. |
| **`UNKNOWN_TENANT`** | Miss, and the (non-empty) `tenant_id` has zero entries anywhere (index not globally empty). A contract-key mismatch — likely a misconfigured client. |
| **`UNKNOWN_MODEL`** | Miss, tenant known, but `(tenant, model)` has zero entries. |
| **`UNKNOWN_HASH_SCHEME`** | Miss, `(tenant, model)` has entries, but none under the request's `hash_scheme`. |

`LookupPDRoute` is a stub today and always returns no hint.

### Diagnosing UNKNOWN_*

The three `UNKNOWN_*` codes distinguish a genuinely novel prefix from a client sending wrong
contract keys (the common silent misconfiguration: mismatched `hash_scheme` or `tenant_id`
between the producer and the gateway). Treat them like an HTTP 4xx — **log, emit a metric,
fail open, and do not retry.** The mismatch is in your client configuration, not the server.
See [LookupRoute & ranking](/docs/concepts/lookuproute/#diagnostics-telling-a-novel-prefix-from-a-misconfiguration).

## The ranking knobs behind these codes

| Knob | Where | Default | Off |
|---|---|---|---|
| `minimumMatchedTokens` | CachePolicy | 64 | `0` |
| `routingFloorScore` | CachePolicy | `"0.1"` | `"0"` |
| `strategy.enableChainMatching` | CachePolicy | true | false |
| `strategy.requireChain` | CachePolicy | false | (n/a) |
| `strategy.enableTenantHot` | CachePolicy | true | false |
| `affinityRouting` | CachePolicy | `Enabled` | `Disabled` |
| `PressureWeight` | server RankerConfig | 1.0 | 0 |
| `SLOTightTTFTMs` | server RankerConfig | 200ms | 0 |
| `SLOTightBias` | server RankerConfig | 1.0 | 0 |
| `TenantHotMaxAge` | server RankerConfig | 5m | 0 |
| `TenantHotMinHitRate` | server RankerConfig | 0.1 | — |

## RenderTemplate

| Code | Status |
|---|---|
| `OK` | Emitted (the render path is a stub returning `OK` today). |
| `TEMPLATE_NOT_FOUND` | Specified; not yet emitted. |
| `RENDER_ERROR` | Specified; not yet emitted. |

## Ack

`Ack` carries no reason codes today (`accepted: true`, reason unset).

## Related pages

- [LookupRoute & ranking](/docs/concepts/lookuproute/) — how the codes are decided.
- [The gRPC contract](/docs/concepts/grpc-contract/) — why the codes are strings.
