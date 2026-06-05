package index

import "testing"

// TestHasAnyForTenantReportsPresence pins the contract-diagnostics introspection
// surface: HasAnyForTenant must be true iff the tenant has at least one prefix
// entry anywhere in the index (any model, any hash_scheme). Used by LookupRoute
// miss-path classification to distinguish a wrong tenant_id (the
// tenant-mismatch case) from a genuinely novel prefix.
func TestHasAnyForTenantReportsPresence(t *testing.T) {
	idx := New()

	if idx.HasAnyForTenant("ic-smoke") {
		t.Fatalf("empty index: HasAnyForTenant should be false")
	}

	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m", Tenant: "ic-smoke", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}},
	})

	if !idx.HasAnyForTenant("ic-smoke") {
		t.Fatalf("tenant with one prefix: HasAnyForTenant should be true")
	}
	if idx.HasAnyForTenant("default") {
		t.Fatalf("untouched tenant: HasAnyForTenant should be false (would surface UNKNOWN_TENANT)")
	}
}

// TestHasAnyForTenantModelReportsPresence pins the (tenant, model) introspection:
// true iff any prefix entry exists for (tenant, model) across any hash_scheme.
// Used to distinguish UNKNOWN_MODEL from UNKNOWN_HASH_SCHEME on the miss path.
func TestHasAnyForTenantModelReportsPresence(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m1", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}},
	})

	if !idx.HasAnyForTenantModel("t", "m1") {
		t.Fatalf("(t, m1) has entries: HasAnyForTenantModel should be true")
	}
	if idx.HasAnyForTenantModel("t", "m2") {
		t.Fatalf("(t, m2) has no entries: HasAnyForTenantModel should be false (would surface UNKNOWN_MODEL)")
	}
	if idx.HasAnyForTenantModel("other-tenant", "m1") {
		t.Fatalf("(other-tenant, m1): tenant has nothing — HasAnyForTenantModel must be false")
	}
}

// TestHasAnyForTenantModelSchemeReportsPresence pins the scheme-scoped
// introspection: true iff (tenant, model, hash_scheme) has at least one entry.
// Used to surface UNKNOWN_HASH_SCHEME — the scheme-mismatch case where a
// vLLM-version bump changes the scheme tag from "vllm" to "vllm-v1" and every
// lookup misses.
func TestHasAnyForTenantModelSchemeReportsPresence(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}},
	})

	if !idx.HasAnyForTenantModelScheme("t", "m", "vllm") {
		t.Fatalf("(t, m, vllm) populated: HasAnyForTenantModelScheme should be true")
	}
	if idx.HasAnyForTenantModelScheme("t", "m", "vllm-v1") {
		t.Fatalf("(t, m, vllm-v1) is the scheme-mismatch case: must be false")
	}
	if idx.HasAnyForTenantModelScheme("t", "m", "sglang") {
		t.Fatalf("untouched scheme must be false")
	}
}

// TestHasAnyTracksRemoval guards against a stale-counter bug: deleting the last
// replica of a prefix must drop the introspection back to false. removeReplicaLocked
// already maintains prefixesByTenant + servingByScope in lockstep with the
// prefix map, so this test exercises the dependency rather than re-implementing it.
func TestHasAnyTracksRemoval(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}},
	})

	// Evict via the public API (ALL_CLEARED for the only replica).
	idx.ApplyEvent(Event{
		Type: EventAllCleared, ReplicaID: "r1", Model: "m", Tenant: "t",
	})

	if idx.HasAnyForTenant("t") {
		t.Fatalf("after the last replica is cleared, HasAnyForTenant should be false")
	}
	if idx.HasAnyForTenantModel("t", "m") {
		t.Fatalf("after clear, HasAnyForTenantModel should be false")
	}
	if idx.HasAnyForTenantModelScheme("t", "m", "vllm") {
		t.Fatalf("after clear, HasAnyForTenantModelScheme should be false")
	}
}

// TestLookupRouteClassifiesUnknownTenant pins the smallest diagnostic emission:
// a wrong tenant_id surfaces as StrategyUnknownTenant, never StrategyNone (which
// would surface as NO_HINT and hide the misconfiguration).
func TestLookupRouteClassifiesUnknownTenant(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m", Tenant: "ic-smoke", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}},
	})

	got := idx.LookupRoute(LookupRequest{
		Model: "m", Tenant: "default", HashScheme: "vllm", PrefixHash: hash("p"),
	})
	if got.Strategy != StrategyUnknownTenant {
		t.Fatalf("wrong tenant_id should classify as StrategyUnknownTenant, got %v", got.Strategy)
	}
	if len(got.Scores) != 0 {
		t.Fatalf("UNKNOWN_TENANT response must carry no scores, got %d", len(got.Scores))
	}
}

// TestLookupRouteClassifiesUnknownModel pins the second-level diagnostic: tenant
// is known (has entries), but the requested model has none under it.
func TestLookupRouteClassifiesUnknownModel(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m1", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}},
	})

	got := idx.LookupRoute(LookupRequest{
		Model: "m2", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p"),
	})
	if got.Strategy != StrategyUnknownModel {
		t.Fatalf("known tenant + unknown model should classify as StrategyUnknownModel, got %v", got.Strategy)
	}
}

// TestLookupRouteClassifiesUnknownHashScheme pins the scheme-mismatch case
// directly: (tenant, model) populated; the lookup's hash_scheme has no entries
// under it.
func TestLookupRouteClassifiesUnknownHashScheme(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}},
	})

	got := idx.LookupRoute(LookupRequest{
		Model: "m", Tenant: "t", HashScheme: "vllm-v1", PrefixHash: hash("p"),
	})
	if got.Strategy != StrategyUnknownHashScheme {
		t.Fatalf("(t, m) populated under vllm; lookup under vllm-v1 should classify as StrategyUnknownHashScheme, got %v", got.Strategy)
	}
}

// TestLookupRouteWideningOrder pins the rule that the MOST SPECIFIC mismatched
// key wins. A request with both a wrong tenant AND a wrong model receives the
// outermost diagnostic (UNKNOWN_TENANT) — once the tenant is unknown the server
// cannot meaningfully say whether the model would have been right.
func TestLookupRouteWideningOrder(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m1", Tenant: "ic-smoke", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}},
	})

	// Both wrong → UNKNOWN_TENANT wins (the widest level).
	got := idx.LookupRoute(LookupRequest{
		Model: "m-other", Tenant: "other-tenant", HashScheme: "sglang", PrefixHash: hash("p"),
	})
	if got.Strategy != StrategyUnknownTenant {
		t.Fatalf("wider mismatched key (tenant) must win the diagnostic; got %v", got.Strategy)
	}

	// Tenant correct, model wrong, scheme also wrong → UNKNOWN_MODEL.
	got = idx.LookupRoute(LookupRequest{
		Model: "m-other", Tenant: "ic-smoke", HashScheme: "sglang", PrefixHash: hash("p"),
	})
	if got.Strategy != StrategyUnknownModel {
		t.Fatalf("with tenant correct, model wrong must dominate scheme wrong; got %v", got.Strategy)
	}
}

// TestLookupRoutePrefixMissOnPopulatedKeysStillNoHint pins the non-regression:
// when every contract key has entries but no replica holds THIS prefix, the
// result is still StrategyNone — the genuine novel-prefix case. (Empty index
// → fail-open NO_HINT also still holds; covered by other tests.)
//
// Pinned twice: once with TENANT_HOT disabled (so the prefix-miss path lands
// directly in the diagnostic classifier) and once via a different replica that
// CANNOT trigger TENANT_HOT (no stats reported → not "warm"). Both paths must
// land at StrategyNone, not at a diagnostic code.
func TestLookupRoutePrefixMissOnPopulatedKeysStillNoHint(t *testing.T) {
	t.Run("TENANT_HOT disabled — diagnostic path on prefix miss", func(t *testing.T) {
		idx := New(WithRanker(RankerConfig{})) // disables TENANT_HOT entirely
		idx.Ingest(Update{
			ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: hash("known"), TokenCount: 10}},
		})

		got := idx.LookupRoute(LookupRequest{
			Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("novel"),
		})
		if got.Strategy != StrategyNone {
			t.Fatalf("(t, m, vllm) populated, this prefix novel — must stay StrategyNone (NO_HINT), got %v", got.Strategy)
		}
	})

	t.Run("no stats — TENANT_HOT can't fire", func(t *testing.T) {
		idx := New()
		idx.Ingest(Update{
			ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: hash("known"), TokenCount: 10}},
			// no Stats → not "warm" for TENANT_HOT
		})

		got := idx.LookupRoute(LookupRequest{
			Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("novel"),
		})
		if got.Strategy != StrategyNone {
			t.Fatalf("populated keys, novel prefix, no warm replica → StrategyNone; got %v", got.Strategy)
		}
	})
}

// TestLookupRouteEmptyHashSchemeStaysNoHint pins the explicit carve-out in the
// design doc: an empty hash_scheme is a contract violation (caller failed to
// supply a required key), NOT a mismatch. It continues to surface as
// StrategyNone (NO_HINT), not as UNKNOWN_HASH_SCHEME — the UNKNOWN_* codes
// diagnose set-but-wrong keys.
func TestLookupRouteEmptyHashSchemeStaysNoHint(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}},
	})

	got := idx.LookupRoute(LookupRequest{
		Model: "m", Tenant: "t", HashScheme: "", PrefixHash: hash("p"),
	})
	if got.Strategy != StrategyNone {
		t.Fatalf("empty hash_scheme is a contract violation, not a mismatch; must stay StrategyNone (NO_HINT), got %v", got.Strategy)
	}
}

// TestLookupRouteEmptyTenantStaysNoHint extends the same carve-out to
// tenant_id: an unspecified tenant is a contract violation (the caller
// didn't supply a required key), not a mismatch. Without this rule, the
// untenanted bucket (key.tenant == "") becomes a load-bearing "tenant" and
// any caller that fails to set the field gets a misleading UNKNOWN_TENANT
// — which the docs reserve for callers that supplied a value that does
// not match anything held. Symmetric to the empty-hash_scheme rule.
func TestLookupRouteEmptyTenantStaysNoHint(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}},
	})

	got := idx.LookupRoute(LookupRequest{
		Model: "m", Tenant: "", HashScheme: "vllm", PrefixHash: hash("p"),
	})
	if got.Strategy != StrategyNone {
		t.Fatalf("empty tenant_id is a contract violation, not a mismatch; must stay StrategyNone (NO_HINT), got %v", got.Strategy)
	}
}

// TestLookupRouteEmptyModelStaysNoHint extends the carve-out to model_id —
// same shape as the tenant and hash_scheme rules. An unspecified model is a
// missing key, not a wrong-value mismatch, so it must NOT surface as
// UNKNOWN_MODEL.
func TestLookupRouteEmptyModelStaysNoHint(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}},
	})

	got := idx.LookupRoute(LookupRequest{
		Model: "", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p"),
	})
	if got.Strategy != StrategyNone {
		t.Fatalf("empty model_id is a contract violation, not a mismatch; must stay StrategyNone (NO_HINT), got %v", got.Strategy)
	}
}

// TestLookupRouteTenantHotStillFires guards against a layering bug where the
// new classifier could shadow TENANT_HOT. When TENANT_HOT's preconditions are
// met (warm stats + at least one prefix entry in the requested
// (tenant, model, hash_scheme)), the lookup must surface StrategyTenantHot,
// not a diagnostic code.
func TestLookupRouteTenantHotStillFires(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "warm", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.8},
	})

	got := idx.LookupRoute(LookupRequest{
		Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("novel"),
	})
	if got.Strategy != StrategyTenantHot {
		t.Fatalf("TENANT_HOT preconditions met — must surface as StrategyTenantHot, not a diagnostic; got %v", got.Strategy)
	}
}

// TestLookupRouteChainBearingMissDiagnoses pins the chain path: a chain-bearing
// request that misses (no replica holds the first block) and has a wrong
// hash_scheme should still classify as UNKNOWN_HASH_SCHEME. Chain misses
// never fall through to TENANT_HOT (see the index code's block-level
// matching comment), so the classifier runs directly.
func TestLookupRouteChainBearingMissDiagnoses(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{
			BlockHashes:      [][]byte{hash("b1"), hash("b2")},
			BlockTokenCounts: []int32{16, 16},
		}},
	})

	got := idx.LookupRoute(LookupRequest{
		Model: "m", Tenant: "t", HashScheme: "vllm-v1",
		BlockHashes:      [][]byte{hash("b1"), hash("b2")},
		BlockTokenCounts: []int32{16, 16},
	})
	if got.Strategy != StrategyUnknownHashScheme {
		t.Fatalf("chain miss with wrong hash_scheme must classify as StrategyUnknownHashScheme, got %v", got.Strategy)
	}
}
