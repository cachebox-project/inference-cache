package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestTenantQuotaExemptsProbeTenant pins the server-internal defense against
// an operator-created CacheTenant claiming the reserved probe tenant id.
// CacheTenant.spec.tenantID is a free-form string, so without this exemption a
// malicious or careless operator could install
// CacheTenant{tenantID: "inferencecache.io/probe", maxIndexEntries: 0} and
// silently break Stage A of every CacheBackend functional probe (the ingest
// would be evicted before it lands). The probe tenant is server-internal state
// under a server-controlled tenant id; no operator-supplied CacheTenant should
// govern it.
func TestTenantQuotaExemptsProbeTenant(t *testing.T) {
	store := NewPolicyStore()
	store.ReplaceSnapshot(nil, []ResolvedTenant{
		{TenantID: ProbeTenantID, MaxIndexEntries: 0, IsolationMode: "Fairness"},
		// A normal tenant still gets its quota honored.
		{TenantID: "team-a", MaxIndexEntries: 1000},
	})
	if _, ok := store.TenantQuota(ProbeTenantID); ok {
		t.Fatal("TenantQuota(probe tenant) reported a quota; want exemption (fail open)")
	}
	if max, ok := store.TenantQuota("team-a"); !ok || max != 1000 {
		t.Fatalf("normal tenant quota was disturbed by probe exemption: got (%d, %v), want (1000, true)", max, ok)
	}
}

// TestPolicyPropagationVersionIsV5 pins the wire-format version. v2 accompanied
// the Tenants slice; v3 accompanied ResolvedPolicy.Eviction (per-namespace
// cap-eviction algorithm); v4 accompanied ResolvedPolicy.MinimumMatchedTokens
// (the result-side matched-tokens floor); v5 accompanied
// ResolvedPolicy.RoutingFloorScore (the per-namespace post-score floor for
// the distinguishing-power-aware LookupRoute ranker). A controller/server
// version mismatch outside the accepted band is rejected with a clear
// "unsupported version" rather than a decode error.
func TestPolicyPropagationVersionIsV5(t *testing.T) {
	if PolicyPropagationVersion != 5 {
		t.Fatalf("PolicyPropagationVersion = %d, want 5", PolicyPropagationVersion)
	}
	// PolicyMinimumAcceptedVersion bounds the lenience window for older bodies.
	// v3 and v4 must be accepted so a server-first rollout does not drop a
	// v3/v4 controller's policy state mid-upgrade (normalizePolicySnapshotForVersion
	// fills the missing fields with their server-side defaults); bodies below
	// v3 are still rejected — there is no documented path to normalize the
	// older Tenants / Eviction shapes.
	if PolicyMinimumAcceptedVersion != 3 {
		t.Fatalf("PolicyMinimumAcceptedVersion = %d, want 3", PolicyMinimumAcceptedVersion)
	}
}

// TestPolicySnapshotV3AcceptedWithFloorDefault pins the server-first rollout
// invariant: a v5 server MUST accept a v3 controller's snapshot AND normalize
// BOTH missing fields — minimumMatchedTokens to DefaultMinimumMatchedTokens
// and routingFloorScore to DefaultRoutingFloorScore — on each policy.
// Without those normalizations, the v3 body would decode the missing fields
// as their zero values (`int32(0)` / `nil` pointer) — the v4 / v5 explicit-
// opt-outs — silently disabling both floors for every namespace with a CR
// mid-rollout. The all-other-knobs assertion (TTL, eviction, prefix gate,
// timeout, tenant quota) protects against a regression where v3 itself
// stops being accepted, which would drop every policy field, not just the
// new ones.
func TestPolicySnapshotV3AcceptedWithFloorDefault(t *testing.T) {
	store := NewPolicyStore()
	srv := httptest.NewServer(NewPolicyHTTPHandler(store))
	defer srv.Close()

	// A v3 body: no minimumMatchedTokens key on the policy entry, version 3.
	// Two policies stress the normalization loop, and the rich set of other
	// fields proves the v3 path still preserves them as-is.
	v3Body := []byte(`{
        "version": 3,
        "policies": [
            {"namespace": "team-a", "evictionTTL": 900000000000, "minimumPrefixTokens": 32, "lookupTimeoutMs": 25, "eviction": "lfu"},
            {"namespace": "team-b", "evictionTTL": 3600000000000}
        ],
        "tenants": [
            {"tenantID": "team-a", "maxIndexEntries": 100000, "isolationMode": "Fairness"}
        ]
    }`)
	resp, err := srv.Client().Post(srv.URL, "application/json", bytes.NewReader(v3Body))
	if err != nil {
		t.Fatalf("post v3 body: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("v3 body status = %d, want 204 (server-first rollout MUST accept the controller's older version)", resp.StatusCode)
	}

	pA, ok := store.Lookup("team-a")
	if !ok {
		t.Fatal("team-a policy missing from store after v3 push")
	}
	// The v3-missing fields must be normalized to their server-side defaults —
	// otherwise a server-first rollout silently disables the floors for every
	// namespace that already had a CR.
	if pA.MinimumMatchedTokens != DefaultMinimumMatchedTokens {
		t.Fatalf("team-a MinimumMatchedTokens after v3 push = %d, want DefaultMinimumMatchedTokens (%d) — v3 → v4 matched-tokens normalization missing", pA.MinimumMatchedTokens, DefaultMinimumMatchedTokens)
	}
	if pA.RoutingFloorScore == nil {
		t.Fatalf("team-a RoutingFloorScore after v3 push is nil — v3 → v5 routing-floor normalization missing (must synthesize DefaultRoutingFloorScore)")
	}
	if *pA.RoutingFloorScore != DefaultRoutingFloorScore {
		t.Fatalf("team-a RoutingFloorScore after v3 push = %v, want DefaultRoutingFloorScore (%v) — v3 → v5 routing-floor normalization synthesized the wrong value", *pA.RoutingFloorScore, DefaultRoutingFloorScore)
	}
	// Every other knob the v3 body carried must reach the store unchanged.
	if pA.EvictionTTL != 900_000_000_000 || pA.MinimumPrefixTokens != 32 || pA.LookupTimeoutMs != 25 || pA.Eviction != "lfu" {
		t.Fatalf("team-a non-floor fields disturbed by normalization: %+v", pA)
	}

	pB, ok := store.Lookup("team-b")
	if !ok {
		t.Fatal("team-b policy missing from store after v3 push")
	}
	if pB.MinimumMatchedTokens != DefaultMinimumMatchedTokens {
		t.Fatalf("team-b MinimumMatchedTokens after v3 push = %d, want DefaultMinimumMatchedTokens (%d)", pB.MinimumMatchedTokens, DefaultMinimumMatchedTokens)
	}
	if pB.RoutingFloorScore == nil || *pB.RoutingFloorScore != DefaultRoutingFloorScore {
		t.Fatalf("team-b RoutingFloorScore after v3 push = %v, want &DefaultRoutingFloorScore (%v)", pB.RoutingFloorScore, DefaultRoutingFloorScore)
	}

	// Tenant quotas survive the version normalization unchanged.
	if q, ok := store.TenantQuota("team-a"); !ok || q != 100000 {
		t.Fatalf("team-a quota after v3 push = (%d, %v), want (100000, true)", q, ok)
	}
}

// TestPolicySnapshotV4ExplicitZeroPreservedAndRoutingFloorNormalized pins
// two complementary v4 rollout invariants:
//
//  1. An EXPLICIT minimumMatchedTokens=0 on a v4 body (the documented
//     opt-out, useful for raw-recall benchmarks) must NOT have its zero
//     rewritten to the default. The matched-tokens normalization only fires
//     for v3 (and below); v4 bodies reach the store byte-for-byte for that
//     field.
//  2. The missing routingFloorScore on the same v4 body MUST be normalized
//     to DefaultRoutingFloorScore. v4 predates the routing-floor field, so
//     a v5 server receiving a v4 body must synthesize the default the same
//     way it does for v3 — otherwise a v4 controller pushing to a v5 server
//     during a server-first rollout silently disables the new floor for
//     every namespace.
//
// Written against a literal v4 body (not PolicyPropagationVersion, which is
// now v5) so the v4-specific behavior under v3→v4→v5 server stays pinned
// even after the constant advances.
func TestPolicySnapshotV4ExplicitZeroPreservedAndRoutingFloorNormalized(t *testing.T) {
	store := NewPolicyStore()
	srv := httptest.NewServer(NewPolicyHTTPHandler(store))
	defer srv.Close()

	// Literal v4 body: minimumMatchedTokens explicitly 0, routingFloorScore
	// absent (v4 did not have the field).
	v4Body := []byte(`{
        "version": 4,
        "policies": [
            {"namespace": "raw-recall", "minimumMatchedTokens": 0}
        ]
    }`)
	resp, err := srv.Client().Post(srv.URL, "application/json", bytes.NewReader(v4Body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204 (v4 body must be accepted by v5 server)", resp.StatusCode)
	}

	if got := store.MinimumMatchedTokens("raw-recall"); got != 0 {
		t.Fatalf("explicit v4 opt-out got rewritten to %d, want 0 — v4 body must NOT be normalized on minimumMatchedTokens", got)
	}

	// The v4 body did not carry routingFloorScore. The v4 → v5 normalization
	// must synthesize the default; otherwise a server-first rollout silently
	// disables the routing floor for this namespace.
	p, ok := store.Lookup("raw-recall")
	if !ok {
		t.Fatal("raw-recall policy missing from store after v4 push")
	}
	if p.RoutingFloorScore == nil {
		t.Fatal("RoutingFloorScore after v4 push is nil — v4 → v5 routing-floor normalization missing")
	}
	if *p.RoutingFloorScore != DefaultRoutingFloorScore {
		t.Fatalf("RoutingFloorScore after v4 push = %v, want DefaultRoutingFloorScore (%v) — v4 → v5 routing-floor normalization synthesized the wrong value", *p.RoutingFloorScore, DefaultRoutingFloorScore)
	}
}

// TestPolicySnapshotV5ExplicitRoutingFloorZeroPreserved pins the v5-side
// invariant complementary to the v3/v4 normalization tests above: a v5
// controller that EXPLICITLY pushes routingFloorScore=0 (the documented
// opt-out, useful for raw-recall benchmarks) must NOT have its zero
// rewritten to the default. The normalization only fires for v3 / v4
// bodies; v5 bodies reach the store byte-for-byte for routingFloorScore.
func TestPolicySnapshotV5ExplicitRoutingFloorZeroPreserved(t *testing.T) {
	store := NewPolicyStore()
	srv := httptest.NewServer(NewPolicyHTTPHandler(store))
	defer srv.Close()

	zero := float32(0)
	body, err := json.Marshal(PolicySnapshot{
		Version: PolicyPropagationVersion,
		Policies: []ResolvedPolicy{
			{Namespace: "raw-recall", RoutingFloorScore: &zero},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := srv.Client().Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}

	if got := store.RoutingFloorScore("raw-recall"); got != 0 {
		t.Fatalf("explicit v5 opt-out got rewritten to %v, want 0 — v5 body must NOT be normalized", got)
	}
}

// TestPolicySnapshotVersionTooOldRejected pins the lower edge of the
// lenience band: a v2 body (or anything below PolicyMinimumAcceptedVersion)
// MUST still be rejected, so the band does not silently extend backward and
// admit a controller pushing under a schema this server no longer knows how
// to interpret.
func TestPolicySnapshotVersionTooOldRejected(t *testing.T) {
	store := NewPolicyStore()
	srv := httptest.NewServer(NewPolicyHTTPHandler(store))
	defer srv.Close()

	resp, err := srv.Client().Post(srv.URL, "application/json", bytes.NewReader([]byte(`{"version":2,"policies":[]}`)))
	if err != nil {
		t.Fatalf("post v2 body: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("v2 body status = %d, want 400 (below PolicyMinimumAcceptedVersion)", resp.StatusCode)
	}
}

func TestPolicySnapshotRoundTripCarriesPoliciesAndTenants(t *testing.T) {
	store := NewPolicyStore()
	srv := httptest.NewServer(NewPolicyHTTPHandler(store))
	defer srv.Close()

	snap := PolicySnapshot{
		Version:  PolicyPropagationVersion,
		Policies: []ResolvedPolicy{{Namespace: "team-a", MinimumPrefixTokens: 16}},
		Tenants: []ResolvedTenant{
			{TenantID: "team-a", MaxIndexEntries: 1000, IsolationMode: "Fairness"},
			{TenantID: "team-b", MaxIndexEntries: 0},
		},
	}
	body, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := srv.Client().Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}

	// Policies landed (existing axis).
	if _, ok := store.Lookup("team-a"); !ok {
		t.Fatal("policy team-a not stored")
	}
	// Tenant quotas landed and are keyed by tenantID.
	if max, ok := store.TenantQuota("team-a"); !ok || max != 1000 {
		t.Fatalf("TenantQuota(team-a) = (%d, %v), want (1000, true)", max, ok)
	}
	// A configured 0 budget is present and distinct from "no quota".
	if max, ok := store.TenantQuota("team-b"); !ok || max != 0 {
		t.Fatalf("TenantQuota(team-b) = (%d, %v), want (0, true)", max, ok)
	}
	// An unconfigured tenant fails open.
	if _, ok := store.TenantQuota("team-z"); ok {
		t.Fatal("TenantQuota(team-z) should report no quota (fail open)")
	}

	got := store.TenantQuotas()
	if len(got) != 2 || got[0].TenantID != "team-a" || got[1].TenantID != "team-b" {
		t.Fatalf("TenantQuotas() = %+v, want sorted [team-a team-b]", got)
	}
}

func TestReplaceSnapshotRevertsRemovedTenant(t *testing.T) {
	store := NewPolicyStore()
	store.ReplaceSnapshot(nil, []ResolvedTenant{{TenantID: "team-a", MaxIndexEntries: 5}})
	if _, ok := store.TenantQuota("team-a"); !ok {
		t.Fatal("team-a quota should be present after first push")
	}
	// Replace-on-write: a snapshot without team-a reverts it to no quota.
	store.ReplaceSnapshot(nil, nil)
	if _, ok := store.TenantQuota("team-a"); ok {
		t.Fatal("team-a quota should be gone after replace-on-write with empty snapshot")
	}
}

func TestReplaceSnapshotDropsEmptyTenantID(t *testing.T) {
	store := NewPolicyStore()
	store.ReplaceSnapshot(nil, []ResolvedTenant{{TenantID: "", MaxIndexEntries: 5}})
	if _, ok := store.TenantQuota(""); ok {
		t.Fatal("an empty tenant ID must not be stored (would shadow empty-tenant lookups)")
	}
}

// TestReplaceSnapshotClampsNegativeBudget pins the trust-boundary sanitization:
// the CRD enforces maxIndexEntries>=0, but a hand-crafted /policy POST could send
// a negative budget, which the index would read as "no enforcement" (eviction is
// skipped for a negative cap) — silently turning an attempted cap into unbounded.
// ReplaceSnapshot must clamp it to the design minimum of 0 (admit nothing).
func TestReplaceSnapshotClampsNegativeBudget(t *testing.T) {
	store := NewPolicyStore()
	store.ReplaceSnapshot(nil, []ResolvedTenant{{TenantID: "team-a", MaxIndexEntries: -1}})
	max, ok := store.TenantQuota("team-a")
	if !ok {
		t.Fatal("a negative budget must still register an (enforced) quota, not fail open")
	}
	if max != 0 {
		t.Fatalf("TenantQuota(team-a) = %d, want 0 (negative clamped to the minimum)", max)
	}
}
