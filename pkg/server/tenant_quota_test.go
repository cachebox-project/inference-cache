package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestPolicyPropagationVersionIsV2 pins the wire-format version. The bump from
// v1 → v2 accompanied the Tenants slice; a stale controller writing v1 is then
// rejected with a clear "unsupported version" rather than a decode error.
func TestPolicyPropagationVersionIsV2(t *testing.T) {
	if PolicyPropagationVersion != 2 {
		t.Fatalf("PolicyPropagationVersion = %d, want 2", PolicyPropagationVersion)
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
