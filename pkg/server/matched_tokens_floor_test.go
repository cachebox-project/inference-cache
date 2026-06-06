package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/cachebox-project/inference-cache/pkg/index"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// Server-side matched_tokens floor for LookupRoute. Trivial
// chat-template-only matches (1-block, ~16 tokens identical across every
// replica) used to surface as PREFIX_MATCH and inflated operator-visible
// routing metrics by ~3× without changing routing quality. The floor
// downgrades sub-floor matches to NO_HINT so the gateway round-robins
// honestly. These tests pin the four observable behaviors: the default
// applies when no CachePolicy is set; the policy value overrides; the
// explicit 0 opt-out disables; replicas with a real match aren't dragged
// down by sub-floor siblings.

// TestPolicyStoreMinimumMatchedTokensFallsBackToDefaultWhenNoPolicy pins the
// resolver's "no policy → server default" rule. An unconfigured tenant must
// see the safety floor; otherwise the bug the ticket describes (every match,
// however trivial, counted as a routing hit) would persist for every namespace
// that hasn't installed a CachePolicy — which is the common case.
func TestPolicyStoreMinimumMatchedTokensFallsBackToDefaultWhenNoPolicy(t *testing.T) {
	store := NewPolicyStore()
	if got := store.MinimumMatchedTokens("never-configured"); got != DefaultMinimumMatchedTokens {
		t.Fatalf("MinimumMatchedTokens(no-policy) = %d, want DefaultMinimumMatchedTokens (%d)", got, DefaultMinimumMatchedTokens)
	}
}

// TestPolicyStoreMinimumMatchedTokensRespectsPolicyValue pins that an explicit
// CachePolicy value wins as-is — including the explicit 0 opt-out that lets
// operators disable the floor on purpose (e.g. raw-recall benchmarking).
// Without this carve-out the server-wide default would clamp every namespace
// to >=64 and remove the disable-the-floor primitive.
func TestPolicyStoreMinimumMatchedTokensRespectsPolicyValue(t *testing.T) {
	store := NewPolicyStore()
	store.Replace([]ResolvedPolicy{
		{Namespace: "ns-strict", MinimumMatchedTokens: 256},
		{Namespace: "ns-disabled", MinimumMatchedTokens: 0},
	})
	if got := store.MinimumMatchedTokens("ns-strict"); got != 256 {
		t.Fatalf("strict ns floor = %d, want 256", got)
	}
	if got := store.MinimumMatchedTokens("ns-disabled"); got != 0 {
		t.Fatalf("disabled ns floor = %d, want 0 (explicit opt-out)", got)
	}
}

// TestPolicyStoreMinimumMatchedTokensClampsNegativeToZero defends against a
// hand-crafted /policy POST that carries a negative value (the CRD's
// Minimum=0 marker doesn't reach a controller-bypass caller). The resolver
// must never return a negative threshold — the service treats <=0 as "no
// floor", so a negative leaking through would silently disable enforcement
// instead of clamping to the safest interpretation.
func TestPolicyStoreMinimumMatchedTokensClampsNegativeToZero(t *testing.T) {
	store := NewPolicyStore()
	store.Replace([]ResolvedPolicy{{Namespace: "ns-bad", MinimumMatchedTokens: -1}})
	if got := store.MinimumMatchedTokens("ns-bad"); got != 0 {
		t.Fatalf("negative floor = %d, want 0 (clamped)", got)
	}
}

// TestLookupRouteAppliesDefaultMatchedTokensFloorWhenNoPolicy is the core
// behavioral assertion of the ticket: a trivial 1-block match (16 tokens —
// the chat-template framing every replica has identically) below the default
// floor must surface as NO_HINT, not PREFIX_MATCH, when the tenant has no
// CachePolicy at all. This is the path the cache-stress benchmark proxy log
// exercises in production for ~70% of its responses.
func TestLookupRouteAppliesDefaultMatchedTokensFloorWhenNoPolicy(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "no-policy-tenant", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("chat-template"), TokenCount: 16}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "no-policy-tenant", HashScheme: "vllm",
		PrefixHash: []byte("chat-template"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT — 16-token match below default floor (%d) should not surface as PREFIX_MATCH",
			resp.GetReasonCode(), DefaultMinimumMatchedTokens)
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("sub-floor match must downgrade to NO_HINT with empty scores, got %+v", resp.GetReplicaScores())
	}
}

// TestLookupRouteKeepsPrefixMatchAtDefaultFloorWhenNoPolicy pins the
// boundary: a match exactly at the floor passes, so the threshold is
// `matched_tokens >= floor` (not strict `>`). A strict-greater-than reading
// would fail every match that lands precisely at the documented 4-block
// minimum; this test catches that mis-read.
func TestLookupRouteKeepsPrefixMatchAtDefaultFloorWhenNoPolicy(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "no-policy-tenant", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: DefaultMinimumMatchedTokens}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "no-policy-tenant", HashScheme: "vllm",
		PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH — match exactly at the floor should pass (>=, not >)", resp.GetReasonCode())
	}
	if got := resp.GetReplicaScores()[0].GetMatchedTokens(); got != DefaultMinimumMatchedTokens {
		t.Fatalf("matched_tokens = %d, want %d (the boundary value)", got, DefaultMinimumMatchedTokens)
	}
}

// TestLookupRoutePolicyMatchedTokensFloorOverridesDefault verifies the
// per-namespace knob: a CachePolicy with MinimumMatchedTokens=256 must reject
// a 100-token match that would clear the server-wide default of 64. Without
// this the policy field has no observable effect — it would be a no-op
// surface, exactly the inert-field anti-pattern §5 forbids.
func TestLookupRoutePolicyMatchedTokensFloorOverridesDefault(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-strict", MinimumMatchedTokens: 256},
	})
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "team-strict", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 100}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-strict", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT — 100-token match below policy floor (256) should downgrade", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("sub-floor match must downgrade to NO_HINT with empty scores, got %+v", resp.GetReplicaScores())
	}
}

// TestLookupRoutePolicyMatchedTokensFloorZeroDisablesEnforcement verifies the
// explicit opt-out: an operator who sets MinimumMatchedTokens=0 wants every
// match reported. Without this exact 0-disables semantics there is no way to
// reproduce the pre-behavior for benchmarking / debugging the
// ranker's raw recall.
func TestLookupRoutePolicyMatchedTokensFloorZeroDisablesEnforcement(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "raw-recall", MinimumMatchedTokens: 0},
	})
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "raw-recall", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 1}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "raw-recall", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH — 0-floor must accept any match", resp.GetReasonCode())
	}
	if got := resp.GetReplicaScores()[0].GetMatchedTokens(); got != 1 {
		t.Fatalf("matched_tokens = %d, want 1 (full opt-out preserved)", got)
	}
}

// TestLookupRouteMatchedTokensFloorFiltersBelowFloorReplicasKeepsTheRest pins
// the per-replica filter: a real long-prefix match (one replica clears the
// floor) must still surface, even when a sibling replica reports only the
// trivial 1-block chain. The floor filters individual sub-floor scores but
// keeps the surviving ones — otherwise a noisy replica would poison routing
// for every well-warmed peer in the same response.
func TestLookupRouteMatchedTokensFloorFiltersBelowFloorReplicasKeepsTheRest(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "mixed-warmth", MinimumMatchedTokens: 64},
	})
	// Both replicas hold the same chain head; A holds the leading 4 blocks
	// (4 × 16 = 64 tokens, clears floor), B holds just block b1 (16 tokens,
	// fails the floor). After filtering the response must keep A and drop B.
	chainCounts := []int32{16, 16, 16, 16}
	chainHashes := [][]byte{[]byte("b1"), []byte("b2"), []byte("b3"), []byte("b4")}
	svc.index.Ingest(index.Update{
		ReplicaID: "long-prefix-A", Model: "m", Tenant: "mixed-warmth", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{BlockHashes: chainHashes, BlockTokenCounts: chainCounts}},
	})
	svc.index.Ingest(index.Update{
		ReplicaID: "trivial-only-B", Model: "m", Tenant: "mixed-warmth", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{BlockHashes: chainHashes[:1], BlockTokenCounts: chainCounts[:1]}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "mixed-warmth", HashScheme: "vllm",
		BlockHashes: chainHashes, BlockTokenCounts: chainCounts,
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH — at least one replica clears the floor", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 1 {
		t.Fatalf("expected exactly one surviving replica after the floor filter, got %d (%+v)",
			len(resp.GetReplicaScores()), resp.GetReplicaScores())
	}
	if got := resp.GetReplicaScores()[0].GetReplicaId(); got != "long-prefix-A" {
		t.Fatalf("survivor = %q, want long-prefix-A (trivial-only-B held just 16 tokens, below floor 64)", got)
	}
	if got := resp.GetReplicaScores()[0].GetMatchedTokens(); got != 64 {
		t.Fatalf("matched_tokens = %d, want 64 (A's full 4-block leading run)", got)
	}
}

// TestLookupRouteSubFloorMatchEmitsNoHintMetric pins the operator-facing
// observability fix the ticket calls out: trivial matches must increment
// reason_code=NO_HINT, NOT reason_code=PREFIX_MATCH, on
// inferencecache_lookup_route_calls_total. Without this assertion the metric
// pipeline could silently keep emitting the inflated PREFIX_MATCH series
// while the gateway sees the (correctly-downgraded) NO_HINT — a regression
// that splits the wire view from the dashboard view.
func TestLookupRouteSubFloorMatchEmitsNoHintMetric(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "no-policy-tenant", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("trivial"), TokenCount: 16}},
	})

	if _, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "no-policy-tenant", HashScheme: "vllm",
		PrefixHash: []byte("trivial"),
	}); err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}

	h := promhttp.HandlerFor(svc.metrics.registry, promhttp.HandlerOpts{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	wantSeries := `inferencecache_lookup_route_calls_total{hint_used="false",model="m",reason_code="NO_HINT"} 1`
	if !strings.Contains(body, wantSeries) {
		t.Errorf("metrics body missing downgraded NO_HINT counter: want substring %q\n----\n%s", wantSeries, body)
	}
	// Belt-and-braces: the inflated PREFIX_MATCH series for this model must
	// NOT be present in this body — a sub-floor lookup contributes nothing
	// to the PREFIX_MATCH counter.
	dontWant := `inferencecache_lookup_route_calls_total{hint_used="true",model="m",reason_code="PREFIX_MATCH"}`
	if strings.Contains(body, dontWant) {
		t.Errorf("metrics body unexpectedly carried a PREFIX_MATCH series for a sub-floor lookup: %q\n----\n%s", dontWant, body)
	}
}
