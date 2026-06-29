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

// TestLookupRouteEmitsUnknownHashScheme pins the scheme-mismatch wire shape:
// ingest under hash_scheme="vllm"; lookup under hash_scheme="vllm-v1" →
// reason_code UNKNOWN_HASH_SCHEME (not NO_HINT). The string surfaces directly
// on the gRPC envelope the gateway reads — so a misconfigured SDK can debug
// the mismatch without out-of-band inspection.
func TestLookupRouteEmitsUnknownHashScheme(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 10}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "t", HashScheme: "vllm-v1", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "UNKNOWN_HASH_SCHEME" {
		t.Fatalf("reason = %q, want UNKNOWN_HASH_SCHEME (scheme-mismatch case)", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("UNKNOWN_HASH_SCHEME must carry no scores, got %d", len(resp.GetReplicaScores()))
	}
}

// TestLookupRouteEmitsUnknownTenant pins the tenant-mismatch wire shape:
// ingest under tenant_id="ic-smoke" (kvevent-subscriber's
// --tenant-id=$(POD_NAMESPACE) convention); lookup under tenant_id="default"
// (a naive gateway-SDK default) → reason_code UNKNOWN_TENANT.
func TestLookupRouteEmitsUnknownTenant(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "r1", Model: "m", Tenant: "ic-smoke", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 10}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "default", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "UNKNOWN_TENANT" {
		t.Fatalf("reason = %q, want UNKNOWN_TENANT (tenant-mismatch case)", resp.GetReasonCode())
	}
}

// TestLookupRouteEmitsUnknownModel pins the model-mismatch surface: the tenant
// has prefix entries under model "m1"; a request for model "m2" within that
// same tenant lands on UNKNOWN_MODEL, not UNKNOWN_TENANT.
func TestLookupRouteEmitsUnknownModel(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "r1", Model: "m1", Tenant: "t", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 10}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m2", TenantId: "t", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "UNKNOWN_MODEL" {
		t.Fatalf("reason = %q, want UNKNOWN_MODEL", resp.GetReasonCode())
	}
}

// TestLookupRouteGenuineMissStillNoHint guards the non-regression at the handler
// level: when every contract key is populated and a real replica's prefix is
// asked for, PREFIX_MATCH still fires; when every contract key is populated
// but THIS prefix is novel (and no warm replica exists), NO_HINT is still the
// answer — the diagnostic codes are STRICTLY for key-level mismatches.
func TestLookupRouteGenuineMissStillNoHint(t *testing.T) {
	t.Run("real PREFIX_MATCH unchanged", func(t *testing.T) {
		svc := newTestService()
		// TokenCount=128 clears the DefaultMinimumMatchedTokens floor
		// — the subtest pins "matching keys + a real prefix
		// stays PREFIX_MATCH", separate from the floor.
		svc.index.Ingest(index.Update{
			ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 128}},
		})
		resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
			ModelId: "m", TenantId: "t", HashScheme: "vllm", PrefixHash: []byte("p"),
		})
		if err != nil {
			t.Fatalf("LookupRoute: %v", err)
		}
		if resp.GetReasonCode() != "PREFIX_MATCH" {
			t.Fatalf("matching keys + real prefix must stay PREFIX_MATCH, got %q", resp.GetReasonCode())
		}
	})

	t.Run("matching keys + novel prefix stays NO_HINT", func(t *testing.T) {
		svc := newTestService()
		// Disable affinity so the StrategyNone downgrade stays NO_HINT
		// (the historical shape this subtest pins). The affinity
		// behavior on the same scenario is covered in
		// affinity_routing_test.go.
		fal := false
		svc.policies.Replace([]ResolvedPolicy{{Namespace: "t", AffinityRouting: &fal}})
		// Use Stats: nil so the warm-replica TENANT_HOT path can't fire — we
		// want to isolate the "real miss" branch end-to-end.
		svc.index.Ingest(index.Update{
			ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("known"), TokenCount: 10}},
		})
		resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
			ModelId: "m", TenantId: "t", HashScheme: "vllm", PrefixHash: []byte("novel"),
		})
		if err != nil {
			t.Fatalf("LookupRoute: %v", err)
		}
		if resp.GetReasonCode() != "NO_HINT" {
			t.Fatalf("populated keys + novel prefix must stay NO_HINT, got %q", resp.GetReasonCode())
		}
	})
}

// TestLookupRouteEmptyHashSchemeStaysNoHint pins the carve-out at the handler:
// an empty hash_scheme is a contract violation (not a mismatch) and continues
// to surface as NO_HINT. The UNKNOWN_* codes diagnose set-but-wrong keys only.
//
// This complements TestLookupRouteEmptyHashSchemeFailsOpenOverGRPC, which
// guards that the empty-scheme path also blocks TENANT_HOT leakage.
func TestLookupRouteEmptyHashSchemeStaysNoHint(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 10}},
	})
	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "t", HashScheme: "", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("empty hash_scheme is a contract violation, not a key mismatch; must stay NO_HINT, got %q",
			resp.GetReasonCode())
	}
}

// TestLookupRouteDiagnosticsMetricLabels guards that the new reason_code values
// flow through to inferencecache_lookup_route_calls_total as label values, with
// hint_used=false (no scores). The counter is a `string`-keyed label, so the
// new values appear automatically — this test confirms the wiring rather than
// asserting on a new metric.
func TestLookupRouteDiagnosticsMetricLabels(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "r1", Model: "m", Tenant: "ic-smoke", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 10}},
	})

	// One call per new diagnostic so each new label value gets a series.
	cases := []struct {
		name     string
		req      *icpb.LookupRouteRequest
		wantCode string
	}{
		{
			name:     "UNKNOWN_TENANT",
			req:      &icpb.LookupRouteRequest{ModelId: "m", TenantId: "default", HashScheme: "vllm", PrefixHash: []byte("p")},
			wantCode: "UNKNOWN_TENANT",
		},
		{
			name:     "UNKNOWN_MODEL",
			req:      &icpb.LookupRouteRequest{ModelId: "other", TenantId: "ic-smoke", HashScheme: "vllm", PrefixHash: []byte("p")},
			wantCode: "UNKNOWN_MODEL",
		},
		{
			name:     "UNKNOWN_HASH_SCHEME",
			req:      &icpb.LookupRouteRequest{ModelId: "m", TenantId: "ic-smoke", HashScheme: "vllm-v1", PrefixHash: []byte("p")},
			wantCode: "UNKNOWN_HASH_SCHEME",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := svc.LookupRoute(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("LookupRoute: %v", err)
			}
			if resp.GetReasonCode() != tc.wantCode {
				t.Fatalf("reason = %q, want %q", resp.GetReasonCode(), tc.wantCode)
			}
		})
	}

	// Scrape the metrics handler and assert each new reason_code label value
	// shows up at least once under inferencecache_lookup_route_calls_total
	// with hint_used="false" (no scores on any diagnostic path). The model
	// label takes the value the caller queried with — UNKNOWN_MODEL's request
	// uses "other", so its series label reflects that.
	h := promhttp.HandlerFor(svc.metrics.registry, promhttp.HandlerOpts{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`inferencecache_lookup_route_calls_total{hint_used="false",model="m",reason_code="UNKNOWN_TENANT"}`,
		`inferencecache_lookup_route_calls_total{hint_used="false",model="other",reason_code="UNKNOWN_MODEL"}`,
		`inferencecache_lookup_route_calls_total{hint_used="false",model="m",reason_code="UNKNOWN_HASH_SCHEME"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics body missing series: %s\n----\n%s", want, body)
		}
	}
}
