// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cachebox-project/inference-cache/internal/controlplaneapi"
	"github.com/cachebox-project/inference-cache/internal/index"
)

func TestPolicyStoreRankerPreservesPresenceAndRejectsInvalidWireValues(t *testing.T) {
	zero := float32(0)
	ttft := int32(150)
	maxAge := 2 * time.Minute
	tooLarge := float32(index.MaxSLOTightBias + 1)
	negativeAge := -time.Second
	store := NewPolicyStore()
	store.Replace([]controlplaneapi.ResolvedPolicy{
		{Namespace: "configured", RankerOverrides: &controlplaneapi.ResolvedRankerOverrides{
			PressureWeight: &zero, SLOTightTTFTMs: &ttft, TenantHotMaxAge: &maxAge,
		}},
		{Namespace: "invalid", RankerOverrides: &controlplaneapi.ResolvedRankerOverrides{
			SLOTightBias: &tooLarge, TenantHotMaxAge: &negativeAge,
		}},
	})

	if _, ok := store.Ranker("missing"); ok {
		t.Fatal("missing policy must report no ranker override")
	}
	configured, ok := store.Ranker("configured")
	if !ok || configured.PressureWeight == nil || *configured.PressureWeight != 0 ||
		configured.SLOTightTTFTMs == nil || *configured.SLOTightTTFTMs != 150 ||
		configured.TenantHotMaxAge == nil || *configured.TenantHotMaxAge != 2*time.Minute {
		t.Fatalf("configured overrides = %+v, ok=%v", configured, ok)
	}
	invalid, ok := store.Ranker("invalid")
	if !ok {
		t.Fatal("configured ranker object must remain distinguishable from no policy")
	}
	if invalid.SLOTightBias != nil || invalid.TenantHotMaxAge != nil {
		t.Fatalf("invalid wire values were not dropped: %+v", invalid)
	}
}

func TestPolicySnapshotV7InheritsRankerBaseline(t *testing.T) {
	store := NewPolicyStore()
	srv := httptest.NewServer(NewPolicyHTTPHandler(store))
	defer srv.Close()
	resp, err := http.Post(srv.URL, "application/json", bytes.NewBufferString(`{"version":7,"policies":[{"namespace":"legacy"}]}`))
	if err != nil {
		t.Fatalf("post v7 policy: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("v7 policy status = %d, want 204", resp.StatusCode)
	}
	if _, ok := store.Ranker("legacy"); ok {
		t.Fatal("v7 policy without rankerOverrides must inherit the index baseline")
	}
}

func TestServiceWiresPolicyStoreAsRankerResolver(t *testing.T) {
	zero := float32(0)
	svc := New()
	svc.policies.Replace([]controlplaneapi.ResolvedPolicy{{
		Namespace: "pressure-off",
		RankerOverrides: &controlplaneapi.ResolvedRankerOverrides{
			PressureWeight: &zero,
		},
	}})
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "pressure-off", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 100}},
		Stats:    &index.ReplicaStats{Pressure: 1},
	})
	scores := svc.index.Lookup(index.LookupRequest{Tenant: "pressure-off", Model: "m", HashScheme: "vllm", PrefixHash: []byte("p")})
	if len(scores) != 1 || scores[0].Score != 100 {
		t.Fatalf("service ranker override = %+v, want score 100", scores)
	}
}
