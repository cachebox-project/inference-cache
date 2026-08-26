// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controlplaneapi

import (
	"encoding/json"
	"testing"
	"time"
)

func TestProbeResultAllPassed(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		result ProbeResult
		want   bool
	}{
		{
			name: "all explicit successes",
			result: ProbeResult{
				Ingest: ProbeStageOK, Routing: ProbeStageOK, T2: ProbeStageSkipped,
			},
			want: true,
		},
		{name: "zero value fails closed", result: ProbeResult{}, want: false},
		{
			name: "failed stage",
			result: ProbeResult{
				Ingest: ProbeStageOK, Routing: ProbeStageFailed, T2: ProbeStageSkipped,
			},
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.result.AllPassed(); got != tc.want {
				t.Fatalf("AllPassed() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestPolicySnapshotWireShape(t *testing.T) {
	t.Parallel()

	score := float32(0)
	pressureWeight := float32(2)
	sloTightTTFTMs := int32(150)
	sloTightBias := float32(0)
	tenantHotMinHitRate := float32(0.25)
	tenantHotMaxAge := 2 * time.Minute
	body, err := json.Marshal(PolicySnapshot{
		Version: PolicyPropagationVersion,
		Policies: []ResolvedPolicy{{
			Namespace:         "team-a",
			RoutingFloorScore: &score,
			RankerOverrides: &ResolvedRankerOverrides{
				PressureWeight:      &pressureWeight,
				SLOTightTTFTMs:      &sloTightTTFTMs,
				SLOTightBias:        &sloTightBias,
				TenantHotMinHitRate: &tenantHotMinHitRate,
				TenantHotMaxAge:     &tenantHotMaxAge,
			},
		}},
		Tenants: []ResolvedTenant{{TenantID: "tenant-a", MaxIndexEntries: 10}},
	})
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	want := `{"version":8,"policies":[{"namespace":"team-a","routingFloorScore":0,"rankerOverrides":{"pressureWeight":2,"sloTightTTFTMs":150,"sloTightBias":0,"tenantHotMinHitRate":0.25,"tenantHotMaxAge":120000000000}}],"tenants":[{"tenantID":"tenant-a","maxIndexEntries":10}]}`
	if string(body) != want {
		t.Fatalf("snapshot JSON = %s, want %s", body, want)
	}
}
