package controlplaneapi

import (
	"encoding/json"
	"testing"
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
	body, err := json.Marshal(PolicySnapshot{
		Version: PolicyPropagationVersion,
		Policies: []ResolvedPolicy{{
			Namespace:         "team-a",
			RoutingFloorScore: &score,
		}},
		Tenants: []ResolvedTenant{{TenantID: "tenant-a", MaxIndexEntries: 10}},
	})
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	want := `{"version":7,"policies":[{"namespace":"team-a","routingFloorScore":0}],"tenants":[{"tenantID":"tenant-a","maxIndexEntries":10}]}`
	if string(body) != want {
		t.Fatalf("snapshot JSON = %s, want %s", body, want)
	}
}
