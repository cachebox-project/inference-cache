// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"testing"
	"time"
)

type staticRankerResolver map[string]RankerOverrides

func (r staticRankerResolver) Ranker(tenant string) (RankerOverrides, bool) {
	overrides, ok := r[tenant]
	return overrides, ok
}

func TestRankerResolverAppliesPerTenantOverrides(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	zeroFloat := float32(0)
	zeroDuration := time.Duration(0)
	resolver := staticRankerResolver{
		"pressure-off": {PressureWeight: &zeroFloat},
		"tenant-hot-off": {
			TenantHotMaxAge: &zeroDuration,
		},
	}
	idx := New(
		withClock(clk.now),
		WithTTL(time.Hour),
		WithRanker(DefaultRankerConfig()),
		WithRankerResolver(resolver),
	)

	for _, tenant := range []string{"baseline", "pressure-off", "tenant-hot-off"} {
		idx.Ingest(Update{
			ReplicaID: "r-" + tenant, Model: "m", Tenant: tenant, HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: hash("known"), TokenCount: 100}},
			Stats:    &ReplicaStats{Pressure: 1, HitRate: 0.9},
		})
	}

	baseline := idx.Lookup(LookupRequest{Tenant: "baseline", Model: "m", HashScheme: "vllm", PrefixHash: hash("known")})
	if len(baseline) != 1 || baseline[0].Score != 0 {
		t.Fatalf("baseline pressure penalty = %+v, want one zero-score replica", baseline)
	}
	overridden := idx.Lookup(LookupRequest{Tenant: "pressure-off", Model: "m", HashScheme: "vllm", PrefixHash: hash("known")})
	if len(overridden) != 1 || overridden[0].Score != 100 {
		t.Fatalf("pressure-off override = %+v, want score 100", overridden)
	}

	hotBaseline := idx.LookupRoute(LookupRequest{Tenant: "baseline", Model: "m", HashScheme: "vllm", PrefixHash: hash("missing")})
	if hotBaseline.Strategy != StrategyTenantHot {
		t.Fatalf("baseline miss strategy = %v, want TENANT_HOT", hotBaseline.Strategy)
	}
	hotDisabled := idx.LookupRoute(LookupRequest{Tenant: "tenant-hot-off", Model: "m", HashScheme: "vllm", PrefixHash: hash("missing")})
	if hotDisabled.Strategy != StrategyNone {
		t.Fatalf("tenant-hot-off miss strategy = %v, want no hint", hotDisabled.Strategy)
	}
}

func TestRankerResolverPreservesBaselineAndExplicitAllZero(t *testing.T) {
	zeroFloat := float32(0)
	zeroInt := int32(0)
	zeroDuration := time.Duration(0)
	allZero := RankerOverrides{
		PressureWeight:      &zeroFloat,
		SLOTightTTFTMs:      &zeroInt,
		SLOTightBias:        &zeroFloat,
		TenantHotMinHitRate: &zeroFloat,
		TenantHotMaxAge:     &zeroDuration,
	}
	baseline := RankerConfig{
		PressureWeight: 2, SLOTightTTFTMs: 400, SLOTightBias: 3,
		TenantHotMinHitRate: 0.5, TenantHotMaxAge: time.Minute,
	}
	idx := New(WithRanker(baseline), WithRankerResolver(staticRankerResolver{"all-zero": allZero}))

	if got := idx.rankerFor("missing"); got != baseline {
		t.Fatalf("missing resolver entry = %+v, want baseline %+v", got, baseline)
	}
	if got := idx.rankerFor("all-zero"); got != (RankerConfig{}) {
		t.Fatalf("explicit all-zero override = %+v, want all zero", got)
	}
}
