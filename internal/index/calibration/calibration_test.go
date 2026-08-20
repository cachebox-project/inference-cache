// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package calibration

import (
	"bytes"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCheckedInSyntheticTraceSelectsCandidateAndCurrentResult(t *testing.T) {
	traceFile, err := os.Open("testdata/c1_synthetic_trace.json")
	if err != nil {
		t.Fatalf("open trace: %v", err)
	}
	trace, err := Load(traceFile)
	if closeErr := traceFile.Close(); closeErr != nil {
		t.Fatalf("close trace: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	result := Calibrate(trace)
	if trace.Provenance.Kind != "synthetic" {
		t.Fatalf("provenance kind = %q, want synthetic", trace.Provenance.Kind)
	}
	want := Config{
		PressureWeight:        0.5,
		SLOTightTTFTMillis:    200,
		SLOTightBias:          1,
		TenantHotMinHitRate:   0.2,
		TenantHotMaxAgeMillis: 120_000,
	}
	if result.BestConfig != want {
		t.Fatalf("synthetic candidate = %+v, want %+v", result.BestConfig, want)
	}
	if result.BestMetrics.PrefixHitRatePct != 100 || result.BestMetrics.TenantHotHitRatePct != 100 {
		t.Fatalf("best metrics = %+v, want both fixture hit rates at 100%%", result.BestMetrics)
	}
	got, err := MarshalResult(result)
	if err != nil {
		t.Fatalf("MarshalResult: %v", err)
	}
	committed, err := os.ReadFile("testdata/c1_synthetic_result.json")
	if err != nil {
		t.Fatalf("read committed result: %v", err)
	}
	if !bytes.Equal(got, committed) {
		t.Fatal("c1_synthetic_result.json is stale; run make ranker-calibration")
	}
}

func TestCalibrateSeparatesKnobEffects(t *testing.T) {
	trace := Trace{
		SchemaVersion: SchemaVersion,
		Name:          "unit",
		Provenance:    Provenance{Kind: "synthetic", Source: "unit test"},
		TTLMillis:     100_000,
		Sweep: Sweep{
			PressureWeights:       []float64{0, 0.5, 1},
			SLOTightTTFTMillis:    []int32{100, 200},
			SLOTightBiases:        []float64{0, 1},
			TenantHotMinHitRates:  []float64{0.1, 0.2},
			TenantHotMaxAgeMillis: []int64{60_000, 120_000},
		},
		Observations: []Observation{
			{
				ID: "pressure", Kind: ObservationPrefix, AtMillis: 100_000,
				Tenant: "tenant-a", Model: "model-a", HashScheme: "vllm",
				PrefixHash: "p", TokenCount: 320,
				Replicas: []ReplicaObservation{
					{ID: "hot", PrefixReportedAtMillis: 100_000, StatsReportedAtMillis: 100_000, ReportedPrefix: true, MatchedTokens: 320, HitRate: 0.8, Pressure: 0.8},
					{ID: "cool", PrefixReportedAtMillis: 100_000, StatsReportedAtMillis: 100_000, ReportedPrefix: true, MatchedTokens: 256, HitRate: 0.4, Pressure: 0.1, ObservedHit: true},
					{ID: "decoy", PrefixReportedAtMillis: 100_000, StatsReportedAtMillis: 100_000, HitRate: 0.1},
				},
			},
			{
				ID: "tenant-hot", Kind: ObservationTenantHot, AtMillis: 100_000,
				Tenant: "tenant-a", Model: "model-a", HashScheme: "vllm",
				PrefixHash: "other", TokenCount: 320,
				Replicas: []ReplicaObservation{{
					ID: "hot", PrefixReportedAtMillis: 100_000, StatsReportedAtMillis: 100_000,
					HitRate: 1, ObservedHit: true,
				}},
			},
		},
	}
	if err := trace.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	result := Calibrate(trace)
	if result.BestConfig.PressureWeight != 0.5 {
		t.Fatalf("PressureWeight = %v, want 0.5", result.BestConfig.PressureWeight)
	}
	if result.BestMetrics.PrefixHitRatePct != 100 {
		t.Fatalf("PrefixHitRatePct = %v, want 100", result.BestMetrics.PrefixHitRatePct)
	}
	if got := len(result.Curves["pressure_weight"]); got != 3 {
		t.Fatalf("pressure curve points = %d, want 3", got)
	}
}

func TestLoadRejectsUnknownAndInvalidFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		json string
		want string
	}{
		{"unknown", `{"schema_version":1,"unknown":true}`, "unknown field"},
		{"version", `{"schema_version":2}`, "schema_version"},
		{"trailing", `{"schema_version":1} {}`, "trailing JSON"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(strings.NewReader(tc.json))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestTraceValidationRejectsInvalidFields(t *testing.T) {
	valid := func() Trace {
		return Trace{
			SchemaVersion: SchemaVersion,
			Name:          "unit",
			Provenance:    Provenance{Kind: "synthetic", Source: "unit test"},
			TTLMillis:     1,
			Sweep: Sweep{
				PressureWeights:       []float64{0.5},
				SLOTightTTFTMillis:    []int32{200},
				SLOTightBiases:        []float64{1},
				TenantHotMinHitRates:  []float64{0.2},
				TenantHotMaxAgeMillis: []int64{60_000},
			},
			Observations: []Observation{
				{
					ID: "prefix", Kind: ObservationPrefix, Tenant: "t", Model: "m",
					HashScheme: "vllm", PrefixHash: "p", TokenCount: 1,
					Replicas: []ReplicaObservation{{
						ID: "r", ReportedPrefix: true, MatchedTokens: 1,
					}},
				},
				{
					ID: "tenant-hot", Kind: ObservationTenantHot, Tenant: "t", Model: "m",
					HashScheme: "vllm", PrefixHash: "p", TokenCount: 1,
					Replicas: []ReplicaObservation{{ID: "r"}},
				},
			},
		}
	}

	for _, tc := range []struct {
		name   string
		mutate func(*Trace)
		want   string
	}{
		{"version", func(trace *Trace) { trace.SchemaVersion++ }, "schema_version"},
		{"name", func(trace *Trace) { trace.Name = "" }, "trace name"},
		{"provenance kind", func(trace *Trace) { trace.Provenance.Kind = "unknown" }, "captured or synthetic"},
		{"provenance source", func(trace *Trace) { trace.Provenance.Source = "" }, "provenance source"},
		{"ttl", func(trace *Trace) { trace.TTLMillis = 0 }, "ttl_ms"},
		{"ttl overflow", func(trace *Trace) { trace.TTLMillis = maxDurationMillis + 1 }, "maximum representable"},
		{"empty sweep", func(trace *Trace) { trace.Sweep.PressureWeights = nil }, "every sweep dimension"},
		{"empty observations", func(trace *Trace) { trace.Observations = nil }, "at least one observation"},
		{"missing prefix observation", func(trace *Trace) { trace.Observations = trace.Observations[1:] }, "one prefix and one tenant_hot"},
		{"missing tenant-hot observation", func(trace *Trace) { trace.Observations = trace.Observations[:1] }, "one prefix and one tenant_hot"},
		{"invalid observation", func(trace *Trace) { trace.Observations[0].Kind = "unknown" }, "observation 0"},
		{"duplicate observation", func(trace *Trace) { trace.Observations = append(trace.Observations, trace.Observations[0]) }, "duplicate id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := valid()
			tc.mutate(&trace)
			if err := trace.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestSweepValidationRejectsInvalidValues(t *testing.T) {
	valid := func() Sweep {
		return Sweep{
			PressureWeights:       []float64{0.5},
			SLOTightTTFTMillis:    []int32{200},
			SLOTightBiases:        []float64{1},
			TenantHotMinHitRates:  []float64{0.2},
			TenantHotMaxAgeMillis: []int64{60_000},
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Sweep)
		want   string
	}{
		{"pressure", func(sweep *Sweep) { sweep.PressureWeights[0] = -1 }, "non-negative"},
		{"pressure float32 overflow", func(sweep *Sweep) { sweep.PressureWeights[0] = math.MaxFloat64 }, "float32"},
		{"bias float32 overflow", func(sweep *Sweep) { sweep.SLOTightBiases[0] = math.MaxFloat64 }, "float32"},
		{"hit rate", func(sweep *Sweep) { sweep.TenantHotMinHitRates[0] = 2 }, "invalid rate"},
		{"ttft", func(sweep *Sweep) { sweep.SLOTightTTFTMillis[0] = 0 }, "non-positive"},
		{"max age", func(sweep *Sweep) { sweep.TenantHotMaxAgeMillis[0] = 0 }, "non-positive"},
		{"max age overflow", func(sweep *Sweep) { sweep.TenantHotMaxAgeMillis[0] = maxDurationMillis + 1 }, "maximum representable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sweep := valid()
			tc.mutate(&sweep)
			if err := sweep.validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestObservationValidationRejectsInvalidReplicas(t *testing.T) {
	valid := func() Observation {
		return Observation{
			ID: "o", Kind: ObservationPrefix, Tenant: "t", Model: "m",
			HashScheme: "vllm", PrefixHash: "p", TokenCount: 1,
			Replicas: []ReplicaObservation{{ID: "r", ReportedPrefix: true, MatchedTokens: 1}},
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Observation)
		want   string
	}{
		{"identity", func(observation *Observation) { observation.ID = "" }, "required"},
		{"kind", func(observation *Observation) { observation.Kind = "unknown" }, "prefix or tenant_hot"},
		{"tokens", func(observation *Observation) { observation.TokenCount = 0 }, "token_count"},
		{"negative ttft budget", func(observation *Observation) { observation.TTFTBudgetMillis = -1 }, "ttft_budget_ms"},
		{"replicas", func(observation *Observation) { observation.Replicas = nil }, "at least one replica"},
		{"replica id", func(observation *Observation) { observation.Replicas[0].ID = "" }, "id is required"},
		{"matched tokens", func(observation *Observation) { observation.Replicas[0].MatchedTokens = 0 }, "matched_tokens"},
		{"negative unused matched tokens", func(observation *Observation) {
			observation.Replicas[0].ReportedPrefix = false
			observation.Replicas[0].MatchedTokens = -1
		}, "matched_tokens must be non-negative"},
		{"rate", func(observation *Observation) { observation.Replicas[0].HitRate = 2 }, "finite values"},
		{"duplicate replica", func(observation *Observation) {
			observation.Replicas = append(observation.Replicas, observation.Replicas[0])
		}, "duplicate replica"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observation := valid()
			tc.mutate(&observation)
			if err := observation.validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestMarshalResultWrapsJSONErrors(t *testing.T) {
	_, err := MarshalResult(Result{BestConfig: Config{PressureWeight: math.NaN()}})
	if err == nil || !strings.Contains(err.Error(), "marshal calibration result") {
		t.Fatalf("MarshalResult error = %v, want wrapped JSON error", err)
	}
}

func TestReplayTenantHotMissWithoutCandidate(t *testing.T) {
	trace := Trace{TTLMillis: int64(time.Minute / time.Millisecond)}
	observation := Observation{
		ID: "tenant-hot", Kind: ObservationTenantHot, AtMillis: 100_000,
		Tenant: "t", Model: "m", HashScheme: "vllm", PrefixHash: "novel", TokenCount: 1,
		Replicas: []ReplicaObservation{{
			ID: "cold", PrefixReportedAtMillis: 100_000, StatsReportedAtMillis: 100_000,
			HitRate: 0.1, ObservedHit: true,
		}},
	}
	config := Config{TenantHotMinHitRate: 0.2, TenantHotMaxAgeMillis: 60_000}
	if replayObservation(trace, observation, config) {
		t.Fatal("replayObservation = hit, want miss when every tenant-hot candidate is below the floor")
	}
}

func TestReplayUsesObservationClockAtTenantHotBoundary(t *testing.T) {
	trace := Trace{TTLMillis: int64(time.Minute / time.Millisecond)}
	observation := Observation{
		ID: "tenant-hot-boundary", Kind: ObservationTenantHot, AtMillis: 100_000,
		Tenant: "t", Model: "m", HashScheme: "vllm", PrefixHash: "novel", TokenCount: 1,
		Replicas: []ReplicaObservation{{
			ID: "warm", PrefixReportedAtMillis: 40_001, StatsReportedAtMillis: 40_001,
			HitRate: 0.8, ObservedHit: true,
		}},
	}
	config := Config{TenantHotMinHitRate: 0.2, TenantHotMaxAgeMillis: 60_000}
	if !replayObservation(trace, observation, config) {
		t.Fatal("replayObservation = miss, want hit for stats one millisecond inside the replay window")
	}
}

func TestReplayUsesIndependentPrefixAndStatsTimestamps(t *testing.T) {
	trace := Trace{TTLMillis: int64(time.Minute / time.Millisecond)}
	observation := Observation{
		ID: "independent-clocks", Kind: ObservationPrefix, AtMillis: 100_000,
		Tenant: "t", Model: "m", HashScheme: "vllm", PrefixHash: "p", TokenCount: 320,
		Replicas: []ReplicaObservation{
			{
				ID: "deep-stale-stats", PrefixReportedAtMillis: 100_000, StatsReportedAtMillis: 0,
				ReportedPrefix: true, MatchedTokens: 320, Pressure: 1, ObservedHit: true,
			},
			{
				ID: "shallow-fresh-stats", PrefixReportedAtMillis: 100_000, StatsReportedAtMillis: 100_000,
				ReportedPrefix: true, MatchedTokens: 256,
			},
		},
	}
	config := Config{PressureWeight: 1, TenantHotMaxAgeMillis: 60_000}
	if !replayObservation(trace, observation, config) {
		t.Fatal("replayObservation = miss, want stale pressure ignored while fresh prefix remains routable")
	}
}

func TestReplayExcludesTTLExpiredServingEntries(t *testing.T) {
	trace := Trace{TTLMillis: int64(time.Minute / time.Millisecond)}
	observation := Observation{
		ID: "expired-serving", Kind: ObservationTenantHot, AtMillis: 100_000,
		Tenant: "t", Model: "m", HashScheme: "vllm", PrefixHash: "novel", TokenCount: 1,
		Replicas: []ReplicaObservation{{
			ID: "stale", PrefixReportedAtMillis: 40_000, StatsReportedAtMillis: 100_000,
			HitRate: 1, ObservedHit: true,
		}},
	}
	config := Config{TenantHotMinHitRate: 0.2, TenantHotMaxAgeMillis: 120_000}
	if replayObservation(trace, observation, config) {
		t.Fatal("replayObservation = hit, want TTL-expired serving entry evicted before lookup")
	}
}

func TestObservationRejectsFutureReplicaReport(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ReplicaObservation)
		want   string
	}{
		{"prefix", func(replica *ReplicaObservation) { replica.PrefixReportedAtMillis = 11 }, "prefix_reported_at_ms"},
		{"stats", func(replica *ReplicaObservation) { replica.StatsReportedAtMillis = 11 }, "stats_reported_at_ms"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observation := Observation{
				ID: "future", Kind: ObservationPrefix, AtMillis: 10,
				Tenant: "t", Model: "m", HashScheme: "vllm", PrefixHash: "p", TokenCount: 1,
				Replicas: []ReplicaObservation{{ID: "r", ReportedPrefix: true, MatchedTokens: 1}},
			}
			tc.mutate(&observation.Replicas[0])
			if err := observation.validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate error = %v, want substring %q", err, tc.want)
			}
		})
	}
}
