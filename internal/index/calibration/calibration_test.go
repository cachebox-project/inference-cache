// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package calibration

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cachebox-project/inference-cache/internal/index"
)

func TestCheckedInTraceSelectsDefaultConfigAndCurrentResult(t *testing.T) {
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
	defaults := index.DefaultRankerConfig()
	best := result.BestConfig
	if float32(best.PressureWeight) != defaults.PressureWeight ||
		best.SLOTightTTFTMillis != defaults.SLOTightTTFTMs ||
		float32(best.SLOTightBias) != defaults.SLOTightBias ||
		float32(best.TenantHotMinHitRate) != defaults.TenantHotMinHitRate ||
		time.Duration(best.TenantHotMaxAgeMillis)*time.Millisecond != defaults.TenantHotMaxAge {
		t.Fatalf("calibrated config = %+v, DefaultRankerConfig = %+v", best, defaults)
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
					{ID: "hot", ReportedAtMillis: 100_000, ReportedPrefix: true, MatchedTokens: 320, HitRate: 0.8, Pressure: 0.8},
					{ID: "cool", ReportedAtMillis: 100_000, ReportedPrefix: true, MatchedTokens: 256, HitRate: 0.4, Pressure: 0.1, ObservedHit: true},
					{ID: "decoy", ReportedAtMillis: 100_000, HitRate: 0.1},
				},
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

func TestReplayTenantHotMissWithoutCandidate(t *testing.T) {
	trace := Trace{TTLMillis: int64(time.Minute / time.Millisecond)}
	observation := Observation{
		ID: "tenant-hot", Kind: ObservationTenantHot, AtMillis: 100_000,
		Tenant: "t", Model: "m", HashScheme: "vllm", PrefixHash: "novel", TokenCount: 1,
		Replicas: []ReplicaObservation{{
			ID: "cold", ReportedAtMillis: 100_000, HitRate: 0.1, ObservedHit: true,
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
			ID: "warm", ReportedAtMillis: 40_001, HitRate: 0.8, ObservedHit: true,
		}},
	}
	config := Config{TenantHotMinHitRate: 0.2, TenantHotMaxAgeMillis: 60_000}
	if !replayObservation(trace, observation, config) {
		t.Fatal("replayObservation = miss, want hit for stats one millisecond inside the replay window")
	}
}

func TestObservationRejectsFutureReplicaReport(t *testing.T) {
	observation := Observation{
		ID: "future", Kind: ObservationPrefix, AtMillis: 10,
		Tenant: "t", Model: "m", HashScheme: "vllm", PrefixHash: "p", TokenCount: 1,
		Replicas: []ReplicaObservation{{
			ID: "r", ReportedAtMillis: 11, ReportedPrefix: true, MatchedTokens: 1,
		}},
	}
	if err := observation.validate(); err == nil || !strings.Contains(err.Error(), "after observation") {
		t.Fatalf("validate error = %v, want future-report rejection", err)
	}
}
