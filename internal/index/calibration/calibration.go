// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

// Package calibration replays routing observations through the
// production index ranker and sweeps RankerConfig candidates.
package calibration

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/cachebox-project/inference-cache/internal/index"
)

const SchemaVersion = 1

const maxDurationMillis int64 = (1<<63 - 1) / int64(time.Millisecond)

const (
	ObservationPrefix    = "prefix"
	ObservationTenantHot = "tenant_hot"
)

type Provenance struct {
	Kind        string `json:"kind"`
	Source      string `json:"source"`
	Description string `json:"description"`
}

type Sweep struct {
	PressureWeights       []float64 `json:"pressure_weights"`
	SLOTightTTFTMillis    []int32   `json:"slo_tight_ttft_ms"`
	SLOTightBiases        []float64 `json:"slo_tight_biases"`
	TenantHotMinHitRates  []float64 `json:"tenant_hot_min_hit_rates"`
	TenantHotMaxAgeMillis []int64   `json:"tenant_hot_max_age_ms"`
}

type Trace struct {
	SchemaVersion int           `json:"schema_version"`
	Name          string        `json:"name"`
	Provenance    Provenance    `json:"provenance"`
	TTLMillis     int64         `json:"ttl_ms"`
	Sweep         Sweep         `json:"sweep"`
	Observations  []Observation `json:"observations"`
}

type Observation struct {
	ID               string               `json:"id"`
	Kind             string               `json:"kind"`
	AtMillis         int64                `json:"at_ms"`
	Tenant           string               `json:"tenant"`
	Model            string               `json:"model"`
	HashScheme       string               `json:"hash_scheme"`
	PrefixHash       string               `json:"prefix_hash"`
	TokenCount       int32                `json:"token_count"`
	TTFTBudgetMillis int32                `json:"ttft_budget_ms,omitempty"`
	Replicas         []ReplicaObservation `json:"replicas"`
}

type ReplicaObservation struct {
	ID                     string  `json:"id"`
	PrefixReportedAtMillis int64   `json:"prefix_reported_at_ms"`
	StatsReportedAtMillis  int64   `json:"stats_reported_at_ms"`
	ReportedPrefix         bool    `json:"reported_prefix"`
	MatchedTokens          int32   `json:"matched_tokens"`
	HitRate                float32 `json:"hit_rate"`
	Pressure               float32 `json:"pressure"`
	ObservedHit            bool    `json:"observed_hit"`
}

type Config struct {
	PressureWeight        float64 `json:"pressure_weight"`
	SLOTightTTFTMillis    int32   `json:"slo_tight_ttft_ms"`
	SLOTightBias          float64 `json:"slo_tight_bias"`
	TenantHotMinHitRate   float64 `json:"tenant_hot_min_hit_rate"`
	TenantHotMaxAgeMillis int64   `json:"tenant_hot_max_age_ms"`
}

type Metrics struct {
	PrefixRequests      int     `json:"prefix_requests"`
	PrefixHits          int     `json:"prefix_hits"`
	PrefixHitRatePct    float64 `json:"prefix_hit_rate_pct"`
	TenantHotRequests   int     `json:"tenant_hot_requests"`
	TenantHotHits       int     `json:"tenant_hot_hits"`
	TenantHotHitRatePct float64 `json:"tenant_hot_hit_rate_pct"`
	MacroHitRatePct     float64 `json:"macro_hit_rate_pct"`
}

type CurvePoint struct {
	Value   float64 `json:"value"`
	Metrics Metrics `json:"metrics"`
}

type Result struct {
	SchemaVersion int                     `json:"schema_version"`
	TraceName     string                  `json:"trace_name"`
	Provenance    Provenance              `json:"provenance"`
	Observations  int                     `json:"observations"`
	BestConfig    Config                  `json:"best_config"`
	BestMetrics   Metrics                 `json:"best_metrics"`
	Curves        map[string][]CurvePoint `json:"curves"`
}

func Load(r io.Reader) (Trace, error) {
	var trace Trace
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&trace); err != nil {
		return Trace{}, fmt.Errorf("decode calibration trace: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Trace{}, errors.New("decode calibration trace: trailing JSON value")
	}
	if err := trace.Validate(); err != nil {
		return Trace{}, err
	}
	return trace, nil
}

func (t Trace) Validate() error {
	if t.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schema_version = %d, want %d", t.SchemaVersion, SchemaVersion)
	}
	if t.Name == "" {
		return errors.New("trace name is required")
	}
	if t.Provenance.Kind != "captured" && t.Provenance.Kind != "synthetic" {
		return fmt.Errorf("provenance kind %q must be captured or synthetic", t.Provenance.Kind)
	}
	if t.Provenance.Source == "" {
		return errors.New("provenance source is required")
	}
	if t.TTLMillis <= 0 {
		return errors.New("ttl_ms must be positive")
	}
	if t.TTLMillis > maxDurationMillis {
		return fmt.Errorf("ttl_ms exceeds maximum representable duration (%d ms)", maxDurationMillis)
	}
	if err := t.Sweep.validate(); err != nil {
		return err
	}
	if len(t.Observations) == 0 {
		return errors.New("at least one observation is required")
	}
	seen := make(map[string]struct{}, len(t.Observations))
	seenKinds := make(map[string]bool, 2)
	for i, observation := range t.Observations {
		if err := observation.validate(); err != nil {
			return fmt.Errorf("observation %d: %w", i, err)
		}
		if _, ok := seen[observation.ID]; ok {
			return fmt.Errorf("observation %d: duplicate id %q", i, observation.ID)
		}
		seen[observation.ID] = struct{}{}
		seenKinds[observation.Kind] = true
	}
	if !seenKinds[ObservationPrefix] || !seenKinds[ObservationTenantHot] {
		return errors.New("trace must contain at least one prefix and one tenant_hot observation")
	}
	return nil
}

func (s Sweep) validate() error {
	if len(s.PressureWeights) == 0 || len(s.SLOTightTTFTMillis) == 0 ||
		len(s.SLOTightBiases) == 0 || len(s.TenantHotMinHitRates) == 0 ||
		len(s.TenantHotMaxAgeMillis) == 0 {
		return errors.New("every sweep dimension must contain at least one value")
	}
	for _, value := range append(append([]float64{}, s.PressureWeights...), s.SLOTightBiases...) {
		if !finiteNonNegative(value) {
			return fmt.Errorf("sweep contains invalid non-negative value %v", value)
		}
	}
	for _, value := range s.TenantHotMinHitRates {
		if !finiteRate(value) {
			return fmt.Errorf("tenant_hot_min_hit_rates contains invalid rate %v", value)
		}
	}
	for _, value := range s.SLOTightTTFTMillis {
		if value <= 0 {
			return fmt.Errorf("slo_tight_ttft_ms contains non-positive value %d", value)
		}
	}
	for _, value := range s.TenantHotMaxAgeMillis {
		if value <= 0 {
			return fmt.Errorf("tenant_hot_max_age_ms contains non-positive value %d", value)
		}
		if value > maxDurationMillis {
			return fmt.Errorf("tenant_hot_max_age_ms contains value exceeding maximum representable duration: %d", value)
		}
	}
	return nil
}

func (o Observation) validate() error {
	if o.ID == "" || o.Tenant == "" || o.Model == "" || o.HashScheme == "" || o.PrefixHash == "" {
		return errors.New("id, tenant, model, hash_scheme, and prefix_hash are required")
	}
	if o.Kind != ObservationPrefix && o.Kind != ObservationTenantHot {
		return fmt.Errorf("kind %q must be prefix or tenant_hot", o.Kind)
	}
	if o.TokenCount <= 0 {
		return errors.New("token_count must be positive")
	}
	if o.TTFTBudgetMillis < 0 {
		return errors.New("ttft_budget_ms must be non-negative")
	}
	if len(o.Replicas) == 0 {
		return errors.New("at least one replica is required")
	}
	seen := make(map[string]struct{}, len(o.Replicas))
	for i, replica := range o.Replicas {
		if replica.ID == "" {
			return fmt.Errorf("replica %d: id is required", i)
		}
		if replica.PrefixReportedAtMillis > o.AtMillis {
			return fmt.Errorf("replica %q: prefix_reported_at_ms is after observation", replica.ID)
		}
		if replica.StatsReportedAtMillis > o.AtMillis {
			return fmt.Errorf("replica %q: stats_reported_at_ms is after observation", replica.ID)
		}
		if replica.MatchedTokens < 0 {
			return fmt.Errorf("replica %q: matched_tokens must be non-negative", replica.ID)
		}
		if replica.ReportedPrefix && replica.MatchedTokens == 0 {
			return fmt.Errorf("replica %q: matched_tokens must be positive when reported_prefix is true", replica.ID)
		}
		if !finiteRate(float64(replica.HitRate)) || !finiteRate(float64(replica.Pressure)) {
			return fmt.Errorf("replica %q: hit_rate and pressure must be finite values in [0,1]", replica.ID)
		}
		if _, ok := seen[replica.ID]; ok {
			return fmt.Errorf("duplicate replica id %q", replica.ID)
		}
		seen[replica.ID] = struct{}{}
	}
	return nil
}

func finiteNonNegative(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

func finiteRate(value float64) bool {
	return finiteNonNegative(value) && value <= 1
}

func Calibrate(trace Trace) Result {
	bestConfig, bestMetrics := bestGridPoint(trace)
	return Result{
		SchemaVersion: SchemaVersion,
		TraceName:     trace.Name,
		Provenance:    trace.Provenance,
		Observations:  len(trace.Observations),
		BestConfig:    bestConfig,
		BestMetrics:   bestMetrics,
		Curves: map[string][]CurvePoint{
			"pressure_weight":         pressureCurve(trace, bestConfig),
			"slo_tight_ttft_ms":       sloThresholdCurve(trace, bestConfig),
			"slo_tight_bias":          sloBiasCurve(trace, bestConfig),
			"tenant_hot_min_hit_rate": tenantHotRateCurve(trace, bestConfig),
			"tenant_hot_max_age_ms":   tenantHotAgeCurve(trace, bestConfig),
		},
	}
}

func bestGridPoint(trace Trace) (Config, Metrics) {
	var best Config
	var bestMetrics Metrics
	first := true
	for _, pressureWeight := range trace.Sweep.PressureWeights {
		for _, ttft := range trace.Sweep.SLOTightTTFTMillis {
			for _, bias := range trace.Sweep.SLOTightBiases {
				for _, hitRate := range trace.Sweep.TenantHotMinHitRates {
					for _, maxAge := range trace.Sweep.TenantHotMaxAgeMillis {
						candidate := Config{
							PressureWeight:        pressureWeight,
							SLOTightTTFTMillis:    ttft,
							SLOTightBias:          bias,
							TenantHotMinHitRate:   hitRate,
							TenantHotMaxAgeMillis: maxAge,
						}
						metrics := Replay(trace, candidate)
						if first || better(metrics, candidate, bestMetrics, best) {
							best, bestMetrics, first = candidate, metrics, false
						}
					}
				}
			}
		}
	}
	return best, bestMetrics
}

func better(candidateMetrics Metrics, candidate Config, bestMetrics Metrics, best Config) bool {
	if candidateMetrics.MacroHitRatePct != bestMetrics.MacroHitRatePct {
		return candidateMetrics.MacroHitRatePct > bestMetrics.MacroHitRatePct
	}
	if candidateMetrics.PrefixHitRatePct != bestMetrics.PrefixHitRatePct {
		return candidateMetrics.PrefixHitRatePct > bestMetrics.PrefixHitRatePct
	}
	if candidateMetrics.TenantHotHitRatePct != bestMetrics.TenantHotHitRatePct {
		return candidateMetrics.TenantHotHitRatePct > bestMetrics.TenantHotHitRatePct
	}
	// Conservative deterministic tie-break: prefer the least invasive score
	// multipliers and shortest fallback window among equally accurate points.
	if candidate.PressureWeight != best.PressureWeight {
		return candidate.PressureWeight < best.PressureWeight
	}
	if candidate.SLOTightBias != best.SLOTightBias {
		return candidate.SLOTightBias < best.SLOTightBias
	}
	if candidate.SLOTightTTFTMillis != best.SLOTightTTFTMillis {
		return candidate.SLOTightTTFTMillis < best.SLOTightTTFTMillis
	}
	if candidate.TenantHotMaxAgeMillis != best.TenantHotMaxAgeMillis {
		return candidate.TenantHotMaxAgeMillis < best.TenantHotMaxAgeMillis
	}
	return candidate.TenantHotMinHitRate > best.TenantHotMinHitRate
}

func Replay(trace Trace, config Config) Metrics {
	metrics := Metrics{}
	for _, observation := range trace.Observations {
		hit := replayObservation(trace, observation, config)
		switch observation.Kind {
		case ObservationPrefix:
			metrics.PrefixRequests++
			if hit {
				metrics.PrefixHits++
			}
		case ObservationTenantHot:
			metrics.TenantHotRequests++
			if hit {
				metrics.TenantHotHits++
			}
		}
	}
	metrics.PrefixHitRatePct = percentage(metrics.PrefixHits, metrics.PrefixRequests)
	metrics.TenantHotHitRatePct = percentage(metrics.TenantHotHits, metrics.TenantHotRequests)
	samples := 0
	if metrics.PrefixRequests > 0 {
		metrics.MacroHitRatePct += metrics.PrefixHitRatePct
		samples++
	}
	if metrics.TenantHotRequests > 0 {
		metrics.MacroHitRatePct += metrics.TenantHotHitRatePct
		samples++
	}
	if samples > 0 {
		metrics.MacroHitRatePct /= float64(samples)
	}
	return metrics
}

func replayObservation(trace Trace, observation Observation, config Config) bool {
	anchor := time.UnixMilli(observation.AtMillis)
	ranker := index.RankerConfig{
		PressureWeight:      float32(config.PressureWeight),
		SLOTightTTFTMs:      config.SLOTightTTFTMillis,
		SLOTightBias:        float32(config.SLOTightBias),
		TenantHotMinHitRate: float32(config.TenantHotMinHitRate),
		TenantHotMaxAge:     time.Duration(config.TenantHotMaxAgeMillis) * time.Millisecond,
	}
	idx := index.New(
		index.WithTTL(time.Duration(trace.TTLMillis)*time.Millisecond),
		index.WithRanker(ranker),
		index.WithClock(func() time.Time { return anchor }),
	)
	observedHits := make(map[string]bool, len(observation.Replicas))
	for _, replica := range observation.Replicas {
		hash := observation.PrefixHash
		tokens := replica.MatchedTokens
		if !replica.ReportedPrefix {
			hash = "serving/" + observation.ID + "/" + replica.ID
			tokens = 1
		}
		idx.Ingest(index.Update{
			ReplicaID:  replica.ID,
			Model:      observation.Model,
			Tenant:     observation.Tenant,
			HashScheme: observation.HashScheme,
			Timestamp:  time.UnixMilli(replica.PrefixReportedAtMillis),
			Prefixes: []index.PrefixRef{{
				PrefixHash: []byte(hash),
				TokenCount: tokens,
			}},
		})
		idx.Ingest(index.Update{
			ReplicaID:  replica.ID,
			Model:      observation.Model,
			Tenant:     observation.Tenant,
			HashScheme: observation.HashScheme,
			Timestamp:  time.UnixMilli(replica.StatsReportedAtMillis),
			Stats: &index.ReplicaStats{
				HitRate:  replica.HitRate,
				Pressure: replica.Pressure,
			},
		})
		observedHits[replica.ID] = replica.ObservedHit
	}
	result := idx.LookupRoute(index.LookupRequest{
		Model:        observation.Model,
		Tenant:       observation.Tenant,
		HashScheme:   observation.HashScheme,
		PrefixHash:   []byte(observation.PrefixHash),
		TokenCount:   observation.TokenCount,
		TTFTBudgetMs: observation.TTFTBudgetMillis,
	})
	wantStrategy := index.StrategyPrefixMatch
	if observation.Kind == ObservationTenantHot {
		wantStrategy = index.StrategyTenantHot
	}
	return result.Strategy == wantStrategy && len(result.Scores) > 0 && observedHits[result.Scores[0].ReplicaID]
}

func percentage(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) * 100 / float64(denominator)
}

func pressureCurve(trace Trace, best Config) []CurvePoint {
	return floatCurve(trace.Sweep.PressureWeights, func(value float64) Metrics {
		candidate := best
		candidate.PressureWeight = value
		return Replay(trace, candidate)
	})
}

func sloBiasCurve(trace Trace, best Config) []CurvePoint {
	return floatCurve(trace.Sweep.SLOTightBiases, func(value float64) Metrics {
		candidate := best
		candidate.SLOTightBias = value
		return Replay(trace, candidate)
	})
}

func tenantHotRateCurve(trace Trace, best Config) []CurvePoint {
	return floatCurve(trace.Sweep.TenantHotMinHitRates, func(value float64) Metrics {
		candidate := best
		candidate.TenantHotMinHitRate = value
		return Replay(trace, candidate)
	})
}

func floatCurve(values []float64, replay func(float64) Metrics) []CurvePoint {
	values = append([]float64(nil), values...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	points := make([]CurvePoint, 0, len(values))
	for _, value := range values {
		points = append(points, CurvePoint{Value: float64(value), Metrics: replay(value)})
	}
	return points
}

func sloThresholdCurve(trace Trace, best Config) []CurvePoint {
	values := append([]int32(nil), trace.Sweep.SLOTightTTFTMillis...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	points := make([]CurvePoint, 0, len(values))
	for _, value := range values {
		candidate := best
		candidate.SLOTightTTFTMillis = value
		points = append(points, CurvePoint{Value: float64(value), Metrics: Replay(trace, candidate)})
	}
	return points
}

func tenantHotAgeCurve(trace Trace, best Config) []CurvePoint {
	values := append([]int64(nil), trace.Sweep.TenantHotMaxAgeMillis...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	points := make([]CurvePoint, 0, len(values))
	for _, value := range values {
		candidate := best
		candidate.TenantHotMaxAgeMillis = value
		points = append(points, CurvePoint{Value: float64(value), Metrics: Replay(trace, candidate)})
	}
	return points
}

func MarshalResult(result Result) ([]byte, error) {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal calibration result: %w", err)
	}
	return append(data, '\n'), nil
}
