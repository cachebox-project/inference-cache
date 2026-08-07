// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controlplaneapi

// ProbeTenantID is reserved for synthetic functional-probe state.
const ProbeTenantID = "inferencecache.io/probe"

// ProbeReplicaPrefix cannot collide with Kubernetes pod names because it uses
// underscores, which RFC 1123 pod names disallow.
const ProbeReplicaPrefix = "__probe-"

// ProbeTokenCount is the token count in the synthetic probe block.
const ProbeTokenCount = int32(16)

// BackendTypeLMCache is the CacheBackend type for which the server runs the
// optional tier-2 probe stage.
const BackendTypeLMCache = "LMCache"

// ProbeStageResult is one stage's outcome in a /probe response.
type ProbeStageResult string

const (
	ProbeStageOK      ProbeStageResult = "ok"
	ProbeStageFailed  ProbeStageResult = "failed"
	ProbeStageSkipped ProbeStageResult = "skipped"
)

const (
	ProbeStageIngest  = "ingest"
	ProbeStageRouting = "routing"
	ProbeStageT2      = "t2"
)

// ProbeRequest identifies the deterministic functional probe to run.
type ProbeRequest struct {
	Backend     string `json:"backend"`
	Model       string `json:"model"`
	HashScheme  string `json:"hashScheme"`
	BackendType string `json:"backendType,omitempty"`
}

// ProbeResult reports the outcome of each functional-probe stage.
type ProbeResult struct {
	Backend string            `json:"backend"`
	Ingest  ProbeStageResult  `json:"ingest"`
	Routing ProbeStageResult  `json:"routing"`
	T2      ProbeStageResult  `json:"t2"`
	Errors  []ProbeStageError `json:"errors,omitempty"`
}

// ProbeStageError carries an operator-facing error for one probe stage.
type ProbeStageError struct {
	Stage   string `json:"stage"`
	Message string `json:"message"`
}

// AllPassed reports whether every stage explicitly passed or was skipped.
func (r ProbeResult) AllPassed() bool {
	return stagePassed(r.Ingest) && stagePassed(r.Routing) && stagePassed(r.T2)
}

func stagePassed(stage ProbeStageResult) bool {
	return stage == ProbeStageOK || stage == ProbeStageSkipped
}
