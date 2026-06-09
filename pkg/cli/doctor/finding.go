// Package doctor defines the diagnostic result vocabulary shared by the
// `inferencecache doctor` pre-flight checks and their output formatters.
//
// The package is deliberately a dependency-free leaf: it knows nothing about
// Kubernetes, gRPC, or HTTP. The check implementations (./checks) produce
// [Finding] values and the formatters (./output) render a [Report]; both
// import this package, never the reverse. That keeps the result vocabulary —
// the stable Codes operators grep for and the OK/WARN/FAIL severity ladder CI
// gates on — decoupled from how findings are gathered or displayed.
package doctor

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Status is the severity of a single [Finding]. The zero value is
// [StatusOK] so a freshly-constructed Finding is benign until proven
// otherwise.
type Status int

const (
	// StatusOK means the checked condition is healthy. OK findings are
	// surfaced (operators want to know what IS working, not just what is
	// broken) but never affect the exit code.
	StatusOK Status = iota
	// StatusInfo is advisory: a condition worth surfacing that is not a
	// problem. Like OK it never affects the exit code. Used where a default
	// applies and the operator should simply be aware (e.g. a namespace with
	// no CachePolicy falls back to server defaults).
	StatusInfo
	// StatusWarn is a likely-misconfiguration the operator should look at but
	// that does not, on its own, mean the cache plane is down. Any WARN drives
	// the process exit code to 1.
	StatusWarn
	// StatusFail is a hard failure: a control-plane component is unreachable or
	// reporting an unhealthy state. Any FAIL drives the process exit code to 2.
	StatusFail
)

// String returns the stable uppercase label used in human and table output and
// as the JSON enum value. Unknown values render as "UNKNOWN" rather than a Go
// %d so malformed output never silently parses as a real status.
func (s Status) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusInfo:
		return "INFO"
	case StatusWarn:
		return "WARN"
	case StatusFail:
		return "FAIL"
	default:
		return "UNKNOWN"
	}
}

// MarshalJSON renders the status as its stable string label (e.g. "WARN") so
// the documented JSON schema is self-describing rather than leaking the
// internal iota ordering.
func (s Status) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// Stable finding codes. Each code is a permanent, greppable identifier for a
// distinct diagnostic outcome — operators and CI pipelines key off these, so
// they MUST NOT be renumbered or repurposed once shipped (only retired). The
// prefix groups the code by check:
//
//	SV — server gRPC reachability/health
//	SN — /snapshot endpoint
//	PL — /policy endpoint
//	PB — /probe endpoint (controller-driven functional self-test)
//	CB — per-CacheBackend health
//	EP — engine-pod injection audit
//	OP — orphan-pod check
//	CT — CacheTenant health
//	CP — CachePolicy coverage
const (
	// CodeAPIReadFailed: a Kubernetes List/Get doctor needs failed (apiserver
	// unreachable, RBAC denial). The check could not draw a conclusion.
	CodeAPIReadFailed = "API001"

	// CodeServerUnreachable: the gRPC dial or Health/Check call to the
	// cache-plane server failed outright (connection refused, timeout, TLS).
	CodeServerUnreachable = "SV001"
	// CodeServerNotServing: Health/Check returned a state other than SERVING.
	CodeServerNotServing = "SV002"
	// CodeServerServing: Health/Check returned SERVING.
	CodeServerServing = "SV003"

	// CodeSnapshotUnreachable: the HTTP GET to /snapshot failed to connect or
	// returned a non-200 status.
	CodeSnapshotUnreachable = "SN001"
	// CodeSnapshotBadBody: /snapshot returned 200 but the body did not parse as
	// JSON.
	CodeSnapshotBadBody = "SN002"
	// CodeSnapshotUnauthenticated: /snapshot answered on the unauthenticated
	// path (no SA token was presented, or auth is not yet enforced). Advisory
	// until the snapshot auth hardening is wired everywhere.
	CodeSnapshotUnauthenticated = "SN003"
	// CodeSnapshotOK: /snapshot returned 200 with a JSON-parseable body.
	CodeSnapshotOK = "SN004"
	// CodeSnapshotAuthGated: /snapshot is reachable but rejected the request
	// with 401/403 — auth is enforced and doctor lacked a valid (audience-bound)
	// token. The endpoint is healthy; doctor just could not verify the body, so
	// this degrades gracefully to a WARN rather than a connectivity FAIL.
	CodeSnapshotAuthGated = "SN005"

	// CodePolicyRouteMissing: the /policy route is not wired (connection
	// refused / dial failure).
	CodePolicyRouteMissing = "PL001"
	// CodePolicyRouteWired: the /policy route answered with an expected status
	// (2xx / 401 / 403 / 405), proving the route exists.
	CodePolicyRouteWired = "PL002"
	// CodePolicyRouteUnexpected: the /policy route answered, but with an
	// unexpected status (e.g. 5xx) — the route is mounted but the server or its
	// middleware is erroring. Worth a look, but not a missing route.
	CodePolicyRouteUnexpected = "PL003"

	// CodeProbeRouteMissing: the /probe route (controller-driven functional
	// self-test endpoint, same auth profile as /snapshot + /policy) is not
	// wired — connection refused / dial failure / HTTP 404 = route not mounted.
	CodeProbeRouteMissing = "PB001"
	// CodeProbeRouteWired: the /probe route answered with an expected status
	// (2xx / 401 / 403 / 405), proving the route exists.
	CodeProbeRouteWired = "PB002"
	// CodeProbeRouteUnexpected: the /probe route answered with an unexpected
	// status (e.g. 5xx) — mounted but erroring.
	CodeProbeRouteUnexpected = "PB003"

	// CodeBackendNotReady: the CacheBackend's Ready condition is not True.
	CodeBackendNotReady = "CB001"
	// CodeBackendSelectorMismatch: status.matchedEnginePods is 0 — the engine
	// selector most likely matches no pods (label drift / engine not deployed).
	CodeBackendSelectorMismatch = "CB002"
	// CodeBackendNotReportingState: no KV event has ever been observed for the
	// backend — BOTH the durable status.firstKVEventObservedAt latch and the
	// current-view status.indexParticipation.lastEventAt are unset, so the
	// engine's KV-event publisher has never been heard from. A drained backend
	// that has ever observed an event keeps the latch and is NOT flagged here
	// (its cleared lastEventAt is expected). Zero warm prefixes alone does NOT
	// trigger this: an idle backend with a fresh event is healthy.
	CodeBackendNotReportingState = "CB003"
	// CodeBackendStale: status.indexParticipation.lastEventAt is older than the
	// staleness window — KV events have stopped flowing.
	CodeBackendStale = "CB004"
	// CodeBackendEndpointUnreachable: status.endpoint is empty or not reachable
	// over TCP.
	CodeBackendEndpointUnreachable = "CB005"
	// CodeBackendHealthy: the CacheBackend passed every per-backend check.
	CodeBackendHealthy = "CB006"
	// CodeBackendFunctionalProbeFailing: the CacheBackend's FunctionalProbeOK
	// condition is present but not True — the controller's end-to-end functional
	// self-test (/probe) is failing or inconclusive for this backend, which is
	// why Ready may be downgraded. Surfaces the underlying reason the bare Ready
	// bit does not.
	CodeBackendFunctionalProbeFailing = "CB007"

	// CodeEnginePodNotInjected: a pod matching a CacheBackend's engineSelector
	// has no InjectedByCacheBackend Event — it may be serving uncached.
	CodeEnginePodNotInjected = "EP001"
	// CodeEnginePodInjected: a matched engine pod carries the injection Event.
	CodeEnginePodInjected = "EP002"

	// CodeOrphanPod: a pod has a NoMatchingCacheBackend Event — it expected
	// injection but matched no CacheBackend (likely operator misconfiguration).
	CodeOrphanPod = "OP001"

	// CodeTenantQuotaExceeded: a CacheTenant's QuotaExceeded condition is True.
	CodeTenantQuotaExceeded = "CT001"
	// CodeTenantHealthy: a CacheTenant is within quota.
	CodeTenantHealthy = "CT002"

	// CodePolicyCoverageMissing: a namespace containing CacheBackends has no
	// CachePolicy (server defaults apply — advisory, not an error).
	CodePolicyCoverageMissing = "CP001"
	// CodePolicyCoveragePresent: a namespace with CacheBackends has exactly one
	// CachePolicy.
	CodePolicyCoveragePresent = "CP002"
	// CodePolicyCoverageDuplicate: a namespace has more than one CachePolicy.
	// Admission allows at most one per namespace but is best-effort (it can race
	// concurrent CREATEs), and the controller deterministically keeps only the
	// lexicographically-first — so the extras are inert and worth surfacing.
	CodePolicyCoverageDuplicate = "CP003"
)

// Finding is a single diagnostic result. The struct doubles as the stable JSON
// schema element emitted by `--output=json`, so its json tags are part of the
// tool's contract: `code`, `status`, `check`, `resource`, `message`. Renaming a
// field is a breaking change for any pipeline parsing the output.
type Finding struct {
	// Code is the stable greppable identifier (e.g. "CB002"). See the Code*
	// constants.
	Code string `json:"code"`
	// Status is the severity of this finding.
	Status Status `json:"status"`
	// Check names the check group that produced the finding (e.g.
	// "ServerReachability"). Lets operators correlate a code with the section
	// of the run that emitted it.
	Check string `json:"check"`
	// Resource identifies the Kubernetes object or endpoint the finding is
	// about, in a stable "kind/namespace/name" or "host:port" form. Empty for
	// cluster-wide findings.
	Resource string `json:"resource,omitempty"`
	// Message is the human-readable explanation, including any reason/hint the
	// operator needs to act.
	Message string `json:"message"`
}

// Report is the ordered collection of findings produced by a doctor run. It is
// append-only during a run and read-only afterwards; the formatters consume it.
type Report struct {
	// Findings are stored in the order checks emitted them — checks run in a
	// fixed order so the human output reads as a top-to-bottom narrative.
	Findings []Finding `json:"findings"`
}

// Add appends a finding to the report.
func (r *Report) Add(f Finding) {
	r.Findings = append(r.Findings, f)
}

// Addf is a convenience that constructs and appends a finding with a
// fmt-formatted message.
func (r *Report) Addf(code string, status Status, check, resource, format string, args ...any) {
	r.Add(Finding{
		Code:     code,
		Status:   status,
		Check:    check,
		Resource: resource,
		Message:  fmt.Sprintf(format, args...),
	})
}

// Worst returns the highest severity among all findings, or [StatusOK] for an
// empty report.
func (r *Report) Worst() Status {
	worst := StatusOK
	for _, f := range r.Findings {
		if f.Status > worst {
			worst = f.Status
		}
	}
	return worst
}

// ExitCode maps the worst finding severity to a process exit code suitable for
// CI gating: 0 when nothing is worse than INFO, 1 when the worst is WARN, 2
// when any FAIL is present.
func (r *Report) ExitCode() int {
	switch r.Worst() {
	case StatusFail:
		return 2
	case StatusWarn:
		return 1
	default:
		return 0
	}
}

// Counts returns the number of findings at each status, keyed by the status
// label ("OK"/"INFO"/"WARN"/"FAIL"). Statuses with no findings are present with
// a zero count so callers (summary lines, dashboards) get a stable shape.
func (r *Report) Counts() map[string]int {
	counts := map[string]int{
		StatusOK.String():   0,
		StatusInfo.String(): 0,
		StatusWarn.String(): 0,
		StatusFail.String(): 0,
	}
	for _, f := range r.Findings {
		counts[f.Status.String()]++
	}
	return counts
}

// FindingsByStatus returns the findings whose status equals want, preserving
// emission order. Used by the human formatter to print the OK / WARN / FAIL
// sections.
func (r *Report) FindingsByStatus(want Status) []Finding {
	var out []Finding
	for _, f := range r.Findings {
		if f.Status == want {
			out = append(out, f)
		}
	}
	return out
}

// statusOrder is the descending severity order the human formatter walks so the
// most urgent section (FAIL) prints first.
var statusOrder = []Status{StatusFail, StatusWarn, StatusInfo, StatusOK}

// OrderedStatuses returns the statuses in descending severity order (FAIL,
// WARN, INFO, OK). Exposed so formatters share one canonical ordering.
func OrderedStatuses() []Status {
	out := make([]Status, len(statusOrder))
	copy(out, statusOrder)
	return out
}

// SortedCodes returns the distinct finding codes present in the report, sorted
// lexicographically. Convenience for tests and summaries.
func (r *Report) SortedCodes() []string {
	seen := map[string]struct{}{}
	for _, f := range r.Findings {
		seen[f.Code] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}
