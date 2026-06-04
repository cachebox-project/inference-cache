// Package checks implements the `inferencecache doctor` pre-flight diagnostics.
//
// Each exported Check* function inspects one facet of an inference-cache
// installation (server reachability, per-CacheBackend health, engine-pod
// injection, tenant quota, …) and returns a slice of [doctor.Finding]. The
// functions take narrow, injectable dependencies — a controller-runtime
// client.Client for Kubernetes reads, a [HealthChecker] for the gRPC health
// ping, an [HTTPDoer] for the HTTP endpoint probes, and a [TCPDialer] for raw
// endpoint reachability — so every check is unit-testable against a fake k8s
// client and an in-process gRPC/HTTP server with no real cluster.
//
// [Run] wires the checks together in the fixed order the operator-facing
// output documents and returns an aggregated [doctor.Report]. The orchestration
// is deliberately read-only: doctor never mutates cluster state.
package checks

import (
	"context"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cachebox-project/inference-cache/pkg/cli/doctor"
)

// DefaultStaleWindow is how long a CacheBackend may go without a fresh KV event
// before doctor flags it as stale ("within last 2m" per the doctor spec).
const DefaultStaleWindow = 2 * time.Minute

// DefaultOrphanWindow bounds how far back doctor looks for NoMatchingCacheBackend
// pod Events when hunting for orphaned engine pods.
const DefaultOrphanWindow = 24 * time.Hour

// Condition type and Event reason strings doctor READS. These mirror the
// strings the controller writes (internal/controller) and the cache server
// emits; they are duplicated here rather than imported because the writers live
// in an internal package and because doctor must keep working as a standalone
// client even if a check's target condition is not yet wired (it then simply
// finds nothing). Keep these in lockstep with the producers.
const (
	conditionReady         = "Ready"
	conditionQuotaExceeded = "QuotaExceeded"

	eventInjectedByCacheBackend = "InjectedByCacheBackend"
	// eventNoMatchingCacheBackend is the orphan-pod signal the OrphanPods check
	// (OP001) reads. NOTE: no controller emits this Event today — only the
	// InjectedByCacheBackend Event is wired (engine_pod_events_controller). The
	// orphan check is forward-looking scaffolding per the doctor spec: it is a
	// no-op until the engine-pod binding work adds an emitter for pods that match
	// no CacheBackend. Until then OP001 cannot fire against a real cluster.
	eventNoMatchingCacheBackend = "NoMatchingCacheBackend"
)

// HealthChecker is the subset of the generated gRPC health client doctor needs.
// The real *grpc_health_v1.HealthClient satisfies it, and tests can supply an
// in-process bufconn client or a hand-rolled stub.
type HealthChecker interface {
	Check(ctx context.Context, in *healthpb.HealthCheckRequest, opts ...grpc.CallOption) (*healthpb.HealthCheckResponse, error)
}

// HTTPDoer is the subset of *http.Client doctor needs for the /snapshot and
// /policy probes. httptest servers' clients satisfy it directly.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// TCPDialer reports whether a raw TCP connection to addr ("host:port")
// succeeds. A nil error means reachable. Injected so endpoint reachability is
// testable without opening real sockets.
type TCPDialer func(ctx context.Context, addr string) error

// Deps bundles everything [Run] needs. The caller (cmd/inferencecache) builds
// the real implementations from the operator's kubeconfig and the discovered
// server endpoint; tests build fakes. Any of Health, HTTP, or DialTCP may be
// nil when the server endpoint could not be resolved — the affected checks then
// emit a FAIL rather than panicking.
type Deps struct {
	// K8s reads CacheBackends, CacheTenants, CachePolicies, Pods, and Events.
	K8s client.Client
	// Namespace scopes the Kubernetes reads. Empty means all namespaces.
	Namespace string

	// Health pings the server's grpc.health.v1 service. nil if the gRPC target
	// could not be dialed.
	Health HealthChecker
	// ServerTarget is the host:port the gRPC health ping addresses, shown in
	// findings for operator context.
	ServerTarget string

	// HTTP issues the /snapshot and /policy probes. nil if the server HTTP
	// endpoint could not be resolved.
	HTTP HTTPDoer
	// SnapshotURL and PolicyURL are the fully-qualified probe targets.
	SnapshotURL string
	PolicyURL   string
	// Token is the ServiceAccount bearer token presented to /snapshot. Empty
	// means doctor probes the unauthenticated path and flags the auth state.
	Token string

	// DialTCP probes raw reachability of a CacheBackend's status.endpoint. nil
	// disables the TCP sub-probe (endpoint presence is still checked).
	DialTCP TCPDialer

	// Now supplies the reference time for staleness/recency math. Defaults to
	// time.Now when nil.
	Now func() time.Time
	// StaleWindow overrides DefaultStaleWindow when non-zero.
	StaleWindow time.Duration
	// OrphanWindow overrides DefaultOrphanWindow when non-zero.
	OrphanWindow time.Duration

	// SkipEndpointChecks omits the three live control-plane endpoint probes
	// (server gRPC health, /snapshot, /policy) and runs only the declarative
	// cluster-configuration checks. Set by `doctor --config-only` for operators
	// validating CacheBackend/CacheTenant/CachePolicy configuration without a
	// reachable cache server (e.g. in CI, or before exposing the Service).
	SkipEndpointChecks bool
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d Deps) staleWindow() time.Duration {
	if d.StaleWindow > 0 {
		return d.StaleWindow
	}
	return DefaultStaleWindow
}

func (d Deps) orphanWindow() time.Duration {
	if d.OrphanWindow > 0 {
		return d.OrphanWindow
	}
	return DefaultOrphanWindow
}

// Run executes every doctor check in the documented order and returns the
// aggregated report. The order is stable so the human output reads as a
// top-to-bottom narrative: control-plane endpoints first, then the
// CacheBackend/engine-pod data path, then tenant and policy configuration.
func Run(ctx context.Context, d Deps) *doctor.Report {
	report := &doctor.Report{}
	now := d.now()

	add := func(fs []doctor.Finding) {
		for _, f := range fs {
			report.Add(f)
		}
	}

	// 1–3: control-plane endpoint reachability (skippable via --config-only).
	if !d.SkipEndpointChecks {
		add(ServerReachability(ctx, d.Health, d.ServerTarget))
		add(SnapshotReachability(ctx, d.HTTP, d.SnapshotURL, d.Token))
		add(PolicyReachability(ctx, d.HTTP, d.PolicyURL))
	}

	// 4: per-CacheBackend health.
	add(CacheBackendHealth(ctx, d.K8s, d.Namespace, now, d.staleWindow(), d.DialTCP))

	// 5: engine-pod injection audit.
	add(EnginePodInjectionAudit(ctx, d.K8s, d.Namespace))

	// 6: orphan-pod check.
	add(OrphanPods(ctx, d.K8s, d.Namespace, now, d.orphanWindow()))

	// 7: CacheTenant health.
	add(CacheTenantHealth(ctx, d.K8s, d.Namespace))

	// 8: CachePolicy coverage.
	add(CachePolicyCoverage(ctx, d.K8s, d.Namespace))

	return report
}

// findCondition returns the condition of the given type, or nil if absent.
func findCondition(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

// selectorMatches reports whether every key/value in matchLabels is present on
// labels. An empty selector matches nothing (a CacheBackend with no
// engineSelector claims no pods), mirroring the controller's equality-based
// MatchLabels semantics. This is intentionally NOT the
// "empty selector matches everything" labels.Everything() behavior.
func selectorMatches(matchLabels, labels map[string]string) bool {
	if len(matchLabels) == 0 {
		return false
	}
	for k, v := range matchLabels {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// listEventsFor returns the Events in ns (or all namespaces when ns is empty)
// involving the named object of the given kind. Doctor filters in code rather
// than via a field selector so the same path works against the fake client used
// in tests (which does not implement server-side field selection).
func listEventsFor(ctx context.Context, c client.Client, ns, kind, name string) ([]corev1.Event, error) {
	var events corev1.EventList
	if err := c.List(ctx, &events, client.InNamespace(ns)); err != nil {
		return nil, err
	}
	var out []corev1.Event
	for i := range events.Items {
		e := events.Items[i]
		if e.InvolvedObject.Kind == kind && e.InvolvedObject.Name == name {
			out = append(out, e)
		}
	}
	return out, nil
}

// hasEventReason reports whether any of the events carries the given reason.
func hasEventReason(events []corev1.Event, reason string) bool {
	for i := range events {
		if events[i].Reason == reason {
			return true
		}
	}
	return false
}

// eventTime returns the most informative timestamp on an Event, preferring the
// series/last-observed time and falling back through lastTimestamp,
// eventTime, and the object creation time.
func eventTime(e corev1.Event) time.Time {
	if e.Series != nil && !e.Series.LastObservedTime.IsZero() {
		return e.Series.LastObservedTime.Time
	}
	if !e.LastTimestamp.IsZero() {
		return e.LastTimestamp.Time
	}
	if !e.EventTime.IsZero() {
		return e.EventTime.Time
	}
	return e.CreationTimestamp.Time
}

// resourceRef formats a stable "kind/namespace/name" reference for findings.
func resourceRef(kind, namespace, name string) string {
	return strings.ToLower(kind) + "/" + namespace + "/" + name
}
