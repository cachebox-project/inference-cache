package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// kvReadinessTrue is the "everything upstream said Ready=True" baseline
// the gate composes on. The probe gate only fires from this starting
// state; from any other state the gate is a no-op.
var kvReadinessTrue = kvReadiness{
	readyStatus:     metav1.ConditionTrue,
	readyReason:     reasonKVEventsObserved,
	readyMessage:    "ok",
	degradedStatus:  metav1.ConditionFalse,
	degradedReason:  reasonNotDegraded,
	degradedMessage: "not degraded",
}

func newProbeBackend(name string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:        cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"},
		},
	}
}

// fakeProbeServer is a minimal /probe responder that returns whatever
// ProbeResult is produced by the supplied callback. `respond` MUST be
// non-nil — the helper does not synthesize an unreachable mode (transport-
// failure tests build a closed-server URL directly; see
// TestProbeClientRunTransportError). Counts calls so rate-limit tests can
// assert "exactly one probe call across these reconciles".
type fakeProbeServer struct {
	*httptest.Server
	calls atomic.Int64
}

func newFakeProbeServer(t *testing.T, respond func(req cacheserver.ProbeRequest) cacheserver.ProbeResult) *fakeProbeServer {
	t.Helper()
	if respond == nil {
		t.Fatalf("newFakeProbeServer: respond callback must be non-nil")
	}
	f := &fakeProbeServer{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		var req cacheserver.ProbeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(respond(req))
	}))
	t.Cleanup(f.Close)
	return f
}

// TestProbeRateLimiterForgetDropsKey confirms the cleanup hook the
// reconciler's NotFound branch calls when a CacheBackend is deleted.
// Without this hook, the per-(namespace, name) sync.Map would
// accumulate one entry per deleted backend forever on a long-lived
// controller against a churning fleet.
func TestProbeRateLimiterForgetDropsKey(t *testing.T) {
	limiter := &probeRateLimiter{}
	now := time.Unix(1_000_000, 0)
	limiter.markCalled("team-a/cb-1", now)
	limiter.markCalled("team-a/cb-2", now)

	if got := limiter.lastCalled("team-a/cb-1"); got != now {
		t.Fatalf("cb-1 should be present; got %v", got)
	}
	limiter.forget("team-a/cb-1")
	if got := limiter.lastCalled("team-a/cb-1"); !got.IsZero() {
		t.Errorf("cb-1 should be removed by forget; got %v", got)
	}
	// Untouched siblings remain.
	if got := limiter.lastCalled("team-a/cb-2"); got != now {
		t.Errorf("forget(cb-1) must not affect cb-2; got %v", got)
	}
	// forget on an unknown key is a no-op.
	limiter.forget("team-a/cb-never")
}

// TestEvaluateFunctionalProbeUpstreamNotReadyShortCircuits pins the cascade-
// prevention rule the gate has to honor: when the KV-event verdict already
// says Ready=False, the probe call is skipped (no metric, no HTTP, no
// condition). A broken upstream can't be diagnosed by a downstream probe,
// and bombarding the server for backends that can't be Ready anyway is
// pure noise.
func TestEvaluateFunctionalProbeUpstreamNotReadyShortCircuits(t *testing.T) {
	srv := newFakeProbeServer(t, func(cacheserver.ProbeRequest) cacheserver.ProbeResult {
		return cacheserver.ProbeResult{Backend: "x", Ingest: cacheserver.ProbeStageOK, Routing: cacheserver.ProbeStageOK, T2: cacheserver.ProbeStageSkipped}
	})
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}

	upstream := kvReadinessTrue
	upstream.readyStatus = metav1.ConditionFalse
	upstream.readyReason = reasonAwaitingFirstKVEvent

	v := evaluateFunctionalProbe(context.Background(), newProbeBackend("cb"), upstream,
		client, &probeRateLimiter{}, time.Second, time.Now())

	if v.shouldWriteCondition {
		t.Errorf("upstream Ready=False must not write FunctionalProbeOK; got %+v", v.condition)
	}
	if srv.calls.Load() != 0 {
		t.Errorf("expected 0 probe HTTP calls, got %d", srv.calls.Load())
	}
}

// TestEvaluateFunctionalProbeBypassAnnotation pins the operator escape
// hatch: annotation inferencecache.io/skip-functional-probe="true" flips
// FunctionalProbeOK=True with reason=ProbeBypassed and short-circuits the
// HTTP call, so the Ready gate is unaffected and the server isn't probed.
func TestEvaluateFunctionalProbeBypassAnnotation(t *testing.T) {
	srv := newFakeProbeServer(t, func(cacheserver.ProbeRequest) cacheserver.ProbeResult {
		t.Fatalf("probe call should be skipped when bypassed")
		return cacheserver.ProbeResult{}
	})
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}

	backend := newProbeBackend("cb")
	backend.Annotations = map[string]string{annotationSkipFunctionalProbe: "true"}

	v := evaluateFunctionalProbe(context.Background(), backend, kvReadinessTrue,
		client, &probeRateLimiter{}, time.Second, time.Now())

	if !v.shouldWriteCondition || v.condition.Status != metav1.ConditionTrue || v.condition.Reason != reasonProbeBypassed {
		t.Fatalf("want FunctionalProbeOK=True/ProbeBypassed; got %+v", v.condition)
	}
	if v.downgradeReady {
		t.Errorf("bypass must NOT downgrade Ready")
	}
}

// TestEvaluateFunctionalProbeBypassParserIsStrict pins the conservative
// annotation parser: only the exact string "true" counts. Any other value
// (capitalized, whitespace, "1", "yes") leaves the gate enabled, so a
// fat-fingered annotation can't quietly disable the gate.
func TestEvaluateFunctionalProbeBypassParserIsStrict(t *testing.T) {
	srv := newFakeProbeServer(t, func(cacheserver.ProbeRequest) cacheserver.ProbeResult {
		return cacheserver.ProbeResult{Backend: "x", Ingest: cacheserver.ProbeStageOK, Routing: cacheserver.ProbeStageOK, T2: cacheserver.ProbeStageSkipped}
	})
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}

	for _, value := range []string{"True", "TRUE", "1", "yes", " true ", ""} {
		t.Run(fmt.Sprintf("value=%q", value), func(t *testing.T) {
			backend := newProbeBackend("cb")
			backend.Annotations = map[string]string{annotationSkipFunctionalProbe: value}
			v := evaluateFunctionalProbe(context.Background(), backend, kvReadinessTrue,
				client, &probeRateLimiter{}, time.Second, time.Now())
			if v.shouldWriteCondition && v.condition.Reason == reasonProbeBypassed {
				t.Errorf("value %q must not be parsed as bypass", value)
			}
		})
	}
}

// TestEvaluateFunctionalProbeNilClientDisabled pins the "no probe
// configured" path. A nil/empty client is the local-dev shape; the gate
// must NOT write a condition that pretends the probe ran.
func TestEvaluateFunctionalProbeNilClientDisabled(t *testing.T) {
	v := evaluateFunctionalProbe(context.Background(), newProbeBackend("cb"), kvReadinessTrue,
		nil, &probeRateLimiter{}, time.Second, time.Now())
	if v.shouldWriteCondition {
		t.Fatalf("nil client must not write a condition; got %+v", v.condition)
	}
}

// TestEvaluateFunctionalProbeRateLimit pins the per-(namespace, name)
// rate-limit: a successful call records a timestamp; a second invocation
// inside the window skips the HTTP call and inherits the condition.
func TestEvaluateFunctionalProbeRateLimit(t *testing.T) {
	srv := newFakeProbeServer(t, func(cacheserver.ProbeRequest) cacheserver.ProbeResult {
		return cacheserver.ProbeResult{Backend: "team-a/cb", Ingest: cacheserver.ProbeStageOK, Routing: cacheserver.ProbeStageOK, T2: cacheserver.ProbeStageSkipped}
	})
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}
	limiter := &probeRateLimiter{}
	now := time.Unix(1_000_000, 0)

	v1 := evaluateFunctionalProbe(context.Background(), newProbeBackend("cb"), kvReadinessTrue, client, limiter, 30*time.Second, now)
	if !v1.shouldWriteCondition || v1.condition.Status != metav1.ConditionTrue {
		t.Fatalf("first call should write OK; got %+v", v1.condition)
	}
	// Simulate the reconciler committing the status patch — only then is
	// the rate-limit slot recorded (so a failed patch doesn't burn the
	// window). This is the contract: gate returns commitMark; caller
	// invokes it after patchStatus succeeds.
	if v1.commitMark == nil {
		t.Fatalf("successful call must return a commitMark closure")
	}
	v1.commitMark()

	// Same instant — well inside the 30s window — must NOT call again.
	v2 := evaluateFunctionalProbe(context.Background(), newProbeBackend("cb"), kvReadinessTrue, client, limiter, 30*time.Second, now.Add(5*time.Second))
	if v2.shouldWriteCondition {
		t.Errorf("rate-limited call must not write a condition; got %+v", v2.condition)
	}
	if v2.requeueAfter <= 0 || v2.requeueAfter > 30*time.Second {
		t.Errorf("rate-limited verdict should requeue inside the remaining window; got %v", v2.requeueAfter)
	}
	if srv.calls.Load() != 1 {
		t.Errorf("expected exactly 1 probe call across two reconciles within the rate-limit window; got %d", srv.calls.Load())
	}

	// After the window — a fresh call lands.
	v3 := evaluateFunctionalProbe(context.Background(), newProbeBackend("cb"), kvReadinessTrue, client, limiter, 30*time.Second, now.Add(31*time.Second))
	if !v3.shouldWriteCondition {
		t.Errorf("post-window call should write a condition; got %+v", v3.condition)
	}
	if srv.calls.Load() != 2 {
		t.Errorf("expected 2 probe calls across the window boundary; got %d", srv.calls.Load())
	}
	if v3.commitMark != nil {
		v3.commitMark()
	}
}

// TestEvaluateFunctionalProbeRateLimitedFailurePreservesDowngrade is the
// regression guard for the Blocking issue raised in PR review: a
// rate-limited reconcile that hits an existing FunctionalProbeOK=False
// MUST re-apply the Ready downgrade — otherwise the status patch
// would silently overwrite the prior Ready=False/Probe*Failed with the
// upstream KV gate's Ready=True/KVEventsObserved, and the operator-visible
// signal would mislead.
func TestEvaluateFunctionalProbeRateLimitedFailurePreservesDowngrade(t *testing.T) {
	srv := newFakeProbeServer(t, func(cacheserver.ProbeRequest) cacheserver.ProbeResult {
		return cacheserver.ProbeResult{
			Ingest: cacheserver.ProbeStageFailed, Routing: cacheserver.ProbeStageSkipped, T2: cacheserver.ProbeStageSkipped,
			Errors: []cacheserver.ProbeStageError{{Stage: cacheserver.ProbeStageIngest, Message: "broken"}},
		}
	})
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}
	limiter := &probeRateLimiter{}
	now := time.Unix(1_000_000, 0)

	// First reconcile: probe returns ingest=failed. Gate downgrades Ready
	// + writes FunctionalProbeOK=False. Simulate patch commit so the
	// rate-limit slot lands.
	backend := newProbeBackend("cb")
	v1 := evaluateFunctionalProbe(context.Background(), backend, kvReadinessTrue, client, limiter, 30*time.Second, now)
	if !v1.downgradeReady || v1.readyReason != reasonProbeIngestFailed {
		t.Fatalf("first call should downgrade Ready/ProbeIngestFailed; got %+v", v1)
	}
	v1.commitMark()
	// Simulate the patch having landed the FunctionalProbeOK=False
	// condition: a watch event on the resulting status now triggers a
	// second reconcile within the rate-limit window.
	backend.Status.Conditions = append(backend.Status.Conditions, v1.condition)

	v2 := evaluateFunctionalProbe(context.Background(), backend, kvReadinessTrue, client, limiter, 30*time.Second, now.Add(5*time.Second))
	if v2.shouldWriteCondition {
		t.Errorf("rate-limited reconcile must not overwrite existing condition; got %+v", v2.condition)
	}
	if !v2.downgradeReady {
		t.Fatalf("rate-limited reconcile MUST re-apply Ready downgrade when existing FunctionalProbeOK is False (else KV gate's Ready=True silently overwrites the failure); got verdict %+v", v2)
	}
	if v2.readyReason != reasonProbeIngestFailed || v2.readyMessage == "" {
		t.Errorf("re-applied downgrade should inherit the existing reason/message; got reason=%q message=%q", v2.readyReason, v2.readyMessage)
	}
	if srv.calls.Load() != 1 {
		t.Errorf("rate-limited reconcile must skip the HTTP call; got %d total calls", srv.calls.Load())
	}
	if v2.requeueAfter <= 0 {
		t.Errorf("rate-limited verdict must schedule a requeue so recovery doesn't depend on watch events; got %v", v2.requeueAfter)
	}
}

// TestEvaluateFunctionalProbeHTTPErrorPreservesExistingFailure pins the
// rule that a transient /probe HTTP error after a known stage failure
// must NOT silently upgrade Ready to True, NOR fade the False condition
// to Unknown (which would lose the original failure reason and then
// upgrade on the next reconcile). When an existing FunctionalProbeOK=
// False is present, an HTTP error preserves the existing condition
// (no new write) AND re-applies the Ready downgrade — symmetric to the
// rate-limited path's behavior. False stays sticky until a successful
// probe call resolves it.
func TestEvaluateFunctionalProbeHTTPErrorPreservesExistingFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "outage", http.StatusInternalServerError)
	}))
	defer srv.Close()
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}

	backend := newProbeBackend("cb")
	backend.Status.Conditions = []metav1.Condition{{
		Type:    conditionTypeFunctionalProbeOK,
		Status:  metav1.ConditionFalse,
		Reason:  reasonProbeIngestFailed,
		Message: "synthesized event not in index",
	}}

	v := evaluateFunctionalProbe(context.Background(), backend, kvReadinessTrue,
		client, &probeRateLimiter{}, time.Second, time.Now())

	if v.shouldWriteCondition {
		t.Fatalf("HTTP error with existing False must NOT write a new condition (would fade False→Unknown and lose the failure reason); got %+v", v.condition)
	}
	if !v.downgradeReady {
		t.Fatalf("HTTP error with existing False MUST re-apply Ready downgrade so the upstream KV gate doesn't overwrite Ready=False with True; got %+v", v)
	}
	if v.readyReason != reasonProbeIngestFailed {
		t.Errorf("re-applied downgrade should inherit existing reason; got %q, want %q", v.readyReason, reasonProbeIngestFailed)
	}
	if v.requeueAfter <= 0 {
		t.Errorf("HTTP error verdict must schedule a requeue; got %v", v.requeueAfter)
	}
}

// TestEvaluateFunctionalProbeHTTPErrorSchedulesRequeue pins the requeue
// behavior on transport failures: without an explicit requeueAfter, a
// transient outage could leave FunctionalProbeOK=Unknown on a quiet backend
// indefinitely (recovery depends on watch events that may not fire). The
// verdict always requeues at the rate-limit cadence so recovery happens
// on its own.
func TestEvaluateFunctionalProbeHTTPErrorSchedulesRequeue(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "outage", http.StatusInternalServerError)
	}))
	defer srv.Close()
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}

	v := evaluateFunctionalProbe(context.Background(), newProbeBackend("cb-fresh"), kvReadinessTrue,
		client, &probeRateLimiter{}, 45*time.Second, time.Now())

	if v.condition.Status != metav1.ConditionUnknown {
		t.Fatalf("want Unknown on a fresh HTTP error; got %+v", v.condition)
	}
	if v.requeueAfter != 45*time.Second {
		t.Errorf("HTTP error verdict must requeue at rateLimit cadence; got %v, want 45s", v.requeueAfter)
	}
}

// TestEvaluateFunctionalProbeDisabledClearsExistingCondition pins the
// "probe was turned off" path: if a backend carries a prior
// FunctionalProbeOK condition and the operator unsets --server-probe-url
// (or otherwise nils the client), the gate emits a removeCondition verdict
// so the stale operator-visible signal doesn't linger forever.
func TestEvaluateFunctionalProbeDisabledClearsExistingCondition(t *testing.T) {
	backend := newProbeBackend("cb")
	backend.Status.Conditions = []metav1.Condition{{
		Type:   conditionTypeFunctionalProbeOK,
		Status: metav1.ConditionTrue,
		Reason: reasonProbeOK,
	}}
	v := evaluateFunctionalProbe(context.Background(), backend, kvReadinessTrue, nil, &probeRateLimiter{}, time.Second, time.Now())
	if !v.removeCondition || v.shouldWriteCondition {
		t.Fatalf("want removeCondition=true (no write); got %+v", v)
	}

	// Without an existing condition, disabling is a true no-op.
	backendFresh := newProbeBackend("cb-fresh")
	v2 := evaluateFunctionalProbe(context.Background(), backendFresh, kvReadinessTrue, nil, &probeRateLimiter{}, time.Second, time.Now())
	if v2.removeCondition || v2.shouldWriteCondition {
		t.Errorf("nil client + no existing condition must be a no-op; got %+v", v2)
	}
}

// TestEvaluateFunctionalProbeDisabledCleansEvenWhenUpstreamNotReady pins
// the rule that the disabled-client cleanup runs BEFORE the upstream-not-
// Ready short-circuit. Otherwise a backend in RolloutInProgress /
// AwaitingFirstKVEvent / ReplicasUnavailable would keep its stale
// FunctionalProbeOK condition indefinitely after --server-probe-url is
// disabled, because the cascade-short-circuit would return an empty
// verdict before the cleanup ran.
func TestEvaluateFunctionalProbeDisabledCleansEvenWhenUpstreamNotReady(t *testing.T) {
	backend := newProbeBackend("cb")
	backend.Status.Conditions = []metav1.Condition{{
		Type:   conditionTypeFunctionalProbeOK,
		Status: metav1.ConditionTrue,
		Reason: reasonProbeOK,
	}}
	// Upstream says Ready=False (e.g. AwaitingFirstKVEvent).
	upstream := kvReadinessTrue
	upstream.readyStatus = metav1.ConditionFalse
	upstream.readyReason = reasonAwaitingFirstKVEvent

	// Probe client is disabled. The cleanup MUST still fire.
	v := evaluateFunctionalProbe(context.Background(), backend, upstream, nil, &probeRateLimiter{}, time.Second, time.Now())
	if !v.removeCondition {
		t.Fatalf("disabled-client cleanup must run regardless of upstream readiness; got %+v", v)
	}
}

// TestEvaluateFunctionalProbeStageFailureDowngradesReady covers the central
// shipping behavior: a server-reported stage failure flips
// FunctionalProbeOK=False AND downgrades Ready with the stage-specific
// reason. One table case per stage so a future stage addition is an
// obvious diff.
func TestEvaluateFunctionalProbeStageFailureDowngradesReady(t *testing.T) {
	for _, tc := range []struct {
		name       string
		result     cacheserver.ProbeResult
		wantReason string
		wantMsg    string
	}{
		{
			name: "ingest failed",
			result: cacheserver.ProbeResult{
				Ingest: cacheserver.ProbeStageFailed, Routing: cacheserver.ProbeStageSkipped, T2: cacheserver.ProbeStageSkipped,
				Errors: []cacheserver.ProbeStageError{{Stage: cacheserver.ProbeStageIngest, Message: "synthesized event did not land"}},
			},
			wantReason: reasonProbeIngestFailed,
			wantMsg:    "synthesized event did not land",
		},
		{
			name: "routing failed",
			result: cacheserver.ProbeResult{
				Ingest: cacheserver.ProbeStageOK, Routing: cacheserver.ProbeStageFailed, T2: cacheserver.ProbeStageSkipped,
				Errors: []cacheserver.ProbeStageError{{Stage: cacheserver.ProbeStageRouting, Message: "LookupRoute returned NO_HINT"}},
			},
			wantReason: reasonProbeRoutingFailed,
			wantMsg:    "LookupRoute returned NO_HINT",
		},
		{
			name: "t2 failed",
			result: cacheserver.ProbeResult{
				Ingest: cacheserver.ProbeStageOK, Routing: cacheserver.ProbeStageOK, T2: cacheserver.ProbeStageFailed,
				Errors: []cacheserver.ProbeStageError{{Stage: cacheserver.ProbeStageT2, Message: "put failed: connection refused"}},
			},
			wantReason: reasonProbeT2Failed,
			wantMsg:    "put failed: connection refused",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFakeProbeServer(t, func(cacheserver.ProbeRequest) cacheserver.ProbeResult { return tc.result })
			client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}

			v := evaluateFunctionalProbe(context.Background(), newProbeBackend("cb"), kvReadinessTrue,
				client, &probeRateLimiter{}, time.Second, time.Now())
			if !v.shouldWriteCondition {
				t.Fatalf("expected condition write on stage failure; got %+v", v)
			}
			if v.condition.Status != metav1.ConditionFalse || v.condition.Reason != tc.wantReason {
				t.Errorf("condition = %+v, want status=False reason=%s", v.condition, tc.wantReason)
			}
			if !strings.Contains(v.condition.Message, tc.wantMsg) {
				t.Errorf("condition message %q should include stage diagnostic %q", v.condition.Message, tc.wantMsg)
			}
			if !v.downgradeReady || v.readyReason != tc.wantReason {
				t.Errorf("downgradeReady=%v readyReason=%q; want true + %s", v.downgradeReady, v.readyReason, tc.wantReason)
			}
		})
	}
}

// TestEvaluateFunctionalProbeAllPassedWritesOK pins the happy path: every
// stage `ok` (or `skipped`) → FunctionalProbeOK=True with reason=ProbeOK
// and no Ready downgrade.
func TestEvaluateFunctionalProbeAllPassedWritesOK(t *testing.T) {
	srv := newFakeProbeServer(t, func(cacheserver.ProbeRequest) cacheserver.ProbeResult {
		return cacheserver.ProbeResult{
			Backend: "team-a/cb", Ingest: cacheserver.ProbeStageOK,
			Routing: cacheserver.ProbeStageOK, T2: cacheserver.ProbeStageSkipped,
		}
	})
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}

	v := evaluateFunctionalProbe(context.Background(), newProbeBackend("cb"), kvReadinessTrue,
		client, &probeRateLimiter{}, time.Second, time.Now())

	if !v.shouldWriteCondition || v.condition.Status != metav1.ConditionTrue || v.condition.Reason != reasonProbeOK {
		t.Fatalf("want FunctionalProbeOK=True/ProbeOK; got %+v", v.condition)
	}
	if v.downgradeReady {
		t.Errorf("success must NOT downgrade Ready; got %+v", v)
	}
}

// TestEvaluateFunctionalProbeHTTPErrorIsUnknown pins the fail-soft posture
// on transport errors: HTTP failure → FunctionalProbeOK=Unknown/ProbeError
// and Ready is LEFT ALONE. A brief server outage must not flap every
// backend Ready=False (the same noise mode the snapshot poller already
// avoids).
func TestEvaluateFunctionalProbeHTTPErrorIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal", http.StatusInternalServerError)
	}))
	defer srv.Close()
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}

	v := evaluateFunctionalProbe(context.Background(), newProbeBackend("cb"), kvReadinessTrue,
		client, &probeRateLimiter{}, time.Second, time.Now())

	if !v.shouldWriteCondition || v.condition.Status != metav1.ConditionUnknown || v.condition.Reason != reasonProbeError {
		t.Fatalf("want Unknown/ProbeError; got %+v", v.condition)
	}
	if v.downgradeReady {
		t.Errorf("HTTP error must NOT downgrade Ready; got %+v", v)
	}
}

// TestEvaluateFunctionalProbeHTTPErrorDoesNotRateLimit confirms the rate-
// limit only counts SUCCESSFUL probe calls: a failed call leaves the
// rate-limit slot unset so the next reconcile retries immediately, and a
// flapping server resolves quickly once it comes back.
func TestEvaluateFunctionalProbeHTTPErrorDoesNotRateLimit(t *testing.T) {
	fail := atomic.Bool{}
	fail.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cacheserver.ProbeResult{Backend: "x", Ingest: cacheserver.ProbeStageOK, Routing: cacheserver.ProbeStageOK, T2: cacheserver.ProbeStageSkipped})
	}))
	defer srv.Close()
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}
	limiter := &probeRateLimiter{}
	now := time.Unix(1_000_000, 0)

	v1 := evaluateFunctionalProbe(context.Background(), newProbeBackend("cb"), kvReadinessTrue, client, limiter, time.Minute, now)
	if v1.condition.Status != metav1.ConditionUnknown {
		t.Fatalf("expected Unknown on HTTP failure; got %+v", v1.condition)
	}

	fail.Store(false)
	v2 := evaluateFunctionalProbe(context.Background(), newProbeBackend("cb"), kvReadinessTrue, client, limiter, time.Minute, now.Add(time.Second))
	if v2.condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected True after server recovers, even within rate-limit window; got %+v", v2.condition)
	}
}

// TestEvaluateFunctionalProbeRequestShape pins the wire-contract:
// backend is namespace/name; model is the fixed sentinel; hashScheme is
// the lowercased engine value with vllm as the default; backendType is
// the spec.type string.
func TestEvaluateFunctionalProbeRequestShape(t *testing.T) {
	var seen cacheserver.ProbeRequest
	srv := newFakeProbeServer(t, func(req cacheserver.ProbeRequest) cacheserver.ProbeResult {
		seen = req
		return cacheserver.ProbeResult{Backend: req.Backend, Ingest: cacheserver.ProbeStageOK, Routing: cacheserver.ProbeStageOK, T2: cacheserver.ProbeStageSkipped}
	})
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}

	backend := newProbeBackend("cb-1")
	backend.Spec.Integration.Engine = "SGLang" // mixed-case → lowercased on the wire

	_ = evaluateFunctionalProbe(context.Background(), backend, kvReadinessTrue,
		client, &probeRateLimiter{}, time.Second, time.Now())

	if seen.Backend != "team-a/cb-1" {
		t.Errorf("Backend = %q, want team-a/cb-1", seen.Backend)
	}
	if seen.Model != probeFixedModel {
		t.Errorf("Model = %q, want %q", seen.Model, probeFixedModel)
	}
	if seen.HashScheme != "sglang" {
		t.Errorf("HashScheme = %q, want sglang (lower-cased)", seen.HashScheme)
	}
	if seen.BackendType != string(cachev1alpha1.CacheBackendTypeLMCache) {
		t.Errorf("BackendType = %q, want %q", seen.BackendType, cachev1alpha1.CacheBackendTypeLMCache)
	}
}

// TestProbeHashSchemeForBackendDefaults covers the explicit defaulting and
// trim logic on the engine field.
func TestProbeHashSchemeForBackendDefaults(t *testing.T) {
	cases := []struct {
		name   string
		engine *string
		want   string
	}{
		{"nil integration", nil, "vllm"},
		{"empty engine", stringPtr(""), "vllm"},
		{"whitespace engine", stringPtr("   "), "vllm"},
		{"vllm lowercase", stringPtr("vllm"), "vllm"},
		{"VLLM uppercase", stringPtr("VLLM"), "vllm"},
		{"sglang mixed", stringPtr("SGLang"), "sglang"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := newProbeBackend("cb")
			if tc.engine == nil {
				backend.Spec.Integration = nil
			} else {
				backend.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: *tc.engine}
			}
			if got := probeHashSchemeForBackend(backend); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func stringPtr(s string) *string { return &s }

// TestProbeResultMetricEmitsPerStage confirms the controller-runtime
// metric registration: every probe call increments three counters
// (ingest/routing/t2) with one observation each. The metric is
// PROCESS-WIDE (registered on the controller-runtime registry, not a
// per-Service one), so the test asserts a +1 delta against the per-label
// baseline rather than absolute counter values — running this test
// repeatedly in-process, or having another test reuse the same label
// tuple, must NOT cause a spurious failure.
func TestProbeResultMetricEmitsPerStage(t *testing.T) {
	const backendKey = "team-a/cb-metric"
	srv := newFakeProbeServer(t, func(cacheserver.ProbeRequest) cacheserver.ProbeResult {
		return cacheserver.ProbeResult{
			Backend: backendKey, Ingest: cacheserver.ProbeStageOK,
			Routing: cacheserver.ProbeStageOK, T2: cacheserver.ProbeStageSkipped,
		}
	})
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}

	// Snapshot per-label baselines BEFORE the call so the delta assertion
	// is robust to whatever prior state the counters hold (another test
	// in this package reusing the label, the same test run twice in
	// -count=2, etc.).
	type stageCheck struct {
		stage, result string
	}
	stages := []stageCheck{
		{cacheserver.ProbeStageIngest, "ok"},
		{cacheserver.ProbeStageRouting, "ok"},
		{cacheserver.ProbeStageT2, "skipped"},
	}
	before := make(map[stageCheck]float64, len(stages))
	for _, s := range stages {
		before[s] = metricCounter(t, backendKey, s.stage, s.result)
	}

	backend := newProbeBackend("cb-metric")
	_ = evaluateFunctionalProbe(context.Background(), backend, kvReadinessTrue, client, &probeRateLimiter{}, time.Second, time.Now())

	for _, s := range stages {
		got := metricCounter(t, backendKey, s.stage, s.result)
		if got != before[s]+1 {
			t.Errorf("metric stage=%s result=%s = %v, want %v (baseline %v + 1)", s.stage, s.result, got, before[s]+1, before[s])
		}
	}
}

// metricCounter reads the inferencecache_backend_probe_result_total counter for
// one label combination by collecting the metric directly. Avoids pulling
// in the prometheus/testutil dependency just for one assertion (the
// pkg/server tests use the same trick).
func metricCounter(t *testing.T, backend, stage, result string) float64 {
	t.Helper()
	m, err := probeResultMetric.GetMetricWithLabelValues(backend, stage, result)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	var dtoMetric dto.Metric
	if err := m.(prometheus.Counter).Write(&dtoMetric); err != nil {
		t.Fatalf("metric.Write: %v", err)
	}
	return dtoMetric.GetCounter().GetValue()
}

// TestEvaluateFunctionalProbeContextCancellation pins ctx propagation: a
// cancelled context surfaces as an HTTP error (Unknown), not a panic or a
// silent True.
func TestEvaluateFunctionalProbeContextCancellation(t *testing.T) {
	srv := newFakeProbeServer(t, func(cacheserver.ProbeRequest) cacheserver.ProbeResult {
		time.Sleep(50 * time.Millisecond)
		return cacheserver.ProbeResult{Ingest: cacheserver.ProbeStageOK, Routing: cacheserver.ProbeStageOK, T2: cacheserver.ProbeStageSkipped}
	})
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	v := evaluateFunctionalProbe(ctx, newProbeBackend("cb"), kvReadinessTrue,
		client, &probeRateLimiter{}, time.Second, time.Now())
	if v.condition.Status != metav1.ConditionUnknown {
		t.Fatalf("cancelled ctx should surface as Unknown; got %+v", v.condition)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("test setup: ctx should be cancelled")
	}
}
