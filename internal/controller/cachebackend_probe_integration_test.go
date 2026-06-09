package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// TestIntegrationFunctionalProbeGate exercises the controller-side probe
// gate against a real apiserver (envtest) and a real httptest.Server
// playing the /probe endpoint. It is the end-to-end seam between the
// reconciler's updateManagedStatus and the operator-visible
// FunctionalProbeOK condition — the same path the production binary
// drives once the controller actually calls /probe.
//
// Each sub-test stands up a fresh namespace + backend, drives it through
// the KV-event gate to Ready=True, then verifies the gate's verdict for
// one probe-response shape.
func TestIntegrationFunctionalProbeGate(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	ctx := context.Background()

	t.Run("happy path writes ProbeOK and keeps Ready=True", func(t *testing.T) {
		var calls atomic.Int64
		srv := newProbeIntegrationServer(t, func(req cacheserver.ProbeRequest) cacheserver.ProbeResult {
			calls.Add(1)
			return cacheserver.ProbeResult{
				Backend: req.Backend,
				Ingest:  cacheserver.ProbeStageOK,
				Routing: cacheserver.ProbeStageOK,
				T2:      cacheserver.ProbeStageSkipped,
			}
		})
		r := &CacheBackendReconciler{
			Client: k8s, Scheme: scheme, Log: logr.Discard(),
			ProbeClient:    &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()},
			ProbeRateLimit: time.Millisecond,
		}
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, gatedLMCacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		patchLastEventAt(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		cb := getBackend(t, r, "cache", ns)
		probe := findCondition(cb.Status.Conditions, conditionTypeFunctionalProbeOK)
		if probe == nil || probe.Status != metav1.ConditionTrue || probe.Reason != reasonProbeOK {
			t.Fatalf("FunctionalProbeOK = %+v, want True/ProbeOK", probe)
		}
		ready := findCondition(cb.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionTrue {
			t.Fatalf("Ready = %+v, want True (probe succeeded; KV gate said Ready)", ready)
		}
		if calls.Load() == 0 {
			t.Fatalf("expected at least 1 probe HTTP call, got 0")
		}
	})

	t.Run("ingest failure downgrades Ready with ProbeIngestFailed", func(t *testing.T) {
		srv := newProbeIntegrationServer(t, func(req cacheserver.ProbeRequest) cacheserver.ProbeResult {
			return cacheserver.ProbeResult{
				Backend: req.Backend,
				Ingest:  cacheserver.ProbeStageFailed,
				Routing: cacheserver.ProbeStageSkipped,
				T2:      cacheserver.ProbeStageSkipped,
				Errors:  []cacheserver.ProbeStageError{{Stage: cacheserver.ProbeStageIngest, Message: "synthesized event not in index"}},
			}
		})
		r := &CacheBackendReconciler{
			Client: k8s, Scheme: scheme, Log: logr.Discard(),
			ProbeClient:    &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()},
			ProbeRateLimit: time.Millisecond,
		}
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, gatedLMCacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		patchLastEventAt(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		cb := getBackend(t, r, "cache", ns)
		probe := findCondition(cb.Status.Conditions, conditionTypeFunctionalProbeOK)
		if probe == nil || probe.Status != metav1.ConditionFalse || probe.Reason != reasonProbeIngestFailed {
			t.Fatalf("FunctionalProbeOK = %+v, want False/ProbeIngestFailed", probe)
		}
		if !strings.Contains(probe.Message, "synthesized event not in index") {
			t.Errorf("FunctionalProbeOK message should carry the stage diagnostic; got %q", probe.Message)
		}
		ready := findCondition(cb.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reasonProbeIngestFailed {
			t.Fatalf("Ready = %+v, want False/ProbeIngestFailed (probe gate must downgrade Ready)", ready)
		}
	})

	t.Run("rate-limited second reconcile after failure preserves Ready downgrade", func(t *testing.T) {
		// Regression guard: the watch event triggered by the failure-status
		// patch causes an immediate second reconcile. With a tight rate-limit
		// (so the second call is suppressed) AND an existing
		// FunctionalProbeOK=False condition on the CR, the gate must
		// re-apply the Ready downgrade — otherwise the upstream KV gate's
		// Ready=True silently overwrites the failure verdict.
		var calls atomic.Int64
		srv := newProbeIntegrationServer(t, func(req cacheserver.ProbeRequest) cacheserver.ProbeResult {
			calls.Add(1)
			return cacheserver.ProbeResult{
				Backend: req.Backend, Ingest: cacheserver.ProbeStageFailed,
				Routing: cacheserver.ProbeStageSkipped, T2: cacheserver.ProbeStageSkipped,
				Errors: []cacheserver.ProbeStageError{{Stage: cacheserver.ProbeStageIngest, Message: "synthesized event not in index"}},
			}
		})
		r := &CacheBackendReconciler{
			Client: k8s, Scheme: scheme, Log: logr.Discard(),
			ProbeClient:    &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()},
			ProbeRateLimit: time.Hour, // wide enough to guarantee the second reconcile is rate-limited
		}
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, gatedLMCacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		patchLastEventAt(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns) // first probe call → FunctionalProbeOK=False, Ready=False
		reconcile(t, r, "cache", ns) // rate-limited second reconcile

		cb := getBackend(t, r, "cache", ns)
		probe := findCondition(cb.Status.Conditions, conditionTypeFunctionalProbeOK)
		if probe == nil || probe.Status != metav1.ConditionFalse || probe.Reason != reasonProbeIngestFailed {
			t.Fatalf("FunctionalProbeOK after rate-limited reconcile = %+v, want False/ProbeIngestFailed (preserved)", probe)
		}
		ready := findCondition(cb.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reasonProbeIngestFailed {
			t.Fatalf("Ready after rate-limited reconcile = %+v, want False/ProbeIngestFailed (downgrade re-applied)", ready)
		}
		if calls.Load() != 1 {
			t.Errorf("rate-limited reconcile must skip the HTTP call; got %d total calls", calls.Load())
		}
	})

	t.Run("annotation bypass keeps Ready=True without calling /probe", func(t *testing.T) {
		var calls atomic.Int64
		srv := newProbeIntegrationServer(t, func(cacheserver.ProbeRequest) cacheserver.ProbeResult {
			calls.Add(1)
			t.Fatalf("server reached despite bypass annotation")
			return cacheserver.ProbeResult{}
		})
		r := &CacheBackendReconciler{
			Client: k8s, Scheme: scheme, Log: logr.Discard(),
			ProbeClient:    &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()},
			ProbeRateLimit: time.Millisecond,
		}
		ns := freshNS(t, k8s)
		cb := gatedLMCacheBackend("cache", ns)
		cb.Annotations = map[string]string{annotationSkipFunctionalProbe: "true"}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		patchLastEventAt(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		got := getBackend(t, r, "cache", ns)
		probe := findCondition(got.Status.Conditions, conditionTypeFunctionalProbeOK)
		if probe == nil || probe.Status != metav1.ConditionTrue || probe.Reason != reasonProbeBypassed {
			t.Fatalf("FunctionalProbeOK = %+v, want True/ProbeBypassed", probe)
		}
		ready := findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionTrue {
			t.Fatalf("Ready = %+v, want True (bypass must not downgrade)", ready)
		}
		if calls.Load() != 0 {
			t.Fatalf("bypass annotation must skip the HTTP call; got %d probe calls", calls.Load())
		}
	})

	t.Run("nil ProbeClient leaves Ready/Progressing/Degraded unchanged", func(t *testing.T) {
		// Reconciler WITHOUT ProbeClient — production-equivalent of the
		// --server-probe-url="" code path. No FunctionalProbeOK condition
		// should appear; existing Ready/Progressing/Degraded should be
		// the only conditions written. Same backend setup as the happy
		// path; the only delta is ProbeClient=nil.
		r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard()}
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, gatedLMCacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		patchLastEventAt(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		cb := getBackend(t, r, "cache", ns)
		if probe := findCondition(cb.Status.Conditions, conditionTypeFunctionalProbeOK); probe != nil {
			t.Fatalf("nil ProbeClient must not write FunctionalProbeOK; got %+v", probe)
		}
		if ready := findCondition(cb.Status.Conditions, conditionTypeReady); ready == nil || ready.Status != metav1.ConditionTrue {
			t.Fatalf("Ready = %+v, want True (KV gate alone)", ready)
		}
	})

	t.Run("cleanup removes FunctionalProbeOK alongside the other conditions", func(t *testing.T) {
		srv := newProbeIntegrationServer(t, func(req cacheserver.ProbeRequest) cacheserver.ProbeResult {
			return cacheserver.ProbeResult{
				Backend: req.Backend,
				Ingest:  cacheserver.ProbeStageOK, Routing: cacheserver.ProbeStageOK, T2: cacheserver.ProbeStageSkipped,
			}
		})
		r := &CacheBackendReconciler{
			Client: k8s, Scheme: scheme, Log: logr.Discard(),
			ProbeClient:    &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()},
			ProbeRateLimit: time.Millisecond,
		}
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, gatedLMCacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		patchLastEventAt(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		// Confirm the condition was written.
		cb := getBackend(t, r, "cache", ns)
		if findCondition(cb.Status.Conditions, conditionTypeFunctionalProbeOK) == nil {
			t.Fatalf("FunctionalProbeOK should be present before cleanup")
		}

		// Flip to External so the reconciler runs reconcileExternal → which
		// invokes cleanupOwnedWorkload → the conditions-clear branch.
		before := cb.DeepCopy()
		cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
		cb.Spec.Endpoint = "lm://test.example.com:9999"
		if err := k8s.Patch(ctx, cb, client.MergeFrom(before)); err != nil {
			t.Fatalf("patch to External: %v", err)
		}
		reconcile(t, r, "cache", ns)

		got := &cachev1alpha1.CacheBackend{}
		if err := k8s.Get(ctx, types.NamespacedName{Name: "cache", Namespace: ns}, got); err != nil {
			t.Fatalf("get after switch: %v", err)
		}
		if probe := findCondition(got.Status.Conditions, conditionTypeFunctionalProbeOK); probe != nil {
			t.Fatalf("FunctionalProbeOK should be cleared by cleanupOwnedWorkload; got %+v", probe)
		}
	})
}

// newProbeIntegrationServer returns an httptest.Server that responds to
// POST /probe with whatever ProbeResult the callback produces, mirroring
// the server-side handler's JSON contract.
func newProbeIntegrationServer(t *testing.T, respond func(cacheserver.ProbeRequest) cacheserver.ProbeResult) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req cacheserver.ProbeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(respond(req))
	}))
	t.Cleanup(srv.Close)
	return srv
}
