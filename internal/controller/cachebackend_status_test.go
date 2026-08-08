// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sync/atomic"
	"testing"
	"time"
)

func TestReconcileLMCacheReadyWhenReplicasAvailable(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.Replicas = 1
	dep.Status.UpdatedReplicas = 1
	dep.Status.AvailableReplicas = 1
	dep.Status.ReadyReplicas = 1
	if err := r.Status().Update(context.Background(), dep); err != nil {
		t.Fatalf("update deployment status: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %+v, want True", cond)
	}
}

func TestManagedReadinessGatesReadyOnRollout(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(2)

	cases := []struct {
		name       string
		dep        appsv1.Deployment
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "fresh create, nothing ready",
			dep:        appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Generation: 1}},
			wantStatus: metav1.ConditionFalse,
			wantReason: conditionReasonRolloutInProgress,
		},
		{
			name: "stale rollout after image change (old pods still available)",
			dep: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
				Status:     appsv1.DeploymentStatus{ObservedGeneration: 1, UpdatedReplicas: 0, AvailableReplicas: 2, ReadyReplicas: 2},
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: conditionReasonRolloutInProgress,
		},
		{
			name: "rolled out and available",
			dep: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
				Status:     appsv1.DeploymentStatus{ObservedGeneration: 2, UpdatedReplicas: 2, AvailableReplicas: 2, ReadyReplicas: 2},
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: conditionReasonBackendReady,
		},
		{
			name: "rolled out but replicas unavailable",
			dep: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
				Status:     appsv1.DeploymentStatus{ObservedGeneration: 2, UpdatedReplicas: 2, AvailableReplicas: 1, ReadyReplicas: 1},
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: conditionReasonReplicasUnavailable,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, reason, _ := managedReadiness(cb, &tc.dep)
			if status != tc.wantStatus || reason != tc.wantReason {
				t.Fatalf("managedReadiness = %v/%q, want %v/%q", status, reason, tc.wantStatus, tc.wantReason)
			}
		})
	}
}

func TestManagedReadinessZeroReplicasNotReady(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(0)
	// Even a fully-observed Deployment with 0/0 replicas must not be Ready.
	dep := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 1},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 1},
	}
	if status, reason, _ := managedReadiness(cb, &dep); status == metav1.ConditionTrue {
		t.Fatalf("managedReadiness for 0 replicas = %v/%q, want non-True", status, reason)
	}
}

// TestReconcileLifecycleExitsClearProbeRateLimiter pins the cleanup hook
// every lifecycle-exit path must call — without it, a CR that returns to
// the managed path within the prior 30s window keeps a stale lastCalled
// timestamp on r.probeLimiter, the very first reconcile on re-entry skips
// the /probe call entirely (rate-limited), and Ready=True is published
// with no fresh FunctionalProbeOK verdict to back it. One table-driven
// test for the places that must call probeLimiter.forget:
// reconcileExternal, reconcileUnmanaged.
func TestReconcileLifecycleExitsClearProbeRateLimiter(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*cachev1alpha1.CacheBackend)
	}{
		{
			name: "managed → External",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
				cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
				cb.Spec.RemoteStorage = externalLMCacheStorage("external.ns1.svc:8080")
			},
		},
		{
			name: "managed → Unmanaged (StatefulSet kind)",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newScheme(t)
			r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))
			reconcile(t, r, "cache", "ns1")

			// Plant a rate-limit entry so the clearing assertion is not
			// vacuous on an empty map. The key matches what
			// evaluateFunctionalProbe + the lifecycle exits use:
			// "namespace/name".
			key := "ns1/cache"
			now := time.Unix(2_000_000, 0)
			r.probeLimiter.markCalled(key, now)
			if got := r.probeLimiter.lastCalled(key); got != now {
				t.Fatalf("planted rate-limit precondition failed: lastCalled = %v, want %v (test would be vacuous without a planted value)", got, now)
			}

			// Trigger the lifecycle exit.
			switching := getBackend(t, r, "cache", "ns1")
			tc.mutate(switching)
			if err := r.Update(context.Background(), switching); err != nil {
				t.Fatalf("apply lifecycle-exit spec change: %v", err)
			}
			reconcile(t, r, "cache", "ns1")

			// The rate-limit entry MUST be cleared. A retained entry would
			// suppress the first /probe call on the managed → exit → managed
			// return path inside the 30s window.
			if got := r.probeLimiter.lastCalled(key); !got.IsZero() {
				t.Fatalf("probeLimiter.lastCalled(%q) = %v after lifecycle exit; want zero (forget) — re-entry inside the 30s window would skip the first probe call", key, got)
			}
		})
	}
}

// TestReconcileLifecycleExitsClearEngineCompatibility pins that every managed →
// non-managed lifecycle exit clears the managed-only EngineCompatibility
// advisory. Without it, a backend that surfaced an injected-engine crash-loop
// warning and is then flipped to External or an unmanaged kind would keep
// advertising a stale incompatibility no path re-evaluates — the same staleness
// the sibling T2Degraded clear guards against.
func TestReconcileLifecycleExitsClearEngineCompatibility(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*cachev1alpha1.CacheBackend)
	}{
		{
			name: "managed → External",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
				cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
				cb.Spec.RemoteStorage = externalLMCacheStorage("external.ns1.svc:8080")
			},
		},
		{
			name: "managed → Unmanaged (StatefulSet kind)",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newScheme(t)
			r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))
			reconcile(t, r, "cache", "ns1")

			// Plant a False/InjectedEngineCrashLooping condition as if a prior
			// managed reconcile had observed an injected engine crash-looping,
			// so the clearing assertion below is not vacuous.
			live := getBackend(t, r, "cache", "ns1")
			live.Status.Conditions = append(live.Status.Conditions, metav1.Condition{
				Type:               conditionTypeEngineCompatibility,
				Status:             metav1.ConditionFalse,
				Reason:             reasonInjectedEngineCrashLooping,
				Message:            "planted",
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: live.Generation,
			})
			if err := r.Status().Update(context.Background(), live); err != nil {
				t.Fatalf("plant EngineCompatibility precondition: %v", err)
			}
			if c := findCondition(getBackend(t, r, "cache", "ns1").Status.Conditions, conditionTypeEngineCompatibility); c == nil {
				t.Fatalf("planted EngineCompatibility precondition failed (test would be vacuous)")
			}

			// Trigger the lifecycle exit.
			switching := getBackend(t, r, "cache", "ns1")
			tc.mutate(switching)
			if err := r.Update(context.Background(), switching); err != nil {
				t.Fatalf("apply lifecycle-exit spec change: %v", err)
			}
			reconcile(t, r, "cache", "ns1")

			if c := findCondition(getBackend(t, r, "cache", "ns1").Status.Conditions, conditionTypeEngineCompatibility); c != nil {
				t.Fatalf("EngineCompatibility = %+v after %s; want cleared (a stale managed-only advisory would linger on a CR no managed path re-evaluates)", c, tc.name)
			}
		})
	}
}

// TestUpdateManagedStatusPreservesEngineCompatibilityOnListError pins the
// preserve-on-list-error contract: when detectEngineConnectorCrashLoop cannot
// observe the engine pods (pod List fails → observed=false), updateManagedStatus
// must LEAVE any existing EngineCompatibility condition in place rather than
// clear it. Clearing on a transient list failure and re-asserting on the next
// success would flap the Warning event and briefly hide an active
// incompatibility. The managed reconcile still completes (the list error is
// soft on every path that reads pods), so the condition's survival is the whole
// assertion.
func TestUpdateManagedStatusPreservesEngineCompatibilityOnListError(t *testing.T) {
	scheme := newScheme(t)
	// No engineSelector → the matchedEnginePods refresh does not list pods;
	// the detector's namespace-wide PodList is the one the interceptor below
	// fails, and the server-instance cascade soft-fails on the same error.
	listErr := errors.New("synthetic pod-list failure")
	funcs := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return listErr
			}
			return c.List(ctx, list, opts...)
		},
	}
	r := newReconcilerWithInterceptor(scheme, funcs, lmcacheBackend("cache", "ns1"))

	// First reconcile creates the managed Deployment/Service and publishes the
	// baseline status; the interceptor only fails pod Lists, which are soft.
	reconcile(t, r, "cache", "ns1")

	// Plant a False/InjectedEngineCrashLooping condition as if a prior reconcile
	// (with the pods observable) had surfaced an injected engine crash-loop.
	live := getBackend(t, r, "cache", "ns1")
	live.Status.Conditions = append(live.Status.Conditions, metav1.Condition{
		Type:               conditionTypeEngineCompatibility,
		Status:             metav1.ConditionFalse,
		Reason:             reasonInjectedEngineCrashLooping,
		Message:            "planted",
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: live.Generation,
	})
	if err := r.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("plant EngineCompatibility precondition: %v", err)
	}
	if c := findCondition(getBackend(t, r, "cache", "ns1").Status.Conditions, conditionTypeEngineCompatibility); c == nil {
		t.Fatalf("planted EngineCompatibility precondition failed (test would be vacuous)")
	}

	// Reconcile again with the pod List still failing: detectEngineConnectorCrashLoop
	// returns ("", false), so updateManagedStatus must skip both the set and the
	// clear and leave the planted condition untouched.
	reconcile(t, r, "cache", "ns1")

	c := findCondition(getBackend(t, r, "cache", "ns1").Status.Conditions, conditionTypeEngineCompatibility)
	if c == nil {
		t.Fatalf("EngineCompatibility was cleared on a transient pod-list failure; want preserved (observed=false must not clear)")
	}
	if c.Status != metav1.ConditionFalse || c.Reason != reasonInjectedEngineCrashLooping {
		t.Fatalf("EngineCompatibility = %s/%s after list error; want False/InjectedEngineCrashLooping (preserved unchanged)", c.Status, c.Reason)
	}
}

// TestReconcileLMCacheStatusIndependentOfApplyError pins the other half of the
// fix: status is derived from the live Deployment, not gated on apply success.
// Even when apply fails (here, a Forbidden as if an admission webhook rejected
// the write), the CR must still publish what the Deployment reports instead
// of remaining frozen at a stale snapshot.
//
// At the same time, Status.ObservedGeneration must NOT advance to the new CR
// generation when apply for that generation failed — otherwise clients can't
// tell from the status whether the controller has caught up.
func TestReconcileLMCacheStatusIndependentOfApplyError(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)

	var blockDeploymentUpdate atomic.Bool
	gr := schema.GroupResource{Group: "apps", Resource: "deployments"}
	funcs := interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok && blockDeploymentUpdate.Load() {
				return apierrors.NewForbidden(gr, obj.GetName(), errors.New("denied by admission webhook"))
			}
			return c.Update(ctx, obj, opts...)
		},
	}
	r := newReconcilerWithInterceptor(scheme, funcs, cb)

	reconcile(t, r, "cache", "ns1")
	markDeploymentReady(t, r, "cache", "ns1", 1)
	// Sanity: after a successful first reconcile, observedGeneration tracks
	// the CR generation (1).
	if got := getBackend(t, r, "cache", "ns1").Status.ObservedGeneration; got != 1 {
		t.Fatalf("status.observedGeneration after successful first reconcile = %d, want 1", got)
	}

	// Now flip the gate so the next applyDeployment Update is rejected. Force
	// an Update to happen by changing the image in the CR.
	blockDeploymentUpdate.Store(true)
	live := getBackend(t, r, "cache", "ns1")
	live.Spec.RemoteStorage.LMCacheServer.Image = "example.com/lmcache-server:v9"
	live.Generation = 2
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update CR: %v", err)
	}

	// Reconcile must surface an error (so controller-runtime requeues) but the
	// status pass must still publish observed state from the live Deployment.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	}); err == nil {
		t.Fatalf("reconcile returned nil, want error (apply was blocked)")
	}

	updated := getBackend(t, r, "cache", "ns1")
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %+v, want True (status must reflect live Deployment regardless of apply error)", cond)
	}
	// Apply for generation 2 failed, so observedGeneration must NOT have
	// advanced to 2 — it should still report 1 (the last generation we
	// successfully drove the live state toward). Same for the Ready
	// condition's ObservedGeneration, so the (status, condition) pair stays
	// internally consistent.
	if got := updated.Status.ObservedGeneration; got != 1 {
		t.Fatalf("status.observedGeneration = %d, want 1 (apply failed; must not advance to current CR gen)", got)
	}
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond == nil || cond.ObservedGeneration != 1 {
		t.Fatalf("Ready condition ObservedGeneration = %d, want 1", cond.ObservedGeneration)
	}
	if got := getDeployment(t, r, "cache", "ns1").Spec.Template.Spec.Containers[0].Image; got == "example.com/lmcache-server:v9" {
		t.Fatalf("deployment image was updated despite Forbidden — interceptor was not exercised")
	}
}
