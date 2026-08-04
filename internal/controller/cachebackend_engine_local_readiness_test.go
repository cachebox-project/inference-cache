package controller

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
)

func TestEvaluateEngineLocalReadiness(t *testing.T) {
	backend := engineLocalBackendFixture()
	current := func(name string) corev1.Pod {
		return engineLocalPodFixture(name, backend, backend.Generation, true)
	}

	terminating := current("terminating")
	now := metav1.NewTime(time.Now())
	terminating.DeletionTimestamp = &now
	failed := current("failed")
	failed.Status.Phase = corev1.PodFailed

	skipped := current("skipped")
	skipped.Annotations = map[string]string{
		podwebhook.AnnotationSkip:          "true",
		podwebhook.AnnotationInjectSkipped: podwebhook.InjectSkippedReasonSkipAnnotation,
	}

	missingUID := current("missing-uid")
	delete(missingUID.Annotations, podwebhook.AnnotationInjectedByUID)
	missingGeneration := current("missing-generation")
	delete(missingGeneration.Annotations, podwebhook.AnnotationInjectedGeneration)
	malformedGeneration := current("malformed-generation")
	malformedGeneration.Annotations[podwebhook.AnnotationInjectedGeneration] = "not-a-number"
	wrongOwner := current("wrong-owner")
	wrongOwner.Annotations[podwebhook.AnnotationInjectedBy] = backend.Namespace + "/other"
	futureGeneration := current("future-generation")
	futureGeneration.Annotations[podwebhook.AnnotationInjectedGeneration] =
		strconv.FormatInt(backend.Generation+1, 10)
	stale := engineLocalPodFixture("stale", backend, backend.Generation-1, true)
	unavailable := current("unavailable")
	unavailable.Status.Conditions[0].Status = corev1.ConditionFalse

	tests := []struct {
		name        string
		pods        []corev1.Pod
		ready       metav1.ConditionStatus
		readyReason string
		progressing metav1.ConditionStatus
		degraded    metav1.ConditionStatus
		messageHas  string
	}{
		{
			name:        "no matching pods waits",
			ready:       metav1.ConditionFalse,
			readyReason: reasonAwaitingEnginePods,
			progressing: metav1.ConditionTrue,
			degraded:    metav1.ConditionFalse,
		},
		{
			name:        "terminal and terminating pods are ignored",
			pods:        []corev1.Pod{terminating, failed},
			ready:       metav1.ConditionFalse,
			readyReason: reasonAwaitingEnginePods,
			progressing: metav1.ConditionTrue,
			degraded:    metav1.ConditionFalse,
		},
		{
			name:        "all skipped is ready",
			pods:        []corev1.Pod{skipped},
			ready:       metav1.ConditionTrue,
			readyReason: reasonAllEnginePodsSkipped,
			progressing: metav1.ConditionFalse,
			degraded:    metav1.ConditionFalse,
		},
		{
			name:        "skipped pod does not block a ready participant",
			pods:        []corev1.Pod{skipped, current("ready")},
			ready:       metav1.ConditionTrue,
			readyReason: reasonEnginePodsReady,
			progressing: metav1.ConditionFalse,
			degraded:    metav1.ConditionFalse,
			messageHas:  "1 additional matching Pods explicitly skipped",
		},
		{
			name:        "missing receipt degrades",
			pods:        []corev1.Pod{missingUID},
			ready:       metav1.ConditionFalse,
			readyReason: reasonEnginePodsNotInjected,
			progressing: metav1.ConditionFalse,
			degraded:    metav1.ConditionTrue,
			messageHas:  "missing-uid",
		},
		{
			name:        "malformed generation degrades as not injected",
			pods:        []corev1.Pod{malformedGeneration},
			ready:       metav1.ConditionFalse,
			readyReason: reasonEnginePodsNotInjected,
			progressing: metav1.ConditionFalse,
			degraded:    metav1.ConditionTrue,
			messageHas:  "malformed-generation",
		},
		{
			name:        "missing generation degrades as not injected",
			pods:        []corev1.Pod{missingGeneration},
			ready:       metav1.ConditionFalse,
			readyReason: reasonEnginePodsNotInjected,
			progressing: metav1.ConditionFalse,
			degraded:    metav1.ConditionTrue,
			messageHas:  "missing-generation",
		},
		{
			name:        "missing receipt takes precedence over mismatched receipt",
			pods:        []corev1.Pod{wrongOwner, missingUID},
			ready:       metav1.ConditionFalse,
			readyReason: reasonEnginePodsNotInjected,
			progressing: metav1.ConditionFalse,
			degraded:    metav1.ConditionTrue,
			messageHas:  "missing-uid",
		},
		{
			name:        "wrong owner degrades",
			pods:        []corev1.Pod{wrongOwner},
			ready:       metav1.ConditionFalse,
			readyReason: reasonEnginePodsInjectionMismatch,
			progressing: metav1.ConditionFalse,
			degraded:    metav1.ConditionTrue,
			messageHas:  "wrong-owner",
		},
		{
			name:        "future generation degrades",
			pods:        []corev1.Pod{futureGeneration},
			ready:       metav1.ConditionFalse,
			readyReason: reasonEnginePodsInjectionMismatch,
			progressing: metav1.ConditionFalse,
			degraded:    metav1.ConditionTrue,
			messageHas:  "future-generation",
		},
		{
			name:        "stale generation is progressing even while a current pod is unavailable",
			pods:        []corev1.Pod{stale, unavailable},
			ready:       metav1.ConditionFalse,
			readyReason: reasonEnginePodsRolloutInProgress,
			progressing: metav1.ConditionTrue,
			degraded:    metav1.ConditionFalse,
			messageHas:  "stale",
		},
		{
			name:        "current generation unavailable degrades",
			pods:        []corev1.Pod{unavailable},
			ready:       metav1.ConditionFalse,
			readyReason: reasonEnginePodsUnavailable,
			progressing: metav1.ConditionFalse,
			degraded:    metav1.ConditionTrue,
			messageHas:  "unavailable",
		},
		{
			name:        "all current generation pods ready",
			pods:        []corev1.Pod{current("b"), current("a")},
			ready:       metav1.ConditionTrue,
			readyReason: reasonEnginePodsReady,
			progressing: metav1.ConditionFalse,
			degraded:    metav1.ConditionFalse,
			messageHas:  "2/2 engine Pods",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := evaluateEngineLocalReadiness(backend, tc.pods)
			if got.readyStatus != tc.ready || got.readyReason != tc.readyReason {
				t.Fatalf("Ready = %s/%s, want %s/%s; verdict=%+v",
					got.readyStatus, got.readyReason, tc.ready, tc.readyReason, got)
			}
			if got.progressingStatus != tc.progressing {
				t.Fatalf("Progressing = %s/%s, want %s; verdict=%+v",
					got.progressingStatus, got.progressingReason, tc.progressing, got)
			}
			if got.degradedStatus != tc.degraded {
				t.Fatalf("Degraded = %s/%s, want %s; verdict=%+v",
					got.degradedStatus, got.degradedReason, tc.degraded, got)
			}
			if tc.messageHas != "" && !strings.Contains(got.readyMessage, tc.messageHas) {
				t.Fatalf("Ready message %q does not contain %q", got.readyMessage, tc.messageHas)
			}
		})
	}
}

func TestReconcileEngineLocalPreservesReadinessOnPodListError(t *testing.T) {
	scheme := newScheme(t)
	backend := engineLocalBackendFixture()
	backend.Generation = 2
	backend.Status.ObservedGeneration = 1
	meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             reasonEnginePodsReady,
		Message:            "last successful observation",
		ObservedGeneration: 1,
	})

	listErr := errors.New("synthetic engine Pod list failure")
	funcs := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return listErr
			}
			return c.List(ctx, list, opts...)
		},
	}
	r := newReconcilerWithInterceptor(scheme, funcs, backend)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: backend.Name, Namespace: backend.Namespace},
	})
	if err == nil || !strings.Contains(err.Error(), listErr.Error()) {
		t.Fatalf("Reconcile error = %v, want wrapped Pod list error", err)
	}

	got := getBackend(t, r, backend.Name, backend.Namespace)
	if got.Status.ObservedGeneration != 1 {
		t.Fatalf("observedGeneration = %d, want preserved 1", got.Status.ObservedGeneration)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, conditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionTrue ||
		ready.Reason != reasonEnginePodsReady ||
		ready.Message != "last successful observation" {
		t.Fatalf("Ready = %+v, want last successful observation preserved", ready)
	}
}

func engineLocalBackendFixture() *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "hicache",
			Namespace:  "engines",
			UID:        types.UID("hicache-uid"),
			Generation: 3,
		},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeSGLangHiCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine:   "sglang",
				Mode:     cachev1alpha1.CacheBackendIntegrationModeOffload,
				Role:     cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
				FailOpen: ptrBool(true),
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "sglang"},
			},
			HiCache: &cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"},
		},
	}
}

func engineLocalPodFixture(
	name string,
	backend *cachev1alpha1.CacheBackend,
	generation int64,
	ready bool,
) corev1.Pod {
	readyStatus := corev1.ConditionFalse
	if ready {
		readyStatus = corev1.ConditionTrue
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: backend.Namespace,
			Labels:    map[string]string{"app": "sglang"},
			Annotations: map[string]string{
				podwebhook.AnnotationInjectedBy:         backend.Namespace + "/" + backend.Name,
				podwebhook.AnnotationInjectedByUID:      string(backend.UID),
				podwebhook.AnnotationInjectedGeneration: strconv.FormatInt(generation, 10),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: readyStatus,
			}},
		},
	}
}
