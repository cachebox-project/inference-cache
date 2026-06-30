package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
)

// TestIntegrationEngineCompatibilityCondition exercises the advisory
// EngineCompatibility condition end-to-end through a real reconcile: an engine
// pod the backend injected a connector into that is stuck in CrashLoopBackOff
// (the hybrid-attention signature) surfaces False/InjectedEngineCrashLooping
// plus a Warning event; the condition is absent when the engine is healthy or
// the injected-by-uid does not match; and it never drives Ready.
func TestIntegrationEngineCompatibilityCondition(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	rec := events.NewFakeRecorder(256)
	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard(), Recorder: rec}
	ctx := context.Background()

	// mkBackend creates a managed LMCache backend with an engineSelector, rolls
	// its Deployment out so reconcile reaches updateManagedStatus, and returns
	// the namespace + the apiserver-assigned CacheBackend UID.
	mkBackend := func(t *testing.T) (string, string) {
		t.Helper()
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackendWithSelector("cache", ns, matchedSelector)); err != nil {
			t.Fatalf("create backend: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)
		var cb cachev1alpha1.CacheBackend
		if err := k8s.Get(ctx, types.NamespacedName{Name: "cache", Namespace: ns}, &cb); err != nil {
			t.Fatalf("get backend: %v", err)
		}
		return ns, string(cb.UID)
	}

	// createEnginePod creates a pod stamped injected-by + injected-by-uid for
	// THIS backend, with one container in the given waiting reason (set via the
	// status subresource — envtest has no kubelet). Empty reason marks it Running.
	createEnginePod := func(t *testing.T, ns, uid, podName, waitingReason string) {
		t.Helper()
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: ns,
				Labels:    matchedSelector,
				Annotations: map[string]string{
					podwebhook.AnnotationInjectedBy:    ns + "/cache",
					podwebhook.AnnotationInjectedByUID: uid,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "vllm", Image: "registry.example.com/vllm:test"}}},
		}
		if err := k8s.Create(ctx, p); err != nil {
			t.Fatalf("create engine pod: %v", err)
		}
		before := p.DeepCopy()
		st := corev1.ContainerStatus{Name: "vllm"}
		if waitingReason != "" {
			st.State.Waiting = &corev1.ContainerStateWaiting{Reason: waitingReason}
		} else {
			st.State.Running = &corev1.ContainerStateRunning{}
		}
		p.Status.ContainerStatuses = []corev1.ContainerStatus{st}
		if err := k8s.Status().Patch(ctx, p, client.MergeFrom(before)); err != nil {
			t.Fatalf("patch engine pod status: %v", err)
		}
	}

	t.Run("crashLoopingInjectedEngineSurfacesIncompatibleAndEvents", func(t *testing.T) {
		ns, uid := mkBackend(t)
		_ = drainEvents(rec) // clear setup events
		createEnginePod(t, ns, uid, "engine-0", crashLoopBackOffReason)
		reconcile(t, r, "cache", ns)

		cb := getBackend(t, r, "cache", ns)
		ec := findCondition(cb.Status.Conditions, conditionTypeEngineCompatibility)
		if ec == nil || ec.Status != metav1.ConditionFalse || ec.Reason != reasonInjectedEngineCrashLooping {
			t.Fatalf("EngineCompatibility = %+v, want False/InjectedEngineCrashLooping", ec)
		}
		// Advisory: it must not become the Ready reason. Ready is driven by the
		// managed Deployment + KV-event gate, not engine-pod compatibility.
		if ready := findCondition(cb.Status.Conditions, conditionTypeReady); ready != nil && ready.Reason == reasonInjectedEngineCrashLooping {
			t.Fatalf("EngineCompatibility must not drive Ready, got Ready reason %s", ready.Reason)
		}
		expectEvent(t, drainEvents(rec), reasonInjectedEngineCrashLooping)
	})

	t.Run("recoveredInjectedEngineClearsCondition", func(t *testing.T) {
		ns, uid := mkBackend(t)
		createEnginePod(t, ns, uid, "engine-0", crashLoopBackOffReason)
		reconcile(t, r, "cache", ns)
		if ec := findCondition(getBackend(t, r, "cache", ns).Status.Conditions, conditionTypeEngineCompatibility); ec == nil ||
			ec.Status != metav1.ConditionFalse || ec.Reason != reasonInjectedEngineCrashLooping {
			t.Fatalf("EngineCompatibility = %+v, want False/InjectedEngineCrashLooping before recovery", ec)
		}

		// The engine container recovers (CrashLoopBackOff → Running). Patch the
		// pod's container status via the status subresource (envtest has no
		// kubelet) and reconcile: the managed path must REMOVE the condition.
		var p corev1.Pod
		if err := k8s.Get(ctx, types.NamespacedName{Name: "engine-0", Namespace: ns}, &p); err != nil {
			t.Fatalf("get engine pod: %v", err)
		}
		before := p.DeepCopy()
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  "vllm",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}
		if err := k8s.Status().Patch(ctx, &p, client.MergeFrom(before)); err != nil {
			t.Fatalf("patch engine pod status to Running: %v", err)
		}
		reconcile(t, r, "cache", ns)

		if ec := findCondition(getBackend(t, r, "cache", ns).Status.Conditions, conditionTypeEngineCompatibility); ec != nil {
			t.Fatalf("EngineCompatibility = %+v after the engine recovered; want absent (False→removed on observed-healthy)", ec)
		}
	})

	t.Run("healthyInjectedEngineHasNoCondition", func(t *testing.T) {
		ns, uid := mkBackend(t)
		createEnginePod(t, ns, uid, "engine-0", "") // Running
		reconcile(t, r, "cache", ns)
		if ec := findCondition(getBackend(t, r, "cache", ns).Status.Conditions, conditionTypeEngineCompatibility); ec != nil {
			t.Fatalf("EngineCompatibility = %+v, want absent for a healthy engine", ec)
		}
	})

	t.Run("uidMismatchedInjectedPodIsIgnored", func(t *testing.T) {
		ns, _ := mkBackend(t)
		createEnginePod(t, ns, "stale-uid", "engine-0", crashLoopBackOffReason)
		reconcile(t, r, "cache", ns)
		if ec := findCondition(getBackend(t, r, "cache", ns).Status.Conditions, conditionTypeEngineCompatibility); ec != nil {
			t.Fatalf("EngineCompatibility = %+v, want absent (UID mismatch must be ignored)", ec)
		}
	})
}
