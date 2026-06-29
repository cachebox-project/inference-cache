package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
)

// TestIntegrationEngineCompatibilityCondition exercises the advisory
// EngineCompatibility condition end-to-end through a real reconcile: an engine
// pod the backend injected a connector into that is stuck in CrashLoopBackOff
// (the hybrid-attention signature) surfaces False/EngineConnectorIncompatible,
// and the condition is absent when the injected engine is healthy. It also
// pins the advisory invariant: EngineCompatibility never drives Ready.
func TestIntegrationEngineCompatibilityCondition(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard()}
	ctx := context.Background()

	// mkBackend creates a managed LMCache backend with an engineSelector and
	// rolls its Deployment out so reconcile reaches updateManagedStatus.
	mkBackend := func(t *testing.T) string {
		t.Helper()
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackendWithSelector("cache", ns, matchedSelector)); err != nil {
			t.Fatalf("create backend: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)
		return ns
	}

	// createEnginePod creates a selector-matched pod stamped injected-by THIS
	// backend, with one container in the given waiting reason (set via the
	// status subresource — envtest has no kubelet to produce it). An empty
	// waitingReason marks the container Running.
	createEnginePod := func(t *testing.T, ns, podName, waitingReason string) {
		t.Helper()
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        podName,
				Namespace:   ns,
				Labels:      matchedSelector,
				Annotations: map[string]string{podwebhook.AnnotationInjectedBy: ns + "/cache"},
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

	t.Run("crashLoopingInjectedEngineSurfacesIncompatible", func(t *testing.T) {
		ns := mkBackend(t)
		createEnginePod(t, ns, "engine-0", crashLoopBackOffReason)
		reconcile(t, r, "cache", ns)

		cb := getBackend(t, r, "cache", ns)
		ec := findCondition(cb.Status.Conditions, conditionTypeEngineCompatibility)
		if ec == nil || ec.Status != metav1.ConditionFalse || ec.Reason != reasonEngineConnectorIncompatible {
			t.Fatalf("EngineCompatibility = %+v, want False/EngineConnectorIncompatible", ec)
		}
		// Advisory: it must not BECOME the Ready reason. Ready is driven by the
		// managed Deployment + KV-event gate, not engine-pod compatibility.
		if ready := findCondition(cb.Status.Conditions, conditionTypeReady); ready != nil && ready.Reason == reasonEngineConnectorIncompatible {
			t.Fatalf("EngineCompatibility must not drive Ready, got Ready reason %s", ready.Reason)
		}
	})

	t.Run("healthyInjectedEngineHasNoCondition", func(t *testing.T) {
		ns := mkBackend(t)
		createEnginePod(t, ns, "engine-0", "") // Running
		reconcile(t, r, "cache", ns)
		if ec := findCondition(getBackend(t, r, "cache", ns).Status.Conditions, conditionTypeEngineCompatibility); ec != nil {
			t.Fatalf("EngineCompatibility = %+v, want absent for a healthy engine", ec)
		}
	})
}
