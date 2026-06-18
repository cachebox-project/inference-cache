package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

// TestIntegrationEngineKernelHealthGate exercises the EngineKernelsHealthy
// condition and the strict-mode Ready downgrade end-to-end against a real
// apiserver (envtest). It reuses the exact harness from
// TestIntegrationFunctionalProbeGate and TestIntegrationKVEventReadinessGate:
// skipWithoutEnvtest / startEnv / freshNS / gatedLMCacheBackend /
// setDeploymentHTTPReady / patchLastEventAt / reconcile / getBackend /
// findCondition.
//
// Two scenarios:
//  1. Report-only FAIL — EngineKernelsHealthy=False/KernelLoadFailed is
//     written, but Ready stays True (fail-open; the kernel gate never downgrades
//     Ready unless the annotation is "strict").
//  2. Strict FAIL — Ready is downgraded to False/EngineKernelDegraded alongside
//     EngineKernelsHealthy=False/KernelLoadFailed.
func TestIntegrationEngineKernelHealthGate(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	ctx := context.Background()

	// createEnginePodsWithKernelFail creates a matched engine pod whose
	// lmcache-kernel-check init-container has terminated with a FAIL message,
	// simulating a CUDA kernel mismatch. The pod's labels match the
	// engineSelector set on the CacheBackend by kernelCheckBackend.
	createEnginePodsWithKernelFail := func(t *testing.T, ns string, cb *cachev1alpha1.CacheBackend, strict bool) {
		t.Helper()
		sel := kernelCheckEngineLabels()
		// The pod's admitted mode is recorded on its kernel-check init container
		// env — that, not the CR annotation, is what the gate reads to decide a
		// strict Ready downgrade.
		var initEnv []corev1.EnvVar
		if strict {
			initEnv = []corev1.EnvVar{{Name: adapterruntime.EnvKernelCheckStrict, Value: "1"}}
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "engine-pod-0",
				Namespace: ns,
				Labels:    sel,
				// Stamp the webhook's injected-by pair so the kernel gate's
				// ownership filter attributes this pod to THIS backend.
				Annotations: map[string]string{
					podwebhook.AnnotationInjectedBy:    ns + "/" + cb.Name,
					podwebhook.AnnotationInjectedByUID: string(cb.UID),
				},
			},
			Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{{
					Name:  adapterruntime.LMCacheKernelCheckContainerName,
					Image: "registry.example.com/lmcache-kernel-check:test",
					Env:   initEnv,
				}},
				Containers: []corev1.Container{{
					Name:  "vllm",
					Image: "registry.example.com/vllm:test",
				}},
			},
		}
		if err := k8s.Create(ctx, pod); err != nil {
			t.Fatalf("create engine pod: %v", err)
		}
		// Set the init-container status to Terminated with a FAIL message via
		// the status subresource — the exact path the kubelet uses.
		podKey := types.NamespacedName{Name: pod.Name, Namespace: ns}
		var livePod corev1.Pod
		if err := k8s.Get(ctx, podKey, &livePod); err != nil {
			t.Fatalf("get pod for status update: %v", err)
		}
		before := livePod.DeepCopy()
		livePod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
			Name: adapterruntime.LMCacheKernelCheckContainerName,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 1,
					Message:  adapterruntime.KernelCheckMsgFailPrefix + " lmcache c_ops CUDA kernel version mismatch",
				},
			},
		}}
		if err := k8s.Status().Patch(ctx, &livePod, client.MergeFrom(before)); err != nil {
			t.Fatalf("patch pod init-container status: %v", err)
		}
	}

	t.Run("report-only FAIL writes EngineKernelsHealthy=False but keeps Ready=True", func(t *testing.T) {
		r := &CacheBackendReconciler{
			Client: k8s, Scheme: scheme, Log: logr.Discard(),
			// APIReader left nil: falls back to r.Client (acceptable in tests
			// without a real uncached reader). Matches the probe integration test
			// pattern.
		}
		ns := freshNS(t, k8s)

		// Use a backend WITHOUT the strict annotation — default is report-only
		// (fail-open): the kernel gate surfaces the condition but must not
		// downgrade Ready.
		cb := kernelCheckBackend("cache", ns, "" /* no strict annotation */)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}

		// First reconcile creates the Deployment.
		reconcile(t, r, "cache", ns)
		// Drive the backend to Ready=True via the KV-event gate.
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		patchLastEventAt(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		// Confirm Ready=True before injecting the failure.
		cb0 := getBackend(t, r, "cache", ns)
		if ready := findCondition(cb0.Status.Conditions, conditionTypeReady); ready == nil || ready.Status != metav1.ConditionTrue {
			t.Fatalf("pre-condition: Ready = %+v, want True before injecting kernel failure", ready)
		}

		// Inject a matched engine pod with a FAIL kernel-check status, admitted
		// in report-only mode (no strict env).
		createEnginePodsWithKernelFail(t, ns, cb, false)

		// Reconcile: the kernel gate reads the pod's init-container status.
		reconcile(t, r, "cache", ns)

		got := getBackend(t, r, "cache", ns)

		// EngineKernelsHealthy=False/KernelLoadFailed must be written.
		kc := findCondition(got.Status.Conditions, conditionTypeEngineKernelsHealthy)
		if kc == nil {
			t.Fatalf("EngineKernelsHealthy condition missing; conditions = %+v", got.Status.Conditions)
		}
		if kc.Status != metav1.ConditionFalse {
			t.Errorf("EngineKernelsHealthy.Status = %q, want False", kc.Status)
		}
		if kc.Reason != reasonKernelLoadFailed {
			t.Errorf("EngineKernelsHealthy.Reason = %q, want %q", kc.Reason, reasonKernelLoadFailed)
		}

		// Ready must STAY True — report-only never downgrades.
		ready := findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionTrue {
			t.Fatalf("Ready = %+v, want True (report-only mode must not downgrade Ready)", ready)
		}
	})

	t.Run("strict FAIL downgrades Ready=False/EngineKernelDegraded", func(t *testing.T) {
		r := &CacheBackendReconciler{
			Client: k8s, Scheme: scheme, Log: logr.Discard(),
		}
		ns := freshNS(t, k8s)

		// Use a backend WITH the strict annotation: kernel mismatch must
		// downgrade Ready to False.
		cb := kernelCheckBackend("cache", ns, adapterruntime.KernelCheckModeStrict)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}

		// DEPLOY-TIME path: the cache-server Deployment is Available, but the
		// engine pod fails its strict kernel check before any KV event, so the
		// upstream KV-event gate has NOT yet been satisfied — deliberately do
		// NOT patchLastEventAt. Without the strict override, Ready would be
		// pinned to AwaitingFirstKVEvent and mask the root cause.
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		cb0 := getBackend(t, r, "cache", ns)
		if ready := findCondition(cb0.Status.Conditions, conditionTypeReady); ready == nil ||
			ready.Status != metav1.ConditionFalse || ready.Reason != reasonAwaitingFirstKVEvent {
			t.Fatalf("pre-condition: Ready = %+v, want False/%s before injecting kernel failure", ready, reasonAwaitingFirstKVEvent)
		}

		// Inject matching pod with FAIL status, admitted in STRICT mode (its
		// init-container env carries KERNEL_CHECK_STRICT=1 — pod truth, what the
		// gate reads to downgrade Ready).
		createEnginePodsWithKernelFail(t, ns, cb, true)
		reconcile(t, r, "cache", ns)

		got := getBackend(t, r, "cache", ns)

		// EngineKernelsHealthy=False/KernelLoadFailed.
		kc := findCondition(got.Status.Conditions, conditionTypeEngineKernelsHealthy)
		if kc == nil {
			t.Fatalf("EngineKernelsHealthy condition missing; conditions = %+v", got.Status.Conditions)
		}
		if kc.Status != metav1.ConditionFalse {
			t.Errorf("EngineKernelsHealthy.Status = %q, want False", kc.Status)
		}
		if kc.Reason != reasonKernelLoadFailed {
			t.Errorf("EngineKernelsHealthy.Reason = %q, want %q", kc.Reason, reasonKernelLoadFailed)
		}

		// Ready must be downgraded to False with EngineKernelDegraded.
		ready := findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil {
			t.Fatalf("Ready condition missing after strict kernel failure; conditions = %+v", got.Status.Conditions)
		}
		if ready.Status != metav1.ConditionFalse {
			t.Errorf("Ready.Status = %q, want False (strict mode must downgrade Ready)", ready.Status)
		}
		if ready.Reason != reasonEngineKernelDegraded {
			t.Errorf("Ready.Reason = %q, want %q", ready.Reason, reasonEngineKernelDegraded)
		}
	})
}

// kernelCheckEngineLabels returns the MatchLabels used by kernelCheckBackend's
// engineSelector so test helpers that create matching engine pods use the same
// labels.
func kernelCheckEngineLabels() map[string]string {
	return map[string]string{"app": "test-engine-kernel"}
}

// kernelCheckBackend builds a managed LMCache CacheBackend with a non-nil
// EngineSelector so the kernel-check gate has matched pods to inspect. The
// KV-event gate (gatedLMCacheBackend) is inherited. kernelCheckMode is set as
// the AnnotationLMCacheKernelCheck annotation value; pass "" to leave the
// annotation absent (report-only default).
func kernelCheckBackend(name, ns, kernelCheckMode string) *cachev1alpha1.CacheBackend {
	cb := gatedLMCacheBackend(name, ns)
	cb.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{
		MatchLabels: kernelCheckEngineLabels(),
	}
	if kernelCheckMode != "" {
		if cb.Annotations == nil {
			cb.Annotations = map[string]string{}
		}
		cb.Annotations[adapterruntime.AnnotationLMCacheKernelCheck] = kernelCheckMode
	}
	return cb
}
