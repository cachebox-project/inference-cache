package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestIntegrationCacheBackendResources exercises spec.resources end-to-end
// against a real apiserver: the kubebuilder default stamps memory limits
// on every admitted CacheBackend (so the lmcache-server pod is bounded by
// the cgroup rather than OOM-killed under T2 load), and an operator-
// supplied spec.resources is threaded verbatim into the rendered
// Deployment's container.
//
// The unit-level renderer test
// (TestVLLMLMCacheResolveCacheServerHonorsSpecResources) constructs
// CacheBackend objects directly and therefore does NOT see the apiserver
// stamping kubebuilder defaults — the default-marker behavior must be
// exercised here, where envtest's apiserver applies the marker before the
// reconciler reads the spec back.
func TestIntegrationCacheBackendResources(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard()}
	ctx := context.Background()

	t.Run("DefaultStampsMemoryLimitsOnMinimalCacheBackend", func(t *testing.T) {
		// A minimal CacheBackend with no spec.resources arrives at the
		// reconciler with the CRD-schema default already stamped by the
		// apiserver. The rendered lmcache-server container therefore
		// MUST carry memory requests + limits — the contract that
		// stops the OOM-kill cliff under heavy T2 write load.
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackend("cache", ns)); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		reconcile(t, r, "cache", ns)

		// Re-read the persisted CacheBackend so we observe the
		// apiserver-stamped default rather than the in-memory copy
		// we created.
		cb := getBackend(t, r, "cache", ns)
		if cb.Spec.Resources == nil {
			t.Fatalf("spec.resources nil after apiserver defaulting; kubebuilder default marker not applied")
		}
		wantReq := resource.MustParse("4Gi")
		if got := cb.Spec.Resources.Requests[corev1.ResourceMemory]; got.Cmp(wantReq) != 0 {
			t.Fatalf("spec.resources.requests[memory] = %v, want %v", got.String(), wantReq.String())
		}
		wantLim := resource.MustParse("8Gi")
		if got := cb.Spec.Resources.Limits[corev1.ResourceMemory]; got.Cmp(wantLim) != 0 {
			t.Fatalf("spec.resources.limits[memory] = %v, want %v", got.String(), wantLim.String())
		}

		dep := getDeployment(t, r, "cache", ns)
		container := dep.Spec.Template.Spec.Containers[0]
		if got := container.Resources.Requests[corev1.ResourceMemory]; got.Cmp(wantReq) != 0 {
			t.Fatalf("container Requests[memory] = %v, want %v (default-stamped)", got.String(), wantReq.String())
		}
		if got := container.Resources.Limits[corev1.ResourceMemory]; got.Cmp(wantLim) != 0 {
			t.Fatalf("container Limits[memory] = %v, want %v (default-stamped)", got.String(), wantLim.String())
		}
	})

	t.Run("OperatorOverrideHonored", func(t *testing.T) {
		// An operator-supplied spec.resources is the explicit tuning
		// knob; the rendered container MUST reflect it byte-for-byte.
		// Pins the contract: the kubebuilder default never silently
		// overrides an operator value.
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.Resources = &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("12Gi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			},
		}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		reconcile(t, r, "cache", ns)

		dep := getDeployment(t, r, "cache", ns)
		container := dep.Spec.Template.Spec.Containers[0]
		wantReq := resource.MustParse("12Gi")
		if got := container.Resources.Requests[corev1.ResourceMemory]; got.Cmp(wantReq) != 0 {
			t.Fatalf("container Requests[memory] = %v, want operator-supplied %v", got.String(), wantReq.String())
		}
		wantLim := resource.MustParse("16Gi")
		if got := container.Resources.Limits[corev1.ResourceMemory]; got.Cmp(wantLim) != 0 {
			t.Fatalf("container Limits[memory] = %v, want operator-supplied %v", got.String(), wantLim.String())
		}
	})
}
