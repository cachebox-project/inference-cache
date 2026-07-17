package controller

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// TestBuildDeploymentRecreateStrategyForHostNetwork covers the rollout strategy a
// hostNetwork cache-server needs. Such a pod binds its ports directly on the node,
// so the default RollingUpdate would surge a second pod onto the same host ports:
// it CrashLoops failing to bind while the old pod still holds them (in practice the
// scheduler rejects it earlier still, since the apiserver defaults
// hostPort=containerPort for hostNetwork pods). Recreate tears the old pod down
// first. Backends that stay on the pod network must keep the default strategy.
func TestBuildDeploymentRecreateStrategyForHostNetwork(t *testing.T) {
	r := &CacheBackendReconciler{}
	const ns = "default"

	t.Run("HostNetworkGetsRecreate", func(t *testing.T) {
		dep := r.buildDeployment(mooncakeBackend("cache", ns), &corev1.PodSpec{
			HostNetwork: true,
			Containers:  []corev1.Container{{Name: "master"}},
		})
		if got := dep.Spec.Strategy.Type; got != appsv1.RecreateDeploymentStrategyType {
			t.Fatalf("strategy = %q, want %q (hostNetwork pods collide on node ports)",
				got, appsv1.RecreateDeploymentStrategyType)
		}
		if !dep.Spec.Template.Spec.HostNetwork {
			t.Fatal("hostNetwork was not propagated into the pod template")
		}
	})

	t.Run("PodNetworkKeepsDefaultStrategy", func(t *testing.T) {
		dep := r.buildDeployment(lmcacheBackend("cache", ns), &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "server"}},
		})
		if got := dep.Spec.Strategy.Type; got != "" {
			t.Fatalf("strategy = %q, want empty (apiserver defaults to RollingUpdate)", got)
		}
	})
}

// TestClampSingletonReplicas pins the reconciler's last line of defense.
// Admission rejects spec.replicas>1 / spec.autoscaling for a singleton backend, but
// ValidateUpdate only rejects violations an edit *introduces* — an object written
// before the rule existed stays in etcd with replicas=3 and is never re-validated.
// Rendering that faithfully would put several servers on the cluster: host-network
// masters contending for node ports or splitting the store, or several Redis pods
// partitioning the (sglang, LMCache) L2 keyspace. So the reconciler clamps rather
// than obeys — for BOTH singleton reasons.
func TestClampSingletonReplicas(t *testing.T) {
	i32 := func(v int32) *int32 { return &v }
	// sglangLMCache / vllmLMCache: the pair drives singleton-ness on the pod network
	// (no hostNetwork), so these isolate the (sglang, LMCache) trigger from the
	// host-network one.
	sglangLMCache := func() *cachev1alpha1.CacheBackend {
		cb := &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{
			Type:        cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "sglang"},
		}}
		return cb
	}
	vllmLMCache := func() *cachev1alpha1.CacheBackend {
		return &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{
			Type:        cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"},
		}}
	}
	for _, tc := range []struct {
		name        string
		backend     *cachev1alpha1.CacheBackend
		hostNetwork bool
		replicas    *int32
		want        *int32
	}{
		{"HostNetworkGrandfatheredScaleOutClamped", vllmLMCache(), true, i32(3), i32(1)},
		{"HostNetworkSingletonUntouched", vllmLMCache(), true, i32(1), i32(1)},
		{"HostNetworkDisabledStaysDisabled", vllmLMCache(), true, i32(0), i32(0)},
		{"HostNetworkNilReplicasUntouched", vllmLMCache(), true, nil, nil},
		{"SGLangRedisGrandfatheredScaleOutClamped", sglangLMCache(), false, i32(3), i32(1)},
		{"SGLangRedisSingletonUntouched", sglangLMCache(), false, i32(1), i32(1)},
		{"SGLangRedisDisabledStaysDisabled", sglangLMCache(), false, i32(0), i32(0)},
		{"VLLMLMCacheScalesFreely", vllmLMCache(), false, i32(3), i32(3)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := &appsv1.DeploymentSpec{
				Replicas: tc.replicas,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{HostNetwork: tc.hostNetwork}},
			}
			clampSingletonReplicas(spec, tc.backend)
			switch {
			case tc.want == nil && spec.Replicas != nil:
				t.Fatalf("replicas = %d, want nil", *spec.Replicas)
			case tc.want != nil && spec.Replicas == nil:
				t.Fatalf("replicas = nil, want %d", *tc.want)
			case tc.want != nil && *spec.Replicas != *tc.want:
				t.Fatalf("replicas = %d, want %d", *spec.Replicas, *tc.want)
			}
		})
	}
}

// TestBuildDeploymentClampsGrandfatheredHostNetworkReplicas proves the clamp is
// wired into the render path, not merely available as a helper.
func TestBuildDeploymentClampsGrandfatheredHostNetworkReplicas(t *testing.T) {
	r := &CacheBackendReconciler{}
	cb := mooncakeBackend("cache", "default")
	three := int32(3)
	cb.Spec.Replicas = &three

	dep := r.buildDeployment(cb, &corev1.PodSpec{
		HostNetwork: true,
		Containers:  []corev1.Container{{Name: "master"}},
	})
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Fatalf("replicas = %v, want 1 — a grandfathered spec.replicas=3 must not schedule three masters", dep.Spec.Replicas)
	}
}

// TestHeadlessnessDiverges pins the recreate trigger in applyService. spec.clusterIP
// is immutable, so a Service can never be migrated in place between headless
// ("None") and a virtual ClusterIP in either direction — it must be recreated. An
// unassigned live value ("") means the apiserver has not allocated one yet and must
// never trigger a delete, or a transient read would churn the Service.
func TestHeadlessnessDiverges(t *testing.T) {
	for _, tc := range []struct {
		name          string
		live, desired string
		want          bool
	}{
		{"UnassignedLiveNeverDiverges", "", corev1.ClusterIPNone, false},
		{"UnassignedLiveNeverDivergesForVirtualIP", "", "", false},
		{"VirtualIPWantsHeadless", "10.96.0.10", corev1.ClusterIPNone, true},
		{"HeadlessWantsVirtualIP", corev1.ClusterIPNone, "", true},
		{"HeadlessStaysHeadless", corev1.ClusterIPNone, corev1.ClusterIPNone, false},
		{"VirtualIPStaysVirtualIP", "10.96.0.10", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := headlessnessDiverges(tc.live, tc.desired); got != tc.want {
				t.Fatalf("headlessnessDiverges(%q, %q) = %v, want %v", tc.live, tc.desired, got, tc.want)
			}
		})
	}
}
