package controller

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// TestBuildDeploymentRecreateStrategyForHostNetwork covers the rollout strategy a
// hostNetwork cache-server needs. Such a pod binds its ports directly on the node
// (the apiserver defaults hostPort=containerPort when hostNetwork is set), so the
// default RollingUpdate would surge a second pod onto the same host ports: it
// either fails the scheduler's NodePorts predicate or CrashLoops on bind until the
// old pod exits. Recreate tears the old pod down first. Backends that stay on the
// pod network must keep the default strategy.
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
