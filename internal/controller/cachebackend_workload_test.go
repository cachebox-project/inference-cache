// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"testing"
)

func TestReconcileManagedWorkloadOverrides(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	grace := int64(45)
	runtimeClass := "gvisor"
	cb.Spec.RemoteStorage.Workload = &cachev1alpha1.CacheBackendManagedWorkloadSpec{
		NodeSelector:                  map[string]string{"pool": "cache"},
		Affinity:                      &corev1.Affinity{},
		Tolerations:                   []corev1.Toleration{{Key: "cache", Operator: corev1.TolerationOpExists}},
		TopologySpreadConstraints:     []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: corev1.ScheduleAnyway}},
		ImagePullSecrets:              []corev1.LocalObjectReference{{Name: "registry-auth"}},
		ServiceAccountName:            "cache-provider",
		SecurityContext:               &corev1.PodSecurityContext{RunAsNonRoot: func() *bool { v := true; return &v }()},
		PriorityClassName:             "cache-critical",
		SchedulerName:                 "cache-scheduler",
		RuntimeClassName:              &runtimeClass,
		TerminationGracePeriodSeconds: &grace,
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	pod := getDeployment(t, r, "cache", "ns1").Spec.Template.Spec
	if pod.NodeSelector["pool"] != "cache" || pod.Affinity == nil || len(pod.Tolerations) != 1 || len(pod.TopologySpreadConstraints) != 1 {
		t.Fatalf("managed workload scheduling not applied: %+v", pod)
	}
	if pod.ServiceAccountName != "cache-provider" || pod.SchedulerName != "cache-scheduler" || pod.PriorityClassName != "cache-critical" {
		t.Fatalf("managed workload identity/scheduler not applied: %+v", pod)
	}
	if len(pod.ImagePullSecrets) != 1 || pod.ImagePullSecrets[0].Name != "registry-auth" ||
		pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot {
		t.Fatalf("managed workload pull/security settings not applied: %+v", pod)
	}
	if pod.RuntimeClassName == nil || *pod.RuntimeClassName != "gvisor" ||
		pod.TerminationGracePeriodSeconds == nil || *pod.TerminationGracePeriodSeconds != 45 {
		t.Fatalf("managed workload runtime/grace settings not applied: %+v", pod)
	}

	// Removing the block must clear every controller-owned override and restore
	// materialized Kubernetes defaults rather than stranding stale placement on
	// the existing Deployment.
	live := getBackend(t, r, "cache", "ns1")
	live.Spec.RemoteStorage.Workload = nil
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("remove managed workload overrides: %v", err)
	}
	reconcile(t, r, "cache", "ns1")
	pod = getDeployment(t, r, "cache", "ns1").Spec.Template.Spec
	if pod.NodeSelector != nil || pod.Affinity != nil || pod.Tolerations != nil || pod.TopologySpreadConstraints != nil || pod.ImagePullSecrets != nil ||
		pod.ServiceAccountName != "" || pod.SecurityContext != nil || pod.PriorityClassName != "" ||
		pod.RuntimeClassName != nil {
		t.Fatalf("removed managed workload settings survived reconciliation: %+v", pod)
	}
	if pod.SchedulerName != "default-scheduler" ||
		pod.TerminationGracePeriodSeconds == nil || *pod.TerminationGracePeriodSeconds != 30 {
		t.Fatalf("managed workload defaults not restored after removal: %+v", pod)
	}
}

func TestReconcileLMCacheImageOverride(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.RemoteStorage.Redis.Image = "registry.example.com/redis:pinned"
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	if got := dep.Spec.Template.Spec.Containers[0].Image; got != "registry.example.com/redis:pinned" {
		t.Fatalf("container image = %q, want overridden image", got)
	}
}

func TestReconcileLMCacheUpdatesImage(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.RemoteStorage.Redis.Image = "example.com/redis:v2"
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update image: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if got := getDeployment(t, r, "cache", "ns1").Spec.Template.Spec.Containers[0].Image; got != "example.com/redis:v2" {
		t.Fatalf("deployment image = %q, want updated image", got)
	}
}

func TestReconcileServicePortDriftCorrected(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")

	// Drift the owned Service out-of-band: drop its Redis port.
	var svc corev1.Service
	if err := r.Get(context.Background(), types.NamespacedName{Name: "cache", Namespace: "ns1"}, &svc); err != nil {
		t.Fatalf("get service: %v", err)
	}
	svc.Spec.Ports = nil
	if err := r.Update(context.Background(), &svc); err != nil {
		t.Fatalf("drift service: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if err := r.Get(context.Background(), types.NamespacedName{Name: "cache", Namespace: "ns1"}, &svc); err != nil {
		t.Fatalf("re-get service: %v", err)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 6379 {
		t.Fatalf("service ports = %v, want Redis 6379 restored after drift", svc.Spec.Ports)
	}
}

func TestReconcileLMCacheUpgradeFromColocatedAllInOne(t *testing.T) {
	// Upgrading an existing Deployment that the retired colocated all-in-one
	// builder created (single container named "vllm" referencing pod-level
	// volumes "cache-home" + "shm") to the managed Redis shape (single
	// container named "redis-l2", no pod-level volumes) must REPLACE
	// both the container set AND the dangling adapter-owned volumes. Leaving
	// the old volumes would carry stale config from the previous shape
	// forever.
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	r := newReconciler(scheme, cb)

	// Seed the live Deployment with the old colocated container + volume
	// shape so the reconciler's update path (not the create path) is exercised.
	reconcile(t, r, "cache", "ns1")
	live := getDeployment(t, r, "cache", "ns1")
	live.Spec.Template.Spec.Containers = []corev1.Container{
		{
			Name:    "vllm",
			Image:   "lmcache/vllm-openai:latest",
			Command: []string{"vllm", "serve", "meta-llama/Llama-3.1-8B-Instruct"},
			Args:    []string{"--enable-prefix-caching"},
		},
	}
	live.Spec.Template.Spec.Volumes = []corev1.Volume{
		{Name: "cache-home", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "shm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}},
	}
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("seed pre-upgrade deployment: %v", err)
	}

	reconcile(t, r, "cache", "ns1")

	pod := getDeployment(t, r, "cache", "ns1").Spec.Template.Spec
	if len(pod.Containers) != 1 || pod.Containers[0].Name != "redis-l2" {
		t.Fatalf("containers = %v, want exactly 1 redis-l2 after upgrade", containerNames(pod.Containers))
	}
	for _, v := range pod.Volumes {
		if v.Name == "cache-home" || v.Name == "shm" {
			t.Fatalf("stale colocated-rendering volume %q survived the upgrade: %v", v.Name, volumeNames(pod.Volumes))
		}
	}
}

func TestReconcileManagedPodSpecPrunesStaleContainersAndVolumesOnUpgrade(t *testing.T) {
	// Simulates a live Deployment from the previous colocated all-in-one
	// rendering: a "vllm" container referencing pod-level cache-home + shm
	// volumes.
	live := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:    "vllm",
				Image:   "lmcache/vllm-openai:latest",
				Command: []string{"vllm", "serve", "meta-llama/Llama-3.1-8B-Instruct"},
			},
		},
		Volumes: []corev1.Volume{
			{Name: "cache-home", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "shm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}},
		},
	}
	// The standalone desired shape: a "lmcache-server" container, no
	// pod-level volumes.
	desired := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:    "lmcache-server",
				Image:   "lmcache/standalone:v0.4.7",
				Command: []string{"lmcache_server"},
				Args:    []string{"0.0.0.0", "65432", "cpu"},
			},
		},
	}

	reconcileManagedPodSpec(live, desired)

	if len(live.Containers) != 1 || live.Containers[0].Name != "lmcache-server" {
		t.Fatalf("containers = %v, want exactly [lmcache-server] after upgrade", containerNames(live.Containers))
	}
	if len(live.Volumes) != 0 {
		t.Fatalf("Volumes = %v, want empty after upgrade (stale colocated-rendering volumes must be pruned)", volumeNames(live.Volumes))
	}
}

func TestReconcileManagedPodSpecAdoptsAdapterVolumesOnSteadyStateUpdate(t *testing.T) {
	// Volumes are adapter-owned (per the KVCacheRuntimeAdapter contract), so
	// the reconciler always propagates them from desired — even on a
	// same-container reconcile. This corrects out-of-band drift and lets an
	// adapter add/change pod-level volumes without simultaneously changing
	// the container set.
	live := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "lmcache-server", Image: "lmcache/standalone:v1"}},
		Volumes: []corev1.Volume{
			{Name: "drift", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		},
	}
	desired := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "lmcache-server", Image: "lmcache/standalone:v2"}},
		Volumes: []corev1.Volume{
			{Name: "intended", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		},
	}

	reconcileManagedPodSpec(live, desired)

	if got := live.Containers[0].Image; got != "lmcache/standalone:v2" {
		t.Fatalf("container image = %q, want updated to v2", got)
	}
	if len(live.Volumes) != 1 || live.Volumes[0].Name != "intended" {
		t.Fatalf("Volumes = %v, want adapter-owned [intended] (drift corrected)", volumeNames(live.Volumes))
	}
}

func TestReconcileManagedPodSpecCopiesOverrideFields(t *testing.T) {
	// The pod-level override fields the controller owns must be reconciled
	// from desired even on the in-place update path.
	live := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "lmcache-server"}},
	}
	gracePeriod := int64(45)
	desired := &corev1.PodSpec{
		Containers:                    []corev1.Container{{Name: "lmcache-server"}},
		NodeSelector:                  map[string]string{"accelerator": "h100"},
		ServiceAccountName:            "backend-sa",
		SchedulerName:                 "custom-scheduler",
		PriorityClassName:             "high",
		TerminationGracePeriodSeconds: &gracePeriod,
	}

	reconcileManagedPodSpec(live, desired)

	if live.NodeSelector["accelerator"] != "h100" {
		t.Fatalf("NodeSelector not reconciled: %v", live.NodeSelector)
	}
	if live.ServiceAccountName != "backend-sa" {
		t.Fatalf("ServiceAccountName = %q, want backend-sa", live.ServiceAccountName)
	}
	if live.SchedulerName != "custom-scheduler" {
		t.Fatalf("SchedulerName = %q, want custom-scheduler", live.SchedulerName)
	}
	if live.PriorityClassName != "high" {
		t.Fatalf("PriorityClassName = %q, want high", live.PriorityClassName)
	}
	if live.TerminationGracePeriodSeconds == nil || *live.TerminationGracePeriodSeconds != 45 {
		t.Fatalf("TerminationGracePeriodSeconds = %v, want 45", live.TerminationGracePeriodSeconds)
	}
}

func TestReconcileManagedContainerUpdatesInPlace(t *testing.T) {
	// Same-name container update: adapter-owned fields propagate from
	// desired — including Ports and probes, since the Service targets the
	// container's named port and Ready is gated on the probe. The adapter
	// renders Port.Protocol explicitly (ProtocolTCP), so the copy doesn't
	// churn against API-server defaulting.
	live := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:    "lmcache-server",
				Image:   "lmcache/standalone:v1",
				Command: []string{"old"},
				Args:    []string{"--old"},
				Env:     []corev1.EnvVar{{Name: "OLD", Value: "x"}},
				Ports:   []corev1.ContainerPort{{Name: "stale", ContainerPort: 1234, Protocol: corev1.ProtocolTCP}},
			},
		},
	}
	newProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("lmcache")},
		},
	}
	desired := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:           "lmcache-server",
				Image:          "lmcache/standalone:v2",
				Command:        []string{"new"},
				Args:           []string{"--new"},
				Env:            []corev1.EnvVar{{Name: "NEW", Value: "y"}},
				Ports:          []corev1.ContainerPort{{Name: "lmcache", ContainerPort: 65432, Protocol: corev1.ProtocolTCP}},
				ReadinessProbe: newProbe,
			},
		},
	}

	reconcileManagedContainer(live, desired)

	c := live.Containers[0]
	if c.Image != "lmcache/standalone:v2" || c.Command[0] != "new" || c.Args[0] != "--new" {
		t.Fatalf("spec-driven fields not updated: image=%q command=%v args=%v", c.Image, c.Command, c.Args)
	}
	if len(c.Env) != 1 || c.Env[0].Name != "NEW" {
		t.Fatalf("Env = %v, want [NEW=y]", c.Env)
	}
	if len(c.Ports) != 1 || c.Ports[0].ContainerPort != 65432 || c.Ports[0].Name != "lmcache" {
		t.Fatalf("Ports = %v, want desired [lmcache:65432] (Service TargetPort lookups depend on this)", c.Ports)
	}
	if c.ReadinessProbe == nil || c.ReadinessProbe.TCPSocket == nil {
		t.Fatalf("ReadinessProbe = %v, want desired TCP probe propagated", c.ReadinessProbe)
	}
}

func TestReconcileManagedContainerEmptyDesiredIsNoop(t *testing.T) {
	live := &corev1.PodSpec{Containers: []corev1.Container{{Name: "lmcache-server"}}}
	reconcileManagedContainer(live, &corev1.PodSpec{})
	if len(live.Containers) != 1 || live.Containers[0].Name != "lmcache-server" {
		t.Fatalf("empty desired must not touch live; got %v", containerNames(live.Containers))
	}
}
