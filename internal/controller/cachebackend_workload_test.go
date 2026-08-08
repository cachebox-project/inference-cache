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

func TestReconcileLMCacheImageOverride(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.RemoteStorage.LMCacheServer.Image = "registry.example.com/lmcache-server:pinned"
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	if got := dep.Spec.Template.Spec.Containers[0].Image; got != "registry.example.com/lmcache-server:pinned" {
		t.Fatalf("container image = %q, want overridden image", got)
	}
}

func TestReconcileLMCacheUpdatesImage(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.RemoteStorage.LMCacheServer.Image = "example.com/lmcache-server:v2"
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update image: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if got := getDeployment(t, r, "cache", "ns1").Spec.Template.Spec.Containers[0].Image; got != "example.com/lmcache-server:v2" {
		t.Fatalf("deployment image = %q, want updated image", got)
	}
}

func TestReconcileLMCacheScalesReplicas(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.Replicas = ptrInt32(3)
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update replicas: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 3 {
		t.Fatalf("deployment replicas = %v, want 3 after scale", dep.Spec.Replicas)
	}
}

func TestReconcileServicePortDriftCorrected(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")

	// Drift the owned Service out-of-band: drop two ports.
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
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 65432 {
		t.Fatalf("service ports = %v, want lm:// 65432 restored after drift", svc.Spec.Ports)
	}
}

func TestReconcileLMCacheUpdatesPodOverrides(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.Template = &cachev1alpha1.CacheBackendPodSpecOverride{
		NodeSelector:       map[string]string{"accelerator": "h100"},
		ServiceAccountName: "backend-sa",
	}
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update template overrides: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	spec := getDeployment(t, r, "cache", "ns1").Spec.Template.Spec
	if spec.NodeSelector["accelerator"] != "h100" {
		t.Fatalf("nodeSelector not reconciled: %v", spec.NodeSelector)
	}
	if spec.ServiceAccountName != "backend-sa" {
		t.Fatalf("serviceAccountName = %q, want backend-sa", spec.ServiceAccountName)
	}
}

func TestReconcileLMCacheUpgradeFromColocatedAllInOne(t *testing.T) {
	// Upgrading an existing Deployment that the retired colocated all-in-one
	// builder created (single container named "vllm" referencing pod-level
	// volumes "cache-home" + "shm") to the standalone shape (single
	// container named "lmcache-server", no pod-level volumes) must REPLACE
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
	if len(pod.Containers) != 1 || pod.Containers[0].Name != "lmcache-server" {
		t.Fatalf("containers = %v, want exactly 1 lmcache-server after upgrade", containerNames(pod.Containers))
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
