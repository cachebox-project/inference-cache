// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	builtinruntime "github.com/cachebox-project/inference-cache/internal/adapters/builtin/runtime"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

func TestReconcileCanonicalHostOnlyCacheCreatesNoProviderWorkload(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("host-only", "ns1")
	cb.Spec.RemoteStorage = nil
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	r := newReconciler(scheme, cb)

	reconcile(t, r, cb.Name, cb.Namespace)

	if _, err := getOptionalDeployment(t, r, cb.Name, cb.Namespace); !apierrors.IsNotFound(err) {
		t.Fatalf("deployment lookup error = %v, want NotFound", err)
	}
	var service corev1.Service
	if err := r.Get(context.Background(), types.NamespacedName{Name: cb.Name, Namespace: cb.Namespace}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("service lookup error = %v, want NotFound", err)
	}
	got := getBackend(t, r, cb.Name, cb.Namespace)
	if got.Status.RemoteStorage != nil {
		t.Fatalf("status.remoteStorage = %+v, want nil for host-only hierarchy", got.Status.RemoteStorage)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, conditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionUnknown || ready.Reason != reasonConnectorUnverified {
		t.Fatalf("Ready = %+v, want Unknown/%s until an engine Pod is observed", ready, reasonConnectorUnverified)
	}
}

func TestReconcileCanonicalSGLangHiCacheWithRemoteStorageIsUnmanaged(t *testing.T) {
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "hicache-remote", Namespace: "ns1", Generation: 1},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeSGLang,
			Type:    cachev1alpha1.CacheBackendTypeSGLangHiCache,
			HiCache: &cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"},
			RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis, Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
				Redis: &cachev1alpha1.RedisRemoteStorageSpec{},
			},
		},
	}
	r := newReconciler(scheme, cb)
	reconcile(t, r, cb.Name, cb.Namespace)

	if _, err := getOptionalDeployment(t, r, cb.Name, cb.Namespace); !apierrors.IsNotFound(err) {
		t.Fatalf("deployment lookup error = %v, want NotFound", err)
	}
	got := getBackend(t, r, cb.Name, cb.Namespace)
	if got.Status.RemoteStorage != nil {
		t.Fatalf("status.remoteStorage = %+v, want nil for unsupported binding", got.Status.RemoteStorage)
	}
	if ready := meta.FindStatusCondition(got.Status.Conditions, conditionTypeReady); ready != nil {
		t.Fatalf("unsupported binding published Ready condition: %+v", ready)
	}
}

func TestReconcileCanonicalHostOnlyCacheReportsEngineDiagnostics(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("host-only-kernel", "ns1")
	cb.UID = types.UID("host-only-kernel-uid")
	cb.Spec.RemoteStorage = nil
	cb.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{"app": "engine"}}
	pod := strictPodWithKernelStatus(termed(1, enginebinding.KernelCheckMsgFailPrefix+" lmcache c_ops failed"))
	pod.ObjectMeta = metav1.ObjectMeta{
		Name: "engine", Namespace: cb.Namespace, Labels: map[string]string{"app": "engine"},
		Annotations: map[string]string{
			podwebhook.AnnotationInjectedBy: cb.Namespace + "/" + cb.Name, podwebhook.AnnotationInjectedByUID: string(cb.UID),
		},
	}
	pod.Spec.Containers = []corev1.Container{{Name: "vllm"}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "vllm", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: crashLoopBackOffReason}},
	}}
	r := newReconciler(scheme, cb, &pod)
	reconcile(t, r, cb.Name, cb.Namespace)

	got := getBackend(t, r, cb.Name, cb.Namespace)
	kernels := meta.FindStatusCondition(got.Status.Conditions, conditionTypeEngineKernelsHealthy)
	if kernels == nil || kernels.Status != metav1.ConditionFalse || kernels.Reason != reasonKernelLoadFailed {
		t.Fatalf("EngineKernelsHealthy = %+v, want False/%s", kernels, reasonKernelLoadFailed)
	}
	compatibility := meta.FindStatusCondition(got.Status.Conditions, conditionTypeEngineCompatibility)
	if compatibility == nil || compatibility.Status != metav1.ConditionFalse || compatibility.Reason != reasonInjectedEngineCrashLooping {
		t.Fatalf("EngineCompatibility = %+v, want False/%s", compatibility, reasonInjectedEngineCrashLooping)
	}
}

func TestReconcileManagedToExternalCleansUpProviderChildren(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))
	reconcile(t, r, "cache", "ns1")
	if _, err := getOptionalDeployment(t, r, "cache", "ns1"); err != nil {
		t.Fatalf("expected managed Redis deployment: %v", err)
	}

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.RemoteStorage = externalRedisStorage("external.ns1.svc:6379")
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("switch to external Redis: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	var deps appsv1.DeploymentList
	if err := r.List(context.Background(), &deps, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(deps.Items) != 0 {
		t.Fatalf("deployments = %d, want 0 after switch to External", len(deps.Items))
	}
	var svcs corev1.ServiceList
	if err := r.List(context.Background(), &svcs, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list services: %v", err)
	}
	if len(svcs.Items) != 0 {
		t.Fatalf("services = %d, want 0 after switch to External", len(svcs.Items))
	}
	got := getBackend(t, r, "cache", "ns1")
	if got.Status.RemoteStorage == nil || got.Status.RemoteStorage.Endpoint != "external.ns1.svc:6379" {
		t.Fatalf("status.remoteStorage = %+v, want mirrored external endpoint", got.Status.RemoteStorage)
	}
}

func TestReconcileExternalAdvancesObservedGeneration(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("ext", "default")
	cb.Generation = 7
	cb.Spec.RemoteStorage = externalRedisStorage("external.default.svc:6379")
	cb.Status.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageStatus{
		Provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis, Endpoint: "external.default.svc:6379",
	}
	r := newReconciler(scheme, cb)
	reconcile(t, r, "ext", "default")
	if got := getBackend(t, r, "ext", "default").Status.ObservedGeneration; got != 7 {
		t.Fatalf("status.observedGeneration = %d, want 7", got)
	}
}

func TestReconcileUnmanagedTypeNoop(t *testing.T) {
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendType("unsupported")},
	}
	r := newReconciler(scheme, cb)
	reconcile(t, r, "cache", "ns1")
	var deps appsv1.DeploymentList
	if err := r.List(context.Background(), &deps, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(deps.Items) != 0 {
		t.Fatalf("deployments = %d, want 0 for unmanaged type", len(deps.Items))
	}
}

func TestReconcileEventsOnlyUnsupportedPairIsUnmanaged(t *testing.T) {
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1", Generation: 1},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:        cachev1alpha1.CacheBackendType("unsupported"),
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{Mode: cachev1alpha1.CacheBackendIntegrationModeEventsOnly},
		},
	}
	r := newReconciler(scheme, cb)
	reconcile(t, r, "cache", "ns1")
	got := getBackend(t, r, "cache", "ns1")
	if ready := findCondition(got.Status.Conditions, conditionTypeReady); ready != nil {
		t.Fatalf("unsupported-pair EventsOnly published Ready: %+v", ready)
	}
}

func TestReconcileEventsOnlyAdapterRejectingNilBindingIsUnmanaged(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.RemoteStorage = nil
	cb.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
	r := newReconciler(scheme, cb)
	r.Registry = adapterruntime.NewRegistry()
	r.Registry.Register(remoteOnlyRuntimeAdapter{KVCacheRuntimeAdapter: builtinruntime.NewVLLMLMCacheMPAdapter(builtinruntime.SubscriberConfig{})})
	reconcile(t, r, "cache", "ns1")
	if ready := findCondition(getBackend(t, r, "cache", "ns1").Status.Conditions, conditionTypeReady); ready != nil {
		t.Fatalf("adapter rejecting nil binding published Ready: %+v", ready)
	}
}

func TestReconcileEventsOnlyTakesPrecedenceOverExternal(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.RemoteStorage = externalRedisStorage("external-cache.ns1.svc:6379")
	cb.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
	r := newReconciler(scheme, cb)
	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1")
	if got.Status.RemoteStorage != nil {
		t.Fatalf("status.remoteStorage = %+v, want nil because EventsOnly wins", got.Status.RemoteStorage)
	}
	ready := findCondition(got.Status.Conditions, conditionTypeReady)
	if ready == nil || ready.Reason == conditionReasonExternalEndpointAccepted {
		t.Fatalf("EventsOnly did not take precedence; Ready = %+v", ready)
	}
}

func TestReconcileExternalMirrorsRedisEndpointAndSetsReady(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("example", "default")
	cb.Generation = 3
	cb.Spec.RemoteStorage = externalRedisStorage("external-cache.default.svc:6379")
	r := newReconciler(scheme, cb)
	reconcile(t, r, "example", "default")

	got := getBackend(t, r, "example", "default")
	if got.Status.RemoteStorage == nil || got.Status.RemoteStorage.Endpoint != "external-cache.default.svc:6379" || got.Status.RemoteStorage.Ready != metav1.ConditionTrue {
		t.Fatalf("status.remoteStorage = %+v, want ready mirrored Redis endpoint", got.Status.RemoteStorage)
	}
	remoteReady := findCondition(got.Status.Conditions, conditionTypeRemoteStorageReady)
	if remoteReady == nil || remoteReady.Status != metav1.ConditionTrue || remoteReady.Reason != reasonRemoteStorageReady {
		t.Fatalf("RemoteStorageReady = %+v, want True/%s", remoteReady, reasonRemoteStorageReady)
	}
	ready := findCondition(got.Status.Conditions, conditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionUnknown || ready.Reason != reasonConnectorUnverified || ready.ObservedGeneration != 3 {
		t.Fatalf("Ready = %+v, want Unknown/%s generation 3", ready, reasonConnectorUnverified)
	}
}

func TestReconcileExternalInvalidRedisEndpointSetsReadyFalse(t *testing.T) {
	for _, endpoint := range []string{
		"https://cache.example.com:443/api", "cache.example.com", "cache.example.com:not-a-port",
		"cache.example.com:0", "cache.example.com:70000", "2001:db8::1", "cache example:6379",
	} {
		t.Run(endpoint, func(t *testing.T) {
			scheme := newScheme(t)
			cb := lmcacheBackend("ext-bad", "default")
			cb.Spec.RemoteStorage = externalRedisStorage(endpoint)
			r := newReconciler(scheme, cb)
			reconcile(t, r, cb.Name, cb.Namespace)

			got := getBackend(t, r, cb.Name, cb.Namespace)
			remoteReady := findCondition(got.Status.Conditions, conditionTypeRemoteStorageReady)
			if remoteReady == nil || remoteReady.Status != metav1.ConditionFalse || remoteReady.Reason != conditionReasonExternalEndpointInvalid {
				t.Fatalf("RemoteStorageReady = %+v, want False/%s", remoteReady, conditionReasonExternalEndpointInvalid)
			}
			if !strings.Contains(remoteReady.Message, "spec.remoteStorage.endpoint") {
				t.Fatalf("RemoteStorageReady message = %q, want canonical field", remoteReady.Message)
			}
		})
	}
}

func TestReconcileExternalMissingRedisEndpointClearsStatus(t *testing.T) {
	for _, endpoint := range []string{"", "   \t  "} {
		t.Run(endpoint, func(t *testing.T) {
			scheme := newScheme(t)
			cb := lmcacheBackend("ext-missing", "default")
			cb.Spec.RemoteStorage = externalRedisStorage(endpoint)
			cb.Status.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageStatus{
				Provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis, Endpoint: "stale.default.svc:6379", Ready: metav1.ConditionTrue,
			}
			r := newReconciler(scheme, cb)
			reconcile(t, r, cb.Name, cb.Namespace)

			got := getBackend(t, r, cb.Name, cb.Namespace)
			if got.Status.RemoteStorage == nil || got.Status.RemoteStorage.Endpoint != "" || got.Status.RemoteStorage.Ready != metav1.ConditionFalse {
				t.Fatalf("status.remoteStorage = %+v, want present but not ready and empty endpoint", got.Status.RemoteStorage)
			}
			remoteReady := findCondition(got.Status.Conditions, conditionTypeRemoteStorageReady)
			if remoteReady == nil || remoteReady.Status != metav1.ConditionFalse || remoteReady.Reason != conditionReasonExternalEndpointMissing {
				t.Fatalf("RemoteStorageReady = %+v, want False/%s", remoteReady, conditionReasonExternalEndpointMissing)
			}
		})
	}
}
