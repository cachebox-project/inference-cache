// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func TestReconcileManagedRedisCreatesSingletonWorkload(t *testing.T) {
	backend := lmcacheBackend("cache", "ns1")
	reconciler := newReconciler(newScheme(t), backend)

	reconcile(t, reconciler, backend.Name, backend.Namespace)

	deployment := getDeployment(t, reconciler, backend.Name, backend.Namespace)
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 {
		t.Fatalf("managed Redis replicas = %v, want 1", deployment.Spec.Replicas)
	}
	if len(deployment.Spec.Template.Spec.Containers) != 1 || deployment.Spec.Template.Spec.Containers[0].Name != "redis-l2" {
		t.Fatalf("managed Redis containers = %+v", deployment.Spec.Template.Spec.Containers)
	}
	var service corev1.Service
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: backend.Name, Namespace: backend.Namespace}, &service); err != nil {
		t.Fatalf("get managed Redis Service: %v", err)
	}
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 6379 {
		t.Fatalf("managed Redis Service ports = %+v", service.Spec.Ports)
	}

	got := getBackend(t, reconciler, backend.Name, backend.Namespace)
	wantEndpoint := "cache.ns1.svc.cluster.local:6379"
	if got.Status.RemoteStorage == nil || got.Status.RemoteStorage.Provider != cachev1alpha1.CacheBackendRemoteStorageProviderRedis || got.Status.RemoteStorage.Endpoint != wantEndpoint {
		t.Fatalf("remote-storage status = %+v, want Redis endpoint %q", got.Status.RemoteStorage, wantEndpoint)
	}
}

func TestReconcileExternalRedisCreatesNoWorkload(t *testing.T) {
	backend := lmcacheBackend("external", "ns1")
	backend.Spec.RemoteStorage = externalRedisStorage("redis.example:6379")
	reconciler := newReconciler(newScheme(t), backend)

	reconcile(t, reconciler, backend.Name, backend.Namespace)

	assertNoManagedWorkload(t, reconciler, backend.Name, backend.Namespace)
	got := getBackend(t, reconciler, backend.Name, backend.Namespace)
	if got.Status.RemoteStorage == nil || got.Status.RemoteStorage.Endpoint != "redis.example:6379" || got.Status.RemoteStorage.Ready != metav1.ConditionTrue {
		t.Fatalf("external Redis status = %+v", got.Status.RemoteStorage)
	}
	ready := findCondition(got.Status.Conditions, conditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionUnknown || ready.Reason != reasonConnectorUnverified {
		t.Fatalf("Ready = %+v, want Unknown/%s until an injected engine Pod is observed", ready, reasonConnectorUnverified)
	}
}

func TestReconcileHostOnlyMPHasNoProviderWorkload(t *testing.T) {
	backend := lmcacheBackend("host-only", "ns1")
	backend.Spec.RemoteStorage = nil
	reconciler := newReconciler(newScheme(t), backend)

	reconcile(t, reconciler, backend.Name, backend.Namespace)

	assertNoManagedWorkload(t, reconciler, backend.Name, backend.Namespace)
	got := getBackend(t, reconciler, backend.Name, backend.Namespace)
	if got.Status.RemoteStorage != nil {
		t.Fatalf("host-only backend published remote-storage status: %+v", got.Status.RemoteStorage)
	}
}

func assertNoManagedWorkload(t *testing.T, reconciler *CacheBackendReconciler, name, namespace string) {
	t.Helper()
	key := types.NamespacedName{Name: name, Namespace: namespace}
	if err := reconciler.Get(context.Background(), key, &appsv1.Deployment{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Deployment lookup error = %v, want NotFound", err)
	}
	if err := reconciler.Get(context.Background(), key, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Service lookup error = %v, want NotFound", err)
	}
}
