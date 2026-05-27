package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func TestCacheBackendReconcileNoop(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	backend := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeLMCache},
	}
	reconciler := &CacheBackendReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(backend).Build(),
		Scheme: scheme,
		Log:    logr.Discard(),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "example", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile existing CacheBackend: %v", err)
	}
}

func TestCacheBackendReconcileMirrorsExternalEndpointToStatus(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	backend := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: "external-cache.default.svc:8080",
		},
	}
	reconciler := &CacheBackendReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&cachev1alpha1.CacheBackend{}).
			WithObjects(backend).
			Build(),
		Scheme: scheme,
		Log:    logr.Discard(),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "example", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile external CacheBackend: %v", err)
	}

	var updated cachev1alpha1.CacheBackend
	if err := reconciler.Get(ctx, types.NamespacedName{Name: "example", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get updated CacheBackend: %v", err)
	}
	if updated.Status.Endpoint != "external-cache.default.svc:8080" {
		t.Fatalf("status.endpoint = %q, want spec endpoint", updated.Status.Endpoint)
	}
}

func TestCacheBackendReconcileClearsRemovedExternalEndpointFromStatus(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	backend := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeExternal,
		},
		Status: cachev1alpha1.CacheBackendStatus{
			Endpoint: "stale-cache.default.svc:8080",
		},
	}
	reconciler := &CacheBackendReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&cachev1alpha1.CacheBackend{}).
			WithObjects(backend).
			Build(),
		Scheme: scheme,
		Log:    logr.Discard(),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "example", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile external CacheBackend with cleared endpoint: %v", err)
	}

	var updated cachev1alpha1.CacheBackend
	if err := reconciler.Get(ctx, types.NamespacedName{Name: "example", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get updated CacheBackend: %v", err)
	}
	if updated.Status.Endpoint != "" {
		t.Fatalf("status.endpoint = %q, want empty", updated.Status.Endpoint)
	}
}

func TestCacheBackendReconcilePreservesManagedEndpointStatus(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	backend := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
		},
		Status: cachev1alpha1.CacheBackendStatus{
			Endpoint: "managed-cache.default.svc:8080",
		},
	}
	reconciler := &CacheBackendReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&cachev1alpha1.CacheBackend{}).
			WithObjects(backend).
			Build(),
		Scheme: scheme,
		Log:    logr.Discard(),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "example", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile managed CacheBackend: %v", err)
	}

	var updated cachev1alpha1.CacheBackend
	if err := reconciler.Get(ctx, types.NamespacedName{Name: "example", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get updated CacheBackend: %v", err)
	}
	if updated.Status.Endpoint != "managed-cache.default.svc:8080" {
		t.Fatalf("status.endpoint = %q, want managed endpoint preserved", updated.Status.Endpoint)
	}
}

func TestCacheBackendReconcileIgnoresMissingObject(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	reconciler := &CacheBackendReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme: scheme,
		Log:    logr.Discard(),
	}

	_, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile missing CacheBackend: %v", err)
	}
}
