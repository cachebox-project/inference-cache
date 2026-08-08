// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	builtinadapters "github.com/cachebox-project/inference-cache/internal/adapters/builtin"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"testing"
)

type remoteOnlyRuntimeAdapter struct {
	adapterruntime.KVCacheRuntimeAdapter
}

func (remoteOnlyRuntimeAdapter) SupportsBinding(binding *backendadapter.Binding) bool {
	return binding != nil
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}
	return scheme
}

func newReconciler(scheme *runtime.Scheme, objs ...client.Object) *CacheBackendReconciler {
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}).
		WithObjects(objs...).
		Build()
	r := &CacheBackendReconciler{
		Client: c,
		Scheme: scheme,
		Log:    logr.Discard(),
		// Seed a real serverInstanceCascade so lifecycle tests that
		// assert on the in-process shadow / lastAt / counted maps
		// actually exercise the clear path rather than skipping
		// the check on a nil pointer.
		serverInstanceCascade: newServerInstanceCascade(),
	}
	configureTestRegistries(r)
	return r
}

func configureTestRegistries(r *CacheBackendReconciler) {
	if r.Registry != nil && r.BackendRegistry != nil {
		return
	}
	registries := builtinadapters.New(builtinadapters.Options{})
	if r.Registry == nil {
		r.Registry = registries.Runtime
	}
	if r.BackendRegistry == nil {
		r.BackendRegistry = registries.Storage
	}
}

func setupTestCacheBackendReconciler(mgr ctrl.Manager, r *CacheBackendReconciler) error {
	configureTestRegistries(r)
	return r.SetupWithManager(mgr)
}

func externalLMCacheStorage(endpoint string) *cachev1alpha1.CacheBackendRemoteStorageSpec {
	return &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		Endpoint:  endpoint,
	}
}

func reconcile(t *testing.T, r *CacheBackendReconciler, name, namespace string) {
	t.Helper()
	configureTestRegistries(r)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name, Namespace: namespace},
	}); err != nil {
		t.Fatalf("reconcile %s/%s: %v", namespace, name, err)
	}
}

func ptrInt32(v int32) *int32 { return &v }

// lmcacheBackend is the shared managed-backend fixture. It opts OUT of the
// KV-event readiness gate via the inferencecache.io/require-kv-events:
// "false" annotation so the many tests that assert rollout-driven Ready /
// Degraded conditions, HPA behavior, apply-error status, and transition
// Events keep exercising exactly that — orthogonal to the gate. Tests that
// exercise the gate itself build backends without this annotation (or
// override it).
func lmcacheBackend(name, namespace string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Generation:  1,
			Annotations: map[string]string{"inferencecache.io/require-kv-events": "false"},
		},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider:      cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
				Ownership:     cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
				LMCacheServer: &cachev1alpha1.LMCacheServerRemoteStorageSpec{},
			},
		},
	}
}

func getDeployment(t *testing.T, r *CacheBackendReconciler, name, namespace string) *appsv1.Deployment {
	t.Helper()
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &dep); err != nil {
		t.Fatalf("get deployment %s/%s: %v", namespace, name, err)
	}
	return &dep
}

func getOptionalDeployment(t *testing.T, r *CacheBackendReconciler, name, namespace string) (*appsv1.Deployment, error) {
	t.Helper()
	var dep appsv1.Deployment
	err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &dep)
	return &dep, err
}

func getBackend(t *testing.T, r *CacheBackendReconciler, name, namespace string) *cachev1alpha1.CacheBackend {
	t.Helper()
	var cb cachev1alpha1.CacheBackend
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &cb); err != nil {
		t.Fatalf("get CacheBackend %s/%s: %v", namespace, name, err)
	}
	return &cb
}

// mooncakeBackend is the managed-Mooncake fixture, mirroring lmcacheBackend:
// it opts OUT of the KV-event readiness gate so the rollout-driven Ready
// assertion is orthogonal to the gate.
func mooncakeBackend(name, namespace string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Generation:  1,
			Annotations: map[string]string{"inferencecache.io/require-kv-events": "false"},
		},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderMooncake,
				Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
			},
		},
	}
}

func volumeNames(vs []corev1.Volume) []string {
	names := make([]string, len(vs))
	for i := range vs {
		names[i] = vs[i].Name
	}
	return names
}

func containerNames(cs []corev1.Container) []string {
	names := make([]string, len(cs))
	for i := range cs {
		names[i] = cs[i].Name
	}
	return names
}

func TestReconcileIgnoresMissingObject(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme)

	reconcile(t, r, "missing", "default")
}

// newReconcilerWithInterceptor wires a fake client whose write methods are
// wrapped by funcs, so a test can inject 409 Conflict / 403 Forbidden errors
// for specific resources and exercise the reconciler's retry + status paths.
func newReconcilerWithInterceptor(scheme *runtime.Scheme, funcs interceptor.Funcs, objs ...client.Object) *CacheBackendReconciler {
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
	r := &CacheBackendReconciler{
		Client:                c,
		Scheme:                scheme,
		Log:                   logr.Discard(),
		serverInstanceCascade: newServerInstanceCascade(),
	}
	configureTestRegistries(r)
	return r
}

func containsStr(s []string, want string) bool {
	for _, v := range s {
		if v == want {
			return true
		}
	}
	return false
}

func findCondition(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}
