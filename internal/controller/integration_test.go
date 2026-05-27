package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// TestIntegrationCacheBackendReconcile exercises the reconciler against a real
// kube-apiserver (envtest) so it validates behavior the fake client cannot: real
// API-server defaulting, and that a converged reconcile issues no child writes
// against that defaulting (the hot-loop regression class).
//
// It is skipped unless KUBEBUILDER_ASSETS is set (e.g. `make test-env`), so the
// default `go test ./...` in CI does not require envtest binaries.
func TestIntegrationCacheBackendReconcile(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run with `KUBEBUILDER_ASSETS=$(make test-env) go test` for envtest")
	}
	logf.SetLogger(zap.New(zap.UseDevMode(true)))

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Logf("stop envtest: %v", err)
		}
	})

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}

	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("build client: %v", err)
	}

	ctx := context.Background()
	const ns = "c2-itest"
	if err := k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "itest", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeLMCache,
			Replicas: ptrInt32(2),
		},
	}
	if err := k8s.Create(ctx, cb); err != nil {
		t.Fatalf("create CacheBackend: %v", err)
	}

	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "itest", Namespace: ns}}
	reconcileOnce := func() {
		t.Helper()
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}

	// Create the workload.
	reconcileOnce()

	depKey := types.NamespacedName{Name: "itest", Namespace: ns}
	var dep appsv1.Deployment
	if err := k8s.Get(ctx, depKey, &dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 2 {
		t.Fatalf("deployment replicas = %v, want 2", dep.Spec.Replicas)
	}
	if owner := metav1.GetControllerOf(&dep); owner == nil || owner.Kind != "CacheBackend" || owner.Name != "itest" {
		t.Fatalf("deployment controller owner = %+v, want CacheBackend/itest", owner)
	}
	// The API server defaulted fields the builder omits (proves we run against real defaulting).
	if dep.Spec.Template.Spec.RestartPolicy == "" || dep.Spec.Template.Spec.DNSPolicy == "" {
		t.Fatalf("expected API-server pod defaults to be applied, got restartPolicy=%q dnsPolicy=%q",
			dep.Spec.Template.Spec.RestartPolicy, dep.Spec.Template.Spec.DNSPolicy)
	}

	var svc corev1.Service
	if err := k8s.Get(ctx, depKey, &svc); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if svc.Spec.ClusterIP == "" {
		t.Fatalf("expected API server to allocate a ClusterIP")
	}
	if owner := metav1.GetControllerOf(&svc); owner == nil || owner.Name != "itest" {
		t.Fatalf("service controller owner = %+v, want CacheBackend/itest", owner)
	}

	var updated cachev1alpha1.CacheBackend
	if err := k8s.Get(ctx, depKey, &updated); err != nil {
		t.Fatalf("get CacheBackend: %v", err)
	}
	if updated.Status.Endpoint != "itest.c2-itest.svc.cluster.local:8000" {
		t.Fatalf("status.endpoint = %q", updated.Status.Endpoint)
	}
	if updated.Status.ObservedGeneration != updated.Generation {
		t.Fatalf("status.observedGeneration = %d, want %d", updated.Status.ObservedGeneration, updated.Generation)
	}

	// No-churn: a converged reconcile must not rewrite the children, even though the
	// API server has applied defaulting that the rendered objects omit.
	reconcileOnce()
	depRV := getRV(t, k8s, ctx, depKey, &appsv1.Deployment{})
	svcRV := getRV(t, k8s, ctx, depKey, &corev1.Service{})
	reconcileOnce()
	if got := getRV(t, k8s, ctx, depKey, &appsv1.Deployment{}); got != depRV {
		t.Fatalf("deployment churned against real defaulting: RV %s -> %s", depRV, got)
	}
	if got := getRV(t, k8s, ctx, depKey, &corev1.Service{}); got != svcRV {
		t.Fatalf("service churned against real defaulting: RV %s -> %s", svcRV, got)
	}

	// Scale: editing spec.replicas reaches the Deployment.
	if err := k8s.Get(ctx, depKey, &updated); err != nil {
		t.Fatalf("re-get CacheBackend: %v", err)
	}
	updated.Spec.Replicas = ptrInt32(3)
	if err := k8s.Update(ctx, &updated); err != nil {
		t.Fatalf("update replicas: %v", err)
	}
	reconcileOnce()
	if err := k8s.Get(ctx, depKey, &dep); err != nil {
		t.Fatalf("re-get deployment: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 3 {
		t.Fatalf("deployment replicas = %v, want 3 after scale", dep.Spec.Replicas)
	}
}

func getRV(t *testing.T, k8s client.Client, ctx context.Context, key types.NamespacedName, obj client.Object) string {
	t.Helper()
	if err := k8s.Get(ctx, key, obj); err != nil {
		t.Fatalf("get %T for resourceVersion: %v", obj, err)
	}
	return obj.GetResourceVersion()
}
