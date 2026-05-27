package controller

import (
	"context"
	"testing"

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

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

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
	return &CacheBackendReconciler{Client: c, Scheme: scheme, Log: logr.Discard()}
}

func reconcile(t *testing.T, r *CacheBackendReconciler, name, namespace string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name, Namespace: namespace},
	}); err != nil {
		t.Fatalf("reconcile %s/%s: %v", namespace, name, err)
	}
}

func ptrInt32(v int32) *int32 { return &v }

func lmcacheBackend(name, namespace string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Generation: 1},
		Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeLMCache},
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

func getBackend(t *testing.T, r *CacheBackendReconciler, name, namespace string) *cachev1alpha1.CacheBackend {
	t.Helper()
	var cb cachev1alpha1.CacheBackend
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &cb); err != nil {
		t.Fatalf("get CacheBackend %s/%s: %v", namespace, name, err)
	}
	return &cb
}

func TestReconcileLMCacheCreatesWorkload(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(2)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 2 {
		t.Fatalf("deployment replicas = %v, want 2", dep.Spec.Replicas)
	}
	owner := metav1.GetControllerOf(dep)
	if owner == nil || owner.Kind != "CacheBackend" || owner.Name != "cache" || owner.Controller == nil || !*owner.Controller {
		t.Fatalf("deployment controller owner = %+v, want controller ref to CacheBackend/cache", owner)
	}

	containers := dep.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(containers))
	}
	c := containers[0]
	if c.Image == "" {
		t.Fatalf("container image is empty")
	}
	if !containsStr(c.Args, "--kv-transfer-config") || !containsStr(c.Args, "--kv-events-config") || !containsStr(c.Args, "--enable-prefix-caching") {
		t.Fatalf("args missing LMCache/KV-event flags: %v", c.Args)
	}
	if !hasEnv(c.Env, "VLLM_USE_V1", "1") {
		t.Fatalf("env missing VLLM_USE_V1=1: %v", c.Env)
	}
	if len(c.Ports) != 3 {
		t.Fatalf("ports = %d, want 3 (http, kv-events, kv-replay)", len(c.Ports))
	}

	svc := &corev1.Service{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "cache", Namespace: "ns1"}, svc); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("service type = %q, want ClusterIP", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 3 {
		t.Fatalf("service ports = %d, want 3", len(svc.Spec.Ports))
	}
	if so := metav1.GetControllerOf(svc); so == nil || so.Name != "cache" {
		t.Fatalf("service controller owner = %+v, want CacheBackend/cache", so)
	}

	updated := getBackend(t, r, "cache", "ns1")
	wantEndpoint := "cache.ns1.svc.cluster.local:8000"
	if updated.Status.Endpoint != wantEndpoint {
		t.Fatalf("status.endpoint = %q, want %q", updated.Status.Endpoint, wantEndpoint)
	}
	if updated.Status.Health != cachev1alpha1.CacheBackendHealthPending {
		t.Fatalf("status.health = %q, want Pending (no ready replicas yet)", updated.Status.Health)
	}
	if updated.Status.ObservedGeneration != 1 {
		t.Fatalf("status.observedGeneration = %d, want 1", updated.Status.ObservedGeneration)
	}
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("Ready condition = %+v, want present and False", cond)
	}
}

func TestReconcileLMCacheImageOverride(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.BackendConfig = map[string]string{"image": "registry.example.com/vllm-lmcache:pinned"}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	if got := dep.Spec.Template.Spec.Containers[0].Image; got != "registry.example.com/vllm-lmcache:pinned" {
		t.Fatalf("container image = %q, want overridden image", got)
	}
}

func TestReconcileLMCacheIdempotent(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")
	depRV := getDeployment(t, r, "cache", "ns1").ResourceVersion
	var svc1 corev1.Service
	if err := r.Get(context.Background(), types.NamespacedName{Name: "cache", Namespace: "ns1"}, &svc1); err != nil {
		t.Fatalf("get service: %v", err)
	}
	svcRV := svc1.ResourceVersion

	reconcile(t, r, "cache", "ns1")

	var deps appsv1.DeploymentList
	if err := r.List(context.Background(), &deps, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(deps.Items) != 1 {
		t.Fatalf("deployments = %d, want exactly 1 after repeated reconcile", len(deps.Items))
	}
	var svcs corev1.ServiceList
	if err := r.List(context.Background(), &svcs, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list services: %v", err)
	}
	if len(svcs.Items) != 1 {
		t.Fatalf("services = %d, want exactly 1 after repeated reconcile", len(svcs.Items))
	}

	// A converged reconcile must not rewrite children, or the Owns() watch spins a
	// hot-loop. The fake client bumps ResourceVersion on every write.
	if got := getDeployment(t, r, "cache", "ns1").ResourceVersion; got != depRV {
		t.Fatalf("deployment ResourceVersion changed on no-op reconcile: %q -> %q", depRV, got)
	}
	if got := svcs.Items[0].ResourceVersion; got != svcRV {
		t.Fatalf("service ResourceVersion changed on no-op reconcile: %q -> %q", svcRV, got)
	}
}

func TestReconcileLMCacheUpdatesImage(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.BackendConfig = map[string]string{"image": "example.com/vllm-lmcache:v2"}
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update image: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if got := getDeployment(t, r, "cache", "ns1").Spec.Template.Spec.Containers[0].Image; got != "example.com/vllm-lmcache:v2" {
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

func TestReconcileLMCacheReadyWhenReplicasAvailable(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	dep.Status.ReadyReplicas = 1
	if err := r.Status().Update(context.Background(), dep); err != nil {
		t.Fatalf("update deployment status: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	if updated.Status.Health != cachev1alpha1.CacheBackendHealthReady {
		t.Fatalf("status.health = %q, want Ready", updated.Status.Health)
	}
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %+v, want True", cond)
	}
}

func TestReconcileStatefulSetKindDeferred(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	var deps appsv1.DeploymentList
	if err := r.List(context.Background(), &deps, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(deps.Items) != 0 {
		t.Fatalf("deployments = %d, want 0 (StatefulSet kind deferred to C3)", len(deps.Items))
	}
}

func TestReconcileUnmanagedTypeNoop(t *testing.T) {
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeMooncake},
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

func TestReconcileExternalMirrorsEndpointToStatus(t *testing.T) {
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: "external-cache.default.svc:8080",
		},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "example", "default")

	if got := getBackend(t, r, "example", "default").Status.Endpoint; got != "external-cache.default.svc:8080" {
		t.Fatalf("status.endpoint = %q, want spec endpoint", got)
	}
}

func TestReconcileExternalClearsRemovedEndpoint(t *testing.T) {
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeExternal},
		Status:     cachev1alpha1.CacheBackendStatus{Endpoint: "stale-cache.default.svc:8080"},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "example", "default")

	if got := getBackend(t, r, "example", "default").Status.Endpoint; got != "" {
		t.Fatalf("status.endpoint = %q, want empty", got)
	}
}

func TestReconcileIgnoresMissingObject(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme)

	reconcile(t, r, "missing", "default")
}

func containsStr(s []string, want string) bool {
	for _, v := range s {
		if v == want {
			return true
		}
	}
	return false
}

func hasEnv(env []corev1.EnvVar, name, value string) bool {
	for _, e := range env {
		if e.Name == name {
			return e.Value == value
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
