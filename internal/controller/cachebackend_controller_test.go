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
	"k8s.io/apimachinery/pkg/util/intstr"
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
	if c.Name != "lmcache-server" {
		t.Fatalf("container name = %q, want lmcache-server (standalone server, not the all-in-one vLLM)", c.Name)
	}
	if c.Image == "" {
		t.Fatalf("container image is empty")
	}
	if !containsStr(c.Command, "lmcache_server") {
		t.Fatalf("container command = %v, want to start with lmcache_server", c.Command)
	}
	if len(c.Ports) != 1 || c.Ports[0].ContainerPort != 65432 {
		t.Fatalf("ports = %v, want exactly one port on 65432 (lm:// scheme)", c.Ports)
	}

	svc := &corev1.Service{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "cache", Namespace: "ns1"}, svc); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("service type = %q, want ClusterIP", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 65432 {
		t.Fatalf("service ports = %v, want exactly one port on 65432", svc.Spec.Ports)
	}
	if so := metav1.GetControllerOf(svc); so == nil || so.Name != "cache" {
		t.Fatalf("service controller owner = %+v, want CacheBackend/cache", so)
	}
	wantSelector := map[string]string{
		"app.kubernetes.io/name":       "cachebackend",
		"app.kubernetes.io/instance":   "cache",
		"app.kubernetes.io/managed-by": "inference-cache-controller",
	}
	for k, v := range wantSelector {
		if svc.Spec.Selector[k] != v {
			t.Fatalf("service selector[%q] = %q, want %q", k, svc.Spec.Selector[k], v)
		}
	}

	updated := getBackend(t, r, "cache", "ns1")
	wantEndpoint := "cache.ns1.svc.cluster.local:65432"
	if updated.Status.Endpoint != wantEndpoint {
		t.Fatalf("status.endpoint = %q, want %q (engine-agnostic host:port; lm:// prefix is the adapter's job)", updated.Status.Endpoint, wantEndpoint)
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
	cb.Spec.BackendConfig = map[string]string{"image": "registry.example.com/lmcache-server:pinned"}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	if got := dep.Spec.Template.Spec.Containers[0].Image; got != "registry.example.com/lmcache-server:pinned" {
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
	live.Spec.BackendConfig = map[string]string{"image": "example.com/lmcache-server:v2"}
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

func TestReconcileLMCacheReadyWhenReplicasAvailable(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.Replicas = 1
	dep.Status.UpdatedReplicas = 1
	dep.Status.AvailableReplicas = 1
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

func TestManagedHealthGatesReadyOnRollout(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(2)

	cases := []struct {
		name string
		dep  appsv1.Deployment
		want cachev1alpha1.CacheBackendHealth
	}{
		{
			name: "fresh create, nothing ready",
			dep:  appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Generation: 1}},
			want: cachev1alpha1.CacheBackendHealthPending,
		},
		{
			name: "stale rollout after image change (old pods still available)",
			dep: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
				Status:     appsv1.DeploymentStatus{ObservedGeneration: 1, UpdatedReplicas: 0, AvailableReplicas: 2, ReadyReplicas: 2},
			},
			want: cachev1alpha1.CacheBackendHealthPending,
		},
		{
			name: "rolled out and available",
			dep: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
				Status:     appsv1.DeploymentStatus{ObservedGeneration: 2, UpdatedReplicas: 2, AvailableReplicas: 2, ReadyReplicas: 2},
			},
			want: cachev1alpha1.CacheBackendHealthReady,
		},
		{
			name: "rolled out but replicas unavailable",
			dep: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
				Status:     appsv1.DeploymentStatus{ObservedGeneration: 2, UpdatedReplicas: 2, AvailableReplicas: 1, ReadyReplicas: 1},
			},
			want: cachev1alpha1.CacheBackendHealthDegraded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, _, _ := managedHealth(cb, &tc.dep)
			if got != tc.want {
				t.Fatalf("managedHealth = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestManagedHealthZeroReplicasNotReady(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(0)
	// Even a fully-observed Deployment with 0/0 replicas must not be Ready.
	dep := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 1},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 1},
	}
	if got, status, _, _ := managedHealth(cb, &dep); got == cachev1alpha1.CacheBackendHealthReady || status == metav1.ConditionTrue {
		t.Fatalf("managedHealth for 0 replicas = %q/%v, want non-Ready", got, status)
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

func TestReconcileTypeSwitchToExternalCleansUpChildren(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")
	// Child workload exists.
	if _, err := getOptionalDeployment(t, r, "cache", "ns1"); err != nil {
		t.Fatalf("expected deployment after managed reconcile: %v", err)
	}

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	live.Spec.Endpoint = "external.ns1.svc:8080"
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("switch to external: %v", err)
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
	if got := getBackend(t, r, "cache", "ns1").Status.Endpoint; got != "external.ns1.svc:8080" {
		t.Fatalf("status.endpoint = %q, want mirrored external endpoint", got)
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

func TestReconcileSwitchToStatefulSetClearsStaleStatus(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")
	if ep := getBackend(t, r, "cache", "ns1").Status.Endpoint; ep == "" {
		t.Fatalf("expected a published endpoint after managed reconcile")
	}

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("switch to StatefulSet kind: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	if updated.Status.Endpoint != "" {
		t.Fatalf("status.endpoint = %q, want cleared after no longer managed", updated.Status.Endpoint)
	}
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond != nil {
		t.Fatalf("Ready condition = %+v, want removed", cond)
	}
	var deps appsv1.DeploymentList
	if err := r.List(context.Background(), &deps, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(deps.Items) != 0 {
		t.Fatalf("deployments = %d, want 0 after switch to StatefulSet kind", len(deps.Items))
	}
}

func TestReconcileExternalAdvancesObservedGeneration(t *testing.T) {
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "ext", Namespace: "default", Generation: 7},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: "external.default.svc:8080",
		},
		Status: cachev1alpha1.CacheBackendStatus{Endpoint: "external.default.svc:8080"},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "ext", "default")

	// Endpoint is unchanged, but observedGeneration must still advance.
	if got := getBackend(t, r, "ext", "default").Status.ObservedGeneration; got != 7 {
		t.Fatalf("status.observedGeneration = %d, want 7", got)
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

func TestReconcileLMCacheCaseInsensitiveEngine(t *testing.T) {
	// Common user spellings ("vLLM", "VLLM") must route to the canonical
	// RuntimeVLLM, not silently drop the CR into the unmanaged path.
	for _, engine := range []string{"vLLM", "VLLM", "vllm"} {
		t.Run(engine, func(t *testing.T) {
			scheme := newScheme(t)
			cb := lmcacheBackend("cache", "ns1")
			cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: engine}
			r := newReconciler(scheme, cb)

			reconcile(t, r, "cache", "ns1")

			dep, err := getOptionalDeployment(t, r, "cache", "ns1")
			if err != nil {
				t.Fatalf("expected a managed Deployment for engine=%q, got error: %v", engine, err)
			}
			if got := dep.Spec.Template.Spec.Containers[0].Name; got != "lmcache-server" {
				t.Fatalf("container = %q, want lmcache-server (engine=%q must resolve to RuntimeVLLM)", got, engine)
			}
		})
	}
}

func TestReconcileLMCacheUpgradeFromColocatedAllInOne(t *testing.T) {
	// Upgrading an existing Deployment that the retired C2 builder created
	// (single container named "vllm" referencing pod-level volumes
	// "cache-home" + "shm") to the C6 standalone shape (single container
	// named "lmcache-server", no pod-level volumes) must REPLACE both the
	// container set AND the dangling adapter-owned volumes. Leaving the old
	// volumes would carry stale config from the previous shape forever.
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
			t.Fatalf("stale C2 volume %q survived the upgrade: %v", v.Name, volumeNames(pod.Volumes))
		}
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

// The pod-spec reconcile helpers are tested as pure functions because the
// fake client doesn't set CreationTimestamp on Update, which means the
// CreateOrUpdate mutate path in applyDeployment always takes the "create"
// branch (wholesale Spec copy). Testing the in-place update branch the way
// real-apiserver reconciles take requires direct calls; an envtest covering
// the same lives behind KUBEBUILDER_ASSETS.

func TestReconcileManagedPodSpecPrunesStaleContainersAndVolumesOnUpgrade(t *testing.T) {
	// Simulates a live Deployment from C2's colocated all-in-one: a "vllm"
	// container referencing pod-level cache-home + shm volumes.
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
	// The C6 standalone desired shape: a "lmcache-server" container, no
	// pod-level volumes.
	desired := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:    "lmcache-server",
				Image:   "lmcache/standalone:latest",
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
		t.Fatalf("Volumes = %v, want empty after upgrade (stale C2 volumes must be pruned)", volumeNames(live.Volumes))
	}
}

func TestReconcileManagedPodSpecPreservesVolumesOnSteadyStateUpdate(t *testing.T) {
	// When the container set is unchanged (e.g. user just bumps the image
	// override), Volumes must NOT be rewritten — overwriting them on every
	// reconcile would churn the Owns(Deployment) watch.
	original := []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	live := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "lmcache-server", Image: "lmcache/standalone:v1"}},
		Volumes:    original,
	}
	desired := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "lmcache-server", Image: "lmcache/standalone:v2"}},
		// desired carries no Volumes — a steady-state reconcile must not
		// drop the live ones just because desired declines to specify any.
	}

	reconcileManagedPodSpec(live, desired)

	if got := live.Containers[0].Image; got != "lmcache/standalone:v2" {
		t.Fatalf("container image = %q, want updated to v2", got)
	}
	if len(live.Volumes) != 1 || live.Volumes[0].Name != "data" {
		t.Fatalf("Volumes = %v, want original [data] preserved on steady-state update", volumeNames(live.Volumes))
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

func TestStringSetEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b map[string]struct{}
		want bool
	}{
		{"both empty", map[string]struct{}{}, map[string]struct{}{}, true},
		{"same keys", map[string]struct{}{"x": {}, "y": {}}, map[string]struct{}{"y": {}, "x": {}}, true},
		{"different sizes", map[string]struct{}{"x": {}}, map[string]struct{}{"x": {}, "y": {}}, false},
		{"same size, different keys", map[string]struct{}{"x": {}}, map[string]struct{}{"y": {}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stringSetEqual(tc.a, tc.b); got != tc.want {
				t.Fatalf("stringSetEqual = %v, want %v", got, tc.want)
			}
		})
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

func findCondition(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}
