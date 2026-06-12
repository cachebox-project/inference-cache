package controller

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}, &appsv1.StatefulSet{}, &corev1.PersistentVolumeClaim{}).
		WithObjects(objs...).
		Build()
	return &CacheBackendReconciler{
		Client: c,
		Scheme: scheme,
		Log:    logr.Discard(),
		// Seed a real serverInstanceCascade so lifecycle tests that
		// assert on the in-process shadow / lastAt / counted maps
		// actually exercise the clear path rather than skipping
		// the check on a nil pointer.
		serverInstanceCascade: newServerInstanceCascade(),
	}
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
		Spec: cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeLMCache},
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

func getStatefulSet(t *testing.T, r *CacheBackendReconciler, name, namespace string) *appsv1.StatefulSet {
	t.Helper()
	var sts appsv1.StatefulSet
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &sts); err != nil {
		t.Fatalf("get statefulset %s/%s: %v", namespace, name, err)
	}
	return &sts
}

func getOptionalStatefulSet(t *testing.T, r *CacheBackendReconciler, name, namespace string) (*appsv1.StatefulSet, error) {
	t.Helper()
	var sts appsv1.StatefulSet
	err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &sts)
	return &sts, err
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
	if updated.Status.ObservedGeneration != 1 {
		t.Fatalf("status.observedGeneration = %d, want 1", updated.Status.ObservedGeneration)
	}
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != conditionReasonRolloutInProgress {
		t.Fatalf("Ready condition = %+v, want False/RolloutInProgress (no ready replicas yet)", cond)
	}
}

func TestReconcileLMCacheImageOverride(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.BackendConfig = map[string]string{"serverImage": "registry.example.com/lmcache-server:pinned"}
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
	live.Spec.BackendConfig = map[string]string{"serverImage": "example.com/lmcache-server:v2"}
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update image: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if got := getDeployment(t, r, "cache", "ns1").Spec.Template.Spec.Containers[0].Image; got != "example.com/lmcache-server:v2" {
		t.Fatalf("deployment image = %q, want updated image", got)
	}
}

// TestReconcileLMCacheProfileSwitchGPUToCPU is retired: the "profile"
// backendConfig key and the all-in-one vLLM+LMCache container shape it
// switched between are gone. The CacheBackend now renders a CPU-only
// standalone lmcache-server regardless of the engine the user runs
// alongside it — engine choice (GPU vs CPU image) is the user's, not a
// CacheBackend toggle. The substrate's CPU canary now exercises the engine
// pod wiring (via the future mutating webhook), not a CacheBackend profile
// switch.

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
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %+v, want True", cond)
	}
}

func TestManagedReadinessGatesReadyOnRollout(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(2)

	cases := []struct {
		name       string
		dep        appsv1.Deployment
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "fresh create, nothing ready",
			dep:        appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Generation: 1}},
			wantStatus: metav1.ConditionFalse,
			wantReason: conditionReasonRolloutInProgress,
		},
		{
			name: "stale rollout after image change (old pods still available)",
			dep: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
				Status:     appsv1.DeploymentStatus{ObservedGeneration: 1, UpdatedReplicas: 0, AvailableReplicas: 2, ReadyReplicas: 2},
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: conditionReasonRolloutInProgress,
		},
		{
			name: "rolled out and available",
			dep: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
				Status:     appsv1.DeploymentStatus{ObservedGeneration: 2, UpdatedReplicas: 2, AvailableReplicas: 2, ReadyReplicas: 2},
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: conditionReasonBackendReady,
		},
		{
			name: "rolled out but replicas unavailable",
			dep: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
				Status:     appsv1.DeploymentStatus{ObservedGeneration: 2, UpdatedReplicas: 2, AvailableReplicas: 1, ReadyReplicas: 1},
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: conditionReasonReplicasUnavailable,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, reason, _ := managedReadiness(cb, &tc.dep)
			if status != tc.wantStatus || reason != tc.wantReason {
				t.Fatalf("managedReadiness = %v/%q, want %v/%q", status, reason, tc.wantStatus, tc.wantReason)
			}
		})
	}
}

func TestManagedReadinessZeroReplicasNotReady(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(0)
	// Even a fully-observed Deployment with 0/0 replicas must not be Ready.
	dep := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 1},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 1},
	}
	if status, reason, _ := managedReadiness(cb, &dep); status == metav1.ConditionTrue {
		t.Fatalf("managedReadiness for 0 replicas = %v/%q, want non-True", status, reason)
	}
}

func TestManagedReadinessForStatefulSetUsesReadyReplicas(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(2)
	replicas := int32(2)

	cases := []struct {
		name       string
		status     appsv1.StatefulSetStatus
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name: "rolled out and ready",
			status: appsv1.StatefulSetStatus{
				ObservedGeneration: 1,
				UpdatedReplicas:    2,
				ReadyReplicas:      2,
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: conditionReasonBackendReady,
		},
		{
			name: "rolled out but not ready",
			status: appsv1.StatefulSetStatus{
				ObservedGeneration: 1,
				UpdatedReplicas:    2,
				ReadyReplicas:      1,
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: conditionReasonReplicasUnavailable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
				Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
				Status:     tc.status,
			}
			status, reason, message := managedReadinessForWorkload(cb, observationFromStatefulSet(sts))
			if status != tc.wantStatus || reason != tc.wantReason {
				t.Fatalf("managedReadinessForWorkload = %v/%q (%q), want %v/%q", status, reason, message, tc.wantStatus, tc.wantReason)
			}
			if !strings.Contains(message, "replicas ready") {
				t.Fatalf("message = %q, want StatefulSet readiness wording", message)
			}
		})
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

// TestReconcileTypeSwitchToExternalClearsObservedServerInstance asserts
// that status.observedServerInstance is cleared when a managed
// CacheBackend transitions to External — leaving a stale latch on an
// External backend would surface a UID that no longer maps to any
// controller-managed pod, and a subsequent flip back to managed
// would inherit the stale baseline and either false-cascade
// immediately or false-pin a non-existent pod set. This is the
// lifecycle contract reconcileExternal encodes; a status-field flip
// is exactly the kind of seam tests must hold, alongside the
// preserved-fields contract (firstKVEventObservedAt must survive).
func TestReconcileTypeSwitchToExternalClearsObservedServerInstance(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")
	// Plant BOTH a baseline ObservedServerInstance AND an in-memory
	// shadow value, simulating a managed period that had observed a
	// Ready cache-server pod. The test then verifies that the
	// External transition clears BOTH — without the planted shadow,
	// the shadow assertion would vacuously pass on an empty map.
	live := getBackend(t, r, "cache", "ns1")
	live.Status.ObservedServerInstance = "cache-pod-uid:0"
	if err := r.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("plant baseline observedServerInstance: %v", err)
	}
	plantedKey := cascadeKey{namespace: live.Namespace, name: live.Name, uid: string(live.UID)}
	r.serverInstanceCascade.recordAttempt(plantedKey, "cache-pod-uid:0")
	if got := r.serverInstanceCascade.lastAttempt(plantedKey); got != "cache-pod-uid:0" {
		t.Fatalf("planted shadow precondition failed: lastAttempt = %q, want %q (test would be vacuous without a planted value)", got, "cache-pod-uid:0")
	}

	// Confirm preserved fields we expect NOT to be clobbered alongside
	// the latch (firstKVEventObservedAt + indexParticipation must
	// survive the External transition per reconcileExternal's godoc).
	preserved := getBackend(t, r, "cache", "ns1")
	preserved.Status.FirstKVEventObservedAt = &metav1.Time{Time: time.Unix(1_000_000_000, 0).UTC()}
	if err := r.Status().Update(context.Background(), preserved); err != nil {
		t.Fatalf("plant firstKVEventObservedAt: %v", err)
	}

	// Switch to External.
	switching := getBackend(t, r, "cache", "ns1")
	switching.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	switching.Spec.Endpoint = "external.ns1.svc:8080"
	if err := r.Update(context.Background(), switching); err != nil {
		t.Fatalf("switch to external: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1")
	if got.Status.ObservedServerInstance != "" {
		t.Fatalf("status.observedServerInstance = %q, want cleared on managed→External transition", got.Status.ObservedServerInstance)
	}
	if got.Status.FirstKVEventObservedAt == nil {
		t.Fatalf("status.firstKVEventObservedAt was clobbered on External transition; it must survive as a monotonic latch")
	}
	// The in-memory cascade shadow MUST also be cleared. A retained
	// shadow would let a later External→managed transition resolve
	// effectivePrior to the prior-period currentID and false-cascade
	// the engine fleet on the first new Ready pod.
	if shadow := r.serverInstanceCascade.lastAttempt(plantedKey); shadow != "" {
		t.Fatalf("cascade shadow = %q after managed→External transition; want cleared (a lingering shadow would false-cascade on the return path)", shadow)
	}
}

// TestReconcileSwitchToUnsupportedTypeClearsObservedServerInstance asserts
// the same clearing for the managed→unsupported-runtime transition
// (reconcileUnmanaged path).
func TestReconcileSwitchToUnsupportedTypeClearsObservedServerInstance(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")
	live := getBackend(t, r, "cache", "ns1")
	live.Status.ObservedServerInstance = "cache-pod-uid:0"
	if err := r.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("plant baseline observedServerInstance: %v", err)
	}
	plantedKey := cascadeKey{namespace: live.Namespace, name: live.Name, uid: string(live.UID)}
	r.serverInstanceCascade.recordAttempt(plantedKey, "cache-pod-uid:0")
	if got := r.serverInstanceCascade.lastAttempt(plantedKey); got != "cache-pod-uid:0" {
		t.Fatalf("planted shadow precondition failed: lastAttempt = %q, want %q", got, "cache-pod-uid:0")
	}

	switching := getBackend(t, r, "cache", "ns1")
	switching.Spec.Type = cachev1alpha1.CacheBackendTypeMooncake
	if err := r.Update(context.Background(), switching); err != nil {
		t.Fatalf("switch to unsupported type: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1")
	if got.Status.ObservedServerInstance != "" {
		t.Fatalf("status.observedServerInstance = %q, want cleared on managed→unmanaged transition", got.Status.ObservedServerInstance)
	}
	// In-memory shadow must also be cleared on the unmanaged path.
	if shadow := r.serverInstanceCascade.lastAttempt(plantedKey); shadow != "" {
		t.Fatalf("cascade shadow = %q after managed→unmanaged transition; want cleared", shadow)
	}
}

// TestReconcileSwitchToInvalidStorageClearsObservedServerInstance
// asserts that the InvalidStorage gate (reconcileInvalidStorage)
// clears status.observedServerInstance AND wipes the in-process
// cascade shadow. Without the clearing, a backend whose
// configuration is later corrected would inherit the prior-period
// baseline and false-cascade on the first new Ready pod even
// though the engine fleet has been waiting on a fail-open backend
// the whole time.
func TestReconcileSwitchToInvalidStorageClearsObservedServerInstance(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")
	// Plant a baseline AND a shadow value as if a prior managed
	// period had observed a Ready cache-server pod. Without planting
	// the shadow, the post-transition shadow assertion would pass
	// vacuously on an empty map.
	live := getBackend(t, r, "cache", "ns1")
	live.Status.ObservedServerInstance = "cache-pod-uid:0"
	if err := r.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("plant baseline observedServerInstance: %v", err)
	}
	plantedKey := cascadeKey{namespace: live.Namespace, name: live.Name, uid: string(live.UID)}
	r.serverInstanceCascade.recordAttempt(plantedKey, "cache-pod-uid:0")
	if got := r.serverInstanceCascade.lastAttempt(plantedKey); got != "cache-pod-uid:0" {
		t.Fatalf("planted shadow precondition failed: lastAttempt = %q, want %q", got, "cache-pod-uid:0")
	}

	// Flip into the InvalidStorage gate: replicas > 1 with a single
	// ReadWriteOnce PVC is the canonical trigger.
	switching := getBackend(t, r, "cache", "ns1")
	two := int32(2)
	switching.Spec.Replicas = &two
	switching.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{
		PVC: &cachev1alpha1.CacheBackendPVCSpec{
			Size: resource.MustParse("10Gi"),
		},
	}
	if err := r.Update(context.Background(), switching); err != nil {
		t.Fatalf("switch to invalid storage: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1")
	if got.Status.ObservedServerInstance != "" {
		t.Fatalf("status.observedServerInstance = %q, want cleared on InvalidStorage gate", got.Status.ObservedServerInstance)
	}
	if shadow := r.serverInstanceCascade.lastAttempt(plantedKey); shadow != "" {
		t.Fatalf("cascade shadow = %q after InvalidStorage gate; want cleared (lingering shadow would false-cascade when the operator fixes the config)", shadow)
	}
}

// TestReconcileLifecycleExitsClearProbeRateLimiter pins the cleanup hook
// every lifecycle-exit path must call — without it, a CR that returns to
// the managed path within the prior 30s window keeps a stale lastCalled
// timestamp on r.probeLimiter, the very first reconcile on re-entry skips
// the /probe call entirely (rate-limited), and Ready=True is published
// with no fresh FunctionalProbeOK verdict to back it. One table-driven
// test for the three places that must call probeLimiter.forget:
// reconcileExternal, reconcileUnmanaged, reconcileInvalidStorage.
func TestReconcileLifecycleExitsClearProbeRateLimiter(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*cachev1alpha1.CacheBackend)
	}{
		{
			name: "managed → External",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
				cb.Spec.Endpoint = "external.ns1.svc:8080"
			},
		},
		{
			name: "managed → Unmanaged (unsupported type)",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.Type = cachev1alpha1.CacheBackendTypeMooncake
			},
		},
		{
			name: "managed → InvalidStorage gate",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				two := int32(2)
				cb.Spec.Replicas = &two
				cb.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{
					PVC: &cachev1alpha1.CacheBackendPVCSpec{Size: resource.MustParse("10Gi")},
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newScheme(t)
			r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))
			reconcile(t, r, "cache", "ns1")

			// Plant a rate-limit entry so the clearing assertion is not
			// vacuous on an empty map. The key matches what
			// evaluateFunctionalProbe + the lifecycle exits use:
			// "namespace/name".
			key := "ns1/cache"
			now := time.Unix(2_000_000, 0)
			r.probeLimiter.markCalled(key, now)
			if got := r.probeLimiter.lastCalled(key); got != now {
				t.Fatalf("planted rate-limit precondition failed: lastCalled = %v, want %v (test would be vacuous without a planted value)", got, now)
			}

			// Trigger the lifecycle exit.
			switching := getBackend(t, r, "cache", "ns1")
			tc.mutate(switching)
			if err := r.Update(context.Background(), switching); err != nil {
				t.Fatalf("apply lifecycle-exit spec change: %v", err)
			}
			reconcile(t, r, "cache", "ns1")

			// The rate-limit entry MUST be cleared. A retained entry would
			// suppress the first /probe call on the managed → exit → managed
			// return path inside the 30s window.
			if got := r.probeLimiter.lastCalled(key); !got.IsZero() {
				t.Fatalf("probeLimiter.lastCalled(%q) = %v after lifecycle exit; want zero (forget) — re-entry inside the 30s window would skip the first probe call", key, got)
			}
		})
	}
}

func TestReconcileStatefulSetKindCreatesStatefulSet(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
	cb.Spec.Replicas = ptrInt32(2)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	if _, err := getOptionalDeployment(t, r, "cache", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("StatefulSet deploymentKind must not provision a Deployment; get err=%v", err)
	}
	sts := getStatefulSet(t, r, "cache", "ns1")
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 2 {
		t.Fatalf("statefulset replicas = %v, want 2", sts.Spec.Replicas)
	}
	if sts.Spec.ServiceName != "cache" {
		t.Fatalf("statefulset serviceName = %q, want cache", sts.Spec.ServiceName)
	}
	owner := metav1.GetControllerOf(sts)
	if owner == nil || owner.Kind != "CacheBackend" || owner.Name != "cache" || owner.Controller == nil || !*owner.Controller {
		t.Fatalf("statefulset controller owner = %+v, want controller ref to CacheBackend/cache", owner)
	}
	if ep := getBackend(t, r, "cache", "ns1").Status.Endpoint; ep != "cache.ns1.svc.cluster.local:65432" {
		t.Fatalf("status.endpoint = %q, want service endpoint", ep)
	}
}

func TestReconcileStatefulSetDoesNotScheduleDeploymentCascadePoll(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
	r := newReconciler(scheme, cb)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	})
	if err != nil {
		t.Fatalf("reconcile StatefulSet backend: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("StatefulSet reconcile RequeueAfter = %s, want 0; Deployment restart-cascade polling does not apply", result.RequeueAfter)
	}
}

func TestReconcileSwitchToStatefulSetShedsDeployment(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")
	if ep := getBackend(t, r, "cache", "ns1").Status.Endpoint; ep == "" {
		t.Fatalf("expected a published endpoint after managed reconcile")
	}
	baseline := getBackend(t, r, "cache", "ns1")
	baseline.Status.ObservedServerInstance = "deployment-pod-uid:0"
	if err := r.Status().Update(context.Background(), baseline); err != nil {
		t.Fatalf("plant observedServerInstance: %v", err)
	}
	plantedKey := cascadeKey{namespace: baseline.Namespace, name: baseline.Name, uid: string(baseline.UID)}
	r.serverInstanceCascade.recordAttempt(plantedKey, "deployment-pod-uid:0")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("switch to StatefulSet kind: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	if updated.Status.Endpoint != "cache.ns1.svc.cluster.local:65432" {
		t.Fatalf("status.endpoint = %q, want service endpoint after switching to StatefulSet", updated.Status.Endpoint)
	}
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond == nil || cond.Reason != conditionReasonRolloutInProgress {
		t.Fatalf("Ready condition = %+v, want rollout status from StatefulSet", cond)
	}
	if updated.Status.ObservedServerInstance != "" {
		t.Fatalf("status.observedServerInstance = %q, want cleared after switch to StatefulSet", updated.Status.ObservedServerInstance)
	}
	if shadow := r.serverInstanceCascade.lastAttempt(plantedKey); shadow != "" {
		t.Fatalf("cascade shadow = %q after switch to StatefulSet; want cleared", shadow)
	}
	if _, err := getOptionalDeployment(t, r, "cache", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("deployment should be deleted after switch to StatefulSet kind; get err=%v", err)
	}
	getStatefulSet(t, r, "cache", "ns1")
}

func TestReconcileSwitchFromStatefulSetToDeploymentShedsStatefulSet(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	getStatefulSet(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindDeployment
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("switch to Deployment kind: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if _, err := getOptionalStatefulSet(t, r, "cache", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("statefulset should be deleted after switch to Deployment kind; get err=%v", err)
	}
	getDeployment(t, r, "cache", "ns1")
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

func TestReconcileExternalSetsReadyTrue(t *testing.T) {
	// External admission accepts spec.endpoint at write time, so the
	// readiness signal is "operator says this endpoint exists and we
	// accepted it" — there's no Service to wait on. Consumers (the
	// future readiness gate, kubectl get cb, the indexParticipation
	// poller for External) must see Ready=True.
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "ext", Namespace: "default", Generation: 3},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: "ext.default.svc:8080",
		},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "ext", "default")

	got := getBackend(t, r, "ext", "default")
	ready := findCondition(got.Status.Conditions, "Ready")
	if ready == nil {
		t.Fatalf("Ready condition missing; conditions = %v", got.Status.Conditions)
	}
	if ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready status = %q, want %q", ready.Status, metav1.ConditionTrue)
	}
	if ready.Reason != "ExternalEndpointAccepted" {
		t.Fatalf("Ready reason = %q, want ExternalEndpointAccepted", ready.Reason)
	}
	if ready.ObservedGeneration != 3 {
		t.Fatalf("Ready.observedGeneration = %d, want 3", ready.ObservedGeneration)
	}
	progressing := findCondition(got.Status.Conditions, "Progressing")
	if progressing == nil {
		t.Fatalf("Progressing condition missing; conditions = %v", got.Status.Conditions)
	}
	if progressing.Status != metav1.ConditionFalse {
		t.Fatalf("Progressing status = %q, want %q", progressing.Status, metav1.ConditionFalse)
	}
}

func TestReconcileExternalInvalidEndpointSetsReadyFalse(t *testing.T) {
	// An External CR with a non-empty but malformed spec.endpoint must
	// be marked Ready=False/ExternalEndpointInvalid — current admission
	// rejects these at write time, but a CR stored before the shape
	// rule shipped can still carry e.g. `https://...`. Without this,
	// the controller would advertise the broken value as Ready=True
	// and the pod webhook would inject a URL the engine can't parse.
	scheme := newScheme(t)
	for _, tc := range []struct {
		name, endpoint string
	}{
		{"bad-scheme", "https://cache.example.com:443/api"},
		{"portless-host", "cache.example.com"},
		{"unbracketed-ipv6", "2001:db8::1"},
		{"embedded-whitespace", "cache example:8200"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cb := &cachev1alpha1.CacheBackend{
				ObjectMeta: metav1.ObjectMeta{Name: "ext-bad", Namespace: "default"},
				Spec: cachev1alpha1.CacheBackendSpec{
					Type:     cachev1alpha1.CacheBackendTypeExternal,
					Endpoint: tc.endpoint,
				},
			}
			r := newReconciler(scheme, cb)
			reconcile(t, r, "ext-bad", "default")

			got := getBackend(t, r, "ext-bad", "default")
			ready := findCondition(got.Status.Conditions, "Ready")
			if ready == nil || ready.Status != metav1.ConditionFalse {
				t.Fatalf("Ready condition = %+v, want Status=False", ready)
			}
			if ready.Reason != "ExternalEndpointInvalid" {
				t.Fatalf("Ready reason = %q, want ExternalEndpointInvalid", ready.Reason)
			}
		})
	}
}

func TestReconcileExternalEmptyEndpointSetsReadyFalse(t *testing.T) {
	// Admission rejects this case at the webhook, but a CR already in etcd
	// from before the webhook was installed must still publish a visible
	// Ready=False so operators can see why the CR isn't usable instead of
	// finding the condition simply absent.
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "ext-no-ep", Namespace: "default"},
		Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeExternal},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "ext-no-ep", "default")

	got := getBackend(t, r, "ext-no-ep", "default")
	ready := findCondition(got.Status.Conditions, "Ready")
	if ready == nil || ready.Status != metav1.ConditionFalse {
		t.Fatalf("Ready condition = %+v, want Status=False", ready)
	}
	if ready.Reason != "ExternalEndpointMissing" {
		t.Fatalf("Ready reason = %q, want ExternalEndpointMissing", ready.Reason)
	}
	// Progressing reason mirrors Ready's reason on the missing path so
	// `kubectl describe` shows a coherent pair.
	progressing := findCondition(got.Status.Conditions, "Progressing")
	if progressing == nil || progressing.Reason != "ExternalEndpointMissing" {
		t.Fatalf("Progressing = %+v, want reason ExternalEndpointMissing", progressing)
	}
}

func TestReconcileExternalWhitespaceEndpointTreatedAsMissing(t *testing.T) {
	// Admission rejects a whitespace-only spec.endpoint, but a CR already
	// in etcd from before admission was installed can still carry one.
	// The reconciler must treat it as missing — publishing a raw
	// "LMCACHE_REMOTE_URL=lm://   " to the engine env is worse than
	// publishing nothing, and Ready=True on whitespace would mislead
	// every downstream consumer.
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "ext-ws", Namespace: "default"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: "   \t  ",
		},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "ext-ws", "default")

	got := getBackend(t, r, "ext-ws", "default")
	if got.Status.Endpoint != "" {
		t.Fatalf("status.endpoint = %q, want empty (whitespace must be trimmed)", got.Status.Endpoint)
	}
	ready := findCondition(got.Status.Conditions, "Ready")
	if ready == nil || ready.Status != metav1.ConditionFalse {
		t.Fatalf("Ready = %+v, want Status=False", ready)
	}
	if ready.Reason != "ExternalEndpointMissing" {
		t.Fatalf("Ready reason = %q, want ExternalEndpointMissing", ready.Reason)
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
	return &CacheBackendReconciler{
		Client:                c,
		Scheme:                scheme,
		Log:                   logr.Discard(),
		serverInstanceCascade: newServerInstanceCascade(),
	}
}

// TestReconcileLMCacheConflictThenConverge guards against a stuck-Degraded
// regression: a Deployment Update inside applyDeployment races the kube
// Deployment controller's status writes and returns 409. Without retry, the
// reconcile aborts and the CR's Ready condition is frozen at whatever the
// last successful pass observed — typically "pod not yet Ready". With
// RetryOnConflict in place, apply converges within a reconcile pass and the
// CR reports Ready=True once the underlying Deployment is healthy.
func TestReconcileLMCacheConflictThenConverge(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)

	var conflictsRemaining int32 = 3 // first 3 Deployment Updates → 409
	funcs := interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok {
				if atomic.LoadInt32(&conflictsRemaining) > 0 {
					atomic.AddInt32(&conflictsRemaining, -1)
					return apierrors.NewConflict(
						schema.GroupResource{Group: "apps", Resource: "deployments"},
						obj.GetName(),
						errors.New("the object has been modified; please apply your changes to the latest version and try again"),
					)
				}
			}
			return c.Update(ctx, obj, opts...)
		},
	}
	r := newReconcilerWithInterceptor(scheme, funcs, cb)

	// First reconcile creates the Deployment + Service (Create, not Update —
	// no conflict possible at this step).
	reconcile(t, r, "cache", "ns1")
	markDeploymentReady(t, r, "cache", "ns1", 1)

	// Force the next reconcile to issue a real Update against the Deployment.
	// (Image override mutates the managed container in-place; a no-op reconcile
	// would not call Update at all.)
	live := getBackend(t, r, "cache", "ns1")
	live.Spec.BackendConfig = map[string]string{"serverImage": "example.com/lmcache-server:v9"}
	live.Generation = 2
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update CR: %v", err)
	}

	// Second reconcile: applyDeployment hits 3 conflicts, retries through them,
	// eventually succeeds; the status step then publishes Ready.
	reconcile(t, r, "cache", "ns1")

	if remaining := atomic.LoadInt32(&conflictsRemaining); remaining != 0 {
		t.Fatalf("conflictsRemaining = %d, want 0 (RetryOnConflict should consume them)", remaining)
	}
	if got := getDeployment(t, r, "cache", "ns1").Spec.Template.Spec.Containers[0].Image; got != "example.com/lmcache-server:v9" {
		t.Fatalf("deployment image = %q, want %q (apply did not converge under conflict)", got, "example.com/lmcache-server:v9")
	}
	updated := getBackend(t, r, "cache", "ns1")
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %+v, want True (CR is stuck despite Deployment being ready)", cond)
	}
}

// TestReconcileLMCacheStatusIndependentOfApplyError pins the other half of the
// fix: status is derived from the live Deployment, not gated on apply success.
// Even when apply fails (here, a Forbidden as if an admission webhook rejected
// the write), the CR must still publish what the Deployment reports instead
// of remaining frozen at a stale snapshot.
//
// At the same time, Status.ObservedGeneration must NOT advance to the new CR
// generation when apply for that generation failed — otherwise clients can't
// tell from the status whether the controller has caught up.
func TestReconcileLMCacheStatusIndependentOfApplyError(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)

	var blockDeploymentUpdate atomic.Bool
	gr := schema.GroupResource{Group: "apps", Resource: "deployments"}
	funcs := interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok && blockDeploymentUpdate.Load() {
				return apierrors.NewForbidden(gr, obj.GetName(), errors.New("denied by admission webhook"))
			}
			return c.Update(ctx, obj, opts...)
		},
	}
	r := newReconcilerWithInterceptor(scheme, funcs, cb)

	reconcile(t, r, "cache", "ns1")
	markDeploymentReady(t, r, "cache", "ns1", 1)
	// Sanity: after a successful first reconcile, observedGeneration tracks
	// the CR generation (1).
	if got := getBackend(t, r, "cache", "ns1").Status.ObservedGeneration; got != 1 {
		t.Fatalf("status.observedGeneration after successful first reconcile = %d, want 1", got)
	}

	// Now flip the gate so the next applyDeployment Update is rejected. Force
	// an Update to happen by changing the image in the CR.
	blockDeploymentUpdate.Store(true)
	live := getBackend(t, r, "cache", "ns1")
	live.Spec.BackendConfig = map[string]string{"serverImage": "example.com/lmcache-server:v9"}
	live.Generation = 2
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update CR: %v", err)
	}

	// Reconcile must surface an error (so controller-runtime requeues) but the
	// status pass must still publish observed state from the live Deployment.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	}); err == nil {
		t.Fatalf("reconcile returned nil, want error (apply was blocked)")
	}

	updated := getBackend(t, r, "cache", "ns1")
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %+v, want True (status must reflect live Deployment regardless of apply error)", cond)
	}
	// Apply for generation 2 failed, so observedGeneration must NOT have
	// advanced to 2 — it should still report 1 (the last generation we
	// successfully drove the live state toward). Same for the Ready
	// condition's ObservedGeneration, so the (status, condition) pair stays
	// internally consistent.
	if got := updated.Status.ObservedGeneration; got != 1 {
		t.Fatalf("status.observedGeneration = %d, want 1 (apply failed; must not advance to current CR gen)", got)
	}
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond == nil || cond.ObservedGeneration != 1 {
		t.Fatalf("Ready condition ObservedGeneration = %d, want 1", cond.ObservedGeneration)
	}
	if got := getDeployment(t, r, "cache", "ns1").Spec.Template.Spec.Containers[0].Image; got == "example.com/lmcache-server:v9" {
		t.Fatalf("deployment image was updated despite Forbidden — interceptor was not exercised")
	}
}

// TestReconcileLMCacheEndpointHeldUntilServiceExists pins the endpoint
// invariant: Status.Endpoint must only advertise an address that corresponds
// to a *live* Service. When applyService is rejected on the first reconcile
// (so the Service was never created), the CR must not publish the desired
// endpoint — clients/gateways would route to a non-existent target.
func TestReconcileLMCacheEndpointHeldUntilServiceExists(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)

	gr := schema.GroupResource{Group: "", Resource: "services"}
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*corev1.Service); ok {
				return apierrors.NewForbidden(gr, obj.GetName(), errors.New("denied by admission webhook"))
			}
			return c.Create(ctx, obj, opts...)
		},
	}
	r := newReconcilerWithInterceptor(scheme, funcs, cb)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	}); err == nil {
		t.Fatalf("reconcile returned nil, want error (Service create was blocked)")
	}

	// Deployment is created (only Service apply was blocked), so the status
	// pass runs and publishes the Ready + Progressing conditions. But
	// Status.Endpoint must stay empty until a live Service backs it.
	if _, err := getOptionalDeployment(t, r, "cache", "ns1"); err != nil {
		t.Fatalf("expected deployment to be created (only Service was blocked): %v", err)
	}
	if got := getBackend(t, r, "cache", "ns1").Status.Endpoint; got != "" {
		t.Fatalf("status.endpoint = %q, want \"\" (no live Service exists yet)", got)
	}
}

// TestReconcileLMCacheDeploymentVanishedAfterApply pins the behavior when the
// managed Deployment disappears between a successful apply and the post-apply
// Get (out-of-band delete, GC). Reconcile must NOT silently report success —
// there is no observed state to publish, so the controller must requeue.
//
// The interceptor counts Deployment Get calls in the reconcile pass under
// test: the 1st Get is inside applyDeployment's CreateOrUpdate (Get-then-
// Update — must pass through so apply itself succeeds); the 2nd Get is the
// post-apply read in reconcileManaged, which we swallow as NotFound to
// simulate the live object being deleted between those two steps.
func TestReconcileLMCacheDeploymentVanishedAfterApply(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)

	var depGetCount atomic.Int32
	var armed atomic.Bool
	funcs := interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok && armed.Load() {
				if depGetCount.Add(1) == 2 {
					return apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "deployments"}, key.Name)
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}
	r := newReconcilerWithInterceptor(scheme, funcs, cb)

	// First reconcile creates everything and publishes initial status.
	reconcile(t, r, "cache", "ns1")
	markDeploymentReady(t, r, "cache", "ns1", 1)

	// Arm the interceptor for the next reconcile. The 1st Deployment Get
	// (inside applyDeployment's CreateOrUpdate) passes through so apply
	// converges as normal; the 2nd Get (the post-apply read in
	// reconcileManaged) returns NotFound, simulating the live Deployment
	// being deleted between apply and Get within the same reconcile pass.
	armed.Store(true)
	defer armed.Store(false)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	}); err == nil {
		t.Fatalf("reconcile returned nil, want error (Deployment vanished after apply)")
	}
	if got := depGetCount.Load(); got < 2 {
		t.Fatalf("expected at least 2 Deployment Gets in the armed reconcile; got %d", got)
	}
}

// TestReconcileLMCacheDeploymentLosesOwnershipAfterApply pins the foreign-
// ownership race on the no-apply-error path: applyDeployment succeeded (we
// own the live Deployment after Update), but between Update and the post-
// apply Get, the live Deployment's controller ref was changed out-of-band.
// Returning nil there would silently report success AND, since we no longer
// own the object, the Owns() watch would stop delivering events — so the
// CacheBackend would never re-reconcile. Reconcile must therefore synthesize
// an error to requeue.
func TestReconcileLMCacheDeploymentLosesOwnershipAfterApply(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)

	// The interceptor strips Deployment owner refs on the 2nd Get per
	// reconcile (the post-apply read). The 1st Get (inside applyDeployment's
	// CreateOrUpdate) passes through unchanged so apply itself succeeds —
	// the bug we're guarding is only visible *after* a successful apply.
	var depGetCount atomic.Int32
	var armed atomic.Bool
	otherCtrl := true
	foreignOwner := metav1.OwnerReference{
		APIVersion: "example.com/v1", Kind: "OtherKind",
		Name: "other", UID: "other-uid",
		Controller: &otherCtrl, BlockOwnerDeletion: &otherCtrl,
	}
	funcs := interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if dep, ok := obj.(*appsv1.Deployment); ok && armed.Load() {
				if depGetCount.Add(1) == 2 {
					dep.OwnerReferences = []metav1.OwnerReference{foreignOwner}
				}
			}
			return nil
		},
	}
	r := newReconcilerWithInterceptor(scheme, funcs, cb)

	// First reconcile lands the Deployment with us as controller (no event
	// from the interceptor yet — armed is still false).
	reconcile(t, r, "cache", "ns1")
	markDeploymentReady(t, r, "cache", "ns1", 1)

	armed.Store(true)
	defer armed.Store(false)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	}); err == nil {
		t.Fatalf("reconcile returned nil, want error (Deployment lost controller ref between apply and Get)")
	}
}

// TestReconcileLMCacheForeignDeploymentNoStatusLeak pins the foreign-ownership
// guard on the Deployment status path: if a Deployment with the matching name
// already exists but is owned by another controller, applyDeployment fails
// (SetControllerReference returns AlreadyOwned). The reconciler must NOT
// derive Ready from that foreign workload — that would mark the CacheBackend
// Ready based on someone else's pods.
func TestReconcileLMCacheForeignDeploymentNoStatusLeak(t *testing.T) {
	scheme := newScheme(t)

	// A foreign Deployment with the same name as the CacheBackend, owned by
	// some unrelated CR. Populate status as Ready so a leaky status read
	// would mark the CacheBackend Ready.
	foreignOwner := true
	foreign := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cache", Namespace: "ns1", Generation: 1,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "example.com/v1", Kind: "OtherKind",
				Name: "other", UID: "other-uid",
				Controller: &foreignOwner, BlockOwnerDeletion: &foreignOwner,
			}},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptrInt32(1),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "other"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "other"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "x", Image: "x:1"}}},
			},
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1, Replicas: 1,
			UpdatedReplicas: 1, AvailableReplicas: 1, ReadyReplicas: 1,
		},
	}
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	// Wire autoscaling so reconcileHPA would otherwise create an HPA targeting
	// the same-named (foreign) Deployment. The fix must skip both Service and
	// HPA applies when applyDeployment fails — running them after a
	// foreign-ownership failure could scale another controller's workload or
	// expose its pods through our Service.
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
	r := newReconciler(scheme, cb, foreign)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	}); err == nil {
		t.Fatalf("reconcile returned nil, want error (Deployment already owned by another controller)")
	}

	updated := getBackend(t, r, "cache", "ns1")
	if cond := findCondition(updated.Status.Conditions, conditionTypeReady); cond != nil && cond.Status == metav1.ConditionTrue {
		t.Fatalf("Ready condition True, must not be derived from foreign Deployment")
	}
	// No Service should have been created either — applying a Service that
	// selects pods of a foreign Deployment is just as wrong as adopting it.
	var svcs corev1.ServiceList
	if err := r.List(context.Background(), &svcs, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list services: %v", err)
	}
	if len(svcs.Items) != 0 {
		t.Fatalf("services = %d, want 0 (dependent applies must be skipped when applyDeployment fails)", len(svcs.Items))
	}
	// And no HPA, despite spec.autoscaling being set — otherwise the HPA
	// would scale the foreign Deployment by name.
	var hpas autoscalingv2.HorizontalPodAutoscalerList
	if err := r.List(context.Background(), &hpas, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list HPAs: %v", err)
	}
	if len(hpas.Items) != 0 {
		t.Fatalf("HPAs = %d, want 0 (HPA must not target a foreign Deployment)", len(hpas.Items))
	}
}

// TestReconcileLMCacheForeignServiceNoEndpointLeak pins the foreign-ownership
// guard on the Service endpoint path: if a Service with the matching name
// already exists but is owned by another controller, applyService fails
// (AlreadyOwned). Status.Endpoint must NOT advertise that foreign Service's
// address; clients/gateways would route to the wrong workload.
func TestReconcileLMCacheForeignServiceNoEndpointLeak(t *testing.T) {
	scheme := newScheme(t)

	foreignOwner := true
	foreign := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cache", Namespace: "ns1",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "example.com/v1", Kind: "OtherKind",
				Name: "other", UID: "other-uid",
				Controller: &foreignOwner, BlockOwnerDeletion: &foreignOwner,
			}},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{"app": "other"},
			Ports: []corev1.ServicePort{{
				Name: "http", Port: 7777, TargetPort: intstr.FromInt(7777),
			}},
		},
	}
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	r := newReconciler(scheme, cb, foreign)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	}); err == nil {
		t.Fatalf("reconcile returned nil, want error (Service already owned by another controller)")
	}
	if got := getBackend(t, r, "cache", "ns1").Status.Endpoint; got != "" {
		t.Fatalf("status.endpoint = %q, want empty (foreign Service must not leak into status)", got)
	}
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
