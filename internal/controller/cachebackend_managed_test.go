// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sync/atomic"
	"testing"
)

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

func TestReconcileLegacyCacheWithTypedObservationRetainsProviderWorkload(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("legacy-observed", "ns1")
	cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "model-a"}
	r := newReconciler(scheme, cb)

	reconcile(t, r, cb.Name, cb.Namespace)

	if _, err := getOptionalDeployment(t, r, cb.Name, cb.Namespace); err != nil {
		t.Fatalf("managed deployment was not retained: %v", err)
	}
	var service corev1.Service
	if err := r.Get(context.Background(), types.NamespacedName{Name: cb.Name, Namespace: cb.Namespace}, &service); err != nil {
		t.Fatalf("managed service was not retained: %v", err)
	}
	got := getBackend(t, r, cb.Name, cb.Namespace)
	wantEndpoint := "legacy-observed.ns1.svc.cluster.local:65432"
	if got.Status.Endpoint != wantEndpoint {
		t.Fatalf("status.endpoint = %q, want %q", got.Status.Endpoint, wantEndpoint)
	}
}

// TestReconcileManagedMooncake is the C2-reconciles-Mooncake DoD: a canonical
// Mooncake remote provider must reconcile into a managed mooncake_master
// Deployment + Service, and
// status.endpoint must be the master's RPC host:port (the engine-agnostic
// address the pod webhook later turns into mooncakestore://). The RPC port
// being first in the rendered Service is what makes serviceEndpoint resolve it.
func TestReconcileManagedMooncake(t *testing.T) {
	scheme := newScheme(t)
	cb := mooncakeBackend("cache", "ns1")
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	owner := metav1.GetControllerOf(dep)
	if owner == nil || owner.Kind != "CacheBackend" || owner.Name != "cache" {
		t.Fatalf("deployment controller owner = %+v, want CacheBackend/cache", owner)
	}
	containers := dep.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(containers))
	}
	c := containers[0]
	if c.Name != "mooncake-master" {
		t.Fatalf("container name = %q, want mooncake-master", c.Name)
	}
	if c.Image == "" {
		t.Fatalf("container image is empty")
	}
	if !containsStr(c.Command, "mooncake_master") {
		t.Fatalf("container command = %v, want to start with mooncake_master", c.Command)
	}
	if len(c.Ports) == 0 || c.Ports[0].ContainerPort != 50051 {
		t.Fatalf("first container port = %v, want RPC port 50051 first", c.Ports)
	}

	svc := &corev1.Service{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "cache", Namespace: "ns1"}, svc); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("service type = %q, want ClusterIP", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) == 0 || svc.Spec.Ports[0].Port != 50051 {
		t.Fatalf("first service port = %v, want RPC port 50051 first (serviceEndpoint uses Ports[0])", svc.Spec.Ports)
	}

	updated := getBackend(t, r, "cache", "ns1")
	wantEndpoint := "cache.ns1.svc.cluster.local:50051"
	if updated.Status.Endpoint != wantEndpoint {
		t.Fatalf("status.endpoint = %q, want %q (master RPC host:port; mooncakestore:// prefix is the adapter's job)", updated.Status.Endpoint, wantEndpoint)
	}
	if updated.Status.ObservedGeneration != 1 {
		t.Fatalf("status.observedGeneration = %d, want 1", updated.Status.ObservedGeneration)
	}
}

func TestReconcileCanonicalManagedMooncake(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("canonical-mooncake", "ns1")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderMooncake,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		Mooncake:  &cachev1alpha1.MooncakeRemoteStorageSpec{},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, cb.Name, cb.Namespace)

	dep := getDeployment(t, r, cb.Name, cb.Namespace)
	if got := dep.Spec.Template.Spec.Containers[0].Name; got != "mooncake-master" {
		t.Fatalf("container name = %q, want mooncake-master", got)
	}
	got := getBackend(t, r, cb.Name, cb.Namespace)
	if want := "canonical-mooncake.ns1.svc.cluster.local:50051"; got.Status.Endpoint != want {
		t.Fatalf("status.endpoint = %q, want %q", got.Status.Endpoint, want)
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

func TestReconcileLMCacheCaseInsensitiveEngine(t *testing.T) {
	// The canonical VLLM runtime must route to the managed adapter path.
	for _, runtime := range []cachev1alpha1.CacheBackendRuntime{cachev1alpha1.CacheBackendRuntimeVLLM} {
		t.Run(string(runtime), func(t *testing.T) {
			scheme := newScheme(t)
			cb := lmcacheBackend("cache", "ns1")
			cb.Spec.Runtime = runtime
			r := newReconciler(scheme, cb)

			reconcile(t, r, "cache", "ns1")

			dep, err := getOptionalDeployment(t, r, "cache", "ns1")
			if err != nil {
				t.Fatalf("expected a managed Deployment for runtime=%q, got error: %v", runtime, err)
			}
			if got := dep.Spec.Template.Spec.Containers[0].Name; got != "lmcache-server" {
				t.Fatalf("container = %q, want lmcache-server (runtime=%q must resolve to RuntimeVLLM)", got, runtime)
			}
		})
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
	live.Spec.RemoteStorage.LMCacheServer.Image = "example.com/lmcache-server:v9"
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
