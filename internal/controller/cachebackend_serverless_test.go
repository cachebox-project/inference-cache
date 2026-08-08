// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	builtinruntime "github.com/cachebox-project/inference-cache/internal/adapters/builtin/runtime"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"testing"
	"time"
)

func TestReconcileCanonicalHostOnlyCacheCreatesNoProviderWorkload(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("host-only", "ns1")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.RemoteStorage = nil
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
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
	if got.Status.Endpoint != "" {
		t.Fatalf("status.endpoint = %q, want empty for host-only hierarchy", got.Status.Endpoint)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, conditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != conditionReasonHostOnlyActive {
		t.Fatalf("Ready = %+v, want True/%s", ready, conditionReasonHostOnlyActive)
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
				Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
				Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
				Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
			},
		},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, cb.Name, cb.Namespace)

	if _, err := getOptionalDeployment(t, r, cb.Name, cb.Namespace); !apierrors.IsNotFound(err) {
		t.Fatalf("deployment lookup error = %v, want NotFound", err)
	}
	got := getBackend(t, r, cb.Name, cb.Namespace)
	if got.Status.Endpoint != "" {
		t.Fatalf("status.endpoint = %q, want empty for unsupported binding", got.Status.Endpoint)
	}
	if ready := meta.FindStatusCondition(got.Status.Conditions, conditionTypeReady); ready != nil {
		t.Fatalf("unsupported binding published Ready condition: %+v", ready)
	}
}

func TestReconcileCanonicalHostOnlyCacheReportsEngineDiagnostics(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("host-only-kernel", "ns1")
	cb.UID = types.UID("host-only-kernel-uid")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{
		MatchLabels: map[string]string{"app": "engine"},
	}
	pod := strictPodWithKernelStatus(termed(1, enginebinding.KernelCheckMsgFailPrefix+" lmcache c_ops failed"))
	pod.ObjectMeta = metav1.ObjectMeta{
		Name:      "engine",
		Namespace: cb.Namespace,
		Labels:    map[string]string{"app": "engine"},
		Annotations: map[string]string{
			podwebhook.AnnotationInjectedBy:    cb.Namespace + "/" + cb.Name,
			podwebhook.AnnotationInjectedByUID: string(cb.UID),
		},
	}
	pod.Spec.Containers = []corev1.Container{{Name: "vllm"}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "vllm",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason: crashLoopBackOffReason,
		}},
	}}
	r := newReconciler(scheme, cb, &pod)

	reconcile(t, r, cb.Name, cb.Namespace)

	got := getBackend(t, r, cb.Name, cb.Namespace)
	kernels := meta.FindStatusCondition(got.Status.Conditions, conditionTypeEngineKernelsHealthy)
	if kernels == nil || kernels.Status != metav1.ConditionFalse || kernels.Reason != reasonKernelLoadFailed {
		t.Fatalf("EngineKernelsHealthy = %+v, want False/%s", kernels, reasonKernelLoadFailed)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, conditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reasonEngineKernelDegraded {
		t.Fatalf("Ready = %+v, want False/%s", ready, reasonEngineKernelDegraded)
	}
	compatibility := meta.FindStatusCondition(got.Status.Conditions, conditionTypeEngineCompatibility)
	if compatibility == nil || compatibility.Status != metav1.ConditionFalse ||
		compatibility.Reason != reasonInjectedEngineCrashLooping {
		t.Fatalf("EngineCompatibility = %+v, want False/%s", compatibility, reasonInjectedEngineCrashLooping)
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
	live.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	live.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	live.Spec.RemoteStorage = externalLMCacheStorage("external.ns1.svc:8080")
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
	switching.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	switching.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	switching.Spec.RemoteStorage = externalLMCacheStorage("external.ns1.svc:8080")
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

// TestReconcileSwitchToStatefulSetClearsObservedServerInstance asserts
// the same clearing for the managed→unsupported-runtime transition
// (reconcileUnmanaged path). The StatefulSet deployment-kind is
// currently the canonical unmanaged trigger.
func TestReconcileSwitchToStatefulSetClearsObservedServerInstance(t *testing.T) {
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))

	reconcile(t, r, "cache", "ns1")
	live := getBackend(t, r, "cache", "ns1")
	live.Status.ObservedServerInstance = "cache-pod-uid:0"
	// Plant a stale KV-event-gate anchor too: the unmanaged transition must
	// reset it so a later re-entry (managed or events-only) starts a fresh
	// firstEventTimeout window rather than reusing this pre-unmanaged time.
	staleAnchor := metav1.NewTime(time.Now().Add(-time.Hour))
	live.Status.FirstAvailableAt = &staleAnchor
	if err := r.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("plant baseline observedServerInstance: %v", err)
	}
	plantedKey := cascadeKey{namespace: live.Namespace, name: live.Name, uid: string(live.UID)}
	r.serverInstanceCascade.recordAttempt(plantedKey, "cache-pod-uid:0")
	if got := r.serverInstanceCascade.lastAttempt(plantedKey); got != "cache-pod-uid:0" {
		t.Fatalf("planted shadow precondition failed: lastAttempt = %q, want %q", got, "cache-pod-uid:0")
	}

	switching := getBackend(t, r, "cache", "ns1")
	switching.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
	if err := r.Update(context.Background(), switching); err != nil {
		t.Fatalf("switch to StatefulSet: %v", err)
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
	// The stale KV-event-gate anchor must be reset — otherwise an Offload→
	// Unmanaged→EventsOnly path (which clears endpoint/observedServerInstance,
	// so the events-only re-anchor heuristic can't detect the transition) would
	// reuse this pre-unmanaged time and breach the first-event window instantly.
	if got.Status.FirstAvailableAt != nil {
		t.Fatalf("status.firstAvailableAt = %v, want cleared on managed→unmanaged transition", got.Status.FirstAvailableAt)
	}
}

func TestReconcileSwitchToSGLangHiCacheCleansManagedState(t *testing.T) {
	scheme := newScheme(t)
	managed := lmcacheBackend("cache", "ns1")
	managed.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{
		MinReplicas: ptrInt32(1),
		MaxReplicas: 3,
	}
	r := newReconciler(
		scheme,
		managed,
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-0", Namespace: "ns1", Labels: map[string]string{"app": "sglang"}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-1", Namespace: "ns1", Labels: map[string]string{"app": "sglang"}}},
	)
	reconcile(t, r, "cache", "ns1")
	var managedHPA autoscalingv2.HorizontalPodAutoscaler
	if err := r.Get(context.Background(), types.NamespacedName{Name: "cache", Namespace: "ns1"}, &managedHPA); err != nil {
		t.Fatalf("get managed HPA before switch: %v", err)
	}

	live := getBackend(t, r, "cache", "ns1")
	matched := int32(2)
	live.Status.Endpoint = "cache.ns1.svc:65432"
	live.Status.ObservedServerInstance = "cache-pod-uid:0"
	live.Status.MatchedEnginePods = &matched
	live.Status.IndexParticipation = &cachev1alpha1.CacheBackendIndexParticipation{PrefixCount: 7}
	meta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
		Type:   conditionTypeReady,
		Status: metav1.ConditionTrue,
		Reason: "Available",
	})
	if err := r.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("plant managed status: %v", err)
	}

	switching := getBackend(t, r, "cache", "ns1")
	switching.Generation = 2
	switching.Spec.Type = cachev1alpha1.CacheBackendTypeSGLangHiCache
	switching.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
	switching.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		Mode: cachev1alpha1.CacheBackendIntegrationModeOffload,
		Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
	}
	switching.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{
		MatchLabels: map[string]string{"app": "sglang"},
	}
	switching.Spec.Autoscaling = nil
	switching.Spec.HiCache = &cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"}
	if err := r.Update(context.Background(), switching); err != nil {
		t.Fatalf("switch to SGLangHiCache: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if _, err := getOptionalDeployment(t, r, "cache", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("managed Deployment still exists after switch: %v", err)
	}
	var service corev1.Service
	if err := r.Get(context.Background(), types.NamespacedName{Name: "cache", Namespace: "ns1"}, &service); !apierrors.IsNotFound(err) {
		t.Fatalf("managed Service still exists after switch: %v", err)
	}
	var hpa autoscalingv2.HorizontalPodAutoscaler
	if err := r.Get(context.Background(), types.NamespacedName{Name: "cache", Namespace: "ns1"}, &hpa); !apierrors.IsNotFound(err) {
		t.Fatalf("managed HPA still exists after switch: %v", err)
	}
	got := getBackend(t, r, "cache", "ns1")
	if got.Status.Endpoint != "" || got.Status.ObservedServerInstance != "" {
		t.Fatalf("stale managed endpoint/server instance survived: %+v", got.Status)
	}
	if len(got.Status.Conditions) != 0 {
		t.Fatalf("SGLangHiCache first commit must publish no conditions, got %v", got.Status.Conditions)
	}
	if got.Status.ObservedGeneration != got.Generation {
		t.Fatalf("observedGeneration = %d, want generation %d", got.Status.ObservedGeneration, got.Generation)
	}
	if got.Status.MatchedEnginePods == nil || *got.Status.MatchedEnginePods != 2 {
		t.Fatalf("matchedEnginePods = %v, want preserved 2", got.Status.MatchedEnginePods)
	}
	if got.Status.IndexParticipation == nil || got.Status.IndexParticipation.PrefixCount != 7 {
		t.Fatalf("indexParticipation = %+v, want preserved", got.Status.IndexParticipation)
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
		t.Fatalf("deployments = %d, want 0 (StatefulSet kind deferred — managed Deployments only for now)", len(deps.Items))
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
			Runtime:       cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:          cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: externalLMCacheStorage("external.default.svc:8080"),
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
	// An arbitrary unsupported value exercises the admission-bypassed
	// "unsupported type → reconcileUnmanaged" path.
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
	// An EventsOnly backend whose (engine, type) pair has no registered
	// adapter must reconcile as UNMANAGED, NOT as active events-only.
	// Admission rejects an unsupported pair at write time, but a
	// stored/admission-bypassed CR reaching the controller must not be
	// advertised as a working routing tier: the pod webhook can't select an
	// adapter for an unsupported pair, so it could never inject the
	// kvevent-subscriber and no KV event would ever flow. dispatch confirms an
	// adapter is selectable before routing to reconcileEventsOnly; on failure it
	// falls to reconcileUnmanaged. The arbitrary value below is the unsupported
	// fixture.
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1", Generation: 1},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendType("unsupported"),
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Mode: cachev1alpha1.CacheBackendIntegrationModeEventsOnly,
			},
		},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1")
	// reconcileUnmanaged removes the Ready / Progressing conditions; the
	// events-only path (reconcileEventsOnly) would have PUBLISHED them. Their
	// absence is the discriminator between "reconciled as unmanaged" and
	// "reconciled as active events-only".
	if ready := findCondition(got.Status.Conditions, conditionTypeReady); ready != nil {
		t.Fatalf("unsupported-pair events-only must NOT publish Ready (unmanaged path); got %+v", ready)
	}
	if prog := findCondition(got.Status.Conditions, conditionTypeProgressing); prog != nil {
		t.Fatalf("unsupported-pair events-only must NOT publish Progressing (unmanaged path); got %+v", prog)
	}
	// And no workload is provisioned (unmanaged sheds everything).
	var deps appsv1.DeploymentList
	if err := r.List(context.Background(), &deps, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(deps.Items) != 0 {
		t.Fatalf("deployments = %d, want 0 for unmanaged events-only", len(deps.Items))
	}
}

func TestReconcileEventsOnlyAdapterRejectingHostOnlyBindingIsUnmanaged(t *testing.T) {
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1", Generation: 1},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Mode: cachev1alpha1.CacheBackendIntegrationModeEventsOnly,
			},
		},
	}
	r := newReconciler(scheme, cb)
	r.Registry = adapterruntime.NewRegistry()
	r.Registry.Register(remoteOnlyRuntimeAdapter{KVCacheRuntimeAdapter: builtinruntime.NewVLLMLMCacheAdapter(builtinruntime.SubscriberConfig{})})

	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1")
	if ready := findCondition(got.Status.Conditions, conditionTypeReady); ready != nil {
		t.Fatalf("events-only adapter rejecting nil binding must not publish Ready; got %+v", ready)
	}
	if prog := findCondition(got.Status.Conditions, conditionTypeProgressing); prog != nil {
		t.Fatalf("events-only adapter rejecting nil binding must not publish Progressing; got %+v", prog)
	}
}

func TestReconcileEventsOnlyTakesPrecedenceOverExternal(t *testing.T) {
	// An admission-bypassed object that sets both externally owned remote storage
	// and integration.mode=EventsOnly must reconcile via the events-only path.
	// Admission rejects this pair, so this is defense-in-depth for stored CRs. If
	// it reconciled as external storage it would publish an endpoint and allow KV
	// connector injection, violating events-only's "no connector, no server"
	// contract. The vLLM/LMCache pair has a registered adapter, so
	// the events-only adapter-selectability check passes and the events-only
	// reconcile runs.
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1", Generation: 1},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime:       cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:          cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: externalLMCacheStorage("external-cache.ns1.svc:8200"),
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Mode: cachev1alpha1.CacheBackendIntegrationModeEventsOnly,
			},
		},
	}
	r := newReconciler(scheme, cb)
	r.Registry = adapterruntime.NewRegistry()
	r.Registry.Register(builtinruntime.NewVLLMLMCacheAdapter(builtinruntime.SubscriberConfig{}))

	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1")

	// status.endpoint stays EMPTY — events-only publishes no endpoint. The
	// external-ownership path would have mirrored
	// spec.remoteStorage.endpoint here.
	if got.Status.Endpoint != "" {
		t.Fatalf("status.endpoint = %q, want empty (events-only wins over External; no endpoint mirrored)", got.Status.Endpoint)
	}

	// Ready is published by the events-only gate (AwaitingFirstKVEvent before any
	// event), NOT by the external-ownership path
	// (ExternalEndpointAccepted). The reason is
	// the discriminator between the two reconcile paths.
	ready := findCondition(got.Status.Conditions, conditionTypeReady)
	if ready == nil {
		t.Fatalf("events-only must publish Ready; conditions = %v", got.Status.Conditions)
	}
	if ready.Reason == conditionReasonExternalEndpointAccepted {
		t.Fatalf("Ready reason = %q — reconciled as External, but EventsOnly must take precedence", ready.Reason)
	}
	if ready.Status != metav1.ConditionFalse || ready.Reason != reasonAwaitingFirstKVEvent {
		t.Fatalf("Ready = %+v, want False/AwaitingFirstKVEvent (events-only gate before any KV event)", ready)
	}

	// No workload is provisioned (events-only is server-less).
	var deps appsv1.DeploymentList
	if err := r.List(context.Background(), &deps, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(deps.Items) != 0 {
		t.Fatalf("deployments = %d, want 0 for events-only", len(deps.Items))
	}
	var svcs corev1.ServiceList
	if err := r.List(context.Background(), &svcs, client.InNamespace("ns1")); err != nil {
		t.Fatalf("list services: %v", err)
	}
	if len(svcs.Items) != 0 {
		t.Fatalf("services = %d, want 0 for events-only", len(svcs.Items))
	}
}

func TestReconcileExternalMirrorsEndpointToStatus(t *testing.T) {
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime:       cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:          cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: externalLMCacheStorage("external-cache.default.svc:8080"),
		},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "example", "default")

	if got := getBackend(t, r, "example", "default").Status.Endpoint; got != "external-cache.default.svc:8080" {
		t.Fatalf("status.endpoint = %q, want spec.remoteStorage.endpoint", got)
	}
}

func TestReconcileExternalSetsReadyTrue(t *testing.T) {
	// Admission accepts spec.remoteStorage.endpoint for external ownership at
	// write time, so the
	// readiness signal is "operator says this endpoint exists and we
	// accepted it" — there's no Service to wait on. Consumers (the
	// future readiness gate, kubectl get cb, the indexParticipation
	// poller for External) must see Ready=True.
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "ext", Namespace: "default", Generation: 3},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime:       cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:          cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: externalLMCacheStorage("ext.default.svc:8080"),
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
	// An externally owned CR with a non-empty but malformed
	// spec.remoteStorage.endpoint must
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
		{"non-numeric-port", "cache.example.com:not-a-port"},
		{"zero-port", "cache.example.com:0"},
		{"out-of-range-port", "cache.example.com:70000"},
		{"unbracketed-ipv6", "2001:db8::1"},
		{"embedded-whitespace", "cache example:8200"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cb := &cachev1alpha1.CacheBackend{
				ObjectMeta: metav1.ObjectMeta{Name: "ext-bad", Namespace: "default"},
				Spec: cachev1alpha1.CacheBackendSpec{
					Runtime:       cachev1alpha1.CacheBackendRuntimeVLLM,
					Type:          cachev1alpha1.CacheBackendTypeLMCache,
					RemoteStorage: externalLMCacheStorage(tc.endpoint),
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
			if !strings.Contains(ready.Message, "spec.remoteStorage.endpoint") {
				t.Fatalf("Ready message = %q, want canonical field spec.remoteStorage.endpoint", ready.Message)
			}
		})
	}
}

func TestReconcileCanonicalExternalInvalidEndpointNamesCanonicalField(t *testing.T) {
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "canonical-ext-bad", Namespace: "default"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
				Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
				Endpoint:  "https://cache.example.com:443/api",
			},
		},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, cb.Name, cb.Namespace)

	got := getBackend(t, r, cb.Name, cb.Namespace)
	ready := findCondition(got.Status.Conditions, "Ready")
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "ExternalEndpointInvalid" {
		t.Fatalf("Ready condition = %+v, want False/ExternalEndpointInvalid", ready)
	}
	if !strings.Contains(ready.Message, "spec.remoteStorage.endpoint") {
		t.Fatalf("Ready message = %q, want canonical field spec.remoteStorage.endpoint", ready.Message)
	}
	if strings.Contains(ready.Message, "spec.endpoint") {
		t.Fatalf("Ready message = %q, must not name deprecated spec.endpoint", ready.Message)
	}
}

func TestReconcileCanonicalExternalEndpointUsesProviderProtocol(t *testing.T) {
	scheme := newScheme(t)
	tests := []struct {
		name       string
		runtime    cachev1alpha1.CacheBackendRuntime
		provider   cachev1alpha1.CacheBackendRemoteStorageProvider
		endpoint   string
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "redis rejects lm scheme",
			runtime:    cachev1alpha1.CacheBackendRuntimeSGLang,
			provider:   cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
			endpoint:   "lm://redis.example:6379",
			wantStatus: metav1.ConditionFalse,
			wantReason: conditionReasonExternalEndpointInvalid,
		},
		{
			name:       "mooncake accepts explicit scheme",
			runtime:    cachev1alpha1.CacheBackendRuntimeVLLM,
			provider:   cachev1alpha1.CacheBackendRemoteStorageProviderMooncake,
			endpoint:   "mooncakestore://cache.example:50051",
			wantStatus: metav1.ConditionTrue,
			wantReason: conditionReasonExternalEndpointAccepted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := &cachev1alpha1.CacheBackend{
				ObjectMeta: metav1.ObjectMeta{Name: "external", Namespace: "default"},
				Spec: cachev1alpha1.CacheBackendSpec{
					Runtime: tt.runtime,
					Type:    cachev1alpha1.CacheBackendTypeLMCache,
					RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
						Provider:  tt.provider,
						Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
						Endpoint:  tt.endpoint,
					},
				},
			}
			r := newReconciler(scheme, cb)
			reconcile(t, r, cb.Name, cb.Namespace)

			ready := findCondition(getBackend(t, r, cb.Name, cb.Namespace).Status.Conditions, conditionTypeReady)
			if ready == nil || ready.Status != tt.wantStatus || ready.Reason != tt.wantReason {
				t.Fatalf("Ready = %+v, want %s/%s", ready, tt.wantStatus, tt.wantReason)
			}
		})
	}
}

func TestReconcileCanonicalExternalUnsupportedBindingStaysUnmanaged(t *testing.T) {
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "external-redis", Namespace: "default", Generation: 2},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
				Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
				Endpoint:  "redis.example:6379",
			},
		},
		Status: cachev1alpha1.CacheBackendStatus{
			Endpoint: "stale.example:6379",
			Conditions: []metav1.Condition{{
				Type:   conditionTypeReady,
				Status: metav1.ConditionTrue,
				Reason: conditionReasonExternalEndpointAccepted,
			}},
		},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, cb.Name, cb.Namespace)

	got := getBackend(t, r, cb.Name, cb.Namespace)
	if got.Status.Endpoint != "" {
		t.Fatalf("status.endpoint = %q, want cleared for unsupported external binding", got.Status.Endpoint)
	}
	if ready := findCondition(got.Status.Conditions, conditionTypeReady); ready != nil {
		t.Fatalf("Ready = %+v, want absent for unmanaged unsupported external binding", ready)
	}
	if got.Status.ObservedGeneration != cb.Generation {
		t.Fatalf("status.observedGeneration = %d, want %d", got.Status.ObservedGeneration, cb.Generation)
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
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime:       cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:          cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: externalLMCacheStorage(""),
		},
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
	// Admission rejects a whitespace-only spec.remoteStorage.endpoint, but a
	// caller that bypasses admission can still construct one.
	// The reconciler must treat it as missing — publishing a raw
	// "LMCACHE_REMOTE_URL=lm://   " to the engine env is worse than
	// publishing nothing, and Ready=True on whitespace would mislead
	// every downstream consumer.
	scheme := newScheme(t)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "ext-ws", Namespace: "default"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime:       cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:          cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: externalLMCacheStorage("   \t  "),
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
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime:       cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:          cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: externalLMCacheStorage(""),
		},
		Status: cachev1alpha1.CacheBackendStatus{Endpoint: "stale-cache.default.svc:8080"},
	}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "example", "default")

	if got := getBackend(t, r, "example", "default").Status.Endpoint; got != "" {
		t.Fatalf("status.endpoint = %q, want empty", got)
	}
}
