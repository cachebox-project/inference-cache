package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// ---- PVC reconciliation -----------------------------------------------------

func persistentBackend(name, namespace, size string, storageClass *string) *cachev1alpha1.CacheBackend {
	cb := lmcacheBackend(name, namespace)
	cb.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{
		PVC: &cachev1alpha1.CacheBackendPVCSpec{
			Size:             resource.MustParse(size),
			StorageClassName: storageClass,
		},
	}
	return cb
}

func getPVC(t *testing.T, r *CacheBackendReconciler, name, namespace string) *corev1.PersistentVolumeClaim {
	t.Helper()
	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &pvc); err != nil {
		t.Fatalf("get PVC %s/%s: %v", namespace, name, err)
	}
	return &pvc
}

func TestReconcilePVCCreatedWhenPersistent(t *testing.T) {
	scheme := newScheme(t)
	sc := "fast"
	cb := persistentBackend("cache", "ns1", "20Gi", &sc)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	pvc := getPVC(t, r, "cache", "ns1")
	if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "20Gi" {
		t.Fatalf("PVC size = %q, want 20Gi", got.String())
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "fast" {
		t.Fatalf("PVC storageClassName = %v, want fast", pvc.Spec.StorageClassName)
	}
	owner := metav1.GetControllerOf(pvc)
	if owner == nil || owner.Kind != "CacheBackend" || owner.Name != "cache" {
		t.Fatalf("PVC controller owner = %+v, want CacheBackend/cache", owner)
	}

	// Pod template mounts the PVC at cache-home.
	dep := getDeployment(t, r, "cache", "ns1")
	var cacheHome *corev1.Volume
	for i := range dep.Spec.Template.Spec.Volumes {
		v := dep.Spec.Template.Spec.Volumes[i]
		if v.Name == "cache-home" {
			cacheHome = &v
			break
		}
	}
	if cacheHome == nil || cacheHome.PersistentVolumeClaim == nil || cacheHome.PersistentVolumeClaim.ClaimName != "cache" {
		t.Fatalf("cache-home volume should reference PVC 'cache', got %+v", cacheHome)
	}
	if cacheHome.EmptyDir != nil {
		t.Fatalf("cache-home volume should not also be EmptyDir when persistent: %+v", cacheHome.EmptyDir)
	}
}

func TestReconcileNoPVCWhenEphemeral(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	var pvcs corev1.PersistentVolumeClaimList
	if err := r.List(context.Background(), &pvcs); err != nil {
		t.Fatalf("list PVCs: %v", err)
	}
	if len(pvcs.Items) != 0 {
		t.Fatalf("PVCs = %d, want 0 when storage is not requested", len(pvcs.Items))
	}
	if got := getBackend(t, r, "cache", "ns1").Status.Capacity; got != "" {
		t.Fatalf("status.capacity = %q, want empty for ephemeral backend", got)
	}
}

func TestReconcilePVCKeptWhenStorageRemoved(t *testing.T) {
	// Removing spec.storage from a persistent backend must NOT auto-delete the
	// PVC — silent data loss in response to a spec edit is a serious footgun.
	// The PVC stays owner-referenced so it is GC'd when the CR is deleted.
	scheme := newScheme(t)
	cb := persistentBackend("cache", "ns1", "10Gi", nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	pvcUID := getPVC(t, r, "cache", "ns1").UID

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.Storage = nil
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update backend: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	// PVC must still exist, with the same identity.
	pvc := getPVC(t, r, "cache", "ns1")
	if pvc.UID != pvcUID {
		t.Fatalf("PVC UID changed after spec.storage removed: %q -> %q (PVC was recreated)", pvcUID, pvc.UID)
	}
	// Pod volume source should have reverted to EmptyDir so the pod still starts.
	dep := getDeployment(t, r, "cache", "ns1")
	for i := range dep.Spec.Template.Spec.Volumes {
		v := dep.Spec.Template.Spec.Volumes[i]
		if v.Name == "cache-home" && v.EmptyDir == nil {
			t.Fatalf("cache-home volume should revert to EmptyDir after spec.storage removed, got %+v", v.VolumeSource)
		}
	}
}

func TestReconcilePVCSizePatchedOnUpdate(t *testing.T) {
	scheme := newScheme(t)
	cb := persistentBackend("cache", "ns1", "10Gi", nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.Storage.PVC.Size = resource.MustParse("25Gi")
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update size: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	pvc := getPVC(t, r, "cache", "ns1")
	if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "25Gi" {
		t.Fatalf("PVC size after update = %q, want 25Gi", got.String())
	}
}

func TestReconcilePVCKeptOnSwitchToExternal(t *testing.T) {
	// Switching a managed backend to External is a spec edit. Like the
	// storage-drop path, the PVC is preserved to avoid silent data loss on a
	// mistaken edit. Owner-ref GC still cleans it up when the CR is deleted.
	scheme := newScheme(t)
	cb := persistentBackend("cache", "ns1", "10Gi", nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	pvcUID := getPVC(t, r, "cache", "ns1").UID

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	live.Spec.Endpoint = "external.ns1.svc:8080"
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("switch to external: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	pvc := getPVC(t, r, "cache", "ns1")
	if pvc.UID != pvcUID {
		t.Fatalf("PVC UID changed after switch to External: %q -> %q", pvcUID, pvc.UID)
	}
}

// ---- HPA reconciliation -----------------------------------------------------

func autoscalingBackend(name, namespace string, min, max int32, targetCPU *int32) *cachev1alpha1.CacheBackend {
	cb := lmcacheBackend(name, namespace)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{
		MinReplicas:                 ptrInt32(min),
		MaxReplicas:                 max,
		TargetCPUUtilizationPercent: targetCPU,
	}
	return cb
}

func getHPA(t *testing.T, r *CacheBackendReconciler, name, namespace string) *autoscalingv2.HorizontalPodAutoscaler {
	t.Helper()
	var hpa autoscalingv2.HorizontalPodAutoscaler
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &hpa); err != nil {
		t.Fatalf("get HPA %s/%s: %v", namespace, name, err)
	}
	return &hpa
}

func TestReconcileHPACreated(t *testing.T) {
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 2, 5, ptrInt32(60))
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	hpa := getHPA(t, r, "cache", "ns1")
	if hpa.Spec.ScaleTargetRef.Kind != "Deployment" || hpa.Spec.ScaleTargetRef.Name != "cache" {
		t.Fatalf("HPA target = %+v, want Deployment/cache", hpa.Spec.ScaleTargetRef)
	}
	if hpa.Spec.MinReplicas == nil || *hpa.Spec.MinReplicas != 2 || hpa.Spec.MaxReplicas != 5 {
		t.Fatalf("HPA min/max = %v/%d, want 2/5", hpa.Spec.MinReplicas, hpa.Spec.MaxReplicas)
	}
	if owner := metav1.GetControllerOf(hpa); owner == nil || owner.Name != "cache" {
		t.Fatalf("HPA controller owner = %+v, want CacheBackend/cache", owner)
	}
	if len(hpa.Spec.Metrics) != 1 {
		t.Fatalf("HPA metrics = %d, want 1", len(hpa.Spec.Metrics))
	}
	m := hpa.Spec.Metrics[0]
	if m.Type != autoscalingv2.ResourceMetricSourceType || m.Resource == nil || m.Resource.Name != corev1.ResourceCPU {
		t.Fatalf("HPA metric = %+v, want CPU resource metric", m)
	}
	if m.Resource.Target.AverageUtilization == nil || *m.Resource.Target.AverageUtilization != 60 {
		t.Fatalf("HPA target CPU = %v, want 60", m.Resource.Target.AverageUtilization)
	}
}

func TestReconcileHPADefaults(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	hpa := getHPA(t, r, "cache", "ns1")
	if hpa.Spec.MinReplicas == nil || *hpa.Spec.MinReplicas != defaultHPAMinReplicas {
		t.Fatalf("default min replicas = %v, want %d", hpa.Spec.MinReplicas, defaultHPAMinReplicas)
	}
	target := hpa.Spec.Metrics[0].Resource.Target.AverageUtilization
	if target == nil || *target != defaultHPATargetCPUUtilizationPercent {
		t.Fatalf("default target CPU = %v, want %d", target, defaultHPATargetCPUUtilizationPercent)
	}
}

func TestReconcileHPAUpdated(t *testing.T) {
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 1, 3, ptrInt32(50))
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.Autoscaling.MaxReplicas = 10
	live.Spec.Autoscaling.TargetCPUUtilizationPercent = ptrInt32(80)
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update autoscaling: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	hpa := getHPA(t, r, "cache", "ns1")
	if hpa.Spec.MaxReplicas != 10 {
		t.Fatalf("HPA max after update = %d, want 10", hpa.Spec.MaxReplicas)
	}
	if got := hpa.Spec.Metrics[0].Resource.Target.AverageUtilization; got == nil || *got != 80 {
		t.Fatalf("HPA target CPU after update = %v, want 80", got)
	}
}

func TestReconcileHPADeletedWhenAutoscalingCleared(t *testing.T) {
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 1, 3, nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	_ = getHPA(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.Autoscaling = nil
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("clear autoscaling: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	var hpas autoscalingv2.HorizontalPodAutoscalerList
	if err := r.List(context.Background(), &hpas); err != nil {
		t.Fatalf("list HPAs: %v", err)
	}
	if len(hpas.Items) != 0 {
		t.Fatalf("HPAs = %d, want 0 after autoscaling cleared", len(hpas.Items))
	}
}

func TestReconcileInitialReplicasFromAutoscalingMin(t *testing.T) {
	// With autoscaling configured, the Deployment must come up at the HPA's
	// minReplicas — otherwise it briefly runs below the HPA floor on first
	// apply (and may publish ScaledToZero status if spec.replicas defaults to
	// zero on a different shape).
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 3, 6, nil)
	// Even with spec.replicas explicitly set, the HPA's floor wins on init.
	cb.Spec.Replicas = ptrInt32(1)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 3 {
		t.Fatalf("initial deployment replicas = %v, want 3 (autoscaling.minReplicas)", dep.Spec.Replicas)
	}
}

func TestReconcileInitialReplicasDefaultsToOneWithAutoscaling(t *testing.T) {
	// Autoscaling without minReplicas → default 1 (matching the HPA default
	// the controller renders).
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 5}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Fatalf("initial deployment replicas = %v, want 1 (default autoscaling floor)", dep.Spec.Replicas)
	}
}

func TestReconcileDeploymentRespectsHPAReplicas(t *testing.T) {
	// When an HPA owns the replica count, the reconciler must not overwrite
	// dep.Spec.Replicas back to spec.Replicas — that would let the controller
	// and the HPA fight, churning the rollout.
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 1, 5, nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	// HPA scales the Deployment to 4 replicas (simulated).
	dep := getDeployment(t, r, "cache", "ns1")
	scaled := int32(4)
	dep.Spec.Replicas = &scaled
	if err := r.Update(context.Background(), dep); err != nil {
		t.Fatalf("update deployment replicas: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	dep = getDeployment(t, r, "cache", "ns1")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 4 {
		t.Fatalf("deployment replicas = %v, want 4 (HPA-managed, not reset by reconciler)", dep.Spec.Replicas)
	}
}

// ---- Status (Progressing, Capacity, observedGeneration) ---------------------

func TestStatusProgressingTrueWhilePending(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	if updated.Status.Health != cachev1alpha1.CacheBackendHealthPending {
		t.Fatalf("status.health = %q, want Pending right after create", updated.Status.Health)
	}
	prog := findCondition(updated.Status.Conditions, conditionTypeProgressing)
	if prog == nil || prog.Status != metav1.ConditionTrue {
		t.Fatalf("Progressing condition = %+v, want True while Pending", prog)
	}
}

func TestStatusProgressingFalseOnceReady(t *testing.T) {
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
	prog := findCondition(updated.Status.Conditions, conditionTypeProgressing)
	if prog == nil || prog.Status != metav1.ConditionFalse || prog.Reason != "Synced" {
		t.Fatalf("Progressing condition = %+v, want False/Synced once Ready", prog)
	}
}

func TestStatusProgressingFalseWhenDegraded(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(2)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	// Simulate a rolled-out Deployment that has lost some replicas: rollout
	// has finished (Progressing should be False) but Ready is False because
	// not enough replicas are available.
	dep := getDeployment(t, r, "cache", "ns1")
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.Replicas = 2
	dep.Status.UpdatedReplicas = 2
	dep.Status.AvailableReplicas = 1
	dep.Status.ReadyReplicas = 1
	if err := r.Status().Update(context.Background(), dep); err != nil {
		t.Fatalf("update deployment status: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	if updated.Status.Health != cachev1alpha1.CacheBackendHealthDegraded {
		t.Fatalf("status.health = %q, want Degraded", updated.Status.Health)
	}
	prog := findCondition(updated.Status.Conditions, conditionTypeProgressing)
	if prog == nil || prog.Status != metav1.ConditionFalse || prog.Reason != "Degraded" {
		t.Fatalf("Progressing condition = %+v, want False/Degraded", prog)
	}
}

func TestStatusCapacityReflectsPVCSize(t *testing.T) {
	scheme := newScheme(t)
	cb := persistentBackend("cache", "ns1", "20Gi", nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	if got := getBackend(t, r, "cache", "ns1").Status.Capacity; got != "20Gi" {
		t.Fatalf("status.capacity = %q, want 20Gi", got)
	}
}

func TestStatusObservedGenerationTracksSpec(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	if got := getBackend(t, r, "cache", "ns1").Status.ObservedGeneration; got != 1 {
		t.Fatalf("initial observedGeneration = %d, want 1", got)
	}

	// Bump the spec → bump generation.
	live := getBackend(t, r, "cache", "ns1")
	live.Generation = 5
	live.Spec.Replicas = ptrInt32(3)
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update spec: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if got := getBackend(t, r, "cache", "ns1").Status.ObservedGeneration; got != 5 {
		t.Fatalf("status.observedGeneration after update = %d, want 5", got)
	}
}

// ---- progressingFromHealth pure-function coverage ---------------------------

func TestProgressingFromHealthExhaustive(t *testing.T) {
	cases := []struct {
		health     cachev1alpha1.CacheBackendHealth
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{cachev1alpha1.CacheBackendHealthReady, metav1.ConditionFalse, "Synced"},
		{cachev1alpha1.CacheBackendHealthPending, metav1.ConditionTrue, "RolloutInProgress"},
		{cachev1alpha1.CacheBackendHealthDegraded, metav1.ConditionFalse, "Degraded"},
		{cachev1alpha1.CacheBackendHealthFailed, metav1.ConditionFalse, "RolloutInProgress"},
	}
	for _, tc := range cases {
		t.Run(string(tc.health), func(t *testing.T) {
			status, reason, _ := progressingFromHealth(tc.health, "RolloutInProgress", "msg")
			if status != tc.wantStatus {
				t.Fatalf("status = %v, want %v", status, tc.wantStatus)
			}
			if reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

func TestManagedCapacity(t *testing.T) {
	if got := managedCapacity(lmcacheBackend("cache", "ns1")); got != "" {
		t.Fatalf("ephemeral capacity = %q, want empty", got)
	}
	persistent := persistentBackend("cache", "ns1", "50Gi", nil)
	if got := managedCapacity(persistent); got != "50Gi" {
		t.Fatalf("persistent capacity = %q, want 50Gi", got)
	}
}

// ---- HPA-aware health -------------------------------------------------------

func TestDesiredReplicasPrefersHPAWhenAutoscalingSet(t *testing.T) {
	cb := autoscalingBackend("cache", "ns1", 1, 5, nil)
	// User-set spec.replicas should be ignored once autoscaling is in charge —
	// the HPA's writes to dep.spec.replicas are authoritative.
	cb.Spec.Replicas = ptrInt32(1)
	dep := newDep(4)
	if got := desiredReplicas(cb, dep); got != 4 {
		t.Fatalf("desiredReplicas = %d, want 4 (HPA-driven)", got)
	}
}

func TestDesiredReplicasFallbackToSpecWhenNoAutoscaling(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(3)
	dep := newDep(7) // out-of-band edit; not HPA-managed.
	if got := desiredReplicas(cb, dep); got != 3 {
		t.Fatalf("desiredReplicas = %d, want 3 (spec.replicas wins without autoscaling)", got)
	}
}

func TestManagedHealthIgnoresSpecReplicasUnderHPA(t *testing.T) {
	// spec.replicas=0 with autoscaling set must NOT trip the ScaledToZero
	// guard — the HPA owns the count, and minReplicas>=1 is enforced by the
	// kubebuilder validation on autoscaling.minReplicas.
	cb := autoscalingBackend("cache", "ns1", 1, 3, nil)
	cb.Spec.Replicas = ptrInt32(0)
	dep := newDep(2)
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.UpdatedReplicas = 2
	dep.Status.AvailableReplicas = 2

	health, status, _, _ := managedHealth(cb, dep)
	if health != cachev1alpha1.CacheBackendHealthReady || status != metav1.ConditionTrue {
		t.Fatalf("managedHealth = %q/%v, want Ready/True under HPA with 2/2 replicas", health, status)
	}
}

func newDep(replicas int32) *appsv1.Deployment {
	r := replicas
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: &r},
	}
}

// ---- Resource requests for HPA-friendly scaling -----------------------------

// ---- Invalid-spec handling (PVC + multi-replica) ----------------------------

func TestReconcilePersistentMultiReplicaMarksFailed(t *testing.T) {
	// PVC + replicas > 1 is unsafe with a single ReadWriteOnce PVC. The
	// controller refuses to apply children and surfaces the error via status.
	scheme := newScheme(t)
	cb := persistentBackend("cache", "ns1", "10Gi", nil)
	cb.Spec.Replicas = ptrInt32(3)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	// No children applied for the invalid spec.
	if _, err := getOptionalDeployment(t, r, "cache", "ns1"); err == nil {
		t.Fatalf("expected no Deployment for invalid spec")
	}
	updated := getBackend(t, r, "cache", "ns1")
	if updated.Status.Health != cachev1alpha1.CacheBackendHealthFailed {
		t.Fatalf("status.health = %q, want Failed", updated.Status.Health)
	}
	cond := findCondition(updated.Status.Conditions, conditionTypeReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "InvalidStorageConfiguration" {
		t.Fatalf("Ready condition = %+v, want False/InvalidStorageConfiguration", cond)
	}
}

func TestReconcilePersistentAutoscalingMaxRepliasMarksFailed(t *testing.T) {
	scheme := newScheme(t)
	cb := persistentBackend("cache", "ns1", "10Gi", nil)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 4}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	if updated.Status.Health != cachev1alpha1.CacheBackendHealthFailed {
		t.Fatalf("status.health = %q, want Failed", updated.Status.Health)
	}
}

// ---- Resources upgrade path -------------------------------------------------

func TestReconcileAdoptsResourcesOnUpgrade(t *testing.T) {
	// Pre-existing C2-era Deployments lack CPU/memory requests. The new HPA
	// depends on a CPU request being present, so the update path must
	// reconcile Resources onto the live container, not just image/args/env.
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	r := newReconciler(scheme, cb)
	reconcile(t, r, "cache", "ns1")

	// Simulate an older Deployment in the cluster: empty Resources.
	dep := getDeployment(t, r, "cache", "ns1")
	dep.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{}
	if err := r.Update(context.Background(), dep); err != nil {
		t.Fatalf("blank container resources: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	dep = getDeployment(t, r, "cache", "ns1")
	got := dep.Spec.Template.Spec.Containers[0].Resources.Requests
	if got == nil {
		t.Fatalf("expected requests to be reconciled onto upgraded container")
	}
	cpu, ok := got[corev1.ResourceCPU]
	if !ok || cpu.IsZero() {
		t.Fatalf("container CPU request after reconcile = %v, want non-zero", got)
	}
}

func TestLMCacheContainerHasCPURequest(t *testing.T) {
	// HPA CPU utilization is computed against the pod's CPU request; without
	// it, the HPA never gets usable metrics and never scales. Regression test
	// for a Codex review finding.
	scheme := newScheme(t)
	r := newReconciler(scheme, lmcacheBackend("cache", "ns1"))
	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	c := dep.Spec.Template.Spec.Containers[0]
	cpu, ok := c.Resources.Requests[corev1.ResourceCPU]
	if !ok || cpu.IsZero() {
		t.Fatalf("container should declare a CPU request for HPA, got requests=%v", c.Resources.Requests)
	}
	if _, ok := c.Resources.Requests[corev1.ResourceMemory]; !ok {
		t.Fatalf("container should declare a memory request, got requests=%v", c.Resources.Requests)
	}
}
