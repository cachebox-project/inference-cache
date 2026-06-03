package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// withPVC sets spec.storage.pvc on a backend fixture (size + optional class).
func withPVC(cb *cachev1alpha1.CacheBackend, size string, class *string) *cachev1alpha1.CacheBackend {
	cb.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{
		PVC: &cachev1alpha1.CacheBackendPVCSpec{
			Size:             resource.MustParse(size),
			StorageClassName: class,
		},
	}
	return cb
}

func getPVC(t *testing.T, r *CacheBackendReconciler, name, namespace string) *corev1.PersistentVolumeClaim {
	t.Helper()
	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &pvc); err != nil {
		t.Fatalf("get pvc %s/%s: %v", namespace, name, err)
	}
	return &pvc
}

func getOptionalPVC(r *CacheBackendReconciler, name, namespace string) (*corev1.PersistentVolumeClaim, error) {
	var pvc corev1.PersistentVolumeClaim
	err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &pvc)
	return &pvc, err
}

// pvcStorageRequest returns the PVC's requested storage size.
func pvcStorageRequest(pvc *corev1.PersistentVolumeClaim) resource.Quantity {
	return pvc.Spec.Resources.Requests[corev1.ResourceStorage]
}

func TestReconcilePersistentStorageCreatesPVCAndMounts(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	withPVC(cb, "10Gi", nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	pvc := getPVC(t, r, "cache-data", "ns1")
	if owner := metav1.GetControllerOf(pvc); owner == nil || owner.Kind != "CacheBackend" || owner.Name != "cache" || owner.Controller == nil || !*owner.Controller {
		t.Fatalf("pvc controller owner = %+v, want controller ref to CacheBackend/cache (GC on CR delete)", owner)
	}
	if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Fatalf("pvc access modes = %v, want [ReadWriteOnce]", pvc.Spec.AccessModes)
	}
	if got := pvcStorageRequest(pvc); got.Cmp(resource.MustParse("10Gi")) != 0 {
		t.Fatalf("pvc requested size = %q, want 10Gi", got.String())
	}

	dep := getDeployment(t, r, "cache", "ns1")
	pod := dep.Spec.Template.Spec
	var dataVol *corev1.Volume
	for i := range pod.Volumes {
		if pod.Volumes[i].PersistentVolumeClaim != nil && pod.Volumes[i].PersistentVolumeClaim.ClaimName == "cache-data" {
			dataVol = &pod.Volumes[i]
		}
	}
	if dataVol == nil {
		t.Fatalf("deployment pod has no PVC-backed volume for claim cache-data; volumes=%v", pod.Volumes)
	}
	c := pod.Containers[0]
	var mount *corev1.VolumeMount
	for i := range c.VolumeMounts {
		if c.VolumeMounts[i].Name == dataVol.Name {
			mount = &c.VolumeMounts[i]
		}
	}
	if mount == nil {
		t.Fatalf("lmcache-server container has no mount for volume %q; mounts=%v", dataVol.Name, c.VolumeMounts)
	}
	if !strings.HasPrefix(mount.MountPath, "/") {
		t.Fatalf("mount path = %q, want an absolute in-container path", mount.MountPath)
	}
}

func TestReconcileNoStorageNoPVC(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1") // no spec.storage.pvc
	cb.Spec.Replicas = ptrInt32(1)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	if _, err := getOptionalPVC(r, "cache-data", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("expected no PVC for an ephemeral backend; get err=%v", err)
	}
	dep := getDeployment(t, r, "cache", "ns1")
	for _, v := range dep.Spec.Template.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			t.Fatalf("ephemeral backend should mount no PVC-backed volume; got %+v", v)
		}
	}
	if cb := getBackend(t, r, "cache", "ns1"); cb.Status.Capacity != "" {
		t.Fatalf("status.capacity = %q, want empty for an ephemeral backend", cb.Status.Capacity)
	}
}

func TestReconcilePersistentStorageRemovedKeepsPVCAndWarns(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	withPVC(cb, "10Gi", nil)
	r, rec := newReconcilerWithRecorder(t, cb)

	reconcile(t, r, "cache", "ns1")
	getPVC(t, r, "cache-data", "ns1") // exists after first reconcile
	drainEvents(rec)

	// Operator removes spec.storage.pvc.
	fresh := getBackend(t, r, "cache", "ns1")
	fresh.Spec.Storage = nil
	if err := r.Update(context.Background(), fresh); err != nil {
		t.Fatalf("update backend: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	// Adopt-and-keep: the owner-referenced PVC is retained, not deleted.
	if _, err := getOptionalPVC(r, "cache-data", "ns1"); err != nil {
		t.Fatalf("PVC must be retained on spec.storage.pvc removal (adopt-and-keep); get err=%v", err)
	}
	if n := countEvents(drainEvents(rec), eventReasonOrphanedPVCRetained); n != 1 {
		t.Fatalf("OrphanedPVCRetained events after removal = %d, want exactly 1", n)
	}

	// Fire-once: the warning is on the steady-state path, but repeat reconciles
	// must NOT re-emit it (annotation-guarded).
	reconcile(t, r, "cache", "ns1")
	reconcile(t, r, "cache", "ns1")
	if n := countEvents(drainEvents(rec), eventReasonOrphanedPVCRetained); n != 0 {
		t.Fatalf("OrphanedPVCRetained re-fired on resync = %d times, want 0 (must warn once per orphaning)", n)
	}

	// The pod reverts to ephemeral — no PVC-backed volume.
	dep := getDeployment(t, r, "cache", "ns1")
	for _, v := range dep.Spec.Template.Spec.Volumes {
		if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == "cache-data" {
			t.Fatalf("pod should no longer mount the retained PVC after storage removal; got %+v", v)
		}
	}
}

// countEvents returns how many recorded events contain substr.
func countEvents(events []string, substr string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, substr) {
			n++
		}
	}
	return n
}

func TestReconcilePVCSizeIncreasePatched(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	withPVC(cb, "5Gi", nil)
	r := newReconciler(scheme, cb)
	reconcile(t, r, "cache", "ns1")

	fresh := getBackend(t, r, "cache", "ns1")
	fresh.Spec.Storage.PVC.Size = resource.MustParse("10Gi")
	if err := r.Update(context.Background(), fresh); err != nil {
		t.Fatalf("update backend: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	pvc := getPVC(t, r, "cache-data", "ns1")
	if got := pvcStorageRequest(pvc); got.Cmp(resource.MustParse("10Gi")) != 0 {
		t.Fatalf("pvc requested size = %q, want patched up to 10Gi", got.String())
	}
}

func TestReconcilePVCSizeDecreaseIgnored(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	withPVC(cb, "10Gi", nil)
	r := newReconciler(scheme, cb)
	reconcile(t, r, "cache", "ns1")

	fresh := getBackend(t, r, "cache", "ns1")
	fresh.Spec.Storage.PVC.Size = resource.MustParse("5Gi")
	if err := r.Update(context.Background(), fresh); err != nil {
		t.Fatalf("update backend: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	pvc := getPVC(t, r, "cache-data", "ns1")
	if got := pvcStorageRequest(pvc); got.Cmp(resource.MustParse("10Gi")) != 0 {
		t.Fatalf("pvc requested size = %q, want 10Gi retained (k8s does not support shrinking)", got.String())
	}
}

func TestReconcilePVCStorageClassChangeIgnored(t *testing.T) {
	scheme := newScheme(t)
	fast := "fast"
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	withPVC(cb, "10Gi", &fast)
	r := newReconciler(scheme, cb)
	reconcile(t, r, "cache", "ns1")

	if pvc := getPVC(t, r, "cache-data", "ns1"); pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "fast" {
		t.Fatalf("pvc storageClassName = %v, want fast on create", pvc.Spec.StorageClassName)
	}

	fresh := getBackend(t, r, "cache", "ns1")
	slow := "slow"
	fresh.Spec.Storage.PVC.StorageClassName = &slow
	if err := r.Update(context.Background(), fresh); err != nil {
		t.Fatalf("update backend: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	pvc := getPVC(t, r, "cache-data", "ns1")
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "fast" {
		t.Fatalf("pvc storageClassName = %v, want fast retained (StorageClass is immutable in k8s)", pvc.Spec.StorageClassName)
	}
}

func TestReconcileMultiReplicaPersistentGated(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(3)
	withPVC(cb, "10Gi", nil)
	r, rec := newReconcilerWithRecorder(t, cb)

	reconcile(t, r, "cache", "ns1")

	if _, err := getOptionalDeployment(t, r, "cache", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("multi-replica persistent backend must NOT provision a Deployment; get err=%v", err)
	}
	if _, err := getOptionalPVC(r, "cache-data", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("multi-replica persistent backend must NOT provision a PVC; get err=%v", err)
	}
	cbb := getBackend(t, r, "cache", "ns1")
	cond := findCondition(cbb.Status.Conditions, conditionTypeReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != conditionReasonInvalidStorageConfiguration {
		t.Fatalf("Ready condition = %+v, want False/InvalidStorageConfiguration", cond)
	}
	expectEvent(t, drainEvents(rec), conditionReasonInvalidStorageConfiguration)
}

func TestReconcileMultiReplicaViaAutoscalingGated(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
	withPVC(cb, "10Gi", nil)
	r, rec := newReconcilerWithRecorder(t, cb)

	reconcile(t, r, "cache", "ns1")

	if _, err := getOptionalDeployment(t, r, "cache", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("persistent backend with autoscaling.maxReplicas>1 must NOT provision a Deployment; get err=%v", err)
	}
	if _, err := getOptionalPVC(r, "cache-data", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("persistent backend with autoscaling.maxReplicas>1 must NOT provision a PVC; get err=%v", err)
	}
	cbb := getBackend(t, r, "cache", "ns1")
	cond := findCondition(cbb.Status.Conditions, conditionTypeReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != conditionReasonInvalidStorageConfiguration {
		t.Fatalf("Ready condition = %+v, want False/InvalidStorageConfiguration", cond)
	}
	expectEvent(t, drainEvents(rec), conditionReasonInvalidStorageConfiguration)
}

func TestReconcilePersistentSingleToMultiReplicaShedsWorkloadAndEndpoint(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	withPVC(cb, "10Gi", nil)
	r, rec := newReconcilerWithRecorder(t, cb)

	// Valid single-replica persistent backend: provisions Deployment + PVC + endpoint.
	reconcile(t, r, "cache", "ns1")
	getDeployment(t, r, "cache", "ns1")
	getPVC(t, r, "cache-data", "ns1")
	if ep := getBackend(t, r, "cache", "ns1").Status.Endpoint; ep == "" {
		t.Fatalf("expected status.endpoint populated for the valid persistent backend")
	}
	drainEvents(rec)

	// Operator bumps to multi-replica → the gate trips. The invalid config must
	// not be left RUNNING: shed the workload and clear the endpoint so the pod
	// webhook stops wiring engines, while the PVC is retained (data survives).
	fresh := getBackend(t, r, "cache", "ns1")
	fresh.Spec.Replicas = ptrInt32(3)
	if err := r.Update(context.Background(), fresh); err != nil {
		t.Fatalf("update backend: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if _, err := getOptionalDeployment(t, r, "cache", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("Deployment must be shed when the backend becomes InvalidStorageConfiguration; get err=%v", err)
	}
	cbb := getBackend(t, r, "cache", "ns1")
	if cbb.Status.Endpoint != "" {
		t.Fatalf("status.endpoint = %q, want cleared while gated (pod webhook must stop wiring engines)", cbb.Status.Endpoint)
	}
	cond := findCondition(cbb.Status.Conditions, conditionTypeReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != conditionReasonInvalidStorageConfiguration {
		t.Fatalf("Ready = %+v, want False/InvalidStorageConfiguration", cond)
	}
	if _, err := getOptionalPVC(r, "cache-data", "ns1"); err != nil {
		t.Fatalf("PVC must be retained while gated (adopt-and-keep; data survives); get err=%v", err)
	}
	expectEvent(t, drainEvents(rec), conditionReasonInvalidStorageConfiguration)
}

func TestReconcilePersistentStaleReplicasUnderAutoscalingCeilingOneNotGated(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	// Stale spec.replicas=3, but the HPA owns the count and caps it at 1 — no
	// multi-attach hazard, so the backend must be provisioned, not gated.
	cb.Spec.Replicas = ptrInt32(3)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 1}
	withPVC(cb, "10Gi", nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	getPVC(t, r, "cache-data", "ns1")   // provisioned
	getDeployment(t, r, "cache", "ns1") // provisioned
	cond := findCondition(getBackend(t, r, "cache", "ns1").Status.Conditions, conditionTypeReady)
	if cond != nil && cond.Reason == conditionReasonInvalidStorageConfiguration {
		t.Fatalf("backend wrongly gated: autoscaling ceiling is 1, so stale spec.replicas=3 must be ignored")
	}
}

func TestReconcilePVCExplicitEmptyStorageClassPreserved(t *testing.T) {
	scheme := newScheme(t)
	empty := ""
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	withPVC(cb, "10Gi", &empty)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	pvc := getPVC(t, r, "cache-data", "ns1")
	// Explicit "" opts out of the default StorageClass (static binding); it must
	// be preserved as "", NOT collapsed to nil (which would use the default).
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "" {
		t.Fatalf("storageClassName = %v, want explicit \"\" preserved (opt-out of default StorageClass)", pvc.Spec.StorageClassName)
	}
}

func TestReconcileCapacityFromBoundPVC(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	withPVC(cb, "10Gi", nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	// Before bind, capacity is empty (the request is not the reality).
	if cbb := getBackend(t, r, "cache", "ns1"); cbb.Status.Capacity != "" {
		t.Fatalf("status.capacity = %q, want empty before the PVC binds", cbb.Status.Capacity)
	}

	// Simulate the binder: PVC goes Bound with its actual provisioned capacity.
	pvc := getPVC(t, r, "cache-data", "ns1")
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}
	if err := r.Status().Update(context.Background(), pvc); err != nil {
		t.Fatalf("update pvc status: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	cbb := getBackend(t, r, "cache", "ns1")
	if cbb.Status.Capacity != "10Gi" {
		t.Fatalf("status.capacity = %q, want 10Gi from the bound PVC", cbb.Status.Capacity)
	}
}
