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
	ctrl "sigs.k8s.io/controller-runtime"

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

func TestReconcilePersistentStorageRefusesToAdoptUnownedPVC(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	withPVC(cb, "10Gi", nil)
	// An operator-created PVC that merely shares the derived name <cb>-data,
	// owned by nobody. Adopting it (adding our controller owner ref) would make
	// it GC'd with the CacheBackend — irreversible data loss.
	foreign := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "cache-data", Namespace: "ns1"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	r := newReconciler(scheme, cb, foreign)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	}); err == nil {
		t.Fatalf("reconcile must error rather than adopt an unowned PVC named cache-data")
	}

	// The foreign PVC must NOT have been adopted (no controller owner ref added).
	pvc := getPVC(t, r, "cache-data", "ns1")
	if owner := metav1.GetControllerOf(pvc); owner != nil {
		t.Fatalf("unowned PVC was adopted (controller owner=%+v); must be left untouched to avoid GC/data-loss", owner)
	}
	// And no Deployment was provisioned (apply aborted on the PVC error).
	if _, derr := getOptionalDeployment(t, r, "cache", "ns1"); !apierrors.IsNotFound(derr) {
		t.Fatalf("Deployment must not be provisioned when PVC apply fails; get err=%v", derr)
	}
}

func TestReconcileExternalIgnoresStoragePVC(t *testing.T) {
	// External backends are operator-managed: the controller provisions no
	// workload and no PVC, so spec.storage.pvc is a no-op for them (handling
	// External persistence is out of scope — the operator configures it on the
	// pre-existing cache). Pin that no-op: no PVC is created, and status mirrors
	// the endpoint as usual.
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "user-supplied.example:8080"
	withPVC(cb, "10Gi", nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	if _, err := getOptionalPVC(r, "cache-data", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("External backend must not provision a PVC for spec.storage.pvc; get err=%v", err)
	}
	if ep := getBackend(t, r, "cache", "ns1").Status.Endpoint; ep != "user-supplied.example:8080" {
		t.Fatalf("status.endpoint = %q, want the mirrored External endpoint", ep)
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

func TestReconcileStatefulSetPersistentStorageUsesVolumeClaimTemplates(t *testing.T) {
	scheme := newScheme(t)
	fast := "fast"
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
	cb.Spec.Replicas = ptrInt32(3)
	withPVC(cb, "10Gi", &fast)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	if _, err := getOptionalDeployment(t, r, "cache", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("StatefulSet persistent backend must not provision a Deployment; get err=%v", err)
	}
	if _, err := getOptionalPVC(r, "cache-data", "ns1"); !apierrors.IsNotFound(err) {
		t.Fatalf("StatefulSet persistent backend must not provision a shared PVC; get err=%v", err)
	}
	sts := getStatefulSet(t, r, "cache", "ns1")
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 3 {
		t.Fatalf("statefulset replicas = %v, want 3", sts.Spec.Replicas)
	}
	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("volumeClaimTemplates = %d, want 1: %#v", len(sts.Spec.VolumeClaimTemplates), sts.Spec.VolumeClaimTemplates)
	}
	tpl := sts.Spec.VolumeClaimTemplates[0]
	if tpl.Name == "" {
		t.Fatalf("volumeClaimTemplate name is empty")
	}
	if got := tpl.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(resource.MustParse("10Gi")) != 0 {
		t.Fatalf("volumeClaimTemplate size = %q, want 10Gi", got.String())
	}
	if tpl.Spec.StorageClassName == nil || *tpl.Spec.StorageClassName != "fast" {
		t.Fatalf("volumeClaimTemplate storageClassName = %v, want fast", tpl.Spec.StorageClassName)
	}
	mounts := sts.Spec.Template.Spec.Containers[0].VolumeMounts
	found := false
	for _, mount := range mounts {
		if mount.Name == tpl.Name && strings.HasPrefix(mount.MountPath, "/") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("lmcache-server mounts = %v, want absolute mount for volumeClaimTemplate %q", mounts, tpl.Name)
	}
	cond := findCondition(getBackend(t, r, "cache", "ns1").Status.Conditions, conditionTypeReady)
	if cond != nil && cond.Reason == conditionReasonInvalidStorageConfiguration {
		t.Fatalf("StatefulSet per-replica PVC backend must not be gated as InvalidStorageConfiguration")
	}
}

func TestReconcileStatefulSetStorageEditSurfacesImmutableDrift(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	sts := getStatefulSet(t, r, "cache", "ns1")
	if len(sts.Spec.VolumeClaimTemplates) != 0 {
		t.Fatalf("initial volumeClaimTemplates = %d, want none", len(sts.Spec.VolumeClaimTemplates))
	}

	fresh := getBackend(t, r, "cache", "ns1")
	fresh.Generation = 2
	withPVC(fresh, "20Gi", nil)
	if err := r.Update(context.Background(), fresh); err != nil {
		t.Fatalf("update backend storage: %v", err)
	}

	reconcile(t, r, "cache", "ns1")

	sts = getStatefulSet(t, r, "cache", "ns1")
	if len(sts.Spec.VolumeClaimTemplates) != 0 {
		t.Fatalf("StatefulSet volumeClaimTemplates were mutated after creation: %#v", sts.Spec.VolumeClaimTemplates)
	}
	for _, mount := range sts.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.Name == "cache-data" {
			t.Fatalf("pod template mounted new immutable storage claim after creation: mounts=%v", sts.Spec.Template.Spec.Containers[0].VolumeMounts)
		}
	}
	updated := getBackend(t, r, "cache", "ns1")
	cond := findCondition(updated.Status.Conditions, conditionTypeReady)
	wantReason := conditionReasonImmutableStatefulSetStorage
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != wantReason {
		t.Fatalf("Ready condition = %+v, want False/%s", cond, wantReason)
	}
	if updated.Status.ObservedGeneration != 2 {
		t.Fatalf("status.observedGeneration = %d, want 2 so the immutable-storage condition is tied to the edited spec", updated.Status.ObservedGeneration)
	}
}

func TestReconcileStorageRemovedViaKindSwitchStillWarnsAndKeepsPVC(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	withPVC(cb, "10Gi", nil)
	r, rec := newReconcilerWithRecorder(t, cb)

	reconcile(t, r, "cache", "ns1") // provisions the PVC on the managed path
	getPVC(t, r, "cache-data", "ns1")
	drainEvents(rec)

	// Operator removes storage AND switches to StatefulSet. The adopt-and-keep
	// warning must still fire (it lives in dispatch), and the PVC must stay.
	fresh := getBackend(t, r, "cache", "ns1")
	fresh.Spec.Storage = nil
	fresh.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
	if err := r.Update(context.Background(), fresh); err != nil {
		t.Fatalf("update backend: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if _, err := getOptionalPVC(r, "cache-data", "ns1"); err != nil {
		t.Fatalf("PVC must be retained across the kind switch (adopt-and-keep); get err=%v", err)
	}
	if n := countEvents(drainEvents(rec), eventReasonOrphanedPVCRetained); n != 1 {
		t.Fatalf("OrphanedPVCRetained on the unmanaged/bypass path = %d, want 1", n)
	}
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
