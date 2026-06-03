package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestIntegrationCacheBackendPersistentStorage exercises spec.storage.pvc
// against a real apiserver (envtest): PVC provisioning, owner references that
// drive GC, the volume mount on the rendered pod, churn-free resync, and the
// multi-replica gate. It deliberately does NOT assert a live PVC bind or
// status.capacity — envtest runs no volume provisioner, so a PVC stays Pending
// forever there. The capacity-on-bind path is covered by the fake-client unit
// test (which simulates the binder writing status.capacity) and by the
// default-install smoke (a real kind StorageClass binds for real).
func TestIntegrationCacheBackendPersistentStorage(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard()}
	ctx := context.Background()

	t.Run("PVCProvisionedOwnedAndMounted", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.Replicas = ptrInt32(1)
		withPVC(cb, "1Gi", nil)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		reconcile(t, r, "cache", ns)

		pvc := getPVC(t, r, "cache-data", ns)
		owner := metav1.GetControllerOf(pvc)
		if owner == nil || owner.Kind != "CacheBackend" || owner.Name != "cache" ||
			owner.Controller == nil || !*owner.Controller ||
			owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
			t.Fatalf("pvc owner = %+v, want controller+blockOwnerDeletion ref to CacheBackend/cache (GC on CR delete)", owner)
		}
		if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
			t.Fatalf("pvc access modes = %v, want [ReadWriteOnce]", pvc.Spec.AccessModes)
		}
		if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(resource.MustParse("1Gi")) != 0 {
			t.Fatalf("pvc requested size = %q, want 1Gi", got.String())
		}

		dep := getDeployment(t, r, "cache", ns)
		pod := dep.Spec.Template.Spec
		foundVol := false
		for _, v := range pod.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == "cache-data" {
				foundVol = true
			}
		}
		if !foundVol {
			t.Fatalf("pod has no PVC-backed volume for claim cache-data; volumes=%v", pod.Volumes)
		}
		mounted := false
		for _, m := range pod.Containers[0].VolumeMounts {
			if m.Name == "cache-data" {
				mounted = true
			}
		}
		if !mounted {
			t.Fatalf("lmcache-server container does not mount cache-data; mounts=%v", pod.Containers[0].VolumeMounts)
		}
	})

	t.Run("PVCNoChurnOnResync", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.Replicas = ptrInt32(1)
		withPVC(cb, "1Gi", nil)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		reconcile(t, r, "cache", ns)
		rv := getRV(t, r, "cache-data", ns, &corev1.PersistentVolumeClaim{})
		reconcile(t, r, "cache", ns)
		if got := getRV(t, r, "cache-data", ns, &corev1.PersistentVolumeClaim{}); got != rv {
			t.Fatalf("PVC churned across reconciles: RV %s -> %s", rv, got)
		}
	})

	t.Run("MultiReplicaGatedNoPVCNoWorkload", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.Replicas = ptrInt32(3)
		withPVC(cb, "1Gi", nil)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)

		if _, err := getOptionalPVC(r, "cache-data", ns); !apierrors.IsNotFound(err) {
			t.Fatalf("multi-replica persistent backend must not provision a PVC; get err=%v", err)
		}
		if _, err := getOptionalDeployment(t, r, "cache", ns); !apierrors.IsNotFound(err) {
			t.Fatalf("multi-replica persistent backend must not provision a Deployment; get err=%v", err)
		}
		cond := findCondition(getBackend(t, r, "cache", ns).Status.Conditions, conditionTypeReady)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != conditionReasonInvalidStorageConfiguration {
			t.Fatalf("Ready condition = %+v, want False/InvalidStorageConfiguration", cond)
		}
	})
}
