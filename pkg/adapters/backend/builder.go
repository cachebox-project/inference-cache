package backend

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// Workload is the set of child objects a managed CacheBackend reconciles into.
// Owner references and server-side apply are the controller's responsibility;
// a Builder only renders the desired shape from the spec.
type Workload struct {
	Deployment *appsv1.Deployment
	Service    *corev1.Service
	// PVC is the optional PersistentVolumeClaim mounted into the backend pod
	// when the CacheBackend spec requests persistent storage. It is nil when
	// storage is ephemeral (cache-home backed by EmptyDir).
	PVC *corev1.PersistentVolumeClaim
	// Endpoint is the in-cluster address engines should use for this backend
	// (published to status for C6 to inject).
	Endpoint string
}

// Builder renders the desired Workload for a single CacheBackendType. It is the
// lightweight seam that keeps engine/runtime flags out of the reconciler. The C5
// module formalizes this into the KVCacheRuntimeAdapter interface and C6
// extends the vLLM+LMCache rendering; until then this stays minimal.
type Builder interface {
	// Type is the CacheBackendType this builder handles.
	Type() cachev1alpha1.CacheBackendType
	// Build renders the desired Deployment + Service from the CacheBackend spec.
	Build(cb *cachev1alpha1.CacheBackend) (*Workload, error)
}

// registry maps a backend type to its builder. Phase 1 ships LMCache only; other
// managed types (SGLangHiCache, AIBrix, Mooncake, NIXL) are out of scope here.
var registry = map[cachev1alpha1.CacheBackendType]Builder{
	cachev1alpha1.CacheBackendTypeLMCache: lmCacheBuilder{},
}

// For returns the Builder for a backend type, if one is registered.
func For(t cachev1alpha1.CacheBackendType) (Builder, bool) {
	b, ok := registry[t]
	return b, ok
}
