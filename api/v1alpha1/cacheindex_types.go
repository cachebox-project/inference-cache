package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// CacheIndexSpec is intentionally empty: CacheIndex is a status-only,
// controller-maintained reflection of the server's in-memory cache aggregate.
// There is nothing for a user to configure on the spec.
type CacheIndexSpec struct{}

// ReplicaCacheStatus is the latest reported cache health for one engine replica.
type ReplicaCacheStatus struct {
	// ID is the replica identifier (the engine/pod that reported the state).
	ID string `json:"id"`
	// Tenant is the tenant the replica reports under. The subscriber sidecar
	// derives it from the engine pod's namespace, so two pods sharing a
	// metadata.name across namespaces are kept as separate replica rows.
	// Listed alongside ID in the map-list key so cross-tenant collisions
	// cannot violate the listMapKey uniqueness invariant.
	Tenant string `json:"tenant"`
	// CacheMemoryBytes is the cache memory the replica reports using.
	// +optional
	CacheMemoryBytes int64 `json:"cacheMemoryBytes,omitempty"`
	// HitRate is the replica's rolling cache hit rate in [0,1], as a decimal
	// string (e.g. "0.78"). A string is used because CRDs avoid floats for
	// cross-language portability.
	// +optional
	HitRate string `json:"hitRate,omitempty"`
	// Pressure is the replica's memory-pressure heuristic in [0,1], as a decimal
	// string (e.g. "0.65").
	// +optional
	Pressure string `json:"pressure,omitempty"`
	// LastUpdate is when this replica's reported state last *changed*. Like the
	// top-level status.lastUpdated, the controller writes status only on change,
	// so a replica that keeps reporting identical stats does not advance this —
	// it marks the last change, not the last observation. (Reporter liveness is
	// observable via the server's /metrics, not this field.)
	// +optional
	LastUpdate metav1.Time `json:"lastUpdate,omitempty"`
}

// TenantCacheStatus is the aggregate cache footprint for one tenant.
type TenantCacheStatus struct {
	// ID is the tenant identifier.
	ID string `json:"id"`
	// MemoryUsed is the approximate cache memory attributed to the tenant
	// (summed over the tenant's distinct replicas).
	// +optional
	MemoryUsed int64 `json:"memoryUsed,omitempty"`
	// HitRate is the tenant's mean replica hit rate in [0,1], as a decimal
	// string (e.g. "0.82").
	// +optional
	HitRate string `json:"hitRate,omitempty"`
}

// PrefixSummary summarizes the prefix entries held across the cluster.
type PrefixSummary struct {
	// Total is the number of distinct prefixes currently in the index.
	// Always rendered (including 0) — it's the core summary, not omitempty.
	Total int64 `json:"total"`
	// Hot is the number of prefixes with access count above the hot threshold.
	// Always 0 until per-prefix access counting is implemented; rendered
	// explicitly so an empty index shows hot: 0 rather than omitting it.
	Hot int64 `json:"hot"`
}

// PrefixStatus holds the prefix summary. The summary is nested (rather than
// flattened onto status.prefixes) to match the contract shape
// status.prefixes.summary.{total,hot} and leave room for future per-prefix detail.
type PrefixStatus struct {
	// Summary aggregates the prefix entries held across the cluster.
	// +optional
	Summary PrefixSummary `json:"summary,omitempty"`
}

// CacheIndexStatus is the observed, cluster-wide cache aggregate the controller
// reflects from the server's in-memory index. Metadata only — never KV tensors
// or prompt text.
type CacheIndexStatus struct {
	// Replicas is the per-replica cache health.
	// +optional
	// +listType=map
	// +listMapKey=tenant
	// +listMapKey=id
	Replicas []ReplicaCacheStatus `json:"replicas,omitempty"`
	// Tenants is the per-tenant cache footprint.
	// +optional
	// +listType=map
	// +listMapKey=id
	Tenants []TenantCacheStatus `json:"tenants,omitempty"`
	// Prefixes summarizes prefix entries in the index.
	// +optional
	Prefixes PrefixStatus `json:"prefixes,omitempty"`
	// ObservedServer is the server endpoint the aggregate was scraped from.
	// +optional
	ObservedServer string `json:"observedServer,omitempty"`
	// LastUpdated is when the observed aggregate last *changed*. The controller
	// writes the status only on change, so a steady-state poll that finds no
	// change does not advance this — it marks the last data change, not the last
	// poll. (Poller liveness is observable via controller health/metrics.)
	// +optional
	LastUpdated metav1.Time `json:"lastUpdated,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=ci
// +kubebuilder:printcolumn:name="Prefixes",type=integer,JSONPath=`.status.prefixes.summary.total`
// +kubebuilder:printcolumn:name="Changed",type=date,JSONPath=`.status.lastUpdated`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CacheIndex is a cluster-scoped, status-only reflection of the server's
// in-memory cache aggregate (the CacheIndex). It exists for observability
// (`kubectl get cacheindex`); it is not a routing-decision substrate.
type CacheIndex struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CacheIndexSpec   `json:"spec,omitempty"`
	Status CacheIndexStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CacheIndexList contains a list of CacheIndex.
type CacheIndexList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CacheIndex `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &CacheIndex{}, &CacheIndexList{})
		return nil
	})
}
