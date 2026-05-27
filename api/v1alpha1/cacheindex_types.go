package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// CacheIndexSpec is intentionally empty: CacheIndex is a status-only,
// controller-maintained reflection of the server's in-memory cache aggregate.
// There is nothing for a user to configure on the spec.
type CacheIndexSpec struct{}

// ReplicaCacheStatus is the latest reported cache health for one engine replica.
type ReplicaCacheStatus struct {
	// ID is the replica identifier (the engine/pod that reported the state).
	ID string `json:"id"`
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
	// LastUpdate is when the replica's state was last observed.
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
	// +optional
	Total int64 `json:"total,omitempty"`
	// Hot is the number of prefixes with access count above the hot threshold.
	// Always 0 until per-prefix access counting is implemented.
	// +optional
	Hot int64 `json:"hot,omitempty"`
}

// CacheIndexStatus is the observed, cluster-wide cache aggregate the controller
// reflects from the server's in-memory index. Metadata only — never KV tensors
// or prompt text.
type CacheIndexStatus struct {
	// Replicas is the per-replica cache health.
	// +optional
	// +listType=map
	// +listMapKey=id
	Replicas []ReplicaCacheStatus `json:"replicas,omitempty"`
	// Tenants is the per-tenant cache footprint.
	// +optional
	// +listType=map
	// +listMapKey=id
	Tenants []TenantCacheStatus `json:"tenants,omitempty"`
	// Prefixes summarizes prefix entries in the index.
	// +optional
	Prefixes PrefixSummary `json:"prefixes,omitempty"`
	// ObservedServer is the server endpoint the aggregate was scraped from.
	// +optional
	ObservedServer string `json:"observedServer,omitempty"`
	// LastUpdated is when the controller last refreshed this status.
	// +optional
	LastUpdated metav1.Time `json:"lastUpdated,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=ci
// +kubebuilder:printcolumn:name="Prefixes",type=integer,JSONPath=`.status.prefixes.total`
// +kubebuilder:printcolumn:name="Updated",type=date,JSONPath=`.status.lastUpdated`
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
	SchemeBuilder.Register(&CacheIndex{}, &CacheIndexList{})
}
