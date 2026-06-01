package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:validation:Enum=Fairness

// CacheTenantIsolationMode identifies how tenant cache isolation is enforced.
type CacheTenantIsolationMode string

const (
	CacheTenantIsolationModeFairness CacheTenantIsolationMode = "Fairness"
)

// CacheTenantSpec defines tenant identity and quota.
type CacheTenantSpec struct {
	// TenantID is the external tenant identifier used by gateway and engine traffic.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	TenantID string `json:"tenantID"`

	// Quota bounds this tenant's cache footprint.
	// +optional
	Quota *CacheTenantQuotaSpec `json:"quota,omitempty"`

	// IsolationMode controls how this tenant shares cache resources.
	// +optional
	// +kubebuilder:default=Fairness
	IsolationMode CacheTenantIsolationMode `json:"isolationMode,omitempty"`

	// Crypto is reserved for future tenant encryption settings.
	// +optional
	Crypto *CacheTenantCryptoSpec `json:"crypto,omitempty"`
}

// CacheTenantQuotaSpec defines tenant cache quotas.
//
// Only resources the cache plane authoritatively owns get a max* field. The
// index entry table is ours, so MaxIndexEntries is enforced (over-budget evicts
// the tenant's oldest entries under Fairness). A per-tenant memory budget is
// deliberately absent: the engine KV cache is a shared, tenant-unaware LRU pool,
// so the control plane can neither enforce a byte budget nor honestly attribute
// bytes per tenant on a shared engine. Per-tenant byte isolation is an
// engine/runtime concern (separate engine Deployments + pod memory limits).
type CacheTenantQuotaSpec struct {
	// MaxIndexEntries is the maximum number of distinct prefixes the tenant may
	// hold in the index. The unit is the distinct prefix key
	// (tenant, model, hash_scheme, prefix_hash): a prefix held by several replicas
	// counts once, not once per replica. Over budget, the tenant's oldest prefixes
	// are evicted (Fairness). 0 is a valid cap (admit nothing).
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxIndexEntries *int64 `json:"maxIndexEntries,omitempty"`
}

// CacheTenantCryptoSpec is reserved for future cryptographic isolation.
// +kubebuilder:pruning:PreserveUnknownFields
// +kubebuilder:validation:MaxProperties=0
type CacheTenantCryptoSpec struct{}

// CacheTenantStatus defines observed tenant state.
//
// No MemoryUsed field: per-tenant memory is not honestly observable on a shared,
// tenant-unaware engine (ReplicaStats.cache_memory_bytes is the engine total and
// would be double-counted across tenants sharing it). Operators who want memory
// signals read the cluster-wide CacheIndex tenant aggregate / Prometheus.
type CacheTenantStatus struct {
	// ObservedGeneration is the latest generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// IndexEntries is the observed number of distinct prefixes the tenant holds in
	// the index (same unit as spec.quota.maxIndexEntries — a multi-replica prefix
	// counts once). A nil pointer means "not yet computed" (no snapshot observed),
	// distinct from an observed 0.
	// +optional
	IndexEntries *int64 `json:"indexEntries,omitempty"`

	// Conditions describe the latest observations of the tenant.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=ct
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantID`
// +kubebuilder:printcolumn:name="Entries",type=integer,JSONPath=`.status.indexEntries`
// +kubebuilder:printcolumn:name="Quota",type=integer,JSONPath=`.spec.quota.maxIndexEntries`
// +kubebuilder:printcolumn:name="Isolation",type=string,JSONPath=`.spec.isolationMode`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CacheTenant is the Schema for cache tenant identity and quota.
type CacheTenant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	Spec   CacheTenantSpec   `json:"spec"`
	Status CacheTenantStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CacheTenantList contains a list of CacheTenant.
type CacheTenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CacheTenant `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &CacheTenant{}, &CacheTenantList{})
		return nil
	})
}
