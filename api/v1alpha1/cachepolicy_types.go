package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:validation:Enum=LRU;LFU

// CachePolicyEvictionAlgorithm identifies how cache entries are evicted.
type CachePolicyEvictionAlgorithm string

const (
	CachePolicyEvictionAlgorithmLRU CachePolicyEvictionAlgorithm = "LRU"
	CachePolicyEvictionAlgorithmLFU CachePolicyEvictionAlgorithm = "LFU"
)

// +kubebuilder:validation:Enum=All;Tenant;Namespace

// CachePolicyTenantScopeType identifies the tenant scope a policy applies to.
type CachePolicyTenantScopeType string

const (
	CachePolicyTenantScopeAll       CachePolicyTenantScopeType = "All"
	CachePolicyTenantScopeTenant    CachePolicyTenantScopeType = "Tenant"
	CachePolicyTenantScopeNamespace CachePolicyTenantScopeType = "Namespace"
)

// CachePolicySpec defines cache lookup and eviction policy.
type CachePolicySpec struct {
	// Eviction is the cache eviction algorithm.
	// +optional
	Eviction CachePolicyEvictionAlgorithm `json:"eviction,omitempty"`

	// TTL is the maximum time a cache entry should remain usable.
	// +optional
	TTL *metav1.Duration `json:"ttl,omitempty"`

	// MinimumPrefixTokens is the minimum prefix length required before lookup.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinimumPrefixTokens *int32 `json:"minimumPrefixTokens,omitempty"`

	// LookupTimeoutMs bounds cache lookup latency in milliseconds.
	// +optional
	// +kubebuilder:validation:Minimum=0
	LookupTimeoutMs *int32 `json:"lookupTimeoutMs,omitempty"`

	// FailOpen keeps inference serving when the cache is unreachable.
	// +optional
	// +kubebuilder:default=true
	FailOpen *bool `json:"failOpen,omitempty"`

	// TenantScope restricts where this policy applies.
	// +optional
	TenantScope *CachePolicyTenantScopeSpec `json:"tenantScope,omitempty"`
}

// CachePolicyTenantScopeSpec selects the tenants affected by a policy.
type CachePolicyTenantScopeSpec struct {
	// Type is the scope strategy.
	// +optional
	Type CachePolicyTenantScopeType `json:"type,omitempty"`

	// TenantRef names a CacheTenant in the same namespace.
	// +optional
	TenantRef string `json:"tenantRef,omitempty"`

	// TenantID matches an external tenant identifier.
	// +optional
	TenantID string `json:"tenantID,omitempty"`

	// MatchLabels selects CacheTenant resources by label.
	// +optional
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

// CachePolicyStatus defines observed policy state.
type CachePolicyStatus struct {
	// ObservedGeneration is the latest generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the latest observations of the policy.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=cpol
// +kubebuilder:printcolumn:name="Eviction",type=string,JSONPath=`.spec.eviction`
// +kubebuilder:printcolumn:name="FailOpen",type=boolean,JSONPath=`.spec.failOpen`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CachePolicy is the Schema for cache lookup and eviction policy.
type CachePolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CachePolicySpec   `json:"spec,omitempty"`
	Status CachePolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CachePolicyList contains a list of CachePolicy.
type CachePolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CachePolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &CachePolicy{}, &CachePolicyList{})
		return nil
	})
}
