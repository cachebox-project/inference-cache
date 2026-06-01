package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// CachePolicyEvictionAlgorithm identifies an index entry-eviction algorithm.
// The enum is intentionally narrow today (LRU only) and grows as additional
// algorithms are implemented in pkg/index. The field is reserved on the
// v1alpha1 surface so the schema is forward-compatible; pkg/index currently
// runs LRU-by-`lastSeen` unconditionally and the controller does not yet
// propagate this field into ResolvedPolicy.
type CachePolicyEvictionAlgorithm string

const (
	// CachePolicyEvictionAlgorithmLRU evicts the entry with the oldest
	// lastSeen timestamp first. This is the implementation today.
	CachePolicyEvictionAlgorithmLRU CachePolicyEvictionAlgorithm = "LRU"
)

// CachePolicySpec defines cache lookup and eviction policy.
type CachePolicySpec struct {
	// Eviction is the index entry-eviction algorithm. Reserved on
	// v1alpha1 for forward compatibility; defaults to LRU. Today pkg/index
	// runs LRU unconditionally and this field is not yet consulted by the
	// controller. Additional algorithms extend the enum and gain a
	// ResolvedPolicy propagation path as their implementations land.
	// +optional
	// +kubebuilder:validation:Enum=LRU
	// +kubebuilder:default=LRU
	Eviction CachePolicyEvictionAlgorithm `json:"eviction,omitempty"`

	// EvictionTTL is the maximum time a cache entry should remain usable.
	// +optional
	EvictionTTL *metav1.Duration `json:"evictionTTL,omitempty"`

	// MinimumPrefixTokens is the minimum prefix length required before lookup.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinimumPrefixTokens *int32 `json:"minimumPrefixTokens,omitempty"`

	// LookupTimeoutMs bounds cache lookup latency in milliseconds.
	// +optional
	// +kubebuilder:validation:Minimum=0
	LookupTimeoutMs *int32 `json:"lookupTimeoutMs,omitempty"`
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
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CachePolicy is the Schema for cache lookup and eviction policy.
type CachePolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	Spec   CachePolicySpec   `json:"spec"`
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
