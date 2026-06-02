package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// CachePolicyEvictionAlgorithm identifies an index entry-eviction algorithm.
// Each value has a corresponding implementation in pkg/index; the enum grows
// as new algorithms land. The choice is per-namespace: the controller flattens
// it (lower-cased) into ResolvedPolicy.Eviction. The index reads it when the
// entry cap is exceeded (to order victims) and, for LFU, on the lookup path (to
// record which entries a delivered hint credits — a timed-out lookup credits
// nothing, and the bump never changes a lookup result). The TTL sweep runs
// regardless of algorithm.
type CachePolicyEvictionAlgorithm string

const (
	// CachePolicyEvictionAlgorithmLRU evicts the entry with the oldest
	// lastSeen timestamp first (least recently seen). This is the default.
	CachePolicyEvictionAlgorithmLRU CachePolicyEvictionAlgorithm = "LRU"
	// CachePolicyEvictionAlgorithmLFU evicts the entry with the lowest access
	// count first (least frequently used), breaking ties on the oldest
	// lastSeen. Access counts do not age — the TTL sweep handles staleness
	// regardless of algorithm, so LFU only governs cap-based eviction.
	CachePolicyEvictionAlgorithmLFU CachePolicyEvictionAlgorithm = "LFU"
)

// CachePolicySpec defines cache lookup and eviction policy.
type CachePolicySpec struct {
	// Eviction is the index entry-eviction algorithm applied when the index
	// exceeds its entry cap. LRU evicts the oldest-by-lastSeen entry first;
	// LFU evicts the lowest-access-count entry first (ties broken on oldest
	// lastSeen). The TTL sweep removes stale entries regardless of this
	// choice, so LFU does not pin hot-but-stale entries. Defaults to LRU.
	// +optional
	// +kubebuilder:validation:Enum=LRU;LFU
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
