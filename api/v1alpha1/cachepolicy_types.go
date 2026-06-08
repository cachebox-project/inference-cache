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

	// MinimumMatchedTokens is the minimum number of MATCHED prefix tokens a
	// LookupRoute response must achieve before PREFIX_MATCH is returned.
	// Distinct from MinimumPrefixTokens, which is a request-side gate on
	// effective prefix tokens BEFORE the index is consulted; this is a
	// result-side floor applied AFTER the lookup, against the actual matched
	// overlap. Replicas whose match falls below this threshold are filtered
	// from the response; if no replica clears the floor, the reason_code is
	// downgraded to NO_HINT so the gateway round-robins honestly instead of
	// being credited with a trivial chat-template-only match. Defaults to 64
	// (4 blocks at the typical 16-token block size — well above the framing
	// tokens identical across every replica). Set to 0 to disable the floor
	// entirely.
	// +optional
	// +kubebuilder:default=64
	// +kubebuilder:validation:Minimum=0
	MinimumMatchedTokens *int32 `json:"minimumMatchedTokens,omitempty"`

	// RoutingFloorScore is the per-replica score below which a PREFIX_MATCH
	// response downgrades to NO_HINT. The LookupRoute ranker computes
	//
	//   score = matched_tokens × freshness × distinguishing_power
	//
	// where distinguishing_power = 1 - (num_replicas_holding_match /
	// num_replicas_in_scope). Overlaps held by every replica (chat-template
	// framing, RAG corpus headers, custom system prompts shared across the
	// deployment) collapse to distinguishing_power=0 → score=0 → caught
	// by this floor. The default "0.1" catches just the score=0 case while
	// passing any non-zero distinguishing match through; raise (e.g. "5")
	// for stricter routing-signal hygiene; "0" disables the floor entirely
	// (raw-recall benchmarking, ranker debugging).
	//
	// Distinct from MinimumPrefixTokens: that field is a request-side gate
	// on the request's claimed prefix length BEFORE the index is consulted;
	// this is a result-side floor on the per-replica score AFTER the
	// distinguishing-power-aware ranker has scored every candidate.
	//
	// Composes with MinimumMatchedTokens (above): the MatchedTokens floor is
	// applied first (filters sub-floor replicas per-replica), then the
	// RoutingFloorScore floor is applied to the top-ranked survivor's
	// score. Both floors can downgrade a PREFIX_MATCH to NO_HINT
	// independently; an operator can disable either by setting it to its
	// opt-out value (0 / "0").
	//
	// Encoded as a stringified float to avoid introducing the first
	// float-typed field into the CachePolicy schema (the others are
	// int32 / duration). Validated by the +kubebuilder:validation:Pattern
	// marker: digits with an optional decimal part, no sign.
	//
	// +optional
	// +kubebuilder:default="0.1"
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)(\.[0-9]+)?$`
	RoutingFloorScore *string `json:"routingFloorScore,omitempty"`

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
