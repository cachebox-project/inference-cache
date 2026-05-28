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
type CacheTenantQuotaSpec struct {
	// MaxMemoryBytes is the maximum cache memory attributed to the tenant.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxMemoryBytes *int64 `json:"maxMemoryBytes,omitempty"`

	// MaxIndexEntries is the maximum number of index entries attributed to the tenant.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxIndexEntries *int64 `json:"maxIndexEntries,omitempty"`
}

// CacheTenantCryptoSpec is reserved for future cryptographic isolation.
// +kubebuilder:pruning:PreserveUnknownFields
// +kubebuilder:validation:MaxProperties=0
type CacheTenantCryptoSpec struct{}

// CacheTenantStatus defines observed tenant state.
type CacheTenantStatus struct {
	// ObservedGeneration is the latest generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// MemoryUsed is the observed tenant cache memory in bytes.
	// +optional
	MemoryUsed *int64 `json:"memoryUsed,omitempty"`

	// IndexEntries is the observed tenant index entry count.
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
// +kubebuilder:printcolumn:name="TenantID",type=string,JSONPath=`.spec.tenantID`
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
