package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// CacheBackendSpec defines the desired state of a cache backend.
type CacheBackendSpec struct {
	// Type identifies the backing cache implementation.
	// +optional
	Type string `json:"type,omitempty"`

	// Endpoint is the optional network address for an existing backend.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
}

// CacheBackendStatus defines the observed state of a cache backend.
type CacheBackendStatus struct {
	// Conditions describe the latest observations of the backend.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=cb
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.spec.endpoint`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CacheBackend is the Schema for the cachebackends API.
type CacheBackend struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CacheBackendSpec   `json:"spec,omitempty"`
	Status CacheBackendStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CacheBackendList contains a list of CacheBackend.
type CacheBackendList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CacheBackend `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CacheBackend{}, &CacheBackendList{})
}
