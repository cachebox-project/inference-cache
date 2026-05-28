package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PDTopologySpec defines prefill/decode topology for phase-disaggregated serving.
type PDTopologySpec struct {
	// PrefillPools declares pools that can serve prefill work.
	// +optional
	// +listType=map
	// +listMapKey=name
	PrefillPools []PDPoolSpec `json:"prefillPools,omitempty"`

	// DecodePools declares pools that can serve decode work.
	// +optional
	// +listType=map
	// +listMapKey=name
	DecodePools []PDPoolSpec `json:"decodePools,omitempty"`

	// AcceleratorTypes declares the accelerator classes used by the pools.
	// +optional
	// +listType=map
	// +listMapKey=name
	AcceleratorTypes []PDAcceleratorTypeSpec `json:"acceleratorTypes,omitempty"`
}

// PDPoolSpec declares a prefill or decode pool.
type PDPoolSpec struct {
	// Name is the pool identifier.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// MatchLabels selects pods or nodes belonging to this pool.
	// +optional
	MatchLabels map[string]string `json:"matchLabels,omitempty"`

	// Replicas is the desired pool size.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// AcceleratorType references spec.acceleratorTypes by name.
	// +optional
	AcceleratorType string `json:"acceleratorType,omitempty"`
}

// PDAcceleratorTypeSpec describes an accelerator class used by a topology.
type PDAcceleratorTypeSpec struct {
	// Name is the accelerator type identifier.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Vendor is the accelerator vendor or runtime family.
	// +optional
	Vendor string `json:"vendor,omitempty"`

	// Model is the accelerator model or SKU.
	// +optional
	Model string `json:"model,omitempty"`

	// MatchLabels identifies nodes or pods that expose this accelerator type.
	// +optional
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

// PDTopologyStatus defines observed topology state.
type PDTopologyStatus struct {
	// ObservedGeneration is the latest generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the latest observations of the topology.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=pdt
// +kubebuilder:printcolumn:name="Prefill",type=string,JSONPath=`.spec.prefillPools[*].name`
// +kubebuilder:printcolumn:name="Decode",type=string,JSONPath=`.spec.decodePools[*].name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PDTopology is the Schema for prefill/decode topology declarations.
type PDTopology struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PDTopologySpec   `json:"spec,omitempty"`
	Status PDTopologyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PDTopologyList contains a list of PDTopology.
type PDTopologyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PDTopology `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &PDTopology{}, &PDTopologyList{})
		return nil
	})
}
