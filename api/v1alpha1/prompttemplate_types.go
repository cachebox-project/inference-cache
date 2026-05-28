package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:validation:Enum=Stable;Mutable

// PromptTemplateSlotType identifies whether a slot contributes to the stable prefix.
type PromptTemplateSlotType string

const (
	PromptTemplateSlotTypeStable  PromptTemplateSlotType = "Stable"
	PromptTemplateSlotTypeMutable PromptTemplateSlotType = "Mutable"
)

// PromptTemplateSpec defines a prompt template and its cache-relevant slots.
type PromptTemplateSpec struct {
	// Body is the template text consumed by the rendering engine.
	// +kubebuilder:validation:Required
	Body string `json:"body"`

	// Slots declares the stable and mutable slots used by the template body.
	// +optional
	// +listType=map
	// +listMapKey=name
	Slots []PromptTemplateSlot `json:"slots,omitempty"`
}

// PromptTemplateSlot declares one named template slot.
type PromptTemplateSlot struct {
	// Name is the slot identifier used by the template body.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Type identifies whether the slot belongs to the stable prefix or mutable suffix.
	// +kubebuilder:validation:Required
	Type PromptTemplateSlotType `json:"type"`

	// Required marks whether callers must provide a value for this slot.
	// +optional
	Required *bool `json:"required,omitempty"`

	// Description is human-readable slot documentation.
	// +optional
	Description string `json:"description,omitempty"`
}

// PromptTemplateStatus defines observed prompt template state.
type PromptTemplateStatus struct {
	// ObservedGeneration is the latest generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// TemplateRevision is a stable revision identifier for cache invalidation.
	// +optional
	TemplateRevision string `json:"templateRevision,omitempty"`

	// Conditions describe the latest observations of the prompt template.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=pt
// +kubebuilder:printcolumn:name="Revision",type=string,JSONPath=`.status.templateRevision`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PromptTemplate is the Schema for cache-aware prompt templates.
type PromptTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PromptTemplateSpec   `json:"spec,omitempty"`
	Status PromptTemplateStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PromptTemplateList contains a list of PromptTemplate.
type PromptTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PromptTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &PromptTemplate{}, &PromptTemplateList{})
		return nil
	})
}
