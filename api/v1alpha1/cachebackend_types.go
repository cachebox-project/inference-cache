package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CacheBackendType identifies the backing cache implementation.
type CacheBackendType string

const (
	CacheBackendTypeLMCache       CacheBackendType = "LMCache"
	CacheBackendTypeSGLangHiCache CacheBackendType = "SGLangHiCache"
	CacheBackendTypeAIBrix        CacheBackendType = "AIBrix"
	CacheBackendTypeMooncake      CacheBackendType = "Mooncake"
	CacheBackendTypeNIXL          CacheBackendType = "NIXL"
	CacheBackendTypeExternal      CacheBackendType = "External"
)

// +kubebuilder:validation:Enum=Deployment;StatefulSet

// CacheBackendDeploymentKind identifies the Kubernetes workload kind used for managed backends.
type CacheBackendDeploymentKind string

const (
	CacheBackendDeploymentKindDeployment  CacheBackendDeploymentKind = "Deployment"
	CacheBackendDeploymentKindStatefulSet CacheBackendDeploymentKind = "StatefulSet"
)

// +kubebuilder:validation:Enum=ReadOnly;WriteOnly;ReadWrite

// CacheBackendIntegrationRole identifies how an engine should interact with the cache backend.
type CacheBackendIntegrationRole string

const (
	CacheBackendIntegrationRoleReadOnly  CacheBackendIntegrationRole = "ReadOnly"
	CacheBackendIntegrationRoleWriteOnly CacheBackendIntegrationRole = "WriteOnly"
	CacheBackendIntegrationRoleReadWrite CacheBackendIntegrationRole = "ReadWrite"
)

// +kubebuilder:validation:Enum=Pending;Ready;Degraded;Failed

// CacheBackendHealth summarizes the observed backend health.
type CacheBackendHealth string

const (
	CacheBackendHealthPending  CacheBackendHealth = "Pending"
	CacheBackendHealthReady    CacheBackendHealth = "Ready"
	CacheBackendHealthDegraded CacheBackendHealth = "Degraded"
	CacheBackendHealthFailed   CacheBackendHealth = "Failed"
)

// CacheBackendSpec defines the desired state of a cache backend.
type CacheBackendSpec struct {
	// Type identifies the backing cache implementation.
	// +optional
	Type CacheBackendType `json:"type,omitempty"`

	// DeploymentKind identifies whether a managed backend is reconciled as a Deployment or StatefulSet.
	// +optional
	DeploymentKind CacheBackendDeploymentKind `json:"deploymentKind,omitempty"`

	// Replicas is the desired number of backend workload replicas.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// Storage describes persistent storage requested by the backend.
	// +optional
	Storage *CacheBackendStorageSpec `json:"storage,omitempty"`

	// Integration describes how inference engines should use the cache backend.
	// +optional
	Integration *CacheBackendIntegrationSpec `json:"integration,omitempty"`

	// EngineSelector selects engine pods or runtimes this cache backend applies to.
	// +optional
	EngineSelector *CacheBackendEngineSelector `json:"engineSelector,omitempty"`

	// BackendConfig contains backend-specific string settings.
	// +optional
	BackendConfig map[string]string `json:"backendConfig,omitempty"`

	// Template provides pod-level overrides for managed backend workloads.
	// +optional
	Template *CacheBackendPodSpecOverride `json:"template,omitempty"`

	// Endpoint is the optional network address for an existing backend.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
}

// CacheBackendStorageSpec defines storage settings for a cache backend.
type CacheBackendStorageSpec struct {
	// PVC describes a persistent volume claim used by the backend.
	// +optional
	PVC *CacheBackendPVCSpec `json:"pvc,omitempty"`
}

// CacheBackendPVCSpec defines PVC-backed storage settings.
type CacheBackendPVCSpec struct {
	// Size is the requested persistent storage size.
	// +kubebuilder:validation:Required
	Size resource.Quantity `json:"size"`

	// StorageClassName is the optional StorageClass for the PVC.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// CacheBackendIntegrationSpec describes engine integration behavior.
type CacheBackendIntegrationSpec struct {
	// Engine identifies the inference engine integration, such as SGLang or vLLM.
	// +optional
	Engine string `json:"engine,omitempty"`

	// Role controls whether the engine reads from, writes to, or fully participates in the cache.
	// +optional
	Role CacheBackendIntegrationRole `json:"role,omitempty"`

	// LookupTimeoutMs bounds cache lookup latency in milliseconds.
	// +optional
	// +kubebuilder:validation:Minimum=0
	LookupTimeoutMs *int32 `json:"lookupTimeoutMs,omitempty"`

	// MinimumPrefixTokens is the minimum prefix length required before cache lookup is attempted.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinimumPrefixTokens *int32 `json:"minimumPrefixTokens,omitempty"`
}

// CacheBackendEngineSelector selects engines by labels.
type CacheBackendEngineSelector struct {
	// MatchLabels is a map of labels that selected engines must match.
	// +optional
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

// CacheBackendPodSpecOverride defines optional pod-level overrides applied to managed backend pods.
type CacheBackendPodSpecOverride struct {
	// NodeSelector constrains backend pods to nodes with matching labels.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity configures backend pod scheduling affinity.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Tolerations allow backend pods to schedule onto tainted nodes.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// TopologySpreadConstraints configures backend pod spreading across topology domains.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// ImagePullSecrets references secrets used to pull backend pod images.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// ServiceAccountName is the service account used by backend pods.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// SecurityContext configures pod-level security settings for backend pods.
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`

	// PriorityClassName is the priority class assigned to backend pods.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// SchedulerName selects the scheduler used for backend pods.
	// +optional
	SchedulerName string `json:"schedulerName,omitempty"`

	// RuntimeClassName selects the runtime class used for backend pods.
	// +optional
	RuntimeClassName *string `json:"runtimeClassName,omitempty"`

	// TerminationGracePeriodSeconds configures graceful shutdown for backend pods.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
}

// CacheBackendStatus defines the observed state of a cache backend.
type CacheBackendStatus struct {
	// Endpoint is the observed endpoint clients should use for this backend.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Health summarizes the observed backend health.
	// +optional
	Health CacheBackendHealth `json:"health,omitempty"`

	// IndexEntries is the observed number of cache index entries for this backend.
	// +optional
	// +kubebuilder:validation:Minimum=0
	IndexEntries *int64 `json:"indexEntries,omitempty"`

	// Conditions describe the latest observations of the backend.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=cb
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Health",type=string,JSONPath=`.status.health`
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.status.endpoint`
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
