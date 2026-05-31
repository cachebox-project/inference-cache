package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
//
// Persistent storage (spec.storage.pvc) and the autoscaling spec
// (spec.autoscaling) are both surfaced here for v1alpha1 forward-compat. The
// autoscaling spec is reconciled into a HorizontalPodAutoscaler today;
// spec.storage.pvc is accepted but inert until a follow-up wires it through
// to the runtime adapter's rendered pod (there is no PVC provisioning, no
// volume injection, and no status.capacity reporting until then).
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

	// Autoscaling configures horizontal autoscaling for the managed backend
	// workload. When set, the controller reconciles a HorizontalPodAutoscaler
	// owned by this CacheBackend; the HPA then drives the underlying workload's
	// replica count, overriding spec.replicas.
	// +optional
	Autoscaling *CacheBackendAutoscalingSpec `json:"autoscaling,omitempty"`

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

	// AllowCrossNamespace opts the CacheBackend into referencing an Endpoint
	// that resolves into a Kubernetes Service in a different namespace from
	// this object. Without this opt-in admission rejects such Endpoints,
	// because a cross-namespace reference crosses a tenancy/RBAC boundary that
	// the cluster operator should explicitly acknowledge. Endpoints that are
	// not in-cluster Service DNS (external hostnames, IPs) are unaffected.
	// +optional
	AllowCrossNamespace bool `json:"allowCrossNamespace,omitempty"`
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

// CacheBackendAutoscalingSpec configures horizontal autoscaling of the managed
// backend workload via a HorizontalPodAutoscaler. Cache-aware (custom-metric)
// autoscaling is deferred to a later module; Phase 1 supports a CPU-utilization
// target, which is sufficient to demonstrate scale-up under load.
//
// +kubebuilder:validation:XValidation:rule="!has(self.minReplicas) || self.minReplicas <= self.maxReplicas",message="minReplicas must not exceed maxReplicas"
type CacheBackendAutoscalingSpec struct {
	// MinReplicas is the lower bound for the HPA replica count. Defaults to 1
	// when unset.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MinReplicas *int32 `json:"minReplicas,omitempty"`

	// MaxReplicas is the upper bound for the HPA replica count.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	MaxReplicas int32 `json:"maxReplicas"`

	// TargetCPUUtilizationPercent is the average per-pod CPU utilization the
	// HPA targets. Defaults to 80 when unset.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	TargetCPUUtilizationPercent *int32 `json:"targetCPUUtilizationPercent,omitempty"`
}

// CacheBackendIntegrationSpec describes engine integration behavior.
//
// Per-namespace lookup tuning lives on CachePolicy, not here: the lookup
// deadline and the minimum-prefix-token gate are configured via
// CachePolicy.spec.lookupTimeoutMs and CachePolicy.spec.minimumPrefixTokens,
// which are the surfaces actually wired into the server's ResolvedPolicy.
type CacheBackendIntegrationSpec struct {
	// Engine identifies the inference engine integration, such as SGLang or vLLM.
	// +optional
	Engine string `json:"engine,omitempty"`

	// Role controls whether the engine reads from, writes to, or fully participates in the cache.
	// +optional
	Role CacheBackendIntegrationRole `json:"role,omitempty"`

	// FailOpen controls whether the engine treats cache lookups as a soft
	// dependency. When true (the default), an unreachable or degraded cache
	// backend MUST fall back to local prefill and never fail a serving
	// request — the cache is an optimization, not a serving dependency. When
	// explicitly set to false the engine fails requests on cache
	// unreachability ("fail-closed"); the cache becomes a serving
	// dependency, which is loud and visible via a Warning Event on the
	// owning CacheBackend.
	//
	// The flag is plumbed by the engine adapter; per-request fail-open
	// behavior at the engine level is owned by vLLM+LMCache (the connector
	// honors this flag).
	// +optional
	// +kubebuilder:default=true
	FailOpen *bool `json:"failOpen,omitempty"`

	// EngineOverrides lets the operator amend the non-reserved args / env
	// the pod-mutating webhook injects into the engine container, on top
	// of what the runtime adapter would otherwise inject. Useful for
	// tuning adapter-injected knobs (e.g. CPU-vLLM running against the
	// LMCache integration with non-default chunk size / serdes / model
	// length) and for future engines that surface their own non-reserved
	// flags through the same adapter interface.
	//
	// EngineOverrides does NOT turn the integration off: every reserved
	// arg/env the adapter declares is hard-rejected at admission, so an
	// operator who wants to skip injection entirely on a particular pod
	// should use the inferencecache.io/skip-inject pod annotation instead.
	//
	// Admission rejects overrides that overlap the adapter's reserved
	// args/env (the ones strictly required for the integration to
	// function); the operator gets a field-scoped error naming the
	// offending flag/env and the adapter rather than discovering it via a
	// crashed engine. See the package doc for the rationale.
	// +optional
	EngineOverrides *EngineInjectionOverrides `json:"engineOverrides,omitempty"`
}

// EngineInjectionOverrides describes how the operator wants to amend the
// args / env the pod-mutating webhook injects into the engine container.
// The override surface is SCOPED to entries the runtime adapter itself
// contributes (added or modified) during InjectEngineConfig — user
// pod-template args / env that the adapter does not touch are protected,
// and a Suppress or Override naming them is a silent no-op. This keeps the
// CR from mutating engine-pod-template state the engine-pod owner did not
// invite the CacheBackend to touch.
//
// Known-fragile: nothing here is type-checked against the engine binary, so
// an override on an adapter-owned non-reserved value can still break the
// engine in subtle ways the validator can't catch (e.g. an aggressive
// `--max-model-len` OOMing the engine). Admission only blocks overrides
// that overlap the adapter's reserved set — the args/env strictly required
// for the integration itself to function.
type EngineInjectionOverrides struct {
	// Args injected into the engine container, in addition to what the
	// adapter would inject. Merged by leading flag token (e.g.
	// "--max-model-len"): an override entry whose leading token matches
	// an adapter-owned canonical entry replaces it; entries whose token
	// is in neither the adapter-owned set nor the user pod-template are
	// appended; entries colliding with a user-template flag the adapter
	// did not touch are a silent no-op. Order is preserved.
	//
	// Admission rejects entries whose leading flag token overlaps
	// the adapter's ReservedArgs().
	// +optional
	Args []string `json:"args,omitempty"`

	// SuppressArgs lists leading flag names (e.g. "--some-tunable-flag")
	// the adapter MUST NOT inject. Admission rejects entries that overlap
	// the adapter's ReservedArgs(). A suppressed flag is removed from the
	// adapter's canonical contribution before Args merges in, so
	// suppress-then-re-add is a supported pattern for overriding a
	// non-reserved adapter-owned flag's value. Suppress does NOT touch
	// user pod-template flags the adapter did not inject.
	// +optional
	SuppressArgs []string `json:"suppressArgs,omitempty"`

	// Env upserted into the engine container by Name, scoped to
	// adapter-owned canonical entries. A Name matching an adapter-owned
	// entry is replaced; a Name not seen on the user pod-template is
	// appended; a Name colliding with a user-template env the adapter
	// did not touch is a silent no-op. Admission rejects entries whose
	// Name overlaps the adapter's ReservedEnv().
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// SuppressEnv lists env var Names the adapter MUST NOT inject.
	// Admission rejects entries that overlap the adapter's ReservedEnv().
	// Suppress does NOT touch user pod-template env the adapter did not
	// inject.
	// +optional
	SuppressEnv []string `json:"suppressEnv,omitempty"`
}

// IntegrationFailOpen returns the effective fail-open behavior for a
// CacheBackend integration spec. Missing spec or nil field defaults to true,
// matching the API default — the cache is an optimization, never a serving
// dependency. Engine adapters consult this helper to set the engine connector
// flags consistently across the spec→adapter path.
func IntegrationFailOpen(spec *CacheBackendIntegrationSpec) bool {
	if spec == nil || spec.FailOpen == nil {
		return true
	}
	return *spec.FailOpen
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

	// Capacity is a human-readable summary of the backend's provisioned
	// capacity (e.g. the requested PVC size when persistent storage is
	// actually wired through to the cache server). It is informational;
	// clients must not parse it. The field is intentionally left empty
	// until the storage wire-up follow-up lands — the rendered cache-server
	// pod has no data volume to attach a PVC to today, so reporting a
	// requested size as "provisioned" would mislead operators.
	// +optional
	Capacity string `json:"capacity,omitempty"`

	// FailOpen mirrors the effective spec.integration.failOpen value the
	// controller most recently observed. Surfaced so operators can confirm
	// whether the cache is currently a soft optimization (true) or a
	// serving dependency (false) without re-reading the integration spec.
	// +optional
	FailOpen *bool `json:"failOpen,omitempty"`

	// ObservedGeneration is the .metadata.generation last reconciled by the controller.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// IndexParticipation summarizes this CacheBackend's contribution to the
	// cluster-wide cache index — populated by the CacheIndex poller (it groups
	// the server's /snapshot replicas by the owning CacheBackend and projects
	// the per-backend slice here). nil until the poller has observed at least
	// one snapshot; absence of data on a single scrape never clears it.
	// +optional
	IndexParticipation *CacheBackendIndexParticipation `json:"indexParticipation,omitempty"`

	// Conditions describe the latest observations of the backend.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CacheBackendIndexParticipation is the per-backend slice of the cluster-wide
// CacheIndex, projected from the server's /snapshot replicas[]. The poller
// resolves each replica to its engine pod by (tenant, replica_id) and then
// attributes it to the owning CacheBackend — either via the engine pod's
// `inferencecache.io/injected-by` annotation (the authoritative wiring
// signal stamped by the pod webhook) or, for pods that bypassed the
// webhook, via a deterministic first-match on `spec.engineSelector.
// matchLabels`. The poller writes write-only-on-change and never clears
// it on a single failed scrape (soft state).
type CacheBackendIndexParticipation struct {
	// PrefixCount is the sum of distinct prefix entries currently attributed
	// to this backend's replicas. Zero is a valid observed value — it means
	// the backend is up but holds no warm prefixes yet.
	// +kubebuilder:validation:Minimum=0
	PrefixCount int64 `json:"prefixCount"`

	// LastEventAt is the most recent KV-event timestamp observed for any of
	// this backend's replicas. nil until the first event arrives; downstream
	// readiness gates (e.g. "ready once at least one event seen") MUST treat
	// nil as "not yet observed" rather than zero time.
	// +optional
	LastEventAt *metav1.Time `json:"lastEventAt,omitempty"`

	// HitRate is the prefix-count-weighted average cache hit rate across this
	// backend's replicas, formatted as a decimal string in [0,1] (matching
	// the cluster-wide CacheIndex.status.replicas[].hitRate convention — see
	// CRD-codegen note on floats in CRDs). nil until the replica stats
	// reporter emits per-replica hitRate into the index; do not interpret a
	// missing value as 0.
	// +optional
	HitRate *string `json:"hitRate,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=cb
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Health",type=string,JSONPath=`.status.health`
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.status.endpoint`
// +kubebuilder:printcolumn:name="Prefixes",type=integer,JSONPath=`.status.indexParticipation.prefixCount`
// +kubebuilder:printcolumn:name="LastEvent",type=date,JSONPath=`.status.indexParticipation.lastEventAt`
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
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &CacheBackend{}, &CacheBackendList{})
		return nil
	})
}
