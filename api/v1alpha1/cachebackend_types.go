// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:validation:Enum=VLLM;SGLang

// CacheBackendRuntime identifies the inference runtime whose engine Pods are
// wired to this cache hierarchy.
type CacheBackendRuntime string

const (
	CacheBackendRuntimeVLLM   CacheBackendRuntime = "VLLM"
	CacheBackendRuntimeSGLang CacheBackendRuntime = "SGLang"

	// CacheBackendDomainLabel is the sole supported engineSelector key. Its
	// namespace-scoped value identifies one runtime/model/KV-layout/trust
	// compatibility domain. Engine Pods may carry other labels, but cache
	// ownership is intentionally independent of them.
	CacheBackendDomainLabel = "inferencecache.io/cache-domain"
)

// +kubebuilder:validation:Enum=LMCache;SGLangHiCache

// CacheBackendType identifies the backing cache implementation.
type CacheBackendType string

const (
	CacheBackendTypeLMCache       CacheBackendType = "LMCache"
	CacheBackendTypeSGLangHiCache CacheBackendType = "SGLangHiCache"
)

// +kubebuilder:validation:Enum=Redis

// CacheBackendRemoteStorageProvider identifies the technology used for the
// optional shared/remote cache tier.
type CacheBackendRemoteStorageProvider string

const CacheBackendRemoteStorageProviderRedis CacheBackendRemoteStorageProvider = "Redis"

// +kubebuilder:validation:Enum=Managed;External

// CacheBackendRemoteStorageOwnership identifies who owns the remote provider's
// lifecycle.
type CacheBackendRemoteStorageOwnership string

const (
	CacheBackendRemoteStorageOwnershipManaged  CacheBackendRemoteStorageOwnership = "Managed"
	CacheBackendRemoteStorageOwnershipExternal CacheBackendRemoteStorageOwnership = "External"
)

// +kubebuilder:validation:Enum=PodLocal;NodeLocal

// LMCacheTopology identifies where the LMCache multiprocess server runs
// relative to the selected inference-engine Pods. LMCache is MP-only in the
// canonical API, so the process model is not repeated as an extra API level.
type LMCacheTopology string

const (
	LMCacheTopologyPodLocal  LMCacheTopology = "PodLocal"
	LMCacheTopologyNodeLocal LMCacheTopology = "NodeLocal"
)

// +kubebuilder:validation:Enum=Multiprocess

// LMCacheConnectorMode identifies the connector protocol reflected in status.
// Multiprocess is the only canonical LMCache data plane.
type LMCacheConnectorMode string

const (
	LMCacheConnectorModeMultiprocess LMCacheConnectorMode = "Multiprocess"
)

// +kubebuilder:validation:Enum=ReadOnly;WriteOnly;ReadWrite

// CacheBackendIntegrationRole identifies how an engine should interact with the cache backend.
type CacheBackendIntegrationRole string

const (
	CacheBackendIntegrationRoleReadOnly  CacheBackendIntegrationRole = "ReadOnly"
	CacheBackendIntegrationRoleWriteOnly CacheBackendIntegrationRole = "WriteOnly"
	CacheBackendIntegrationRoleReadWrite CacheBackendIntegrationRole = "ReadWrite"
)

// +kubebuilder:validation:Enum=Offload;EventsOnly

// CacheBackendIntegrationMode selects which cache tiers an engine is wired for.
type CacheBackendIntegrationMode string

const (
	// CacheBackendIntegrationModeOffload is the default: the engine is wired for
	// cache-aware routing (tier-1) AND the configured offload tier (tier-2).
	// Server-backed adapters provision a managed backend; engine-local adapters
	// such as native SGLang HiCache configure the engine Pod directly.
	CacheBackendIntegrationModeOffload CacheBackendIntegrationMode = "Offload"
	// CacheBackendIntegrationModeEventsOnly wires the engine for cache-aware
	// routing (tier-1) ONLY: the kvevent-subscriber observation sidecar is
	// injected, but NO KV connector is loaded into the engine and NO backend
	// server is provisioned. This is the supported integration for
	// hybrid-attention models (Qwen3.6/Next gated-DeltaNet, Mamba/Jamba, KDA,
	// Falcon-H, Granite-hybrid, …): vLLM disables its hybrid KV-cache manager
	// the moment any KV connector is loaded (KV-spec unification then fails at
	// init), so they cannot take the tier-2 connector — but their KV events
	// coexist fine with the hybrid manager, so routing still works. Also a
	// lighter deployment for routing-only users who do not want an offload tier.
	CacheBackendIntegrationModeEventsOnly CacheBackendIntegrationMode = "EventsOnly"
)

// +kubebuilder:validation:Enum=write_back;write_through;write_through_selective

// SGLangHiCacheWritePolicy controls when SGLang copies KV pages to host memory.
type SGLangHiCacheWritePolicy string

const (
	SGLangHiCacheWriteBack             SGLangHiCacheWritePolicy = "write_back"
	SGLangHiCacheWriteThrough          SGLangHiCacheWritePolicy = "write_through"
	SGLangHiCacheWriteThroughSelective SGLangHiCacheWritePolicy = "write_through_selective"
)

// +kubebuilder:validation:Enum=direct;kernel;kernel_ascend

// SGLangHiCacheIOBackend selects SGLang's host/device transfer implementation.
type SGLangHiCacheIOBackend string

const (
	SGLangHiCacheIODirect       SGLangHiCacheIOBackend = "direct"
	SGLangHiCacheIOKernel       SGLangHiCacheIOBackend = "kernel"
	SGLangHiCacheIOKernelAscend SGLangHiCacheIOBackend = "kernel_ascend"
)

// +kubebuilder:validation:Enum=layer_first;page_first;page_first_direct;page_first_kv_split;page_head

// SGLangHiCacheMemoryLayout selects SGLang's host-memory tensor layout.
type SGLangHiCacheMemoryLayout string

const (
	SGLangHiCacheMemoryLayerFirst       SGLangHiCacheMemoryLayout = "layer_first"
	SGLangHiCacheMemoryPageFirst        SGLangHiCacheMemoryLayout = "page_first"
	SGLangHiCacheMemoryPageFirstDirect  SGLangHiCacheMemoryLayout = "page_first_direct"
	SGLangHiCacheMemoryPageFirstKVSplit SGLangHiCacheMemoryLayout = "page_first_kv_split"
	SGLangHiCacheMemoryPageHead         SGLangHiCacheMemoryLayout = "page_head"
)

// SGLangHiCacheSpec configures SGLang's native, engine-local host-memory cache.
// Exactly one of SizeGB and Ratio must be set. Optional tuning fields are
// passed to SGLang only when explicitly configured, so the engine version owns
// their defaults.
type SGLangHiCacheSpec struct {
	// SizeGB sets the host KV-cache pool size in decimal gigabytes.
	// Mutually exclusive with Ratio.
	// +optional
	// +kubebuilder:validation:Minimum=1
	SizeGB *int32 `json:"sizeGB,omitempty"`

	// Ratio sets the host KV-cache pool size relative to the device KV-cache
	// pool. It is a string because Kubernetes APIs avoid floating-point fields.
	// Admission requires a finite number greater than zero.
	// Mutually exclusive with SizeGB.
	// +optional
	Ratio string `json:"ratio,omitempty"`

	// WritePolicy maps to --hicache-write-policy.
	// +optional
	WritePolicy SGLangHiCacheWritePolicy `json:"writePolicy,omitempty"`

	// IOBackend maps to --hicache-io-backend.
	// +optional
	IOBackend SGLangHiCacheIOBackend `json:"ioBackend,omitempty"`

	// MemoryLayout maps to --hicache-mem-layout.
	// +optional
	MemoryLayout SGLangHiCacheMemoryLayout `json:"memoryLayout,omitempty"`
}

// LMCachePodLocalServerSpec configures the CacheBackend-owned LMCache MP
// server injected into each selected engine Pod.
type LMCachePodLocalServerSpec struct {
	// Image is the digest-pinned LMCache server image. CacheBackend owns this
	// cache component but never changes the inference-engine image.
	Image string `json:"image"`

	// Port is the loopback port used by the engine-side connector.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`

	// L1Capacity is the server's host-memory cache capacity. The renderer sizes
	// /dev/shm to this value plus 1Gi; container memory requests and limits must
	// each cover that complete budget.
	// +kubebuilder:validation:XValidation:rule="quantity(string(self)).isGreaterThan(quantity('0'))",message="l1Capacity must be greater than zero"
	L1Capacity resource.Quantity `json:"l1Capacity"`

	// MaxWorkers bounds the MP server worker pool for this engine Pod.
	// +kubebuilder:validation:Minimum=1
	MaxWorkers int32 `json:"maxWorkers"`

	// Resources are applied to the injected MP server container. Admission
	// requires a positive CPU request and requires both the memory request and
	// memory limit to cover l1Capacity plus 1Gi of /dev/shm headroom.
	Resources corev1.ResourceRequirements `json:"resources"`
}

// LMCachePodLocalSpec configures one MP server per selected engine Pod.
type LMCachePodLocalSpec struct {
	// Server is the CacheBackend-owned MP server configuration.
	Server *LMCachePodLocalServerSpec `json:"server"`
}

// LMCacheNodeLocalServerSpec configures the controller-owned LMCache MP server
// shared by selected engine Pods on one node chosen by the inference system.
type LMCacheNodeLocalServerSpec struct {
	// Image is the digest-pinned LMCache server image. The same image is used
	// by the lightweight engine startup gate; CacheBackend never changes the
	// inference-engine image.
	Image string `json:"image"`

	// Port is the node-bound LMCache MP data port.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`

	// HTTPPort is the node-bound FastAPI health/control port used by probes and
	// by the engine startup gate to verify the same-node server identity.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	HTTPPort int32 `json:"httpPort"`

	// L1Capacity is one shared host-memory budget per active engine node, not per
	// selected engine Pod.
	// +kubebuilder:validation:XValidation:rule="quantity(string(self)).isGreaterThan(quantity('0'))",message="l1Capacity must be greater than zero"
	L1Capacity resource.Quantity `json:"l1Capacity"`

	// MaxGPUWorkers bounds workers serving GPU-backed engine clients and must
	// cover the maximum number of engine instances expected on one node.
	// +kubebuilder:validation:Minimum=1
	MaxGPUWorkers int32 `json:"maxGPUWorkers"`

	// MaxCPUWorkers bounds host-side storage workers.
	// +kubebuilder:validation:Minimum=1
	MaxCPUWorkers int32 `json:"maxCPUWorkers"`

	// Resources are applied to every per-node server Pod. Admission requires
	// memory request and limit headroom above the shared L1 budget.
	Resources corev1.ResourceRequirements `json:"resources"`
}

// LMCacheNodeLocalSchedulingSpec configures server-Pod scheduling details that
// are independent of node placement. The controller derives the exact node
// from already-scheduled selected engine Pods; these fields never constrain or
// rewrite inference-engine placement.
type LMCacheNodeLocalSchedulingSpec struct {
	// Tolerations are merged with the tolerations of an engine already running
	// on the target node. They allow the server Pod to pass that node's taints
	// without selecting a different node.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`

	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// +optional
	SchedulerName string `json:"schedulerName,omitempty"`

	// RuntimeClassName overrides the runtime inherited from the engine Pod used
	// to place this server. Clusters that do not make the NVIDIA runtime the
	// default can use this field to provide GPU visibility without reserving
	// allocatable GPUs.
	// +optional
	RuntimeClassName *string `json:"runtimeClassName,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
}

// LMCacheNodeLocalSpec configures one shared MP server per node that currently
// hosts at least one selected engine Pod.
type LMCacheNodeLocalSpec struct {
	Server *LMCacheNodeLocalServerSpec `json:"server"`

	// IdleRetentionSeconds keeps an otherwise healthy per-node server alive
	// after the final selected engine leaves that node. A new matching engine
	// scheduled there during the window reuses the same server Pod and shared
	// L1. Zero requests immediate deletion.
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=86400
	IdleRetentionSeconds int32 `json:"idleRetentionSeconds"`

	// Scheduling optionally overrides server-Pod runtime and operational fields.
	// It cannot select nodes; inference-engine scheduling remains authoritative.
	// +optional
	Scheduling *LMCacheNodeLocalSchedulingSpec `json:"scheduling,omitempty"`
}

// LMCacheEngineSpec configures the engine-side LMCache implementation. These
// fields apply to the connector or node-local MP worker, not to a remote
// storage provider.
type LMCacheEngineSpec struct {
	// Topology selects the canonical LMCache MP server placement.
	Topology LMCacheTopology `json:"topology"`

	// PodLocal configures one MP server in each selected engine Pod.
	// +optional
	PodLocal *LMCachePodLocalSpec `json:"podLocal,omitempty"`

	// NodeLocal configures a controller-owned, engine-demand-driven per-node MP
	// server pool.
	// +optional
	NodeLocal *LMCacheNodeLocalSpec `json:"nodeLocal,omitempty"`

	// ChunkSizeTokens is the number of tokens in an LMCache chunk.
	// +optional
	// +kubebuilder:validation:Minimum=1
	ChunkSizeTokens *int32 `json:"chunkSizeTokens,omitempty"`
}

// RemoteStorageTLSSpec configures server-authenticated TLS for a remote L3.
// Secret references are namespace-local to the CacheBackend.
type RemoteStorageTLSSpec struct {
	// CACertificate selects a PEM CA bundle used to verify the provider.
	CACertificate corev1.SecretKeySelector `json:"caCertificate"`

	// ServerName overrides the DNS name verified in the provider certificate.
	// When omitted, clients verify the endpoint hostname.
	// +optional
	ServerName string `json:"serverName,omitempty"`
}

// RedisAuthenticationSpec configures Redis ACL/password authentication without
// placing credentials directly in the CacheBackend or engine arguments.
type RedisAuthenticationSpec struct {
	// Username selects the optional Redis ACL username.
	// +optional
	Username *corev1.SecretKeySelector `json:"username,omitempty"`

	// Password selects the required Redis password/token.
	Password corev1.SecretKeySelector `json:"password"`
}

// RedisRemoteStorageSpec configures a Redis remote-storage provider. Image and
// Resources are managed-workload settings; Authentication, TLS, and Database
// describe the engine/server binding for either ownership mode.
type RedisRemoteStorageSpec struct {
	// Image is used only when ownership is Managed.
	// +optional
	Image string `json:"image,omitempty"`

	// Resources are applied to the managed Redis container.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Authentication references namespace-local Redis credentials.
	// +optional
	Authentication *RedisAuthenticationSpec `json:"authentication,omitempty"`

	// TLS configures certificate verification for Redis over TLS.
	// +optional
	TLS *RemoteStorageTLSSpec `json:"tls,omitempty"`

	// Database selects the Redis logical database.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Database *int32 `json:"database,omitempty"`
}

// CacheBackendManagedWorkloadSpec configures Pod scheduling and security for a
// controller-managed remote-storage workload. Provider topology and scaling do
// not belong here: each provider must expose those semantics through its own
// typed configuration rather than treating replicas as interchangeable.
type CacheBackendManagedWorkloadSpec struct {
	// NodeSelector constrains provider Pods to nodes with matching labels.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity configures provider Pod scheduling affinity.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Tolerations allow provider Pods to schedule onto tainted nodes.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// TopologySpreadConstraints configures provider Pod spreading across
	// topology domains.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// ImagePullSecrets references Secrets used to pull provider images.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// ServiceAccountName is the ServiceAccount used by provider Pods.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// SecurityContext configures Pod-level security settings for provider Pods.
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`

	// PriorityClassName is the priority class assigned to provider Pods.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// SchedulerName selects the scheduler used for provider Pods.
	// +optional
	SchedulerName string `json:"schedulerName,omitempty"`

	// RuntimeClassName selects the runtime class used for provider Pods.
	// +optional
	RuntimeClassName *string `json:"runtimeClassName,omitempty"`

	// TerminationGracePeriodSeconds configures graceful provider shutdown.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
}

// CacheBackendRemoteStorageSpec configures the optional shared/remote tier.
// Omitting this object in the canonical API requests an engine-local,
// host-only hierarchy and never implicitly provisions infrastructure.
type CacheBackendRemoteStorageSpec struct {
	// Provider identifies the remote-storage technology.
	Provider CacheBackendRemoteStorageProvider `json:"provider"`

	// Ownership identifies whether inference-cache manages the provider
	// workload or connects to operator-managed infrastructure.
	Ownership CacheBackendRemoteStorageOwnership `json:"ownership"`

	// Endpoint is required for External ownership and rejected for Managed
	// ownership, whose endpoint is controller-observed.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Workload configures Pod scheduling and security for controller-managed
	// provider workloads. It is rejected when ownership is External.
	// +optional
	Workload *CacheBackendManagedWorkloadSpec `json:"workload,omitempty"`

	// Redis contains Redis-owned configuration.
	// +optional
	Redis *RedisRemoteStorageSpec `json:"redis,omitempty"`
}

// CacheBackendObservationSpec configures KV-event observation independently
// from engine-cache wiring and remote-storage lifecycle.
type CacheBackendObservationSpec struct {
	// ModelID is attached to observed cache events.
	// +optional
	ModelID string `json:"modelID,omitempty"`

	// FirstEventTimeout bounds how long readiness waits for the first KV event.
	// +optional
	// +kubebuilder:default="5m"
	FirstEventTimeout *metav1.Duration `json:"firstEventTimeout,omitempty"`
}

// CacheBackendSpec defines the desired state of a cache backend.
type CacheBackendSpec struct {
	// Runtime identifies the inference runtime. Values are case-sensitive: use
	// VLLM or SGLang.
	Runtime CacheBackendRuntime `json:"runtime"`

	// Type identifies the engine-side cache implementation and defaults to
	// LMCache. Supported values are LMCache and SGLangHiCache. Provider
	// technology and ownership are selected independently through remoteStorage;
	// omitting remoteStorage requests a host-only hierarchy.
	// +optional
	// +kubebuilder:default=LMCache
	Type CacheBackendType `json:"type,omitempty"`

	// LMCache configures the engine-side LMCache implementation. It is valid
	// only when type=LMCache.
	// +optional
	LMCache *LMCacheEngineSpec `json:"lmCache,omitempty"`

	// RemoteStorage configures an optional shared/remote cache tier. Its
	// provider and ownership are independent from runtime and type. When this
	// field is omitted from the canonical API, no provider is provisioned.
	// +optional
	RemoteStorage *CacheBackendRemoteStorageSpec `json:"remoteStorage,omitempty"`

	// Observation configures KV-event observation independently from cache
	// offload and provider lifecycle.
	// +optional
	Observation *CacheBackendObservationSpec `json:"observation,omitempty"`

	// Integration describes how inference engines should use the cache backend.
	// +optional
	Integration *CacheBackendIntegrationSpec `json:"integration,omitempty"`

	// EngineSelector selects which engine pods this CacheBackend claims. A
	// non-empty selector must contain exactly one MatchLabels entry whose key is
	// inferencecache.io/cache-domain. The value is an operator-chosen,
	// namespace-scoped compatibility-domain ID; the selected Pods must carry the
	// same label. Pods may carry other labels, but they do not participate in
	// CacheBackend ownership. The full metav1.LabelSelector surface
	// (matchExpressions, operator-based selection) is NOT exposed today.
	// Admission requires the domain value to be unique among CacheBackends in
	// the same namespace. Every engine Pod must have exactly one CacheBackend
	// owner.
	// Pods that match get runtime-adapter engine wiring injected by the
	// mutating Pod admission webhook at pod CREATE time. LMCache MP adapters
	// inject the local connector immediately; when an optional managed Redis
	// tier is configured, its address is read from status.remoteStorage.endpoint.
	// Engine-local adapters such as native SGLang HiCache require no endpoint.
	// Admission is CREATE-only; recovery or a configuration update requires
	// recreating the pod (e.g. `kubectl rollout restart`), not editing its
	// live labels.
	//
	// This describes the default spec.integration.mode=Offload path. For
	// spec.integration.mode=EventsOnly no KV connector wiring (env vars +
	// CLI args) is injected and no connector or remote-storage endpoint is
	// published — the kvevent-subscriber observation sidecar alone is
	// injected (see below), so matched pods report cache state for routing
	// without offloading KV to a backend server.
	//
	// The kvevent-subscriber observation sidecar is appended in addition
	// to the engine wiring only when the controller is started with
	// --kvevent-subscriber-image set (empty by default) AND the matched
	// CacheBackend has a model id configured. Without those, the engine
	// is wired but no sidecar is added.
	//
	// In spec.integration.mode=EventsOnly the above "engine wiring" is
	// absent: no KV connector is injected, so the sidecar is the ONLY thing
	// the webhook adds. With the subscriber image or model id unset nothing
	// is wired at all and the pod carries no injected-by stamp (the webhook
	// admits it untouched).
	//
	// The match is evaluated once at pod CREATE — pods whose labels change
	// after creation are not re-evaluated; the wiring is sticky to the
	// life of the pod. To opt a specific pod out of injection regardless
	// of label match, set the annotation
	// `inferencecache.io/skip-inject: "true"` on the pod template.
	//
	// See docs/concepts/cachebackend-engine-binding.md for the full
	// lifecycle, an annotated example, and common failure modes.
	// +optional
	EngineSelector *CacheBackendEngineSelector `json:"engineSelector,omitempty"`

	// HiCache configures SGLang's native, engine-local hierarchical cache. It
	// is required for type=SGLangHiCache and rejected for other backend types.
	// The selected engine Pods own the host-memory allocation; no cache-server
	// workload or network endpoint is created.
	// +optional
	HiCache *SGLangHiCacheSpec `json:"hiCache,omitempty"`

	// AllowCrossNamespace opts the CacheBackend into referencing an Endpoint
	// that resolves into a Kubernetes Service in a different namespace from
	// this object. Without this opt-in admission rejects such Endpoints,
	// because a cross-namespace reference crosses a tenancy/RBAC boundary that
	// the cluster operator should explicitly acknowledge. Endpoints that are
	// not in-cluster Service DNS (external hostnames, IPs) are unaffected.
	// +optional
	AllowCrossNamespace bool `json:"allowCrossNamespace,omitempty"`
}

// CacheBackendIntegrationSpec describes engine integration behavior.
//
// Per-namespace lookup tuning lives on CachePolicy, not here: the lookup
// deadline and the minimum-prefix-token gate are configured via
// CachePolicy.spec.lookupTimeoutMs and CachePolicy.spec.minimumPrefixTokens,
// which are the surfaces actually wired into the server's ResolvedPolicy.
type CacheBackendIntegrationSpec struct {
	// Mode selects which cache tiers the engine is wired for. Defaults to
	// Offload — cache-aware routing (tier-1) PLUS the KV-offload connector
	// (tier-2), with a controller-provisioned backend server. EventsOnly wires
	// routing only: the kvevent-subscriber sidecar is injected, but no KV
	// connector is loaded into the engine and no backend server is provisioned.
	// Mode takes precedence over engine-cache configuration: when EventsOnly is
	// selected, spec.lmCache and other host-tier settings are not injected into
	// the engine. Operators should omit them to avoid implying an active tier.
	// EventsOnly is the supported integration for hybrid-attention models that
	// cannot take a vLLM KV connector (and a lighter routing-only deployment for
	// anyone who does not want an offload tier). Because EventsOnly provisions
	// no server, connector and remote-storage status stay empty; Ready is still
	// gated on the first observed KV event.
	// SGLangHiCache supports Offload only and is rejected with EventsOnly.
	// See the CacheBackendIntegrationMode godoc.
	// +optional
	// +kubebuilder:default=Offload
	Mode CacheBackendIntegrationMode `json:"mode,omitempty"`

	// Role controls whether the engine reads from, writes to, or fully
	// participates in the cache. Defaults to ReadWrite — full participation.
	// ReadOnly / WriteOnly are specialised producer/consumer roles operators
	// opt into explicitly.
	//
	// Support is backend-specific. LMCache currently supports only ReadWrite:
	// SGLang has no directional role split, and the validated LMCache 0.5.3
	// vLLM MP connector does not enforce kv_consumer / kv_producer. Admission
	// rejects ReadOnly / WriteOnly for every LMCache backend rather than expose
	// directionality the data plane does not honor.
	// +optional
	// +kubebuilder:default=ReadWrite
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
	// The flag is plumbed by the engine adapter as INFERENCECACHE_FAIL_OPEN
	// (both shipping LMCache adapters — vLLM+LMCache and SGLang+LMCache —
	// inject it). Per-request fail-open enforcement at the engine level is the
	// engine/connector's responsibility; the cache plane surfaces the bit so
	// the engine can honor it.
	// SGLangHiCache accepts only the default true value and does not inject this
	// env var because native HiCache exposes no equivalent fail-closed control.
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

// EngineInjectionOverrides is the in-between knob between "take the
// adapter's canonical injection" and "skip injection entirely" (the latter
// owned by the inferencecache.io/skip-inject pod annotation). The four
// primitives compose: Env upserts by Name and SuppressEnv removes by Name;
// Args replaces by leading flag token or appends, and SuppressArgs removes
// by leading flag token. Suppress runs before merge, so suppress-then-re-add
// is a supported pattern for overriding a non-reserved adapter-owned flag
// value. For adapter-backed Spec.Type values (LMCache and the future
// adapter-backed types), entries that overlap the runtime adapter's
// ReservedArgs() or ReservedEnv() are hard-rejected at admission with a
// field-scoped error naming the offending token and the adapter, so a
// misconfiguration fails at kubectl apply rather than as a crashed engine
// pod later. Spec.Type=External does not consult an adapter (no canonical
// injection happens), so the override surface there is structurally
// meaningless and the reserved-overlap check is skipped.
//
// See docs/concepts/cachebackend-engine-overrides.md for the baseline
// canonical injection (annotated RESERVED / TUNABLE), five worked
// before/after examples, and the "when NOT to use this" guidance.
//
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

// IntegrationMode returns the effective integration mode for a CacheBackend
// integration spec. Missing spec or empty field defaults to Offload, matching
// the API default — full routing plus the selected offload tier. The
// admission defaulter materialises the field on submitted objects; this helper
// is the read-time fallback for callers that bypass the apiserver (raw-struct
// test invocation, partial deserialization).
func IntegrationMode(spec *CacheBackendIntegrationSpec) CacheBackendIntegrationMode {
	if spec == nil || spec.Mode == "" {
		return CacheBackendIntegrationModeOffload
	}
	return spec.Mode
}

// IsEventsOnly reports whether the backend is wired for events-only (tier-1
// routing) integration — no KV connector, no provisioned server. It is the
// single predicate the adapter, webhook, controller, and validator share so the
// mode's three-layer wiring (inject / reconcile / admit) stays in lockstep.
func (s *CacheBackendSpec) IsEventsOnly() bool {
	return IntegrationMode(s.Integration) == CacheBackendIntegrationModeEventsOnly
}

// CacheBackendEngineSelector selects engines by labels.
type CacheBackendEngineSelector struct {
	// MatchLabels is a map of labels that selected engines must match.
	// +optional
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

// CacheBackendConnectorStatus reports the engine-to-cache connector separately
// from the optional remote L3 provider. PodLocal loopback and NodeLocal
// node-derived addresses deliberately do not appear as a generic endpoint.
type CacheBackendConnectorStatus struct {
	// Mode is Multiprocess for the canonical LMCache data plane.
	Mode LMCacheConnectorMode `json:"mode,omitempty"`

	// Topology is the effective MP server placement.
	Topology LMCacheTopology `json:"topology,omitempty"`

	// MatchedEnginePods is the number of selected engine Pods observed.
	// +kubebuilder:validation:Minimum=0
	MatchedEnginePods int32 `json:"matchedEnginePods,omitempty"`

	// ReadyEnginePods is the number of selected engine Pods whose connector is
	// ready.
	// +kubebuilder:validation:Minimum=0
	ReadyEnginePods int32 `json:"readyEnginePods,omitempty"`

	// DesiredServers is one per selected engine Pod for PodLocal and one per
	// distinct active scheduled engine node for NodeLocal.
	// +kubebuilder:validation:Minimum=0
	DesiredServers int32 `json:"desiredServers,omitempty"`

	// ReadyServers is the number of healthy MP servers.
	// +kubebuilder:validation:Minimum=0
	ReadyServers int32 `json:"readyServers,omitempty"`

	// CoveredEnginePods is the number of selected engine Pods with a healthy,
	// reachable MP server.
	// +kubebuilder:validation:Minimum=0
	CoveredEnginePods int32 `json:"coveredEnginePods,omitempty"`

	// UncoveredEnginePods is the selected engine Pods without a healthy,
	// reachable MP server.
	// +kubebuilder:validation:Minimum=0
	UncoveredEnginePods int32 `json:"uncoveredEnginePods,omitempty"`

	// EnginePodCoverage reports the connector verdict for every active selected
	// engine Pod. The list is keyed by Pod name and sorted by name by the
	// controller so operators can identify the exact uncovered instance.
	// +optional
	// +listType=map
	// +listMapKey=name
	EnginePodCoverage []CacheBackendEnginePodCoverageStatus `json:"enginePodCoverage,omitempty"`
}

// CacheBackendEnginePodCoverageStatus is the per-engine connector verdict.
// Ready means the engine Pod itself is Ready; Covered means the current
// CacheBackend generation has exactly one healthy reachable MP server for it.
type CacheBackendEnginePodCoverageStatus struct {
	// Name is the selected engine Pod name in the CacheBackend namespace.
	Name string `json:"name"`

	// NodeName is the scheduled node. It is empty while the Pod is pending.
	// +optional
	NodeName string `json:"nodeName,omitempty"`

	Ready bool `json:"ready"`

	Covered bool `json:"covered"`

	// Reason is a stable machine-readable explanation of the coverage verdict.
	Reason string `json:"reason"`
}

// CacheBackendRemoteStorageStatus reports the optional shared L3 independently
// from connector health. Endpoint is meaningful here because an L3 provider is
// globally addressable; MP connector endpoints are Pod/node local and omitted.
type CacheBackendRemoteStorageStatus struct {
	Provider CacheBackendRemoteStorageProvider `json:"provider,omitempty"`

	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Ready is True, False, or Unknown once the controller has evaluated the
	// provider.
	// +optional
	Ready metav1.ConditionStatus `json:"ready,omitempty"`
}

// CacheBackendStatus defines the observed state of a cache backend.
type CacheBackendStatus struct {
	// Connector reports the engine-side connector and MP server topology.
	// +optional
	Connector *CacheBackendConnectorStatus `json:"connector,omitempty"`

	// RemoteStorage reports the optional remote L3 independently from the
	// connector.
	// +optional
	RemoteStorage *CacheBackendRemoteStorageStatus `json:"remoteStorage,omitempty"`

	// MatchedEnginePods is the number of pods in this CacheBackend's namespace
	// whose labels match spec.engineSelector at the last reconcile. The field
	// is a pointer so nil ("not yet computed") is distinguishable from 0
	// ("computed and zero current matches"). 0 covers any current
	// zero-match state — the engine Deployment has not been created
	// yet, it has been scaled to zero, or the selector and the engine
	// Deployment's pod labels have drifted apart. (Pods carrying a
	// `deletionTimestamp` are NOT filtered out today; the count is a
	// raw List of matching pods.) When engine pods are expected and 0
	// persists, label drift
	// is the most likely diagnosis: the mutating Pod webhook silently
	// no-ops on pods whose labels miss the selector, so the engine
	// runs uncached.
	//
	// This is a snapshot at reconcile time, not a real-time counter: it
	// is not updated on every pod birth/death. For per-pod real-time
	// visibility, watch the K8s `InjectedByCacheBackend` Event for
	// injected pods and `SkippedByOperator` / `inferencecache.io/inject-skipped`
	// for pods that explicitly opted out (visible in `kubectl describe pod`).
	// +optional
	// +kubebuilder:validation:Minimum=0
	MatchedEnginePods *int32 `json:"matchedEnginePods,omitempty"`

	// EngineSelectorMessage explains the current engineSelector matching
	// observation when it needs operator attention. It is set when
	// spec.engineSelector.matchLabels is configured but matchedEnginePods is
	// observed as 0 while engine pods are expected, and cleared once at least
	// one pod matches, the matching Deployment is intentionally scaled to zero,
	// or the selector is removed. The message echoes the selector so an operator
	// can compare it directly with engine Deployment pod-template labels.
	// +optional
	EngineSelectorMessage string `json:"engineSelectorMessage,omitempty"`

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

	// FirstKVEventObservedAt latches the first time the KV-event readiness
	// gate observed status.indexParticipation.lastEventAt populated for this
	// backend. It is the durable "have we EVER seen a KV event" signal the
	// gate needs: lastEventAt itself is a current-view projection the
	// CacheIndex poller legitimately clears when a backend's replicas drain
	// (scale-down, prefixes TTL'd), so reading it alone would let a backend
	// that already passed the gate regress to AwaitingFirstKVEvent. Written
	// write-once by the controller and never cleared (a monotonic marker; the
	// gate is a first-event startup probe, not an ongoing liveness check). It
	// is inert while the backend is not managed (External / unsupported
	// runtime), and remains set so a return to the managed path stays Ready
	// without re-gating — consistent with the "ever observed" contract.
	// +optional
	FirstKVEventObservedAt *metav1.Time `json:"firstKVEventObservedAt,omitempty"`

	// FirstAvailableAt is the stable anchor for the firstEventTimeout clock.
	// It latches one of two events depending on the integration mode:
	//   - Offload: the first time the effective engine-side connector and any
	//     fail-closed remote-storage dependency were observed Ready.
	//   - EventsOnly: the first reconcile. A server-less backend has no
	//     workload to become Available, so it is "up" the moment it exists
	//     and the firstEventTimeout clock starts immediately.
	// In both cases it is a latched timestamp rather than a live condition's
	// LastTransitionTime: a live condition resets on an availability flap,
	// which would restart the timeout window and let a backend that already
	// breached the timeout (Degraded / NoKVEventsObserved) bounce back to
	// AwaitingFirstKVEvent without any KV event — contradicting the "once
	// Degraded, stays Degraded until an event arrives" contract. Anchoring on
	// this latched value keeps the elapsed window monotonic WITHIN a serving
	// mode, so Degraded is sticky. It survives availability flaps and a
	// recreated managed Redis Deployment (the gate re-evaluates from the prior
	// anchor, safe because a Redis restart does not change the engine event
	// source). It is NOT immortal across a mode change, though: an
	// Offload→EventsOnly flip re-anchors it to the flip moment (and also
	// bypasses the sticky NoKVEventsObserved reason) so the flip gets a fresh
	// first-event window instead of inheriting the old mode's availability time
	// or timed-out verdict; and an unmanaged transition clears it so a later
	// managed/events-only re-entry starts fresh. (Inert only on an Offload
	// backend that has not yet reported Available; on the EventsOnly path it is
	// set on the first reconcile.)
	// +optional
	FirstAvailableAt *metav1.Time `json:"firstAvailableAt,omitempty"`

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
// signal stamped by the pod webhook) or, for manually attached subscriber Pods
// that bypassed the webhook, via a deterministic metadata.name-ordered selector
// fallback. Admission rejects ambiguous ownership; the poller
// writes write-only-on-change and never clears it on a single failed scrape
// (soft state).
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

	// T2HitRate is the query-weighted reload hit-rate of the tier-2 (external
	// offload, e.g. LMCache) cache across this backend's replicas, formatted as
	// a decimal string in [0,1]. Sourced from the engines'
	// vllm:external_prefix_cache_{hits,queries}_total counters and projected by
	// the CacheIndex poller.
	//
	// Presence is load-bearing here: nil means the tier-2 cache has NOT been
	// exercised yet (no external lookups across any replica) — distinct from
	// "0". A value of "0" means the tier WAS queried but served zero reloads:
	// tier-2 is wired but not actually helping. That is the operator-visible
	// signature of a silently-degraded offload tier — a store/connection
	// failure, an under-sized remote server, or a scheduler/worker hash
	// mismatch all surface here as "0" rather than as nothing at all. A
	// healthy reusing workload reads well above 0.
	// +optional
	T2HitRate *string `json:"t2HitRate,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=cb
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Matched",type=integer,JSONPath=`.status.matchedEnginePods`
// +kubebuilder:printcolumn:name="Remote",type=string,JSONPath=`.status.remoteStorage.endpoint`
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
