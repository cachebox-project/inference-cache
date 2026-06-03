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

// CacheBackendSpec defines the desired state of a cache backend.
//
// Persistent storage (spec.storage.pvc) and the autoscaling spec
// (spec.autoscaling) are both reconciled today. The autoscaling spec drives a
// HorizontalPodAutoscaler; spec.storage.pvc — on a managed Deployment backend
// (deploymentKind=Deployment, the default) — provisions a PersistentVolumeClaim
// owner-referenced to the CacheBackend (reclaimed only when the CacheBackend is
// deleted), mounts it into the cache-server pod at the runtime adapter's
// declared data path, and reports the bound PVC's actual size in
// status.capacity. (A StatefulSet backend is routed to the unmanaged path today
// — see DeploymentKind — so it provisions nothing, storage included; per-replica
// volumeClaimTemplates are a follow-up.) spec.storage.pvc requires a single replica — a ReadWriteOnce
// PVC cannot be multi-attached, so a persistent backend with replicas (or an
// autoscaling ceiling) > 1 is surfaced Ready=False/InvalidStorageConfiguration
// rather than provisioned; per-replica persistent storage via StatefulSet is a
// separate follow-up. NOTE: provisioning + mounting the PVC does not by itself
// make the cache server spill KV to it — switching the LMCache server to a
// disk-backed storage device is a separate follow-up; until then the volume is
// attached but the server keeps KV in memory.
type CacheBackendSpec struct {
	// Type identifies the backing cache implementation. Defaults to LMCache —
	// the standalone lmcache-server workload Phase-1 ships; operators pick
	// External (operator-supplied endpoint) or another type explicitly when
	// they need to. The CRD does not constrain Type to an enum today; the
	// admission validator's runtime-adapter check is the authoritative reject
	// for unsupported (engine, type) pairs.
	// +optional
	// +kubebuilder:default=LMCache
	Type CacheBackendType `json:"type,omitempty"`

	// DeploymentKind identifies whether a managed backend is reconciled as a
	// Deployment or StatefulSet. Defaults to Deployment — the only kind the
	// Phase-1 reconciler templates; StatefulSet is reserved for future
	// per-replica-PVC topologies and is a no-op today.
	// +optional
	// +kubebuilder:default=Deployment
	DeploymentKind CacheBackendDeploymentKind `json:"deploymentKind,omitempty"`

	// Replicas is the desired number of backend workload replicas. Defaults
	// to 1 — a conservative single-replica deployment; operators opt into
	// horizontal scale via spec.autoscaling.
	//
	// When spec.autoscaling is set, the HPA owns the live replica count and
	// the autoscaling floor (spec.autoscaling.minReplicas) is auto-defaulted
	// to spec.replicas on FIRST APPLY ONLY by the admission defaulter.
	// Subsequent edits to spec.replicas do NOT move the autoscaling floor —
	// minReplicas is operator-owned (and operator-pinned via the apiserver
	// field manager) after first apply, matching the standard Kubernetes HPA
	// convention that scaling intent flows through HPA fields once an HPA
	// owns the workload. To widen or narrow the autoscaling band post-apply,
	// edit spec.autoscaling.minReplicas directly.
	// +optional
	// +kubebuilder:default=1
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

	// EngineSelector selects which engine pods this CacheBackend claims via
	// equality-based label matching over the pod's labels: every key/value
	// in MatchLabels must be present on the pod. The full
	// metav1.LabelSelector surface (matchExpressions, operator-based
	// selection) is NOT exposed today — only MatchLabels.
	// Pods that match get LMCache engine wiring (env vars + CLI args)
	// injected by the mutating Pod admission webhook at pod CREATE time,
	// PROVIDED the matched CacheBackend's status.endpoint has been
	// published by the time admission runs. The webhook fail-opens
	// (admits the pod unmodified) when status.endpoint is empty — so a
	// pod that loses the race against the reconciler is admitted
	// uncached for its whole lifetime. Admission is CREATE-only;
	// recovery is to recreate the pod (e.g. `kubectl rollout restart`),
	// not to edit the live pod's labels.
	//
	// The kvevent-subscriber observation sidecar is appended in addition
	// to the engine wiring only when the controller is started with
	// --kvevent-subscriber-image set (empty by default) AND the matched
	// CacheBackend has a model id configured. Without those, the engine
	// is wired but no sidecar is added.
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

	// BackendConfig contains backend-specific string settings.
	// +optional
	BackendConfig map[string]string `json:"backendConfig,omitempty"`

	// Template provides pod-level overrides for managed backend workloads.
	// +optional
	Template *CacheBackendPodSpecOverride `json:"template,omitempty"`

	// Endpoint is the operator-supplied network address for an
	// External backend the controller does NOT provision. The field
	// is type-scoped: it is REQUIRED when spec.type is External and
	// REJECTED at admission for every other type (managed backends
	// learn their endpoint from the controller-rendered Service and
	// would silently overwrite a user-supplied value, so admission
	// surfaces the misconfiguration loudly at write time).
	//
	// Allowed shapes for External (both forms require a non-empty
	// port — the LMCache connector dials TCP, so admission rejects
	// portless hosts):
	//   - bare host:port (canonical; the LMCache engine adapter
	//     prepends the lm:// scheme on injection)
	//   - lm://host:port (operators who prefer to be explicit)
	// IPv6 literals must be bracketed: [::1]:8200. Other schemes
	// (https://, http://, ...) and path/query/fragment components
	// are rejected at admission — they would produce an invalid
	// LMCACHE_REMOTE_URL when concatenated with the lm:// prefix at
	// injection time.
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
	// Size is the requested persistent storage size. It can be increased later
	// (the controller patches the PVC request up, and the StorageClass expands
	// the volume if it allows expansion); it cannot be decreased — Kubernetes
	// does not support shrinking a PVC, so a smaller value is ignored.
	// +kubebuilder:validation:Required
	Size resource.Quantity `json:"size"`

	// StorageClassName is the optional StorageClass for the PVC. Omit (leave
	// null) to use the cluster default StorageClass; set it to the empty string
	// ("") to explicitly opt out of the default (static / no-provisioner
	// binding) — the controller preserves the distinction. It is immutable after
	// the PVC is created — Kubernetes rejects StorageClass changes — so a later
	// edit is ignored (the controller logs and keeps the existing class).
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
	// MinReplicas is the lower bound for the HPA replica count. The
	// admission defaulter computes the default at write time from
	// spec.replicas (which itself defaults to 1) so the HPA's floor matches
	// the operator-declared baseline rather than a hard-coded constant. This
	// is a FIRST-APPLY-ONLY default: the defaulter never overwrites an
	// operator-set value, AND once stamped the field is owned by the
	// apiserver field manager — subsequent edits to spec.replicas do NOT
	// recompute or move minReplicas, matching the standard Kubernetes HPA
	// convention that scaling intent flows through HPA fields once an HPA
	// owns the workload. To widen or narrow the autoscaling band post-apply,
	// edit spec.autoscaling.minReplicas directly. Operators who want a
	// non-default floor on first apply set the field explicitly.
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
	// Engine identifies the inference engine integration, such as vllm or
	// sglang. Defaults to vllm — the only runtime ID with a shipping adapter
	// today (sglang is wired but no adapter ships in v1alpha1). The CRD-level
	// default only applies when spec.integration is materialised on the
	// submitted object; when integration is omitted entirely the
	// [adapterruntime.ResolveRuntimeID] helper applies the same vllm fallback
	// at read time, so the admission validator, reconciler, and pod webhook
	// all converge on the same effective engine.
	// +optional
	// +kubebuilder:default=vllm
	Engine string `json:"engine,omitempty"`

	// Role controls whether the engine reads from, writes to, or fully
	// participates in the cache. Defaults to ReadWrite — full participation,
	// matching the [enginewire.IntegrationRole] read-time fallback for an
	// omitted integration block. ReadOnly / WriteOnly are specialised
	// producer/consumer roles operators opt into explicitly.
	// +optional
	// +kubebuilder:default=ReadWrite
	Role CacheBackendIntegrationRole `json:"role,omitempty"`

	// FirstEventTimeout bounds how long a managed backend may sit
	// Ready=False with reason AwaitingFirstKVEvent — the managed cache-backend
	// workload is Available but no KV event has been observed yet — before
	// the controller flips it to Ready=False/Degraded=True with reason
	// NoKVEventsObserved.
	//
	// The KV-event readiness gate holds Ready until at least one KV event
	// has been observed for this backend's replicas
	// (status.indexParticipation.lastEventAt, projected from engine-pod
	// reports). That proves the engine's ZMQ KV-event publisher is actually
	// publishing — not merely that the managed workload rolled out. An engine
	// can be serving HTTP while its publisher is silent (mis-configured
	// --kv-events-config, ZMQ bind failure, in-process publisher crash), or no
	// engine pods may be attached to the backend at all; either way the cache
	// plane silently degrades to NO_HINT on every lookup, and this gate makes
	// that loud.
	//
	// The timeout clock starts when the managed cache-backend workload first
	// reports Available. The gate is on by default and opt-out per CacheBackend
	// via the annotation inferencecache.io/require-kv-events: "false". Backends
	// of spec.type External are always exempt (their readiness is determined by
	// admission accepting the endpoint, and they never enter this gate).
	//
	// A zero or negative value is treated as unset and falls back to the 5m
	// default — the field carries no meaningful "wait forever" or "fail
	// immediately" semantics.
	//
	// The value is a Go duration string (e.g. "90s", "5m", "1h"). The CRD
	// schema types it as a string; a malformed value is rejected when
	// admission decodes the object into this typed field, and if admission is
	// bypassed the controller's typed read fails loudly (it never silently
	// mis-parses). This matches how the API treats every metav1.Duration
	// field; no extra CRD-level format constraint is imposed.
	// +optional
	// +kubebuilder:default="5m"
	FirstEventTimeout *metav1.Duration `json:"firstEventTimeout,omitempty"`

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

	// Capacity is a human-readable summary of the backend's provisioned
	// capacity: the bound PersistentVolumeClaim's actual capacity when
	// spec.storage.pvc is set and the PVC has bound (the real provisioned size,
	// which may exceed the request), or empty for an ephemeral backend or while
	// the PVC is still pending (e.g. a WaitForFirstConsumer StorageClass that
	// binds only once the pod schedules). It is informational; clients must not
	// parse it.
	// +optional
	Capacity string `json:"capacity,omitempty"`

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
	// visibility, watch the K8s `InjectedByCacheBackend` Event the
	// controller emits on the engine pod after the mutating Pod webhook
	// stamps it (visible in `kubectl describe pod`).
	// +optional
	// +kubebuilder:validation:Minimum=0
	MatchedEnginePods *int32 `json:"matchedEnginePods,omitempty"`

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

	// FirstAvailableAt latches the first time the managed cache-backend
	// workload was observed Available — the stable anchor for the
	// firstEventTimeout clock. It is deliberately a latched timestamp rather
	// than the live Deployment's Available condition LastTransitionTime: that
	// condition resets on an availability flap, which would restart the
	// timeout window and let a backend that already breached the timeout
	// (Degraded / NoKVEventsObserved) bounce back to AwaitingFirstKVEvent
	// without any KV event — contradicting the "once Degraded, stays Degraded
	// until an event arrives" contract. Anchoring on this write-once value
	// keeps the elapsed window monotonic, so Degraded is sticky. Written
	// write-once when the workload first reports Available and never cleared
	// (inert while the backend is not managed). A genuinely recreated managed
	// Deployment keeps the prior anchor; the gate re-evaluates from it, which
	// is safe because the engine event source is unchanged by a cache-server
	// restart.
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
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Matched",type=integer,JSONPath=`.status.matchedEnginePods`
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
