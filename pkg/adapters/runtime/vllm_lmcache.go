package runtime

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

// vllm+LMCache canonical defaults. These template a standalone LMCache server
// pod that vLLM engines connect to via LMCACHE_REMOTE_URL=lm://<svc>:<port>,
// matching the upstream "share KV across instances" topology
// (https://docs.lmcache.ai/getting_started/quickstart/share_kv_cache.html and
// the LMCache Dockerfile.standalone in
// https://github.com/LMCache/LMCache/tree/dev/docker). Defaults are overridable
// via CacheBackend.Spec.BackendConfig so a real deployment can pin to digests
// without a code change.
const (
	// defaultLMCacheServerImage is the upstream standalone LMCache server
	// image; :latest is the overridable default (production should pin to a
	// digest via BackendConfig).
	defaultLMCacheServerImage = "lmcache/standalone:latest"
	// defaultLMCacheServerPort is the canonical lm:// port the LMCache
	// docs use for the standalone server.
	defaultLMCacheServerPort = int32(65432)
	// defaultLMCacheServerHost is the bind address inside the pod.
	defaultLMCacheServerHost = "0.0.0.0"
	// defaultLMCacheServerStorage is the LMCache server storage device; "cpu"
	// (the default, in-memory) is the only widely-supported option today.
	defaultLMCacheServerStorage = "cpu"
	// defaultLMCacheServerPortName is the named container port other parts of
	// the system can address by name without hard-coding the integer.
	defaultLMCacheServerPortName = "lmcache"

	// defaultLMCacheDataVolumeName / defaultLMCacheDataMountPath name the
	// persistent data volume the adapter DECLARES (via
	// [ResolvedCacheServer.DataVolume]) when the CacheBackend sets
	// spec.storage.pvc. The reconciler provisions a PVC owner-referenced to the
	// CacheBackend and mounts it at this path on the lmcache-server container.
	//
	// NOTE: declaring the data volume provisions + attaches the PVC, but it does
	// NOT by itself make KV survive a pod restart. The lmcache-server still runs
	// with the in-memory storage device (defaultLMCacheServerStorage = "cpu"),
	// so the mounted volume is not yet written to. Switching the server to a
	// disk-backed device that spills to this exact directory is a deliberately
	// separate follow-up (the supported on-server disk mechanism depends on the
	// pinned LMCache server version and is not a simple positional-arg flip — see
	// the storage section of docs/design/cachebackend-api.md). Splitting the
	// version-agnostic provisioning half (here) from the server-side device
	// switch lets PVC provisioning, owner-ref GC, the multi-replica gate, and
	// status.capacity land independently.
	defaultLMCacheDataVolumeName = "cache-data"
	defaultLMCacheDataMountPath  = "/var/lib/lmcache"

	// BackendConfig override keys. Keep them short, kebab-free, JSON-friendly
	// since they round-trip through CacheBackend.Spec.BackendConfig (a
	// map[string]string).
	// cfgKeyServerImage is the BackendConfig key that overrides the
	// lmcache-server container image. The name is deliberately distinct from
	// the legacy "image" key (which addressed the all-in-one vLLM container
	// the previous reconciler rendered) so an existing CR carrying
	// `backendConfig.image: vllm/vllm-openai:...` does not silently render an
	// lmcache-server pod with the wrong image — the legacy key is now
	// ignored and the lmcache-server falls back to its default image.
	cfgKeyServerImage   = "serverImage"
	cfgKeyServerCommand = "serverCommand"
)

// Engine env var names. Re-exported from the internal enginewire package so
// downstream callers (admission validators, integration tests, future
// adapter authors) can assert on the wire contract without importing an
// internal/ path. The constants live in enginewire so adapters that speak
// the same wire (vLLM+LMCache and the External passthrough today) share a
// single source of truth.
const (
	EnvLMCacheRemoteURL       = enginewire.EnvLMCacheRemoteURL
	EnvLMCacheRemoteSerde     = enginewire.EnvLMCacheRemoteSerde
	EnvLMCacheChunkSize       = enginewire.EnvLMCacheChunkSize
	EnvLMCacheLocalCPU        = enginewire.EnvLMCacheLocalCPU
	EnvLMCacheMaxLocalCPU     = enginewire.EnvLMCacheMaxLocalCPU
	EnvVLLMUseV1              = enginewire.EnvVLLMUseV1
	EnvInferenceCacheFailOpen = enginewire.EnvInferenceCacheFailOpen
	// EngineContainerName is the conventional name for the vLLM container in
	// an engine pod the adapter mutates. When no container with this name is
	// present, a single-container pod is treated as the engine; a multi-
	// container pod is rejected.
	EngineContainerName = enginewire.EngineContainerName
	// SubscriberContainerName is the well-known name for the kvevent-
	// subscriber sidecar the adapter renders. Webhook callers use it to
	// short-circuit re-admission (skip the append if a container by this
	// name is already present), and operators can `kubectl logs <pod> -c
	// kvevent-subscriber` without guessing.
	SubscriberContainerName = "kvevent-subscriber"
)

// Defaults the kvevent-subscriber sidecar carries when the operator does
// not pin them via controller flags. Vendor-neutral; production should set
// SubscriberImage to a digest-pinned image and PolicyServerGRPCAddress to
// the in-cluster Service DNS the operator's policy server actually exposes.
const (
	// DefaultSubscriberImage is the well-known dev-tag the Makefile's
	// subscriber-image target emits; operators pass it (or a
	// production-pinned digest) to the controller's
	// --kvevent-subscriber-image flag to enable auto-attach.
	//
	// Auto-attach is opt-in by design: when no image is configured the
	// adapter returns no sidecar at all. A pulled-but-unavailable image
	// would put the sidecar container into ImagePullBackOff, which keeps
	// the engine pod from going Ready and would violate the "cache is an
	// optimisation, never a serving dependency" posture. Defaulting off
	// keeps the default install safe — operators turn auto-attach on
	// when they're ready to ship a subscriber image alongside the
	// controller.
	DefaultSubscriberImage = "ghcr.io/cachebox-project/inference-cache-subscriber:dev"
	// DefaultPolicyServerGRPCAddress is the in-cluster Service DNS the
	// kvevent-subscriber sidecar dials by default. Mirrors the assumption
	// the controller's HTTP poller already makes about the policy server's
	// Deployment landing in the inference-cache-system namespace.
	DefaultPolicyServerGRPCAddress = "inference-cache-server.inference-cache-system.svc.cluster.local:9090"

	// vLLM engine convention: the KV-event ZMQ PUB endpoint binds on :5557
	// by default (the reference stack's --kv-events-config sets
	// endpoint=tcp://*:5557). Parameterising via the adapter (not
	// hardcoding in the webhook) lets SGLang or another engine adapter
	// pick a different port without touching the webhook.
	defaultEngineZMQPortStr = "5557"

	// subscriberHashScheme is the canonical hash-scheme tag the vLLM
	// subscriber carries. Hard-coded for this adapter (vLLM's block-hash
	// scheme is distinct from SGLang's, and the cache plane keys on the
	// scheme to keep them from collapsing).
	subscriberHashScheme = "vllm"

	// modelBackendConfigKey is the BackendConfig key the adapter reads the
	// served model id from when rendering the subscriber sidecar. Mirrors
	// the key the reconciler canary already writes (`backendConfig.model:
	// <served model>`); kept as a constant so a future rename ripples
	// through one place.
	modelBackendConfigKey = "model"
)

// vllmLMCacheAdapter wires vLLM engine pods to the LMCache backend that
// CacheBackend (type=LMCache) provisions. ResolveCacheServer renders a
// standalone lmcache-server pod + Service (the engine connects to it via
// LMCACHE_REMOTE_URL=lm://<svc>:65432); InjectEngineConfig adds the
// --kv-transfer-config arg and the LMCACHE_* env vars to the vLLM container,
// merging with what the pod template already carries; ObservationSidecar
// returns the kvevent-subscriber container the webhook appends so the engine
// pod auto-attaches to the policy server with no out-of-band steps.
//
// Phase 1 only wires vLLM. SGLang HiCache and Mooncake adapters will live in
// their own files when those backends are picked up.
type vllmLMCacheAdapter struct {
	// subscriberImage is the image the kvevent-subscriber sidecar runs.
	// Empty (the default) disables sidecar auto-attach — ObservationSidecar
	// returns nil — so an unconfigured controller install doesn't push
	// engine pods into ImagePullBackOff on a nonexistent default image.
	subscriberImage string
	// policyServerGRPCAddress overrides the default in-cluster Service DNS
	// the sidecar dials to ReportCacheState. Empty falls back to
	// [DefaultPolicyServerGRPCAddress].
	policyServerGRPCAddress string
}

// NewVLLMLMCacheAdapter returns the adapter that wires vLLM engine pods to an
// LMCache CacheBackend. The optional [Option] helpers let the controller pin
// the subscriber sidecar's image + policy-server target; the no-arg form
// reproduces the package defaults and keeps tests + the nil-Registry
// fallback paths working.
func NewVLLMLMCacheAdapter(opts ...Option) KVCacheRuntimeAdapter {
	var cfg Options
	for _, o := range opts {
		o(&cfg)
	}
	return vllmLMCacheAdapter{
		subscriberImage:         cfg.SubscriberImage,
		policyServerGRPCAddress: cfg.PolicyServerGRPCAddress,
	}
}

// Supports matches vLLM runtimes against an LMCache CacheBackend. Any other
// (runtime, backend) combination is left for another adapter — a future
// admission validator surfaces unsupported pairs as ErrNoAdapter.
func (vllmLMCacheAdapter) Supports(runtime RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	if cache == nil {
		return false
	}
	return runtime == RuntimeVLLM && cache.Spec.Type == cachev1alpha1.CacheBackendTypeLMCache
}

// SupportedPairs lets the registry expose this adapter's canonical pair to
// admission error messages so a user who asked for an unsupported pair can
// see what they could have asked for instead.
func (vllmLMCacheAdapter) SupportedPairs() []SupportedPair {
	return []SupportedPair{{Runtime: RuntimeVLLM, Backend: cachev1alpha1.CacheBackendTypeLMCache}}
}

// ReservedArgs returns the leading flag tokens this adapter injects and that
// the LMCache integration cannot function without. The validating webhook
// blocks an spec.integration.engineOverrides entry that tries to override or
// suppress any of these so the operator cannot silently un-wire the connector.
//
//   - "--kv-transfer-config" is the LMCache connector configuration the engine
//     reads at startup; suppressing it means no LMCache wiring at all.
//
// Other tunables the operator may legitimately want to change (e.g. perf
// knobs surfaced as backendConfig keys) are deliberately NOT reserved.
func (vllmLMCacheAdapter) ReservedArgs() []string {
	return []string{defaultEngineKVTransferConfigArg}
}

// EngineContainerName returns [EngineContainerName] — the canonical name the
// vLLM engine container carries on a pod the adapter mutates. The pod
// webhook resolves the override target via this method so admission overrides
// land on the same container [InjectEngineConfig] modified.
func (vllmLMCacheAdapter) EngineContainerName() string { return EngineContainerName }

// ReservedEnv returns the env var names this adapter injects and that the
// LMCache integration cannot function without:
//
//   - LMCACHE_REMOTE_URL is the address of the rendered cache server; an
//     override re-points the engine at a different cache than the CR
//     resolved to.
//   - VLLM_USE_V1 selects the vLLM v1 codepath the LMCache connector targets.
//   - INFERENCECACHE_FAIL_OPEN mirrors spec.integration.failOpen onto the
//     pod; allowing an override would silently desync the pod from the CR
//     contract and from status.failOpen.
//
// Tunables (LMCACHE_CHUNK_SIZE / LMCACHE_REMOTE_SERDE / LMCACHE_LOCAL_CPU /
// LMCACHE_MAX_LOCAL_CPU_SIZE) are perf/mode knobs the operator may legitimately
// want to change and are deliberately NOT reserved.
func (vllmLMCacheAdapter) ReservedEnv() []string {
	return []string{
		EnvLMCacheRemoteURL,
		EnvVLLMUseV1,
		EnvInferenceCacheFailOpen,
	}
}

// ResolveCacheServer renders the standalone LMCache server's container set
// and the Service's port set. The reconciler owns ObjectMeta, the Service
// Selector, the workload kind (Deployment vs StatefulSet), and owner
// references — all of which depend on the CacheBackend identity, not on the
// adapter. Returning only PodSpec.Containers / PodSpec.Volumes and
// Service.Spec.Ports / Service.Spec.Type keeps the seam clean: an adapter
// rendering identical containers for two CacheBackends in different
// namespaces never has to learn about names.
func (vllmLMCacheAdapter) ResolveCacheServer(cache *cachev1alpha1.CacheBackend) (*ResolvedCacheServer, error) {
	if cache == nil {
		return nil, fmt.Errorf("resolve cache server: cache is nil")
	}
	cfg := cache.Spec.BackendConfig
	image := enginewire.ConfigOr(cfg, cfgKeyServerImage, defaultLMCacheServerImage)

	command, args := serverCommand(cfg)
	container := corev1.Container{
		Name:            "lmcache-server",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         command,
		Args:            args,
		Ports: []corev1.ContainerPort{
			{Name: defaultLMCacheServerPortName, ContainerPort: defaultLMCacheServerPort, Protocol: corev1.ProtocolTCP},
		},
		// A TCP-socket readiness probe on the lm:// port gates AvailableReplicas
		// (and therefore the CacheBackend's Ready condition, via managedReadiness)
		// on the LMCache server actually accepting connections — otherwise
		// status could flip Ready before the server is reachable.
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(defaultLMCacheServerPortName)},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			FailureThreshold:    6,
		},
		// CPU + memory requests are added ONLY when autoscaling is configured
		// — a CPU-utilization HPA needs the CPU request as the utilization
		// denominator, so the scaler can't move without one. Non-autoscaled
		// backends keep main's pre-existing "no requests" rendering so this
		// change doesn't alter scheduling for users not opting into HPA. A
		// future first-class spec field can promote these from defaults.
		Resources: defaultServerResources(cache),
	}

	pod := &corev1.PodSpec{
		Containers: []corev1.Container{container},
	}

	svc := &corev1.Service{
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       defaultLMCacheServerPortName,
					Port:       defaultLMCacheServerPort,
					TargetPort: intstr.FromString(defaultLMCacheServerPortName),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	// Declare the persistent data volume when the operator asked for one. The
	// adapter is the only layer that knows where its data lives, so it names the
	// volume + mount path; the reconciler provisions the PVC and mounts it. When
	// spec.storage.pvc is unset, DataVolume stays nil and the server runs
	// ephemeral exactly as before — status quo preserved.
	//
	// The adapter does NOT add the corev1.Volume / VolumeMount itself: it has no
	// PVC name (the reconciler owns CacheBackend identity), and declaring intent
	// keeps the controller generic across future backends. See the constant doc
	// for why the server's storage device is not switched to disk here.
	var dataVolume *AdapterDataVolume
	if cache.Spec.Storage != nil && cache.Spec.Storage.PVC != nil {
		dataVolume = &AdapterDataVolume{
			VolumeName: defaultLMCacheDataVolumeName,
			MountPath:  defaultLMCacheDataMountPath,
		}
	}

	return &ResolvedCacheServer{PodSpec: pod, Service: svc, DataVolume: dataVolume}, nil
}

// defaultServerResources returns the requests baked into the lmcache-server
// container. The defaults are a conservative floor sized for a small KV
// working set + an HPA-usable CPU baseline (a CPU-utilization HPA can't
// compute a denominator without a CPU request). They are applied ONLY when
// the CacheBackend opts into autoscaling, so backends that don't use an HPA
// keep the previous "no requests" rendering and don't see scheduling
// behaviour change on upgrade. A future first-class spec field can override.
func defaultServerResources(cache *cachev1alpha1.CacheBackend) corev1.ResourceRequirements {
	if cache == nil || cache.Spec.Autoscaling == nil {
		return corev1.ResourceRequirements{}
	}
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}
}

// InjectEngineConfig adds the LMCache connector arg and LMCACHE_* env to the
// vLLM container in pod, delegating to the shared engine-wire helper. The
// External backend adapter calls the same helper with an operator-supplied
// endpoint, keeping the rendered engine wiring byte-identical regardless of
// who owns the cache lifecycle.
//
// spec.integration.role maps onto LMCache's kv_role in the connector
// config: ReadOnly → kv_consumer, WriteOnly → kv_producer, ReadWrite
// (and unset / unknown) → kv_both.
func (vllmLMCacheAdapter) InjectEngineConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	return enginewire.InjectVLLMLMCache(pod, endpoint, cache)
}

// InjectRouterConfig is a no-op for LMCache: the LMCache topology has no
// router component the controller needs to wire. Returning nil keeps the
// interface contract satisfied so a Registry caller can blindly invoke both
// Inject* paths on a per-pod basis without branching on backend type — per
// [KVCacheRuntimeAdapter.InjectRouterConfig]: "backends without a router
// component should return nil without touching pod." Input validation is
// intentionally skipped so a router-less backend never forces callers to
// special-case it.
func (vllmLMCacheAdapter) InjectRouterConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	_ = pod
	_ = endpoint
	_ = cache
	return nil
}

// ObservationSidecar returns the kvevent-subscriber container the Pod
// webhook appends to a vLLM engine pod so its KV-cache events flow to the
// policy server with no out-of-band bring-up. The container shares the
// engine pod's network namespace, so the subscriber dials the engine over
// 127.0.0.1 (the vLLM ZMQ PUB endpoint defaults to :5557); identity flags
// are derived from cache + pod (--replica-id from pod.Name via the
// downward API, --tenant-id from pod.Namespace ditto, --model-id from
// cache.Spec.BackendConfig["model"], --hash-scheme fixed to "vllm") so
// the CR is the single source of truth.
//
// The flag surface here is deliberately the intersection of what the
// shipped kvevent-subscriber binary accepts: passing flags the binary
// doesn't know would crash the sidecar on startup (Go's flag package
// rejects unknown flags). Stats-path flags (--engine-metrics-url,
// --stats-interval, etc.) are added when the binary itself learns to
// scrape and emit ReplicaStats.
//
// Returns (nil, nil) when the served model id is not derivable from the CR
// — the subscriber's --model-id flag is required, so emitting a container
// that would CrashLoopBackOff is worse than skipping. The webhook logs the
// skip; once the operator sets spec.backendConfig.model the next pod
// admission picks it up.
func (a vllmLMCacheAdapter) ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	if cache == nil {
		return nil, fmt.Errorf("observation sidecar: cache is nil")
	}
	if pod == nil {
		return nil, fmt.Errorf("observation sidecar: pod is nil")
	}
	// Auto-attach is opt-in: when the operator hasn't configured a
	// subscriber image via the controller flag, skip the sidecar
	// entirely. A nonexistent image would put the sidecar container into
	// ImagePullBackOff, which keeps the engine pod from going Ready —
	// that turns the cache into a serving dependency, the exact failure
	// mode the fail-open posture exists to avoid. See
	// [DefaultSubscriberImage] for the build-tag operators pin to.
	if a.subscriberImage == "" {
		return nil, nil
	}
	modelID := enginewire.ConfigOr(cache.Spec.BackendConfig, modelBackendConfigKey, "")
	if modelID == "" {
		// No --model-id ⇒ subscriber binary would refuse to start; skip
		// the append and let the next admission pick it up once the
		// operator sets spec.backendConfig.model.
		return nil, nil
	}
	serverAddr := a.policyServerGRPCAddress
	if serverAddr == "" {
		serverAddr = DefaultPolicyServerGRPCAddress
	}
	image := a.subscriberImage

	nonRoot := true
	noPrivEsc := false
	readOnlyRoot := true
	uid := int64(65532)
	return &corev1.Container{
		Name:            SubscriberContainerName,
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		// pod.Name is empty at admission for generateName pods; resolve
		// via the downward API so the value is filled in at container
		// start. K8s expands $(VAR) references in args from the
		// container's own env, which lets the literal CR-derived fields
		// (model id, hash scheme) live next to the dynamically resolved
		// ones in one place.
		Env: []corev1.EnvVar{
			{
				Name:      "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}},
			},
			{
				Name:      "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}},
			},
		},
		Args: []string{
			"--engine-endpoint=tcp://127.0.0.1:" + defaultEngineZMQPortStr,
			"--server=" + serverAddr,
			"--replica-id=$(POD_NAME)",
			"--tenant-id=$(POD_NAMESPACE)",
			"--model-id=" + modelID,
			"--hash-scheme=" + subscriberHashScheme,
			// This adapter pairs vLLM with LMCache, a separate L2 cache tier
			// that retains blocks after the engine evicts them from GPU. vLLM
			// emits BlockRemoved on every GPU eviction even when the block is
			// still resident at LMCache; forwarding that as PREFIX_EVICTED
			// would drop a routing hint the replica can still cheaply serve
			// from L2 — the gateway then routes elsewhere and wastes the L2
			// hit. Keep the entry until its freshness TTL expires; soft state
			// means a stale hint is a cache miss at worst, while a missing one
			// routes the request away from its warm replica.
			"--ignore-block-removed=true",
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot:             &nonRoot,
			RunAsUser:                &uid,
			AllowPrivilegeEscalation: &noPrivEsc,
			ReadOnlyRootFilesystem:   &readOnlyRoot,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}, nil
}

// Package-local aliases to the engine-wire helpers. Kept so the in-place
// unit tests in vllm_lmcache_test.go continue to assert on the wire format
// through the canonical adapter API surface. New tests for the shared wire
// (LMCache + External) belong in pkg/adapters/runtime/internal/enginewire.
const defaultEngineKVTransferConfigArg = "--kv-transfer-config"

var (
	kvTransferConfig = enginewire.KVTransferConfig
	upsertArgPair    = enginewire.UpsertArgPair
)

// ValidateLMCacheEndpoint re-exports [enginewire.ValidateLMCacheEndpoint]
// so consumers outside pkg/adapters/runtime — the validating webhook in
// internal/webhook/v1alpha1, the C2 reconciler in internal/controller,
// and the pod webhook in internal/webhook/pod — can call the same
// endpoint-shape check that admission uses. Go's internal-package rule
// keeps the enginewire subpackage adapter-scoped; this re-export is the
// public seam those layers reach for. Returns nil when s is a valid
// LMCache endpoint, otherwise an error whose message describes the
// shape problem (kubectl admission paths wrap it in field.Invalid;
// reconciler/pod-webhook paths surface the message in a status reason
// or fail-open log).
func ValidateLMCacheEndpoint(s string) error {
	return enginewire.ValidateLMCacheEndpoint(s)
}

// serverCommand returns the LMCache server command + args, with a single
// BackendConfig override hook (cfgKeyServerCommand) for users who want to
// switch to the newer `python3 -m lmcache.v1.multiprocess.server` form once
// it stabilises. The default targets the older `lmcache_server <host> <port>
// <storage>` form because it has a documented port (65432) and arg layout.
func serverCommand(cfg map[string]string) (command, args []string) {
	if raw := enginewire.ConfigOr(cfg, cfgKeyServerCommand, ""); raw != "" {
		fields := strings.Fields(raw)
		if len(fields) > 0 {
			return []string{fields[0]}, fields[1:]
		}
	}
	return []string{"lmcache_server"}, []string{
		defaultLMCacheServerHost,
		fmt.Sprintf("%d", defaultLMCacheServerPort),
		defaultLMCacheServerStorage,
	}
}
