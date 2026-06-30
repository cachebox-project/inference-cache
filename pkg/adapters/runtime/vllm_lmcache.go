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
	// image, pinned to a specific version rather than a floating :latest.
	//
	// Why pin off :latest: the lmcache-server and the lmcache *client*
	// compiled into the vLLM engine communicate over a versioned wire
	// protocol. A floating :latest can drift to a server build whose protocol
	// no longer matches the engine's client; the mismatch disables tier-2
	// offload silently (stores fail, 0 hits, no surfaced error). Pinning
	// removes that silent-drift risk and makes renders reproducible.
	//
	// This is NOT auto-aligned with the engine's client: IC has no source of
	// truth for the engine image's lmcache client version (it is operator-
	// supplied or pip-installed at runtime). Operators MUST keep the version
	// here (or their backendConfig.serverImage override) wire-compatible with
	// their engine's lmcache client — see the "LMCache server / client version
	// alignment" section in docs/design/cachebackend-api.md.
	//
	// Overridable via backendConfig.serverImage (production should pin to a
	// digest there).
	//
	// TODO: wire-test and digest-pin before production. v0.4.7 is version-
	// aligned (it exists upstream and matches the lmcache 0.4.7 client used in
	// validation), but the standalone server image itself was not independently
	// wire-tested here — confirm against a tested build and prefer an @sha256:
	// digest. Do not substitute an invented digest.
	defaultLMCacheServerImage = "lmcache/standalone:v0.4.7"
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
	EnvPythonHashSeed         = enginewire.EnvPythonHashSeed
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
//   - PYTHONHASHSEED pins the deterministic NONE_HASH that seeds vLLM's
//     prefix-cache block-hash chain across the scheduler + TP worker
//     processes; an override re-randomizes it under TP>1 and LMCache reload
//     silently 0-hits (full recompute, no crash, no error). The failure mode
//     is invisible, so the operator must not be able to suppress it.
//
// Tunables (LMCACHE_CHUNK_SIZE / LMCACHE_REMOTE_SERDE / LMCACHE_LOCAL_CPU /
// LMCACHE_MAX_LOCAL_CPU_SIZE) are perf/mode knobs the operator may legitimately
// want to change and are deliberately NOT reserved.
func (vllmLMCacheAdapter) ReservedEnv() []string {
	return []string{
		EnvLMCacheRemoteURL,
		EnvVLLMUseV1,
		EnvInferenceCacheFailOpen,
		EnvPythonHashSeed,
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
func (vllmLMCacheAdapter) ResolveCacheServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	if cache == nil {
		return nil, nil, fmt.Errorf("resolve cache server: cache is nil")
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
		// Container resources come from spec.resources (CRD-defaulted to a
		// 4Gi request / 8Gi memory limit so every CacheBackend is bounded
		// by the cgroup rather than OOM-killed under T2 write load). When
		// autoscaling is set, the helper additionally fills in a CPU
		// request fallback so a CPU-utilization HPA has a denominator —
		// never overwriting an operator-supplied CPU request.
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
	return pod, svc, nil
}

// defaultServerResources resolves the Container.Resources block for the
// lmcache-server. spec.resources (CRD-defaulted to a 4Gi memory request /
// 8Gi memory limit) is the operator-owned baseline and is passed through
// verbatim. When spec.autoscaling is set, the helper additionally fills in
// a CPU request fallback (250m) so a CPU-utilization HPA has a denominator
// — the fallback never overwrites an operator-supplied CPU request. The
// returned ResourceRequirements is a fresh value so callers never alias
// into the CR's spec; mutating the result MUST NOT propagate back into the
// informer-cached object.
func defaultServerResources(cache *cachev1alpha1.CacheBackend) corev1.ResourceRequirements {
	var out corev1.ResourceRequirements
	if cache != nil && cache.Spec.Resources != nil {
		out = *cache.Spec.Resources.DeepCopy()
	}
	if cache == nil || cache.Spec.Autoscaling == nil {
		return out
	}
	// nil-safe init: spec.resources may have been omitted (or carried
	// only Limits), so Requests can be nil here even though we are
	// about to write into it for the HPA fallback below.
	if out.Requests == nil {
		out.Requests = corev1.ResourceList{}
	}
	// CPU-only fallback: the autoscaling spec drives a
	// targetCPUUtilizationPercent HPA, which needs a *positive* CPU
	// request as the denominator. The admission validator admits
	// requests.cpu: "0" (zero is a valid kubelet shape — "no
	// guaranteed minimum"), but with autoscaling it gives the HPA a
	// zero denominator and breaks utilization math; so we treat
	// present-but-non-positive identically to absent and replace it
	// with the fallback. A positive operator-supplied value survives
	// untouched.
	//
	// Memory is NOT auto-filled — spec.resources (carrying the
	// CRD-stamped memory default) is the canonical source for memory,
	// and synthesising a memory request here would override an
	// operator-supplied limits-only shape.
	cpu, hasCPU := out.Requests[corev1.ResourceCPU]
	if !hasCPU || cpu.Sign() <= 0 {
		out.Requests[corev1.ResourceCPU] = resource.MustParse("250m")
	}
	return out
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

// ObservationSidecar returns the kvevent-subscriber container the Pod webhook
// appends to a vLLM engine pod so its KV-cache events flow to the policy
// server with no out-of-band bring-up. The container, its identity flags, and
// its (nil, nil) skip cases are produced by the shared [buildKVEventSubscriber]
// — the subscriber shape is identical for every vLLM-engine L2 backend
// (LMCache, Mooncake) because the KV-event stream comes from vLLM itself, not
// from the L2 store. See that helper for the full contract.
func (a vllmLMCacheAdapter) ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	return buildKVEventSubscriber(a.subscriberImage, a.policyServerGRPCAddress, cache, pod)
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
