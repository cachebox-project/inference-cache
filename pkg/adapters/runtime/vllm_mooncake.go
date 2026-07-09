package runtime

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

// vLLM+Mooncake canonical defaults. These template a standalone Mooncake store
// "master" pod that vLLM engines connect to via
// LMCACHE_REMOTE_URL=mooncakestore://<svc>:<rpc-port>. Mooncake is integrated
// as an LMCache *remote backend* (the durable, network-addressable store the
// project designates the shared/scalable path — see
// docs/design/lmcache-server-persistence.md): the engine runs the
// ordinary LMCache connector — see [enginewire.InjectVLLMMooncake] — pointed at
// the mooncakestore:// scheme, so the only thing that distinguishes this
// adapter from the vLLM+LMCache adapter on the engine side is the remote-URL
// scheme. The server side, by contrast, is entirely Mooncake's own: a
// mooncake_master process (RPC + an embedded HTTP metadata server) rather than
// an lmcache-server.
//
// Defaults are overridable via CacheBackend.Spec.BackendConfig so a real
// deployment can pin to digests without a code change.
const (
	// defaultMooncakeMasterImage is the upstream Mooncake image, pinned to a
	// specific version rather than a floating :latest.
	//
	// kvcacheai/mooncake:0.3.11.post1 is the only version tag published on
	// Docker Hub (https://hub.docker.com/r/kvcacheai/mooncake) and matches the
	// mooncake-transfer-engine 0.3.11.post1 release on PyPI, so the master and
	// the engine-side transfer-engine client are version-aligned when the
	// operator pins the engine's pip package to the same release. Pinning off
	// :latest removes the silent-drift risk a floating tag carries between the
	// master and the client (a mismatched wire protocol disables tier-2 offload
	// silently — the same failure class documented for LMCache in
	// docs/design/cachebackend-api.md).
	//
	// Overridable via backendConfig.serverImage (production should pin to a
	// digest there).
	//
	// The reference is FULLY QUALIFIED (docker.io/...) on purpose: a CRI-O node
	// without short-name resolution configured (registry aliases or an
	// unqualified-search-registries list — the common default) rejects a bare
	// short name ("short-name … did not resolve to an alias, and no
	// containers-registries.conf was found"), so an unqualified default
	// ImagePullBackOffs there. containerd resolves short names against
	// docker.io by default, but the explicit registry is safe on both.
	//
	// TODO(cachebox): digest-pin before production. That `mooncake_master` is on
	// PATH and the RPC / metadata / metrics ports (50051 / 8080 / 9003) match
	// what this adapter renders are confirmed against the real image on a live
	// cluster (the master boots and reaches serving); the remaining hardening is
	// an @sha256: digest here. Do not substitute an invented digest.
	//
	// This default fails SAFE, not silently: if the image is wrong (no
	// `mooncake_master` on PATH, different flags/ports) the master pod
	// CrashLoops / ImagePullBackOffs, the managed Deployment never reports
	// Available, and the RPC-port readiness probe below keeps the CacheBackend
	// at Ready=False (RolloutInProgress / ReplicasUnavailable) — the operator
	// sees the breakage in `kubectl get cachebackend`, not a green-but-dead
	// backend. And because the cache is fail-open, engines fall back to local
	// prefill regardless, so a broken master is never a serving outage. An
	// operator can repoint to a known-good image/digest via backendConfig
	// .serverImage without a code change.
	defaultMooncakeMasterImage = "docker.io/kvcacheai/mooncake:0.3.11.post1"

	// defaultMooncakeMasterRPCPort is the Mooncake master's RPC port — the
	// address vLLM's LMCache connector dials via the mooncakestore:// URL.
	// 50051 is the master's documented default (--rpc_port).
	defaultMooncakeMasterRPCPort = int32(50051)
	// defaultMooncakeMetadataPort is the master's embedded HTTP metadata
	// server port (--http_metadata_server_port). The Mooncake transfer engine
	// can use this instead of an external etcd/redis metadata service.
	defaultMooncakeMetadataPort = int32(8080)
	// defaultMooncakeMetricsPort is the master's Prometheus metrics port
	// (--metrics_port default 9003). Exposed as a container port for scraping;
	// not part of the engine wire.
	defaultMooncakeMetricsPort = int32(9003)
	// defaultMooncakeMasterHost is the bind address inside the pod for the
	// embedded HTTP metadata server.
	defaultMooncakeMasterHost = "0.0.0.0"

	// Port names other parts of the system can address without hard-coding the
	// integer. The RPC port name is the readiness-probe + Service-endpoint
	// target. K8s caps container/Service port names at 15 characters, so the
	// metrics port drops the "mooncake-" prefix to stay within the limit.
	mooncakeRPCPortName      = "mooncake-rpc"
	mooncakeMetadataPortName = "mooncake-meta"
	mooncakeMetricsPortName  = "metrics"

	// mooncakeMasterContainerName is the canonical name of the master
	// container the adapter renders.
	mooncakeMasterContainerName = "mooncake-master"
)

// vllmMooncakeAdapter wires vLLM engine pods to the Mooncake store that
// CacheBackend (type=Mooncake) provisions. ResolveCacheServer renders a
// standalone mooncake_master pod + Service (the engine connects to it via
// LMCACHE_REMOTE_URL=mooncakestore://<svc>:50051); InjectEngineConfig adds the
// --kv-transfer-config arg and the LMCACHE_* env vars to the vLLM container via
// the shared LMCache-connector wire (merging, never clobbering);
// ObservationSidecar returns the same kvevent-subscriber container the LMCache
// adapter does (the KV-event stream is engine-side, so the sidecar shape is
// identical) so the engine pod auto-attaches to the policy server with no
// out-of-band steps.
//
// Why the engine wire is the LMCache connector and not vLLM's native
// MooncakeStoreConnector: the native connector is configured exclusively
// through a MOONCAKE_CONFIG_PATH JSON file (it has no env-var surface for the
// master address), and the pod-mutating webhook can only inject env + args —
// it cannot write a file into a user-owned engine container. Routing the
// controller-resolved master endpoint through LMCACHE_REMOTE_URL=
// mooncakestore://… is the only path that lets status.endpoint reach the engine
// via injection alone, and it matches the locked design decision that Mooncake
// "fits the lm://-style RemoteBackend wire" (docs/design/lmcache-server-persistence.md).
// The native connector
// remains available to operators who pre-bake their own config file; this
// adapter targets the auto-wired path.
type vllmMooncakeAdapter struct {
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

// NewVLLMMooncakeAdapter returns the adapter that wires vLLM engine pods to a
// Mooncake CacheBackend. The optional [Option] helpers let the controller pin
// the subscriber sidecar's image + policy-server target (shared with the
// vLLM+LMCache adapter via [DefaultRegistry]); the no-arg form reproduces the
// package defaults and keeps tests + the nil-Registry fallback paths working.
func NewVLLMMooncakeAdapter(opts ...Option) KVCacheRuntimeAdapter {
	var cfg Options
	for _, o := range opts {
		o(&cfg)
	}
	return vllmMooncakeAdapter{
		subscriberImage:         cfg.SubscriberImage,
		policyServerGRPCAddress: cfg.PolicyServerGRPCAddress,
	}
}

// Supports matches vLLM runtimes against a Mooncake CacheBackend. Any other
// (runtime, backend) combination is left for another adapter; admission
// surfaces unsupported pairs as ErrNoAdapter.
func (vllmMooncakeAdapter) Supports(runtime RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	if cache == nil {
		return false
	}
	return runtime == RuntimeVLLM && cache.Spec.Type == cachev1alpha1.CacheBackendTypeMooncake
}

// SupportedPairs lets the registry expose this adapter's canonical pair to
// admission error messages so a user who asked for an unsupported pair can
// see what they could have asked for instead.
func (vllmMooncakeAdapter) SupportedPairs() []SupportedPair {
	return []SupportedPair{{Runtime: RuntimeVLLM, Backend: cachev1alpha1.CacheBackendTypeMooncake}}
}

// ReservedArgs returns the leading flag tokens this adapter injects and that
// the integration cannot function without. Mooncake speaks the LMCache
// connector, so the reserved arg is the same as the vLLM+LMCache adapter's:
//
//   - "--kv-transfer-config" is the LMCache connector configuration the engine
//     reads at startup; suppressing it means no Mooncake wiring at all.
func (vllmMooncakeAdapter) ReservedArgs() []string {
	return []string{defaultEngineKVTransferConfigArg}
}

// ReservedEnv returns the env var names this adapter injects and that the
// integration cannot function without. Identical to the vLLM+LMCache adapter's
// set because Mooncake reuses the LMCache connector wire:
//
//   - LMCACHE_REMOTE_URL is the mooncakestore:// address of the rendered
//     Mooncake master; an override re-points the engine at a different store
//     than the CR resolved to.
//   - VLLM_USE_V1 selects the vLLM v1 codepath the LMCache connector targets.
//   - INFERENCECACHE_FAIL_OPEN mirrors spec.integration.failOpen onto the pod;
//     allowing an override would silently desync the pod from the CR contract.
//   - PYTHONHASHSEED pins the deterministic NONE_HASH that seeds vLLM's
//     prefix-cache block-hash chain across the scheduler + TP worker processes;
//     an override re-randomizes it under TP>1 and reload silently 0-hits.
//
// Tunables (LMCACHE_CHUNK_SIZE / LMCACHE_REMOTE_SERDE / LMCACHE_LOCAL_CPU /
// LMCACHE_MAX_LOCAL_CPU_SIZE) are perf/mode knobs the operator may legitimately
// want to change and are deliberately NOT reserved.
func (vllmMooncakeAdapter) ReservedEnv() []string {
	return []string{
		EnvLMCacheRemoteURL,
		EnvVLLMUseV1,
		EnvInferenceCacheFailOpen,
		EnvPythonHashSeed,
	}
}

// EngineContainerName returns [EngineContainerName] — the canonical name the
// vLLM engine container carries on a pod the adapter mutates. The pod webhook
// resolves the override target via this method so admission overrides land on
// the same container [vllmMooncakeAdapter.InjectEngineConfig] modified.
func (vllmMooncakeAdapter) EngineContainerName() string { return EngineContainerName }

// ResolveCacheServer renders the standalone Mooncake master's container set and
// the Service's port set. As with the LMCache adapter, the reconciler owns
// ObjectMeta, the Service Selector, the workload kind, and owner references —
// all of which depend on the CacheBackend identity, not on the adapter — so
// this returns only PodSpec.Containers and Service.Spec.Ports/Type.
//
// The RPC port is rendered FIRST in both the container and the Service so the
// reconciler's engine-agnostic serviceEndpoint helper (which formats
// status.endpoint from the Service's first port) points the engine at the
// master's mooncakestore:// RPC address, not the metadata port.
func (vllmMooncakeAdapter) ResolveCacheServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	if cache == nil {
		return nil, nil, fmt.Errorf("resolve cache server: cache is nil")
	}
	cfg := cache.Spec.BackendConfig
	image := enginewire.ConfigOr(cfg, cfgKeyServerImage, defaultMooncakeMasterImage)

	command, args := mooncakeMasterCommand(cfg)
	container := corev1.Container{
		Name:            mooncakeMasterContainerName,
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         command,
		Args:            args,
		Ports: []corev1.ContainerPort{
			// RPC first — it is the mooncakestore:// endpoint the engine dials
			// and the port serviceEndpoint publishes into status.endpoint.
			{Name: mooncakeRPCPortName, ContainerPort: defaultMooncakeMasterRPCPort, Protocol: corev1.ProtocolTCP},
			{Name: mooncakeMetadataPortName, ContainerPort: defaultMooncakeMetadataPort, Protocol: corev1.ProtocolTCP},
			{Name: mooncakeMetricsPortName, ContainerPort: defaultMooncakeMetricsPort, Protocol: corev1.ProtocolTCP},
		},
		// A TCP-socket readiness probe on the RPC port gates AvailableReplicas
		// (and therefore the CacheBackend's Ready condition, via managedReadiness)
		// on the Mooncake master actually accepting connections — otherwise
		// status could flip Ready before the store is reachable.
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(mooncakeRPCPortName)},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			FailureThreshold:    6,
		},
		// Resources come from spec.resources (CRD-defaulted to a 4Gi request /
		// 8Gi limit) with the same autoscaling CPU-request fallback the LMCache
		// server uses — shared helper, identical semantics.
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
					Name:       mooncakeRPCPortName,
					Port:       defaultMooncakeMasterRPCPort,
					TargetPort: intstr.FromString(mooncakeRPCPortName),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       mooncakeMetadataPortName,
					Port:       defaultMooncakeMetadataPort,
					TargetPort: intstr.FromString(mooncakeMetadataPortName),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
	return pod, svc, nil
}

// InjectEngineConfig adds the LMCache connector arg and LMCACHE_* env to the
// vLLM container in pod, delegating to the shared engine-wire helper with the
// mooncakestore:// remote-URL scheme. The merge contract (preserve existing
// args/env, idempotent, sidecars untouched) is identical to the vLLM+LMCache
// path — see [enginewire.InjectVLLMMooncake].
//
// spec.integration.role maps onto LMCache's kv_role exactly as for the LMCache
// adapter: ReadOnly → kv_consumer, WriteOnly → kv_producer, ReadWrite (and
// unset / unknown) → kv_both.
func (vllmMooncakeAdapter) InjectEngineConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	return enginewire.InjectVLLMMooncake(pod, endpoint, cache)
}

// InjectRouterConfig is a no-op for Mooncake: the topology has no router
// component the controller needs to wire. Returning nil keeps the interface
// contract satisfied so a Registry caller can blindly invoke both Inject* paths
// per-pod without branching on backend type — per
// [KVCacheRuntimeAdapter.InjectRouterConfig]: "backends without a router
// component should return nil without touching pod."
func (vllmMooncakeAdapter) InjectRouterConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	_ = pod
	_ = endpoint
	_ = cache
	return nil
}

// ObservationSidecar returns the kvevent-subscriber container the Pod webhook
// appends to a vLLM engine pod. The subscriber observes vLLM's own ZMQ
// KV-event stream, which is independent of the L2 store, so the container is
// byte-identical to the vLLM+LMCache adapter's — both delegate to the shared
// [RenderSubscriberSidecar] with the vLLM engine dialect (--hash-scheme=vllm,
// the vLLM ZMQ PUB port). See that helper for the full contract (opt-in image
// gate, required model id, downward-API identity, --ignore-block-removed
// rationale for L2 tiers).
func (a vllmMooncakeAdapter) ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	return RenderSubscriberSidecar(SubscriberSidecarParams{
		Image:            a.subscriberImage,
		ServerAddr:       a.policyServerGRPCAddress,
		Cache:            cache,
		Pod:              pod,
		HashScheme:       subscriberHashScheme,
		EngineZMQPortStr: defaultEngineZMQPortStr,
	})
}

// mooncakeMasterCommand returns the Mooncake master command + args, with a
// single BackendConfig override hook (cfgKeyServerCommand) for operators who
// need to change the master's flags (a different metadata backend, HA mode,
// etc.). The default launches the master with its RPC port, Prometheus metrics
// port, and the embedded HTTP metadata server so the simplest deployment needs
// no external etcd/redis.
//
// PORT CONSTRAINT: the override MUST keep the RPC port at
// [defaultMooncakeMasterRPCPort] (50051) and the HTTP metadata port at
// [defaultMooncakeMetadataPort] (8080). [ResolveCacheServer] pins the Service
// ports, the container ports, the readiness probe, and (via the reconciler's
// serviceEndpoint) status.endpoint to those two values — they are NOT derived
// from this command string, because a free-form command can't be reliably
// parsed for flag values. So an override that changes `--rpc_port` or
// `--http_metadata_server_port` desyncs the master from the Service: the
// readiness probe and engine wire would target dead ports and the backend
// would stay Ready=False. Change the metadata backend / HA flags here freely;
// do NOT change the ports. (Synchronized port config — backendConfig keys that
// drive both the command and the Service — is a deliberate non-goal for
// v1alpha1; the LMCache adapter's serverCommand carries the same constraint.)
func mooncakeMasterCommand(cfg map[string]string) (command, args []string) {
	if raw := enginewire.ConfigOr(cfg, cfgKeyServerCommand, ""); raw != "" {
		fields := strings.Fields(raw)
		if len(fields) > 0 {
			return []string{fields[0]}, fields[1:]
		}
	}
	return []string{"mooncake_master"}, []string{
		fmt.Sprintf("--rpc_port=%d", defaultMooncakeMasterRPCPort),
		fmt.Sprintf("--metrics_port=%d", defaultMooncakeMetricsPort),
		"--enable_http_metadata_server=true",
		"--http_metadata_server_host=" + defaultMooncakeMasterHost,
		fmt.Sprintf("--http_metadata_server_port=%d", defaultMooncakeMetadataPort),
	}
}

// Compile-time assertion: the adapter implements the full C5 interface.
var _ KVCacheRuntimeAdapter = vllmMooncakeAdapter{}
