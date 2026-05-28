package runtime

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
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

	// vLLM engine-side defaults — what an engine pod must carry to use a
	// remote LMCache server. The CPU-safe LMCACHE_REMOTE_SERDE is "naive";
	// "cachegen" is faster but pulls in CUDA-only codepaths, so it is left
	// for the user to opt into via BackendConfig once running on GPU.
	defaultEngineLMCacheChunkSize    = "256"
	defaultEngineLMCacheRemoteSerde  = "naive"
	defaultEngineLMCacheLocalCPU     = "False"
	defaultEngineLMCacheMaxLocalCPU  = "20"
	defaultEngineKVTransferConfig    = `{"kv_connector":"LMCacheConnectorV1","kv_role":"kv_both"}`
	defaultEngineKVTransferConfigArg = "--kv-transfer-config"
	defaultEngineVLLMUseV1           = "1"

	// BackendConfig override keys. Keep them short, kebab-free, JSON-friendly
	// since they round-trip through CacheBackend.Spec.BackendConfig (a
	// map[string]string).
	cfgKeyServerImage   = "image"
	cfgKeyServerCommand = "serverCommand"
	cfgKeyChunkSize     = "chunkSize"
	cfgKeyRemoteSerde   = "remoteSerde"
	cfgKeyLocalCPU      = "localCPU"
	cfgKeyMaxLocalCPU   = "maxLocalCPU"
)

// Engine env var names. Exported so the C6 mutating webhook (PR2) and tests in
// other packages can assert on them without re-stringifying the contract.
const (
	EnvLMCacheRemoteURL   = "LMCACHE_REMOTE_URL"
	EnvLMCacheRemoteSerde = "LMCACHE_REMOTE_SERDE"
	EnvLMCacheChunkSize   = "LMCACHE_CHUNK_SIZE"
	EnvLMCacheLocalCPU    = "LMCACHE_LOCAL_CPU"
	EnvLMCacheMaxLocalCPU = "LMCACHE_MAX_LOCAL_CPU_SIZE"
	EnvVLLMUseV1          = "VLLM_USE_V1"
	// EngineContainerName is the conventional name for the vLLM container in
	// an engine pod the adapter mutates. When no container with this name is
	// present, the adapter injects into every container — the same defensive
	// merge other adapters use.
	EngineContainerName = "vllm"
)

// vllmLMCacheAdapter wires vLLM engine pods to the LMCache backend that
// CacheBackend (type=LMCache) provisions. ResolveCacheServer renders a
// standalone lmcache-server pod + Service (the engine connects to it via
// LMCACHE_REMOTE_URL=lm://<svc>:65432); InjectEngineConfig adds the
// --kv-transfer-config arg and the LMCACHE_* env vars to the vLLM container,
// merging with what the pod template already carries.
//
// Phase 1 only wires vLLM. SGLang HiCache and Mooncake adapters will live in
// their own files when those backends are picked up.
type vllmLMCacheAdapter struct{}

// NewVLLMLMCacheAdapter returns the adapter that wires vLLM engine pods to an
// LMCache CacheBackend. It is registered by [DefaultRegistry].
func NewVLLMLMCacheAdapter() KVCacheRuntimeAdapter {
	return vllmLMCacheAdapter{}
}

// Supports matches vLLM runtimes against an LMCache CacheBackend. Any other
// (runtime, backend) combination is left for another adapter — the C7
// admission validator surfaces unsupported pairs as ErrNoAdapter.
func (vllmLMCacheAdapter) Supports(runtime RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	if cache == nil {
		return false
	}
	return runtime == RuntimeVLLM && cache.Spec.Type == cachev1alpha1.CacheBackendTypeLMCache
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
	image := configOr(cfg, cfgKeyServerImage, defaultLMCacheServerImage)

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

// InjectEngineConfig adds the LMCache connector arg and LMCACHE_* env to the
// vLLM container in pod. It merges: existing args/env on the vLLM container
// are preserved, repeat injections are idempotent, sidecars are left alone.
// If no container is named [EngineContainerName] the adapter targets every
// container in the pod — pod templates that name vLLM differently still get
// wired, at the cost of duplicating env on innocent sidecars. Adapter users
// who care should name the engine container `vllm`.
func (vllmLMCacheAdapter) InjectEngineConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	if err := validateInjectInputs(pod, endpoint, cache, "engine"); err != nil {
		return err
	}
	cfg := cache.Spec.BackendConfig
	env := []corev1.EnvVar{
		{Name: EnvLMCacheRemoteURL, Value: lmcacheRemoteURL(endpoint)},
		{Name: EnvLMCacheRemoteSerde, Value: configOr(cfg, cfgKeyRemoteSerde, defaultEngineLMCacheRemoteSerde)},
		{Name: EnvLMCacheChunkSize, Value: configOr(cfg, cfgKeyChunkSize, defaultEngineLMCacheChunkSize)},
		{Name: EnvLMCacheLocalCPU, Value: configOr(cfg, cfgKeyLocalCPU, defaultEngineLMCacheLocalCPU)},
		{Name: EnvLMCacheMaxLocalCPU, Value: configOr(cfg, cfgKeyMaxLocalCPU, defaultEngineLMCacheMaxLocalCPU)},
		{Name: EnvVLLMUseV1, Value: defaultEngineVLLMUseV1},
	}
	args := []string{defaultEngineKVTransferConfigArg, defaultEngineKVTransferConfig}

	targets := engineContainerIndices(pod)
	for _, i := range targets {
		for _, e := range env {
			pod.Containers[i].Env = upsertEnv(pod.Containers[i].Env, e)
		}
		pod.Containers[i].Args = upsertArgPair(pod.Containers[i].Args, args[0], args[1])
	}
	return nil
}

// InjectRouterConfig is a no-op for LMCache: the LMCache topology has no
// router component the controller needs to wire. Returning nil keeps the
// interface contract satisfied so a Registry caller can blindly invoke both
// Inject* paths on a per-pod basis without branching on backend type.
func (vllmLMCacheAdapter) InjectRouterConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	if err := validateInjectInputs(pod, endpoint, cache, "router"); err != nil {
		return err
	}
	return nil
}

// engineContainerIndices returns the indices of containers the adapter should
// mutate: only the container named [EngineContainerName] when present, or all
// containers otherwise. The fallback exists so a pod template that names the
// engine container differently still gets wired in PR1's webhook-less
// reconcile-only path; the documented convention is to use the canonical
// name.
func engineContainerIndices(pod *corev1.PodSpec) []int {
	for i := range pod.Containers {
		if pod.Containers[i].Name == EngineContainerName {
			return []int{i}
		}
	}
	all := make([]int, len(pod.Containers))
	for i := range pod.Containers {
		all[i] = i
	}
	return all
}

// upsertArgPair inserts or updates the flag/value pair `flag value` in args,
// preserving every other arg. If flag is already present its immediately
// following value is replaced; otherwise the pair is appended. A trailing
// flag with no value is treated as missing.
func upsertArgPair(args []string, flag, value string) []string {
	for i, a := range args {
		if a == flag {
			if i+1 < len(args) {
				args[i+1] = value
				return args
			}
			return append(args, value)
		}
	}
	return append(args, flag, value)
}

// validateInjectInputs centralises the bad-input checks Inject*Config shares.
// The role tag flows into the error message so callers can tell which path
// rejected the input.
func validateInjectInputs(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend, role string) error {
	if pod == nil {
		return fmt.Errorf("inject %s config: pod is nil", role)
	}
	if cache == nil {
		return fmt.Errorf("inject %s config: cache is nil", role)
	}
	if endpoint == "" {
		return fmt.Errorf("inject %s config: endpoint is empty", role)
	}
	if len(pod.Containers) == 0 {
		return fmt.Errorf("inject %s config: pod has no containers", role)
	}
	return nil
}

// lmcacheRemoteURL prefixes an engine-agnostic host:port endpoint with the
// LMCache lm:// scheme. An endpoint already carrying lm:// (e.g. when a user
// pre-wired their status.endpoint) is passed through unchanged.
func lmcacheRemoteURL(endpoint string) string {
	if strings.HasPrefix(endpoint, "lm://") {
		return endpoint
	}
	return "lm://" + endpoint
}

// serverCommand returns the LMCache server command + args, with a single
// BackendConfig override hook (cfgKeyServerCommand) for users who want to
// switch to the newer `python3 -m lmcache.v1.multiprocess.server` form once
// it stabilises. The default targets the older `lmcache_server <host> <port>
// <storage>` form because it has a documented port (65432) and arg layout.
func serverCommand(cfg map[string]string) (command, args []string) {
	if raw := configOr(cfg, cfgKeyServerCommand, ""); raw != "" {
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

// configOr reads key from cfg or returns fallback when key is absent or empty.
// Mirrors the helper retired from pkg/adapters/backend so the adapter is
// self-contained (the legacy package is removed in this change).
func configOr(cfg map[string]string, key, fallback string) string {
	if v, ok := cfg[key]; ok && v != "" {
		return v
	}
	return fallback
}
