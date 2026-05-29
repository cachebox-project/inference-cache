package runtime

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	defaultEngineKVTransferConfigArg = "--kv-transfer-config"
	defaultEngineVLLMUseV1           = "1"

	// kvRole values map the CacheBackend integration role onto LMCache's
	// kv_role semantics carried in the --kv-transfer-config JSON: kv_consumer
	// reads from the cache only, kv_producer writes only, kv_both reads and
	// writes. The default (when integration is unset) is kv_both, matching
	// the ReadWrite role's behaviour.
	kvRoleConsumer = "kv_consumer"
	kvRoleProducer = "kv_producer"
	kvRoleBoth     = "kv_both"

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
	cfgKeyChunkSize     = "chunkSize"
	cfgKeyRemoteSerde   = "remoteSerde"
	cfgKeyLocalCPU      = "localCPU"
	cfgKeyMaxLocalCPU   = "maxLocalCPU"
)

// Engine env var names. Exported so a future mutating webhook and tests in
// other packages can assert on them without re-stringifying the contract.
const (
	EnvLMCacheRemoteURL   = "LMCACHE_REMOTE_URL"
	EnvLMCacheRemoteSerde = "LMCACHE_REMOTE_SERDE"
	// EnvInferenceCacheFailOpen mirrors spec.integration.failOpen onto the
	// engine pod so a future engine-side hook can decide whether to fall
	// back to local prefill on cache unreachability (true) or treat the
	// cache as a hard serving dependency (false). The LMCache connector
	// today is fail-open by default at runtime regardless of this value;
	// surfacing the bit lets the engine layer enforce fail-closed
	// semantics when that work lands, and matches the API/design contract
	// that this flag is plumbed by the engine adapter.
	EnvInferenceCacheFailOpen = "INFERENCECACHE_FAIL_OPEN"
	EnvLMCacheChunkSize       = "LMCACHE_CHUNK_SIZE"
	EnvLMCacheLocalCPU        = "LMCACHE_LOCAL_CPU"
	EnvLMCacheMaxLocalCPU     = "LMCACHE_MAX_LOCAL_CPU_SIZE"
	EnvVLLMUseV1              = "VLLM_USE_V1"
	// EngineContainerName is the conventional name for the vLLM container in
	// an engine pod the adapter mutates. When no container with this name is
	// present, the adapter injects into every container — the same defensive
	// merge other adapters use.
	EngineContainerName = "vllm"
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
	// DefaultSubscriberImage is the development-tag default image the
	// kvevent-subscriber sidecar runs. The Makefile's subscriber-image
	// target emits this tag; production operators override via the
	// controller's --kvevent-subscriber-image flag.
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
	// subscriberImage overrides the default kvevent-subscriber sidecar
	// image when set. Empty falls back to [DefaultSubscriberImage].
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
		// A TCP-socket readiness probe on the lm:// port gates AvailableReplicas
		// (and therefore the CacheBackend's Ready condition, via managedHealth)
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
	return pod, svc, nil
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
// vLLM container in pod. It merges: existing args/env on the vLLM container
// are preserved, repeat injections are idempotent, sidecars are left alone.
// The engine container is identified by the canonical name
// [EngineContainerName]; a single-container pod is also accepted (the lone
// container is treated as the engine). A multi-container pod whose
// containers are not named `vllm` is rejected — silently mutating every
// container would inject vLLM-only flags onto sidecars and crash them.
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
		{Name: EnvInferenceCacheFailOpen, Value: failOpenString(cache)},
	}
	// spec.integration.role maps onto LMCache's kv_role in the connector
	// config: ReadOnly → kv_consumer, WriteOnly → kv_producer, ReadWrite
	// (and unset / unknown) → kv_both.
	args := []string{defaultEngineKVTransferConfigArg, kvTransferConfig(integrationRole(cache))}

	i, err := engineContainerIndex(pod)
	if err != nil {
		return err
	}
	for _, e := range env {
		pod.Containers[i].Env = upsertEnv(pod.Containers[i].Env, e)
	}
	pod.Containers[i].Args = upsertArgPair(pod.Containers[i].Args, args[0], args[1])
	return nil
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
	modelID := configOr(cache.Spec.BackendConfig, modelBackendConfigKey, "")
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
	if image == "" {
		image = DefaultSubscriberImage
	}

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

// engineContainerIndex returns the index of the engine container the adapter
// should mutate. The engine container is identified by the canonical name
// [EngineContainerName]; a single-container pod is also accepted (the lone
// container is assumed to be the engine). Multi-container pods that don't
// name a container `vllm` are rejected — blindly mutating every container
// would inject vLLM-only flags (e.g. `--kv-transfer-config`) onto sidecars,
// which would crash them on startup. The pod-template owner should name the
// engine container `vllm` so the adapter has a stable target.
func engineContainerIndex(pod *corev1.PodSpec) (int, error) {
	for i := range pod.Containers {
		if pod.Containers[i].Name == EngineContainerName {
			return i, nil
		}
	}
	if len(pod.Containers) == 1 {
		return 0, nil
	}
	names := make([]string, len(pod.Containers))
	for i := range pod.Containers {
		names[i] = pod.Containers[i].Name
	}
	return -1, fmt.Errorf("inject engine config: pod has %d containers %v but none is named %q; injecting vLLM flags into unrelated sidecars would crash them — name the engine container %q",
		len(pod.Containers), names, EngineContainerName, EngineContainerName)
}

// upsertArgPair inserts or updates the flag/value pair `flag value` in args,
// preserving every other arg. Both the two-arg form (`--flag`, `value`) and
// the equals form (`--flag=value`) are recognised: an existing entry in
// either form is updated in place (to the two-arg form), no duplicate is
// appended. A trailing two-arg `--flag` with no value is treated as missing.
// Normalising on the two-arg form keeps the rendered args stable across
// repeat injections so an idempotent reconcile doesn't churn.
func upsertArgPair(args []string, flag, value string) []string {
	prefix := flag + "="
	for i, a := range args {
		switch {
		case a == flag:
			if i+1 < len(args) {
				args[i+1] = value
				return args
			}
			return append(args, value)
		case strings.HasPrefix(a, prefix):
			// Replace the single equals-form entry with the two-arg form.
			// Splice in `flag, value` at position i.
			args[i] = flag
			out := make([]string, 0, len(args)+1)
			out = append(out, args[:i+1]...)
			out = append(out, value)
			out = append(out, args[i+1:]...)
			return out
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

// failOpenString returns the bool form of the effective fail-open mode for
// the engine env. The CRD defaults to fail-open via the defaulting webhook;
// the helper handles nil Integration / nil failOpen too — pre-defaulting
// code paths shouldn't crash. The rendered string is "true" / "false" so the
// engine-side hook can parse it without LMCache-specific knowledge.
func failOpenString(cache *cachev1alpha1.CacheBackend) string {
	if cachev1alpha1.IntegrationFailOpen(cache.Spec.Integration) {
		return "true"
	}
	return "false"
}

// integrationRole returns the engine's participation role from cache.Spec.
// Integration, defaulting to ReadWrite (matching the CRD's documented
// behaviour when integration is unset).
func integrationRole(cache *cachev1alpha1.CacheBackend) cachev1alpha1.CacheBackendIntegrationRole {
	if cache.Spec.Integration == nil || cache.Spec.Integration.Role == "" {
		return cachev1alpha1.CacheBackendIntegrationRoleReadWrite
	}
	return cache.Spec.Integration.Role
}

// kvTransferConfig renders the --kv-transfer-config JSON for the given role.
// An unrecognised role falls back to kv_both so a future CRD value (added
// after this adapter ships) is not silently dropped from the kv path.
func kvTransferConfig(role cachev1alpha1.CacheBackendIntegrationRole) string {
	kvRole := kvRoleBoth
	switch role {
	case cachev1alpha1.CacheBackendIntegrationRoleReadOnly:
		kvRole = kvRoleConsumer
	case cachev1alpha1.CacheBackendIntegrationRoleWriteOnly:
		kvRole = kvRoleProducer
	case cachev1alpha1.CacheBackendIntegrationRoleReadWrite:
		kvRole = kvRoleBoth
	}
	return fmt.Sprintf(`{"kv_connector":"LMCacheConnectorV1","kv_role":%q}`, kvRole)
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
