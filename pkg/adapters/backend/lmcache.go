package backend

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// Defaults templated from the A2 reference stack
// (docs/reference-stack/manifests/deployment.yaml). They are overridable via
// spec.backendConfig keys until a later module promotes them to first-class spec fields.
const (
	// defaultLMCacheImage serves vLLM with the LMCache connector pre-installed.
	// A pinned digest belongs in production config (see VERSIONS.md); :latest is
	// the overridable default so automation never bakes in the non-applyable
	// placeholder digest the raw manifest ships.
	defaultLMCacheImage = "lmcache/vllm-openai:latest"
	defaultLMCacheModel = "meta-llama/Llama-3.1-8B-Instruct"

	defaultLMCacheChunkSize       = "256"
	defaultLMCacheLocalCPU        = "True"
	defaultLMCacheMaxLocalCPUSize = "20"
	defaultHFTokenSecretName      = "hf-token"

	// CPU profile (backendConfig profile=cpu): a GPU-free vLLM engine for substrate
	// validation off-GPU. It keeps prefix caching + the KV-event publisher but drops
	// the LMCache connector — real LMCache offload requires a GPU, so the default
	// (gpu) profile owns that. Mirrors docs/reference-stack/manifests/cpu-local.
	// The upstream CPU image is arch-tagged (latest-arm64 / latest-x86_64) with no
	// safe multi-arch default, so backendConfig.image is REQUIRED for this profile.
	defaultCPUModel        = "Qwen/Qwen2.5-0.5B-Instruct"
	defaultCPUKVCacheSpace = "4"

	// API-server pod defaults for the two override fields that are server-defaulted.
	// Baking them into the rendered template keeps the update path churn-free (the
	// reconciled value matches the live, defaulted object).
	defaultSchedulerName                 = "default-scheduler"
	defaultTerminationGracePeriodSeconds = int64(30)

	// backendConfig override keys.
	cfgKeyImage         = "image"
	cfgKeyModel         = "model"
	cfgKeyHFTokenSecret = "hfTokenSecret"
	cfgKeyProfile       = "profile"

	// profile values for the profile backendConfig key.
	profileGPU = "gpu"
	profileCPU = "cpu"

	portHTTP     = 8000
	portKVEvents = 5557
	portKVReplay = 5558

	// kvTransferConfig wires vLLM's KV read/write path through LMCache.
	kvTransferConfig = `{"kv_connector":"LMCacheConnectorV1","kv_role":"kv_both"}`
	// kvEventsConfig enables the ZMQ KV-cache event publisher (BlockStored /
	// BlockRemoved / AllBlocksCleared) the cache plane subscribes to.
	kvEventsConfig = `{"enable_kv_cache_events":true,"publisher":"zmq","endpoint":"tcp://*:5557","replay_endpoint":"tcp://*:5558","buffer_steps":10000,"topic":"kv-events"}`
)

// lmCacheBuilder renders the vLLM+LMCache backend workload from a CacheBackend.
type lmCacheBuilder struct{}

func (lmCacheBuilder) Type() cachev1alpha1.CacheBackendType {
	return cachev1alpha1.CacheBackendTypeLMCache
}

func (lmCacheBuilder) Build(cb *cachev1alpha1.CacheBackend) (*Workload, error) {
	if cb == nil {
		return nil, fmt.Errorf("nil CacheBackend")
	}

	name := cb.Name
	namespace := cb.Namespace
	cfg := cb.Spec.BackendConfig

	hfSecret := configOr(cfg, cfgKeyHFTokenSecret, defaultHFTokenSecretName)

	replicas := int32(1)
	if cb.Spec.Replicas != nil {
		replicas = *cb.Spec.Replicas
	}

	selector := selectorLabels(name)
	podLabels := podTemplateLabels(name)

	var container corev1.Container
	var shmSize resource.Quantity
	if strings.EqualFold(configOr(cfg, cfgKeyProfile, profileGPU), profileCPU) {
		// The CPU image is arch-tagged upstream with no safe multi-arch default,
		// so it must be supplied explicitly (e.g. vllm/vllm-openai-cpu:latest-arm64).
		image := configOr(cfg, cfgKeyImage, "")
		if image == "" {
			return nil, fmt.Errorf("backendConfig.profile=cpu requires backendConfig.image (an arch-tagged CPU image, e.g. vllm/vllm-openai-cpu:latest-arm64)")
		}
		model := configOr(cfg, cfgKeyModel, defaultCPUModel)
		container = cpuEngineContainer(image, model, hfSecret)
		shmSize = resource.MustParse("4Gi")
	} else {
		image := configOr(cfg, cfgKeyImage, defaultLMCacheImage)
		model := configOr(cfg, cfgKeyModel, defaultLMCacheModel)
		container = lmCacheEngineContainer(image, model, hfSecret)
		shmSize = resource.MustParse("8Gi")
	}

	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{container},
		Volumes: []corev1.Volume{
			{
				Name:         "cache-home",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			},
			{
				Name: "shm",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						Medium:    corev1.StorageMediumMemory,
						SizeLimit: ptrQuantity(shmSize),
					},
				},
			},
		},
	}
	applyPodOverrides(&podSpec, cb.Spec.Template)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    podLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec:       podSpec,
			},
		},
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    podLabels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selector,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: portHTTP, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP},
				{Name: "kv-events", Port: portKVEvents, TargetPort: intstr.FromString("kv-events"), Protocol: corev1.ProtocolTCP},
				{Name: "kv-replay", Port: portKVReplay, TargetPort: intstr.FromString("kv-replay"), Protocol: corev1.ProtocolTCP},
			},
		},
	}

	return &Workload{
		Deployment: deployment,
		Service:    service,
		Endpoint:   fmt.Sprintf("%s.%s.svc.cluster.local:%d", name, namespace, portHTTP),
	}, nil
}

// lmCacheEngineContainer renders the GPU vLLM+LMCache container (default profile):
// vLLM reads/writes KV through the LMCache connector, with prefix caching and the
// KV-event publisher enabled.
func lmCacheEngineContainer(image, model, hfSecret string) corev1.Container {
	c := baseEngineContainer(image, model, hfSecret)
	c.Args = []string{
		fmt.Sprintf("--port=%d", portHTTP),
		"--enable-prefix-caching",
		"--kv-transfer-config", kvTransferConfig,
		"--kv-events-config", kvEventsConfig,
	}
	c.Env = append([]corev1.EnvVar{
		{Name: "VLLM_USE_V1", Value: "1"},
		{Name: "LMCACHE_CHUNK_SIZE", Value: defaultLMCacheChunkSize},
		{Name: "LMCACHE_LOCAL_CPU", Value: defaultLMCacheLocalCPU},
		{Name: "LMCACHE_MAX_LOCAL_CPU_SIZE", Value: defaultLMCacheMaxLocalCPUSize},
	}, hfTokenEnv(hfSecret))
	c.Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
	}
	return c
}

// cpuEngineContainer renders a GPU-free vLLM container (profile=cpu): no GPU limit
// and no LMCache connector, but prefix caching and the KV-event publisher stay on so
// the substrate (engine config + KV-event stream) can be validated off-GPU.
func cpuEngineContainer(image, model, hfSecret string) corev1.Container {
	c := baseEngineContainer(image, model, hfSecret)
	c.Args = []string{
		fmt.Sprintf("--port=%d", portHTTP),
		"--dtype=bfloat16",
		"--max-model-len=8192",
		"--enforce-eager",
		"--enable-prefix-caching",
		"--kv-events-config", kvEventsConfig,
	}
	c.Env = append([]corev1.EnvVar{
		{Name: "VLLM_CPU_KVCACHE_SPACE", Value: defaultCPUKVCacheSpace},
	}, hfTokenEnv(hfSecret))
	return c
}

// baseEngineContainer holds the parts shared by every profile (name, image,
// command, ports, readiness probe, mounts); args/env/resources are profile-specific.
func baseEngineContainer(image, model, hfSecret string) corev1.Container {
	return corev1.Container{
		Name:            "vllm",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"vllm", "serve", model},
		Ports: []corev1.ContainerPort{
			{Name: "http", ContainerPort: portHTTP, Protocol: corev1.ProtocolTCP},
			{Name: "kv-events", ContainerPort: portKVEvents, Protocol: corev1.ProtocolTCP},
			{Name: "kv-replay", ContainerPort: portKVReplay, Protocol: corev1.ProtocolTCP},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/health",
					Port: intstr.FromString("http"),
				},
			},
			InitialDelaySeconds: 60,
			PeriodSeconds:       10,
			FailureThreshold:    60,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "cache-home", MountPath: "/root/.cache/huggingface"},
			{Name: "shm", MountPath: "/dev/shm"},
		},
	}
}

// hfTokenEnv injects the optional HF_TOKEN secret ref so gated models can pull; it
// is optional so ungated models (e.g. the CPU profile default) run without it.
func hfTokenEnv(secret string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: "HF_TOKEN",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secret},
				Key:                  "token",
				Optional:             ptrTo(true),
			},
		},
	}
}

// selectorLabels are the immutable identity labels for a backend's child objects.
func selectorLabels(name string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "cachebackend",
		"app.kubernetes.io/instance":   name,
		"app.kubernetes.io/managed-by": "inference-cache-controller",
	}
}

// podTemplateLabels add backend/engine identity on top of the selector labels.
func podTemplateLabels(name string) map[string]string {
	labels := selectorLabels(name)
	labels["inferencecache.io/backend-type"] = "lmcache"
	labels["inferencecache.io/engine"] = "vllm"
	return labels
}

// applyPodOverrides copies optional pod-level scheduling/security overrides from
// the spec onto the rendered pod spec. The server-defaulted fields (schedulerName,
// terminationGracePeriodSeconds) are always set to their defaults when unset so the
// rendered template matches the API-server-defaulted object and updates don't churn.
func applyPodOverrides(spec *corev1.PodSpec, override *cachev1alpha1.CacheBackendPodSpecOverride) {
	spec.SchedulerName = defaultSchedulerName
	spec.TerminationGracePeriodSeconds = ptrTo(defaultTerminationGracePeriodSeconds)
	if override == nil {
		return
	}
	spec.NodeSelector = override.NodeSelector
	spec.Affinity = override.Affinity
	spec.Tolerations = override.Tolerations
	spec.TopologySpreadConstraints = override.TopologySpreadConstraints
	spec.ImagePullSecrets = override.ImagePullSecrets
	spec.ServiceAccountName = override.ServiceAccountName
	spec.SecurityContext = override.SecurityContext
	spec.PriorityClassName = override.PriorityClassName
	spec.RuntimeClassName = override.RuntimeClassName
	if override.SchedulerName != "" {
		spec.SchedulerName = override.SchedulerName
	}
	if override.TerminationGracePeriodSeconds != nil {
		spec.TerminationGracePeriodSeconds = override.TerminationGracePeriodSeconds
	}
}

func configOr(cfg map[string]string, key, fallback string) string {
	if v, ok := cfg[key]; ok && v != "" {
		return v
	}
	return fallback
}

func ptrTo[T any](v T) *T { return &v }

func ptrQuantity(q resource.Quantity) *resource.Quantity { return &q }
