package backend

import (
	"fmt"

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

	// API-server pod defaults for the two override fields that are server-defaulted.
	// Baking them into the rendered template keeps the update path churn-free (the
	// reconciled value matches the live, defaulted object).
	defaultSchedulerName                 = "default-scheduler"
	defaultTerminationGracePeriodSeconds = int64(30)

	// backendConfig override keys.
	cfgKeyImage         = "image"
	cfgKeyModel         = "model"
	cfgKeyHFTokenSecret = "hfTokenSecret"

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

	image := configOr(cfg, cfgKeyImage, defaultLMCacheImage)
	model := configOr(cfg, cfgKeyModel, defaultLMCacheModel)
	hfSecret := configOr(cfg, cfgKeyHFTokenSecret, defaultHFTokenSecretName)

	replicas := int32(1)
	if cb.Spec.Replicas != nil {
		replicas = *cb.Spec.Replicas
	}

	selector := selectorLabels(name)
	podLabels := podTemplateLabels(name)

	container := corev1.Container{
		Name:            "vllm",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"vllm", "serve", model},
		Args: []string{
			fmt.Sprintf("--port=%d", portHTTP),
			"--enable-prefix-caching",
			"--kv-transfer-config", kvTransferConfig,
			"--kv-events-config", kvEventsConfig,
		},
		Env: []corev1.EnvVar{
			{Name: "VLLM_USE_V1", Value: "1"},
			{Name: "LMCACHE_CHUNK_SIZE", Value: defaultLMCacheChunkSize},
			{Name: "LMCACHE_LOCAL_CPU", Value: defaultLMCacheLocalCPU},
			{Name: "LMCACHE_MAX_LOCAL_CPU_SIZE", Value: defaultLMCacheMaxLocalCPUSize},
			{
				Name: "HF_TOKEN",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: hfSecret},
						Key:                  "token",
						// Optional so ungated models run without the secret present;
						// the A2 reference requires it for the gated default model.
						Optional: ptrTo(true),
					},
				},
			},
		},
		Ports: []corev1.ContainerPort{
			{Name: "http", ContainerPort: portHTTP, Protocol: corev1.ProtocolTCP},
			{Name: "kv-events", ContainerPort: portKVEvents, Protocol: corev1.ProtocolTCP},
			{Name: "kv-replay", ContainerPort: portKVReplay, Protocol: corev1.ProtocolTCP},
		},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("1"),
			},
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
						SizeLimit: ptrQuantity(resource.MustParse("8Gi")),
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
