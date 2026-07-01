package runtime

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

// buildKVEventSubscriber renders the kvevent-subscriber sidecar the Pod webhook
// appends to a vLLM engine pod so its KV-cache events flow to the policy server
// with no out-of-band bring-up. It is shared by every adapter whose backend is
// a vLLM-engine L2 cache tier — the vLLM+LMCache adapter and the vLLM+Mooncake
// adapter today — because the KV-event stream is produced by vLLM itself (its
// ZMQ publisher), independent of which L2 store the engine offloads to. The
// scheme tag is therefore "vllm" for both, and the --ignore-block-removed
// eviction-forwarding policy is derived from the CR's integration mode (see
// below), so the rendered container is identical across those adapters for a
// given mode and lives here as the single source of truth.
//
// The container shares the engine pod's network namespace, so the subscriber
// dials the engine over 127.0.0.1 (the vLLM ZMQ PUB endpoint defaults to
// :5557); identity flags are derived from cache + pod (--replica-id from
// pod.Name via the downward API, --tenant-id from pod.Namespace ditto,
// --model-id from cache.Spec.BackendConfig["model"], --hash-scheme fixed to
// "vllm") so the CR is the single source of truth.
//
// The flag surface here is deliberately the intersection of what the shipped
// kvevent-subscriber binary accepts: passing flags the binary doesn't know
// would crash the sidecar on startup (Go's flag package rejects unknown flags).
// Stats-path flags (--engine-metrics-url, --stats-interval, etc.) are added
// when the binary itself learns to scrape and emit ReplicaStats.
//
// Returns (nil, nil) when subscriberImage is empty (auto-attach is opt-in —
// a nonexistent default image would put the sidecar into ImagePullBackOff and
// keep the engine pod from going Ready, turning the cache into a serving
// dependency the fail-open posture exists to avoid) or when the served model
// id is not derivable from the CR (the subscriber's --model-id flag is
// required, so emitting a container that would CrashLoopBackOff is worse than
// skipping; the webhook logs the skip and the next admission picks it up once
// the operator sets spec.backendConfig.model). policyServerGRPCAddress falls
// back to [DefaultPolicyServerGRPCAddress] when empty.
func buildKVEventSubscriber(subscriberImage, policyServerGRPCAddress string, cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	if cache == nil {
		return nil, fmt.Errorf("observation sidecar: cache is nil")
	}
	if pod == nil {
		return nil, fmt.Errorf("observation sidecar: pod is nil")
	}
	if subscriberImage == "" {
		return nil, nil
	}
	modelID := enginewire.ConfigOr(cache.Spec.BackendConfig, modelBackendConfigKey, "")
	if modelID == "" {
		return nil, nil
	}
	serverAddr := policyServerGRPCAddress
	if serverAddr == "" {
		serverAddr = DefaultPolicyServerGRPCAddress
	}

	// Eviction-forwarding policy is mode-dependent. In Offload mode the paired
	// L2 tier (LMCache, or a Mooncake store) retains a block after the engine
	// evicts it from GPU, so vLLM's BlockRemoved does NOT mean the prefix is
	// gone — forwarding it as PREFIX_EVICTED would drop a routing hint the
	// replica can still cheaply serve from L2, so suppress it
	// (--ignore-block-removed=true) and let the hint age out on its freshness
	// TTL. In EventsOnly mode there is NO L2 retaining blocks, so a BlockRemoved
	// genuinely means the prefix is gone and the hint MUST be pruned — omit the
	// flag (the subscriber binary defaults it to false). Soft state means a
	// stale hint is a cache miss at worst, while a missing one routes the
	// request away from its warm replica — the opposite risk in each mode,
	// hence the opposite default. (Mooncake is always an L2 store, so it takes
	// the Offload branch unless an operator sets EventsOnly, which is a
	// contradiction the admission validator already rejects.)
	args := []string{
		"--engine-endpoint=tcp://127.0.0.1:" + defaultEngineZMQPortStr,
		"--server=" + serverAddr,
		"--replica-id=$(POD_NAME)",
		"--tenant-id=$(POD_NAMESPACE)",
		"--model-id=" + modelID,
		"--hash-scheme=" + subscriberHashScheme,
	}
	if !cache.Spec.IsEventsOnly() {
		args = append(args, "--ignore-block-removed=true")
	}

	nonRoot := true
	noPrivEsc := false
	readOnlyRoot := true
	uid := int64(65532)
	return &corev1.Container{
		Name:            SubscriberContainerName,
		Image:           subscriberImage,
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
		Args: args,
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
