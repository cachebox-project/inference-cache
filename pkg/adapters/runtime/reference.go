package runtime

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// RuntimeReference is the [RuntimeID] the in-tree reference adapter matches.
// It is deliberately not "vllm" or "sglang" — those names are reserved for
// the real production adapters, so unit tests for the registry can register
// the reference without colliding with production adapter names.
const RuntimeReference RuntimeID = "reference"

// EnvCacheEndpoint is the environment variable the reference adapter writes
// to every container in an engine pod, set to the endpoint argument of
// InjectEngineConfig. It is exported so tests in this package and downstream
// callers (admission validation, future adapter authors taking the reference
// as a template) can assert on it.
const EnvCacheEndpoint = "INFERENCECACHE_CACHE_ENDPOINT"

// EnvRouterEndpoint is the env var the reference adapter writes to router
// pods, distinct from [EnvCacheEndpoint] so a single pod hosting both roles
// (uncommon, but legal) keeps the two signals separable.
const EnvRouterEndpoint = "INFERENCECACHE_ROUTER_ENDPOINT"

// referenceAdapter is the in-tree, dependency-free adapter that exercises the
// [KVCacheRuntimeAdapter] contract end to end without binding to any real
// engine. It accepts every CacheBackend type for runtime [RuntimeReference],
// renders no cache-server (the wiring is endpoint-only), and demonstrates the
// idempotent merge contract that real adapters (vLLM+LMCache today, future
// SGLang HiCache) must honour.
type referenceAdapter struct{}

// NewReferenceAdapter returns a [KVCacheRuntimeAdapter] suitable for
// exercising the [Registry] in tests and as a worked example of the merge
// contract.
func NewReferenceAdapter() KVCacheRuntimeAdapter {
	return referenceAdapter{}
}

// Supports matches the reference runtime regardless of backend type. Real
// adapters narrow on cache.Spec.Type (and, for some, cache.Spec.Integration).
func (referenceAdapter) Supports(runtime RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	if cache == nil {
		return false
	}
	return runtime == RuntimeReference
}

// ResolveCacheServer renders no cache-server: the reference adapter wires
// engine/router pods directly to whatever endpoint the reconciler discovered,
// matching backends (such as LMCache) that colocate the cache with the engine.
func (referenceAdapter) ResolveCacheServer(*cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return nil, nil, nil
}

// InjectEngineConfig sets [EnvCacheEndpoint] on every container in pod,
// merging with existing env entries: an existing entry with the same name is
// updated in place (no duplicates), and unrelated entries are left untouched.
// A nil or container-less pod is reported as an error so callers notice
// caller-side bugs instead of silently producing a no-op.
func (referenceAdapter) InjectEngineConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	return injectEndpointEnv(pod, endpoint, cache, EnvCacheEndpoint, "engine")
}

// InjectRouterConfig sets [EnvRouterEndpoint] on every container in pod with
// the same merge semantics as [referenceAdapter.InjectEngineConfig].
func (referenceAdapter) InjectRouterConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	return injectEndpointEnv(pod, endpoint, cache, EnvRouterEndpoint, "router")
}

// injectEndpointEnv is the shared implementation behind the reference
// adapter's two inject paths. It is the worked example future adapters
// should mirror: validate inputs, locate the role-specific containers, and
// upsert (never blindly append) the env var that names the cache endpoint.
func injectEndpointEnv(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend, envName, role string) error {
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
	for i := range pod.Containers {
		pod.Containers[i].Env = upsertEnv(pod.Containers[i].Env, corev1.EnvVar{Name: envName, Value: endpoint})
	}
	return nil
}

// upsertEnv returns env with name set to value: updates in place if the entry
// exists, appends otherwise. Used by the reference adapter so a second call
// to Inject*Config never produces duplicate env entries and never disturbs
// unrelated ones — the same property real adapters must preserve.
func upsertEnv(env []corev1.EnvVar, want corev1.EnvVar) []corev1.EnvVar {
	for i := range env {
		if env[i].Name == want.Name {
			env[i].Value = want.Value
			env[i].ValueFrom = want.ValueFrom
			return env
		}
	}
	return append(env, want)
}
