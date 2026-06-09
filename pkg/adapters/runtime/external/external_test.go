package external_test

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/external"
)

// externalBackend returns an External CacheBackend with a vLLM engine
// integration set. Used by every test that drives the adapter directly.
func externalBackend(endpoint string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "ext-cache", Namespace: "default"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: endpoint,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
		},
	}
}

func TestSupports_AcceptsVLLMExternal(t *testing.T) {
	a := external.NewAdapter()
	cb := externalBackend("lm://cache.example:8200")
	if !a.Supports(runtimeadapter.RuntimeVLLM, cb) {
		t.Fatalf("External adapter must support (vllm, External); got false")
	}
}

func TestSupports_RejectsManagedTypes(t *testing.T) {
	a := external.NewAdapter()
	for _, bt := range []cachev1alpha1.CacheBackendType{
		cachev1alpha1.CacheBackendTypeLMCache,
		cachev1alpha1.CacheBackendTypeMooncake,
		cachev1alpha1.CacheBackendTypeAIBrix,
		cachev1alpha1.CacheBackendTypeNIXL,
		cachev1alpha1.CacheBackendTypeSGLangHiCache,
	} {
		cb := externalBackend("lm://x:1")
		cb.Spec.Type = bt
		if a.Supports(runtimeadapter.RuntimeVLLM, cb) {
			t.Fatalf("External adapter must NOT support backend type %q", bt)
		}
	}
}

func TestSupports_RejectsNonVLLMRuntime(t *testing.T) {
	a := external.NewAdapter()
	cb := externalBackend("lm://x:1")
	if a.Supports(runtimeadapter.RuntimeSGLang, cb) {
		t.Fatalf("External adapter must NOT support SGLang yet (engine wire is LMCache-shaped today)")
	}
}

func TestSupports_NilCache(t *testing.T) {
	a := external.NewAdapter()
	if a.Supports(runtimeadapter.RuntimeVLLM, nil) {
		t.Fatalf("Supports must be false for nil cache")
	}
}

func TestResolveCacheServer_ReturnsNil(t *testing.T) {
	a := external.NewAdapter()
	cb := externalBackend("lm://x:1")
	resolved, err := a.ResolveCacheServer(cb)
	if err != nil {
		t.Fatalf("ResolveCacheServer must not error for a valid External CR: %v", err)
	}
	if resolved != nil {
		t.Fatalf("ResolveCacheServer must return nil for External (no controller-managed cache-server); got %+v", resolved)
	}
}

func TestResolveCacheServer_NilCacheErrors(t *testing.T) {
	a := external.NewAdapter()
	if _, err := a.ResolveCacheServer(nil); err == nil {
		t.Fatalf("ResolveCacheServer must error on nil cache")
	}
}

func TestInjectEngineConfig_MatchesLMCacheWire(t *testing.T) {
	// The External adapter must produce a byte-identical engine wire to the
	// managed vLLM+LMCache adapter when both are pointed at the same
	// endpoint and integration spec — that's the load-bearing invariant
	// (the engine cannot tell the cache was operator-managed).
	endpoint := "external-cache.example:8200"
	cbExt := externalBackend(endpoint)

	cbLM := cbExt.DeepCopy()
	cbLM.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	cbLM.Spec.Endpoint = ""

	pod := singleEnginePod()
	if err := external.NewAdapter().InjectEngineConfig(&pod.Spec, endpoint, cbExt); err != nil {
		t.Fatalf("External.InjectEngineConfig: %v", err)
	}

	// Build the same pod via the managed adapter to compare.
	lmReg := runtimeadapter.DefaultRegistry()
	lmAdapter, err := lmReg.Select(runtimeadapter.RuntimeVLLM, cbLM)
	if err != nil {
		t.Fatalf("DefaultRegistry must select an adapter for vllm/LMCache: %v", err)
	}
	lmPod := singleEnginePod()
	if err := lmAdapter.InjectEngineConfig(&lmPod.Spec, endpoint, cbLM); err != nil {
		t.Fatalf("LMCache.InjectEngineConfig: %v", err)
	}

	// Same env shape + values.
	if !sameEnv(pod.Spec.Containers[0].Env, lmPod.Spec.Containers[0].Env) {
		t.Fatalf("External engine env diverges from LMCache:\n  external = %v\n  lmcache  = %v",
			pod.Spec.Containers[0].Env, lmPod.Spec.Containers[0].Env)
	}
	// Same args ordering.
	if !equalStrings(pod.Spec.Containers[0].Args, lmPod.Spec.Containers[0].Args) {
		t.Fatalf("External engine args diverge from LMCache:\n  external = %v\n  lmcache  = %v",
			pod.Spec.Containers[0].Args, lmPod.Spec.Containers[0].Args)
	}

	// Spot-check the operator-supplied endpoint is the one wired (not a
	// controller-resolved Service DNS).
	if got := envValue(pod.Spec.Containers[0].Env, runtimeadapter.EnvLMCacheRemoteURL); got != "lm://"+endpoint {
		t.Fatalf("LMCACHE_REMOTE_URL = %q, want %q", got, "lm://"+endpoint)
	}
}

func TestInjectEngineConfig_IdempotentOnRepeatCall(t *testing.T) {
	a := external.NewAdapter()
	cb := externalBackend("lm://idem:1")
	pod := singleEnginePod()
	for i := 0; i < 3; i++ {
		if err := a.InjectEngineConfig(&pod.Spec, "lm://idem:1", cb); err != nil {
			t.Fatalf("InjectEngineConfig pass %d: %v", i, err)
		}
	}
	// One LMCACHE_REMOTE_URL entry, not three.
	count := 0
	for _, e := range pod.Spec.Containers[0].Env {
		if e.Name == runtimeadapter.EnvLMCacheRemoteURL {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("LMCACHE_REMOTE_URL count = %d after 3 injections, want 1", count)
	}
	// One --kv-transfer-config pair.
	flagCount := 0
	for _, a := range pod.Spec.Containers[0].Args {
		if a == "--kv-transfer-config" {
			flagCount++
		}
	}
	if flagCount != 1 {
		t.Fatalf("--kv-transfer-config count = %d after 3 injections, want 1", flagCount)
	}
}

func TestInjectEngineConfig_EmptyEndpointErrors(t *testing.T) {
	a := external.NewAdapter()
	pod := singleEnginePod()
	cb := externalBackend("")
	if err := a.InjectEngineConfig(&pod.Spec, "", cb); err == nil {
		t.Fatalf("InjectEngineConfig must error on empty endpoint")
	} else if !strings.Contains(err.Error(), "endpoint is empty") {
		t.Fatalf("error message must name endpoint: %v", err)
	}
}

func TestInjectRouterConfig_NoOp(t *testing.T) {
	a := external.NewAdapter()
	pod := singleEnginePod()
	before := deepCopyContainers(pod.Spec.Containers)
	cb := externalBackend("lm://x:1")
	if err := a.InjectRouterConfig(&pod.Spec, "lm://x:1", cb); err != nil {
		t.Fatalf("InjectRouterConfig must be a no-op: %v", err)
	}
	if !sameContainers(before, pod.Spec.Containers) {
		t.Fatalf("InjectRouterConfig mutated the pod:\n  before = %v\n  after  = %v", before, pod.Spec.Containers)
	}
}

func TestObservationSidecar_ReturnsNil(t *testing.T) {
	a := external.NewAdapter()
	cb := externalBackend("lm://x:1")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"}}
	sidecar, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar must not error: %v", err)
	}
	if sidecar != nil {
		t.Fatalf("ObservationSidecar must return nil for External; got %+v", sidecar)
	}
}

func TestSupportedPairs_ListsExternal(t *testing.T) {
	a := external.NewAdapter()
	lister, ok := a.(runtimeadapter.PairLister)
	if !ok {
		t.Fatalf("External adapter must implement PairLister so admission error messages list it")
	}
	pairs := lister.SupportedPairs()
	if len(pairs) != 1 {
		t.Fatalf("SupportedPairs count = %d, want 1: %v", len(pairs), pairs)
	}
	if pairs[0].Runtime != runtimeadapter.RuntimeVLLM || pairs[0].Backend != cachev1alpha1.CacheBackendTypeExternal {
		t.Fatalf("SupportedPairs[0] = %+v, want {vllm, External}", pairs[0])
	}
}

// singleEnginePod returns a fresh single-container engine pod with the
// canonical container name, an existing user --model arg, and a user-set
// PRESERVE env entry. The injection contract must never drop or alter
// either of these.
func singleEnginePod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine", Namespace: "default"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  runtimeadapter.EngineContainerName,
				Image: "vllm/vllm-openai:dev",
				Args:  []string{"--model", "Qwen/Qwen2.5-0.5B-Instruct"},
				Env:   []corev1.EnvVar{{Name: "PRESERVE", Value: "yes"}},
			}},
		},
	}
}

func sameEnv(a, b []corev1.EnvVar) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func deepCopyContainers(in []corev1.Container) []corev1.Container {
	out := make([]corev1.Container, len(in))
	for i := range in {
		out[i] = *in[i].DeepCopy()
	}
	return out
}

func sameContainers(a, b []corev1.Container) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Image != b[i].Image {
			return false
		}
		if !equalStrings(a[i].Args, b[i].Args) {
			return false
		}
		if !sameEnv(a[i].Env, b[i].Env) {
			return false
		}
	}
	return true
}
