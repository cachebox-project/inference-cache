package runtime

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func newLMCacheBackend(cfg map[string]string) *cachev1alpha1.CacheBackend {
	cb := newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "vllm")
	cb.Spec.BackendConfig = cfg
	return cb
}

func TestVLLMLMCacheSupports(t *testing.T) {
	a := NewVLLMLMCacheAdapter()

	cases := []struct {
		name    string
		runtime RuntimeID
		cache   *cachev1alpha1.CacheBackend
		want    bool
	}{
		{"vllm+lmcache", RuntimeVLLM, newLMCacheBackend(nil), true},
		{"vllm+external", RuntimeVLLM, newCacheBackend(cachev1alpha1.CacheBackendTypeExternal, "vllm"), false},
		{"vllm+mooncake", RuntimeVLLM, newCacheBackend(cachev1alpha1.CacheBackendTypeMooncake, "vllm"), false},
		{"sglang+lmcache", RuntimeSGLang, newLMCacheBackend(nil), false},
		{"reference+lmcache", RuntimeReference, newLMCacheBackend(nil), false},
		{"nil cache", RuntimeVLLM, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.Supports(tc.runtime, tc.cache); got != tc.want {
				t.Fatalf("Supports(%q, %+v) = %v, want %v", tc.runtime, tc.cache, got, tc.want)
			}
		})
	}
}

func TestVLLMLMCacheResolveCacheServer(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)

	pod, svc, err := a.ResolveCacheServer(cb)
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	if pod == nil || svc == nil {
		t.Fatalf("ResolveCacheServer returned nil pod or svc")
	}

	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(pod.Containers))
	}
	c := pod.Containers[0]
	if c.Name != "lmcache-server" {
		t.Fatalf("container name = %q, want lmcache-server", c.Name)
	}
	if c.Image != "lmcache/standalone:latest" {
		t.Fatalf("container image = %q, want lmcache/standalone:latest default", c.Image)
	}
	if len(c.Command) != 1 || c.Command[0] != "lmcache_server" {
		t.Fatalf("command = %v, want [lmcache_server]", c.Command)
	}
	wantArgs := []string{"0.0.0.0", "65432", "cpu"}
	if len(c.Args) != len(wantArgs) {
		t.Fatalf("args = %v, want %v", c.Args, wantArgs)
	}
	for i, want := range wantArgs {
		if c.Args[i] != want {
			t.Fatalf("args[%d] = %q, want %q", i, c.Args[i], want)
		}
	}
	if len(c.Ports) != 1 || c.Ports[0].ContainerPort != 65432 {
		t.Fatalf("ports = %v, want a single 65432 port", c.Ports)
	}

	// Service spec: adapter fills Type + Ports only — ObjectMeta and Selector
	// are the reconciler's responsibility (see KVCacheRuntimeAdapter docs).
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("svc.Spec.Type = %q, want ClusterIP", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 65432 {
		t.Fatalf("svc.Spec.Ports = %v, want a single 65432 port", svc.Spec.Ports)
	}
	if svc.Spec.Selector != nil {
		t.Fatalf("svc.Spec.Selector = %v, want nil (reconciler owns the selector)", svc.Spec.Selector)
	}
	if svc.Name != "" || svc.Namespace != "" {
		t.Fatalf("svc ObjectMeta = %q/%q, want empty (reconciler owns ObjectMeta)", svc.Namespace, svc.Name)
	}
}

func TestVLLMLMCacheResolveCacheServerImageOverride(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(map[string]string{"image": "registry.example.com/lmcache:pinned"})

	pod, _, err := a.ResolveCacheServer(cb)
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	if got := pod.Containers[0].Image; got != "registry.example.com/lmcache:pinned" {
		t.Fatalf("container image = %q, want overridden", got)
	}
}

func TestVLLMLMCacheResolveCacheServerCommandOverride(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(map[string]string{
		"serverCommand": "python3 -m lmcache.v1.multiprocess.server --cpu-buffer-size 60",
	})

	pod, _, err := a.ResolveCacheServer(cb)
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	c := pod.Containers[0]
	if len(c.Command) != 1 || c.Command[0] != "python3" {
		t.Fatalf("command = %v, want [python3]", c.Command)
	}
	wantArgs := []string{"-m", "lmcache.v1.multiprocess.server", "--cpu-buffer-size", "60"}
	if len(c.Args) != len(wantArgs) {
		t.Fatalf("args = %v, want %v", c.Args, wantArgs)
	}
	for i, want := range wantArgs {
		if c.Args[i] != want {
			t.Fatalf("args[%d] = %q, want %q", i, c.Args[i], want)
		}
	}
}

func TestVLLMLMCacheResolveCacheServerNilCache(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	if _, _, err := a.ResolveCacheServer(nil); err == nil {
		t.Fatalf("ResolveCacheServer(nil) returned no error")
	}
}

func TestVLLMLMCacheInjectEngineConfig(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: EngineContainerName,
				Args: []string{"--enable-prefix-caching", "--max-model-len", "8192"},
				Env: []corev1.EnvVar{
					{Name: "HF_TOKEN", Value: "secret-token"},
				},
			},
			{
				Name: "sidecar",
				Env:  []corev1.EnvVar{{Name: "SIDECAR_VAR", Value: "untouched"}},
			},
		},
	}

	if err := a.InjectEngineConfig(pod, "cache.ns1.svc.cluster.local:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}

	engine := pod.Containers[0]
	url, ok := lookupEnv(engine.Env, EnvLMCacheRemoteURL)
	if !ok || url != "lm://cache.ns1.svc.cluster.local:65432" {
		t.Fatalf("%s = (%q, %v), want lm://cache.ns1.svc.cluster.local:65432", EnvLMCacheRemoteURL, url, ok)
	}
	if v, _ := lookupEnv(engine.Env, EnvLMCacheRemoteSerde); v != "naive" {
		t.Fatalf("%s = %q, want naive (CPU-safe default)", EnvLMCacheRemoteSerde, v)
	}
	if v, _ := lookupEnv(engine.Env, EnvLMCacheChunkSize); v != "256" {
		t.Fatalf("%s = %q, want 256", EnvLMCacheChunkSize, v)
	}
	if v, _ := lookupEnv(engine.Env, EnvLMCacheLocalCPU); v != "False" {
		t.Fatalf("%s = %q, want False (remote-only by default)", EnvLMCacheLocalCPU, v)
	}
	if v, _ := lookupEnv(engine.Env, EnvVLLMUseV1); v != "1" {
		t.Fatalf("%s = %q, want 1", EnvVLLMUseV1, v)
	}
	// Existing env on the engine container is preserved.
	if v, _ := lookupEnv(engine.Env, "HF_TOKEN"); v != "secret-token" {
		t.Fatalf("HF_TOKEN was clobbered: got %q, want secret-token", v)
	}
	// Existing args are preserved + the connector arg pair is appended.
	if !containsArg(engine.Args, "--enable-prefix-caching") {
		t.Fatalf("--enable-prefix-caching was dropped: %v", engine.Args)
	}
	if !containsArgPair(engine.Args, defaultEngineKVTransferConfigArg, defaultEngineKVTransferConfig) {
		t.Fatalf("connector args missing: %v", engine.Args)
	}

	// Sidecar is not the engine container, so it should be untouched.
	sidecar := pod.Containers[1]
	if _, ok := lookupEnv(sidecar.Env, EnvLMCacheRemoteURL); ok {
		t.Fatalf("sidecar got LMCache env injected; the adapter should target only the engine container")
	}
	if v, _ := lookupEnv(sidecar.Env, "SIDECAR_VAR"); v != "untouched" {
		t.Fatalf("SIDECAR_VAR was clobbered: got %q", v)
	}
}

func TestVLLMLMCacheInjectEngineConfigFallbackToAllContainersWhenNoEngineName(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	// No container named "vllm" — adapter falls back to mutating every container
	// so a non-standard pod template still gets wired (documented contract).
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine"}, {Name: "sidecar"}}}

	if err := a.InjectEngineConfig(pod, "cache.ns1.svc.cluster.local:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	for _, c := range pod.Containers {
		if _, ok := lookupEnv(c.Env, EnvLMCacheRemoteURL); !ok {
			t.Fatalf("container %q missing %s; fallback should target every container", c.Name, EnvLMCacheRemoteURL)
		}
	}
}

func TestVLLMLMCacheInjectEngineConfigIdempotent(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}

	if err := a.InjectEngineConfig(pod, "first.svc:65432", cb); err != nil {
		t.Fatalf("first InjectEngineConfig: %v", err)
	}
	if err := a.InjectEngineConfig(pod, "second.svc:65432", cb); err != nil {
		t.Fatalf("second InjectEngineConfig: %v", err)
	}

	envs := pod.Containers[0].Env
	urlMatches := 0
	for _, e := range envs {
		if e.Name == EnvLMCacheRemoteURL {
			urlMatches++
			if e.Value != "lm://second.svc:65432" {
				t.Fatalf("idempotent inject did not update value: got %q", e.Value)
			}
		}
	}
	if urlMatches != 1 {
		t.Fatalf("expected exactly 1 %s entry after second inject, got %d", EnvLMCacheRemoteURL, urlMatches)
	}

	// Args: the connector arg pair must appear exactly once.
	flagCount := 0
	valueCount := 0
	for _, a := range pod.Containers[0].Args {
		if a == defaultEngineKVTransferConfigArg {
			flagCount++
		}
		if a == defaultEngineKVTransferConfig {
			valueCount++
		}
	}
	if flagCount != 1 || valueCount != 1 {
		t.Fatalf("connector arg pair count = (flag %d, value %d), want (1, 1)", flagCount, valueCount)
	}
}

func TestVLLMLMCacheInjectEngineConfigConfigOverrides(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(map[string]string{
		"chunkSize":   "512",
		"remoteSerde": "cachegen",
		"localCPU":    "True",
		"maxLocalCPU": "40",
	})
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
	if err := a.InjectEngineConfig(pod, "x.svc:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	checks := map[string]string{
		EnvLMCacheChunkSize:   "512",
		EnvLMCacheRemoteSerde: "cachegen",
		EnvLMCacheLocalCPU:    "True",
		EnvLMCacheMaxLocalCPU: "40",
	}
	for name, want := range checks {
		if v, _ := lookupEnv(pod.Containers[0].Env, name); v != want {
			t.Fatalf("%s = %q, want %q (BackendConfig override)", name, v, want)
		}
	}
}

func TestVLLMLMCacheInjectEngineConfigPassesThroughLMScheme(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
	// A caller that already prefixed lm:// must not produce lm://lm://.
	if err := a.InjectEngineConfig(pod, "lm://already.scheme:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	url, _ := lookupEnv(pod.Containers[0].Env, EnvLMCacheRemoteURL)
	if url != "lm://already.scheme:65432" {
		t.Fatalf("%s = %q, want pass-through (no double prefix)", EnvLMCacheRemoteURL, url)
	}
	if strings.HasPrefix(url, "lm://lm://") {
		t.Fatalf("double lm:// prefix in %q", url)
	}
}

func TestVLLMLMCacheInjectEngineConfigBadInput(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	good := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
	cases := []struct {
		name string
		fn   func() error
	}{
		{"nil pod", func() error { return a.InjectEngineConfig(nil, "x.svc:65432", cb) }},
		{"nil cache", func() error { return a.InjectEngineConfig(good, "x.svc:65432", nil) }},
		{"empty endpoint", func() error { return a.InjectEngineConfig(good, "", cb) }},
		{"no containers", func() error { return a.InjectEngineConfig(&corev1.PodSpec{}, "x.svc:65432", cb) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestVLLMLMCacheInjectRouterConfigIsNoop(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "router", Env: []corev1.EnvVar{{Name: "EXISTING", Value: "x"}}}}}
	if err := a.InjectRouterConfig(pod, "x.svc:65432", cb); err != nil {
		t.Fatalf("InjectRouterConfig: %v", err)
	}
	// LMCache has no router; the pod must come back untouched (existing env kept,
	// no LMCache env added).
	if len(pod.Containers[0].Env) != 1 || pod.Containers[0].Env[0].Name != "EXISTING" {
		t.Fatalf("InjectRouterConfig modified container env: %v", pod.Containers[0].Env)
	}
}

func TestVLLMLMCacheInjectRouterConfigStillValidatesInputs(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	good := &corev1.PodSpec{Containers: []corev1.Container{{Name: "router"}}}
	cases := []struct {
		name string
		fn   func() error
	}{
		{"nil pod", func() error { return a.InjectRouterConfig(nil, "x", cb) }},
		{"nil cache", func() error { return a.InjectRouterConfig(good, "x", nil) }},
		{"empty endpoint", func() error { return a.InjectRouterConfig(good, "", cb) }},
		{"no containers", func() error { return a.InjectRouterConfig(&corev1.PodSpec{}, "x", cb) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestDefaultRegistryResolvesVLLMLMCache(t *testing.T) {
	r := DefaultRegistry()
	if r.Len() == 0 {
		t.Fatalf("DefaultRegistry has no adapters")
	}
	got, err := r.Select(RuntimeVLLM, newLMCacheBackend(nil))
	if err != nil {
		t.Fatalf("Select(vllm, LMCache): %v", err)
	}
	if _, ok := got.(vllmLMCacheAdapter); !ok {
		t.Fatalf("Select returned %T, want vllmLMCacheAdapter", got)
	}
}

func TestUpsertArgPairAppendsAndReplaces(t *testing.T) {
	// Append when missing.
	got := upsertArgPair([]string{"--keep"}, "--flag", "v1")
	want := []string{"--keep", "--flag", "v1"}
	if !equalStrSlice(got, want) {
		t.Fatalf("upsertArgPair append = %v, want %v", got, want)
	}
	// Replace value when flag already present.
	got = upsertArgPair([]string{"--flag", "old", "--other"}, "--flag", "new")
	want = []string{"--flag", "new", "--other"}
	if !equalStrSlice(got, want) {
		t.Fatalf("upsertArgPair replace = %v, want %v", got, want)
	}
	// Trailing flag with no value: append the value.
	got = upsertArgPair([]string{"--flag"}, "--flag", "v")
	want = []string{"--flag", "v"}
	if !equalStrSlice(got, want) {
		t.Fatalf("upsertArgPair trailing-flag = %v, want %v", got, want)
	}
}

func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func containsArgPair(args []string, flag, value string) bool {
	for i, a := range args {
		if a == flag && i+1 < len(args) && args[i+1] == value {
			return true
		}
	}
	return false
}

func equalStrSlice(a, b []string) bool {
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
