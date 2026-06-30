package sglang

import (
	"flag"
	"io"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

func newSGLangBackend(cfg map[string]string) *cachev1alpha1.CacheBackend {
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:          cachev1alpha1.CacheBackendTypeLMCache,
			Integration:   &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "sglang"},
			BackendConfig: cfg,
		},
	}
	return cb
}

func TestSGLangSupports(t *testing.T) {
	a := NewAdapter()
	cases := []struct {
		name    string
		runtime runtimeadapter.RuntimeID
		cache   *cachev1alpha1.CacheBackend
		want    bool
	}{
		{"sglang+lmcache", runtimeadapter.RuntimeSGLang, newSGLangBackend(nil), true},
		{"vllm+lmcache", runtimeadapter.RuntimeVLLM, newSGLangBackend(nil), false},
		{"sglang+external", runtimeadapter.RuntimeSGLang, &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeExternal}}, false},
		{"sglang+mooncake", runtimeadapter.RuntimeSGLang, &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeMooncake}}, false},
		{"nil cache", runtimeadapter.RuntimeSGLang, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.Supports(tc.runtime, tc.cache); got != tc.want {
				t.Fatalf("Supports(%q, %+v) = %v, want %v", tc.runtime, tc.cache, got, tc.want)
			}
		})
	}
}

func TestSGLangSupportedPairs(t *testing.T) {
	a := NewAdapter().(interface {
		SupportedPairs() []runtimeadapter.SupportedPair
	})
	got := a.SupportedPairs()
	want := []runtimeadapter.SupportedPair{{Runtime: runtimeadapter.RuntimeSGLang, Backend: cachev1alpha1.CacheBackendTypeLMCache}}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("SupportedPairs = %v, want %v", got, want)
	}
}

func TestSGLangResolveCacheServer(t *testing.T) {
	a := NewAdapter()
	pod, svc, err := a.ResolveCacheServer(newSGLangBackend(nil))
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	if pod == nil || svc == nil {
		t.Fatalf("ResolveCacheServer returned nil pod or svc")
	}
	// The lmcache-server is engine-agnostic and shared with the vLLM adapter
	// (the exhaustive resource/probe edge-cases live in the runtime package's
	// vllm_lmcache_test.go); here we pin that SGLang renders the same standalone
	// server so a (sglang, LMCache) backend provisions a real cache server.
	if len(pod.Containers) != 1 || pod.Containers[0].Name != "lmcache-server" {
		t.Fatalf("containers = %+v, want a single lmcache-server", pod.Containers)
	}
	if pod.Containers[0].Image != "lmcache/standalone:v0.4.7" {
		t.Fatalf("image = %q, want lmcache/standalone:v0.4.7 default", pod.Containers[0].Image)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 65432 {
		t.Fatalf("svc ports = %v, want a single 65432 port", svc.Spec.Ports)
	}
}

func TestSGLangResolveCacheServerImageOverride(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(map[string]string{"serverImage": "registry.example.com/lmcache:pinned"})
	pod, _, err := a.ResolveCacheServer(cb)
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	if got := pod.Containers[0].Image; got != "registry.example.com/lmcache:pinned" {
		t.Fatalf("image = %q, want overridden", got)
	}
}

func TestSGLangResolveCacheServerNilCache(t *testing.T) {
	if _, _, err := NewAdapter().ResolveCacheServer(nil); err == nil {
		t.Fatalf("ResolveCacheServer(nil) returned no error")
	}
}

func TestSGLangInjectEngineConfig(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: enginewire.SGLangEngineContainerName,
				Args: []string{"--page-size", "64"},
				Env:  []corev1.EnvVar{{Name: "HF_TOKEN", Value: "secret-token"}},
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
	if url, ok := lookupEnv(engine.Env, enginewire.EnvLMCacheRemoteURL); !ok || url != "lm://cache.ns1.svc.cluster.local:65432" {
		t.Fatalf("%s = (%q, %v), want lm://cache.ns1.svc.cluster.local:65432", enginewire.EnvLMCacheRemoteURL, url, ok)
	}
	if v, ok := lookupEnv(engine.Env, enginewire.EnvLMCacheUseExperimental); !ok || v != "True" {
		t.Fatalf("%s = (%q, %v), want True", enginewire.EnvLMCacheUseExperimental, v, ok)
	}
	if v, _ := lookupEnv(engine.Env, enginewire.EnvLMCacheRemoteSerde); v != "naive" {
		t.Fatalf("%s = %q, want naive (CPU-safe default)", enginewire.EnvLMCacheRemoteSerde, v)
	}
	if v, _ := lookupEnv(engine.Env, enginewire.EnvLMCacheChunkSize); v != "256" {
		t.Fatalf("%s = %q, want 256", enginewire.EnvLMCacheChunkSize, v)
	}
	if v, _ := lookupEnv(engine.Env, enginewire.EnvInferenceCacheFailOpen); v == "" {
		t.Fatalf("%s missing", enginewire.EnvInferenceCacheFailOpen)
	}

	// Critical SGLang-vs-vLLM difference: VLLM_USE_V1 and PYTHONHASHSEED are
	// vLLM-only and MUST NOT be injected for SGLang (no v1 codepath; SGLang's
	// sha256-based prefix hashing does not depend on PYTHONHASHSEED). Injecting
	// them would be cargo-culted noise at best and confusing at worst.
	if _, ok := lookupEnv(engine.Env, enginewire.EnvVLLMUseV1); ok {
		t.Fatalf("%s was injected for SGLang; it is vLLM-only", enginewire.EnvVLLMUseV1)
	}
	if _, ok := lookupEnv(engine.Env, enginewire.EnvPythonHashSeed); ok {
		t.Fatalf("%s was injected for SGLang; it is vLLM-only", enginewire.EnvPythonHashSeed)
	}

	// --enable-lmcache (bare flag, not a --kv-transfer-config pair) is appended.
	if !containsArg(engine.Args, enginewire.SGLangEnableLMCacheArg) {
		t.Fatalf("engine args missing %s: %v", enginewire.SGLangEnableLMCacheArg, engine.Args)
	}
	// SGLang must NOT get vLLM's connector arg.
	if containsArg(engine.Args, "--kv-transfer-config") {
		t.Fatalf("--kv-transfer-config (vLLM-only) injected for SGLang: %v", engine.Args)
	}
	// Existing args/env preserved.
	if !containsArg(engine.Args, "--page-size") {
		t.Fatalf("--page-size was dropped: %v", engine.Args)
	}
	if v, _ := lookupEnv(engine.Env, "HF_TOKEN"); v != "secret-token" {
		t.Fatalf("HF_TOKEN clobbered: got %q", v)
	}
	// Sidecar untouched.
	if _, ok := lookupEnv(pod.Containers[1].Env, enginewire.EnvLMCacheRemoteURL); ok {
		t.Fatalf("sidecar got LMCache env injected; adapter should target only the engine container")
	}
}

func TestSGLangInjectEngineConfigSingleContainerPodAcceptsAnyName(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine"}}}
	if err := a.InjectEngineConfig(pod, "cache.ns1.svc:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	if _, ok := lookupEnv(pod.Containers[0].Env, enginewire.EnvLMCacheRemoteURL); !ok {
		t.Fatalf("single-container pod missing %s; should have been treated as the engine", enginewire.EnvLMCacheRemoteURL)
	}
}

func TestSGLangInjectEngineConfigMultiContainerWithoutSGLangNameErrors(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "engine"},
		{Name: "sidecar"},
	}}
	err := a.InjectEngineConfig(pod, "cache.ns1.svc:65432", cb)
	if err == nil {
		t.Fatalf("expected an error for multi-container pod without an sglang-named container")
	}
	for _, c := range pod.Containers {
		if _, ok := lookupEnv(c.Env, enginewire.EnvLMCacheRemoteURL); ok {
			t.Fatalf("container %q got env injected before the error: %v", c.Name, c.Env)
		}
	}
}

func TestSGLangInjectEngineConfigIdempotent(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName}}}
	if err := a.InjectEngineConfig(pod, "first.svc:65432", cb); err != nil {
		t.Fatalf("first InjectEngineConfig: %v", err)
	}
	if err := a.InjectEngineConfig(pod, "second.svc:65432", cb); err != nil {
		t.Fatalf("second InjectEngineConfig: %v", err)
	}
	// URL updated in place, exactly one entry.
	urls := 0
	for _, e := range pod.Containers[0].Env {
		if e.Name == enginewire.EnvLMCacheRemoteURL {
			urls++
			if e.Value != "lm://second.svc:65432" {
				t.Fatalf("idempotent inject did not update value: got %q", e.Value)
			}
		}
	}
	if urls != 1 {
		t.Fatalf("expected exactly 1 %s entry, got %d", enginewire.EnvLMCacheRemoteURL, urls)
	}
	// --enable-lmcache appears exactly once (no duplicate on re-inject).
	flags := 0
	for _, arg := range pod.Containers[0].Args {
		if arg == enginewire.SGLangEnableLMCacheArg {
			flags++
		}
	}
	if flags != 1 {
		t.Fatalf("%s count = %d, want 1", enginewire.SGLangEnableLMCacheArg, flags)
	}
}

func TestSGLangInjectEngineConfigConfigOverrides(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(map[string]string{
		"chunkSize":   "512",
		"remoteSerde": "cachegen",
		"localCPU":    "True",
		"maxLocalCPU": "40",
	})
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName}}}
	if err := a.InjectEngineConfig(pod, "x.svc:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	checks := map[string]string{
		enginewire.EnvLMCacheChunkSize:   "512",
		enginewire.EnvLMCacheRemoteSerde: "cachegen",
		enginewire.EnvLMCacheLocalCPU:    "True",
		enginewire.EnvLMCacheMaxLocalCPU: "40",
	}
	for name, want := range checks {
		if v, _ := lookupEnv(pod.Containers[0].Env, name); v != want {
			t.Fatalf("%s = %q, want %q (BackendConfig override)", name, v, want)
		}
	}
}

func TestSGLangInjectEngineConfigFailOpen(t *testing.T) {
	a := NewAdapter()
	trueVal, falseVal := true, false
	cases := []struct {
		name     string
		failOpen *bool
		want     string
	}{
		{"default (unset → true)", nil, "true"},
		{"explicit true", &trueVal, "true"},
		{"explicit false", &falseVal, "false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cb := newSGLangBackend(nil)
			cb.Spec.Integration.FailOpen = tc.failOpen
			pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName}}}
			if err := a.InjectEngineConfig(pod, "x.svc:65432", cb); err != nil {
				t.Fatalf("InjectEngineConfig: %v", err)
			}
			if v, _ := lookupEnv(pod.Containers[0].Env, enginewire.EnvInferenceCacheFailOpen); v != tc.want {
				t.Fatalf("%s = %q, want %q", enginewire.EnvInferenceCacheFailOpen, v, tc.want)
			}
		})
	}
}

func TestSGLangInjectEngineConfigPassesThroughLMScheme(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName}}}
	if err := a.InjectEngineConfig(pod, "lm://already.scheme:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	url, _ := lookupEnv(pod.Containers[0].Env, enginewire.EnvLMCacheRemoteURL)
	if url != "lm://already.scheme:65432" {
		t.Fatalf("%s = %q, want pass-through (no double prefix)", enginewire.EnvLMCacheRemoteURL, url)
	}
}

func TestSGLangInjectEngineConfigBadInput(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	good := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName}}}
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

func TestSGLangInjectRouterConfigIsNoop(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "router", Env: []corev1.EnvVar{{Name: "EXISTING", Value: "x"}}}}}
	if err := a.InjectRouterConfig(pod, "x.svc:65432", cb); err != nil {
		t.Fatalf("InjectRouterConfig: %v", err)
	}
	if len(pod.Containers[0].Env) != 1 || pod.Containers[0].Env[0].Name != "EXISTING" {
		t.Fatalf("InjectRouterConfig modified container env: %v", pod.Containers[0].Env)
	}
	// Truly a no-op even on bad input (router-less backend must never force
	// callers to special-case it).
	if err := a.InjectRouterConfig(nil, "x", cb); err != nil {
		t.Fatalf("InjectRouterConfig(nil pod) = %v, want nil", err)
	}
}

func TestSGLangObservationSidecarShape(t *testing.T) {
	a := NewAdapter(runtimeadapter.WithSubscriberImage(runtimeadapter.DefaultSubscriberImage))
	cb := newSGLangBackend(map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-a", Namespace: "engines"}}

	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c == nil {
		t.Fatalf("ObservationSidecar returned nil for sglang+LMCache with a model + image set")
	}
	if c.Name != runtimeadapter.SubscriberContainerName {
		t.Fatalf("container name = %q, want %q", c.Name, runtimeadapter.SubscriberContainerName)
	}
	if !envHasFieldRef(c.Env, "POD_NAME", "metadata.name") || !envHasFieldRef(c.Env, "POD_NAMESPACE", "metadata.namespace") {
		t.Fatalf("downward-API env missing: %v", c.Env)
	}
	wantArgs := []string{
		"--engine-endpoint=tcp://127.0.0.1:5557",
		"--server=" + runtimeadapter.DefaultPolicyServerGRPCAddress,
		"--replica-id=$(POD_NAME)",
		"--tenant-id=$(POD_NAMESPACE)",
		"--model-id=Qwen/Qwen2.5-0.5B-Instruct",
		// Load-bearing: the index keys on hash_scheme, so the SGLang subscriber
		// MUST tag its reports "sglang" to stay disjoint from vLLM entries.
		"--hash-scheme=sglang",
		// LMCache is an L2 tier behind SGLang, same as vLLM+LMCache — drop
		// BlockRemoved rather than forward it as PREFIX_EVICTED.
		"--ignore-block-removed=true",
	}
	for _, want := range wantArgs {
		if !containsArg(c.Args, want) {
			t.Fatalf("subscriber args missing %q; args = %v", want, c.Args)
		}
	}
	if c.SecurityContext == nil || c.SecurityContext.RunAsNonRoot == nil || !*c.SecurityContext.RunAsNonRoot {
		t.Fatalf("SecurityContext must run non-root; got %+v", c.SecurityContext)
	}
}

func TestSGLangObservationSidecarArgsParseAgainstSubscriberFlagSet(t *testing.T) {
	// The Go flag package exits on unknown flags, so a sidecar arg the
	// kvevent-subscriber binary doesn't recognise crashes the container at
	// startup. Parse the rendered args through a FlagSet mirroring the binary's
	// event-path flag surface and assert they parse cleanly. Keep in sync with
	// cmd/kvevent-subscriber/main.go.
	a := NewAdapter(runtimeadapter.WithSubscriberImage(runtimeadapter.DefaultSubscriberImage))
	cb := newSGLangBackend(map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-a", Namespace: "engines"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil || c == nil {
		t.Fatalf("ObservationSidecar: (%v, %v)", c, err)
	}

	fs := flag.NewFlagSet("kvevent-subscriber", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.String("engine-endpoint", "", "")
	fs.String("topic", "", "")
	fs.String("server", "", "")
	fs.String("replica-id", "", "")
	fs.String("model-id", "", "")
	fs.String("tenant-id", "", "")
	fs.String("hash-scheme", "", "")
	fs.Duration("window", 0, "")
	fs.Bool("ignore-block-removed", false, "")
	if err := fs.Parse(c.Args); err != nil {
		t.Fatalf("rendered sidecar args rejected by subscriber FlagSet: %v\nargs = %v", err, c.Args)
	}
	// Belt-and-suspenders: parse a control case that should fail, so the
	// FlagSet isn't silently accepting unknown flags (rules out a tautology
	// if someone passes the wrong FlagSet mode).
	if err := fs.Parse(append(c.Args, "--definitely-not-a-real-flag=x")); err == nil {
		t.Fatalf("control: FlagSet must reject unknown flag --definitely-not-a-real-flag")
	}
}

func TestSGLangObservationSidecarHonoursOptions(t *testing.T) {
	a := NewAdapter(
		runtimeadapter.WithSubscriberImage("registry.example.com/subscriber:pinned"),
		runtimeadapter.WithPolicyServerGRPCAddress("ic-server.custom-ns.svc.cluster.local:9090"),
	)
	cb := newSGLangBackend(map[string]string{"model": "MyOrg/MyModel"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-z", Namespace: "engines"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil || c == nil {
		t.Fatalf("ObservationSidecar: (%v, %v)", c, err)
	}
	if c.Image != "registry.example.com/subscriber:pinned" {
		t.Fatalf("image override ignored: got %q", c.Image)
	}
	if !containsArg(c.Args, "--server=ic-server.custom-ns.svc.cluster.local:9090") {
		t.Fatalf("server address override ignored; args = %v", c.Args)
	}
}

func TestSGLangObservationSidecarSkipsWithoutModel(t *testing.T) {
	a := NewAdapter(runtimeadapter.WithSubscriberImage(runtimeadapter.DefaultSubscriberImage))
	cb := newSGLangBackend(nil)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-a"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c != nil {
		t.Fatalf("expected nil sidecar when backendConfig.model is unset, got %+v", c)
	}
}

func TestSGLangObservationSidecarSkipsWithoutImage(t *testing.T) {
	a := NewAdapter() // no image configured → auto-attach opt-out
	cb := newSGLangBackend(map[string]string{"model": "MyOrg/MyModel"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-a"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c != nil {
		t.Fatalf("expected nil sidecar when subscriber image is unconfigured, got %+v", c)
	}
}

func TestSGLangObservationSidecarBadInput(t *testing.T) {
	a := NewAdapter(runtimeadapter.WithSubscriberImage(runtimeadapter.DefaultSubscriberImage))
	cb := newSGLangBackend(map[string]string{"model": "m"})
	cases := []struct {
		name string
		cb   *cachev1alpha1.CacheBackend
		pod  *corev1.Pod
	}{
		{"nil cache", nil, &corev1.Pod{}},
		{"nil pod", cb, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := a.ObservationSidecar(tc.cb, tc.pod); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestSGLangReservedArgs(t *testing.T) {
	got := NewAdapter().ReservedArgs()
	if len(got) != 1 || got[0] != enginewire.SGLangEnableLMCacheArg {
		t.Fatalf("ReservedArgs = %v, want [%s]", got, enginewire.SGLangEnableLMCacheArg)
	}
}

func TestSGLangReservedEnv(t *testing.T) {
	got := NewAdapter().ReservedEnv()
	want := []string{
		enginewire.EnvLMCacheRemoteURL,
		enginewire.EnvLMCacheUseExperimental,
		enginewire.EnvInferenceCacheFailOpen,
	}
	if len(got) != len(want) {
		t.Fatalf("ReservedEnv = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ReservedEnv = %v, want %v", got, want)
		}
	}
	// Negative control: vLLM-only env MUST NOT be reserved for SGLang (it is
	// never injected), and the LMCACHE_* tunables stay overridable.
	forbidden := map[string]bool{
		enginewire.EnvVLLMUseV1:          true,
		enginewire.EnvPythonHashSeed:     true,
		enginewire.EnvLMCacheChunkSize:   true,
		enginewire.EnvLMCacheRemoteSerde: true,
	}
	for _, name := range got {
		if forbidden[name] {
			t.Errorf("env %q must NOT be reserved for SGLang", name)
		}
	}
}

func TestSGLangEngineContainerName(t *testing.T) {
	if got := NewAdapter().EngineContainerName(); got != enginewire.SGLangEngineContainerName {
		t.Fatalf("EngineContainerName = %q, want %q", got, enginewire.SGLangEngineContainerName)
	}
}

// --- local test helpers (the runtime package's helpers live in a different
// test package and aren't importable here) ---

func lookupEnv(env []corev1.EnvVar, name string) (string, bool) {
	for _, e := range env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func envHasFieldRef(env []corev1.EnvVar, name, path string) bool {
	for _, e := range env {
		if e.Name == name && e.ValueFrom != nil && e.ValueFrom.FieldRef != nil && e.ValueFrom.FieldRef.FieldPath == path {
			return true
		}
	}
	return false
}
