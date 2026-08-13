// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"flag"
	"io"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

func typedSGLangBackend() *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeSGLang,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			LMCache: &cachev1alpha1.LMCacheEngineSpec{
				Topology: cachev1alpha1.LMCacheTopologyPodLocal,
				PodLocal: &cachev1alpha1.LMCachePodLocalSpec{Server: &cachev1alpha1.LMCachePodLocalServerSpec{
					Image:      "registry.example/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Port:       6500,
					L1Capacity: resource.MustParse("4Gi"),
					MaxWorkers: 4,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("5Gi")},
						Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("5Gi")},
					},
				}},
			},
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite},
		},
	}
}

func observedTypedSGLangBackend(model string) *cachev1alpha1.CacheBackend {
	backend := typedSGLangBackend()
	backend.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: model}
	return backend
}

func TestSGLangLMCacheSelectsOnlyTypedPodLocal(t *testing.T) {
	adapter := NewSGLangLMCacheAdapter(SubscriberConfig{})
	cache := typedSGLangBackend()
	if !adapter.Supports(runtimeadapter.RuntimeSGLang, cache) {
		t.Fatal("typed PodLocal SGLang backend was not selected")
	}
	cache.Spec.LMCache.Topology = ""
	if adapter.Supports(runtimeadapter.RuntimeSGLang, cache) {
		t.Fatal("topology-less SGLang backend selected after legacy removal")
	}
	cache.Spec.LMCache = nil
	cache.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
	if !adapter.Supports(runtimeadapter.RuntimeSGLang, cache) {
		t.Fatal("events-only SGLang backend was not selected for subscriber wiring")
	}
}

func TestSGLangLMCacheInjectsTypedMP(t *testing.T) {
	adapter := NewSGLangLMCacheAdapter(SubscriberConfig{})
	cache := typedSGLangBackend()
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: SGLangEngineContainerName, Image: "engine:test", Args: []string{"serve", "model", "--page-size", "64"}}}}
	binding := &backendadapter.Binding{Protocol: backendadapter.ProtocolRESP, Endpoint: "redis.ns1.svc.cluster.local:6379"}
	if err := adapter.InjectEngineConfig(pod, binding, cache); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	engine := pod.Containers[0]
	if !hasArg(engine.Args, SGLangEnableLMCacheArg) || !hasArg(engine.Args, SGLangConfigFileArg) {
		t.Fatalf("engine args = %v, want typed MP flags", engine.Args)
	}
	if len(pod.InitContainers) != 1 || pod.InitContainers[0].Name != lmCacheMPServerContainerName {
		t.Fatalf("init containers = %+v, want MP server native sidecar", pod.InitContainers)
	}
	if got := strings.Join(pod.InitContainers[0].Args, " "); !strings.Contains(got, "resp") {
		t.Fatalf("MP server args = %q, want Redis RESP L3 binding", got)
	}
}

func TestSGLangLMCacheValidatesPageSize(t *testing.T) {
	adapter := NewSGLangLMCacheAdapter(SubscriberConfig{}).(runtimeadapter.LMCacheMPRuntimeAdapter)
	cache := typedSGLangBackend()
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: SGLangEngineContainerName, Args: []string{"--page-size", "64"}}}}}
	if err := adapter.ValidateMPEnginePod(pod, cache); err != nil {
		t.Fatalf("ValidateMPEnginePod: %v", err)
	}
	pod.Spec.Containers[0].Args = []string{"--page-size", "100"}
	if err := adapter.ValidateMPEnginePod(pod, cache); err == nil {
		t.Fatal("ValidateMPEnginePod accepted page size that does not divide chunk size")
	}
}

func TestSGLangSupports(t *testing.T) {
	a := NewSGLangLMCacheAdapter(SubscriberConfig{})
	cases := []struct {
		name    string
		runtime runtimeadapter.RuntimeID
		cache   *cachev1alpha1.CacheBackend
		want    bool
	}{
		{"sglang+lmcache", runtimeadapter.RuntimeSGLang, typedSGLangBackend(), true},
		{"vllm+lmcache", runtimeadapter.RuntimeVLLM, typedSGLangBackend(), false},
		{"sglang+unsupported", runtimeadapter.RuntimeSGLang, &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendType("unsupported")}}, false},
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
	a := NewSGLangLMCacheAdapter(SubscriberConfig{}).(interface {
		SupportedPairs() []runtimeadapter.SupportedPair
	})
	got := a.SupportedPairs()
	want := []runtimeadapter.SupportedPair{{Runtime: runtimeadapter.RuntimeSGLang, Backend: cachev1alpha1.CacheBackendTypeLMCache}}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("SupportedPairs = %v, want %v", got, want)
	}
}

func TestSGLangInjectsMetricsOnlyWhenSubscriberWillAttach(t *testing.T) {
	cache := typedSGLangBackend()
	cache.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "gemma"}
	pod := &corev1.PodSpec{Containers: []corev1.Container{{
		Name: SGLangEngineContainerName, Image: "sglang:connector-ready", Args: []string{"--model", "gemma"},
	}}}
	adapter := NewSGLangLMCacheAdapter(SubscriberConfig{Image: "subscriber:pinned"})
	if err := adapter.InjectEngineConfig(pod, nil, cache); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	if !containsArg(pod.Containers[0].Args, SGLangEnableMetricsArg) {
		t.Fatalf("engine args missing %s required by subscriber: %v", SGLangEnableMetricsArg, pod.Containers[0].Args)
	}

	withoutSubscriber := typedSGLangBackend()
	pod = &corev1.PodSpec{Containers: []corev1.Container{{
		Name: SGLangEngineContainerName, Image: "sglang:connector-ready", Args: []string{"--model", "gemma"},
	}}}
	if err := NewSGLangLMCacheAdapter(SubscriberConfig{}).InjectEngineConfig(pod, nil, withoutSubscriber); err != nil {
		t.Fatalf("InjectEngineConfig without subscriber: %v", err)
	}
	if containsArg(pod.Containers[0].Args, SGLangEnableMetricsArg) {
		t.Fatalf("engine args unexpectedly enabled metrics without a subscriber: %v", pod.Containers[0].Args)
	}
}

func TestSGLangMetricsInjectionRequiresEngineContainer(t *testing.T) {
	cache := observedTypedSGLangBackend("gemma")
	err := ensureSGLangMetricsForSubscriber(
		&corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}, {Name: "sidecar"}}},
		cache,
		SubscriberConfig{Image: "subscriber:pinned"},
	)
	if err == nil || !strings.Contains(err.Error(), SGLangEngineContainerName) {
		t.Fatalf("ensureSGLangMetricsForSubscriber error = %v, want missing engine container", err)
	}
}

func TestSGLangValidateTypedMPEnginePodPageSize(t *testing.T) {
	adapter := NewSGLangLMCacheAdapter(SubscriberConfig{}).(runtimeadapter.LMCacheMPRuntimeAdapter)
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "pair form", args: []string{"--page-size", "64"}},
		{name: "equals form", args: []string{"--page-size=128"}},
		{name: "missing", wantErr: "explicitly declare --page-size"},
		{name: "not a divisor", args: []string{"--page-size=96"}, wantErr: "chunk size 256 must be a multiple"},
		{name: "zero", args: []string{"--page-size=0"}, wantErr: "positive integer"},
		{name: "not an integer", args: []string{"--page-size=large"}, wantErr: "positive integer"},
		{name: "missing pair value", args: []string{"--page-size", "--model-path", "model"}, wantErr: "malformed"},
		{name: "duplicate", args: []string{"--page-size=1", "--page-size", "64"}, wantErr: "duplicated"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: SGLangEngineContainerName,
				Args: tc.args,
			}}}}
			err := adapter.ValidateMPEnginePod(pod, typedSGLangBackend())
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateMPEnginePod: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateMPEnginePod error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestSGLangValidateRejectsIncompleteTopology(t *testing.T) {
	adapter := NewSGLangLMCacheAdapter(SubscriberConfig{}).(runtimeadapter.LMCacheMPRuntimeAdapter)
	validPod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: SGLangEngineContainerName, Args: []string{"--page-size", "64"}}}}}
	tests := []struct {
		name    string
		pod     *corev1.Pod
		cache   *cachev1alpha1.CacheBackend
		wantErr string
	}{
		{name: "nil pod", cache: typedSGLangBackend(), wantErr: "pod is nil"},
		{name: "nil cache", pod: validPod, wantErr: "configuration is missing"},
		{name: "missing LMCache", pod: validPod, cache: &cachev1alpha1.CacheBackend{}, wantErr: "configuration is missing"},
		{name: "missing PodLocal server", pod: validPod, cache: func() *cachev1alpha1.CacheBackend {
			cache := typedSGLangBackend()
			cache.Spec.LMCache.PodLocal = nil
			return cache
		}(), wantErr: "PodLocal server configuration is missing"},
		{name: "missing NodeLocal server", pod: validPod, cache: func() *cachev1alpha1.CacheBackend {
			cache := typedSGLangBackend()
			cache.Spec.LMCache.Topology = cachev1alpha1.LMCacheTopologyNodeLocal
			cache.Spec.LMCache.PodLocal = nil
			return cache
		}(), wantErr: "NodeLocal server configuration is missing"},
		{name: "unsupported topology", pod: validPod, cache: func() *cachev1alpha1.CacheBackend {
			cache := typedSGLangBackend()
			cache.Spec.LMCache.Topology = "Remote"
			return cache
		}(), wantErr: "topology \"Remote\" is not implemented"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := adapter.ValidateMPEnginePod(tc.pod, tc.cache)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateMPEnginePod error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestSGLangInjectRejectsInvalidInputs(t *testing.T) {
	adapter := NewSGLangLMCacheAdapter(SubscriberConfig{})
	validPod := corev1.PodSpec{Containers: []corev1.Container{{Name: SGLangEngineContainerName}}}
	tests := []struct {
		name    string
		pod     *corev1.PodSpec
		cache   *cachev1alpha1.CacheBackend
		wantErr string
	}{
		{name: "nil pod", cache: typedSGLangBackend(), wantErr: "pod is nil"},
		{name: "nil cache", pod: &validPod, wantErr: "cache is nil"},
		{name: "missing LMCache", pod: &validPod, cache: &cachev1alpha1.CacheBackend{}, wantErr: "typed server configuration is required"},
		{name: "missing PodLocal server", pod: &validPod, cache: func() *cachev1alpha1.CacheBackend {
			cache := typedSGLangBackend()
			cache.Spec.LMCache.PodLocal = nil
			return cache
		}(), wantErr: "typed PodLocal server configuration is required"},
		{name: "missing NodeLocal server", pod: &validPod, cache: func() *cachev1alpha1.CacheBackend {
			cache := typedSGLangBackend()
			cache.Spec.LMCache.Topology = cachev1alpha1.LMCacheTopologyNodeLocal
			cache.Spec.LMCache.PodLocal = nil
			return cache
		}(), wantErr: "typed NodeLocal server configuration is required"},
		{name: "unsupported topology", pod: &validPod, cache: func() *cachev1alpha1.CacheBackend {
			cache := typedSGLangBackend()
			cache.Spec.LMCache.Topology = "Remote"
			return cache
		}(), wantErr: "topology \"Remote\" is not implemented"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := adapter.InjectEngineConfig(tc.pod, nil, tc.cache)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("InjectEngineConfig error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestSGLangInjectRouterConfigIsNoop(t *testing.T) {
	a := NewSGLangLMCacheAdapter(SubscriberConfig{})
	cb := typedSGLangBackend()
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "router", Env: []corev1.EnvVar{{Name: "EXISTING", Value: "x"}}}}}
	if err := a.InjectRouterConfig(pod, respBinding("x.svc:65432"), cb); err != nil {
		t.Fatalf("InjectRouterConfig: %v", err)
	}
	if len(pod.Containers[0].Env) != 1 || pod.Containers[0].Env[0].Name != "EXISTING" {
		t.Fatalf("InjectRouterConfig modified container env: %v", pod.Containers[0].Env)
	}
	// Truly a no-op even on bad input (router-less backend must never force
	// callers to special-case it).
	if err := a.InjectRouterConfig(nil, respBinding("x"), cb); err != nil {
		t.Fatalf("InjectRouterConfig(nil pod) = %v, want nil", err)
	}
}

func TestSGLangObservationSidecarShape(t *testing.T) {
	a := NewSGLangLMCacheAdapter(SubscriberConfig{Image: DefaultSubscriberImage})
	cb := observedTypedSGLangBackend("Qwen/Qwen2.5-0.5B-Instruct")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-a", Namespace: "engines"}}

	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c == nil {
		t.Fatalf("ObservationSidecar returned nil for sglang+LMCache with a model + image set")
	}
	if c.Name != enginebinding.SubscriberContainerName {
		t.Fatalf("container name = %q, want %q", c.Name, enginebinding.SubscriberContainerName)
	}
	if !envHasFieldRef(c.Env, "POD_NAME", "metadata.name") || !envHasFieldRef(c.Env, "POD_NAMESPACE", "metadata.namespace") {
		t.Fatalf("downward-API env missing: %v", c.Env)
	}
	wantArgs := []string{
		"--engine-endpoint=tcp://127.0.0.1:5557",
		"--server=" + DefaultPolicyServerGRPCAddress,
		"--replica-id=$(POD_NAME)",
		"--tenant-id=$(POD_NAMESPACE)",
		"--model-id=Qwen/Qwen2.5-0.5B-Instruct",
		// Load-bearing: the index keys on hash_scheme, so the SGLang subscriber
		// MUST tag its reports "sglang" to stay disjoint from vLLM entries.
		"--hash-scheme=sglang",
		"--engine-metrics-url=http://127.0.0.1:30000/metrics",
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
	a := NewSGLangLMCacheAdapter(SubscriberConfig{Image: DefaultSubscriberImage})
	cb := observedTypedSGLangBackend("Qwen/Qwen2.5-0.5B-Instruct")
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
	fs.String("engine-metrics-url", "", "")
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
	a := NewSGLangLMCacheAdapter(SubscriberConfig{
		Image:                   "registry.example.com/subscriber:pinned",
		PolicyServerGRPCAddress: "ic-server.custom-ns.svc.cluster.local:9090",
	})
	cb := observedTypedSGLangBackend("MyOrg/MyModel")
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
	a := NewSGLangLMCacheAdapter(SubscriberConfig{Image: DefaultSubscriberImage})
	cb := typedSGLangBackend()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-a"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c != nil {
		t.Fatalf("expected nil sidecar when observation.modelID is unset, got %+v", c)
	}
}

func TestSGLangObservationSidecarSkipsWithoutImage(t *testing.T) {
	a := NewSGLangLMCacheAdapter(SubscriberConfig{}) // no image configured → auto-attach opt-out
	cb := observedTypedSGLangBackend("MyOrg/MyModel")
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
	a := NewSGLangLMCacheAdapter(SubscriberConfig{Image: DefaultSubscriberImage})
	cb := observedTypedSGLangBackend("m")
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
	got := NewSGLangLMCacheAdapter(SubscriberConfig{}).ReservedArgs()
	want := []string{SGLangEnableLMCacheArg, SGLangConfigFileArg, SGLangEnableMetricsArg}
	if len(got) != len(want) {
		t.Fatalf("ReservedArgs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ReservedArgs = %v, want %v", got, want)
		}
	}
}

func TestSGLangEngineContainerName(t *testing.T) {
	if got := NewSGLangLMCacheAdapter(SubscriberConfig{}).EngineContainerName(); got != SGLangEngineContainerName {
		t.Fatalf("EngineContainerName = %q, want %q", got, SGLangEngineContainerName)
	}
}

func TestSGLangReservedEnv(t *testing.T) {
	got := NewSGLangLMCacheAdapter(SubscriberConfig{}).ReservedEnv()
	want := []string{EnvLMCacheUseExperimental, EnvInferenceCacheFailOpen}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReservedEnv = %v, want %v", got, want)
	}
}
