// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

func newHiCacheBackend(spec *cachev1alpha1.SGLangHiCacheSpec) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "hicache", Namespace: "ns1"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeSGLang,
			Type:    cachev1alpha1.CacheBackendTypeSGLangHiCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Mode: cachev1alpha1.CacheBackendIntegrationModeOffload,
				Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "sglang"},
			},
			HiCache: spec,
		},
	}
}

func TestHiCacheAdapterContract(t *testing.T) {
	adapter := NewSGLangHiCacheAdapter()
	cache := newHiCacheBackend(&cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"})

	if !adapter.Supports(runtimeadapter.RuntimeSGLang, cache) {
		t.Fatal("SGLangHiCache adapter does not support its canonical pair")
	}
	if adapter.Supports(runtimeadapter.RuntimeVLLM, cache) {
		t.Fatal("SGLangHiCache adapter unexpectedly supports vLLM")
	}
	cache.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	if adapter.Supports(runtimeadapter.RuntimeSGLang, cache) {
		t.Fatal("SGLangHiCache adapter unexpectedly supports LMCache")
	}

	if !adapter.SupportsBinding(nil) {
		t.Fatal("SGLangHiCache adapter must accept a nil host-only binding")
	}
	if adapter.SupportsBinding(&backendadapter.Binding{Protocol: backendadapter.ProtocolRESP}) {
		t.Fatal("SGLangHiCache adapter unexpectedly accepts remote storage")
	}
}

func TestHiCacheInjectsOnlyRequestedFlags(t *testing.T) {
	size := int32(64)
	cache := newHiCacheBackend(&cachev1alpha1.SGLangHiCacheSpec{
		SizeGB:       &size,
		WritePolicy:  cachev1alpha1.SGLangHiCacheWriteThroughSelective,
		IOBackend:    cachev1alpha1.SGLangHiCacheIODirect,
		MemoryLayout: cachev1alpha1.SGLangHiCacheMemoryPageFirstKVSplit,
	})
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  SGLangEngineContainerName,
				Image: "sglang:test",
				Args:  []string{"--model-path", "model"},
				Env:   []corev1.EnvVar{{Name: "KEEP", Value: "true"}},
			},
			{Name: "metrics", Args: []string{"--keep"}},
		},
		Volumes: []corev1.Volume{{Name: "keep"}},
	}
	beforeNonArgs := pod.DeepCopy()

	if err := NewSGLangHiCacheAdapter().InjectEngineConfig(pod, nil, cache); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	args := pod.Containers[0].Args
	for flag, want := range map[string]string{
		SGLangHiCacheSizeArg:         "64",
		SGLangHiCacheWritePolicyArg:  "write_through_selective",
		SGLangHiCacheIOBackendArg:    "direct",
		SGLangHiCacheMemoryLayoutArg: "page_first_kv_split",
	} {
		if got, ok := testArgValue(args, flag); !ok || got != want {
			t.Errorf("%s = (%q, %v), want %q", flag, got, ok, want)
		}
	}
	if !containsArg(args, SGLangEnableHiCacheArg) {
		t.Fatalf("args missing %s: %v", SGLangEnableHiCacheArg, args)
	}
	if _, ok := testArgValue(args, SGLangHiCacheRatioArg); ok {
		t.Fatalf("%s was injected with sizeGB: %v", SGLangHiCacheRatioArg, args)
	}
	if !reflect.DeepEqual(pod.Containers[0].Env, beforeNonArgs.Containers[0].Env) ||
		!reflect.DeepEqual(pod.Containers[1], beforeNonArgs.Containers[1]) ||
		!reflect.DeepEqual(pod.Volumes, beforeNonArgs.Volumes) {
		t.Fatalf("HiCache injection changed env, sidecars, or volumes:\nbefore=%+v\nafter=%+v", beforeNonArgs, pod)
	}
}

func TestHiCacheOptionalFieldsStayOmitted(t *testing.T) {
	cache := newHiCacheBackend(&cachev1alpha1.SGLangHiCacheSpec{Ratio: "1.5"})
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "only"}}}
	if err := NewSGLangHiCacheAdapter().InjectEngineConfig(pod, nil, cache); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	if got, ok := testArgValue(pod.Containers[0].Args, SGLangHiCacheRatioArg); !ok || got != "1.5" {
		t.Fatalf("%s = (%q, %v), want 1.5", SGLangHiCacheRatioArg, got, ok)
	}
	for _, flag := range []string{
		SGLangHiCacheWritePolicyArg,
		SGLangHiCacheIOBackendArg,
		SGLangHiCacheMemoryLayoutArg,
	} {
		if _, ok := testArgValue(pod.Containers[0].Args, flag); ok {
			t.Errorf("unset optional field injected %s: %v", flag, pod.Containers[0].Args)
		}
	}
}

func TestHiCacheMatchingArgsArePreserved(t *testing.T) {
	cache := newHiCacheBackend(&cachev1alpha1.SGLangHiCacheSpec{
		Ratio: "2.0",
	})
	originalArgs := []string{
		"--model-path", "model",
		SGLangEnableHiCacheArg,
		SGLangHiCacheRatioArg + "=2",
		SGLangHiCacheWritePolicyArg, "write_through",
		"--hicache-io-backend=kernel",
		SGLangHiCacheMemoryLayoutArg, "page_first",
	}
	pod := &corev1.PodSpec{Containers: []corev1.Container{{
		Name: SGLangEngineContainerName,
		Args: append([]string(nil), originalArgs...),
	}}}
	if err := NewSGLangHiCacheAdapter().InjectEngineConfig(pod, nil, cache); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	if !reflect.DeepEqual(pod.Containers[0].Args, originalArgs) {
		t.Fatalf("matching user args changed:\n got %v\nwant %v", pod.Containers[0].Args, originalArgs)
	}
}

func TestHiCacheConflictsFailAtomically(t *testing.T) {
	base := newHiCacheBackend(&cachev1alpha1.SGLangHiCacheSpec{
		Ratio:       "2",
		WritePolicy: cachev1alpha1.SGLangHiCacheWriteThrough,
	})
	cases := []struct {
		name string
		args []string
	}{
		{"different pair value", []string{SGLangHiCacheRatioArg, "3"}},
		{"different equals value", []string{SGLangHiCacheRatioArg + "=3"}},
		{"opposite sizing mode", []string{SGLangHiCacheSizeArg, "8"}},
		{"duplicate sizing flag", []string{SGLangHiCacheRatioArg, "2", SGLangHiCacheRatioArg + "=2"}},
		{"malformed sizing flag", []string{SGLangHiCacheRatioArg}},
		{"different optional value", []string{SGLangHiCacheWritePolicyArg, "write_back"}},
		{"enable carries value", []string{SGLangEnableHiCacheArg + "=true"}},
		{"duplicate enable", []string{SGLangEnableHiCacheArg, SGLangEnableHiCacheArg}},
		{"LMCache enabled", []string{SGLangEnableLMCacheArg}},
		{"LMCache config", []string{SGLangConfigFileArg, "/tmp/lmcache.yaml"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: SGLangEngineContainerName,
					Args: append([]string(nil), tc.args...),
					Env:  []corev1.EnvVar{{Name: "KEEP", Value: "yes"}},
				}},
				Volumes: []corev1.Volume{{Name: "keep"}},
			}
			before := pod.DeepCopy()
			if err := NewSGLangHiCacheAdapter().InjectEngineConfig(pod, nil, base); err == nil {
				t.Fatal("InjectEngineConfig returned no error")
			}
			if !reflect.DeepEqual(pod, before) {
				t.Fatalf("failed injection mutated pod:\nbefore=%+v\nafter=%+v", before, pod)
			}
		})
	}
}

func TestHiCacheOmittedOptionalArgsFailAtomicallyWhenMalformedOrDuplicated(t *testing.T) {
	cache := newHiCacheBackend(&cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"})
	cases := []struct {
		name string
		args []string
	}{
		{"malformed write policy", []string{SGLangHiCacheWritePolicyArg}},
		{"duplicate write policy", []string{SGLangHiCacheWritePolicyArg, "write_back", SGLangHiCacheWritePolicyArg + "=write_through"}},
		{"malformed io backend", []string{SGLangHiCacheIOBackendArg}},
		{"duplicate io backend", []string{SGLangHiCacheIOBackendArg, "direct", SGLangHiCacheIOBackendArg + "=kernel"}},
		{"malformed memory layout", []string{SGLangHiCacheMemoryLayoutArg}},
		{"duplicate memory layout", []string{SGLangHiCacheMemoryLayoutArg, "layer_first", SGLangHiCacheMemoryLayoutArg + "=page_first"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.PodSpec{Containers: []corev1.Container{{
				Name: SGLangEngineContainerName,
				Args: append([]string(nil), tc.args...),
				Env:  []corev1.EnvVar{{Name: "KEEP", Value: "yes"}},
			}}}
			before := pod.DeepCopy()
			if err := NewSGLangHiCacheAdapter().InjectEngineConfig(pod, nil, cache); err == nil {
				t.Fatal("InjectEngineConfig returned no error")
			}
			if !reflect.DeepEqual(pod, before) {
				t.Fatalf("failed injection mutated pod:\nbefore=%+v\nafter=%+v", before, pod)
			}
		})
	}
}

func TestHiCacheRejectsInvalidBackendAtAdapterBoundary(t *testing.T) {
	falseValue := false
	zero := int32(0)
	cases := []struct {
		name   string
		mutate func(*cachev1alpha1.CacheBackend)
	}{
		{"missing config", func(cache *cachev1alpha1.CacheBackend) { cache.Spec.HiCache = nil }},
		{"both capacities", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.HiCache.SizeGB = &zero
		}},
		{"zero size", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.HiCache.Ratio = ""
			cache.Spec.HiCache.SizeGB = &zero
		}},
		{"invalid ratio", func(cache *cachev1alpha1.CacheBackend) { cache.Spec.HiCache.Ratio = "NaN" }},
		{"wrong engine", func(cache *cachev1alpha1.CacheBackend) { cache.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM }},
		{"events only", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
		}},
		{"read only", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Integration.Role = cachev1alpha1.CacheBackendIntegrationRoleReadOnly
		}},
		{"fail closed", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Integration.FailOpen = &falseValue
		}},
		{"autoscaling", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 2}
		}},
		{"missing selector", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.EngineSelector = nil
		}},
		{"reserved arg override", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Integration.EngineOverrides = &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{SGLangHiCacheRatioArg + "=3"},
			}
		}},
		{"reserved arg suppression", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Integration.EngineOverrides = &cachev1alpha1.EngineInjectionOverrides{
				SuppressArgs: []string{SGLangEnableHiCacheArg},
			}
		}},
		{"invalid write policy", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.HiCache.WritePolicy = "sometimes"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := newHiCacheBackend(&cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"})
			tc.mutate(cache)
			pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "sglang"}}}
			if err := NewSGLangHiCacheAdapter().InjectEngineConfig(pod, nil, cache); err == nil {
				t.Fatal("InjectEngineConfig returned no error")
			}
			if len(pod.Containers[0].Args) != 0 {
				t.Fatalf("invalid config partially injected args: %v", pod.Containers[0].Args)
			}
		})
	}
}

func TestHiCacheMultiContainerRequiresSGLangName(t *testing.T) {
	cache := newHiCacheBackend(&cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"})
	pod := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "engine"},
		{Name: "metrics"},
	}}
	if err := NewSGLangHiCacheAdapter().InjectEngineConfig(pod, nil, cache); err == nil ||
		!strings.Contains(err.Error(), `none is named "sglang"`) {
		t.Fatalf("InjectEngineConfig error = %v, want missing sglang container", err)
	}
}

func TestHiCacheReservedArgs(t *testing.T) {
	got := NewSGLangHiCacheAdapter().ReservedArgs()
	want := []string{
		SGLangEnableHiCacheArg,
		SGLangHiCacheSizeArg,
		SGLangHiCacheRatioArg,
		SGLangHiCacheWritePolicyArg,
		SGLangHiCacheIOBackendArg,
		SGLangHiCacheMemoryLayoutArg,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReservedArgs = %v, want %v", got, want)
	}
	if got := NewSGLangHiCacheAdapter().ReservedEnv(); len(got) != 0 {
		t.Fatalf("ReservedEnv = %v, want empty", got)
	}
}

func TestHiCacheObservationSidecarReusesSGLangRenderer(t *testing.T) {
	cache := newHiCacheBackend(&cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"})
	cache.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "model-a"}
	adapter := NewSGLangHiCacheAdapter(
		runtimeadapter.WithSubscriberImage("subscriber:test"),
		runtimeadapter.WithPolicyServerGRPCAddress("policy:50051"),
	)
	sidecar, err := adapter.ObservationSidecar(cache, &corev1.Pod{})
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if sidecar == nil {
		t.Fatal("ObservationSidecar returned nil")
	}
	joined := strings.Join(sidecar.Args, " ")
	for _, want := range []string{"--model-id=model-a", "--hash-scheme=sglang", "--server=policy:50051"} {
		if !strings.Contains(joined, want) {
			t.Errorf("sidecar args %q missing %q", joined, want)
		}
	}
}

func testArgValue(args []string, flag string) (string, bool) {
	values, malformed := argValues(args, flag)
	if malformed || len(values) != 1 {
		return "", false
	}
	return values[0], true
}
