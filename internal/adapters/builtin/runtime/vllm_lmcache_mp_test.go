// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

func newTypedVLLMMPBackend() *cachev1alpha1.CacheBackend {
	chunkSize := int32(256)
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			LMCache: &cachev1alpha1.LMCacheEngineSpec{
				Topology:        cachev1alpha1.LMCacheTopologyPodLocal,
				ChunkSizeTokens: &chunkSize,
				PodLocal: &cachev1alpha1.LMCachePodLocalSpec{Server: &cachev1alpha1.LMCachePodLocalServerSpec{
					Image:      testLMCacheServerImage,
					Port:       6500,
					L1Capacity: resource.MustParse("4Gi"),
					MaxWorkers: 2,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("5Gi")},
						Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("6Gi")},
					},
				}},
			},
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
		},
	}
}

func newVLLMMPEnginePod(args ...string) *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
		Name:  EngineContainerName,
		Image: "vllm:connector-ready",
		Args:  args,
	}}}}
}

func TestVLLMLMCacheMPRegistrySelection(t *testing.T) {
	registry := runtimeadapter.NewRegistry()
	registry.Register(NewVLLMLMCacheMPAdapter(SubscriberConfig{}))

	typed, err := registry.Select(runtimeadapter.RuntimeVLLM, newTypedVLLMMPBackend())
	if err != nil {
		t.Fatalf("select typed adapter: %v", err)
	}
	if _, ok := typed.(vllmLMCacheMPAdapter); !ok {
		t.Fatalf("typed adapter = %T, want vllmLMCacheMPAdapter", typed)
	}
}

func TestVLLMLMCacheMPReservedSurface(t *testing.T) {
	adapter := NewVLLMLMCacheMPAdapter(SubscriberConfig{})
	if got, want := adapter.ReservedArgs(), []string{defaultEngineKVTransferConfigArg, vllmDisableHybridKVCacheArg}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ReservedArgs = %v, want %v", got, want)
	}
	if got, want := adapter.ReservedEnv(), []string{EnvPythonHashSeed, EnvInferenceCacheFailOpen}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ReservedEnv = %v, want %v", got, want)
	}
}

func TestEnginePortFromContainer(t *testing.T) {
	mk := func(command, args []string, env []corev1.EnvVar) *corev1.Pod {
		return &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: EngineContainerName, Command: command, Args: args, Env: env,
		}}}}
	}
	tests := []struct {
		name string
		pod  *corev1.Pod
		want string
	}{
		{name: "space form", pod: mk(nil, []string{"--port", "40000"}, nil), want: "40000"},
		{name: "equals form", pod: mk(nil, []string{"--port=41000"}, nil), want: "41000"},
		{name: "absent", pod: mk(nil, []string{"--model", "m"}, nil)},
		{name: "malformed", pod: mk(nil, []string{"--port", "abc"}, nil)},
		{name: "out of range", pod: mk(nil, []string{"--port", "70000"}, nil)},
		{name: "last valid wins", pod: mk(nil, []string{"--port=30000", "--port", "31000"}, nil), want: "31000"},
		{name: "invalid last keeps prior valid", pod: mk(nil, []string{"--port=33000", "--port", "abc"}, nil), want: "33000"},
		{name: "command and args", pod: mk([]string{"launch", "--port=30000"}, []string{"--port=31000"}, nil), want: "31000"},
		{name: "literal env reference", pod: mk(nil, []string{"--port=$(ENGINE_PORT)"}, []corev1.EnvVar{{Name: "ENGINE_PORT", Value: "42000"}}), want: "42000"},
		{name: "valueFrom is not statically resolvable", pod: mk(nil, []string{"--port=$(ENGINE_PORT)"}, []corev1.EnvVar{{Name: "ENGINE_PORT", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := enginePortFromContainer(tc.pod, EngineContainerName); got != tc.want {
				t.Fatalf("enginePortFromContainer() = %q, want %q", got, tc.want)
			}
		})
	}
	if got := enginePortFromContainer(nil, EngineContainerName); got != "" {
		t.Fatalf("enginePortFromContainer(nil) = %q, want empty", got)
	}
}

func TestVLLMLMCacheMPObservationSidecarMetricsURL(t *testing.T) {
	adapter := NewVLLMLMCacheMPAdapter(SubscriberConfig{Image: DefaultSubscriberImage})
	cache := newTypedVLLMMPBackend()
	cache.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "Qwen/Qwen2.5-0.5B-Instruct"}

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "default port", args: []string{"--model", "m"}, want: "http://127.0.0.1:8000/metrics"},
		{name: "custom port", args: []string{"--model", "m", "--port", "40000"}, want: "http://127.0.0.1:40000/metrics"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := newVLLMMPEnginePod(tc.args...)
			sidecar, err := adapter.ObservationSidecar(cache, pod)
			if err != nil || sidecar == nil {
				t.Fatalf("ObservationSidecar() = (%+v, %v)", sidecar, err)
			}
			if got, ok := testArgValue(sidecar.Args, "--engine-metrics-url"); !ok || got != tc.want {
				t.Fatalf("--engine-metrics-url = %q (present=%t), want %q; args=%v", got, ok, tc.want, sidecar.Args)
			}
		})
	}
}

func TestVLLMLMCacheMPValidateEngineParallelism(t *testing.T) {
	adapter := NewVLLMLMCacheMPAdapter(SubscriberConfig{}).(runtimeadapter.LMCacheMPRuntimeAdapter)
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "defaults"},
		{name: "TP pair", args: []string{"--tensor-parallel-size", "2"}},
		{name: "TP short equals", args: []string{"-tp=2"}},
		{name: "TP duplicate aliases", args: []string{"--tensor-parallel-size=2", "-tp", "2"}, wantErr: "duplicated"},
		{name: "TP malformed", args: []string{"--tensor-parallel-size"}, wantErr: "malformed"},
		{name: "TP zero", args: []string{"--tensor-parallel-size=0"}, wantErr: "positive integer"},
		{name: "PP two", args: []string{"--pipeline-parallel-size=2"}, wantErr: "pipeline parallel size 2"},
		{name: "DP two", args: []string{"-dp", "2"}, wantErr: "data parallel size 2"},
		{name: "external DP rank", args: []string{"--data-parallel-rank=0"}, wantErr: "multi-process data parallel flag"},
		{name: "hybrid flag value", args: []string{"--disable-hybrid-kv-cache-manager=false"}, wantErr: "boolean flag"},
		{name: "hybrid split value", args: []string{"--disable-hybrid-kv-cache-manager", "false"}, wantErr: "boolean flag"},
		{name: "duplicate transfer config", args: []string{"--kv-transfer-config", "{}", "--kv-transfer-config={}"}, wantErr: "at most once"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := adapter.ValidateMPEnginePod(newVLLMMPEnginePod(tc.args...), newTypedVLLMMPBackend())
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

func TestVLLMLMCacheMPKVTransferConfigRoles(t *testing.T) {
	tests := []struct {
		role cachev1alpha1.CacheBackendIntegrationRole
		want string
	}{
		{role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite, want: kvRoleBoth},
		{role: "", want: kvRoleBoth},
	}
	for _, tc := range tests {
		t.Run(string(tc.role), func(t *testing.T) {
			raw, err := vllmMPKVTransferConfigJSON(tc.role, 6500)
			if err != nil {
				t.Fatalf("vllmMPKVTransferConfigJSON: %v", err)
			}
			var got vllmMPKVTransferConfig
			if err := json.Unmarshal([]byte(raw), &got); err != nil {
				t.Fatalf("unmarshal config %q: %v", raw, err)
			}
			if got.Connector != vllmLMCacheMPConnectorName ||
				got.ConnectorModule != vllmLMCacheMPConnectorModulePath ||
				got.Role != tc.want ||
				got.ExtraConfig.Host != "tcp://127.0.0.1" ||
				got.ExtraConfig.Port != "6500" {
				t.Fatalf("config = %+v", got)
			}
		})
	}
	if _, err := vllmMPKVTransferConfigJSON("future", 6500); err == nil {
		t.Fatal("unknown role unexpectedly admitted")
	}
}

func TestVLLMLMCacheMPInjectsCommonServerAndExternalConnector(t *testing.T) {
	adapter := NewVLLMLMCacheMPAdapter(SubscriberConfig{})
	cache := newTypedVLLMMPBackend()
	pod := newVLLMMPEnginePod("--model", "meta-llama/Meta-Llama-3-8B-Instruct", "--tensor-parallel-size=2")
	pod.Spec.Containers[0].Env = []corev1.EnvVar{
		{Name: "KEEP_ME", Value: "yes"},
		{Name: EnvPythonHashSeed, Value: "random"},
	}

	if err := adapter.InjectEngineConfig(&pod.Spec, respBinding("redis.ns1.svc.cluster.local:6379"), cache); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	engine := &pod.Spec.Containers[0]
	if !containsArg(engine.Args, vllmDisableHybridKVCacheArg) {
		t.Fatalf("engine args missing %s: %v", vllmDisableHybridKVCacheArg, engine.Args)
	}
	values, malformed := argValues(engine.Args, defaultEngineKVTransferConfigArg)
	if malformed || len(values) != 1 {
		t.Fatalf("kv transfer config values=%v malformed=%v args=%v", values, malformed, engine.Args)
	}
	var config vllmMPKVTransferConfig
	if err := json.Unmarshal([]byte(values[0]), &config); err != nil {
		t.Fatalf("unmarshal injected config: %v", err)
	}
	if config.Connector != vllmLMCacheMPConnectorName || config.ConnectorModule != vllmLMCacheMPConnectorModulePath || config.Role != kvRoleBoth {
		t.Fatalf("injected connector config = %+v", config)
	}
	if got, ok := lookupEnv(engine.Env, EnvPythonHashSeed); !ok || got != "0" {
		t.Fatalf("%s = %q, %v", EnvPythonHashSeed, got, ok)
	}
	if got, ok := lookupEnv(engine.Env, EnvInferenceCacheFailOpen); !ok || got != "true" {
		t.Fatalf("%s = %q, %v", EnvInferenceCacheFailOpen, got, ok)
	}
	if got, ok := lookupEnv(engine.Env, "KEEP_ME"); !ok || got != "yes" {
		t.Fatalf("unrelated env was not preserved: %q, %v", got, ok)
	}
	server := findInitContainer(pod.Spec.InitContainers, lmCacheMPServerContainerName)
	if server == nil {
		t.Fatalf("common MP server missing: %+v", pod.Spec.InitContainers)
	}
	if server.Image != cache.Spec.LMCache.PodLocal.Server.Image || !containsArg(server.Args, "--l2-adapter") {
		t.Fatalf("MP server = %+v", server)
	}
	if findVolume(pod.Spec.Volumes, lmCacheMPConfigVolumeName) != nil || hasMount(engine.VolumeMounts, lmCacheMPConfigVolumeName) {
		t.Fatalf("vLLM must not receive SGLang YAML config volume: volumes=%+v mounts=%+v", pod.Spec.Volumes, engine.VolumeMounts)
	}
	if findVolume(pod.Spec.Volumes, lmCacheMPShmVolumeName) == nil || !hasMount(engine.VolumeMounts, lmCacheMPShmVolumeName) {
		t.Fatalf("shared MP shm missing: volumes=%+v mounts=%+v", pod.Spec.Volumes, engine.VolumeMounts)
	}

	want := pod.Spec.DeepCopy()
	if err := adapter.InjectEngineConfig(&pod.Spec, respBinding("redis.ns1.svc.cluster.local:6379"), cache); err != nil {
		t.Fatalf("second InjectEngineConfig: %v", err)
	}
	if !reflect.DeepEqual(&pod.Spec, want) {
		t.Fatalf("typed vLLM MP injection is not idempotent\n got=%+v\nwant=%+v", pod.Spec, *want)
	}
}

func TestVLLMLMCacheMPInjectsRedisSecretReferencesIntoServerOnly(t *testing.T) {
	adapter := NewVLLMLMCacheMPAdapter(SubscriberConfig{})
	cache := newTypedVLLMMPBackend()
	pod := newVLLMMPEnginePod("--model", "model")
	binding := &backendadapter.Binding{
		Protocol: backendadapter.ProtocolRESP,
		Endpoint: "redis.example:6379",
		Redis: &backendadapter.RedisBinding{Authentication: &cachev1alpha1.RedisAuthenticationSpec{
			Username: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "redis-auth"},
				Key:                  "username",
			},
			Password: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "redis-auth"},
				Key:                  "password",
			},
		}},
	}

	if err := adapter.InjectEngineConfig(&pod.Spec, binding, cache); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	server := findInitContainer(pod.Spec.InitContainers, lmCacheMPServerContainerName)
	if server == nil {
		t.Fatal("common MP server missing")
	}
	assertSecretEnv := func(name, key string) {
		t.Helper()
		for i := range server.Env {
			if server.Env[i].Name != name {
				continue
			}
			ref := server.Env[i].ValueFrom
			if ref == nil || ref.SecretKeyRef == nil || ref.SecretKeyRef.Name != "redis-auth" || ref.SecretKeyRef.Key != key {
				t.Fatalf("%s = %+v, want redis-auth/%s SecretKeyRef", name, server.Env[i], key)
			}
			return
		}
		t.Fatalf("%s missing", name)
	}
	assertSecretEnv(lmCacheRESPUsernameEnv, "username")
	assertSecretEnv(lmCacheRESPPasswordEnv, "password")
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == lmCacheRESPUsernameEnv || env.Name == lmCacheRESPPasswordEnv {
			t.Fatalf("Redis credential reference leaked into engine env: %+v", env)
		}
	}
}

func TestVLLMLMCacheMPInjectionCollisionIsAtomic(t *testing.T) {
	adapter := NewVLLMLMCacheMPAdapter(SubscriberConfig{})
	cache := newTypedVLLMMPBackend()
	pod := newVLLMMPEnginePod("--model", "model")
	pod.Spec.InitContainers = []corev1.Container{{Name: lmCacheMPServerContainerName, Image: "user-owned"}}
	want := pod.Spec.DeepCopy()
	if err := adapter.InjectEngineConfig(&pod.Spec, (*backendadapter.Binding)(nil), cache); err == nil {
		t.Fatal("foreign MP server collision unexpectedly admitted")
	}
	if !reflect.DeepEqual(&pod.Spec, want) {
		t.Fatalf("failed injection mutated PodSpec\n got=%+v\nwant=%+v", pod.Spec, *want)
	}
}
