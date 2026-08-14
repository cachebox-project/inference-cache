// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package pod

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/go-logr/logr"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	builtinadapters "github.com/cachebox-project/inference-cache/internal/adapters/builtin"
	builtinruntime "github.com/cachebox-project/inference-cache/internal/adapters/builtin/runtime"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

const (
	testVLLMEngineContainerName = "vllm"
	testSubscriberImage         = "subscriber:test"
	testRetiredLMCacheRemoteURL = "LMCACHE_REMOTE_URL"
	testEnvLMCacheChunkSize     = "LMCACHE_CHUNK_SIZE"
	testEnvVLLMUseV1            = "VLLM_USE_V1"
	testEnvPythonHashSeed       = "PYTHONHASHSEED"
	testReferenceCacheEndpoint  = "INFERENCECACHE_CACHE_ENDPOINT"
	testRuntimeReference        = adapterruntime.RuntimeID("reference")
)

func newVLLMRegistry(configs ...builtinruntime.SubscriberConfig) *adapterruntime.Registry {
	var config builtinruntime.SubscriberConfig
	if len(configs) > 0 {
		config = configs[0]
	}
	registry := adapterruntime.NewRegistry()
	registry.Register(builtinruntime.NewVLLMLMCacheMPAdapter(config))
	return registry
}

// referenceRuntimeAdapter is a webhook-local fixture for the public runtime
// extension contract. It deliberately renders no observation sidecar.
type referenceRuntimeAdapter struct{}

func (referenceRuntimeAdapter) Supports(runtime adapterruntime.RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	return cache != nil && runtime == testRuntimeReference
}

func (referenceRuntimeAdapter) SupportsBinding(binding *backendadapter.Binding) bool {
	return binding != nil && binding.Protocol != ""
}

func (referenceRuntimeAdapter) InjectEngineConfig(pod *corev1.PodSpec, binding *backendadapter.Binding, _ *cachev1alpha1.CacheBackend) error {
	if pod == nil || binding == nil {
		return errors.New("reference fixture requires pod and binding")
	}
	for i := range pod.Containers {
		pod.Containers[i].Env = referenceUpsertEnv(pod.Containers[i].Env, corev1.EnvVar{Name: testReferenceCacheEndpoint, Value: binding.Endpoint})
	}
	return nil
}

func (referenceRuntimeAdapter) InjectRouterConfig(*corev1.PodSpec, *backendadapter.Binding, *cachev1alpha1.CacheBackend) error {
	return nil
}

func (referenceRuntimeAdapter) ObservationSidecar(*cachev1alpha1.CacheBackend, *corev1.Pod) (*corev1.Container, error) {
	return nil, nil
}

func (referenceRuntimeAdapter) ReservedArgs() []string      { return nil }
func (referenceRuntimeAdapter) ReservedEnv() []string       { return nil }
func (referenceRuntimeAdapter) EngineContainerName() string { return "" }

func referenceUpsertEnv(env []corev1.EnvVar, want corev1.EnvVar) []corev1.EnvVar {
	for i := range env {
		if env[i].Name == want.Name {
			env[i] = want
			return env
		}
	}
	return append(env, want)
}

func externalRedisStorage(endpoint string) *cachev1alpha1.CacheBackendRemoteStorageSpec {
	return &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		Endpoint:  endpoint,
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}
}

// newScheme returns a scheme with corev1 + the CRD types registered so a
// fake client can list CacheBackends and the handler can json-unmarshal Pods.
func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("clientgoscheme.AddToScheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("cachev1alpha1.AddToScheme: %v", err)
	}
	return s
}

// newRequest wraps a Pod as a CREATE admission.Request. namespace mirrors
// the URL-derived namespace the apiserver always sets, even when the pod's
// metadata.namespace is empty (which is the common shape during CREATE).
func newRequest(t *testing.T, pod *corev1.Pod, namespace string) admission.Request {
	t.Helper()
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("marshal pod: %v", err)
	}
	return admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			UID:       types.UID("test-uid"),
			Operation: admissionv1.Create,
			Namespace: namespace,
			Kind:      metav1.GroupVersionKind{Version: "v1", Kind: "Pod"},
			Resource:  metav1.GroupVersionResource{Version: "v1", Resource: "pods"},
			Object:    runtime.RawExtension{Raw: raw},
		},
	}
}

// applyPatches reconstructs the admitted pod by applying a Response's JSON
// patches to the original raw object. The handler returns
// admission.PatchResponseFromRaw which generates a JSON-patch sequence; we
// need to apply it to confirm what the apiserver would end up persisting.
func applyPatches(t *testing.T, orig []byte, resp admission.Response) *corev1.Pod {
	t.Helper()
	if !resp.Allowed {
		t.Fatalf("response not allowed: %+v", resp.Result)
	}
	patchJSON, err := json.Marshal(resp.Patches)
	if err != nil {
		t.Fatalf("marshal patches: %v", err)
	}
	patched, err := applyJSONPatch(orig, patchJSON)
	if err != nil {
		t.Fatalf("apply patches: %v", err)
	}
	var out corev1.Pod
	if err := json.Unmarshal(patched, &out); err != nil {
		t.Fatalf("unmarshal patched pod: %v", err)
	}
	return &out
}

// applyJSONPatch applies an RFC 6902 patch to the original raw JSON,
// using evanphx/json-patch — already a transitive dep of controller-runtime,
// so no new module dependency for tests.
func applyJSONPatch(orig, patchJSON []byte) ([]byte, error) {
	p, err := jsonpatch.DecodePatch(patchJSON)
	if err != nil {
		return nil, fmt.Errorf("decode patch: %w", err)
	}
	return p.Apply(orig)
}

// vllmEnginePod returns a minimal vLLM engine Pod template with the
// canonical container name, labels the test caller can vary, and a single
// user-set env var the handler MUST preserve.
func vllmEnginePod(name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  testVLLMEngineContainerName,
				Image: "vllm/vllm-openai-cpu:latest",
				Env: []corev1.EnvVar{
					{Name: "USER_FLAG", Value: "preserved"},
				},
				Args: []string{"--model", "Qwen/Qwen2.5-0.5B-Instruct"},
			}},
		},
	}
}

// sglangEnginePod builds a pod whose engine container is named "sglang" — the
// SGLang adapter's EngineContainerName — so the webhook routes it through the
// SGLang+LMCache injection path. (Literal "sglang" rather than a re-export: the
// enginewire constant lives in an internal package this test can't import.)
func sglangEnginePod(name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "sglang",
				Image: "lmsysorg/sglang:latest",
				Env: []corev1.EnvVar{
					{Name: "USER_FLAG", Value: "preserved"},
				},
				Args: []string{"--model-path", "Qwen/Qwen2.5-0.5B-Instruct", "--page-size", "64"},
			}},
		},
	}
}

// readyCacheBackend returns a typed host-only CacheBackend with a vLLM
// integration and an EngineSelector keyed on a single label.
// The metadata.uid is set to a stable fake so the webhook's
// AnnotationInjectedByUID stamp has a value to compare against in tests
// that assert the annotation contents (a real apiserver would assign one
// on Create; the fake client does not, so we set it here).
func readyCacheBackend(name, namespace string, selector map[string]string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID("cb-" + namespace + "-" + name + "-uid"),
		},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			LMCache: &cachev1alpha1.LMCacheEngineSpec{
				Topology: cachev1alpha1.LMCacheTopologyPodLocal,
				PodLocal: &cachev1alpha1.LMCachePodLocalSpec{Server: &cachev1alpha1.LMCachePodLocalServerSpec{
					Image: "registry.example.com/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Port:  6500, L1Capacity: resource.MustParse("4Gi"), MaxWorkers: 2,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("5Gi")},
						Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("6Gi")},
					},
				}},
			},
			RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
				Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
				Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
			},
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: selector},
		},
		Status: cachev1alpha1.CacheBackendStatus{
			RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageStatus{
				Provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
				Endpoint: name + "." + namespace + ".svc.cluster.local:6379",
				Ready:    metav1.ConditionTrue,
			},
		},
	}
}

func newHandler(t *testing.T, objs ...client.Object) *EngineInjector {
	t.Helper()
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	return &EngineInjector{
		Reader:   c,
		Registry: builtinadapters.New(builtinadapters.Options{}).Runtime,
		Log:      logr.Discard(),
	}
}

// newHandlerWithSubscriber returns a handler whose registry has the
// kvevent-subscriber image configured, opting in to the sidecar auto-attach
// path. Tests that exercise the sidecar behaviour use this helper; tests
// that only need the engine config injection (or that want to confirm the
// no-image default produces no sidecar) use newHandler.
func newHandlerWithSubscriber(t *testing.T, objs ...client.Object) *EngineInjector {
	t.Helper()
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	reg := newVLLMRegistry(builtinruntime.SubscriberConfig{Image: testSubscriberImage})
	return &EngineInjector{
		Reader:   c,
		Registry: reg,
		Log:      logr.Discard(),
	}
}

func typedVLLMPodLocalBackend(name, namespace string, selector map[string]string) *cachev1alpha1.CacheBackend {
	cb := readyCacheBackend(name, namespace, selector)
	chunkSize := int32(256)
	cb.Spec.LMCache = &cachev1alpha1.LMCacheEngineSpec{
		Topology:        cachev1alpha1.LMCacheTopologyPodLocal,
		ChunkSizeTokens: &chunkSize,
		PodLocal: &cachev1alpha1.LMCachePodLocalSpec{Server: &cachev1alpha1.LMCachePodLocalServerSpec{
			Image:      "registry.example.com/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Port:       6500,
			L1Capacity: resource.MustParse("4Gi"),
			MaxWorkers: 2,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("5Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("6Gi")},
			},
		}},
	}
	cb.Spec.RemoteStorage = nil
	cb.Status.RemoteStorage = nil
	return cb
}

func typedVLLMNodeLocalBackend(name, namespace string, selector map[string]string) *cachev1alpha1.CacheBackend {
	cb := typedVLLMPodLocalBackend(name, namespace, selector)
	cb.Generation = 3
	cb.Spec.LMCache.Topology = cachev1alpha1.LMCacheTopologyNodeLocal
	cb.Spec.LMCache.PodLocal = nil
	cb.Spec.LMCache.NodeLocal = &cachev1alpha1.LMCacheNodeLocalSpec{
		Server: &cachev1alpha1.LMCacheNodeLocalServerSpec{
			Image: "registry.example.com/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Port:  6555, HTTPPort: 18080,
			L1Capacity: resource.MustParse("8Gi"), MaxGPUWorkers: 4, MaxCPUWorkers: 4,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("9Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("9Gi")},
			},
		},
		Scheduling: &cachev1alpha1.LMCacheNodeLocalSchedulingSpec{},
	}
	return cb
}

func TestHandle_TypedVLLMWithoutCapabilityAnnotationsUsesDedicatedMPAdapter(t *testing.T) {
	const ns = "engines"
	cb := typedVLLMPodLocalBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("typed MP pod should be injected without capability annotations: allowed=%v patches=%d result=%+v",
			resp.Allowed, len(resp.Patches), resp.Result)
	}
}

func TestHandle_TypedNodeLocalVLLMGatesOnOwnershipVerifiedSameNodeServer(t *testing.T) {
	const ns = "engines"
	cb := typedVLLMNodeLocalBackend("node-cache", ns, map[string]string{"app": "vllm-node"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-node", map[string]string{"app": "vllm-node"})
	pod.Spec.NodeSelector = map[string]string{"inference-system.io/pool": "owned"}
	pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{Weight: 1}}}}
	wantNodeSelector := map[string]string{"inference-system.io/pool": "owned"}
	wantAffinity := pod.Spec.Affinity.DeepCopy()
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("NodeLocal injection: Allowed=%v patches=%d result=%+v", resp.Allowed, len(resp.Patches), resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if findInitContainerByName(mutated.Spec.InitContainers, "lmcache-mp-server") != nil {
		t.Fatalf("NodeLocal engine received PodLocal server: %+v", mutated.Spec.InitContainers)
	}
	gate := findInitContainerByName(mutated.Spec.InitContainers, "lmcache-node-local-gate")
	if gate == nil || !strings.Contains(strings.Join(gate.Args, " "), "/healthcheck") || !strings.Contains(strings.Join(gate.Args, " "), "/config") {
		t.Fatalf("ownership-verifying NodeLocal gate = %+v", gate)
	}
	config := testArgValue(mutated.Spec.Containers[0].Args, "--kv-transfer-config")
	if !strings.Contains(config, `tcp://$(INFERENCECACHE_NODE_IP)`) {
		t.Fatalf("node-derived vLLM config = %q", config)
	}
	if mutated.Spec.HostNetwork || mutated.Spec.HostIPC {
		t.Fatalf("engine entered host namespace: hostNetwork=%v hostIPC=%v", mutated.Spec.HostNetwork, mutated.Spec.HostIPC)
	}
	if !reflect.DeepEqual(mutated.Spec.NodeSelector, wantNodeSelector) || !reflect.DeepEqual(mutated.Spec.Affinity, wantAffinity) {
		t.Fatalf("NodeLocal mutated inference-owned placement: selector=%v affinity=%+v", mutated.Spec.NodeSelector, mutated.Spec.Affinity)
	}
	if got := mutated.Annotations[AnnotationInjectedByUID]; got != string(cb.UID) {
		t.Fatalf("injected backend UID = %q", got)
	}
	if got := mutated.Labels[LabelLMCacheMPMetrics]; got != "" {
		t.Fatalf("NodeLocal engine must not advertise a PodLocal metrics sidecar: %s=%q", LabelLMCacheMPMetrics, got)
	}
}

func TestHandle_TypedPodLocalVLLMUsesDedicatedMPAdapter(t *testing.T) {
	const ns = "engines"
	cb := typedVLLMPodLocalBackend("vllm-typed", ns, map[string]string{"app": "vllm-mp"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-mp", map[string]string{"app": "vllm-mp"})
	pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args, "--tensor-parallel-size=2")
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("typed vLLM MP injection: Allowed=%v patches=%d result=%+v", resp.Allowed, len(resp.Patches), resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	server := findInitContainerByName(mutated.Spec.InitContainers, "lmcache-mp-server")
	if server == nil || server.Image != cb.Spec.LMCache.PodLocal.Server.Image {
		t.Fatalf("typed MP server = %+v", server)
	}
	if findInitContainerByName(mutated.Spec.InitContainers, "lmcache-mp-worker") != nil {
		t.Fatalf("typed vLLM wire fell through to legacy worker: %+v", mutated.Spec.InitContainers)
	}
	mustHaveArgFlag(t, mutated, "--disable-hybrid-kv-cache-manager")
	config := testArgValue(mutated.Spec.Containers[0].Args, "--kv-transfer-config")
	for _, want := range []string{
		`"kv_connector":"LMCacheMPConnector"`,
		`"kv_connector_module_path":"lmcache.integration.vllm.lmcache_mp_connector"`,
		`"kv_role":"kv_both"`,
		`"lmcache.mp.host":"tcp://127.0.0.1"`,
		`"lmcache.mp.port":"6500"`,
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("kv-transfer-config %q missing %q", config, want)
		}
	}
	mustHaveEnv(t, mutated, testEnvPythonHashSeed, "0")
	if got := mutated.Labels[LabelLMCacheMPMetrics]; got != LabelLMCacheMPMetricsEnabled {
		t.Fatalf("label %s = %q", LabelLMCacheMPMetrics, got)
	}

	incompatible := vllmEnginePod("engine-pp", map[string]string{"app": "vllm-mp"})
	incompatible.Annotations = pod.Annotations
	incompatible.Spec.Containers[0].Args = append(incompatible.Spec.Containers[0].Args, "--pipeline-parallel-size=2")
	incompatibleReq := newRequest(t, incompatible, ns)
	incompatibleResp := h.Handle(context.Background(), incompatibleReq)
	if !incompatibleResp.Allowed || len(incompatibleResp.Patches) != 0 {
		t.Fatalf("unsupported PP must admit unchanged: Allowed=%v patches=%d result=%+v",
			incompatibleResp.Allowed, len(incompatibleResp.Patches), incompatibleResp.Result)
	}
	if incompatibleResp.Result == nil || !strings.Contains(incompatibleResp.Result.Message, "pipeline parallel size 2") {
		t.Fatalf("unsupported PP diagnostic = %+v", incompatibleResp.Result)
	}
}

func TestHandle_MatchAndInject_SGLang(t *testing.T) {
	// Covers the production pod-webhook selection path for (sglang, LMCache):
	// the nil-registry fallback now includes the SGLang adapter, so a SGLang
	// engine pod matching a (sglang, LMCache) CacheBackend is injected with
	// SGLang's LMCache MP wire (--enable-lmcache + --lmcache-config-file +
	// LMCACHE_USE_EXPERIMENTAL), NOT vLLM's --kv-transfer-config / VLLM_USE_V1 /
	// PYTHONHASHSEED — and NOT the lm:// LMCACHE_REMOTE_URL, which MP mode ignores.
	// Without the SGLang registration in the fallback, the webhook would fail-open
	// and the pod would boot unwired.
	const ns = "engines"
	cb := readyCacheBackend("sg-primary", ns, map[string]string{"app": "sglang"})
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang // override the readyCacheBackend vLLM default
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}
	h := newHandler(t, cb) // helper explicitly injects the complete built-in composition
	pod := sglangEnginePod("sg-engine-a", map[string]string{"app": "sglang"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	if len(resp.Patches) == 0 {
		t.Fatalf("expected JSON patches (SGLang engine config injection), got none")
	}

	mutated := applyPatches(t, req.Object.Raw, resp)
	mustHaveEnv(t, mutated, "USER_FLAG", "preserved")
	mustHaveArgFlag(t, mutated, "--enable-lmcache")
	mustHaveArgFlag(t, mutated, "--lmcache-config-file")
	mustHaveEnv(t, mutated, "LMCACHE_USE_EXPERIMENTAL", "True")

	// Proof it went through the SGLang MP path: the vLLM-only connector arg/env
	// must be absent and the common MP server native sidecar must be present.
	for _, c := range mutated.Spec.Containers {
		if c.Name != "sglang" {
			continue
		}
		for _, a := range c.Args {
			if a == "--kv-transfer-config" {
				t.Fatalf("SGLang pod got vLLM's --kv-transfer-config: %v", c.Args)
			}
		}
		for _, e := range c.Env {
			if e.Name == testEnvVLLMUseV1 || e.Name == testEnvPythonHashSeed {
				t.Fatalf("SGLang pod got vLLM-only env %q (SGLang injects neither)", e.Name)
			}
			if e.Name == testRetiredLMCacheRemoteURL {
				t.Fatalf("SGLang MP wire must not inject retired %s", testRetiredLMCacheRemoteURL)
			}
		}
	}
	if findInitContainerByName(mutated.Spec.InitContainers, "lmcache-mp-server") == nil {
		t.Fatalf("MP server sidecar not injected; initContainers = %+v", mutated.Spec.InitContainers)
	}
	if got := mutated.Labels[LabelLMCacheMPMetrics]; got != "true" {
		t.Fatalf("typed SGLang pod label %s=%q, want true", LabelLMCacheMPMetrics, got)
	}
}

func TestHandle_TypedPodLocalSGLangUsesCommonMPServer(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("sg-typed", ns, map[string]string{"app": "sglang"})
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}
	chunkSize := int32(256)
	cb.Spec.LMCache = &cachev1alpha1.LMCacheEngineSpec{
		Topology:        cachev1alpha1.LMCacheTopologyPodLocal,
		ChunkSizeTokens: &chunkSize,
		PodLocal: &cachev1alpha1.LMCachePodLocalSpec{Server: &cachev1alpha1.LMCachePodLocalServerSpec{
			Image:      "registry.example.com/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Port:       6500,
			L1Capacity: resource.MustParse("4Gi"),
			MaxWorkers: 2,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("5Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("6Gi")},
			},
		}},
	}
	h := newHandler(t, cb)
	pod := sglangEnginePod("sg-engine-a", map[string]string{"app": "sglang"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("typed SGLang injection: Allowed=%v patches=%d result=%+v", resp.Allowed, len(resp.Patches), resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	mustHaveArgFlag(t, mutated, "--enable-lmcache")
	mustHaveArgPair(t, mutated, "--lmcache-config-file", "/var/run/inference-cache/lmcache/client.yaml")
	server := findInitContainerByName(mutated.Spec.InitContainers, "lmcache-mp-server")
	if server == nil {
		t.Fatalf("typed MP server missing: %+v", mutated.Spec.InitContainers)
	}
	if server.Image != cb.Spec.LMCache.PodLocal.Server.Image || server.Image == mutated.Spec.Containers[0].Image {
		t.Fatalf("server image = %q, engine image = %q", server.Image, mutated.Spec.Containers[0].Image)
	}
	joined := strings.Join(append(server.Command, server.Args...), " ")
	if !strings.Contains(joined, "lmcache server") || !strings.Contains(joined, "--http-port 8080") || strings.Contains(joined, "python3 -m") {
		t.Fatalf("typed server command = %s", joined)
	}
	if server.StartupProbe == nil || server.ReadinessProbe == nil || server.LivenessProbe == nil {
		t.Fatalf("typed server probes missing: %+v", server)
	}
	if got := mutated.Labels[LabelLMCacheMPMetrics]; got != LabelLMCacheMPMetricsEnabled {
		t.Fatalf("label %s = %q, want %q", LabelLMCacheMPMetrics, got, LabelLMCacheMPMetricsEnabled)
	}
	if findInitContainerByName(mutated.Spec.InitContainers, "lmcache-mp-worker") != nil {
		t.Fatalf("typed wire fell through to legacy worker: %+v", mutated.Spec.InitContainers)
	}

	// The compatibility guard must fail open atomically: an incompatible page
	// size admits the inference Pod but renders neither the server nor half of
	// the engine wire. The response message is the actionable admission trace;
	// controller status subsequently counts the Pod as uncovered.
	incompatible := sglangEnginePod("sg-engine-incompatible", map[string]string{"app": "sglang"})
	incompatible.Spec.Containers[0].Args = []string{"--model-path", "Qwen/Qwen2.5-0.5B-Instruct", "--page-size=96"}
	incompatibleReq := newRequest(t, incompatible, ns)
	incompatibleResp := h.Handle(context.Background(), incompatibleReq)
	if !incompatibleResp.Allowed || len(incompatibleResp.Patches) != 0 {
		t.Fatalf("incompatible page size must admit unchanged: Allowed=%v patches=%d result=%+v",
			incompatibleResp.Allowed, len(incompatibleResp.Patches), incompatibleResp.Result)
	}
	if incompatibleResp.Result == nil || !strings.Contains(incompatibleResp.Result.Message, "chunk size 256 must be a multiple") {
		t.Fatalf("incompatible page-size diagnostic = %+v", incompatibleResp.Result)
	}
}

func TestHandle_MatchAndInject_SGLangHiCacheWithoutEndpoint(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("hicache", ns, map[string]string{"app": "sglang"})
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeSGLangHiCache
	cb.Spec.LMCache = nil
	cb.Spec.RemoteStorage = nil
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.HiCache = &cachev1alpha1.SGLangHiCacheSpec{
		Ratio:        "2.0",
		WritePolicy:  cachev1alpha1.SGLangHiCacheWriteThrough,
		IOBackend:    cachev1alpha1.SGLangHiCacheIOKernel,
		MemoryLayout: cachev1alpha1.SGLangHiCacheMemoryPageFirst,
	}
	cb.Status.RemoteStorage = nil

	h := newHandler(t, cb)
	pod := sglangEnginePod("sg-engine-a", map[string]string{"app": "sglang"})
	req := newRequest(t, pod, ns)
	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("endpoint-free HiCache injection = Allowed %v, patches %d", resp.Allowed, len(resp.Patches))
	}

	mutated := applyPatches(t, req.Object.Raw, resp)
	mustHaveArgFlag(t, mutated, "--enable-hierarchical-cache")
	mustHaveArgPair(t, mutated, "--hicache-ratio", "2.0")
	mustHaveArgPair(t, mutated, "--hicache-write-policy", "write_through")
	mustHaveArgPair(t, mutated, "--hicache-io-backend", "kernel")
	mustHaveArgPair(t, mutated, "--hicache-mem-layout", "page_first")
	if got := mutated.Annotations[AnnotationInjectedBy]; got != ns+"/"+cb.Name {
		t.Fatalf("%s = %q, want %q", AnnotationInjectedBy, got, ns+"/"+cb.Name)
	}
	for _, env := range mutated.Spec.Containers[0].Env {
		if strings.HasPrefix(env.Name, "LMCACHE_") {
			t.Fatalf("native HiCache injected LMCache env %q", env.Name)
		}
	}
	if len(mutated.Spec.InitContainers) != 0 || len(mutated.Spec.Volumes) != 0 {
		t.Fatalf("native HiCache injected LMCache sidecars/volumes: init=%v volumes=%v",
			mutated.Spec.InitContainers, mutated.Spec.Volumes)
	}
}

func TestHandle_CanonicalSGLangHiCacheWithRemoteStorageFailsOpen(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("hicache-remote", ns, map[string]string{"app": "sglang"})
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeSGLangHiCache
	cb.Spec.LMCache = nil
	cb.Spec.Runtime = ""
	cb.Spec.HiCache = &cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"}
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}

	pod := sglangEnginePod("sg-engine-a", map[string]string{"app": "sglang"})
	req := newRequest(t, pod, ns)
	resp := newHandler(t, cb).Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("unsupported cache binding must fail open: %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("unsupported cache binding produced %d patches, want original Pod unchanged", len(resp.Patches))
	}
}

func TestHandle_SGLangHiCacheConflictFailsOpenWithoutPartialInjection(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("hicache", ns, map[string]string{"app": "sglang"})
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeSGLangHiCache
	cb.Spec.LMCache = nil
	cb.Spec.RemoteStorage = nil
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.HiCache = &cachev1alpha1.SGLangHiCacheSpec{
		Ratio:       "2",
		WritePolicy: cachev1alpha1.SGLangHiCacheWriteThrough,
	}
	cb.Status.RemoteStorage = nil

	pod := sglangEnginePod("sg-engine-a", map[string]string{"app": "sglang"})
	pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args, "--hicache-ratio=3")
	req := newRequest(t, pod, ns)
	resp := newHandler(t, cb).Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("conflict must fail open: %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("conflict produced %d patches, want original Pod unchanged", len(resp.Patches))
	}
}

func TestHandle_LMCacheBackend_NeverMovesEnginePodOntoHostNetwork(t *testing.T) {
	// The typed MP path must stay on the overlay; moving the engine to the host
	// network would break restricted Pod Security namespaces.
	const ns = "engines"
	cb := readyCacheBackend("lm", ns, map[string]string{"app": "vllm"})
	h := newHandlerWithSubscriber(t, cb)
	req := newRequest(t, vllmEnginePod("engine-a", map[string]string{"app": "vllm"}), ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed; result=%+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)

	if mutated.Spec.HostNetwork {
		t.Fatal("LMCache engine pod was moved onto the host network; the default path must stay on the overlay")
	}
	if mutated.Spec.DNSPolicy != "" {
		t.Fatalf("LMCache engine pod dnsPolicy rewritten to %q; want the cluster default", mutated.Spec.DNSPolicy)
	}
}

func TestHandle_AppendsObservationSidecar(t *testing.T) {
	// The vLLM/LMCache adapter returns a kvevent-subscriber sidecar
	// the webhook MUST append after InjectEngineConfig, with identity flags
	// derived from the CR + pod. This is the one test that pins the end-to-
	// end auto-attach behaviour at the admission boundary. Auto-attach is
	// opt-in via the controller flag; the handler helper here mirrors that
	// wiring.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "Qwen/Qwen2.5-0.5B-Instruct"}
	h := newHandlerWithSubscriber(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("expected Allowed with patches; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if len(mutated.Spec.Containers) != 2 {
		t.Fatalf("expected 2 containers (engine + subscriber), got %d: %v", len(mutated.Spec.Containers), containerNames(mutated))
	}
	sub := findContainer(mutated, enginebinding.SubscriberContainerName)
	if sub == nil {
		t.Fatalf("subscriber sidecar missing; containers = %v", containerNames(mutated))
	}
	if !argPresent(sub.Args, "--model-id=Qwen/Qwen2.5-0.5B-Instruct") {
		t.Fatalf("--model-id derived from cb.spec.observation.modelID missing; args = %v", sub.Args)
	}
	if !argPresent(sub.Args, "--replica-id=$(POD_NAME)") {
		t.Fatalf("--replica-id MUST use downward-API POD_NAME; args = %v", sub.Args)
	}
	if !argPresent(sub.Args, "--tenant-id=$(POD_NAMESPACE)") {
		t.Fatalf("--tenant-id MUST use downward-API POD_NAMESPACE; args = %v", sub.Args)
	}
	if !argPresent(sub.Args, "--engine-metrics-url=http://127.0.0.1:8000/metrics") {
		t.Fatalf("vLLM subscriber must scrape :8000/metrics; args = %v", sub.Args)
	}
	// Appending the observation sidecar must not regress typed MP injection.
	mustHaveArgFlag(t, mutated, "--kv-transfer-config")
	if findInitContainerByName(mutated.Spec.InitContainers, "lmcache-mp-server") == nil {
		t.Fatal("LMCache MP server missing after subscriber injection")
	}
}

func TestHandle_AppendsObservationSidecar_SGLang(t *testing.T) {
	// SGLang counterpart of TestHandle_AppendsObservationSidecar: a matched
	// SGLang engine pod must get the kvevent-subscriber sidecar tagged
	// --hash-scheme=sglang (the load-bearing scheme tag) + --ignore-block-removed
	// (LMCache is an L2 tier). Builds the relevant adapters with the subscriber
	// image option used by cmd/controller because the no-option registry
	// renders no sidecar (auto-attach opt-in).
	const ns = "engines"
	cb := readyCacheBackend("sg-primary", ns, map[string]string{"app": "sglang"})
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}
	cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "Qwen/Qwen2.5-0.5B-Instruct"}

	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
	config := builtinruntime.SubscriberConfig{Image: testSubscriberImage}
	reg := newVLLMRegistry(config)
	reg.Register(builtinruntime.NewSGLangLMCacheAdapter(config))
	h := &EngineInjector{Reader: c, Registry: reg, Log: logr.Discard()}

	pod := sglangEnginePod("sg-engine-a", map[string]string{"app": "sglang"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("expected Allowed with patches; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if len(mutated.Spec.Containers) != 2 {
		t.Fatalf("expected 2 containers (sglang engine + subscriber), got %d: %v", len(mutated.Spec.Containers), containerNames(mutated))
	}
	sub := findContainer(mutated, enginebinding.SubscriberContainerName)
	if sub == nil {
		t.Fatalf("subscriber sidecar missing; containers = %v", containerNames(mutated))
	}
	if !argPresent(sub.Args, "--hash-scheme=sglang") {
		t.Fatalf("SGLang subscriber MUST tag --hash-scheme=sglang; args = %v", sub.Args)
	}
	if !argPresent(sub.Args, "--engine-metrics-url=http://127.0.0.1:30000/metrics") {
		t.Fatalf("SGLang subscriber must scrape :30000/metrics; args = %v", sub.Args)
	}
	if !argPresent(sub.Args, "--model-id=Qwen/Qwen2.5-0.5B-Instruct") {
		t.Fatalf("--model-id derived from cb.spec.observation.modelID missing; args = %v", sub.Args)
	}
	if !argPresent(sub.Args, "--ignore-block-removed=true") {
		t.Fatalf("SGLang+LMCache subscriber MUST set --ignore-block-removed=true (L2 tier); args = %v", sub.Args)
	}
	// Appending the sidecar must not regress the SGLang engine-side injection.
	mustHaveArgFlag(t, mutated, "--enable-lmcache")
}

func TestHandle_SidecarAppendIsIdempotent(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "MyOrg/MyModel"}
	h := newHandlerWithSubscriber(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})

	first := h.Handle(context.Background(), newRequest(t, pod, ns))
	injected := applyPatches(t, newRequest(t, pod, ns).Object.Raw, first)
	if len(injected.Spec.Containers) != 2 {
		t.Fatalf("first admission must add the sidecar; containers = %v", containerNames(injected))
	}

	second := h.Handle(context.Background(), newRequest(t, injected, ns))
	if !second.Allowed {
		t.Fatalf("re-admission rejected: %+v", second.Result)
	}
	if len(second.Patches) != 0 {
		t.Fatalf("re-admission of fully-injected pod must produce no patches, got %d: %+v", len(second.Patches), second.Patches)
	}
}

// eventsOnlyCacheBackend returns an events-only (tier-1 routing) LMCache
// CacheBackend: type=LMCache, spec.integration.mode=EventsOnly, a served model
// id (so ObservationSidecar emits a container), an engineSelector, and no
// remote storage. The absent endpoint is the expected steady state, not a
// not-yet-reconciled race.
func eventsOnlyCacheBackend(name, namespace string, selector map[string]string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID("cb-" + namespace + "-" + name + "-uid"),
		},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Mode: cachev1alpha1.CacheBackendIntegrationModeEventsOnly,
				Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: selector},
			Observation:    &cachev1alpha1.CacheBackendObservationSpec{ModelID: "Qwen/Qwen2.5-0.5B-Instruct"},
		},
		// No remote-storage status: the webhook must inject the subscriber anyway.
	}
}

func TestHandle_EventsOnly_EmptyEndpoint_InjectsSubscriberWithoutConnector(t *testing.T) {
	// An events-only backend has no status.remoteStorage by design (no
	// provisioned server), but it must NOT fail-open the way a managed backend
	// with a not-yet-published endpoint does. The webhook injects: the pod is
	// patched, the kvevent-subscriber sidecar is appended, the injected-by
	// annotations are stamped, and the engine container gets NO connector
	// wiring (InjectEngineConfig is a no-op in events-only mode).
	const ns = "engines"
	cb := eventsOnlyCacheBackend("routing-only", ns, map[string]string{"app": "vllm"})
	h := newHandlerWithSubscriber(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	if len(resp.Patches) == 0 {
		t.Fatalf("events-only backend with no remote-storage endpoint must inject, not fail open; got no patches")
	}

	mutated := applyPatches(t, req.Object.Raw, resp)

	// The subscriber sidecar IS appended — that is the whole point of
	// events-only (the routing tier observes KV events via the sidecar).
	if len(mutated.Spec.Containers) != 2 {
		t.Fatalf("expected 2 containers (engine + subscriber), got %d: %v",
			len(mutated.Spec.Containers), containerNames(mutated))
	}
	sub := findContainer(mutated, enginebinding.SubscriberContainerName)
	if sub == nil {
		t.Fatalf("subscriber sidecar missing; containers = %v", containerNames(mutated))
	}
	if !argPresent(sub.Args, "--model-id=Qwen/Qwen2.5-0.5B-Instruct") {
		t.Fatalf("--model-id derived from cb.spec.observation.modelID missing; args = %v", sub.Args)
	}

	// The injected-by + injected-by-uid annotations are stamped — proving the
	// webhook took the inject path (not fail-open, which strips them).
	if got, want := mutated.Annotations[AnnotationInjectedBy], ns+"/"+cb.Name; got != want {
		t.Fatalf("annotation %s: got %q, want %q", AnnotationInjectedBy, got, want)
	}
	if got, want := mutated.Annotations[AnnotationInjectedByUID], string(cb.UID); got != want {
		t.Fatalf("annotation %s: got %q, want %q (matched CR UID)", AnnotationInjectedByUID, got, want)
	}

	// The engine container gets NO KV connector wiring: events-only loads no
	// connector (a hybrid-attention model's KV-cache manager would be disabled
	// by one). The user's own env/args survive untouched.
	engine := findContainer(mutated, testVLLMEngineContainerName)
	if engine == nil {
		t.Fatalf("engine container missing; containers = %v", containerNames(mutated))
	}
	for _, e := range engine.Env {
		if strings.HasPrefix(e.Name, "LMCACHE_") {
			t.Fatalf("events-only engine container must carry NO LMCACHE_* env; found %s=%q", e.Name, e.Value)
		}
	}
	for _, a := range engine.Args {
		if a == "--kv-transfer-config" {
			t.Fatalf("events-only engine container must carry NO --kv-transfer-config; args = %v", engine.Args)
		}
	}
	// The user-set env/arg on the engine pod template survive.
	if !argPresent(engine.Args, "--model") {
		t.Fatalf("user pod-template arg --model dropped; args = %v", engine.Args)
	}
}

func TestHandle_OffloadManagedBackend_EmptyEndpoint_FailsOpen(t *testing.T) {
	// Contrast with the events-only case above: an Offload (default-mode)
	// managed backend whose status.remoteStorage.endpoint is not yet published must fail open
	// — admit unmodified, no subscriber sidecar, no injected-by annotation —
	// because the connector it would wire needs a real dial target. This pins
	// that the events-only inject path is mode-gated, not a blanket
	// "inject on empty endpoint".
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}
	cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "Qwen/Qwen2.5-0.5B-Instruct"}
	cb.Status.RemoteStorage = nil
	h := newHandlerWithSubscriber(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed (fail-open), got: %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("Offload managed backend with empty status.remoteStorage.endpoint must fail open (no patches), got %d: %+v",
			len(resp.Patches), resp.Patches)
	}
	// Fail-open never stamps injected-by; the inbound pod carried none, so a
	// re-applied pod is byte-identical (zero patches above already proves it,
	// but assert the annotation absence explicitly for clarity).
	if pod.Annotations[AnnotationInjectedBy] != "" {
		t.Fatalf("fail-open path must not stamp %s", AnnotationInjectedBy)
	}
}

func TestHandle_EventsOnly_NoSubscriberImage_InjectsNothingNoStamp(t *testing.T) {
	// An events-only backend whose subscriber image is unconfigured (the
	// default-install posture: --kvevent-subscriber-image is empty, so the
	// adapter's ObservationSidecar returns nil) has NOTHING to wire — the
	// connector injection is a no-op for events-only, and no sidecar is
	// appended. The webhook must therefore inject nothing AND stamp no
	// injected-by/injected-by-uid annotation: a stamp on an untouched pod
	// would trip the downstream InjectedByCacheBackend event controller on a
	// non-existent injection. Contrast with
	// TestHandle_EventsOnly_EmptyEndpoint_InjectsSubscriberWithoutConnector,
	// where the subscriber image IS configured → subscriber appended +
	// injected-by stamped.
	const ns = "engines"
	cb := eventsOnlyCacheBackend("routing-only", ns, map[string]string{"app": "vllm"})
	// newHandler uses the complete built-in nil-Registry fallback with NO
	// subscriber image configured, so ObservationSidecar returns nil.
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed (fail-open, no wiring), got: %+v", resp.Result)
	}
	// Nothing was wired: no connector (events-only no-op), no sidecar (no
	// image) → the inbound pod (which carries no injection annotations) is
	// byte-identical, so zero patches.
	if len(resp.Patches) != 0 {
		t.Fatalf("events-only with no subscriber image must inject nothing, got %d patches: %+v",
			len(resp.Patches), resp.Patches)
	}

	// No subscriber container appended.
	mutated := applyPatches(t, req.Object.Raw, resp)
	if c := findContainer(mutated, enginebinding.SubscriberContainerName); c != nil {
		t.Fatalf("no subscriber image configured — must NOT append a sidecar; found %+v", c)
	}
	// No injected-by / injected-by-uid stamped — nothing was wired.
	if got := mutated.Annotations[AnnotationInjectedBy]; got != "" {
		t.Fatalf("no-wiring events-only must NOT stamp %s; got %q", AnnotationInjectedBy, got)
	}
	if got := mutated.Annotations[AnnotationInjectedByUID]; got != "" {
		t.Fatalf("no-wiring events-only must NOT stamp %s; got %q", AnnotationInjectedByUID, got)
	}
}

func TestHandle_EventsOnly_NoSubscriber_StripsForgedInjectedBy(t *testing.T) {
	// Defence-in-depth on the no-wiring events-only path: an inbound pod that
	// forged injected-by/injected-by-uid (copy/pasted from a real injection,
	// or set by an attacker with pod-create RBAC) must come out of admission
	// with those annotations STRIPPED when the webhook wired nothing —
	// otherwise the downstream InjectedByCacheBackend event controller would
	// fire on a pod the webhook never touched. The events-only no-wiring path
	// routes through failOpen, which strips those annotations — the same
	// fail-open no-injection contract every other un-wired path follows.
	const ns = "engines"
	cb := eventsOnlyCacheBackend("routing-only", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb) // no subscriber image → nothing to wire
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Annotations = map[string]string{
		AnnotationInjectedBy:         ns + "/routing-only",
		AnnotationInjectedByUID:      string(cb.UID),
		AnnotationInjectedGeneration: fmt.Sprint(cb.Generation),
	}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed (fail-open), got: %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if got := mutated.Annotations[AnnotationInjectedBy]; got != "" {
		t.Fatalf("forged %s must be stripped on no-wiring fail-open; got %q", AnnotationInjectedBy, got)
	}
	if got := mutated.Annotations[AnnotationInjectedByUID]; got != "" {
		t.Fatalf("forged %s must be stripped on no-wiring fail-open; got %q", AnnotationInjectedByUID, got)
	}
	if got := mutated.Annotations[AnnotationInjectedGeneration]; got != "" {
		t.Fatalf("forged %s must be stripped on no-wiring fail-open; got %q", AnnotationInjectedGeneration, got)
	}
}

func TestHandle_EventsOnly_PrebakedSubscriber_NotClaimedNoStamp(t *testing.T) {
	// An events-only pod that already carries a hand-authored container named
	// like the subscriber: the webhook stays idempotent (does NOT append a
	// second one), but it must NOT claim that pre-existing container as its own
	// wiring — it neither authored nor verified it (the operator's copy may
	// carry the wrong image/args and emit no usable events). So it stamps NO
	// injected-by and routes through the fail-open no-wiring path; the events
	// controller then never falsely reports the pod as injected, while a correct
	// hand-baked subscriber is still attributed via the engineSelector path.
	const ns = "engines"
	cb := eventsOnlyCacheBackend("routing-only", ns, map[string]string{"app": "vllm"})
	h := newHandlerWithSubscriber(t, cb) // subscriber image IS configured
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
		Name:  enginebinding.SubscriberContainerName,
		Image: "operator/hand-baked-subscriber:wrong",
	})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)

	// Idempotent: still exactly one subscriber-named container (no duplicate append).
	count := 0
	for i := range mutated.Spec.Containers {
		if mutated.Spec.Containers[i].Name == enginebinding.SubscriberContainerName {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 subscriber-named container (no duplicate append), got %d: %v",
			count, containerNames(mutated))
	}
	// The hand-baked container is left as-is — the subscriber is NOT webhook-authoritative.
	if sub := findContainer(mutated, enginebinding.SubscriberContainerName); sub == nil || sub.Image != "operator/hand-baked-subscriber:wrong" {
		t.Fatalf("hand-baked subscriber must be left untouched; got %+v", sub)
	}
	// NOT claimed: the webhook authored no wiring, so it stamps no injected-by.
	if got := mutated.Annotations[AnnotationInjectedBy]; got != "" {
		t.Fatalf("a pre-existing (unverified) subscriber must NOT be claimed as wiring; %s = %q", AnnotationInjectedBy, got)
	}
	if got := mutated.Annotations[AnnotationInjectedByUID]; got != "" {
		t.Fatalf("a pre-existing subscriber must NOT stamp %s; got %q", AnnotationInjectedByUID, got)
	}
}

func TestHandle_EventsOnly_EngineOverrides_DoNotTouchEngineContainer(t *testing.T) {
	// In events-only mode InjectEngineConfig is a no-op, so the engine
	// container is left otherwise untouched. spec.integration.engineOverrides
	// must NOT be applied: the override merge is scoped to adapter-owned
	// args/env, and since the adapter contributed none here, running it would
	// append non-adapter-owned args/env to the engine container, contradicting
	// the "engine container is left untouched" contract. The subscriber sidecar
	// still attaches (subscriber image configured), so the pod is wired and
	// injected-by IS stamped — but the engine container's args/env are
	// byte-identical to the inbound pod template.
	const ns = "engines"
	cb := eventsOnlyCacheBackend("routing-only", ns, map[string]string{"app": "vllm"})
	cb.Spec.Integration.EngineOverrides = &cachev1alpha1.EngineInjectionOverrides{
		Args: []string{"--max-model-len", "8192"},
		Env:  []corev1.EnvVar{{Name: "OVERRIDE_FLAG", Value: "should-not-apply"}},
	}
	h := newHandlerWithSubscriber(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	// Snapshot the engine container's pre-admission args/env to compare after.
	wantArgs := append([]string(nil), pod.Spec.Containers[0].Args...)
	wantEnv := append([]corev1.EnvVar(nil), pod.Spec.Containers[0].Env...)
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)

	engine := findContainer(mutated, testVLLMEngineContainerName)
	if engine == nil {
		t.Fatalf("engine container missing; containers = %v", containerNames(mutated))
	}
	// The override args/env must NOT have been applied.
	if argPresent(engine.Args, "--max-model-len") {
		t.Fatalf("events-only must NOT apply engineOverrides args to the engine container; args = %v", engine.Args)
	}
	for _, e := range engine.Env {
		if e.Name == "OVERRIDE_FLAG" {
			t.Fatalf("events-only must NOT apply engineOverrides env to the engine container; found %s=%q", e.Name, e.Value)
		}
	}
	// The engine container's args/env are unchanged from the inbound template.
	if !reflect.DeepEqual(engine.Args, wantArgs) {
		t.Fatalf("events-only engine args mutated: got %v, want %v (unchanged)", engine.Args, wantArgs)
	}
	if !reflect.DeepEqual(engine.Env, wantEnv) {
		t.Fatalf("events-only engine env mutated: got %v, want %v (unchanged)", engine.Env, wantEnv)
	}
	// The subscriber IS still injected (image configured) and the pod is wired,
	// so injected-by is stamped — confirms the engine-untouched guarantee is
	// independent of the sidecar-append path.
	if sub := findContainer(mutated, enginebinding.SubscriberContainerName); sub == nil {
		t.Fatalf("subscriber sidecar must still attach for a configured events-only backend; containers = %v", containerNames(mutated))
	}
	if got, want := mutated.Annotations[AnnotationInjectedBy], ns+"/"+cb.Name; got != want {
		t.Fatalf("annotation %s: got %q, want %q", AnnotationInjectedBy, got, want)
	}
}

func TestHandle_ManagedBackend_StatusEmpty_FailsOpen(t *testing.T) {
	// Counterpart to the external-ownership path: managed backends MUST wait
	// for status.remoteStorage.endpoint (the reconciler builds it from the rendered
	// Service). spec.remoteStorage.endpoint is admission-rejected for managed
	// ownership, so there's nothing else to fall back on — the webhook must
	// fail-open without injecting until status catches up.
	const ns = "engines"
	cb := readyCacheBackend("managed", ns, map[string]string{"app": "vllm"})
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}
	cb.Status.RemoteStorage = nil
	// No status.remoteStorage.endpoint has been published yet.

	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-managed", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	// The pod must remain entirely unmodified because the managed endpoint has
	// not been observed yet.
	mutated := applyPatches(t, req.Object.Raw, resp)
	if findInitContainerByName(mutated.Spec.InitContainers, "lmcache-mp-server") != nil || containsArgFlag(mutated.Spec.Containers[0].Args, "--kv-transfer-config") {
		t.Fatalf("managed CR with no status.remoteStorage.endpoint unexpectedly injected MP wiring: %+v", mutated.Spec)
	}
}

func TestHandle_ExternalBackend_NoSidecar(t *testing.T) {
	// Negative case: a CacheBackend matched by a
	// runtime whose adapter returns no sidecar (the reference adapter here,
	// standing in for any future External-type adapter) admits the pod
	// without appending a kvevent-subscriber container.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntime(testRuntimeReference)
	cb.Spec.Type = cachev1alpha1.CacheBackendType("reference")
	cb.Spec.LMCache = nil
	cb.Spec.RemoteStorage = externalRedisStorage("redis.example:6379")
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
	reg := adapterruntime.NewRegistry()
	reg.Register(referenceRuntimeAdapter{})
	h := &EngineInjector{Reader: c, Registry: reg, Log: logr.Discard()}
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if c := findContainer(mutated, enginebinding.SubscriberContainerName); c != nil {
		t.Fatalf("External-style backend must NOT get a subscriber sidecar; found %+v", c)
	}
}

func TestHandle_SidecarOptInDefaultsToNoSidecar(t *testing.T) {
	// Default install must NOT auto-attach: when the controller flag is
	// unset, the registry's vLLM adapter renders no sidecar even with a
	// model configured. This protects operators who install the controller
	// without yet shipping a subscriber image — engine pods stay
	// single-container and the cache is purely opt-in for now.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "MyOrg/MyModel"}
	h := newHandler(t, cb) // built-in registry, with no subscriber image
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("engine injection must still happen; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if c := findContainer(mutated, enginebinding.SubscriberContainerName); c != nil {
		t.Fatalf("default install must NOT auto-attach the sidecar; got %+v", c)
	}
	mustHaveArgFlag(t, mutated, "--kv-transfer-config")
}

func TestHandle_SidecarSkippedWithoutModel(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	// Sidecar opt-in via the configured handler, but no observation.modelID
	// — adapter returns (nil, nil) so the engine wiring still happens
	// while the sidecar append is skipped.
	h := newHandlerWithSubscriber(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("engine injection must still happen; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if c := findContainer(mutated, enginebinding.SubscriberContainerName); c != nil {
		t.Fatalf("sidecar must be skipped without a model id; got %+v", c)
	}
	mustHaveArgFlag(t, mutated, "--kv-transfer-config")
}

func TestHandle_SidecarErrorIsFailOpen(t *testing.T) {
	// If the adapter's ObservationSidecar errors, admission must still
	// succeed and the engine-side injection must still apply — the cache
	// is an optimisation, never a serving dependency.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.Runtime = ""
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntime("stub-fail")
	cb.Spec.Type = cachev1alpha1.CacheBackendType("reference")
	cb.Spec.LMCache = nil
	cb.Spec.RemoteStorage = externalRedisStorage("redis.example:6379")
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
	reg := adapterruntime.NewRegistry()
	reg.Register(sidecarErrorAdapter{})
	h := &EngineInjector{Reader: c, Registry: reg, Log: logr.Discard()}
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed (fail-open on sidecar error); got %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	mustHaveEnv(t, mutated, "STUB_INJECTED", "yes")
	if c := findContainer(mutated, enginebinding.SubscriberContainerName); c != nil {
		t.Fatalf("sidecar errored — webhook must not append a partial container, got %+v", c)
	}
}

func TestHandle_PreExistingSidecar_NotDuplicated(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "MyOrg/MyModel"}
	h := newHandlerWithSubscriber(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
		Name:  enginebinding.SubscriberContainerName,
		Image: "operator/pre-baked:tag",
	})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed; got %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	subs := 0
	for _, c := range mutated.Spec.Containers {
		if c.Name == enginebinding.SubscriberContainerName {
			subs++
		}
	}
	if subs != 1 {
		t.Fatalf("expected exactly one %s container after admission, got %d: %v",
			enginebinding.SubscriberContainerName, subs, containerNames(mutated))
	}
}

// sidecarErrorAdapter is a stub adapter whose ObservationSidecar always
// errors so the webhook's fail-open path on the sidecar branch is exercised.
type sidecarErrorAdapter struct{}

func (sidecarErrorAdapter) Supports(adapterruntime.RuntimeID, *cachev1alpha1.CacheBackend) bool {
	return true
}

func (sidecarErrorAdapter) ResolveCacheServer(*cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return nil, nil, nil
}

func (sidecarErrorAdapter) SupportsBinding(*backendadapter.Binding) bool { return true }

func (sidecarErrorAdapter) InjectEngineConfig(pod *corev1.PodSpec, _ *backendadapter.Binding, _ *cachev1alpha1.CacheBackend) error {
	if pod == nil || len(pod.Containers) == 0 {
		return errors.New("nope")
	}
	pod.Containers[0].Env = append(pod.Containers[0].Env, corev1.EnvVar{Name: "STUB_INJECTED", Value: "yes"})
	return nil
}

func (sidecarErrorAdapter) InjectRouterConfig(*corev1.PodSpec, *backendadapter.Binding, *cachev1alpha1.CacheBackend) error {
	return nil
}

func (sidecarErrorAdapter) ObservationSidecar(*cachev1alpha1.CacheBackend, *corev1.Pod) (*corev1.Container, error) {
	return nil, errors.New("synthetic sidecar render failure")
}

func (sidecarErrorAdapter) ReservedArgs() []string      { return nil }
func (sidecarErrorAdapter) ReservedEnv() []string       { return nil }
func (sidecarErrorAdapter) EngineContainerName() string { return testVLLMEngineContainerName }

func findContainer(pod *corev1.Pod, name string) *corev1.Container {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return &pod.Spec.Containers[i]
		}
	}
	return nil
}

func findInitContainerByName(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

func containerNames(pod *corev1.Pod) []string {
	out := make([]string, len(pod.Spec.Containers))
	for i, c := range pod.Spec.Containers {
		out[i] = c.Name
	}
	return out
}

func argPresent(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func TestHandle_NoMatch_Passthrough(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-x", map[string]string{"app": "other"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("expected no patches on pass-through, got %d", len(resp.Patches))
	}
}

func TestHandle_FullyInjected_NoOpPatch(t *testing.T) {
	// When a pod is admitted twice (e.g. via re-admission) the second pass
	// produces an empty patch set: the adapter's upsertEnv/upsertArgPair
	// merges are idempotent, so the second InjectEngineConfig call leaves
	// the spec unchanged. Confirms the handler does NOT depend on an
	// env-presence short-circuit for idempotency — the adapter is the
	// source of truth for the full injected contract.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})

	// First admission produces patches.
	first := h.Handle(context.Background(), newRequest(t, pod, ns))
	if !first.Allowed || len(first.Patches) == 0 {
		t.Fatalf("first admission: Allowed=%v patches=%d", first.Allowed, len(first.Patches))
	}
	injected := applyPatches(t, newRequest(t, pod, ns).Object.Raw, first)

	// Second admission of the already-injected pod is a no-op patch set.
	second := h.Handle(context.Background(), newRequest(t, injected, ns))
	if !second.Allowed {
		t.Fatalf("second admission rejected: %+v", second.Result)
	}
	if len(second.Patches) != 0 {
		t.Fatalf("re-admission of fully-injected pod should emit no patches, got %d: %+v", len(second.Patches), second.Patches)
	}
}

func TestHandle_EndpointNotPublished_FailOpen(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Status.RemoteStorage = nil
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed (fail-open), got: %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("expected no patches on fail-open, got %d", len(resp.Patches))
	}
}

func TestHandle_SkipAnnotation_StampsInjectSkipped(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Annotations = map[string]string{AnnotationSkip: "true"}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	if len(resp.Patches) == 0 {
		t.Fatalf("expected skip-inject path to stamp %s, got no patches", AnnotationInjectSkipped)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if got := mutated.Annotations[AnnotationInjectSkipped]; got != InjectSkippedReasonSkipAnnotation {
		t.Fatalf("annotation %s = %q, want %q", AnnotationInjectSkipped, got, InjectSkippedReasonSkipAnnotation)
	}
}

func TestHandle_EmptyEngineSelector_Skipped(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, nil)
	cb.Spec.EngineSelector = nil
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("nil EngineSelector must not match any pod, got %d patches", len(resp.Patches))
	}
}

func TestHandle_OverlappingSelectors_Rejected(t *testing.T) {
	const ns = "engines"
	cbZebra := readyCacheBackend("zebra", ns, map[string]string{"app": "vllm"})
	cbAlpha := readyCacheBackend("alpha", ns, map[string]string{"app": "vllm", "model": "qwen"})
	h := newHandler(t, cbZebra, cbAlpha)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm", "model": "qwen"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if resp.Allowed {
		t.Fatalf("expected overlapping selectors to deny Pod admission, got Allowed with patches %+v", resp.Patches)
	}
	if resp.Result == nil || !strings.Contains(resp.Result.Message, "multiple CacheBackends") ||
		!strings.Contains(resp.Result.Message, "alpha, zebra") {
		t.Fatalf("denial message = %+v, want deterministic conflicting backend names", resp.Result)
	}
}

func TestHandle_ListError_FailOpen(t *testing.T) {
	const ns = "engines"
	s := newScheme(t)
	wantErr := errors.New("apiserver unavailable")
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return wantErr
			},
		}).
		Build()
	h := &EngineInjector{Reader: c, Log: logr.Discard()}
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed on transient list error (fail-open), got: %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("expected no patches on fail-open, got %d", len(resp.Patches))
	}
}

func TestHandle_NilRegistry_FailsOpen(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
	h := &EngineInjector{Reader: c, Log: logr.Discard()}

	resp := h.Handle(context.Background(), newRequest(t,
		vllmEnginePod("engine-a", map[string]string{"app": "vllm"}), ns))
	if !resp.Allowed || len(resp.Patches) != 0 {
		t.Fatalf("nil registry must fail open without patches: Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	if resp.Result == nil || !strings.Contains(resp.Result.Message, "registry is not configured") {
		t.Fatalf("response message = %v, want missing-registry diagnostic", resp.Result)
	}
}

func TestHandle_AdapterError_FailOpen(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	// Multi-container pod with no container named "vllm" — the vLLM adapter
	// explicitly rejects this rather than mutate sidecars; the handler must
	// fail open and admit the pod unmodified.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "multi", Labels: map[string]string{"app": "vllm"}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "engine", Image: "vllm/vllm-openai-cpu:latest"},
				{Name: "sidecar", Image: "busybox"},
			},
		},
	}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed (fail-open), got: %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("expected no patches on fail-open, got %d", len(resp.Patches))
	}
}

func TestHandle_DecodeError_FailOpen(t *testing.T) {
	h := newHandler(t)
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			UID:       types.UID("decode-fail"),
			Operation: admissionv1.Create,
			Namespace: "ns",
			Object:    runtime.RawExtension{Raw: []byte("not-json")},
		},
	}
	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed on decode error (fail-open), got: %+v", resp.Result)
	}
}

func TestHandle_NoBackendForRuntime_FailOpen(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.Type = cachev1alpha1.CacheBackendType("unsupported") // no built-in adapter
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed (fail-open), got: %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("expected no patches on fail-open, got %d", len(resp.Patches))
	}
}

func TestHandle_RegistryOverride_UsedInsteadOfDefault(t *testing.T) {
	// The handler must consult its Registry if set; install a registry
	// containing only the reference adapter (which writes
	// INFERENCECACHE_CACHE_ENDPOINT, not LMCACHE_*). A successful injection
	// with the reference env on the container proves the override wins.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.Runtime = ""
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntime(testRuntimeReference)
	cb.Spec.Type = cachev1alpha1.CacheBackendType("reference")
	cb.Spec.LMCache = nil
	cb.Spec.RemoteStorage = externalRedisStorage("redis.example:6379")
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
	reg := adapterruntime.NewRegistry()
	reg.Register(referenceRuntimeAdapter{})
	h := &EngineInjector{Reader: c, Registry: reg, Log: logr.Discard()}
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("expected Allowed with patches; got Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	mustHaveEnv(t, mutated, testReferenceCacheEndpoint, cb.Spec.RemoteStorage.Endpoint)
}

func TestHandle_PodNamespaceDefaultedFromRequest(t *testing.T) {
	// During CREATE the apiserver invokes the webhook BEFORE defaulting
	// metadata.namespace from the URL — so the inbound pod typically has
	// pod.Namespace=="" and only req.Namespace is authoritative. The
	// handler must use req.Namespace for the CacheBackend lookup.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Namespace = ""
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("expected match via req.Namespace; got Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
}

func TestSkipAnnotationOptsOut(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"", false},           // empty annotation = no opt-out
		{"true", true},        // canonical truthy
		{"1", true},           // numeric truthy
		{"yes", true},         // free-form truthy
		{"please skip", true}, // free-form note treated as opt-out
		{"false", false},      // explicit falsey
		{"0", false},          // numeric falsey
		{"no", false},         // explicit falsey synonym
		{"OFF", false},        // case-insensitive falsey synonym
	}
	for _, tc := range cases {
		t.Run(tc.val, func(t *testing.T) {
			if got := SkipAnnotationOptsOut(tc.val); got != tc.want {
				t.Fatalf("SkipAnnotationOptsOut(%q): got %v want %v", tc.val, got, tc.want)
			}
		})
	}
}

func TestHandle_SkipAnnotationFalse_StillInjects(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Annotations = map[string]string{
		AnnotationSkip:          "false",
		AnnotationInjectSkipped: InjectSkippedReasonSkipAnnotation,
	}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("explicit skip-inject=false must still inject; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if got := mutated.Annotations[AnnotationInjectSkipped]; got != "" {
		t.Fatalf("annotation %s = %q, want absent when skip-inject=false", AnnotationInjectSkipped, got)
	}
}

func TestHandle_FailOpenClearsStaleInjectSkipped(t *testing.T) {
	const ns = "engines"
	h := newHandler(t /* no CacheBackend seeded, so no selector match */)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Annotations = map[string]string{AnnotationInjectSkipped: InjectSkippedReasonSkipAnnotation}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("fail-open with stale %s must admit with a clearing patch; Allowed=%v patches=%d",
			AnnotationInjectSkipped, resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if got := mutated.Annotations[AnnotationInjectSkipped]; got != "" {
		t.Fatalf("annotation %s = %q, want cleared on fail-open", AnnotationInjectSkipped, got)
	}
}

func TestHandle_SkipAnnotationStampsSkippedReasonAndClearsInjectedBy(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Annotations = map[string]string{
		AnnotationSkip:               "true",
		AnnotationInjectedBy:         ns + "/" + cb.Name,
		AnnotationInjectedByUID:      string(cb.UID),
		AnnotationInjectedGeneration: fmt.Sprint(cb.Generation),
	}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("skip-inject=true must admit with a patch; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if got := mutated.Annotations[AnnotationInjectSkipped]; got != InjectSkippedReasonSkipAnnotation {
		t.Fatalf("annotation %s = %q, want %q", AnnotationInjectSkipped, got, InjectSkippedReasonSkipAnnotation)
	}
	if got := mutated.Annotations[AnnotationInjectedBy]; got != "" {
		t.Fatalf("annotation %s = %q, want cleared on skip path", AnnotationInjectedBy, got)
	}
	if got := mutated.Annotations[AnnotationInjectedByUID]; got != "" {
		t.Fatalf("annotation %s = %q, want cleared on skip path", AnnotationInjectedByUID, got)
	}
	if got := mutated.Annotations[AnnotationInjectedGeneration]; got != "" {
		t.Fatalf("annotation %s = %q, want cleared on skip path", AnnotationInjectedGeneration, got)
	}
}

func mustHaveEnv(t *testing.T, pod *corev1.Pod, name, value string) {
	t.Helper()
	if len(pod.Spec.Containers) == 0 {
		t.Fatalf("no containers")
	}
	c := pod.Spec.Containers[0]
	for _, e := range c.Env {
		if e.Name == name {
			if e.Value != value {
				t.Fatalf("env %s: got %q, want %q", name, e.Value, value)
			}
			return
		}
	}
	t.Fatalf("env %s missing; container env = %v", name, c.Env)
}

func mustHaveArgPair(t *testing.T, pod *corev1.Pod, flag, value string) {
	t.Helper()
	args := pod.Spec.Containers[0].Args
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return
		}
	}
	t.Fatalf("arg pair %s %s missing; args = %v", flag, value, args)
}

func testArgValue(args []string, flag string) string {
	for index, arg := range args {
		if arg == flag && index+1 < len(args) {
			return args[index+1]
		}
		if strings.HasPrefix(arg, flag+"=") {
			return strings.TrimPrefix(arg, flag+"=")
		}
	}
	return ""
}

func mustHaveArgFlag(t *testing.T, pod *corev1.Pod, flag string) {
	t.Helper()
	for _, a := range pod.Spec.Containers[0].Args {
		if a == flag {
			return
		}
	}
	t.Fatalf("arg %s missing; args = %v", flag, pod.Spec.Containers[0].Args)
}

// TestHandle_EngineOverrides_EnvUpsertAndArgAppend drives the full handler
// pipeline through a CacheBackend whose spec.integration.engineOverrides
// adds a new arg, adds an env, and overrides an adapter-owned tunable.
// Pins the admission→merge wiring at the behaviour layer (kubectl-visible
// result), not just the helper's unit tests.
func TestHandle_EngineOverrides_EnvUpsertAndArgAppend(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.Integration.EngineOverrides = &cachev1alpha1.EngineInjectionOverrides{
		Args: []string{"--max-model-len", "8192"},
		Env: []corev1.EnvVar{
			{Name: "FOO", Value: "bar"},
			{Name: "EXTRA_TUNABLE", Value: "512"},
		},
	}
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("expected Allowed with patches; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)

	// New env appended.
	mustHaveEnv(t, mutated, "FOO", "bar")
	mustHaveEnv(t, mutated, "EXTRA_TUNABLE", "512")
	// Canonical typed-MP env still lands unchanged.
	mustHaveEnv(t, mutated, testEnvPythonHashSeed, "0")
	// User-template env preserved.
	mustHaveEnv(t, mutated, "USER_FLAG", "preserved")

	// Added arg present.
	mustHaveArgPair(t, mutated, "--max-model-len", "8192")
	// Reserved arg still injected.
	mustHaveArgFlag(t, mutated, "--kv-transfer-config")
}

// TestHandle_EngineOverrides_DoNotMutateUserTemplate pins the
// adapter-owned scoping at the behaviour layer: a CR-driven Suppress or
// Override that names a user pod-template arg/env the adapter did NOT
// touch is a silent no-op. Catches the regression where the CR could
// silently strip a user's flag or rewrite a user's env value.
func TestHandle_EngineOverrides_DoNotMutateUserTemplate(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.Integration.EngineOverrides = &cachev1alpha1.EngineInjectionOverrides{
		// Try to strip a user flag the adapter doesn't inject.
		SuppressArgs: []string{"--enforce-eager"},
		// Try to rewrite a user env name the adapter doesn't inject.
		Env: []corev1.EnvVar{{Name: "USER_FLAG", Value: "override-wins?"}},
		// Try to suppress the user's own env. Also a no-op.
		SuppressEnv: []string{"USER_FLAG"},
	}
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args, "--enforce-eager")
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)

	// User-owned arg untouched by the CR-driven suppress.
	if !argPresent(mutated.Spec.Containers[0].Args, "--enforce-eager") {
		t.Fatalf("CR suppress wrongly stripped user-owned --enforce-eager; args = %v",
			mutated.Spec.Containers[0].Args)
	}
	// User-owned env untouched by the CR-driven override + suppress.
	mustHaveEnv(t, mutated, "USER_FLAG", "preserved")
	// Canonical injection still landed.
	mustHaveEnv(t, mutated, testEnvPythonHashSeed, "0")
	mustHaveArgFlag(t, mutated, "--kv-transfer-config")
}

// TestHandle_EngineOverrides_NoOverride_ByteIdenticalToBaseline pins the
// backward-compat invariant from locked decision #7: a CacheBackend with no
// engineOverrides block produces an admitted pod byte-identical to the same
// CR reconstructed with EngineOverrides explicitly nil. The handler's
// emitted JSON-patch ops carry no guaranteed ordering (the controller-runtime
// diff implementation walks maps), so we compare what an operator would
// actually observe: the marshalled bytes of the reconstructed pod — the end
// state the apiserver would persist. Catches a reorder/extra container/extra
// env op the override path could leak on the "no override" code path, which
// the previous field-presence checks would have missed.
func TestHandle_EngineOverrides_NoOverride_ByteIdenticalToBaseline(t *testing.T) {
	const ns = "engines"
	// Baseline: no engineOverrides block at all.
	baseline := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	// Equivalent CR: engineOverrides explicitly nil. The CRD serialisation
	// of the two is identical (omitempty), but pinning it at the handler
	// level guards against a future refactor that materialises an empty
	// EngineInjectionOverrides struct mid-flight.
	withNilOverride := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	withNilOverride.Spec.Integration.EngineOverrides = nil

	mutatedRaw := make([][]byte, 0, 2)
	for _, cb := range []*cachev1alpha1.CacheBackend{baseline, withNilOverride} {
		h := newHandler(t, cb)
		pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
		req := newRequest(t, pod, ns)

		resp := h.Handle(context.Background(), req)
		if !resp.Allowed {
			t.Fatalf("expected Allowed, got %+v", resp.Result)
		}
		mutated := applyPatches(t, req.Object.Raw, resp)
		// Sanity: canonical injection lands as expected — so a green test
		// is meaningful (not green by producing an empty patch set).
		mustHaveEnv(t, mutated, testEnvPythonHashSeed, "0")
		mustHaveArgFlag(t, mutated, "--kv-transfer-config")

		raw, err := json.Marshal(mutated)
		if err != nil {
			t.Fatalf("marshal mutated pod: %v", err)
		}
		mutatedRaw = append(mutatedRaw, raw)
	}
	if !bytes.Equal(mutatedRaw[0], mutatedRaw[1]) {
		t.Fatalf("no-override CR and explicit-nil-override CR produced different admitted pods\nbaseline:     %s\nexplicit-nil: %s",
			string(mutatedRaw[0]), string(mutatedRaw[1]))
	}
}

func TestHandle_FailOpenClearsForgedInjectedByAnnotation(t *testing.T) {
	// The AnnotationInjectedBy annotation is user-controllable. Anyone
	// with pod-create RBAC can set it; the webhook does NOT overwrite
	// it on fail-open paths. The engine-pod-events controller treats
	// the annotation as the authoritative "this pod was injected"
	// signal — so a forged or copy-pasted annotation on a pod that
	// never goes through real injection would falsely trigger
	// `InjectedByCacheBackend`. Fix: on fail-open, the webhook strips
	// the annotation if it was preset. The common steady-state path
	// (pod has no annotation) stays at zero patches (covered by the
	// no-forged-annotation test below).
	const ns = "engines"
	cases := []struct {
		name   string
		seedCB bool
		labels map[string]string
	}{
		{name: "no matching CacheBackend", seedCB: false, labels: map[string]string{"app": "router"}},
		{name: "selector matches but endpoint not published", seedCB: true, labels: map[string]string{"app": "vllm"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var h *EngineInjector
			if tc.seedCB {
				cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
				cb.Status.RemoteStorage = nil // exercise fail-open while optional Redis is unavailable
				h = newHandler(t, cb)
			} else {
				h = newHandler(t)
			}

			pod := vllmEnginePod("forger", tc.labels)
			pod.Annotations = map[string]string{AnnotationInjectedBy: ns + "/totally-not-a-real-cb"}
			req := newRequest(t, pod, ns)

			resp := h.Handle(context.Background(), req)
			if !resp.Allowed {
				t.Fatalf("expected Allowed (fail-open): %+v", resp.Result)
			}
			if len(resp.Patches) == 0 {
				t.Fatalf("expected a clearing JSON patch on the fail-open path; got 0 patches")
			}

			mutated := applyPatches(t, req.Object.Raw, resp)
			if got := mutated.Annotations[AnnotationInjectedBy]; got != "" {
				t.Fatalf("forged %s annotation survived fail-open: got %q, want \"\"", AnnotationInjectedBy, got)
			}
		})
	}
}

func TestHandle_FailOpenZeroPatchesWithoutForgedAnnotation(t *testing.T) {
	// The steady-state no-match path on a cluster-wide pod (no
	// engine-related annotations) must remain zero-patches, otherwise
	// the webhook would generate JSON-patch traffic for every Pod
	// CREATE in the cluster just to clear an annotation nobody set.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("unrelated", map[string]string{"app": "router"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed: %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("expected zero patches on no-match without forged annotation; got %d", len(resp.Patches))
	}
}

// TestHandle_KernelCheckInitContainer_AppendedOnGPUPod verifies that a GPU-
// requesting engine pod bound to a managed LMCache CacheBackend gets the
// kernel-check init container appended in Spec.InitContainers. The init
// container name must match the constant from the adapter package so the
// reconciler and the webhook share a single source of truth.
func TestHandle_KernelCheckInitContainer_AppendedOnGPUPod(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)

	// GPU-requesting engine pod: auto mode injects only when nvidia.com/gpu is
	// present. Build it from the canonical helper then add the GPU resource.
	pod := vllmEnginePod("engine-gpu", map[string]string{"app": "vllm"})
	pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("1"),
		},
	}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)

	// The kernel-check init container must be present.
	found := false
	for _, ic := range mutated.Spec.InitContainers {
		if ic.Name == enginebinding.LMCacheKernelCheckContainerName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("kernel-check init container %q missing from Spec.InitContainers; got: %v",
			enginebinding.LMCacheKernelCheckContainerName, initContainerNames(mutated))
	}
	// Engine-side typed MP injection must still have landed.
	mustHaveArgFlag(t, mutated, "--kv-transfer-config")
}

// TestHandle_KernelCheckInitContainer_Idempotent verifies that a second
// admission of a pod that already carries the kernel-check init container does
// NOT double-append it. Exactly one init container by that name must be present.
func TestHandle_KernelCheckInitContainer_Idempotent(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)

	pod := vllmEnginePod("engine-gpu", map[string]string{"app": "vllm"})
	pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("1"),
		},
	}

	// First admission injects the init container.
	first := h.Handle(context.Background(), newRequest(t, pod, ns))
	injected := applyPatches(t, newRequest(t, pod, ns).Object.Raw, first)

	count := 0
	for _, ic := range injected.Spec.InitContainers {
		if ic.Name == enginebinding.LMCacheKernelCheckContainerName {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 kernel-check init container after first admission, got %d: %v",
			count, initContainerNames(injected))
	}

	// Second admission of the already-injected pod must not duplicate it.
	second := h.Handle(context.Background(), newRequest(t, injected, ns))
	if !second.Allowed {
		t.Fatalf("re-admission rejected: %+v", second.Result)
	}
	readmitted := applyPatches(t, newRequest(t, injected, ns).Object.Raw, second)

	count = 0
	for _, ic := range readmitted.Spec.InitContainers {
		if ic.Name == enginebinding.LMCacheKernelCheckContainerName {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 kernel-check init container after re-admission, got %d: %v",
			count, initContainerNames(readmitted))
	}
}

// TestHandle_EventsOnly_StripsPreexistingKernelCheckInitContainer verifies that
// an events-only engine pod carrying a stale/hand-authored lmcache-kernel-check
// init container has it REMOVED on admission. Events-only loads no LMCache
// connector, so the kernel check is irrelevant — and the webhook is
// authoritative for that container, so a leftover strict check must not survive
// to block the pod in Init or be trusted by the controller.
func TestHandle_EventsOnly_StripsPreexistingKernelCheckInitContainer(t *testing.T) {
	const ns = "engines"
	cb := eventsOnlyCacheBackend("routing-only", ns, map[string]string{"app": "vllm"})
	h := newHandlerWithSubscriber(t, cb) // subscriber image set → the pod is wired (patches returned)

	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{
		Name:  enginebinding.LMCacheKernelCheckContainerName,
		Image: "stale-hand-baked-kernel-check:latest",
	})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)

	for _, ic := range mutated.Spec.InitContainers {
		if ic.Name == enginebinding.LMCacheKernelCheckContainerName {
			t.Fatalf("stale kernel-check init container survived events-only admission; init containers = %v",
				initContainerNames(mutated))
		}
	}
	// The subscriber sidecar is still wired (this is a normal events-only inject).
	if findContainer(mutated, enginebinding.SubscriberContainerName) == nil {
		t.Fatalf("subscriber sidecar missing; containers = %v", containerNames(mutated))
	}
}

func TestHandle_KernelCheckInitContainer_ReplacesForged(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)

	pod := vllmEnginePod("engine-gpu", map[string]string{"app": "vllm"})
	pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
	}
	// A hand-authored / forged same-name init container that would bypass the
	// real check if the webhook merely skipped injection (e.g. a fake "OK").
	pod.Spec.InitContainers = []corev1.Container{{
		Name:    enginebinding.LMCacheKernelCheckContainerName,
		Image:   "attacker/fake:latest",
		Command: []string{"echo", "OK"},
	}}

	resp := h.Handle(context.Background(), newRequest(t, pod, ns))
	injected := applyPatches(t, newRequest(t, pod, ns).Object.Raw, resp)

	var got *corev1.Container
	count := 0
	for i := range injected.Spec.InitContainers {
		if injected.Spec.InitContainers[i].Name == enginebinding.LMCacheKernelCheckContainerName {
			got = &injected.Spec.InitContainers[i]
			count++
		}
	}
	if count != 1 || got == nil {
		t.Fatalf("expected exactly 1 kernel-check init container, got %d: %v", count, initContainerNames(injected))
	}
	// The webhook is authoritative: it must REPLACE the forged container with the
	// real rendered one (real interpreter command + the engine's image).
	if len(got.Command) == 0 || got.Command[0] != "python3" {
		t.Errorf("forged kernel-check container not replaced; command = %v, want python3 ...", got.Command)
	}
	if got.Image == "attacker/fake:latest" {
		t.Error("forged kernel-check image survived; the webhook must reuse the engine image")
	}
}

func TestHandle_KernelCheckInitContainer_StrippedWhenAdapterDeclines(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)

	// A CPU engine pod (no GPU request) under the default `auto` mode: the
	// adapter declines to inject. A hand-authored same-name init container must
	// be STRIPPED so it can't masquerade as a real check (the controller trusts
	// the container by name), preserving "auto on a non-GPU pod = absent".
	pod := vllmEnginePod("engine-cpu", map[string]string{"app": "vllm"})
	pod.Spec.InitContainers = []corev1.Container{{
		Name:    enginebinding.LMCacheKernelCheckContainerName,
		Image:   "attacker/fake:latest",
		Command: []string{"echo", "OK"},
	}}

	resp := h.Handle(context.Background(), newRequest(t, pod, ns))
	injected := applyPatches(t, newRequest(t, pod, ns).Object.Raw, resp)

	for _, ic := range injected.Spec.InitContainers {
		if ic.Name == enginebinding.LMCacheKernelCheckContainerName {
			t.Fatalf("forged kernel-check init container survived on a non-GPU (auto) pod; want it stripped: %v",
				initContainerNames(injected))
		}
	}
}

// TestHandle_KernelCheckInitContainer_SkipAnnotationSuppresses verifies that
// setting AnnotationSkip on a pod suppresses the kernel-check init container
// injection. The skip path still stamps AnnotationInjectSkipped (so the events
// controller can distinguish an operator opt-out from a no-match), but it must
// not inject any engine wiring or init container.
func TestHandle_KernelCheckInitContainer_SkipAnnotationSuppresses(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)

	pod := vllmEnginePod("engine-gpu", map[string]string{"app": "vllm"})
	pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("1"),
		},
	}
	pod.Annotations = map[string]string{AnnotationSkip: "true"}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed (skip annotation), got: %+v", resp.Result)
	}
	// The skip path stamps AnnotationInjectSkipped (so the events controller
	// can tell an operator opt-out apart from a no-match), but it must inject
	// NOTHING else — in particular no kernel-check init container.
	mutated := applyPatches(t, req.Object.Raw, resp)
	if got := mutated.Annotations[AnnotationInjectSkipped]; got != InjectSkippedReasonSkipAnnotation {
		t.Fatalf("annotation %s = %q, want %q", AnnotationInjectSkipped, got, InjectSkippedReasonSkipAnnotation)
	}
	for _, ic := range mutated.Spec.InitContainers {
		if ic.Name == enginebinding.LMCacheKernelCheckContainerName {
			t.Fatalf("kernel-check init container must be absent when skip annotation is set; found %+v", ic)
		}
	}
}

// TestHandle_KernelCheckInitContainer_SkippedForEventsOnly verifies that an
// events-only backend does NOT get the lmcache-kernel-check init container even
// when the kernel-check is forced on (strict mode + a GPU-requesting pod, which
// would normally inject it). Events-only loads no LMCache KV connector, so the
// native-kernel check is irrelevant — and in strict mode the init container's
// non-zero exit would hold the engine pod in Init and block it from serving.
func TestHandle_KernelCheckInitContainer_SkippedForEventsOnly(t *testing.T) {
	const ns = "engines"
	cb := eventsOnlyCacheBackend("routing-only", ns, map[string]string{"app": "vllm"})
	// Force the kernel-check on the strongest way possible: strict mode. On an
	// Offload backend this would inject (and, on a strict failure, block the
	// pod). Events-only must skip it regardless.
	cb.Annotations = map[string]string{
		enginebinding.AnnotationLMCacheKernelCheck: enginebinding.KernelCheckModeStrict,
	}
	h := newHandlerWithSubscriber(t, cb)

	// GPU-requesting engine pod — auto mode would also inject for this, so strict
	// + GPU is the maximal trigger condition.
	pod := vllmEnginePod("engine-gpu", map[string]string{"app": "vllm"})
	pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
	}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)

	// No kernel-check init container for events-only.
	for _, ic := range mutated.Spec.InitContainers {
		if ic.Name == enginebinding.LMCacheKernelCheckContainerName {
			t.Fatalf("events-only pod must NOT get the %q init container; init containers = %v",
				enginebinding.LMCacheKernelCheckContainerName, initContainerNames(mutated))
		}
	}

	// Sanity: the events-only wiring still happened (subscriber sidecar
	// appended), so this is not a fail-open no-op masquerading as a skip.
	if sub := findContainer(mutated, enginebinding.SubscriberContainerName); sub == nil {
		t.Fatalf("events-only subscriber sidecar missing; containers = %v", containerNames(mutated))
	}
}

// TestHandle_KernelCheckInitContainer_AppendedOnOffloadStrict is the regression
// counterpart to the events-only skip above: an Offload (default-mode) backend
// with the same strict annotation + GPU pod STILL gets the kernel-check init
// container. This pins the skip to events-only mode, not a blanket suppression.
func TestHandle_KernelCheckInitContainer_AppendedOnOffloadStrict(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Annotations = map[string]string{
		enginebinding.AnnotationLMCacheKernelCheck: enginebinding.KernelCheckModeStrict,
	}
	h := newHandler(t, cb)

	pod := vllmEnginePod("engine-gpu", map[string]string{"app": "vllm"})
	pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
	}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)

	found := false
	for _, ic := range mutated.Spec.InitContainers {
		if ic.Name == enginebinding.LMCacheKernelCheckContainerName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Offload (strict) pod must still get the %q init container; init containers = %v",
			enginebinding.LMCacheKernelCheckContainerName, initContainerNames(mutated))
	}
}

// TestHandle_EventsOnlyExternal_NoConnectorWiring verifies that an
// admission-bypassed external-storage + mode=EventsOnly object gets NO KV
// connector wiring. Admission's rejectEventsOnlyMisconfiguration rejects this
// pair, so it can only reach the webhook via a stored / bypassed object — but if
// it does, events-only's "no connector" contract must win over remote storage. The
// vLLM+LMCache adapter would otherwise inject the LMCache
// connector; the webhook skips InjectEngineConfig for events-only regardless of
// the selected adapter.
func TestHandle_EventsOnlyExternal_NoConnectorWiring(t *testing.T) {
	const (
		ns       = "engines"
		endpoint = "external-cache.example:8200"
	)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ext-eo",
			Namespace: ns,
			UID:       types.UID("cb-ext-eo-uid"),
		},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime:       cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:          cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: externalRedisStorage(endpoint),
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Mode: cachev1alpha1.CacheBackendIntegrationModeEventsOnly,
				Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "vllm"},
			},
			Observation: &cachev1alpha1.CacheBackendObservationSpec{ModelID: "Qwen/Qwen2.5-0.5B-Instruct"},
		},
	}

	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
	// Configure the shipping vLLM adapter's subscriber image so the events-only
	// sidecar path is live.
	reg := newVLLMRegistry(builtinruntime.SubscriberConfig{Image: testSubscriberImage})
	h := &EngineInjector{Reader: c, Registry: reg, Log: logr.Discard()}

	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)

	engine := findContainer(mutated, testVLLMEngineContainerName)
	if engine == nil {
		t.Fatalf("engine container missing; containers = %v", containerNames(mutated))
	}
	// No LMCACHE_* env (the external remote binding would otherwise set LMCACHE_REMOTE_URL).
	for _, e := range engine.Env {
		if strings.HasPrefix(e.Name, "LMCACHE_") {
			t.Fatalf("events-only+External engine container must carry NO LMCACHE_* env; found %s=%q", e.Name, e.Value)
		}
	}
	// No --kv-transfer-config connector arg.
	for _, a := range engine.Args {
		if a == "--kv-transfer-config" {
			t.Fatalf("events-only+External engine container must carry NO --kv-transfer-config; args = %v", engine.Args)
		}
	}
	// No kernel-check init container either; assert the contract end-to-end.
	for _, ic := range mutated.Spec.InitContainers {
		if ic.Name == enginebinding.LMCacheKernelCheckContainerName {
			t.Fatalf("events-only+External pod must NOT get the kernel-check init container; init containers = %v",
				initContainerNames(mutated))
		}
	}
}

// initContainerNames returns init container names for diagnostic messages.
func initContainerNames(pod *corev1.Pod) []string {
	out := make([]string, len(pod.Spec.InitContainers))
	for i, c := range pod.Spec.InitContainers {
		out[i] = c.Name
	}
	return out
}

// pin the GroupVersionKind so a future api/v1alpha1 split (e.g. moving
// CacheBackend out of the unversioned core scheme) doesn't silently break
// the webhook's client.List call.
func TestCacheBackendGVKRegistered(t *testing.T) {
	s := newScheme(t)
	gvks, _, err := s.ObjectKinds(&cachev1alpha1.CacheBackend{})
	if err != nil {
		t.Fatalf("ObjectKinds: %v", err)
	}
	want := schema.GroupVersionKind{Group: "inferencecache.io", Version: "v1alpha1", Kind: "CacheBackend"}
	for _, g := range gvks {
		if g == want {
			return
		}
	}
	t.Fatalf("missing GVK %v; got %v", want, gvks)
}
