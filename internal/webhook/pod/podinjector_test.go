package pod

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/go-logr/logr"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
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
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

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
				Name:  adapterruntime.EngineContainerName,
				Image: "vllm/vllm-openai-cpu:latest",
				Env: []corev1.EnvVar{
					{Name: "USER_FLAG", Value: "preserved"},
				},
				Args: []string{"--model", "Qwen/Qwen2.5-0.5B-Instruct"},
			}},
		},
	}
}

// readyCacheBackend returns a CacheBackend with status.endpoint published,
// a vLLM integration, and an EngineSelector keyed on a single label.
func readyCacheBackend(name, namespace string, selector map[string]string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: selector},
		},
		Status: cachev1alpha1.CacheBackendStatus{
			Endpoint: name + ".cache-ns.svc.cluster.local:65432",
			Health:   cachev1alpha1.CacheBackendHealthReady,
		},
	}
}

func newHandler(t *testing.T, objs ...client.Object) *EngineInjector {
	t.Helper()
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	return &EngineInjector{
		Reader: c,
		Log:    logr.Discard(),
	}
}

func TestHandle_MatchAndInject(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	if len(resp.Patches) == 0 {
		t.Fatalf("expected JSON patches, got none")
	}

	mutated := applyPatches(t, req.Object.Raw, resp)
	mustHaveEnv(t, mutated, "USER_FLAG", "preserved")
	mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheRemoteURL,
		"lm://"+cb.Status.Endpoint)
	mustHaveEnv(t, mutated, adapterruntime.EnvVLLMUseV1, "1")
	if got, want := mutated.Annotations[AnnotationInjectedBy], ns+"/"+cb.Name; got != want {
		t.Fatalf("annotation %s: got %q, want %q", AnnotationInjectedBy, got, want)
	}
	mustHaveArgPair(t, mutated, "--model", "Qwen/Qwen2.5-0.5B-Instruct")
	mustHaveArgFlag(t, mutated, "--kv-transfer-config")
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

func TestHandle_PartialEnvOnly_StillConverges(t *testing.T) {
	// Regression for the round-5 Codex finding: a pod that already carries
	// LMCACHE_REMOTE_URL but is missing the rest of the contract (no
	// VLLM_USE_V1, no --kv-transfer-config arg) MUST still get the
	// remaining wiring filled in. A lenient env-presence short-circuit
	// would leave the pod permanently misconfigured; the adapter is the
	// source of truth and we always call it.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  adapterruntime.EnvLMCacheRemoteURL,
		Value: "lm://stale.example:65432",
	})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("partial-wired pod must still get the missing fields; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	// The stale URL is overwritten with the canonical one for the matched
	// backend, and the missing pieces are added.
	mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheRemoteURL, "lm://"+cb.Status.Endpoint)
	mustHaveEnv(t, mutated, adapterruntime.EnvVLLMUseV1, "1")
	mustHaveArgFlag(t, mutated, "--kv-transfer-config")
}

func TestHandle_EndpointNotPublished_FailOpen(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Status.Endpoint = ""
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

func TestHandle_SkipAnnotation_Passthrough(t *testing.T) {
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
	if len(resp.Patches) != 0 {
		t.Fatalf("expected no patches when opt-out annotation set, got %d", len(resp.Patches))
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

// TestHandle_OverlappingSelectors_FirstNameWins exercises the shared
// attribution rule between the pod webhook and the CacheIndex poller:
// when two CacheBackends in the same namespace have overlapping
// EngineSelectors that both match the engine pod, BOTH surfaces must
// pick the same backend — the one sorted first by metadata.name —
// otherwise the engine is wired to one backend's endpoint while
// status.indexParticipation reports the other as the owner.
//
// The matching poller-side assertion lives in
// TestRefreshOverlappingSelectorsFirstNameWins
// (internal/controller/cacheindex_controller_test.go).
func TestHandle_OverlappingSelectors_FirstNameWins(t *testing.T) {
	const ns = "engines"
	// Create the backends in non-alphabetical order so a name-sort is
	// observably different from raw List order.
	cbZebra := readyCacheBackend("zebra", ns, map[string]string{"app": "vllm"})
	cbAlpha := readyCacheBackend("alpha", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cbZebra, cbAlpha)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got: %+v", resp.Result)
	}
	if len(resp.Patches) == 0 {
		t.Fatal("expected injection patches when at least one backend matches")
	}
	// The `inferencecache.io/injected-by` annotation records who claimed
	// the pod. Sorted-by-name "alpha" must win over "zebra".
	want := ns + "/alpha"
	var got string
	for _, p := range resp.Patches {
		if p.Path == "/metadata/annotations" || p.Path == "/metadata/annotations/"+jsonPatchEscape(AnnotationInjectedBy) {
			if anno, ok := p.Value.(map[string]any); ok {
				if v, ok := anno[AnnotationInjectedBy].(string); ok {
					got = v
				}
			} else if s, ok := p.Value.(string); ok {
				got = s
			}
		}
	}
	if got != want {
		t.Fatalf("injected-by annotation = %q, want %q (deterministic name-sort: alpha < zebra)", got, want)
	}
}

// jsonPatchEscape is the JSON-Pointer escaping for "/" and "~" in JSON
// Patch paths (RFC 6901). Used here only to match the annotation path
// regardless of how the controller-runtime patch emitter renders it.
func jsonPatchEscape(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	s = strings.ReplaceAll(s, "/", "~1")
	return s
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
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeAIBrix // no adapter in DefaultRegistry
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
	cb.Spec.Integration.Engine = string(adapterruntime.RuntimeReference)
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
	reg := adapterruntime.NewRegistry()
	reg.Register(adapterruntime.NewReferenceAdapter())
	h := &EngineInjector{Reader: c, Registry: reg, Log: logr.Discard()}
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("expected Allowed with patches; got Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	mustHaveEnv(t, mutated, adapterruntime.EnvCacheEndpoint, cb.Status.Endpoint)
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
			if got := skipAnnotationOptsOut(tc.val); got != tc.want {
				t.Fatalf("skipAnnotationOptsOut(%q): got %v want %v", tc.val, got, tc.want)
			}
		})
	}
}

func TestHandle_SkipAnnotationFalse_StillInjects(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Annotations = map[string]string{AnnotationSkip: "false"}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("explicit skip-inject=false must still inject; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
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

func mustHaveArgFlag(t *testing.T, pod *corev1.Pod, flag string) {
	t.Helper()
	for _, a := range pod.Spec.Containers[0].Args {
		if a == flag {
			return
		}
	}
	t.Fatalf("arg %s missing; args = %v", flag, pod.Spec.Containers[0].Args)
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
