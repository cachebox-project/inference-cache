package pod

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	externaladapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime/external"
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

// newHandlerWithSubscriber returns a handler whose registry has the
// kvevent-subscriber image configured, opting in to the sidecar auto-attach
// path. Tests that exercise the sidecar behaviour use this helper; tests
// that only need the engine config injection (or that want to confirm the
// no-image default produces no sidecar) use newHandler.
func newHandlerWithSubscriber(t *testing.T, objs ...client.Object) *EngineInjector {
	t.Helper()
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	reg := adapterruntime.DefaultRegistry(
		adapterruntime.WithSubscriberImage(adapterruntime.DefaultSubscriberImage),
	)
	return &EngineInjector{
		Reader:   c,
		Registry: reg,
		Log:      logr.Discard(),
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

func TestHandle_AppendsObservationSidecar(t *testing.T) {
	// The vLLM/LMCache adapter returns a kvevent-subscriber sidecar
	// the webhook MUST append after InjectEngineConfig, with identity flags
	// derived from the CR + pod. This is the one test that pins the end-to-
	// end auto-attach behaviour at the admission boundary. Auto-attach is
	// opt-in via the controller flag; the handler helper here mirrors that
	// wiring.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.BackendConfig = map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"}
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
	sub := findContainer(mutated, adapterruntime.SubscriberContainerName)
	if sub == nil {
		t.Fatalf("subscriber sidecar missing; containers = %v", containerNames(mutated))
	}
	if !argPresent(sub.Args, "--model-id=Qwen/Qwen2.5-0.5B-Instruct") {
		t.Fatalf("--model-id derived from cb.spec.backendConfig.model missing; args = %v", sub.Args)
	}
	if !argPresent(sub.Args, "--replica-id=$(POD_NAME)") {
		t.Fatalf("--replica-id MUST use downward-API POD_NAME; args = %v", sub.Args)
	}
	if !argPresent(sub.Args, "--tenant-id=$(POD_NAMESPACE)") {
		t.Fatalf("--tenant-id MUST use downward-API POD_NAMESPACE; args = %v", sub.Args)
	}
	// The engine container is still wired with LMCache env — appending the
	// sidecar must not regress the engine-side injection.
	mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheRemoteURL, "lm://"+cb.Status.Endpoint)
}

func TestHandle_SidecarAppendIsIdempotent(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.BackendConfig = map[string]string{"model": "MyOrg/MyModel"}
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

func TestHandle_ExternalBackend_InjectsOperatorEndpoint(t *testing.T) {
	// A pod that matches an External CR's engine selector must come out
	// of admission wired to the operator-supplied endpoint via the
	// LMCache engine wire format — the controller doesn't render a
	// Service for the cache, so the only source of truth for the
	// address is spec.endpoint (mirrored to status.endpoint by
	// reconcileExternal).
	const (
		ns       = "engines"
		endpoint = "external-cache.example:8200"
	)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "ext", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: endpoint,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "vllm"},
			},
		},
		Status: cachev1alpha1.CacheBackendStatus{Endpoint: endpoint},
	}

	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
	// Mirror what cmd/controller wires: DefaultRegistry + the External
	// adapter registered on top. Without External in the registry the
	// webhook would fail-open with "no adapter" and leave the engine
	// unwired — that's the very gap the External adapter closes.
	reg := adapterruntime.DefaultRegistry()
	reg.Register(externaladapter.NewAdapter())
	h := &EngineInjector{Reader: c, Registry: reg, Log: logr.Discard()}

	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)

	// LMCACHE_REMOTE_URL must be the operator-supplied endpoint with the
	// lm:// scheme prepended, identical to what the managed adapter
	// would write for the same endpoint.
	mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheRemoteURL, "lm://"+endpoint)
	mustHaveEnv(t, mutated, adapterruntime.EnvVLLMUseV1, "1")
	// User --model arg survives the merge — the adapter only adds; it
	// never clobbers user-set args.
	if !containsArgPairLocal(mutated.Spec.Containers[0].Args, "--model", "Qwen/Qwen2.5-0.5B-Instruct") {
		t.Fatalf("user --model arg was lost; args = %v", mutated.Spec.Containers[0].Args)
	}
	// External path attaches no observation sidecar — the controller has
	// no observability seam into an operator-managed cache.
	if c := findContainer(mutated, adapterruntime.SubscriberContainerName); c != nil {
		t.Fatalf("External backend must NOT get a subscriber sidecar; found %+v", c)
	}
	if mutated.Annotations[AnnotationInjectedBy] != ns+"/ext" {
		t.Fatalf("annotation %s = %q, want %q",
			AnnotationInjectedBy, mutated.Annotations[AnnotationInjectedBy], ns+"/ext")
	}
}

func TestHandle_ExternalBackend_StatusEmpty_FallsBackToSpec(t *testing.T) {
	// Pod admission is CREATE-only — if an engine pod admits before the
	// controller has mirrored spec.endpoint into status.endpoint, the
	// webhook would fail-open and leave the pod unwired *forever* (no
	// re-admission on subsequent status updates). For External CRs the
	// authoritative source is spec.endpoint, so the webhook falls back
	// to it. Without this fallback, applying the External CacheBackend
	// and the engine Deployment in the same kubectl apply silently
	// produces unwired engine pods.
	const (
		ns       = "engines"
		endpoint = "external-cache.example:8200"
	)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "ext", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: endpoint,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "vllm"},
			},
		},
		// Deliberately no Status: simulates the race where pod admission
		// fires before reconcileExternal has patched status.endpoint.
	}

	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
	reg := adapterruntime.DefaultRegistry()
	reg.Register(externaladapter.NewAdapter())
	h := &EngineInjector{Reader: c, Registry: reg, Log: logr.Discard()}

	pod := vllmEnginePod("engine-race", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheRemoteURL, "lm://"+endpoint)
}

func TestHandle_ExternalBackend_PrefersSpecOverStaleStatus(t *testing.T) {
	// When the operator updates spec.endpoint for an External CR but a
	// new engine pod admits before the reconciler patches status, the
	// pod must be wired to the NEW spec.endpoint — not the stale
	// status.endpoint. Pod admission is CREATE-only, so a pod wired to
	// the old address on admission stays misrouted forever.
	const (
		ns          = "engines"
		freshSpec   = "new-cache.example:8200"
		staleStatus = "old-cache.example:8200"
	)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "ext", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: freshSpec,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "vllm"},
			},
		},
		Status: cachev1alpha1.CacheBackendStatus{Endpoint: staleStatus},
	}

	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
	reg := adapterruntime.DefaultRegistry()
	reg.Register(externaladapter.NewAdapter())
	h := &EngineInjector{Reader: c, Registry: reg, Log: logr.Discard()}

	pod := vllmEnginePod("engine-stale", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	// Must use spec.endpoint, NOT the stale status.endpoint.
	mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheRemoteURL, "lm://"+freshSpec)
	for _, e := range mutated.Spec.Containers[0].Env {
		if e.Name == adapterruntime.EnvLMCacheRemoteURL && e.Value == "lm://"+staleStatus {
			t.Fatalf("pod wired to stale status.endpoint %q; should be spec.endpoint %q", staleStatus, freshSpec)
		}
	}
}

func TestHandle_WhitespaceStatusEndpointFailsOpen(t *testing.T) {
	// A CR that predates the trim-in-reconciler change could carry a
	// whitespace-only status.endpoint. The webhook MUST treat that as
	// missing rather than injecting `LMCACHE_REMOTE_URL=lm://   ` which
	// the engine connector would reject at runtime. Defensive trim
	// applies to both External (spec also whitespace, fallback to
	// status) and managed (no fallback, just status).
	const ns = "engines"
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "managed-ws", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "vllm"},
			},
		},
		Status: cachev1alpha1.CacheBackendStatus{Endpoint: "   "},
	}

	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-ws", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	for _, e := range mutated.Spec.Containers[0].Env {
		if e.Name == adapterruntime.EnvLMCacheRemoteURL {
			t.Fatalf("whitespace status.endpoint must not become injected env; got %s=%q", e.Name, e.Value)
		}
	}
}

func TestHandle_ManagedBackend_StatusEmpty_FailsOpen(t *testing.T) {
	// Counterpart to the External fallback: managed backends MUST wait
	// for status.endpoint (the reconciler builds it from the rendered
	// Service). spec.endpoint is admission-rejected on managed types,
	// so there's nothing else to fall back on — the webhook must
	// fail-open without injecting until status catches up.
	const ns = "engines"
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "managed", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "vllm"},
			},
		},
		// No Status.Endpoint published yet.
	}

	h := newHandler(t, cb)
	pod := vllmEnginePod("engine-managed", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	// The pod must NOT have LMCACHE_REMOTE_URL because there's no
	// endpoint to wire it to — the fallback is External-only.
	mutated := applyPatches(t, req.Object.Raw, resp)
	if len(mutated.Spec.Containers) == 0 {
		t.Fatalf("pod has no containers after admission")
	}
	for _, e := range mutated.Spec.Containers[0].Env {
		if e.Name == adapterruntime.EnvLMCacheRemoteURL {
			t.Fatalf("managed CR with no status.endpoint must NOT trigger injection; got %s=%q", e.Name, e.Value)
		}
	}
}

// containsArgPairLocal mirrors the helper in envtest_integration_test.go;
// the two test files don't share state (envtest skips without
// KUBEBUILDER_ASSETS) so each file has its own copy.
func containsArgPairLocal(args []string, flag, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

func TestHandle_ExternalBackend_NoSidecar(t *testing.T) {
	// Negative case: a CacheBackend matched by a
	// runtime whose adapter returns no sidecar (the reference adapter here,
	// standing in for any future External-type adapter) admits the pod
	// without appending a kvevent-subscriber container.
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
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if c := findContainer(mutated, adapterruntime.SubscriberContainerName); c != nil {
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
	cb.Spec.BackendConfig = map[string]string{"model": "MyOrg/MyModel"}
	h := newHandler(t, cb) // default DefaultRegistry — no subscriber image
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("engine injection must still happen; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	if c := findContainer(mutated, adapterruntime.SubscriberContainerName); c != nil {
		t.Fatalf("default install must NOT auto-attach the sidecar; got %+v", c)
	}
	mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheRemoteURL, "lm://"+cb.Status.Endpoint)
}

func TestHandle_SidecarSkippedWithoutModel(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	// Sidecar opt-in via the configured handler, but no backendConfig.model
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
	if c := findContainer(mutated, adapterruntime.SubscriberContainerName); c != nil {
		t.Fatalf("sidecar must be skipped without a model id; got %+v", c)
	}
	mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheRemoteURL, "lm://"+cb.Status.Endpoint)
}

func TestHandle_SidecarErrorIsFailOpen(t *testing.T) {
	// If the adapter's ObservationSidecar errors, admission must still
	// succeed and the engine-side injection must still apply — the cache
	// is an optimisation, never a serving dependency.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.Integration.Engine = "stub-fail"
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
	if c := findContainer(mutated, adapterruntime.SubscriberContainerName); c != nil {
		t.Fatalf("sidecar errored — webhook must not append a partial container, got %+v", c)
	}
}

func TestHandle_PreExistingSidecar_NotDuplicated(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.BackendConfig = map[string]string{"model": "MyOrg/MyModel"}
	h := newHandlerWithSubscriber(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
		Name:  adapterruntime.SubscriberContainerName,
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
		if c.Name == adapterruntime.SubscriberContainerName {
			subs++
		}
	}
	if subs != 1 {
		t.Fatalf("expected exactly one %s container after admission, got %d: %v",
			adapterruntime.SubscriberContainerName, subs, containerNames(mutated))
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

func (sidecarErrorAdapter) InjectEngineConfig(pod *corev1.PodSpec, _ string, _ *cachev1alpha1.CacheBackend) error {
	if pod == nil || len(pod.Containers) == 0 {
		return errors.New("nope")
	}
	pod.Containers[0].Env = append(pod.Containers[0].Env, corev1.EnvVar{Name: "STUB_INJECTED", Value: "yes"})
	return nil
}

func (sidecarErrorAdapter) InjectRouterConfig(*corev1.PodSpec, string, *cachev1alpha1.CacheBackend) error {
	return nil
}

func (sidecarErrorAdapter) ObservationSidecar(*cachev1alpha1.CacheBackend, *corev1.Pod) (*corev1.Container, error) {
	return nil, errors.New("synthetic sidecar render failure")
}

func findContainer(pod *corev1.Pod, name string) *corev1.Container {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return &pod.Spec.Containers[i]
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
