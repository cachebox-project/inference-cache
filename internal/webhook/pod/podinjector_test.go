package pod

import (
	"bytes"
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
			Type: cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: selector},
		},
		Status: cachev1alpha1.CacheBackendStatus{
			Endpoint: name + ".cache-ns.svc.cluster.local:65432",
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
	// Pin the webhook-only proof-of-injection annotation against the
	// matched CR's UID. The engine-pod-events controller skips emission
	// when this doesn't match; a regression in the success-path stamp
	// would break the binding signal end-to-end.
	if got, want := mutated.Annotations[AnnotationInjectedByUID], string(cb.UID); got != want {
		t.Fatalf("annotation %s: got %q, want %q (matched CR UID)", AnnotationInjectedByUID, got, want)
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

// eventsOnlyCacheBackend returns an events-only (tier-1 routing) LMCache
// CacheBackend: type=LMCache, spec.integration.mode=EventsOnly, a served model
// id (so ObservationSidecar emits a container), an engineSelector, and NO
// status.endpoint. It provisions no server, so the absent endpoint is the
// expected steady state — not a not-yet-reconciled race.
func eventsOnlyCacheBackend(name, namespace string, selector map[string]string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID("cb-" + namespace + "-" + name + "-uid"),
		},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Mode:   cachev1alpha1.CacheBackendIntegrationModeEventsOnly,
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: selector},
			BackendConfig:  map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"},
		},
		// No Status.Endpoint — events-only provisions no server, so the
		// reconciler leaves it empty. The webhook MUST inject anyway.
	}
}

func TestHandle_EventsOnly_EmptyEndpoint_InjectsSubscriberWithoutConnector(t *testing.T) {
	// An events-only backend has an EMPTY status.endpoint by design (no
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
		t.Fatalf("events-only backend with empty status.endpoint must INJECT, not fail-open; got no patches")
	}

	mutated := applyPatches(t, req.Object.Raw, resp)

	// The subscriber sidecar IS appended — that is the whole point of
	// events-only (the routing tier observes KV events via the sidecar).
	if len(mutated.Spec.Containers) != 2 {
		t.Fatalf("expected 2 containers (engine + subscriber), got %d: %v",
			len(mutated.Spec.Containers), containerNames(mutated))
	}
	sub := findContainer(mutated, adapterruntime.SubscriberContainerName)
	if sub == nil {
		t.Fatalf("subscriber sidecar missing; containers = %v", containerNames(mutated))
	}
	if !argPresent(sub.Args, "--model-id=Qwen/Qwen2.5-0.5B-Instruct") {
		t.Fatalf("--model-id derived from cb.spec.backendConfig.model missing; args = %v", sub.Args)
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
	engine := findContainer(mutated, adapterruntime.EngineContainerName)
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
	// managed backend whose status.endpoint is not yet published MUST fail-open
	// — admit unmodified, no subscriber sidecar, no injected-by annotation —
	// because the connector it would wire needs a real dial target. This pins
	// that the events-only inject path is mode-gated, not a blanket
	// "inject on empty endpoint".
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	cb.Spec.BackendConfig = map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"}
	cb.Status.Endpoint = "" // Offload mode, reconciler hasn't published yet.
	h := newHandlerWithSubscriber(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed (fail-open), got: %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("Offload managed backend with empty status.endpoint must fail-open (no patches), got %d: %+v",
			len(resp.Patches), resp.Patches)
	}
	// Fail-open never stamps injected-by; the inbound pod carried none, so a
	// re-applied pod is byte-identical (zero patches above already proves it,
	// but assert the annotation absence explicitly for clarity).
	if pod.Annotations[AnnotationInjectedBy] != "" {
		t.Fatalf("fail-open path must not stamp %s", AnnotationInjectedBy)
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

func TestHandle_ExternalBackend_InvalidSpecEndpoint_FailsOpen(t *testing.T) {
	// A pre-existing External CR carrying a malformed spec.endpoint
	// (stored before the shape rule shipped) must not be wired —
	// injecting LMCACHE_REMOTE_URL=lm://https://... or lm://2001:db8::1
	// would crash the engine at startup. effectiveEndpoint applies the
	// same shape check the admission webhook uses and returns "" for
	// invalid values, so the existing fail-open branch admits the pod
	// un-wired and the operator sees the shape error in the response
	// reason instead of an engine-pod crash log.
	const ns = "engines"
	for _, tc := range []struct {
		name, endpoint string
	}{
		{"bad-scheme", "https://cache.example.com:443/api"},
		{"portless-host", "cache.example.com"},
		{"embedded-whitespace", "cache example:8200"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cb := &cachev1alpha1.CacheBackend{
				ObjectMeta: metav1.ObjectMeta{Name: "ext-bad", Namespace: ns},
				Spec: cachev1alpha1.CacheBackendSpec{
					Type:     cachev1alpha1.CacheBackendTypeExternal,
					Endpoint: tc.endpoint,
					Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
						Engine: "vllm",
						Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
					},
					EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
						MatchLabels: map[string]string{"app": "vllm"},
					},
				},
			}
			s := newScheme(t)
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
			reg := adapterruntime.DefaultRegistry()
			reg.Register(externaladapter.NewAdapter())
			h := &EngineInjector{Reader: c, Registry: reg, Log: logr.Discard()}

			pod := vllmEnginePod("engine", map[string]string{"app": "vllm"})
			req := newRequest(t, pod, ns)
			resp := h.Handle(context.Background(), req)
			if !resp.Allowed {
				t.Fatalf("expected Allowed (fail-open), got %+v", resp.Result)
			}
			// Zero patches — no injection happened.
			if len(resp.Patches) != 0 {
				t.Fatalf("expected no patches on invalid endpoint; got %d: %v", len(resp.Patches), resp.Patches)
			}
			// Response message must name spec.endpoint, not status.endpoint.
			if msg := resp.Result.Message; !strings.Contains(msg, "spec.endpoint") {
				t.Fatalf("fail-open reason should mention spec.endpoint for External; got %q", msg)
			}
		})
	}
}

func TestHandle_ExternalBackend_StatusEmpty_UsesSpecDirectly(t *testing.T) {
	// Pod admission is CREATE-only — if an engine pod admits before the
	// controller has mirrored spec.endpoint into status.endpoint, the
	// webhook would fail-open and leave the pod unwired *forever* (no
	// re-admission on subsequent status updates). For External CRs the
	// webhook sources the endpoint from spec.endpoint directly (NOT
	// "falling back" — effectiveEndpoint type-scopes the source so
	// External never reads status.endpoint, preventing wiring against a
	// stale mirror during an endpoint update). Without this, applying
	// the External CacheBackend and the engine Deployment in the same
	// kubectl apply silently produces unwired engine pods.
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

func TestHandle_ExternalBackend_UpperCaseSchemeNormalised(t *testing.T) {
	// Admission lowercases the scheme during shape validation, so
	// `LM://cache.example:8200` admits. The pod webhook must then
	// normalise to lower-case `lm://` at injection — passing the
	// operator-typed value through verbatim would produce
	// `LMCACHE_REMOTE_URL=lm://LM://cache.example:8200`, a double-
	// prefix the engine connector rejects.
	const (
		ns            = "engines"
		operatorTyped = "LM://cache.example.com:8200"
	)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "ext-up", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: operatorTyped,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "vllm"},
			},
		},
	}

	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cb).Build()
	reg := adapterruntime.DefaultRegistry()
	reg.Register(externaladapter.NewAdapter())
	h := &EngineInjector{Reader: c, Registry: reg, Log: logr.Discard()}

	pod := vllmEnginePod("engine-up", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	mutated := applyPatches(t, req.Object.Raw, resp)
	// Must be the canonical lower-case scheme, with the original
	// host portion preserved verbatim.
	mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheRemoteURL, "lm://cache.example.com:8200")
}

func TestHandle_WhitespaceStatusEndpointFailsOpen(t *testing.T) {
	// A CR that predates the trim-in-reconciler change could carry a
	// whitespace-only status.endpoint. The webhook MUST treat that as
	// missing rather than injecting `LMCACHE_REMOTE_URL=lm://   ` which
	// the engine connector would reject at runtime. The defensive trim
	// applies to whichever field effectiveEndpoint reads for the CR's
	// type — spec.endpoint for External (which never reads status), and
	// status.endpoint for managed.
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

func (sidecarErrorAdapter) ResolveCacheServer(*cachev1alpha1.CacheBackend) (*adapterruntime.ResolvedCacheServer, error) {
	return nil, nil
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

func (sidecarErrorAdapter) ReservedArgs() []string      { return nil }
func (sidecarErrorAdapter) ReservedEnv() []string       { return nil }
func (sidecarErrorAdapter) EngineContainerName() string { return adapterruntime.EngineContainerName }

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
		AnnotationSkip:          "true",
		AnnotationInjectedBy:    ns + "/" + cb.Name,
		AnnotationInjectedByUID: string(cb.UID),
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
			// Override a tunable canonical env value, which is allowed
			// because LMCACHE_CHUNK_SIZE is NOT reserved.
			{Name: adapterruntime.EnvLMCacheChunkSize, Value: "512"},
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
	// Override wins for the tunable name (LMCACHE_CHUNK_SIZE is an
	// adapter-owned canonical entry — the override surface can touch it).
	mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheChunkSize, "512")
	// Canonical reserved env still landed unchanged.
	mustHaveEnv(t, mutated, adapterruntime.EnvVLLMUseV1, "1")
	mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheRemoteURL, "lm://"+cb.Status.Endpoint)
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
	mustHaveEnv(t, mutated, adapterruntime.EnvVLLMUseV1, "1")
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
		mustHaveEnv(t, mutated, adapterruntime.EnvVLLMUseV1, "1")
		mustHaveEnv(t, mutated, adapterruntime.EnvLMCacheRemoteURL, "lm://"+cb.Status.Endpoint)
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
				cb.Status.Endpoint = "" // force the endpoint-not-published fail-open path
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
