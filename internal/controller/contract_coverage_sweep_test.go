package controller

// Cross-system contract coverage and edge-case sweep for the CacheIndex
// poller's per-backend attribution path. These tests are derived from the
// upstream surfaces they verify — the pod webhook (selectCacheBackend), the
// poller (attributePod), and the kvevent subscriber sidecar
// (replica_id = pod_name, tenant_id = pod_namespace from ObservationSidecar
// in pkg/adapters/runtime). A future rename or sort-policy change on EITHER
// surface must fail one of these tests.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/cachebox-project/inference-cache/pkg/index"
)

// TestWebhookPollerSelectorFallbackAgreement is the load-bearing
// agreement test for the selector fallback. The pod webhook
// (internal/webhook/pod.selectCacheBackend) and the CacheIndex poller
// (attributePod) BOTH sort CacheBackends by metadata.name ascending and
// take the first selector match. They are two independent code paths
// against the same contract: when an engine pod matches more than one
// CacheBackend, both surfaces must converge on the same backend, or the
// webhook will wire the engine to one backend's endpoint while the
// poller's status writer attributes its participation to another. Each
// surface has its own first-by-name test already; this test is the one
// place that asserts the two surfaces agree. A future rename of the
// sort key (or accidental switch to creationTimestamp) on either side
// must fail HERE.
func TestWebhookPollerSelectorFallbackAgreement(t *testing.T) {
	const ns = "agree-ns"
	labels := map[string]string{"app": "vllm"}

	// Two CacheBackends with identical selectors. "alpha" should win on
	// both surfaces (lexicographically before "zebra"). The two helpers
	// produce different shapes — readyCacheBackendForSweep includes
	// status.endpoint (the webhook needs it to inject) and cbFixture is
	// selector-only (the poller's attribution doesn't depend on endpoint).
	cbAlphaWebhook := readyCacheBackendForSweep("alpha", ns, labels)
	cbZebraWebhook := readyCacheBackendForSweep("zebra", ns, labels)

	// Surface 1 — webhook: admit a fresh engine pod and read the
	// injected-by annotation the handler stamps. The annotation records
	// the CacheBackend the webhook actually wired the engine to.
	annotated := runPodWebhookAndCaptureInjectedBy(t, ns,
		cbAlphaWebhook, cbZebraWebhook, labels)
	webhookChose, ok := parseInjectedByForSweep(annotated)
	if !ok || webhookChose.Namespace != ns {
		t.Fatalf("webhook annotation %q parsed to %+v; want namespace %q", annotated, webhookChose, ns)
	}

	// Surface 2 — poller: feed the SAME backends + a same-shaped engine
	// pod into the poller, with the annotation stripped so attribution
	// falls through the selector path (not the annotation shortcut). The
	// snapshot is keyed by the sidecar-identity convention
	// (replica_id = pod_name, tenant_id = pod_namespace). The fallback's
	// winner is observable as the backend whose
	// status.indexParticipation.prefixCount is non-zero after the refresh.
	cbAlphaPoller := cbFixture("alpha", ns, labels)
	cbZebraPoller := cbFixture("zebra", ns, labels)
	const podName = "engine-a"
	pollerPod := enginePod(podName, ns, labels) // no injected-by annotation
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: podName, Tenant: ns, PrefixCount: 7, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbAlphaPoller, cbZebraPoller},
		[]*corev1.Pod{pollerPod},
		&served, &mu)
	defer srv.Close()
	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("poller refresh: %v", err)
	}
	pollerChose := pickAttributedBackend(t, cl, []string{"alpha", "zebra"}, ns)

	// The actual cross-system agreement assertion: ONE rename on either
	// surface (sort field, selector semantics, etc.) drives these apart.
	if pollerChose.Name != webhookChose.Name || pollerChose.Namespace != webhookChose.Namespace {
		t.Fatalf("webhook+poller fallback disagreement: webhook chose %s/%s, poller chose %s/%s",
			webhookChose.Namespace, webhookChose.Name,
			pollerChose.Namespace, pollerChose.Name)
	}
	// Both must converge on lex-first ("alpha"). A pivot to a different
	// sort policy must be a deliberate joint change, with the doc
	// comments on both surfaces updated.
	if webhookChose.Name != "alpha" {
		t.Fatalf("both surfaces should converge on 'alpha' (first by name); both chose %q", webhookChose.Name)
	}
}

// TestRefreshNotFoundAndSuccessSameTickSameNamespace closes the
// remaining branch on attributePod's pod-Get caller. Two replicas in
// the same tick, same namespace: one pod missing (NotFound → silently
// drop), one pod present (success → attribute). The invariant is that the
// NotFound replica MUST NOT taint the namespace (taint is reserved for
// transient errors) — the successful replica still gets attribution. A
// drift that lumped NotFound into the taint branch would zero the
// successful backend's prefixCount, hiding a live cache from operators.
func TestRefreshNotFoundAndSuccessSameTickSameNamespace(t *testing.T) {
	const ns = "default"
	cbLive := cbFixture("live", ns, map[string]string{"app": "vllm-live"})
	cbStale := cbFixture("stale", ns, map[string]string{"app": "vllm-stale"})
	// Only the LIVE pod exists in the apiserver. The STALE replica's pod
	// was deleted (e.g. scale-down between snapshot ingest and this tick),
	// so the Get returns NotFound.
	livePod := enginePod("vllm-live-0", ns, map[string]string{"app": "vllm-live"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-live-0", Tenant: ns, PrefixCount: 5, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
			{ReplicaID: "vllm-stale-0", Tenant: ns, PrefixCount: 99, LastEventAt: time.Unix(1_700_000_500, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbLive, cbStale},
		[]*corev1.Pod{livePod},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	live := getBackendDirect(t, cl, "live", ns)
	if live.Status.IndexParticipation == nil || live.Status.IndexParticipation.PrefixCount != 5 {
		t.Fatalf("live backend should be attributed despite a sibling NotFound; got %+v",
			live.Status.IndexParticipation)
	}
	stale := getBackendDirect(t, cl, "stale", ns)
	if stale.Status.IndexParticipation == nil || stale.Status.IndexParticipation.PrefixCount != 0 {
		t.Fatalf("stale backend's pod is NotFound → its prefixCount must be 0, got %d",
			stale.Status.IndexParticipation.PrefixCount)
	}
}

// TestRefreshSelectorIsStrictSubsetOfPodLabels pins the
// matchLabelsSelects semantics: a selector that requires {app:vllm}
// matches a pod that has those plus extras (model, version, role).
// Engine pods in the wild carry a chunky label set (the Deployment
// template's labels plus downward-API tagging); the selector is allowed
// to be a STRICT subset. Without this test, a regression that swapped to
// map equality (DeepEqual) instead of subset semantics would silently
// stop attributing every real engine pod whose labels gained an extra tag.
func TestRefreshSelectorIsStrictSubsetOfPodLabels(t *testing.T) {
	const ns = "default"
	cb := cbFixture("backend", ns, map[string]string{"app": "vllm"})
	// Pod has the required label AND extras the selector does NOT mention.
	pod := enginePod("vllm-0", ns, map[string]string{
		"app":     "vllm",
		"model":   "qwen-0.5b",
		"version": "v2",
		"role":    "engine",
	})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: ns, PrefixCount: 3, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cb},
		[]*corev1.Pod{pod},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got := getBackendDirect(t, cl, "backend", ns)
	if got.Status.IndexParticipation == nil || got.Status.IndexParticipation.PrefixCount != 3 {
		t.Fatalf("selector %v is a strict subset of pod labels %v but attribution missed; got %+v",
			cb.Spec.EngineSelector.MatchLabels, pod.Labels, got.Status.IndexParticipation)
	}
}

// TestRefreshAnnotationNameIsPrefixOfAnotherBackend protects against a
// class of bug seen during earlier review: matching by name PREFIX instead
// of exact name. The pod's injected-by annotation points at the backend
// literally named "cache"; a sibling backend "cache-2" also exists in the
// same namespace. The annotation-lookup path MUST resolve to "cache" and
// never bleed into "cache-2". The current code uses a NamespacedName map
// (O(1) exact lookup) which is safe — this test is the regression guard
// so a future refactor that switches to strings.HasPrefix wouldn't pass
// review.
func TestRefreshAnnotationNameIsPrefixOfAnotherBackend(t *testing.T) {
	const ns = "default"
	cbExact := cbFixture("cache", ns, map[string]string{"app": "vllm-exact"})
	cbLonger := cbFixture("cache-2", ns, map[string]string{"app": "vllm-longer"})
	// Labels match "cache-2"'s selector but the annotation pins the
	// attribution to "cache" — the annotation path must win.
	pod := enginePodInjectedBy("vllm-0", ns, ns, "cache",
		map[string]string{"app": "vllm-longer"})

	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: ns, PrefixCount: 11, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbExact, cbLonger},
		[]*corev1.Pod{pod},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	exact := getBackendDirect(t, cl, "cache", ns)
	if exact.Status.IndexParticipation == nil || exact.Status.IndexParticipation.PrefixCount != 11 {
		t.Fatalf("annotation 'cache' must resolve to exact 'cache'; got %+v", exact.Status.IndexParticipation)
	}
	longer := getBackendDirect(t, cl, "cache-2", ns)
	if longer.Status.IndexParticipation == nil || longer.Status.IndexParticipation.PrefixCount != 0 {
		t.Fatalf("backend 'cache-2' must NOT inherit attribution intended for prefix sibling 'cache'; got %+v",
			longer.Status.IndexParticipation)
	}
}

// TestRefreshSamePodNameAcrossTenantsIsFailSoft pins fail-soft
// behaviour when the snapshot reports the SAME replica_id under two
// different tenants in the same tick. With the sidecar-identity convention
// (replica_id = pod_name) this should not happen in practice because two
// pods named the same across namespaces are independent objects. But
// fail-soft is the right disposition: each replica is processed
// independently against its own (tenant=namespace, replicaID=podname) pod
// lookup, no panic, no cross-pollination of attribution between tenants.
// The test asserts both backends, in different namespaces, get their own
// attributed slice and the poller does not crash on the collision.
func TestRefreshSamePodNameAcrossTenantsIsFailSoft(t *testing.T) {
	const podName = "shared-0"
	const nsA = "ns-a"
	const nsB = "ns-b"
	cbA := cbFixture("backend-a", nsA, map[string]string{"app": "vllm"})
	cbB := cbFixture("backend-b", nsB, map[string]string{"app": "vllm"})
	podA := enginePod(podName, nsA, map[string]string{"app": "vllm"})
	podB := enginePod(podName, nsB, map[string]string{"app": "vllm"})

	var mu sync.Mutex
	tsA := time.Unix(1_700_000_000, 0).UTC()
	tsB := time.Unix(1_700_000_500, 0).UTC()
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: podName, Tenant: nsA, PrefixCount: 4, LastEventAt: tsA},
			{ReplicaID: podName, Tenant: nsB, PrefixCount: 9, LastEventAt: tsB},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbA, cbB},
		[]*corev1.Pod{podA, podB},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh (cross-namespace replicaID collision): %v", err)
	}
	a := getBackendDirect(t, cl, "backend-a", nsA)
	if a.Status.IndexParticipation == nil || a.Status.IndexParticipation.PrefixCount != 4 {
		t.Fatalf("backend-a (ns-a) attribution = %+v, want prefixCount 4 from same-name pod in ns-a",
			a.Status.IndexParticipation)
	}
	b := getBackendDirect(t, cl, "backend-b", nsB)
	if b.Status.IndexParticipation == nil || b.Status.IndexParticipation.PrefixCount != 9 {
		t.Fatalf("backend-b (ns-b) attribution = %+v, want prefixCount 9 from same-name pod in ns-b",
			b.Status.IndexParticipation)
	}
	// Last-event timestamps must stay tenant-scoped: each backend gets
	// its OWN pod's LastEventAt, not the cross-tenant max.
	if a.Status.IndexParticipation.LastEventAt == nil || !a.Status.IndexParticipation.LastEventAt.Time.Equal(tsA) {
		t.Fatalf("backend-a lastEventAt = %v, want %v (ns-a row only)", a.Status.IndexParticipation.LastEventAt, tsA)
	}
	if b.Status.IndexParticipation.LastEventAt == nil || !b.Status.IndexParticipation.LastEventAt.Time.Equal(tsB) {
		t.Fatalf("backend-b lastEventAt = %v, want %v (ns-b row only)", b.Status.IndexParticipation.LastEventAt, tsB)
	}
}

// ----- helpers scoped to the sweep -----

// readyCacheBackendForSweep mirrors the unexported readyCacheBackend helper
// in internal/webhook/pod (which we can't import across packages). The
// webhook injects only when status.endpoint is populated, so the agreement
// test needs a CacheBackend that the webhook will pick up — not the
// selector-only cbFixture form the poller tests use.
func readyCacheBackendForSweep(name, namespace string, selector map[string]string) *cachev1alpha1.CacheBackend {
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

// runPodWebhookAndCaptureInjectedBy admits an engine pod via the
// EngineInjector and returns the injected-by annotation written onto the
// post-admission pod. Mirrors how the controller-runtime webhook server
// would invoke Handle() at admission time without standing up a full
// envtest — the agreement scenario doesn't need a real apiserver, just
// the two surfaces' actual selection logic.
func runPodWebhookAndCaptureInjectedBy(t *testing.T, namespace string,
	cb1, cb2 *cachev1alpha1.CacheBackend, podLabels map[string]string) string {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("clientgoscheme.AddToScheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("cachev1alpha1.AddToScheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cb1, cb2).Build()
	h := &podwebhook.EngineInjector{Reader: c, Log: logr.Discard()}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-a", Labels: podLabels},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  adapterruntime.EngineContainerName,
				Image: "vllm/vllm-openai-cpu:latest",
				Args:  []string{"--model", "qwen"},
			}},
		},
	}
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("marshal pod: %v", err)
	}
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			UID:       types.UID("sweep-uid"),
			Operation: admissionv1.Create,
			Namespace: namespace,
			Kind:      metav1.GroupVersionKind{Version: "v1", Kind: "Pod"},
			Resource:  metav1.GroupVersionResource{Version: "v1", Resource: "pods"},
			Object:    runtime.RawExtension{Raw: raw},
		},
	}
	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("webhook response not Allowed: %+v", resp.Result)
	}
	if len(resp.Patches) == 0 {
		t.Fatal("expected JSON patches from webhook (overlapping selectors should produce a match)")
	}
	annotationEscaped := jsonPatchEscapeForSweep(podwebhook.AnnotationInjectedBy)
	for _, p := range resp.Patches {
		// The webhook can emit the annotation in two patch shapes:
		// either an add of the whole annotations map, or an add of the
		// single annotation key. Handle both rather than assuming one.
		switch v := p.Value.(type) {
		case map[string]any:
			if s, ok := v[podwebhook.AnnotationInjectedBy].(string); ok && s != "" {
				return s
			}
		case string:
			if p.Path == "/metadata/annotations/"+annotationEscaped {
				return v
			}
		}
	}
	t.Fatalf("webhook did not stamp %q annotation; patches = %+v",
		podwebhook.AnnotationInjectedBy, resp.Patches)
	return ""
}

// pickAttributedBackend returns the (namespace, name) of the single
// CacheBackend in the supplied set whose status.indexParticipation is
// populated with a non-zero prefixCount. Fails the test if zero or more
// than one match — both surfaces must converge on exactly one winner.
func pickAttributedBackend(t *testing.T, cl client.Client, names []string, namespace string) types.NamespacedName {
	t.Helper()
	var winners []types.NamespacedName
	for _, n := range names {
		cb := getBackendDirect(t, cl, n, namespace)
		if cb.Status.IndexParticipation != nil && cb.Status.IndexParticipation.PrefixCount > 0 {
			winners = append(winners, types.NamespacedName{Namespace: namespace, Name: n})
		}
	}
	if len(winners) != 1 {
		t.Fatalf("expected exactly one attributed backend; got %v", winners)
	}
	return winners[0]
}

// parseInjectedByForSweep splits a "namespace/name" injected-by annotation
// into a NamespacedName. Returns ok=false on a malformed value so the
// caller can produce a context-specific failure message.
func parseInjectedByForSweep(value string) (types.NamespacedName, bool) {
	ns, name, ok := strings.Cut(value, "/")
	if !ok || ns == "" || name == "" {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{Namespace: ns, Name: name}, true
}

// jsonPatchEscapeForSweep is the JSON-Pointer escaping for "/" and "~" in
// JSON Patch paths (RFC 6901). Matches the helper of the same shape in
// the webhook tests so the agreement test can identify the annotation
// patch regardless of how controller-runtime renders it.
func jsonPatchEscapeForSweep(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	s = strings.ReplaceAll(s, "/", "~1")
	return s
}
