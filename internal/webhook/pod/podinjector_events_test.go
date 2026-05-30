package pod

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// newHandlerWithRecorder builds a handler whose Recorder is a buffered fake
// recorder. The buffer is intentionally generous so a test never blocks on
// emit; assertions read the channel with a non-blocking drain.
func newHandlerWithRecorder(t *testing.T, objs ...interface{}) (*EngineInjector, *events.FakeRecorder) {
	t.Helper()
	s := newScheme(t)
	builder := fake.NewClientBuilder().WithScheme(s)
	for _, o := range objs {
		if cb, ok := o.(*cachev1alpha1.CacheBackend); ok {
			builder = builder.WithObjects(cb)
		}
	}
	c := builder.Build()
	rec := events.NewFakeRecorder(16)
	return &EngineInjector{
		Reader:   c,
		Log:      logr.Discard(),
		Recorder: rec,
	}, rec
}

func drainRecorder(rec *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func containsAll(t *testing.T, events []string, want string) {
	t.Helper()
	for _, e := range events {
		if strings.Contains(e, want) {
			return
		}
	}
	t.Fatalf("expected event containing %q; got %v", want, events)
}

func containsNone(t *testing.T, events []string, want string) {
	t.Helper()
	for _, e := range events {
		if strings.Contains(e, want) {
			t.Fatalf("did not expect event containing %q; got %v", want, events)
		}
	}
}

func TestHandle_EmitsInjectedEventOnSuccessPath(t *testing.T) {
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h, rec := newHandlerWithRecorder(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("expected Allowed with patches; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
	events := drainRecorder(rec)
	// The FakeRecorder format is "<eventtype> <reason> <message>".
	containsAll(t, events, "Normal "+eventReasonInjected)
	containsAll(t, events, ns+"/"+cb.Name)
	containsNone(t, events, eventReasonNoMatchingCacheBackend)
}

func TestHandle_EmitsNoMatchEventWhenNamespaceHasCacheBackend(t *testing.T) {
	// A pod in a namespace that DOES have a claim-capable CacheBackend
	// but whose labels miss its selector is the actual operator-drift
	// case. The NoMatchingCacheBackend Event must fire to surface the
	// drift.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h, rec := newHandlerWithRecorder(t, cb)
	pod := vllmEnginePod("not-an-engine", map[string]string{"app": "router"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("expected no patches on no-match, got %d", len(resp.Patches))
	}
	events := drainRecorder(rec)
	containsAll(t, events, "Normal "+eventReasonNoMatchingCacheBackend)
	containsAll(t, events, "uncached")
	containsNone(t, events, eventReasonInjected)
}

func TestHandle_SuppressesNoMatchEventInNamespaceWithoutCacheBackend(t *testing.T) {
	// The webhook is configured for every Pod CREATE cluster-wide
	// (failurePolicy=ignore, scope=*) so it sees pods in every
	// namespace — including namespaces that have nothing to do with
	// this cache plane. Emitting a NoMatchingCacheBackend Event on
	// every such pod would flood the cluster-wide event stream and
	// regress the noise floor in unrelated workloads. Gate: emit only
	// when the namespace has at least one claim-capable CacheBackend.
	const ns = "unrelated"
	h, rec := newHandlerWithRecorder(t /* no CacheBackend objects */)
	pod := vllmEnginePod("some-app", map[string]string{"app": "router"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Fatalf("expected no patches on no-match, got %d", len(resp.Patches))
	}
	events := drainRecorder(rec)
	if len(events) != 0 {
		t.Fatalf("expected no events in a namespace with no claim-capable CacheBackend; got %v", events)
	}
}

func TestHandle_SuppressesNoMatchEventWhenAllSelectorsAreEmpty(t *testing.T) {
	// A CacheBackend with no selector (or an empty MatchLabels map)
	// is not claim-capable — the webhook skips it. From the no-match
	// event's perspective such a CR doesn't count: there's nothing to
	// drift away from. Confirms that "namespace has a CB" alone isn't
	// enough; the CB must have a non-empty selector for the event to
	// be a meaningful "drift" signal.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, nil)
	cb.Spec.EngineSelector = nil
	h, rec := newHandlerWithRecorder(t, cb)
	pod := vllmEnginePod("any-pod", map[string]string{"app": "router"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	events := drainRecorder(rec)
	if len(events) != 0 {
		t.Fatalf("expected no events when no CB is claim-capable; got %v", events)
	}
}

func TestHandle_NoEventOnReadmissionOfAlreadyInjectedPod(t *testing.T) {
	// A pod whose AnnotationInjectedBy is already set arrives at the
	// webhook (e.g. a re-invocation chain or a manual re-admission via
	// `kubectl replace`). The adapter's merge is idempotent so the patch
	// set is empty — and the InjectedByCacheBackend event must NOT
	// re-fire. The dedupe is the simplest signal a previous admission
	// already injected the pod: AnnotationInjectedBy non-empty.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h, rec := newHandlerWithRecorder(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Annotations = map[string]string{AnnotationInjectedBy: ns + "/" + cb.Name}
	req := newRequest(t, pod, ns)

	// First, run the handler. We don't require an empty patch set here
	// because the env/arg upserts haven't actually been applied yet —
	// the synthetic pre-set annotation is enough to trip the dedupe.
	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	events := drainRecorder(rec)
	containsNone(t, events, eventReasonInjected)
}

func TestHandle_NoEventOnSkipAnnotation(t *testing.T) {
	// A pod that opts out via AnnotationSkip never reaches the match
	// step, so neither InjectedByCacheBackend nor NoMatchingCacheBackend
	// fires. The skip is a deliberate user choice and should not turn
	// into an event-stream noise source.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h, rec := newHandlerWithRecorder(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	pod.Annotations = map[string]string{AnnotationSkip: "true"}
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	events := drainRecorder(rec)
	containsNone(t, events, eventReasonInjected)
	containsNone(t, events, eventReasonNoMatchingCacheBackend)
}

func TestHandle_NoEventOnDryRun(t *testing.T) {
	// Dry-run admissions go through the webhook (sideEffects=None means
	// the webhook promises no side effects on dry-run). Writing K8s
	// Events on a dry-run admission would persist in etcd and violate
	// the contract.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h, rec := newHandlerWithRecorder(t, cb)
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)
	dry := true
	req.DryRun = &dry

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected Allowed, got %+v", resp.Result)
	}
	events := drainRecorder(rec)
	if len(events) != 0 {
		t.Fatalf("expected no events on dry-run admission; got %v", events)
	}
}

func TestHandle_NilRecorderSafe(t *testing.T) {
	// The Recorder is optional — when nil the handler must still
	// admission/mutate correctly. Defense in depth: cmd/controller
	// always wires it, but a test or an alternative embedding may
	// not.
	const ns = "engines"
	cb := readyCacheBackend("primary", ns, map[string]string{"app": "vllm"})
	h := newHandler(t, cb) // no Recorder
	pod := vllmEnginePod("engine-a", map[string]string{"app": "vllm"})
	req := newRequest(t, pod, ns)

	resp := h.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("expected Allowed with patches even with nil Recorder; Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
	}
}
