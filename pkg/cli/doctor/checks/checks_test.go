package checks

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/cli/doctor"
)

// --- shared helpers ---------------------------------------------------------

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("clientgoscheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("cachev1alpha1: %v", err)
	}
	return s
}

func fakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
}

// codesOf returns the finding codes in order for compact assertions.
func codesOf(fs []doctor.Finding) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Code
	}
	return out
}

func hasCode(fs []doctor.Finding, code string) *doctor.Finding {
	for i := range fs {
		if fs[i].Code == code {
			return &fs[i]
		}
	}
	return nil
}

// listErrClient wraps a client and fails List for object lists matching failOn.
type listErrClient struct {
	client.Client
	failOn func(client.ObjectList) bool
	err    error
}

func (c listErrClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.failOn(list) {
		return c.err
	}
	return c.Client.List(ctx, list, opts...)
}

func ptr[T any](v T) *T { return &v }

// --- fixtures ---------------------------------------------------------------

func readyCond(status metav1.ConditionStatus, reason, msg string) metav1.Condition {
	return metav1.Condition{Type: conditionReady, Status: status, Reason: reason, Message: msg}
}

func healthyBackend(now time.Time) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "good", Namespace: "ns1"},
		Spec: cachev1alpha1.CacheBackendSpec{
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{"app": "engine"}},
		},
		Status: cachev1alpha1.CacheBackendStatus{
			Endpoint:          "10.0.0.5:8200",
			MatchedEnginePods: ptr(int32(2)),
			Conditions:        []metav1.Condition{readyCond(metav1.ConditionTrue, "KVEventObserved", "ready")},
			IndexParticipation: &cachev1alpha1.CacheBackendIndexParticipation{
				PrefixCount: 5,
				LastEventAt: &metav1.Time{Time: now.Add(-10 * time.Second)},
			},
		},
	}
}

// --- ServerReachability -----------------------------------------------------

type stubHealth struct {
	resp *healthpb.HealthCheckResponse
	err  error
}

func (s stubHealth) Check(_ context.Context, _ *healthpb.HealthCheckRequest, _ ...grpc.CallOption) (*healthpb.HealthCheckResponse, error) {
	return s.resp, s.err
}

// realHealthClient spins up an in-process gRPC server (bufconn) with the
// grpc.health.v1 service registered, returning a connected client. This
// exercises ServerReachability against a genuine gRPC server, not a stub.
func realHealthClient(t *testing.T, serving healthpb.HealthCheckResponse_ServingStatus) HealthChecker {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", serving)
	healthpb.RegisterHealthServer(srv, hs)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(); srv.Stop() })
	return healthpb.NewHealthClient(conn)
}

func TestServerReachability(t *testing.T) {
	ctx := context.Background()
	t.Run("nil client", func(t *testing.T) {
		fs := ServerReachability(ctx, nil, "host:9090")
		if hasCode(fs, doctor.CodeServerUnreachable) == nil {
			t.Fatalf("want SV001, got %v", codesOf(fs))
		}
	})
	t.Run("check error", func(t *testing.T) {
		fs := ServerReachability(ctx, stubHealth{err: errors.New("refused")}, "host:9090")
		if hasCode(fs, doctor.CodeServerUnreachable) == nil {
			t.Fatalf("want SV001, got %v", codesOf(fs))
		}
	})
	t.Run("not serving", func(t *testing.T) {
		fs := ServerReachability(ctx, stubHealth{resp: &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_NOT_SERVING}}, "host:9090")
		f := hasCode(fs, doctor.CodeServerNotServing)
		if f == nil || f.Status != doctor.StatusFail {
			t.Fatalf("want SV002 FAIL, got %v", codesOf(fs))
		}
	})
	t.Run("serving via real gRPC server", func(t *testing.T) {
		fs := ServerReachability(ctx, realHealthClient(t, healthpb.HealthCheckResponse_SERVING), "host:9090")
		f := hasCode(fs, doctor.CodeServerServing)
		if f == nil || f.Status != doctor.StatusOK {
			t.Fatalf("want SV003 OK, got %v", codesOf(fs))
		}
	})
}

// --- SnapshotReachability ---------------------------------------------------

func TestSnapshotReachability(t *testing.T) {
	ctx := context.Background()

	t.Run("nil doer", func(t *testing.T) {
		fs := SnapshotReachability(ctx, nil, "", "")
		if hasCode(fs, doctor.CodeSnapshotUnreachable) == nil {
			t.Fatalf("want SN001, got %v", codesOf(fs))
		}
	})

	t.Run("non-200", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		fs := SnapshotReachability(ctx, srv.Client(), srv.URL, "")
		if hasCode(fs, doctor.CodeSnapshotUnreachable) == nil {
			t.Fatalf("want SN001, got %v", codesOf(fs))
		}
	})

	t.Run("200 bad body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("not json"))
		}))
		defer srv.Close()
		fs := SnapshotReachability(ctx, srv.Client(), srv.URL, "")
		f := hasCode(fs, doctor.CodeSnapshotBadBody)
		if f == nil || f.Status != doctor.StatusWarn {
			t.Fatalf("want SN002 WARN, got %v", codesOf(fs))
		}
	})

	t.Run("200 valid json unauthenticated", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "" {
				t.Errorf("did not expect Authorization header")
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer srv.Close()
		fs := SnapshotReachability(ctx, srv.Client(), srv.URL, "")
		if hasCode(fs, doctor.CodeSnapshotOK) == nil {
			t.Fatalf("want SN004, got %v", codesOf(fs))
		}
		if hasCode(fs, doctor.CodeSnapshotUnauthenticated) == nil {
			t.Fatalf("want SN003 INFO for unauthenticated path, got %v", codesOf(fs))
		}
	})

	t.Run("200 valid json with token", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer tok" {
				t.Errorf("Authorization = %q, want Bearer tok", r.Header.Get("Authorization"))
			}
			_, _ = w.Write([]byte(`{}`))
		}))
		defer srv.Close()
		fs := SnapshotReachability(ctx, srv.Client(), srv.URL, "tok")
		if hasCode(fs, doctor.CodeSnapshotOK) == nil {
			t.Fatalf("want SN004, got %v", codesOf(fs))
		}
		if hasCode(fs, doctor.CodeSnapshotUnauthenticated) != nil {
			t.Fatalf("did not expect SN003 when a token is presented, got %v", codesOf(fs))
		}
	})

	t.Run("auth-gated degrades to SN005 WARN, not a FAIL", func(t *testing.T) {
		for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			fs := SnapshotReachability(ctx, srv.Client(), srv.URL, "")
			srv.Close()
			f := hasCode(fs, doctor.CodeSnapshotAuthGated)
			if f == nil || f.Status != doctor.StatusWarn {
				t.Fatalf("status %d: want SN005 WARN, got %v", status, codesOf(fs))
			}
			if hasCode(fs, doctor.CodeSnapshotUnreachable) != nil {
				t.Fatalf("status %d: auth-gated must not be SN001 FAIL", status)
			}
		}
	})

	t.Run("transport error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		url := srv.URL
		client := srv.Client()
		srv.Close() // now connections are refused
		fs := SnapshotReachability(ctx, client, url, "")
		if hasCode(fs, doctor.CodeSnapshotUnreachable) == nil {
			t.Fatalf("want SN001 on transport error, got %v", codesOf(fs))
		}
	})
}

// --- PolicyReachability -----------------------------------------------------

func TestPolicyReachability(t *testing.T) {
	ctx := context.Background()

	t.Run("nil doer", func(t *testing.T) {
		fs := PolicyReachability(ctx, nil, "")
		if hasCode(fs, doctor.CodePolicyRouteMissing) == nil {
			t.Fatalf("want PL001, got %v", codesOf(fs))
		}
	})

	t.Run("405 still wired", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}))
		defer srv.Close()
		fs := PolicyReachability(ctx, srv.Client(), srv.URL+"/policy")
		f := hasCode(fs, doctor.CodePolicyRouteWired)
		if f == nil || f.Status != doctor.StatusOK {
			t.Fatalf("want PL002 OK, got %v", codesOf(fs))
		}
	})

	t.Run("404 means route not mounted (PL001 FAIL)", func(t *testing.T) {
		// A bare ServeMux with nothing registered at /policy returns 404.
		srv := httptest.NewServer(http.NewServeMux())
		defer srv.Close()
		fs := PolicyReachability(ctx, srv.Client(), srv.URL+"/policy")
		f := hasCode(fs, doctor.CodePolicyRouteMissing)
		if f == nil || f.Status != doctor.StatusFail {
			t.Fatalf("want PL001 FAIL on 404, got %v", codesOf(fs))
		}
	})

	t.Run("5xx is mounted-but-erroring (PL003 WARN)", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		fs := PolicyReachability(ctx, srv.Client(), srv.URL+"/policy")
		f := hasCode(fs, doctor.CodePolicyRouteUnexpected)
		if f == nil || f.Status != doctor.StatusWarn {
			t.Fatalf("want PL003 WARN on 500, got %v", codesOf(fs))
		}
	})

	t.Run("401 from auth is still wired (PL002 OK)", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		}))
		defer srv.Close()
		fs := PolicyReachability(ctx, srv.Client(), srv.URL+"/policy")
		if hasCode(fs, doctor.CodePolicyRouteWired) == nil {
			t.Fatalf("want PL002 OK on 401, got %v", codesOf(fs))
		}
	})

	t.Run("transport error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		url := srv.URL + "/policy"
		client := srv.Client()
		srv.Close()
		fs := PolicyReachability(ctx, client, url)
		if hasCode(fs, doctor.CodePolicyRouteMissing) == nil {
			t.Fatalf("want PL001, got %v", codesOf(fs))
		}
	})
}

func TestProbeReachability(t *testing.T) {
	ctx := context.Background()

	t.Run("nil doer", func(t *testing.T) {
		if hasCode(ProbeReachability(ctx, nil, ""), doctor.CodeProbeRouteMissing) == nil {
			t.Fatal("want PB001 for nil doer")
		}
	})
	t.Run("405 wired (PB002)", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusMethodNotAllowed) }))
		defer srv.Close()
		f := hasCode(ProbeReachability(ctx, srv.Client(), srv.URL+"/probe"), doctor.CodeProbeRouteWired)
		if f == nil || f.Status != doctor.StatusOK {
			t.Fatal("want PB002 OK on 405")
		}
	})
	t.Run("404 not mounted (PB001)", func(t *testing.T) {
		srv := httptest.NewServer(http.NewServeMux())
		defer srv.Close()
		if hasCode(ProbeReachability(ctx, srv.Client(), srv.URL+"/probe"), doctor.CodeProbeRouteMissing) == nil {
			t.Fatal("want PB001 FAIL on 404")
		}
	})
	t.Run("5xx unexpected (PB003)", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) }))
		defer srv.Close()
		f := hasCode(ProbeReachability(ctx, srv.Client(), srv.URL+"/probe"), doctor.CodeProbeRouteUnexpected)
		if f == nil || f.Status != doctor.StatusWarn {
			t.Fatal("want PB003 WARN on 502")
		}
	})
}

// --- CacheBackendHealth -----------------------------------------------------

func okDial(context.Context, string) error  { return nil }
func badDial(context.Context, string) error { return errors.New("no route") }

func TestCacheBackendHealth(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	t.Run("healthy", func(t *testing.T) {
		c := fakeClient(t, healthyBackend(now))
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		if len(fs) != 1 || fs[0].Code != doctor.CodeBackendHealthy {
			t.Fatalf("want single CB006, got %v", codesOf(fs))
		}
	})

	t.Run("fresh misconfigured backend, no status", func(t *testing.T) {
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: "ns1"},
			Spec: cachev1alpha1.CacheBackendSpec{
				EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{"app": "missing"}},
			},
		}
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "ns1", now, DefaultStaleWindow, okDial)
		for _, want := range []string{doctor.CodeBackendNotReady, doctor.CodeBackendSelectorMismatch, doctor.CodeBackendNotReportingState, doctor.CodeBackendEndpointUnreachable} {
			if hasCode(fs, want) == nil {
				t.Errorf("want %s, got %v", want, codesOf(fs))
			}
		}
		if hasCode(fs, doctor.CodeBackendHealthy) != nil {
			t.Errorf("did not expect CB006 for a broken backend")
		}
	})

	t.Run("selector mismatch falls back to live pod count", func(t *testing.T) {
		// No status.matchedEnginePods, but a live pod matches the selector =>
		// no mismatch finding.
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "live", Namespace: "ns1"},
			Spec: cachev1alpha1.CacheBackendSpec{
				EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{"app": "engine"}},
			},
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns1", Labels: map[string]string{"app": "engine"}}}
		c := fakeClient(t, cb, pod)
		fs := CacheBackendHealth(ctx, c, "ns1", now, DefaultStaleWindow, okDial)
		if hasCode(fs, doctor.CodeBackendSelectorMismatch) != nil {
			t.Fatalf("did not expect CB002 when a live pod matches, got %v", codesOf(fs))
		}
	})

	t.Run("stale events", func(t *testing.T) {
		cb := healthyBackend(now)
		cb.Status.IndexParticipation.LastEventAt = &metav1.Time{Time: now.Add(-10 * time.Minute)}
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		f := hasCode(fs, doctor.CodeBackendStale)
		if f == nil || f.Status != doctor.StatusWarn {
			t.Fatalf("want CB004 WARN, got %v", codesOf(fs))
		}
	})

	t.Run("endpoint unreachable via dialer", func(t *testing.T) {
		cb := healthyBackend(now)
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, badDial)
		f := hasCode(fs, doctor.CodeBackendEndpointUnreachable)
		if f == nil {
			t.Fatalf("want CB005, got %v", codesOf(fs))
		}
	})

	t.Run("nil dialer skips tcp probe", func(t *testing.T) {
		cb := healthyBackend(now)
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, nil)
		if len(fs) != 1 || fs[0].Code != doctor.CodeBackendHealthy {
			t.Fatalf("want CB006 with nil dialer, got %v", codesOf(fs))
		}
	})

	t.Run("list error", func(t *testing.T) {
		c := listErrClient{Client: fakeClient(t), failOn: func(client.ObjectList) bool { return true }, err: errors.New("boom")}
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		f := hasCode(fs, doctor.CodeAPIReadFailed)
		if f == nil || f.Status != doctor.StatusFail {
			t.Fatalf("want API001 FAIL, got %v", codesOf(fs))
		}
	})
}

func TestCacheBackendHealthMessageBranches(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	t.Run("ready false surfaces reason", func(t *testing.T) {
		cb := healthyBackend(now)
		cb.Status.Conditions = []metav1.Condition{readyCond(metav1.ConditionFalse, "NoKVEventsObserved", "no events seen")}
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		f := hasCode(fs, doctor.CodeBackendNotReady)
		if f == nil || !strings.Contains(f.Message, "NoKVEventsObserved") {
			t.Fatalf("CB001 should surface reason, got %v", fs)
		}
	})

	t.Run("FunctionalProbeOK false surfaces CB007", func(t *testing.T) {
		cb := healthyBackend(now)
		cb.Status.Conditions = append(cb.Status.Conditions, metav1.Condition{
			Type: conditionFunctionalProbeOK, Status: metav1.ConditionFalse,
			Reason: "ProbeFailed", Message: "lookup returned NO_HINT for a seeded prefix",
		})
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		f := hasCode(fs, doctor.CodeBackendFunctionalProbeFailing)
		if f == nil || !strings.Contains(f.Message, "ProbeFailed") {
			t.Fatalf("want CB007 surfacing the probe reason, got %v", codesOf(fs))
		}
	})

	t.Run("FunctionalProbeOK true does not flag", func(t *testing.T) {
		cb := healthyBackend(now)
		cb.Status.Conditions = append(cb.Status.Conditions, metav1.Condition{
			Type: conditionFunctionalProbeOK, Status: metav1.ConditionTrue, Reason: "ProbeOK",
		})
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		if hasCode(fs, doctor.CodeBackendFunctionalProbeFailing) != nil {
			t.Fatalf("FunctionalProbeOK=True must not flag CB007, got %v", codesOf(fs))
		}
		if len(fs) != 1 || fs[0].Code != doctor.CodeBackendHealthy {
			t.Fatalf("a fully-healthy backend with ProbeOK should be CB006, got %v", codesOf(fs))
		}
	})

	t.Run("FunctionalProbeOK absent on External backend is ignored", func(t *testing.T) {
		// External backends skip the managed axes entirely, including the probe.
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "ext", Namespace: "ns1"},
			Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeExternal, Endpoint: "h:1"},
			Status: cachev1alpha1.CacheBackendStatus{
				Endpoint:   "h:1",
				Conditions: []metav1.Condition{readyCond(metav1.ConditionTrue, "EndpointAccepted", "ok")},
			},
		}
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		if hasCode(fs, doctor.CodeBackendFunctionalProbeFailing) != nil {
			t.Fatalf("External backend must not evaluate FunctionalProbeOK, got %v", codesOf(fs))
		}
	})

	t.Run("prefix present but lastEventAt nil is not-reporting", func(t *testing.T) {
		// Zero warm prefixes is a VALID state; the not-reporting signal is the
		// absence of any KV event (lastEventAt nil), reported as CB003 — never a
		// CB003 driven by prefixCount==0 alone.
		cb := healthyBackend(now)
		cb.Status.IndexParticipation = &cachev1alpha1.CacheBackendIndexParticipation{PrefixCount: 3, LastEventAt: nil}
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		if hasCode(fs, doctor.CodeBackendNotReportingState) == nil {
			t.Fatalf("want CB003 for nil lastEventAt, got %v", codesOf(fs))
		}
		if hasCode(fs, doctor.CodeBackendStale) != nil {
			t.Fatalf("should not be CB004 when lastEventAt is nil, got %v", codesOf(fs))
		}
	})

	t.Run("drained backend that ever observed an event is healthy", func(t *testing.T) {
		// lastEventAt is cleared on drain, but firstKVEventObservedAt is the
		// durable latch — a backend that already proved its publisher works is
		// not "never reporting".
		cb := healthyBackend(now)
		cb.Status.FirstKVEventObservedAt = &metav1.Time{Time: now.Add(-1 * time.Hour)}
		cb.Status.IndexParticipation = &cachev1alpha1.CacheBackendIndexParticipation{PrefixCount: 0, LastEventAt: nil}
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		if hasCode(fs, doctor.CodeBackendNotReportingState) != nil {
			t.Fatalf("drained backend with firstKVEventObservedAt set must not be CB003, got %v", codesOf(fs))
		}
		if len(fs) != 1 || fs[0].Code != doctor.CodeBackendHealthy {
			t.Fatalf("want CB006, got %v", codesOf(fs))
		}
	})

	t.Run("zero prefixes with a fresh event is healthy", func(t *testing.T) {
		cb := healthyBackend(now)
		cb.Status.IndexParticipation = &cachev1alpha1.CacheBackendIndexParticipation{PrefixCount: 0, LastEventAt: &metav1.Time{Time: now.Add(-5 * time.Second)}}
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		if len(fs) != 1 || fs[0].Code != doctor.CodeBackendHealthy {
			t.Fatalf("prefixCount==0 with a fresh event must be CB006 (idle is healthy), got %v", codesOf(fs))
		}
	})

	t.Run("pod-list error is inconclusive (API001), not a selector mismatch", func(t *testing.T) {
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "ns1"},
			Spec:       cachev1alpha1.CacheBackendSpec{EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{"app": "engine"}}},
		}
		c := listErrClient{Client: fakeClient(t, cb), failOn: func(l client.ObjectList) bool {
			_, ok := l.(*corev1.PodList)
			return ok
		}, err: errors.New("pods down")}
		fs := CacheBackendHealth(ctx, c, "ns1", now, DefaultStaleWindow, okDial)
		if hasCode(fs, doctor.CodeAPIReadFailed) == nil {
			t.Fatalf("pod-list error should be API001 (inconclusive), got %v", codesOf(fs))
		}
		if hasCode(fs, doctor.CodeBackendSelectorMismatch) != nil {
			t.Fatalf("pod-list error must NOT be reported as CB002, got %v", codesOf(fs))
		}
	})

	t.Run("External backend skips pod-match and index-participation axes", func(t *testing.T) {
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "ext", Namespace: "ns1"},
			Spec: cachev1alpha1.CacheBackendSpec{
				Type:     cachev1alpha1.CacheBackendTypeExternal,
				Endpoint: "cache.example.com:8200",
			},
			Status: cachev1alpha1.CacheBackendStatus{
				Endpoint:   "cache.example.com:8200",
				Conditions: []metav1.Condition{readyCond(metav1.ConditionTrue, "EndpointAccepted", "ready")},
			},
		}
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		if len(fs) != 1 || fs[0].Code != doctor.CodeBackendHealthy {
			t.Fatalf("External backend should be CB006 (no CB002/CB003 false positives), got %v", codesOf(fs))
		}
	})

	t.Run("selectorless managed backend skips the matched-pods axis", func(t *testing.T) {
		cb := healthyBackend(now)
		cb.Spec.EngineSelector = nil
		cb.Status.MatchedEnginePods = nil
		c := fakeClient(t, cb)
		fs := CacheBackendHealth(ctx, c, "", now, DefaultStaleWindow, okDial)
		if hasCode(fs, doctor.CodeBackendSelectorMismatch) != nil {
			t.Fatalf("a backend with no engineSelector has nothing to mismatch, got %v", codesOf(fs))
		}
	})
}

func TestPrefixCountNil(t *testing.T) {
	if prefixCount(nil) != 0 {
		t.Error("prefixCount(nil) must be 0")
	}
}

// --- EnginePodInjectionAudit ------------------------------------------------

func podEvent(podName, ns, reason, uid string, when time.Time) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: podName + "-" + reason, Namespace: ns},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: ns, Name: podName, UID: k8stypes.UID(uid)},
		Reason:         reason,
		Message:        "msg",
		LastTimestamp:  metav1.Time{Time: when},
	}
}

// podEventV1 builds an events.k8s.io/v1 Event keyed to the pod via the
// Regarding field — the shape the cache-plane controller actually emits
// (k8s.io/client-go/tools/events recorder writes this API, not core/v1).
// Test fixtures use this so the audit's "annotation-or-Event" fallback is
// validated against the real event surface produced in live clusters.
func podEventV1(podName, ns, reason, uid string, when time.Time) *eventsv1.Event {
	return &eventsv1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: podName + "-" + reason + "-v1", Namespace: ns},
		Regarding:  corev1.ObjectReference{Kind: "Pod", Namespace: ns, Name: podName, UID: k8stypes.UID(uid)},
		Reason:     reason,
		Note:       "msg-v1",
		EventTime:  metav1.MicroTime{Time: when},
	}
}

func TestEnginePodInjectionAudit(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	t.Run("no selectored backends returns nil without listing pods/events", func(t *testing.T) {
		// Only a selectorless backend exists → nothing to claim, early return.
		c := fakeClient(t, &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "nosel", Namespace: "ns1"}})
		if fs := EnginePodInjectionAudit(ctx, c, ""); fs != nil {
			t.Fatalf("want nil for selectorless-only, got %v", codesOf(fs))
		}
	})

	backend := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "be", Namespace: "ns1", UID: "be-uid"},
		Spec: cachev1alpha1.CacheBackendSpec{
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{"app": "engine"}},
		},
	}
	// also a selector-less backend (skipped) for coverage
	noSelector := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "nosel", Namespace: "ns1"}}
	// Injected via the durable annotation, validated against the backend UID.
	injected := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "inj", Namespace: "ns1", UID: "inj-uid", Labels: map[string]string{"app": "engine"}, Annotations: map[string]string{annotationInjectedBy: "ns1/be", annotationInjectedByUID: "be-uid"}}}
	// A FORGED injected-by annotation whose UID does not match the backend — must
	// NOT be trusted; with no Event it falls through to EP001.
	forged := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "forged", Namespace: "ns1", UID: "forged-uid", Labels: map[string]string{"app": "engine"}, Annotations: map[string]string{annotationInjectedBy: "ns1/be", annotationInjectedByUID: "wrong-uid"}}}
	// Injected per the LEGACY core/v1 Event (matched to its UID), missing the
	// annotation. Exercises the back-compat path for older clusters / third-party
	// recorders.
	injectedViaLegacyEvent := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "inj-evt", Namespace: "ns1", UID: "inj-evt-uid", Labels: map[string]string{"app": "engine"}}}
	// Injected per the MODERN events.k8s.io/v1 Event (Regarding.UID matched).
	// This is the shape the cache-plane controller actually emits today, so this
	// case pins the audit against the production event surface.
	injectedViaModernEvent := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "inj-evt-v1", Namespace: "ns1", UID: "inj-evt-v1-uid", Labels: map[string]string{"app": "engine"}}}
	notInjected := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "uninj", Namespace: "ns1", UID: "uninj-uid", Labels: map[string]string{"app": "engine"}}}
	unrelated := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "ns1", Labels: map[string]string{"app": "web"}}}

	c := fakeClient(t, backend, noSelector, injected, forged, injectedViaLegacyEvent, injectedViaModernEvent, notInjected, unrelated,
		podEvent("inj-evt", "ns1", eventInjectedByCacheBackend, "inj-evt-uid", now),
		podEventV1("inj-evt-v1", "ns1", eventInjectedByCacheBackend, "inj-evt-v1-uid", now))

	fs := EnginePodInjectionAudit(ctx, c, "")
	injectedCount, notInjectedCount := 0, 0
	for _, f := range fs {
		switch f.Code {
		case doctor.CodeEnginePodInjected:
			injectedCount++
		case doctor.CodeEnginePodNotInjected:
			notInjectedCount++
		}
	}
	// inj (annotation+uid), inj-evt (legacy event+uid), inj-evt-v1 (modern
	// event+uid) => 3 EP002.
	if injectedCount != 3 {
		t.Errorf("want 3 EP002 (validated annotation + UID-matched event in BOTH event APIs), got %d (%v)", injectedCount, codesOf(fs))
	}
	// forged (uid mismatch, no event) and uninj (nothing) => 2 EP001.
	if notInjectedCount != 2 {
		t.Errorf("want 2 EP001 (forged annotation + bare pod), got %d (%v)", notInjectedCount, codesOf(fs))
	}
	if f := hasCode(fs, doctor.CodeEnginePodNotInjected); f == nil || f.Status != doctor.StatusWarn {
		t.Errorf("EP001 must be WARN")
	}
	// 'unrelated' must not appear; 5 matching pods => 5 findings.
	if len(fs) != 5 {
		t.Errorf("expected exactly 5 findings (one per matching pod), got %v", codesOf(fs))
	}

	t.Run("backend list error", func(t *testing.T) {
		c := listErrClient{Client: fakeClient(t), failOn: func(l client.ObjectList) bool {
			_, ok := l.(*cachev1alpha1.CacheBackendList)
			return ok
		}, err: errors.New("x")}
		fs := EnginePodInjectionAudit(ctx, c, "")
		if hasCode(fs, doctor.CodeAPIReadFailed) == nil {
			t.Fatalf("want API001, got %v", codesOf(fs))
		}
	})

	t.Run("pod list error", func(t *testing.T) {
		c := listErrClient{Client: fakeClient(t, backend), failOn: func(l client.ObjectList) bool {
			_, ok := l.(*corev1.PodList)
			return ok
		}, err: errors.New("x")}
		fs := EnginePodInjectionAudit(ctx, c, "")
		if hasCode(fs, doctor.CodeAPIReadFailed) == nil {
			t.Fatalf("want API001, got %v", codesOf(fs))
		}
	})

	t.Run("legacy event list error", func(t *testing.T) {
		// Use a pod WITHOUT the injected-by annotation so the audit falls through
		// to the (failing) Event list rather than short-circuiting on the
		// annotation.
		unannotated := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "uninj", Namespace: "ns1", Labels: map[string]string{"app": "engine"}}}
		c := listErrClient{Client: fakeClient(t, backend, unannotated), failOn: func(l client.ObjectList) bool {
			_, ok := l.(*corev1.EventList)
			return ok
		}, err: errors.New("x")}
		fs := EnginePodInjectionAudit(ctx, c, "")
		if hasCode(fs, doctor.CodeAPIReadFailed) == nil {
			t.Fatalf("want API001, got %v", codesOf(fs))
		}
	})

	t.Run("modern event list error", func(t *testing.T) {
		// Same setup but fail the events.k8s.io/v1 list — production controllers
		// emit via that API, so a permissions gap there must also surface as
		// API001 rather than a silent skip.
		unannotated := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "uninj", Namespace: "ns1", Labels: map[string]string{"app": "engine"}}}
		c := listErrClient{Client: fakeClient(t, backend, unannotated), failOn: func(l client.ObjectList) bool {
			_, ok := l.(*eventsv1.EventList)
			return ok
		}, err: errors.New("x")}
		fs := EnginePodInjectionAudit(ctx, c, "")
		if hasCode(fs, doctor.CodeAPIReadFailed) == nil {
			t.Fatalf("want API001, got %v", codesOf(fs))
		}
	})
}

// --- OrphanPods -------------------------------------------------------------

func TestOrphanPods(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	// Recent legacy-API orphan + recent modern-API orphan: both must light up
	// OP001, so the check works whether the future emitter uses core/v1 or
	// events.k8s.io/v1.
	recentLegacy := podEvent("orphan-legacy", "ns1", eventNoMatchingCacheBackend, "", now.Add(-1*time.Hour))
	recentModern := podEventV1("orphan-modern", "ns1", eventNoMatchingCacheBackend, "", now.Add(-30*time.Minute))
	// Outside the window: ignored.
	old := podEvent("stale", "ns1", eventNoMatchingCacheBackend, "", now.Add(-48*time.Hour))
	oldModern := podEventV1("stale-modern", "ns1", eventNoMatchingCacheBackend, "", now.Add(-48*time.Hour))
	// Wrong reason: ignored across both APIs.
	wrongReason := podEvent("ok", "ns1", "SomethingElse", "", now)
	wrongReasonModern := podEventV1("ok-modern", "ns1", "SomethingElse", "", now)

	c := fakeClient(t, recentLegacy, recentModern, old, oldModern, wrongReason, wrongReasonModern)
	fs := OrphanPods(ctx, c, "", now, DefaultOrphanWindow)
	if len(fs) != 2 {
		t.Fatalf("want 2 OP001 findings (legacy + modern Event APIs in window), got %d (%v)", len(fs), codesOf(fs))
	}
	for _, f := range fs {
		if f.Code != doctor.CodeOrphanPod || f.Status != doctor.StatusWarn {
			t.Errorf("each OP001 finding must be WARN; got %+v", f)
		}
	}

	t.Run("legacy list error", func(t *testing.T) {
		c := listErrClient{Client: fakeClient(t), failOn: func(l client.ObjectList) bool {
			_, ok := l.(*corev1.EventList)
			return ok
		}, err: errors.New("x")}
		fs := OrphanPods(ctx, c, "", now, DefaultOrphanWindow)
		if hasCode(fs, doctor.CodeAPIReadFailed) == nil {
			t.Fatalf("want API001, got %v", codesOf(fs))
		}
	})
	t.Run("modern list error", func(t *testing.T) {
		c := listErrClient{Client: fakeClient(t), failOn: func(l client.ObjectList) bool {
			_, ok := l.(*eventsv1.EventList)
			return ok
		}, err: errors.New("x")}
		fs := OrphanPods(ctx, c, "", now, DefaultOrphanWindow)
		if hasCode(fs, doctor.CodeAPIReadFailed) == nil {
			t.Fatalf("want API001, got %v", codesOf(fs))
		}
	})
}

// --- CacheTenantHealth ------------------------------------------------------

func TestCacheTenantHealth(t *testing.T) {
	ctx := context.Background()

	over := &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "ns1"},
		Spec:       cachev1alpha1.CacheTenantSpec{TenantID: "tenant-a", Quota: &cachev1alpha1.CacheTenantQuotaSpec{MaxIndexEntries: ptr(int64(10))}},
		Status: cachev1alpha1.CacheTenantStatus{
			IndexEntries: ptr(int64(42)),
			Conditions:   []metav1.Condition{{Type: conditionQuotaExceeded, Status: metav1.ConditionTrue, Message: "evicting oldest"}},
		},
	}
	within := &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: "t2", Namespace: "ns1"},
		Spec:       cachev1alpha1.CacheTenantSpec{TenantID: "tenant-b"},
		Status:     cachev1alpha1.CacheTenantStatus{Conditions: []metav1.Condition{{Type: conditionQuotaExceeded, Status: metav1.ConditionFalse}}},
	}
	noCondition := &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: "t3", Namespace: "ns1"},
		Spec:       cachev1alpha1.CacheTenantSpec{TenantID: "tenant-c"},
	}

	c := fakeClient(t, over, within, noCondition)
	fs := CacheTenantHealth(ctx, c, "")
	if f := hasCode(fs, doctor.CodeTenantQuotaExceeded); f == nil || f.Status != doctor.StatusWarn {
		t.Fatalf("want CT001 WARN, got %v", codesOf(fs))
	}
	// over -> CT001, within + noCondition -> CT002 (x2)
	healthy := 0
	for _, f := range fs {
		if f.Code == doctor.CodeTenantHealthy {
			healthy++
		}
	}
	if healthy != 2 {
		t.Errorf("want 2 CT002, got %d (%v)", healthy, codesOf(fs))
	}
	// surface the entry count and quota in the message.
	if f := hasCode(fs, doctor.CodeTenantQuotaExceeded); f != nil {
		for _, want := range []string{"42", "10", "evicting oldest"} {
			if !strings.Contains(f.Message, want) {
				t.Errorf("CT001 message %q missing %q", f.Message, want)
			}
		}
	}

	t.Run("list error", func(t *testing.T) {
		c := listErrClient{Client: fakeClient(t), failOn: func(client.ObjectList) bool { return true }, err: errors.New("x")}
		fs := CacheTenantHealth(ctx, c, "")
		if hasCode(fs, doctor.CodeAPIReadFailed) == nil {
			t.Fatalf("want API001, got %v", codesOf(fs))
		}
	})
}

// --- CachePolicyCoverage ----------------------------------------------------

func TestCachePolicyCoverage(t *testing.T) {
	ctx := context.Background()

	t.Run("missing and present", func(t *testing.T) {
		beA := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "covered"}}
		beB := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "bare"}}
		pol := &cachev1alpha1.CachePolicy{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "covered"}}
		c := fakeClient(t, beA, beB, pol)
		fs := CachePolicyCoverage(ctx, c, "")
		// deterministic order: "bare" (missing/INFO) then "covered" (present/OK)
		if got := codesOf(fs); len(got) != 2 || got[0] != doctor.CodePolicyCoverageMissing || got[1] != doctor.CodePolicyCoveragePresent {
			t.Fatalf("got %v, want [CP001 CP002]", got)
		}
		if hasCode(fs, doctor.CodePolicyCoverageMissing).Status != doctor.StatusInfo {
			t.Errorf("CP001 must be INFO")
		}
	})

	t.Run("no backends", func(t *testing.T) {
		c := fakeClient(t)
		if fs := CachePolicyCoverage(ctx, c, ""); fs != nil {
			t.Fatalf("want nil for no backends, got %v", codesOf(fs))
		}
	})

	t.Run("backend list error", func(t *testing.T) {
		c := listErrClient{Client: fakeClient(t), failOn: func(l client.ObjectList) bool {
			_, ok := l.(*cachev1alpha1.CacheBackendList)
			return ok
		}, err: errors.New("x")}
		fs := CachePolicyCoverage(ctx, c, "")
		if hasCode(fs, doctor.CodeAPIReadFailed) == nil {
			t.Fatalf("want API001, got %v", codesOf(fs))
		}
	})

	t.Run("policy list error", func(t *testing.T) {
		be := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns1"}}
		c := listErrClient{Client: fakeClient(t, be), failOn: func(l client.ObjectList) bool {
			_, ok := l.(*cachev1alpha1.CachePolicyList)
			return ok
		}, err: errors.New("x")}
		fs := CachePolicyCoverage(ctx, c, "")
		if hasCode(fs, doctor.CodeAPIReadFailed) == nil {
			t.Fatalf("want API001, got %v", codesOf(fs))
		}
	})
}

// --- helpers ----------------------------------------------------------------

func TestSelectorMatches(t *testing.T) {
	if selectorMatches(nil, map[string]string{"a": "b"}) {
		t.Error("empty selector must match nothing")
	}
	if selectorMatches(map[string]string{"a": "b"}, map[string]string{"a": "c"}) {
		t.Error("mismatched value must not match")
	}
	if !selectorMatches(map[string]string{"a": "b"}, map[string]string{"a": "b", "x": "y"}) {
		t.Error("subset selector must match")
	}
}

func TestFindCondition(t *testing.T) {
	conds := []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}}
	if findCondition(conds, "Ready") == nil {
		t.Error("want Ready")
	}
	if findCondition(conds, "Absent") != nil {
		t.Error("want nil for absent type")
	}
}

func TestLegacyEventTime(t *testing.T) {
	base := time.Now().Truncate(time.Second)
	series := corev1.Event{Series: &corev1.EventSeries{LastObservedTime: metav1.MicroTime{Time: base}}}
	if !legacyEventTime(series).Equal(base) {
		t.Error("series time preferred")
	}
	last := corev1.Event{LastTimestamp: metav1.Time{Time: base}}
	if !legacyEventTime(last).Equal(base) {
		t.Error("lastTimestamp fallback")
	}
	etime := corev1.Event{EventTime: metav1.MicroTime{Time: base}}
	if !legacyEventTime(etime).Equal(base) {
		t.Error("eventTime fallback")
	}
	created := corev1.Event{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.Time{Time: base}}}
	if !legacyEventTime(created).Equal(base) {
		t.Error("creationTimestamp fallback")
	}
}

func TestModernEventTime(t *testing.T) {
	base := time.Now().Truncate(time.Second)
	// The events.k8s.io/v1 type has no LastTimestamp; the canonical fields are
	// Series.LastObservedTime and EventTime, with CreationTimestamp as a final
	// fallback.
	series := eventsv1.Event{Series: &eventsv1.EventSeries{LastObservedTime: metav1.MicroTime{Time: base}}}
	if !modernEventTime(series).Equal(base) {
		t.Error("series time preferred")
	}
	etime := eventsv1.Event{EventTime: metav1.MicroTime{Time: base}}
	if !modernEventTime(etime).Equal(base) {
		t.Error("eventTime fallback")
	}
	created := eventsv1.Event{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.Time{Time: base}}}
	if !modernEventTime(created).Equal(base) {
		t.Error("creationTimestamp fallback")
	}
}

func TestResourceRef(t *testing.T) {
	if got := resourceRef("CacheBackend", "ns", "x"); got != "cachebackend/ns/x" {
		t.Errorf("got %q", got)
	}
}

// --- Run orchestration ------------------------------------------------------

func TestRun(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)

	snap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer snap.Close()

	deps := Deps{
		K8s:          fakeClient(t, healthyBackend(now)),
		Health:       stubHealth{resp: &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}},
		ServerTarget: "host:9090",
		HTTP:         snap.Client(),
		SnapshotURL:  snap.URL,
		PolicyURL:    snap.URL,
		ProbeURL:     snap.URL,
		Token:        "tok",
		DialTCP:      okDial,
		Now:          func() time.Time { return now },
		StaleWindow:  5 * time.Minute,
		OrphanWindow: 12 * time.Hour,
	}
	report := Run(ctx, deps)
	// Endpoint checks present.
	for _, want := range []string{doctor.CodeServerServing, doctor.CodeSnapshotOK, doctor.CodePolicyRouteWired, doctor.CodeProbeRouteWired, doctor.CodeBackendHealthy} {
		if hasCode(report.Findings, want) == nil {
			t.Errorf("Run missing %s; got %v", want, codesOf(report.Findings))
		}
	}
	if report.ExitCode() != 0 {
		t.Errorf("healthy run should exit 0, got %d", report.ExitCode())
	}

	t.Run("config-only skips endpoint checks and uses defaults", func(t *testing.T) {
		// Zero Now/windows exercise the default fallbacks.
		deps := Deps{K8s: fakeClient(t, healthyBackend(time.Now())), SkipEndpointChecks: true}
		report := Run(ctx, deps)
		for _, unwanted := range []string{doctor.CodeServerServing, doctor.CodeSnapshotOK, doctor.CodePolicyRouteWired, doctor.CodeProbeRouteWired} {
			if hasCode(report.Findings, unwanted) != nil {
				t.Errorf("config-only must skip %s", unwanted)
			}
		}
		if hasCode(report.Findings, doctor.CodeBackendHealthy) == nil {
			t.Errorf("config-only must still run CacheBackend check")
		}
	})
}
