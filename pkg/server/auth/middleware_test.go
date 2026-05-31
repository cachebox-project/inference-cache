package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
)

const (
	expectedSA = "system:serviceaccount:inference-cache-system:inference-cache-controller-manager"
	otherSA    = "system:serviceaccount:other:somebody"
)

// fakeReviewer is a hand-rolled TokenReviewer so the unit tests don't pull in
// the full fake clientset machinery. Behaviour per token comes from `respond`.
type fakeReviewer struct {
	mu      sync.Mutex
	calls   int
	respond func(token string) (*authnv1.TokenReview, error)
}

func (f *fakeReviewer) CreateTokenReview(_ context.Context, tr *authnv1.TokenReview) (*authnv1.TokenReview, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return f.respond(tr.Spec.Token)
}

func (f *fakeReviewer) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type recordingRecorder struct {
	mu      sync.Mutex
	results []Result
}

func (r *recordingRecorder) RecordAuthResult(result Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, result)
}

func (r *recordingRecorder) Last() Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.results) == 0 {
		return ""
	}
	return r.results[len(r.results)-1]
}

func (r *recordingRecorder) All() []Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Result, len(r.results))
	copy(out, r.results)
	return out
}

func newTestAuth(t *testing.T, reviewer TokenReviewer, recorder ResultRecorder, now func() time.Time) *Authenticator {
	t.Helper()
	a, err := NewAuthenticator(Options{
		Reviewer:               reviewer,
		ExpectedServiceAccount: expectedSA,
		Recorder:               recorder,
		Now:                    now,
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	return a
}

// okHandler increments calls when invoked; lets each test confirm whether
// the wrapped handler actually ran.
type okHandler struct{ calls atomic.Int32 }

func (h *okHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	h.calls.Add(1)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// doRequest fires one request through the middleware-wrapped handler and
// returns the response status.
func doRequest(t *testing.T, h http.Handler, header string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/snapshot", nil)
	if header != "" {
		req.Header.Set("Authorization", header)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestMiddleware_RejectsAndAdmits(t *testing.T) {
	reviewer := &fakeReviewer{respond: func(token string) (*authnv1.TokenReview, error) {
		switch token {
		case "good":
			return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{
				Authenticated: true,
				User:          authnv1.UserInfo{Username: expectedSA},
			}}, nil
		case "wrong-sa":
			return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{
				Authenticated: true,
				User:          authnv1.UserInfo{Username: otherSA},
			}}, nil
		case "bad-token":
			return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{
				Authenticated: false,
			}}, nil
		case "boom":
			return nil, errors.New("apiserver unavailable")
		case "status-error":
			// TokenReview returned a non-error HTTP response but with
			// Status.Error populated. kube-apiserver populates this field
			// when the authenticator chain itself failed to run to
			// completion (webhook timeout, parse error, downstream cert
			// failure) — NOT for plain bad tokens, which return
			// Authenticated=false with Status.Error empty. The middleware
			// treats Status.Error as a server-side fault: 503 +
			// result="error" so an apiserver/authenticator hiccup fires
			// the same operator alert as a transport-level review error.
			return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{
				Error:         "webhook authenticator timeout",
				Authenticated: false,
			}}, nil
		case "nil-review":
			// Defensive: a fake/buggy reviewer that returns (nil, nil) must
			// not panic on the .Status access; we treat it as the same
			// fail-closed branch as Status.Error.
			return nil, nil
		}
		return nil, errors.New("unexpected token")
	}}
	recorder := &recordingRecorder{}
	auth := newTestAuth(t, reviewer, recorder, nil)
	inner := &okHandler{}
	h := auth.Middleware(inner)

	cases := []struct {
		name       string
		header     string
		wantStatus int
		wantResult Result
		wantInner  bool
	}{
		{"missing header", "", http.StatusUnauthorized, ResultUnauth, false},
		{"empty bearer", "Bearer ", http.StatusUnauthorized, ResultUnauth, false},
		{"non-bearer scheme", "Basic dXNlcjpwYXNz", http.StatusUnauthorized, ResultUnauth, false},
		{"invalid token", "Bearer bad-token", http.StatusUnauthorized, ResultUnauth, false},
		{"wrong identity", "Bearer wrong-sa", http.StatusForbidden, ResultForbidden, false},
		{"valid token", "Bearer good", http.StatusOK, ResultOK, true},
		{"apiserver error", "Bearer boom", http.StatusServiceUnavailable, ResultError, false},
		// Status.Error populated means the authenticator chain could not
		// answer — webhook timeout, parse error, downstream cert failure.
		// That's a server-side fault, not a client auth failure: 503 +
		// result="error" so it shows up on the same alert surface as a
		// transport-level apiserver hiccup. Plain bad tokens
		// (Authenticated=false, Error="") still take the 401 path above.
		{"token review status.error -> 503 (authenticator-chain fault, not a routine 401)", "Bearer status-error", http.StatusServiceUnavailable, ResultError, false},
		{"nil token review", "Bearer nil-review", http.StatusServiceUnavailable, ResultError, false},
		// RFC 7235 §2.1: auth schemes are case-insensitive tokens. Real-world
		// clients overwhelmingly send "Bearer", but the middleware accepts any
		// case form so a future client that sends "bearer" / "BEARER" isn't
		// rejected by a strict prefix match.
		{"lowercase bearer scheme", "bearer good", http.StatusOK, ResultOK, true},
		{"uppercase bearer scheme", "BEARER good", http.StatusOK, ResultOK, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := inner.calls.Load()
			got := doRequest(t, h, tc.header)
			if got != tc.wantStatus {
				t.Fatalf("status = %d, want %d", got, tc.wantStatus)
			}
			if recorder.Last() != tc.wantResult {
				t.Fatalf("recorder.Last() = %q, want %q", recorder.Last(), tc.wantResult)
			}
			ran := inner.calls.Load() != before
			if ran != tc.wantInner {
				t.Fatalf("handler invoked = %v, want %v", ran, tc.wantInner)
			}
		})
	}
}

// TestMiddleware_CachesValidatedTokens proves a second hit with the same token
// inside the TTL does not re-call TokenReview, while a different token does.
// A clock the test controls keeps the assertion deterministic.
func TestMiddleware_CachesValidatedTokens(t *testing.T) {
	reviewer := &fakeReviewer{respond: func(string) (*authnv1.TokenReview, error) {
		return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{
			Authenticated: true,
			User:          authnv1.UserInfo{Username: expectedSA},
		}}, nil
	}}
	clock := time.Unix(1_700_000_000, 0)
	now := func() time.Time { return clock }
	recorder := &recordingRecorder{}
	auth := newTestAuth(t, reviewer, recorder, now)
	inner := &okHandler{}
	h := auth.Middleware(inner)

	if code := doRequest(t, h, "Bearer good"); code != http.StatusOK {
		t.Fatalf("first call status = %d, want 200", code)
	}
	if reviewer.Calls() != 1 {
		t.Fatalf("reviewer.Calls after first = %d, want 1", reviewer.Calls())
	}

	// Second call with the same token, still inside TTL → no new TokenReview.
	clock = clock.Add(10 * time.Second)
	if code := doRequest(t, h, "Bearer good"); code != http.StatusOK {
		t.Fatalf("second call status = %d, want 200", code)
	}
	if reviewer.Calls() != 1 {
		t.Fatalf("reviewer.Calls after cached call = %d, want 1", reviewer.Calls())
	}

	// A different token still triggers a TokenReview.
	if code := doRequest(t, h, "Bearer other"); code != http.StatusOK {
		t.Fatalf("third call status = %d, want 200", code)
	}
	if reviewer.Calls() != 2 {
		t.Fatalf("reviewer.Calls after distinct token = %d, want 2", reviewer.Calls())
	}

	// Advance past TTL → original token must re-validate.
	clock = clock.Add(DefaultCacheTTL + time.Second)
	if code := doRequest(t, h, "Bearer good"); code != http.StatusOK {
		t.Fatalf("post-TTL call status = %d, want 200", code)
	}
	if reviewer.Calls() != 3 {
		t.Fatalf("reviewer.Calls after TTL expiry = %d, want 3", reviewer.Calls())
	}
}

// TestMiddleware_CacheEvictsWhenOverCap exercises the bounded-TTL eviction so
// pathological token churn cannot grow the cache unboundedly.
func TestMiddleware_CacheEvictsWhenOverCap(t *testing.T) {
	reviewer := &fakeReviewer{respond: func(string) (*authnv1.TokenReview, error) {
		return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{
			Authenticated: true,
			User:          authnv1.UserInfo{Username: expectedSA},
		}}, nil
	}}
	clock := time.Unix(1_700_000_000, 0)
	a, err := NewAuthenticator(Options{
		Reviewer:               reviewer,
		ExpectedServiceAccount: expectedSA,
		CacheMaxEntries:        2,
		Now:                    func() time.Time { return clock },
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	h := a.Middleware(&okHandler{})

	tokens := []string{"a", "b", "c"}
	for _, tok := range tokens {
		if code := doRequest(t, h, "Bearer "+tok); code != http.StatusOK {
			t.Fatalf("token %q status = %d", tok, code)
		}
	}
	a.mu.Lock()
	got := len(a.cache)
	a.mu.Unlock()
	if got > 2 {
		t.Fatalf("cache size = %d, want <=2", got)
	}
}

func TestNewAuthenticator_RequiredFields(t *testing.T) {
	if _, err := NewAuthenticator(Options{}); err == nil {
		t.Fatalf("expected error when Reviewer is nil")
	}
	r := &fakeReviewer{respond: func(string) (*authnv1.TokenReview, error) { return nil, nil }}
	if _, err := NewAuthenticator(Options{Reviewer: r}); err == nil {
		t.Fatalf("expected error when ExpectedServiceAccount is empty")
	}
}
