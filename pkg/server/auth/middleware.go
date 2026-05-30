package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Result tags the outcome of a single auth attempt. The middleware reports it
// to a ResultRecorder so the wrapping HTTP server can publish a Prometheus
// counter without this package depending on the metrics registry.
type Result string

const (
	ResultOK        Result = "ok"
	ResultUnauth    Result = "unauth"    // missing/invalid bearer
	ResultForbidden Result = "forbidden" // valid token, wrong identity
	ResultError     Result = "error"     // TokenReview API error
)

// ResultRecorder is the metric hook the middleware invokes on every request.
// It is called exactly once per call.
type ResultRecorder interface {
	RecordAuthResult(r Result)
}

// ResultRecorderFunc adapts a plain func into a ResultRecorder.
type ResultRecorderFunc func(Result)

func (f ResultRecorderFunc) RecordAuthResult(r Result) { f(r) }

// DefaultCacheTTL is how long a successful TokenReview is cached.
//
// The CacheIndex poller scrapes every ~30s today (DefaultRefreshInterval). A
// 30s TTL means one TokenReview per scrape under steady state — the cache
// avoids hammering the apiserver if the cadence ever tightens while staying
// short enough that a SA token rotation (or a Pod restart) is picked up on
// the next scrape rather than persisting stale auth state.
const DefaultCacheTTL = 30 * time.Second

// DefaultCacheMaxEntries bounds the LRU. The expected steady-state population
// is one (the controller's projected token), so the cap exists only to absorb
// transient rotation churn.
const DefaultCacheMaxEntries = 32

// TokenReviewer is the slice of kubernetes.Interface the middleware actually
// uses. Keeping it narrow lets unit tests pass a hand-rolled fake instead of
// the full fake clientset.
type TokenReviewer interface {
	CreateTokenReview(ctx context.Context, tr *authnv1.TokenReview) (*authnv1.TokenReview, error)
}

// clientsetReviewer adapts a real kubernetes.Interface to TokenReviewer.
type clientsetReviewer struct {
	client kubernetes.Interface
}

func (c clientsetReviewer) CreateTokenReview(ctx context.Context, tr *authnv1.TokenReview) (*authnv1.TokenReview, error) {
	return c.client.AuthenticationV1().TokenReviews().Create(ctx, tr, metav1.CreateOptions{})
}

// FromClientset wraps a real clientset for use with NewAuthenticator.
func FromClientset(client kubernetes.Interface) TokenReviewer {
	return clientsetReviewer{client: client}
}

// Authenticator validates bearer tokens against the apiserver's TokenReview
// endpoint and admits only the configured ServiceAccount.
type Authenticator struct {
	reviewer   TokenReviewer
	expectedSA string // "system:serviceaccount:<ns>:<sa>"
	recorder   ResultRecorder
	cacheTTL   time.Duration
	cacheMax   int
	now        func() time.Time
	mu         sync.Mutex
	cache      map[string]cacheEntry
}

type cacheEntry struct {
	expires time.Time
}

// Options configures an Authenticator.
type Options struct {
	// Reviewer creates TokenReview objects against the apiserver.
	Reviewer TokenReviewer
	// ExpectedServiceAccount is the canonical SA username the controller
	// authenticates with, e.g. "system:serviceaccount:inference-cache-system:inference-cache-controller-manager".
	ExpectedServiceAccount string
	// Recorder receives one Result per request. Optional; nil disables metrics.
	Recorder ResultRecorder
	// CacheTTL controls how long a successful TokenReview is reused. <=0 → DefaultCacheTTL.
	CacheTTL time.Duration
	// CacheMaxEntries bounds the in-process LRU. <=0 → DefaultCacheMaxEntries.
	CacheMaxEntries int
	// Now is the clock; nil → time.Now. Exposed for tests.
	Now func() time.Time
}

// NewAuthenticator constructs an Authenticator. ExpectedServiceAccount must be
// non-empty; Reviewer must be non-nil.
func NewAuthenticator(opts Options) (*Authenticator, error) {
	if opts.Reviewer == nil {
		return nil, errors.New("auth: Reviewer is required")
	}
	if opts.ExpectedServiceAccount == "" {
		return nil, errors.New("auth: ExpectedServiceAccount is required")
	}
	ttl := opts.CacheTTL
	if ttl <= 0 {
		ttl = DefaultCacheTTL
	}
	cap := opts.CacheMaxEntries
	if cap <= 0 {
		cap = DefaultCacheMaxEntries
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Authenticator{
		reviewer:   opts.Reviewer,
		expectedSA: opts.ExpectedServiceAccount,
		recorder:   opts.Recorder,
		cacheTTL:   ttl,
		cacheMax:   cap,
		now:        now,
		cache:      make(map[string]cacheEntry),
	}, nil
}

// Middleware returns an http.Handler that authenticates the request and then
// invokes next. Each invocation records exactly one Result on the recorder.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := extractBearer(r.Header.Get("Authorization"))
		if !ok {
			a.record(ResultUnauth)
			http.Error(w, "unauthorized\n", http.StatusUnauthorized)
			return
		}

		hash := sha256Hex(token)
		if a.cacheHit(hash) {
			a.record(ResultOK)
			next.ServeHTTP(w, r)
			return
		}

		tr, err := a.reviewer.CreateTokenReview(r.Context(), &authnv1.TokenReview{
			Spec: authnv1.TokenReviewSpec{Token: token},
		})
		if err != nil {
			// Fail-closed: don't admit on apiserver flakes.
			a.record(ResultError)
			http.Error(w, "token review unavailable\n", http.StatusServiceUnavailable)
			return
		}
		if !tr.Status.Authenticated {
			a.record(ResultUnauth)
			http.Error(w, "unauthorized\n", http.StatusUnauthorized)
			return
		}
		if tr.Status.User.Username != a.expectedSA {
			a.record(ResultForbidden)
			http.Error(w, "forbidden\n", http.StatusForbidden)
			return
		}

		a.cachePut(hash)
		a.record(ResultOK)
		next.ServeHTTP(w, r)
	})
}

// extractBearer returns the token portion of an "Authorization: Bearer …"
// header, or ("", false) if the header is missing or malformed.
func extractBearer(h string) (string, bool) {
	const prefix = "Bearer "
	if h == "" {
		return "", false
	}
	if !strings.HasPrefix(h, prefix) {
		return "", false
	}
	tok := strings.TrimSpace(h[len(prefix):])
	if tok == "" {
		return "", false
	}
	return tok, true
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (a *Authenticator) record(r Result) {
	if a.recorder != nil {
		a.recorder.RecordAuthResult(r)
	}
}

// cacheHit reports whether hash is in the cache and has not expired. Expired
// entries are removed on access.
func (a *Authenticator) cacheHit(hash string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.cache[hash]
	if !ok {
		return false
	}
	if !a.now().Before(e.expires) {
		delete(a.cache, hash)
		return false
	}
	return true
}

// cachePut stores hash with the configured TTL. If the cache is at capacity,
// expired entries are pruned first; if still full, the entry expiring soonest
// is evicted (approximate LRU — sufficient for a cache that holds 1-2 entries
// in practice).
func (a *Authenticator) cachePut(hash string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	if len(a.cache) >= a.cacheMax {
		// First pass: prune anything already expired.
		for k, e := range a.cache {
			if !now.Before(e.expires) {
				delete(a.cache, k)
			}
		}
		// Still full? Evict the entry that will expire next.
		if len(a.cache) >= a.cacheMax {
			var (
				victim    string
				victimExp time.Time
				first     = true
			)
			for k, e := range a.cache {
				if first || e.expires.Before(victimExp) {
					victim, victimExp = k, e.expires
					first = false
				}
			}
			delete(a.cache, victim)
		}
	}
	a.cache[hash] = cacheEntry{expires: now.Add(a.cacheTTL)}
}
