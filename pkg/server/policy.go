package server

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// PolicyPropagationVersion identifies the schema of the /policy snapshot the
// server accepts. Bumped on a breaking schema change so a stale controller
// can refuse to push (the controller writes the same constant on each PUT).
const PolicyPropagationVersion = 1

// ResolvedPolicy is the slice of CachePolicy the server actually enforces:
// only the fields the policy server needs at lookup/sweep time. The CRD
// types live in api/v1alpha1; the controller flattens them into this shape
// before pushing so pkg/server has no dependency on the CRD package.
//
// Zero values mean "unset / use server default":
//   - EvictionTTL <= 0       → fall back to index.DefaultTTL (via the global
//     WithTTL the binary configured).
//   - MinimumPrefixTokens <= 0 → no threshold (every prefix-hash hit returns).
//   - LookupTimeoutMs <= 0   → no deadline (lookup runs to completion).
type ResolvedPolicy struct {
	// Namespace identifies the CachePolicy's namespace, which in phase-1 is
	// the tenant boundary: a LookupRoute carrying tenant_id="foo" resolves
	// against the CachePolicy in namespace "foo".
	Namespace string `json:"namespace"`

	EvictionTTL         time.Duration `json:"evictionTTL,omitempty"`
	MinimumPrefixTokens int32         `json:"minimumPrefixTokens,omitempty"`
	LookupTimeoutMs     int32         `json:"lookupTimeoutMs,omitempty"`
}

// PolicySnapshot is the full set of CachePolicies the controller pushes on
// each reconcile. Replace-on-write: the controller is the source of truth,
// so the server discards its prior state and adopts the new snapshot. A
// CachePolicy that disappears between snapshots reverts that namespace to
// the server default.
type PolicySnapshot struct {
	Version  int              `json:"version"`
	Policies []ResolvedPolicy `json:"policies"`
}

// PolicyStore is the server-side cache of resolved policies, indexed by
// namespace. Reads (TTL/Lookup) take the read lock; PUTs from /policy take
// the write lock and replace the map atomically. Satisfies index.TTLResolver.
type PolicyStore struct {
	mu       sync.RWMutex
	policies map[string]ResolvedPolicy
}

// NewPolicyStore returns an empty store. Until the controller pushes a
// snapshot, every Lookup returns the zero ResolvedPolicy (= server defaults).
func NewPolicyStore() *PolicyStore {
	return &PolicyStore{policies: make(map[string]ResolvedPolicy)}
}

// Replace swaps the in-memory snapshot. Idempotent — pushing the same
// snapshot twice produces the same observable state.
func (s *PolicyStore) Replace(policies []ResolvedPolicy) {
	next := make(map[string]ResolvedPolicy, len(policies))
	for _, p := range policies {
		if p.Namespace == "" {
			// Defensive: a snapshot entry without a namespace can't be
			// routed by tenant_id, so dropping it is safer than poisoning
			// the empty-string key (which would silently shadow any
			// lookup with an empty tenant).
			continue
		}
		next[p.Namespace] = p
	}
	s.mu.Lock()
	s.policies = next
	s.mu.Unlock()
}

// Lookup returns the resolved policy for a namespace and whether one was
// configured (false → caller should use server defaults).
func (s *PolicyStore) Lookup(namespace string) (ResolvedPolicy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.policies[namespace]
	return p, ok
}

// Snapshot returns a copy of the current policies, sorted by namespace for
// deterministic test output and /policy GET (if added later).
func (s *PolicyStore) Snapshot() []ResolvedPolicy {
	s.mu.RLock()
	out := make([]ResolvedPolicy, 0, len(s.policies))
	for _, p := range s.policies {
		out = append(out, p)
	}
	s.mu.RUnlock()
	return out
}

// TTL satisfies index.TTLResolver: returns the per-namespace EvictionTTL, or
// 0 if none is configured (the index then falls back to its global default).
func (s *PolicyStore) TTL(tenant string) time.Duration {
	if p, ok := s.Lookup(tenant); ok {
		return p.EvictionTTL
	}
	return 0
}

// MinimumPrefixTokens returns the per-namespace minimum matched-token
// threshold for LookupRoute. 0 means no threshold.
func (s *PolicyStore) MinimumPrefixTokens(tenant string) int32 {
	if p, ok := s.Lookup(tenant); ok {
		return p.MinimumPrefixTokens
	}
	return 0
}

// LookupTimeout returns the per-namespace LookupRoute deadline as a
// time.Duration. Zero means no deadline.
func (s *PolicyStore) LookupTimeout(tenant string) time.Duration {
	if p, ok := s.Lookup(tenant); ok && p.LookupTimeoutMs > 0 {
		return time.Duration(p.LookupTimeoutMs) * time.Millisecond
	}
	return 0
}

// NewPolicyHTTPHandler returns the HTTP handler for the /policy endpoint
// backed by the supplied store. It is exposed so the controller's tests
// can stand up an in-process server with the *exact same* decode/replace
// path that the binary mounts at /policy — guarding against schema drift
// between the controller's marshal and the server's decode.
func NewPolicyHTTPHandler(store *PolicyStore) http.HandlerFunc {
	return policyHandler(store)
}

// policyHandler accepts a full snapshot from the controller and replaces the
// in-memory state. Replace-on-write semantics: any CachePolicy not present in
// the body is treated as "no policy" → server defaults.
//
// The endpoint is intentionally internal (no auth here, same as /snapshot);
// hardening (NetworkPolicy + authn) is tracked separately. Limited body size
// to bound memory if a buggy controller sends a runaway snapshot.
func policyHandler(store *PolicyStore) http.HandlerFunc {
	const maxBytes = 1 << 20 // 1 MiB — comfortably above any realistic snapshot
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodPut {
			w.Header().Set("Allow", "POST, PUT")
			http.Error(w, "method not allowed\n", http.StatusMethodNotAllowed)
			return
		}
		body := http.MaxBytesReader(w, r.Body, maxBytes)
		defer func() { _ = body.Close() }()
		dec := json.NewDecoder(body)
		dec.DisallowUnknownFields()
		var snap PolicySnapshot
		if err := dec.Decode(&snap); err != nil {
			http.Error(w, "decode policy snapshot: "+err.Error()+"\n", http.StatusBadRequest)
			return
		}
		if snap.Version != PolicyPropagationVersion {
			http.Error(w, "unsupported policy snapshot version\n", http.StatusBadRequest)
			return
		}
		store.Replace(snap.Policies)
		w.WriteHeader(http.StatusNoContent)
	}
}
