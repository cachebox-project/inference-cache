package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// PolicyPropagationVersion identifies the schema of the /policy snapshot the
// server accepts. Bumped on a breaking schema change so a stale controller
// can refuse to push (the controller writes the same constant on each push).
//
// v2 added the Tenants slice (CacheTenant quota propagation). v3 added
// ResolvedPolicy.Eviction (per-namespace cap-eviction algorithm). The version
// field is the forward-looking guard: a server rejects a body whose version it
// does not recognize, so a mismatched controller/server pair fails loudly with a
// clear "unsupported version" rather than silently losing fields. (An older
// server would instead reject a newer body's unknown field at decode time under
// DisallowUnknownFields — also a hard failure, just a less descriptive one.)
const PolicyPropagationVersion = 3

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
//   - Eviction == ""         → LRU (the index default and the kubebuilder default).
type ResolvedPolicy struct {
	// Namespace identifies the CachePolicy's namespace, which in phase-1 is
	// the tenant boundary: a LookupRoute carrying tenant_id="foo" resolves
	// against the CachePolicy in namespace "foo".
	Namespace string `json:"namespace"`

	EvictionTTL         time.Duration `json:"evictionTTL,omitempty"`
	MinimumPrefixTokens int32         `json:"minimumPrefixTokens,omitempty"`
	LookupTimeoutMs     int32         `json:"lookupTimeoutMs,omitempty"`
	// Eviction is the eviction algorithm in lower-case canonical form
	// ("lru" / "lfu"). The controller lower-cases the CRD's upper-case enum when
	// flattening. Empty means the server default (LRU). The index consults it on
	// the cap-based sweep (to order victims) and, for LFU, on the lookup path (to
	// capture which entries a delivered hint credits); the TTL sweep is
	// algorithm-independent.
	Eviction string `json:"eviction,omitempty"`
}

// ResolvedTenant is the slice of a CacheTenant the server enforces at ingest
// time: the tenant's external identity plus its index-entry budget. The CRD
// types live in api/v1alpha1; the controller flattens them into this shape so
// pkg/server has no dependency on the CRD package (mirrors ResolvedPolicy).
//
// Identity note: TenantID is the CacheTenant's spec.tenantID — the same value
// a CacheStateUpdate carries in tenant_id — NOT the CR's metadata.name. That is
// the join key the index matches an ingest against.
//
// There is deliberately no memory budget: the engine KV cache is a shared,
// tenant-unaware pool, so the control plane can neither enforce nor honestly
// attribute bytes per tenant. Only the index entry table — which the server
// owns — is enforceable.
type ResolvedTenant struct {
	TenantID        string `json:"tenantID"`
	MaxIndexEntries int64  `json:"maxIndexEntries"`
	// IsolationMode is carried for forward-compat / observability. Phase-1 only
	// implements Fairness (evict the tenant's own oldest entries); other modes
	// are a separate effort.
	IsolationMode string `json:"isolationMode,omitempty"`
}

// PolicySnapshot is the full set of CachePolicies + CacheTenants the controller
// pushes on each reconcile. Pushed via POST to /policy (PUT is accepted too for
// callers that prefer it). Replace-on-write: the controller is the source of
// truth, so the server discards its prior state and adopts the new snapshot. A
// CachePolicy/CacheTenant that disappears between snapshots reverts that
// namespace/tenant to the server default (no policy / no quota).
type PolicySnapshot struct {
	Version  int              `json:"version"`
	Policies []ResolvedPolicy `json:"policies"`
	Tenants  []ResolvedTenant `json:"tenants,omitempty"`
}

// PolicyStore is the server-side cache of resolved policies (indexed by
// namespace) and resolved tenant quotas (indexed by tenant ID). Reads take
// the read lock; pushes from /policy (POST or PUT) take the write lock and
// replace the maps atomically. Satisfies index.TTLResolver,
// index.TenantQuotaResolver, and index.EvictionResolver.
//
// The two indices use different keys on purpose: a CachePolicy is keyed by its
// namespace (phase-1 tenant boundary for lookups), while a CacheTenant quota is
// keyed by spec.tenantID (the value an ingest carries). They are separate axes,
// so they live in separate maps under the same lock.
type PolicyStore struct {
	mu       sync.RWMutex
	policies map[string]ResolvedPolicy
	tenants  map[string]ResolvedTenant
}

// NewPolicyStore returns an empty store. Until the controller pushes a
// snapshot, every Lookup returns the zero ResolvedPolicy (= server defaults)
// and every TenantQuota reports "no quota" (= unbounded, fail open).
func NewPolicyStore() *PolicyStore {
	return &PolicyStore{
		policies: make(map[string]ResolvedPolicy),
		tenants:  make(map[string]ResolvedTenant),
	}
}

// Replace swaps the full snapshot to a policies-only state: it installs the
// given policies AND clears any tenant quotas, exactly equivalent to
// ReplaceSnapshot(policies, nil). Retained as a convenience for callers that
// don't exercise the tenant-quota axis (mostly tests); it delegates so it can
// never leave a stale tenant table behind. Idempotent.
func (s *PolicyStore) Replace(policies []ResolvedPolicy) {
	s.ReplaceSnapshot(policies, nil)
}

// ReplaceSnapshot atomically swaps BOTH the policy and tenant-quota state under
// a single write lock, so a reader never observes new policies paired with the
// previous tenant table (or vice versa). This is the path the /policy handler
// uses; the policies-only Replace delegates here with nil tenants.
// Replace-on-write: a tenant absent from the new snapshot reverts to "no quota"
// (unbounded, fail open).
func (s *PolicyStore) ReplaceSnapshot(policies []ResolvedPolicy, tenants []ResolvedTenant) {
	nextPolicies := make(map[string]ResolvedPolicy, len(policies))
	for _, p := range policies {
		if p.Namespace == "" {
			continue // see Replace: an unkeyed entry can't be routed.
		}
		nextPolicies[p.Namespace] = p
	}
	nextTenants := make(map[string]ResolvedTenant, len(tenants))
	for _, t := range tenants {
		if t.TenantID == "" {
			// Defensive: a quota with no tenant ID can't be matched against any
			// ingest, and the empty key would shadow lookups for an empty
			// tenant. Drop it rather than poison the table.
			continue
		}
		// Sanitize the wire input at the trust boundary: the CRD enforces
		// maxIndexEntries >= 0, but a hand-crafted /policy POST could carry a
		// negative budget, which the index reads as "no enforcement" (eviction is
		// skipped for maxPrefixes < 0). That would silently turn an attempted cap
		// into unbounded — the opposite of intent. Clamp to the design minimum of
		// 0 (the strictest valid cap, "admit nothing") so a malformed budget can
		// never disable enforcement.
		if t.MaxIndexEntries < 0 {
			t.MaxIndexEntries = 0
		}
		nextTenants[t.TenantID] = t
	}
	s.mu.Lock()
	s.policies = nextPolicies
	s.tenants = nextTenants
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
	sort.Slice(out, func(a, b int) bool { return out[a].Namespace < out[b].Namespace })
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

// Eviction satisfies index.EvictionResolver: returns the per-namespace
// cap-eviction algorithm in lower-case canonical form ("lru" / "lfu"), or ""
// when no policy is configured (the index then defaults to LRU). The index
// normalizes the value, so an unexpected string degrades to LRU rather than
// erroring.
func (s *PolicyStore) Eviction(tenant string) string {
	if p, ok := s.Lookup(tenant); ok {
		return p.Eviction
	}
	return ""
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

// TenantQuota satisfies index.TenantQuotaResolver: returns the tenant's maximum
// index-entry budget and whether a quota is configured. ok=false (no matching
// CacheTenant, or the resolver is nil) means no enforcement — the index leaves
// the tenant unbounded (fail open / soft state). A configured budget of 0 is a
// valid, enforceable choice (admit nothing) and is distinct from "no quota".
//
// The reserved probe tenant (ProbeTenantID) is unconditionally exempt from
// quota: CacheTenant.spec.tenantID is a free-form string, so without this
// exemption an operator could create CacheTenant{tenantID: "inferencecache.io/
// probe", maxIndexEntries: 0} and silently break Stage A of every
// CacheBackend functional probe (the ingest would be evicted before it lands).
// The probe is server-internal state under a server-controlled tenant id; no
// operator-supplied CacheTenant should govern it.
func (s *PolicyStore) TenantQuota(tenant string) (maxEntries int64, ok bool) {
	if tenant == ProbeTenantID {
		return 0, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tenants[tenant]
	if !ok {
		return 0, false
	}
	return t.MaxIndexEntries, true
}

// TenantQuotas returns a copy of the current tenant quotas, sorted by tenant ID
// for deterministic test output.
func (s *PolicyStore) TenantQuotas() []ResolvedTenant {
	s.mu.RLock()
	out := make([]ResolvedTenant, 0, len(s.tenants))
	for _, t := range s.tenants {
		out = append(out, t)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(a, b int) bool { return out[a].TenantID < out[b].TenantID })
	return out
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
// The endpoint is intentionally internal. Auth + NetworkPolicy gating live
// in server.New, where the same TokenReview-backed bearer middleware that
// protects /snapshot and /probe is also applied here — all three
// controller-facing endpoints share one controller-SA identity. The
// handler itself stays auth-agnostic so tests (and any future internal
// caller) can mount it directly. Body size is capped at 1 MiB to bound
// memory if a buggy controller sends a runaway snapshot.
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
		store.ReplaceSnapshot(snap.Policies, snap.Tenants)
		w.WriteHeader(http.StatusNoContent)
	}
}
