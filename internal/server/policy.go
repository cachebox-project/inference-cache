// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/cachebox-project/inference-cache/internal/controlplaneapi"
)

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
	policies map[string]controlplaneapi.ResolvedPolicy
	tenants  map[string]controlplaneapi.ResolvedTenant
}

// NewPolicyStore returns an empty store. Until the controller pushes a
// snapshot, every Lookup returns the zero ResolvedPolicy (= server defaults)
// and every TenantQuota reports "no quota" (= unbounded, fail open).
func NewPolicyStore() *PolicyStore {
	return &PolicyStore{
		policies: make(map[string]controlplaneapi.ResolvedPolicy),
		tenants:  make(map[string]controlplaneapi.ResolvedTenant),
	}
}

// Replace swaps the full snapshot to a policies-only state: it installs the
// given policies AND clears any tenant quotas, exactly equivalent to
// ReplaceSnapshot(policies, nil). Retained as a convenience for callers that
// don't exercise the tenant-quota axis (mostly tests); it delegates so it can
// never leave a stale tenant table behind. Idempotent.
func (s *PolicyStore) Replace(policies []controlplaneapi.ResolvedPolicy) {
	s.ReplaceSnapshot(policies, nil)
}

// ReplaceSnapshot atomically swaps BOTH the policy and tenant-quota state under
// a single write lock, so a reader never observes new policies paired with the
// previous tenant table (or vice versa). This is the path the /policy handler
// uses; the policies-only Replace delegates here with nil tenants.
// Replace-on-write: a tenant absent from the new snapshot reverts to "no quota"
// (unbounded, fail open).
func (s *PolicyStore) ReplaceSnapshot(policies []controlplaneapi.ResolvedPolicy, tenants []controlplaneapi.ResolvedTenant) {
	nextPolicies := make(map[string]controlplaneapi.ResolvedPolicy, len(policies))
	for _, p := range policies {
		if p.Namespace == "" {
			continue // see Replace: an unkeyed entry can't be routed.
		}
		nextPolicies[p.Namespace] = p
	}
	nextTenants := make(map[string]controlplaneapi.ResolvedTenant, len(tenants))
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
func (s *PolicyStore) Lookup(namespace string) (controlplaneapi.ResolvedPolicy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.policies[namespace]
	return p, ok
}

// Snapshot returns a copy of the current policies, sorted by namespace for
// deterministic test output and /policy GET (if added later).
func (s *PolicyStore) Snapshot() []controlplaneapi.ResolvedPolicy {
	s.mu.RLock()
	out := make([]controlplaneapi.ResolvedPolicy, 0, len(s.policies))
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

// MinimumPrefixTokens returns the per-namespace minimum REQUESTED prefix
// token threshold for LookupRoute (the request-side pre-lookup gate). 0 means
// no threshold. Distinct from MinimumMatchedTokens, which gates the realized
// match AFTER the lookup runs.
func (s *PolicyStore) MinimumPrefixTokens(tenant string) int32 {
	if p, ok := s.Lookup(tenant); ok {
		return p.MinimumPrefixTokens
	}
	return 0
}

// MinimumMatchedTokens returns the per-namespace MATCHED prefix token floor
// applied to LookupRoute responses. When a tenant has a CachePolicy the field
// value wins as-is (including the explicit 0 opt-out — "I want every match
// reported, even trivial ones"); when no policy exists the server-wide
// DefaultMinimumMatchedTokens applies so the safety floor still fires for
// unconfigured tenants. <0 round-trips to 0 (no enforcement) — the resolver
// never returns a negative threshold to callers.
func (s *PolicyStore) MinimumMatchedTokens(tenant string) int32 {
	if p, ok := s.Lookup(tenant); ok {
		if p.MinimumMatchedTokens < 0 {
			return 0
		}
		return p.MinimumMatchedTokens
	}
	return controlplaneapi.DefaultMinimumMatchedTokens
}

// RoutingFloorScore returns the per-namespace post-score floor applied to
// the distinguishing-power-aware LookupRoute ranking. Resolution rules:
//
//   - No CachePolicy at all for this namespace  → DefaultRoutingFloorScore
//     (the safety floor fires for the common unconfigured-tenant case).
//   - CachePolicy present but RoutingFloorScore field absent on the wire
//     (nil pointer)                              → DefaultRoutingFloorScore
//     (a legacy / hand-crafted body that didn't carry the field falls back
//     to the safety floor, NOT to opt-out — silent opt-out would be the
//     wrong inference from "field missing").
//   - CachePolicy present, RoutingFloorScore == &0 → 0 (the operator
//     EXPLICITLY opted out — raw-recall benchmarking, ranker debugging).
//   - CachePolicy present, RoutingFloorScore == &x → x as-is.
//
// Negative values clamp to 0 — the same effective behavior as the operator
// opt-out, NOT the safety default. The choice between "clamp negative to 0"
// (current) and "clamp negative to DefaultRoutingFloorScore" (alternative)
// is a judgment call on truly malformed wire input:
//   - Clamp to 0: the buildLookupResponse check `floor > 0` short-circuits,
//     no replicas are downgraded. Equivalent to the explicit opt-out.
//   - Clamp to default: replicas below DefaultRoutingFloorScore would be
//     downgraded as if the operator had said nothing.
//
// Both are defensible. We pick the clamp-to-0 path because the CRD pattern
// validator already rejects negatives at admission, AND the controller-side
// flatten path (resolveOnePolicy) ALSO falls back to default on parse
// failure / negative. So the only path that lands a negative here is a
// hand-crafted /policy POST that bypassed both gates — at which point
// "treat as opt-out and don't enforce" is the same kind of fail-open
// behavior the rest of the hot path uses for unknown / malformed input.
// The defensive behavior of preventing a negative threshold from
// silently disabling enforcement (which `score < negative` would, since
// no score is less than a negative number) is still satisfied — the
// clamp is what prevents that pathology.
func (s *PolicyStore) RoutingFloorScore(tenant string) float32 {
	p, ok := s.Lookup(tenant)
	if !ok {
		return controlplaneapi.DefaultRoutingFloorScore
	}
	if p.RoutingFloorScore == nil {
		// Policy is installed but did not carry this field (legacy / hand-
		// crafted body). Apply the safety floor, not the opt-out.
		return controlplaneapi.DefaultRoutingFloorScore
	}
	if *p.RoutingFloorScore < 0 {
		return 0
	}
	return *p.RoutingFloorScore
}

// AffinityRoutingEnabled returns whether the per-namespace consistent-hash
// fallback fires on the LookupRoute NO_HINT path. Resolution rules:
//
//   - No CachePolicy for this namespace                      → DefaultAffinityRoutingEnabled
//   - CachePolicy present but AffinityRouting field absent  → DefaultAffinityRoutingEnabled
//   - CachePolicy present, AffinityRouting == &true         → true
//   - CachePolicy present, AffinityRouting == &false        → false
//
// The "field absent → default" branch is what makes a server-first rollout
// safe: a v5 controller body lands here with AffinityRouting == nil and is
// treated as the kubebuilder-defaulted Enabled, not silently disabled.
// normalizePolicySnapshotForVersion converts those nil pointers to &true at
// decode time, so by the time the resolver runs every wire-decoded
// ResolvedPolicy has a non-nil AffinityRouting; the nil-check here defends
// against direct-constructed ResolvedPolicy values (tests, in-process
// callers) where the wire normalizer never ran.
func (s *PolicyStore) AffinityRoutingEnabled(namespace string) bool {
	p, ok := s.Lookup(namespace)
	if !ok {
		return controlplaneapi.DefaultAffinityRoutingEnabled
	}
	if p.AffinityRouting == nil {
		return controlplaneapi.DefaultAffinityRoutingEnabled
	}
	return *p.AffinityRouting
}

// LookupTimeout returns the per-namespace LookupRoute deadline as a
// time.Duration. Zero means no deadline.
func (s *PolicyStore) LookupTimeout(tenant string) time.Duration {
	if p, ok := s.Lookup(tenant); ok && p.LookupTimeoutMs > 0 {
		return time.Duration(p.LookupTimeoutMs) * time.Millisecond
	}
	return 0
}

// ChainMatchingEnabled reports whether LookupRoute may use block-hash chain
// longest-prefix matching for this tenant. Missing policy/strategy/field all
// preserve the historical default: chain matching enabled.
func (s *PolicyStore) ChainMatchingEnabled(tenant string) bool {
	p, ok := s.Lookup(tenant)
	if !ok || p.Strategy == nil || p.Strategy.EnableChainMatching == nil {
		return controlplaneapi.DefaultEnableChainMatching
	}
	return *p.Strategy.EnableChainMatching
}

// ChainRequired reports whether LookupRoute requests for this tenant must
// carry a block-hash chain. Missing policy/strategy/field preserves the
// historical default: legacy exact-prefix callers are still accepted.
func (s *PolicyStore) ChainRequired(tenant string) bool {
	p, ok := s.Lookup(tenant)
	if !ok || p.Strategy == nil || p.Strategy.RequireChain == nil {
		return controlplaneapi.DefaultRequireChain
	}
	return *p.Strategy.RequireChain
}

// TenantHotEnabled reports whether the TENANT_HOT fallback may surface for
// this tenant. Missing policy/strategy/field preserves the historical default.
func (s *PolicyStore) TenantHotEnabled(tenant string) bool {
	p, ok := s.Lookup(tenant)
	if !ok || p.Strategy == nil || p.Strategy.EnableTenantHot == nil {
		return controlplaneapi.DefaultEnableTenantHot
	}
	return *p.Strategy.EnableTenantHot
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
	if tenant == controlplaneapi.ProbeTenantID {
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
func (s *PolicyStore) TenantQuotas() []controlplaneapi.ResolvedTenant {
	s.mu.RLock()
	out := make([]controlplaneapi.ResolvedTenant, 0, len(s.tenants))
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
		var snap controlplaneapi.PolicySnapshot
		if err := dec.Decode(&snap); err != nil {
			http.Error(w, "decode policy snapshot: "+err.Error()+"\n", http.StatusBadRequest)
			return
		}
		// Accept any version in [PolicyMinimumAcceptedVersion, PolicyPropagationVersion].
		// Anything outside that range is a hard 400: a controller too old for
		// fields the server now load-bears on (`unsupported policy snapshot
		// version` here), or a controller too new for the server to recognize
		// (typically caught one layer earlier by DisallowUnknownFields above,
		// which surfaces as a `decode policy snapshot: json: unknown field "..."`
		// — also fail-loud, just attributed to the decoder rather than this
		// branch). Both outcomes give the operator a specific diagnostic.
		if snap.Version < controlplaneapi.PolicyMinimumAcceptedVersion || snap.Version > controlplaneapi.PolicyPropagationVersion {
			http.Error(w, "unsupported policy snapshot version\n", http.StatusBadRequest)
			return
		}
		// Normalize older bodies so server-first rollouts (newer server, older
		// controller still pushing v3/v4/v5/v6) preserve every other knob a CR
		// carries. Today: older bodies may omit minimumMatchedTokens,
		// routingFloorScore, strategy, or affinityRouting.
		// JSON decodes the missing fields to their zero values
		// (int32(0) / nil *float32), which would be indistinguishable from
		// the explicit opt-outs. Fill in the server defaults so the
		// post-rollout effective policy matches the no-CachePolicy fallback
		// paths. See normalizePolicySnapshotForVersion below for the
		// version-by-version field-by-field details.
		normalizePolicySnapshotForVersion(&snap)
		store.ReplaceSnapshot(snap.Policies, snap.Tenants)
		w.WriteHeader(http.StatusNoContent)
	}
}

// normalizePolicySnapshotForVersion rewrites an accepted body so the
// in-memory store sees the same shape regardless of which (supported) wire
// version the controller pushed.
//
// Four normalizations today:
//
//   - **v3 → v4 minimumMatchedTokens default.** A v3 body's missing field
//     would otherwise land as 0 (the v4 explicit opt-out), silently
//     disabling the matched-tokens floor for every namespace with a CR
//     during a server-first rollout. Filling in DefaultMinimumMatchedTokens
//     makes a v3-carrying policy's effective floor match the no-CachePolicy
//     fallback PolicyStore.MinimumMatchedTokens applies to tenants without
//     a CR.
//   - **v3/v4 → v5 routingFloorScore default.** Same pattern, one field
//     later: a v3 or v4 body has no routingFloorScore key, which the v5
//     server would otherwise decode as float32(0) — the explicit opt-out
//     for the distinguishing-power floor — silently disabling that floor
//     for every namespace with a CR during a server-first rollout. Filling
//     in DefaultRoutingFloorScore matches the no-CachePolicy fallback
//     PolicyStore.RoutingFloorScore applies.
//   - **v3/v4/v5 → v6 strategy defaults.** Missing strategy gates preserve
//     the pre-v6 behavior: chain matching enabled, chain not required, and
//     tenant-hot enabled.
//   - **v3/v4/v5/v6 → v7 affinityRouting default.** A body older than v7 has
//     no affinityRouting key; the nil *bool is filled with
//     DefaultAffinityRoutingEnabled so a server-first rollout does not
//     silently disable the consistent-hash fallback for namespaces with a CR.
//
// Bodies already at PolicyPropagationVersion are returned untouched so an
// operator's explicit opt-out (e.g. `routingFloorScore: 0` for raw-recall
// benchmarking, or `enableTenantHot: false`, or `affinityRouting: false`) reaches the store as written.
func normalizePolicySnapshotForVersion(snap *controlplaneapi.PolicySnapshot) {
	if snap.Version >= controlplaneapi.PolicyPropagationVersion {
		return
	}
	if snap.Version < 4 {
		for i := range snap.Policies {
			if snap.Policies[i].MinimumMatchedTokens == 0 {
				snap.Policies[i].MinimumMatchedTokens = controlplaneapi.DefaultMinimumMatchedTokens
			}
		}
	}
	if snap.Version < 5 {
		// A v3 or v4 body has no routingFloorScore key, so the decoded pointer
		// is nil. Synthesize the safety default so a server-first rollout does
		// not silently disable the floor for every namespace with a CR. An
		// operator's explicit `routingFloorScore: 0` opt-out is already a
		// non-nil pointer (= &0) and reaches the store as written; the nil
		// branch only fires for the missing-field case.
		for i := range snap.Policies {
			if snap.Policies[i].RoutingFloorScore == nil {
				v := controlplaneapi.DefaultRoutingFloorScore
				snap.Policies[i].RoutingFloorScore = &v
			}
		}
	}
	if snap.Version < 6 {
		for i := range snap.Policies {
			applyResolvedLookupStrategyDefaults(&snap.Policies[i])
		}
	}
	if snap.Version < 7 {
		// A v3/v4/v5/v6 body has no affinityRouting key, so the decoded pointer
		// is nil. Synthesize the safety default so a server-first rollout
		// does not silently disable affinity for every namespace with a CR.
		// An operator's explicit `affinityRouting: Disabled` is already a
		// non-nil &false and reaches the store as written; the nil branch
		// only fires for the missing-field case.
		def := controlplaneapi.DefaultAffinityRoutingEnabled
		for i := range snap.Policies {
			if snap.Policies[i].AffinityRouting == nil {
				v := def
				snap.Policies[i].AffinityRouting = &v
			}
		}
	}
}

func applyResolvedLookupStrategyDefaults(p *controlplaneapi.ResolvedPolicy) {
	if p.Strategy == nil {
		p.Strategy = &controlplaneapi.ResolvedLookupStrategy{}
	}
	if p.Strategy.EnableChainMatching == nil {
		v := controlplaneapi.DefaultEnableChainMatching
		p.Strategy.EnableChainMatching = &v
	}
	if p.Strategy.RequireChain == nil {
		v := controlplaneapi.DefaultRequireChain
		p.Strategy.RequireChain = &v
	}
	if p.Strategy.EnableTenantHot == nil {
		v := controlplaneapi.DefaultEnableTenantHot
		p.Strategy.EnableTenantHot = &v
	}
}
