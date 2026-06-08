package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// Default for the periodic re-push tick. The server's policy store is
// in-memory soft state, so re-pushing keeps a restarted server in sync
// even if it missed the most recent reconcile.
const DefaultPolicyPushInterval = 30 * time.Second

// ControlPlaneReconciler watches BOTH CachePolicy and CacheTenant resources
// cluster-wide and PUSHES one combined resolved snapshot to the policy server's
// /policy endpoint. Mirror image of CacheIndexPoller: same controller binary,
// opposite direction (controller-writes-to-server here vs. controller-reads-
// from-server in the snapshot scrape).
//
// One reconciler watches both CR types on purpose. The server's policy store is
// replace-on-write: each push fully replaces the previous state. Two separate
// reconcilers (one per CR) pushing independently would clobber each other's
// slice — whichever pushed last would drop the other's data. Listing and
// flattening both types into a single snapshot avoids that race entirely.
//
// Push semantics: on every Reconcile (a create/update/delete of either CR type)
// AND on a periodic tick, the reconciler lists all CachePolicies and all
// CacheTenants in the cluster, flattens them into ResolvedPolicy entries (one
// per namespace) and ResolvedTenant entries (one per tenant ID), and POSTs the
// full combined snapshot. The server adopts replace-on-write so a deleted CR
// reverts its namespace/tenant to server defaults. (The server accepts PUT too,
// for callers that prefer that idempotent verb.)
//
// The reconciler intentionally never modifies CachePolicy.status or
// CacheTenant.status — those surfaces are written by the CacheIndex poller
// (tenant status) or reserved for future writers (policy propagation health).
type ControlPlaneReconciler struct {
	Client client.Client
	Log    logr.Logger

	// ServerPolicyURL is the URL the snapshot is POSTed to (e.g.
	// http://inference-cache-server:8081/policy). Required.
	ServerPolicyURL string
	// HTTPClient is the client used for the push. Nil → a 5s-timeout default.
	HTTPClient *http.Client
	// PushInterval is the self-healing tick cadence. <=0 → DefaultPolicyPushInterval.
	PushInterval time.Duration

	// BearerTokenPath is the file the projected ServiceAccount token is
	// mounted at, sent as Authorization: Bearer on every push so the server's
	// TokenReview middleware can authenticate the controller.
	// "" → DefaultBearerTokenPath. A path that does not exist is treated as
	// "no token configured" — the POST goes out unauthenticated and the
	// server's 401 surfaces as a normal failing tick (returning a retryable
	// error to controller-runtime). Mirrors the CacheIndexPoller's posture.
	BearerTokenPath string

	// pushMu serializes pushSnapshot calls. Without it the watch-driven
	// reconciler and the periodic ticker can race: tick T1 lists the world
	// then a delete reconcile R1 lists+POSTs the new (empty) snapshot, then
	// T1 POSTs its now-stale list — re-introducing the deleted policy on
	// the server until the next tick. Both list AND POST are inside the
	// critical section so the ordering between snapshots is total.
	pushMu sync.Mutex
}

// The reconciler only READS CachePolicy and CacheTenant resources —
// propagation is a one-way push to the policy server, and CR status writing
// for these surfaces is handled elsewhere (CacheTenant.status by the
// CacheIndex poller) or reserved for future work.
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachepolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachetenants,verbs=get;list;watch

// Reconcile is triggered on any CachePolicy or CacheTenant create/update/delete;
// it lists every CachePolicy and CacheTenant in the cluster and pushes the full
// combined snapshot. Per-event reconciles are deliberately coarse (no per-CR
// diffing): the server adopts replace-on-write, the snapshot is small, and a
// single push is simpler to reason about than incremental deltas.
func (r *ControlPlaneReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	if err := r.pushSnapshot(ctx); err != nil {
		// Returning the error gets controller-runtime to back off and retry,
		// which is exactly what we want if the server is briefly unreachable.
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// Start runs the periodic re-push loop. Satisfies manager.Runnable so a
// stopped controller stops the ticker, and so leader election gates it.
func (r *ControlPlaneReconciler) Start(ctx context.Context) error {
	interval := r.PushInterval
	if interval <= 0 {
		interval = DefaultPolicyPushInterval
	}
	logger := r.logger(ctx)
	logger.Info("starting control-plane push loop", "url", r.ServerPolicyURL, "interval", interval)

	// Push once on startup so a restarted server gets state without waiting
	// a full tick. A failure here is logged, not fatal — the reconciler
	// will retry on the next CR event or tick.
	if err := r.pushSnapshot(ctx); err != nil {
		logger.Error(err, "initial control-plane push failed")
	}

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := r.pushSnapshot(ctx); err != nil {
				logger.Error(err, "control-plane push failed")
			}
		}
	}
}

// NeedLeaderElection serializes pushes to a single replica, matching the
// CacheIndex poller and keeping the policy server's view stable when
// multiple controller replicas are running.
func (r *ControlPlaneReconciler) NeedLeaderElection() bool { return true }

// SetupWithManager registers both the watch-driven reconciler and the
// periodic re-push runnable on the same manager.
func (r *ControlPlaneReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ServerPolicyURL == "" {
		return fmt.Errorf("ControlPlaneReconciler.ServerPolicyURL is required")
	}
	// One controller, two watched types: a change to EITHER CachePolicy or
	// CacheTenant enqueues a reconcile that re-pushes the full combined
	// snapshot. The watched object's identity is irrelevant (Reconcile lists
	// the whole cluster), so a plain enqueue of the changed object is enough.
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.CachePolicy{}).
		Watches(&cachev1alpha1.CacheTenant{}, &handler.EnqueueRequestForObject{}).
		Complete(r); err != nil {
		return err
	}
	return mgr.Add(r)
}

// pushSnapshot lists every CachePolicy AND CacheTenant in the cluster, resolves
// them into the shapes the server consumes, and POSTs the full combined
// snapshot. Always pushes a full snapshot (replace-on-write) so deletions
// propagate naturally.
//
// Serialized via pushMu so two concurrent pushes can't reorder list-then-POST
// pairs and let an older snapshot land after a newer one.
func (r *ControlPlaneReconciler) pushSnapshot(ctx context.Context) error {
	r.pushMu.Lock()
	defer r.pushMu.Unlock()

	var policies cachev1alpha1.CachePolicyList
	if err := r.Client.List(ctx, &policies); err != nil {
		if apierrors.IsNotFound(err) {
			// CRD not installed yet — nothing to push. Don't error so the
			// initial Start() push doesn't spam logs in a half-installed cluster.
			return nil
		}
		return fmt.Errorf("list CachePolicies: %w", err)
	}

	var tenants cachev1alpha1.CacheTenantList
	if err := r.Client.List(ctx, &tenants); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("list CacheTenants: %w", err)
		}
		// CacheTenant CRD not installed — push policies only, no tenant quotas.
	}

	snap := cacheserver.PolicySnapshot{
		Version:  cacheserver.PolicyPropagationVersion,
		Policies: resolvePolicies(policies.Items),
		Tenants:  resolveTenants(tenants.Items),
	}

	body, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal policy snapshot: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.ServerPolicyURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build policy request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Send the projected SA token so the server's TokenReview middleware can
	// admit this push. bearerToken errors are surfaced separately so a
	// missing or unreadable token shows up in the controller's log with the
	// expected path, rather than silently degrading to an unauthenticated
	// push that the server rejects as 401. An absent token file (local-dev
	// out-of-cluster) is NOT an error — the POST goes out unauthenticated
	// and the server's auth posture decides what happens next.
	token, tokenErr := r.bearerToken()
	if tokenErr != nil {
		r.logger(ctx).Error(tokenErr, "read bearer token; pushing unauthenticated")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := r.httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("push policies to %s: %w", r.ServerPolicyURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("push policies to %s: unexpected status %d", r.ServerPolicyURL, resp.StatusCode)
	}
	return nil
}

// bearerToken reads the projected ServiceAccount token. Re-read on every
// push so kubelet rotations are picked up without process restarts; the
// file is tmpfs so the read is cheap. Error semantics mirror
// CacheIndexPoller.bearerToken — file missing → ("", nil); present but
// unreadable → ("", wrappedError) so the operator can diagnose.
func (r *ControlPlaneReconciler) bearerToken() (string, error) {
	path := r.BearerTokenPath
	if path == "" {
		path = DefaultBearerTokenPath
	}
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("read bearer token %q: %w", path, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// resolvePolicies flattens CachePolicy CRs into the server-side shape with a
// deterministic outcome even when multiple CachePolicies share a namespace.
//
// A validating webhook now rejects a second CachePolicy per namespace at
// admission, but that check is best-effort (it can be raced by concurrent
// CREATEs, and CRs created before the webhook shipped may already coexist), so
// the controller must still pick deterministically when it does see multiple
// CRs in one namespace. The server's PolicyStore is keyed by namespace, so
// something has to pick. We sort by (namespace, name) and emit the FIRST entry
// per namespace — i.e. the alphabetically smallest name wins, deterministically,
// regardless of Kubernetes list order. This remains the authoritative tie-break;
// the webhook is fast operator feedback layered on top, not a replacement.
func resolvePolicies(items []cachev1alpha1.CachePolicy) []cacheserver.ResolvedPolicy {
	if len(items) == 0 {
		return []cacheserver.ResolvedPolicy{}
	}
	sorted := make([]cachev1alpha1.CachePolicy, len(items))
	copy(sorted, items)
	sort.Slice(sorted, func(a, b int) bool {
		if sorted[a].Namespace != sorted[b].Namespace {
			return sorted[a].Namespace < sorted[b].Namespace
		}
		return sorted[a].Name < sorted[b].Name
	})
	out := make([]cacheserver.ResolvedPolicy, 0, len(sorted))
	for i := range sorted {
		// First entry per namespace wins; skip the rest of the namespace's run.
		if i > 0 && sorted[i].Namespace == sorted[i-1].Namespace {
			continue
		}
		out = append(out, resolveOnePolicy(&sorted[i]))
	}
	return out
}

// resolveOnePolicy maps one CR into the server's enforcement shape. Phase-1
// namespace == tenant boundary: the resolved entry is keyed by the CR's
// namespace, which the server matches against LookupRoute's tenant_id.
func resolveOnePolicy(cp *cachev1alpha1.CachePolicy) cacheserver.ResolvedPolicy {
	rp := cacheserver.ResolvedPolicy{Namespace: cp.Namespace}
	if cp.Spec.EvictionTTL != nil {
		rp.EvictionTTL = cp.Spec.EvictionTTL.Duration
	}
	if cp.Spec.MinimumPrefixTokens != nil {
		rp.MinimumPrefixTokens = *cp.Spec.MinimumPrefixTokens
	}
	if cp.Spec.MinimumMatchedTokens != nil {
		rp.MinimumMatchedTokens = *cp.Spec.MinimumMatchedTokens
	}
	if cp.Spec.RoutingFloorScore != nil {
		// The CRD's +kubebuilder:validation:Pattern marker guarantees a
		// well-formed unsigned decimal at admission, so ParseFloat should
		// succeed for any value the apiserver accepted. The fallback is
		// defensive against a hand-crafted /policy POST or a future schema
		// drift: a parse error leaves rp.RoutingFloorScore at its zero
		// value, which the server treats as the explicit opt-out for
		// namespaces with a CachePolicy installed (different from the
		// no-policy default that fires DefaultRoutingFloorScore). The
		// CRD-side validator is the authoritative gate here; this clamp
		// is purely the second line of defence.
		if f, err := strconv.ParseFloat(*cp.Spec.RoutingFloorScore, 32); err == nil && f >= 0 {
			rp.RoutingFloorScore = float32(f)
		}
	}
	if cp.Spec.LookupTimeoutMs != nil {
		rp.LookupTimeoutMs = *cp.Spec.LookupTimeoutMs
	}
	// Flatten the eviction algorithm to its lower-case canonical form. The CRD
	// enum is upper-case (K8s convention) and defaults to LRU; an empty value
	// (CR predating apiserver defaulting, or constructed without it) also maps
	// to LRU so the server never sees an ambiguous algorithm.
	rp.Eviction = string(cachev1alpha1.CachePolicyEvictionAlgorithmLRU)
	if cp.Spec.Eviction != "" {
		rp.Eviction = string(cp.Spec.Eviction)
	}
	rp.Eviction = strings.ToLower(rp.Eviction)
	return rp
}

// resolveTenants flattens CacheTenant CRs into the server-side quota shape,
// keyed by spec.tenantID. Only tenants that carry an enforceable budget
// (quota.maxIndexEntries set) are emitted — a tenant without one has no
// dimension the server can enforce, so it is omitted and the server leaves it
// unbounded (fail open). When multiple CacheTenants declare the same tenantID,
// the first one (sorted by namespace, name) that carries a budget wins,
// deterministically regardless of apiserver list ordering — mirroring
// resolvePolicies' dedup.
//
// A validating webhook rejects same-namespace tenantID duplicates at admission,
// but tenantID identity is namespace-blind here, so duplicates ACROSS
// namespaces still reach this dedup (the webhook intentionally permits them).
// This deterministic pick is the authoritative resolution for that case; the
// shadowed CR is surfaced via Ready=False/DuplicateTenantID status (see the
// CacheIndex status writer's effectiveTenantOwners).
//
// Identity: the emitted key is spec.tenantID (the value an ingest carries in
// CacheStateUpdate.tenant_id), NOT the CR's metadata.name. That is the join the
// index uses to match an ingest against a quota.
func resolveTenants(items []cachev1alpha1.CacheTenant) []cacheserver.ResolvedTenant {
	if len(items) == 0 {
		return nil
	}
	sorted := make([]cachev1alpha1.CacheTenant, len(items))
	copy(sorted, items)
	sort.Slice(sorted, func(a, b int) bool {
		if sorted[a].Namespace != sorted[b].Namespace {
			return sorted[a].Namespace < sorted[b].Namespace
		}
		return sorted[a].Name < sorted[b].Name
	})
	out := make([]cacheserver.ResolvedTenant, 0, len(sorted))
	seen := make(map[string]struct{}, len(sorted))
	for i := range sorted {
		rt, ok := resolveOneTenant(&sorted[i])
		if !ok {
			continue
		}
		if _, dup := seen[rt.TenantID]; dup {
			continue
		}
		seen[rt.TenantID] = struct{}{}
		out = append(out, rt)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// resolveOneTenant maps one CacheTenant into the server's enforcement shape,
// returning ok=false when the tenant has no enforceable budget (no tenantID, or
// no quota.maxIndexEntries) so the caller omits it (fail open). A budget of 0 is
// a valid enforceable cap and IS emitted.
func resolveOneTenant(ct *cachev1alpha1.CacheTenant) (cacheserver.ResolvedTenant, bool) {
	if ct.Spec.TenantID == "" {
		return cacheserver.ResolvedTenant{}, false
	}
	if ct.Spec.Quota == nil || ct.Spec.Quota.MaxIndexEntries == nil {
		return cacheserver.ResolvedTenant{}, false
	}
	return cacheserver.ResolvedTenant{
		TenantID:        ct.Spec.TenantID,
		MaxIndexEntries: *ct.Spec.Quota.MaxIndexEntries,
		IsolationMode:   string(ct.Spec.IsolationMode),
	}, true
}

func (r *ControlPlaneReconciler) httpClient() *http.Client {
	if r.HTTPClient != nil {
		return r.HTTPClient
	}
	return &http.Client{Timeout: 5 * time.Second}
}

func (r *ControlPlaneReconciler) logger(ctx context.Context) logr.Logger {
	if r.Log.GetSink() != nil {
		return r.Log
	}
	return log.FromContext(ctx)
}
