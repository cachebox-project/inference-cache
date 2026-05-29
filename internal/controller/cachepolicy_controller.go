package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// Default for the periodic re-push tick. The server's policy store is
// in-memory soft state, so re-pushing keeps a restarted server in sync
// even if it missed the most recent reconcile.
const DefaultPolicyPushInterval = 30 * time.Second

// CachePolicyReconciler watches CachePolicy resources cluster-wide and
// PUSHES the resolved snapshot to the policy server's /policy endpoint.
// Mirror image of CacheIndexPoller: same controller binary, opposite
// direction (controller-writes-to-server here vs. controller-reads-from-
// server in the snapshot scrape).
//
// Push semantics: on every Reconcile (a CachePolicy create/update/delete)
// AND on a periodic tick the reconciler lists all CachePolicies in the
// cluster, flattens them into ResolvedPolicy entries (one per namespace,
// keyed by the CR's namespace), and PUTs the full snapshot. The server
// adopts replace-on-write so a deleted CachePolicy reverts its namespace
// to server defaults.
//
// The reconciler intentionally never modifies CachePolicy.status — that
// surface is reserved for future status writers (e.g. propagation health).
type CachePolicyReconciler struct {
	Client client.Client
	Log    logr.Logger

	// ServerPolicyURL is the URL the snapshot is POSTed to (e.g.
	// http://inference-cache-server:8080/policy). Required.
	ServerPolicyURL string
	// HTTPClient is the client used for the push. Nil → a 5s-timeout default.
	HTTPClient *http.Client
	// PushInterval is the self-healing tick cadence. <=0 → DefaultPolicyPushInterval.
	PushInterval time.Duration
}

// The reconciler only READS CachePolicy resources — propagation is a
// one-way push to the policy server, and CR status writing is reserved
// for future work.
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachepolicies,verbs=get;list;watch

// Reconcile is triggered on any CachePolicy create/update/delete; it lists
// every CachePolicy in the cluster and pushes the full snapshot. Per-event
// reconciles are deliberately coarse (no per-CR diffing): the server
// adopts replace-on-write, the snapshot is small, and a single push is
// simpler to reason about than incremental deltas.
func (r *CachePolicyReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	if err := r.pushSnapshot(ctx); err != nil {
		// Returning the error gets controller-runtime to back off and retry,
		// which is exactly what we want if the server is briefly unreachable.
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// Start runs the periodic re-push loop. Satisfies manager.Runnable so a
// stopped controller stops the ticker, and so leader election gates it.
func (r *CachePolicyReconciler) Start(ctx context.Context) error {
	interval := r.PushInterval
	if interval <= 0 {
		interval = DefaultPolicyPushInterval
	}
	logger := r.logger(ctx)
	logger.Info("starting CachePolicy push loop", "url", r.ServerPolicyURL, "interval", interval)

	// Push once on startup so a restarted server gets state without waiting
	// a full tick. A failure here is logged, not fatal — the reconciler
	// will retry on the next CR event or tick.
	if err := r.pushSnapshot(ctx); err != nil {
		logger.Error(err, "initial CachePolicy push failed")
	}

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := r.pushSnapshot(ctx); err != nil {
				logger.Error(err, "CachePolicy push failed")
			}
		}
	}
}

// NeedLeaderElection serializes pushes to a single replica, matching the
// CacheIndex poller and keeping the policy server's view stable when
// multiple controller replicas are running.
func (r *CachePolicyReconciler) NeedLeaderElection() bool { return true }

// SetupWithManager registers both the watch-driven reconciler and the
// periodic re-push runnable on the same manager.
func (r *CachePolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ServerPolicyURL == "" {
		return fmt.Errorf("CachePolicyReconciler.ServerPolicyURL is required")
	}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.CachePolicy{}).
		Complete(r); err != nil {
		return err
	}
	return mgr.Add(r)
}

// pushSnapshot lists every CachePolicy in the cluster, resolves it into the
// shape the server consumes, and PUTs the full snapshot. Always pushes a
// full snapshot (replace-on-write) so deletions propagate naturally.
func (r *CachePolicyReconciler) pushSnapshot(ctx context.Context) error {
	var list cachev1alpha1.CachePolicyList
	if err := r.Client.List(ctx, &list); err != nil {
		if apierrors.IsNotFound(err) {
			// CRD not installed yet — nothing to push. Don't error so the
			// initial Start() push doesn't spam logs in a half-installed cluster.
			return nil
		}
		return fmt.Errorf("list CachePolicies: %w", err)
	}

	snap := cacheserver.PolicySnapshot{
		Version:  cacheserver.PolicyPropagationVersion,
		Policies: resolvePolicies(list.Items),
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

// resolvePolicies flattens CachePolicy CRs into the server-side shape, with
// deterministic ordering (sorted by namespace) so equivalent cluster state
// always produces an identical request body. This also keeps the test golden
// path stable.
func resolvePolicies(items []cachev1alpha1.CachePolicy) []cacheserver.ResolvedPolicy {
	out := make([]cacheserver.ResolvedPolicy, 0, len(items))
	for i := range items {
		out = append(out, resolveOnePolicy(&items[i]))
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Namespace < out[b].Namespace })
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
	if cp.Spec.LookupTimeoutMs != nil {
		rp.LookupTimeoutMs = *cp.Spec.LookupTimeoutMs
	}
	return rp
}

func (r *CachePolicyReconciler) httpClient() *http.Client {
	if r.HTTPClient != nil {
		return r.HTTPClient
	}
	return &http.Client{Timeout: 5 * time.Second}
}

func (r *CachePolicyReconciler) logger(ctx context.Context) logr.Logger {
	if r.Log.GetSink() != nil {
		return r.Log
	}
	return log.FromContext(ctx)
}
