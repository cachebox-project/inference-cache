package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	"github.com/cachebox-project/inference-cache/pkg/index"
)

// Defaults for the CacheIndex status poller.
const (
	DefaultCacheIndexName  = "cluster-default"
	DefaultRefreshInterval = 30 * time.Second

	// DefaultBearerTokenPath is the in-cluster location of the audience-bound
	// projected ServiceAccount token the controller uses on its read/probe HTTP
	// channels to the policy server:
	//   - the CacheIndexPoller scraping GET /snapshot (in this file), and
	//   - the ProbeClient POSTing /probe (probe_client.go).
	// The default install (config/manager/manager.yaml) projects a token with
	// audience auth.ControllerAudience here; the
	// kubelet rewrites the file on rotation, and both callers re-read it on
	// every request so a rotated token is picked up immediately.
	//
	// This is intentionally NOT the default automount path
	// (/var/run/secrets/kubernetes.io/serviceaccount/token), which carries
	// the apiserver-bound token used by the controller-runtime client. Two
	// distinct tokens, two distinct paths, two distinct audiences: a leak
	// of one is useless on the other surface.
	DefaultBearerTokenPath = "/var/run/secrets/inferencecache.io/controller-token/token"
)

// CacheIndexPoller periodically scrapes the server's internal /snapshot endpoint
// and reflects the cluster-wide cache aggregate into the singleton CacheIndex
// CR status — the server exposes the data, the controller scrapes it and writes
// the CR. It is a leader-elected manager Runnable, not an event-driven
// reconciler, because the data source is the server's in-memory index, not the
// CR itself.
type CacheIndexPoller struct {
	Client      client.Client
	Log         logr.Logger
	SnapshotURL string        // e.g. http://inference-cache-server:8081/snapshot
	Interval    time.Duration // refresh cadence; <=0 → DefaultRefreshInterval
	HTTPClient  *http.Client  // optional; injected in tests
	Name        string        // singleton CR name; "" → DefaultCacheIndexName
	// BearerTokenPath is the file the projected ServiceAccount token is
	// mounted at. "" → DefaultBearerTokenPath. A path that does not exist is
	// treated as "no token configured" — the scrape goes out unauthenticated
	// and the server's 401 surfaces as a normal fail-soft skipped tick.
	// Local development without a token mounted still works this way. A
	// present-but-unreadable token (permissions / IO error) is surfaced as
	// an error in the controller's log so the operator sees the real cause
	// instead of misattributing the 401 to a server-side identity mismatch.
	BearerTokenPath string
}

// The poller reads via the manager's cached client (a Get is backed by an
// informer, hence list+watch) and creates the singleton; it never updates,
// patches, or deletes the resource itself — only its status subresource.
// It also lists CacheBackends across all namespaces and patches each
// backend's status.indexParticipation projection from the same /snapshot
// scrape (see [refreshCacheBackendParticipation]).
// +kubebuilder:rbac:groups=inferencecache.io,resources=cacheindices,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=inferencecache.io,resources=cacheindices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachetenants,verbs=get;list;watch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachetenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends,verbs=get;list;watch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Start runs the refresh loop until ctx is done. Satisfies manager.Runnable.
func (p *CacheIndexPoller) Start(ctx context.Context) error {
	interval := p.Interval
	if interval <= 0 {
		interval = DefaultRefreshInterval
	}
	logger := p.logger(ctx)
	logger.Info("starting CacheIndex poller", "snapshotURL", p.SnapshotURL, "interval", interval)

	if err := p.refresh(ctx); err != nil {
		logger.Error(err, "initial CacheIndex refresh failed")
	}

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := p.refresh(ctx); err != nil {
				logger.Error(err, "CacheIndex refresh failed")
			}
		}
	}
}

// NeedLeaderElection ensures only the elected leader writes the CR.
func (p *CacheIndexPoller) NeedLeaderElection() bool { return true }

// refresh scrapes the snapshot, ensures the singleton CR exists, and writes its
// status only when the meaningful aggregate changed (timestamps are ignored for
// change detection to avoid needless writes under steady traffic).
func (p *CacheIndexPoller) refresh(ctx context.Context) error {
	name := p.name()

	// Ensure the singleton exists FIRST, so `kubectl get cacheindex` shows it
	// even before — or without — a successful snapshot scrape (e.g. the server
	// isn't reachable yet). Its status is filled on the next successful tick.
	var ci cachev1alpha1.CacheIndex
	switch err := p.Client.Get(ctx, types.NamespacedName{Name: name}, &ci); {
	case apierrors.IsNotFound(err):
		ci = cachev1alpha1.CacheIndex{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := p.Client.Create(ctx, &ci); err != nil {
			return fmt.Errorf("create CacheIndex %q: %w", name, err)
		}
	case err != nil:
		return fmt.Errorf("get CacheIndex %q: %w", name, err)
	}

	// bearerToken errors are surfaced separately from the scrape so a missing
	// or unreadable projected token shows up in the controller's logs with the
	// expected path, rather than silently degrading to an unauthenticated
	// scrape that the server rejects as 401. The token is still treated as
	// optional (empty token → unauthenticated request, useful for local-dev
	// runs where the controller isn't pod-scoped) but a real read failure
	// (e.g. file exists but unreadable) is no longer indistinguishable from
	// "no token configured."
	token, tokenErr := p.bearerToken()
	if tokenErr != nil {
		p.logger(ctx).Error(tokenErr, "read bearer token; scraping unauthenticated")
	}
	snap, err := fetchSnapshot(ctx, p.httpClient(), p.SnapshotURL, token)
	if err != nil {
		// Soft-state: a single failed scrape must NOT clear the cluster-wide
		// CacheIndex status nor the per-backend indexParticipation projection.
		// The caller logs the error.
		return err
	}
	desired := buildCacheIndexStatus(snap, p.SnapshotURL, time.Now())

	// Three projections of ONE scrape, so the cluster-wide CacheIndex, the
	// per-CacheBackend indexParticipation, and the per-CacheTenant statuses can
	// never disagree with each other.

	// Projection 1 — per-CacheBackend status.indexParticipation. Run it BEFORE
	// the cluster-wide CacheIndex write so it still happens if that write errors
	// (e.g. transient conflict). Non-fatal — log and continue (fail-soft per
	// backend, matching the single failing-replica → single backend-skipped model).
	if perr := p.refreshCacheBackendParticipation(ctx, snap); perr != nil {
		p.logger(ctx).Error(perr, "project per-backend index participation")
	}

	// Projection 2 — cluster-wide CacheIndex status, write-only-on-change.
	if !statusEqual(ci.Status, desired) {
		ci.Status = desired
		if err := p.Client.Status().Update(ctx, &ci); err != nil {
			return fmt.Errorf("update CacheIndex %q status: %w", name, err)
		}
	}

	// Projection 3 — per-CacheTenant status.
	return p.reconcileTenantStatuses(ctx, snap)
}

// reconcileTenantStatuses writes each CacheTenant's observed status from the
// snapshot's per-tenant aggregate. Tenants are matched by spec.tenantID — NOT
// by metadata.name — because tenantID is the identity an ingest carries and the
// index aggregates by. Status().Patch write-only-on-change keeps steady traffic
// from churning resourceVersions (the same discipline as the CacheIndex write
// and the CacheBackend status writers). A patch failure for one tenant does not
// abort the others.
func (p *CacheIndexPoller) reconcileTenantStatuses(ctx context.Context, snap index.Snapshot) error {
	var tenants cachev1alpha1.CacheTenantList
	if err := p.Client.List(ctx, &tenants); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // CacheTenant CRD not installed — nothing to project.
		}
		return fmt.Errorf("list CacheTenants: %w", err)
	}

	observedByID := make(map[string]index.TenantSnapshot, len(snap.Tenants))
	for _, t := range snap.Tenants {
		observedByID[t.TenantID] = t
	}
	// Two CacheTenant CRs can declare the same spec.tenantID. The control-plane
	// reconciler dedups before pushing /policy, so only ONE quota is enforced per
	// tenantID — the first by (namespace, name). Resolve the same winner here so a
	// shadowed duplicate doesn't publish status claiming its own (non-effective)
	// budget is enforced.
	owners := effectiveTenantOwners(tenants.Items)

	var errs []error
	for i := range tenants.Items {
		ct := &tenants.Items[i]
		// We only reach here after a SUCCESSFUL scrape, so a tenant missing from
		// snap.Tenants genuinely holds zero distinct prefixes right now (it has no
		// activity, was evicted to zero, or has a zero budget) — that is an
		// observed 0, not "unknown". Synthesize a zero row so status.indexEntries
		// reflects 0 rather than staying nil.
		obs, ok := observedByID[ct.Spec.TenantID]
		if !ok {
			obs = index.TenantSnapshot{TenantID: ct.Spec.TenantID}
		}
		// Any CR whose tenantID is owned by a DIFFERENT CacheTenant is a shadowed
		// duplicate — whether or not it declares a quota of its own. A no-quota
		// duplicate that reported Ready=True/NoQuota would imply the tenant is
		// unenforced, when in fact the owning CR enforces a budget for that same
		// tenantID; flag it so the operator sees the conflict.
		var shadowedBy *types.NamespacedName
		if owner, owned := owners[ct.Spec.TenantID]; owned &&
			(owner.Namespace != ct.Namespace || owner.Name != ct.Name) {
			o := owner
			shadowedBy = &o
		}
		desired := buildCacheTenantStatus(ct, obs, shadowedBy)
		if cacheTenantStatusEqual(ct.Status, desired) {
			continue
		}
		patched := ct.DeepCopy()
		patched.Status = desired
		if err := p.Client.Status().Patch(ctx, patched, client.MergeFrom(ct)); err != nil {
			errs = append(errs, fmt.Errorf("patch CacheTenant %q status: %w", client.ObjectKeyFromObject(ct), err))
		}
	}
	return errors.Join(errs...)
}

// refreshCacheBackendParticipation projects the cluster-wide snapshot into each
// CacheBackend's status.indexParticipation. Attribution rules:
//
//   - The subscriber sidecar runs inside the engine pod and reports
//     replica_id=<pod-name>, tenant_id=<pod-namespace>. The CacheBackend
//     points at the same engine pod via spec.engineSelector.matchLabels.
//     So: look up the engine pod by (replica.Tenant, replica.ReplicaID),
//     then attribute to the FIRST in-namespace CacheBackend whose
//     EngineSelector matches the pod's labels — mirroring the pod
//     webhook's "first-match wins" engine-wiring rule (see
//     internal/webhook/pod/podinjector.go's selectCacheBackend). Two
//     backends with overlapping selectors must agree on which one owns
//     the pod, or status will disagree with what the engine was actually
//     wired to.
//   - A replica with no engine pod found (pod was deleted between events
//     and now) is dropped — its contributions only show up in the
//     cluster-wide CacheIndex.
//   - A CacheBackend with no EngineSelector (or empty MatchLabels) cannot
//     claim any replica and is skipped; otherwise EngineSelector would
//     match every pod by vacuous truth and steal everyone else's stats.
//
// Per-backend indexParticipation.HitRate stays nil here. The snapshot now
// carries a per-replica presence bit (ReplicaSnapshot.StatsReported) that the
// cluster-aggregate CacheIndex projection uses, but this per-backend path
// aggregates MANY replicas onto one backend and has no defined
// backend-level hit-rate reduction (mean? token-weighted? across which
// replicas — stats-bearing only?). Emitting one without that decision would be
// arbitrary, and a fabricated 0 would mislead operators, so backend hit-rate
// aggregation is deliberately left to a follow-up; the presence bit added by
// the pointer-harmonize change is consumed only by CacheIndex.status.
func (p *CacheIndexPoller) refreshCacheBackendParticipation(ctx context.Context, snap index.Snapshot) error {
	var backends cachev1alpha1.CacheBackendList
	if err := p.Client.List(ctx, &backends); err != nil {
		return fmt.Errorf("list CacheBackends: %w", err)
	}
	if len(backends.Items) == 0 {
		// No backends present (e.g. the last one was just deleted) -> prune any
		// lingering tier-2 gauge series so a stale value can't keep alerting
		// after the fleet is gone. A List error above is a different case: we
		// return before here and preserve series (we don't know the state).
		reconcileBackendT2HitRateSeries(nil, nil, nil)
		return nil
	}

	// Group CacheBackends by namespace for fast scoped iteration, and by
	// (namespace, name) for O(1) annotation-based lookup. A backend with no
	// EngineSelector or empty MatchLabels is excluded from the selector-
	// matching fallback (otherwise an unset selector would silently claim
	// every pod) but is still discoverable by annotation: if a webhook ever
	// chose it (perhaps when its selector was non-empty in the past) the
	// annotation is still authoritative.
	backendsByNS := make(map[string][]int)
	backendsByNSName := make(map[types.NamespacedName]int)
	for i := range backends.Items {
		cb := &backends.Items[i]
		backendsByNSName[types.NamespacedName{Namespace: cb.Namespace, Name: cb.Name}] = i
		if cb.Spec.EngineSelector == nil || len(cb.Spec.EngineSelector.MatchLabels) == 0 {
			continue
		}
		backendsByNS[cb.Namespace] = append(backendsByNS[cb.Namespace], i)
	}
	// Sort the per-namespace backend lists by metadata.name. This gives
	// "first match" a deterministic meaning across poller restarts and
	// makes the selector-fallback result independent of apiserver List
	// ordering — operators who rely on the fallback can predict the winner.
	for ns := range backendsByNS {
		idxs := backendsByNS[ns]
		sort.Slice(idxs, func(a, b int) bool {
			return backends.Items[idxs[a]].Name < backends.Items[idxs[b]].Name
		})
	}

	type agg struct {
		prefixCount   int64
		lastEventAt   time.Time
		t2HitTokens   int64
		t2QueryTokens int64
	}
	// Seed perBackend for EVERY CacheBackend so attributePod can return an
	// annotation-owned backend even after its selector was removed (or for
	// a manually annotated pod whose backend has no selector at all)
	// without dereferencing a nil entry. The decision of whether to write
	// a noise zero is taken at the write step below, not at seed time.
	perBackend := make(map[int]*agg, len(backends.Items))
	for i := range backends.Items {
		perBackend[i] = &agg{}
	}

	// Pod-attribution cache for the duration of this tick: a single engine
	// pod can host many replicas/prefixes across (model, hash_scheme) and
	// always presents the same labels + annotations, so we never need more
	// than one Get per (namespace, pod-name) per tick. ownerIdx == -1 means
	// "looked up and no owner"; the entry caches the negative result.
	type podKey struct{ ns, name string }
	podAttrs := make(map[podKey]int)
	// taintedNamespaces tracks namespaces where at least one pod lookup
	// failed with a transient (non-NotFound) error. We cannot distinguish
	// "this replica had no owner" from "we don't know who its owner was"
	// in those namespaces, so we preserve every prior CacheBackend status
	// in them rather than risk publishing a false drain on a backend whose
	// replicas all happened to error out this tick.
	taintedNamespaces := make(map[string]struct{})

	for _, r := range snap.Replicas {
		if r.Tenant == "" || r.ReplicaID == "" {
			continue
		}
		if _, tainted := taintedNamespaces[r.Tenant]; tainted {
			continue
		}
		key := podKey{r.Tenant, r.ReplicaID}
		ownerIdx, cached := podAttrs[key]
		if !cached {
			var pod corev1.Pod
			err := p.Client.Get(ctx, types.NamespacedName{Namespace: r.Tenant, Name: r.ReplicaID}, &pod)
			switch {
			case apierrors.IsNotFound(err):
				// Engine pod is gone — common after a scale-down. The
				// cluster-wide CacheIndex still reflects the data until TTL.
				podAttrs[key] = -1
				continue
			case err != nil:
				// Transient API error: taint the namespace so we don't
				// publish under-counted projections for any backend in it.
				// The next successful tick will resume normal projection.
				p.logger(ctx).V(1).Info("lookup engine pod failed; tainting namespace to preserve prior CacheBackend status",
					"namespace", r.Tenant, "name", r.ReplicaID, "err", err.Error())
				taintedNamespaces[r.Tenant] = struct{}{}
				continue
			}
			ownerIdx = p.attributePod(&pod, backendsByNS[r.Tenant], backendsByNSName, backends.Items)
			podAttrs[key] = ownerIdx
		}
		if ownerIdx < 0 {
			continue
		}
		a := perBackend[ownerIdx]
		a.prefixCount += int64(r.PrefixCount)
		if r.LastEventAt.After(a.lastEventAt) {
			a.lastEventAt = r.LastEventAt
		}
		a.t2HitTokens += r.T2HitTokens
		a.t2QueryTokens += r.T2QueryTokens
	}

	// exercised tracks the backends whose tier-2 gauge we set this tick, so
	// stale series can be pruned below (see reconcileBackendT2HitRateSeries).
	exercised := map[t2Key]struct{}{}
	for i, a := range perBackend {
		cb := &backends.Items[i]
		if _, tainted := taintedNamespaces[cb.Namespace]; tainted {
			// Soft-state: preserve prior status when we couldn't compute
			// a trustworthy projection for this backend's namespace.
			continue
		}
		desired := &cachev1alpha1.CacheBackendIndexParticipation{PrefixCount: a.prefixCount}
		if !a.lastEventAt.IsZero() {
			t := metav1.NewTime(a.lastEventAt)
			desired.LastEventAt = &t
		}
		// Presence-aware tier-2 (external offload) hit-rate: surface it only
		// once the tier was actually queried. A "0" here — queried, zero
		// reloads — is the signal of a silently-degraded offload tier; nil
		// means "not yet exercised" (no external lookups), which must NOT read
		// as 0. Clamp defends against any counter anomaly.
		//
		// This ratio (and so the T2Degraded condition + the BackendT2Degraded
		// `== 0` alert) is LIFETIME-CUMULATIVE: hits/queries over all the tier-2
		// traffic the backend's replicas have ever reported. By design it flags a
		// tier that has NEVER served a reload — the silent-from-start failures this
		// signal targets (PYTHONHASHSEED, server OOM, version skew each produce zero
		// reloads from the first query). A backend that served some reloads and only
		// LATER regresses keeps a non-zero lifetime ratio, so `== 0` will not trip;
		// that mid-life regression is caught instead by the windowed, per-pod
		// LMCacheT2NoHits alert (rate(hits)==0 over a window). Keeping this
		// cumulative keeps the condition and the alert consistent.
		if a.t2QueryTokens > 0 {
			ratio := float64(a.t2HitTokens) / float64(a.t2QueryTokens)
			if ratio < 0 {
				ratio = 0
			} else if ratio > 1 {
				ratio = 1
			}
			rate := formatRate(float32(ratio))
			desired.T2HitRate = &rate
			// Mirror the value onto the Prometheus gauge (the Alertmanager
			// surface — CR status is not scraped) and record it so the gauge's
			// stale series can be pruned after the loop.
			key := t2Key{cb.Namespace, cb.Name}
			backendT2HitRate.WithLabelValues(key.label()).Set(ratio)
			backendT2QueryTokensTotal.WithLabelValues(key.label()).Add(float64(t2QueryDelta(key, a.t2QueryTokens)))
			exercised[key] = struct{}{}
		}
		// "Don't write noise zeros" gate: a backend that has no selector
		// configured AND has never published participation AND has no
		// real attribution this tick stays nil — writing a noise zero
		// would invite operators to read meaning where there is none.
		// A real non-zero attribution (e.g. via the injected-by
		// annotation pointing at a selector-less backend) bypasses the
		// gate so the data is still surfaced.
		isZeroState := desired.PrefixCount == 0 && desired.LastEventAt == nil && desired.HitRate == nil && desired.T2HitRate == nil
		hasSelector := cb.Spec.EngineSelector != nil && len(cb.Spec.EngineSelector.MatchLabels) > 0
		if isZeroState && !hasSelector && cb.Status.IndexParticipation == nil {
			continue
		}
		if participationEqual(cb.Status.IndexParticipation, desired) {
			continue
		}
		before := cb.DeepCopy()
		cb.Status.IndexParticipation = desired
		if err := p.Client.Status().Patch(ctx, cb, client.MergeFrom(before)); err != nil {
			// Single-backend failure must not block the rest of the projection.
			p.logger(ctx).Error(err, "patch CacheBackend indexParticipation",
				"cacheBackend", cb.Namespace+"/"+cb.Name)
			cb.Status.IndexParticipation = before.Status.IndexParticipation
			continue
		}
	}
	// Prune tier-2 gauge series for backends that drained (no longer exercised)
	// or were deleted; namespaces tainted this tick are left untouched.
	present := make(map[t2Key]struct{}, len(backends.Items))
	for i := range backends.Items {
		present[t2Key{backends.Items[i].Namespace, backends.Items[i].Name}] = struct{}{}
	}
	reconcileBackendT2HitRateSeries(exercised, present, taintedNamespaces)
	return nil
}

// matchLabelsSelects mirrors the metav1.LabelSelector MatchLabels semantics:
// every (k,v) in want must appear in have. Empty `want` returns false to
// match the caller's "no selector ⇒ no claim" guard (so a selector that
// accidentally got cleared in flight doesn't suddenly claim every pod).
func matchLabelsSelects(want, have map[string]string) bool {
	if len(want) == 0 {
		return false
	}
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// attributePod returns the index into backends of the CacheBackend that owns
// the given engine pod, or -1 if none can be determined. Two-step resolution:
//
//  1. If the pod carries the webhook's `inferencecache.io/injected-by`
//     annotation, parse it as namespace/name and resolve directly. This is
//     the authoritative signal: it records the CacheBackend the webhook
//     actually wired the engine to. Annotation in another namespace is
//     ignored (cross-namespace attribution would be misleading).
//  2. Fallback for pods that bypassed the webhook (manual sidecar, opt-out
//     annotation): iterate the namespace's CacheBackends sorted by name and
//     take the first whose EngineSelector matches the pod's labels —
//     mirroring the webhook's first-match rule but ordered deterministically
//     by name so the poller is stable across restarts.
func (p *CacheIndexPoller) attributePod(pod *corev1.Pod, nsBackends []int, byNSName map[types.NamespacedName]int, items []cachev1alpha1.CacheBackend) int {
	if raw := pod.Annotations[podwebhook.AnnotationInjectedBy]; raw != "" {
		ns, name, ok := strings.Cut(raw, "/")
		if ok && ns == pod.Namespace {
			if idx, found := byNSName[types.NamespacedName{Namespace: ns, Name: name}]; found {
				return idx
			}
			// Annotation references a backend that no longer exists.
			// Don't fall back — the operator's intent was explicit.
			return -1
		}
	}
	for _, i := range nsBackends {
		cb := &items[i]
		if matchLabelsSelects(cb.Spec.EngineSelector.MatchLabels, pod.Labels) {
			return i
		}
	}
	return -1
}

// participationEqual is the no-churn guard: skip the Status().Patch when the
// projected fields are identical to what's already published. Uses semantic
// equality so a *string pointer with the same value is treated as equal.
func participationEqual(a, b *cachev1alpha1.CacheBackendIndexParticipation) bool {
	return equality.Semantic.DeepEqual(a, b)
}

func (p *CacheIndexPoller) name() string {
	if p.Name != "" {
		return p.Name
	}
	return DefaultCacheIndexName
}

func (p *CacheIndexPoller) httpClient() *http.Client {
	if p.HTTPClient != nil {
		return p.HTTPClient
	}
	return &http.Client{Timeout: 5 * time.Second}
}

func (p *CacheIndexPoller) logger(ctx context.Context) logr.Logger {
	if p.Log.GetSink() != nil {
		return p.Log
	}
	return log.FromContext(ctx)
}

// bearerToken reads the projected ServiceAccount token. Re-read on every
// scrape so kubelet rotations are picked up without process restarts; the
// file is tmpfs so the read is cheap.
//
// Error semantics:
//   - File missing → ("", nil). Treated as "no token configured" so a local
//     out-of-cluster run still flows through the same code path; the scrape
//     goes out unauthenticated and the server rejects 401, which is the
//     correct posture for that environment.
//   - File present but unreadable (permissions, IO error, etc.) →
//     ("", wrappedError). Caller surfaces this in the log so the operator
//     sees the real cause instead of misattributing the eventual 401 to a
//     server-side identity mismatch.
func (p *CacheIndexPoller) bearerToken() (string, error) {
	path := p.BearerTokenPath
	if path == "" {
		path = DefaultBearerTokenPath
	}
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// Local-dev / out-of-cluster: no token mounted is expected.
		return "", nil
	case err != nil:
		return "", fmt.Errorf("read bearer token %q: %w", path, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// fetchSnapshot GETs and decodes the server's /snapshot JSON. When token is
// non-empty it is sent as an Authorization: Bearer header so the server's
// auth middleware can validate it via TokenReview.
func fetchSnapshot(ctx context.Context, hc *http.Client, url, token string) (index.Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return index.Snapshot{}, fmt.Errorf("build snapshot request %q: %w", url, err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return index.Snapshot{}, fmt.Errorf("scrape snapshot %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return index.Snapshot{}, fmt.Errorf("snapshot %s: unexpected status %d", url, resp.StatusCode)
	}
	var snap index.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return index.Snapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	return snap, nil
}

// buildCacheIndexStatus converts an index snapshot into CacheIndex status.
func buildCacheIndexStatus(snap index.Snapshot, serverURL string, now time.Time) cachev1alpha1.CacheIndexStatus {
	st := cachev1alpha1.CacheIndexStatus{
		Prefixes: cachev1alpha1.PrefixStatus{
			Summary: cachev1alpha1.PrefixSummary{Total: int64(snap.TotalPrefixes), Hot: int64(snap.HotPrefixes)},
		},
		ObservedServer: serverURL,
		LastUpdated:    metav1.NewTime(now),
	}
	// Cluster-wide CacheIndex.status.replicas is keyed by `id` alone
	// (v1alpha1 surface). Two scenarios complicate that:
	//   - Prefix-only replicas (no stats reported yet) should not appear
	//     here at all — surfacing them would fabricate `0` for hitRate,
	//     pressure, memory, and lastUpdate while the type says these are
	//     latest reported stats. They are still tracked per-backend via
	//     CacheBackend.status.indexParticipation.
	//   - Two stats-reporting replicas sharing a name across namespaces
	//     collide on `id` — pick the lexicographically-later tenant
	//     deterministically so the chosen row is stable across ticks.
	//     The `tenant` field on each row keeps the source identifiable.
	byID := make(map[string]index.ReplicaSnapshot, len(snap.Replicas))
	for _, r := range snap.Replicas {
		if r.LastUpdate.IsZero() {
			continue
		}
		if existing, ok := byID[r.ReplicaID]; ok && existing.Tenant >= r.Tenant {
			continue
		}
		byID[r.ReplicaID] = r
	}
	for _, r := range byID {
		row := cachev1alpha1.ReplicaCacheStatus{
			ID:               r.ReplicaID,
			Tenant:           r.Tenant,
			CacheMemoryBytes: r.CacheMemoryBytes,
			Pressure:         formatRate(r.Pressure),
			LastUpdate:       metav1.NewTime(r.LastUpdate),
		}
		// HitRate is a *string that must stay nil when the replica's stats
		// reporter hasn't emitted yet — a fabricated "0" reads as a real 0%
		// hit rate. Emit it when the presence bit is set OR (skew fallback) the
		// row has a non-zero LastUpdate: an OLDER /snapshot producer does not
		// send statsReported (decodes false), but a non-zero lastUpdate means it
		// DID report stats, so we must not drop its real hitRate on a
		// controller-first rollout. Every row here already passed the
		// LastUpdate.IsZero() filter above, so a new server (which sets
		// StatsReported whenever it has a stats entry, i.e. a lastUpdate) and an
		// old server agree via this fallback.
		if r.StatsReported || !r.LastUpdate.IsZero() {
			row.HitRate = ptrTo(formatRate(r.HitRate))
		}
		st.Replicas = append(st.Replicas, row)
	}
	sort.Slice(st.Replicas, func(a, b int) bool { return st.Replicas[a].ID < st.Replicas[b].ID })
	for _, t := range snap.Tenants {
		row := cachev1alpha1.TenantCacheStatus{
			ID: t.TenantID,
			// IndexEntries is a *int64. The cluster aggregate always carries a
			// real count when it emits a tenant row (this projection only runs
			// after a successful scrape), so it is always present here; nil is
			// reserved for "not yet computed" to match
			// CacheTenant.status.indexEntries.
			IndexEntries: ptrTo(t.IndexEntries),
			// Deprecated field, hard-zeroed here (NOT copied from the snapshot):
			// the controller is authoritative for keeping it 0 even when talking
			// to an older/skewed server that still reports a non-zero (double-
			// counted) per-tenant memory in its /snapshot. See
			// TenantCacheStatus.MemoryUsed.
			MemoryUsed: 0,
		}
		// HitRate stays nil until a replica of this tenant has reported stats
		// (HitRateReported), so an observed mean of 0 is distinguishable from
		// "no stats reported yet". Skew fallback: an OLDER /snapshot producer
		// does not send hitRateReported (decodes false) but does send a non-zero
		// mean HitRate when it had samples, so a non-zero HitRate means it
		// reported — don't drop it on a controller-first rollout. The only
		// residual old-server ambiguity is a genuine 0% mean, which has no
		// signal on the old wire and reads as nil (the same "0"-vs-unreported
		// ambiguity this change removes for new servers); acceptable degradation
		// until the server side ships the presence bit.
		if t.HitRateReported || t.HitRate != 0 {
			row.HitRate = ptrTo(formatRate(t.HitRate))
		}
		st.Tenants = append(st.Tenants, row)
	}
	return st
}

// ptrTo returns a pointer to v. Used for the presence-aware CacheIndex status
// fields (HitRate, IndexEntries) that stay nil when unreported.
func ptrTo[T any](v T) *T { return &v }

// formatRate renders a [0,1] rate as a short decimal string (float32 precision).
func formatRate(f float32) string {
	return strconv.FormatFloat(float64(f), 'f', -1, 32)
}

// statusEqual compares two statuses ignoring timestamps, so steady-state
// traffic that only advances lastSeen doesn't trigger needless CR writes.
func statusEqual(a, b cachev1alpha1.CacheIndexStatus) bool {
	return reflect.DeepEqual(normalizeForCompare(a), normalizeForCompare(b))
}

func normalizeForCompare(s cachev1alpha1.CacheIndexStatus) cachev1alpha1.CacheIndexStatus {
	s.LastUpdated = metav1.Time{}
	if s.Replicas != nil {
		replicas := make([]cachev1alpha1.ReplicaCacheStatus, len(s.Replicas))
		copy(replicas, s.Replicas)
		for i := range replicas {
			replicas[i].LastUpdate = metav1.Time{}
		}
		s.Replicas = replicas
	}
	return s
}

// Condition types written onto CacheTenant.status.
const (
	tenantConditionReady         = "Ready"
	tenantConditionQuotaExceeded = "QuotaExceeded"
)

// tenantHasQuota reports whether a CacheTenant declares an enforceable
// index-entry budget — the same condition resolveOneTenant uses to decide
// whether to push it to /policy.
func tenantHasQuota(ct *cachev1alpha1.CacheTenant) bool {
	return ct.Spec.TenantID != "" && ct.Spec.Quota != nil && ct.Spec.Quota.MaxIndexEntries != nil
}

// effectiveTenantOwners returns, per spec.tenantID, the CacheTenant the
// control-plane reconciler treats as authoritative: the first quota-bearing CR
// by (namespace, name). This MUST match resolveTenants' dedup tie-break so the
// status writer and the /policy pusher agree on which CR's budget is enforced.
// Only quota-bearing CRs participate (a CR with no budget enforces nothing).
func effectiveTenantOwners(items []cachev1alpha1.CacheTenant) map[string]types.NamespacedName {
	ordered := make([]int, 0, len(items))
	for i := range items {
		if tenantHasQuota(&items[i]) {
			ordered = append(ordered, i)
		}
	}
	sort.Slice(ordered, func(a, b int) bool {
		ia, ib := &items[ordered[a]], &items[ordered[b]]
		if ia.Namespace != ib.Namespace {
			return ia.Namespace < ib.Namespace
		}
		return ia.Name < ib.Name
	})
	owners := make(map[string]types.NamespacedName, len(ordered))
	for _, i := range ordered {
		ct := &items[i]
		if _, taken := owners[ct.Spec.TenantID]; taken {
			continue
		}
		owners[ct.Spec.TenantID] = types.NamespacedName{Namespace: ct.Namespace, Name: ct.Name}
	}
	return owners
}

// buildCacheTenantStatus projects one snapshot tenant aggregate onto a
// CacheTenant's status. It is only called after a successful /snapshot scrape,
// so obs is always a live reading: the matched snapshot row, or a synthesized
// zero row when the tenant has no current activity (both are observed values,
// not "unknown").
//
//   - IndexEntries is the live distinct-prefix count (0 for an idle, drained, or
//     zero-budget tenant). It stays nil only before the first successful scrape
//     ever writes status — i.e. nil means "not computed yet", not "zero".
//   - Ready=True: the controller has a live reading for the tenant.
//   - QuotaExceeded reflects the latest reading against the entry budget.
//     Enforcement evicts at ingest, so it normally reads False; it can briefly
//     flip True between an over-budget ingest and the next scrape.
//
// shadowedBy is non-nil when this CR declares a quota but another CacheTenant
// owns the same spec.tenantID and is the one actually enforced. Such a duplicate
// must NOT report its own budget as effective: it goes Ready=False/Duplicate and
// QuotaExceeded=False/NotEffective so the operator sees it is being ignored.
func buildCacheTenantStatus(ct *cachev1alpha1.CacheTenant, obs index.TenantSnapshot, shadowedBy *types.NamespacedName) cachev1alpha1.CacheTenantStatus {
	st := cachev1alpha1.CacheTenantStatus{ObservedGeneration: ct.Generation}
	// Seed from existing conditions so meta.SetStatusCondition keeps each
	// condition's LastTransitionTime stable when its Status doesn't flip.
	if ct.Status.Conditions != nil {
		st.Conditions = append([]metav1.Condition(nil), ct.Status.Conditions...)
	}

	indexEntries := obs.IndexEntries
	entries := indexEntries
	st.IndexEntries = &entries

	if shadowedBy != nil {
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               tenantConditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             "DuplicateTenantID",
			Message:            fmt.Sprintf("spec.tenantID %q is also declared by CacheTenant %s, which is the effective owner; this CacheTenant is ignored", ct.Spec.TenantID, shadowedBy.String()),
			ObservedGeneration: ct.Generation,
		})
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               tenantConditionQuotaExceeded,
			Status:             metav1.ConditionFalse,
			Reason:             "NotEffective",
			Message:            fmt.Sprintf("not the effective owner of tenantID %q (CacheTenant %s is)", ct.Spec.TenantID, shadowedBy.String()),
			ObservedGeneration: ct.Generation,
		})
		return st
	}

	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               tenantConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Observed",
		Message:            "projected from the latest server snapshot read",
		ObservedGeneration: ct.Generation,
	})

	quotaStatus, quotaReason, quotaMsg := metav1.ConditionFalse, "NoQuota", "no index-entry quota configured"
	if ct.Spec.Quota != nil && ct.Spec.Quota.MaxIndexEntries != nil {
		budget := *ct.Spec.Quota.MaxIndexEntries
		if indexEntries > budget {
			quotaStatus = metav1.ConditionTrue
			quotaReason = "OverEntryBudget"
			quotaMsg = fmt.Sprintf("index entries %d exceed budget %d", indexEntries, budget)
		} else {
			quotaReason = "WithinBudget"
			quotaMsg = fmt.Sprintf("index entries within budget %d", budget)
		}
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               tenantConditionQuotaExceeded,
		Status:             quotaStatus,
		Reason:             quotaReason,
		Message:            quotaMsg,
		ObservedGeneration: ct.Generation,
	})

	return st
}

// cacheTenantStatusEqual compares two tenant statuses ignoring each condition's
// LastTransitionTime, so a no-op reconcile (same Status/Reason/Message) doesn't
// patch and churn resourceVersions.
func cacheTenantStatusEqual(a, b cachev1alpha1.CacheTenantStatus) bool {
	return reflect.DeepEqual(normalizeTenantForCompare(a), normalizeTenantForCompare(b))
}

func normalizeTenantForCompare(s cachev1alpha1.CacheTenantStatus) cachev1alpha1.CacheTenantStatus {
	if s.Conditions != nil {
		conds := make([]metav1.Condition, len(s.Conditions))
		copy(conds, s.Conditions)
		for i := range conds {
			conds[i].LastTransitionTime = metav1.Time{}
		}
		s.Conditions = conds
	}
	return s
}
