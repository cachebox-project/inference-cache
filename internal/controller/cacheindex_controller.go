package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	SnapshotURL string        // e.g. http://inference-cache-server:8080/snapshot
	Interval    time.Duration // refresh cadence; <=0 → DefaultRefreshInterval
	HTTPClient  *http.Client  // optional; injected in tests
	Name        string        // singleton CR name; "" → DefaultCacheIndexName
}

// The poller reads via the manager's cached client (a Get is backed by an
// informer, hence list+watch) and creates the singleton; it never updates,
// patches, or deletes the resource itself — only its status subresource.
// It also lists CacheBackends across all namespaces and patches each
// backend's status.indexParticipation projection from the same /snapshot
// scrape (see [refreshCacheBackendParticipation]).
// +kubebuilder:rbac:groups=inferencecache.io,resources=cacheindices,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=inferencecache.io,resources=cacheindices/status,verbs=get;update;patch
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

	snap, err := fetchSnapshot(ctx, p.httpClient(), p.SnapshotURL)
	if err != nil {
		// Soft-state: a single failed scrape must NOT clear the cluster-wide
		// CacheIndex status nor the per-backend indexParticipation projection.
		// The caller logs the error.
		return err
	}
	desired := buildCacheIndexStatus(snap, p.SnapshotURL, time.Now())

	// Project the same snapshot into each CacheBackend's status.indexParticipation
	// BEFORE we touch the cluster-wide CacheIndex, so the projection runs even
	// if the CacheIndex update itself errors (e.g. transient conflict). Errors
	// during projection are non-fatal — log and continue — to keep parity with
	// the single failing-replica → single backend-skipped fail-soft model.
	if perr := p.refreshCacheBackendParticipation(ctx, snap); perr != nil {
		p.logger(ctx).Error(perr, "project per-backend index participation")
	}

	if statusEqual(ci.Status, desired) {
		return nil
	}
	ci.Status = desired
	if err := p.Client.Status().Update(ctx, &ci); err != nil {
		return fmt.Errorf("update CacheIndex %q status: %w", name, err)
	}
	return nil
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
// Per-replica HitRate stays nil until the snapshot carries an explicit
// presence bit for it (planned with the stats-reporter follow-up).
// Surfacing a fabricated 0 here would mislead operators.
func (p *CacheIndexPoller) refreshCacheBackendParticipation(ctx context.Context, snap index.Snapshot) error {
	var backends cachev1alpha1.CacheBackendList
	if err := p.Client.List(ctx, &backends); err != nil {
		return fmt.Errorf("list CacheBackends: %w", err)
	}
	if len(backends.Items) == 0 {
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
		prefixCount int64
		lastEventAt time.Time
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
	}

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
		// "Don't write noise zeros" gate: a backend that has no selector
		// configured AND has never published participation AND has no
		// real attribution this tick stays nil — writing a noise zero
		// would invite operators to read meaning where there is none.
		// A real non-zero attribution (e.g. via the injected-by
		// annotation pointing at a selector-less backend) bypasses the
		// gate so the data is still surfaced.
		isZeroState := desired.PrefixCount == 0 && desired.LastEventAt == nil && desired.HitRate == nil
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

// fetchSnapshot GETs and decodes the server's /snapshot JSON.
func fetchSnapshot(ctx context.Context, hc *http.Client, url string) (index.Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return index.Snapshot{}, fmt.Errorf("build snapshot request %q: %w", url, err)
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
		st.Replicas = append(st.Replicas, cachev1alpha1.ReplicaCacheStatus{
			ID:               r.ReplicaID,
			Tenant:           r.Tenant,
			CacheMemoryBytes: r.CacheMemoryBytes,
			HitRate:          formatRate(r.HitRate),
			Pressure:         formatRate(r.Pressure),
			LastUpdate:       metav1.NewTime(r.LastUpdate),
		})
	}
	sort.Slice(st.Replicas, func(a, b int) bool { return st.Replicas[a].ID < st.Replicas[b].ID })
	for _, t := range snap.Tenants {
		st.Tenants = append(st.Tenants, cachev1alpha1.TenantCacheStatus{
			ID:         t.TenantID,
			MemoryUsed: t.MemoryUsed,
			HitRate:    formatRate(t.HitRate),
		})
	}
	return st
}

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
