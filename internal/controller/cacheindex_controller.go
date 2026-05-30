package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
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
//     then attribute to every CacheBackend in that namespace whose
//     EngineSelector matches the pod's labels.
//   - A replica with no engine pod found (pod was deleted between events
//     and now) is dropped — its contributions only show up in the
//     cluster-wide CacheIndex.
//   - A CacheBackend with no EngineSelector (or empty MatchLabels) cannot
//     claim any replica and is skipped; otherwise EngineSelector would
//     match every pod by vacuous truth and steal everyone else's stats.
//   - If two CacheBackends in the same namespace both select the same
//     engine pod, the replica is attributed to BOTH — the operator is
//     free to overlap selectors and each backend's status reflects what
//     it sees.
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

	// Group CacheBackends by namespace for fast scoped iteration. A backend
	// with no EngineSelector or empty MatchLabels is skipped entirely (so an
	// operator who hasn't selected anything doesn't accidentally claim all
	// replicas in their namespace).
	backendsByNS := make(map[string][]int)
	for i := range backends.Items {
		cb := &backends.Items[i]
		if cb.Spec.EngineSelector == nil || len(cb.Spec.EngineSelector.MatchLabels) == 0 {
			continue
		}
		backendsByNS[cb.Namespace] = append(backendsByNS[cb.Namespace], i)
	}

	type agg struct {
		prefixCount int64
		lastEventAt time.Time
	}
	// Seed perBackend with every selectable CacheBackend (including ones
	// with no matching replicas this tick) so a successful snapshot that
	// drains a backend's replicas publishes prefixCount=0 instead of
	// leaving stale positive state behind.
	perBackend := make(map[int]*agg, len(backends.Items))
	for _, idxs := range backendsByNS {
		for _, i := range idxs {
			perBackend[i] = &agg{}
		}
	}

	// Pod-label lookup cache for the duration of this tick: a single engine
	// pod can host many replicas/prefixes across (model, hash_scheme) and
	// always presents the same labels, so we never need more than one Get
	// per (namespace, pod-name) per tick.
	type podKey struct{ ns, name string }
	podLabels := make(map[podKey]map[string]string)
	missingPods := make(map[podKey]struct{})

	for _, r := range snap.Replicas {
		if r.Tenant == "" || r.ReplicaID == "" {
			continue
		}
		nsBackends := backendsByNS[r.Tenant]
		if len(nsBackends) == 0 {
			continue
		}
		key := podKey{r.Tenant, r.ReplicaID}
		if _, gone := missingPods[key]; gone {
			continue
		}
		labels, ok := podLabels[key]
		if !ok {
			var pod corev1.Pod
			err := p.Client.Get(ctx, types.NamespacedName{Namespace: r.Tenant, Name: r.ReplicaID}, &pod)
			switch {
			case apierrors.IsNotFound(err):
				// Replica's engine pod is gone — common after a scale-down.
				// Skip attribution (the cluster-wide CacheIndex still
				// reflects the data until TTL eviction).
				missingPods[key] = struct{}{}
				continue
			case err != nil:
				// Transient API error: log + skip this replica only; don't
				// poison the rest of the tick.
				p.logger(ctx).V(1).Info("lookup engine pod for replica attribution failed; skipping",
					"namespace", r.Tenant, "name", r.ReplicaID, "err", err.Error())
				missingPods[key] = struct{}{}
				continue
			}
			labels = pod.Labels
			podLabels[key] = labels
		}
		for _, i := range nsBackends {
			cb := &backends.Items[i]
			if !matchLabelsSelects(cb.Spec.EngineSelector.MatchLabels, labels) {
				continue
			}
			a := perBackend[i]
			a.prefixCount += int64(r.PrefixCount)
			if r.LastEventAt.After(a.lastEventAt) {
				a.lastEventAt = r.LastEventAt
			}
		}
	}

	for i, a := range perBackend {
		cb := &backends.Items[i]
		desired := &cachev1alpha1.CacheBackendIndexParticipation{PrefixCount: a.prefixCount}
		if !a.lastEventAt.IsZero() {
			t := metav1.NewTime(a.lastEventAt)
			desired.LastEventAt = &t
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
	for _, r := range snap.Replicas {
		st.Replicas = append(st.Replicas, cachev1alpha1.ReplicaCacheStatus{
			ID:               r.ReplicaID,
			CacheMemoryBytes: r.CacheMemoryBytes,
			HitRate:          formatRate(r.HitRate),
			Pressure:         formatRate(r.Pressure),
			LastUpdate:       metav1.NewTime(r.LastUpdate),
		})
	}
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
