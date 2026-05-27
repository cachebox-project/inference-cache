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
// +kubebuilder:rbac:groups=inferencecache.io,resources=cacheindices,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=inferencecache.io,resources=cacheindices/status,verbs=get;update;patch

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
		return err
	}
	desired := buildCacheIndexStatus(snap, p.SnapshotURL, time.Now())

	if statusEqual(ci.Status, desired) {
		return nil
	}
	ci.Status = desired
	if err := p.Client.Status().Update(ctx, &ci); err != nil {
		return fmt.Errorf("update CacheIndex %q status: %w", name, err)
	}
	return nil
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
		return index.Snapshot{}, err
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
