package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// t2Key identifies a CacheBackend's tier-2 series.
type t2Key struct{ ns, name string }

// label is the series' single Prometheus label value: the canonical
// <namespace>/<name>, matching probeResultMetric's `backend` label so the two
// per-CacheBackend controller metrics identify a backend the same way.
func (k t2Key) label() string { return k.ns + "/" + k.name }

// backendT2HitRate is `inferencecache_backend_t2_hit_rate{backend}` — the
// query-weighted tier-2 (external offload, e.g. LMCache) reload hit-rate per
// CacheBackend in [0,1], projected by the CacheIndex poller from
// status.indexParticipation.t2HitRate. The `backend` label is the canonical
// <namespace>/<name> (matching inferencecache_backend_probe_result_total) so one
// label uniquely identifies the CacheBackend; this vec carries NO own namespace
// label — Prometheus injects the install `namespace` from the controller scrape
// target, and an own namespace label would collide with it (shadowed to
// exported_namespace under the default honorLabels=false).
//
// A series is PRESENT only once the tier has been exercised (external lookups
// observed for the backend's replicas). The value is the CUMULATIVE hit-rate
// those replicas report, so it persists (does not reset) while a backend is
// registered but momentarily idle. A value of 0 means the tier was queried but
// served zero reloads — the alertable signal of a silently-degraded offload tier
// (a store/connection failure, an under-sized remote server, or a
// scheduler/worker hash mismatch). The poller deletes the series when the
// backend's replicas leave the index snapshot (their engine pods are gone or
// their index entries TTL-expired) or the backend is removed (see
// reconcileBackendT2HitRateSeries) — NOT merely because the backend went idle:
// the gauge holds the last cumulative hit-rate the replicas reported, so it
// stays put while they stay registered. A degraded-then-idle backend therefore
// keeps exporting 0 until its replicas drain; the BackendT2Degraded alert avoids
// paging it by gating on rate(backendT2QueryTokensTotal) — an idle backend has no
// query growth, so its rate is ~0. This mirrors the lifecycle of
// CacheBackend.status.indexParticipation.t2HitRate (the CR status is not
// scraped) and the T2Degraded condition.
//
// HA note: the CacheIndexPoller that Set()s and prunes these series is
// leader-gated (NeedLeaderElection), so under multiple controller replicas only
// the leader's process writes them — non-leaders never touch the vecs and export
// nothing. A deposed leader's manager stops on lease loss and the pod restarts as
// a non-leader, so its last-written series do not outlive that restart. Even
// during a deposed leader's brief shutdown scrape window the BackendT2Degraded
// alert cannot mis-fire: that process's query counter is frozen (rate ~0, below
// the gate), the alert's 5m `for:` dwell cannot be met by a few-second transient,
// and `max by (namespace, backend)` collapses any duplicate.
var backendT2HitRate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "inferencecache_backend_t2_hit_rate",
	Help: "Tier-2 (external offload) reload hit-rate per CacheBackend in [0,1]; the series is present only once the tier is exercised, and 0 means it is wired but serving zero reloads.",
}, []string{"backend"})

// backendT2QueryTokensTotal is `inferencecache_backend_t2_query_tokens_total{backend}`
// — a MONOTONIC counter of tier-2 (external offload) query tokens observed for the
// CacheBackend. It is the ACTIVITY signal the BackendT2Degraded alert gates on:
// rate() over it separates a backend that is actively being queried (and degraded
// when backendT2HitRate is 0) from one that took a few cold misses and went idle
// (no query growth → rate ~0 → must not page).
//
// The CacheIndex poller observes a per-tick AGGREGATE cumulative — summed over the
// backend's currently-attributed (tenant, replica) snapshot rows — which is NOT
// itself monotonic: a replica or tenant draining out of the aggregate, or an
// engine restart, makes the sum DROP. Exporting that sum directly would let rate()
// read the drop as a counter reset and manufacture phantom activity. Instead the
// poller accumulates only the POSITIVE per-tick deltas into this counter (see
// t2QueryDelta), so the exported series is monotonic and rate() reflects real query
// growth — robust to replica/tenant churn and engine restarts. Same `backend`
// label, present-when-exercised lifecycle, and stale-series pruning as
// backendT2HitRate.
var backendT2QueryTokensTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "inferencecache_backend_t2_query_tokens_total",
	Help: "Monotonic count of tier-2 (external offload) query tokens observed per CacheBackend (positive per-poll deltas of the engine's cumulative; drops from replica/tenant churn or restart are clamped out). Use rate() to gate on tier-2 activity.",
}, []string{"backend"})

func init() {
	crmetrics.Registry.MustRegister(backendT2HitRate, backendT2QueryTokensTotal)
}

// lastT2QueryTokens records the previous tick's aggregate cumulative query tokens
// per backend, so t2QueryDelta can emit only the positive increment. Touched only
// from the (single-goroutine, serial) poller refresh — never concurrently — so it
// needs no lock.
var lastT2QueryTokens = map[t2Key]int64{}

// t2QueryDelta returns the monotonic increment to add to backendT2QueryTokensTotal
// for `key` given the latest aggregate cumulative `current`, recording `current`
// as the new baseline. It returns 0 when the cumulative dropped (a replica/tenant
// draining out of the per-backend aggregate, or an engine restart) so the exported
// counter never goes backwards and rate() sees no phantom activity, and 0 on the
// first observation (establish the baseline without spiking from the pre-existing
// cumulative).
func t2QueryDelta(key t2Key, current int64) int64 {
	last, seen := lastT2QueryTokens[key]
	lastT2QueryTokens[key] = current
	if !seen || current <= last {
		return 0
	}
	return current - last
}

// t2SeriesTracked is the set of label-sets currently emitted for backendT2HitRate
// + backendT2QueryTokensTotal. Touched only from the (single-goroutine, serial)
// poller refresh — never concurrently — so it needs no lock.
var t2SeriesTracked = map[t2Key]struct{}{}

// reconcileBackendT2HitRateSeries prunes stale backendT2HitRate +
// backendT2QueryTokensTotal series after a poll tick. `exercised` is the set of
// backends whose series were set this tick; `present` is every CacheBackend
// currently listed; `tainted` is the set of namespaces the tick could not trust
// (transient API errors). A series is dropped when its backend is gone (not
// present), or is present and its namespace was NOT tainted yet had no replica
// reporting tier-2 queries this tick (drained out of the index snapshot, or never
// exercised). Backends in a tainted namespace are left untouched.
func reconcileBackendT2HitRateSeries(exercised, present map[t2Key]struct{}, tainted map[string]struct{}) {
	for k := range exercised {
		t2SeriesTracked[k] = struct{}{}
	}
	for k := range t2SeriesTracked {
		_, listed := present[k]
		_, isTainted := tainted[k.ns]
		_, ex := exercised[k]
		if !listed || (!isTainted && !ex) {
			backendT2HitRate.DeleteLabelValues(k.label())
			backendT2QueryTokensTotal.DeleteLabelValues(k.label())
			delete(lastT2QueryTokens, k)
			delete(t2SeriesTracked, k)
		}
	}
}

// resetBackendT2HitRateForTest clears the metrics + tracking state between tests.
func resetBackendT2HitRateForTest() {
	backendT2HitRate.Reset()
	backendT2QueryTokensTotal.Reset()
	t2SeriesTracked = map[t2Key]struct{}{}
	lastT2QueryTokens = map[t2Key]int64{}
}
