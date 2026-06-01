package server

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/cachebox-project/inference-cache/pkg/server/auth"
)

// metricNamespace is the prefix for this project's own metrics (tech spec
// §4.3): every inference-cache metric name is `inferencecache_*`. The /metrics
// registry additionally exposes the standard Go runtime and process collectors
// (`go_*`, `process_*`) — conventional for Prometheus exporters and useful for
// ops — which are not part of the §4.3 schema.
const metricNamespace = "inferencecache"

// serverMetrics holds the Prometheus registry and collectors for one Service.
//
// We use a per-Service registry rather than the global default one so that (a)
// the server binary's metrics stay isolated from the controller binary, which
// registers to controller-runtime's registry, and (b) tests can construct
// multiple Services without "duplicate metrics collector registration" panics.
//
// The registry exposes the liveness gauge `inferencecache_server_up` plus the
// metrics wired since B5: `index_entries`, the `lookup_route_*` call/latency
// series, `snapshot_auth_total`, and `tenant_evictions_total` (see
// docs/reference/metrics.md). The full §4.3 metric schema is still owned by the
// standalone metric-schema work (F3); these are the subset shipped so far.
type serverMetrics struct {
	registry        *prometheus.Registry
	up              prometheus.Gauge
	indexEntries    *prometheus.GaugeVec
	lookupCalls     *prometheus.CounterVec
	lookupLatency   *prometheus.HistogramVec
	tenantEvictions *prometheus.CounterVec
	snapshotAuth    *prometheus.CounterVec
}

func newServerMetrics() *serverMetrics {
	up := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Name:      "server_up",
		Help:      "1 if the cache policy server is serving requests, 0 otherwise.",
	})
	indexEntries := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Name:      "index_entries",
		Help:      "Distinct prefix entries currently held in the cache index, per model.",
	}, []string{"model"})
	lookupCalls := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricNamespace,
		Name:      "lookup_route_calls_total",
		Help:      "LookupRoute calls by model, reason_code, and whether a hint was returned.",
	}, []string{"model", "reason_code", "hint_used"})
	lookupLatency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricNamespace,
		Name:      "lookup_route_latency_seconds",
		Help:      "Server-side LookupRoute latency, per model.",
		// Cache-path lookups target sub-millisecond; bucket from 100µs up.
		Buckets: []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1},
	}, []string{"model"})
	tenantEvictions := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricNamespace,
		Name:      "tenant_evictions_total",
		Help:      "Index prefixes evicted to enforce a CacheTenant quota, by tenant and reason.",
	}, []string{"tenant_id", "reason"})
	snapshotAuth := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricNamespace,
		Name:      "snapshot_auth_total",
		Help:      "Authentication outcomes for the internal /snapshot endpoint (ok|unauth|forbidden|error).",
	}, []string{"result"})

	registry := prometheus.NewRegistry()
	registry.MustRegister(
		up,
		indexEntries,
		lookupCalls,
		lookupLatency,
		tenantEvictions,
		snapshotAuth,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return &serverMetrics{
		registry:        registry,
		up:              up,
		indexEntries:    indexEntries,
		lookupCalls:     lookupCalls,
		lookupLatency:   lookupLatency,
		tenantEvictions: tenantEvictions,
		snapshotAuth:    snapshotAuth,
	}
}

// RecordAuthResult is invoked once per /snapshot request by the auth
// middleware. Satisfies auth.ResultRecorder.
func (m *serverMetrics) RecordAuthResult(result auth.Result) {
	m.snapshotAuth.WithLabelValues(string(result)).Inc()
}

// SetIndexEntries reports the live prefix-entry count for a model. It satisfies
// index.Metrics so the index can push counts as it mutates.
func (m *serverMetrics) SetIndexEntries(model string, entries int) {
	m.indexEntries.WithLabelValues(model).Set(float64(entries))
}

// AddTenantEvictions records n quota-driven entry evictions for a tenant.
// Satisfies index.Metrics.
func (m *serverMetrics) AddTenantEvictions(tenantID, reason string, n int) {
	m.tenantEvictions.WithLabelValues(tenantID, reason).Add(float64(n))
}

// observeLookup records one LookupRoute call's outcome and latency.
func (m *serverMetrics) observeLookup(model, reasonCode string, hintUsed bool, d time.Duration) {
	m.lookupCalls.WithLabelValues(model, reasonCode, strconv.FormatBool(hintUsed)).Inc()
	m.lookupLatency.WithLabelValues(model).Observe(d.Seconds())
}
