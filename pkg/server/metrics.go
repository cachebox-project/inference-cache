package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// metricNamespace prefixes every metric this server emits, per the public
// schema in the tech spec (§4.3): all names are `inferencecache_*`.
const metricNamespace = "inferencecache"

// serverMetrics holds the Prometheus registry and collectors for one Service.
//
// We use a per-Service registry rather than the global default one so that (a)
// the server binary's metrics stay isolated from the controller binary, which
// registers to controller-runtime's registry, and (b) tests can construct
// multiple Services without "duplicate metrics collector registration" panics.
//
// The full §4.3 metric schema (hit_rate, lookup latency, index_entries, …) is
// owned by the standalone metric-schema work (F3); B5 ships only the endpoint
// plus the documented liveness gauge `inferencecache_server_up`.
type serverMetrics struct {
	registry *prometheus.Registry
	up       prometheus.Gauge
}

func newServerMetrics() *serverMetrics {
	up := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Name:      "server_up",
		Help:      "1 if the cache policy server is serving requests, 0 otherwise.",
	})

	registry := prometheus.NewRegistry()
	registry.MustRegister(
		up,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return &serverMetrics{registry: registry, up: up}
}
