package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// TestTenantEvictionsMetricExposed is the metrics-output check for the public
// inferencecache_tenant_evictions_total counter: it must be registered in the
// server's /metrics registry, created lazily on the first eviction, and rendered
// with the documented (tenant_id, reason) labels. It scrapes the registry the
// same way /metrics does (promhttp over the per-Service registry) and matches
// the exposition text.
func TestTenantEvictionsMetricExposed(t *testing.T) {
	m := newServerMetrics()
	scrape := func() string {
		h := promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		return rec.Body.String()
	}

	// A labeled counter has no series until its first observation, so a server
	// that never evicts emits nothing for this metric.
	if got := scrape(); strings.Contains(got, "inferencecache_tenant_evictions_total") {
		t.Fatalf("metric present before any eviction:\n%s", got)
	}

	m.AddTenantEvictions("team-a", "over_entries", 3)

	// Prometheus renders labels alphabetically: reason before tenant_id.
	want := `inferencecache_tenant_evictions_total{reason="over_entries",tenant_id="team-a"} 3`
	if got := scrape(); !strings.Contains(got, want) {
		t.Fatalf("/metrics missing %q; body:\n%s", want, got)
	}
}

// TestIndexEvictionsMetricExposed is the /metrics-output check for the public
// inferencecache_index_evictions_total counter (cap/TTL sweeps): registered in
// the server registry, created lazily on first eviction, and rendered with the
// documented (algorithm, reason) labels. Guards against the collector being
// dropped from the registry or its public label names drifting — the index's
// fake-metrics unit test can't catch either. Mirrors the tenant-evictions check.
func TestIndexEvictionsMetricExposed(t *testing.T) {
	m := newServerMetrics()
	scrape := func() string {
		h := promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		return rec.Body.String()
	}

	if got := scrape(); strings.Contains(got, "inferencecache_index_evictions_total") {
		t.Fatalf("metric present before any eviction:\n%s", got)
	}

	m.AddIndexEvictions("lfu", "cap", 2)
	m.AddIndexEvictions("lru", "ttl", 1)

	// Labels render alphabetically: algorithm before reason.
	for _, want := range []string{
		`inferencecache_index_evictions_total{algorithm="lfu",reason="cap"} 2`,
		`inferencecache_index_evictions_total{algorithm="lru",reason="ttl"} 1`,
	} {
		if got := scrape(); !strings.Contains(got, want) {
			t.Fatalf("/metrics missing %q; body:\n%s", want, got)
		}
	}
}
