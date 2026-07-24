// Package engine is the KV-event subscriber. It runs as a sidecar next to a vLLM
// engine replica, subscribes to the engine's KV cache events over ZMQ, decodes
// them, and reports cache state to the inferencecache-server over gRPC.
//
// Two independent paths share one gRPC client:
//   - Event path: ZMQ → EventBatch → ReportCacheState (prefix adds) +
//     PublishEvent (removals/clears), debounced on a short window.
//   - Stats path: engine load → a statsScraper → StatsReporter → ReportCacheState
//     (stats-only CacheStateUpdate populating cacheMemoryBytes / hitRate / pressure
//     on its own cadence, default ~10s). The load source is selectable: an HTTP GET
//     against the engine's Prometheus /metrics (MetricsScraper, the default), or the
//     VllmEngine GetLoads gRPC RPC (GRPCLoadsScraper) for gRPC engines that expose
//     no /metrics endpoint.
//
// Metadata only — never KV tensors or prompt text. Fail-soft on both paths:
// neither a ZMQ drop nor a scrape failure can stall the engine. The package is
// built into the kvevent-subscriber binary (cmd/kvevent-subscriber).
package engine
