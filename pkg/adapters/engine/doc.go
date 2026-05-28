// Package engine is the KV-event subscriber. It runs as a sidecar next to a vLLM
// engine replica, subscribes to the engine's KV cache events over ZMQ, decodes
// them, and reports cache state to the inferencecache-server over gRPC
// (ReportCacheState for adds, PublishEvent for evictions/clears). Metadata only —
// never KV tensors or prompt text. It is built into the kvevent-subscriber binary
// (cmd/kvevent-subscriber).
package engine
