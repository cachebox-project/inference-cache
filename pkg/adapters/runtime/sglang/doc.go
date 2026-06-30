// Package sglang holds the controller-side runtime adapter that wires SGLang
// engine pods to a managed LMCache CacheBackend — the (sglang, LMCache) pair.
// It is the SGLang sibling of the in-tree vLLM+LMCache adapter
// (pkg/adapters/runtime) and the External passthrough adapter
// (pkg/adapters/runtime/external): a separate package, gated on the SGLang
// runtime id, registered alongside the others in cmd/controller and both
// admission/pod webhooks.
//
// Owner: the controller. Like external, this package imports its parent
// pkg/adapters/runtime for the [runtime.KVCacheRuntimeAdapter] interface and
// the [runtime.RuntimeID] constants, so it cannot be registered inside
// runtime.DefaultRegistry without an import cycle — the three production wiring
// sites add it explicitly (see [NewAdapter]).
//
// SGLang adopted vLLM's KV-event wire wholesale: --kv-events-config drives a
// ZmqEventPublisher emitting the same msgspec array-like BlockStored /
// BlockRemoved / AllBlocksCleared tuples, so the shipped kvevent-subscriber
// binary decodes SGLang's stream unchanged — the only difference is the
// --hash-scheme=sglang tag that keeps SGLang prefixes in their own index
// domain (no cross-engine false hits against vLLM entries with identical
// prefix bytes). The engine-side LMCache *launch* surface differs from vLLM
// (--enable-lmcache + LMCACHE_USE_EXPERIMENTAL rather than
// --kv-transfer-config), and that wire lives in the internal enginewire
// package; the lmcache-server and subscriber-sidecar rendering is shared with
// the vLLM adapter (pkg/adapters/runtime/lmcache_shared.go).
package sglang
