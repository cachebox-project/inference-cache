// Package sglang holds the controller-side runtime adapters that wire SGLang
// engine pods to LMCache or native HiCache.
// It is the SGLang sibling of the in-tree vLLM+LMCache adapter: a separate
// package gated on the SGLang runtime id and registered by
// internal/adapters/builtin.
//
// Owner: the controller. This package imports its parent
// pkg/adapters/runtime for the [runtime.KVCacheRuntimeAdapter] interface and
// the [runtime.RuntimeID] constants, so it cannot be registered inside
// runtime.NewCoreRegistry without an import cycle. The built-in composition
// root adds them once for every production and nil-fallback path (see
// [NewAdapter] and [NewHiCacheAdapter]).
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
// package. Managed cache-server rendering belongs to
// internal/adapters/builtin/storage; subscriber-sidecar helpers remain shared with
// the vLLM adapter in pkg/adapters/runtime/lmcache_shared.go.
package sglang
