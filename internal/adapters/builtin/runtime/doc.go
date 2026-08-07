// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

// Package runtime contains the runtime-adapter implementations shipped by the
// controller binary. Public extension contracts remain in pkg/adapters/runtime;
// this internal package owns the concrete vLLM+LMCache, SGLang+LMCache, and
// SGLang+HiCache integrations and their engine wire rendering.
//
// SGLang adopted vLLM's KV-event wire wholesale: --kv-events-config drives a
// ZmqEventPublisher emitting the same msgspec array-like BlockStored /
// BlockRemoved / AllBlocksCleared tuples, so the shipped kvevent-subscriber
// binary decodes SGLang's stream unchanged — the only difference is the
// --hash-scheme=sglang tag that keeps SGLang prefixes in their own index
// domain (no cross-engine false hits against vLLM entries with identical
// prefix bytes). The engine-side LMCache *launch* surface differs from vLLM
// (--enable-lmcache + LMCACHE_USE_EXPERIMENTAL rather than
// --kv-transfer-config). Managed cache-server rendering belongs to
// internal/adapters/builtin/storage; subscriber-sidecar rendering remains shared
// in pkg/adapters/runtime/kvevent_subscriber.go, with common defaults in
// pkg/adapters/runtime/lmcache_shared.go.
package runtime
