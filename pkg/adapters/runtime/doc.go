// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

// Package runtime is the controller-owned runtime-adapter seam: the plug-point
// that keeps engine-specific cache wiring out of the core CacheBackend
// reconciler. Adapters implement [KVCacheRuntimeAdapter] (lifted from
// OEP-0010) to inject engine/router pod configuration for a given (runtime,
// CacheBackend) pair. Concrete adapters shipped by the controller live under
// internal/adapters/builtin/runtime; this package remains the designated seam
// for build-time out-of-tree adapters. The source contract is pre-stable and
// currently has no external consumers; custom controller forks must pin a
// repository revision and update their adapters when this interface changes.
// The [Registry] selects an adapter via each
// adapter's Supports method; the required SupportsBinding and injection methods
// consume the structured remote-storage binding, where nil means host-only.
package runtime
