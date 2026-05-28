// Package runtime is the controller-owned runtime-adapter seam: the plug-point
// that keeps engine-specific cache wiring out of the core CacheBackend
// reconciler. Adapters implement [KVCacheRuntimeAdapter] (lifted from
// OEP-0010) to render the cache-server side and to inject engine/router pod
// configuration for a given (runtime, CacheBackend) pair. The [Registry]
// selects an adapter via each adapter's Supports method; admission can call
// [Registry.Select] to validate that a (runtime, backend) combination is
// supported.
package runtime
