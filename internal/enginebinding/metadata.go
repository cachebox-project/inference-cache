// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package enginebinding

import (
	"strconv"
	"strings"
)

const (
	// LabelLMCacheMPMetrics marks an engine Pod whose successfully injected
	// PodLocal LMCache native sidecar exposes Prometheus metrics. The optional
	// observability overlay selects this label and scrapes the sidecar directly;
	// it is deliberately not applied to legacy in-process connectors.
	LabelLMCacheMPMetrics = "inferencecache.io/lmcache-mp-metrics"

	// LabelLMCacheMPMetricsEnabled is the selector value used by the shipped
	// LMCache PodMonitor.
	LabelLMCacheMPMetricsEnabled = "true"

	// LabelLMCacheNodeLocalServer identifies a controller-owned NodeLocal MP
	// server Pod.
	LabelLMCacheNodeLocalServer = "inferencecache.io/lmcache-node-server"

	// LabelCacheBackendUID is the immutable, label-safe identity used to list
	// one CacheBackend's NodeLocal server Pods.
	LabelCacheBackendUID = "inferencecache.io/cache-backend-uid"

	// AnnotationNodeLocalOwner records the namespace/name of the CacheBackend
	// whose NodeLocal server configuration the Pod carries.
	AnnotationNodeLocalOwner = "inferencecache.io/node-local-owner"

	// AnnotationNodeLocalOwnerUID authenticates the name against the current
	// CacheBackend UID so delete/recreate races cannot claim a stale server.
	AnnotationNodeLocalOwnerUID = "inferencecache.io/node-local-owner-uid"

	// AnnotationNodeLocalGeneration records the exact CacheBackend generation
	// rendered into a NodeLocal server Pod.
	AnnotationNodeLocalGeneration = "inferencecache.io/node-local-generation"

	// AnnotationNodeLocalTargetNode records the engine-selected node for which
	// this server Pod was rendered. The Pod still uses scheduler-bound exact
	// node affinity rather than spec.nodeName so hostPort conflicts are checked.
	AnnotationNodeLocalTargetNode = "inferencecache.io/node-local-target-node"

	// AnnotationNodeLocalShmName records the controller-derived POSIX shared
	// memory object owned by this CacheBackend's NodeLocal server pool.
	AnnotationNodeLocalShmName = "inferencecache.io/node-local-shm-name"

	// AnnotationNodeLocalIdleSince records when the final active engine left a
	// node. The controller removes it when demand returns and deletes the server
	// only after the configured idle-retention window expires.
	AnnotationNodeLocalIdleSince = "inferencecache.io/node-local-idle-since"

	// AnnotationSkip lets an operator explicitly opt a pod out of injection.
	AnnotationSkip = "inferencecache.io/skip-inject"

	// AnnotationInjectedBy records the namespace/name of the CacheBackend that
	// wired an engine pod.
	AnnotationInjectedBy = "inferencecache.io/injected-by"

	// AnnotationInjectedByUID records the UID of the CacheBackend observed at
	// admission time and prevents stale name-only binding claims.
	AnnotationInjectedByUID = "inferencecache.io/injected-by-uid"

	// AnnotationInjectedGeneration records the CacheBackend generation whose
	// connector/server configuration was rendered into the immutable Pod. It
	// lets status distinguish current wiring from Pods that predate a spec
	// update and therefore require recreation.
	AnnotationInjectedGeneration = "inferencecache.io/injected-generation"

	// AnnotationInjectSkipped marks an intentional operator opt-out.
	AnnotationInjectSkipped = "inferencecache.io/inject-skipped"

	// InjectSkippedReasonSkipAnnotation is the stable opt-out reason value.
	InjectSkippedReasonSkipAnnotation = "skip-inject-annotation"
)

// SkipAnnotationOptsOut reports whether an annotation value disables
// injection. ParseBool truthy values and unrecognized non-empty values opt out;
// explicit false values and common false synonyms leave injection enabled.
func SkipAnnotationOptsOut(value string) bool {
	if value == "" {
		return false
	}
	if parsed, err := strconv.ParseBool(value); err == nil {
		return parsed
	}
	switch strings.ToLower(value) {
	case "no", "off", "disable", "disabled":
		return false
	default:
		return true
	}
}
