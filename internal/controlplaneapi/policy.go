// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controlplaneapi

import "time"

// PolicyPropagationVersion identifies the current /policy snapshot schema.
const PolicyPropagationVersion = 7

// PolicyMinimumAcceptedVersion is the oldest /policy schema understood by the
// current server. Older additive schemas are normalized by the server.
const PolicyMinimumAcceptedVersion = 3

// Shared policy defaults must be identical on both sides of the /policy wire.
const (
	DefaultMinimumMatchedTokens   int32   = 64
	DefaultRoutingFloorScore      float32 = 0.1
	DefaultEnableChainMatching            = true
	DefaultRequireChain                   = false
	DefaultEnableTenantHot                = true
	DefaultAffinityRoutingEnabled         = true
)

// ResolvedPolicy is the flattened CachePolicy shape enforced by the server.
// The controller is responsible for converting CRD values into this wire type.
type ResolvedPolicy struct {
	Namespace            string                  `json:"namespace"`
	EvictionTTL          time.Duration           `json:"evictionTTL,omitempty"`
	MinimumPrefixTokens  int32                   `json:"minimumPrefixTokens,omitempty"`
	MinimumMatchedTokens int32                   `json:"minimumMatchedTokens,omitempty"`
	RoutingFloorScore    *float32                `json:"routingFloorScore,omitempty"`
	LookupTimeoutMs      int32                   `json:"lookupTimeoutMs,omitempty"`
	AffinityRouting      *bool                   `json:"affinityRouting,omitempty"`
	Eviction             string                  `json:"eviction,omitempty"`
	Strategy             *ResolvedLookupStrategy `json:"strategy,omitempty"`
}

// ResolvedLookupStrategy carries the server-enforced LookupRoute strategy
// gates flattened from CachePolicy.spec.strategy.
type ResolvedLookupStrategy struct {
	EnableChainMatching *bool `json:"enableChainMatching,omitempty"`
	RequireChain        *bool `json:"requireChain,omitempty"`
	EnableTenantHot     *bool `json:"enableTenantHot,omitempty"`
}

// ResolvedTenant carries the CacheTenant fields enforced by the server.
type ResolvedTenant struct {
	TenantID        string `json:"tenantID"`
	MaxIndexEntries int64  `json:"maxIndexEntries"`
	IsolationMode   string `json:"isolationMode,omitempty"`
}

// PolicySnapshot is the complete replace-on-write payload sent to /policy.
type PolicySnapshot struct {
	Version  int              `json:"version"`
	Policies []ResolvedPolicy `json:"policies"`
	Tenants  []ResolvedTenant `json:"tenants,omitempty"`
}
