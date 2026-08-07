// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controlplaneapi

import "time"

// Snapshot is the controller-facing representation returned by GET /snapshot.
// Its JSON tags are a private compatibility contract between independently
// rolled controller and server binaries.
type Snapshot struct {
	Replicas      []ReplicaSnapshot `json:"replicas"`
	Tenants       []TenantSnapshot  `json:"tenants"`
	TotalPrefixes int               `json:"totalPrefixes"`
	HotPrefixes   int               `json:"hotPrefixes"`
}

// ReplicaSnapshot is the /snapshot representation of one replica's latest
// aggregate state. Presence bits preserve the distinction between an observed
// zero and a value that an older or not-yet-reporting producer omitted.
type ReplicaSnapshot struct {
	ReplicaID        string    `json:"replicaId"`
	Tenant           string    `json:"tenant,omitempty"`
	CacheMemoryBytes int64     `json:"cacheMemoryBytes"`
	HitRate          float32   `json:"hitRate"`
	Pressure         float32   `json:"pressure"`
	LastUpdate       time.Time `json:"lastUpdate"`
	PrefixCount      int       `json:"prefixCount"`
	LastEventAt      time.Time `json:"lastEventAt,omitempty"`
	StatsReported    bool      `json:"statsReported,omitempty"`
	T2HitTokens      int64     `json:"t2HitTokens,omitempty"`
	T2QueryTokens    int64     `json:"t2QueryTokens,omitempty"`
}

// TenantSnapshot is the /snapshot representation of one tenant's aggregate
// footprint. MemoryUsed is deprecated and intentionally remains a required
// zero-valued JSON key for controller/server skew compatibility.
type TenantSnapshot struct {
	TenantID        string  `json:"tenantId"`
	IndexEntries    int64   `json:"indexEntries"`
	HitRate         float32 `json:"hitRate"`
	HitRateReported bool    `json:"hitRateReported,omitempty"`
	// Deprecated: always 0; read ReplicaSnapshot.CacheMemoryBytes instead.
	MemoryUsed int64 `json:"memoryUsed"`
}
