// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	controlplaneapi "github.com/cachebox-project/inference-cache/internal/controlplaneapi"
	"github.com/cachebox-project/inference-cache/pkg/index"
)

// snapshotForControlPlane maps the index-owned domain snapshot to the private
// controller/server HTTP DTO. Keep this field-by-field: aliases would make a
// mutable index refactor an accidental wire-contract change.
func snapshotForControlPlane(src index.Snapshot) controlplaneapi.Snapshot {
	dst := controlplaneapi.Snapshot{
		TotalPrefixes: src.TotalPrefixes,
		HotPrefixes:   src.HotPrefixes,
	}
	if src.Replicas != nil {
		dst.Replicas = make([]controlplaneapi.ReplicaSnapshot, len(src.Replicas))
		for i, replica := range src.Replicas {
			dst.Replicas[i] = controlplaneapi.ReplicaSnapshot{
				ReplicaID:        replica.ReplicaID,
				Tenant:           replica.Tenant,
				CacheMemoryBytes: replica.CacheMemoryBytes,
				HitRate:          replica.HitRate,
				Pressure:         replica.Pressure,
				LastUpdate:       replica.LastUpdate,
				PrefixCount:      replica.PrefixCount,
				LastEventAt:      replica.LastEventAt,
				StatsReported:    replica.StatsReported,
				T2HitTokens:      replica.T2HitTokens,
				T2QueryTokens:    replica.T2QueryTokens,
			}
		}
	}
	if src.Tenants != nil {
		dst.Tenants = make([]controlplaneapi.TenantSnapshot, len(src.Tenants))
		for i, tenant := range src.Tenants {
			dst.Tenants[i] = controlplaneapi.TenantSnapshot{
				TenantID:        tenant.TenantID,
				IndexEntries:    tenant.IndexEntries,
				HitRate:         tenant.HitRate,
				HitRateReported: tenant.HitRateReported,
				MemoryUsed:      tenant.MemoryUsed,
			}
		}
	}
	return dst
}
