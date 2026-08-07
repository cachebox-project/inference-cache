// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"reflect"
	"testing"
	"time"

	controlplaneapi "github.com/cachebox-project/inference-cache/internal/controlplaneapi"
	"github.com/cachebox-project/inference-cache/pkg/index"
)

func TestSnapshotForControlPlaneMapsEveryField(t *testing.T) {
	lastUpdate := time.Unix(1_700_000_000, 0).UTC()
	lastEvent := lastUpdate.Add(time.Minute)
	src := index.Snapshot{
		TotalPrefixes: 11,
		HotPrefixes:   2,
		Replicas: []index.ReplicaSnapshot{{
			ReplicaID:        "replica-a",
			Tenant:           "tenant-a",
			CacheMemoryBytes: 4096,
			HitRate:          0.75,
			Pressure:         0.25,
			LastUpdate:       lastUpdate,
			PrefixCount:      7,
			LastEventAt:      lastEvent,
			StatsReported:    true,
			T2HitTokens:      600,
			T2QueryTokens:    1000,
		}},
		Tenants: []index.TenantSnapshot{{
			TenantID:        "tenant-a",
			IndexEntries:    11,
			HitRate:         0.75,
			HitRateReported: true,
			MemoryUsed:      0,
		}},
	}

	want := controlplaneapi.Snapshot{
		TotalPrefixes: 11,
		HotPrefixes:   2,
		Replicas: []controlplaneapi.ReplicaSnapshot{{
			ReplicaID:        "replica-a",
			Tenant:           "tenant-a",
			CacheMemoryBytes: 4096,
			HitRate:          0.75,
			Pressure:         0.25,
			LastUpdate:       lastUpdate,
			PrefixCount:      7,
			LastEventAt:      lastEvent,
			StatsReported:    true,
			T2HitTokens:      600,
			T2QueryTokens:    1000,
		}},
		Tenants: []controlplaneapi.TenantSnapshot{{
			TenantID:        "tenant-a",
			IndexEntries:    11,
			HitRate:         0.75,
			HitRateReported: true,
			MemoryUsed:      0,
		}},
	}

	if got := snapshotForControlPlane(src); !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot mapping = %+v, want %+v", got, want)
	}
	if got := snapshotForControlPlane(index.Snapshot{}); got.Replicas != nil || got.Tenants != nil {
		t.Fatalf("zero snapshot changed nil-slice wire semantics: %+v", got)
	}
}
