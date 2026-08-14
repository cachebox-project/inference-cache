// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"testing"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func TestEffectiveProviderDefaultsHandleNilCache(t *testing.T) {
	if got := effectiveProviderResources(nil); got != nil {
		t.Fatalf("effectiveProviderResources(nil) = %+v, want nil", got)
	}
	if got := defaultServerResources(nil); len(got.Requests) != 0 || len(got.Limits) != 0 {
		t.Fatalf("defaultServerResources(nil) = %+v, want empty requirements", got)
	}
	if got := effectiveProviderImage(nil, cachev1alpha1.CacheBackendRemoteStorageProviderRedis, "fallback:image"); got != "fallback:image" {
		t.Fatalf("effectiveProviderImage(nil) = %q, want fallback:image", got)
	}
}
