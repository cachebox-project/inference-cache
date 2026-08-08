// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"github.com/go-logr/logr"
	"strings"
	"testing"
)

func TestDispatchRequiresAdapterRegistries(t *testing.T) {
	r := &CacheBackendReconciler{}
	_, err := r.dispatch(context.Background(), logr.Discard(), lmcacheBackend("cache", "ns1"))
	if err == nil || !strings.Contains(err.Error(), "adapter registries are not configured") {
		t.Fatalf("dispatch error = %v, want missing-registry error", err)
	}
}
