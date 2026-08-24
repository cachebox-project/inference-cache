// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

func TestValidateOptionsRequiresDigestPinnedCleanupImage(t *testing.T) {
	valid := defaultOptions()
	valid.nodeLocalCleanupImage = "registry.example/inference-cache-shm-cleanup@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := validateOptions(valid); err != nil {
		t.Fatalf("valid options: %v", err)
	}

	for _, image := range []string{"", "registry.example/inference-cache-shm-cleanup:latest"} {
		opts := defaultOptions()
		opts.nodeLocalCleanupImage = image
		if err := validateOptions(opts); err == nil {
			t.Fatalf("cleanup image %q was accepted without a sha256 digest", image)
		}
	}
}
