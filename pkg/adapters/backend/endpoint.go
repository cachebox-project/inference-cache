// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"unicode"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// ValidateExternalEndpoint validates the bare host:port endpoint accepted by
// the selected remote-storage provider.
func ValidateExternalEndpoint(provider cachev1alpha1.CacheBackendRemoteStorageProvider, endpoint string) error {
	if provider != cachev1alpha1.CacheBackendRemoteStorageProviderRedis {
		return fmt.Errorf("remote-storage provider %q has no endpoint protocol", provider)
	}
	value := strings.TrimSpace(endpoint)
	if value == "" {
		return fmt.Errorf("endpoint is empty")
	}
	if strings.Contains(value, "://") {
		return fmt.Errorf("endpoint schemes are not supported for remoteStorage.provider=%s; use bare host:port", provider)
	}
	if strings.ContainsFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return fmt.Errorf("endpoint must not contain whitespace or control characters")
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf("endpoint must be a non-empty host and port (for example redis.example.com:6379)")
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return fmt.Errorf("endpoint port %q must be an integer in 1-65535", port)
	}
	return nil
}
