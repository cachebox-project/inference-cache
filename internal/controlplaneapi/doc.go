// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

// Package controlplaneapi defines the private HTTP wire contracts shared by
// the inference-cache controller and server binaries.
//
// These types are not a supported external Go API. Their JSON representation
// is the compatibility boundary for the controller-to-server /policy and
// /probe endpoints.
package controlplaneapi
