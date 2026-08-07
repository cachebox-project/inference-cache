// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

// Package render is the mutable-slot prompt rendering engine (the "wedge"): it turns
// templated prompts into stable cache keys so a gateway's cache-aware routing matches
// on real prompts. Used by inferencecache-server's RenderTemplate RPC; kept importable
// as a standalone library for the OSS standardization play.
package render
