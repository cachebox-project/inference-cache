// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

// Package index implements the inference-cache server's mutable cache-state
// index, populated from engine KV events and queried by LookupRoute.
//
// The index engine (the in-memory store, ingestion, eviction, ranking) runs only
// in the server binary. Snapshot types are index-owned domain values; the server
// maps them explicitly to the controller-facing HTTP contract in
// internal/controlplaneapi.
//
// The package is intentionally decoupled from the gRPC/proto layer: callers
// translate wire messages into its domain types. Its contents are soft state;
// losing them causes cache misses, never wrong answers.
package index
