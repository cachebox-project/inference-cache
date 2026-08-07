// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

// Package index is part of inferencecache-server: the cluster cache-state aggregator
// (the CacheIndex), populated from engine KV events and queried by LookupRoute.
// Observability and routing input only — not a routing-decision substrate.
//
// The index engine (the in-memory store, ingestion, eviction, ranking) runs only
// in the server binary. Snapshot types are index-owned domain values; the server
// maps them explicitly to the controller-facing HTTP contract in
// internal/controlplaneapi.
package index
