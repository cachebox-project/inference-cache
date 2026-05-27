// Package index is part of inferencecache-server: the cluster cache-state aggregator
// (the CacheIndex), populated from engine KV events and queried by LookupRoute.
// Observability and routing input only — not a routing-decision substrate.
//
// The index engine (the in-memory store, ingestion, eviction, ranking) runs only
// in the server binary. The Snapshot* types are the one deliberate exception:
// they are the read-only wire contract for the server's /snapshot HTTP endpoint
// and are intentionally shared — the controller's CacheIndex poller imports them
// to decode that endpoint when reflecting the aggregate into CacheIndex status.
package index
