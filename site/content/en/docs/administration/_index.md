---
title: "Administration"
linkTitle: "Administration"
weight: 7
description: >
  Operating inference-cache at scale — observability, sizing, security, and troubleshooting.
no_list: true
---

This section covers the operational concerns of running inference-cache in production.

## Monitoring & operations

### [Observability & Alerts]({{< relref "/docs/administration/observability-and-alerts/" >}})

The opt-in Prometheus alert bundle, what each alert means and when it fires, and the
scraping you must configure for the server, controller, and injected LMCache MP sidecars.

### [Index sizing]({{< relref "/docs/administration/index-sizing/" >}})

How the in-memory index consumes memory, the pod-budget table, the sizing formula, and the
levers (TTL, eviction, quota) for keeping it in bounds.

## Security

### [gRPC TLS]({{< relref "/docs/administration/grpc-tls/" >}})

The locked TLS design, why gRPC is plaintext by default, and how to enable the opt-in TLS
overlay.

## Troubleshooting

### [Troubleshooting]({{< relref "/docs/administration/troubleshooting/" >}})

The `CacheBackend` readiness runbook (per condition and reason) and the `inferencecache
doctor` check catalog.
