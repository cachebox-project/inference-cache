---
title: "Tasks"
linkTitle: "Tasks"
weight: 6
description: >
  Step-by-step guides for common inference-cache operations.
no_list: true
---

These guides walk through the operations you perform against a running inference-cache
install — from the first `CacheBackend` to tuning, isolation, and monitoring.

- **[Deploy a CacheBackend](/docs/tasks/deploy-a-cache-backend/)** — the five-minute
  quickstart: the minimum `CacheBackend`, what it wires up, and how to read its readiness.
- **[Bind an engine](/docs/tasks/bind-an-engine/)** — the selector → webhook → injection
  lifecycle, the reserved args/env you cannot override, and its failure modes.
- **[Tune lookup and eviction](/docs/tasks/tune-lookup-and-eviction/)** — worked
  `CachePolicy` examples for match floors, score floors, timeouts, and eviction.
- **[Isolate tenants](/docs/tasks/isolate-tenants/)** — identity, entry-count quota, and the
  recommended pattern for true per-tenant byte isolation.
- **[Monitor the cache plane](/docs/tasks/monitor-the-cache-plane/)** — `kubectl` surfaces,
  metrics, and the `doctor` pre-flight check.
