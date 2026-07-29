---
title: "Installation"
linkTitle: "Installation"
weight: 2
description: >
  Installing inference-cache into a Kubernetes cluster
---

<!-- toc -->
- [Before you begin](#before-you-begin)
- [1. Install cert-manager](#1-install-cert-manager)
- [2. Install inference-cache](#2-install-inference-cache)
- [3. Verify the install](#3-verify-the-install)
- [4. (Optional) Install the observability bundle](#4-optional-install-the-observability-bundle)
- [Local development cluster](#local-development-cluster)
- [Uninstall](#uninstall)
<!-- /toc -->

## Before you begin

inference-cache runs two control-plane workloads (`inferencecache-controller-manager` and
`inference-cache-server`) in the `inference-cache-system` namespace. Make sure the
following are true:

- A Kubernetes cluster is running (v1.27+ recommended) and `kubectl` can reach it.
- You can apply cluster-scoped resources (CRDs, RBAC, webhook configurations).
- **cert-manager v1.0+** is installed — the controller serves admission webhooks over TLS,
  and the default install provisions the webhook serving certificate through cert-manager.

## 1. Install cert-manager

The controller serves seven admission webhook entries over TLS across two Kubernetes
webhook configurations: mutating defaults for `CacheBackend`, `CachePolicy`, and
`CacheTenant`; validation for those same three resources; and a mutating Pod webhook that
auto-injects engine configuration into pods matching a `CacheBackend`'s
`spec.engineSelector`. The default install provisions a self-signed `Issuer` and a
`Certificate` for the webhook serving cert and relies on cert-manager's
`cert-manager.io/inject-ca-from` annotation to inject the CA bundle into both webhook
configurations.

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.1/cert-manager.yaml
kubectl -n cert-manager wait --for=condition=Available deployment --all --timeout=180s
```

## 2. Install inference-cache

The default Kustomize overlay brings up both control-plane components:

```bash
git clone https://github.com/cachebox-project/inference-cache.git
cd inference-cache
kubectl apply -k config/default
```

This creates:

- The CRDs (`CacheBackend`, `CachePolicy`, `CacheTenant`, `CacheIndex`, `PromptTemplate`,
  `PDTopology`).
- `inferencecache-controller-manager` — the reconciler + admission webhooks.
- `inference-cache-server` — the gRPC policy server plus the HTTP probe/metrics surface,
  fronted by a `ClusterIP` Service named `inference-cache-server` with ports
  `grpc:9090`, `http:8080`, and `snapshot:8081`.

{{% alert title="Note" color="info" %}}
The server **fails closed by default.** In production it must be started with
`--allowed-controller-sa` set so that its controller-facing `/snapshot`, `/policy`, and
`/probe` endpoints are authenticated. The default overlay wires this for you; if you run
the server binary by hand for local development, pass `--insecure-disable-auth` explicitly.
{{% /alert %}}

gRPC on `:9090` is **plaintext by default** — see
[gRPC TLS]({{< relref "/docs/administration/grpc-tls/" >}}) for the opt-in TLS overlay and why the default
is plaintext.

## 3. Verify the install

```bash
kubectl -n inference-cache-system wait --for=condition=Available deployment --all --timeout=180s
kubectl get cacheindex cluster-default -o yaml
```

Once both pods are `Ready`, the cluster-scoped, controller-maintained `CacheIndex`
singleton `cluster-default` reports live cluster-wide cache state. On a fresh install with
no engine workload it is empty — that is expected.

## 4. (Optional) Install the observability bundle

A Prometheus alert bundle for the operational silent-failure patterns this system has hit
in production ships under `config/observability/`. It is **not** part of `config/default` —
the alerts are opt-in so that installs without prometheus-operator CRDs are not affected by
an unknown `apiVersion`.

For prometheus-operator / kube-prometheus installs:

```bash
kubectl apply -k config/observability
```

This ships three resources: a `ServiceMonitor` (scrapes `inference-cache-server:8080`), a
`PodMonitor` (scrapes the controller pod's `:8080` — required for the controller-side
alerts to have a series to evaluate), and a `PrometheusRule` carrying the alerts.

{{% alert title="Caveat — Prometheus Operator selectors" color="warning" %}}
All three custom resources carry example labels (`prometheus: k8s`) that match the upstream
kube-prometheus stack. If your `Prometheus` custom resource's selectors use a different
label set (for example `release: my-prom` from the `kube-prometheus-stack` Helm chart),
`kubectl apply -k` succeeds but Prometheus silently ignores the resources. See
[Observability & Alerts]({{< relref "/docs/administration/observability-and-alerts/" >}}) for the full
discussion, and for the extra engine-pod scrape the tier-2 alert needs.
{{% /alert %}}

## Local development cluster

To create a kind cluster for controller development:

```bash
make dev-cluster
```

By default this creates or reuses a cluster named `inference-cache`. Override the name and
node image with:

```bash
make dev-cluster KIND_CLUSTER=cache-dev KIND_NODE_IMAGE=kindest/node:v1.31.0
```

You do **not** need a GPU to exercise the substrate's engine-config and KV-event path —
vLLM's v1 engine and its KV-cache event publisher both run on CPU. See the reference stack
under `docs/reference-stack/` in the repository for a CPU-only kind manifest.

## Uninstall

```bash
kubectl delete -k config/default
```

If you installed the observability bundle, remove it too:

```bash
kubectl delete -k config/observability
```
