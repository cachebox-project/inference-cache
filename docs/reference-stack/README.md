# vLLM and SGLang typed LMCache MP reference stack

This directory demonstrates the current inference-cache integration: a typed
`CacheBackend` selects an inference-engine pod and the operator injects an
LMCache multiprocess native sidecar plus the engine connector configuration.
An optional Redis tier provides explicit cross-Pod sharing.

The repository does not own or replace the engine image. Use a digest-pinned
vLLM or SGLang image containing a connector/package compatible with the pinned
LMCache server. Normal engine startup is the authoritative compatibility check.

> The GPU manifests require Kubernetes 1.29 or later for native sidecars and an
> NVIDIA GPU. The CPU-only manifest validates engine prefix caching and KV-event
> decoding, but does not run the LMCache MP data plane.

## Layout

| Path | Purpose |
|---|---|
| [`VERSIONS.md`](VERSIONS.md) | Image and validation evidence; read before substituting engine images. |
| [`GPU-RUNBOOK.md`](GPU-RUNBOOK.md) | GPU sizing and operational notes. |
| [`manifests/deployment.yaml`](manifests/deployment.yaml) | vLLM + typed host-only LMCache MP. |
| [`manifests/sglang-lmcache/`](manifests/sglang-lmcache/) | SGLang + typed LMCache MP + explicit external Redis. |
| [`manifests/cpu-local/`](manifests/cpu-local/) | CPU-only engine/event check without LMCache. |
| [`scripts/`](scripts/) | Event subscriber, prefix-hit test, and MP-only install smoke. |

There is no Helm values reference in this phase. The repository has not
validated an upstream chart API that can faithfully express the operator-owned
native sidecar contract, so inventing a chart mapping would be unsafe.

## Install the operator

Install the digest-pinned manifest attached to the selected inference-cache
release before creating the reference `CacheBackend`:

```bash
RELEASE_TAG=vX.Y.Z
kubectl apply -f "inference-cache-${RELEASE_TAG}.yaml"
kubectl -n inference-cache-system wait \
  --for=condition=Available deployment --all --timeout=180s
```

Use your normal release installation instead in a managed cluster. The
mutating webhook must be available before the engine Deployment is created;
otherwise admission fails open and the existing pod remains unwired until it is
recreated.

## vLLM GPU path

The manifest contains a deliberately non-pullable engine-image placeholder.
Replace it with a compatible digest-pinned image; do not change the
`CacheBackend` server image to disguise an incompatible engine package.

```bash
kind create cluster --name inference-cache-substrate --config kind/cluster.yaml
helm repo add nvdp https://nvidia.github.io/k8s-device-plugin
helm install nvdp nvdp/nvidia-device-plugin -n kube-system

kubectl apply -f manifests/namespace.yaml
kubectl -n cache-substrate create secret generic hf-token \
  --from-literal=token="$HF_TOKEN"

# Install the operator, replace the engine placeholder, then create the typed
# CacheBackend and matching Deployment from the same YAML stream.
kubectl apply -f manifests/deployment.yaml -f manifests/service.yaml
kubectl -n cache-substrate rollout status deploy/vllm-lmcache-llama-8b --timeout=20m
```

The vLLM reference is host-only: `status.remoteStorage` is intentionally absent.
For cross-Pod sharing, explicitly select Redis as shown by the SGLang reference
or [`config/samples/cachebackend-lmcache.yaml`](../../config/samples/cachebackend-lmcache.yaml).
The removed `LMCacheServer` and legacy IP-wired Mooncake provider shapes are not
automatically translated because doing so would silently change L3 and sharing
semantics. Mooncake remains future typed MP L2 work.

## Verify traffic and KV events

```bash
pip install -r scripts/requirements.txt
python scripts/kv_events_subscriber.py \
  --endpoint tcp://localhost:30557 --topic kv-events --max 200 --json

./scripts/prefix_cache_hit_test.sh
```

A successful run shows a prefix-cache counter increase on the repeated prefix
and `BlockStored` events. Event captures contain hashes and counts, not prompt
text. The Service deliberately does not expose MP control/data ports.

## SGLang GPU path

See [`manifests/sglang-lmcache/README.md`](manifests/sglang-lmcache/README.md).
That manifest uses the same typed `CacheBackend` contract and makes Redis an
explicit external L3 choice.

## CPU-only event check

This path has no LMCache offload and needs no GPU. Match the CPU image tag to
the host architecture before applying it.

```bash
kind create cluster --name inference-cache-substrate --config kind/cluster.yaml
kubectl apply -f manifests/namespace.yaml -f manifests/cpu-local/deployment.yaml
kubectl -n cache-substrate rollout status deploy/vllm-cpu-sanity --timeout=30m

pip install -r scripts/requirements.txt
python scripts/kv_events_subscriber.py --endpoint tcp://localhost:30557 --topic kv-events &
MODEL=Qwen/Qwen2.5-0.5B-Instruct ./scripts/prefix_cache_hit_test.sh
```

Without a cluster or image pull, validate only the consumer and redaction path:

```bash
python scripts/kv_events_synthetic_publisher.py --bind 'tcp://*:5557' &
python scripts/kv_events_subscriber.py --endpoint tcp://localhost:5557 --max 4
python scripts/test_kv_events.py
```

The `default_install_smoke.sh` gate installs the current MP-only schema, checks
typed Pod admission and managed Redis, and re-applies the bundle to cover the
in-place alpha upgrade path without starting a real engine.

## Teardown

```bash
kind delete cluster --name inference-cache-substrate
```
