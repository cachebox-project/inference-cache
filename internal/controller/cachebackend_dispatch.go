// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
)

// dispatch routes a CacheBackend by integration mode and effective remote
// storage ownership. EventsOnly and canonical host-only configurations shed
// managed provider workloads; External storage mirrors its configured endpoint
// to status; Managed Redis storage is rendered by the selected provider
// adapter. Unsupported combinations also shed any
// previously managed workload.
func (r *CacheBackendReconciler) dispatch(ctx context.Context, logger logr.Logger, backend *cachev1alpha1.CacheBackend) (ctrl.Result, error) {
	if r.Registry == nil || r.BackendRegistry == nil {
		return ctrl.Result{}, fmt.Errorf("adapter registries are not configured")
	}
	registry := r.Registry
	runtimeID := adapterruntime.ResolveRuntimeID(backend)
	storage := backend.Spec.EffectiveRemoteStorage()
	if !isTypedLMCacheNodeLocal(backend) {
		if err := r.cleanupLMCacheNodeLocalServerPods(ctx, backend); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Events-only (tier-1 routing) provisions no backend server: the engine is
	// wired for cache-aware routing via the kvevent-subscriber alone, with no KV
	// connector and nothing to deploy. Shed any workload a prior Offload
	// generation owned, then run the server-less status path (the KV-event
	// readiness gate, no Service/endpoint/cascade). Checked before the
	// StatefulSet routing because the mode decides provisioning regardless of
	// provider workload.
	//
	// EventsOnly is checked before external remote-storage ownership so it takes
	// precedence over provider lifecycle. An admission-bypassed object carrying
	// both must not publish an external endpoint or inject a KV connector. Letting
	// EventsOnly win here mirrors the
	// webhook's adapter-independent connector skip, so both layers agree on the
	// mode's precedence over remote storage.
	//
	// First confirm an adapter is selectable for this (runtime, backend) pair.
	// A stored / admission-bypassed EventsOnly CR with an unsupported (engine,
	// type) pair would otherwise be reconciled as ACTIVE events-only even though
	// the pod webhook can't select an adapter for it (so it can never inject the
	// subscriber → no events ever flow). Treat the no-adapter case the same as
	// the Offload no-adapter path below: shed any workload and reconcile as
	// unmanaged (no Ready/Progressing published), so the CR isn't advertised as
	// a working routing tier the substrate can never feed.
	if backend.Spec.IsEventsOnly() {
		adapter, err := registry.Select(runtimeID, backend)
		if err != nil {
			logger.V(1).Info("no runtime adapter for events-only backend; treating as unmanaged",
				"runtime", runtimeID, "type", backend.Spec.Type,
				"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
			return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
		}
		if !adapter.SupportsBinding(nil) {
			logger.V(1).Info("runtime adapter does not support host-only events; treating as unmanaged",
				"runtime", runtimeID, "type", backend.Spec.Type,
				"namespace", backend.Namespace, "name", backend.Name)
			return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
		}
		if err := r.cleanupOwnedWorkload(ctx, backend); err != nil {
			return ctrl.Result{}, err
		}
		return r.reconcileEventsOnly(ctx, backend)
	}

	adapter, err := registry.Select(runtimeID, backend)
	if err != nil {
		// No adapter knows how to wire this (runtime, backend) pair. The
		// admission validator rejects this at write time, so reaching this branch
		// means a CR already in etcd from before the webhook was installed (or
		// with a registry that has since shrunk).
		logger.V(1).Info("no runtime adapter for backend",
			"runtime", runtimeID, "type", backend.Spec.Type,
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
	}

	if storage != nil && storage.Ownership == cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal {
		protocol, err := backendadapter.ProtocolFor(storage)
		if err != nil {
			logger.V(1).Info("external storage has no supported binding protocol; treating as unmanaged",
				"runtime", runtimeID, "type", backend.Spec.EffectiveCacheType(), "provider", storage.Provider,
				"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
			return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
		}
		binding := backendadapter.BindingFor(storage, protocol, storage.Endpoint)
		if !adapter.SupportsBinding(binding) {
			logger.V(1).Info("runtime adapter does not accept external-storage binding; treating as unmanaged",
				"runtime", runtimeID, "type", backend.Spec.EffectiveCacheType(), "protocol", protocol,
				"namespace", backend.Namespace, "name", backend.Name)
			return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
		}
		// A backend switched from a managed type to External must shed its workload.
		if err := r.cleanupOwnedWorkload(ctx, backend); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileLMCacheNodeLocalServerPods(ctx, backend, binding); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.reconcileExternal(ctx, backend)
	}

	if storage == nil {
		if err := r.cleanupOwnedWorkload(ctx, backend); err != nil {
			return ctrl.Result{}, err
		}
		if !adapter.SupportsBinding(nil) {
			logger.V(1).Info("runtime adapter does not support host-only caching; treating as unmanaged",
				"runtime", runtimeID, "type", backend.Spec.EffectiveCacheType(),
				"namespace", backend.Namespace, "name", backend.Name)
			return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
		}
		// Native HiCache remains endpoint-free and intentionally publishes no
		// Ready condition until its separate readiness contract is implemented.
		if backend.Spec.EffectiveCacheType() == cachev1alpha1.CacheBackendTypeSGLangHiCache {
			return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
		}
		if err := r.reconcileLMCacheNodeLocalServerPods(ctx, backend, nil); err != nil {
			return ctrl.Result{}, err
		}
		return r.reconcileHostOnly(ctx, backend)
	}

	provider, err := r.BackendRegistry.Select(storage)
	if err != nil {
		logger.V(1).Info("no remote-storage provider for backend; treating as unmanaged",
			"provider", storage.Provider, "ownership", storage.Ownership,
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
	}
	rendered, err := provider.Render(backend)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("render remote storage for %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	desiredService := r.buildService(backend, rendered.Service)
	binding := backendadapter.BindingFor(storage, rendered.Protocol, serviceEndpoint(desiredService))
	if !adapter.SupportsBinding(binding) {
		logger.V(1).Info("runtime adapter does not accept remote-storage binding; treating as unmanaged",
			"runtime", runtimeID, "type", backend.Spec.EffectiveCacheType(), "protocol", rendered.Protocol,
			"namespace", backend.Namespace, "name", backend.Name)
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
	}
	if err := r.reconcileLMCacheNodeLocalServerPods(ctx, backend, binding); err != nil {
		return ctrl.Result{}, err
	}

	return r.reconcileManaged(ctx, logger, backend, rendered)
}
