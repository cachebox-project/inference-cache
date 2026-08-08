// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// reconcileManaged renders the cache-server PodSpec + Service via the runtime
// adapter, wraps them into a Deployment + Service owned by the CR, and
// publishes the resolved endpoint to status.
//
// Apply drives desired state; status reflects observed state. The two must not
// block each other: if a desired-state write fails (e.g. a transient API-server
// conflict or a webhook rejection), we still publish status from whatever the
// live Deployment reports, so the user-visible CR field is never held hostage
// to apply churn. Any apply error is surfaced after the status pass so
// controller-runtime requeues.
func (r *CacheBackendReconciler) reconcileManaged(ctx context.Context, logger logr.Logger, backend *cachev1alpha1.CacheBackend, rendered *backendadapter.RenderedStorage) (ctrl.Result, error) {
	podSpec, svcSpec := rendered.PodSpec, rendered.Service
	if podSpec == nil || svcSpec == nil {
		// Engine-local adapters such as native SGLang HiCache intentionally
		// render no cache-server. Reuse the unmanaged lifecycle to shed any
		// previously owned workload and clear server-backed status.
		logger.V(1).Info("adapter rendered no cache-server; treating as unmanaged",
			"namespace", backend.Namespace, "name", backend.Name)
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
	}

	dep := r.buildDeployment(backend, podSpec)
	svc := r.buildService(backend, svcSpec)

	// Skip Service + HPA when applyDeployment failed. The HPA targets the
	// Deployment by name, so running it after a foreign-ownership failure
	// could scale another controller's workload; the Service is independent
	// but pointless to expose alongside a Deployment we don't own. Status
	// observation still runs below (it has its own ownership guards) so the
	// CR isn't held hostage to apply churn.
	applyErr := r.applyDeployment(ctx, backend, dep)
	if applyErr == nil {
		if svcErr := r.applyService(ctx, backend, svc); svcErr != nil {
			applyErr = svcErr
		}
		if hpaErr := r.reconcileHPA(ctx, backend, dep); hpaErr != nil && applyErr == nil {
			applyErr = hpaErr
		}
	}

	var live appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKeyFromObject(dep), &live); err != nil {
		if apierrors.IsNotFound(err) && applyErr != nil {
			// Apply failed before creating the Deployment, so there is no
			// observed state to publish — surface the apply error to requeue.
			return ctrl.Result{}, applyErr
		}
		// Either a transient Get error, or NotFound after a successful apply
		// (deleted out-of-band between apply and Get). Both must requeue;
		// silently reporting success here would freeze status at a stale
		// snapshot.
		return ctrl.Result{}, fmt.Errorf("get deployment %s/%s: %w", dep.Namespace, dep.Name, err)
	}
	// Never publish status derived from a foreign workload. The common case
	// is an AlreadyOwned collision during apply (applyErr is set; surface
	// it). The race case is that apply succeeded but the live Deployment's
	// controller ref was changed out-of-band between Update and this Get —
	// applyErr is nil, but we no longer own the object. Returning nil there
	// would silently mark the reconcile successful AND lose the owned-object
	// watch (no future event would re-trigger), so synthesize an explicit
	// error to requeue.
	if !metav1.IsControlledBy(&live, backend) {
		if applyErr != nil {
			return ctrl.Result{}, applyErr
		}
		return ctrl.Result{}, fmt.Errorf("deployment %s/%s lost controller reference after apply", dep.Namespace, dep.Name)
	}

	// Endpoint is derived from the *live* Service, not the desired one: if
	// applyService failed (Forbidden, conflict-budget exhausted, foreign
	// ownership, ...) we must not advertise an endpoint that doesn't exist,
	// has stale ports, or points at a Service we don't own. Empty endpoint
	// when the Service hasn't materialized or is owned by someone else.
	var liveSvc corev1.Service
	endpoint := ""
	if err := r.Get(ctx, client.ObjectKeyFromObject(svc), &liveSvc); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get service %s/%s: %w", svc.Namespace, svc.Name, err)
		}
	} else if metav1.IsControlledBy(&liveSvc, backend) {
		endpoint = serviceEndpoint(&liveSvc)
	}

	requeueAfter, statusErr := r.updateManagedStatus(ctx, backend, endpoint, &live, applyErr == nil)
	// Do NOT short-circuit on statusErr — the cascade is independent
	// recovery for stale engine sockets and must not be skipped just
	// because the unrelated managed-status patch (matchedEnginePods,
	// Ready / Progressing / Degraded conditions, …) hit a
	// transient conflict. The cascade has its own patchStatus path
	// for the latch field, gated separately. Capture the error and
	// return it AFTER the cascade has run.

	// Cache-server restart cascade: when the Ready cache-server pod
	// SERVER-INSTANCE IDENTIFIER changes (either a pod UID swap or a
	// restart-sum advance from an in-place kubelet-driven container
	// restart — see currentServerInstanceID's godoc for the shape),
	// cascade-restart every engine Deployment that was injected
	// against this backend so they re-establish their LMCache client
	// socket (the upstream LMServerConnector opens its TCP socket in
	// __init__ only and silently fails every subsequent PUT with EPIPE
	// after a server restart, until the engine pod itself rolls). Always
	// runs (even when applyErr != nil OR updateManagedStatus errored),
	// since the cascade is independent of whether THIS reconcile pass
	// made a successful apply or a successful unrelated status update:
	// a transient apply / status-write churn must not delay engine
	// recovery from a cache-server outage. A non-zero cascadeWait means
	// the rate-limit window suppressed the cascade; honor it on the
	// requeue so we retry exactly at the boundary.
	cascadeWait := r.reconcileServerInstance(ctx, logger, backend)
	if cascadeWait > 0 && (requeueAfter == 0 || cascadeWait < requeueAfter) {
		requeueAfter = cascadeWait
	}
	// Schedule an unconditional periodic re-poll of the cache-server
	// pod set on managed backends. Reason: an in-place container
	// restart (kubelet respawning a crashed cache-server container
	// without bumping pod.UID) does NOT change owned-Deployment status
	// counts, and the controller deliberately does not watch Pods
	// cluster-wide (see refreshMatchedEnginePods godoc). The
	// matched-engine-pods cadence above does not cover this case
	// either: when an operator removes spec.engineSelector after
	// engines were injected, len(matchedEnginePods)→0 and that
	// cadence stops firing, leaving in-place restarts unobservable
	// until something unrelated triggers a reconcile. Pinning a
	// floor at the rate-limit interval bounds the observation
	// latency for in-place restarts at one cadence (cheap: one
	// Pod List + one Deployment Get per backend per cadence).
	pollCadence := r.minServerRestartCascadeInterval()
	if requeueAfter == 0 || pollCadence < requeueAfter {
		requeueAfter = pollCadence
	}

	if applyErr != nil {
		// Return the error so controller-runtime's workqueue
		// rate-limiter requeues the reconcile. Per the
		// sigs.k8s.io/controller-runtime/pkg/reconcile contract, when
		// the error is non-nil the `Result` is ignored — including any
		// RequeueAfter we might set here — so there is no point
		// pretending to schedule the cascade retry at the rate-limit
		// boundary on this path. The rate-limiter's backoff cadence is
		// the actual retry schedule; the next successful reconcile
		// then re-enters the cascade path at its own boundary.
		return ctrl.Result{}, applyErr
	}
	if statusErr != nil {
		// Surface the deferred status-write failure after the cascade
		// has had its chance to recover engine FDs. Same workqueue
		// rate-limiter semantics as the applyErr path: Result is
		// ignored when err != nil.
		return ctrl.Result{}, statusErr
	}

	logger.V(1).Info("reconciled managed CacheBackend",
		"namespace", backend.Namespace, "name", backend.Name, "endpoint", endpoint)
	// requeueAfter is the tighter of two gate-driven schedules (see
	// minNonZero in updateManagedStatus):
	//   * KV-event gate: non-zero while in the AwaitingFirstKVEvent window,
	//     so the automatic Degraded flip fires when firstEventTimeout
	//     elapses without an event — without waiting for the next periodic
	//     resync.
	//   * Functional-probe gate: non-zero on every probe path that did NOT
	//     advance the rate-limit window (rate-limited, HTTP-error) AND on
	//     the success/per-stage-failure paths that DID — schedules the next
	//     /probe call at the rate-limit-window expiry so a quiet stuck
	//     backend re-probes without relying on incidental external watch
	//     events.
	// Either gate's non-zero value (or the smaller of both) lands here.
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}
