// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"fmt"
	"regexp"
	"strings"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const lmcacheKVEventPort int32 = 5557

var sha256ImagePattern = regexp.MustCompile(`^[^[:space:]@]+@sha256:[a-f0-9]{64}$`)

// validateLMCacheTopology enforces the canonical MP-only LMCache shape while
// leaving a topology-less legacy object untouched during repository migration.
// The presence of topology/podLocal/nodeLocal is the explicit boundary between
// the old flat inputs and the new API; the two shapes can never be mixed.
func validateLMCacheTopology(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb == nil || cb.Spec.LMCache == nil {
		return nil
	}

	lm := cb.Spec.LMCache
	lmPath := field.NewPath("spec", "lmCache")
	hasMPShape := lm.Topology != "" || lm.PodLocal != nil || lm.NodeLocal != nil
	if !hasMPShape {
		return nil // grandfathered flat-field/IP shape
	}

	var errs field.ErrorList
	if cb.Spec.EffectiveCacheType() != cachev1alpha1.CacheBackendTypeLMCache {
		// validateCacheHierarchy owns the clearer type error.
		return nil
	}
	if cb.Spec.IsEventsOnly() {
		errs = append(errs, field.Forbidden(lmPath,
			"LMCache topology is invalid with integration.mode=EventsOnly because that mode injects no KV connector or MP server"))
	}

	// Flat fields are compatibility inputs, not alternate spellings for MP.
	if lm.HostMemory != nil {
		errs = append(errs, field.Forbidden(lmPath.Child("hostMemory"),
			"legacy flat field cannot be mixed with the MP topology; use podLocal.server.l1Capacity"))
	}
	if strings.TrimSpace(lm.WorkerImage) != "" {
		errs = append(errs, field.Forbidden(lmPath.Child("workerImage"),
			"legacy flat field cannot be mixed with the MP topology; use podLocal.server.image"))
	}
	if lm.WorkerPort != nil {
		errs = append(errs, field.Forbidden(lmPath.Child("workerPort"),
			"legacy flat field cannot be mixed with the MP topology; use podLocal.server.port"))
	}
	if strings.TrimSpace(lm.RemoteSerde) != "" {
		errs = append(errs, field.Forbidden(lmPath.Child("remoteSerde"),
			"remoteSerde belongs to the legacy in-process connector and is not supported by LMCache MP"))
	}

	storage := cb.Spec.EffectiveRemoteStorage()
	if storage != nil {
		switch storage.Provider {
		case cachev1alpha1.CacheBackendRemoteStorageProviderRedis:
			// Redis/RESP is the initial shared L3 for both engines.
		case cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer:
			errs = append(errs, field.NotSupported(
				field.NewPath("spec", "remoteStorage", "provider"), storage.Provider,
				[]string{string(cachev1alpha1.CacheBackendRemoteStorageProviderRedis)},
			))
		case cachev1alpha1.CacheBackendRemoteStorageProviderMooncake:
			errs = append(errs, field.NotSupported(
				field.NewPath("spec", "remoteStorage", "provider"), storage.Provider,
				[]string{string(cachev1alpha1.CacheBackendRemoteStorageProviderRedis)},
			))
		}
	}

	switch lm.Topology {
	case "":
		errs = append(errs, field.Required(lmPath.Child("topology"),
			"required when podLocal or nodeLocal is configured"))
	case cachev1alpha1.LMCacheTopologyPodLocal:
		if lm.PodLocal == nil {
			errs = append(errs, field.Required(lmPath.Child("podLocal"),
				"required when topology=PodLocal"))
		} else if lm.PodLocal.Server == nil {
			errs = append(errs, field.Required(lmPath.Child("podLocal", "server"),
				"required when topology=PodLocal"))
		} else {
			errs = append(errs, validatePodLocalServer(lm.PodLocal.Server, lmPath.Child("podLocal", "server"))...)
		}
		if lm.NodeLocal != nil {
			errs = append(errs, field.Forbidden(lmPath.Child("nodeLocal"),
				"must be omitted when topology=PodLocal"))
		}
	case cachev1alpha1.LMCacheTopologyNodeLocal:
		if lm.NodeLocal == nil {
			errs = append(errs, field.Required(lmPath.Child("nodeLocal"),
				"required when topology=NodeLocal"))
		} else if lm.NodeLocal.Server == nil {
			errs = append(errs, field.Required(lmPath.Child("nodeLocal", "server"),
				"required when topology=NodeLocal"))
		} else {
			errs = append(errs, validateNodeLocalServer(lm.NodeLocal.Server, lmPath.Child("nodeLocal", "server"))...)
		}
		if lm.PodLocal != nil {
			errs = append(errs, field.Forbidden(lmPath.Child("podLocal"),
				"must be omitted when topology=NodeLocal"))
		}
		errs = append(errs, field.Forbidden(lmPath.Child("topology"),
			"NodeLocal is reserved for Phase 8 and is not implemented; use PodLocal"))
	default:
		errs = append(errs, field.NotSupported(lmPath.Child("topology"), lm.Topology,
			[]string{string(cachev1alpha1.LMCacheTopologyPodLocal), string(cachev1alpha1.LMCacheTopologyNodeLocal)}))
	}

	return errs
}

func validatePodLocalServer(server *cachev1alpha1.LMCachePodLocalServerSpec, path *field.Path) field.ErrorList {
	if server == nil {
		return nil
	}
	errs := validateMPServer(
		server.Image,
		server.Port,
		&server.L1Capacity,
		server.Resources,
		path,
	)
	if server.MaxWorkers < 1 {
		errs = append(errs, field.Invalid(path.Child("maxWorkers"), server.MaxWorkers, "must be at least 1"))
	}
	return errs
}

func validateNodeLocalServer(server *cachev1alpha1.LMCacheNodeLocalServerSpec, path *field.Path) field.ErrorList {
	if server == nil {
		return nil
	}
	errs := validateMPServer(
		server.Image,
		server.Port,
		&server.L1Capacity,
		server.Resources,
		path,
	)
	if server.MaxGPUWorkers < 1 {
		errs = append(errs, field.Invalid(path.Child("maxGPUWorkers"), server.MaxGPUWorkers, "must be at least 1"))
	}
	if server.MaxCPUWorkers < 1 {
		errs = append(errs, field.Invalid(path.Child("maxCPUWorkers"), server.MaxCPUWorkers, "must be at least 1"))
	}
	return errs
}

func validateMPServer(
	image string,
	port int32,
	l1Capacity *resource.Quantity,
	resources corev1.ResourceRequirements,
	path *field.Path,
) field.ErrorList {
	var errs field.ErrorList
	trimmedImage := strings.TrimSpace(image)
	switch {
	case trimmedImage == "":
		errs = append(errs, field.Required(path.Child("image"), "a CacheBackend-owned LMCache MP server image is required"))
	case !sha256ImagePattern.MatchString(trimmedImage):
		errs = append(errs, field.Invalid(path.Child("image"), image,
			"must be pinned by sha256 digest (for example registry.example/lmcache@sha256:<64-hex-digest>)"))
	}

	if port < 1 || port > 65535 {
		errs = append(errs, field.Invalid(path.Child("port"), port, "must be between 1 and 65535"))
	} else if port == lmcacheKVEventPort {
		errs = append(errs, field.Invalid(path.Child("port"), port,
			fmt.Sprintf("collides with the engine KV-event publisher port %d", lmcacheKVEventPort)))
	}

	if l1Capacity == nil || l1Capacity.Sign() <= 0 {
		var bad any
		if l1Capacity != nil {
			bad = l1Capacity.String()
		}
		errs = append(errs, field.Invalid(path.Child("l1Capacity"), bad, "must be greater than zero"))
	}
	errs = append(errs, validateMPServerResourceRequirements(resources, path.Child("resources"))...)

	cpuRequest, hasCPURequest := resources.Requests[corev1.ResourceCPU]
	if !hasCPURequest || cpuRequest.Sign() <= 0 {
		errs = append(errs, field.Required(path.Child("resources", "requests").Key(string(corev1.ResourceCPU)),
			"a positive CPU request is required for the MP server"))
	}
	memoryRequest, hasMemoryRequest := resources.Requests[corev1.ResourceMemory]
	if !hasMemoryRequest || memoryRequest.Sign() <= 0 {
		errs = append(errs, field.Required(path.Child("resources", "requests").Key(string(corev1.ResourceMemory)),
			"a positive memory request is required for the MP server"))
	} else if l1Capacity != nil && l1Capacity.Sign() > 0 && memoryRequest.Cmp(*l1Capacity) <= 0 {
		errs = append(errs, field.Invalid(path.Child("resources", "requests").Key(string(corev1.ResourceMemory)),
			memoryRequest.String(), fmt.Sprintf("must be greater than l1Capacity %s so the server has explicit memory headroom", l1Capacity.String())))
	}

	memoryLimit, hasMemoryLimit := resources.Limits[corev1.ResourceMemory]
	if !hasMemoryLimit || memoryLimit.Sign() <= 0 {
		errs = append(errs, field.Required(path.Child("resources", "limits").Key(string(corev1.ResourceMemory)),
			"a positive memory limit is required for the MP server"))
	} else {
		if hasMemoryRequest && memoryLimit.Cmp(memoryRequest) < 0 {
			errs = append(errs, field.Invalid(path.Child("resources", "limits").Key(string(corev1.ResourceMemory)),
				memoryLimit.String(), fmt.Sprintf("must be greater than or equal to the memory request %s", memoryRequest.String())))
		}
		if l1Capacity != nil && l1Capacity.Sign() > 0 && memoryLimit.Cmp(*l1Capacity) <= 0 {
			errs = append(errs, field.Invalid(path.Child("resources", "limits").Key(string(corev1.ResourceMemory)),
				memoryLimit.String(), fmt.Sprintf("must be greater than l1Capacity %s so the server has explicit memory headroom", l1Capacity.String())))
		}
	}

	return errs
}

// validateMPServerResourceRequirements mirrors the generic provider-resource
// admission rules for the independently owned MP server resource block. Keep
// this at the API boundary: otherwise malformed extended resources are only
// discovered when the mutated engine Pod is submitted to the apiserver.
func validateMPServerResourceRequirements(resources corev1.ResourceRequirements, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	if len(resources.Claims) > 0 {
		errs = append(errs, field.Forbidden(path.Child("claims"),
			"MP server resource claims are not supported because the injector does not own pod.spec.resourceClaims"))
	}

	checkList := func(list corev1.ResourceList, kind string) {
		for name, quantity := range list {
			itemPath := path.Child(kind).Key(string(name))
			if msg, ok := validateContainerResourceName(name); !ok {
				errs = append(errs, field.Invalid(itemPath, string(name), msg))
				continue
			}
			if quantity.Sign() < 0 {
				errs = append(errs, field.Invalid(itemPath, quantity.String(), "must be a non-negative quantity"))
			}
			if !isOvercommittableResource(name) && !strings.HasPrefix(string(name), "hugepages-") {
				if _, ok := quantity.AsInt64(); !ok {
					errs = append(errs, field.Invalid(itemPath, quantity.String(),
						fmt.Sprintf("%q is an extended resource and must be an integer quantity", name)))
				}
			}
			if strings.HasPrefix(string(name), "hugepages-") && quantity.Sign() > 0 {
				pageSize, err := resource.ParseQuantity(strings.TrimPrefix(string(name), "hugepages-"))
				if err == nil && pageSize.Sign() > 0 && quantity.Value()%pageSize.Value() != 0 {
					errs = append(errs, field.Invalid(itemPath, quantity.String(),
						fmt.Sprintf("must be a multiple of the page size %s", pageSize.String())))
				}
			}
		}
	}
	checkList(resources.Requests, "requests")
	checkList(resources.Limits, "limits")

	for name, request := range resources.Requests {
		limit, hasLimit := resources.Limits[name]
		if !isOvercommittableResource(name) && !hasLimit {
			errs = append(errs, field.Invalid(path.Child("requests").Key(string(name)), request.String(),
				fmt.Sprintf("%q is non-overcommittable and must also have an equal limit", name)))
			continue
		}
		if !hasLimit {
			continue
		}
		if isOvercommittableResource(name) && limit.Cmp(request) < 0 {
			errs = append(errs, field.Invalid(path.Child("limits").Key(string(name)), limit.String(),
				fmt.Sprintf("must be greater than or equal to request %s", request.String())))
		}
		if !isOvercommittableResource(name) && limit.Cmp(request) != 0 {
			errs = append(errs, field.Invalid(path.Child("limits").Key(string(name)), limit.String(),
				fmt.Sprintf("must equal request %s for non-overcommittable resource %q", request.String(), name)))
		}
	}
	return errs
}

// rejectUnimplementedRedisBindingFeatures keeps the newly typed credential/TLS
// contract from becoming accepted-but-ignored configuration. Phase 2 removes
// this gate when the structured runtime binding and secret mounts are rendered.
func rejectUnimplementedRedisBindingFeatures(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb == nil || cb.Spec.RemoteStorage == nil || cb.Spec.RemoteStorage.Redis == nil {
		return nil
	}
	redis := cb.Spec.RemoteStorage.Redis
	path := field.NewPath("spec", "remoteStorage", "redis")
	var errs field.ErrorList
	if redis.Authentication != nil {
		errs = append(errs, field.Forbidden(path.Child("authentication"),
			"Redis authentication is typed but not rendered until Phase 2; refusing inert credentials"))
	}
	if redis.TLS != nil {
		errs = append(errs, field.Forbidden(path.Child("tls"),
			"Redis TLS is typed but not rendered until Phase 2; refusing inert TLS configuration"))
	}
	if redis.Database != nil {
		errs = append(errs, field.Forbidden(path.Child("database"),
			"Redis database selection is typed but not rendered until Phase 2; refusing inert adapter configuration"))
	}
	return errs
}
