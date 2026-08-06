package storage

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// LMCache standalone-server defaults. Canonical resources override them
// through remoteStorage.lmCacheServer; deprecated BackendConfig keys remain
// readable only for legacy resources.
const (
	// The server and the LMCache client compiled into the engine communicate
	// over a versioned wire protocol, so this must never become a floating tag.
	// Operators must keep an override compatible with the client in their
	// engine image.
	//
	// TODO(cachebox): wire-test and digest-pin this image before production.
	// v0.4.7 matches the client used in validation, but the standalone image
	// was not independently wire-tested here. Do not substitute an invented
	// digest.
	defaultLMCacheServerImage    = "lmcache/standalone:v0.4.7"
	defaultLMCacheServerPort     = int32(65432)
	defaultLMCacheServerHost     = "0.0.0.0"
	defaultLMCacheServerStorage  = "cpu"
	defaultLMCacheServerPortName = "lmcache"

	cfgKeyServerImage   = "serverImage"
	cfgKeyServerCommand = "serverCommand"
)

// ResolveLMCacheServer renders the provider-owned standalone LMCache server.
// The reconciler supplies identity, selectors, workload kind, and ownership.
func ResolveLMCacheServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	if cache == nil {
		return nil, nil, fmt.Errorf("resolve cache server: cache is nil")
	}
	cfg := legacyProviderConfig(cache)
	image := effectiveProviderImage(
		cache,
		cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
		cfgKeyServerImage,
		defaultLMCacheServerImage,
	)

	command, args := lmCacheServerCommand(cfg)
	if typed := effectiveProviderCommand(cache, cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer); len(typed) > 0 {
		command, args = typed[:1], typed[1:]
	}
	container := corev1.Container{
		Name:            "lmcache-server",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         command,
		Args:            args,
		Ports: []corev1.ContainerPort{
			{Name: defaultLMCacheServerPortName, ContainerPort: defaultLMCacheServerPort, Protocol: corev1.ProtocolTCP},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(defaultLMCacheServerPortName)},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			FailureThreshold:    6,
		},
		Resources: defaultServerResources(cache),
	}

	pod := &corev1.PodSpec{Containers: []corev1.Container{container}}
	service := &corev1.Service{
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       defaultLMCacheServerPortName,
					Port:       defaultLMCacheServerPort,
					TargetPort: intstr.FromString(defaultLMCacheServerPortName),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
	return pod, service, nil
}

// defaultServerResources returns a deep-copied provider resource block and
// supplies the CPU request required by a CPU-utilization HPA.
func defaultServerResources(cache *cachev1alpha1.CacheBackend) corev1.ResourceRequirements {
	var out corev1.ResourceRequirements
	if resources := effectiveProviderResources(cache); resources != nil {
		out = *resources.DeepCopy()
	}
	if cache == nil || cache.Spec.Autoscaling == nil {
		return out
	}
	if out.Requests == nil {
		out.Requests = corev1.ResourceList{}
	}
	cpu, hasCPU := out.Requests[corev1.ResourceCPU]
	if !hasCPU || cpu.Sign() <= 0 {
		out.Requests[corev1.ResourceCPU] = resource.MustParse("250m")
	}
	return out
}

func lmCacheServerCommand(cfg map[string]string) (command, args []string) {
	if raw := configOr(cfg, cfgKeyServerCommand, ""); raw != "" {
		fields := strings.Fields(raw)
		if len(fields) > 0 {
			return []string{fields[0]}, fields[1:]
		}
	}
	return []string{"lmcache_server"}, []string{
		defaultLMCacheServerHost,
		fmt.Sprintf("%d", defaultLMCacheServerPort),
		defaultLMCacheServerStorage,
	}
}
