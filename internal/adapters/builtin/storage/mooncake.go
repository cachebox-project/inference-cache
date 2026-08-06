package storage

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// Mooncake provider defaults. Canonical resources override them through the
// typed remoteStorage.mooncake fields; deprecated BackendConfig keys remain
// readable only for legacy resources.
const (
	// This reference is fully qualified for CRI-O nodes without short-name
	// resolution and pinned to the release validated with the matching
	// mooncake-transfer-engine client.
	//
	// TODO(cachebox): digest-pin this image before production. The master
	// command and port surface were validated against this tag; do not
	// substitute an invented digest.
	defaultMooncakeMasterImage   = "docker.io/kvcacheai/mooncake:0.3.11.post1"
	defaultMooncakeMasterRPCPort = int32(50051)
	defaultMooncakeMetadataPort  = int32(8080)
	defaultMooncakeMetricsPort   = int32(9003)
	defaultMooncakeMasterHost    = "0.0.0.0"
	mooncakeRPCPortName          = "mooncake-rpc"
	mooncakeMetadataPortName     = "mooncake-meta"
	mooncakeMetricsPortName      = "metrics"
	mooncakeMasterContainerName  = "mooncake-master"
)

// ResolveMooncakeServer renders the provider-owned Mooncake master workload.
func ResolveMooncakeServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	if cache == nil {
		return nil, nil, fmt.Errorf("resolve cache server: cache is nil")
	}
	cfg := legacyProviderConfig(cache)
	image := effectiveProviderImage(
		cache,
		cachev1alpha1.CacheBackendRemoteStorageProviderMooncake,
		cfgKeyServerImage,
		defaultMooncakeMasterImage,
	)

	command, args := mooncakeMasterCommand(cfg)
	if typed := effectiveProviderCommand(cache, cachev1alpha1.CacheBackendRemoteStorageProviderMooncake); len(typed) > 0 {
		command, args = typed[:1], typed[1:]
	}
	container := corev1.Container{
		Name:            mooncakeMasterContainerName,
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         command,
		Args:            args,
		Ports: []corev1.ContainerPort{
			{Name: mooncakeRPCPortName, ContainerPort: defaultMooncakeMasterRPCPort, Protocol: corev1.ProtocolTCP},
			{Name: mooncakeMetadataPortName, ContainerPort: defaultMooncakeMetadataPort, Protocol: corev1.ProtocolTCP},
			{Name: mooncakeMetricsPortName, ContainerPort: defaultMooncakeMetricsPort, Protocol: corev1.ProtocolTCP},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(mooncakeRPCPortName)},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			FailureThreshold:    6,
		},
		Resources: defaultServerResources(cache),
	}

	// Mooncake is a peer-to-peer transfer mesh. The master returns the node
	// holding a block, then the engine dials that node on a dynamically
	// negotiated port. Host networking plus a headless Service publishes the
	// node address without limiting the data path to declared Service ports.
	pod := &corev1.PodSpec{
		Containers:  []corev1.Container{container},
		HostNetwork: true,
		DNSPolicy:   corev1.DNSClusterFirstWithHostNet,
	}
	service := &corev1.Service{
		Spec: corev1.ServiceSpec{
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: corev1.ClusterIPNone,
			Ports: []corev1.ServicePort{
				{
					Name:       mooncakeRPCPortName,
					Port:       defaultMooncakeMasterRPCPort,
					TargetPort: intstr.FromString(mooncakeRPCPortName),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       mooncakeMetadataPortName,
					Port:       defaultMooncakeMetadataPort,
					TargetPort: intstr.FromString(mooncakeMetadataPortName),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
	return pod, service, nil
}

func mooncakeMasterCommand(cfg map[string]string) (command, args []string) {
	// Command overrides must retain the fixed RPC and metadata ports rendered
	// into the container, readiness probe, Service, status endpoint, and engine
	// URL. Free-form command text cannot safely drive those structured fields.
	if raw := configOr(cfg, cfgKeyServerCommand, ""); raw != "" {
		fields := strings.Fields(raw)
		if len(fields) > 0 {
			return []string{fields[0]}, fields[1:]
		}
	}
	return []string{"mooncake_master"}, []string{
		fmt.Sprintf("--rpc_port=%d", defaultMooncakeMasterRPCPort),
		fmt.Sprintf("--metrics_port=%d", defaultMooncakeMetricsPort),
		"--enable_http_metadata_server=true",
		"--http_metadata_server_host=" + defaultMooncakeMasterHost,
		fmt.Sprintf("--http_metadata_server_port=%d", defaultMooncakeMetadataPort),
	}
}
