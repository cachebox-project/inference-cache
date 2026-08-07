package runtime

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
)

const (
	DefaultSubscriberImage = "ghcr.io/cachebox-project/inference-cache-subscriber:dev"

	DefaultPolicyServerGRPCAddress = "inference-cache-server.inference-cache-system.svc.cluster.local:9090"
)

// SubscriberConfig contains the shipping subscriber settings shared by the
// built-in runtime adapters. Its zero value preserves the existing behavior:
// no sidecar image is attached, and an empty server address selects the
// in-cluster default when a sidecar is rendered.
type SubscriberConfig struct {
	Image                   string
	PolicyServerGRPCAddress string
}

type subscriberSidecarParams struct {
	Config           SubscriberConfig
	Cache            *cachev1alpha1.CacheBackend
	Pod              *corev1.Pod
	HashScheme       string
	EngineZMQPortStr string
}

func renderSubscriberSidecar(p subscriberSidecarParams) (*corev1.Container, error) {
	if p.Cache == nil {
		return nil, fmt.Errorf("observation sidecar: cache is nil")
	}
	if p.Pod == nil {
		return nil, fmt.Errorf("observation sidecar: pod is nil")
	}
	if p.Config.Image == "" {
		return nil, nil
	}
	modelID := p.Cache.Spec.EffectiveObservationModelID()
	if modelID == "" {
		return nil, nil
	}
	serverAddr := p.Config.PolicyServerGRPCAddress
	if serverAddr == "" {
		serverAddr = DefaultPolicyServerGRPCAddress
	}

	args := []string{
		"--engine-endpoint=tcp://127.0.0.1:" + p.EngineZMQPortStr,
		"--server=" + serverAddr,
		"--replica-id=$(POD_NAME)",
		"--tenant-id=$(POD_NAMESPACE)",
		"--model-id=" + modelID,
		"--hash-scheme=" + p.HashScheme,
	}
	if !p.Cache.Spec.IsEventsOnly() {
		args = append(args, "--ignore-block-removed=true")
	}

	nonRoot := true
	noPrivEsc := false
	readOnlyRoot := true
	uid := int64(65532)
	return &corev1.Container{
		Name:            enginebinding.SubscriberContainerName,
		Image:           p.Config.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			{
				Name:      "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}},
			},
			{
				Name:      "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}},
			},
		},
		Args: args,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot:             &nonRoot,
			RunAsUser:                &uid,
			AllowPrivilegeEscalation: &noPrivEsc,
			ReadOnlyRootFilesystem:   &readOnlyRoot,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}, nil
}
