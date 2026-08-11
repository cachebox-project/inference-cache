// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"
	"strconv"
	"strings"

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
	// EngineMetricsPortStr is the engine's DEFAULT Prometheus /metrics port on
	// pod-local 127.0.0.1 (vLLM 8000, SGLang 30000). It sets --engine-metrics-url
	// so the stats scraper reads the right endpoint; the scraper's per-engine
	// metric profile is selected separately by --hash-scheme. Empty omits the
	// flag, leaving the binary's :8000 default (the pre-wiring behavior). A
	// non-default engine --port (see EngineContainerName) overrides it.
	EngineMetricsPortStr string
	// EngineContainerName identifies the engine container whose --port arg, when
	// present, overrides EngineMetricsPortStr — both vLLM and SGLang serve
	// /metrics on their HTTP --port, so an operator running a custom port is
	// still scraped there. Empty disables the override (default port is used).
	EngineContainerName string
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
	metricsPort := p.EngineMetricsPortStr
	if custom := enginePortFromContainer(p.Pod, p.EngineContainerName); custom != "" {
		metricsPort = custom
	}
	if metricsPort != "" {
		args = append(args, "--engine-metrics-url=http://127.0.0.1:"+metricsPort+"/metrics")
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

// enginePortFromContainer parses the engine's HTTP port from the named
// container's --port flag ("--port N" or "--port=N"), scanning its full argv —
// Command followed by Args, since Kubernetes may carry engine flags in either.
// Both vLLM and SGLang serve /metrics on that port. Returns "" when the
// container, or a well-formed numeric --port, is absent — so the caller falls
// back to the engine's default metrics port. This keeps load-aware hints working
// when an operator runs the engine on a custom --port instead of the default.
//
// The LAST valid occurrence wins, matching how the engine's own argument parser
// resolves a repeated --port to the one it binds. The value is normalized through
// strconv (e.g. "+31000" → "31000") so the scraped URL matches the bound port.
//
// A Kubernetes "$(VAR)" reference is resolved against the container's own env
// (see resolveEnvRef): "--port=$(ENGINE_PORT)" with env ENGINE_PORT=40000 scrapes
// :40000. Only literal env values resolve — a var backed by valueFrom
// (configMap/secret/fieldRef) or an undefined var can't be resolved statically,
// so the derivation falls back to the engine's default port (and a wrong port
// then fails loud on scrape rather than delivering stats for the wrong endpoint).
func enginePortFromContainer(pod *corev1.Pod, containerName string) string {
	if pod == nil || containerName == "" {
		return ""
	}
	idx, err := EngineContainerIndexNamed(&pod.Spec, containerName)
	if err != nil {
		return ""
	}
	c := pod.Spec.Containers[idx]
	argv := append(append([]string(nil), c.Command...), c.Args...)
	port := ""
	for i, a := range argv {
		var v string
		switch {
		case strings.HasPrefix(a, "--port="):
			v = strings.TrimPrefix(a, "--port=")
		case a == "--port" && i+1 < len(argv):
			v = argv[i+1]
		default:
			continue
		}
		v = resolveEnvRef(v, c.Env)
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n < 65536 {
			port = strconv.Itoa(n)
		}
	}
	return port
}

// resolveEnvRef resolves a whole-value Kubernetes "$(VAR)" argument reference
// against the container's own env, mirroring how the kubelet expands $(VAR) in
// args/command. Only literal env values are resolvable statically; a var backed
// by valueFrom, or an undefined var, resolves to "" so the caller falls back to
// the default port. A non-reference value passes through unchanged.
func resolveEnvRef(v string, env []corev1.EnvVar) string {
	if !strings.HasPrefix(v, "$(") || !strings.HasSuffix(v, ")") {
		return v
	}
	name := v[2 : len(v)-1]
	for _, e := range env {
		if e.Name == name {
			return e.Value // "" when only ValueFrom is set (unresolvable here)
		}
	}
	return ""
}
