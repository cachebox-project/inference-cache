// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

const (
	lmCacheNodeLocalGateContainerName = "lmcache-node-local-gate"
	lmCacheNodeLocalGateManagedEnv    = "INFERENCECACHE_NODE_LOCAL_GATE"
	lmCacheNodeLocalGateManagedValue  = "true"
	lmCacheNodeLocalShmVolumeName     = "lmcache-node-shm"
	lmCacheNodeLocalShmCleanupName    = "lmcache-node-shm-cleanup"
	lmCacheNodeLocalShmCleanupGate    = "inferencecache.io/await-shm-quiescence"
	lmCacheNodeLocalShmCleanupPath    = "/var/run/inference-cache/shm-pool"
	lmCacheNodeLocalConfigVolumeName  = "lmcache-node-config"
	lmCacheNodeLocalConfigMountPath   = "/var/run/inference-cache/lmcache-node"
	lmCacheNodeLocalConfigFilePath    = lmCacheNodeLocalConfigMountPath + "/client.yaml"
	lmCacheNodeIPEnv                  = "INFERENCECACHE_NODE_IP"
	lmCacheNodeLocalShmHostRoot       = "/dev/shm/inference-cache"
	hostPathComponentMaxLength        = 255
)

// RenderLMCacheNodeLocalServerPod renders the engine-neutral server for one
// node that already hosts a selected engine. Exact-node affinity leaves binding
// to the scheduler (so hostPort conflicts remain visible) and never steers the
// engine itself. The controller adds the owner reference; no Service is part of
// the NodeLocal MP contract.
func RenderLMCacheNodeLocalServerPod(cache *cachev1alpha1.CacheBackend, binding *backendadapter.Binding, nodeName string, source *corev1.Pod) (*corev1.Pod, error) {
	if cache == nil || cache.Spec.LMCache == nil || cache.Spec.LMCache.Topology != cachev1alpha1.LMCacheTopologyNodeLocal ||
		cache.Spec.LMCache.NodeLocal == nil || cache.Spec.LMCache.NodeLocal.Server == nil {
		return nil, fmt.Errorf("render LMCache NodeLocal server Pod: complete typed NodeLocal configuration is required")
	}
	if cache.UID == "" {
		return nil, fmt.Errorf("render LMCache NodeLocal server Pod: CacheBackend UID is empty")
	}
	if strings.TrimSpace(nodeName) == "" || source == nil || source.Spec.NodeName != nodeName {
		return nil, fmt.Errorf("render LMCache NodeLocal server Pod: a scheduled source engine on the target node is required")
	}
	server := cache.Spec.LMCache.NodeLocal.Server
	scheduling := cache.Spec.LMCache.NodeLocal.Scheduling
	if err := validateLMCacheNodeLocalServerConfig(server, effectiveLMCacheChunkSize(cache.Spec.LMCache)); err != nil {
		return nil, err
	}
	l2Adapter, bindingEnv, err := renderLMCacheMPL2Binding(binding)
	if err != nil {
		return nil, err
	}
	l1GiB, err := quantityAsGiB(server.L1Capacity)
	if err != nil {
		return nil, err
	}

	identity := lmCacheNodeLocalInstanceID(cache)
	shmHostPath, err := NodeLocalServerShmHostPath(cache)
	if err != nil {
		return nil, err
	}
	args := []string{
		"server",
		"--instance-id", identity,
		lmCacheMPTransferModeArg, lmCacheMPTransferModeLMCacheDriven,
		lmCacheMPL1NonLazyArg,
		lmCacheMPShmNameArg, "",
		"--host", "$(" + lmCacheNodeIPEnv + ")",
		"--port", strconv.FormatInt(int64(server.Port), 10),
		"--http-host", "$(" + lmCacheNodeIPEnv + ")",
		"--http-port", strconv.FormatInt(int64(server.HTTPPort), 10),
		"--chunk-size", strconv.FormatInt(int64(effectiveLMCacheChunkSize(cache.Spec.LMCache)), 10),
		"--l1-size-gb", l1GiB,
		"--eviction-policy", "LRU",
		"--max-gpu-workers", strconv.FormatInt(int64(server.MaxGPUWorkers), 10),
		"--max-cpu-workers", strconv.FormatInt(int64(server.MaxCPUWorkers), 10),
	}
	if l2Adapter != "" {
		args = append(args, "--l2-adapter", l2Adapter)
	}
	env := []corev1.EnvVar{
		{Name: "NVIDIA_VISIBLE_DEVICES", Value: "all"},
		{Name: lmCacheMPServerManagedEnv, Value: lmCacheMPServerManagedValue},
		{Name: lmCacheNodeIPEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"}}},
	}
	for i := range bindingEnv {
		env = UpsertEnv(env, bindingEnv[i])
	}
	container := corev1.Container{
		Name:            lmCacheMPServerContainerName,
		Image:           strings.TrimSpace(server.Image),
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"lmcache"},
		Args:            args,
		Env:             env,
		Resources:       *server.Resources.DeepCopy(),
		VolumeMounts: []corev1.VolumeMount{{
			Name: lmCacheNodeLocalShmVolumeName, MountPath: lmCacheMPShmMountPath,
		}},
		Ports: []corev1.ContainerPort{
			{Name: lmCacheMPServerPortName, ContainerPort: server.Port, HostPort: server.Port, Protocol: corev1.ProtocolTCP},
			{Name: lmCacheMPHTTPPortName, ContainerPort: server.HTTPPort, HostPort: server.HTTPPort, Protocol: corev1.ProtocolTCP},
		},
		StartupProbe:    lmCacheMPHTTPProbeForPort(lmCacheMPHTTPPortName, 3, 40),
		ReadinessProbe:  lmCacheMPHTTPProbeForPort(lmCacheMPHTTPPortName, 5, 3),
		LivenessProbe:   lmCacheMPHTTPProbeForPort(lmCacheMPHTTPPortName, 10, 3),
		SecurityContext: lmCacheMPServerSecurityContext(nil),
	}
	labels := map[string]string{
		"app.kubernetes.io/name":                  "lmcache-mp-server",
		"app.kubernetes.io/managed-by":            "inference-cache-controller",
		enginebinding.LabelLMCacheNodeLocalServer: "true",
		enginebinding.LabelCacheBackendUID:        string(cache.UID),
		enginebinding.LabelLMCacheMPMetrics:       enginebinding.LabelLMCacheMPMetricsEnabled,
	}
	annotations := map[string]string{
		enginebinding.AnnotationNodeLocalOwner:      cache.Namespace + "/" + cache.Name,
		enginebinding.AnnotationNodeLocalOwnerUID:   string(cache.UID),
		enginebinding.AnnotationNodeLocalGeneration: strconv.FormatInt(cache.Generation, 10),
		enginebinding.AnnotationNodeLocalTargetNode: nodeName,
	}
	pathType := corev1.HostPathDirectoryOrCreate
	noToken := false
	enableServiceLinks := false
	grace := int64(30)
	tolerations := append([]corev1.Toleration(nil), source.Spec.Tolerations...)
	imagePullSecrets := append([]corev1.LocalObjectReference(nil), source.Spec.ImagePullSecrets...)
	priorityClassName := source.Spec.PriorityClassName
	schedulerName := source.Spec.SchedulerName
	runtimeClassName := copyStringPtr(source.Spec.RuntimeClassName)
	serviceAccountName := ""
	var podSecurityContext *corev1.PodSecurityContext
	if scheduling != nil {
		tolerations = mergeTolerations(tolerations, scheduling.Tolerations)
		imagePullSecrets = mergeLocalObjectReferences(imagePullSecrets, scheduling.ImagePullSecrets)
		serviceAccountName = scheduling.ServiceAccountName
		podSecurityContext = scheduling.SecurityContext.DeepCopy()
		if scheduling.PriorityClassName != "" {
			priorityClassName = scheduling.PriorityClassName
		}
		if scheduling.SchedulerName != "" {
			schedulerName = scheduling.SchedulerName
		}
		if scheduling.RuntimeClassName != nil {
			runtimeClassName = copyStringPtr(scheduling.RuntimeClassName)
		}
		if scheduling.TerminationGracePeriodSeconds != nil {
			grace = *scheduling.TerminationGracePeriodSeconds
		}
	}
	podSpec := corev1.PodSpec{
		HostNetwork:                   true,
		HostIPC:                       false,
		DNSPolicy:                     corev1.DNSClusterFirstWithHostNet,
		AutomountServiceAccountToken:  &noToken,
		EnableServiceLinks:            &enableServiceLinks,
		Affinity:                      exactNodeAffinity(nodeName),
		Tolerations:                   tolerations,
		ImagePullSecrets:              imagePullSecrets,
		ServiceAccountName:            serviceAccountName,
		SecurityContext:               podSecurityContext,
		PriorityClassName:             priorityClassName,
		SchedulerName:                 schedulerName,
		RuntimeClassName:              runtimeClassName,
		TerminationGracePeriodSeconds: &grace,
		RestartPolicy:                 corev1.RestartPolicyAlways,
		Containers:                    []corev1.Container{container},
		Volumes: []corev1.Volume{{
			Name: lmCacheNodeLocalShmVolumeName,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: shmHostPath, Type: &pathType,
			}},
		}},
	}
	if podSpec.SchedulerName == "" {
		podSpec.SchedulerName = "default-scheduler"
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: NodeLocalServerPodName(cache.Name, nodeName), Namespace: cache.Namespace,
			Labels: copyStringMap(labels), Annotations: copyStringMap(annotations),
		},
		Spec: podSpec,
	}, nil
}

const nodeLocalShmCleanupScript = `import os, stat
root = os.environ["SHM_POOL_PATH"]
def remove_entry(path):
    mode = os.lstat(path).st_mode
    if stat.S_ISDIR(mode):
        with os.scandir(path) as entries:
            for entry in entries:
                remove_entry(entry.path)
        os.rmdir(path)
    else:
        os.unlink(path)
with os.scandir(root) as entries:
    for entry in entries:
        remove_entry(entry.path)
`

func nodeLocalShmHelperSecurityContext() *corev1.SecurityContext {
	no := false
	zero := int64(0)
	readOnlyRoot := true
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: &no,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		ReadOnlyRootFilesystem:   &readOnlyRoot,
		RunAsNonRoot:             &no,
		RunAsUser:                &zero,
		RunAsGroup:               &zero,
		SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

func nodeLocalShmHelperResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
	}
}

// NodeLocalCleanupPodName returns the stable name of one backend/node cleanup
// intent. The name deliberately depends on the immutable backend UID rather
// than the reusable CacheBackend name.
func NodeLocalCleanupPodName(uid types.UID, nodeName string) string {
	sum := sha256.Sum256([]byte(string(uid) + "\x00" + nodeName))
	return fmt.Sprintf("lmcache-shm-cleanup-%x", sum[:12])
}

// RenderLMCacheNodeLocalCleanupPod renders a gated, one-shot cleanup intent.
// It mounts only the exact UID directory; the controller removes the scheduling
// gate after all prior consumers have disappeared.
func RenderLMCacheNodeLocalCleanupPod(cache *cachev1alpha1.CacheBackend, nodeName, image string, source *corev1.Pod) (*corev1.Pod, error) {
	if cache == nil || cache.UID == "" || strings.TrimSpace(nodeName) == "" || strings.TrimSpace(image) == "" || source == nil {
		return nil, fmt.Errorf("render LMCache NodeLocal SHM cleanup Pod: backend UID, target node, image, and scheduling source are required")
	}
	shmHostPath, err := NodeLocalServerShmHostPath(cache)
	if err != nil {
		return nil, err
	}
	image = strings.TrimSpace(image)
	pathType := corev1.HostPathDirectoryOrCreate
	noToken := false
	enableServiceLinks := false
	grace := int64(5)
	cleanup := corev1.Container{
		Name:            lmCacheNodeLocalShmCleanupName,
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"python3", "-c"},
		Args:            []string{nodeLocalShmCleanupScript},
		Env:             []corev1.EnvVar{{Name: "SHM_POOL_PATH", Value: lmCacheNodeLocalShmCleanupPath}},
		Resources:       nodeLocalShmHelperResources(),
		VolumeMounts:    []corev1.VolumeMount{{Name: lmCacheNodeLocalShmVolumeName, MountPath: lmCacheNodeLocalShmCleanupPath}},
		SecurityContext: nodeLocalShmHelperSecurityContext(),
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NodeLocalCleanupPodName(cache.UID, nodeName),
			Namespace: cache.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":                   "lmcache-shm-cleanup",
				"app.kubernetes.io/managed-by":             "inference-cache-controller",
				enginebinding.LabelLMCacheNodeLocalCleanup: "true",
				enginebinding.LabelCacheBackendUID:         string(cache.UID),
			},
			Annotations: map[string]string{
				enginebinding.AnnotationNodeLocalOwner:      cache.Namespace + "/" + cache.Name,
				enginebinding.AnnotationNodeLocalOwnerUID:   string(cache.UID),
				enginebinding.AnnotationNodeLocalTargetNode: nodeName,
			},
		},
		Spec: corev1.PodSpec{
			HostNetwork:                   false,
			HostPID:                       false,
			HostIPC:                       false,
			AutomountServiceAccountToken:  &noToken,
			EnableServiceLinks:            &enableServiceLinks,
			Affinity:                      exactNodeAffinity(nodeName),
			Tolerations:                   append([]corev1.Toleration(nil), source.Spec.Tolerations...),
			ImagePullSecrets:              append([]corev1.LocalObjectReference(nil), source.Spec.ImagePullSecrets...),
			PriorityClassName:             source.Spec.PriorityClassName,
			SchedulerName:                 source.Spec.SchedulerName,
			TerminationGracePeriodSeconds: &grace,
			RestartPolicy:                 corev1.RestartPolicyOnFailure,
			SchedulingGates:               []corev1.PodSchedulingGate{{Name: lmCacheNodeLocalShmCleanupGate}},
			Containers:                    []corev1.Container{cleanup},
			Volumes: []corev1.Volume{{
				Name: lmCacheNodeLocalShmVolumeName,
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: shmHostPath, Type: &pathType,
				}},
			}},
		},
	}, nil
}

// NodeLocalServerPodName returns the stable object name for one backend/node
// pair. It is exported for controller lifecycle reconciliation.
func NodeLocalServerPodName(backendName, nodeName string) string {
	sum := sha256.Sum256([]byte(nodeName))
	suffix := fmt.Sprintf("%x", sum[:6])
	const maxName = 253
	maxPrefix := maxName - len(suffix) - 1
	prefix := strings.TrimRight(backendName, ".-")
	if len(prefix) > maxPrefix {
		prefix = strings.TrimRight(prefix[:maxPrefix], ".-")
	}
	return prefix + "-" + suffix
}

func nodeLocalBackendUID(cache *cachev1alpha1.CacheBackend) (string, error) {
	if cache == nil || cache.UID == "" {
		return "", fmt.Errorf("derive LMCache NodeLocal host path: CacheBackend UID is empty")
	}
	uid := string(cache.UID)
	for _, char := range uid {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return "", fmt.Errorf("derive LMCache NodeLocal host path: CacheBackend UID contains unsafe character %q", char)
	}
	if len(uid) > hostPathComponentMaxLength {
		return "", fmt.Errorf("derive LMCache NodeLocal host path: %d-byte UID exceeds path-component limit %d", len(uid), hostPathComponentMaxLength)
	}
	return uid, nil
}

// NodeLocalServerShmHostPath returns the host tmpfs directory mounted as
// /dev/shm by one CacheBackend's NodeLocal servers and engines. Mounting only
// the UID directory keeps CUDA/PyTorch auxiliary IPC objects from normally
// behaving co-located pools out of each other's directory while retaining the
// node-local CUDA IPC path. The LMCache L1 itself is private pinned memory.
func NodeLocalServerShmHostPath(cache *cachev1alpha1.CacheBackend) (string, error) {
	uid, err := nodeLocalBackendUID(cache)
	if err != nil {
		return "", err
	}
	return lmCacheNodeLocalShmHostRoot + "/" + uid, nil
}

func exactNodeAffinity(nodeName string) *corev1.Affinity {
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{nodeName}}},
		}}},
	}}
}

func mergeTolerations(base, extra []corev1.Toleration) []corev1.Toleration {
	out := append([]corev1.Toleration(nil), base...)
	for i := range extra {
		found := false
		for j := range out {
			if equality.Semantic.DeepEqual(out[j], extra[i]) {
				found = true
				break
			}
		}
		if !found {
			out = append(out, extra[i])
		}
	}
	return out
}

func mergeLocalObjectReferences(base, extra []corev1.LocalObjectReference) []corev1.LocalObjectReference {
	out := append([]corev1.LocalObjectReference(nil), base...)
	seen := make(map[string]struct{}, len(out))
	for i := range out {
		seen[out[i].Name] = struct{}{}
	}
	for i := range extra {
		if _, found := seen[extra[i].Name]; found {
			continue
		}
		seen[extra[i].Name] = struct{}{}
		out = append(out, extra[i])
	}
	return out
}

func validateLMCacheNodeLocalServerConfig(server *cachev1alpha1.LMCacheNodeLocalServerSpec, chunkSize int32) error {
	if server == nil {
		return fmt.Errorf("render LMCache NodeLocal server: configuration is nil")
	}
	if strings.TrimSpace(server.Image) == "" {
		return fmt.Errorf("render LMCache NodeLocal server: image is empty")
	}
	ports := []int32{server.Port, server.HTTPPort}
	seen := map[int32]struct{}{}
	for _, port := range ports {
		if port < 1 || port > 65535 {
			return fmt.Errorf("render LMCache NodeLocal server: port %d is outside 1-65535", port)
		}
		if _, duplicate := seen[port]; duplicate {
			return fmt.Errorf("render LMCache NodeLocal server: host ports must be distinct")
		}
		seen[port] = struct{}{}
	}
	if chunkSize < 1 || server.L1Capacity.Sign() <= 0 || server.MaxGPUWorkers < 1 || server.MaxCPUWorkers < 1 {
		return fmt.Errorf("render LMCache NodeLocal server: chunk size, L1 capacity, and worker counts must be positive")
	}
	return nil
}

func lmCacheNodeLocalInstanceID(cache *cachev1alpha1.CacheBackend) string {
	return fmt.Sprintf("%s/%s@%s#%d", cache.Namespace, cache.Name, cache.UID, cache.Generation)
}

func lmCacheMPHTTPProbeForPort(portName string, period, failures int32) *corev1.Probe {
	probe := lmCacheMPHTTPProbe(period, failures)
	probe.HTTPGet.Port.StrVal = portName
	return probe
}

// renderLMCacheNodeLocalEngine injects the backend-UID-scoped host SHM mount
// and a startup gate. It deliberately leaves every engine placement field
// unchanged; the controller follows the engine onto its scheduled node.
func renderLMCacheNodeLocalEngine(pod *corev1.PodSpec, engineContainerName string, cache *cachev1alpha1.CacheBackend, writeClientConfig bool) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("render LMCache NodeLocal engine: pod spec is nil")
	}
	engineIndex, err := EngineContainerIndexNamed(pod, engineContainerName)
	if err != nil {
		return "", err
	}
	if cache == nil || cache.Spec.LMCache == nil || cache.Spec.LMCache.NodeLocal == nil ||
		cache.Spec.LMCache.NodeLocal.Server == nil || cache.UID == "" {
		return "", fmt.Errorf("render LMCache NodeLocal engine: complete typed NodeLocal configuration and CacheBackend UID are required")
	}
	server := cache.Spec.LMCache.NodeLocal.Server
	if err := validateLMCacheNodeLocalServerConfig(server, effectiveLMCacheChunkSize(cache.Spec.LMCache)); err != nil {
		return "", err
	}
	shmHostPath, err := NodeLocalServerShmHostPath(cache)
	if err != nil {
		return "", err
	}
	work := pod.DeepCopy()
	engine := &work.Containers[engineIndex]
	owned := lmCacheNodeLocalWireIsOurs(pod)
	nodeIPEnv := corev1.EnvVar{
		Name: lmCacheNodeIPEnv,
		ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
			FieldPath: "status.hostIP",
		}},
	}
	for i := range engine.Env {
		if engine.Env[i].Name == lmCacheNodeIPEnv && !owned && !equality.Semantic.DeepEqual(engine.Env[i], nodeIPEnv) {
			return "", fmt.Errorf("render LMCache NodeLocal engine: environment variable %q is reserved for the node address", lmCacheNodeIPEnv)
		}
	}
	engine.Env = UpsertEnv(engine.Env, nodeIPEnv)

	shmMount := corev1.VolumeMount{Name: lmCacheNodeLocalShmVolumeName, MountPath: lmCacheMPShmMountPath}
	if existing := mountAtPath(engine.VolumeMounts, lmCacheMPShmMountPath); existing != nil {
		if err := checkNodeLocalHostShm(work.Volumes, *existing, shmHostPath); err != nil {
			return "", err
		}
		shmMount.Name = existing.Name
	} else {
		pathType := corev1.HostPathDirectoryOrCreate
		work.Volumes, err = adoptVolume(work.Volumes, corev1.Volume{
			Name: lmCacheNodeLocalShmVolumeName,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: shmHostPath, Type: &pathType,
			}},
		}, owned)
		if err != nil {
			return "", err
		}
		engine.VolumeMounts = upsertMountByName(engine.VolumeMounts, shmMount)
	}

	configPath := ""
	gateMounts := []corev1.VolumeMount{}
	if writeClientConfig {
		configPath = lmCacheNodeLocalConfigFilePath
		if existing := mountAtPath(engine.VolumeMounts, lmCacheNodeLocalConfigMountPath); existing != nil &&
			!(owned && existing.Name == lmCacheNodeLocalConfigVolumeName) {
			return "", fmt.Errorf("render LMCache NodeLocal engine: engine already mounts reserved config path %q", lmCacheNodeLocalConfigMountPath)
		}
		work.Volumes, err = adoptVolume(work.Volumes, corev1.Volume{
			Name:         lmCacheNodeLocalConfigVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		}, owned)
		if err != nil {
			return "", err
		}
		configMount := corev1.VolumeMount{Name: lmCacheNodeLocalConfigVolumeName, MountPath: lmCacheNodeLocalConfigMountPath}
		engine.VolumeMounts = upsertMountByName(engine.VolumeMounts, configMount)
		gateMounts = append(gateMounts, configMount)
	}

	gate := lmCacheNodeLocalGateContainer(cache, configPath, gateMounts)
	work.InitContainers, err = adoptNodeLocalGate(work.InitContainers, gate, owned)
	if err != nil {
		return "", err
	}
	*pod = *work
	return configPath, nil
}

func lmCacheNodeLocalGateContainer(cache *cachev1alpha1.CacheBackend, configPath string, mounts []corev1.VolumeMount) corev1.Container {
	server := cache.Spec.LMCache.NodeLocal.Server
	const gateScript = `import json, os, time, urllib.request
ip = os.environ["INFERENCECACHE_NODE_IP"]
host = "[" + ip + "]" if ":" in ip else ip
base = "http://%s:%s" % (host, os.environ["EXPECTED_HTTP_PORT"])
expected = {
    "instance_id": os.environ["EXPECTED_INSTANCE_ID"],
    "supported_transfer_mode": "lmcache_driven",
    "shm_name": "",
    "port": int(os.environ["EXPECTED_MP_PORT"]),
    "chunk_size": int(os.environ["EXPECTED_CHUNK_SIZE"]),
    "max_gpu_workers": int(os.environ["EXPECTED_MAX_GPU_WORKERS"]),
    "max_cpu_workers": int(os.environ["EXPECTED_MAX_CPU_WORKERS"]),
}
while True:
    try:
        with urllib.request.urlopen(base + "/config", timeout=2) as response:
            config = json.load(response)
        mp = config.get("mp", {})
        http = config.get("http", {})
        for key, value in expected.items():
            if mp.get(key) != value:
                raise RuntimeError("server config %s=%r, expected %r" % (key, mp.get(key), value))
        memory = config.get("storage_manager", {}).get("l1_manager_config", {}).get("memory_config", {})
        if memory.get("use_lazy") is not False:
            raise RuntimeError("effective L1 use_lazy=%r, expected False" % memory.get("use_lazy"))
        if memory.get("shm_name") != "":
            raise RuntimeError("effective L1 shm_name=%r, expected empty" % memory.get("shm_name"))
        if http.get("http_port") != int(os.environ["EXPECTED_HTTP_PORT"]):
            raise RuntimeError("server HTTP port does not match")
        with urllib.request.urlopen(base + "/healthcheck", timeout=2) as response:
            health = json.load(response)
        if health.get("status") != "healthy":
            raise RuntimeError("server health is %r" % health)
        config_path = os.environ.get("CLIENT_CONFIG_PATH", "")
        if config_path:
            tmp = config_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as output:
                # LMCache's SGLang MP adapter adds the tcp:// scheme itself;
                # unlike vLLM's connector JSON, this YAML field is a bare host.
                output.write('chunk_size: %s\nmp_host: "%s"\nmp_port: %s\n' % (expected["chunk_size"], ip, expected["port"]))
            os.replace(tmp, config_path)
        print("verified healthy same-node LMCache server " + expected["instance_id"], flush=True)
        break
    except Exception as exc:
        print("waiting for ownership-verified same-node LMCache server: %s" % exc, flush=True)
        time.sleep(2)
`
	return corev1.Container{
		Name:            lmCacheNodeLocalGateContainerName,
		Image:           strings.TrimSpace(server.Image),
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"python3", "-c"},
		Args:            []string{gateScript},
		Env: []corev1.EnvVar{
			{Name: lmCacheNodeLocalGateManagedEnv, Value: lmCacheNodeLocalGateManagedValue},
			{Name: lmCacheNodeIPEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"}}},
			{Name: "EXPECTED_INSTANCE_ID", Value: lmCacheNodeLocalInstanceID(cache)},
			{Name: "EXPECTED_MP_PORT", Value: strconv.FormatInt(int64(server.Port), 10)},
			{Name: "EXPECTED_HTTP_PORT", Value: strconv.FormatInt(int64(server.HTTPPort), 10)},
			{Name: "EXPECTED_CHUNK_SIZE", Value: strconv.FormatInt(int64(effectiveLMCacheChunkSize(cache.Spec.LMCache)), 10)},
			{Name: "EXPECTED_MAX_GPU_WORKERS", Value: strconv.FormatInt(int64(server.MaxGPUWorkers), 10)},
			{Name: "EXPECTED_MAX_CPU_WORKERS", Value: strconv.FormatInt(int64(server.MaxCPUWorkers), 10)},
			{Name: "CLIENT_CONFIG_PATH", Value: configPath},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
		},
		VolumeMounts:    mounts,
		SecurityContext: lmCacheMPServerSecurityContext(nil),
	}
}

func checkNodeLocalHostShm(volumes []corev1.Volume, mount corev1.VolumeMount, wantHostPath string) error {
	if mount.ReadOnly || mount.SubPath != "" || mount.SubPathExpr != "" {
		return fmt.Errorf("render LMCache NodeLocal engine: /dev/shm must be a writable whole-volume hostPath mount")
	}
	for i := range volumes {
		if volumes[i].Name != mount.Name {
			continue
		}
		hostPath := volumes[i].HostPath
		if hostPath == nil || hostPath.Path != wantHostPath || hostPath.Type == nil || *hostPath.Type != corev1.HostPathDirectoryOrCreate {
			return fmt.Errorf("render LMCache NodeLocal engine: /dev/shm must use UID-scoped hostPath %q with type DirectoryOrCreate for cross-Pod CUDA IPC", wantHostPath)
		}
		return nil
	}
	return fmt.Errorf("render LMCache NodeLocal engine: /dev/shm mount references missing volume %q", mount.Name)
}

func lmCacheNodeLocalWireIsOurs(pod *corev1.PodSpec) bool {
	if pod == nil {
		return false
	}
	gate := findContainerByName(pod.InitContainers, lmCacheNodeLocalGateContainerName)
	if gate == nil {
		return false
	}
	for i := range gate.Env {
		if gate.Env[i].Name == lmCacheNodeLocalGateManagedEnv && gate.Env[i].Value == lmCacheNodeLocalGateManagedValue {
			return true
		}
	}
	return false
}

func adoptNodeLocalGate(containers []corev1.Container, want corev1.Container, owned bool) ([]corev1.Container, error) {
	for i := range containers {
		if containers[i].Name != want.Name {
			continue
		}
		if !owned {
			return nil, fmt.Errorf("render LMCache NodeLocal engine: init container name %q is reserved for the ownership-verification gate", want.Name)
		}
		containers[i] = want
		return containers, nil
	}
	return append(containers, want), nil
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func copyStringPtr(in *string) *string {
	if in == nil {
		return nil
	}
	out := new(string)
	*out = *in
	return out
}
