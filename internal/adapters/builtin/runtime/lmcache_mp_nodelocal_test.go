// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

func newNodeLocalBackend(runtime cachev1alpha1.CacheBackendRuntime) *cachev1alpha1.CacheBackend {
	chunk := int32(256)
	runtimeClass := "nvidia"
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "node-cache", Namespace: "team-a", UID: types.UID("11111111-2222-3333-4444-555555555555"), Generation: 7},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: runtime,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			LMCache: &cachev1alpha1.LMCacheEngineSpec{
				Topology:        cachev1alpha1.LMCacheTopologyNodeLocal,
				ChunkSizeTokens: &chunk,
				NodeLocal: &cachev1alpha1.LMCacheNodeLocalSpec{
					Server: &cachev1alpha1.LMCacheNodeLocalServerSpec{
						Image:         testLMCacheServerImage,
						Port:          6555,
						HTTPPort:      18080,
						L1Capacity:    resource.MustParse("8Gi"),
						MaxGPUWorkers: 4,
						MaxCPUWorkers: 8,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("9Gi")},
							Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("10Gi")},
						},
					},
					Scheduling: &cachev1alpha1.LMCacheNodeLocalSchedulingSpec{
						RuntimeClassName: &runtimeClass,
					},
				},
			},
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{
				"app": "engine",
			}},
		},
	}
}

func nodeLocalSourceEngine() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-a", Namespace: "team-a"}, Spec: corev1.PodSpec{
		NodeName: "gpu-node-a", Tolerations: []corev1.Toleration{{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists}},
		Containers: []corev1.Container{{Name: EngineContainerName}},
	}}
}

func TestRenderLMCacheNodeLocalServerPod(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	serverPod, err := RenderLMCacheNodeLocalServerPod(cache, nil, "gpu-node-a", nodeLocalSourceEngine())
	if err != nil {
		t.Fatalf("RenderLMCacheNodeLocalServerPod: %v", err)
	}
	if serverPod.Name != NodeLocalServerPodName(cache.Name, "gpu-node-a") || serverPod.Namespace != cache.Namespace {
		t.Fatalf("server Pod identity = %s/%s", serverPod.Namespace, serverPod.Name)
	}
	pod := serverPod.Spec
	if !pod.HostNetwork || pod.HostIPC || pod.DNSPolicy != corev1.DNSClusterFirstWithHostNet {
		t.Fatalf("host boundary = hostNetwork:%v hostIPC:%v dns:%s", pod.HostNetwork, pod.HostIPC, pod.DNSPolicy)
	}
	if pod.RuntimeClassName == nil || *pod.RuntimeClassName != "nvidia" || len(pod.NodeSelector) != 0 ||
		pod.Affinity == nil || pod.Affinity.NodeAffinity == nil {
		t.Fatalf("placement = runtimeClass:%v nodeSelector:%v affinity:%+v", pod.RuntimeClassName, pod.NodeSelector, pod.Affinity)
	}
	required := pod.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if required == nil || len(required.NodeSelectorTerms) != 1 || len(required.NodeSelectorTerms[0].MatchFields) != 1 ||
		required.NodeSelectorTerms[0].MatchFields[0].Key != "metadata.name" ||
		!reflect.DeepEqual(required.NodeSelectorTerms[0].MatchFields[0].Values, []string{"gpu-node-a"}) {
		t.Fatalf("exact-node scheduler affinity = %+v", required)
	}
	wantShmPath := "/dev/shm/inference-cache/11111111-2222-3333-4444-555555555555"
	if len(pod.Volumes) != 1 || pod.Volumes[0].HostPath == nil || pod.Volumes[0].HostPath.Path != wantShmPath ||
		pod.Volumes[0].HostPath.Type == nil || *pod.Volumes[0].HostPath.Type != corev1.HostPathDirectoryOrCreate {
		t.Fatalf("volumes = %+v, want UID-scoped hostPath %q", pod.Volumes, wantShmPath)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %d, want one", len(pod.Containers))
	}
	server := pod.Containers[0]
	if len(server.Ports) != 2 {
		t.Fatalf("ports = %+v", server.Ports)
	}
	for _, port := range server.Ports {
		if port.HostPort != port.ContainerPort || port.HostPort == 0 {
			t.Fatalf("port does not declare matching hostPort: %+v", port)
		}
	}
	args := strings.Join(server.Args, " ")
	for _, want := range []string{
		"--instance-id team-a/node-cache@11111111-2222-3333-4444-555555555555#7",
		"--host $(INFERENCECACHE_NODE_IP)", "--http-port 18080",
		"--max-gpu-workers 4", "--max-cpu-workers 8",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("server args %q missing %q", args, want)
		}
	}
	if !IsLMCacheMPCUDAServerProfile(server.Args) {
		t.Fatalf("server args do not explicitly select the CUDA/private-pinned-L1 profile: %v", server.Args)
	}
	if got := serverPod.Annotations[enginebinding.AnnotationNodeLocalOwnerUID]; got != string(cache.UID) {
		t.Fatalf("owner UID annotation = %q", got)
	}
	if got := serverPod.Labels[enginebinding.LabelCacheBackendUID]; got != string(cache.UID) {
		t.Fatalf("owner UID label = %q", got)
	}
	if got := serverPod.Annotations[enginebinding.AnnotationNodeLocalTargetNode]; got != "gpu-node-a" {
		t.Fatalf("target-node annotation = %q", got)
	}
	if _, found := serverPod.Annotations["inferencecache.io/node-local-shm-name"]; found {
		t.Fatalf("obsolete POSIX SHM identity annotation remains: %v", serverPod.Annotations)
	}
}

func TestRenderLMCacheNodeLocalCleanupPod(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	cache.Spec.LMCache.NodeLocal.Scheduling = &cachev1alpha1.LMCacheNodeLocalSchedulingSpec{
		ImagePullSecrets: []corev1.LocalObjectReference{{Name: "cache-pull"}},
	}
	source := nodeLocalSourceEngine()
	source.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "engine-pull"}}
	cleanup, err := RenderLMCacheNodeLocalCleanupPod(cache, "gpu-node-a", testNodeLocalCleanupImage, source)
	if err != nil {
		t.Fatal(err)
	}
	if cleanup.Name != NodeLocalCleanupPodName(cache.UID, "gpu-node-a") || cleanup.Spec.NodeName != "" || len(cleanup.Spec.SchedulingGates) != 1 {
		t.Fatalf("cleanup identity/initial gate = name:%q node:%q gates:%+v", cleanup.Name, cleanup.Spec.NodeName, cleanup.Spec.SchedulingGates)
	}
	if cleanup.Spec.HostNetwork || cleanup.Spec.HostPID || cleanup.Spec.HostIPC || cleanup.Spec.AutomountServiceAccountToken == nil || *cleanup.Spec.AutomountServiceAccountToken {
		t.Fatalf("cleanup host boundary = %+v", cleanup.Spec)
	}
	if cleanup.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("cleanup restart policy = %q, want Never so controller retries terminal failures", cleanup.Spec.RestartPolicy)
	}
	if len(cleanup.Spec.Volumes) != 1 || cleanup.Spec.Volumes[0].HostPath == nil ||
		cleanup.Spec.Volumes[0].HostPath.Path != "/dev/shm/inference-cache/11111111-2222-3333-4444-555555555555" {
		t.Fatalf("cleanup volumes = %+v", cleanup.Spec.Volumes)
	}
	if len(cleanup.Spec.ImagePullSecrets) != 1 || cleanup.Spec.ImagePullSecrets[0].Name != "cache-pull" {
		t.Fatalf("cleanup imagePullSecrets = %+v", cleanup.Spec.ImagePullSecrets)
	}
	container := cleanup.Spec.Containers[0]
	if container.Image != testNodeLocalCleanupImage || !reflect.DeepEqual(container.Command, []string{"/node-local-shm-cleanup"}) ||
		!reflect.DeepEqual(container.Args, []string{lmCacheNodeLocalShmCleanupPath}) {
		t.Fatalf("cleanup container = image:%q command:%v args:%v", container.Image, container.Command, container.Args)
	}
	security := container.SecurityContext
	if security == nil || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation ||
		security.RunAsUser == nil || *security.RunAsUser != 0 || security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem {
		t.Fatalf("cleanup security context = %+v", security)
	}
	controller := true
	cleanup.OwnerReferences = []metav1.OwnerReference{{UID: cache.UID, Controller: &controller}}
	if !IsLMCacheNodeLocalCleanupPod(cleanup, cache, "gpu-node-a", testNodeLocalCleanupImage) {
		t.Fatal("rendered cleanup Pod did not satisfy its executable contract")
	}
	cleanup.Spec.Containers[0].Command = []string{"/bin/true"}
	if IsLMCacheNodeLocalCleanupPod(cleanup, cache, "gpu-node-a", testNodeLocalCleanupImage) {
		t.Fatal("cleanup Pod with a foreign command satisfied the executable contract")
	}
}

func TestLMCacheNodeLocalCleanupSucceededRequiresHelperStatus(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{NodeName: "gpu-node-a"}, Status: corev1.PodStatus{
		Phase: corev1.PodSucceeded,
		ContainerStatuses: []corev1.ContainerStatus{{Name: "foreign", State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
		}}},
	}}
	if LMCacheNodeLocalCleanupSucceeded(pod, "gpu-node-a") {
		t.Fatal("foreign successful container was accepted as cleanup success")
	}
	pod.Status.ContainerStatuses[0].Name = lmCacheNodeLocalShmCleanupName
	if !LMCacheNodeLocalCleanupSucceeded(pod, "gpu-node-a") {
		t.Fatal("successful cleanup helper status was rejected")
	}
}

func TestRenderLMCacheNodeLocalCleanupRetryPod(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	cleanup, err := RenderLMCacheNodeLocalCleanupPod(cache, "gpu-node-a", testNodeLocalCleanupImage, nodeLocalSourceEngine())
	if err != nil {
		t.Fatal(err)
	}
	cleanup.Spec.SchedulingGates = nil
	cleanup.Spec.NodeName = "gpu-node-a"
	retry, err := RenderLMCacheNodeLocalCleanupRetryPod(cache, cleanup)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Name != NodeLocalCleanupRetryPodName(cache.UID, "gpu-node-a", cleanup.Name) ||
		!IsNodeLocalCleanupPodName(cache.UID, "gpu-node-a", retry.Name) || retry.Spec.NodeName != "" || len(retry.Spec.SchedulingGates) != 1 {
		t.Fatalf("cleanup retry identity/gate = name:%q node:%q gates:%+v", retry.Name, retry.Spec.NodeName, retry.Spec.SchedulingGates)
	}
	if retry.Spec.Containers[0].Image != cleanup.Spec.Containers[0].Image || retry.Spec.Volumes[0].HostPath.Path != cleanup.Spec.Volumes[0].HostPath.Path {
		t.Fatalf("cleanup retry changed image or hostPath: image:%q volumes:%+v", retry.Spec.Containers[0].Image, retry.Spec.Volumes)
	}
}

func TestNodeLocalCleanupRejectsInvalidInputs(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	base := NodeLocalCleanupPodName(cache.UID, "gpu-node-a")
	if IsNodeLocalCleanupPodName(cache.UID, "gpu-node-a", base+"-retry-short") {
		t.Fatal("short cleanup retry suffix was accepted")
	}
	if IsNodeLocalCleanupPodName(cache.UID, "gpu-node-a", base+"-retry-zzzzzzzzzzzz") {
		t.Fatal("non-hex cleanup retry suffix was accepted")
	}
	if _, err := RenderLMCacheNodeLocalCleanupPod(nil, "gpu-node-a", testNodeLocalCleanupImage, nodeLocalSourceEngine()); err == nil {
		t.Fatal("nil backend was accepted for cleanup rendering")
	}
	unsafeUID := cache.DeepCopy()
	unsafeUID.UID = "unsafe/uid"
	if _, err := RenderLMCacheNodeLocalCleanupPod(unsafeUID, "gpu-node-a", testNodeLocalCleanupImage, nodeLocalSourceEngine()); err == nil {
		t.Fatal("unsafe backend UID was accepted for cleanup rendering")
	}
	if _, err := RenderLMCacheNodeLocalCleanupRetryPod(nil, nil); err == nil {
		t.Fatal("nil backend and failed Pod were accepted for retry rendering")
	}
	failed := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "failed"}}
	if _, err := RenderLMCacheNodeLocalCleanupRetryPod(cache, failed); err == nil {
		t.Fatal("cleanup retry without target node was accepted")
	}
	if _, err := NodeLocalServerShmHostPath(nil); err == nil {
		t.Fatal("nil backend produced a NodeLocal SHM hostPath")
	}
}

func TestRenderLMCacheNodeLocalServerPodWithRedisL2(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	pod, err := RenderLMCacheNodeLocalServerPod(cache, &backendadapter.Binding{
		Protocol: backendadapter.ProtocolRESP,
		Endpoint: "redis.team-a.svc.cluster.local:6379",
	}, "gpu-node-a", nodeLocalSourceEngine())
	if err != nil {
		t.Fatalf("RenderLMCacheNodeLocalServerPod: %v", err)
	}
	server := pod.Spec.Containers[0]
	args := strings.Join(server.Args, " ")
	if !strings.Contains(args, "--l2-adapter") || !strings.Contains(args, "redis.team-a.svc.cluster.local") || !strings.Contains(args, `"port":6379`) {
		t.Fatalf("server args = %v, want typed Redis L2 adapter", server.Args)
	}
}

func TestRenderLMCacheNodeLocalServerPodSchedulingOverridesAndMerges(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	grace := int64(9)
	runtimeClass := "cache-nvidia"
	runAsNonRoot := true
	cache.Spec.LMCache.NodeLocal.Scheduling = &cachev1alpha1.LMCacheNodeLocalSchedulingSpec{
		Tolerations: []corev1.Toleration{
			{Key: "source", Operator: corev1.TolerationOpExists},
			{Key: "cache", Operator: corev1.TolerationOpExists},
		},
		ImagePullSecrets:              []corev1.LocalObjectReference{{Name: "source-pull"}, {Name: "cache-pull"}},
		ServiceAccountName:            "cache-server",
		SecurityContext:               &corev1.PodSecurityContext{RunAsNonRoot: &runAsNonRoot},
		PriorityClassName:             "cache-priority",
		SchedulerName:                 "cache-scheduler",
		RuntimeClassName:              &runtimeClass,
		TerminationGracePeriodSeconds: &grace,
	}
	source := nodeLocalSourceEngine()
	source.Spec.Tolerations = []corev1.Toleration{{Key: "source", Operator: corev1.TolerationOpExists}}
	source.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "source-pull"}}
	source.Spec.PriorityClassName = "engine-priority"
	source.Spec.SchedulerName = "engine-scheduler"

	server, err := RenderLMCacheNodeLocalServerPod(cache, nil, "gpu-node-a", source)
	if err != nil {
		t.Fatalf("RenderLMCacheNodeLocalServerPod: %v", err)
	}
	got := server.Spec
	if len(got.Tolerations) != 2 || len(got.ImagePullSecrets) != 2 {
		t.Fatalf("merged scheduling = tolerations:%+v pullSecrets:%+v", got.Tolerations, got.ImagePullSecrets)
	}
	if got.ServiceAccountName != "cache-server" || got.SecurityContext == nil || got.SecurityContext.RunAsNonRoot == nil || !*got.SecurityContext.RunAsNonRoot ||
		got.PriorityClassName != "cache-priority" || got.SchedulerName != "cache-scheduler" || got.RuntimeClassName == nil || *got.RuntimeClassName != runtimeClass ||
		got.TerminationGracePeriodSeconds == nil || *got.TerminationGracePeriodSeconds != grace {
		t.Fatalf("server scheduling overrides = %+v", got)
	}
}

func TestNodeLocalServerPodNameIsStableAndBounded(t *testing.T) {
	nameA := NodeLocalServerPodName(strings.Repeat("cache", 70), "gpu-node-a")
	nameB := NodeLocalServerPodName(strings.Repeat("cache", 70), "gpu-node-b")
	if len(nameA) > 253 || nameA == nameB || nameA != NodeLocalServerPodName(strings.Repeat("cache", 70), "gpu-node-a") {
		t.Fatalf("server names are not stable, bounded, and node-distinct: %q %q", nameA, nameB)
	}
}

func TestNodeLocalServerShmHostPathIsStableAndUIDScoped(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	want := "/dev/shm/inference-cache/11111111-2222-3333-4444-555555555555"
	got, err := NodeLocalServerShmHostPath(cache)
	if err != nil || got != want {
		t.Fatalf("NodeLocalServerShmHostPath = %q, %v; want %q", got, err, want)
	}
	cache.Generation++
	stable, err := NodeLocalServerShmHostPath(cache)
	if err != nil || stable != want {
		t.Fatalf("same-UID replacement host path = %q, %v; want %q", stable, err, want)
	}
	cache.UID = types.UID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	distinct, err := NodeLocalServerShmHostPath(cache)
	if err != nil || distinct == want {
		t.Fatalf("different-UID host path = %q, %v; must differ from %q", distinct, err, want)
	}
}

func TestNodeLocalServerShmHostPathRejectsUnsafeUID(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	cache.UID = types.UID("unsafe/uid")
	if _, err := NodeLocalServerShmHostPath(cache); err == nil || !strings.Contains(err.Error(), "unsafe character") {
		t.Fatalf("unsafe UID error = %v", err)
	}
	cache.UID = types.UID(strings.Repeat("a", hostPathComponentMaxLength+1))
	if _, err := NodeLocalServerShmHostPath(cache); err == nil || !strings.Contains(err.Error(), "exceeds path-component limit") {
		t.Fatalf("oversized UID error = %v", err)
	}
}

func TestVLLMNodeLocalEngineInjection(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	pod := newVLLMMPEnginePod("--tensor-parallel-size", "1")
	pod.Spec.NodeSelector = map[string]string{"inference-system.io/pool": "owned"}
	pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{Weight: 1}}}}
	wantSelector := map[string]string{"inference-system.io/pool": "owned"}
	wantAffinity := pod.Spec.Affinity.DeepCopy()
	adapter := NewVLLMLMCacheMPAdapter(SubscriberConfig{})
	if err := adapter.InjectEngineConfig(&pod.Spec, nil, cache); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	if pod.Spec.HostNetwork || pod.Spec.HostIPC {
		t.Fatalf("engine unexpectedly entered host network/IPC: %+v", pod.Spec)
	}
	if !reflect.DeepEqual(pod.Spec.NodeSelector, wantSelector) || !reflect.DeepEqual(pod.Spec.Affinity, wantAffinity) {
		t.Fatalf("engine-owned placement was mutated: selector=%v affinity=%+v", pod.Spec.NodeSelector, pod.Spec.Affinity)
	}
	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != lmCacheNodeLocalGateContainerName {
		t.Fatalf("init containers = %+v", pod.Spec.InitContainers)
	}
	if findContainerByName(pod.Spec.InitContainers, lmCacheMPServerContainerName) != nil {
		t.Fatal("NodeLocal engine received a PodLocal native sidecar")
	}
	engine := pod.Spec.Containers[0]
	if !envHasFieldRef(engine.Env, lmCacheNodeIPEnv, "status.hostIP") {
		t.Fatalf("engine node IP env = %+v", engine.Env)
	}
	joined := strings.Join(engine.Args, " ")
	if !strings.Contains(joined, `tcp://$(INFERENCECACHE_NODE_IP)`) || !strings.Contains(joined, `"lmcache.mp.port":"6555"`) ||
		!strings.Contains(joined, `"lmcache.mp.mp_transfer_mode":"lmcache_driven"`) {
		t.Fatalf("vLLM args do not carry node-derived endpoint: %s", joined)
	}
	shmMount := mountAtPath(engine.VolumeMounts, "/dev/shm")
	if shmMount == nil {
		t.Fatalf("engine mounts = %+v, want host /dev/shm", engine.VolumeMounts)
	}
	var shmVolume *corev1.Volume
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == shmMount.Name {
			shmVolume = &pod.Spec.Volumes[i]
			break
		}
	}
	if shmVolume == nil || shmVolume.HostPath == nil || shmVolume.HostPath.Path != "/dev/shm/inference-cache/11111111-2222-3333-4444-555555555555" ||
		shmVolume.HostPath.Type == nil || *shmVolume.HostPath.Type != corev1.HostPathDirectoryOrCreate {
		t.Fatalf("engine SHM volume = %+v, want backend UID directory", shmVolume)
	}
	gate := pod.Spec.InitContainers[0]
	if !strings.Contains(gate.Args[0], "/config") || !strings.Contains(gate.Args[0], "EXPECTED_INSTANCE_ID") ||
		!strings.Contains(gate.Args[0], `"supported_transfer_mode": "lmcache_driven"`) ||
		!strings.Contains(gate.Args[0], `memory.get("use_lazy") is not False`) ||
		!strings.Contains(gate.Args[0], `memory.get("shm_name") != ""`) {
		t.Fatalf("gate script does not verify live server identity: %q", gate.Args[0])
	}
	if _, found := lookupEnv(gate.Env, "EXPECTED_SHM_NAME"); found {
		t.Fatal("gate still carries obsolete UID-derived POSIX SHM identity")
	}

	before := pod.Spec.DeepCopy()
	if err := adapter.InjectEngineConfig(&pod.Spec, nil, cache); err != nil {
		t.Fatalf("idempotent InjectEngineConfig: %v", err)
	}
	if !reflect.DeepEqual(before, &pod.Spec) {
		t.Fatal("NodeLocal vLLM injection is not idempotent")
	}
}

func TestSGLangNodeLocalEngineInjectionWritesEngineSpecificConfig(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeSGLang)
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
		Name: SGLangEngineContainerName, Image: "sglang:lmcache", Args: []string{"--page-size", "64"},
	}}}}
	adapter := NewSGLangLMCacheAdapter(SubscriberConfig{})
	if err := adapter.InjectEngineConfig(&pod.Spec, nil, cache); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	engine := pod.Spec.Containers[0]
	if !hasArg(engine.Args, SGLangEnableLMCacheArg) || !strings.Contains(strings.Join(engine.Args, " "), lmCacheNodeLocalConfigFilePath) {
		t.Fatalf("SGLang args = %v", engine.Args)
	}
	gate := findContainerByName(pod.Spec.InitContainers, lmCacheNodeLocalGateContainerName)
	if gate == nil || !strings.Contains(gate.Args[0], `mp_host: "%s"`) || strings.Contains(gate.Args[0], `mp_host: "tcp://%s"`) {
		t.Fatalf("SGLang config-writing gate = %+v", gate)
	}
}

func TestNodeLocalEnginePlacementIsPreserved(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	cache.Spec.LMCache.NodeLocal.Scheduling = nil
	pod := newVLLMMPEnginePod()
	pod.Spec.NodeSelector = map[string]string{"inferencecache.io/lmcache-mp": "false"}
	if err := NewVLLMLMCacheMPAdapter(SubscriberConfig{}).InjectEngineConfig(&pod.Spec, nil, cache); err != nil {
		t.Fatalf("inference-owned selector should not conflict: %v", err)
	}
	if !reflect.DeepEqual(pod.Spec.NodeSelector, map[string]string{"inferencecache.io/lmcache-mp": "false"}) {
		t.Fatalf("engine nodeSelector mutated: %v", pod.Spec.NodeSelector)
	}
}

func TestSGLangNodeLocalEngineInjectionDoesNotRequireSchedulingOverrides(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeSGLang)
	cache.Spec.LMCache.NodeLocal.Scheduling = nil
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
		Name: SGLangEngineContainerName, Image: "sglang:lmcache", Args: []string{"--page-size", "64"},
	}}}}
	if err := NewSGLangLMCacheAdapter(SubscriberConfig{}).InjectEngineConfig(&pod.Spec, nil, cache); err != nil {
		t.Fatalf("optional scheduling overrides should not be required: %v", err)
	}
}

func TestRenderLMCacheNodeLocalServerPodRejectsInvalidContracts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*cachev1alpha1.CacheBackend)
		want   string
	}{
		{name: "missing UID", mutate: func(cache *cachev1alpha1.CacheBackend) { cache.UID = "" }, want: "UID is empty"},
		{name: "empty image", mutate: func(cache *cachev1alpha1.CacheBackend) { cache.Spec.LMCache.NodeLocal.Server.Image = " " }, want: "image is empty"},
		{name: "invalid port", mutate: func(cache *cachev1alpha1.CacheBackend) { cache.Spec.LMCache.NodeLocal.Server.Port = 0 }, want: "outside 1-65535"},
		{name: "duplicate host ports", mutate: func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.LMCache.NodeLocal.Server.HTTPPort = cache.Spec.LMCache.NodeLocal.Server.Port
		}, want: "host ports must be distinct"},
		{name: "zero worker count", mutate: func(cache *cachev1alpha1.CacheBackend) { cache.Spec.LMCache.NodeLocal.Server.MaxGPUWorkers = 0 }, want: "must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
			tt.mutate(cache)
			_, err := RenderLMCacheNodeLocalServerPod(cache, nil, "gpu-node-a", nodeLocalSourceEngine())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
	if _, err := RenderLMCacheNodeLocalServerPod(nil, nil, "gpu-node-a", nodeLocalSourceEngine()); err == nil || !strings.Contains(err.Error(), "complete typed") {
		t.Fatalf("nil CacheBackend error = %v", err)
	}
	if _, err := RenderLMCacheNodeLocalServerPod(newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM), nil, "", nodeLocalSourceEngine()); err == nil || !strings.Contains(err.Error(), "scheduled source") {
		t.Fatalf("missing target node error = %v", err)
	}
}

func TestNodeLocalEngineInjectionRejectsReservedWireCollisionsAtomically(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.PodSpec)
		want   string
	}{
		{
			name: "node IP env",
			mutate: func(pod *corev1.PodSpec) {
				pod.Containers[0].Env = append(pod.Containers[0].Env, corev1.EnvVar{Name: lmCacheNodeIPEnv, Value: "192.0.2.1"})
			},
			want: "environment variable",
		},
		{
			name: "gate name",
			mutate: func(pod *corev1.PodSpec) {
				pod.InitContainers = append(pod.InitContainers, corev1.Container{Name: lmCacheNodeLocalGateContainerName, Image: "user/gate:latest"})
			},
			want: "init container name",
		},
		{
			name: "read-only shared memory",
			mutate: func(pod *corev1.PodSpec) {
				pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "shm", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev/shm"}}})
				pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "shm", MountPath: "/dev/shm", ReadOnly: true})
			},
			want: "writable whole-volume",
		},
		{
			name: "non-host shared memory",
			mutate: func(pod *corev1.PodSpec) {
				pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "shm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
				pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "shm", MountPath: "/dev/shm"})
			},
			want: "UID-scoped hostPath",
		},
		{
			name: "whole host shared memory",
			mutate: func(pod *corev1.PodSpec) {
				pathType := corev1.HostPathDirectory
				pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "shm", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev/shm", Type: &pathType}}})
				pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "shm", MountPath: "/dev/shm"})
			},
			want: "UID-scoped hostPath",
		},
		{
			name: "missing shared memory volume",
			mutate: func(pod *corev1.PodSpec) {
				pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "missing", MountPath: "/dev/shm"})
			},
			want: "references missing volume",
		},
	}
	adapter := NewVLLMLMCacheMPAdapter(SubscriberConfig{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
			pod := newVLLMMPEnginePod()
			tt.mutate(&pod.Spec)
			before := pod.Spec.DeepCopy()
			err := adapter.InjectEngineConfig(&pod.Spec, nil, cache)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
			if !reflect.DeepEqual(before, &pod.Spec) {
				t.Fatal("failed NodeLocal injection partially mutated PodSpec")
			}
		})
	}
}

func TestRenderLMCacheNodeLocalEngineRejectsMissingInputs(t *testing.T) {
	cache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	if _, err := renderLMCacheNodeLocalEngine(nil, EngineContainerName, cache, false); err == nil || !strings.Contains(err.Error(), "pod spec is nil") {
		t.Fatalf("nil pod error = %v", err)
	}
	pod := newVLLMMPEnginePod().Spec
	cache.UID = ""
	if _, err := renderLMCacheNodeLocalEngine(&pod, EngineContainerName, cache, false); err == nil || !strings.Contains(err.Error(), "complete typed") {
		t.Fatalf("missing backend identity error = %v", err)
	}
}

func TestNodeLocalAdaptersValidateTopologyContract(t *testing.T) {
	vllmCache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeVLLM)
	vllmPod := newVLLMMPEnginePod("--tensor-parallel-size", "1")
	vllm := vllmLMCacheMPAdapter{}
	if !vllm.Supports("vllm", vllmCache) {
		t.Fatal("vLLM adapter does not advertise typed NodeLocal support")
	}
	if err := vllm.ValidateMPEnginePod(vllmPod, vllmCache); err != nil {
		t.Fatalf("validate vLLM NodeLocal engine: %v", err)
	}
	vllmCache.Spec.LMCache.NodeLocal.Scheduling = nil
	if err := vllm.ValidateMPEnginePod(vllmPod, vllmCache); err != nil {
		t.Fatalf("vLLM optional scheduling rejected: %v", err)
	}
	vllmCache.Spec.LMCache.Topology = "Unsupported"
	if err := vllm.ValidateMPEnginePod(vllmPod, vllmCache); err == nil || !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("vLLM unsupported topology error = %v", err)
	}

	sglangCache := newNodeLocalBackend(cachev1alpha1.CacheBackendRuntimeSGLang)
	sglangPod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
		Name: SGLangEngineContainerName, Args: []string{"--page-size", "64"},
		Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{gpuResourceName: resource.MustParse("1")}},
	}}}}
	sglang := sglangLMCacheAdapter{}
	if !sglang.Supports("sglang", sglangCache) {
		t.Fatal("SGLang adapter does not advertise typed NodeLocal support")
	}
	if err := sglang.ValidateMPEnginePod(sglangPod, sglangCache); err != nil {
		t.Fatalf("validate SGLang NodeLocal engine: %v", err)
	}
	sglangCache.Spec.LMCache.NodeLocal.Server = nil
	if err := sglang.ValidateMPEnginePod(sglangPod, sglangCache); err == nil || !strings.Contains(err.Error(), "server configuration") {
		t.Fatalf("SGLang missing server error = %v", err)
	}
	sglangCache.Spec.LMCache.Topology = "Unsupported"
	if err := sglang.ValidateMPEnginePod(sglangPod, sglangCache); err == nil || !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("SGLang unsupported topology error = %v", err)
	}
}
