package sglang

import (
	"flag"
	"io"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

func newSGLangBackend(cfg map[string]string) *cachev1alpha1.CacheBackend {
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:          cachev1alpha1.CacheBackendTypeLMCache,
			Integration:   &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "sglang"},
			BackendConfig: cfg,
		},
	}
	return cb
}

func findInitContainer(cs []corev1.Container, name string) *corev1.Container {
	for i := range cs {
		if cs[i].Name == name {
			return &cs[i]
		}
	}
	return nil
}

func findVolume(vs []corev1.Volume, name string) *corev1.Volume {
	for i := range vs {
		if vs[i].Name == name {
			return &vs[i]
		}
	}
	return nil
}

func hasMount(ms []corev1.VolumeMount, name string) bool {
	for _, m := range ms {
		if m.Name == name {
			return true
		}
	}
	return false
}

func TestSGLangSupports(t *testing.T) {
	a := NewAdapter()
	cases := []struct {
		name    string
		runtime runtimeadapter.RuntimeID
		cache   *cachev1alpha1.CacheBackend
		want    bool
	}{
		{"sglang+lmcache", runtimeadapter.RuntimeSGLang, newSGLangBackend(nil), true},
		{"vllm+lmcache", runtimeadapter.RuntimeVLLM, newSGLangBackend(nil), false},
		{"sglang+external", runtimeadapter.RuntimeSGLang, &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeExternal}}, false},
		{"sglang+mooncake", runtimeadapter.RuntimeSGLang, &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeMooncake}}, false},
		{"nil cache", runtimeadapter.RuntimeSGLang, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.Supports(tc.runtime, tc.cache); got != tc.want {
				t.Fatalf("Supports(%q, %+v) = %v, want %v", tc.runtime, tc.cache, got, tc.want)
			}
		})
	}
}

func TestSGLangSupportedPairs(t *testing.T) {
	a := NewAdapter().(interface {
		SupportedPairs() []runtimeadapter.SupportedPair
	})
	got := a.SupportedPairs()
	want := []runtimeadapter.SupportedPair{{Runtime: runtimeadapter.RuntimeSGLang, Backend: cachev1alpha1.CacheBackendTypeLMCache}}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("SupportedPairs = %v, want %v", got, want)
	}
}

func TestSGLangResolveCacheServer(t *testing.T) {
	a := NewAdapter()
	pod, svc, err := a.ResolveCacheServer(newSGLangBackend(nil))
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	if pod == nil || svc == nil {
		t.Fatalf("ResolveCacheServer returned nil pod or svc")
	}
	// SGLang MP mode offloads to a shared Redis L2 (not the lm:// server — lm://
	// is not a valid MP --l2-adapter type). The exhaustive render edge-cases live
	// in the runtime package's redis_l2_test.go; here we pin that a (sglang,
	// LMCache) backend provisions the Redis L2 store.
	if len(pod.Containers) != 1 || pod.Containers[0].Name != "redis-l2" {
		t.Fatalf("containers = %+v, want a single redis-l2", pod.Containers)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 6379 {
		t.Fatalf("svc ports = %v, want a single 6379 port", svc.Spec.Ports)
	}
}

func TestSGLangResolveCacheServerImageOverride(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(map[string]string{"redisImage": "registry.example.com/redis:pinned"})
	pod, _, err := a.ResolveCacheServer(cb)
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	if got := pod.Containers[0].Image; got != "registry.example.com/redis:pinned" {
		t.Fatalf("image = %q, want overridden", got)
	}
}

func TestSGLangResolveCacheServerNilCache(t *testing.T) {
	if _, _, err := NewAdapter().ResolveCacheServer(nil); err == nil {
		t.Fatalf("ResolveCacheServer(nil) returned no error")
	}
}

func TestSGLangInjectEngineConfig(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  enginewire.SGLangEngineContainerName,
				Image: "sglang:test",
				Args:  []string{"--page-size", "64"},
				Env:   []corev1.EnvVar{{Name: "HF_TOKEN", Value: "secret-token"}},
			},
			{
				Name: "sidecar",
				Env:  []corev1.EnvVar{{Name: "SIDECAR_VAR", Value: "untouched"}},
			},
		},
	}

	if err := a.InjectEngineConfig(pod, "cache.ns1.svc.cluster.local:6379", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}

	engine := pod.Containers[0]

	// MP-mode engine wire: connector on + config-file, and the lm:// env is GONE.
	if !containsArg(engine.Args, enginewire.SGLangEnableLMCacheArg) {
		t.Fatalf("engine args missing %s: %v", enginewire.SGLangEnableLMCacheArg, engine.Args)
	}
	if !containsArg(engine.Args, enginewire.SGLangConfigFileArg) {
		t.Fatalf("engine args missing %s: %v", enginewire.SGLangConfigFileArg, engine.Args)
	}
	if v, ok := lookupEnv(engine.Env, enginewire.EnvLMCacheUseExperimental); !ok || v != "True" {
		t.Fatalf("%s = (%q, %v), want True", enginewire.EnvLMCacheUseExperimental, v, ok)
	}
	if v, ok := lookupEnv(engine.Env, enginewire.EnvInferenceCacheFailOpen); !ok || v == "" {
		t.Fatalf("%s missing", enginewire.EnvInferenceCacheFailOpen)
	}
	// The old lm:// env is NOT injected — SGLang MP mode ignores it.
	if _, ok := lookupEnv(engine.Env, enginewire.EnvLMCacheRemoteURL); ok {
		t.Fatalf("%s injected — SGLang MP mode must not use the lm:// env", enginewire.EnvLMCacheRemoteURL)
	}
	// vLLM-only env/args stay absent.
	if _, ok := lookupEnv(engine.Env, enginewire.EnvVLLMUseV1); ok {
		t.Fatalf("%s (vLLM-only) injected for SGLang", enginewire.EnvVLLMUseV1)
	}
	if _, ok := lookupEnv(engine.Env, enginewire.EnvPythonHashSeed); ok {
		t.Fatalf("%s (vLLM-only) injected for SGLang", enginewire.EnvPythonHashSeed)
	}
	if containsArg(engine.Args, "--kv-transfer-config") {
		t.Fatalf("--kv-transfer-config (vLLM-only) injected for SGLang: %v", engine.Args)
	}
	// Existing args/env preserved.
	if !containsArg(engine.Args, "--page-size") {
		t.Fatalf("--page-size was dropped: %v", engine.Args)
	}
	if v, _ := lookupEnv(engine.Env, "HF_TOKEN"); v != "secret-token" {
		t.Fatalf("HF_TOKEN clobbered: got %q", v)
	}

	// The MP-worker native sidecar is injected (an initContainer, restartPolicy
	// Always), version-aligned to the engine image, GPU-visible but GPU-less, and
	// pointing its resp --l2-adapter at the endpoint Redis.
	worker := findInitContainer(pod.InitContainers, "lmcache-mp-worker")
	if worker == nil {
		t.Fatalf("MP-worker sidecar not injected: initContainers = %+v", pod.InitContainers)
	}
	if worker.RestartPolicy == nil || *worker.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Fatalf("worker is not a native sidecar (restartPolicy Always): %v", worker.RestartPolicy)
	}
	if worker.Image != "sglang:test" {
		t.Fatalf("worker image = %q, want the engine image (version-aligned default)", worker.Image)
	}
	if v, _ := lookupEnv(worker.Env, "NVIDIA_VISIBLE_DEVICES"); v != "all" {
		t.Fatalf("worker NVIDIA_VISIBLE_DEVICES = %q, want all (GPU-less sidecar must see the GPU for CUDA-IPC)", v)
	}
	joined := strings.Join(worker.Args, " ")
	for _, want := range []string{`"type":"resp"`, `"host":"cache.ns1.svc.cluster.local"`, `"port":6379`} {
		if !strings.Contains(joined, want) {
			t.Fatalf("worker --l2-adapter missing %s: %s", want, joined)
		}
	}

	// Shared volumes (/dev/shm memory-backed) + engine mounts.
	if findVolume(pod.Volumes, "lmcache-config") == nil {
		t.Fatalf("config volume missing: %v", pod.Volumes)
	}
	shm := findVolume(pod.Volumes, "lmcache-dshm")
	if shm == nil || shm.EmptyDir == nil || shm.EmptyDir.Medium != corev1.StorageMediumMemory {
		t.Fatalf("/dev/shm is not a memory-backed emptyDir: %+v", shm)
	}
	if !hasMount(engine.VolumeMounts, "lmcache-config") || !hasMount(engine.VolumeMounts, "lmcache-dshm") {
		t.Fatalf("engine volume mounts missing config/dshm: %v", engine.VolumeMounts)
	}

	// The non-engine sidecar container is untouched.
	if len(pod.Containers[1].Env) != 1 || pod.Containers[1].Env[0].Name != "SIDECAR_VAR" {
		t.Fatalf("non-engine sidecar was mutated: %v", pod.Containers[1].Env)
	}
}

func TestSGLangInjectEngineConfigSingleContainerPodAcceptsAnyName(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine", Image: "img"}}}
	if err := a.InjectEngineConfig(pod, "cache.ns1.svc:6379", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	if !containsArg(pod.Containers[0].Args, enginewire.SGLangConfigFileArg) {
		t.Fatalf("single-container pod missing %s; should have been treated as the engine", enginewire.SGLangConfigFileArg)
	}
}

func TestSGLangInjectEngineConfigMultiContainerWithoutSGLangNameErrors(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "engine"},
		{Name: "sidecar"},
	}}
	err := a.InjectEngineConfig(pod, "cache.ns1.svc:65432", cb)
	if err == nil {
		t.Fatalf("expected an error for multi-container pod without an sglang-named container")
	}
	for _, c := range pod.Containers {
		if _, ok := lookupEnv(c.Env, enginewire.EnvLMCacheRemoteURL); ok {
			t.Fatalf("container %q got env injected before the error: %v", c.Name, c.Env)
		}
	}
}

func TestSGLangInjectEngineConfigIdempotent(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName, Image: "img"}}}
	if err := a.InjectEngineConfig(pod, "first.svc:6379", cb); err != nil {
		t.Fatalf("first InjectEngineConfig: %v", err)
	}
	if err := a.InjectEngineConfig(pod, "second.svc:6379", cb); err != nil {
		t.Fatalf("second InjectEngineConfig: %v", err)
	}
	// --enable-lmcache appears exactly once (no duplicate on re-inject).
	flags := 0
	for _, arg := range pod.Containers[0].Args {
		if arg == enginewire.SGLangEnableLMCacheArg {
			flags++
		}
	}
	if flags != 1 {
		t.Fatalf("%s count = %d, want 1", enginewire.SGLangEnableLMCacheArg, flags)
	}
	// Exactly one worker sidecar and two volumes (config + dshm) — re-inject
	// upserts by name rather than appending duplicates.
	workers := 0
	for _, c := range pod.InitContainers {
		if c.Name == "lmcache-mp-worker" {
			workers++
		}
	}
	if workers != 1 {
		t.Fatalf("worker sidecar count = %d, want 1", workers)
	}
	if len(pod.Volumes) != 2 {
		t.Fatalf("volume count = %d, want 2 (config + dshm, not duplicated): %v", len(pod.Volumes), pod.Volumes)
	}
	// Re-inject updates the worker's L2 endpoint in place (second wins).
	worker := findInitContainer(pod.InitContainers, "lmcache-mp-worker")
	if worker == nil || !strings.Contains(strings.Join(worker.Args, " "), `"host":"second.svc"`) {
		t.Fatalf("re-inject did not update the worker's L2 endpoint: %+v", worker)
	}
}

func TestSGLangInjectEngineConfigConfigOverrides(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(map[string]string{
		"chunkSize":   "512",
		"l1SizeGB":    "8",
		"workerImage": "registry.example/lmcache-worker:pinned",
		"mpPort":      "6000",
	})
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName, Image: "img"}}}
	if err := a.InjectEngineConfig(pod, "x.svc:6379", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	worker := findInitContainer(pod.InitContainers, "lmcache-mp-worker")
	if worker == nil {
		t.Fatalf("no worker sidecar")
	}
	if worker.Image != "registry.example/lmcache-worker:pinned" {
		t.Fatalf("worker image = %q, want the workerImage override", worker.Image)
	}
	joined := strings.Join(worker.Args, " ")
	for _, want := range []string{"--chunk-size 512", "--l1-size-gb 8", "--port 6000"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("worker args missing %q: %s", want, joined)
		}
	}
	if !containsArg(pod.Containers[0].Args, enginewire.SGLangConfigFileArg) {
		t.Fatalf("engine missing %s", enginewire.SGLangConfigFileArg)
	}
}

func TestSGLangInjectEngineConfigReusesExistingDevShm(t *testing.T) {
	// GPU engine manifests commonly mount their own tmpfs at /dev/shm. Appending a
	// SECOND mount at the same mountPath makes the Pod invalid (the API server
	// rejects duplicate mountPaths), so injection must REUSE the engine's volume for
	// the worker rather than adding its own.
	a := NewAdapter()
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:         enginewire.SGLangEngineContainerName,
			Image:        "sglang:test",
			VolumeMounts: []corev1.VolumeMount{{Name: "dshm", MountPath: "/dev/shm"}},
		}},
		Volumes: []corev1.Volume{{
			Name:         "dshm",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}},
		}},
	}
	if err := a.InjectEngineConfig(pod, "r.svc:6379", newSGLangBackend(nil)); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	n := 0
	for _, m := range pod.Containers[0].VolumeMounts {
		if m.MountPath == "/dev/shm" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("engine has %d mounts at /dev/shm, want exactly 1 (a duplicate mountPath is an invalid Pod): %+v", n, pod.Containers[0].VolumeMounts)
	}
	if findVolume(pod.Volumes, "lmcache-dshm") != nil {
		t.Fatalf("adapter added its own lmcache-dshm volume despite the engine already mounting /dev/shm")
	}
	w := findInitContainer(pod.InitContainers, "lmcache-mp-worker")
	if w == nil {
		t.Fatalf("worker not injected")
	}
	var workerShm string
	for _, m := range w.VolumeMounts {
		if m.MountPath == "/dev/shm" {
			workerShm = m.Name
		}
	}
	if workerShm != "dshm" {
		t.Fatalf("worker /dev/shm volume = %q, want the engine's existing %q — the MP data path needs both containers on the SAME volume", workerShm, "dshm")
	}
}

func TestSGLangInjectEngineConfigRejectsConfigPathCollision(t *testing.T) {
	// The config mount path is adapter-owned: the worker WRITES the MP config there.
	// A pre-existing mount can neither be duplicated (invalid Pod) nor safely reused
	// (a ConfigMap mount is read-only), so injection must reject with a clear reason
	// — the webhook turns that into a fail-open admit.
	pod := &corev1.PodSpec{Containers: []corev1.Container{{
		Name:         enginewire.SGLangEngineContainerName,
		Image:        "sglang:test",
		VolumeMounts: []corev1.VolumeMount{{Name: "operator-cfg", MountPath: "/etc/lmcache"}},
	}}}
	err := NewAdapter().InjectEngineConfig(pod, "r.svc:6379", newSGLangBackend(nil))
	if err == nil {
		t.Fatalf("want an error when the engine already mounts the adapter-owned config path")
	}
	if !strings.Contains(err.Error(), "/etc/lmcache") || !strings.Contains(err.Error(), "operator-cfg") {
		t.Fatalf("error must name the path and the conflicting volume, got: %v", err)
	}
}

func TestSGLangInjectEngineConfigRejectsForeignReservedNames(t *testing.T) {
	// Mutating admission must never erase an operator's container or volume. A
	// pre-existing object carrying one of the adapter's reserved names — but NOT
	// rendered by the adapter (no marker env) — is a foreign collision: reject, so
	// the pod webhook fails open and the pod admits un-wired rather than corrupted.
	// Silently skipping is not an option for the worker: the engine gets
	// --lmcache-config-file regardless and would block on a config nothing writes.
	engine := corev1.Container{Name: enginewire.SGLangEngineContainerName, Image: "sglang:test"}
	cases := []struct {
		name string
		pod  *corev1.PodSpec
		want string // fragment the error must name so the operator can find it
	}{
		{
			name: "container squats the worker name",
			pod: &corev1.PodSpec{
				Containers:     []corev1.Container{engine},
				InitContainers: []corev1.Container{{Name: "lmcache-mp-worker", Image: "operator/own:v1"}},
			},
			want: "lmcache-mp-worker",
		},
		{
			name: "volume squats the config-volume name",
			pod: &corev1.PodSpec{
				Containers: []corev1.Container{engine},
				Volumes: []corev1.Volume{{
					Name:         "lmcache-config",
					VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{}},
				}},
			},
			want: "lmcache-config",
		},
		{
			name: "volume squats the dshm-volume name",
			pod: &corev1.PodSpec{
				Containers: []corev1.Container{engine},
				Volumes: []corev1.Volume{{
					Name:         "lmcache-dshm",
					VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/mnt/data"}},
				}},
			},
			want: "lmcache-dshm",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.pod.DeepCopy()
			err := NewAdapter().InjectEngineConfig(tc.pod, "r.svc:6379", newSGLangBackend(nil))
			if err == nil {
				t.Fatalf("want an error when %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error must name the conflicting object %q, got: %v", tc.want, err)
			}
			// The operator's object must survive verbatim — an error that still
			// clobbered the pod would defeat the purpose of rejecting.
			if !reflect.DeepEqual(tc.pod.InitContainers, before.InitContainers) {
				t.Fatalf("initContainers mutated despite the rejection:\n got %+v\nwant %+v", tc.pod.InitContainers, before.InitContainers)
			}
			if !reflect.DeepEqual(tc.pod.Volumes, before.Volumes) {
				t.Fatalf("volumes mutated despite the rejection:\n got %+v\nwant %+v", tc.pod.Volumes, before.Volumes)
			}
		})
	}
}

func TestSGLangInjectEngineConfigReinjectionConvergesOnCurrentRender(t *testing.T) {
	// The marker env on the worker is what tells OUR container from an operator's
	// squat — and it must keep doing so when the render legitimately CHANGES (a moved
	// status.endpoint here). Value-equality against a fresh render would misread this
	// as foreign; the second injection must instead converge the worker on the new
	// endpoint rather than reject it, duplicate it, or leave the stale one.
	a := NewAdapter()
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName, Image: "img"}}}
	if err := a.InjectEngineConfig(pod, "first.svc:6379", newSGLangBackend(nil)); err != nil {
		t.Fatalf("first InjectEngineConfig: %v", err)
	}
	if err := a.InjectEngineConfig(pod, "second.svc:6379", newSGLangBackend(nil)); err != nil {
		t.Fatalf("second InjectEngineConfig: %v", err)
	}
	w := findInitContainer(pod.InitContainers, "lmcache-mp-worker")
	if w == nil {
		t.Fatalf("worker not injected")
	}
	script := strings.Join(w.Args, " ")
	if !strings.Contains(script, "second.svc") {
		t.Fatalf("worker did not converge on the current endpoint; --l2-adapter still reads: %s", script)
	}
	if strings.Contains(script, "first.svc") {
		t.Fatalf("worker kept the stale endpoint from the first injection: %s", script)
	}
}

func TestSGLangInjectEngineConfigRejectsUnwritableDevShm(t *testing.T) {
	// A reused /dev/shm carries the MP data path, which WRITES. A read-only mount, or
	// one backed by a projection source the kubelet always mounts read-only, would
	// fail deep inside LMCache at runtime — reject at admission instead.
	engine := func(m corev1.VolumeMount) corev1.Container {
		return corev1.Container{
			Name: enginewire.SGLangEngineContainerName, Image: "sglang:test",
			VolumeMounts: []corev1.VolumeMount{m},
		}
	}
	cases := []struct {
		name string
		pod  *corev1.PodSpec
		want string
	}{
		{
			name: "read-only mount",
			pod: &corev1.PodSpec{
				Containers: []corev1.Container{engine(corev1.VolumeMount{Name: "dshm", MountPath: "/dev/shm", ReadOnly: true})},
				Volumes: []corev1.Volume{{
					Name:         "dshm",
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}},
				}},
			},
			want: "read-only",
		},
		{
			name: "configMap-backed volume",
			pod: &corev1.PodSpec{
				Containers: []corev1.Container{engine(corev1.VolumeMount{Name: "shm-cfg", MountPath: "/dev/shm"})},
				Volumes: []corev1.Volume{{
					Name:         "shm-cfg",
					VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{}},
				}},
			},
			want: "configMap",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewAdapter().InjectEngineConfig(tc.pod, "r.svc:6379", newSGLangBackend(nil))
			if err == nil {
				t.Fatalf("want an error when the engine's /dev/shm is not writable scratch (%s)", tc.name)
			}
			if !strings.Contains(err.Error(), "/dev/shm") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error must name /dev/shm and %q, got: %v", tc.want, err)
			}
		})
	}
}

func TestSGLangInjectEngineConfigReusesWritableNonEmptyDirDevShm(t *testing.T) {
	// The writability guard must not over-reject: a source that HAS a readOnly flag
	// but leaves it false is writable, and the engine's /dev/shm must still be reused
	// (a second mount at the same path is an invalid Pod). Pins that the guard keys
	// on the flag's value, not on the source being exotic.
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: enginewire.SGLangEngineContainerName, Image: "sglang:test",
			VolumeMounts: []corev1.VolumeMount{{Name: "nfs-shm", MountPath: "/dev/shm"}},
		}},
		Volumes: []corev1.Volume{{
			Name:         "nfs-shm",
			VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: "s", Path: "/p", ReadOnly: false}},
		}},
	}
	if err := NewAdapter().InjectEngineConfig(pod, "r.svc:6379", newSGLangBackend(nil)); err != nil {
		t.Fatalf("InjectEngineConfig rejected a writable /dev/shm: %v", err)
	}
	w := findInitContainer(pod.InitContainers, "lmcache-mp-worker")
	if w == nil {
		t.Fatalf("worker not injected")
	}
	for _, m := range w.VolumeMounts {
		if m.MountPath == "/dev/shm" && m.Name != "nfs-shm" {
			t.Fatalf("worker /dev/shm volume = %q, want the engine's existing %q", m.Name, "nfs-shm")
		}
	}
}

func TestSGLangInjectEngineConfigWorkerSeesTheGPU(t *testing.T) {
	// GPU visibility on the worker is LOAD-BEARING, not incidental: the engine hands
	// it a device UUID and LMCache's CUDA-IPC wrapper resolves that to a local index,
	// which fails unless the device is visible here. GPU-validated — with visibility
	// revoked the worker dies on "Device UUID <uuid> not found in the discovered
	// devices" and the engine never reaches ready.
	//
	// It cannot be narrowed to the engine's own device: the device plugin assigns the
	// UUID at kubelet time, after this mutation runs. The isolation trade-off is
	// documented for operators in docs/design/cachebackend-api.md. This test exists so
	// the env is not dropped as dead weight — the failure it prevents is a wedged
	// engine, not a cache miss.
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName, Image: "sglang:test"}}}
	if err := NewAdapter().InjectEngineConfig(pod, "r.svc:6379", newSGLangBackend(nil)); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	w := findInitContainer(pod.InitContainers, "lmcache-mp-worker")
	if w == nil {
		t.Fatalf("worker not injected")
	}
	if v, _ := lookupEnv(w.Env, "NVIDIA_VISIBLE_DEVICES"); v != "all" {
		t.Fatalf("worker NVIDIA_VISIBLE_DEVICES = %q, want \"all\" — without it the CUDA-IPC UUID lookup fails and the engine hangs behind the startup probe", v)
	}
	// It must stay GPU-less at the scheduler: a device-plugin request would burn a
	// second GPU and hand the worker a DIFFERENT device than the engine's.
	if _, ok := w.Resources.Limits["nvidia.com/gpu"]; ok {
		t.Fatalf("worker requests nvidia.com/gpu — it must consume no device-plugin allocation: %v", w.Resources.Limits)
	}
}

func TestSGLangInjectEngineConfigWorkerRestrictedSecurityContext(t *testing.T) {
	// This mutation lands BEFORE Pod Security admission, so the worker must carry the
	// container-only Restricted requirements itself — else it turns an admissible
	// engine pod into a REJECTED one in a restricted namespace (the inverse of
	// fail-open). And it must add NO capabilities (an added cap is itself a Restricted
	// violation; IPC_LOCK is not needed — GPU access is via device files, not caps).
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName, Image: "sglang:test"}}}
	if err := NewAdapter().InjectEngineConfig(pod, "r.svc:6379", newSGLangBackend(nil)); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	w := findInitContainer(pod.InitContainers, "lmcache-mp-worker")
	if w == nil {
		t.Fatalf("worker not injected")
	}
	sc := w.SecurityContext
	if sc == nil {
		t.Fatalf("worker has no securityContext — a restricted namespace would reject the pod")
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Errorf("allowPrivilegeEscalation = %v, want false (container-only Restricted requirement)", sc.AllowPrivilegeEscalation)
	}
	if sc.Capabilities == nil || len(sc.Capabilities.Add) > 0 {
		t.Errorf("capabilities.add = %v, want none (an added cap is a Restricted violation)", sc.Capabilities)
	}
	dropsAll := false
	if sc.Capabilities != nil {
		for _, c := range sc.Capabilities.Drop {
			if c == "ALL" {
				dropsAll = true
			}
		}
	}
	if !dropsAll {
		t.Errorf("capabilities.drop = %v, want [ALL] (container-only Restricted requirement)", sc.Capabilities)
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("seccompProfile = %v, want RuntimeDefault", sc.SeccompProfile)
	}
}

func TestSGLangInjectEngineConfigWorkerMirrorsEngineUserIdentity(t *testing.T) {
	// The worker runs the operator's engine image (by default), so it must not force
	// its own UID like the distroless subscriber does — it mirrors the engine's user
	// identity instead, staying exactly as (non-)root as the pod was admitted to be.
	nonRoot := true
	uid := int64(1000)
	gid := int64(2000)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{
		Name:  enginewire.SGLangEngineContainerName,
		Image: "sglang:test",
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot: &nonRoot, RunAsUser: &uid, RunAsGroup: &gid,
		},
	}}}
	if err := NewAdapter().InjectEngineConfig(pod, "r.svc:6379", newSGLangBackend(nil)); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	w := findInitContainer(pod.InitContainers, "lmcache-mp-worker")
	sc := w.SecurityContext
	if sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot {
		t.Errorf("runAsNonRoot not mirrored from engine: %v", sc.RunAsNonRoot)
	}
	if sc.RunAsUser == nil || *sc.RunAsUser != uid {
		t.Errorf("runAsUser = %v, want mirrored %d", sc.RunAsUser, uid)
	}
	if sc.RunAsGroup == nil || *sc.RunAsGroup != gid {
		t.Errorf("runAsGroup = %v, want mirrored %d", sc.RunAsGroup, gid)
	}
	// And it does NOT force a read-only rootfs or a fixed UID when the engine sets
	// none — that would risk breaking the vendor image's writes / CUDA-IPC.
	pod2 := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName, Image: "sglang:test"}}}
	_ = NewAdapter().InjectEngineConfig(pod2, "r.svc:6379", newSGLangBackend(nil))
	w2 := findInitContainer(pod2.InitContainers, "lmcache-mp-worker")
	if w2.SecurityContext.RunAsUser != nil {
		t.Errorf("runAsUser forced to %v when engine set none — must inherit from the pod, not override the image", w2.SecurityContext.RunAsUser)
	}
	if w2.SecurityContext.ReadOnlyRootFilesystem != nil {
		t.Errorf("readOnlyRootFilesystem set — the worker writes to its rootfs; must not force it")
	}
}

func TestSGLangInjectEngineConfigMirrorsDevShmSubPath(t *testing.T) {
	// Sharing the same VOLUME is not enough: if the engine mounts /dev/shm with a
	// subPath and the worker mounts the volume root, the two land on DIFFERENT
	// directories — the pod admits cleanly and then transfers no KV. Mirror the
	// subPath so both resolve to the same place.
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: enginewire.SGLangEngineContainerName, Image: "sglang:test",
			VolumeMounts: []corev1.VolumeMount{{Name: "scratch", MountPath: "/dev/shm", SubPath: "shm"}},
		}},
		Volumes: []corev1.Volume{{
			Name:         "scratch",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}},
		}},
	}
	if err := NewAdapter().InjectEngineConfig(pod, "r.svc:6379", newSGLangBackend(nil)); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	w := findInitContainer(pod.InitContainers, "lmcache-mp-worker")
	if w == nil {
		t.Fatalf("worker not injected")
	}
	var got *corev1.VolumeMount
	for i := range w.VolumeMounts {
		if w.VolumeMounts[i].MountPath == "/dev/shm" {
			got = &w.VolumeMounts[i]
		}
	}
	if got == nil {
		t.Fatalf("worker has no /dev/shm mount: %+v", w.VolumeMounts)
	}
	if got.Name != "scratch" || got.SubPath != "shm" {
		t.Fatalf("worker /dev/shm = (volume %q, subPath %q), want (scratch, shm) — both containers must resolve to the SAME directory", got.Name, got.SubPath)
	}
}

func TestSGLangInjectEngineConfigRejectsUnshareableDevShm(t *testing.T) {
	// Shapes the worker cannot safely share: an expansion it cannot reproduce in its
	// own env, and source-level read-only that the mount-level readOnly check misses.
	engine := func(m corev1.VolumeMount) corev1.Container {
		return corev1.Container{
			Name: enginewire.SGLangEngineContainerName, Image: "sglang:test",
			VolumeMounts: []corev1.VolumeMount{m},
		}
	}
	csiReadOnly := true
	cases := []struct {
		name string
		pod  *corev1.PodSpec
		want string
	}{
		{
			name: "subPathExpr cannot be reproduced in the worker's env",
			pod: &corev1.PodSpec{
				Containers: []corev1.Container{engine(corev1.VolumeMount{Name: "scratch", MountPath: "/dev/shm", SubPathExpr: "$(POD_NAME)/shm"})},
				Volumes: []corev1.Volume{{
					Name:         "scratch",
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}},
				}},
			},
			want: "subPathExpr",
		},
		{
			name: "read-only persistentVolumeClaim source",
			pod: &corev1.PodSpec{
				Containers: []corev1.Container{engine(corev1.VolumeMount{Name: "pvc-shm", MountPath: "/dev/shm"})},
				Volumes: []corev1.Volume{{
					Name: "pvc-shm",
					VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "c", ReadOnly: true,
					}},
				}},
			},
			want: "persistentVolumeClaim",
		},
		{
			name: "read-only csi source",
			pod: &corev1.PodSpec{
				Containers: []corev1.Container{engine(corev1.VolumeMount{Name: "csi-shm", MountPath: "/dev/shm"})},
				Volumes: []corev1.Volume{{
					Name: "csi-shm",
					VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
						Driver: "d.example.com", ReadOnly: &csiReadOnly,
					}},
				}},
			},
			want: "csi",
		},
		// A source-level readOnly is NOT overridden by a mount-level readOnly:false,
		// so every in-tree source carrying its own flag must be caught. These shapes
		// are exotic at /dev/shm, but the failure they produce is the silent one.
		{
			name: "read-only nfs source",
			pod: &corev1.PodSpec{
				Containers: []corev1.Container{engine(corev1.VolumeMount{Name: "nfs-shm", MountPath: "/dev/shm", ReadOnly: false})},
				Volumes: []corev1.Volume{{
					Name:         "nfs-shm",
					VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: "s", Path: "/p", ReadOnly: true}},
				}},
			},
			want: "nfs",
		},
		{
			name: "read-only rbd source",
			pod: &corev1.PodSpec{
				Containers: []corev1.Container{engine(corev1.VolumeMount{Name: "rbd-shm", MountPath: "/dev/shm"})},
				Volumes: []corev1.Volume{{
					Name:         "rbd-shm",
					VolumeSource: corev1.VolumeSource{RBD: &corev1.RBDVolumeSource{RBDImage: "i", ReadOnly: true}},
				}},
			},
			want: "rbd",
		},
		{
			name: "read-only cephfs source",
			pod: &corev1.PodSpec{
				Containers: []corev1.Container{engine(corev1.VolumeMount{Name: "ceph-shm", MountPath: "/dev/shm"})},
				Volumes: []corev1.Volume{{
					Name:         "ceph-shm",
					VolumeSource: corev1.VolumeSource{CephFS: &corev1.CephFSVolumeSource{Monitors: []string{"m"}, ReadOnly: true}},
				}},
			},
			want: "cephfs",
		},
		{
			name: "read-only azureFile source",
			pod: &corev1.PodSpec{
				Containers: []corev1.Container{engine(corev1.VolumeMount{Name: "az-shm", MountPath: "/dev/shm"})},
				Volumes: []corev1.Volume{{
					Name:         "az-shm",
					VolumeSource: corev1.VolumeSource{AzureFile: &corev1.AzureFileVolumeSource{SecretName: "s", ShareName: "sh", ReadOnly: true}},
				}},
			},
			want: "azureFile",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewAdapter().InjectEngineConfig(tc.pod, "r.svc:6379", newSGLangBackend(nil))
			if err == nil {
				t.Fatalf("want an error when the engine's /dev/shm is unshareable (%s)", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error must name %q, got: %v", tc.want, err)
			}
		})
	}
}

func TestSGLangInjectEngineConfigWorkerHasMemoryBudget(t *testing.T) {
	// The worker holds the L1 in a memory-backed tmpfs charged to its cgroup, so it
	// must carry a matching memory request+limit (l1SizeGB + 1Gi) — otherwise the L1
	// is invisible to the scheduler and can overcommit the node.
	pod := &corev1.PodSpec{Containers: []corev1.Container{{
		Name: enginewire.SGLangEngineContainerName, Image: "sglang:test",
	}}}
	cb := newSGLangBackend(map[string]string{"l1SizeGB": "8"})
	if err := NewAdapter().InjectEngineConfig(pod, "r.svc:6379", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	w := findInitContainer(pod.InitContainers, "lmcache-mp-worker")
	if w == nil {
		t.Fatalf("worker not injected")
	}
	want := resource.MustParse("9Gi") // 8Gi L1 + 1Gi headroom
	req, hasReq := w.Resources.Requests[corev1.ResourceMemory]
	lim, hasLim := w.Resources.Limits[corev1.ResourceMemory]
	if !hasReq || !hasLim {
		t.Fatalf("worker must carry a memory request AND limit, got %+v", w.Resources)
	}
	if req.Cmp(want) != 0 || lim.Cmp(want) != 0 {
		t.Fatalf("worker memory = req %s / lim %s, want %s (l1SizeGB + 1Gi headroom)", req.String(), lim.String(), want.String())
	}
	// The tmpfs sizeLimit must agree with the container budget.
	v := findVolume(pod.Volumes, "lmcache-dshm")
	if v == nil || v.EmptyDir == nil || v.EmptyDir.SizeLimit == nil {
		t.Fatalf("/dev/shm tmpfs must be size-bounded, got %+v", v)
	}
	if v.EmptyDir.SizeLimit.Cmp(want) != 0 {
		t.Fatalf("tmpfs sizeLimit = %s, want %s (must match the worker's memory budget)", v.EmptyDir.SizeLimit.String(), want.String())
	}
}

func TestSGLangInjectEngineConfigSanitizesNumericConfig(t *testing.T) {
	// chunkSize/mpPort/l1SizeGB flow into the worker's `sh -c` command; a
	// non-positive-integer (typo, or a shell-injection attempt) MUST fall back to
	// the safe default and never reach the shell verbatim.
	// wantArg = the default arg that must appear; danger = the fragment that must
	// NOT (asserting the bad value was not substituted, precisely — a short value
	// like "0" is a substring of 127.0.0.1, so check the arg-in-context instead).
	cases := []struct{ key, bad, wantArg, danger string }{
		{"chunkSize", "256; rm -rf /", "--chunk-size 256", "rm -rf"},
		{"chunkSize", "$(evil)", "--chunk-size 256", "$(evil)"},
		{"mpPort", "5555 && curl evil", "--port 5555", "curl evil"},
		{"mpPort", "-1", "--port 5555", "--port -1"},
		{"mpPort", "99999", "--port 5555", "--port 99999"}, // > 65535 → default
		{"l1SizeGB", "4; cat /etc/passwd", "--l1-size-gb 4", "/etc/passwd"},
		{"l1SizeGB", "abc", "--l1-size-gb 4", "--l1-size-gb abc"},
		{"l1SizeGB", "0", "--l1-size-gb 4", "--l1-size-gb 0"},
		{"l1SizeGB", "999999999", "--l1-size-gb 4", "--l1-size-gb 999999999"}, // huge → default (bounded /dev/shm)
	}
	for _, tc := range cases {
		t.Run(tc.key+"="+tc.bad, func(t *testing.T) {
			cb := newSGLangBackend(map[string]string{tc.key: tc.bad})
			pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName, Image: "img"}}}
			if err := NewAdapter().InjectEngineConfig(pod, "r.svc:6379", cb); err != nil {
				t.Fatalf("InjectEngineConfig: %v", err)
			}
			joined := strings.Join(findInitContainer(pod.InitContainers, "lmcache-mp-worker").Args, " ")
			if !strings.Contains(joined, tc.wantArg) {
				t.Fatalf("want %q (sanitized to default); worker command: %s", tc.wantArg, joined)
			}
			if strings.Contains(joined, tc.danger) {
				t.Fatalf("unsanitized value reached the worker shell command (%q): %s", tc.danger, joined)
			}
		})
	}
}

func TestSGLangInjectEngineConfigFailOpen(t *testing.T) {
	a := NewAdapter()
	trueVal, falseVal := true, false
	cases := []struct {
		name     string
		failOpen *bool
		want     string
	}{
		{"default (unset → true)", nil, "true"},
		{"explicit true", &trueVal, "true"},
		{"explicit false", &falseVal, "false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cb := newSGLangBackend(nil)
			cb.Spec.Integration.FailOpen = tc.failOpen
			pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName}}}
			if err := a.InjectEngineConfig(pod, "x.svc:65432", cb); err != nil {
				t.Fatalf("InjectEngineConfig: %v", err)
			}
			if v, _ := lookupEnv(pod.Containers[0].Env, enginewire.EnvInferenceCacheFailOpen); v != tc.want {
				t.Fatalf("%s = %q, want %q", enginewire.EnvInferenceCacheFailOpen, v, tc.want)
			}
		})
	}
}

func TestSGLangInjectEngineConfigBadInput(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	good := &corev1.PodSpec{Containers: []corev1.Container{{Name: enginewire.SGLangEngineContainerName}}}
	cases := []struct {
		name string
		fn   func() error
	}{
		{"nil pod", func() error { return a.InjectEngineConfig(nil, "x.svc:65432", cb) }},
		{"nil cache", func() error { return a.InjectEngineConfig(good, "x.svc:65432", nil) }},
		{"empty endpoint", func() error { return a.InjectEngineConfig(good, "", cb) }},
		{"no containers", func() error { return a.InjectEngineConfig(&corev1.PodSpec{}, "x.svc:65432", cb) }},
		// The resp --l2-adapter takes an INTEGER port, emitted unquoted into JSON. A
		// non-numeric or out-of-range port would render invalid JSON, the worker would
		// fail to parse it and never bind its ZMQ port, and the engine would sit behind
		// the startup probe forever — reject at admission and let the webhook fail open.
		{"non-numeric port", func() error { return a.InjectEngineConfig(good, "r.svc:redis", cb) }},
		{"port out of range", func() error { return a.InjectEngineConfig(good, "r.svc:70000", cb) }},
		{"zero port", func() error { return a.InjectEngineConfig(good, "r.svc:0", cb) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestSGLangInjectRouterConfigIsNoop(t *testing.T) {
	a := NewAdapter()
	cb := newSGLangBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "router", Env: []corev1.EnvVar{{Name: "EXISTING", Value: "x"}}}}}
	if err := a.InjectRouterConfig(pod, "x.svc:65432", cb); err != nil {
		t.Fatalf("InjectRouterConfig: %v", err)
	}
	if len(pod.Containers[0].Env) != 1 || pod.Containers[0].Env[0].Name != "EXISTING" {
		t.Fatalf("InjectRouterConfig modified container env: %v", pod.Containers[0].Env)
	}
	// Truly a no-op even on bad input (router-less backend must never force
	// callers to special-case it).
	if err := a.InjectRouterConfig(nil, "x", cb); err != nil {
		t.Fatalf("InjectRouterConfig(nil pod) = %v, want nil", err)
	}
}

func TestSGLangObservationSidecarShape(t *testing.T) {
	a := NewAdapter(runtimeadapter.WithSubscriberImage(runtimeadapter.DefaultSubscriberImage))
	cb := newSGLangBackend(map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-a", Namespace: "engines"}}

	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c == nil {
		t.Fatalf("ObservationSidecar returned nil for sglang+LMCache with a model + image set")
	}
	if c.Name != runtimeadapter.SubscriberContainerName {
		t.Fatalf("container name = %q, want %q", c.Name, runtimeadapter.SubscriberContainerName)
	}
	if !envHasFieldRef(c.Env, "POD_NAME", "metadata.name") || !envHasFieldRef(c.Env, "POD_NAMESPACE", "metadata.namespace") {
		t.Fatalf("downward-API env missing: %v", c.Env)
	}
	wantArgs := []string{
		"--engine-endpoint=tcp://127.0.0.1:5557",
		"--server=" + runtimeadapter.DefaultPolicyServerGRPCAddress,
		"--replica-id=$(POD_NAME)",
		"--tenant-id=$(POD_NAMESPACE)",
		"--model-id=Qwen/Qwen2.5-0.5B-Instruct",
		// Load-bearing: the index keys on hash_scheme, so the SGLang subscriber
		// MUST tag its reports "sglang" to stay disjoint from vLLM entries.
		"--hash-scheme=sglang",
		// LMCache is an L2 tier behind SGLang, same as vLLM+LMCache — drop
		// BlockRemoved rather than forward it as PREFIX_EVICTED.
		"--ignore-block-removed=true",
	}
	for _, want := range wantArgs {
		if !containsArg(c.Args, want) {
			t.Fatalf("subscriber args missing %q; args = %v", want, c.Args)
		}
	}
	if c.SecurityContext == nil || c.SecurityContext.RunAsNonRoot == nil || !*c.SecurityContext.RunAsNonRoot {
		t.Fatalf("SecurityContext must run non-root; got %+v", c.SecurityContext)
	}
}

func TestSGLangObservationSidecarArgsParseAgainstSubscriberFlagSet(t *testing.T) {
	// The Go flag package exits on unknown flags, so a sidecar arg the
	// kvevent-subscriber binary doesn't recognise crashes the container at
	// startup. Parse the rendered args through a FlagSet mirroring the binary's
	// event-path flag surface and assert they parse cleanly. Keep in sync with
	// cmd/kvevent-subscriber/main.go.
	a := NewAdapter(runtimeadapter.WithSubscriberImage(runtimeadapter.DefaultSubscriberImage))
	cb := newSGLangBackend(map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-a", Namespace: "engines"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil || c == nil {
		t.Fatalf("ObservationSidecar: (%v, %v)", c, err)
	}

	fs := flag.NewFlagSet("kvevent-subscriber", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.String("engine-endpoint", "", "")
	fs.String("topic", "", "")
	fs.String("server", "", "")
	fs.String("replica-id", "", "")
	fs.String("model-id", "", "")
	fs.String("tenant-id", "", "")
	fs.String("hash-scheme", "", "")
	fs.Duration("window", 0, "")
	fs.Bool("ignore-block-removed", false, "")
	if err := fs.Parse(c.Args); err != nil {
		t.Fatalf("rendered sidecar args rejected by subscriber FlagSet: %v\nargs = %v", err, c.Args)
	}
	// Belt-and-suspenders: parse a control case that should fail, so the
	// FlagSet isn't silently accepting unknown flags (rules out a tautology
	// if someone passes the wrong FlagSet mode).
	if err := fs.Parse(append(c.Args, "--definitely-not-a-real-flag=x")); err == nil {
		t.Fatalf("control: FlagSet must reject unknown flag --definitely-not-a-real-flag")
	}
}

func TestSGLangObservationSidecarHonoursOptions(t *testing.T) {
	a := NewAdapter(
		runtimeadapter.WithSubscriberImage("registry.example.com/subscriber:pinned"),
		runtimeadapter.WithPolicyServerGRPCAddress("ic-server.custom-ns.svc.cluster.local:9090"),
	)
	cb := newSGLangBackend(map[string]string{"model": "MyOrg/MyModel"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-z", Namespace: "engines"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil || c == nil {
		t.Fatalf("ObservationSidecar: (%v, %v)", c, err)
	}
	if c.Image != "registry.example.com/subscriber:pinned" {
		t.Fatalf("image override ignored: got %q", c.Image)
	}
	if !containsArg(c.Args, "--server=ic-server.custom-ns.svc.cluster.local:9090") {
		t.Fatalf("server address override ignored; args = %v", c.Args)
	}
}

func TestSGLangObservationSidecarSkipsWithoutModel(t *testing.T) {
	a := NewAdapter(runtimeadapter.WithSubscriberImage(runtimeadapter.DefaultSubscriberImage))
	cb := newSGLangBackend(nil)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-a"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c != nil {
		t.Fatalf("expected nil sidecar when backendConfig.model is unset, got %+v", c)
	}
}

func TestSGLangObservationSidecarSkipsWithoutImage(t *testing.T) {
	a := NewAdapter() // no image configured → auto-attach opt-out
	cb := newSGLangBackend(map[string]string{"model": "MyOrg/MyModel"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sglang-a"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c != nil {
		t.Fatalf("expected nil sidecar when subscriber image is unconfigured, got %+v", c)
	}
}

func TestSGLangObservationSidecarBadInput(t *testing.T) {
	a := NewAdapter(runtimeadapter.WithSubscriberImage(runtimeadapter.DefaultSubscriberImage))
	cb := newSGLangBackend(map[string]string{"model": "m"})
	cases := []struct {
		name string
		cb   *cachev1alpha1.CacheBackend
		pod  *corev1.Pod
	}{
		{"nil cache", nil, &corev1.Pod{}},
		{"nil pod", cb, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := a.ObservationSidecar(tc.cb, tc.pod); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestSGLangReservedArgs(t *testing.T) {
	got := NewAdapter().ReservedArgs()
	want := []string{enginewire.SGLangEnableLMCacheArg, enginewire.SGLangConfigFileArg}
	if len(got) != len(want) {
		t.Fatalf("ReservedArgs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ReservedArgs = %v, want %v", got, want)
		}
	}
}

func TestSGLangReservedEnv(t *testing.T) {
	got := NewAdapter().ReservedEnv()
	want := []string{
		enginewire.EnvLMCacheUseExperimental,
		enginewire.EnvInferenceCacheFailOpen,
	}
	if len(got) != len(want) {
		t.Fatalf("ReservedEnv = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ReservedEnv = %v, want %v", got, want)
		}
	}
	// Negative control: vLLM-only env MUST NOT be reserved for SGLang (never
	// injected), the LMCACHE_* tunables stay overridable, and LMCACHE_REMOTE_URL
	// (the old lm:// wire) is gone in MP mode so it must not be reserved either.
	forbidden := map[string]bool{
		enginewire.EnvVLLMUseV1:          true,
		enginewire.EnvPythonHashSeed:     true,
		enginewire.EnvLMCacheChunkSize:   true,
		enginewire.EnvLMCacheRemoteSerde: true,
		enginewire.EnvLMCacheRemoteURL:   true,
	}
	for _, name := range got {
		if forbidden[name] {
			t.Errorf("env %q must NOT be reserved for SGLang", name)
		}
	}
}

func TestSGLangEngineContainerName(t *testing.T) {
	if got := NewAdapter().EngineContainerName(); got != enginewire.SGLangEngineContainerName {
		t.Fatalf("EngineContainerName = %q, want %q", got, enginewire.SGLangEngineContainerName)
	}
}

// --- local test helpers (the runtime package's helpers live in a different
// test package and aren't importable here) ---

func lookupEnv(env []corev1.EnvVar, name string) (string, bool) {
	for _, e := range env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func envHasFieldRef(env []corev1.EnvVar, name, path string) bool {
	for _, e := range env {
		if e.Name == name && e.ValueFrom != nil && e.ValueFrom.FieldRef != nil && e.ValueFrom.FieldRef.FieldPath == path {
			return true
		}
	}
	return false
}
