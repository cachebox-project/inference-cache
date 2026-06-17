package runtime

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func gpuEnginePod(image string) *corev1.Pod {
	return &corev1.Pod{
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:  EngineContainerName,
			Image: image,
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{gpuResourceName: resource.MustParse("1")},
			},
		}}},
	}
}

func cbWithKernelCheck(mode string) *cachev1alpha1.CacheBackend {
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns"},
		Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeLMCache},
	}
	if mode != "" {
		cb.Annotations = map[string]string{AnnotationLMCacheKernelCheck: mode}
	}
	return cb
}

func TestKernelCheckAutoInjectsOnGPUPod(t *testing.T) {
	a := NewVLLMLMCacheAdapter().(InitContainerProvider)
	c, err := a.KernelCheckInitContainer(cbWithKernelCheck(""), gpuEnginePod("vllm/img:cu129"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Fatal("expected an init container for a GPU LMCache engine pod under auto mode")
	}
	if c.Name != LMCacheKernelCheckContainerName {
		t.Errorf("name = %q, want %q", c.Name, LMCacheKernelCheckContainerName)
	}
	if c.Image != "vllm/img:cu129" {
		t.Errorf("image = %q, want engine image", c.Image)
	}
	if c.TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		t.Errorf("terminationMessagePolicy = %q, want File", c.TerminationMessagePolicy)
	}
	// auto == report-only: the command is shell-wrapped so the init container
	// always exits 0 (fail-open), but still runs the detector script.
	if len(c.Command) == 0 || c.Command[0] != "/bin/sh" {
		t.Errorf("auto/report-only command = %v, want /bin/sh wrapper", c.Command)
	}
	if c.Command[len(c.Command)-1] != kernelCheckScript {
		t.Error("command must carry the detector script as the final element")
	}
	for _, e := range c.Env {
		if e.Name == EnvKernelCheckStrict && e.Value == "1" {
			t.Error("auto mode must not set STRICT=1")
		}
	}
	if _, ok := c.Resources.Limits[gpuResourceName]; ok {
		t.Error("init container must not request nvidia.com/gpu")
	}
}

func TestKernelCheckCommandFormByMode(t *testing.T) {
	a := NewVLLMLMCacheAdapter().(InitContainerProvider)

	// report-only: shell wrapper that unconditionally exits 0 (fail-open at the
	// pod level — python3-missing / OOM during import must not block the pod).
	ro, _ := a.KernelCheckInitContainer(cbWithKernelCheck(KernelCheckModeReportOnly), gpuEnginePod("img"))
	if ro == nil || ro.Command[0] != "/bin/sh" {
		t.Fatalf("report-only command = %v, want /bin/sh wrapper", commandOf(ro))
	}
	joined := strings.Join(ro.Command, " ")
	if !strings.Contains(joined, "exit 0") {
		t.Errorf("report-only wrapper must force exit 0; got %v", ro.Command)
	}

	// strict: python3 runs directly so a real failure (incl. inability to run)
	// propagates and holds the pod.
	st, _ := a.KernelCheckInitContainer(cbWithKernelCheck(KernelCheckModeStrict), gpuEnginePod("img"))
	if st == nil || st.Command[0] != "python3" {
		t.Fatalf("strict command = %v, want direct python3", commandOf(st))
	}
	if strings.Join(st.Command, " ") == joined {
		t.Error("strict command must differ from the report-only fail-open wrapper")
	}
}

func commandOf(c *corev1.Container) []string {
	if c == nil {
		return nil
	}
	return c.Command
}

func TestKernelCheckAutoSkipsCPUPod(t *testing.T) {
	a := NewVLLMLMCacheAdapter().(InitContainerProvider)
	cpuPod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
		Name: EngineContainerName, Image: "vllm/cpu",
	}}}}
	c, err := a.KernelCheckInitContainer(cbWithKernelCheck(""), cpuPod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c != nil {
		t.Fatal("auto mode must skip a non-GPU engine pod")
	}
}

func TestKernelCheckOffSkipsEvenGPU(t *testing.T) {
	a := NewVLLMLMCacheAdapter().(InitContainerProvider)
	c, _ := a.KernelCheckInitContainer(cbWithKernelCheck(KernelCheckModeOff), gpuEnginePod("img"))
	if c != nil {
		t.Fatal("off mode must never inject")
	}
}

func TestKernelCheckReportOnlyInjectsOnCPU(t *testing.T) {
	a := NewVLLMLMCacheAdapter().(InitContainerProvider)
	cpuPod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName, Image: "img"}}}}
	c, _ := a.KernelCheckInitContainer(cbWithKernelCheck(KernelCheckModeReportOnly), cpuPod)
	if c == nil {
		t.Fatal("report-only must inject regardless of GPU request")
	}
	for _, e := range c.Env {
		if e.Name == EnvKernelCheckStrict && e.Value == "1" {
			t.Error("report-only must not set STRICT=1")
		}
	}
}

func TestKernelCheckStrictSetsStrictEnv(t *testing.T) {
	a := NewVLLMLMCacheAdapter().(InitContainerProvider)
	c, _ := a.KernelCheckInitContainer(cbWithKernelCheck(KernelCheckModeStrict), gpuEnginePod("img"))
	if c == nil {
		t.Fatal("strict must inject")
	}
	got := ""
	for _, e := range c.Env {
		if e.Name == EnvKernelCheckStrict {
			got = e.Value
		}
	}
	if got != "1" {
		t.Errorf("STRICT env = %q, want \"1\"", got)
	}
}

func TestKernelCheckMultiContainerNoEngineNameSkips(t *testing.T) {
	a := NewVLLMLMCacheAdapter().(InitContainerProvider)
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: "foo", Image: "a", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{gpuResourceName: resource.MustParse("1")}}},
		{Name: "bar", Image: "b"},
	}}}
	c, err := a.KernelCheckInitContainer(cbWithKernelCheck(KernelCheckModeStrict), pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c != nil {
		t.Fatal("must skip when the engine container can't be resolved (multi-container, no 'vllm' name)")
	}
}

func TestKernelCheckCopiesEngineEnvironment(t *testing.T) {
	a := NewVLLMLMCacheAdapter().(InitContainerProvider)
	nonRoot := true
	engineSC := &corev1.SecurityContext{RunAsNonRoot: &nonRoot}
	pod := gpuEnginePod("img")
	eng := &pod.Spec.Containers[0]
	eng.ImagePullPolicy = corev1.PullAlways
	eng.SecurityContext = engineSC
	eng.WorkingDir = "/work"
	eng.Env = []corev1.EnvVar{{Name: "LD_LIBRARY_PATH", Value: "/opt/cuda/lib64"}}
	eng.EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "engine-cfg"}}}}
	eng.VolumeMounts = []corev1.VolumeMount{{Name: "cuda", MountPath: "/opt/cuda"}}

	c, err := a.KernelCheckInitContainer(cbWithKernelCheck(""), pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Fatal("expected an init container")
	}
	// Pull policy copied (mutable-tag/Always skew protection).
	if c.ImagePullPolicy != corev1.PullAlways {
		t.Errorf("ImagePullPolicy = %q, want copied PullAlways", c.ImagePullPolicy)
	}
	// SecurityContext copied (restricted-PSA admissibility), deep — not an alias.
	if c.SecurityContext == nil || c.SecurityContext.RunAsNonRoot == nil || !*c.SecurityContext.RunAsNonRoot {
		t.Errorf("SecurityContext not copied from engine: %+v", c.SecurityContext)
	}
	if c.SecurityContext == engineSC {
		t.Error("SecurityContext must be a deep copy, not an alias of the engine container's")
	}
	// Env / EnvFrom / VolumeMounts / WorkingDir copied so the check loads c_ops
	// in the engine's real environment (an engine LD_LIBRARY_PATH the checker
	// lacked would false-fail it).
	if c.WorkingDir != "/work" {
		t.Errorf("WorkingDir = %q, want copied /work", c.WorkingDir)
	}
	gotEnv := false
	for _, e := range c.Env {
		if e.Name == "LD_LIBRARY_PATH" && e.Value == "/opt/cuda/lib64" {
			gotEnv = true
		}
	}
	if !gotEnv {
		t.Errorf("engine Env not copied onto init container: %+v", c.Env)
	}
	if len(c.EnvFrom) != 1 || c.EnvFrom[0].ConfigMapRef == nil || c.EnvFrom[0].ConfigMapRef.Name != "engine-cfg" {
		t.Errorf("engine EnvFrom not copied: %+v", c.EnvFrom)
	}
	if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].Name != "cuda" || c.VolumeMounts[0].MountPath != "/opt/cuda" {
		t.Errorf("engine VolumeMounts not copied: %+v", c.VolumeMounts)
	}
}
