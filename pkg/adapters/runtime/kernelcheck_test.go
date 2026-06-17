package runtime

import (
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
	if len(c.Command) == 0 || c.Command[0] != "python3" {
		t.Errorf("command = %v, want python3 ...", c.Command)
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

func TestKernelCheckCopiesEnginePullPolicyAndSecurityContext(t *testing.T) {
	a := NewVLLMLMCacheAdapter().(InitContainerProvider)
	nonRoot := true
	engineSC := &corev1.SecurityContext{RunAsNonRoot: &nonRoot}
	pod := gpuEnginePod("img")
	pod.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways
	pod.Spec.Containers[0].SecurityContext = engineSC
	c, err := a.KernelCheckInitContainer(cbWithKernelCheck(""), pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Fatal("expected an init container")
	}
	// ImagePullPolicy copied from the engine (not hard-coded), so a mutable tag
	// with Always can't run a stale cached check image.
	if c.ImagePullPolicy != corev1.PullAlways {
		t.Errorf("ImagePullPolicy = %q, want copied PullAlways", c.ImagePullPolicy)
	}
	// SecurityContext copied so a restricted-PSA-compliant engine pod stays
	// admissible after the init container is appended.
	if c.SecurityContext == nil || c.SecurityContext.RunAsNonRoot == nil || !*c.SecurityContext.RunAsNonRoot {
		t.Errorf("SecurityContext not copied from engine: %+v", c.SecurityContext)
	}
	// Must be a deep copy, not an alias into the informer-cached pod.
	if c.SecurityContext == engineSC {
		t.Error("SecurityContext must be a deep copy, not an alias of the engine container's")
	}
}
