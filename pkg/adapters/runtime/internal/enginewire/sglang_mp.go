package enginewire

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// SGLang engine-side wire — LMCache multiprocess (MP) mode.
//
// SGLang does NOT consume a standalone lm:// server the way vLLM does; it drives
// LMCache through a node-local MP worker. The engine attaches to that worker over
// ZMQ (mp_host/mp_port) + a shared-memory data path, configured by a
// --lmcache-config-file (the lm://-style LMCACHE_* env is ignored). The worker
// holds L1 (host memory, in /dev/shm) and offloads to a shared L2 (its
// --l2-adapter — the managed Redis this backend provisions). GPU-validated; full
// design + evidence in docs/design/sglang-lmcache-mp-mode.md.
const (
	// EnvLMCacheUseExperimental gates SGLang's experimental LMCache path; it MUST
	// be "True" for --enable-lmcache to engage the connector.
	EnvLMCacheUseExperimental = "LMCACHE_USE_EXPERIMENTAL"
	lmcacheUseExperimentalVal = "True"
	// SGLangEngineContainerName is the conventional name of the SGLang engine
	// container in a pod the adapter mutates. A single-container pod is also
	// treated as the engine.
	SGLangEngineContainerName = "sglang"
	// SGLangEnableLMCacheArg turns the LMCache connector on (a store_true flag, no
	// value). Exported so the adapter can reserve it against engineOverrides.
	SGLangEnableLMCacheArg = "--enable-lmcache"
	// SGLangConfigFileArg points the engine at the MP config file the worker
	// writes. Exported so the adapter can reserve it (suppressing it un-wires MP
	// mode).
	SGLangConfigFileArg = "--lmcache-config-file"

	// sglangMPWorkerContainerName is the node-local MP worker native sidecar.
	sglangMPWorkerContainerName = "lmcache-mp-worker"
	// sglangConfigVolumeName / MountPath / FileName: the shared dir the worker
	// writes the MP config into and the engine reads via --lmcache-config-file.
	sglangConfigVolumeName = "lmcache-config"
	sglangConfigMountPath  = "/etc/lmcache"
	sglangConfigFileName   = "config.yaml"
	// sglangShmVolumeName / MountPath: the tmpfs the MP L1 lives in. Too small
	// (default 64Mi) silently falls back to slow pickle serialization, so it is
	// sized from the L1 budget.
	sglangShmVolumeName = "lmcache-dshm"
	sglangShmMountPath  = "/dev/shm"

	sglangDefaultMPPort   = "5555"
	sglangDefaultL1SizeGB = "4"

	// Upper bounds for the sanitized numeric tunables (see sglangIntInRangeOr).
	sglangMaxChunkSize = 65536 // generous; chunk sizes are small
	sglangMaxTCPPort   = 65535 // a valid TCP port
	sglangMaxL1SizeGB  = 1024  // 1 TiB — bounded so ParseQuantity always sizes /dev/shm

	// BackendConfig override keys.
	cfgKeyWorkerImage = "workerImage"
	cfgKeyL1SizeGB    = "l1SizeGB"
	cfgKeyMPPort      = "mpPort"
)

// InjectSGLangLMCache wires an SGLang engine pod for LMCache MP mode. It mutates
// pod in place, idempotently (a re-injection is a no-op), and adds:
//
//   - a node-local MP-worker native sidecar (a restartPolicy: Always init
//     container) that writes the MP config file then runs the LMCache MP server on
//     127.0.0.1, offloading to the shared L2 (resp -> the managed Redis endpoint).
//     NVIDIA_VISIBLE_DEVICES=all lets the GPU-less sidecar CUDA-IPC the engine's
//     GPU without consuming a device-plugin allocation;
//   - shared emptyDir volumes for the config file and /dev/shm (the L1 tier);
//   - on the engine container: --enable-lmcache, --lmcache-config-file, the
//     LMCACHE_USE_EXPERIMENTAL + INFERENCECACHE_FAIL_OPEN env, and the shared
//     volume mounts.
//
// endpoint is the managed Redis L2 address (host:port) the reconciler published to
// status.endpoint; it is used only to build the worker's resp --l2-adapter (the
// engine itself dials the local worker, never this endpoint). The engine container is
// [SGLangEngineContainerName]; a single-container pod is accepted, a
// multi-container pod with no `sglang` container is rejected.
//
// Note: unlike the old lm:// wire, this does NOT inject LMCACHE_REMOTE_URL / serde
// / local-CPU env — SGLang MP mode ignores it.
func InjectSGLangLMCache(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	if err := ValidateInjectInputs(pod, endpoint, cache, "engine"); err != nil {
		return err
	}
	i, err := EngineContainerIndexNamed(pod, SGLangEngineContainerName)
	if err != nil {
		return err
	}
	cfg := cache.Spec.BackendConfig
	// SECURITY: chunkSize/mpPort/l1SizeGB are substituted into the worker's `sh -c`
	// command and into resource sizing, so they MUST be plain positive integers — a
	// non-integer (typo or a shell-metacharacter injection attempt) falls back to
	// the safe default and never reaches the shell. sglangPositiveIntOr is the
	// sanitization boundary; it also guarantees the /dev/shm sizeLimit is bounded.
	chunkSize := sglangIntInRangeOr(cfg, cfgKeyChunkSize, defaultChunkSize, sglangMaxChunkSize)
	mpPort := sglangIntInRangeOr(cfg, cfgKeyMPPort, sglangDefaultMPPort, sglangMaxTCPPort)
	l1SizeGB := sglangIntInRangeOr(cfg, cfgKeyL1SizeGB, sglangDefaultL1SizeGB, sglangMaxL1SizeGB)

	l2Adapter, err := sglangL2AdapterJSON(endpoint)
	if err != nil {
		return err
	}

	worker := sglangMPWorkerContainer(pod.Containers[i].Image, cfg, chunkSize, mpPort, l1SizeGB, l2Adapter)
	pod.InitContainers = upsertContainerByName(pod.InitContainers, worker)

	pod.Volumes = upsertVolumeByName(pod.Volumes, corev1.Volume{
		Name:         sglangConfigVolumeName,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
	pod.Volumes = upsertVolumeByName(pod.Volumes, sglangShmVolume(l1SizeGB))

	c := &pod.Containers[i]
	c.Args = UpsertFlag(c.Args, SGLangEnableLMCacheArg)
	c.Args = UpsertArgPair(c.Args, SGLangConfigFileArg, sglangConfigMountPath+"/"+sglangConfigFileName)
	c.Env = UpsertEnv(c.Env, corev1.EnvVar{Name: EnvLMCacheUseExperimental, Value: lmcacheUseExperimentalVal})
	c.Env = UpsertEnv(c.Env, corev1.EnvVar{Name: EnvInferenceCacheFailOpen, Value: FailOpenString(cache)})
	c.VolumeMounts = upsertMountByName(c.VolumeMounts, corev1.VolumeMount{Name: sglangConfigVolumeName, MountPath: sglangConfigMountPath})
	c.VolumeMounts = upsertMountByName(c.VolumeMounts, corev1.VolumeMount{Name: sglangShmVolumeName, MountPath: sglangShmMountPath})
	return nil
}

// sglangMPWorkerContainer builds the node-local MP-worker native sidecar. It
// writes the engine's config file (its own mp_host/mp_port) then execs the MP
// server — both in this container so, gated by the startupProbe, the config exists
// and the server listens before the engine starts. The worker image defaults to
// the engine image (guaranteeing the same lmcache version — the two speak the MP
// wire) and is overridable via backendConfig.workerImage.
func sglangMPWorkerContainer(engineImage string, cfg map[string]string, chunkSize, mpPort, l1SizeGB, l2Adapter string) corev1.Container {
	image := ConfigOr(cfg, cfgKeyWorkerImage, engineImage)
	configPath := sglangConfigMountPath + "/" + sglangConfigFileName
	// The validated invocation is `python3 -m lmcache.v1.multiprocess.server`
	// (the documented `lmcache server` CLI is the equivalent entrypoint). mp_host
	// is 127.0.0.1 — the worker shares the engine pod's network namespace.
	script := fmt.Sprintf(
		"set -e; printf 'chunk_size: %s\\nmp_host: \"127.0.0.1\"\\nmp_port: %s\\n' > %s; "+
			"exec python3 -m lmcache.v1.multiprocess.server --host 127.0.0.1 --port %s "+
			"--chunk-size %s --l1-size-gb %s --eviction-policy LRU --l2-adapter %s",
		chunkSize, mpPort, configPath, mpPort, chunkSize, l1SizeGB, shellSingleQuote(l2Adapter))

	always := corev1.ContainerRestartPolicyAlways
	return corev1.Container{
		Name:          sglangMPWorkerContainerName,
		Image:         image,
		RestartPolicy: &always, // native sidecar: starts + gates ready before the engine
		Command:       []string{"sh", "-c"},
		Args:          []string{script},
		// The GPU-less sidecar must SEE the engine's GPU to CUDA-IPC its KV; it
		// consumes no device-plugin allocation (no nvidia.com/gpu limit).
		Env: []corev1.EnvVar{{Name: "NVIDIA_VISIBLE_DEVICES", Value: "all"}},
		VolumeMounts: []corev1.VolumeMount{
			{Name: sglangConfigVolumeName, MountPath: sglangConfigMountPath},
			{Name: sglangShmVolumeName, MountPath: sglangShmMountPath},
		},
		// The MP server binds mp_port on loopback, which a pod-IP tcp/http probe
		// cannot reach — so exec a loopback check inside the container. This gates
		// the engine's start on the ZMQ server being up (fail-open at the shared L2
		// is separate: the worker starts L1-only even when Redis is unreachable).
		StartupProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: []string{
					"python3", "-c",
					fmt.Sprintf("import socket; socket.create_connection(('127.0.0.1',%s),1)", mpPort),
				}},
			},
			PeriodSeconds:    3,
			FailureThreshold: 40,
		},
		SecurityContext: &corev1.SecurityContext{
			Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"IPC_LOCK"}},
		},
	}
}

// sglangL2AdapterJSON returns the worker's --l2-adapter config: the resp adapter
// pointed at the managed Redis endpoint (host:port).
//
// Operator-supplied ("bring your own") L2 stores are deliberately NOT supported
// yet: skipping the managed Redis clears status.endpoint, and the pod webhook's
// empty-endpoint gate then skips injection entirely, so a BYO backend would cache
// nothing. Supporting it needs that gate to become adapter-aware first — the gate
// exists only to protect vLLM's lm:// dial target, which SGLang MP does not have.
// See the tracking issue linked from the package doc.
func sglangL2AdapterJSON(endpoint string) (string, error) {
	host, port, ok := splitLMCacheHostPort(strings.TrimSpace(endpoint))
	if !ok || host == "" || port == "" {
		return "", fmt.Errorf("inject engine config: endpoint %q is not a host:port for the resp L2 adapter", endpoint)
	}
	// port is emitted unquoted (the resp adapter expects an integer).
	return fmt.Sprintf(`{"type":"resp","host":%q,"port":%s}`, host, port), nil
}

// sglangShmVolume returns the /dev/shm tmpfs volume sized from the L1 budget
// (l1SizeGB + 1Gi headroom). l1SizeGB is a sanitized positive integer (see
// sglangPositiveIntOr), so the sizeLimit is always bounded — a memory-backed
// emptyDir must never be left unbounded or it can exhaust node memory.
func sglangShmVolume(l1SizeGB string) corev1.Volume {
	v := corev1.Volume{
		Name: sglangShmVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory},
		},
	}
	if q, err := resource.ParseQuantity(l1SizeGB + "Gi"); err == nil {
		q.Add(resource.MustParse("1Gi"))
		v.EmptyDir.SizeLimit = &q
	}
	return v
}

// sglangIntInRangeOr returns cfg[key] iff it is an integer in [1, max], else
// fallback. This is a hard sanitization boundary: chunkSize/mpPort/l1SizeGB are
// substituted into the worker's `sh -c` command and into resource sizing, so a
// non-integer — a typo or an injection attempt like "4; rm -rf /" — must never
// reach the shell, AND an out-of-range value (a port > 65535, or an l1SizeGB so
// large that resource.ParseQuantity can't size /dev/shm and leaves it unbounded)
// must be rejected. It falls back to the (in-range integer) default rather than
// failing injection, so a mistyped tunable degrades to the default instead of
// crashing the pod webhook. fallback MUST itself be an in-range positive integer.
func sglangIntInRangeOr(cfg map[string]string, key, fallback string, max int) string {
	v := strings.TrimSpace(ConfigOr(cfg, key, ""))
	if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= max {
		return v
	}
	return fallback
}

// shellSingleQuote wraps s in single quotes for safe use in a `sh -c` script,
// escaping any embedded single quotes. The L2 JSON contains double quotes, so
// single-quoting keeps them intact.
func shellSingleQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// upsertContainerByName replaces the container with the same Name, or appends it.
func upsertContainerByName(cs []corev1.Container, c corev1.Container) []corev1.Container {
	for i := range cs {
		if cs[i].Name == c.Name {
			cs[i] = c
			return cs
		}
	}
	return append(cs, c)
}

// upsertVolumeByName replaces the volume with the same Name, or appends it.
func upsertVolumeByName(vs []corev1.Volume, v corev1.Volume) []corev1.Volume {
	for i := range vs {
		if vs[i].Name == v.Name {
			vs[i] = v
			return vs
		}
	}
	return append(vs, v)
}

// upsertMountByName replaces the volume mount with the same Name, or appends it.
func upsertMountByName(ms []corev1.VolumeMount, m corev1.VolumeMount) []corev1.VolumeMount {
	for i := range ms {
		if ms[i].Name == m.Name {
			ms[i] = m
			return ms
		}
	}
	return append(ms, m)
}
