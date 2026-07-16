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
	// envSGLangMPWorkerManaged marks the MP worker as adapter-rendered, so a
	// re-injection can tell OUR container from an operator's that happens to carry
	// the same name (see sglangWireIsOurs). It is inert to LMCache — an unknown env
	// var the worker never reads.
	envSGLangMPWorkerManaged = "INFERENCECACHE_MP_WORKER"
	sglangMPWorkerManagedVal = "true"
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
// pod in place — atomically (on error pod is untouched) and idempotently (a
// re-injection converges on the current render instead of duplicating it) — and
// adds:
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
// multi-container pod with no `sglang` container is rejected. A pre-existing
// container or volume that squats one of the reserved names this wire renders — and
// that this adapter did not render (see [sglangWireIsOurs]) — is also rejected
// rather than overwritten; the pod webhook turns that into a fail-open admit.
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

	// Mutate a COPY and commit it only on success, so injection is all-or-nothing.
	// Several guards below reject mid-render (a foreign name squat, an unwritable
	// /dev/shm), and an in-place mutator that had already appended half the wire
	// would hand the caller a pod that is neither wired nor pristine. The pod webhook
	// happens to fail open with its own pre-injection copy today, but this function's
	// contract should not depend on that.
	work := pod.DeepCopy()
	c := &work.Containers[i]

	// Did WE wire this pod already? The webhook can be handed a pod template this
	// adapter has mutated before (re-admission, or an operator who copied a rendered
	// spec), and re-injection must converge on the current render rather than
	// duplicate or reject. Ownership is decided ONCE, up front, off the marker our
	// worker carries — a name alone cannot tell our container from an operator's, and
	// value-equality cannot either (a legitimate re-injection changes the endpoint or
	// L1 size). Everything below keys reuse-vs-reject on this.
	owned := sglangWireIsOurs(pod)

	// The config mount path is ADAPTER-OWNED (the worker writes the MP config file
	// there and the engine reads it). A FOREIGN mount already at that path can
	// neither be duplicated (a duplicate mountPath is an invalid Pod) nor safely
	// reused (an operator's ConfigMap/secret mount is read-only, so the worker's
	// write would fail at runtime) — reject with a message that names the fix; the
	// pod webhook turns that into a fail-open admit, so the pod starts un-wired
	// rather than broken. This differs from /dev/shm, which IS reused because it is
	// plain shared scratch tmpfs the operator legitimately owns.
	if existing := mountAtPath(c.VolumeMounts, sglangConfigMountPath); existing != nil && !sglangMountIsOurs(existing, sglangConfigVolumeName, owned) {
		return fmt.Errorf("inject engine config: engine container already mounts %q (volume %q), but that path is reserved for the LMCache MP config file the worker writes — move that mount elsewhere",
			sglangConfigMountPath, existing.Name)
	}

	if work.Volumes, err = adoptVolume(work.Volumes, corev1.Volume{
		Name:         sglangConfigVolumeName,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}, owned); err != nil {
		return err
	}

	// /dev/shm: GPU engine manifests commonly mount their own tmpfs there already.
	// Appending a SECOND mount at the same mountPath makes the Pod INVALID (the API
	// server rejects duplicate mountPaths), so reuse the engine's existing volume for
	// the worker instead — the MP data path only needs both containers on the SAME
	// volume, not on one we created. We add (and size) our own tmpfs only when the
	// engine has none (or when the one there is ours, which we re-render so a changed
	// l1SizeGB resizes the tmpfs too). Caveat: a reused volume is the operator's, so
	// its size is theirs to get right — too small silently degrades L1 to slow pickle
	// serde.
	shmMount := corev1.VolumeMount{Name: sglangShmVolumeName, MountPath: sglangShmMountPath}
	if existing := mountAtPath(c.VolumeMounts, sglangShmMountPath); existing != nil && !sglangMountIsOurs(existing, sglangShmVolumeName, owned) {
		// Not every mount can be reused — a read-only or projection-backed one breaks
		// the MP data path at runtime, deep inside LMCache. Reject at admission and
		// let the webhook fail open.
		if err := sglangCheckShmReusable(work.Volumes, *existing); err != nil {
			return err
		}
		// Mirror the engine's subPath. Both containers must land on the SAME
		// directory, and "same volume" is not enough: an engine mounting subPath
		// "shm" while the worker mounts the volume ROOT gives the two processes
		// different directories, which admits cleanly and then silently transfers no
		// KV — the worst failure shape available.
		shmMount = corev1.VolumeMount{Name: existing.Name, MountPath: sglangShmMountPath, SubPath: existing.SubPath}
	} else {
		if work.Volumes, err = adoptVolume(work.Volumes, sglangShmVolume(l1SizeGB), owned); err != nil {
			return err
		}
		c.VolumeMounts = upsertMountByName(c.VolumeMounts, shmMount)
	}

	worker := sglangMPWorkerContainer(c.Image, cfg, chunkSize, mpPort, l1SizeGB, l2Adapter, shmMount)
	if work.InitContainers, err = adoptContainer(work.InitContainers, worker, owned); err != nil {
		return err
	}

	c.Args = UpsertFlag(c.Args, SGLangEnableLMCacheArg)
	c.Args = UpsertArgPair(c.Args, SGLangConfigFileArg, sglangConfigMountPath+"/"+sglangConfigFileName)
	c.Env = UpsertEnv(c.Env, corev1.EnvVar{Name: EnvLMCacheUseExperimental, Value: lmcacheUseExperimentalVal})
	c.Env = UpsertEnv(c.Env, corev1.EnvVar{Name: EnvInferenceCacheFailOpen, Value: FailOpenString(cache)})
	c.VolumeMounts = upsertMountByName(c.VolumeMounts, corev1.VolumeMount{Name: sglangConfigVolumeName, MountPath: sglangConfigMountPath})

	*pod = *work // commit: every guard passed
	return nil
}

// sglangMountIsOurs reports whether an existing mount is one THIS adapter placed —
// i.e. the pod is one we already wired (owned, per [sglangWireIsOurs]) AND the mount
// names the volume we render for that path. Our own mount is a re-injection to
// converge, not a collision to reject.
func sglangMountIsOurs(m *corev1.VolumeMount, volumeName string, owned bool) bool {
	return owned && m.Name == volumeName
}

// mountAtPath returns the existing mount at mountPath, or nil. Two mounts sharing a
// mountPath make the Pod invalid, so callers reuse the existing volume rather than
// appending their own.
func mountAtPath(ms []corev1.VolumeMount, path string) *corev1.VolumeMount {
	for i := range ms {
		if ms[i].MountPath == path {
			return &ms[i]
		}
	}
	return nil
}

// sglangMPWorkerContainer builds the node-local MP-worker native sidecar. It
// writes the engine's config file (its own mp_host/mp_port) then execs the MP
// server — both in this container so, gated by the startupProbe, the config exists
// and the server listens before the engine starts. The worker image defaults to
// the engine image (guaranteeing the same lmcache version — the two speak the MP
// wire) and is overridable via backendConfig.workerImage.
func sglangMPWorkerContainer(engineImage string, cfg map[string]string, chunkSize, mpPort, l1SizeGB, l2Adapter string, shmMount corev1.VolumeMount) corev1.Container {
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

	// The worker holds the L1 in a memory-backed tmpfs charged to its own cgroup, so
	// it MUST carry a matching memory request+limit — otherwise the L1 is invisible
	// to the scheduler and can overcommit the node into a node-pressure OOM.
	var resources corev1.ResourceRequirements
	if q, ok := sglangMemBudget(l1SizeGB); ok {
		resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceMemory: q},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: q},
		}
	}

	always := corev1.ContainerRestartPolicyAlways
	return corev1.Container{
		Name:          sglangMPWorkerContainerName,
		Image:         image,
		RestartPolicy: &always, // native sidecar: starts + gates ready before the engine
		Command:       []string{"sh", "-c"},
		Args:          []string{script},
		Resources:     resources,
		Env: []corev1.EnvVar{
			// The GPU-less sidecar must SEE the engine's GPU to CUDA-IPC its KV; it
			// consumes no device-plugin allocation (no nvidia.com/gpu limit).
			{Name: "NVIDIA_VISIBLE_DEVICES", Value: "all"},
			// Marks this container as ours so a re-injection converges it instead of
			// mistaking it for an operator's name squat (see sglangWireIsOurs).
			{Name: envSGLangMPWorkerManaged, Value: sglangMPWorkerManagedVal},
		},
		// shmMount is the engine's existing /dev/shm mount when it has one — volume
		// AND subPath mirrored, so the two containers share the same directory
		// without a duplicate mountPath — else our own sized tmpfs.
		VolumeMounts: []corev1.VolumeMount{
			{Name: sglangConfigVolumeName, MountPath: sglangConfigMountPath},
			shmMount,
		},
		// The MP server binds mp_port on loopback, which a pod-IP tcp/http probe
		// cannot reach — so exec a loopback check inside the container. This gates
		// the engine's start on the ZMQ server being up.
		//
		// Gating the engine on the worker is DELIBERATE, and is the accepted
		// fail-open boundary for this pair (see "Fail-open semantics" in
		// docs/design/sglang-lmcache-mp-mode.md). The MP worker is a REQUIRED,
		// co-scheduled component of the serving stack — the out-of-process analog of
		// vLLM's in-process LMCache connector — not a remote dependency: SGLang has
		// no cacheless fallback while --enable-lmcache is on, so letting the engine
		// start before the worker listens makes it hang/abort, which is strictly
		// worse than waiting. The failOpen contract is honored at the tier that can
		// actually be "unavailable" — the SHARED L2: the worker comes up L1-only when
		// Redis is unreachable (GPU-validated), so an L2 outage degrades rather than
		// blocks. A worker that cannot start at all is a pod-health / CacheBackend
		// Degraded condition, exactly as a broken engine connector would be.
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
		// No added capabilities — deliberately. An earlier revision added IPC_LOCK
		// (carried over from the RDMA-oriented reference manifests), but Pod Security
		// permits added capabilities beyond a small allow-list under NEITHER baseline
		// nor restricted, and IPC_LOCK is on neither list. Since this mutation lands
		// BEFORE Pod Security admission, injecting it would turn an otherwise-valid
		// engine pod into a REJECTED one in any enforcing namespace — the cache plane
		// breaking the engine, which inverts the fail-open contract. The MP wire does
		// not need it: the worker moves KV over CUDA-IPC and /dev/shm, not RDMA, and
		// the engine side (which this adapter never grants capabilities to) would have
		// needed it too if the data path did.
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
	// The port is emitted UNQUOTED (the resp adapter expects an integer), so a
	// non-numeric or out-of-range one would render invalid JSON — and the worker
	// would then fail to parse its --l2-adapter and never bind the ZMQ port, leaving
	// the engine wedged behind the startup probe forever. That is worse than not
	// wiring at all, so reject here: the webhook fails open and the pod starts
	// un-wired. status.endpoint is controller-built and always numeric today; this
	// is the boundary check that keeps it that way.
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > sglangMaxTCPPort {
		return "", fmt.Errorf("inject engine config: endpoint %q has port %q, want an integer in 1-%d — the resp L2 adapter takes an integer port", endpoint, port, sglangMaxTCPPort)
	}
	return fmt.Sprintf(`{"type":"resp","host":%q,"port":%s}`, host, port), nil
}

// sglangMemBudget returns the memory budget for the L1 tier: l1SizeGB + 1Gi
// headroom. It sizes BOTH the /dev/shm tmpfs AND the worker container's memory
// request/limit: the L1 lives in a memory-backed emptyDir, which is charged to the
// cgroup of the container that writes it, so a worker with no request/limit would
// not inform scheduling and could overcommit the node into a node-pressure OOM —
// the same bounded-memory posture the Redis L2 render takes. l1SizeGB is a
// sanitized in-range integer (see sglangIntInRangeOr), so this always parses.
func sglangMemBudget(l1SizeGB string) (resource.Quantity, bool) {
	q, err := resource.ParseQuantity(l1SizeGB + "Gi")
	if err != nil {
		return resource.Quantity{}, false
	}
	q.Add(resource.MustParse("1Gi"))
	return q, true
}

// sglangShmVolume returns the /dev/shm tmpfs volume sized from the L1 budget. A
// memory-backed emptyDir must never be left unbounded or it can exhaust node
// memory.
func sglangShmVolume(l1SizeGB string) corev1.Volume {
	v := corev1.Volume{
		Name: sglangShmVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory},
		},
	}
	if q, ok := sglangMemBudget(l1SizeGB); ok {
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

// sglangWireIsOurs reports whether THIS adapter already wired pod — i.e. the MP
// worker sidecar it carries is one we rendered, identified by the marker env var
// our render always stamps ([envSGLangMPWorkerManaged]).
//
// This is the ownership test the reserved-name guards key on, and it must be
// evaluated BEFORE any mutation. Two weaker tests were considered and rejected:
// the container NAME alone cannot distinguish our sidecar from an operator's name
// squat (that is the very thing being decided), and value-EQUALITY against a fresh
// render mislabels a legitimate re-injection as foreign the moment any input
// changes — a moved status.endpoint or a retuned l1SizeGB renders a different, but
// still ours, container. A pod annotation would be the idiomatic marker, but
// InjectEngineConfig only receives the PodSpec.
//
// The marker is forgeable, and that is an accepted boundary rather than a hole: an
// operator who both names their container lmcache-mp-worker AND stamps it with this
// exact marker has, as far as any PodSpec-scoped check can tell, declared it
// adapter-owned — and gets it converged on our render. Nothing in a PodSpec proves
// provenance. What the marker does buy is the case that actually happens: an
// ACCIDENTAL name collision carries no marker, so it is rejected, not overwritten.
func sglangWireIsOurs(pod *corev1.PodSpec) bool {
	for i := range pod.InitContainers {
		if pod.InitContainers[i].Name != sglangMPWorkerContainerName {
			continue
		}
		for _, e := range pod.InitContainers[i].Env {
			// Value included: a same-named env carrying anything else is not the
			// marker our render stamps.
			if e.Name == envSGLangMPWorkerManaged && e.Value == sglangMPWorkerManagedVal {
				return true
			}
		}
	}
	return false
}

// adoptContainer appends want; when an entry with the same name already exists it
// either converges it to want (owned — our own prior injection, so the current
// render wins) or rejects it (not owned).
//
// A container carrying our RESERVED name that we did NOT render is a FOREIGN
// collision: mutating admission must never silently erase an operator's container,
// so reject and let the pod webhook fail open — the pod admits un-wired rather than
// corrupted. Silently leaving the foreign container in place is NOT an option here:
// the engine is given --lmcache-config-file regardless, so it would block at
// startup on a config file that nothing writes. owned comes from
// [sglangWireIsOurs].
func adoptContainer(cs []corev1.Container, want corev1.Container, owned bool) ([]corev1.Container, error) {
	for i := range cs {
		if cs[i].Name != want.Name {
			continue
		}
		if !owned {
			return nil, fmt.Errorf("inject engine config: pod already has a container named %q that this adapter did not render; that name is reserved for the LMCache MP worker — rename your container", want.Name)
		}
		cs[i] = want // our own prior injection — converge on the current render
		return cs, nil
	}
	return append(cs, want), nil
}

// adoptVolume is the volume analog of [adoptContainer]: append, converge our own
// prior injection, or reject a foreign volume squatting one of our reserved names
// (replacing it could corrupt unrelated mounts or invalidate the pod).
func adoptVolume(vs []corev1.Volume, want corev1.Volume, owned bool) ([]corev1.Volume, error) {
	for i := range vs {
		if vs[i].Name != want.Name {
			continue
		}
		if !owned {
			return nil, fmt.Errorf("inject engine config: pod already has a volume named %q that this adapter did not render; that name is reserved for the LMCache MP wire — rename your volume", want.Name)
		}
		vs[i] = want // our own prior injection — converge on the current render
		return vs, nil
	}
	return append(vs, want), nil
}

// sglangCheckShmReusable rejects an engine-owned /dev/shm mount the worker cannot
// safely share. The engine and the worker exchange KV through this volume, so it
// must be WRITABLE and both containers must resolve it to the SAME directory —
// neither of which the kubelet reports back at admission; getting it wrong surfaces
// as a silent no-transfer at runtime, deep inside LMCache.
//
// Read-only comes in two shapes, and both are checked: the MOUNT's readOnly, and the
// SOURCE's — projection sources (configMap / secret / downwardAPI / projected) the
// kubelet always mounts read-only, plus the explicit readOnly on persistentVolumeClaim
// and csi. Sources not named here (emptyDir, hostPath, ephemeral, …) are writable, or
// their writability is the operator's to configure, so they pass.
func sglangCheckShmReusable(vs []corev1.Volume, m corev1.VolumeMount) error {
	if m.ReadOnly {
		return fmt.Errorf("inject engine config: engine container mounts %q read-only (volume %q), but the LMCache MP data path writes there — drop readOnly or mount it elsewhere", sglangShmMountPath, m.Name)
	}
	// subPath is mirrorable (the caller copies it onto the worker's mount);
	// subPathExpr is NOT: it expands $(VAR) from the mounting CONTAINER's env, and the
	// worker's env is not the engine's, so the same expression can resolve to a
	// different directory — or fail to expand at all. Silently landing the two
	// containers on different directories is exactly the failure this guard exists to
	// prevent, so reject rather than guess.
	if m.SubPathExpr != "" {
		return fmt.Errorf("inject engine config: engine container mounts %q with subPathExpr %q (volume %q); the LMCache MP worker cannot reproduce that expansion in its own env — use a literal subPath, or mount %[1]q without it", sglangShmMountPath, m.SubPathExpr, m.Name)
	}
	for i := range vs {
		if vs[i].Name != m.Name {
			continue
		}
		var kind, why string
		switch src := vs[i].VolumeSource; {
		case src.ConfigMap != nil:
			kind, why = "configMap", "which the kubelet mounts read-only"
		case src.Secret != nil:
			kind, why = "secret", "which the kubelet mounts read-only"
		case src.DownwardAPI != nil:
			kind, why = "downwardAPI", "which the kubelet mounts read-only"
		case src.Projected != nil:
			kind, why = "projected", "which the kubelet mounts read-only"
		case src.PersistentVolumeClaim != nil && src.PersistentVolumeClaim.ReadOnly:
			kind, why = "persistentVolumeClaim", "declared readOnly"
		case src.CSI != nil && src.CSI.ReadOnly != nil && *src.CSI.ReadOnly:
			kind, why = "csi", "declared readOnly"
		default:
			return nil
		}
		return fmt.Errorf("inject engine config: engine container mounts %q from a %s volume (%q) %s, but the LMCache MP data path writes there — use an emptyDir (medium: Memory) instead", sglangShmMountPath, kind, m.Name, why)
	}
	return nil
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
