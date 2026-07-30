package runtime

// Shared kvevent-subscriber sidecar defaults. Vendor-neutral; production
// should set the image to a digest-pinned reference and the policy-server
// address to the in-cluster Service DNS the operator's server exposes.
const (
	// SubscriberContainerName is the well-known name for the
	// kvevent-subscriber sidecar. Webhook callers use it to short-circuit
	// re-admission, and operators can address the sidecar without guessing.
	SubscriberContainerName = "kvevent-subscriber"

	// DefaultSubscriberImage is the well-known dev tag the Makefile's
	// subscriber-image target emits. Auto-attach remains opt-in; a missing
	// image must not put an otherwise healthy engine pod in ImagePullBackOff.
	DefaultSubscriberImage = "ghcr.io/cachebox-project/inference-cache-subscriber:dev"

	// DefaultPolicyServerGRPCAddress is the in-cluster Service DNS the
	// kvevent-subscriber sidecar dials by default.
	DefaultPolicyServerGRPCAddress = "inference-cache-server.inference-cache-system.svc.cluster.local:9090"

	// modelBackendConfigKey is the deprecated BackendConfig compatibility key
	// used when observation.modelID is absent on a legacy resource.
	modelBackendConfigKey = "model"
)
