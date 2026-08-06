package builtin

import (
	builtinstorage "github.com/cachebox-project/inference-cache/internal/adapters/builtin/storage"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	sglangadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime/sglang"
)

// Registries contains the complete runtime and remote-storage provider sets
// shipped by the controller binary.
type Registries struct {
	Runtime *adapterruntime.Registry
	Storage *backendadapter.Registry
}

// New constructs the complete built-in registries. Runtime options are applied
// consistently to every adapter that injects the subscriber sidecar.
func New(opts ...adapterruntime.Option) Registries {
	runtimeRegistry := adapterruntime.NewCoreRegistry(opts...)
	runtimeRegistry.Register(sglangadapter.NewAdapter(opts...))
	runtimeRegistry.Register(sglangadapter.NewHiCacheAdapter(opts...))

	return Registries{
		Runtime: runtimeRegistry,
		Storage: builtinstorage.DefaultRegistry(),
	}
}
