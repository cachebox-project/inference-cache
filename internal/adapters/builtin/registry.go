package builtin

import (
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	backendprovider "github.com/cachebox-project/inference-cache/pkg/adapters/backend/provider"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	externaladapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime/external"
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
	runtimeRegistry.Register(externaladapter.NewAdapter())
	runtimeRegistry.Register(sglangadapter.NewAdapter(opts...))
	runtimeRegistry.Register(sglangadapter.NewHiCacheAdapter(opts...))

	return Registries{
		Runtime: runtimeRegistry,
		Storage: backendprovider.DefaultRegistry(),
	}
}
