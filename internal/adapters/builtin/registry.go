package builtin

import (
	builtinruntime "github.com/cachebox-project/inference-cache/internal/adapters/builtin/runtime"
	builtinstorage "github.com/cachebox-project/inference-cache/internal/adapters/builtin/storage"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
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
	runtimeRegistry := adapterruntime.NewRegistry()
	runtimeRegistry.Register(builtinruntime.NewVLLMLMCacheAdapter(opts...))
	runtimeRegistry.Register(builtinruntime.NewSGLangLMCacheAdapter(opts...))
	runtimeRegistry.Register(builtinruntime.NewSGLangHiCacheAdapter(opts...))

	return Registries{
		Runtime: runtimeRegistry,
		Storage: builtinstorage.DefaultRegistry(),
	}
}
