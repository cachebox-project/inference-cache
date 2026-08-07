// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

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

// Options configures the runtime adapters shipped by the controller binary.
// It belongs to the built-in composition rather than the public adapter seam.
type Options struct {
	SubscriberImage         string
	PolicyServerGRPCAddress string
}

// New constructs the complete built-in registries. Subscriber settings are applied
// consistently to every adapter that injects the subscriber sidecar.
func New(opts Options) Registries {
	subscriber := builtinruntime.SubscriberConfig{
		Image:                   opts.SubscriberImage,
		PolicyServerGRPCAddress: opts.PolicyServerGRPCAddress,
	}
	runtimeRegistry := adapterruntime.NewRegistry()
	runtimeRegistry.Register(builtinruntime.NewVLLMLMCacheAdapter(subscriber))
	runtimeRegistry.Register(builtinruntime.NewSGLangLMCacheAdapter(subscriber))
	runtimeRegistry.Register(builtinruntime.NewSGLangHiCacheAdapter(subscriber))

	return Registries{
		Runtime: runtimeRegistry,
		Storage: builtinstorage.DefaultRegistry(),
	}
}
