package testing

import (
	"context"
	"path/filepath"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// SetupEnvTest returns a controller-runtime envtest environment using the repo CRDs.
func SetupEnvTest() *envtest.Environment {
	return &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("config", "crd", "bases"),
			filepath.Join("..", "..", "config", "crd", "bases"),
			filepath.Join("..", "..", "..", "config", "crd", "bases"),
		},
		ErrorIfCRDPathMissing: false,
	}
}

// StartTestManager starts a manager in the background and returns a wait group plus error channel.
func StartTestManager(ctx context.Context, mgr manager.Manager) (*sync.WaitGroup, <-chan error) {
	wg := &sync.WaitGroup{}
	errCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- mgr.Start(ctx)
	}()
	return wg, errCh
}
