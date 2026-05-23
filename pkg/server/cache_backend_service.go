package server

import (
	"context"

	cachev1alpha1pb "github.com/cachebox-project/inference-cache/pkg/server/proto/cache/v1alpha1"
)

// CacheBackendService is the initial no-op gRPC implementation.
type CacheBackendService struct {
	cachev1alpha1pb.UnimplementedCacheBackendServiceServer
}

// Ping reports that the server process is alive.
func (CacheBackendService) Ping(context.Context, *cachev1alpha1pb.PingRequest) (*cachev1alpha1pb.PingResponse, error) {
	return &cachev1alpha1pb.PingResponse{Status: "ok"}, nil
}
