package server

import (
	"context"
	"errors"
	"io"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// inferenceCacheService is the Phase 1 fail-open implementation of the
// InferenceCache contract (see docs/design/grpc-contract.md). Every RPC returns
// a safe no-op default; the real logic (index lookups, template rendering,
// metrics/events) lands in later modules. Clients treat empty results /
// NO_HINT as a no-op and proceed as they normally would.
type inferenceCacheService struct {
	icpb.UnimplementedInferenceCacheServer
}

func newInferenceCacheService() *inferenceCacheService {
	return &inferenceCacheService{}
}

// RenderTemplate: no rendering yet (M7). An empty stable_prefix_hash signals the
// caller to fall back to hashing the raw prompt itself.
func (*inferenceCacheService) RenderTemplate(context.Context, *icpb.RenderTemplateRequest) (*icpb.RenderTemplateResponse, error) {
	return &icpb.RenderTemplateResponse{ReasonCode: "OK"}, nil
}

// LookupRoute: fail open with NO_HINT — the gateway routes as it normally would.
func (*inferenceCacheService) LookupRoute(context.Context, *icpb.LookupRouteRequest) (*icpb.LookupRouteResponse, error) {
	return &icpb.LookupRouteResponse{ReasonCode: "NO_HINT"}, nil
}

func (*inferenceCacheService) LookupPDRoute(context.Context, *icpb.LookupPDRouteRequest) (*icpb.LookupPDRouteResponse, error) {
	return &icpb.LookupPDRouteResponse{ReasonCode: "NO_HINT"}, nil
}

func (*inferenceCacheService) GetCacheState(context.Context, *icpb.GetCacheStateRequest) (*icpb.GetCacheStateResponse, error) {
	return &icpb.GetCacheStateResponse{Summary: &icpb.CacheSummary{}}, nil
}

// ReportCacheState: drain the stream and ack; no index yet (M3/B6).
func (*inferenceCacheService) ReportCacheState(stream icpb.InferenceCache_ReportCacheStateServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return stream.SendAndClose(&icpb.Ack{Accepted: true})
			}
			return err
		}
	}
}

func (*inferenceCacheService) PublishEvent(context.Context, *icpb.CacheEvent) (*icpb.Ack, error) {
	return &icpb.Ack{Accepted: true}, nil
}

// StreamCacheEvents / StreamMetrics: nothing to stream yet (M10) — close cleanly.
func (*inferenceCacheService) StreamCacheEvents(*icpb.StreamEventsRequest, icpb.InferenceCache_StreamCacheEventsServer) error {
	return nil
}

func (*inferenceCacheService) StreamMetrics(*icpb.StreamMetricsRequest, icpb.InferenceCache_StreamMetricsServer) error {
	return nil
}
