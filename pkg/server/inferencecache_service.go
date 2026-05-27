package server

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/cachebox-project/inference-cache/pkg/index"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// Reason codes returned on the lookup path (tech spec §4.2 / grpc-contract.md).
const (
	reasonPrefixMatch = "PREFIX_MATCH"
	reasonNoHint      = "NO_HINT"
	reasonOK          = "OK"
)

// inferenceCacheService implements the InferenceCache contract
// (docs/design/grpc-contract.md). LookupRoute / ReportCacheState / PublishEvent
// / GetCacheState are backed by the in-memory CacheIndex (B6); the remaining
// RPCs (RenderTemplate, LookupPDRoute, streams) stay fail-open stubs until their
// modules land. All lookups remain side-effect-free apart from emitting metrics
// and fail open (empty result + NO_HINT) so the gateway routes as it normally would.
type inferenceCacheService struct {
	icpb.UnimplementedInferenceCacheServer

	index   *index.Index
	metrics *serverMetrics
}

func newInferenceCacheService(idx *index.Index, metrics *serverMetrics) *inferenceCacheService {
	return &inferenceCacheService{index: idx, metrics: metrics}
}

// RenderTemplate: no rendering yet (M7). An empty stable_prefix_hash signals the
// caller to fall back to hashing the raw prompt itself.
func (*inferenceCacheService) RenderTemplate(context.Context, *icpb.RenderTemplateRequest) (*icpb.RenderTemplateResponse, error) {
	return &icpb.RenderTemplateResponse{ReasonCode: reasonOK}, nil
}

// LookupRoute consults the index for replicas holding the request's prefix and
// returns them ranked. No match → empty scores + NO_HINT (fail open).
func (s *inferenceCacheService) LookupRoute(_ context.Context, req *icpb.LookupRouteRequest) (*icpb.LookupRouteResponse, error) {
	start := time.Now()

	scores := s.index.Lookup(index.LookupRequest{
		Model:      req.GetModelId(),
		Tenant:     req.GetTenantId(),
		HashScheme: req.GetHashScheme(),
		PrefixHash: req.GetPrefixHash(),
		TokenCount: req.GetPrefixTokenCount(),
	})

	resp := &icpb.LookupRouteResponse{ReasonCode: reasonNoHint}
	if len(scores) > 0 {
		resp.ReasonCode = reasonPrefixMatch
		resp.ReplicaScores = make([]*icpb.ReplicaScore, 0, len(scores))
		for _, sc := range scores {
			resp.ReplicaScores = append(resp.ReplicaScores, &icpb.ReplicaScore{
				ReplicaId:             sc.ReplicaID,
				Score:                 sc.Score,
				MatchedTokens:         sc.MatchedTokens,
				EstimatedCacheHitProb: sc.EstimatedCacheHitProb,
			})
		}
	}

	elapsed := time.Since(start)
	resp.LookupLatencyUs = elapsed.Microseconds()
	s.metrics.observeLookup(req.GetModelId(), resp.ReasonCode, len(scores) > 0, elapsed)
	return resp, nil
}

// LookupPDRoute: prefill/decode routing is Phase 2 — fail open.
func (*inferenceCacheService) LookupPDRoute(context.Context, *icpb.LookupPDRouteRequest) (*icpb.LookupPDRouteResponse, error) {
	return &icpb.LookupPDRouteResponse{ReasonCode: reasonNoHint}, nil
}

// GetCacheState returns the aggregate held in the index for a (tenant, model).
func (s *inferenceCacheService) GetCacheState(_ context.Context, req *icpb.GetCacheStateRequest) (*icpb.GetCacheStateResponse, error) {
	replicas, totalPrefixes := s.index.CacheState(req.GetTenantId(), req.GetModelId())

	resp := &icpb.GetCacheStateResponse{
		Summary: &icpb.CacheSummary{TotalPrefixes: int64(totalPrefixes)},
	}
	for _, r := range replicas {
		resp.Replicas = append(resp.Replicas, &icpb.ReplicaStats{
			ReplicaId:        r.ReplicaID,
			CacheMemoryBytes: r.CacheMemoryBytes,
			HitRate:          r.HitRate,
			Pressure:         r.Pressure,
		})
	}
	return resp, nil
}

// ReportCacheState ingests replica update deltas (adds/refreshes; removals
// arrive via PublishEvent or expire by TTL) into the index until the client
// half-closes, then acks. A non-EOF Recv error is propagated.
func (s *inferenceCacheService) ReportCacheState(stream icpb.InferenceCache_ReportCacheStateServer) error {
	for {
		update, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return stream.SendAndClose(&icpb.Ack{Accepted: true})
			}
			return err
		}
		s.index.Ingest(updateFromProto(update))
	}
}

// PublishEvent applies a single cache-state delta to the index.
func (s *inferenceCacheService) PublishEvent(_ context.Context, ev *icpb.CacheEvent) (*icpb.Ack, error) {
	if t := eventTypeFromProto(ev.GetType()); t != 0 {
		s.index.ApplyEvent(index.Event{
			Type:       t,
			ReplicaID:  ev.GetReplicaId(),
			Model:      ev.GetModelId(),
			Tenant:     ev.GetTenantId(),
			PrefixHash: ev.GetPrefixHash(),
			Timestamp:  microsToTime(ev.GetTimestampUs()),
		})
	}
	return &icpb.Ack{Accepted: true}, nil
}

// StreamCacheEvents / StreamMetrics: outbound streaming is M10 — close cleanly.
func (*inferenceCacheService) StreamCacheEvents(*icpb.StreamEventsRequest, icpb.InferenceCache_StreamCacheEventsServer) error {
	return nil
}

func (*inferenceCacheService) StreamMetrics(*icpb.StreamMetricsRequest, icpb.InferenceCache_StreamMetricsServer) error {
	return nil
}

// updateFromProto translates a CacheStateUpdate into the index domain type.
func updateFromProto(u *icpb.CacheStateUpdate) index.Update {
	out := index.Update{
		ReplicaID:  u.GetReplicaId(),
		Model:      u.GetModelId(),
		Tenant:     u.GetTenantId(),
		HashScheme: u.GetHashScheme(),
		Timestamp:  microsToTime(u.GetTimestampUs()),
	}
	for _, p := range u.GetPrefixes() {
		out.Prefixes = append(out.Prefixes, index.PrefixRef{
			PrefixHash: p.GetPrefixHash(),
			TokenCount: p.GetTokenCount(),
		})
	}
	if st := u.GetStats(); st != nil {
		out.Stats = &index.ReplicaStats{
			ReplicaID:        st.GetReplicaId(),
			CacheMemoryBytes: st.GetCacheMemoryBytes(),
			HitRate:          st.GetHitRate(),
			Pressure:         st.GetPressure(),
		}
	}
	return out
}

// eventTypeFromProto maps the proto enum to the index event type; returns 0 for
// unspecified/unknown (caller skips).
func eventTypeFromProto(t icpb.CacheEvent_Type) index.EventType {
	switch t {
	case icpb.CacheEvent_PREFIX_ADDED:
		return index.EventPrefixAdded
	case icpb.CacheEvent_PREFIX_EVICTED:
		return index.EventPrefixEvicted
	case icpb.CacheEvent_REPLICA_UPDATED:
		return index.EventReplicaUpdated
	case icpb.CacheEvent_ALL_CLEARED:
		return index.EventAllCleared
	default:
		return 0
	}
}

// microsToTime converts a microsecond Unix timestamp to time.Time; 0 → zero
// time (the index treats that as "now").
func microsToTime(us int64) time.Time {
	if us == 0 {
		return time.Time{}
	}
	return time.UnixMicro(us)
}
