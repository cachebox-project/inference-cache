package engineclient

import "context"

// GRPCTokenizedClient is a placeholder for sending pre-tokenized input over
// vLLM's gRPC frontend (the TokenizedInput{input_ids} message SMG uses). The
// reference stack runs the OpenAI HTTP server, not the gRPC frontend, so this
// is stubbed behind the EngineClient interface until an engine that exposes the
// gRPC frontend is in scope. Implement here without touching callers.
type GRPCTokenizedClient struct{}

// Complete reports ErrNotImplemented.
func (GRPCTokenizedClient) Complete(context.Context, string, string, []uint32, CompletionParams) (Completion, error) {
	return Completion{}, ErrNotImplemented
}
