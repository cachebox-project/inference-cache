// Package vllmengine holds a minimal vendored subset of the SMG vLLM engine gRPC
// contract (GetLoads only), used by the kvevent-subscriber to read engine load
// over gRPC instead of scraping HTTP /metrics. The .pb.go files are generated,
// not hand-edited.
//
// Regenerate with `make proto-gen-vendored` (also run as part of `make
// proto-gen`); CI diffs the output to catch drift. See vllm_engine.proto for the
// contract and why it is kept outside the proto/ module.
package vllmengine

//go:generate make -C ../../../.. proto-gen-vendored
