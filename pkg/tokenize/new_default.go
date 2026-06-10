//go:build !smgcgo

package tokenize

// New returns Unavailable in the default (non-cgo) build: the binary links no
// tokenizer, so the (model, prompt_text) LookupRoute path fails open to NO_HINT.
// Build with `-tags smgcgo` (and link rust/ictokenizer) for the real tokenizer.
func New(Config) Tokenizer { return Unavailable{} }
