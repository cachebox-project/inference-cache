//go:build !smgcgo

package tokenize

// New returns Unavailable in the default (non-cgo) build: the binary links no
// tokenizer, so the (model, prompt_text) LookupRoute path fails open to NO_HINT.
// Build with `-tags smgcgo` (and link rust/ictokenizer) for the real tokenizer.
func New(Config) Tokenizer { return Unavailable{} }

// Enabled reports whether this build can perform server-side tokenization. False
// here (non-cgo build); callers use it to warn when a tokenizer config (e.g.
// --tokenizer-models-dir) is set on a build that can't act on it.
func Enabled() bool { return false }
