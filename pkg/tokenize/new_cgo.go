//go:build smgcgo

package tokenize

// New returns the cgo SMG-backed tokenizer (build tag smgcgo), which links the
// rust/ictokenizer static archive over llm-tokenizer.
func New(cfg Config) Tokenizer { return newSMGTokenizer(cfg.ModelsDir) }
