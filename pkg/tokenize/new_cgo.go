//go:build smgcgo

package tokenize

// New returns the cgo SMG-backed tokenizer (build tag smgcgo), which links the
// rust/ictokenizer static archive over llm-tokenizer. Server-side tokenization is
// fail-closed: it requires an explicit vetted models directory (cfg.ModelsDir).
// With no directory there is nothing to load and nothing safe to resolve a
// request model_id against, so New returns Unavailable and the prompt_text lookup
// path fails open to NO_HINT rather than loading request-controlled artifacts.
func New(cfg Config) Tokenizer {
	if cfg.ModelsDir == "" {
		return Unavailable{}
	}
	return newSMGTokenizer(cfg.ModelsDir)
}
