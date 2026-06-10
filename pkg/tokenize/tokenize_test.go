package tokenize

import (
	"context"
	"errors"
	"testing"
)

// The default build ships no cgo tokenizer, so the server holds an Unavailable
// tokenizer. Both methods must return ErrUnavailable (errors.Is-matchable) so
// the LookupRoute handler can fail open to NO_HINT on the (model, text) path
// without special-casing the error value.
func TestUnavailableFailsOpen(t *testing.T) {
	var tk Tokenizer = Unavailable{}

	if _, err := tk.Encode(context.Background(), "m", []Message{{Role: "user", Content: "hi"}}, EncodeOptions{}); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Encode err = %v, want ErrUnavailable", err)
	}
	if _, err := tk.EncodeText(context.Background(), "m", "hi", EncodeOptions{}); !errors.Is(err, ErrUnavailable) {
		t.Errorf("EncodeText err = %v, want ErrUnavailable", err)
	}
}
