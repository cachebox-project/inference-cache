# ictokenizer

A small C-ABI static library that lets the Go server tokenize a `(model, prompt)`
into engine-aligned token IDs for the dual-input `LookupRoute` path. It is a thin
shim over the upstream [`llm-tokenizer`](https://github.com/lightseekorg/smg) crate
(HuggingFace + tiktoken loaders, chat-template rendering, custom encoders) — the
tokenizer artifacts are loaded, not reimplemented.

It depends **only** on `llm-tokenizer` (pinned by git revision in `Cargo.toml`),
not on the upstream gateway, so the static archive carries no gateway or Python
dependency.

## How it is used

The server consumes this through cgo, behind the `smgcgo` Go build tag
(`pkg/tokenize/cgo_smg.go`). The **default** server build does **not** use it and
needs no Rust toolchain — the `(model, prompt_text)` lookup path simply fails open
to `NO_HINT` there, while the pre-tokenized `token_ids` path keeps working. This
crate is required only for the tokenizer-enabled build.

## Build

```sh
# from the repo root
make tokenize-cgo-build        # cargo build --release (first build fetches the git dep)
```

Produces `rust/ictokenizer/target/release/libictokenizer.a`. Build + test the
tokenizer-enabled Go code with:

```sh
make tokenize-cgo-test                                   # links the archive, runs -tags smgcgo tests
IC_TEST_TOKENIZER=Qwen/Qwen2.5-0.5B-Instruct make tokenize-cgo-test   # exercises a real tokenizer
```

To build a tokenizer-enabled server binary directly:

```sh
make tokenize-cgo-build
CGO_LDFLAGS="-L$(pwd)/rust/ictokenizer/target/release" \
  go build -tags smgcgo -o bin/server ./cmd/server
```

The archive is statically linked into the Go binary (no shared library to ship).
`tokenizer.json` artifacts are resolved per model — either via the server's
`--tokenizer-models-dir` (`<dir>/<model>/tokenizer.json`) or, if unset, by treating
the model id as a path or a HuggingFace id (downloaded; gated models need `HF_TOKEN`).

## C ABI

`ic_tokenizer_create` · `ic_tokenizer_encode_chat` · `ic_tokenizer_encode_text` ·
`ic_tokenizer_free` · `ic_free_ids` · `ic_free_string` (see `src/lib.rs`). Every
buffer the library returns is freed by the matching `ic_*_free` call.
