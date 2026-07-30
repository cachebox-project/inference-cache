# inference-cache documentation site

The source for the inference-cache documentation website, built with
[Hugo](https://gohugo.io/) and the [Docsy](https://www.docsy.dev/) theme (imported as a Hugo
module).

## Prerequisites

- **Hugo Extended** (the SCSS pipeline needs the extended build):
  ```bash
  brew install hugo        # macOS
  # or see https://gohugo.io/installation/
  ```
- Go (Hugo fetches the Docsy theme as a Go module on first build).

No Node/npm toolchain is required to build — the CSS pipeline uses Hugo Extended's built-in
SCSS support.

## Preview locally

```bash
hugo server
```

Then open <http://localhost:1313/>. Pages live-reload as you edit.

## Production build

```bash
hugo --gc --minify
```

The static site is written to `public/`.

## Structure

```
site/
  hugo.toml                     # site config (title, menus, params, Docsy module import)
  go.mod / go.sum               # Hugo module deps (Docsy)
  content/en/docs/              # all documentation pages
    overview/                   # why inference-cache
    installation/               # install guide
    concepts/                   # architecture, the CRDs, LookupRoute, the gRPC contract
    tasks/                      # step-by-step guides
    administration/             # observability, sizing, TLS, troubleshooting
    reference/                  # CRD API, gRPC API, metrics, reason codes, CLI
    developer-guide/            # contributing
  layouts/                      # Docsy theme overrides (partials, shortcodes)
  assets/scss/                  # theme color/style overrides
```

Each page carries Hugo front matter (`title`, `weight`, `description`). Sidebar order is
controlled by the `weight` field; sections are defined by each directory's `_index.md`.
