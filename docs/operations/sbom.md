# SBOM Generation

The release tooling generates SPDX JSON Software Bill of Materials artifacts
with [Syft](https://github.com/anchore/syft). CI installs the pinned Syft
version with checksum verification through `.github/actions/setup-syft`.
For local runs, install the version shown by `make syft-check` or set
`SYFT=/path/to/syft`.

## Source SBOM

```bash
make sbom-release TAG=v0.1.0
```

Output:

```text
dist/sbom/inference-cache-v0.1.0.spdx.json
```

`TAG` is sanitized for filenames by replacing `/` with `_`.
Override the output directory with `SBOM_DIR=/path/to/out`.

## Local Image SBOMs

```bash
make sbom-images TAG=v0.1.0
```

By default this builds the controller, server, and kvevent-subscriber images
locally before scanning them. To scan already-built local images, pass
`SBOM_IMAGE_BUILD=0`.

Outputs:

```text
dist/sbom/inference-cache-controller-v0.1.0.spdx.json
dist/sbom/inference-cache-server-v0.1.0.spdx.json
dist/sbom/inference-cache-subscriber-v0.1.0.spdx.json
```

## Registry Image SBOMs

```bash
make sbom-registry-images TAG=v0.1.0
```

This target resolves each release image tag to an immutable registry digest
before scanning:

```text
ghcr.io/cachebox-project/inference-cache-controller:v0.1.0
ghcr.io/cachebox-project/inference-cache-server:v0.1.0
ghcr.io/cachebox-project/inference-cache-subscriber:v0.1.0
```

For manifest-list images, the target discovers the platforms present in the
registry and scans the intersection with `SBOM_IMAGE_PLATFORMS` (default:
`linux/amd64,linux/arm64`). Platform-specific outputs include the platform in
the filename:

```text
dist/sbom/inference-cache-controller-linux_amd64-v0.1.0.spdx.json
dist/sbom/inference-cache-controller-linux_arm64-v0.1.0.spdx.json
```

If the registry image has no platform list, the target writes a single
component SBOM without a platform suffix.

## Publish-Missing Fallback

Release CI may run:

```bash
make sbom-registry-images \
  TAG=v0.1.0 \
  SBOM_REGISTRY_PUBLISH_MISSING=1 \
  SBOM_IMAGE_CONTEXT=/path/to/release-source \
  SBOM_DOCKERFILE=/path/to/release-source/dockerfiles/Dockerfile
```

With `SBOM_REGISTRY_PUBLISH_MISSING=1`, a tag that is missing from the registry
is built and pushed before scanning. Registry authentication and credential
helper errors still fail closed; only explicit missing-manifest responses enter
the publish fallback. The fallback publishes the configured
`SBOM_IMAGE_PLATFORMS` set and then generates one SBOM per published platform.

Use `make verify-syft-pin` when bumping Syft so the Makefile, workflows, and
setup action stay aligned.
