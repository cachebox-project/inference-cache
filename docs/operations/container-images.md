# Container Image Hardening

The repository ships three Go binaries as container images: the controller,
policy server, and KV-event subscriber. They share one multi-stage Dockerfile
at `dockerfiles/Dockerfile`.

## Runtime Base

Every shipped target uses `gcr.io/distroless/static-debian13:nonroot`, pinned to
the multi-architecture index digest
`sha256:f7f8f729987ad0fdf6b05eeeae94b26e6a0f613bdf46feea7fc40f7bd72953e6`.
The explicit Debian version and immutable digest prevent upstream aliases or
tags from silently changing the runtime. The `static` variant is sufficient
because all three binaries are built with `CGO_ENABLED=0`; it contains neither
a shell nor a package manager. Each target also declares `USER 65532:65532`
and a vector-form entrypoint.

The `golang` image appears only in the builder stage. It is not present in any
shipped runtime image. BusyBox images created by reference-stack smoke scripts
are short-lived test fixtures loaded into kind; they are not release targets.

## Verification

The source-only policy check is available without Docker:

```bash
make verify-minimal-base
make test-minimal-images
```

After building the images, inspect their effective metadata and filesystems:

```bash
make image-build
make verify-minimal-images
```

`verify-minimal-images` checks all three targets for:

- the approved Distroless runtime stage;
- the numeric non-root user and group `65532:65532`;
- the expected component entrypoint and binary;
- the absence of common shells and package managers.

GitHub CI runs the source policy in the lint job and the complete inspection in
the Image Build job. The verifier is deliberately separate from vulnerability
scanning and SBOM generation: those gates cover package risk and inventory,
while this gate enforces the runtime-image shape.

See the upstream [Distroless project](https://github.com/GoogleContainerTools/distroless)
for image contents, supported Debian variants, and signature-verification
guidance.
