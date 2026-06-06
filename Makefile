PROJECT_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
LOCALBIN ?= $(PROJECT_DIR)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

GO_CMD ?= go
GO_FMT ?= gofmt
MODULE := github.com/cachebox-project/inference-cache
GO_VERSION := $(shell awk '/^go /{print $$2}' go.mod | head -n1)

REGISTRY ?= ghcr.io/cachebox-project
TAG ?= $(shell git describe --tags --dirty --always 2>/dev/null || echo dev)
IMG ?= $(REGISTRY)/inference-cache-controller:$(TAG)
SERVER_IMG ?= $(REGISTRY)/inference-cache-server:$(TAG)
SUBSCRIBER_IMG ?= $(REGISTRY)/inference-cache-subscriber:$(TAG)
DOCKER_BUILD_CMD ?= docker
KIND ?= $(shell command -v kind 2>/dev/null || echo $(LOCAL_KIND))
KIND_CLUSTER ?= inference-cache
KIND_NODE_IMAGE ?= kindest/node:v1.31.0

version_pkg = $(MODULE)/pkg/version
LD_FLAGS += -X '$(version_pkg).GitVersion=$(TAG)'
LD_FLAGS += -X '$(version_pkg).GitCommit=$(shell git rev-parse HEAD 2>/dev/null || echo unknown)'

CONTROLLER_TOOLS_VERSION ?= v0.16.5
GOLANGCI_LINT_VERSION ?= v2.12.2
PROTOC_GEN_GO_VERSION ?= v1.36.5
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1
SETUP_ENVTEST_VERSION ?= v0.0.0-20241105200929-48ec3b71211f
KIND_VERSION ?= v0.24.0
ENVTEST_K8S_VERSION ?= 1.31.0
BUF_VERSION ?= v1.69.0
GOVULNCHECK_VERSION ?= v1.3.0
PROMTOOL_VERSION ?= 3.0.1
# SHA256 checksums of the upstream Prometheus release tarballs we extract
# `promtool` from. Sourced from
# https://github.com/prometheus/prometheus/releases/download/v$(PROMTOOL_VERSION)/sha256sums.txt
# — verify against that file when bumping PROMTOOL_VERSION. CI relies on the
# linux-amd64 entry; the others let local dev (Mac arm64/amd64, linux arm64)
# get the same integrity check.
PROMTOOL_SHA256_linux_amd64  ?= 43f6f228ef59e0c2f6994e489c5c76c6671553eaa99ded0aea1cd31366222916
PROMTOOL_SHA256_linux_arm64  ?= 58e8d4f3ab633528fa784740409c529f4a434f8a0e3cf4d2f56e75ce2db69aa8
PROMTOOL_SHA256_darwin_amd64 ?= d45a9dab9ee9f40a27f2b7dde227843753d6f648ccf2d2c8477b9c7ffd75c0a0
PROMTOOL_SHA256_darwin_arm64 ?= 803d1ae747d39a4637ad33df254854f2a76663a6dd4ade0066b7f25617feba3d

CONTROLLER_GEN := $(LOCALBIN)/controller-gen
GOLANGCI_LINT := $(LOCALBIN)/golangci-lint
PROTOC_GEN_GO := $(LOCALBIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(LOCALBIN)/protoc-gen-go-grpc
SETUP_ENVTEST := $(LOCALBIN)/setup-envtest
LOCAL_KIND := $(LOCALBIN)/kind
LOCAL_BUF := $(LOCALBIN)/buf
LOCAL_PROMTOOL := $(LOCALBIN)/promtool
GOVULNCHECK := $(LOCALBIN)/govulncheck
BUF ?= $(shell command -v buf 2>/dev/null || echo $(LOCAL_BUF))
# PROMTOOL is resolved AT RECIPE TIME, not parse time — so the version
# check in the `promtool` target above can replace a stale local binary
# (or refuse a version-mismatched system one) and `verify-prometheus`
# will still pick up the freshly-installed `$(LOCAL_PROMTOOL)`. A
# `$(shell command -v promtool)` at parse time would lock in whatever
# was on PATH when make first started, defeating the version pinning.
RESOLVE_PROMTOOL = if [ -x $(LOCAL_PROMTOOL) ]; then echo $(LOCAL_PROMTOOL); else echo promtool; fi

.PHONY: all
all: build test ## Build binaries and run tests.

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Tools

.PHONY: controller-gen
controller-gen: $(LOCALBIN) ## Install controller-gen locally; reinstall when the pinned version drifts.
	@if ! { [ -x $(CONTROLLER_GEN) ] && $(CONTROLLER_GEN) --version 2>/dev/null | grep -qxF "Version: $(CONTROLLER_TOOLS_VERSION)"; }; then \
		rm -f $(CONTROLLER_GEN); \
		GOBIN=$(LOCALBIN) $(GO_CMD) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION); \
	fi

.PHONY: golangci-lint
golangci-lint: $(LOCALBIN) ## Install golangci-lint locally; reinstall when the pinned version drifts.
	@if ! { [ -x $(GOLANGCI_LINT) ] && $(GOLANGCI_LINT) --version 2>/dev/null | grep -qF "has version $(GOLANGCI_LINT_VERSION:v%=%) "; }; then \
		rm -f $(GOLANGCI_LINT); \
		GOTOOLCHAIN=go$(GO_VERSION) GOBIN=$(LOCALBIN) $(GO_CMD) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
	fi

.PHONY: protoc-gen-go
protoc-gen-go: $(LOCALBIN) ## Install protobuf Go generators locally.
	@test -s $(PROTOC_GEN_GO) || GOBIN=$(LOCALBIN) $(GO_CMD) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	@test -s $(PROTOC_GEN_GO_GRPC) || GOBIN=$(LOCALBIN) $(GO_CMD) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

.PHONY: envtest
envtest: $(LOCALBIN) ## Install setup-envtest locally.
	@test -s $(SETUP_ENVTEST) || GOBIN=$(LOCALBIN) $(GO_CMD) install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_VERSION)

.PHONY: kind
kind: $(LOCALBIN) ## Install kind locally when the system kind binary is unavailable.
	@if command -v kind >/dev/null 2>&1; then \
		true; \
	else \
		test -s $(LOCAL_KIND) || GOBIN=$(LOCALBIN) $(GO_CMD) install sigs.k8s.io/kind@$(KIND_VERSION); \
	fi

.PHONY: buf
buf: $(LOCALBIN) ## Install buf locally when the system buf binary is unavailable.
	@if command -v buf >/dev/null 2>&1; then \
		true; \
	else \
		test -s $(LOCAL_BUF) || GOBIN=$(LOCALBIN) $(GO_CMD) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION); \
	fi

.PHONY: promtool
promtool: $(LOCALBIN) ## Ensure a $(PROMTOOL_VERSION) promtool is available. Downloads with SHA-256 verification.
	@# Version pinning IS the point — CI runs against $(PROMTOOL_VERSION) and
	@# different versions can emit subtly different errors. We accept a
	@# system binary only if its version matches exactly; otherwise we
	@# install the pinned binary locally (replacing any stale local
	@# binary too).
	@if [ -x $(LOCAL_PROMTOOL) ] && \
		$(LOCAL_PROMTOOL) --version 2>&1 | head -1 | grep -qF "version $(PROMTOOL_VERSION) "; then \
		exit 0; \
	fi; \
	if command -v promtool >/dev/null 2>&1 && \
		promtool --version 2>&1 | head -1 | grep -qF "version $(PROMTOOL_VERSION) "; then \
		exit 0; \
	fi; \
	set -e; \
	rm -f $(LOCAL_PROMTOOL); \
	os=$$(uname -s | tr A-Z a-z); \
	arch=$$(uname -m); \
	case $$arch in x86_64) arch=amd64;; aarch64|arm64) arch=arm64;; esac; \
	dir=prometheus-$(PROMTOOL_VERSION).$${os}-$${arch}; \
	case "$${os}_$${arch}" in \
		linux_amd64)  want_sha="$(PROMTOOL_SHA256_linux_amd64)";; \
		linux_arm64)  want_sha="$(PROMTOOL_SHA256_linux_arm64)";; \
		darwin_amd64) want_sha="$(PROMTOOL_SHA256_darwin_amd64)";; \
		darwin_arm64) want_sha="$(PROMTOOL_SHA256_darwin_arm64)";; \
		*) echo "✗ unsupported os/arch: $${os}_$${arch} — add a SHA-256 to Makefile and retry."; exit 1;; \
	esac; \
	if [ -z "$$want_sha" ]; then \
		echo "✗ PROMTOOL_SHA256_$${os}_$${arch} is empty — refusing to install promtool without integrity verification."; \
		exit 1; \
	fi; \
	tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT INT TERM; \
	echo "downloading promtool $(PROMTOOL_VERSION) ($${os}/$${arch})"; \
	curl -fsSL "https://github.com/prometheus/prometheus/releases/download/v$(PROMTOOL_VERSION)/$${dir}.tar.gz" -o "$${tmp}/promtool.tgz"; \
	echo "$$want_sha  $${tmp}/promtool.tgz" | shasum -a 256 -c -; \
	tar -xzf "$${tmp}/promtool.tgz" -C "$${tmp}"; \
	mv "$${tmp}/$${dir}/promtool" $(LOCAL_PROMTOOL); \
	chmod +x $(LOCAL_PROMTOOL)

##@ Development

.PHONY: fmt
fmt: ## Run gofmt.
	$(GO_FMT) -w $$(find . -name '*.go' -not -path './bin/*' -not -path './.cache/*')

.PHONY: vet
vet: ## Run go vet.
	$(GO_CMD) vet ./...

.PHONY: lint
lint: fmt vet ## Run lightweight local lint checks.

.PHONY: ci-lint
ci-lint: golangci-lint ## Run golangci-lint.
	$(GOLANGCI_LINT) run --timeout 5m

.PHONY: tidy
tidy: ## Tidy Go modules.
	$(GO_CMD) mod tidy

.PHONY: generate
generate: controller-gen ## Generate Kubernetes deepcopy code.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./api/..."

.PHONY: manifests
manifests: controller-gen ## Generate CRD, RBAC, and webhook manifests.
	$(CONTROLLER_GEN) rbac:roleName=inference-cache-manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac output:webhook:artifacts:config=config/webhook

.PHONY: proto-gen
proto-gen: protoc-gen-go ## Generate protobuf Go code.
	PATH="$(LOCALBIN):$$PATH" protoc -I proto --go_out=. --go_opt=module=$(MODULE) --go-grpc_out=. --go-grpc_opt=module=$(MODULE) $$(find proto -name '*.proto' | sort)

.PHONY: proto-lint
proto-lint: buf ## Lint the gRPC contract with buf (lint-only; codegen stays on protoc).
	$(BUF) lint

.PHONY: build
build: ## Build controller, server, and kvevent-subscriber binaries.
	$(GO_CMD) build -ldflags="$(LD_FLAGS)" -o bin/controller ./cmd/controller
	$(GO_CMD) build -ldflags="$(LD_FLAGS)" -o bin/server ./cmd/server
	$(GO_CMD) build -ldflags="$(LD_FLAGS)" -o bin/kvevent-subscriber ./cmd/kvevent-subscriber

.PHONY: test
test: ## Run unit tests.
	$(GO_CMD) test ./...

.PHONY: test-race
test-race: ## Run unit tests with the race detector (fresh, no cache).
	$(GO_CMD) test -race -count=1 ./...

.PHONY: vulncheck
vulncheck: $(LOCALBIN) ## Scan dependencies + reachable code for known Go vulnerabilities. Needs network.
	@test -s $(GOVULNCHECK) || GOBIN=$(LOCALBIN) $(GO_CMD) install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	$(GOVULNCHECK) ./...

# Coverage gate. We measure the hand-written logic packages only: generated
# code (proto stubs, deepcopy), entrypoints (cmd/), tooling under hack/, and
# test helpers are excluded because their (un)covered statements would swamp
# the real signal.
#
# We pass -coverpkg=./... so a function exercised by tests in a DIFFERENT
# package (e.g. enginewire helpers tested through vllm_lmcache_test.go)
# counts as covered. Without it, Go reports only same-package "own" coverage,
# which silently understated real coverage for adapter helpers tested
# through their consumers and produced misleading per-package figures
# (e.g. enginewire 37% own vs ~95% via callers).
COVER_MIN ?= 90
COVER_PROFILE ?= cover.out
COVER_PROFILE_LOGIC ?= cover.logic.out
COVER_EXCLUDE := pkg/server/proto/|zz_generated|/cmd/|/hack/|pkg/testing/

.PHONY: cover
cover: ## Run tests with coverage and print the per-function report (logic packages, cross-package counted).
	$(GO_CMD) test ./... -covermode=atomic -coverpkg=./... -coverprofile=$(COVER_PROFILE)
	@grep -vE '$(COVER_EXCLUDE)' $(COVER_PROFILE) > $(COVER_PROFILE_LOGIC)
	@$(GO_CMD) tool cover -func=$(COVER_PROFILE_LOGIC)

.PHONY: cover-check
cover-check: ## Fail if logic-package coverage is below COVER_MIN% (cross-package counted; excludes generated/cmd/test-helper code).
	@$(GO_CMD) test ./... -covermode=atomic -coverpkg=./... -coverprofile=$(COVER_PROFILE) >/dev/null
	@grep -vE '$(COVER_EXCLUDE)' $(COVER_PROFILE) > $(COVER_PROFILE_LOGIC)
	@total=$$($(GO_CMD) tool cover -func=$(COVER_PROFILE_LOGIC) | awk '/^total:/ {gsub(/%/,"",$$3); print $$3}'); \
	if [ -z "$$total" ]; then echo "✗ no coverage data"; exit 1; fi; \
	awk "BEGIN { exit !($$total >= $(COVER_MIN)) }" \
		&& echo "✓ logic coverage $$total% (min $(COVER_MIN)%)" \
		|| { echo "✗ logic coverage $$total% is below the $(COVER_MIN)% gate"; exit 1; }

.PHONY: test-env
test-env: envtest ## Print envtest assets path for local integration tests.
	@$(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path

# Validates against the COMMITTED config/crd/bases +
# config/webhook/manifests.yaml — NOT a freshly regenerated tree. CRD drift
# is enforced separately by `make pre-pr` (gen-drift check) and the
# lint-generated-proto CI job, so this target staying behind committed
# manifests keeps it strictly checking the shipping shape.
.PHONY: verify-samples
verify-samples: envtest ## Run every YAML under config/samples/ through admission via envtest + the CacheBackend webhook (server-side dry-run). Honors top-of-file `# verify-samples: skip`.
	@KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		$(GO_CMD) run ./hack/verify-samples

.PHONY: image-build
image-build: controller-image server-image subscriber-image ## Build controller, server, and kvevent-subscriber images.

.PHONY: controller-image
controller-image: ## Build the controller container image.
	$(DOCKER_BUILD_CMD) build -f dockerfiles/Dockerfile --target controller -t $(IMG) .

.PHONY: server-image
server-image: ## Build the server container image.
	$(DOCKER_BUILD_CMD) build -f dockerfiles/Dockerfile --target server -t $(SERVER_IMG) .

.PHONY: subscriber-image
subscriber-image: ## Build the kvevent-subscriber container image (sidecar auto-attached to engine pods).
	$(DOCKER_BUILD_CMD) build -f dockerfiles/Dockerfile --target subscriber -t $(SUBSCRIBER_IMG) .

.PHONY: dev-cluster
dev-cluster: kind ## Create a local kind cluster for development.
	@if $(KIND) get clusters 2>/dev/null | grep -qx "$(KIND_CLUSTER)"; then \
		echo "kind cluster $(KIND_CLUSTER) already exists"; \
	else \
		$(KIND) create cluster --name $(KIND_CLUSTER) --image $(KIND_NODE_IMAGE) --wait 60s; \
	fi

.PHONY: clean
clean: ## Remove local build outputs.
	rm -rf bin dist

.PHONY: install-hooks
install-hooks: ## Install git hooks (vendor-neutral naming guard) via core.hooksPath.
	git config core.hooksPath .githooks
	@chmod +x .githooks/* 2>/dev/null || true
	@echo "git hooks installed (core.hooksPath=.githooks)"

.PHONY: verify-naming
verify-naming: ## Fail if core-identity files reference OCI/Oracle (see CONTRIBUTING.md).
	@bad=$$(grep -rniEI '\boci\b|oci\.com|oraclecloud|\boracle\b' \
		api proto pkg/server/proto config/crd config/rbac config/default config/manager config/samples config/server config/webhook config/certmanager config/overlays internal PROJECT go.mod 2>/dev/null || true); \
	if [ -n "$$bad" ]; then \
		echo "✗ OCI/Oracle reference in core-identity files (banned per CONTRIBUTING.md):"; \
		echo "$$bad" | sed 's/^/    /'; \
		exit 1; \
	fi; \
	echo "✓ core identity is vendor-neutral"

.PHONY: verify-no-internal-refs
verify-no-internal-refs: ## Fail if tracked files reference internal issue tracker tickets/URLs (see CONTRIBUTING.md).
	@bad=$$(git ls-files \
		| grep -vE '^(Makefile|CONTRIBUTING\.md|\.githooks/pre-commit|\.github/workflows/ci\.yml)$$' \
		| xargs grep -nIEi 'CAC-[0-9]+|linear\.app' 2>/dev/null || true); \
	if [ -n "$$bad" ]; then \
		echo "✗ internal issue-tracker reference in tracked files (banned per CONTRIBUTING.md):"; \
		echo "$$bad" | sed 's/^/    /'; \
		echo "  This is a public repo — link GitHub issues (#123); keep internal tracker IDs/URLs out of tracked files."; \
		exit 1; \
	fi; \
	echo "✓ no internal issue-tracker references"

.PHONY: fmt-check
fmt-check: ## Check Go formatting without modifying files.
	@unformatted=$$(gofmt -l $$(find . -name '*.go' -not -path './bin/*' -not -path './.cache/*')); \
	if [ -n "$$unformatted" ]; then echo "✗ gofmt needed on:"; echo "$$unformatted"; exit 1; fi; \
	echo "✓ gofmt clean"

.PHONY: verify-prometheus
verify-prometheus: promtool ## Lint + unit-test the Prometheus alerting rules under config/observability/.
	@set -e; PROMTOOL=$$($(RESOLVE_PROMTOOL)); \
	echo "==> using $$PROMTOOL ($$($$PROMTOOL --version 2>&1 | head -1))"; \
	echo "==> promtool check rules (flat alerting-rules.yaml)"; \
	$$PROMTOOL check rules config/observability/alerting-rules.yaml; \
	echo "==> promtool test rules (prometheus-rules-tests.yaml)"; \
	cd config/observability && $$PROMTOOL test rules prometheus-rules-tests.yaml
	@echo "==> drift check: PrometheusRule CR spec.groups matches alerting-rules.yaml groups"
	@$(GO_CMD) run ./hack/verify-prometheus-drift config/observability/alerting-rules.yaml config/observability/prometheus-rules.yaml
	@echo "✓ Prometheus rules valid"

.PHONY: ci
ci: verify-naming verify-no-internal-refs fmt-check vet ci-lint verify-prometheus test-race build ## Local CI gate (naming + internal-refs + fmt + vet + lint + Prometheus rules + race tests + build). Run by the pre-push hook.

.PHONY: pre-pr
pre-pr: ci ## Pre-PR gate: CI gate + generated-code drift check + sample admission check + review checklist.
	@$(MAKE) --no-print-directory manifests generate proto-gen >/dev/null
	@gen='config/crd config/rbac/role.yaml config/webhook/manifests.yaml api/v1alpha1/zz_generated.deepcopy.go pkg/server/proto'; \
	if ! git diff --quiet -- $$gen; then \
		echo "✗ generated-code drift — regenerate and commit these files:"; \
		git --no-pager diff --name-only -- $$gen; \
		exit 1; \
	fi
	@echo "✓ no generated-code drift"
	@$(MAKE) --no-print-directory verify-samples
	@echo ""
	@echo "Review checklist before 'gh pr create' (full list in CONTRIBUTING.md):"
	@echo "  [ ] Vendor-neutral naming — no oci/oracle in core identity"
	@echo "  [ ] Change matches the tech spec; CRD/proto backward-compatible for v1alpha1 consumers"
	@echo "  [ ] New/changed behavior has unit tests"
	@echo "  [ ] CI is green"
