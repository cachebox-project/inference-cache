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
DOCKER_BUILD_CMD ?= docker
KIND ?= $(shell command -v kind 2>/dev/null || echo $(LOCAL_KIND))
KIND_CLUSTER ?= inference-cache
KIND_NODE_IMAGE ?= kindest/node:v1.31.0

version_pkg = $(MODULE)/pkg/version
LD_FLAGS += -X '$(version_pkg).GitVersion=$(TAG)'
LD_FLAGS += -X '$(version_pkg).GitCommit=$(shell git rev-parse HEAD 2>/dev/null || echo unknown)'

CONTROLLER_TOOLS_VERSION ?= v0.16.5
GOLANGCI_LINT_VERSION ?= v1.64.8
PROTOC_GEN_GO_VERSION ?= v1.36.5
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1
SETUP_ENVTEST_VERSION ?= v0.0.0-20241105200929-48ec3b71211f
KIND_VERSION ?= v0.24.0
ENVTEST_K8S_VERSION ?= 1.31.0
BUF_VERSION ?= v1.69.0
GOVULNCHECK_VERSION ?= v1.1.4

CONTROLLER_GEN := $(LOCALBIN)/controller-gen
GOLANGCI_LINT := $(LOCALBIN)/golangci-lint
PROTOC_GEN_GO := $(LOCALBIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(LOCALBIN)/protoc-gen-go-grpc
SETUP_ENVTEST := $(LOCALBIN)/setup-envtest
LOCAL_KIND := $(LOCALBIN)/kind
LOCAL_BUF := $(LOCALBIN)/buf
GOVULNCHECK := $(LOCALBIN)/govulncheck
BUF ?= $(shell command -v buf 2>/dev/null || echo $(LOCAL_BUF))

.PHONY: all
all: build test ## Build binaries and run tests.

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Tools

.PHONY: controller-gen
controller-gen: $(LOCALBIN) ## Install controller-gen locally.
	@test -s $(CONTROLLER_GEN) || GOBIN=$(LOCALBIN) $(GO_CMD) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

.PHONY: golangci-lint
golangci-lint: $(LOCALBIN) ## Install golangci-lint locally.
	@test -s $(GOLANGCI_LINT) || GOBIN=$(LOCALBIN) $(GO_CMD) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

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
manifests: controller-gen ## Generate CRD and RBAC manifests.
	$(CONTROLLER_GEN) rbac:roleName=inference-cache-manager-role crd paths="./..." output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac

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
# code (proto stubs, deepcopy), entrypoints (cmd/), and test helpers are
# excluded because their (un)covered statements would swamp the real signal.
COVER_MIN ?= 65
COVER_PROFILE ?= cover.out
COVER_PROFILE_LOGIC ?= cover.logic.out
COVER_EXCLUDE := pkg/server/proto/|zz_generated|/cmd/|pkg/testing/

.PHONY: cover
cover: ## Run tests with coverage and print the per-function report (logic packages).
	$(GO_CMD) test ./... -covermode=atomic -coverprofile=$(COVER_PROFILE)
	@grep -vE '$(COVER_EXCLUDE)' $(COVER_PROFILE) > $(COVER_PROFILE_LOGIC)
	@$(GO_CMD) tool cover -func=$(COVER_PROFILE_LOGIC)

.PHONY: cover-check
cover-check: ## Fail if logic-package coverage is below COVER_MIN% (excludes generated/cmd/test-helper code).
	@$(GO_CMD) test ./... -covermode=atomic -coverprofile=$(COVER_PROFILE) >/dev/null
	@grep -vE '$(COVER_EXCLUDE)' $(COVER_PROFILE) > $(COVER_PROFILE_LOGIC)
	@total=$$($(GO_CMD) tool cover -func=$(COVER_PROFILE_LOGIC) | awk '/^total:/ {gsub(/%/,"",$$3); print $$3}'); \
	if [ -z "$$total" ]; then echo "✗ no coverage data"; exit 1; fi; \
	awk "BEGIN { exit !($$total >= $(COVER_MIN)) }" \
		&& echo "✓ logic coverage $$total% (min $(COVER_MIN)%)" \
		|| { echo "✗ logic coverage $$total% is below the $(COVER_MIN)% gate"; exit 1; }

.PHONY: test-env
test-env: envtest ## Print envtest assets path for local integration tests.
	@$(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path

.PHONY: image-build
image-build: controller-image server-image ## Build controller and server images.

.PHONY: controller-image
controller-image: ## Build the controller container image.
	$(DOCKER_BUILD_CMD) build -f dockerfiles/Dockerfile --target controller -t $(IMG) .

.PHONY: server-image
server-image: ## Build the server container image.
	$(DOCKER_BUILD_CMD) build -f dockerfiles/Dockerfile --target server -t $(SERVER_IMG) .

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
		api proto pkg/server/proto config/crd config/rbac internal PROJECT go.mod 2>/dev/null || true); \
	if [ -n "$$bad" ]; then \
		echo "✗ OCI/Oracle reference in core-identity files (banned per CONTRIBUTING.md):"; \
		echo "$$bad" | sed 's/^/    /'; \
		exit 1; \
	fi; \
	echo "✓ core identity is vendor-neutral"

.PHONY: fmt-check
fmt-check: ## Check Go formatting without modifying files.
	@unformatted=$$(gofmt -l $$(find . -name '*.go' -not -path './bin/*' -not -path './.cache/*')); \
	if [ -n "$$unformatted" ]; then echo "✗ gofmt needed on:"; echo "$$unformatted"; exit 1; fi; \
	echo "✓ gofmt clean"

.PHONY: ci
ci: verify-naming fmt-check vet test-race build ## Local CI gate (naming + fmt + vet + race tests + build). Run by the pre-push hook.

.PHONY: pre-pr
pre-pr: ci ## Pre-PR gate: CI gate + generated-code drift check + review checklist.
	@$(MAKE) --no-print-directory manifests generate proto-gen >/dev/null
	@gen='config/crd config/rbac/role.yaml api/v1alpha1/zz_generated.deepcopy.go pkg/server/proto'; \
	if ! git diff --quiet -- $$gen; then \
		echo "✗ generated-code drift — regenerate and commit these files:"; \
		git --no-pager diff --name-only -- $$gen; \
		exit 1; \
	fi
	@echo "✓ no generated-code drift"
	@echo ""
	@echo "Review checklist before 'gh pr create' (full list in CONTRIBUTING.md):"
	@echo "  [ ] Vendor-neutral naming — no oci/oracle in core identity"
	@echo "  [ ] Change matches the tech spec; CRD/proto backward-compatible for v1alpha1 consumers"
	@echo "  [ ] New/changed behavior has unit tests"
	@echo "  [ ] CI is green"
