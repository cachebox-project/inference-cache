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

CONTROLLER_GEN := $(LOCALBIN)/controller-gen
GOLANGCI_LINT := $(LOCALBIN)/golangci-lint
PROTOC_GEN_GO := $(LOCALBIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(LOCALBIN)/protoc-gen-go-grpc
SETUP_ENVTEST := $(LOCALBIN)/setup-envtest
LOCAL_KIND := $(LOCALBIN)/kind

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

.PHONY: build
build: ## Build controller and server binaries.
	$(GO_CMD) build -ldflags="$(LD_FLAGS)" -o bin/controller ./cmd/controller
	$(GO_CMD) build -ldflags="$(LD_FLAGS)" -o bin/server ./cmd/server

.PHONY: test
test: ## Run unit tests.
	$(GO_CMD) test ./...

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
