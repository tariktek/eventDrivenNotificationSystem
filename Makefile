MODULE   := github.com/tariktek/eventDrivenNotificationSystem
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
LDFLAGS  := -s -w \
	-X $(MODULE)/internal/platform/buildinfo.Version=$(VERSION) \
	-X $(MODULE)/internal/platform/buildinfo.Commit=$(COMMIT)
BINARIES := api worker healthcheck

.DEFAULT_GOAL := help

.PHONY: help
help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## ---------- Development ----------

.PHONY: tidy
tidy: ## Tidy go.mod/go.sum
	go mod tidy

.PHONY: generate
generate: ## Regenerate code from api/openapi.yaml
	go generate ./...

.PHONY: generate-check
generate-check: generate ## Fail if generated code is out of date with the spec
	@git diff --exit-code -- '*.gen.go' || (echo "generated code is stale: run 'make generate' and commit" && exit 1)

.PHONY: fmt
fmt: ## Format code (gofumpt + goimports)
	golangci-lint fmt ./...

.PHONY: lint
lint: ## Run linters
	golangci-lint run ./...

.PHONY: build
build: ## Build all binaries into ./bin
	@for b in $(BINARIES); do \
		echo "building $$b"; \
		CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$$b ./cmd/$$b || exit 1; \
	done

.PHONY: run-api
run-api: ## Run the API locally
	go run ./cmd/api

.PHONY: run-worker
run-worker: ## Run the worker locally (ops server on :8081 to avoid clashing with the API)
	HTTP_ADDR=:8081 go run ./cmd/worker

## ---------- Docker Compose ----------

.PHONY: up
up: ## Build and start the full stack in the background
	VERSION=$(VERSION) COMMIT=$(COMMIT) docker compose up -d --build

.PHONY: down
down: ## Stop the stack (data volumes are kept)
	docker compose down

.PHONY: clean
clean: ## Stop the stack and delete its data volumes
	docker compose down -v

.PHONY: logs
logs: ## Follow application logs
	docker compose logs -f api worker

.PHONY: ps
ps: ## Show container status
	docker compose ps

## ---------- Tests ----------

.PHONY: test
test: test-unit test-integration ## Run all tests (integration tests need Docker)

.PHONY: test-unit
test-unit: ## Run unit tests with the race detector
	go test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## Run integration tests (requires Docker)
	go test -race -count=1 -tags=integration ./...

.PHONY: cover
cover: ## Run unit tests and write an HTML coverage report
	go test -count=1 -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1
