MODULE   := github.com/tariktek/eventDrivenNotificationSystem
VERSION  ?= $(shell git describe --tags --always --dirty || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD || echo none)
LDFLAGS  := -s -w \
	-X $(MODULE)/internal/platform/buildinfo.Version=$(VERSION) \
	-X $(MODULE)/internal/platform/buildinfo.Commit=$(COMMIT)
BINARIES := api worker mockprovider healthcheck

.DEFAULT_GOAL := help

# Recipes are plain commands so they also run from PowerShell/cmd on
# Windows; only "help" needs a Unix shell (grep, awk: use Git Bash or WSL).

.PHONY: help
help: ## List available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

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

# The race detector needs cgo (a C compiler); use it wherever it is available.
RACE := $(if $(filter 1,$(shell go env CGO_ENABLED)),-race,)
FUZZTIME ?= 30s

# -p 1 runs one package at a time: each package starts its own PostgreSQL
# and Redis containers, and the delivery tests measure timing (rate limits,
# scheduled sends) that a machine starting a dozen containers at once would
# disturb.
.PHONY: test
test: ## Run every test: unit, integration and end-to-end (needs Docker)
	go test $(RACE) -count=1 -p 1 -tags=integration ./...

.PHONY: test-unit
test-unit: ## Run unit tests only (no Docker, seconds)
	go test $(RACE) -count=1 ./...

.PHONY: test-e2e
test-e2e: ## Run the end-to-end tests: API + worker + relay + callbacks (needs Docker)
	go test $(RACE) -count=1 -tags=integration ./test/e2e/...

.PHONY: test-fuzz
test-fuzz: ## Fuzz the input parsers for FUZZTIME each (default 30s)
	go test -run "^$$" -fuzz "^FuzzNotification$$" -fuzztime $(FUZZTIME) ./internal/validation/
	go test -run "^$$" -fuzz "^FuzzAnalyzeSMS$$" -fuzztime $(FUZZTIME) ./internal/validation/
	go test -run "^$$" -fuzz "^FuzzParseAndRender$$" -fuzztime $(FUZZTIME) ./internal/msgtemplate/
	go test -run "^$$" -fuzz "^FuzzDecodeCursor$$" -fuzztime $(FUZZTIME) ./internal/repository/postgres/

.PHONY: test-load
test-load: ## Run the k6 load tests against the running stack (make up first; ~10 min)
	docker compose --profile load run --rm k6 run /scripts/steady.js
	docker compose --profile load run --rm k6 run /scripts/flash-sale.js
	docker compose --profile load run --rm k6 run /scripts/provider-faults.js

.PHONY: cover
cover: ## Run all tests and write an HTML coverage report (needs Docker)
	go test -count=1 -p 1 -tags=integration -coverpkg=./internal/... -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1
