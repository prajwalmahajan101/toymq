.PHONY: fmt fmt-check vet lint test ci hooks help

GO        ?= go
GOFMT     ?= gofmt
PKGS      ?= ./...
TIMEOUT   ?= 5m

help:
	@echo "Targets:"
	@echo "  fmt        - gofmt -w on all .go files"
	@echo "  fmt-check  - fail if gofmt would change anything"
	@echo "  vet        - go vet ./..."
	@echo "  lint       - golangci-lint run ./..."
	@echo "  test       - go test -race -timeout $(TIMEOUT) ./..."
	@echo "  ci         - fmt-check + vet + lint + test"
	@echo "  hooks      - install .githooks as the repo hooksPath"

fmt:
	$(GOFMT) -w .

fmt-check:
	@out=$$($(GOFMT) -l .); \
	if [ -n "$$out" ]; then \
	  echo "gofmt would reformat:"; \
	  echo "$$out"; \
	  exit 1; \
	fi

vet:
	$(GO) vet $(PKGS)

lint:
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
	  echo "golangci-lint not installed. Install:"; \
	  echo "  go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	  exit 1; \
	fi
	golangci-lint run $(PKGS)

test:
	$(GO) test -race -timeout $(TIMEOUT) $(PKGS)

ci: fmt-check vet lint test

hooks:
	git config core.hooksPath .githooks
	@echo "Pre-commit hook installed (.githooks/pre-commit)"
