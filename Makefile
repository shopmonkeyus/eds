.PHONY: all build lint release test vet tidy e2e proto proto-check

all: build

proto:
	@cd proto && buf generate

proto-check: proto
	@git diff --exit-code pkg/edsv4prototype/v1

build:
	@go build -v -o /dev/null

release:
	@goreleaser release --snapshot --clean

lint:
	@go fmt ./...

vet:
	@go vet ./...

tidy:
	@go mod tidy

e2e:
	@go run -tags e2e . e2e -v $(E2E_TESTS)

test: tidy build lint vet
	@go test -v -count=1 ./...

