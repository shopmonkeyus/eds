.PHONY: all build lint release test vet tidy e2e

all: build

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
	@go run -tags e2e . e2e -v

test: tidy build lint vet
	@go test -v -count=1 ./...

