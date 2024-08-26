.PHONY: all build lint release test vet

all: build

build:
	@go build -v -o /dev/null

release:
	@goreleaser release --snapshot --clean

lint:
	@go fmt ./...

vet:
	@go vet ./...

test: build lint vet
	@go test -v -count=1 ./...
