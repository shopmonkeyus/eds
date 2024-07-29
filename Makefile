.PHONY: all build lint release test

all: build

build:
	@go build -v -o /dev/null

release:
	@goreleaser release --snapshot --clean

lint:
	@go fmt ./...

test:
	@go test -v ./...
