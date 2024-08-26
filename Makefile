.PHONY: all build lint release test vet tidy

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

test: tidy build lint vet
	@go test -v -count=1 ./...
