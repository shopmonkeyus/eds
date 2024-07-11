.PHONY: all build lint

all: build

build:
	@go build -v -o /dev/null

lint:
	@go fmt ./...
