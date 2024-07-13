<!-- markdownlint-disable-file MD024 MD025 MD041 -->

![shopmonkey!](https://www.shopmonkey.io/static/sm-light-logo-2c92d57bf5d188bb44c1b29353579e1f.svg)

# Overview

This repository contains the reference implementation of the Enterprise Data Streaming server. You can find more detailed information at the [Shopmonkey Developer Portal](https://shopmonkey.dev/eds).

## Download Release Binary

You can download release binary for different operation systems from the [Release](https://github.com/shopmonkeyus/eds-server/releases) section.

## Get Help

```
./eds-server help
```

## Local Development

### Requirements

You will need [Golang](https://go.dev/dl/) version 1.22 or later to use this package.

You will need to install [Nats](https://nats.io/) to use this package.

### Creating a manual release locally

You will need to install [Go Releaser](https://goreleaser.com/install/).

Run the following:

```
goreleaser release --snapshot --clean
```

## License

All files in this repository are licensed under the [MIT license](https://opensource.org/licenses/MIT). See the [LICENSE](./LICENSE) file for details.
