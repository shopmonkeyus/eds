<!-- markdownlint-disable-file MD024 MD025 MD041 -->

![shopmonkey!](https://www.shopmonkey.io/static/sm-light-logo-2c92d57bf5d188bb44c1b29353579e1f.svg)

# Overview

This repository contains the reference implementation of the Enterprise Data Streaming server. You can find more detailed information at the [Shopmonkey Developer Portal](https://shopmonkey.dev/eds).

## Download Release Binary

You can download release binary for different operation systems from the [Release](https://github.com/shopmonkeyus/eds-server/releases) section.

## Requirements

You will need [Golang](https://go.dev/dl/) version 1.20 or later to use this package.

## Basic Usage

After downloading the `eds-server` binary and placing it in your path, you can start the server as follows:
```bash
eds-server --creds <your_server>.creds 'postgresql://root@localhost:26257/test?sslmode=disable'
```

```bash
eds-server --creds <your_server>.creds 'file:///<path-to-eds>/eds-server/echo.sh' --verbose
```
To run the examples, please clone this repo and follow the linked READMEs.

- [EDS Server with Bash target example](./examples/bash/README.md)

- [EDS Server with Python target example](./examples/python/README.md)

 
## Providers

The following are the supported providers:

- [PostgreSQL DB](https://www.postgresql.org/)
- [SQL Server DB](https://www.microsoft.com/en-us/sql-server)
- File - use `file://<PATH>` to stream Json lines via STDIN to an executable provided by PATH

## Advanced Usage

### Logging

You can turn on verbose logging with `--verbose` flag.
You can silence log output with the `--silence` flag.

### Dump Messages

When running the server, you can dump incoming change change events to a file by passing in the `--dump-dir` flag pointing to a folder to place the files. If the folder doesn't exist, it will be created.

### NATS Logging

You can turn on advanced trace logging for communication between the NATS server by using the flag `--trace-nats`.

## License

All files in this repository are licensed under the [MIT license](https://opensource.org/licenses/MIT). See the [LICENSE](./LICENSE) file for details.
