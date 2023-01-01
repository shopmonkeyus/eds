<!-- markdownlint-disable-file MD024 MD025 MD041 -->

![shopmonkey!](https://www.shopmonkey.io/static/sm-light-logo-2c92d57bf5d188bb44c1b29353579e1f.svg)

# Overview

This repository contains the reference implementation of the Enterprise Data Streaming server.

## Requirements

You will need [Golang](https://go.dev/dl/) version 1.19 or later to use this package.

## Basic Usage

You first need to migrate your database to load the schema into a database. The database must exist before running this command:

```bash
go run . migrate --driver postgres --dsn "host=localhost user=root dbname=test port=26257 sslmode=disable TimeZone=US/Central"
```

Replace the `--dsn` flag with your database connection setting. Replace the `--driver` flag with your database driver name.

Once you have a database, you can start the server:

```bash
go run . start --driver postgres --dsn "host=localhost user=root dbname=test port=26257 sslmode=disable TimeZone=US/Central"
```

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
