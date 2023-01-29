<!-- markdownlint-disable-file MD024 MD025 MD041 -->

![shopmonkey!](https://www.shopmonkey.io/static/sm-light-logo-2c92d57bf5d188bb44c1b29353579e1f.svg)

# Overview

This repository contains the reference implementation of the Enterprise Data Streaming server.

## Requirements

You will need [Golang](https://go.dev/dl/) version 1.19 or later to use this package.

## Basic Usage

You first need to migrate your database to load the schema into a database. The database must exist before running this command:

```bash
go run . migrate --url 'postgresql://root@localhost:26257/test?sslmode=disable'
```

Replace the `--url` flag with your provider connection setting

Once you have a database, you can start the server:

```bash
go run . start --url 'postgresql://root@localhost:26257/test?sslmode=disable'
```

You will also need to provide a server credentials file provided by Shopmonkey and your company id. These should be passed in as command line arguments as well:

```bash
--creds server.creds --company-id 1234
```

## Providers

The following are the supported providers:

- [PostgreSQL DB](https://www.postgresql.org/)
- [Cockroach DB](https://www.cockroachlabs.com/) - use the postgres connection string
- [SQL Server DB](https://www.microsoft.com/en-us/sql-server)
- File - use `file://<PATH>` to stream files to a directory provided by PATH

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
