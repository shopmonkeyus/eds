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
- [Snowflake DB](https://www.snowflake.com/)
- File - use `file://<PATH>` to stream Json lines via STDIN to an executable provided by PATH

## Development

These are the instructions to run EDS for local development. This will spin up a postgres and azure-edge database using docker compose.

```bash
./hack/localstack

export SQL_PASS=Asdf1234! && go run . server --server nats://localhost:4222 --company-id 6287a4154d1a72cc5ce091bb "sqlserver://sa:$SQL_PASS@localhost:1433?database=shopmonkey"

export PGPASS=postgres && go run . server --server nats://localhost:4222 --company-id 6287a4154d1a72cc5ce091bb "postgresql://postgres:$PGPASS@localhost:5432/shopmonkey?sslmode=disable"

```

To run EDS with Snowflake, your connection string should be in the format of:

`snowflake://<username>:<password>@<organization_name>-<account>/<database_name>/<schema>?warehouse=<warehouse_name>&client_session_keep_alive=true`

The `client_session_keep_alive=true` portion is optional, but you may run into authentication issues after 4 hours if there is no activity from EDS to Snowflake. See [Snowflake Session Policies](https://docs.snowflake.com/en/user-guide/session-policies) for more details.

A full connection string would look like: `snowflake://jsmith:mypassword@zflycky-cu81015/mydb/PUBLIC?warehouse=COMPUTE_WH&client_session_keep_alive=true`

## Importer

To run the importer side of EDS, utilize the `--importer` flag and specify a pre-signed URL for a Gzipped JSON Lines file. Each line will be a record that will be imported into your database. For assistance in generating the Gzipped JSON Lines file and obtaining the pre-signed URL, please reach out to your representative at Shopmonkey.

### Logging

You can turn on verbose logging with `--verbose` flag.
You can silence log output with the `--silence` flag.

### Dump Messages

When running the server, you can dump incoming change change events to a file by passing in the `--dump-dir` flag pointing to a folder to place the files. If the folder doesn't exist, it will be created.

### NATS Logging

You can turn on advanced trace logging for communication between the NATS server by using the flag `--trace-nats`.

## License

All files in this repository are licensed under the [MIT license](https://opensource.org/licenses/MIT). See the [LICENSE](./LICENSE) file for details.
