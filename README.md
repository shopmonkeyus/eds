<!-- markdownlint-disable-file MD024 MD025 MD041 -->

![shopmonkey!](https://www.shopmonkey.io/static/sm-light-logo-2c92d57bf5d188bb44c1b29353579e1f.svg)

> [!IMPORTANT]
> The main branch is now for the new v3 version. If you're looking for the previous v2 version, use the [v2 branch](https://github.com/shopmonkeyus/eds-server/tree/v2).

# Overview

This repository contains the reference implementation of the Enterprise Data Streaming server. You can find more detailed information at the [Shopmonkey Developer Portal](https://shopmonkey.dev/eds).

## Download Release Binary

You can download release binary for different operation systems from the [Release](https://github.com/shopmonkeyus/eds-server/releases) section.

## Get Help

```
./eds-server help
```

## Usage

There are 3 main commands:

- **import** - used for importing data from your Shopmonkey account into a target destination
- **server** - used for running the EDS server to deliver messages from Shopmonkey to the target destination
- **version** - prints the current version of the software

All commands require a valid Shopmonkey API key. You can specify the key using the command line `--api-key` option or set the environment variable `SM_APIKEY`. Be careful to safeguard this key as it is like a password and will grant any user access to your protected data.

The target driver is configured using the `--url` option. The driver that is selected is based on the scheme/protocol part of the URL. The following drivers are supported:

- **mysql** - used to stream data into a MySQL database
- **postgres** - used to stream data into a PostgreSQL database
- **snowflake** - used to stream data into a Snowflake database
- **s3** - used to stream data into a S3 compatible cloud storage (AWS, Google Cloud, Minio, etc)
- **kafka** - used to stream data into a Kafka topic
- **eventhub** - used to stream data to Microsoft Azure [EventHub](https://azure.microsoft.com/en-us/products/event-hubs)

> Not all driver's currently support importing data

You can get a list of drivers with example URL patterns by running the following:

```
./eds-server server help
```

You can get specific help for a driver with the following:

```
./eds-server server help [driver]
```

Such as:

```
./eds-server server help s3
```

## Importing Data

The import command will take a snapshot of the data in the Shopmonkey Database and import that data to your destination. This is useful to bootstrap or backfill a new destination with existing data. You can then run the server to stream additional incremental changes as they happen.

Example:

```
./eds-server import --api-key 123 --url "postgresql://admin:root@localhost:5432/test?sslmode=disable"
```

This command will import data from the Shopmonkey Database into PostgreSQL database and exit once completed.

> [!CAUTION]
> The import command will remove existing data from the target destination (dependent on the specific driver). Use with caution to not lose data.

## Running the Server

Running the server will start a process which will connect to the Shopmonkey system and stream change data capture (CDC) records in JSON format to the server which will forward them intelligently to the driver for specific handling. The Server will automatically handle logging, crash detection and sending health reports back to Shopmonkey for monitoring.

When the server is started for the first time, it will create a subscription on the Shopmonkey system to register interest in your real-time CDC changes. However, if the server is shutdown after more than 7 days, the subscription will be expired and any pending data will be lost. In this case, you will have to re-import your data and start streaming again.

The server captures data is near real-time as they occur. However, EDS will attempt to intelligent batch data when a large amount of data is pending to speed up data processing. You should expect latencies of around 100-250ms when your system is not under heavy load and around 2-3s when a lot of data is pending processing. EDS server attempts to make a tradeoff of better batching and load during heavy data periods while still providing fast data access during low load periods.

# Local Development

## Requirements

You will need [Golang](https://go.dev/dl/) version 1.22 or later to use this package.

You will need to install [Nats](https://nats.io/) to use this package.

## Creating a manual release locally

You will need to install [Go Releaser](https://goreleaser.com/install/).

Run the following:

```
goreleaser release --snapshot --clean
```

# License

All files in this repository are licensed under the [MIT license](https://opensource.org/licenses/MIT). See the [LICENSE](./LICENSE) file for details.
