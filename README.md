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
- **sqlserver** - used to stream data into a Microsoft SQLServer database
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

The import command will ensure that you have a valid EDS session before running an import. It will ensure that any data that is processed during the import processed will automatically be skipped when the server is started after the import to ensure duplicates aren't processed.

## Running the Server

Running the server will start a process which will connect to the Shopmonkey system and stream change data capture (CDC) records in JSON format to the server which will forward them intelligently to the driver for specific handling. The Server will automatically handle logging, crash detection and sending health reports back to Shopmonkey for monitoring.

When the server is started for the first time, it will create a subscription on the Shopmonkey system to register interest in your real-time CDC changes. However, if the server is shutdown after more than 7 days, the subscription will be expired and any pending data will be lost. In this case, you will have to re-import your data and start streaming again.

The server captures data is near real-time as they occur. However, EDS will attempt to intelligent batch data when a large amount of data is pending to speed up data processing. You should expect latencies of around 100-250ms when your system is not under heavy load and around 2-3s when a lot of data is pending processing. EDS server attempts to make a tradeoff of better batching and load during heavy data periods while still providing fast data access during low load periods.

## Data Directory

By default, the server will store log and data files in the current working directory where you start the server. However, you can change the location of this data directory by setting the `--data-dir` to a writable directory. This directory must exist before starting the server and the server will error on startup if it does not.

## Monitoring the Server

By default the server runs a HTTP server on port `8080`. This can be changed either with the `--port` command line flag or by setting the `PORT` environment variable.

### Health Checks

You can access the `/` default endpoint to perform a health check. If the server is in shutdown mode, it will return a HTTP status code 503 (Service Unavailable).

### Metrics

You can access the `/metrics` endpoint to retrieve [Prometheus](http://prometheus.io/) metrics. The following metrics are available:

- `eds_pending_events`: Gaugae representing the number of pending events.
- `eds_total_events`: Counter representing the total number of events processed.
- `eds_flush_duration_seconds`: Histogram representing the duration of time in second that it takes for the driver to flush data to the destination.
- `eds_flush_count`: Histogram representing the count of events pending when flushed to the destination.

### Session

The server will automatically renew the EDS session with Shopmonkey every 24 hours. This ensures that your server credentials are short lived.

### Health Monitoring

The server will automatically send a health check event with a few system details about your server every minute. This ensures that Shopmonkey is able to monitor your EDS server and provide information for you in our HQ product.

The following system information is sent to Shopmonkey:

- Unique Machine ID
- Private IP Address
- Number of CPUs
- OS Name
- OS Architecture Name (such as arm64)
- Version of Golang that the server was compiled with
- Version of EDS server

You can audit this information by reviewing the [sysinfo.go](https://github.com/shopmonkeyus/eds-server/blob/main/internal/util/sysinfo.go) and [consumer.go](https://github.com/shopmonkeyus/eds-server/blob/main/internal/consumer/consumer.go) files.

In addition, the server also deliver the metrics mentioned above.

### Session Logging

The server will automatically upload server logs to Shopmonkey to assist in observability, monitoring and remediation during error conditions. In addition, we provide these logs as part of the HQ product. The server logs will be sent periodically while the server is running as well as on shutdown or during a server crash.

### Crash Detection

The server will automatically detect crashes, report them to Shopmonkey and restart the system. In the event the server restarts unexpectedly more than 5 times, it will error and exit with a non-zero exit code.

## Auto Update

Coming soon.

# Deployment

You can use Docker to run the server.

The image path is: `us-docker.pkg.dev/shopmonkey-v2/public/eds-server`

Each release will automatically add 2 image tags in the format:

- `us-docker.pkg.dev/shopmonkey-v2/public/eds-server:<version>`
- `us-docker.pkg.dev/shopmonkey-v2/public/eds-server:latest`

For example, for the v3.0.2 release, the Docker image tag to use is:

`us-docker.pkg.dev/shopmonkey-v2/public/eds-server:v3.0.2`

> [!CAUTION]
> You should pin the version of the release when referencing the Docker image in your deployment.

When configuring this image in Docker Compose or Kubernetes you may want to pass in arguments to customize.

You should use the arguments without the name of the binary such as:

```
["server", "--data-dir", "/var/data", "--verbose"]
```

# Local Development

## Requirements

You will need [Golang](https://go.dev/dl/) version 1.22 or later to use this package.

## Creating a manual release locally

You will need to install [Go Releaser](https://goreleaser.com/install/).

Run the following:

```
goreleaser release --snapshot --clean
```

# License

All files in this repository are licensed under the [MIT license](https://opensource.org/licenses/MIT). See the [LICENSE](./LICENSE) file for details.
