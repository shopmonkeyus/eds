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

> [!IMPORTANT]
> The server should be created using HQ. If you do not have access to HQ or the EDS capabilities within HQ, please contact your account team for further assistance. Running the server outside of HQ is not recommended.

Running the server will start a process which will connect to the Shopmonkey system and stream change data capture (CDC) records in JSON format to the server which will forward them intelligently to the driver for specific handling. The Server will automatically handle logging, crash detection and sending health reports back to Shopmonkey for monitoring.

When the server is started for the first time, it will create a subscription on the Shopmonkey system to register interest in your real-time CDC changes. However, if the server is shutdown after more than 7 days, the subscription will be expired and any pending data will be lost. In this case, you will have to re-import your data and start streaming again.

The server captures data is near real-time as they occur. However, EDS will attempt to intelligent batch data when a large amount of data is pending to speed up data processing. You should expect latencies of around 100-250ms when your system is not under heavy load and around 2-3s when a lot of data is pending processing. EDS server attempts to make a tradeoff of better batching and load during heavy data periods while still providing fast data access during low load periods.

## Data Directory

By default, the server will store log and data files in the current working directory where you start the server. However, you can change the location of this data directory by setting the `--data-dir` to a writable directory. This directory will default to `cwd/data` if not provided and the server attempt to make this directory on startup if it does not exist.

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

> [!IMPORTANT]
> You should enroll the server outside of Docker initially and mount the resulting `config.toml` file after setup. This file should be treated as a secret and should not be committed to your Docker image.

# Security

## Verification of Published Software

To verify that the software we release are built by Shopmonkey you can use our [GNU PGP Public Key](./shopmonkey.asc):

```
-----BEGIN PGP PUBLIC KEY BLOCK-----

mDMEZqbX1BYJKwYBBAHaRw8BAQdArMIL32vatKdxrJK2F/aKN+q3hS73CPnUdgpJ
KrxyOYu0LFNob3Btb25rZXksIEluYy4gPGVuZ2luZWVyaW5nQHNob3Btb25rZXku
aW8+iJMEExYKADsWIQTJlG6Z3WVVBa1f6dL+z/SRXnv0kgUCZqbX1AIbAwULCQgH
AgIiAgYVCgkICwIEFgIDAQIeBwIXgAAKCRD+z/SRXnv0kj2FAP9AfaBMaXBGr9OP
vQXHD/dC9DVqu5AWJns98A6OAMxYDAD+IDfjZGf9SsBal9/HE5j6FbuRCcl52Jwx
97f7OrIAhQa4OARmptfUEgorBgEEAZdVAQUBAQdAI7jC9e+tOyLA+k8JWvZu666l
LjXvPznbu9I2dkaLMzcDAQgHiHgEGBYKACAWIQTJlG6Z3WVVBa1f6dL+z/SRXnv0
kgUCZqbX1AIbDAAKCRD+z/SRXnv0kuoWAP91V3SLcNLaXndipxJJ/Z5oQjsyuTDy
3rhqtxmg+EsXVgD/SFc612ihYO2/DFooZ04EU4wwFjj/0u4rxcUdj04u+AI=
=r0eF
-----END PGP PUBLIC KEY BLOCK-----
```

Download this file and name is `shopmonkey.asc` and then import this file into your keyring using GNU GPG:

```
gpg --import shopmonkey.asc
```

Verify any of our released files using the following command:

```
gpg --verify somefile.sig somefile
```

You can also run the command `publickey` to print out the public key:

```
./eds-server publickey
```

## Responsible Disclosure

Shopmonkey, Inc. welcomes feedback from security researchers and the general public to help improve our security. If you believe you have discovered a vulnerability, privacy issue, exposed data, or other security issues in any of our assets, we want to hear from you. This policy outlines steps for reporting vulnerabilities to us, what we expect, what you can expect from us.

### Our Commitment

When working with us, according to this policy, you can expect us to:

- Respond to your report promptly, and work with you to understand and validate your report;
- Strive to keep you informed about the progress of a vulnerability as it is processed;
- Work to remediate discovered vulnerabilities in a timely manner, within our operational constraints; and
- Extend Safe Harbor for your vulnerability research that is related to this policy.

### Our Expectations

In participating in our vulnerability disclosure program in good faith, we ask that you:

- Play by the rules, including following this policy and any other relevant agreements. If there is any inconsistency between this policy and any other applicable terms, the terms of this policy will prevail;
- Report any vulnerability you’ve discovered promptly;
- Avoid violating the privacy of others, disrupting our systems, destroying data, and/or harming user experience;
- Use only the Official Channels to discuss vulnerability information with us;
- Provide us a reasonable amount of time (at least 45 days from the initial report) to resolve the issue before you disclose it publicly;
- Perform testing only on in-scope systems, and respect systems and activities which are out-of-scope;
- If a vulnerability provides unintended access to data: Limit the amount of data you access to the minimum required for effectively demonstrating a Proof of Concept; and cease testing and submit a report immediately if you encounter any user data during testing, such as Personally Identifiable Information (PII), Personal Healthcare Information (PHI), credit card data, or proprietary information;
- You should only interact with test accounts you own or with explicit permission from the account holder; and
- Do not engage in extortion.

### Official Channels

Please report security issues via security@shopmonkey.io, providing all relevant information. The more details you provide, the easier it will be for us to triage and fix the issue.

### Safe Harbor

When conducting vulnerability research, according to this policy, we consider this research conducted under this policy to be:

- Authorized concerning any applicable anti-hacking laws, and we will not initiate or support legal action against you for accidental, good-faith violations of this policy;
- Authorized concerning any relevant anti-circumvention laws, and we will not bring a claim against you for circumvention of technology controls;
- Exempt from restrictions in our Terms of Service (TOS) and/or Acceptable Usage Policy (AUP) that would interfere with conducting security research, and we waive those restrictions on a limited basis; and
- Lawful, helpful to the overall security of the Internet, and conducted in good faith. You are expected, as always, to comply with all applicable laws. If legal action is initiated by a third party against you and you have complied with this policy, we will take steps to make it known that your actions were conducted in compliance with this policy.

If at any time you have concerns or are uncertain whether your security research is consistent with this policy, please submit a report through one of our Official Channels before going any further.

Note that the Safe Harbor applies only to legal claims under the control of the organization participating in this policy, and that the policy does not bind independent third parties.

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
