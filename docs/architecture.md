# EDS Architecture Overview

This document provides a technical deep-dive into the EDS server internals for contributors and debuggers.

> For general usage, supported drivers, and deployment instructions, see the main [README](../README.md).

## System Components

```
Shopmonkey API          NATS JetStream             Destination
(Schema/Export)         (CDC Events)               (Driver Target)
      │                       │                          ▲
      │                       │                          │
      ▼                       ▼                          │
┌─────────────────────────────────────────────────────────────┐
│                         EDS Server                          │
│                                                             │
│   Registry        Consumer        Driver        Tracker     │
│   (Schema)        (NATS)          (Dest)        (State)     │
└─────────────────────────────────────────────────────────────┘
```

## Key Source Files

| Component            | File                                                                                                           | Description                                 |
| -------------------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| **Driver Interface** | [`internal/driver.go`](https://github.com/shopmonkeyus/eds/blob/main/internal/driver.go)                       | Core interface all drivers must implement   |
| **Consumer**         | [`internal/consumer/consumer.go`](https://github.com/shopmonkeyus/eds/blob/main/internal/consumer/consumer.go) | NATS JetStream consumer for CDC events      |
| **Server Command**   | [`cmd/server.go`](https://github.com/shopmonkeyus/eds/blob/main/cmd/server.go)                                 | Main server entry point and wrapper loop    |
| **Import Command**   | [`cmd/import.go`](https://github.com/shopmonkeyus/eds/blob/main/cmd/import.go)                                 | Bulk data import functionality              |
| **Schema Registry**  | [`internal/registry/registry.go`](https://github.com/shopmonkeyus/eds/blob/main/internal/registry/registry.go) | Schema versioning and migration tracking    |
| **Tracker**          | [`internal/tracker/tracker.go`](https://github.com/shopmonkeyus/eds/blob/main/internal/tracker/tracker.go)     | Local SQLite database for state persistence |

## Driver Source Locations

| Driver     | Directory                                                                                                   |
| ---------- | ----------------------------------------------------------------------------------------------------------- |
| PostgreSQL | [`internal/drivers/postgresql/`](https://github.com/shopmonkeyus/eds/tree/main/internal/drivers/postgresql) |
| MySQL      | [`internal/drivers/mysql/`](https://github.com/shopmonkeyus/eds/tree/main/internal/drivers/mysql)           |
| SQL Server | [`internal/drivers/sqlserver/`](https://github.com/shopmonkeyus/eds/tree/main/internal/drivers/sqlserver)   |
| Snowflake  | [`internal/drivers/snowflake/`](https://github.com/shopmonkeyus/eds/tree/main/internal/drivers/snowflake)   |
| S3         | [`internal/drivers/s3/`](https://github.com/shopmonkeyus/eds/tree/main/internal/drivers/s3)                 |
| Kafka      | [`internal/drivers/kafka/`](https://github.com/shopmonkeyus/eds/tree/main/internal/drivers/kafka)           |
| EventHub   | [`internal/drivers/eventhub/`](https://github.com/shopmonkeyus/eds/tree/main/internal/drivers/eventhub)     |
| File       | [`internal/drivers/file/`](https://github.com/shopmonkeyus/eds/tree/main/internal/drivers/file)             |

## Internal Data Flow

1. **Enrollment**: Server registers with Shopmonkey API and receives NATS credentials
2. **Schema Import**: Implement the latest Shopmonkey database schema in the target
3. **Import (Optional)**: Import shop data into the target data sink using the Shopmonkey bulk export API
4. **Streaming**: Consumer subscribes to NATS JetStream `dbchange.*` subjects
5. **Batching**: Events accumulate based on `minPendingLatency`/`maxPendingLatency` settings
6. **Processing**: Driver's `Process()` called for each event, then `Flush()` for the batch
7. **Acknowledgment**: Successfully flushed events are ACK'd to NATS

## Event Structure

CDC events (`DBChangeEvent` in [`internal/dbchange.go`](https://github.com/shopmonkeyus/eds/blob/main/internal/dbchange.go)) contain:

| Field           | Type            | Description                           |
| --------------- | --------------- | ------------------------------------- |
| `ID`            | string          | Unique event identifier               |
| `Table`         | string          | Target table name                     |
| `Operation`     | string          | INSERT, UPDATE, or DELETE             |
| `Key`           | []string        | Primary key values                    |
| `Before`        | json.RawMessage | Previous record state (DELETE/UPDATE) |
| `After`         | json.RawMessage | New record state (INSERT/UPDATE)      |
| `Diff`          | []string        | Changed columns (UPDATE only)         |
| `ModelVersion`  | string          | Schema version identifier             |
| `Timestamp`     | int64           | Event timestamp (Unix millis)         |
| `MVCCTimestamp` | string          | Database MVCC timestamp               |

## Tracker Database

The tracker uses [BuntDB](https://github.com/tidwall/buntdb) (`eds-data.db`) to persist state as key-value pairs.

Common tracker keys:

- `table-export` - Last import timestamps per table
- `table-version-<table>` - Schema version for migration tracking
- `registry:<table>-<version>` - Cached schema definitions
