# Testing Guide

This document covers how to run tests for the EDS project.

## Test Types

EDS uses two types of tests:

1. **Unit Tests** - Test individual functions and components in isolation
2. **End-to-End (E2E) Tests** - Test complete driver workflows with real infrastructure
   - The file [`internal/e2e/e2e.go`](https://github.com/shopmonkeyus/eds/blob/main/internal/e2e/e2e.go) contains the operations that are executed by the end-to-end tests
   - Individual drivers provide functions that validate the success of the test operations via an interface

## Running Tests

The EDS CI/CD pipeline runs the unit tests and end-to-end tests automatically for each pull request. Developers can also run the tests locally using the process described below.

### Unit Tests

```bash
go test ./...
```

### End-to-End Tests

E2E tests use Docker to create containers for the target data sinks. Docker Desktop can be used for this purpose.

```bash
# Start docker containers for local data sinks
docker-compose up -d

# Run all e2e tests
make e2e

# Run specific e2e tests (by driver name)
E2E_TESTS=file make e2e
E2E_TESTS=postgres make e2e
E2E_TESTS="mysql postgres" make e2e
```

Note that tests using third-party services (e.g., Snowflake, S3) require credentials to be specified as environment variables. Refer to the specific test files for more information.

## Key Test Files

| File                                                                                                                     | Description              |
| ------------------------------------------------------------------------------------------------------------------------ | ------------------------ |
| [`internal/consumer/consumer_test.go`](https://github.com/shopmonkeyus/eds/blob/main/internal/consumer/consumer_test.go) | NATS consumer unit tests |
| [`internal/util/*_test.go`](https://github.com/shopmonkeyus/eds/tree/main/internal/util)                                 | Utility function tests   |
| [`internal/drivers/*/sql_test.go`](https://github.com/shopmonkeyus/eds/tree/main/internal/drivers)                       | SQL generation tests     |
| [`internal/e2e/e2e.go`](https://github.com/shopmonkeyus/eds/blob/main/internal/e2e/e2e.go)                               | E2E test framework       |
| [`internal/e2e/driver_*.go`](https://github.com/shopmonkeyus/eds/tree/main/internal/e2e)                                 | Per-driver e2e tests     |
