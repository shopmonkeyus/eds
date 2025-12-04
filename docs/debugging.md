# Debugging Guide

This document provides tips and techniques for debugging the EDS server internals.

> For health checks, metrics endpoints, and monitoring info, see the main [README](../README.md#monitoring-the-server).

## Logging

### Log Levels

```bash
# Normal logging (INFO level)
./eds server --url "..."

# Verbose logging (TRACE level)
./eds server -v --url "..."

# Silent mode (ERROR only)
./eds server -s --url "..."
```

### Log File Locations

Logs are stored in `<data-dir>/<session-id>/logs/`:

```bash
# List log files
ls data/*/logs/

# Tail recent logs
tail -f data/*/logs/eds-*.log
```

## Testing Locally with File Driver

The file driver is useful for local testing without external dependencies:

```bash
# Create output directory
mkdir -p ./test-output

# Run server with file driver
./eds server -v --url "file://./test-output"

# Check output files
ls ./test-output/
cat ./test-output/order/*.json
```

## Attaching Debugger

The consumer and driver run in a separate process from `server`, so extra steps are required to attach a debugger:

1. Build EDS with the following command:
   ```bash
   go build -gcflags="all=-N -l" -ldflags="-w=0" -o eds-debug .
   ```
2. Start the EDS server using the `eds-debug` binary
3. Find the process ID of the `fork` process:
   ```bash
   ps aux | grep 'eds fork'
   ```
4. Add this launch profile to VS Code (`.vscode/launch.json`):
   ```json
   {
     "name": "Attach to Process",
     "type": "go",
     "request": "attach",
     "mode": "local",
     "processId": "<fork process ID>"
   }
   ```
5. Use the VS Code debugger to attach to the process. For other IDEs, refer to the documentation on how to attach the Go debugger to a running process

## Tracker Database

The tracker stores local state using [BuntDB](https://github.com/tidwall/buntdb) in `data/eds-data.db`. This file contains:

- Cached database schema information
- IDs of inserted records (used by Snowflake driver to prevent duplication)

The file is human-readable and uses a Redis-like format. Inspecting it can help debug schema synchronization problems or duplication issues.

> **Warning:** Deleting this file resets EDS state and triggers a schema re-import, which may cause data loss in the target data sink for database-like drivers. Editing the file directly is not supported.
