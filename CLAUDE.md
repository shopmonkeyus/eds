# EDS — Enterprise Data Streaming

> Audience: engineers working in this repo AND Claude Code running here (in IDE or CI). Both need the same guardrails.

## What this is

EDS is the **customer-installed streaming server**. An enterprise customer downloads a signed binary, pairs it to their Shopmonkey company, and it streams their data from our NATS into their own destination.

**It runs on the customer's infrastructure, not ours.** We cannot patch it, restart it, or roll it back. That single fact drives every rule below.

**[`README.md`](./README.md) is the customer-facing document** and covers download, usage, import, running the server, the data directory, monitoring, health checks, auto-update, deployment, and security. Keep it accurate — customers read it.

## ⚠️ Three facts to hold before you change anything

### 1. This repository is PUBLIC

`github.com/shopmonkeyus/eds` — 8 stars, 3 forks, open on the internet.

**Never commit** internal hostnames, cluster names, project IDs, credentials, customer names, or internal URLs. Every diff is also a disclosure review.

Internal-only guidance belongs in Notion, not here. This file is written to be safe in public.

### 2. Old versions run in the wild indefinitely

Auto-update exists, but customers pin, air-gap, and delay. Assume **every version ever released is still running somewhere**.

Backward compatibility is not a courtesy here. It is the contract.

### 3. Enterprise customers move money through this

From the internal release guide, carried forward verbatim because it is the most important sentence anyone has written about this repo:

> Before releasing a new version, double-check with any enterprise customers to be sure the new version will not break any of their current use-cases. Enterprise customers use EDS for processes involving customer money, so potential breaking changes will cause major issues.

## Core Invariants

Non-negotiable. If a later rule seems to conflict with one of these, the invariant wins.

1. **Public repo — no internal detail.** See above.

2. **Each driver is a customer contract.** There are eight destinations:

   `eventhub` · `file` · `kafka` · `mysql` · `postgresql` · `s3` · `snowflake` · `sqlserver`

   A customer has built pipelines, table schemas, and reports on the exact output shape of one of these. **Never change an existing driver's output shape.** Add a new option, defaulted off.

3. **Releases are PGP-signed.** The public key is committed as [`shopmonkey.asc`](./shopmonkey.asc), and the README tells customers to verify with it. Never publish an unsigned release. Never rotate that key without a customer communication plan — verification breaks the moment you do.

4. **The inbound event shape is owned by `changefeed`.** Produced by `GenerateSubject()` at `changefeed/pkg/types/types.go:289`:

   ```
   dbchange.{table}.{operation}.{companyId}.{locationId}.{visibility}.{partition}.{primaryKey}{suffix}
   ```

   `primaryKey` is never empty — `changefeed` returns an error rather than emit one — and it is **always a single token**, because a compound key is joined with `-`. Extra tokens after it come only from `{suffix}`, so a wildcard needs `>` at the tail for that reason.

   Two cautions if you go reading `changefeed` to check this:

   - There is a second builder, `Subject()` at line 180, which joins a compound key with **`.`** instead. It has **no production callers**. Do not treat it as the contract.
   - Both drop `Key[0]` on a compound key, on the assumption it is the region. **Whether that still holds is an open question** — see invariant 3 in `changefeed/CLAUDE.md`. It does not affect EDS today, because no table has a compound primary key.

   EDS consumes it. If it changes upstream, every customer's stream stops. Coordinate with `changefeed`; never patch a mismatch locally.

   **The subject filter is also where tenant isolation lives.** `internal/consumer/consumer.go` builds:

   ```go
   subject := "dbchange.*.*." + companyID + ".*.PUBLIC.>"
   ```

   Two guarantees are enforced by that one string: the paired company is pinned in the `companyId` position, and only `PUBLIC` models are received at all. **Widening either token is a data-breach class change, not a tuning change.** See invariant 7.

   Note the JSON is parsed by our own struct in `internal/dbchange.go`, not by a shared type — nothing upstream will fail to compile if `changefeed` retags a field. That struct currently ignores `region`, `sessionId` and `version`.

5. **Backward compatibility on config and state.** The data directory, session state, and config file are written by older versions and read by newer ones. A migration must handle every prior layout, not just the last.

6. **Never break the enrollment flow.** `enroll` pairs a server to a company. A customer who cannot re-enrol is offline until a human intervenes on their side.

7. **Tenant isolation is absolute.** A paired server receives exactly one company's data. There is no second chance on this class of bug — it is a data breach, not an outage.

8. **Never push to `main`** — all changes go through PRs.

## Stack

- **Go**, `cockroachdb/errors`
- **NATS** — event source
- **`ProtonMail/gopenpgp/v3`** — release signing and verification
- **`charmbracelet/huh`** — interactive setup TUI
- **`denisbrodbeck/machineid`** — machine fingerprint for pairing
- **Drivers**: `aws-sdk-go-v2` (S3), `azeventhubs`, `go-sql-driver/mysql`, Snowflake, PostgreSQL, SQL Server, Kafka
- **`BurntSushi/toml`** — config
- **goreleaser** — release packaging

## Project Structure

```
cmd/
├── server.go             # the server (1207 lines)
├── enroll.go             # pair to a Shopmonkey company
├── import.go             # bulk import (727 lines)
├── download.go, upgrade  # auto-update
├── driver_*.go           # one command per destination
├── publickey.go          # print the PGP public key
├── e2e.go, integrationtest.go, fork.go, version.go, root.go
internal/
├── consumer/             # NATS consumption (908 lines, 1138 lines of tests)
├── drivers/              # the eight destinations
├── driver.go             # the driver interface (456 lines)
├── importer/             # bulk import
├── registry/, tracker/, notification/, upgrade/, api/, osext/, util/
├── e2e/, integrationtest/
docs/
shopmonkey.asc            # PGP public key — customers verify against this
```

`internal/driver.go` defines the interface every driver implements. Read it before adding a destination.

## Releasing

Uses **goreleaser**.

```bash
git checkout main
git tag -a vX.Y.Z -m "description"
git push origin vX.Y.Z
goreleaser release --clean
```

Requirements: `goreleaser` installed, and a GitHub token with `write:packages`.

**Before every release:** confirm with enterprise customers that nothing breaks. See fact 3 above.

## Internal development

*This section is safe to publish. Anything that needs a credential or an internal hostname stays in Notion.*

### Reading customer logs

Customer log messages land in ClickHouse.

1. Find the server name by `companyId` in the `EnterpriseDataStreamServer` table.
2. Use the server ID to find sessions in `EnterpriseDataStreamSession`. Normal sessions last 24 hours; errored ones are shorter. There is an `errored` status field.
3. Use the session ID to find log messages in the ClickHouse monitoring cluster.

Access to the monitoring cluster needs a permission grant. Ask your manager.

### Local development

1. Set `SM_NATS_ACCOUNT_SEED` in your environment. Ask a team member.
2. Start the local backend and wait for the database seed to finish.
3. In the Ops app, select company `Multi-Shop Corp`, then **Tools**, and enable **Enterprise Data Streaming** in company-level entitlements.
4. In the HQ app, set up your EDS server as usual.
   - If EDS does not appear under integrations, log out and back in. The entitlement is read at login.
5. In the shop app, switch into `Multi-Shop LLC Main Street` or a child location of Multi-Shop Corp, then make a change such as creating an estimate. It should stream to your sink.
   - **Shoppy is not part of Multi-Shop Corp.** If you stay in Shoppy you are not testing EDS.

## Testing

```bash
go test ./...
```

`internal/e2e/` and `internal/integrationtest/` hold the heavier suites, with `e2e` and `integrationtest` commands to drive them. `consumer_test.go` is larger than `consumer.go` — treat it as the specification.

## Quality Gate

Before opening a PR:

1. `go build ./...` and `go test ./...` pass
2. **No internal hostname, project ID, cluster name, credential, or customer name in the diff**
3. No existing driver output shape changed
4. Config and state changes handle every prior layout
5. A new driver implements the full `internal/driver.go` interface and has tests
6. The README is updated if customer-facing behaviour changed

## Stop and Confirm

Ask a human before you:

- Change any existing driver's output shape
- Change the `internal/driver.go` interface
- Change enrollment or pairing
- Change the config file or data directory layout
- Change anything in `internal/upgrade/`
- Touch `shopmonkey.asc` or the signing flow
- Cut a release

Every one of these reaches machines we do not control, serving customers who move money through this.

## Technology Guardrails

- Do not add a driver without an owner. Eight destinations is already a lot of surface.
- Do not add a dependency that will not cross-compile. Customers run this on Linux, macOS, and Windows.
- Do not grow the binary needlessly. The README documents building a smaller binary because size matters to customers.
- Do not add telemetry that was not agreed with the customer.

## Known drift

Recorded 2026-08-21. Fix as you touch.

1. **The internal release guide points at `shopmonkeyus/eds-server`.** This repo is `shopmonkeyus/eds`. The old name is stale.
2. That guide also lives in the frozen `engineering` repo. Its content is carried into this file, and the original should point here.
3. The ownership sheet lists EDS under **Andretti**, moved from the retired Lotus squad. Confirm that is still right.

## Runbooks

Runbooks live in the **Notion Runbooks database**.

- "NATS EDS consumer leader matches stream leader leading to delay in processing" — the known EDS-specific entry
- "Tier 1 Handoff", "NATS Alerts" — general

A customer-side EDS failure is not a runbook we can execute. It becomes a support conversation. Keep the README's monitoring and health-check sections accurate, because that is what the customer reads first.
