# MoonCDC

<!-- language-switch -->
[English](README.mbt.md) | [简体中文](README.zh-CN.md)

English is the authoritative version.


A MoonBit PostgreSQL → ClickHouse CDC library and standalone replication tool.
The library and CLI have equal support and share the `run_pipeline` core. Initial
source acceptance covers PG17/18; see the [release scope](docs/RELEASE_SCOPE.md).
The low-level subscription uses native `pgoutput` protocol v1, delivers complete
committed transactions, and provides explicit acknowledgement, durable checkpoints
and bounded reconnection.

**0.1.1 patch candidate: a native PostgreSQL → ClickHouse library and CLI.**
It fixes ClickHouse exception responses, snapshot cancellation ordering and
adjacent-transaction replay boundaries; see the [changelog](CHANGELOG.md).
Version 0.1.0 was published on 2026-10-21. See
[testing](docs/TESTING.md) for validation methods and source-bound evidence.
Delivery is at least once; applications must handle replay.

The fixed product inputs passed 4 Linux PG17/18 library cells and 8 CLI
cells on 2026-10-20. The compiler window's 256-task budget is distinct from the
database matrix: the diagnostic observed a kernel peak of 264 under a temporary
512 ceiling and 384 stop watermark, then restored 256. These are fixture budgets,
not product requirements, permanent permission for 512 tasks, or evidence that a
final release commit passed remote CI. See the version binding in testing.

<a id="安装与使用"></a>

## Installation and usage

Query the candidate version with `moon view pangbit/mooncdc@0.1.1`.
After publication, install with `moon add pangbit/mooncdc@0.1.1` and verify the
resolved version. Until then, the published version is available with
`moon add pangbit/mooncdc@0.1.0`; it does not contain the three patch fixes.
To try the candidate before publication, run the repository examples or
follow [releasing](docs/RELEASING.md) to validate an extracted source-package
consumer. A local consumer pass does not establish registry publication.

Set `preferred_target = "native"` in `moon.mod` and import the package in the
consumer's `moon.pkg`. The examples use the default alias `@mooncdc`:

```moonbit nocheck
///|
import {
  "pangbit/mooncdc",
}
```

```mbt check
///|
test "parse a PostgreSQL WAL position" {
  let position = @mooncdc.Lsn::parse("16/B374D848")
  assert_eq(position.to_string(), "16/B374D848")
  assert_true(position > @mooncdc.Lsn::parse("0/FFFFFFFF"))
}
```

A host embedding `run_pipeline` can observe worker activity and health. `ready`
means running without a detected stall; it does not mean the initial copy is
complete or WAL has caught up.

```mbt check
///|
test "pipeline health before startup and during shutdown" {
  let control = @mooncdc.ApplyControl()
  assert_true(control.health().live)
  assert_false(control.health().ready)
  assert_eq(control.activities().length(), 0)
  control.stop()
  assert_eq(control.health().status, "stopping")
}
```

Wrap pipeline initialization and execution in
`with_health_server(control, () => { ... })` to enable HTTP probes. The default
port is 9001, exposing `/livez` and `/readyz`; no listener starts unless called.
The host must call `control.stop()` and await drain. See
[health contracts](docs/API.md#limits-and-diagnostics).

`run_pipeline` copies all initial rows by default. Use `table_sync_copy=SkipAllTables`,
`IncludeTables([oid, ...])` or `SkipTables([oid, ...])` to select initial table rows.
Skipped tables still receive target initialization and schema setup, and still
join later WAL. This is not a streaming filter. Restart does not refill already
completed tables; see the [pipeline contract](docs/API.md#persistent-table-pipeline).

`with_state_schema_store` persists schema history in a file or PostgreSQL
StateStore. Use separate namespaces for the pipeline's StateStore and schema
store, and pass the same `schemas` to the destination and `run_pipeline`.
See [state stores](docs/API.md#external-state-stores).

A typical consumer follows; complete runnable programs are in
[examples](examples/README.md).

```mbt nocheck
let config = @mooncdc.ConnectionConfig::new(
  host="127.0.0.1", port=55418, user="cdc",
  password=password_from_environment, database="postgres",
  security=@mooncdc.LocalPlaintext,
)
@mooncdc.subscribe(
  config, slot="my_slot", publication="my_publication",
  start=@mooncdc.Resume, checkpoint_path="./state/checkpoint",
  sub => {
    for ;; {
      let tx = sub.next()
      persist_transaction_idempotently(tx) // 业务内容与 tx.id 一起持久化
      sub.ack(tx)                          // 之后才允许推进源端反馈
    }
  },
)
```

For the first run, use the explicit LSN returned when the replication slot is
created: `At(Lsn::parse("0/…"))`. For initialization from a snapshot, use
`copy_snapshot` to obtain the consistent snapshot and slot, write bounded batches
durably, then continue with `AfterSnapshot(position)`; see
[initial snapshots](docs/API.md#initial-snapshot).
You can also call `position.save_checkpoint(path)` and continue in a new process
with `Resume`. `Resume` requires an existing checkpoint and explicitly rejects
corruption, identity mismatches and unrecoverable positions. It never silently
falls back to the latest WAL position.

<a id="保证与边界"></a>

## Guarantees and boundaries

- Only committed transactions are delivered, preserving event order, table/type
  information, transaction IDs and WAL positions.
- `Null`, `Text` and `UnchangedToast` represent SQL NULL, text and unchanged external
  fields. `subscribe(..., binary=true)` requests binary transfer; `Binary(Bytes)`
  retains original bytes for caller decoding with column OIDs. Text remains the
  default, and binary mode can still contain fallback text fields.
- Standalone subscriptions may opt into `protocol=V2Streaming`, buffering
  uncommitted segments until commit. See [API](docs/API.md) for resource and rollback
  contracts. The pipeline continues to use protocol v1.
- `ack` advances only across a contiguous completed prefix. The checkpoint file,
  atomic replacement and parent directory must be synced before feedback changes.
  Received position is not business durability.
- `tx.id` identifies a transaction; `tx.event_id(index)` identifies a data change.
  Repeated metadata does not change data-event IDs.
- `messages=true` supports transaction-bound logical messages and a separate
  callback for nontransactional messages. Payload bytes remain unchanged;
  `schema_snapshot` interprets Supabase ETL DDL payloads. Pipeline schema history
  can be persisted with `run_pipeline(..., schema_store=store)`; see API.
- Custom `Destination` implementations integrate through `copy_snapshot_to` and
  `sub.apply_to`. `Accepted` never authorizes acknowledgement. A durable barrier
  is required for checkpoints/feedback, including empty flushes and normal drain.
- Background heartbeats continue. Frames, transactions, metadata and outstanding
  queues have limits; exceeding them closes the connection without acknowledging.
- SCRAM-SHA-256 and verified remote TLS are supported. Plaintext accepts only
  numeric loopback addresses. Initial passwords are printable ASCII; non-ASCII
  passwords requiring SASLprep are explicitly rejected.
- `run_pipeline` maintains durable per-table states, concurrent independent
  snapshots, failed-copy restart and catch-up. Completed tables retain progress;
  full queues apply backpressure. Newly published tables are discovered and copied
  upon their first WAL event.
- `Value.decode_text(oid)` returns typed Cells, supporting exact numbers,
  date/time, JSON, UUID, bytea and one-dimensional arrays. SQL NULL, JSON null and
  unchanged TOAST remain distinct. Unknown scalars retain text.
- `StateStore` supports file and PostgreSQL backends for pipeline/subscription
  checkpoints and destination metadata. Table-copy errors persist and support
  timed or manual retries. See [API](docs/API.md).
- `ClickHouseDestination` supports ReplacingMergeTree current-state views and
  MergeTree event histories, with snapshot/CDC writes, column DDL and durable
  recovery. See [supported scope](docs/API.md#clickhouse-destination).
- This module supports PostgreSQL → ClickHouse only. It excludes BigQuery,
  Snowflake and Iceberg. DuckLake is a roadmap item, not an implemented feature.
- Automatic primary election and two-phase transactions are not provided. Schema
  support is destination-specific. Cross-system exactly-once delivery is not promised.

`replica identity` controls old tuple availability. `FULL` supplies more old
columns; default primary-key identity may supply only keys or no old tuple.
`UnchangedToast` must not overwrite a business value with NULL. See
[API and support](docs/API.md).

<a id="本地验证"></a>

## Local validation

Known validation limitation: one macOS release ordinary-test run of the merged
source reported `TimeoutError` in copy-exit cleanup and reconnect retry scenarios.
Exactly one retry of that job passed with source, toolchain, commands and timeouts
unchanged. The cause remains unconfirmed; this neither establishes nor fixes a
product defect. See [validation](docs/VALIDATION.md#github-ci-at-the-reviewed-source)
for deadlines, results and source binding.

Use a dedicated Docker Compose project, binding database ports to `127.0.0.1`:

```sh
moon update
docker compose -p mooncdc-test -f integration/compose.yaml up -d --wait
moon run tools/setup-db.mbtx pg18
moon run tools/setup-db.mbtx pg17
moon check --target native --deny-warn
moon test --target native
moon run tools/setup-tls.mbtx pg18
moon run tools/setup-tls.mbtx pg17
MOONCDC_TEST_PORT=55418 MOONCDC_TEST_CA=.test-artifacts/tls-pg18/server.crt moon run tools/live-test.mbtx
MOONCDC_TEST_PORT=55417 MOONCDC_TEST_CA=.test-artifacts/tls-pg17/server.crt moon run tools/live-test.mbtx
moon run examples/index
moon run examples/cache
moon run tools/recovery-test.mbtx
moon run tools/checkpoint-crash-test.mbtx
```

Without `MOONCDC_TEST_PORT`, real database test bodies do not run; ordinary tests
passing does not prove live validation. Setup scripts initialize isolated fixtures,
not existing business databases. Fault tests terminate this project's walsenders
or example child processes. See [TESTING](docs/TESTING.md) for commands, TLS and cleanup.

<a id="文档与维护"></a>

## Documentation and maintenance

See the [standalone replicator](docs/REPLICATOR.md) for entrypoints and configuration.

- [Examples](examples/README.md) · [API](docs/API.md) · [Testing](docs/TESTING.md)
- [Operations and WAL monitoring](docs/OPERATIONS.md) · [Releasing](docs/RELEASING.md)
- [Development](docs/DEVELOPMENT.md) · [Contributing](CONTRIBUTING.md) · [Changelog](CHANGELOG.md)
- [Dependencies and protocol provenance](THIRD_PARTY.md) · [Security](SECURITY.md)

Apache-2.0; see [LICENSE](LICENSE).
