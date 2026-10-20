<a id="当前交付范围"></a>

# Release scope

<!-- language-switch -->
[English](RELEASE_SCOPE.md) | [简体中文](RELEASE_SCOPE.zh-CN.md)

English is the authoritative version.


Confirmed on 2026-10-15: MoonCDC focuses on PostgreSQL → ClickHouse, providing an
embeddable MoonBit library and standalone replicator CLI. Both have equal support
and share the core; initial source versions are PG17/18. This document defines the
version's boundaries. [Feature mapping](FEATURE_PARITY.md),
[release review](COMPLETION_CHECKLIST.md) and [validation](VALIDATION.md) describe
implementation, pending gates and evidence. Narrowing scope is not validation of
new source code.

<a id="产品与参考边界"></a>

## Product and reference boundaries

- Core module: `pangbit/mooncdc`, native backend, one-way PostgreSQL → ClickHouse.
- Standalone tool: `cmd/replicator`, JSON/YAML and environment configuration,
  offline `--check`, signal drain, health and metrics. It calls the library's
  `run_pipeline`; it does not implement a separate replication/recovery engine.
- Primary reference: [Supabase ETL](https://github.com/supabase/etl/tree/c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4),
  commit `c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4`. Alignment covers selected
  sources, ClickHouse, state/recovery and runtime semantics. pglogrepl and Debezium
  supplement protocol and failure scenarios.
- BigQuery, Snowflake, DuckLake and Iceberg are outside this delivery, default
  dependencies, builds, configuration and acceptance. Existing implementations
  remain in unpublished extensions or Git history; DuckLake is a roadmap item,
  not implemented. Extensions can be maintained independently; failures or missing
  real-cloud evidence do not block this version. See [validation](VALIDATION.md)
  for isolation progress.
- No PostgreSQL → PostgreSQL, reverse/bidirectional synchronization or management
  platform. PostgreSQL StateStore/SchemaStore stores recovery metadata, not destination data.

<a id="库与-cli-的公共边界"></a>

## Shared library and CLI boundaries

| Layer | Responsibility | Boundary and reference |
| --- | --- | --- |
| Public library | `ConnectionConfig`, `ClickHouseDestination`, `run_pipeline`, durable StateStore/SchemaStore, `ApplyControl` | The host composes connections, destination, state and control scopes; errors, stop and resource release are observable. See [API](API.md) |
| Shared core | pgoutput v1, per-table snapshots, main/private WAL handover, schema versions, destination barriers, checkpoints, reconnection and promotion proof | Library and CLI must share acknowledgement/recovery algorithms. Public runtime entry is `state_runtime.mbt`; main implementation is in `pipeline.mbt`, `pipeline_copy.mbt`, `subscription.mbt` and `clickhouse_*.mbt` |
| CLI adapter | Parsing, defaults, object construction, signals, probes/metrics and exit codes | `ReplicatorConfig::pipeline` in `cmd/replicator/main.mbt` calls public `run_pipeline`. See [configuration mapping](PIPELINE_CONFIG_MAPPING.md) |
| Low-level compatibility | `subscribe`, `copy_snapshot`, `Destination`, original events and stable IDs | Composable APIs remain; generic interfaces do not expand built-in connector support. Standalone v2 keeps its existing boundary, without a v2 pipeline promise |

Equal support requires the same business capabilities and failure semantics, not
identical configuration, lifecycle wrapping or defaults. CLI COPY defaults are 4/4,
batch and pressure control are enabled, and source migrations default to true.
Library compatibility defaults remain 2/1, opt-in batch/pressure and false source
migrations. Configure comparison cases explicitly and record differences; these
compatibility defaults do not change in this work.

The host must protect configuration, keep namespaces and source/destination
identities stable, request stop and await drain. The CLI performs the corresponding
process duties. Source migration permissions and schema/state configuration follow
the API contract. Unsupported schemas and unsafe recovery must fail explicitly.

<a id="首批验收顺序"></a>

## Initial acceptance order

These criteria apply to both library and CLI. The business matrix includes at
least PG17/18 and both supported ClickHouse engines; CLI also covers JSON/YAML,
environment overrides and offline rejection. Fault combinations run in batches;
record entrypoint, version, engine, fault and actual result. Disabled bodies and
ordinary test totals cannot substitute for these records.

| Order | Required behavior | Reusable entrypoint/evidence |
| --- | --- | --- |
| 1 | Concurrent I/U/D during a nonempty initial snapshot; no omissions across snapshot/WAL handover; no unjustified reset of completed tables; recoverable copy failures | `tools/parallel-copy-test.mbtx`, `tools/replicator-test.mbtx`; parallel COPY [validation](VALIDATION.md) |
| 2 | Updates, deletes, NULL/TOAST, primary key/replica identity and type mapping meet their contracts; verify current-state views and operation history separately | `tools/clickhouse-test.mbtx`; CLI/ClickHouse, type and snapshot/WAL [validation](VALIDATION.md) |
| 3 | Replay after interruption between destination write and checkpoint/feedback; explain stable event IDs and duplicate boundaries for both engines, without silent omissions | `tools/checkpoint-crash-test.mbtx`, `tools/clickhouse-crash-test.mbtx`; [durability contract](API.md#destinations-and-durability) |
| 4 | New-process recovery retains progress, rejects corruption/identity mismatch/unrecoverable positions; explicit invalidated-slot recreation has durable full-table intent | `tools/pipeline-crash-test.mbtx`, `tools/slot-recreate-process-test.mbtx`; process recreation [validation](VALIDATION.md) |
| 5 | Fence the old writer before promotion; prove ancestry, fork, write horizons and state owner, including COPY-time transitions and owner loss | `tools/timeline-promotion-test.mbtx`, `tools/cli-active-promotion-test.mbtx`; active COPY promotion [validation](VALIDATION.md) |
| 6 | ClickHouse timeout, 503, unknown write outcome, cancellation and restart never acknowledge early; normal stop drains and destination failure retains a recovery point | `tools/clickhouse-test.mbtx`, `tools/clickhouse-crash-test.mbtx`; cancellation [validation](VALIDATION.md) |
| 7 | Supported column DDL composes with snapshot/CDC, restart and publication projection; persist schema and reject unknown/unsupported changes | [Schema/ClickHouse scope](API.md#ddl-schema-versions), state-schema [validation](VALIDATION.md); fixed source migration fixture in `integration/reference/etl/` |

Delivery is at least once. `Accepted` does not authorize acknowledgement; only a
valid Commit plus a destination durability barrier covering a contiguous prefix
allows checkpoint/feedback advancement. v1 batches may expose committed-transaction
fragments before the Commit frame, without source-transaction atomic visibility
at the destination. ReplacingMergeTree current state and MergeTree history are
verified separately; cross-system exactly-once is not promised. Promotion recovery
is not automatic election or HA routing.

<a id="交付门槛与版本绑定"></a>

## Delivery gates and version binding

The supported scope is the PostgreSQL → ClickHouse library and CLI. BigQuery,
Iceberg, Snowflake and cloud acceptance are outside the release tree. JSON/YAML,
durable state, snapshot/WAL handover, target barriers, reconnection and supported
promotion/recreation retain API contracts; automatic election and exactly-once
remain outside scope.

Acceptance distinguishes product, compiler, tooling, environment and unexecuted
steps. See [validation](VALIDATION.md) for historical Linux results, the 256-task
compiler budget, 264 observed peak and one-time 512 diagnostic ceiling. Those
results do not establish a final release branch's remote CI or authorize host
limit changes. See [testing](TESTING.md) for current commands and independent
maintenance, and [release review](COMPLETION_CHECKLIST.md) for gates.
