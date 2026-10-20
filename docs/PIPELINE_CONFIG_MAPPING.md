<a id="固定流水线配置映射"></a>

# Pinned pipeline configuration mapping

<!-- language-switch -->
[English](PIPELINE_CONFIG_MAPPING.md) | [简体中文](PIPELINE_CONFIG_MAPPING.zh-CN.md)

English is the authoritative version.


The reference is Supabase ETL `c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4`,
`crates/etl-config/src/shared/pipeline.rs` and `replicator.rs`. MoonCDC retains its
own JSON/YAML structure, not direct compatibility with upstream files. Defaults
below are CLI defaults; library compatibility differences are listed separately.

The scope is the [PostgreSQL → ClickHouse library and CLI](RELEASE_SCOPE.md).
PostgreSQL state/schema stores hold recovery metadata, not destination data.

| Pinned ETL field | MoonCDC field / actual behavior |
|---|---|
| `id`, `publication_name` | `id`, `publication`; nonempty ID generates exclusive state/schema namespaces |
| `pg_connection` | `source`; host/hostaddr/port/database/user/password/security/keepalive plus timeout; validates TLS, loopback plaintext and integer ranges |
| `store_pg_connection` | `state.connection`, reusing source when omitted; configurable state.schema/table |
| `replication_slot.failover` | Same nested field; false by default, supported on PG17/18, validated at startup and retained during recreation |
| `batch` | Enabled by default: ratio=0.2, max_bytes=33554432, max_fill_ms=10000; ClickHouse main/private WAL uses bounded event batches; null restores whole transactions; acknowledgement still follows Commit and target barrier |
| `table_error_retry_delay_ms` | `table_retry_delay_ms`, default 10000; CLI range 1000–86400000 |
| `table_error_retry_max_attempts` | `max_table_retries`, default 5, positive Int |
| `max_table_sync_workers` | `copy_concurrency`, default 4, range 1–256 |
| `max_copy_connections_per_table` | `copy_connections_per_table`, default 4, range 1–256; product of both limits ≤256, checked by division before comparison to avoid overflow |
| `memory_refresh_interval_ms` | Same name, default 100, positive Int; shared sampler for pressure and batching |
| `memory_backpressure` | Same name; activate=0.85/resume=0.75, null disables; pauses source reads while drain/ack/heartbeats continue |
| `table_sync_copy` | Same name, four OID-selection policies; empty/skipped copies still initialize target and apply durability barriers |
| `table_sync_monitor_refresh_interval_ms` | Same name, default 10000, positive Int; samples copy slots and main/private WAL source/standby replay LSN independently; sample failures only increment counters |
| `invalidated_slot_behavior` | Same name, error/recreate, default error; durable full-table recreation, never skipping lost WAL |
| `run_source_migrations` | Same name, default true; health/metrics bind failures reject startup before migration |
| `destination` | ClickHouse only: url/user/password/database/engine, both engines; other targets rejected by core [CLI](REPLICATOR.md). Configuration remains MoonCDC's own object syntax |
| `health` | Separate optional probe; metrics use separate default port 9000 |
| `sentry.dsn`, `supabase.project_ref/api_url/api_key` | Optional CLI reporting: persist terminal table state first, suppress TimedRetry, report final failure, and preserve original errors if reporting fails; bounded local-peer and JSON/YAML CLI evidence, with service/SDK gaps in [validation](VALIDATION.md) |
| `supabase.configcat_sdk_key` | Optional initialization and scoped polling: default Global/60s/30s, project user, V6 preferences/ETag, cached data on failure, cancel/join on exit; controlled JSON/YAML CLI history in [validation](VALIDATION.md). Pinned entrypoint performs no evaluation; full SDK/real CDN acceptance remains incomplete |

Library `run_pipeline` retains copy_concurrency=2, copy_connections_per_table=1,
opt-in batch, source_migrations=false and the compatible positive retry-delay
contract. CLI 4/4 and enabled batching increase connections: each table also needs
exporter, metadata and temporary monitoring connections. The product limit of 256
counts only readers. Environment overrides use identical validation and never echo secrets.

MoonBit Int fields are limited to 2147483647; full upstream u64 configuration is
not supported. Full ConfigCat/Sentry SDK and real-service acceptance remain gaps.
These optional integrations and other destinations do not gate this version, but
retained features still require correct close, redaction and exit behavior. This
mapping preserves differences in library acknowledgement/protocol semantics, CLI
syntax, defaults and type widths; it covers only the runnable settings above.
