<a id="运行指标"></a>

# Runtime metrics

<!-- language-switch -->
[English](METRICS.md) | [简体中文](METRICS.zh-CN.md)

English is the authoritative version.


`ApplyControl::prometheus` and `with_metrics_server` expose control-isolated
Prometheus 0.0.4 text. The CLI listens separately on port 9000 by default; see
[replicator](REPLICATOR.md) for configuration and shutdown. Reading metrics does
not change pipeline state or write checkpoints.

Optional global labels are `pipeline_id`, `destination` and `project`, each up to
1024 UTF-8 bytes. The CLI sets the first two and can set project from
`supabase.project_ref`. Built-in dimensions are fixed workers, paths, states and
memory sources; tables, slots, errors and connection strings do not create series.
Counters live with the control and continue through recovery, replay and recreation;
they are not business deduplication counters. Event counters/histograms appear
only after observations; disabled features do not fabricate zero samples.

| Metric | Type and meaning |
|---|---|
| `mooncdc_live`, `mooncdc_ready`, `mooncdc_lifecycle{state}` | Live gauges for health and initializing/running/stopping, using the probes' stall decision |
| `mooncdc_subscriptions_active`, `mooncdc_outstanding_transactions`, `mooncdc_queued_transactions` | Current registered subscriptions, transactions not contiguously acknowledged, and queued deliveries per worker |
| `mooncdc_observed_wal_lag_bytes` | Maximum observed server-end minus durable bytes across active subscriptions; not current-source lag from periodic SQL |
| `mooncdc_source_lsn_monitors_active{worker_type}` | Independent SQL monitors in main apply and private table_sync catch-up, including monitors without a valid sample |
| `mooncdc_current_wal_received_lag_bytes`, `mooncdc_current_wal_durable_lag_bytes` | Most recent successfully sampled source LSN minus received complete-commit/durable-after-barrier position; maximum per worker, negatives clamped to zero, emitted only with valid samples |
| `mooncdc_received_durable_gap_bytes`, `mooncdc_source_lsn_sample_age_seconds` | Maximum received-minus-durable gap and maximum monotonic age of the latest valid scope sample; stale/missing samples do not prove catch-up |
| `mooncdc_source_monitor_samples_total`, `mooncdc_source_monitor_failures_total` | Accepted independent LSN samples and connection/query/shape/LSN-regression failures, aggregated by worker; failure authorizes neither ack nor source retry |
| `mooncdc_tables{state}` | pending/copying/catchup/ready/errored counts in active durable pipeline state; removed on exit |
| `mooncdc_table_copy_current_wal_lag_bytes`, `mooncdc_table_copy_monitors_active` | Maximum sampled current-source LSN minus initial consistent position per copy slot, and active monitors with samples; removed when monitoring ends |
| `mooncdc_slot_invalidations_total{worker_type="table_sync"}`, `mooncdc_copy_monitor_failures_total` | Copy-slot disappearance/invalidation found by independent sampling, and general connection/query sampling failures |
| `mooncdc_replication_messages_total`, `mooncdc_replication_payload_bytes_total` | Received CopyData frames and payload bytes, including keepalive; excludes outer frame headers and TCP/TLS overhead |
| `mooncdc_replication_stream_ends_total{message_type,worker_type}` | Successfully parsed command_complete/copy_done stream endings; not acknowledgements, business commits or successful recoveries |
| `mooncdc_transactions_received_total`, `mooncdc_changes_received_total` | Decoded Commits and Changes, including replay; event path counts each Change before Commit, whole-transaction path counts at Commit |
| `mooncdc_transactions_written_total`, `mooncdc_changes_written_total` | Successful whole-transaction calls, successful event-path Commit batches, and Changes passed successfully; includes Accepted. Main apply counts before projection, legacy private whole-transaction path after projection, new private event path before projection |
| `mooncdc_wal_batches_written_total` | Event-batch calls returning successfully, including metadata, empty Commit projections and transaction fragments; not commit/ack counts |
| `mooncdc_transactions_durable_total` | New contiguous transactions after main subscription checkpoint storage; private WAL counts after target barrier/handover check returns, without durable main handover until main-stream position is reached; not equivalent to written or Ready |
| `mooncdc_checkpoint_updates_total` | Successful subscription checkpoint updates, not source feedback frames |
| `mooncdc_copy_rows_received_total`, `mooncdc_copy_rows_written_total` | Rows accepted by snapshot callbacks and successful destination calls; written need not be durable |
| `mooncdc_copy_rows_completed_total`, `mooncdc_table_copies_completed_total` | Snapshot rows/tables accumulated after successful TableEnd flush; WAL catch-up/handover may remain incomplete |
| `mooncdc_copy_planned_partitions`, `mooncdc_copy_effective_partitions` | Histograms of planned partitions and actual CTID cursor tasks per table_sync, without table labels. Serial compatibility uses 1 per logical table, parallel physical empty tables use 0; skipped reads emit no sample |
| `mooncdc_copy_partition_estimated_blocks` | Planned estimated physical block span for each parallel task, not actual scanned blocks; open-ended final range still uses the estimate |
| `mooncdc_copy_partitions_completed_total`, `mooncdc_copy_partition_seconds`, `mooncdc_copy_partition_rows` | Task count, elapsed time and rows after cursor reads, callbacks and CLOSE succeed. Callbacks may return Accepted; later TableEnd flush failure retains these samples. Failure/cancellation before CLOSE does not count completion |
| `mooncdc_schema_cleanup_attempts_total`, `mooncdc_schema_cleanup_errors_total` | Main-apply checks of a nonempty cleanup set and caught non-cancellation failures; failure retains candidates and existing checkpoint |
| `mooncdc_schema_cleanups_total`, `mooncdc_schema_cleanup_tables_total`, `mooncdc_schema_cleanup_pruned_versions_total` | Successful prune calls, supplied retention-bound tables and actually removed versions; a prune can remove zero versions |
| `mooncdc_schema_cleanup_skipped_tables_total` | Evaluated owned tables without target retention boundaries; unowned candidates remain without being counted |
| `mooncdc_worker_errors_total`, `mooncdc_worker_retries_total{trigger}` | Failures caught by apply/table workers and actually scheduled timed/manual retries, not all SQL errors |
| `mooncdc_slot_recreation_attempts_total` | Explicit slot recreation entries, including failures; not completion count |
| `mooncdc_timeline_recovery_attempts_total` | Promotion reentries scheduled after main-worker timeline change, old-worker close and flush; subsequent proof can fail, so not completion count |
| `mooncdc_memory_used_bytes`, `mooncdc_memory_total_bytes` | Shared sampler usage/capacity from system/process_cgroup/root_cgroup, not process heap or RSS |
| `mooncdc_memory_backpressure_active`, `mooncdc_memory_backpressure_transitions_total{direction}` | Current pressure pause and sampler-published activate/resume transitions |
| `mooncdc_batch_target_bytes`, `mooncdc_batch_registered_slots` | Current governor heuristic byte target and registered capacity slots, not a hard memory cap |
| `mooncdc_destination_write_seconds{worker_type,replication_path,status}` | COPY/CDC destination writes returning successfully; excludes later separate flush |
| `mooncdc_private_wal_write_and_flush_seconds` | Successful write plus required flush in legacy whole-transaction private WAL; new event path uses destination-write histogram without later separate flush |
| `mooncdc_table_copy_seconds` | Successful TableBegin-to-TableEnd barrier duration, excluding private WAL catch-up |
| `mooncdc_postgres_query_seconds{pool}` | Seconds once per simple query/adaptive FETCH ending in success, error or cancellation; pool is only out_of_band/postgres_store. Multi-statement SQL counts once; FETCH includes consume/callback/pressure waits, without duplicate per-row/flush counting |

The eight real CLI → ClickHouse cells observed both query pools and COPY cursor
completion metrics: four default readers handled 16 partitions; TERM/RECREATE
completed 1001/1003 rows. Five non-INIT phases required positive counts for both
pools; INIT may precede connection establishment. See CLI [validation](VALIDATION.md).
This is not acceptance of every query path or cloud service.

Seconds use boundaries 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10.
Partition-count boundaries are 0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024;
partition rows use 0, 1, 10, 100, 1000, 10000, 100000, 1000000; estimated blocks use
0, 1, 8, 64, 512, 4096, 32768, 262144, 2097152, 16777216, 134217728, 1073741824, 4294967295.
Every family adds `+Inf`, cumulative buckets, sum and count. Counters and buckets
saturate at UInt64 maximum.

Automatic wire/checkpoint/live-state observation is scoped to `run_pipeline`.
Standalone `Subscription::apply_to(control=...)` supplies destination-write metrics,
but public `subscribe` has no control argument and does not establish wire/checkpoint
coverage. Private event WAL registers subscription diagnostics; legacy private
whole-transaction WAL supplies only current-source LSN and received/durable
observations, without queue/observed-lag gauges. Independent source sampling uses
`table_sync_monitor_refresh_interval_ms`, continuing while source reads pause or
target writes/flushes block. Primary queries current WAL; standby queries replay LSN.
Each monitor owns at most one bounded query connection, samples after a fixed delay
following completion, and cancels/joins queries on scope exit. Regressing LSN,
NULL or failure retains old samples/age, without checkpoint, feedback or business
activity updates. Pipeline exit releases persistent store/monitor/batch/subscription
references; cumulative events remain readable.

Query observation covers pipeline startup/reentry, dynamic discovery, main-stream
reconnect, snapshot exporters/metadata/serial and parallel readers, and independent
copy-slot/current-source LSN sampling. CLI passes the same control to state/schema
stores and source migrations. Library `PostgresStateStore::new(..., control=...)`
and `run_source_migrations(config, control=...)` opt in; omission emits nothing.
Public standalone subscribe/copy_snapshot lack control and do not establish SQL
coverage. Timing excludes connection/authentication, state mutex waits,
START_REPLICATION, stream frames and implicit close rollback. Pinned ETL SQLx pool
timing includes pool waits; these two fixed pool names classify requests and do
not imply connection pooling. Observation errors never authorize retries/reconnect,
feedback or checkpoints. See query [validation](VALIDATION.md).

The reference's separate port, `/health` and other-path text behavior follows
Supabase ETL `c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4`. Families map existing
transaction/event/byte/message, write, copy, durability, worker, memory and batch
capabilities while retaining MoonCDC names and explicit semantics. Periodic SQL
for copy slots and main/private WAL current-source LSN, partition/schema cleanup,
and query timing are wired as described; see [validation](VALIDATION.md).
Optional Sentry/Supabase local reporting and gaps, and scoped ConfigCat polling,
are also recorded there. Polling failures emit fixed diagnostics, without a
Prometheus family. Rust/Tokio/jemalloc-specific metrics have no MoonBit equivalent.
This is not complete C8 configuration/observability acceptance; no Prometheus
service or real constrained-environment pressure-metric validation was performed.
