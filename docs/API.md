# API and support contract

The compiler-generated [interface](../pkg.generated.mbti) is authoritative.
Run `moon doc` for browsable API documentation. The public types belong to the root
`pangbit/mooncdc` package; examples are separate executable packages.

The 0.1.0 module supports the PostgreSQL → ClickHouse library and
replicator CLI through the same pipeline core. See [release scope](RELEASE_SCOPE.md).
Other destination implementations are outside this module's scope.

## Connection settings

Prefer `ConnectionConfig::new(host~, user~, password?, database?, port?, security?,
timeout_ms?, hostaddr?, keepalive?)`. It validates configuration without opening sockets;
defaults are port 5432, database postgres, empty password, verified TLS and a 5000ms
connection timeout. Passwords remain excluded from Debug output.

`hostaddr` is an optional numeric IPv4/IPv6 address, without a port or IPv6 scope. It
overrides only the TCP destination: `host` still supplies the TLS certificate name.
Plaintext retains the numeric loopback host restriction and also rejects non-loopback
hostaddr overrides. DNS is used only when hostaddr is absent.

`TcpKeepaliveConfig::new(idle_secs?=30, interval_secs?=10, retries?=3)` enables TCP keepalive
on every PostgreSQL connection. All values must be positive; the operating system can
reject unsupported values. These settings do not replace replication feedback, application
progress detection, or query deadlines, and do not promise an exact disconnect time.

Source compatibility: `ConnectionConfig` gained `hostaddr` and `keepalive` fields. Existing
record literals must add `hostaddr: None, keepalive: TcpKeepaliveConfig::new()` or move to
the constructor. Record updates using `..config` retain both fields automatically.

PostgreSQL startup options select a workload profile independently of connection
and client operation deadlines. Metadata/state/migration sessions use statement,
lock and idle-in-transaction timeouts of 30s/10s/60s. WAL and snapshot readers use
0/30s/0 so long streams and copy callbacks can retain their snapshot. Snapshot
exporters use 0/0/0 because slot creation can wait for writers, and no further
query may invalidate an exported snapshot; their separate reader still has the
30s lock bound. Pipeline slot creation temporarily disables lock_timeout and
restores 30s before reuse. Existing client deadlines and cancellation still apply.
Application names identify queries, state, migrations, WAL, snapshot reader and
exporter in `pg_stat_activity`; per-pipeline/per-table suffixes are not yet supplied.

## Lifecycle

`ApplyControl::prometheus(pipeline_id?, destination?, project?, stall_timeout_ms?=300000)`
renders scoped Prometheus text without changing worker state. Counters belong to the
control and include retries/replays; use a fresh control for a fresh metrics lifetime.
Optional label values are escaped and limited to 1024 UTF-8 bytes each. Built-in labels
never contain relation names, connection strings or error messages.
`with_metrics_server(control, run, port?=9000, listen_address?, pipeline_id?, destination?, project?,
stall_timeout_ms?=300000)` validates and binds a separate IPv4/IPv6 listener before
calling `run`. `/health` returns `OK`; other paths render the same metrics. All methods
are accepted and HEAD omits the body. The scope closes its listener and accepted
connections on normal return, failure and cancellation. It uses a 5s request timeout
and allows at most 128 simultaneous clients. Metrics never acknowledge data or stop
workers. See [metric definitions and limitations](METRICS.md).
Omitting `listen_address` keeps the existing all-interface IPv4/IPv6 listener.
An explicit numeric IPv4 or unbracketed IPv6 address binds only that address,
without DNS resolution or IPv4 fallback. `validate_metrics_listen_address(address)`
checks this syntax without creating a listener; the port remains a separate option.

`run_pipeline(..., memory_backpressure=MemoryBackpressureConfig::new())` enables
shared host/cgroup pressure sampling every 100ms. Activation defaults to 0.85 and
resume to 0.75; resume requires usage strictly below that threshold. The library
option is opt-in. Configuration validates before opening pipeline connections.

Pressure gates new main-WAL reads, snapshot FETCH commands and private table WAL
reads. Existing reads can complete; destination writes, timed flushes, checkpoint
acknowledgements and periodic feedback continue. Pressure activates immediate
flush of pending apply writes ahead of the ordinary timer. Snapshot Accepted rows flush
before waiting. Stop cancels blocked source waits, and the pipeline scope joins
the sampler after its consumers exit. Paused source activity does not appear
stalled, but a concurrent blocked destination write still does.

This is emergency source pausing, not a hard memory limit. Transaction/frame
limits remain independent.
The replicator CLI enables these pressure defaults unless its
`memory_backpressure` field is explicitly null; see [configuration](REPLICATOR.md).

`run_pipeline(..., batch=BatchConfig::new())` separately enables advisory decoded
batch targets: `memory_budget_ratio=0.2`, `max_bytes=33554432`, and
`max_fill_ms=10000`. The ratio must be in (0,1], bytes positive, and fill time
nonnegative; zero flushes each item. The fill deadline starts at the first item
and does not restart when more data arrives. Native UTF-16 row/metadata size hints
are estimates, not heap measurements. Capacity and active ownership determine
each target, capped by `max_bytes`; an indivisible item can exceed it.

With `batch` and a destination that returns true from `supports_wal_batches`,
main WAL and private table WAL decode and release individual events. ClickHouse
opts in; the default for custom destinations is false. `write_wal_batch` receives
an ordered `WalBatch`, whose `end_lsn` is Some only after a validated Commit.
`event_id` preserves the original row ordinal or logical-message LSN across
batch splits, table projection and replay; `replicated_schemas` retains each
event's exact schema. The default writer rejects event batches rather than
passing a partial object to `write_transaction`.

The receiver has one delivery slot, and the apply worker accumulates one batch.
They register two governor positions. A copy worker owns one position during
snapshot and adds its receiver position during event catchup; extra snapshot
readers also register their ownership. Byte target, first-event deadline,
pressure and stop drain partial batches. Every Accepted batch flushes before
another accumulates; Commit also drains, even for an empty table projection.
Checkpoint and table handover advance only after Commit and the cumulative
target barrier. Cancellation, malformed Commit or disconnect before it can leave
visible target fragments, but keep the old checkpoint for stable-ID replay.
This contract does not provide atomic visibility of an entire source transaction.

Public `subscribe`/`next` still deliver complete `Transaction` values. Without
event opt-in, pipeline apply keeps the compatible whole-transaction collector,
registers 16 pending positions plus one decoder and can exceed the target by a
whole transaction. Cumulative transaction byte/event and metadata limits apply
to both decoders; releasing events never resets those limits. A single event,
wire frame, metadata cache or target encoding can exceed the advisory target.
Registrations release on failure and cancellation. This v1 event path is separate
from the opt-in pgoutput v2 collector described below. Pipelines continue to use v1.

Both options share one sampler. With only `batch`, capacity refresh continues
without pressure pausing. `memory_refresh_interval_ms` overrides the period in
`MemoryBackpressureConfig`; otherwise that period, or 100ms, applies. A supplied
period must be positive. Omitting both options preserves library behavior.
The replicator CLI enables a default `batch` when omitted; explicit null disables
it. It shares `memory_refresh_interval_ms` even when pressure is disabled. Library
calls retain opt-in batching; see [configuration](REPLICATOR.md).

`run_pipeline(..., table_sync_monitor_refresh_interval_ms?=10000)` runs a separate
sampler while each copy owns its exported slot, including catchup. Positive Int
milliseconds are required. The sampler queries its own connection, using current
WAL on a primary and replay WAL on a standby. Missing/invalidated private slots cancel
that copy and enter the existing table retry policy; incompatible metadata is fatal.
Other sampling errors are counted and retried after the period. Sampling never advances
the checkpoint. Owner completion, failure and cancellation interrupt and join pending
sampling handshakes/queries. The same interval also samples current source WAL (or
standby replay) LSN through a separate bounded SQL connection for main apply and
private catchup workers. Sampling continues while source reads are paused or the
destination is blocked, and never changes checkpoints, feedback or worker activity.
See [metrics](METRICS.md) for lag positions, sample age and aggregation semantics.

`subscribe(config, slot~, publication~, start~, checkpoint_path~, consume, ...)`
authenticates, identifies the source, validates the slot, enters COPY BOTH, and starts
reader/heartbeat tasks. The callback receives a `Subscription`. Returning, raising or
cancelling the callback stops the tasks and closes local sockets and the checkpoint
lock. Server-side slot deactivation follows TCP closure; the slot itself is retained.
Opening a subscription waits for slot deactivation within the connection timeout; it
never terminates another slot owner. This also permits immediate close/resume cycles.

`next()` yields one immutable `Transaction`. Use one task to call it. Multiple tasks
may finish business work out of order and call `ack(tx)`; acknowledgement is serialized.
Do not retain a subscription beyond its callback. `close()` wakes waiting consumers
and closes the connection; it does not acknowledge or drop a replication slot.

`ack(tx)` records completion and persists only the contiguous prefix. Acknowledging
transaction 3 while 2 is incomplete cannot advance beyond 1. A repeated acknowledgement
in the same session is harmless. A transaction from another session is rejected.
The caller is responsible for persisting its business changes and deduplication ID
atomically before `ack`. In-memory example state cannot survive process failure.

The checkpoint uses SHA-256 integrity checking, a versioned source binding, exclusive
advisory locking, fsync of a temporary file, atomic rename and fsync of the parent.
The parent directory must already exist. Keep checkpoint files on a local filesystem
with these POSIX semantics. A `.lock` file remains after exit; its existence does not
mean the advisory lock is held. A stale `.tmp` is ignored and replaced on the next write.

## External state stores

`subscribe(..., checkpoint_store=store)` uses a `StateStore` instead of `checkpoint_path`.
`run_pipeline(..., state_store=store)` similarly replaces `state_path` and its checkpoint.
Choose one storage option; file-path calls remain compatible. A store namespace belongs
to one pipeline/stream, must remain stable across restart, and must not be reused for a
different source or destination. The same source binding, checksums and replay checks apply.
`SnapshotPosition.save_checkpoint_in(store)` persists an initial snapshot for later Resume.

`StateStore` implements exclusive acquire/release, atomic load, and owner-checked durable
save for Pipeline, ReplicationCheckpoint, Schemas and TableMetadata records. Implementations must
exclude competing processes and reject stale owners; a successful save establishes durability.
Failed saves do not advance the in-memory pipeline/checkpoint. `state_store_status(store)`
observes an atomic status without acquiring ownership. `with_state_store(store, session => ...)`
releases ownership on return, error or cancellation; retained sessions reject further writes.

`session.check()` exposes the existing local lifetime check to external destinations.
Call it before external side effects and after an asynchronous wait that could retire
the scope. It checks that this scope is active; it does not query the backing store,
renew ownership, fence a remote writer, or authorize an acknowledgement.

`FileStateStore.new(existing_directory)` uses an advisory `.lock` plus atomic fsynced records.
`PostgresStateStore.new(config, store_id~, schema?="public", table?="mooncdc_state", control?)` uses
one PostgreSQL session advisory lock per store ID and a shared record table. Acquisition
creates the table if absent (the schema must exist); schema/table names and IDs are escaped.
Writes use synchronous_commit=on and transactional upserts. Concurrent table initialization
is serialized. A failed owner connection is closed and never silently reconnected. A fresh
acquisition is required; backend termination releases the PostgreSQL advisory lock.
Keep the state table outside the source publication; use a separate metadata database
when the source publication includes all tables.
Passing `control` records explicit state queries in its `postgres_store` query histogram,
including failed/cancelled requests, without changing ownership or reconnect rules.

For store-backed pipelines, `Destination.bind_state(session)` runs before startup after
source/destination validation. `session.get_metadata(oid)` / `put_metadata(oid, json)` let
destinations persist creation/schema/generation metadata after the corresponding operation
is durable. Metadata is checksummed, OID-bound and retains exact JSON numeric text; limits
are 16 MiB per record and 127 JSON container levels. The default bind hook does nothing.
Custom destination operations must still fence their own earlier asynchronous writes;
state-store ownership alone does not establish distributed fencing of destination data.

`with_state_schema_store(schema_state, identity~, schemas => ...)` persists the full
versioned schema history through a StateStore rather than a local schema file. Pass
`schemas` to the destination and `run_pipeline(..., schema_store=schemas)`. Use a
separate namespace from the pipeline's StateStore (different directory or PostgreSQL
store ID), since both scopes hold exclusive ownership concurrently. For example,
PostgreSQL IDs `pipeline-1/state` and `pipeline-1/schemas` can share the same metadata
table while keeping independent ownership. Keep both namespaces and the schema
identity stable on restart. This permits recovery without a local schema file.

The checksummed schema envelope, 16 MiB bound, exact-version conflict checks and
checkpoint-based pruning match the file schema implementation. Cache changes occur
only after the record save succeeds; a failed write/prune preserves the previous
cache. The scoped SchemaStore rejects use after return. Existing `with_file_schema_store`
files remain compatible and are not migrated automatically. Custom StateStore
implementations with exhaustive matches must add the new `Schemas` record kind.

## Positions and replay

`At(nonzero_lsn)` requires a new checkpoint path. Choose the consistent point returned
when the slot was created; choosing a later explicit position intentionally omits older
changes. `Resume` requires an intact existing checkpoint. The binding includes PostgreSQL
system identifier, timeline, database, slot and publication. Slots whose restart or
confirmed flush position exceeds the checkpoint are rejected, as are missing/invalidated
slots. Do not share a slot between consumers or manually advance/recreate it.

On a transient connection failure, reconnects preserve complete pending transactions in
memory and restart after the last retained complete transaction; incomplete transactions
are decoded again. Replayed complete transactions already retained in memory are skipped.
After process exit the durable checkpoint governs recovery, so unacknowledged transactions
may replay. Fatal protocol, configuration, limit and checkpoint failures are never retried.
Server shutdown SQLSTATEs 57P01/57P02/57P03 are retryable; other server errors are surfaced.
One reconnect attempt covers connection/authentication, source identification and starting
COPY BOTH. Transient errors or timeouts in any of these stages consume the same bounded
retry budget; each failed connection is closed. Exhaustion surfaces the last attempt's
error. A changed source fails immediately even when retry attempts remain.

`Transaction.id` combines source system, database, slot and commit LSN. XIDs alone are not
unique across wraparound. `event_id(index)` adds the ordinal of data changes only; relation,
type and origin messages return `None`, since their announcements may differ on reconnect.
`commit_time_us` uses the PostgreSQL epoch (2000-01-01 UTC). The JSON representation contains
the business transaction and excludes process-local acknowledgement tokens.

## Data

`Change` supports RelationChanged, TypeChanged, Insert, Update, Delete, Truncate, Origin
and opt-in transactional Message.
Every row change includes an immutable Relation snapshot with columns, key flags, type
OIDs and type modifiers. Schema changes are reflected in subsequent relation metadata;
this is not a complete DDL stream. Custom types retain OID/name metadata and text values.

`OldTuple.Key` contains a replica-identity tuple; non-key positions can be NULL placeholders.
`OldTuple.Full` describes an old row under FULL identity. Updates can have no old tuple.
Consumers needing arbitrary old values should configure FULL and still handle unavailable
TOAST values. `UnchangedToast` means retain the previously known value, or fetch/rebuild
state if it is unavailable. It never means SQL NULL. Truncate can affect multiple relations
and carries cascade/restart-identity flags.

Protocol v1 supports default text transfer or explicit `subscribe(..., binary=true)`.
Binary fields are exposed as `Value::Binary(Bytes)` without UTF-8 conversion or native
SQL type decoding; use each column's OID to interpret its wire format. PostgreSQL may
fall back to text for types without binary output, so handle both Text and Binary.
The transfer choice is retained during reconnect. Adding the Binary variant requires
consumers with exhaustive Value matches to add a branch.
Only committed deliveries and UTF-8 text encoding are supported. Two-phase
transactions and automatic endpoint selection/HA routing are not implemented. Unknown
messages fail explicitly.

`subscribe(..., protocol=V2Streaming)` negotiates `proto_version=2, streaming=on`.
The default is `V1`. v2 accepts Stream Start/Stop/Commit/Abort and XID-prefixed
row, relation, type, truncate and transactional-message records. It retains
interleaved uncommitted transactions privately and delivers a complete Transaction
only at Stream Commit. Root abort discards its buffer; subtransaction abort
truncates at that subtransaction's first arrival offset, including later nested
or released records. Relation/type wire metadata remain connection-scoped.

The transaction byte/event limits additionally bound the aggregate of all open
stream buffers and an ordinary transaction; `max_pending_transactions` also
limits the number of open streamed transactions. Stream-control bytes count
toward the byte limit. Subtransaction abort does not reset cumulative quotas.
An overrun fails without provisional delivery or checkpoint advancement; reopen
with suitable limits to replay. These bounds do not promise an exact RSS limit.
Reconnect retains v2/binary/messages negotiation, discards incomplete buffers,
and replays from the durable checkpoint. Committed row/message IDs use the same
identity scheme as v1. This standalone mode is separate from pipeline WalBatch
delivery; `run_pipeline`, private COPY catchup and the CLI continue to use v1.

`subscribe(..., messages=true)` requests logical messages. Transactional messages from
`pg_logical_emit_message` appear as `Change::Message(LogicalMessage)` in commit order;
rolled-back messages are not delivered. `prefix` is text, `content` is raw Bytes and
`lsn` is the message's own WAL position. `event_id` uses `tx.id + "/message/" + lsn`
for these messages, keeping existing row-event ordinals unchanged.

Nontransactional messages are delivered to the optional `nontransactional_message`
async callback, even if the transaction that emitted them rolls back. If one arrives
without a handler, the subscription fails explicitly. Finish required writes durably
before returning; handler failures stop the subscription without reconnecting. The
callback runs on the reader, with backpressure, while heartbeats remain independent.
Its completion does not independently advance the checkpoint or feedback. A later
acknowledged transaction can cover its position; until then it may replay after a
restart/reconnect. Use source identity plus message LSN for deduplication. A stream
containing only nontransactional messages retains WAL at its prior durable position.
This is a protocol API; Supabase ETL's `supabase_etl_ddl` JSON schema interpretation,
event-trigger installation and schema-store updates are not implemented by it.

## Initial snapshot

`copy_snapshot(config, slot~, publication~, consume, ...)` creates a new persistent
pgoutput slot with an exported snapshot, imports that view into a read-only repeatable
read transaction, and copies publication tables sequentially. The callback receives
`TableBegin(Relation)`, zero or more `TableRows(Relation, rows)`, then `TableEnd(Relation)`.
Empty tables still receive begin/end. Rows contain Text or Null, with full TOAST values.
The callback must finish its target writes durably before returning. It may clear the
target table at TableBegin; only TableEnd marks that table's copy as complete.

After all callbacks and the read transaction succeed, the function returns an opaque
`SnapshotPosition` with a readable `lsn`. Pass it to `subscribe(...,
start=AfterSnapshot(position))` with a new checkpoint path. The handoff checks the source
system, timeline, database, slot and publication before creating the checkpoint. Writes
committed during copying are then replayed from the slot's consistent point. Alternatively,
call `position.save_checkpoint(path)` after durable copying, then start a later process
with `Resume`. Saving uses the same lock/fsync/atomic replacement protocol and refuses
to overwrite an existing checkpoint. Do not
manually advance, recreate or consume that slot between snapshot and subscription.

Publication column lists and row filters are applied to the initial copy. Partition
root/leaf identities follow `publish_via_partition_root`; ordinary inheritance tables
are copied separately without duplicating children. SELECT permissions are required.
RLS is disabled for the reader so insufficient privileges fail instead of silently
copying a subset. Do not change schema or publication membership during the copy;
concurrent DDL reconciliation and automatic table synchronization are not implemented.

`batch_rows` defaults to 256 (range 1–1024). `max_frame_bytes` defaults to 16 MiB and
`max_batch_bytes` to 64 MiB; the latter caps accumulated DataRow wire bytes per query.
Oversized rows/batches fail explicitly, without a partial batch callback. Metadata
queries currently allow up to 1024 tables and 1024 published columns per table.
Callbacks provide backpressure; each database operation uses `connection.timeout_ms`.
Object/string overhead and data retained by the consumer are outside these wire limits.

`copy_connections` defaults to 1 (range 1–256) on `copy_snapshot` and
`copy_snapshot_to`. Values above 1 use multiple read connections importing the same
exported snapshot. Physical tables are split into disjoint CTID ranges, with open
first/last boundaries; partition roots schedule their leaf tables. Planning uses
current storage size and catalog row estimates without scanning the table first.
At most the configured number of readers process those jobs, in addition to the
exporter and metadata reader. Empty tables still produce begin/end and target setup.
Row order is unspecified. All callbacks and destination operations remain serial;
copy batch IDs share one sequence within the snapshot attempt. One failed or cancelled
worker cancels its siblings and closes every imported transaction before returning.

An existing slot is refused. On callback failure, timeout or cancellation all sockets
close, the read transaction ends, and no SnapshotPosition or checkpoint is produced.
The new persistent slot remains, retaining WAL: explicitly inspect/drop that owned slot
and discard partial target state before starting a fresh copy. This API does not persist
per-table progress or resume a partial snapshot. Losing the in-memory handoff position
before saving either form of checkpoint also requires a deliberate fresh bootstrap.

## Destinations and durability

Implement the public `Destination` trait to receive snapshots and committed transactions.

External adapters can construct the existing `CdcError` variants for shared error
classification. The variants and payloads are unchanged; making their constructors
public does not change core error matching or recovery semantics.

`Transaction.destination_events()` and `WalBatch.destination_events()` provide a
read-only `DestinationEvents` view for adapters. It preserves ordered changes,
stable event IDs and exact per-event schemas. `session` is an opaque delivery
lifetime identity for fencing in-memory replay debt, not a durable event ID.
The view has no `end_lsn` or acknowledgement API. A fragment's `commit_lsn` does
not prove receipt of Commit and must never be used to advance a checkpoint.
The existing Transaction, WalBatch, Destination and acknowledgement APIs remain.

When a `schema_store` is supplied to `copy_snapshot_to` or `run_pipeline`,
`bind_snapshot_schema(relation, schema)` receives the exact imported-snapshot schema
after durable schema storage and before `reset_table`. Its default implementation is
empty. Typed destinations should use this version for snapshot rows, rather than infer
the copy version from an unbounded schema-store lookup.
Each write returns `Accepted` or `Durable`. Accepted means the destination owns the work;
Durable means that write **and all earlier accepted writes** on the same ordered instance
are persisted. `flush()` must wait for all earlier accepted writes to become durable or
raise. Returning successfully before persistence violates the contract and can lose data.

`with_destination(destination, run)` calls startup, runs the pipeline and calls shutdown
on success. Startup/shutdown default to no-ops. Errors propagate immediately and skip
shutdown; destination owners must use their own resource scopes to cancel/release tasks
on failure. Do not share one instance among concurrent pipelines: this first contract
has one ordered writer and a cumulative global durability barrier.

`copy_snapshot_to(config, destination, slot~, publication~, ...)` resets each table before
copying. `reset_table` must fence prior copy attempts before clearing data and replay
markers. Empty row writes have no batch ID and create empty tables. Nonempty writes carry
`TableCopyBatchId { attempt, relation_id, sequence }`; each fresh copy generates a new
attempt. A final Accepted result requires flush before that table completes. No snapshot
handoff position is returned after a write or flush failure. This helper does not call
startup/shutdown itself; place it inside with_destination.

Within subscribe, call `sub.apply_to(destination, control~, ...)`. It writes transactions
in order and acknowledges only after Durable or a successful flush. Accepted writes flush
after `flush_every` transactions (default 8, no greater than max_pending_transactions),
on idle `flush_interval_ms` (default 1000), or when `control.stop()` requests a graceful
stop. Stop drains already accepted work, then returns; queued but undispatched source
transactions remain unacknowledged. Cancellation or failure does not drain or acknowledge
unfinished writes. Do not call next/ack concurrently with apply_to.

Use transaction IDs and copy batch IDs to make destination effects idempotent. This
contract does not itself provide schema planning or built-in cloud destinations.
The pipeline below adds durable per-table coordination. Existing checkpoints remain the source
of truth for transaction recovery; the destination persists its own business/replay state.
During concurrent table synchronization, a main transaction's projection can gain tables
between attempts. Deduplicate pipeline row effects with `event_id`, not only `tx.id`, so an
earlier partial projection cannot suppress newly owned table events during replay.

## Persistent table pipeline

`run_pipeline(config, destination, publication~, destination_id~, state_path~, control~,
slot_prefix?="mooncdc", copy_concurrency?=2, copy_connections_per_table?=1,
table_sync_monitor_refresh_interval_ms?=10000, batch_rows?=256,
table_retry_delay_ms?=10000, max_table_retries?=5,
table_sync_copy?=IncludeAllTables, slot_failover?=false,
invalidated_slot_behavior?=RejectInvalidatedSlot)` owns a randomly named main
replication slot and, by default, temporary slots for independent table snapshots. It manages destination
startup/shutdown and keeps a checksummed, atomically replaced state file plus a sibling
`.checkpoint` file. Keep both files, their locks, and the destination's durable data together.
The destination identity is caller supplied and must uniquely identify the target dataset.
Source, destination and slot-prefix mismatches fail before target startup.

`slot_failover=true` requires PostgreSQL 17 or later. It creates failover-enabled
main and table-copy slots, and upgrades an existing main slot when needed.
Disabling the option never downgrades an existing main slot. Private copy slots
become persistent: durable table state owns their deterministic names, and the
pipeline drops them after closing the exporter on success or failure. Startup
cleans recorded copy slots before reconciling publication membership, including
removed tables and restarts with failover disabled. Cleanup waits for inactive
slots within the connection timeout and refuses incompatible plugin/database or
temporary slots. Cleanup failure stops the pipeline; completed handover is kept.

`copy_snapshot` and `copy_snapshot_to` accept the same `slot_failover` option;
their standalone slots remain caller-owned after success or failure. Configure
physical-standby slot synchronization separately. Standalone snapshot handoffs
and `subscribe` retain their strict timeline binding.

A new `run_pipeline` process with `slot_failover=true` can resume its existing
state on a promoted source. System ID, database, slot, publication, destination
and slot prefix must still match. Before target startup it validates the new
`TIMELINE_HISTORY`, finds the stored timeline's fork, and requires the checkpoint,
initial snapshot, table cutoffs and durable target write horizon to precede or
equal that fork. The main slot must already be persistent, failover-enabled and
valid, with restart/confirmed-flush positions no greater than the checkpoint.
Configure the promoted connection and fence the old primary before restarting.

New pipelines persist the write horizon before main/private WAL target or schema
writes and before snapshot schema persistence. Partial v1 batches use Commit-start
LSN plus one, so a fork at the Commit record's start rejects those rows. This is
not an acknowledgement. Each increasing horizon adds a durable state write.
Legacy state without this field remains usable on its original timeline but
cannot be automatically rebound; later writes do not invent its missing history.
Explicit slot recreation initializes a new horizon when resynchronizing all tables.

Promotion persists an intent, rewrites only the checkpoint's source binding at
the same position, then completes the pipeline binding. A restart validates the
same ancestry and slot again and accepts only the old/new checkpoint pair named
by that intent. Completed tables keep their state; an interrupted intent cannot
start target work. A second promotion during an unfinished intent fails closed.
An active pipeline can also re-enter these startup checks when its main worker
reconnects to the same source on a different timeline with `slot_failover=true`.
It first closes and joins every old source worker and flushes accepted target
writes, then reloads the owner-protected records. It retains its destination and
state owner; a failed owner connection remains fatal and requires a fresh scope.
Promotion reentry has a separate count bounded by `max_table_retries`, waits for
`table_retry_delay_ms` in Initializing, and is interruptible by stop. A controller
must fence the old primary and route the configured endpoint to the promoted
source; this does not configure a standby, DNS/HA routing or distributed ownership.
The active CLI matrix covers completed COPY with an independent healthy PostgreSQL
state/schema store; active COPY overlap and other state backends need further live
evidence. See validation [validation scope](VALIDATION.md).

`invalidated_slot_behavior=RecreateInvalidatedSlot` explicitly permits recovery from
an existing owned slot whose WAL has been invalidated. The default rejects it without
resetting the destination or changing its checkpoint. Missing/incompatible slots,
changed source/destination identity, corrupt checkpoints and empty publications remain
fatal; this option does not bypass those checks or enable timeline promotion recovery.

Recreation resets every table state to Pending, drops owned copy slots and the invalid
main slot, creates a new main slot with the requested failover setting, saves its
consistent point as the new checkpoint, then copies the selected tables again. Tables
excluded by `table_sync_copy` are reset and initialized empty; changes lost with the
old WAL cannot be recovered for them. Included tables obtain a new consistent snapshot
and resume WAL from it. Target metadata/schema history is retained for safe reset and
schema reconciliation. Do not manually consume or advance a pipeline's slot.

A temporary pipeline-state version 2 records the durable recreation intent. It contains
only Pending table states and no completed decoding state; the old checkpoint is not
usable as a new-slot resume frontier. A process restarted with the recreate option
finishes or repeats this intent before any target work. It can repeat slot creation
after an interrupted creation reply. Normal operation returns to version 1. Older
library versions reject the in-progress version 2 state; preserve both records and
restart with this implementation and the explicit recreate option to finish recovery.

If a slot becomes invalid while running, all old source tasks close and the destination
flushes before a new source worker group starts. Its bind/startup/shutdown lifetime is
kept across this recovery. The recreation retry count is bounded by `max_table_retries`
and uses `table_retry_delay_ms`; intentional recovery waits are initializing/unready and
stop wakes them immediately. A target flush failure aborts recovery without publishing
a new checkpoint. This is resynchronization after lost WAL, not recovery of lost events.

Pipeline worker connections expose stable `application_name` values in
`pg_stat_activity`: `mooncdc_replication:apply:<pipeline>` for main WAL apply,
`mooncdc_snapshot:table_sync:<pipeline>:<oid>` for snapshot readers, and
`mooncdc_snapshot_export:table_sync:<pipeline>:<oid>` for exporters/private WAL.
`<pipeline>` is the first 16 hex characters of SHA256 of the persisted main slot
name. It remains stable across restarts and retries without exposing destination
configuration or adding a new user identifier. Names fit PostgreSQL's 63-byte
limit, including the largest table OID. Other connections retain workload names.

`table_sync_copy` selects initial rows by source table OID: `IncludeAllTables`
(default), `SkipAllTables`, `IncludeTables([oid, ...])`, or `SkipTables([oid, ...])`.
Duplicate OIDs are rejected before starting; an empty IncludeTables list skips all
initial rows and an empty SkipTables list copies all. This is not a streaming
filter: every published table still joins CDC. Skipped tables still acquire an
independent snapshot/slot, capture schema and decoding masks, reset/create the
destination and finish its empty-write durability barrier before handing over.
They omit rows existing at their own snapshot, including pre-snapshot WAL effects.
Use this policy only when that intentionally partial target state is desired.

Changing the policy on restart affects new/unfinished table copies, including
retries, but never recopies or clears tables that already completed handover.
The policy is supplied for each run rather than persisted as immutable source
identity. Standalone `copy_snapshot` and `copy_snapshot_to` continue to copy rows.

`pipeline_status(path)` reads Pending, Copying, Catchup(cutoff), Ready(cutoff) and
Errored(reason, retry, attempts) per OID.
After restart, only Pending/Copying tables receive a fresh snapshot and reset; completed
tables retain their handover cutoff. Main-stream events before each table's cutoff are filtered.
Row/message events retain their original `event_id`; pipeline truncates split into one event
per table, with `/table/<oid>` appended so fragments from different workers cannot collide.
Destination durability precedes handover. The main checkpoint may advance while another table
is Copying: that table retains its own slot and is reset from a fresh snapshot after a crash.
Unfinished copies recover on restart; errored tables retain their retry policy. Missing checkpoints
after initialization fail instead of resetting progress. Creation-intent recovery may replace
only its own randomly named slot, before any destination writes.

Copy concurrency is 1–16 and destination calls are serialized with a cumulative barrier.
The main worker applies ready tables while bounded table workers copy and privately replay WAL.
Copying includes this private replay. Once a table has durably caught up to the main worker,
its cutoff is saved under the same mutex used by main projection; the temporary slot then closes.
Private replay transaction IDs include `/copy/<temporary-slot>` to distinguish table projections;
event IDs remain tied to the main source lineage. Transactions spanning tables can be delivered
as separate projections during synchronization; cross-table atomicity is not promised by pipeline.
The receiver uses bounded backpressure with independent heartbeats. Newly published tables join
the same worker queue on their first WAL relation/row or applicable upstream DDL message;
empty new tables without those messages are discovered on the next pipeline startup.
Publication removal is reconciled at startup and does not delete destination data.
Copy/private-replay failures are persisted per table while healthy tables continue. Known
transient timeouts, closed connections, connection refusal, shutdown/recovery, connection SQLSTATEs
and lock timeouts use bounded timed retries. Protocol, authentication, schema/value and unclassified
errors require manual retry. Destinations can raise `TableReplicationError::Retryable`, `Manual`
or `Permanent` to classify table-local failures. `Permanent` records NoRetry; exhaustion records
ManualRetry. TimedRetry persists its Unix-millisecond deadline; waiting releases the copy worker.
`control.retry_table(oid)` on an active pipeline resets the budget and queues a fresh snapshot,
returning false for unknown, healthy or NoRetry tables. Restarting alone does not retry ManualRetry.
Every retry fences/reset the unfinished table; it does not replay an old partial snapshot.
State/checkpoint, unsafe source identity and copy-side cumulative-flush failures still stop
the pipeline. A transient main-apply source or destination write/flush failure closes the failed
subscription and replays from the persisted checkpoint in a new subscription. It reuses
`table_retry_delay_ms` and `max_table_retries` with a separate budget for the whole run;
initial source setup consumes the same budget and successful attempts do not reset it.
The destination is started once and table-copy workers remain active; completed tables
are not recopied. Manual/permanent,
unclassified and exhausted failures propagate the original error. A stop during the retry
delay wakes immediately, drains the destination and leaves failed work unacknowledged.
Standalone `Subscription.apply_to` still propagates errors without this retry loop.

Pipeline source connection, identification, slot setup, publication discovery and streaming
I/O opt into worker recovery. Every streaming attempt creates a fresh subscription from
the durable checkpoint; pending transactions are replayed. Pipeline subscriptions disable
the inner reconnect loop, so source and destination failures consume one bounded budget.
Standalone `subscribe` retains its original reconnect policy and pending delivery behavior.
Source identity/slot safety, authentication, protocol, callback and state/schema-store
errors do not become retriable merely because they occur in the worker. Only explicitly
marked I/O/call boundaries enter retry classification; source socket OS errors exclude
permission and path failures. Startup and state/schema-store failures outside a destination
call are not retried. Destinations must keep retry effects idempotent and preserve ordering of
older in-flight operations before accepting replay; an HTTP timeout alone does not prove
that remote work stopped. A destination must classify its own unsafe metadata/recovery
failures as non-retryable rather than reporting a transient write failure.
Main-stream errors never skip individual tables: a failed attempt replays conservatively or
exits after policy/budget evaluation. Additional table/destination transport classifications
remain separate work. Operators must inspect Errored statuses even
while the pipeline is running.
Initial COPY holds the source table lock; subsequent private WAL catchup uses the same
decoding policy as main apply. Cancellation leaves conservative replay positions;
`control.stop()` drains accepted writes. Owned persistent slots retain WAL after exit and
require explicit operator cleanup when the pipeline is permanently retired.

## DDL schema versions

`message.schema_snapshot(commit_lsn~, publication~)` decodes the fixed Supabase ETL
`supabase_etl_ddl` JSON format. It returns None for another prefix/publication and rejects
nontransactional DDL or malformed metadata. It preserves physical column order, primary-key
order separately from replica identity, nullability, defaults and type modifiers. Unknown
JSON fields are ignored. `schema.relation(published_columns)` intersects the publication
mask with DEFAULT/FULL/INDEX/NOTHING identity semantics; a new wire Relation supersedes
this fallback when publication membership changes.

`SchemaId` sorts by commit LSN, then message LSN. Message LSN alone is not a transaction
ordering key. `SchemaId.before_lsn(checkpoint)` converts an exclusive replication position
to an inclusive lookup/retention bound and rejects zero.

The public `SchemaStore` trait supports get/all/put/prune. `with_file_schema_store(path,
identity~, run)` provides an exclusive, checksummed atomic file implementation with a
16 MiB bound. Identity must bind the source/publication/destination. Same-version replay
is idempotent; conflicting schema content fails. Cache publication follows successful
file persistence. `prune([(oid, bound), ...])` preserves the newest version at/before each
bound and every newer version. Choose bounds no newer than both the durable source
checkpoint and any version still required by destination recovery. Access after the
scope closes fails.

`Destination.schema_retention(oid)` opts a table into pipeline cleanup by returning the
earliest schema still referenced by durable destination metadata. Its default is None,
which preserves all versions. ClickHouse reports its current schema, or the earlier
of both endpoints while DDL is Applying. After a checkpoint is successfully persisted,
the pipeline considers tables seen in relation/row/DDL events, takes the minimum of this
retention version and `SchemaId.before_lsn(persisted_checkpoint)`, and prunes only tables
owned by main apply at that checkpoint. Copying, errored and future-cutoff tables are
excluded. Cleanup runs under the pipeline destination lock, so it cannot race a reset
or DDL transition. Lookup/prune errors keep candidates for a later checkpoint; they
do not fail replication or authorize any additional acknowledgement. This is best-effort
maintenance: a table with no retention metadata is reconsidered after a later relation
or DDL event. Custom destinations must report a conservative bound, including pending
DDL and other recovery references, before enabling cleanup. Stored handover decoders also
pin their exact schema version. The pipeline discards that payload only after a durable
checkpoint reaches the handover and the current connection has materialized its decoder;
failed state writes preserve the payload and block pruning for that pass.

Pass the scoped store as `run_pipeline(..., schema_store=store)`. The pipeline records a
full-table bootstrap schema inside each imported snapshot before target reset, using the
snapshot cutoff and message LSN zero. It persists applicable DDL versions before handing
the transaction to the destination. DDL remains a Message in its original transaction
order; destinations call schema_snapshot when applying it and must implement their own
schema evolution policy. Filtered pre-copy/foreign-publication DDL is not applied.
Resume rejects a completed table whose required retained schema is missing. Store files
retain consumed metadata only; upstream debug fields such as current_query are discarded.
External stores can persist `schema.to_json()` and restore it with
`SchemaSnapshot.from_json(json)`. The validated envelope retains both LSNs, full columns
and replica-identity metadata; it uses the same representation as the file store.

`schema.project(relation)` builds a `ReplicatedSchema` with separate full-width publication
and identity masks. `identity_kind()` distinguishes primary, full-row, alternative and missing
identity; `all_primary_keys_replicated()` checks complete source-key coverage independently.
`decode_row(values)` converts a published row using its projected type OIDs. The explicit
constructor validates mask widths and identity membership. `with_schema(next)` retains masks
for metadata-only DDL; a replacement Relation is required when membership/width changes.

With a SchemaStore, `tx.replicated_schemas(index)` returns the immutable schemas selected
for that event: one for Relation/Insert/Update/Delete, one per relation for Truncate,
and an empty array for other events, invalid indexes or raw subscriptions. This keeps
different schema versions within one transaction distinct. Raw changes, JSON and event
IDs remain unchanged. Destinations must apply the returned schema before writing its row;
ClickHouse does this automatically.

Each main attempt starts its schema lookup at the durable checkpoint. A table's handover
can raise that bound; a preceding DDL selects its exact version, and repeated protocol
Relation metadata preserves the connection's established schema lineage. DDL enters a
pending state retaining both masks. If a row arrives without a replacement Relation, the
pipeline combines those masks with the pending snapshot; if no local masks exist, it may
restore the completed copy's persisted decoder once the transaction reaches its cutoff.
Truncate requires a complete connection decoder. Copies seed that decoder from their
imported snapshot and can hand over only a complete state. The cutoff and compact snapshot
ID/publication/identity masks are saved in one pipeline-state write after target durability.
Missing exact snapshots, invalid masks and future handover versions fail closed. Legacy
state without a decoder still accepts a fresh Relation but cannot supply a relation-less
fallback. Source retries reset the main decoder and retain independent copy decoders.

`before.plan_change(after, map_name?)` plans column DDL by physical ordinal, with source-table
changes distinguished from publication-membership changes. It validates destination name
collisions, orders drops before renames and additions, and breaks rename cycles with reserved
temporary names. Type/modifier, nullability and default alterations then follow; each operation
contains the exact expected before/after state. Primary-key changes are reported separately.
The deterministic mapping callback must match table creation and writes (default: identity).
Names in the plan are already mapped. Planning does not execute DDL or imply that a target
supports every operation; destinations must validate capabilities before starting a plan.

The pipeline negotiates logical messages and rejects nontransactional DDL. Actual DDL
production requires upstream source helpers/event triggers. Applications may preinstall
them, call `run_source_migrations(connection, control?)`, or opt into installation with
`run_pipeline(..., source_migrations=true)`. The library defaults this option to false
for compatibility with existing embedded applications; the standalone replicator defaults
its `run_source_migrations` config to true, as does the reference. A stopped control does
not run migrations. Physical standbys skip installation; their primary must have the
helpers installed and replayed before replication starts.
Optional `control` records explicit migration queries as `out_of_band`; pipelines and
the standalone CLI pass their existing control. Query observations never advance state.

The two pinned SQL files in `integration/reference/etl` are embedded unchanged. Installation
requires privileges to create schema/functions and the database-wide event trigger
(PostgreSQL superuser for the latter). The migrator shares `etl._sqlx_migrations`, SHA-384
checksums and the database advisory lock with SQLx 0.9.0. Unrelated successful versions are
preserved; any dirty history or changed checksum for an owned version is rejected. Each
pending migration and success record commit in one transaction. A failed connection closes
and rolls back any uncommitted migration; retry validates committed history. Do not edit
applied SQL or repair checksum history to conceal drift. This API only installs source
helpers, not Supabase's state-store tables, and does not support down migrations.

Migration sessions use a 30s statement/client query timeout, 10s lock timeout and 60s idle
transaction timeout. Connection setup uses `ConnectionConfig.timeout_ms`. Lock ownership
ends with the migration connection scope. Target-specific DDL capabilities remain part of
each destination's contract.

## Typed text cells

`value.decode_text(type_oid, array_element_oid?)` converts PostgreSQL text transfer into
`Cell`: bool, signed int2/int4/int8, unsigned oid, float4/float8, exact numeric text, bytea,
UUID bytes, JSON, date/time/timetz/timestamp/timestamptz and one-dimensional arrays of these
types. Text/name/varchar/bpchar and unknown scalar OIDs remain String. Supply a catalog
element OID for custom arrays. Nested arrays are rejected; explicit lower bounds are
validated and discarded, matching the reference's one-dimensional value representation.
Quoted `"NULL"` stays a string while unquoted NULL becomes SqlNull. ToastUnchanged is
distinct from SqlNull and JSON null. Binary transfer is rejected by this text-only API.
Built-in arrays of interval, network, ranges and other generic types use String elements,
following the pinned rust-postgres type catalog.

JSON numbers retain exact decimal/exponent text in `Json::Number.repr`. The accompanying
Double is only an approximation; use the representation or `stringify()` for lossless
transfer. `Cell` equality compares JSON numbers by that exact representation recursively,
matching ETL's arbitrary-precision JSON configuration (including decimal scale).
JSON nesting is limited to 127 containers, matching serde_json's default budget.

Numeric expands exponents to canonical decimal text, preserving exact scale and
normalizing negative zero without floating-point conversion. Float parsing uses the
platform C runtime in a private C locale, with direct binary32 rounding for float4. UUIDs
are 16 bytes. Dates use astronomical years (1 BC = 0), with the reference's finite calendar
range -262143 through 262142. Date/timestamp infinities remain explicit variants. Time
uses seconds and nanoseconds; seconds=86400 distinguishes 24:00:00 from midnight.
Leap seconds use second 59 and nanoseconds >= 1000000000; fractions beyond nine digits
are truncated, matching the reference. Timetz
preserves local time and its offset; timestamptz normalizes to UTC across date/era changes.
Conversion errors never silently substitute a null or rounded integer.

All connections request ISO/YMD dates, UTC timezone, hexadecimal bytea, extra_float_digits=3
and PostgreSQL interval formatting at startup, including reconnect and snapshot readers.
Raw Value remains available and typed conversion is explicit. This interface does not add
numeric arithmetic or destination-specific coercions.

## ClickHouse destination

`ClickHouseDestination::new` takes `endpoint`, `user`, `password`, `database`,
`publication` and `schema_store`. Supply that same SchemaStore and a persistent
StateStore to `run_pipeline`; the database must already exist. Use a dedicated
StateStore namespace per target. Credentials go in HTTP headers; HTTPS validates
the server using system trust.

The default engine is `ReplacingMergeTree`; `MergeTree` keeps an append-only CDC
history. Optional `max_insert_bytes` defaults to 64 MiB. The byte limit splits encoded
batches, allowing an oversized single row. Operation server budgets are configurable
through `connectivity_timeout_ms` (8000), `schema_timeout_ms` (16000), `ddl_timeout_ms`
(128000) and `insert_timeout_ms` (256000). `client_timeout_epsilon_ms` (4000) is added
to each budget; the existing `timeout_ms` (260000) remains a global client cap.
Increase that cap as well when configuring longer operations. Server settings use
whole seconds floored at one to avoid disabling a limit. Connectivity/schema/DDL
queries set `max_execution_time`; DDL also sets `lock_acquire_timeout`. INSERT uses
the HTTP send/receive budgets and per-statement client deadline, matching the reference.
A client deadline does not prove the server has stopped the request. HTTP writes
are synchronous and return Durable only after server acknowledgement. StateStore
ownership does not fence a previously running remote HTTP request.

An admitted ClickHouse transaction keeps the instance's ordering lock even if its
source caller is cancelled. Replay, table reset and shutdown wait until that batch
finishes or fails under its own per-request deadlines. Cancellation while waiting
for admission remains immediate and sends no requests. This synchronous destination
serializes all tables, so its barrier is broader than the reference's per-table
background-task fences. Cancellation of an admitted batch can take as long as its
remaining operations; an outer timeout cannot override the drain. After a request's
own timeout or a process exit, no additional remote-cancellation or distributed-ordering
guarantee is claimed.

Source underscores are doubled and schema/table names joined with `_`. For example,
`public.my_table` becomes `public_my__table`. ReplacingMergeTree requires published
primary keys and creates a `<target>__current` view; its underlying rows carry
`_etl_version` and `_etl_deleted`. MergeTree permits tables without primary keys and
adds `cdc_operation`, `cdc_lsn`, and `cdc_tx_ordinal`. Primary-key changes produce an
old-key tombstone before the new row. Missing unchanged TOAST values require a FULL
old image; they are never replaced by NULL. NULL arrays and temporal infinities are
rejected. Portable literal defaults are translated conservatively.

Restart preserves completed tables and validates persisted target identity and
physical column names, order, types and engine before writing. Snapshot retries reset
incomplete tables. With the upstream DDL triggers installed, relation events (or the
first row using pending schema metadata) execute ordered column additions, drops,
renames and nullability relaxation. Tightening nullability keeps the physical column
nullable; changing a source default preserves the add-time destination default.
Publication-added scalar columns use Nullable without a default so historical rows
remain NULL; publication-added arrays require resynchronization. Mapped type changes,
ReplacingMergeTree primary-key changes/renames, and nested-parent moves are rejected
before DDL. Source table renames retain the persisted physical destination name.

DDL intent is persisted before external changes. Restart recovers initial creation
or an unambiguous previous/target schema endpoint; intermediate DDL prefixes and
column-name reuse require manual recovery. Older schema versions and unordered
equal-version publication-mask changes cannot drive reverse DDL. Tests cover injected
state-write failures, reconstructed interrupted states and SIGKILL at four creation/DDL
metadata boundaries with recovery by a fresh process for both engines. This does not
simulate power loss or an in-flight server-side INSERT. There is no cross-table transactional
commit guarantee. Copy batches use stable deduplication tokens; stream
replay deduplication depends on ClickHouse engine and server window configuration.

## Limits and diagnostics

`ApplyControl.activities()` returns approximate monotonic inactivity observations
for `run_pipeline` workers. `health(stall_timeout_ms=300000)` evaluates them without
database calls. Copy workers use the configured allowance; WAL workers use the
larger of it and the server's `wal_sender_timeout` (60 seconds when disabled or
unavailable). An age equal to the allowance is inactive. Every worker counts, so a
healthy worker cannot hide a stalled one. Slot acquisition and retry waits have no
activity registration; completed/cancelled workers remove their registrations.

`PipelineHealth.live` permits initialization and draining, but fails on inactivity.
`ready` requires Running, at least one observation and no inactive workers. `status`
is the readiness reason: `ok`, `initializing`, `inactive`, or `stopping`. Stop wins
over inactivity and immediately removes readiness while retaining liveness.
Readiness can be true during initial copy; it is not a durability or lag assertion.
Pipeline-owned WAL feedback requests keepalive replies to observe idle streams.
Receiving keepalives cannot conceal a blocked apply write, flush or checkpoint.
Raw `subscribe`/`apply_to` calls do not register pipeline observations.

`with_health_server(control, run, port=9001, stall_timeout_ms=300000)` binds before
invoking the async host callback. Wrap pipeline initialization and execution in
that callback to expose startup, running and draining. Omitting the wrapper disables
the listener. It serves GET/HEAD `/livez` and `/readyz`, with 200/503 and plain-text
reasons matching the health policy. Unknown paths return 404; other verbs on probe
paths return 405. The listener uses all interfaces with IPv4/IPv6 dual stack and
falls back to IPv4 only for unsupported family/protocol errors. Other bind errors
propagate before the callback starts. Port must be 1–65535 and timeout positive.

The host owns `control.stop()` and awaits pipeline drain. Returning, raising or
cancelling the callback closes the listener and its connections; the wrapper does
not initiate worker shutdown. Each connection handles one request and closes, with
a 5-second request deadline and 128 concurrent connections. Probe handlers make no
database calls. A standalone configuration and signal-handling executable is still
pending.

| Option | Default | Meaning |
|---|---:|---|
| max_frame_bytes | 16 MiB | Protocol body length checked before allocation |
| max_transaction_bytes | 64 MiB | Sum of message bytes per transaction; also aggregate open-buffer cap in v2 |
| max_transaction_events | 100000 | Changes including metadata; also aggregate open-buffer cap in v2 |
| max_pending_transactions | 16 | Outstanding committed transactions; also open streamed transaction count in v2 |
| backpressure | false | Wait for durable acknowledgement when the queue is full |
| max_metadata_entries | 4096 | Maximum relations and types, separately |
| heartbeat_ms | 1000 | Feedback interval; choose well below server wal_sender_timeout |
| max_reconnects | 3 | Total reconnect attempts per subscription |
| reconnect_delay_ms | 250 | Delay between attempts |

These bound retained protocol data, not exact heap/RSS. Decoded strings/objects add overhead.
The reader may assemble one additional bounded transaction before detecting queue overflow.
Metadata entries are individually bounded by frame size. By default a full queue fails;
with backpressure it waits for a durable prefix to free capacity. It never drops
oldest/latest data. After raising limits, resume from the same checkpoint.

Diagnostics report reconnect attempts, last retained complete transaction position, durable
position, server WAL end, queue count, total outstanding count and estimated WAL byte lag.
WAL lag includes database WAL unrelated to this publication and is not an event count.

## Platform and authentication

Native backend, POSIX local filesystems (macOS/Linux). Windows, Wasm and JS are not claimed.
SCRAM-SHA-256 and server-side trust authentication are supported. MD5/password-cleartext
authentication, SCRAM-PLUS/client certificates and non-ASCII SASLprep passwords are not.
`LocalPlaintext` accepts only 127.0.0.1 or ::1. `VerifyFull` trusts system roots and checks
hostname; `VerifyCAFile(path)` checks the supplied CA and hostname. There is no TLS downgrade
or certificate-verification bypass. TLS uses the OpenSSL runtime loaded by the async dependency.
The environment-specific validation report is separate from this intended support contract.
