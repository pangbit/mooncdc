# API and support contract

The compiler-generated [interface](../pkg.generated.mbti) is authoritative.
Run `moon doc` for browsable API documentation. The public types belong to the root
`pangbit/mooncdc` package; examples are separate executable packages.

## Lifecycle

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
Only committed transactions and UTF-8 text encoding are supported.
Streaming large in-progress transactions, two-phase transactions and automatic
failover are not implemented. Unknown messages fail explicitly.

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

An existing slot is refused. On callback failure, timeout or cancellation both sockets
close, the read transaction ends, and no SnapshotPosition or checkpoint is produced.
The new persistent slot remains, retaining WAL: explicitly inspect/drop that owned slot
and discard partial target state before starting a fresh copy. This API does not persist
per-table progress or resume a partial snapshot. Losing the in-memory handoff position
before saving either form of checkpoint also requires a deliberate fresh bootstrap.

## Destinations and durability

Implement the public `Destination` trait to receive snapshots and committed transactions.
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

## Persistent table pipeline

`run_pipeline(config, destination, publication~, destination_id~, state_path~, control~,
slot_prefix?="mooncdc", copy_concurrency?=2, batch_rows?=256)` owns a randomly named main
replication slot and temporary slots for independent table snapshots. It manages destination
startup/shutdown and keeps a checksummed, atomically replaced state file plus a sibling
`.checkpoint` file. Keep both files, their locks, and the destination's durable data together.
The destination identity is caller supplied and must uniquely identify the target dataset.
Source, destination and slot-prefix mismatches fail before target startup.

`pipeline_status(path)` reads Pending, Copying, Catchup(cutoff) and Ready(cutoff) per OID.
After restart, only Pending/Copying tables receive a fresh snapshot and reset; completed
tables retain their handover cutoff. Main-stream events before each table's cutoff are filtered.
Row/message events retain their original `event_id`; pipeline truncates split into one event
per table, with `/table/<oid>` appended so fragments from different workers cannot collide.
Destination durability precedes handover. The main checkpoint may advance while another table
is Copying: that table retains its own slot and is reset from a fresh snapshot after a crash.
Failures propagate; restarting the same state path performs recovery. Missing checkpoints
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
Per-table error isolation/retry and stored decoding-mask recovery remain separate work.
Schema changes during a table copy remain unsupported. Cancellation leaves conservative replay positions;
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
checkpoint and any version still required by destination recovery. Automatic pruning is
not implied. Access after the scope closes fails.

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

`before.plan_change(after, map_name?)` plans column DDL by physical ordinal, with source-table
changes distinguished from publication-membership changes. It validates destination name
collisions, orders drops before renames and additions, and breaks rename cycles with reserved
temporary names. Type/modifier, nullability and default alterations then follow; each operation
contains the exact expected before/after state. Primary-key changes are reported separately.
The deterministic mapping callback must match table creation and writes (default: identity).
Names in the plan are already mapped. Planning does not execute DDL or imply that a target
supports every operation; destinations must validate capabilities before starting a plan.

The pipeline negotiates logical messages and rejects nontransactional DDL. Actual DDL
production requires upstream source helpers/event triggers installed by the operator;
the library never installs database-wide triggers. The pinned unmodified SQL in
`integration/reference/etl` is exercised only in isolated tests. Destination DDL execution,
stored decoding-mask recovery and automatic schema-retention coordination
remain separate work.

## Typed text cells

`value.decode_text(type_oid, array_element_oid?)` converts PostgreSQL text transfer into
`Cell`: bool, signed int2/int4/int8, unsigned oid, float4/float8, exact numeric text, bytea,
UUID bytes, JSON, date/time/timetz/timestamp/timestamptz and one-dimensional arrays of these
types. Text/name/varchar/bpchar and unknown scalar OIDs remain String. Supply a catalog
element OID for custom arrays. Nested arrays are rejected; explicit lower bounds are
validated and discarded, matching the reference's one-dimensional value representation.
Quoted `"NULL"` stays a string while unquoted NULL becomes SqlNull. ToastUnchanged is
distinct from SqlNull and JSON null. Binary transfer is rejected by this text-only API.

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

## Limits and diagnostics

| Option | Default | Meaning |
|---|---:|---|
| max_frame_bytes | 16 MiB | Protocol body length checked before allocation |
| max_transaction_bytes | 64 MiB | Sum of pgoutput message bytes in a transaction |
| max_transaction_events | 100000 | Changes including metadata |
| max_pending_transactions | 16 | Queued plus delivered but unacknowledged transactions |
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
