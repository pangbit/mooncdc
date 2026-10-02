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

`Change` supports RelationChanged, TypeChanged, Insert, Update, Delete, Truncate and Origin.
Every row change includes an immutable Relation snapshot with columns, key flags, type
OIDs and type modifiers. Schema changes are reflected in subsequent relation metadata;
this is not a complete DDL stream. Custom types retain OID/name metadata and text values.

`OldTuple.Key` contains a replica-identity tuple; non-key positions can be NULL placeholders.
`OldTuple.Full` describes an old row under FULL identity. Updates can have no old tuple.
Consumers needing arbitrary old values should configure FULL and still handle unavailable
TOAST values. `UnchangedToast` means retain the previously known value, or fetch/rebuild
state if it is unavailable. It never means SQL NULL. Truncate can affect multiple relations
and carries cascade/restart-identity flags.

Only protocol v1, text transfer, committed transactions and UTF-8 client encoding are
supported. Streaming large in-progress transactions, binary fields, two-phase transactions,
failover and initial snapshots are outside scope. Unknown messages fail explicitly.

## Limits and diagnostics

| Option | Default | Meaning |
|---|---:|---|
| max_frame_bytes | 16 MiB | Protocol body length checked before allocation |
| max_transaction_bytes | 64 MiB | Sum of pgoutput message bytes in a transaction |
| max_transaction_events | 100000 | Changes including metadata |
| max_pending_transactions | 16 | Queued plus delivered but unacknowledged transactions |
| max_metadata_entries | 4096 | Maximum relations and types, separately |
| heartbeat_ms | 1000 | Feedback interval; choose well below server wal_sender_timeout |
| max_reconnects | 3 | Total reconnect attempts per subscription |
| reconnect_delay_ms | 250 | Delay between attempts |

These bound retained protocol data, not exact heap/RSS. Decoded strings/objects add overhead.
The reader may assemble one additional bounded transaction before detecting queue overflow.
Metadata entries are individually bounded by frame size. A full queue fails; it never drops
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
