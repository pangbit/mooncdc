# Executable examples

These programs operate only on the repository's isolated Docker fixture. They create
and remove their own `example_*` tables, publications and slots. Start the services:

```sh
docker compose -p mooncdc-test -f integration/compose.yaml up -d --wait
moon run examples/index
moon run examples/cache
moon run tools/recovery-test.mbtx
```

Set `MOONCDC_EXAMPLE_SERVICE=pg17` to run the same examples against PostgreSQL 17;
the default is `pg18`. Run from the repository root. Docker is used only for setup
SQL; replication traffic flows through the public MoonCDC library's native TCP client.

## Index

[`index/main.mbt`](index/main.mbt) subscribes before submitting an insert/update/delete
transaction and a rollback. It applies the complete committed transaction to a map,
deduplicates by transaction ID, deliberately applies the transaction twice, and compares
the final map with a SQL query. The rolled-back row must never enter the index.

The in-memory index and its deduplication set are process-local. The example always starts
from a fresh empty table/slot; do not reuse its checkpoint after losing the map. A persistent
application must atomically store index changes plus transaction ID before acknowledging.

## Cache invalidation

[`cache/main.mbt`](cache/main.mbt) seeds two database rows and three cache keys. A committed
update/delete invalidates only the two affected keys; the unrelated key remains. The fixture
uses `REPLICA IDENTITY FULL`. If old identity is unavailable, the example fails with an
explanation rather than pretending it can safely invalidate a prior key.

## Durable event file and restart

[`resume/main.mbt`](resume/main.mbt) writes complete transactions plus unique business IDs
to `events.json`. It writes a temporary file, fsyncs it, renames it and fsyncs the parent
before acknowledging. Replayed transaction IDs are deduplicated. This intentionally small
example rewrites the journal; a production sink should use a transactional store or bounded
append log with crash recovery.

`tools/recovery-test.mbtx` builds the native executable and, for each boundary, starts a
separate consumer process, waits for a durable ready marker, sends SIGKILL, then starts a
fresh recovery process. It checks the exit, exact final business IDs and expected duplicates:

| Kill boundary | Expected replay duplicates in event file writer |
|---|---:|
| Before business persistence | 0 |
| After business persistence, before ack | 1 (deduplicated) |
| After durable ack, before feedback | 0 |
| After verified server feedback | 0 |

The script cleans its own example objects and temporary files. It never drops another slot.
The public API never couples business-file persistence and source feedback into an exactly-once
transaction; the duplicate in the second row is part of the intended at-least-once contract.
