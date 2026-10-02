# Operating a subscription

Prepare PostgreSQL with `wal_level=logical`, enough replication slots/walsenders, a
publication and a persistent pgoutput logical slot. Use a dedicated role with LOGIN,
REPLICATION and the required database/table permissions. Creating publications is an
administrative setup action; MoonCDC does not silently create or change publications.

```sql
CREATE PUBLICATION app_changes FOR TABLE public.items;
SELECT slot_name, lsn
FROM pg_create_logical_replication_slot('app_slot', 'pgoutput');
```

Record the returned LSN before sending changes. Start `At` that position with a new
checkpoint path, or use `Resume` with an existing checkpoint. There is no initial snapshot;
pre-existing rows need a separate initialization plan. Keep the publication definition
stable across consumer recovery: a publication name alone cannot detect a changed filter
or column list. Such changes require an explicitly coordinated rebuild/new slot.

Use `REPLICA IDENTITY FULL` if old non-key values are required. With default identity,
delete/update events may provide only identity keys or no old tuple. Tables without a
suitable identity can cause PostgreSQL to reject publication updates/deletes.

Monitor PostgreSQL in addition to client diagnostics:

```sql
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, wal_status,
       invalidation_reason,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes
FROM pg_replication_slots WHERE slot_name = 'app_slot';
```

Alert on sustained WAL growth, disk pressure, inactive consumers and invalidation.
Choose `max_slot_wal_keep_size` and storage alarms deliberately: a finite cap can invalidate
a lagging slot, while an unlimited slot can fill the disk. Set client heartbeat below
`wal_sender_timeout` and avoid blocking the MoonBit event loop with long CPU-bound callbacks.

When a frame/transaction/queue limit is reached, business ack does not advance. Diagnose
the oversized workload or slow sink, adjust the appropriate limit and resume. A missing,
invalidated or externally advanced slot requires operator intervention and usually a new
initialization/snapshot plan. Never delete the checkpoint to conceal an error or resume at
the newest WAL position automatically.

To retire a consumer, stop it, verify the slot is inactive and explicitly remove the slot:

```sql
SELECT pg_drop_replication_slot('app_slot');
```

Dropping a slot removes its recovery history. Library cancellation/close deliberately
does not do this. Keep checkpoints and business deduplication state together in backup
and recovery procedures; restoring an old checkpoint after source WAL is released must fail.
