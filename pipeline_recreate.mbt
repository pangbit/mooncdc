///|
/// Reject lost WAL by default. Explicit recreation resynchronizes every table,
/// honoring the configured initial-copy selection; it cannot restore lost WAL.
pub(all) enum InvalidatedSlotBehavior {
  RejectInvalidatedSlot
  RecreateInvalidatedSlot
} derive(Eq, Debug)

///|
pub extend InvalidatedSlotBehavior with Eq::{equal, not_equal}

///|
pub extend InvalidatedSlotBehavior with Debug::{to_repr}

///|
priv suberror PipelineRecreateRequested {
  PipelineRecreateRequested(Bool)
}

///|
async fn pipeline_slot_invalidated(conn : Connection, source : Source) -> Bool {
  let slot = source.slot
  let query = "SELECT plugin,database,temporary::text,active::text,wal_status,invalidation_reason FROM pg_replication_slots WHERE slot_name='\{slot}'"
  let mut rows = conn.query(query)
  while rows.length() == 1 && cell(rows, 0, 3) == "true" {
    @async.sleep(20)
    rows = conn.query(query)
  }
  if rows.length() != 1 ||
    cell(rows, 0, 0) != "pgoutput" ||
    cell(rows, 0, 1) != source.database ||
    cell(rows, 0, 2) != "false" {
    raise UnsafeResume("missing or incompatible persistent pipeline slot")
  }
  cell(rows, 0, 4) == "lost" || rows[0].get(5) != Some(None)
}

///|
// Version 2 is a durable whole-pipeline resynchronization intent, never a normal
// running state. A crash before completion repeats creation before any target work.
async fn PipelineStore::recreate_slot(
  self : PipelineStore,
  conn : Connection,
  failover : Bool,
  boundary~ : async (String) -> Unit = _ => (),
) -> Unit {
  let path = self.path + ".checkpoint"
  let checkpoint_lock = record_lock(path, self.session)
  defer (if checkpoint_lock is Some(lock) { lock.close() })
  let source = self.state.source
  // Corruption and source mismatch remain fatal even during explicit recreation.
  let previous = match load_record(path, ReplicationCheckpoint, self.session) {
    Some(bytes) => Some(parse_checkpoint(bytes, source))
    None => {
      if self.state.version == 1 && self.state.checkpoint_initialized {
        raise CheckpointError("pipeline checkpoint is missing")
      }
      None
    }
  }
  self.update(state => {
    ..state,
    version: 2,
    initial_lsn: None,
    checkpoint_initialized: false,
    promotion: None,
    tables: state.tables.map(table => {
      ..table,
      phase: Pending,
      decoding: None,
    }),
  })
  boundary("after_intent")
  cleanup_copy_slots(conn, source, self.state.tables.map(table => table.oid))
  let slot = source.slot
  let query = "SELECT plugin,database,temporary::text,active::text FROM pg_replication_slots WHERE slot_name='\{slot}'"
  let mut rows = conn.query(query)
  while rows.length() == 1 && cell(rows, 0, 3) == "true" {
    @async.sleep(20)
    rows = conn.query(query)
  }
  if !rows.is_empty() {
    if rows.length() != 1 ||
      cell(rows, 0, 0) != "pgoutput" ||
      cell(rows, 0, 1) != source.database ||
      cell(rows, 0, 2) != "false" {
      raise UnsafeResume("incompatible owned pipeline slot")
    }
    ignore(conn.query("SELECT pg_drop_replication_slot('\{slot}')"))
  }
  boundary("after_drop")
  let created = conn.query(
    create_slot_sql(slot, temporary=false, export_snapshot=false, failover~),
  )
  if cell(created, 0, 0) != slot || cell(created, 0, 3) != "pgoutput" {
    raise Protocol("pipeline slot recreation metadata")
  }
  let lsn = Lsn::parse(cell(created, 0, 1))
  if previous is Some(previous) && previous > lsn {
    raise UnsafeResume("recreated slot precedes durable consumer position")
  }
  boundary("after_create")
  self.update(state => {
    ..state,
    initial_lsn: Some(lsn.to_string()),
    write_horizon: Some(lsn.to_string()),
  })
  boundary("after_position")
  store_checkpoint(path, source, lsn, self.session)
  boundary("after_checkpoint")
  self.update(state => { ..state, version: 1, checkpoint_initialized: true, })
  boundary("after_complete")
}
