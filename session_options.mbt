///|
priv enum SessionWorkload {
  Queries
  Replication
  SnapshotReader
  SnapshotExporter
  StateStore
  SourceMigrations
}

///|
priv enum SessionWorker {
  ApplyWorker(String)
  TableWorker(String, UInt32)
}

///|
// Keep frame diagnostics independent of application names, slot IDs and OIDs.
// Only these internally selected roles can enter the diagnostic text.
priv enum FrameConnectionRole {
  Unknown
  Workload(SessionWorkload)
  Apply(SessionWorkload)
  TableSync(SessionWorkload)
}

///|
fn frame_connection_role(
  workload : SessionWorkload,
  worker : SessionWorker?,
) -> FrameConnectionRole {
  match worker {
    None => Workload(workload)
    Some(ApplyWorker(_)) => Apply(workload)
    Some(TableWorker(_, _)) => TableSync(workload)
  }
}

///|
fn FrameConnectionRole::description(self : FrameConnectionRole) -> String {
  let (workload, worker) = match self {
    Unknown => return "unknown worker=unspecified"
    Workload(workload) => (workload, "unspecified")
    Apply(workload) => (workload, "apply")
    TableSync(workload) => (workload, "table_sync")
  }
  let role = match workload {
    Queries => "queries"
    Replication => "replication"
    SnapshotReader => "snapshot_reader"
    SnapshotExporter => "snapshot_exporter"
    StateStore => "state_store"
    SourceMigrations => "source_migrations"
  }
  "\{role} worker=\{worker}"
}

///|
fn SessionWorker::suffix(self : SessionWorker) -> String {
  let slot = match self {
    ApplyWorker(slot) | TableWorker(slot, _) => slot
  }
  let digest = @crypto.bytes_to_hex_string(sha256(@utf8.encode(slot)))
  let pipeline = digest[:16].to_owned()
  match self {
    ApplyWorker(_) => ":apply:\{pipeline}"
    TableWorker(_, oid) => ":table_sync:\{pipeline}:\{oid}"
  }
}

///|
fn SessionWorkload::settings(
  self : SessionWorkload,
  worker? : SessionWorker,
) -> (String, String) {
  let (name, statement, lock, idle) = match self {
    Queries => ("mooncdc_queries", 30000, 10000, 60000)
    StateStore => ("mooncdc_state", 30000, 10000, 60000)
    SourceMigrations => ("mooncdc_source_migrations", 30000, 10000, 60000)
    Replication => ("mooncdc_replication", 0, 30000, 0)
    SnapshotReader => ("mooncdc_snapshot", 0, 30000, 0)
    // Exporter cannot execute another statement after exporting its snapshot.
    // The reader uses a separate connection with the normal streaming lock bound.
    SnapshotExporter => ("mooncdc_snapshot_export", 0, 0, 0)
  }
  let suffix = worker.map(worker => worker.suffix()).unwrap_or("")
  // Both the fixed workload bases and generated worker suffixes are ASCII.
  // Preserve the complete identity within PostgreSQL's 63-byte name limit.
  let length = Int32::min(name.length(), 63 - suffix.length())
  (
    name[:length].to_owned() + suffix,
    "-c DateStyle=ISO,YMD -c TimeZone=UTC -c bytea_output=hex -c extra_float_digits=3 -c IntervalStyle=postgres -c statement_timeout=\{statement} -c lock_timeout=\{lock} -c idle_in_transaction_session_timeout=\{idle}",
  )
}
