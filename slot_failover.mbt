///|
fn create_slot_sql(
  slot : String,
  temporary~ : Bool,
  export_snapshot~ : Bool,
  failover~ : Bool,
) -> String raise {
  identifier(slot)
  if failover && temporary {
    raise InvalidConfig("failover slots must be persistent")
  }
  let lifetime = if temporary { " TEMPORARY" } else { "" }
  let options = if failover {
    let snapshot = if export_snapshot { "export" } else { "nothing" }
    "(SNAPSHOT '\{snapshot}', FAILOVER)"
  } else if export_snapshot {
    "EXPORT_SNAPSHOT"
  } else {
    "NOEXPORT_SNAPSHOT"
  }
  "CREATE_REPLICATION_SLOT \{slot}\{lifetime} LOGICAL pgoutput \{options}"
}

///|
async fn require_slot_failover(conn : Connection) -> Unit {
  let version = @string.parse_int(
    cell(conn.query("SHOW server_version_num"), 0, 0),
  )
  if version < 170000 {
    raise InvalidConfig("failover slots require PostgreSQL 17 or later")
  }
}

///|
async fn ensure_slot_failover(conn : Connection, source : Source) -> Unit {
  let slot = source.slot
  identifier(slot)
  let rows = conn.query(
    "SELECT plugin,database,temporary::text,failover::text FROM pg_replication_slots WHERE slot_name='\{slot}'",
  )
  if rows.length() != 1 ||
    cell(rows, 0, 0) != "pgoutput" ||
    cell(rows, 0, 1) != source.database ||
    cell(rows, 0, 2) != "false" {
    raise UnsafeResume("missing or incompatible persistent pipeline slot")
  }
  if cell(rows, 0, 3) == "false" {
    ignore(conn.query("ALTER_REPLICATION_SLOT \{slot} (FAILOVER)"))
  }
}

///|
// Copying is persisted before creating this slot. The durable main-slot/table
// identity also owns cleanup after a crash, removal from the publication, or a
// later configuration change that disables failover.
fn failover_copy_slot(source : Source, oid : UInt) -> String {
  copy_slot_prefix(source.slot) + "f\{oid}"
}

///|
async fn cleanup_copy_slots(
  conn : Connection,
  source : Source,
  tables : Array[UInt],
) -> Unit {
  if tables.is_empty() {
    return
  }
  let names = tables
    .map(oid => "'" + failover_copy_slot(source, oid) + "'")
    .join(",")
  let query = "SELECT slot_name,plugin,database,temporary::text,active::text FROM pg_replication_slots WHERE slot_name IN (\{names})"
  let mut rows = conn.query(query)
  while !rows.is_empty() {
    for row in rows {
      if cell([row], 0, 1) != "pgoutput" ||
        cell([row], 0, 2) != source.database ||
        cell([row], 0, 3) != "false" {
        raise UnsafeResume("incompatible owned copy slot")
      }
      if cell([row], 0, 4) == "false" {
        ignore(
          conn.query(
            "SELECT pg_drop_replication_slot('" + cell([row], 0, 0) + "')",
          ),
        )
      }
    }
    @async.sleep(20)
    rows = conn.query(query)
  }
}

///|
async fn cleanup_copy_slot(
  config : ConnectionConfig,
  source : Source,
  oid : UInt,
  pipeline_source_change? : Bool = false,
) -> Unit {
  @async.with_timeout(config.timeout_ms, () => {
    let conn = connect(
      config,
      16777216,
      true,
      worker?=if pipeline_source_change {
        Some(TableWorker(source.slot, oid))
      } else {
        None
      },
    )
    defer conn.close()
    let actual = identify(conn, config, source.slot, source.publication)
    if actual != source {
      if pipeline_source_change &&
        actual.timeline != source.timeline &&
        { ..actual, timeline: source.timeline, } == source {
        // A copy retry may reach the promoted source before main subscription.
        // No slot is queried or dropped here. The outer pipeline still has to
        // join every old worker and prove ancestry, horizon and synchronized slot.
        raise PipelineSourceChanged
      }
      raise UnsafeResume("copy slot cleanup source changed")
    }
    cleanup_copy_slots(conn, source, [oid])
  })
}

///|
// The deterministic slot is still owned by durable table state when the old
// source disappears during COPY cancellation. Do not let exit cleanup replace
// the source-recovery error: the next pipeline entry identifies the new source
// and strictly cleans all recorded copy slots before starting any table. Startup
// and pre-COPY cleanup remain strict, as do authentication and ownership errors.
async fn cleanup_copy_slot_on_exit(
  config : ConnectionConfig,
  source : Source,
  oid : UInt,
) -> Unit {
  cleanup_copy_slot(config, source, oid) catch {
    @async.TimeoutError
    | @io.PipeClosed
    | @io.ReaderClosed
    | @tls.ConnectionClosed
    | @socket.ResolveHostnameError(_) => ()
    @os_error.OSError(code, context~) => {
      let error = @os_error.OSError(code, context~)
      if !error.is_ECONNREFUSED() {
        raise PipelineCritical(error)
      }
    }
    Server(code, message) =>
      if code != "57P01" && code != "57P02" && code != "57P03" {
        raise PipelineCritical(Server(code, message))
      }
    UnsafeResume("copy slot cleanup source changed") => ()
    error => raise PipelineCritical(error)
  }
}
