///|
/// pgoutput v2 buffers streamed, possibly aborted transactions until Stream Commit.
/// V1 remains the default and the pipeline protocol.
pub(all) enum PgOutputProtocol {
  V1
  V2Streaming
} derive(Eq, Debug)

///|
pub extend PgOutputProtocol with Eq::{equal, not_equal}

///|
pub extend PgOutputProtocol with Debug::{to_repr}

///|
fn identifier(value : String) -> Unit raise {
  if value.is_empty() || value.length() > 63 {
    raise InvalidConfig("identifier length")
  }
  for c in value {
    if !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '_' {
      raise InvalidConfig(
        "slot and publication require lowercase ASCII identifiers",
      )
    }
  }
}

///|
fn cell(rows : Array[Array[String?]], row : Int32, col : Int32) -> String raise {
  guard rows.get(row) is Some(r) && r.get(col) is Some(Some(value)) else {
    raise UnsafeResume("missing source/slot metadata")
  }
  value
}

///|
async fn identify(
  conn : Connection,
  config : ConnectionConfig,
  slot : String,
  publication : String,
) -> Source {
  let rows = conn.query("IDENTIFY_SYSTEM")
  let db = cell(rows, 0, 3)
  if db != config.database {
    raise UnsafeResume("database mismatch")
  }
  {
    system_id: cell(rows, 0, 0),
    timeline: cell(rows, 0, 1),
    database: db,
    slot,
    publication,
  }
}

///|
async fn start_replication(
  conn : Connection,
  source : Source,
  durable : Lsn,
  start : Lsn,
  binary~ : Bool = false,
  messages~ : Bool = false,
  protocol~ : PgOutputProtocol = V1,
) -> Unit {
  validate_replication_position(conn, source, durable, start)
  begin_replication(conn, source, start, binary~, messages~, protocol~)
}

///|
async fn validate_replication_position(
  conn : Connection,
  source : Source,
  durable : Lsn,
  start : Lsn,
) -> Unit {
  // A logical replication connection in database mode permits SQL before COPY BOTH.
  let query = "SELECT plugin, database, restart_lsn::text, confirmed_flush_lsn::text, wal_status, invalidation_reason, active::text FROM pg_replication_slots WHERE slot_name = '\{source.slot}'"
  let mut rows = conn.query(query)
  // Socket close returns before the server necessarily releases its slot. The
  // caller's connection timeout bounds this wait, including another active owner.
  while rows.length() == 1 && cell(rows, 0, 6) == "true" {
    @async.sleep(20)
    rows = conn.query(query)
  }
  if rows.length() != 1 ||
    cell(rows, 0, 0) != "pgoutput" ||
    cell(rows, 0, 1) != source.database {
    raise UnsafeResume("missing or incompatible replication slot")
  }
  if cell(rows, 0, 4) == "lost" || rows[0].get(5) != Some(None) {
    raise UnsafeResume("replication slot invalidated")
  }
  if Lsn::parse(cell(rows, 0, 2)) > durable ||
    Lsn::parse(cell(rows, 0, 3)) > durable {
    raise UnsafeResume("slot has advanced beyond durable consumer position")
  }
  let pubs = conn.query(
    "SELECT pubname FROM pg_publication WHERE pubname = '\{source.publication}'",
  )
  if pubs.length() != 1 {
    raise UnsafeResume("publication missing")
  }
  let current = conn.query("IDENTIFY_SYSTEM")
  if start > Lsn::parse(cell(current, 0, 2)) {
    raise UnsafeResume("start position is ahead of server WAL")
  }
}

///|
// Also used on the connection that owns a newly created temporary snapshot slot.
// Its active=true status is expected; no unrelated connection may bypass preflight.
async fn begin_replication(
  conn : Connection,
  source : Source,
  start : Lsn,
  binary~ : Bool,
  messages~ : Bool,
  protocol~ : PgOutputProtocol = V1,
) -> Unit {
  let out = @buffer.Buffer()
  let transfer = if binary { ", binary 'true'" } else { "" }
  let logical = if messages { ", messages 'true'" } else { "" }
  let (version, streaming) = match protocol {
    V1 => ("1", "")
    V2Streaming => ("2", ", streaming 'on'")
  }
  cstring(
    out,
    "START_REPLICATION SLOT \{source.slot} LOGICAL \{start.to_string()} (proto_version '\{version}', publication_names '\{source.publication}'\{transfer}\{logical}\{streaming})",
  )
  conn.send(b'Q', out.to_bytes())
  let (tag, body) = conn.read()
  if tag == b'E' {
    raise server_error(body)
  }
  if tag != b'W' || body != b"\x00\x00\x00" {
    raise Protocol("expected text COPY BOTH")
  }
}

///|
async fn Connection::feedback(
  self : Connection,
  durable : Lsn,
  request_reply~ : Bool = false,
) -> Unit {
  let out = @buffer.Buffer()
  out.write_byte(b'r')
  // Receive/write/flush/apply are conservatively the business-durable position.
  for _ in 0..<3 {
    out.write_uint64_be(durable.value)
  }
  out.write_int64_be((@async.now() - 946684800000L) * 1000L)
  out.write_byte(if request_reply { b'\x01' } else { b'\x00' })
  self.send(b'd', out.to_bytes())
}
