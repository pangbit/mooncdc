///|
/// Returned only after every snapshot callback succeeds. Keep target writes durable
/// before returning from callbacks, then subscribe with AfterSnapshot(position).
pub struct SnapshotPosition {
  lsn : Lsn
  priv source : Source
}

///|
/// TableBegin and TableEnd are delivered even for empty tables. Snapshot rows use
/// text transfer; SQL NULL remains Null. Callbacks run sequentially with backpressure.
pub(all) enum SnapshotEvent {
  TableBegin(Relation)
  TableRows(Relation, ReadOnlyArray[ReadOnlyArray[Value]])
  TableEnd(Relation)
} derive(Debug)

///|
fn sql_identifier(value : String) -> String raise {
  if value.is_empty() || value.contains("\u0000") {
    raise InvalidConfig("empty or NUL SQL identifier")
  }
  "\"" + value.replace_all(old="\"", new="\"\"") + "\""
}

///|
fn sql_literal(value : String) -> String raise {
  if value.contains("\u0000") {
    raise InvalidConfig("NUL SQL literal")
  }
  "E'" +
  value.replace_all(old="\\", new="\\\\").replace_all(old="'", new="''") +
  "'"
}

///|
async fn Connection::snapshot_query(
  self : Connection,
  sql : String,
  timeout : Int,
  max_bytes : Int,
) -> Array[Array[String?]] {
  @async.with_timeout(timeout, () => self.query(sql, max_result_bytes=max_bytes))
}

///|
/// Copy a publication from an exported consistent snapshot and retain its new
/// persistent slot for incremental consumption. The slot must not already exist.
/// On failure no position/checkpoint is published; the slot is retained for explicit
/// operator recovery. Schema/publication changes during copying are unsupported.
pub async fn copy_snapshot(
  connection : ConnectionConfig,
  slot~ : String,
  publication~ : String,
  consume : async (SnapshotEvent) -> Unit,
  batch_rows? : Int = 256,
  max_frame_bytes? : Int = 16777216,
  max_batch_bytes? : Int = 67108864,
) -> SnapshotPosition {
  copy_snapshot_impl(
    connection,
    slot~,
    publication~,
    consume,
    batch_rows~,
    max_frame_bytes~,
    max_batch_bytes~,
    temporary=false,
    table=None,
  )
}

///|
async fn copy_snapshot_impl(
  connection : ConnectionConfig,
  slot~ : String,
  publication~ : String,
  consume : async (SnapshotEvent) -> Unit,
  batch_rows~ : Int,
  max_frame_bytes~ : Int,
  max_batch_bytes~ : Int,
  temporary~ : Bool,
  table~ : UInt?,
  schema_store? : &SchemaStore,
  after_copy? : async (Connection, SnapshotPosition) -> Unit,
  on_schema? : async (Relation, SchemaSnapshot) -> Unit,
) -> SnapshotPosition {
  identifier(slot)
  identifier(publication)
  if batch_rows <= 0 ||
    batch_rows > 1024 ||
    max_frame_bytes < 64 ||
    max_frame_bytes > 1073741824 ||
    max_batch_bytes < max_frame_bytes {
    raise InvalidConfig("invalid snapshot row/frame/batch limit")
  }
  let exporter = connect(connection, max_frame_bytes, true)
  defer exporter.close()
  let source = @async.with_timeout(connection.timeout_ms, () => {
    identify(exporter, connection, slot, publication)
  })
  let pubs = exporter.snapshot_query(
    "SELECT pubname FROM pg_publication WHERE pubname = '\{publication}'",
    connection.timeout_ms,
    max_batch_bytes,
  )
  if pubs.length() != 1 {
    raise UnsafeResume("publication missing")
  }
  let lifetime = if temporary { " TEMPORARY" } else { "" }
  let created = exporter.snapshot_query(
    "CREATE_REPLICATION_SLOT \{slot}\{lifetime} LOGICAL pgoutput EXPORT_SNAPSHOT",
    connection.timeout_ms,
    max_batch_bytes,
  )
  if created.length() != 1 ||
    cell(created, 0, 0) != slot ||
    cell(created, 0, 3) != "pgoutput" {
    raise Protocol("invalid exported slot metadata")
  }
  let lsn = Lsn::parse(cell(created, 0, 1))
  let snapshot = sql_literal(cell(created, 0, 2))
  let reader = connect(connection, max_frame_bytes, false)
  defer reader.close()
  ignore(
    reader.snapshot_query(
      "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT \{snapshot}; SET LOCAL search_path = pg_catalog; SET LOCAL row_security = off",
      connection.timeout_ms,
      max_batch_bytes,
    ),
  )
  // No commands are sent on the exporter after CREATE_REPLICATION_SLOT: they
  // would invalidate the exported snapshot. The reader owns the imported view.
  let selected = match table {
    Some(oid) => " AND c.oid = \{oid}"
    None => ""
  }
  let tables = reader.snapshot_query(
    "SELECT p.schemaname, p.tablename, p.rowfilter FROM pg_publication_tables p JOIN pg_namespace n ON n.nspname=p.schemaname JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=p.tablename WHERE p.pubname = '\{publication}'\{selected} ORDER BY p.schemaname, p.tablename",
    connection.timeout_ms,
    max_batch_bytes,
  )
  if table is Some(_) && tables.length() != 1 {
    raise UnsafeResume("snapshot table is no longer published")
  }
  for table in tables {
    let namespace_ = cell([table], 0, 0)
    let name = cell([table], 0, 1)
    let qualified = sql_identifier(namespace_) + "." + sql_identifier(name)
    let regclass = sql_literal(qualified) + "::regclass"
    ignore(
      reader.snapshot_query(
        "LOCK TABLE \{qualified} IN ACCESS SHARE MODE",
        connection.timeout_ms,
        max_batch_bytes,
      ),
    )
    let meta = reader.snapshot_query(
      "SELECT oid::text, relreplident::text, relkind::text FROM pg_class WHERE oid = \{regclass}",
      connection.timeout_ms,
      max_batch_bytes,
    )
    let cols = reader.snapshot_query(
      "SELECT a.attname, a.atttypid::text, a.atttypmod::text, (c.relreplident = 'f' OR EXISTS (SELECT 1 FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, pos) WHERE i.indrelid = c.oid AND (i.indisreplident OR (c.relreplident = 'd' AND i.indisprimary)) AND k.pos <= i.indnkeyatts AND a.attnum = k.attnum))::text FROM pg_publication_tables p JOIN pg_namespace n ON n.nspname = p.schemaname JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = p.tablename JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = ANY(p.attnames) WHERE p.pubname = '\{publication}' AND c.oid = \{regclass} ORDER BY a.attnum",
      connection.timeout_ms,
      max_batch_bytes,
    )
    let columns : Array[Column] = cols.map(row => {
      name: cell([row], 0, 0),
      type_oid: @string.parse_uint(cell([row], 0, 1)),
      type_modifier: @string.parse_int(cell([row], 0, 2)),
      key: cell([row], 0, 3) == "true",
    })
    let identity = @utf8.encode(cell(meta, 0, 1))
    if identity.length() != 1 {
      raise Protocol("invalid replica identity")
    }
    let relation : Relation = {
      oid: @string.parse_uint(cell(meta, 0, 0)),
      namespace_,
      name,
      replica_identity: identity[0],
      columns: ReadOnlyArray::from_array(columns),
    }
    if schema_store is Some(store) {
      let schema = reader.snapshot_schema(
        relation.oid,
        lsn,
        connection.timeout_ms,
        max_batch_bytes,
      )
      store.put(schema)
      if on_schema is Some(bind) {
        bind(relation, schema)
      }
    }
    let projection = columns.map(c => sql_identifier(c.name)).join(", ")
    // Inheritance children are listed separately, so ONLY avoids copying them
    // twice. Partition roots expand when selected by publish_via_partition_root.
    let only = if cell(meta, 0, 2) == "p" { "" } else { "ONLY " }
    let filter = match table.get(2) {
      Some(Some(expression)) => " WHERE " + expression
      _ => ""
    }
    ignore(
      reader.snapshot_query(
        "DECLARE mooncdc_snapshot NO SCROLL CURSOR FOR SELECT \{projection} FROM \{only}\{qualified}\{filter}",
        connection.timeout_ms,
        max_batch_bytes,
      ),
    )
    consume(TableBegin(relation))
    for ;; {
      let rows = reader.snapshot_query(
        "FETCH FORWARD \{batch_rows} FROM mooncdc_snapshot",
        connection.timeout_ms,
        max_batch_bytes,
      )
      if rows.is_empty() {
        break
      }
      let values : Array[ReadOnlyArray[Value]] = rows.map(row => {
        if row.length() != columns.length() {
          raise Protocol("snapshot tuple column count")
        }
        ReadOnlyArray::from_array(
          row.map(value => {
            match value {
              Some(text) => Text(text)
              None => Null
            }
          }),
        )
      })
      consume(TableRows(relation, ReadOnlyArray::from_array(values)))
    }
    ignore(
      reader.snapshot_query(
        "CLOSE mooncdc_snapshot",
        connection.timeout_ms,
        max_batch_bytes,
      ),
    )
    consume(TableEnd(relation))
  }
  ignore(
    reader.snapshot_query("COMMIT", connection.timeout_ms, max_batch_bytes),
  )
  let position : SnapshotPosition = { lsn, source, }
  if after_copy is Some(finish) {
    finish(exporter, position)
  }
  position
}
