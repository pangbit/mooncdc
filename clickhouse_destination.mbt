///|
priv struct ChTable {
  schema : ReplicatedSchema
  name : String
  layout : ChLayout
}

///|
/// Synchronous ClickHouse destination. Requires run_pipeline with a StateStore
/// and the same SchemaStore supplied here. ReplacingMergeTree is the default.
/// Supported schema transitions retain durable endpoints for interrupted DDL.
pub struct ClickHouseDestination {
  priv http : ChHttp
  priv database : String
  priv publication : String
  priv schemas : &SchemaStore
  priv engine : ClickHouseEngine
  priv max_insert_bytes : Int
  priv mutex : @async.Mutex
  priv mut session : StateSession?
  priv mut running : Bool
  priv copies : Map[UInt, ReplicatedSchema]
  priv attempts : Map[UInt, String]
  priv tables : Map[UInt, ChTable]
}

///|
pub extend ClickHouseDestination with Destination::{
  bind_state,
  bind_snapshot_schema,
  startup,
  shutdown,
  reset_table,
  write_rows,
  write_transaction,
  flush,
}

///|
/// The database must already exist. Credentials are sent in HTTP headers;
/// HTTPS uses system trust. Use a dedicated StateStore namespace for this target.
pub fn ClickHouseDestination::new(
  endpoint~ : String,
  user~ : String,
  password~ : String,
  database~ : String,
  publication~ : String,
  schema_store~ : &SchemaStore,
  engine? : ClickHouseEngine = ReplacingMergeTree,
  max_insert_bytes? : Int = 67108864,
  timeout_ms? : Int = 260000,
  connectivity_timeout_ms? : Int = 8000,
  schema_timeout_ms? : Int = 16000,
  ddl_timeout_ms? : Int = 128000,
  insert_timeout_ms? : Int = 256000,
  client_timeout_epsilon_ms? : Int = 4000,
) -> ClickHouseDestination raise {
  if database.is_empty() || database.contains("\u0000") || max_insert_bytes <= 0 {
    raise InvalidConfig("invalid ClickHouse database or insert limit")
  }
  identifier(publication)
  {
    http: ChHttp::new(
      endpoint,
      user,
      password,
      timeout_ms,
      connectivity_ms=connectivity_timeout_ms,
      schema_ms=schema_timeout_ms,
      ddl_ms=ddl_timeout_ms,
      insert_ms=insert_timeout_ms,
      epsilon_ms=client_timeout_epsilon_ms,
    ),
    database,
    publication,
    schemas: schema_store,
    engine,
    max_insert_bytes,
    mutex: @async.Mutex(),
    session: None,
    running: false,
    copies: Map([]),
    attempts: Map([]),
    tables: Map([]),
  }
}

///|
fn ClickHouseDestination::state(
  self : ClickHouseDestination,
) -> StateSession raise {
  guard self.session is Some(session) else {
    raise InvalidConfig("ClickHouse requires an active StateStore session")
  }
  session.check()
  session
}

///|
fn ClickHouseDestination::qualified(
  self : ClickHouseDestination,
  name : String,
) -> String {
  ch_quote(self.database, '"') + "." + ch_quote(name, '"')
}

///|
pub impl Destination for ClickHouseDestination with fn bind_state(self, session) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.running {
    raise InvalidConfig("ClickHouse destination is already running")
  }
  session.check()
  self.session = Some(session)
  self.tables.clear()
  self.copies.clear()
  self.attempts.clear()
}

///|
pub impl Destination for ClickHouseDestination with fn startup(self) {
  self.mutex.acquire()
  defer self.mutex.release()
  ignore(self.state())
  let version = self.http
    .query("SELECT version()", operation=Connectivity)
    .trim()
    .split(".")
    .to_array()
  if version.length() < 2 {
    raise Protocol("invalid ClickHouse server version")
  }
  let major = @string.parse_int(version[0])
  let minor = @string.parse_int(version[1])
  if self.engine == ReplacingMergeTree &&
    (major < 23 || (major == 23 && minor < 5)) {
    raise InvalidConfig("ReplacingMergeTree requires ClickHouse 23.5 or later")
  }
  if self.http
    .query(
      "EXISTS DATABASE " + ch_quote(self.database, '"'),
      operation=Connectivity,
    )
    .trim() !=
    "1" {
    raise InvalidConfig("ClickHouse database does not exist")
  }
  self.running = true
}

///|
pub impl Destination for ClickHouseDestination with fn shutdown(self) {
  self.mutex.acquire()
  defer self.mutex.release()
  self.running = false
  self.session = None
  self.tables.clear()
  self.copies.clear()
  self.attempts.clear()
}

///|
fn ClickHouseDestination::check(self : ClickHouseDestination) -> Unit raise {
  ignore(self.state())
  if !self.running {
    raise Closed
  }
}

///|
pub impl Destination for ClickHouseDestination with fn bind_snapshot_schema(
  self,
  relation,
  schema,
) {
  self.mutex.acquire()
  defer self.mutex.release()
  self.check()
  let projected = schema.project(relation)
  ch_validate_schema(projected, self.engine)
  self.copies[relation.oid] = projected
}

///|
pub impl Destination for ClickHouseDestination with fn reset_table(
  self,
  relation,
  attempt~,
) {
  self.mutex.acquire()
  defer self.mutex.release()
  self.check()
  guard self.copies.get(relation.oid) is Some(schema) else {
    raise InvalidConfig("ClickHouse copy requires snapshot schema binding")
  }
  if attempt.is_empty() {
    raise InvalidConfig("empty ClickHouse copy attempt")
  }
  let name = ch_table_name(
    schema.schema.namespace_,
    schema.schema.name,
    self.engine,
  )
  let previous = self.stored_record(relation.oid)
  let drop_name = match previous {
    Some(stored) => self.record_name(stored)
    None => name
  }
  if self.engine == ReplacingMergeTree {
    ignore(
      self.http.query(
        "DROP VIEW IF EXISTS " + self.qualified(drop_name + "__current"),
      ),
    )
  }
  ignore(self.http.query("DROP TABLE IF EXISTS " + self.qualified(drop_name)))
  self.tables.remove(relation.oid)
  self.attempts.remove(relation.oid)
  // Persist the new physical name before creation. A reset interrupted after a
  // source rename must subsequently drop this new table, not only the old one.
  self
  .state()
  .put_metadata(
    relation.oid,
    ToJson::to_json({
      ..self.schema_record(schema, name),
      creating: Some(true),
    }),
  )
  self.create_table(schema, name)
  let table = self.load_layout(schema, name)
  self.save_table(table)
  self.attempts[relation.oid] = attempt
}

///|
pub impl Destination for ClickHouseDestination with fn write_rows(
  self,
  relation,
  rows,
  id,
) {
  self.mutex.acquire()
  defer self.mutex.release()
  self.check()
  guard self.tables.get(relation.oid) is Some(table) &&
    self.attempts.get(relation.oid) is Some(attempt) else {
    raise UnsafeResume("ClickHouse copy has no active attempt")
  }
  if !rows.is_empty() {
    guard id is Some(id) &&
      id.attempt == attempt &&
      id.relation_id == relation.oid else {
      raise UnsafeResume("stale ClickHouse copy batch")
    }
    let encoded = rows.map(row => {
      table.layout.row(
        table.schema.decode_row(row),
        self.engine,
        "INSERT",
        0,
        0,
      )
    })
    self.http.insert(
      self.qualified(table.name),
      table.layout,
      encoded,
      self.max_insert_bytes,
      Some("\{id.attempt}/\{id.relation_id}/\{id.sequence}"),
    )
  }
  Durable
}

///|
pub impl Destination for ClickHouseDestination with fn flush(self) {
  self.mutex.acquire()
  defer self.mutex.release()
  self.check()
}

///|
pub impl Destination for ClickHouseDestination with fn write_transaction(
  self,
  tx,
) {
  self.mutex.acquire()
  defer self.mutex.release()
  self.check()
  let current : Map[UInt, SchemaSnapshot] = Map([])
  for index, change in tx.changes {
    if change is Message(message) {
      if message.schema_snapshot(
          commit_lsn=tx.commit_lsn,
          publication=self.publication,
        )
        is Some(schema) {
        current[schema.oid] = schema
      }
      continue
    }
    match change {
      RelationChanged(relation) =>
        ignore(self.resolve_table(relation, tx.commit_lsn, current))
      Insert(relation, values) | Update(relation, _, values) => {
        let table = self.resolve_table(relation, tx.commit_lsn, current)
        let rows = match change {
          Update(_, old, _) => ch_update_rows(table.schema, old, values)
          _ => [("INSERT", table.schema.decode_row(values))]
        }
        self.insert_events(
          table,
          rows,
          tx.commit_lsn.value,
          ch_event_ordinal(tx, index),
        )
      }
      Delete(relation, old) => {
        let table = self.resolve_table(relation, tx.commit_lsn, current)
        self.insert_events(
          table,
          [("DELETE", ch_old_row(table.schema, old))],
          tx.commit_lsn.value,
          ch_event_ordinal(tx, index),
        )
      }
      Truncate(relations, ..) =>
        for relation in relations {
          let table = self.resolve_table(relation, tx.commit_lsn, current)
          ignore(
            self.http.query("TRUNCATE TABLE " + self.qualified(table.name)),
          )
        }
      _ => ()
    }
  }
  Durable
}

///|
async fn ClickHouseDestination::insert_events(
  self : ClickHouseDestination,
  table : ChTable,
  rows : Array[(String, ReadOnlyArray[Cell])],
  lsn : UInt64,
  ordinal : UInt64,
) -> Unit {
  errdefer self.tables.remove(table.schema.schema.oid)
  let encoded = rows.map(row => {
    table.layout.row(row.1, self.engine, row.0, lsn, ordinal)
  })
  self.http.insert(
    self.qualified(table.name),
    table.layout,
    ReadOnlyArray::from_array(encoded),
    self.max_insert_bytes,
    None,
  )
}
