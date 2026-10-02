///|
/// PostgreSQL durable state records. acquire creates the record table if needed
/// and holds a session advisory lock for this namespace. It never reconnects a
/// lost owner session; release and a new acquisition are required after failure.
pub struct PostgresStateStore {
  priv config : ConnectionConfig
  priv table : String
  priv store_id : String
  priv mut connection : Connection?
  priv mut owner : String?
  priv mut poisoned : Bool
  priv mutex : @async.Mutex
}

///|
pub fn PostgresStateStore::new(
  config : ConnectionConfig,
  store_id~ : String,
  schema? : String = "public",
  table? : String = "mooncdc_state",
) -> PostgresStateStore raise {
  validate_connection(config)
  if store_id.is_empty() {
    raise InvalidConfig("empty state namespace")
  }
  ignore(sql_literal(store_id))
  {
    config,
    table: sql_identifier(schema) + "." + sql_identifier(table),
    store_id,
    connection: None,
    owner: None,
    poisoned: false,
    mutex: @async.Mutex(),
  }
}

///|
pub impl StateStore for PostgresStateStore with fn acquire(self, owner) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.owner is Some(_) {
    raise CheckpointError("state store already in use")
  }
  let conn = connect(self.config, 33554432, false)
  errdefer conn.close()
  @async.with_timeout(self.config.timeout_ms, () => {
    ignore(conn.query("SET synchronous_commit=on; SET search_path=pg_catalog"))
    let lock_key = sql_literal(
      "mooncdc-state/owner/" + self.table + "/" + self.store_id,
    )
    let rows = conn.query(
      "SELECT pg_try_advisory_lock(hashtextextended(\{lock_key},0))::text",
    )
    if cell(rows, 0, 0) != "true" {
      raise CheckpointError("state store already in use")
    }
    let ddl_key = sql_literal("mooncdc-state/ddl/" + self.table)
    ignore(
      conn.query(
        "BEGIN; SELECT pg_advisory_xact_lock(hashtextextended(\{ddl_key},0)); CREATE TABLE IF NOT EXISTS \{self.table}(namespace text NOT NULL,record text NOT NULL,content text NOT NULL,PRIMARY KEY(namespace,record)); COMMIT",
      ),
    )
  })
  self.connection = Some(conn)
  self.owner = Some(owner)
  self.poisoned = false
}

///|
pub impl StateStore for PostgresStateStore with fn release(self, owner) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.owner != Some(owner) {
    raise CheckpointError("stale state owner")
  }
  if self.connection is Some(conn) {
    conn.close()
  }
  self.connection = None
  self.owner = None
  self.poisoned = false
}

///|
async fn PostgresStateStore::query_owned(
  self : PostgresStateStore,
  sql : String,
) -> Array[Array[String?]] {
  guard self.connection is Some(conn) && !self.poisoned else {
    raise CheckpointError("state owner connection is unavailable")
  }
  errdefer {
    self.poisoned = true
    conn.close()
  }
  @async.with_timeout(self.config.timeout_ms, () => conn.query(sql))
}

///|
pub impl StateStore for PostgresStateStore with fn load(self, key) {
  self.mutex.acquire()
  defer self.mutex.release()
  let scope = sql_literal(self.store_id)
  let record = sql_literal(state_record_name(key))
  let sql = "SELECT content FROM \{self.table} WHERE namespace=\{scope} AND record=\{record}"
  let rows = if self.owner is Some(_) {
    self.query_owned(sql)
  } else {
    let conn = connect(self.config, 33554432, false)
    defer conn.close()
    @async.with_timeout(self.config.timeout_ms, () => conn.query(sql)) catch {
      Server("42P01", _) => return None
      error => raise error
    }
  }
  if rows.is_empty() {
    return None
  }
  if rows.length() != 1 {
    raise CheckpointError("duplicate state records")
  }
  let bytes = @base64.decode(cell(rows, 0, 0))
  if bytes.length() > 16777216 {
    raise CheckpointError("state record size")
  }
  Some(bytes)
}

///|
pub impl StateStore for PostgresStateStore with fn save(self, owner, key, bytes) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.owner != Some(owner) {
    raise CheckpointError("stale state owner")
  }
  if bytes.length() > 16777216 {
    raise CheckpointError("state record size")
  }
  let scope = sql_literal(self.store_id)
  let record = sql_literal(state_record_name(key))
  let value = sql_literal(@base64.encode(bytes))
  @async.protect_from_cancel(() => {
    ignore(
      self.query_owned(
        "INSERT INTO \{self.table}(namespace,record,content) VALUES(\{scope},\{record},\{value}) ON CONFLICT(namespace,record) DO UPDATE SET content=EXCLUDED.content",
      ),
    )
  })
}
