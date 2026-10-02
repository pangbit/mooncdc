///|
priv struct ChProcessStore {
  inner : &StateStore
  dir : String
  mut remaining : Int?
  after_save : Bool
}

///|
impl StateStore for ChProcessStore with fn acquire(self, owner) {
  self.inner.acquire(owner)
}

///|
impl StateStore for ChProcessStore with fn release(self, owner) {
  self.inner.release(owner)
}

///|
impl StateStore for ChProcessStore with fn load(self, key) {
  self.inner.load(key)
}

///|
async fn ChProcessStore::boundary(self : ChProcessStore) -> Unit {
  @fs.write_file(self.dir + "/ready", "ready")
  for ;; {
    @async.sleep(1000)
  }
}

///|
impl StateStore for ChProcessStore with fn save(self, owner, key, bytes) {
  let mut stop = false
  if key is TableMetadata(_) && self.remaining is Some(remaining) {
    stop = remaining == 0
    self.remaining = Some(remaining - 1)
  }
  if stop && !self.after_save {
    self.boundary()
  }
  self.inner.save(owner, key, bytes)
  if stop && self.after_save {
    self.boundary()
  }
}

///|
async test "ClickHouse process crash boundary worker" {
  guard @env.get_env_var("MOONCDC_CHCRASH_DIR") is Some(dir) else { return }
  let phase = @env.get_env_var("MOONCDC_CHCRASH_PHASE").unwrap()
  let boundary = @env.get_env_var("MOONCDC_CHCRASH_BOUNDARY").unwrap()
  let engine = if @env.get_env_var("MOONCDC_CHCRASH_ENGINE") ==
    Some("MergeTree") {
    ClickHouseEngine::MergeTree
  } else {
    ReplacingMergeTree
  }
  let http = ChHttp::new(
    "http://127.0.0.1:18123", "cdc", "mooncdc-test-only", 10000,
  )
  if phase == "init" {
    @fs.write_file(dir + "/database", "mooncdc_kill_" + pipeline_nonce())
    @fs.mkdir(dir + "/state")
  }
  let db = @fs.read_file(dir + "/database").text()
  if phase == "cleanup" {
    ignore(http.query("DROP DATABASE IF EXISTS " + ch_quote(db, '"')))
    println("CLICKHOUSE crash worker completed cleanup")
    return
  }
  if phase == "init" {
    ignore(http.query("CREATE DATABASE " + ch_quote(db, '"')))
  }
  let state : ChProcessStore = {
    inner: FileStateStore::new(dir + "/state"),
    dir,
    remaining: None,
    after_save: boundary == "previous" || boundary == "creating_previous",
  }
  let columns = ch_test_columns()
  let before = ch_test_schema(columns, 100)
  let after = ch_test_schema(
    [
      columns[0],
      { ..columns[1], name: "caption", },
      {
        ..columns[1],
        name: "added",
        ordinal: 3,
        default_expression: Some("'initial'"),
      },
    ],
    200,
  )
  with_file_schema_store(dir + "/schemas", identity=db, schemas => {
    with_state_store(state, session => {
      let target = ClickHouseDestination::new(
        endpoint="http://127.0.0.1:18123",
        user="cdc",
        password="mooncdc-test-only",
        database=db,
        publication="p",
        schema_store=schemas,
        engine~,
        timeout_ms=10000,
      )
      target.bind_state(session)
      target.startup()
      defer target.shutdown()
      let table = target.qualified("public_ddl")
      if phase == "init" {
        schemas.put(before.schema)
        schemas.put(after.schema)
        let relation = before.schema.relation(["id", "value"])
        target.bind_snapshot_schema(relation, before.schema)
        target.reset_table(relation, attempt="initial")
        ignore(
          http.query(
            "INSERT INTO " + table + " (id,value) VALUES(1,'survives')",
          ),
        )
      } else if phase == "crash" {
        state.remaining = Some(if state.after_save { 0 } else { 1 })
        if boundary.has_prefix("creating_") {
          let relation = after.schema.relation(["id", "caption", "added"])
          target.bind_snapshot_schema(relation, after.schema)
          target.reset_table(relation, attempt="replacement")
        } else {
          ignore(target.prepare_schema(after))
        }
        fail("crash boundary was not reached")
      } else if phase == "recover" {
        let record = target.stored_record(before.schema.oid).unwrap()
        if boundary.has_prefix("creating_") {
          assert_eq(record.creating, Some(true))
        } else {
          assert_true(record.previous is Some(_))
        }
        ignore(target.prepare_schema(after))
        let record = target.stored_record(before.schema.oid).unwrap()
        assert_true(record.previous is None && record.creating is None)
        assert_eq(record.projected(), after)
        if boundary.has_prefix("creating_") {
          assert_eq(
            http.query("SELECT count() FROM " + table).trim().to_owned(),
            "0",
          )
        } else {
          assert_eq(
            http
            .query("SELECT caption,added FROM " + table + " WHERE id=1")
            .trim()
            .to_owned(),
            "survives\tinitial",
          )
        }
        ignore(target.prepare_schema(after))
        if engine == ReplacingMergeTree {
          ignore(http.query("SELECT count() FROM " + table + "__current"))
        }
      } else {
        fail("unknown crash phase")
      }
    })
  })
  println("CLICKHOUSE crash worker completed " + phase)
}
