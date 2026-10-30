///|
priv struct CleanupDestination {
  bounds : Map[UInt32, SchemaId]
  mut fail : Bool
}

///|
async test "ClickHouse retention preserves both pending DDL endpoints" {
  let dir = @fs.tmpdir(prefix="mooncdc-ch-retention-")
  defer @fs.rmdir(dir, recursive=true)
  @fs.mkdir(dir + "/state")
  with_file_schema_store(dir + "/schema", identity="retention", schemas => {
    with_state_store(FileStateStore::new(dir + "/state"), session => {
      let target = ClickHouseDestination::new(
        endpoint="http://127.0.0.1:1",
        user="unused",
        password="",
        database="test",
        publication="p",
        schema_store=schemas,
      )
      target.bind_state(session)
      // This metadata-only unit exercises no HTTP; live tests use startup.
      target.running = true
      defer target.shutdown()
      assert_eq(target.schema_retention(42U), None)
      let before = ch_test_schema(ch_test_columns(), 100)
      let after = ch_test_schema(ch_test_columns(), 300)
      let old = target.schema_record(before, "public_ddl")
      let current = target.schema_record(after, "public_ddl")
      for record in [current, { ..current, creating: Some(true), }] {
        session.put_metadata(42U, ToJson::to_json(record))
        assert_eq(target.schema_retention(42U), Some(after.schema.id))
      }
      session.put_metadata(
        42U,
        ToJson::to_json({ ..current, previous: Some(ToJson::to_json(old)), }),
      )
      assert_eq(target.schema_retention(42U), Some(before.schema.id))
    })
  })
}

///|
impl Destination for CleanupDestination with fn schema_retention(self, oid) {
  if self.fail {
    raise TestDestinationFailure
  }
  self.bounds.get(oid)
}

///|
impl Destination for CleanupDestination with fn reset_table(
  _self,
  _relation,
  attempt~,
) {
  ignore(attempt)
}

///|
impl Destination for CleanupDestination with fn write_rows(
  _self,
  _relation,
  _rows,
  _id,
) {
  Durable
}

///|
impl Destination for CleanupDestination with fn write_transaction(_self, _tx) {
  Durable
}

///|
impl Destination for CleanupDestination with fn flush(_self) {
  ()
}

///|
priv struct CleanupSchemaStore {
  inner : &SchemaStore
  mut fail : Bool
  calls : Array[ReadOnlyArray[(UInt32, SchemaId)]]
}

///|
extend CleanupSchemaStore with SchemaStore::{put, all}

///|
impl SchemaStore for CleanupSchemaStore with fn get(self, oid, id) {
  self.inner.get(oid, id)
}

///|
impl SchemaStore for CleanupSchemaStore with fn all(self) {
  self.inner.all()
}

///|
impl SchemaStore for CleanupSchemaStore with fn put(self, schema) {
  self.inner.put(schema)
}

///|
impl SchemaStore for CleanupSchemaStore with fn prune(self, bounds) {
  self.calls.push(bounds)
  if self.fail {
    raise TestDestinationFailure
  }
  self.inner.prune(bounds)
}

///|
async test "schema cleanup retains replay DDL endpoints and excludes unowned tables" {
  let dir = @fs.tmpdir(prefix="mooncdc-cleanup-")
  defer @fs.rmdir(dir, recursive=true)
  with_file_schema_store(dir + "/schemas", identity="cleanup", inner => {
    let schemas : CleanupSchemaStore = { inner, fail: false, calls: [], }
    for oid in [1U, 2U, 3U, 4U, 5U, 6U, 7U] {
      for version in [100UL, 200UL, 250UL, 300UL] {
        guard ddl_fixture() is Object(payload) else {
          fail("fixture must be an object")
        }
        payload["oid"] = Json::Json(oid)
        schemas.put(
          parse_schema(Json::object(payload), {
            commit_lsn: { value: version, },
            message_lsn: { value: version - 1, },
          }),
        )
      }
    }
    let path = dir + "/state"
    let state : PipelineState = {
      ..pipeline_fixture(),
      tables: [
        { oid: 1U, phase: Ready("0/10"), decoding: None, },
        { oid: 2U, phase: Ready("0/10"), decoding: None, },
        { oid: 3U, phase: Copying, decoding: None, },
        { oid: 4U, phase: Catchup("0/12C"), decoding: None, },
        { oid: 5U, phase: Errored("failed", ManualRetry, 0), decoding: None, },
        { oid: 6U, phase: Ready("0/10"), decoding: None, },
        { oid: 7U, phase: Ready("0/10"), decoding: None, },
      ],
    }
    let store : PipelineStore = {
      path,
      state,
      session: None,
      mutex: @async.Mutex(),
    }
    store.update(state => state)
    let saved : Lsn = { value: 250, }
    store_checkpoint(path + ".checkpoint", state.source, saved, None)
    let target : CleanupDestination = { bounds: Map([]), fail: false, }
    for oid in [1U, 2U, 3U, 4U, 5U, 7U] {
      target.bounds[oid] = {
        commit_lsn: { value: if oid == 1U { 100 } else { 300 }, },
        message_lsn: { value: 99, },
      }
    }
    let sink : PipelineDestination = {
      copy_monitor_interval_ms: 10000,
      memory: None,
      batch: None,
      destination: target,
      mutex: @async.Mutex(),
      store,
      connection: {
        hostaddr: None,
        keepalive: TcpKeepaliveConfig::new(),
        host: "127.0.0.1",
        port: 1,
        user: "unused",
        password: "",
        database: "unused",
        security: LocalPlaintext,
        timeout_ms: 100,
      },
      control: ApplyControl(),
      batch_rows: 1,
      copy_connections_per_table: 1,
      table_sync_copy: IncludeAllTables,
      slot_failover: false,
      schema_store: Some(schemas),
      frontier: None,
      main_lsn: saved,
      copy_jobs: [],
      copy_ready: @async.CondVar::Cond(),
      retry_delay_ms: 10,
      max_table_retries: 2,
      retry_attempts: Map([]),
      cleanup_candidates: Map([
        (1U, true),
        (2U, true),
        (3U, true),
        (4U, true),
        (5U, true),
        (6U, true),
        (7U, true),
      ]),
      decoding: None,
      copy_decoding: Map([]),
    }
    let checkpoint_bytes = @fs.read_file(path + ".checkpoint").text()
    target.fail = true
    sink.cleanup_schemas()
    assert_true(
      sink.control
      .prometheus()
      .contains(
        "mooncdc_schema_cleanup_errors_total{worker_type=\"apply\"} 1\n",
      ),
    )
    assert_false(
      sink.control.prometheus().contains("mooncdc_schema_cleanups_total"),
    )
    assert_eq(schemas.all().length(), 28)
    assert_eq(sink.cleanup_candidates.length(), 7)
    target.fail = false
    schemas.fail = true
    sink.cleanup_schemas()
    assert_true(
      sink.control
      .prometheus()
      .contains(
        "mooncdc_schema_cleanup_errors_total{worker_type=\"apply\"} 2\n",
      ),
    )
    assert_false(
      sink.control
      .prometheus()
      .contains("mooncdc_schema_cleanup_pruned_versions_total"),
    )
    assert_eq(schemas.all().length(), 28)
    assert_eq(sink.cleanup_candidates.length(), 7)
    schemas.fail = false
    sink.cleanup_schemas()
    let metrics = sink.control.prometheus()
    for
      (name, count) in [
        ("schema_cleanup_attempts_total", 3),
        ("schema_cleanups_total", 1),
        ("schema_cleanup_tables_total", 3),
        ("schema_cleanup_pruned_versions_total", 2),
        ("schema_cleanup_skipped_tables_total", 1),
      ] {
      assert_true(
        metrics.contains(
          "mooncdc_" +
          name +
          "{worker_type=\"apply\"} " +
          count.to_string() +
          "\n",
        ),
      )
    }
    assert_eq(schemas.all().length(), 26)
    assert_eq(schemas.calls.last().unwrap(), [
      (1U, target.bounds[1U]),
      (2U, SchemaId::before_lsn(saved)),
      (7U, SchemaId::before_lsn(saved)),
    ])
    assert_eq(sink.cleanup_candidates.length(), 3)
    for oid in [1U, 3U, 4U, 5U, 6U] {
      assert_eq(schemas.all().filter(s => s.oid == oid).length(), 4)
    }
    for oid in [2U, 7U] {
      assert_eq(
        schemas.all().filter(s => s.oid == oid).map(s => s.id.commit_lsn.value),
        [200UL, 250UL, 300UL],
      )
    }
    assert_eq(@fs.read_file(path + ".checkpoint").text(), checkpoint_bytes)
    // Read failure cannot turn the received/main frontier into a cleanup bound.
    @fs.write_file(path + ".checkpoint", "corrupt")
    sink.cleanup_candidates[2U] = true
    let calls = schemas.calls.length()
    sink.cleanup_schemas()
    assert_eq(schemas.calls.length(), calls)
    assert_true(
      sink.control
      .prometheus()
      .contains(
        "mooncdc_schema_cleanup_errors_total{worker_type=\"apply\"} 3\n",
      ),
    )
    assert_true(sink.cleanup_candidates.contains(2U))
    @fs.write_file(path + ".checkpoint", checkpoint_bytes)
    let fallback = ProbeDestination::new("none", ApplyControl())
    assert_eq(Destination::schema_retention(fallback, 1U), None)
  })
  with_file_schema_store(dir + "/schemas", identity="cleanup", schemas => {
    assert_eq(schemas.all().length(), 26)
  })
}
