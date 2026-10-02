///|
fn pipeline_fixture() -> PipelineState {
  {
    version: 1,
    source: {
      system_id: "system",
      timeline: "1",
      database: "db",
      slot: "owned_slot",
      publication: "pub",
    },
    destination_id: "target",
    initial_lsn: Some("0/10"),
    checkpoint_initialized: true,
    tables: [
      { oid: 1U, phase: Catchup("0/50"), },
      { oid: 2U, phase: Ready("0/70"), },
    ],
  }
}

///|
test "pipeline state integrity rejects invalid progress and duplicate tables" {
  let original = pipeline_fixture()
  let bytes = state_bytes(original)
  assert_eq(parse_pipeline_state(bytes).destination_id, "target")
  let invalid = [
    bytes + b"x",
    b"invalid",
    state_bytes({ ..original, version: 2, }),
    state_bytes({
      ..original,
      tables: [original.tables[0], original.tables[0]],
    }),
    state_bytes({ ..original, initial_lsn: None, }),
    state_bytes({ ..original, tables: [{ oid: 1U, phase: Catchup("0/0"), }], }),
    state_bytes({ ..original, tables: [{ oid: 1U, phase: Ready("0/1"), }], }),
  ]
  for bytes in invalid {
    try parse_pipeline_state(bytes) catch {
      CheckpointError(_) => ()
      e => raise e
    } noraise {
      _ => fail("invalid pipeline state accepted")
    }
  }
}

///|
async test "live pipeline holds three independent snapshot exporters concurrently" {
  if live_config() is Some(config) {
    @async.with_timeout(30000, () => {
      let sql = connect(config, 16777216, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE SCHEMA pipeline_parallel; CREATE TABLE pipeline_parallel.a(id int PRIMARY KEY,value text); CREATE TABLE pipeline_parallel.b(id int PRIMARY KEY,value text); CREATE TABLE pipeline_parallel.d(id int PRIMARY KEY,value text); INSERT INTO pipeline_parallel.a VALUES(1,'a'); INSERT INTO pipeline_parallel.b VALUES(1,'b'); INSERT INTO pipeline_parallel.d VALUES(1,'d'); CREATE PUBLICATION pipeline_parallel FOR TABLE pipeline_parallel.a,pipeline_parallel.b,pipeline_parallel.d",
        ),
      )
      let dir = @fs.tmpdir(prefix="mooncdc-parallel-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/state"
      let control = ApplyControl()
      let probe = PipelineProbe::new(
        dir + "/target",
        sql,
        control,
        "99",
        false,
        false,
        None,
        schema="pipeline_parallel",
        parallel_state=path,
      )
      run_probe_pipeline(
        config,
        probe,
        publication="pipeline_parallel",
        destination_id="parallel",
        state_path=path,
        copy_concurrency=3,
        batch_rows=1,
      )
      for name in ["a", "b", "d"] {
        assert_eq(probe.data.resets.get(name), Some(1))
        assert_probe_table(probe, name)
      }
      drop_slot(sql, pipeline_status(path).slot)
      ignore(
        sql.query(
          "DROP PUBLICATION pipeline_parallel; DROP SCHEMA pipeline_parallel CASCADE",
        ),
      )
      println(
        "LIVE: three independent exported snapshots overlap and converge with WAL",
      )
    })
  }
}

///|
async test "pipeline state failed write preserves cached and persisted progress" {
  let dir = @fs.tmpdir(prefix="mooncdc-state-")
  defer @fs.rmdir(dir, recursive=true)
  let store : PipelineStore = {
    path: dir + "/state",
    session: None,
    state: pipeline_fixture(),
    mutex: @async.Mutex(),
  }
  store.update(state => state)
  @fs.mkdir(store.path + ".tmp")
  try store.set_phase(1U, Ready("0/50")) catch {
    _ => ()
  } noraise {
    _ => fail("failed write ignored")
  }
  assert_true(store.state.tables[0].phase is Catchup(_))
  assert_true(read_pipeline_state(store.path).tables[0].phase is Catchup(_))
  @fs.rmdir(store.path + ".tmp")
  store.set_phase(1U, Ready("0/50"))
  assert_true(read_pipeline_state(store.path).tables[0].phase is Ready(_))
}

///|
test "table cutoff projection preserves original event ids and splits truncate" {
  let decoder = Decoder::new(4096, 100, 10)
  ignore(decoder.decode(begin_fixture()))
  ignore(decoder.decode(relation_fixture("a")))
  let a = decoder.relation(1U)
  let b : Relation = { ..a, oid: 2U, name: "b", }
  let changes : ReadOnlyArray[Change] = [
    Insert(a, [Text("1"), Null]),
    Insert(b, [Text("2"), Null]),
    Truncate([a, b], cascade=false, restart_identity=true),
  ]
  let tx : Transaction = {
    id: "tx",
    xid: 1U,
    commit_lsn: Lsn::parse("0/60"),
    end_lsn: Lsn::parse("0/61"),
    commit_time_us: 0L,
    changes,
    session: "test",
    sequence: 1,
    projected_ids: None,
  }
  let store : PipelineStore = {
    path: "unused",
    session: None,
    state: pipeline_fixture(),
    mutex: @async.Mutex(),
  }
  let projected = store.project(tx)
  assert_eq(projected.changes.length(), 2)
  assert_eq(projected.event_id(0), tx.event_id(0))
  assert_eq(projected.event_id(1), Some(tx.event_id(2).unwrap() + "/table/1"))
  assert_true(
    projected.changes[1] is Truncate([r], cascade=false, restart_identity=true) &&
    r.oid == 1U,
  )
  let boundary = store.project({ ..tx, commit_lsn: Lsn::parse("0/70"), })
  assert_eq(boundary.changes.length(), 4)
  assert_eq(boundary.event_id(1), tx.event_id(1))
  assert_eq(boundary.event_id(2), Some("tx/2/table/1"))
  assert_eq(boundary.event_id(3), Some("tx/2/table/2"))
  let private_b = project_tables(
    tx,
    Map([(2U, Lsn::parse("0/1"))]),
    "pub",
    table_only=true,
  )
  assert_eq(private_b.event_id(0), tx.event_id(1))
  assert_eq(private_b.event_id(1), Some("tx/2/table/2"))
}

///|
priv suberror PipelineProbeFailure {
  PipelineProbeFailure
}

///|
priv struct PipelineProbeData {
  rows : Map[String, Map[String, String?]]
  resets : Map[String, Int]
  applied : Array[String]
} derive(ToJson, @json.FromJson)

///|
priv struct PipelineProbe {
  path : String
  data : PipelineProbeData
  sql : Connection
  control : ApplyControl
  stop_id : String
  mut fail_copy : Bool
  mut inject_copy : Bool
  startup_sql : String?
  schema : String
  parallel_state : String?
  crash_signal : String?
  mut stop_requested : Bool
}

///|
async fn PipelineProbe::new(
  path : String,
  sql : Connection,
  control : ApplyControl,
  stop_id : String,
  fail_copy : Bool,
  inject_copy : Bool,
  startup_sql : String?,
  schema? : String = "pipeline_fixture",
  parallel_state? : String,
  crash_signal? : String,
) -> PipelineProbe {
  let data : PipelineProbeData = if @fs.exists(path) {
    @json.from_json(@json.parse(@fs.read_file(path).text()))
  } else {
    { rows: Map([]), resets: Map([]), applied: [], }
  }
  {
    path,
    data,
    sql,
    control,
    stop_id,
    fail_copy,
    inject_copy,
    startup_sql,
    schema,
    parallel_state,
    crash_signal,
    stop_requested: false,
  }
}

///|
async fn PipelineProbe::persist(self : PipelineProbe) -> Unit {
  save_atomic_file(
    self.path,
    @utf8.encode(ToJson::to_json(self.data).stringify()),
  )
}

///|
impl Destination for PipelineProbe with fn startup(self) {
  if self.startup_sql is Some(sql) {
    ignore(self.sql.query(sql))
  }
}

///|
impl Destination for PipelineProbe with fn reset_table(self, relation, attempt~) {
  assert_true(!attempt.is_empty())
  if self.data.resets.is_empty() && self.parallel_state is Some(path) {
    let prefix = copy_slot_prefix(read_pipeline_state(path).source.slot)
    let query = "SELECT count(*)::text FROM pg_replication_slots WHERE left(slot_name,\{prefix.length()})='\{prefix}'"
    while cell(self.sql.query(query), 0, 0) != "3" {
      @async.sleep(20)
    }
  }
  self.data.rows[relation.name] = Map([])
  self.data.resets[relation.name] = self.data.resets
    .get(relation.name)
    .unwrap_or(0) +
    1
  self.persist()
}

///|
fn probe_value(value : Value) -> String? raise {
  match value {
    Text(text) => Some(text)
    Null => None
    _ => fail("unexpected probe value")
  }
}

///|
impl Destination for PipelineProbe with fn write_rows(self, relation, rows, _id) {
  for row in rows {
    guard row is [Text(id), value] else { fail("probe snapshot row") }
    self.data.rows[relation.name][id] = probe_value(value)
  }
  self.persist()
  if !rows.is_empty() && relation.name == "a" && self.inject_copy {
    self.inject_copy = false
    ignore(
      self.sql.query(
        "BEGIN; UPDATE \{self.schema}.a SET value='during' WHERE id=1; INSERT INTO \{self.schema}.a VALUES(3,'during'); UPDATE \{self.schema}.b SET value='during' WHERE id=1; COMMIT",
      ),
    )
  }
  if !rows.is_empty() && relation.name == "b" {
    if self.crash_signal is Some(path) {
      @fs.write_file(path, "partial target batch persisted")
      @async.sleep(60000)
      fail("crash worker was not terminated")
    }
    if self.fail_copy {
      self.fail_copy = false
      raise PipelineProbeFailure
    }
    if self.stop_id == "99" &&
      !(self.data.rows.get("a") is Some(rows) && rows.contains("99")) {
      // The first COPY batch may call again before the WAL event reaches target.
      ignore(
        self.sql.query(
          "INSERT INTO \{self.schema}.a VALUES(99,'sentinel') ON CONFLICT DO NOTHING",
        ),
      )
    }
  }
  Durable
}

///|
impl Destination for PipelineProbe with fn write_transaction(self, tx) {
  if !self.data.applied.contains(tx.id) {
    for change in tx.changes {
      match change {
        Insert(relation, [Text(id), value])
        | Update(relation, _, [Text(id), value]) => {
          self.data.rows[relation.name][id] = probe_value(value)
          if id == self.stop_id {
            self.stop_requested = true
          }
        }
        Delete(relation, Key([Text(id), ..]))
        | Delete(relation, Full([Text(id), ..])) =>
          self.data.rows[relation.name].remove(id)
        Truncate(relations, ..) =>
          for relation in relations {
            self.data.rows[relation.name].clear()
          }
        RelationChanged(_) | TypeChanged(_) | Origin(_, _) | Message(_) => ()
        _ => fail("unexpected probe change")
      }
    }
    self.data.applied.push(tx.id)
    self.persist()
  }
  Durable
}

///|
impl Destination for PipelineProbe with fn flush(self) {
  self.persist()
}

///|
// A sentinel on A may now arrive while B is still copying. Tests request stop
// only once all table workers have handed over their durable state.
async fn run_probe_pipeline(
  config : ConnectionConfig,
  probe : PipelineProbe,
  publication~ : String,
  destination_id~ : String,
  state_path~ : String,
  copy_concurrency? : Int = 2,
  batch_rows? : Int = 256,
  stop_on_error? : Bool = false,
  retry_oid? : UInt,
) -> Unit {
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      let mut retry = retry_oid
      for ;; {
        @async.sleep(10)
        if probe.control.table_retry is Some(_) && retry is Some(oid) {
          assert_true(probe.control.retry_table(oid))
          retry = None
        }
        if stop_on_error &&
          @fs.exists(state_path) &&
          pipeline_status(state_path).tables.any(table => {
            table.phase is Errored(..)
          }) {
          probe.control.stop()
          break
        }
        if probe.stop_requested &&
          pipeline_status(state_path).tables.all(table => {
            table.phase is (Catchup(_) | Ready(_))
          }) {
          probe.control.stop()
          break
        }
      }
    })
    run_pipeline(
      config,
      probe,
      publication~,
      destination_id~,
      state_path~,
      control=probe.control,
      copy_concurrency~,
      batch_rows~,
    )
    group.return_immediately(())
  })
}

///|
async fn assert_probe_table(probe : PipelineProbe, name : String) -> Unit {
  let rows = probe.sql.query(
    "SELECT id::text, value FROM \{probe.schema}.\{name}",
  )
  assert_eq(probe.data.rows[name].length(), rows.length())
  for row in rows {
    assert_eq(probe.data.rows[name].get(cell([row], 0, 0)), Some(row[1]))
  }
}

///|
async test "live pipeline resumes only unfinished tables and copies newly published tables" {
  if live_config() is Some(config) {
    @async.with_timeout(30000, () => {
      let sql = connect(config, 16777216, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE SCHEMA pipeline_fixture; CREATE TABLE pipeline_fixture.a(id int PRIMARY KEY,value text); CREATE TABLE pipeline_fixture.b(id int PRIMARY KEY,value text); INSERT INTO pipeline_fixture.a VALUES(1,'before'),(2,NULL); INSERT INTO pipeline_fixture.b VALUES(1,'before'),(2,NULL); CREATE PUBLICATION pipeline_fixture FOR TABLE pipeline_fixture.a,pipeline_fixture.b",
        ),
      )
      let dir = @fs.tmpdir(prefix="mooncdc-pipeline-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/state"
      let control = ApplyControl()
      let first = PipelineProbe::new(
        dir + "/target",
        sql,
        control,
        "unused",
        true,
        true,
        None,
      )
      run_probe_pipeline(
        config,
        first,
        publication="pipeline_fixture",
        destination_id="probe",
        state_path=path,
        copy_concurrency=1,
        batch_rows=1,
        stop_on_error=true,
      )
      let interrupted = read_pipeline_state(path)
      assert_true(interrupted.tables[0].phase is (Catchup(_) | Ready(_)))
      assert_true(interrupted.tables[1].phase is Errored(_, ManualRetry, 0))
      assert_true(
        load_checkpoint(path + ".checkpoint", interrupted.source) >=
        Lsn::parse(interrupted.initial_lsn.unwrap()),
      )
      ignore(
        sql.query(
          "BEGIN; UPDATE pipeline_fixture.a SET value='after' WHERE id=1; DELETE FROM pipeline_fixture.a WHERE id=2; INSERT INTO pipeline_fixture.b VALUES(3,'after'); COMMIT",
        ),
      )
      let control = ApplyControl()
      let second = PipelineProbe::new(
        dir + "/target",
        sql,
        control,
        "99",
        false,
        false,
        None,
      )
      run_probe_pipeline(
        config,
        second,
        publication="pipeline_fixture",
        destination_id="probe",
        state_path=path,
        copy_concurrency=2,
        batch_rows=1,
        retry_oid=interrupted.tables[1].oid,
      )
      assert_eq(second.data.resets.get("a"), Some(1))
      assert_eq(second.data.resets.get("b"), Some(2))
      assert_probe_table(second, "a")
      assert_probe_table(second, "b")
      let control = ApplyControl()
      let third = PipelineProbe::new(
        dir + "/target",
        sql,
        control,
        "100",
        false,
        false,
        Some(
          "CREATE TABLE pipeline_fixture.c(id int PRIMARY KEY,value text); INSERT INTO pipeline_fixture.c VALUES(1,'initial'); ALTER PUBLICATION pipeline_fixture ADD TABLE pipeline_fixture.c; BEGIN; INSERT INTO pipeline_fixture.c VALUES(2,'streamed'); INSERT INTO pipeline_fixture.a VALUES(100,'stop'); COMMIT",
        ),
      )
      run_probe_pipeline(
        config,
        third,
        publication="pipeline_fixture",
        destination_id="probe",
        state_path=path,
      )
      assert_eq(third.data.resets.get("a"), Some(1))
      assert_eq(third.data.resets.get("b"), Some(2))
      assert_eq(third.data.resets.get("c"), Some(1))
      for name in ["a", "b", "c"] {
        assert_probe_table(third, name)
      }
      let status = pipeline_status(path)
      assert_eq(status.tables.length(), 3)
      try
        run_pipeline(
          config,
          third,
          publication="pipeline_fixture",
          destination_id="wrong",
          state_path=path,
          control=ApplyControl(),
        )
      catch {
        UnsafeResume(_) => ()
        e => raise e
      } noraise {
        _ => fail("changed target accepted")
      }
      let prefix = copy_slot_prefix(status.slot)
      let query = "SELECT slot_name FROM pg_replication_slots WHERE left(slot_name,\{prefix.length()})='\{prefix}'"
      let mut temporary = sql.query(query)
      while !temporary.is_empty() {
        @async.sleep(20)
        temporary = sql.query(query)
      }
      drop_slot(sql, status.slot)
      ignore(
        sql.query(
          "DROP PUBLICATION pipeline_fixture; DROP SCHEMA pipeline_fixture CASCADE",
        ),
      )
      println(
        "LIVE: per-table durable recovery, fresh failed-copy reset, WAL cutoffs, dynamic table discovery and destination binding passed",
      )
    })
  }
}
