///|
priv struct StoredProbe {
  target : PipelineProbe
  mut session : StateSession?
  mut restored : Int
}

///|
impl Destination for StoredProbe with fn bind_state(self, session) {
  self.session = Some(session)
}

///|
impl Destination for StoredProbe with fn startup(self) {
  let session = self.session.unwrap()
  let tables = self.target.sql.query(
    "SELECT oid::text FROM pg_class WHERE relnamespace='cdc_store_pipeline'::regnamespace AND relkind='r'",
  )
  for row in tables {
    if session.get_metadata(@string.parse_uint(cell([row], 0, 0))) is Some(_) {
      self.restored += 1
    }
  }
  Destination::startup(self.target)
}

///|
impl Destination for StoredProbe with fn reset_table(self, relation, attempt~) {
  Destination::reset_table(self.target, relation, attempt~)
  self.session
  .unwrap()
  .put_metadata(relation.oid, {
    "attempt": attempt.to_json(),
    "name": relation.name.to_json(),
  })
}

///|
impl Destination for StoredProbe with fn write_rows(self, relation, rows, id) {
  Destination::write_rows(self.target, relation, rows, id)
}

///|
impl Destination for StoredProbe with fn write_transaction(self, tx) {
  Destination::write_transaction(self.target, tx)
}

///|
impl Destination for StoredProbe with fn flush(self) {
  Destination::flush(self.target)
}

///|
async fn run_stored_probe(
  config : ConnectionConfig,
  store : &StateStore,
  target : PipelineProbe,
) -> StoredProbe {
  let probe : StoredProbe = { target, session: None, restored: 0, }
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      for ;; {
        @async.sleep(10)
        if target.stop_requested &&
          state_store_status(store) is Some(status) &&
          status.tables.all(table => table.phase is (Ready(_) | Catchup(_))) {
          target.control.stop()
          break
        }
      }
    })
    run_pipeline(
      config,
      probe,
      publication="cdc_store_pipeline",
      destination_id="stored-probe",
      state_store=store,
      control=target.control,
      copy_concurrency=1,
      batch_rows=1,
    )
    group.return_immediately(())
  })
  probe
}

///|
async test "live external state stores drive pipeline checkpoints metadata and restart" {
  if live_config() is Some(config) {
    for backend in ["file", "postgres"] {
      @async.with_timeout(30000, () => {
        let sql = connect(config, 16777216, false)
        defer sql.close()
        ignore(
          sql.query(
            "CREATE SCHEMA cdc_store_pipeline; CREATE TABLE cdc_store_pipeline.a(id int PRIMARY KEY,value text); CREATE TABLE cdc_store_pipeline.b(id int PRIMARY KEY,value text); INSERT INTO cdc_store_pipeline.a VALUES(1,'before'); INSERT INTO cdc_store_pipeline.b VALUES(1,'before'); CREATE PUBLICATION cdc_store_pipeline FOR TABLE cdc_store_pipeline.a,cdc_store_pipeline.b",
          ),
        )
        let dir = @fs.tmpdir(prefix="mooncdc-state-pipeline-")
        defer @fs.rmdir(dir, recursive=true)
        @fs.mkdir(dir + "/records")
        let make_store = fn() -> &StateStore raise {
          if backend == "file" {
            (FileStateStore::new(dir + "/records") : &StateStore)
          } else {
            (
              PostgresStateStore::new(
                config,
                store_id="pipeline",
                schema="cdc_store_pipeline",
              ) : &StateStore)
          }
        }
        let first_store = make_store()
        let first = PipelineProbe::new(
          dir + "/target",
          sql,
          ApplyControl(),
          "99",
          false,
          true,
          None,
          schema="cdc_store_pipeline",
        )
        assert_eq(run_stored_probe(config, first_store, first).restored, 0)
        assert_probe_table(first, "a")
        assert_probe_table(first, "b")
        let saved = parse_pipeline_state(first_store.load(Pipeline).unwrap())
        assert_true(
          parse_checkpoint(
            first_store.load(ReplicationCheckpoint).unwrap(),
            saved.source,
          ) >
          Lsn::parse(saved.initial_lsn.unwrap()),
        )
        ignore(
          sql.query(
            "BEGIN; UPDATE cdc_store_pipeline.a SET value='offline'; INSERT INTO cdc_store_pipeline.b VALUES(2,'offline'); COMMIT",
          ),
        )
        let next_store = make_store()
        let next = PipelineProbe::new(
          dir + "/target",
          sql,
          ApplyControl(),
          "100",
          false,
          false,
          Some("INSERT INTO cdc_store_pipeline.a VALUES(100,'stop')"),
          schema="cdc_store_pipeline",
        )
        let resumed = run_stored_probe(config, next_store, next)
        assert_eq(resumed.restored, 2)
        assert_eq(next.data.resets["a"], 1)
        assert_eq(next.data.resets["b"], 1)
        assert_probe_table(next, "a")
        assert_probe_table(next, "b")
        assert_true(!resumed.session.unwrap().active)
        drop_slot(sql, state_store_status(next_store).unwrap().slot)
        ignore(
          sql.query(
            "DROP PUBLICATION cdc_store_pipeline; DROP SCHEMA cdc_store_pipeline CASCADE",
          ),
        )
        println(
          "LIVE: \{backend} state store pipeline, destination metadata, monotonic checkpoint and restart convergence passed",
        )
      })
    }
  }
}

///|
priv struct FailingStateStore {
  inner : FileStateStore
  mut fail_checkpoint : Bool
}

///|
impl StateStore for FailingStateStore with fn acquire(self, owner) {
  self.inner.acquire(owner)
}

///|
impl StateStore for FailingStateStore with fn release(self, owner) {
  self.inner.release(owner)
}

///|
impl StateStore for FailingStateStore with fn load(self, key) {
  self.inner.load(key)
}

///|
impl StateStore for FailingStateStore with fn save(self, owner, key, bytes) {
  if self.fail_checkpoint && key == ReplicationCheckpoint {
    raise CheckpointError("injected external store failure")
  }
  self.inner.save(owner, key, bytes)
}

///|
async test "live external checkpoint failure leaves ack conservative and supports snapshot resume" {
  if live_config() is Some(config) {
    @async.with_timeout(20000, () => {
      let sql = connect(config, 16777216, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE TABLE cdc_stored_ack(id int PRIMARY KEY); CREATE PUBLICATION cdc_stored_ack FOR TABLE cdc_stored_ack",
        ),
      )
      defer {
        drop_slot(sql, "cdc_stored_ack")
        ignore(
          sql.query(
            "DROP PUBLICATION cdc_stored_ack; DROP TABLE cdc_stored_ack",
          ),
        )
      }
      let dir = @fs.tmpdir(prefix="mooncdc-ack-store-")
      defer @fs.rmdir(dir, recursive=true)
      let store : FailingStateStore = {
        inner: FileStateStore::new(dir),
        fail_checkpoint: false,
      }
      let point = copy_snapshot(
        config,
        slot="cdc_stored_ack",
        publication="cdc_stored_ack",
        _ => (),
      )
      point.save_checkpoint_in(store)
      subscribe(
        config,
        slot="cdc_stored_ack",
        publication="cdc_stored_ack",
        start=Resume,
        checkpoint_store=store,
        sub => {
          ignore(sql.query("INSERT INTO cdc_stored_ack VALUES(1)"))
          let tx = sub.next()
          store.fail_checkpoint = true
          try sub.ack(tx) catch {
            CheckpointError(_) => ()
            error => raise error
          } noraise {
            _ => fail("store failure acknowledged")
          }
          assert_eq(sub.diagnostics().durable, point.lsn)
          assert_eq(
            parse_checkpoint(
              StateStore::load(store, ReplicationCheckpoint).unwrap(),
              point.source,
            ),
            point.lsn,
          )
          store.fail_checkpoint = false
          sub.ack(tx)
          assert_eq(sub.diagnostics().durable, tx.end_lsn)
        },
      )
      subscribe(
        config,
        slot="cdc_stored_ack",
        publication="cdc_stored_ack",
        start=Resume,
        checkpoint_store=store,
        sub => {
          ignore(sql.query("INSERT INTO cdc_stored_ack VALUES(2)"))
          let tx = sub.next()
          assert_true(
            tx.changes.any(change => change is Insert(_, [Text("2")])),
          )
          sub.ack(tx)
        },
      )
      println(
        "LIVE: external snapshot checkpoint, failed persistence/no ack and resumed delivery passed",
      )
    })
  }
}
