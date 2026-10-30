///|
test "pipeline recreation intent is versioned and cannot contain completed tables" {
  let old = pipeline_fixture()
  let intent : PipelineState = {
    ..old,
    version: 2,
    initial_lsn: None,
    checkpoint_initialized: false,
    tables: old.tables.map(table => { ..table, phase: Pending, decoding: None, }),
  }
  let parsed = parse_pipeline_state(state_bytes(intent))
  assert_eq(parsed.version, 2)
  assert_true(parsed.tables.all(table => table.phase is Pending))
  assert_eq(
    parse_pipeline_state(state_bytes({ ..intent, initial_lsn: Some("0/100"), })).initial_lsn,
    Some("0/100"),
  )
  for
    invalid in [
      { ..intent, checkpoint_initialized: true, },
      { ..intent, tables: old.tables, },
      { ..intent, version: 3, },
    ] {
    try parse_pipeline_state(state_bytes(invalid)) catch {
      CheckpointError(_) => ()
      error => raise error
    } noraise {
      _ => fail("unsafe recreation intent accepted")
    }
  }
}

///|
async fn recreate_test_state(
  path : String,
  store : &StateStore?,
) -> PipelineState {
  match store {
    Some(store) => parse_pipeline_state(store.load(Pipeline).unwrap())
    None => read_pipeline_state(path)
  }
}

///|
async fn recreate_test_checkpoint(path : String, store : &StateStore?) -> Bytes {
  match store {
    Some(store) => store.load(ReplicationCheckpoint).unwrap()
    None => @fs.read_file(path + ".checkpoint").binary()
  }
}

///|
async fn run_recreate_probe(
  config : ConnectionConfig,
  target : PipelineProbe,
  path : String,
  store : &StateStore?,
  previous : Lsn,
  policy~ : TableSyncCopy = IncludeAllTables,
  slot_failover~ : Bool = false,
) -> Unit {
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      while true {
        @async.sleep(5)
        let state = recreate_test_state(path, store)
        if state.initial_lsn is Some(initial) &&
          Lsn::parse(initial) > previous &&
          state.tables.all(table => table.phase is (Ready(_) | Catchup(_))) {
          ignore(
            target.sql.query(
              "INSERT INTO slot_recreate.a VALUES(999,'sentinel') ON CONFLICT(id) DO UPDATE SET value=excluded.value",
            ),
          )
          break
        }
      }
      while !target.stop_requested {
        @async.sleep(5)
      }
      target.control.stop()
    })
    match store {
      Some(store) =>
        run_pipeline(
          config,
          target,
          publication="slot_recreate",
          destination_id="recreate",
          control=target.control,
          state_store=store,
          invalidated_slot_behavior=RecreateInvalidatedSlot,
          table_sync_copy=policy,
          copy_connections_per_table=3,
          slot_failover~,
        )
      None =>
        run_pipeline(
          config,
          target,
          publication="slot_recreate",
          destination_id="recreate",
          control=target.control,
          state_path=path,
          invalidated_slot_behavior=RecreateInvalidatedSlot,
          table_sync_copy=policy,
          copy_connections_per_table=3,
          slot_failover~,
        )
    }
    group.return_immediately(())
  })
}

///|
async fn invalidate_test_slot(sql : Connection, slot : String) -> Unit {
  for _ in 0..<8 {
    ignore(sql.query("SELECT pg_switch_wal(); CHECKPOINT"))
    let rows = sql.query(
      "SELECT wal_status,invalidation_reason FROM pg_replication_slots WHERE slot_name='\{slot}'",
    )
    if cell(rows, 0, 0) == "lost" {
      assert_eq(cell(rows, 0, 1), "wal_removed")
      return
    }
  }
  fail("owned test slot was not invalidated")
}

///|
async test "live invalidated pipeline slot rejects by default and explicitly recopies selected tables" {
  if live_config() is Some(config) {
    @async.with_timeout(60000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(sql.query("ALTER SYSTEM SET max_slot_wal_keep_size='0'"))
      ignore(sql.query("SELECT pg_reload_conf()"))
      ignore(
        sql.query(
          "CREATE SCHEMA slot_recreate; CREATE TABLE slot_recreate.a(id int PRIMARY KEY,value text); CREATE TABLE slot_recreate.b(id int PRIMARY KEY,value text); CREATE PUBLICATION slot_recreate FOR TABLE slot_recreate.a,slot_recreate.b",
        ),
      )
      defer @async.protect_from_cancel(() => {
        ignore(
          sql.query(
            "DROP PUBLICATION slot_recreate; DROP SCHEMA slot_recreate CASCADE",
          ),
        )
      })
      let a = @string.parse_uint(
        cell(sql.query("SELECT 'slot_recreate.a'::regclass::oid::text"), 0, 0),
      )
      for
        policy in [
          IncludeAllTables,
          SkipAllTables,
          IncludeTables([a]),
          SkipTables([a]),
        ] {
        ignore(
          sql.query(
            "TRUNCATE slot_recreate.a,slot_recreate.b; INSERT INTO slot_recreate.a VALUES(1,'old'),(2,'deleted'); INSERT INTO slot_recreate.b VALUES(1,'old'),(2,'deleted')",
          ),
        )
        let dir = @fs.tmpdir(prefix="mooncdc-recreate-")
        defer @fs.rmdir(dir, recursive=true)
        let path = dir + "/state"
        let first = PipelineProbe::new(
          dir + "/target",
          sql,
          ApplyControl(),
          "99",
          false,
          false,
          None,
          schema="slot_recreate",
        )
        run_probe_pipeline(
          config,
          first,
          publication="slot_recreate",
          destination_id="recreate",
          state_path=path,
          slot_failover=policy is IncludeAllTables,
        )
        let old = read_pipeline_state(path)
        defer drop_slot(sql, old.source.slot)
        let checkpoint = @fs.read_file(path + ".checkpoint").binary()
        let state = @fs.read_file(path).binary()
        let target_bytes = @fs.read_file(dir + "/target").binary()
        ignore(
          sql.query(
            "BEGIN; UPDATE slot_recreate.a SET value='offline' WHERE id=1; DELETE FROM slot_recreate.a WHERE id=2; UPDATE slot_recreate.b SET value='offline' WHERE id=1; DELETE FROM slot_recreate.b WHERE id=2; COMMIT",
          ),
        )
        invalidate_test_slot(sql, old.source.slot)
        let refused = PipelineProbe::new(
          dir + "/target",
          sql,
          ApplyControl(),
          "999",
          false,
          false,
          None,
          schema="slot_recreate",
        )
        ignore(
          sql.query(
            "ALTER PUBLICATION slot_recreate DROP TABLE slot_recreate.a,slot_recreate.b",
          ),
        )
        try
          run_pipeline(
            config,
            refused,
            publication="slot_recreate",
            destination_id="recreate",
            state_path=path,
            control=refused.control,
            invalidated_slot_behavior=RecreateInvalidatedSlot,
          )
        catch {
          InvalidConfig("publication has no tables") => ()
          error => raise error
        } noraise {
          _ => fail("recreated slot for an empty publication")
        }
        assert_eq(@fs.read_file(path).binary(), state)
        assert_eq(@fs.read_file(path + ".checkpoint").binary(), checkpoint)
        assert_eq(
          cell(
            sql.query(
              "SELECT invalidation_reason FROM pg_replication_slots WHERE slot_name='\{old.source.slot}'",
            ),
            0,
            0,
          ),
          "wal_removed",
        )
        ignore(
          sql.query(
            "ALTER PUBLICATION slot_recreate ADD TABLE slot_recreate.a,slot_recreate.b",
          ),
        )
        try
          run_pipeline(
            config,
            refused,
            publication="slot_recreate",
            destination_id="wrong",
            state_path=path,
            control=refused.control,
            invalidated_slot_behavior=RecreateInvalidatedSlot,
          )
        catch {
          UnsafeResume("pipeline source, destination or slot prefix changed") =>
            ()
          error => raise error
        } noraise {
          _ => fail("recreate ignored destination identity")
        }
        try
          run_pipeline(
            config,
            refused,
            publication="slot_recreate",
            destination_id="recreate",
            state_path=path,
            control=refused.control,
          )
        catch {
          UnsafeResume("replication slot invalidated") => ()
          error => raise error
        } noraise {
          _ => fail("lost WAL silently resumed")
        }
        assert_eq(@fs.read_file(path + ".checkpoint").binary(), checkpoint)
        assert_eq(@fs.read_file(path).binary(), state)
        assert_eq(@fs.read_file(dir + "/target").binary(), target_bytes)
        let target = PipelineProbe::new(
          dir + "/target",
          sql,
          ApplyControl(),
          "999",
          false,
          false,
          None,
          schema="slot_recreate",
        )
        run_recreate_probe(
          config,
          target,
          path,
          None,
          Lsn::parse(old.initial_lsn.unwrap()),
          policy~,
          slot_failover=policy is IncludeAllTables,
        )
        let fresh = read_pipeline_state(path)
        assert_eq(fresh.version, 1)
        assert_true(
          Lsn::parse(fresh.initial_lsn.unwrap()) >
          Lsn::parse(old.initial_lsn.unwrap()),
        )
        assert_true(fresh.checkpoint_initialized)
        assert_eq(
          cell(
            sql.query(
              "SELECT failover::text FROM pg_replication_slots WHERE slot_name='\{fresh.source.slot}'",
            ),
            0,
            0,
          ),
          if policy is IncludeAllTables {
            "true"
          } else {
            "false"
          },
        )
        for name in ["a", "b"] {
          let oid = @string.parse_uint(
            cell(
              sql.query("SELECT 'slot_recreate.\{name}'::regclass::oid::text"),
              0,
              0,
            ),
          )
          assert_eq(target.data.resets.get(name), Some(2))
          if policy.should_copy_table(oid) {
            assert_probe_table(target, name)
          } else {
            assert_false(target.data.rows[name].contains("1"))
            assert_false(target.data.rows[name].contains("2"))
          }
        }
        assert_eq(target.data.rows["a"].get("999"), Some(Some("sentinel")))
        assert_eq(target.control.activities().length(), 0)
      }
      println(
        "LIVE: real WAL-removed slots preserve state/target/checkpoint by default; explicit recreate resnapshots all table states and honors four copy policies",
      )
    })
  }
}

///|
async test "live slot recreation resumes every durable boundary with legacy file and PostgreSQL stores" {
  if live_config() is Some(config) {
    @async.with_timeout(90000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE SCHEMA slot_recreate; CREATE TABLE slot_recreate.a(id int PRIMARY KEY,value text); CREATE TABLE slot_recreate.b(id int PRIMARY KEY,value text); INSERT INTO slot_recreate.a VALUES(1,'new'); INSERT INTO slot_recreate.b VALUES(1,'new'); CREATE PUBLICATION slot_recreate FOR TABLE slot_recreate.a,slot_recreate.b",
        ),
      )
      defer @async.protect_from_cancel(() => {
        ignore(
          sql.query(
            "DROP PUBLICATION slot_recreate; DROP SCHEMA slot_recreate CASCADE",
          ),
        )
      })
      for backend in ["legacy", "file", "postgres"] {
        for
          phase in [
            "after_intent", "after_drop", "after_create", "after_position", "after_checkpoint",
            "after_complete",
          ] {
          ignore(sql.query("DELETE FROM slot_recreate.a WHERE id=999"))
          let dir = @fs.tmpdir(prefix="mooncdc-recreate-boundary-")
          defer @fs.rmdir(dir, recursive=true)
          @fs.mkdir(dir + "/records")
          let store : &StateStore? = match backend {
            "legacy" => None
            "file" => Some(FileStateStore::new(dir + "/records"))
            _ =>
              Some(
                PostgresStateStore::new(
                  config,
                  store_id=pipeline_nonce(),
                  schema="slot_recreate",
                ),
              )
          }
          let path = dir + "/state"
          let conn = connect(config, 1048576, true)
          defer conn.close()
          let slot = "mooncdc_" + pipeline_nonce()
          let source = identify(conn, config, slot, "slot_recreate")
          let created = conn.query(
            create_slot_sql(
              slot,
              temporary=false,
              export_snapshot=false,
              failover=false,
            ),
          )
          let old = Lsn::parse(cell(created, 0, 1))
          defer drop_slot(sql, slot)
          let oid = @string.parse_uint(
            cell(
              sql.query("SELECT 'slot_recreate.a'::regclass::oid::text"),
              0,
              0,
            ),
          )
          let initial : PipelineState = {
            version: 1,
            source,
            destination_id: "recreate",
            initial_lsn: Some(old.to_string()),
            checkpoint_initialized: true,
            write_horizon: None,
            promotion: None,
            tables: [{ oid, phase: Ready(old.to_string()), decoding: None, }],
          }
          let inject = async fn(session : StateSession?) -> Unit {
            let pipeline : PipelineStore = {
              path,
              session,
              state: initial,
              mutex: @async.Mutex(),
            }
            pipeline.update(state => state)
            store_checkpoint(path + ".checkpoint", source, old, session)
            try
              pipeline.recreate_slot(conn, false, boundary=at => {
                if at == phase {
                  raise TestDestinationFailure
                }
              })
            catch {
              TestDestinationFailure => ()
              error => raise error
            } noraise {
              _ => fail("boundary was not injected")
            }
          }
          match store {
            Some(store) =>
              with_state_store(store, session => inject(Some(session)))
            None => inject(None)
          }
          let pending = recreate_test_state(path, store)
          assert_eq(
            pending.version,
            if phase == "after_complete" {
              1
            } else {
              2
            },
          )
          assert_true(pending.tables.all(table => table.phase is Pending))
          let cp = parse_checkpoint(
            recreate_test_checkpoint(path, store),
            source,
          )
          if phase == "after_checkpoint" || phase == "after_complete" {
            assert_true(cp > old)
          } else {
            assert_eq(cp, old)
          }
          let target = PipelineProbe::new(
            dir + "/target",
            sql,
            ApplyControl(),
            "999",
            false,
            false,
            None,
            schema="slot_recreate",
          )
          run_recreate_probe(config, target, path, store, old)
          let finished = recreate_test_state(path, store)
          assert_eq(finished.version, 1)
          assert_true(finished.checkpoint_initialized)
          assert_eq(target.data.resets.get("a"), Some(1))
          assert_eq(target.data.resets.get("b"), Some(1))
          assert_probe_table(target, "a")
          assert_probe_table(target, "b")
          println(
            "LIVE: recreate \{backend} \{phase} recovered without pairing a new slot with the old checkpoint",
          )
        }
      }
    })
  }
}

///|
priv struct RecreateLifecycleProbe {
  target : PipelineProbe
  mut startups : Int32
  mut shutdowns : Int32
}

///|
impl Destination for RecreateLifecycleProbe with fn startup(self) {
  self.startups += 1
  Destination::startup(self.target)
}

///|
impl Destination for RecreateLifecycleProbe with fn shutdown(self) {
  self.shutdowns += 1
}

///|
impl Destination for RecreateLifecycleProbe with fn reset_table(
  self,
  relation,
  attempt~,
) {
  Destination::reset_table(self.target, relation, attempt~)
}

///|
impl Destination for RecreateLifecycleProbe with fn write_rows(
  self,
  relation,
  rows,
  id,
) {
  Destination::write_rows(self.target, relation, rows, id)
}

///|
impl Destination for RecreateLifecycleProbe with fn write_transaction(self, tx) {
  Destination::write_transaction(self.target, tx)
}

///|
impl Destination for RecreateLifecycleProbe with fn flush(self) {
  Destination::flush(self.target)
}

///|
async test "live running pipeline drains old destination before recreating an invalidated slot" {
  if live_config() is Some(config) {
    @async.with_timeout(60000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(sql.query("ALTER SYSTEM SET max_slot_wal_keep_size='0'"))
      ignore(sql.query("SELECT pg_reload_conf()"))
      ignore(
        sql.query(
          "CREATE SCHEMA slot_recreate; CREATE TABLE slot_recreate.a(id int PRIMARY KEY,value text); CREATE TABLE slot_recreate.b(id int PRIMARY KEY,value text); INSERT INTO slot_recreate.a VALUES(1,'old'); INSERT INTO slot_recreate.b VALUES(1,'old'); CREATE PUBLICATION slot_recreate FOR TABLE slot_recreate.a,slot_recreate.b",
        ),
      )
      defer @async.protect_from_cancel(() => {
        ignore(
          sql.query(
            "DROP PUBLICATION slot_recreate; DROP SCHEMA slot_recreate CASCADE",
          ),
        )
      })
      let dir = @fs.tmpdir(prefix="mooncdc-recreate-running-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/state"
      defer (if @fs.exists(path) {
        drop_slot(sql, read_pipeline_state(path).source.slot)
      })
      let target = PipelineProbe::new(
        dir + "/target",
        sql,
        ApplyControl(),
        "999",
        false,
        false,
        None,
        schema="slot_recreate",
      )
      let destination : RecreateLifecycleProbe = {
        target,
        startups: 0,
        shutdowns: 0,
      }
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          while !@fs.exists(path) ||
                !read_pipeline_state(path).tables.all(table => {
                  table.phase is (Catchup(_) | Ready(_))
                }) ||
                read_pipeline_state(path).tables.length() != 2 {
            @async.sleep(5)
          }
          let old = read_pipeline_state(path)
          let slot = old.source.slot
          let query = "SELECT active::text,active_pid::text FROM pg_replication_slots WHERE slot_name='\{slot}'"
          let mut active = sql.query(query)
          while cell(active, 0, 0) != "true" {
            @async.sleep(5)
            active = sql.query(query)
          }
          let pid = @string.parse_int(cell(active, 0, 1))
          ignore(sql.query("SELECT pg_terminate_backend(\{pid})"))
          while cell(sql.query(query), 0, 0) == "true" {
            @async.sleep(1)
          }
          ignore(
            sql.query("UPDATE slot_recreate.a SET value='offline' WHERE id=1"),
          )
          invalidate_test_slot(sql, slot)
          while true {
            @async.sleep(5)
            let state = read_pipeline_state(path)
            if state.version == 1 &&
              state.initial_lsn is Some(initial) &&
              Lsn::parse(initial) > Lsn::parse(old.initial_lsn.unwrap()) &&
              state.tables.all(table => table.phase is (Ready(_) | Catchup(_))) {
              break
            }
          }
          ignore(
            sql.query("INSERT INTO slot_recreate.a VALUES(999,'sentinel')"),
          )
          while !target.stop_requested {
            @async.sleep(5)
          }
          target.control.stop()
        })
        run_pipeline(
          config,
          destination,
          publication="slot_recreate",
          destination_id="recreate",
          state_path=path,
          control=target.control,
          invalidated_slot_behavior=RecreateInvalidatedSlot,
          table_retry_delay_ms=500,
        )
        group.return_immediately(())
      })
      assert_eq((destination.startups, destination.shutdowns), (1, 1))
      assert_eq(target.data.resets.get("a"), Some(2))
      assert_eq(target.data.resets.get("b"), Some(2))
      assert_probe_table(target, "a")
      assert_probe_table(target, "b")
      assert_eq(target.control.activities().length(), 0)
      println(
        "LIVE: source backend termination + real WAL invalidation restarts all source workers, resnapshots tables and retains one destination startup/shutdown lifetime",
      )
    })
  }
}
