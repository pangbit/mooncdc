///|
async test "real process kill at each recreate boundary preserves resynchronization intent" {
  guard @env.get_env_var("MOONCDC_RECREATE_PHASE") is Some(phase) else {
    return
  }
  let config = live_config().unwrap()
  let dir = @env.get_env_var("MOONCDC_RECREATE_DIR").unwrap()
  let path = dir + "/state"
  let backend = @env.get_env_var("MOONCDC_RECREATE_STORE").unwrap()
  let selected = @env.get_env_var("MOONCDC_RECREATE_BOUNDARY").unwrap()
  let external : &StateStore? = match backend {
    "legacy" => None
    "file" => Some(FileStateStore::new(dir + "/records"))
    "postgres" =>
      Some(
        PostgresStateStore::new(
          config,
          store_id="recreate-process",
          schema="recreate_state",
        ),
      )
    _ => fail("unknown recreate backend")
  }
  let state = async fn() -> PipelineState {
    recreate_test_state(path, external)
  }
  let checkpoint = async fn() -> Bytes {
    recreate_test_checkpoint(path, external)
  }
  let capture = async fn(name : String, sql : Connection) -> Unit {
    let current = state()
    @fs.write_file(dir + "/" + name + ".state.raw.txt", state_bytes(current))
    @fs.write_file(dir + "/" + name + ".checkpoint.raw.txt", checkpoint())
    @fs.write_file(
      dir + "/" + name + ".target.raw.txt",
      @fs.read_file(dir + "/target"),
    )
    let rows = sql.query(
      "SELECT slot_name,active::text,wal_status,invalidation_reason,restart_lsn::text,confirmed_flush_lsn::text FROM pg_replication_slots ORDER BY slot_name",
    )
    @fs.write_file(
      dir + "/" + name + ".slots.json",
      rows.to_json().stringify(indent=2),
    )
    let source_rows : Map[String, Json] = Map([])
    for name in ["a", "b"] {
      source_rows[name] = sql
        .query("SELECT id::text,value FROM slot_recreate.\{name} ORDER BY id")
        .to_json()
    }
    @fs.write_file(
      dir + "/" + name + ".source.json",
      source_rows.to_json().stringify(indent=2),
    )
  }
  @async.with_timeout(120000, () => {
    let sql = connect(config, 16777216, false)
    defer sql.close()
    if phase == "prepare" {
      if backend == "file" {
        @fs.mkdir(dir + "/records")
      }
      ignore(sql.query("CREATE SCHEMA recreate_state"))
      ignore(sql.query("ALTER SYSTEM SET max_slot_wal_keep_size='0'"))
      ignore(sql.query("SELECT pg_reload_conf()"))
      ignore(
        sql.query(
          "CREATE SCHEMA slot_recreate; CREATE TABLE slot_recreate.a(id int PRIMARY KEY,value text); CREATE TABLE slot_recreate.b(id int PRIMARY KEY,value text); INSERT INTO slot_recreate.a VALUES(1,'old'),(2,'deleted'); INSERT INTO slot_recreate.b VALUES(1,'old'),(2,'deleted'); INSERT INTO slot_recreate.a SELECT i,CASE WHEN i%2=0 THEN NULL ELSE '中文-'||i::text END FROM generate_series(3,66) i; INSERT INTO slot_recreate.b SELECT * FROM slot_recreate.a WHERE id>=3; CREATE PUBLICATION slot_recreate FOR TABLE slot_recreate.a,slot_recreate.b",
        ),
      )
    }
    with_file_schema_store(dir + "/schemas", identity="recreate-process", schemas => {
      let run = async fn(
        probe : PipelineProbe,
        recreate : Bool,
        copy_concurrency : Int,
      ) -> Unit {
        @async.with_task_group(group => {
          group.spawn_bg(() => {
            while true {
              @async.sleep(10)
              if probe.stop_requested &&
                state().tables.all(table => {
                  table.phase is (Ready(_) | Catchup(_))
                }) {
                probe.control.stop()
                break
              }
            }
          })
          run_pipeline(
            config,
            probe,
            publication="slot_recreate",
            destination_id="recreate-process",
            state_path=if external is None { path } else { "" },
            state_store?=external,
            schema_store=schemas,
            control=probe.control,
            copy_concurrency~,
            copy_connections_per_table=2,
            batch_rows=8,
            invalidated_slot_behavior=if recreate {
              RecreateInvalidatedSlot
            } else {
              RejectInvalidatedSlot
            },
          )
          group.return_immediately(())
        })
      }
      if phase == "prepare" {
        let probe = PipelineProbe::new(
          dir + "/target",
          sql,
          ApplyControl(),
          "99",
          false,
          false,
          None,
          schema="slot_recreate",
        )
        run(probe, false, 2)
        let before = state()
        assert_true(before.tables.all(table => table.phase is Ready(_)))
        assert_true(before.checkpoint_initialized)
        assert_probe_table(probe, "a")
        assert_probe_table(probe, "b")
        capture("before", sql)
        ignore(
          sql.query(
            "BEGIN; UPDATE slot_recreate.a SET value='offline' WHERE id=1; DELETE FROM slot_recreate.a WHERE id IN(2,99); INSERT INTO slot_recreate.a VALUES(150,'offline-insert'); UPDATE slot_recreate.b SET value='offline' WHERE id=1; DELETE FROM slot_recreate.b WHERE id=2; INSERT INTO slot_recreate.b VALUES(150,NULL); COMMIT",
          ),
        )
        invalidate_test_slot(sql, before.source.slot)
        capture("lost", sql)
      } else if phase == "kill" {
        let before = state()
        let old_cp = parse_checkpoint(checkpoint(), before.source)
        let repl = connect(config, 16777216, true)
        defer repl.close()
        assert_true(pipeline_slot_invalidated(sql, before.source))
        if selected == "during_copy" {
          let probe = PipelineProbe::new(
            dir + "/target",
            sql,
            ApplyControl(),
            "unused",
            false,
            false,
            None,
            schema="slot_recreate",
            crash_signal=dir + "/copy.ready",
          )
          @async.with_task_group(group => {
            group.spawn_bg(() => {
              while !@fs.exists(dir + "/copy.ready") {
                @async.sleep(5)
              }
              let current = state()
              assert_eq(current.version, 1)
              assert_eq(current.tables.length(), 2)
              assert_true(current.tables[0].phase is (Ready(_) | Catchup(_)))
              assert_true(current.tables[1].phase is Copying)
              assert_true(Lsn::parse(current.initial_lsn.unwrap()) > old_cp)
              assert_true(
                parse_checkpoint(checkpoint(), current.source) >=
                Lsn::parse(current.initial_lsn.unwrap()),
              )
              assert_probe_table(probe, "a")
              assert_true(probe.data.rows["b"].length() > 0)
              assert_true(
                probe.data.rows["b"].length() <
                sql.query("SELECT id FROM slot_recreate.b").length(),
              )
              capture("boundary", sql)
              @fs.write_file(dir + "/boundary.ready", selected)
            })
            run(probe, true, 1)
          })
          fail("recreated COPY worker returned without process kill")
        }
        let halt = async fn(session : StateSession?) -> Unit {
          let lock = record_lock(path, session)
          defer (if lock is Some(lock) { lock.close() })
          let store : PipelineStore = {
            path,
            session,
            state: before,
            mutex: @async.Mutex(),
          }
          store.recreate_slot(repl, false, boundary=at => {
            if at == selected {
              let current = state()
              assert_eq(
                current.version,
                if at == "after_complete" {
                  1
                } else {
                  2
                },
              )
              assert_true(current.tables.all(table => table.phase is Pending))
              let cp = parse_checkpoint(checkpoint(), current.source)
              if at == "after_checkpoint" || at == "after_complete" {
                assert_true(cp > old_cp)
              } else {
                assert_eq(cp, old_cp)
              }
              capture("boundary", sql)
              @fs.write_file(dir + "/boundary.ready", at)
              @async.sleep(60000)
              fail("recreate boundary worker was not killed")
            }
          })
        }
        match external {
          Some(store) => with_state_store(store, session => halt(Some(session)))
          None => halt(None)
        }
        fail("recreate boundary did not fire")
      } else {
        let before = parse_pipeline_state(
          @fs.read_file(dir + "/before.state.raw.txt").binary(),
        )
        let old_cp = parse_checkpoint(
          @fs.read_file(dir + "/before.checkpoint.raw.txt").binary(),
          before.source,
        )
        let pending = state()
        let target_before = @fs.read_file(dir + "/target").binary()
        let cp_before = checkpoint()
        let state_before = state_bytes(pending)
        if phase == "recover" {
          assert_eq(
            target_before,
            @fs.read_file(
              dir +
              (if selected == "during_copy" {
                "/boundary.target.raw.txt"
              } else {
                "/before.target.raw.txt"
              }),
            ).binary(),
          )
          assert_eq(
            state_before,
            @fs.read_file(dir + "/boundary.state.raw.txt").binary(),
          )
          assert_eq(
            cp_before,
            @fs.read_file(dir + "/boundary.checkpoint.raw.txt").binary(),
          )
          if pending.version == 2 {
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
            try run(refused, false, 2) catch {
              UnsafeResume(_) => ()
              error => raise error
            } noraise {
              _ => fail("incomplete recreate silently resumed")
            }
            assert_eq(state_bytes(state()), state_before)
            assert_eq(checkpoint(), cp_before)
            assert_eq(@fs.read_file(dir + "/target").binary(), target_before)
          }
        } else {
          assert_eq(phase, "restart")
        }
        let probe = PipelineProbe::new(
          dir + "/target",
          sql,
          ApplyControl(),
          if phase == "recover" {
            "999"
          } else {
            "1999"
          },
          false,
          false,
          if phase == "restart" {
            Some(
              "BEGIN; UPDATE slot_recreate.a SET value='restart' WHERE id=1; INSERT INTO slot_recreate.b VALUES(1999,'restart'); INSERT INTO slot_recreate.a VALUES(1999,'sentinel'); COMMIT",
            )
          } else {
            None
          },
          schema="slot_recreate",
        )
        let resets = probe.data.resets.copy()
        @async.with_task_group(group => {
          if phase == "recover" {
            group.spawn_bg(() => {
              while true {
                @async.sleep(5)
                let current = state()
                if current.version == 1 &&
                  current.initial_lsn is Some(initial) &&
                  Lsn::parse(initial) > old_cp &&
                  current.tables.all(table => {
                    table.phase is (Ready(_) | Catchup(_))
                  }) {
                  break
                }
              }
              ignore(
                sql.query("INSERT INTO slot_recreate.a VALUES(999,'sentinel')"),
              )
            })
          }
          run(probe, phase == "recover", 2)
          group.return_immediately(())
        })
        let after = state()
        assert_true(after.source == before.source)
        assert_eq(after.version, 1)
        assert_true(after.checkpoint_initialized)
        assert_true(after.tables.all(table => table.phase is Ready(_)))
        assert_true(parse_checkpoint(checkpoint(), after.source) > old_cp)
        assert_true(Lsn::parse(after.initial_lsn.unwrap()) > old_cp)
        for name in ["a", "b"] {
          assert_probe_table(probe, name)
          assert_eq(
            probe.data.resets[name],
            resets[name] +
            (if phase == "recover" && (selected != "during_copy" || name == "b") {
              1
            } else {
              0
            }),
          )
        }
        assert_eq(probe.control.activities().length(), 0)
        capture(phase, sql)
        if phase == "restart" {
          drop_slot(sql, after.source.slot)
          ignore(
            sql.query(
              "DROP PUBLICATION slot_recreate; DROP SCHEMA slot_recreate CASCADE; DROP SCHEMA recreate_state CASCADE",
            ),
          )
          assert_eq(sql.query("SELECT slot_name FROM pg_replication_slots"), [])
        }
      }
    })
  })
  println(
    "RECREATE: " +
    phase +
    " passed; backend=" +
    backend +
    "; boundary=" +
    selected,
  )
}
