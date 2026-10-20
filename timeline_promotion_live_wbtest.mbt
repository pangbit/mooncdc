///|
async test "pipeline resumes a synchronized failover slot after fenced primary promotion" {
  guard @env.get_env_var("MOONCDC_PROMOTION_PHASE") is Some(phase) else {
    return
  }
  let primary = live_config().unwrap()
  let standby = {
    ..primary,
    port: @string.parse_int(@env.get_env_var("MOONCDC_STANDBY_PORT").unwrap()),
  }
  let config = if phase == "prepare" { primary } else { standby }
  let dir = @env.get_env_var("MOONCDC_PROMOTION_DIR").unwrap()
  let path = dir + "/state"
  let backend = @env.get_env_var("MOONCDC_PROMOTION_STORE").unwrap()
  let external : &StateStore? = match backend {
    "legacy" => None
    "file" => Some(FileStateStore::new(dir + "/records"))
    "postgres" =>
      Some(
        PostgresStateStore::new(
          config,
          store_id="promotion",
          schema="promotion_state",
        ),
      )
    _ => fail("unknown promotion backend")
  }
  let read_state = async fn() -> PipelineState {
    match external {
      Some(store) => parse_pipeline_state(store.load(Pipeline).unwrap())
      None => read_pipeline_state(path)
    }
  }
  let read_cp = async fn() -> Bytes {
    match external {
      Some(store) => store.load(ReplicationCheckpoint).unwrap()
      None => @fs.read_file(path + ".checkpoint").binary()
    }
  }
  let persist = async fn(state : PipelineState, cp : Bytes) -> Unit {
    match external {
      Some(store) =>
        with_state_store(store, session => {
          session.save(Pipeline, state_bytes(state))
          session.save(ReplicationCheckpoint, cp)
        })
      None => {
        save_atomic_file(path, state_bytes(state))
        save_atomic_file(path + ".checkpoint", cp)
      }
    }
  }
  @async.with_timeout(120000, () => {
    let sql = connect(config, 16777216, false)
    defer sql.close()
    if phase == "promote" {
      assert_eq(
        cell(sql.query("SELECT pg_is_in_recovery()::text"), 0, 0),
        "true",
      )
      assert_eq(
        cell(sql.query("SELECT pg_promote(true,30)::text"), 0, 0),
        "true",
      )
      assert_eq(
        cell(sql.query("SELECT pg_is_in_recovery()::text"), 0, 0),
        "false",
      )
    }
    if phase == "prepare" {
      if backend == "file" {
        @fs.mkdir(dir + "/records")
      }
      ignore(sql.query("CREATE SCHEMA promotion_state"))
      ignore(
        sql.query(
          "CREATE SCHEMA promotion_fixture; CREATE TABLE promotion_fixture.a(id int PRIMARY KEY,value text); CREATE TABLE promotion_fixture.b(id int PRIMARY KEY,value text); INSERT INTO promotion_fixture.a VALUES(1,'before'),(2,NULL); INSERT INTO promotion_fixture.b VALUES(1,'before'),(2,NULL); CREATE PUBLICATION promotion_fixture FOR TABLE promotion_fixture.a,promotion_fixture.b",
        ),
      )
    }
    with_file_schema_store(dir + "/schemas", identity="promotion", schemas => {
      let run = async fn(probe : PipelineProbe, failover : Bool) -> Unit {
        @async.with_task_group(group => {
          group.spawn_bg(() => {
            for ;; {
              @async.sleep(10)
              if probe.stop_requested &&
                read_state().tables.all(table => {
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
            publication="promotion_fixture",
            destination_id="promotion-probe",
            state_path=if external is None { path } else { "" },
            state_store?=external,
            schema_store=schemas,
            control=probe.control,
            copy_concurrency=2,
            copy_connections_per_table=2,
            batch_rows=1,
            slot_failover=failover,
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
          true,
          None,
          schema="promotion_fixture",
        )
        run(probe, true)
        let state = read_state()
        assert_true(state.tables.all(table => table.phase is Ready(_)))
        assert_true(state.write_horizon is Some(_))
        let reader = connect(standby, 16777216, false)
        defer reader.close()
        let query = "SELECT failover::text,synced::text,temporary::text,invalidation_reason,confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='\{state.source.slot}'"
        for ;; {
          ignore(
            sql.query(
              "SELECT pg_log_standby_snapshot(); UPDATE promotion_fixture.a SET value=value WHERE id=99",
            ),
          )
          subscribe(
            primary,
            slot=state.source.slot,
            publication="promotion_fixture",
            start=Resume,
            checkpoint_path=if external is None {
              path + ".checkpoint"
            } else {
              ""
            },
            checkpoint_store?=external,
            sub => {
              let tx = sub.next()
              let guard_store : PipelineStore = {
                path,
                session: sub.state_session,
                state: read_state(),
                mutex: @async.Mutex(),
              }
              guard_store.record_write_horizon(tx.end_lsn)
              ignore(Destination::write_transaction(probe, tx))
              sub.ack(tx)
              while Lsn::parse(
                      cell(
                        sql.query(
                          "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='\{state.source.slot}'",
                        ),
                        0,
                        0,
                      ),
                    ) <
                    tx.end_lsn {
                @async.sleep(20)
              }
            },
          )
          let rows = reader.query(query)
          if rows.length() == 1 &&
            rows[0][0] == Some("true") &&
            rows[0][1] == Some("true") &&
            rows[0][2] == Some("false") &&
            rows[0][3] == None {
            break
          }
          @async.sleep(250)
        }
        // Slot synchronization is periodic. Stop producing transactions before
        // requiring the synchronized slot to equal the final durable checkpoint.
        let cp = parse_checkpoint(read_cp(), state.source)
        for ;; {
          let rows = reader.query(query)
          if rows ==
            [
              [
                Some("true"),
                Some("true"),
                Some("false"),
                None,
                Some(cp.to_string()),
              ],
            ] {
            break
          }
          @async.sleep(100)
        }
        let replay = Lsn::parse(
          cell(sql.query("SELECT pg_current_wal_lsn()::text"), 0, 0),
        )
        ignore(sql.query("CHECKPOINT"))
        while Lsn::parse(
                cell(
                  reader.query("SELECT pg_last_wal_replay_lsn()::text"),
                  0,
                  0,
                ),
              ) <
              replay {
          @async.sleep(20)
        }
        assert_probe_table(probe, "a")
        assert_probe_table(probe, "b")
        @fs.write_file(dir + "/before.state", state_bytes(read_state()))
        @fs.write_file(dir + "/before.checkpoint", read_cp())
        @fs.write_file(dir + "/before.target", @fs.read_file(dir + "/target"))
      } else {
        let before = read_state()
        let cp_bytes = read_cp()
        let position = if phase == "recover" {
          Lsn::parse(parse_checkpoint_record(cp_bytes).lsn)
        } else {
          parse_checkpoint(cp_bytes, before.source)
        }
        let repl = connect(standby, 16777216, true)
        defer repl.close()
        let actual = identify(
          repl,
          standby,
          before.source.slot,
          before.source.publication,
        )
        if phase == "promote" {
          assert_eq(actual.system_id, before.source.system_id)
          assert_true(
            timeline_number(actual.timeline) >
            timeline_number(before.source.timeline),
          )
          let history = repl.query("TIMELINE_HISTORY \{actual.timeline}")
          let fork = timeline_fork(
            cell(history, 0, 1),
            timeline_number(before.source.timeline),
            timeline_number(actual.timeline),
          )
          @fs.write_file(
            dir + "/history.json",
            history.to_json().stringify(indent=2),
          )
          let beyond : Lsn = { value: fork.value + 1UL, }
          let wrong_slot = { ..before.source, slot: before.source.slot + "x", }
          let cases : Array[(String, PipelineState, Bytes, Bool)] = [
            (
              "legacy-horizon",
              { ..before, write_horizon: None, },
              cp_bytes,
              true,
            ),
            (
              "write-after-fork",
              { ..before, write_horizon: Some(beyond.to_string()), },
              cp_bytes,
              true,
            ),
            (
              "copy-after-fork",
              {
                ..before,
                tables: before.tables.map(table => {
                  ..table,
                  phase: Ready(beyond.to_string()),
                }),
              },
              cp_bytes,
              true,
            ),
            (
              "checkpoint-after-fork",
              before,
              checkpoint_bytes(before.source, beyond),
              true,
            ),
            (
              "slot-ahead-of-consumer",
              before,
              checkpoint_bytes(
                before.source,
                Lsn::parse(before.initial_lsn.unwrap()),
              ),
              true,
            ),
            (
              "missing-slot",
              { ..before, source: wrong_slot, },
              checkpoint_bytes(wrong_slot, position),
              true,
            ),
            ("failover-disabled", before, cp_bytes, false),
            ("corrupt-checkpoint", before, b"bad", true),
          ]
          for (name, state, bytes, failover) in cases {
            persist(state, bytes)
            let target_before = @fs.read_file(dir + "/target").binary()
            let rejected = PipelineProbe::new(
              dir + "/target",
              sql,
              ApplyControl(),
              "unused",
              false,
              false,
              Some("SELECT 1/0"),
              schema="promotion_fixture",
            )
            try run(rejected, failover) catch {
              UnsafeResume(_) if name != "corrupt-checkpoint" => ()
              CheckpointError(_) if name == "corrupt-checkpoint" => ()
              error => raise error
            } noraise {
              _ => fail("unsafe promotion reached destination: " + name)
            }
            assert_eq(state_bytes(read_state()), state_bytes(state))
            assert_eq(read_cp(), bytes)
            assert_eq(@fs.read_file(dir + "/target").binary(), target_before)
            persist(before, cp_bytes)
            println("PROMOTION rejection passed: " + name)
          }
          if @env.get_env_var("MOONCDC_PROMOTION_BOUNDARY") is Some(selected) {
            let halt = async fn(session : StateSession?) -> Unit {
              let lock = record_lock(path, session)
              defer (if lock is Some(lock) { lock.close() })
              let store : PipelineStore = {
                path,
                session,
                state: read_state(),
                mutex: @async.Mutex(),
              }
              store.recover_promotion(repl, actual, boundary=at => {
                if at == selected {
                  @fs.write_file(
                    dir + "/boundary.state",
                    state_bytes(read_state()),
                  )
                  @fs.write_file(dir + "/boundary.checkpoint", read_cp())
                  @fs.write_file(dir + "/boundary.ready", at)
                  @async.sleep(60000)
                  fail("promotion boundary worker was not killed")
                }
              })
            }
            match external {
              Some(store) =>
                with_state_store(store, session => halt(Some(session)))
              None => halt(None)
            }
            fail("promotion boundary did not fire")
          }
        } else if phase == "recover" {
          assert_eq(
            @fs.read_file(dir + "/target").binary(),
            @fs.read_file(dir + "/before.target").binary(),
          )
          assert_eq(
            position,
            Lsn::parse(
              parse_checkpoint_record(
                @fs.read_file(dir + "/before.checkpoint").binary(),
              ).lsn,
            ),
          )
          assert_eq(
            before.tables.to_json(),
            parse_pipeline_state(@fs.read_file(dir + "/before.state").binary()).tables.to_json(),
          )
          assert_true(actual == before.source || before.promotion is Some(_))
        } else {
          assert_true(actual == before.source)
        }
        let sentinel = if phase == "restart" { "299" } else { "199" }
        let target = PipelineProbe::new(
          dir + "/target",
          sql,
          ApplyControl(),
          sentinel,
          false,
          false,
          Some(
            "BEGIN; UPDATE promotion_fixture.a SET value='\{phase}' WHERE id=1; DELETE FROM promotion_fixture.a WHERE id=2; INSERT INTO promotion_fixture.b VALUES(\{sentinel},'\{phase}'); INSERT INTO promotion_fixture.a VALUES(\{sentinel},'sentinel'); COMMIT",
          ),
          schema="promotion_fixture",
        )
        let resets = target.data.resets.to_json()
        run(target, true)
        let after = read_state()
        assert_true(after.source == actual)
        assert_true(after.promotion is None)
        assert_true(parse_checkpoint(read_cp(), actual) > position)
        assert_eq(target.data.resets.to_json(), resets)
        assert_eq(
          after.tables.map(table => table.oid),
          before.tables.map(table => table.oid),
        )
        assert_true(after.tables.all(table => table.phase is Ready(_)))
        assert_probe_table(target, "a")
        assert_probe_table(target, "b")
        @fs.write_file(dir + "/" + phase + ".state", state_bytes(after))
        @fs.write_file(dir + "/" + phase + ".checkpoint", read_cp())
        @fs.write_file(
          dir + "/" + phase + ".target",
          @fs.read_file(dir + "/target"),
        )
      }
    })
  })
  println("PROMOTION: " + phase + " passed; backend=" + backend)
}
