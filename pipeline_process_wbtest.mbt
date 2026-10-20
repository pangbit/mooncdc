///|
async test "pipeline process crash worker" {
  if @env.get_env_var("MOONCDC_PIPELINE_PHASE") is Some(phase) {
    let config = live_config().unwrap()
    let dir = @env.get_env_var("MOONCDC_PIPELINE_DIR").unwrap()
    let path = dir + "/state"
    let backend = @env.get_env_var("MOONCDC_PIPELINE_STORE").unwrap_or("legacy")
    let retain_decoder = @env.get_env_var("MOONCDC_PIPELINE_DECODER") ==
      Some("retained")
    let slot_failover = @env.get_env_var("MOONCDC_PIPELINE_FAILOVER") ==
      Some("true")
    let external : &StateStore? = match backend {
      "legacy" => None
      "file" => Some(FileStateStore::new(dir + "/records"))
      "postgres" =>
        Some(
          PostgresStateStore::new(
            config,
            store_id="crash",
            schema="pipeline_crash",
          ),
        )
      _ => fail("unknown state backend")
    }
    let state = async fn() -> PipelineState {
      match external {
        Some(store) => parse_pipeline_state(store.load(Pipeline).unwrap())
        None => read_pipeline_state(path)
      }
    }
    let checkpoint = async fn(source : Source) -> Lsn {
      match external {
        Some(store) =>
          parse_checkpoint(store.load(ReplicationCheckpoint).unwrap(), source)
        None => load_checkpoint(path + ".checkpoint", source)
      }
    }
    let sql = connect(config, 16777216, false)
    defer sql.close()
    with_file_schema_store(dir + "/schemas", identity="pipeline-crash", schemas => {
      match phase {
        "init" => {
          if backend == "file" {
            @fs.mkdir(dir + "/records")
          }
          ignore(
            sql.query(
              "CREATE SCHEMA pipeline_crash; CREATE TABLE pipeline_crash.a(id int PRIMARY KEY,value text); CREATE TABLE pipeline_crash.b(id int PRIMARY KEY,value text); INSERT INTO pipeline_crash.a VALUES(1,'before'),(2,NULL); INSERT INTO pipeline_crash.b VALUES(1,'before'),(2,NULL); CREATE PUBLICATION pipeline_crash FOR TABLE pipeline_crash.a,pipeline_crash.b",
            ),
          )
        }
        "crash" => {
          let control = ApplyControl()
          let probe = PipelineProbe::new(
            dir + "/target",
            sql,
            control,
            "unused",
            false,
            !retain_decoder,
            None,
            schema="pipeline_crash",
            crash_signal=dir + "/ready",
          )
          run_pipeline(
            config,
            probe,
            publication="pipeline_crash",
            destination_id="process-probe",
            state_path=if external is None { path } else { "" },
            state_store?=external,
            schema_store=schemas,
            control~,
            copy_concurrency=1,
            batch_rows=1,
            slot_failover~,
          )
          fail("crash worker returned")
        }
        "recover" => {
          let before = state()
          assert_true(before.tables[0].phase is (Catchup(_) | Ready(_)))
          assert_true(before.tables[1].phase is Copying)
          if slot_failover {
            let slot = failover_copy_slot(before.source, before.tables[1].oid)
            let rows = sql.query(
              "SELECT failover::text,temporary::text FROM pg_replication_slots WHERE slot_name='\{slot}'",
            )
            assert_eq(rows, [[Some("true"), Some("false")]])
          }
          let cutoff = match before.tables[0].phase {
            Catchup(lsn) | Ready(lsn) => Lsn::parse(lsn)
            _ => fail("copy not completed")
          }
          if before.tables[0].decoding is Some(decoding) {
            assert_eq(decoding.replicated, [true, true])
            assert_eq(decoding.identity, [true, false])
            ignore(decoding.restore(schemas, before.tables[0].oid, cutoff))
          } else {
            assert_false(retain_decoder)
            assert_true(checkpoint(before.source) >= cutoff)
          }
          if retain_decoder {
            assert_true(before.tables[0].decoding is Some(_))
          }
          assert_true(
            checkpoint(before.source) >= Lsn::parse(before.initial_lsn.unwrap()),
          )
          ignore(
            sql.query(
              "BEGIN; UPDATE pipeline_crash.a SET value='after-kill' WHERE id=1; DELETE FROM pipeline_crash.a WHERE id=2; INSERT INTO pipeline_crash.b VALUES(3,'after-kill'); COMMIT",
            ),
          )
          let control = ApplyControl()
          let probe = PipelineProbe::new(
            dir + "/target",
            sql,
            control,
            "99",
            false,
            false,
            None,
            schema="pipeline_crash",
          )
          @async.with_timeout(30000, () => {
            @async.with_task_group(group => {
              group.spawn_bg(() => {
                for ;; {
                  @async.sleep(10)
                  if probe.stop_requested &&
                    state().tables.all(table => {
                      table.phase is (Ready(_) | Catchup(_))
                    }) {
                    control.stop()
                    break
                  }
                }
              })
              run_pipeline(
                config,
                probe,
                publication="pipeline_crash",
                destination_id="process-probe",
                state_path=if external is None { path } else { "" },
                state_store?=external,
                schema_store=schemas,
                control~,
                batch_rows=1,
                slot_failover~,
              )
              group.return_immediately(())
            })
          })
          assert_eq(probe.data.resets.get("a"), Some(1))
          assert_eq(probe.data.resets.get("b"), Some(2))
          assert_probe_table(probe, "a")
          assert_probe_table(probe, "b")
          assert_true(state().tables.all(table => table.phase is Ready(_)))
          assert_true(state().tables[0].decoding is None)
          let prefix = copy_slot_prefix(before.source.slot)
          assert_eq(
            sql
            .query(
              "SELECT slot_name FROM pg_replication_slots WHERE left(slot_name,\{prefix.length()})='\{prefix}'",
            )
            .length(),
            0,
          )
          assert_true(
            checkpoint(before.source) > Lsn::parse(before.initial_lsn.unwrap()),
          )
        }
        "cleanup" => {
          let present = match external {
            Some(store) => store.load(Pipeline) is Some(_)
            None => @fs.exists(path)
          }
          if present {
            let stored = state()
            @async.with_timeout(config.timeout_ms, () => {
              cleanup_copy_slots(
                sql,
                stored.source,
                stored.tables.map(table => table.oid),
              )
            })
            drop_slot(sql, stored.source.slot)
          }
          ignore(
            sql.query(
              "DROP PUBLICATION pipeline_crash; DROP SCHEMA pipeline_crash CASCADE",
            ),
          )
        }
        _ => fail("unknown process phase")
      }
    })
    println("PIPELINE worker completed \{phase}")
  }
}
