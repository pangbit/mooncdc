///|
async test "active pipeline promotes with legacy and file state owners then restarts" {
  guard @env.get_env_var("MOONCDC_ACTIVE_STORE_PHASE") is Some(phase) else {
    return
  }
  let primary = live_config().unwrap()
  let standby = {
    ..primary,
    port: @string.parse_int(@env.get_env_var("MOONCDC_STANDBY_PORT").unwrap()),
  }
  let backend = @env.get_env_var("MOONCDC_ACTIVE_STORE").unwrap()
  let dir = @env.get_env_var("MOONCDC_ACTIVE_STORE_DIR").unwrap()
  let path = dir + "/state"
  let external : &StateStore? = match backend {
    "legacy" => None
    "file" => Some(FileStateStore::new(dir + "/records"))
    _ => fail("expected legacy or file backend")
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
  @async.with_timeout(120000, () => {
    let sql = connect(
      if phase == "active" {
        primary
      } else {
        standby
      },
      16777216,
      false,
    )
    defer sql.close()
    if phase == "active" {
      if backend == "file" {
        @fs.mkdir(dir + "/records")
      }
      ignore(
        sql.query(
          "CREATE SCHEMA active_store; CREATE TABLE active_store.a(id int PRIMARY KEY,value text); CREATE TABLE active_store.b(id int PRIMARY KEY,value text); INSERT INTO active_store.a SELECT i,CASE WHEN i%2=0 THEN NULL ELSE '中文-'||i::text END FROM generate_series(1,64)i; INSERT INTO active_store.b SELECT * FROM active_store.a; CREATE PUBLICATION active_store FOR TABLE active_store.a,active_store.b",
        ),
      )
    }
    let proxy = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer proxy.close()
    let mut upstream = if phase == "active" {
      primary.port
    } else {
      standby.port
    }
    let routed = { ..primary, port: proxy.addr.port(), }
    let control = ApplyControl()
    let probe = PipelineProbe::new(
      dir + "/target",
      sql,
      control,
      "299",
      false,
      false,
      None,
      schema="active_store",
    )
    let resets_before = probe.data.resets.to_json()
    with_file_schema_store(dir + "/schemas", identity="active-store", schemas => {
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          proxy.run_forever((client, _) => {
            defer client.close()
            try {
              let remote = @socket.Tcp::connect_to_host(
                "127.0.0.1",
                port=upstream,
              )
              defer remote.close()
              let forward = async fn(
                input : @socket.Tcp,
                output : @socket.Tcp,
              ) -> Unit {
                while input.read_some(max_len=65536) is Some(data) {
                  output.write(data)
                }
              }
              ignore(
                @async.any([
                  () => forward(client, remote),
                  () => forward(remote, client),
                ]),
              )
            } catch {
              _ => ()
            }
          })
        })
        let pipeline = group.spawn(() => {
          run_pipeline(
            routed,
            probe,
            publication="active_store",
            destination_id="active-store-probe",
            state_path=if external is None { path } else { "" },
            state_store?=external,
            schema_store=schemas,
            control~,
            copy_concurrency=2,
            copy_connections_per_table=2,
            batch_rows=8,
            slot_failover=true,
          )
        })
        let promoted = connect(standby, 16777216, false)
        defer promoted.close()
        if phase == "active" {
          while probe.data.rows.length() != 2 ||
                !read_state().tables.all(table => table.phase is Ready(_)) {
            // A published Commit beyond both private cutoffs is required for
            // main apply to finish Catchup; keepalives cannot acknowledge it.
            ignore(
              sql.query("UPDATE active_store.a SET value=value WHERE id=1"),
            )
            @async.sleep(100)
          }
          let state = read_state()
          let slot = state.source.slot
          let mut synchronized = false
          for ;; {
            if !synchronized {
              ignore(
                sql.query(
                  "UPDATE active_store.a SET value=value WHERE id=1; SELECT pg_log_standby_snapshot()",
                ),
              )
            }
            let rows = promoted.query(
              "SELECT failover::text,synced::text,temporary::text,COALESCE(invalidation_reason,''),confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='\{slot}'",
            )
            if rows.length() == 1 &&
              rows[0][:4] ==
              [Some("true"), Some("true"), Some("false"), Some("")] {
              synchronized = true
              let cp = parse_checkpoint(read_cp(), state.source)
              if rows[0][4] == Some(cp.to_string()) &&
                read_state().write_horizon is Some(_) {
                break
              }
            }
            @async.sleep(100)
          }
          let before = read_state()
          let cp = read_cp()
          let resets = probe.data.resets.to_json()
          assert_probe_table(probe, "a")
          assert_probe_table(probe, "b")
          @fs.write_file(dir + "/before.state", state_bytes(before))
          @fs.write_file(dir + "/before.checkpoint", cp)
          @fs.write_file(dir + "/cutover.ready", slot)
          while !@fs.exists(dir + "/cutover.done") {
            @async.sleep(20)
          }
          upstream = standby.port
          assert_eq(
            cell(promoted.query("SELECT pg_is_in_recovery()::text"), 0, 0),
            "false",
          )
          ignore(
            promoted.query(
              "BEGIN; UPDATE active_store.a SET value='promoted' WHERE id=1; DELETE FROM active_store.a WHERE id=2; INSERT INTO active_store.a VALUES(199,'promoted'); UPDATE active_store.b SET value=NULL WHERE id=1; INSERT INTO active_store.b VALUES(199,'中文'); COMMIT",
            ),
          )
          for ;; {
            let after = read_state()
            if after.source.timeline != before.source.timeline &&
              probe.data.rows["a"].contains("199") &&
              probe.data.rows["b"].contains("199") &&
              parse_checkpoint(read_cp(), after.source) >
              parse_checkpoint(cp, before.source) {
              assert_eq(after.source.system_id, before.source.system_id)
              assert_eq(after.source.slot, slot)
              assert_true(
                timeline_number(after.source.timeline) >
                timeline_number(before.source.timeline),
              )
              assert_eq(probe.data.resets.to_json(), resets)
              assert_true(after.promotion is None)
              @fs.write_file(dir + "/promoted.state", state_bytes(after))
              @fs.write_file(dir + "/promoted.checkpoint", read_cp())
              while !@fs.exists(dir + "/same-pid.confirmed") {
                @async.sleep(20)
              }
              break
            }
            @async.sleep(20)
          }
          ignore(
            promoted.query(
              "BEGIN; UPDATE active_store.a SET value='same-process' WHERE id=1; INSERT INTO active_store.a VALUES(299,'sentinel'); INSERT INTO active_store.b VALUES(299,NULL); COMMIT",
            ),
          )
        } else {
          ignore(
            promoted.query(
              "BEGIN; UPDATE active_store.a SET value='restart' WHERE id=1; INSERT INTO active_store.a VALUES(399,'restart'); INSERT INTO active_store.b VALUES(399,NULL); UPDATE active_store.a SET value=value WHERE id=299; COMMIT",
            ),
          )
        }
        while !probe.stop_requested ||
              !read_state().tables.all(table => table.phase is Ready(_)) {
          @async.sleep(20)
        }
        // Wait for the final commit's durable checkpoint before graceful stop.
        let expected = Lsn::parse(read_state().write_horizon.unwrap())
        while parse_checkpoint(read_cp(), read_state().source) < expected {
          @async.sleep(20)
        }
        control.stop()
        pipeline.wait()
        for table in ["a", "b"] {
          let rows = promoted.query(
            "SELECT id::text,value FROM active_store.\{table} ORDER BY id",
          )
          assert_eq(probe.data.rows[table].length(), rows.length())
          for row in rows {
            assert_eq(probe.data.rows[table][row[0].unwrap()], row[1])
          }
        }
        if phase == "restart" {
          assert_eq(probe.data.resets.to_json(), resets_before)
        }
        @fs.write_file(dir + "/" + phase + ".state", state_bytes(read_state()))
        @fs.write_file(dir + "/" + phase + ".checkpoint", read_cp())
        group.return_immediately(())
      })
    })
  })
  println("ACTIVE STORE: " + phase + " passed; backend=" + backend)
}
