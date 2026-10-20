///|
async test "only opted copy preflight timeline changes request outer proof before any slot cleanup" {
  for scenario in ["timeline", "backward", "strict", "system", "database"] {
    @async.with_timeout(3000, () => {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let source : Source = {
        system_id: "system",
        timeline: if scenario == "backward" {
          "2"
        } else {
          "1"
        },
        database: "db",
        slot: "slot",
        publication: "pub",
      }
      let actual = {
        ..source,
        timeline: if scenario == "backward" {
          "1"
        } else {
          "2"
        },
        system_id: if scenario == "system" {
          "other"
        } else {
          "system"
        },
        database: if scenario == "database" {
          "other"
        } else {
          "db"
        },
      }
      let config = ConnectionConfig::new(
        host="127.0.0.1",
        port=server.addr.port(),
        user="copy-preflight-fixture",
        database="db",
        security=LocalPlaintext,
      )
      @async.with_task_group(group => {
        let peer = group.spawn(() => {
          let (tcp, _) = server.accept()
          defer tcp.close()
          mock_replication(tcp, "transition", identified=actual)
        })
        try
          cleanup_copy_slot(
            config,
            source,
            42U,
            pipeline_source_change=scenario != "strict",
          )
        catch {
          PipelineSourceChanged =>
            assert_true(scenario == "timeline" || scenario == "backward")
          UnsafeResume("copy slot cleanup source changed") =>
            assert_true(scenario == "strict" || scenario == "system")
          UnsafeResume("database mismatch") => assert_eq(scenario, "database")
          error => raise error
        } noraise {
          _ => fail("unproved copy source transition accepted")
        }
        peer.wait()
      })
    })
  }
}

///|
async test "copy exit defers unavailable or changed source but preserves strict cleanup and fatal guards" {
  for exit_cleanup in [false, true] {
    for
      mode in [
        "healthy", "shutdown-57P01", "shutdown-57P02", "shutdown-57P03", "query-shutdown",
        "disconnect", "timeout", "changed", "foreign", "permission", "authentication",
        "protocol",
      ] {
      @async.with_timeout(3000, () => {
        let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
        defer server.close()
        let source : Source = {
          system_id: "system",
          timeline: "1",
          database: "db",
          slot: "slot",
          publication: "pub",
        }
        let oid = 42U
        let slot = failover_copy_slot(source, oid)
        let config = ConnectionConfig::new(
          host="127.0.0.1",
          port=server.addr.port(),
          user="cleanup-fixture",
          database="db",
          security=LocalPlaintext,
          timeout_ms=100,
        )
        let mut dropped = false
        let mut identified = false
        let mut listed = false
        @async.with_task_group(group => {
          let peer = group.spawn(() => {
            let (tcp, _) = server.accept()
            defer tcp.close()
            let size = Cursor::new(tcp.read_exactly(4)).i32()
            ignore(tcp.read_exactly(size - 4))
            if mode.has_prefix("shutdown-") {
              let code = mode.replace_all(old="shutdown-", new="")
              tcp.write(
                packet(
                  b'E',
                  @utf8.encode("C\{code}\u0000Mshutdown\u0000\u0000"),
                ),
              )
              assert_eq(tcp.read_some(), None)
              return
            }
            if mode == "authentication" {
              tcp.write(packet(b'R', b"\x00\x00\x00\x03"))
              assert_eq(tcp.read_some(), None)
              return
            }
            tcp.write(packet(b'R', b"\x00\x00\x00\x00") + packet(b'Z', b"I"))
            let (tag, body) = read_frame(tcp, 4096)
            assert_eq(tag, b'Q')
            assert_eq(Cursor::new(body).cstring(), "IDENTIFY_SYSTEM")
            identified = true
            match mode {
              "disconnect" => return
              "timeout" => {
                assert_eq(tcp.read_some(), None)
                return
              }
              "query-shutdown" => {
                tcp.write(packet(b'E', b"C57P03\x00Mshutdown\x00\x00"))
                assert_eq(tcp.read_some(), None)
                return
              }
              "protocol" => {
                tcp.write(packet(b'X', b""))
                assert_eq(tcp.read_some(), None)
                return
              }
              _ => ()
            }
            mock_row(tcp, [
              Some("system"),
              Some(if mode == "changed" { "2" } else { "1" }),
              Some("0/100"),
              Some("db"),
            ])
            if mode == "changed" {
              // A different timeline must never receive a slot-list or DROP.
              assert_eq(tcp.read_some(), None)
              return
            }
            let (tag, body) = read_frame(tcp, 4096)
            assert_eq(tag, b'Q')
            let query = Cursor::new(body).cstring()
            assert_true(query.has_prefix("SELECT slot_name,plugin,database"))
            assert_true(query.has_suffix("WHERE slot_name IN ('\{slot}')"))
            listed = true
            if mode == "permission" {
              tcp.write(packet(b'E', b"C42501\x00Mpermission\x00\x00"))
              assert_eq(tcp.read_some(), None)
              return
            }
            mock_row(tcp, [
              Some(slot),
              Some("pgoutput"),
              Some("db"),
              Some(if mode == "foreign" { "true" } else { "false" }),
              Some("false"),
            ])
            if mode == "foreign" {
              // An incompatible slot still fails even during exit cleanup.
              assert_eq(tcp.read_some(), None)
              return
            }
            let (tag, body) = read_frame(tcp, 4096)
            assert_eq(tag, b'Q')
            assert_eq(
              Cursor::new(body).cstring(),
              "SELECT pg_drop_replication_slot('\{slot}')",
            )
            dropped = true
            mock_row(tcp, [None])
            let (tag, body) = read_frame(tcp, 4096)
            assert_eq(tag, b'Q')
            assert_eq(Cursor::new(body).cstring(), query)
            tcp.write(packet(b'Z', b"I"))
            assert_eq(tcp.read_some(), None)
          })
          let deferred = mode.has_prefix("shutdown-") ||
            ["query-shutdown", "disconnect", "timeout", "changed"].contains(
              mode,
            )
          let check_error = fn(error : Error) -> Unit raise {
            match error {
              Server(code, _) =>
                assert_eq(
                  code,
                  if mode == "permission" {
                    "42501"
                  } else if mode == "query-shutdown" {
                    "57P03"
                  } else {
                    mode.replace_all(old="shutdown-", new="")
                  },
                )
              UnsafeResume(message) =>
                assert_eq(
                  message,
                  if mode == "changed" {
                    "copy slot cleanup source changed"
                  } else {
                    "incompatible owned copy slot"
                  },
                )
              Authentication(_) => assert_eq(mode, "authentication")
              Protocol(_) => assert_eq(mode, "protocol")
              @async.TimeoutError => assert_eq(mode, "timeout")
              @io.PipeClosed | @io.ReaderClosed => assert_eq(mode, "disconnect")
              error => raise error
            }
          }
          try {
            if exit_cleanup {
              cleanup_copy_slot_on_exit(config, source, oid)
            } else {
              cleanup_copy_slot(config, source, oid)
            }
          } catch {
            PipelineCritical(error) => {
              assert_true(exit_cleanup && !deferred && mode != "healthy")
              check_error(error)
            }
            error => {
              assert_false(exit_cleanup)
              assert_true(mode != "healthy")
              check_error(error)
            }
          } noraise {
            _ => assert_true(mode == "healthy" || (exit_cleanup && deferred))
          }
          peer.wait()
        })
        assert_eq(dropped, mode == "healthy")
        assert_eq(
          identified,
          !mode.has_prefix("shutdown-") && mode != "authentication",
        )
        assert_eq(listed, ["healthy", "foreign", "permission"].contains(mode))
      })
    }
  }
}

///|
test "failover slot commands keep default grammar and reject temporary slots" {
  assert_eq(
    create_slot_sql(
      "owned",
      temporary=false,
      export_snapshot=false,
      failover=false,
    ),
    "CREATE_REPLICATION_SLOT owned LOGICAL pgoutput NOEXPORT_SNAPSHOT",
  )
  assert_eq(
    create_slot_sql(
      "owned",
      temporary=true,
      export_snapshot=true,
      failover=false,
    ),
    "CREATE_REPLICATION_SLOT owned TEMPORARY LOGICAL pgoutput EXPORT_SNAPSHOT",
  )
  assert_eq(
    create_slot_sql(
      "owned",
      temporary=false,
      export_snapshot=true,
      failover=true,
    ),
    "CREATE_REPLICATION_SLOT owned LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER)",
  )
  for slot in ["owned", "injected;SQL"] {
    try
      create_slot_sql(
        slot,
        temporary=true,
        export_snapshot=false,
        failover=true,
      )
    catch {
      InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid slot configuration accepted")
    }
  }
}

///|
async test "live failover snapshot slot remains persistent after success and callback failure" {
  if live_config() is Some(config) {
    @async.with_timeout(15000, () => {
      let sql = connect(config, 16777216, false)
      defer sql.close()
      for failing in [false, true] {
        let slot = "failover_" + pipeline_nonce()
        defer drop_slot(sql, slot)
        let mut seen = false
        try
          ignore(
            copy_snapshot(
              config,
              slot~,
              publication="mooncdc_test",
              slot_failover=true,
              event => {
                if event is TableBegin(_) {
                  assert_eq(
                    sql.query(
                      "SELECT failover::text,temporary::text FROM pg_replication_slots WHERE slot_name='\{slot}'",
                    ),
                    [[Some("true"), Some("false")]],
                  )
                  seen = true
                  if failing {
                    raise PipelineProbeFailure
                  }
                }
              },
            ),
          )
        catch {
          PipelineProbeFailure => assert_true(failing)
          error => raise error
        } noraise {
          _ => assert_false(failing)
        }
        assert_true(seen)
        assert_eq(
          cell(
            sql.query(
              "SELECT active::text FROM pg_replication_slots WHERE slot_name='\{slot}'",
            ),
            0,
            0,
          ),
          "false",
        )
      }
      println(
        "LIVE: persistent exported failover snapshots survive success and callback failure",
      )
    })
  }
}

///|
async test "live pipeline failover upgrades main slot and cleans copy slots across configuration changes" {
  if live_config() is Some(config) {
    @async.with_timeout(30000, () => {
      let sql = connect(config, 16777216, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE SCHEMA failover_test; CREATE TABLE failover_test.a(id int PRIMARY KEY,value text); CREATE TABLE failover_test.b(id int PRIMARY KEY,value text); INSERT INTO failover_test.a VALUES(1,'a'); INSERT INTO failover_test.b VALUES(1,'b'); CREATE PUBLICATION failover_test FOR TABLE failover_test.a,failover_test.b",
        ),
      )
      defer ignore(
        sql.query(
          "DROP PUBLICATION failover_test; DROP SCHEMA failover_test CASCADE",
        ),
      )
      let dir = @fs.tmpdir(prefix="mooncdc-failover-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/state"
      let stop = ApplyControl()
      run_pipeline(
        config,
        MigrationDestination::{ control: stop, },
        publication="failover_test",
        destination_id="failover-test",
        state_path=path,
        control=stop,
      )
      let source = read_pipeline_state(path).source
      defer drop_slot(sql, source.slot)
      let flag = "SELECT failover::text FROM pg_replication_slots WHERE slot_name='\{source.slot}'"
      assert_eq(cell(sql.query(flag), 0, 0), "false")
      let control = ApplyControl()
      let probe = PipelineProbe::new(
        dir + "/target",
        sql,
        control,
        "99",
        false,
        false,
        None,
        schema="failover_test",
      )
      run_probe_pipeline(
        config,
        probe,
        publication="failover_test",
        destination_id="failover-test",
        state_path=path,
        slot_failover=true,
      )
      assert_eq(cell(sql.query(flag), 0, 0), "true")
      for name in ["a", "b"] {
        assert_probe_table(probe, name)
      }
      let state = read_pipeline_state(path)
      let prefix = copy_slot_prefix(source.slot)
      let copies = "SELECT slot_name FROM pg_replication_slots WHERE left(slot_name,\{prefix.length()})='\{prefix}'"
      assert_eq(sql.query(copies).length(), 0)
      let failed_path = dir + "/failed-state"
      let failed_control = ApplyControl()
      let failed_probe = PipelineProbe::new(
        dir + "/failed-target",
        sql,
        failed_control,
        "unused",
        true,
        false,
        None,
        schema="failover_test",
      )
      run_probe_pipeline(
        config,
        failed_probe,
        publication="failover_test",
        destination_id="failover-failure",
        state_path=failed_path,
        slot_failover=true,
        stop_on_error=true,
        copy_concurrency=1,
      )
      let failed = read_pipeline_state(failed_path)
      defer drop_slot(sql, failed.source.slot)
      assert_eq(
        cell(
          sql.query(
            "SELECT failover::text FROM pg_replication_slots WHERE slot_name='\{failed.source.slot}'",
          ),
          0,
          0,
        ),
        "true",
      )
      let failed_prefix = copy_slot_prefix(failed.source.slot)
      assert_eq(
        sql
        .query(
          "SELECT slot_name FROM pg_replication_slots WHERE left(slot_name,\{failed_prefix.length()})='\{failed_prefix}'",
        )
        .length(),
        0,
      )
      // Simulate loss of cleanup after the durable handover. The table is then
      // unpublished and failover disabled; its recorded ownership must survive
      // until the slot is removed, before publication reconciliation.
      let oid = @string.parse_uint(
        cell(sql.query("SELECT 'failover_test.b'::regclass::oid::text"), 0, 0),
      )
      let slot = failover_copy_slot(source, oid)
      let repl = connect(config, 16777216, true)
      defer repl.close()
      ignore(
        repl.query(
          create_slot_sql(
            slot,
            temporary=false,
            export_snapshot=false,
            failover=true,
          ),
        ),
      )
      ignore(
        sql.query("ALTER PUBLICATION failover_test DROP TABLE failover_test.b"),
      )
      for enabled in [false, true] {
        let control = ApplyControl()
        run_pipeline(
          config,
          MigrationDestination::{ control, },
          publication="failover_test",
          destination_id="failover-test",
          state_path=path,
          control~,
          slot_failover=enabled,
        )
        assert_eq(cell(sql.query(flag), 0, 0), "true")
        assert_eq(sql.query(copies).length(), 0)
      }
      assert_eq(
        read_pipeline_state(path).tables.length(),
        state.tables.length() - 1,
      )
      // Exact-name cleanup refuses a temporary slot, even with an owned name.
      let owned = failover_copy_slot(source, state.tables[0].oid)
      ignore(
        repl.query(
          create_slot_sql(
            owned,
            temporary=true,
            export_snapshot=false,
            failover=false,
          ),
        ),
      )
      try cleanup_copy_slots(repl, source, [state.tables[0].oid]) catch {
        UnsafeResume(_) => ()
        error => raise error
      } noraise {
        _ => fail("temporary slot was removed")
      }
      assert_eq(sql.query(copies).length(), 1)
      println(
        "LIVE: failover main upgrade/no downgrade, private copy cleanup, removed-table recovery and foreign lifetime refusal",
      )
    })
  }
}
