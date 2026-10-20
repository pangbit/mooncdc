///|
async test "replication completion replays unacknowledged transaction but rejects malformed end" {
  for
    scenario in [
      "command", "done", "bad-command", "bad-done", "bad-notice", "bad-status",
    ] {
    @async.with_timeout(5000, () => {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let dir = @fs.tmpdir(prefix="mooncdc-source-complete-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/checkpoint"
      let config : ConnectionConfig = {
        hostaddr: None,
        keepalive: TcpKeepaliveConfig::new(),
        host: "127.0.0.1",
        port: server.addr.port(),
        user: "user",
        password: "",
        database: "db",
        security: LocalPlaintext,
        timeout_ms: 500,
      }
      let source : Source = {
        system_id: "system",
        timeline: "1",
        database: "db",
        slot: "slot",
        publication: "pub",
      }
      save_checkpoint(path, source, Lsn::parse("0/10"))
      let before = @fs.read_file(path).binary()
      let valid = !scenario.has_prefix("bad-")
      let ids : Array[String] = []
      let delivered = @async.CondVar::Cond()
      let retry = ApplyRetry::new(ApplyControl(), 1, 2)
      @async.with_task_group(group => {
        let peer = group.spawn(() => {
          for attempt in 0..<(if valid { 2 } else { 1 }) {
            let (tcp, _) = server.accept()
            defer tcp.close()
            if attempt == 1 {
              mock_replication(tcp, "success")
            } else {
              mock_replication(tcp, "success", stream=tcp => {
                for
                  data in [
                    begin_fixture(),
                    relation_fixture("items"),
                    tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n"),
                    commit_fixture(),
                  ] {
                  let out = @buffer.Buffer()
                  out.write_byte(b'w')
                  out.write_uint64_be(100)
                  out.write_uint64_be(256)
                  out.write_uint64_be(0)
                  out.write_bytes(data)
                  tcp.write(packet(b'd', out.to_bytes()))
                }
                while ids.is_empty() {
                  delivered.wait()
                }
                if valid {
                  tcp.write(
                    packet(b'N', b"SNOTICE\x00C00000\x00Mowned notice\x00\x00"),
                  )
                  tcp.write(packet(b'S', b"application_name\x00owned peer\x00"))
                }
                let (tag, body) = match scenario {
                  "command" => (b'C', b"COPY 0\x00")
                  "done" => (b'c', b"")
                  "bad-command" => (b'C', b"SELECT 1\x00")
                  "bad-done" => (b'c', b"extra")
                  "bad-notice" => (b'N', b"Munterminated")
                  "bad-status" => (b'S', b"application_name\x00")
                  _ => fail("unknown completion scenario")
                }
                tcp.write(packet(tag, body))
                assert_eq(tcp.read_some(), None)
              })
            }
          }
        })
        try
          retry.run(() => {
            subscribe_impl(
              config,
              slot="slot",
              publication="pub",
              start=Resume,
              checkpoint_path=path,
              worker_retry=true,
              sub => {
                let tx = sub.next()
                assert_eq(@fs.read_file(path).binary(), before)
                ids.push(tx.id)
                if ids.length() == 1 {
                  delivered.broadcast()
                  ignore(sub.next())
                  fail("ended stream returned a transaction")
                }
                sub.ack(tx)
              },
            )
          })
        catch {
          Protocol(_) => assert_false(valid)
          error => raise error
        } noraise {
          _ => assert_true(valid)
        }
        peer.wait()
      })
      assert_eq(retry.retries, if valid { 1 } else { 0 })
      assert_eq(ids.length(), if valid { 2 } else { 1 })
      if valid {
        assert_eq(ids[0], ids[1])
        assert_eq(load_checkpoint(path, source), Lsn::parse("0/65"))
      } else {
        assert_eq(@fs.read_file(path).binary(), before)
      }
    })
  }
}

///|
async test "only opted pipeline timeline changes leave strict subscription for outer proof" {
  for scenario in ["timeline", "backward", "strict", "system", "database"] {
    @async.with_timeout(5000, () => {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let dir = @fs.tmpdir(prefix="mooncdc-source-transition-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/checkpoint"
      let config : ConnectionConfig = {
        hostaddr: None,
        keepalive: TcpKeepaliveConfig::new(),
        host: "127.0.0.1",
        port: server.addr.port(),
        user: "user",
        password: "",
        database: "db",
        security: LocalPlaintext,
        timeout_ms: 500,
      }
      let expected : Source = {
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
        ..expected,
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
      save_checkpoint(path, expected, Lsn::parse("0/10"))
      let before = @fs.read_file(path).binary()
      let retry = ApplyRetry::new(ApplyControl(), 1, 5)
      let mut delivered = false
      @async.with_task_group(group => {
        let peer = group.spawn(() => {
          let (tcp, _) = server.accept()
          defer tcp.close()
          // This peer asserts EOF immediately after IDENTIFY_SYSTEM, before any
          // slot/publication/start/feedback query can be sent.
          mock_replication(tcp, "transition", identified=actual)
        })
        try
          retry.run(() => {
            subscribe_impl(
              config,
              slot="slot",
              publication="pub",
              start=Resume,
              checkpoint_path=path,
              worker_retry=true,
              pipeline_source?=if scenario == "strict" {
                None
              } else {
                Some(expected)
              },
              _ => delivered = true,
            )
          })
        catch {
          PipelineSourceChanged =>
            assert_true(scenario == "timeline" || scenario == "backward")
          CheckpointError("source/slot/publication mismatch") =>
            assert_true(scenario == "strict" || scenario == "system")
          UnsafeResume("database mismatch") => assert_eq(scenario, "database")
          error => raise error
        } noraise {
          _ => fail("unproved source transition accepted")
        }
        peer.wait()
      })
      assert_false(delivered)
      assert_eq(retry.retries, 0)
      assert_eq(@fs.read_file(path).binary(), before)
      let lock = @fs.open(path + ".lock", mode=ReadWrite)
      defer lock.close()
      assert_true(lock.try_lock(Exclusive))
    })
  }
}

///|
async test "apply initialization and streaming share one retry budget" {
  let retry = ApplyRetry::new(ApplyControl(), 1, 2)
  let mut initialization = 0
  let result = retry.run(() => {
    @async.sleep(1)
    initialization += 1
    if initialization == 1 {
      raise ApplySourceFailed(Server("57P03", "starting"))
    }
    42
  })
  assert_eq(result, Some(42))
  assert_eq(retry.retries, 1)
  let mut writes = 0
  try
    retry.run(async fn() -> Unit {
      @async.sleep(1)
      writes += 1
      raise ApplyWriteFailed(Retryable("write"))
    })
  catch {
    Retryable(message) => assert_eq(message, "write")
    error => raise error
  } noraise {
    _ => fail("shared budget was reset")
  }
  assert_eq(writes, 2)
  assert_eq(retry.retries, 2)
}

///|
async test "pipeline initialization retries source startup without starting destination" {
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let dir = @fs.tmpdir(prefix="mooncdc-source-init-")
    defer @fs.rmdir(dir, recursive=true)
    let config : ConnectionConfig = {
      hostaddr: None,
      keepalive: TcpKeepaliveConfig::new(),
      host: "127.0.0.1",
      port: server.addr.port(),
      user: "user",
      password: "",
      database: "db",
      security: LocalPlaintext,
      timeout_ms: 500,
    }
    let target = ProbeDestination::new("source", ApplyControl())
    let mut connections = 0
    @async.with_task_group(group => {
      let peer = group.spawn(() => {
        for _ in 0..<3 {
          let (tcp, _) = server.accept()
          defer tcp.close()
          connections += 1
          mock_replication(tcp, "startup")
        }
      })
      try
        run_pipeline(
          config,
          target,
          publication="pub",
          destination_id="target",
          state_path=dir + "/state",
          control=ApplyControl(),
          table_retry_delay_ms=1,
          max_table_retries=2,
        )
      catch {
        @io.PipeClosed | @io.ReaderClosed => ()
        error => raise error
      } noraise {
        _ => fail("startup failure accepted")
      }
      peer.wait()
    })
    assert_eq(connections, 3)
    assert_true(target.events.is_empty())
    assert_false(@fs.exists(dir + "/state"))
    let lock = @fs.open(dir + "/state.lock", mode=ReadWrite)
    defer lock.close()
    assert_true(lock.try_lock(Exclusive))
  })
}

///|
async test "worker subscription retries source stages and replays unacked transactions" {
  for
    scenario in [
      "replay", "exhausted", "changed", "unsafe", "callback", "message", "checkpoint",
    ] {
    @async.with_timeout(10000, () => {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let dir = @fs.tmpdir(prefix="mooncdc-source-retry-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/checkpoint"
      let config : ConnectionConfig = {
        hostaddr: None,
        keepalive: TcpKeepaliveConfig::new(),
        host: "127.0.0.1",
        port: server.addr.port(),
        user: "user",
        password: "",
        database: "db",
        security: LocalPlaintext,
        timeout_ms: 200,
      }
      let source : Source = {
        system_id: "system",
        timeline: "1",
        database: "db",
        slot: "slot",
        publication: "pub",
      }
      let start = Lsn::parse("0/10")
      save_checkpoint(path, source, start)
      let plan = match scenario {
        "replay" =>
          ["startup", "identify", "start", "timeout", "pending", "success"]
        "exhausted" => ["error1", "error2", "error3"]
        "changed" => ["pending", "changed"]
        "unsafe" => ["pending", "success"]
        _ => ["success"]
      }
      let retry = ApplyRetry::new(
        ApplyControl(),
        1,
        if scenario == "exhausted" {
          2
        } else {
          5
        },
      )
      let ids : Array[String] = []
      let delivered = @async.CondVar::Cond()
      let mut connections = 0
      @async.with_task_group(group => {
        let peer = group.spawn(() => {
          for mode in plan {
            let (tcp, _) = server.accept()
            defer tcp.close()
            connections += 1
            if scenario == "unsafe" && mode != "pending" {
              mock_replication(
                tcp,
                "success",
                rejected_slot=[
                  Some("pgoutput"),
                  Some("db"),
                  Some("0/20"),
                  Some("0/20"),
                  Some("reserved"),
                  None,
                  Some("false"),
                ],
                messages=true,
              )
            } else if mode == "pending" || scenario == "message" {
              mock_replication(tcp, "success", messages=true, stream=tcp => {
                let frames = if scenario == "message" {
                  [logical_fixture(false, b"test")]
                } else {
                  [
                    begin_fixture(),
                    relation_fixture("items"),
                    tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n"),
                    commit_fixture(),
                  ]
                }
                for data in frames {
                  let out = @buffer.Buffer()
                  out.write_byte(b'w')
                  out.write_uint64_be(100)
                  out.write_uint64_be(256)
                  out.write_uint64_be(0)
                  out.write_bytes(data)
                  tcp.write(packet(b'd', out.to_bytes()))
                }
                if scenario == "message" {
                  assert_eq(tcp.read_some(), None)
                } else {
                  while ids.is_empty() {
                    delivered.wait()
                  }
                }
              })
            } else {
              mock_replication(tcp, mode, messages=true)
            }
          }
        })
        try
          retry.run(() => {
            subscribe_impl(
              config,
              slot="slot",
              publication="pub",
              start=Resume,
              checkpoint_path=path,
              worker_retry=true,
              max_reconnects=100,
              heartbeat_ms=10000,
              messages=true,
              nontransactional_message=_ => {
                @async.sleep(1)
                raise Retryable("message callback")
              },
              sub => {
                let tx = sub.next()
                assert_eq(sub.diagnostics().reconnects, 0)
                assert_eq(load_checkpoint(path, source), start)
                ids.push(tx.id)
                if scenario == "callback" {
                  raise Retryable("consumer callback")
                }
                if scenario == "checkpoint" {
                  @fs.mkdir(path + ".tmp")
                }
                if (
                    scenario == "replay" ||
                    scenario == "changed" ||
                    scenario == "unsafe"
                  ) &&
                  ids.length() == 1 {
                  delivered.broadcast()
                  ignore(sub.next())
                  fail("failed stream returned another transaction")
                }
                sub.ack(tx)
              },
            )
          })
        catch {
          Server("57P03", message) => {
            assert_eq(scenario, "exhausted")
            assert_eq(message, "error3")
          }
          UnsafeResume(_) => assert_eq(scenario, "unsafe")
          CheckpointError(_) =>
            assert_true(scenario == "changed" || scenario == "checkpoint")
          Retryable(message) => {
            assert_true(scenario == "callback" || scenario == "message")
            assert_eq(
              message,
              if scenario == "message" {
                "message callback"
              } else {
                "consumer callback"
              },
            )
          }
          @os_error.OSError(_, context~) => {
            assert_eq(scenario, "checkpoint")
            assert_true(context.contains("checkpoint.tmp"))
          }
          error => raise error
        } noraise {
          _ => assert_eq(scenario, "replay")
        }
        peer.wait()
      })
      assert_eq(connections, plan.length())
      assert_eq(
        retry.retries,
        if scenario == "replay" {
          5
        } else if scenario == "exhausted" {
          2
        } else if scenario == "changed" || scenario == "unsafe" {
          1
        } else {
          0
        },
      )
      if scenario == "replay" {
        assert_eq(ids.length(), 2)
        assert_eq(ids[0], ids[1])
      }
      assert_eq(
        load_checkpoint(path, source),
        if scenario == "replay" {
          Lsn::parse("0/65")
        } else {
          start
        },
      )
      let lock = @fs.open(path + ".lock", mode=ReadWrite)
      defer lock.close()
      assert_true(lock.try_lock(Exclusive))
    })
  }
}
