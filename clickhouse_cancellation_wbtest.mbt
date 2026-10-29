///|
async test "ClickHouse admitted write survives caller cancellation before replay and shutdown" {
  ch_test_admitted_write_cancellation(false)
}

///|
async test "ClickHouse admitted snapshot write survives caller cancellation before reset and shutdown" {
  ch_test_admitted_write_cancellation(true)
}

///|
async fn ch_test_admitted_write_cancellation(copy : Bool) -> Unit {
  for engine in [ClickHouseEngine::ReplacingMergeTree, MergeTree] {
    for
      action in (if copy {
        ["reset", "shutdown", "deadline"]
      } else {
        ["replay", "shutdown", "deadline"]
      }) {
      @async.with_timeout(5000, () => {
        let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
        defer server.close()
        let dir = @fs.tmpdir(prefix="mooncdc-ch-cancel-")
        defer @fs.rmdir(dir, recursive=true)
        @fs.mkdir(dir + "/state")
        with_file_schema_store(dir + "/schemas", identity="cancellation", schemas => {
          with_state_store(FileStateStore::new(dir + "/state"), session => {
            let target = ClickHouseDestination::new(
              endpoint="http://127.0.0.1:\{server.addr.port()}",
              user="test",
              password="",
              database="test",
              publication="pub",
              schema_store=schemas,
              engine~,
              timeout_ms=if action == "deadline" { 100 } else { 1000 },
            )
            target.bind_state(session)
            target.running = true
            let schema = ch_test_schema(ch_test_columns(), 100)
            let relation = schema.schema.relation(["id", "value"])
            target.copies[42U] = schema
            target.attempts[42U] = "first"
            let copy_id : TableCopyBatchId = {
              attempt: "first",
              relation_id: relation.oid,
              sequence: 1,
            }
            target.tables[42U] = {
              schema,
              name: "test",
              layout: ChLayout::new(
                ReadOnlyArray::from_array(
                  [("id", "Int32"), ("value", "String")] +
                  ch_metadata_columns(engine),
                ),
              ),
            }
            let tx = {
              ..decoding_tx(200, [Insert(relation, [Text("1"), Text("old")])]),
              decoded_schemas: Some(Map([(0, [schema])])),
            }
            let replay = {
              ..decoding_tx(210, [
                Truncate([relation], cascade=false, restart_identity=false),
              ]),
              decoded_schemas: Some(Map([(0, [schema])])),
            }
            let requests : Array[String] = []
            let changed = @async.CondVar::Cond()
            let mut release = false
            let mut old_finished = false
            let mut replay_finished = false
            let mut deadline = false
            @async.with_task_group(group => {
              group.spawn_bg(() => {
                server.run_forever(
                  (tcp, _) => {
                    let conn = @http.ServerConnection::new(tcp)
                    defer conn.close()
                    ignore(conn.read_request())
                    let body = @utf8.decode_lossy(conn.read_all().binary())
                    if body.has_prefix("INSERT") {
                      requests.push("insert")
                      changed.broadcast()
                      while !release {
                        changed.wait()
                      }
                      requests.push("insert-finished")
                    } else if action == "reset" {
                      // Fail at the first reset request: this test observes the
                      // ordering fence without simulating table creation.
                      assert_true(body.has_prefix("DROP "))
                      requests.push("reset")
                      conn.send_response(500, "Injected reset failure")
                      conn.end_response()
                      return
                    } else {
                      assert_true(body.has_prefix("TRUNCATE"))
                      requests.push("truncate")
                    }
                    conn.send_response(200, "OK")
                    conn.end_response()
                  },
                  allow_failure=true,
                )
              })
              let old = group.spawn(() => {
                let result = @async.handle_cancellation(() => {
                  if copy {
                    target.write_rows(
                      relation,
                      [[Text("1"), Text("old")]],
                      Some(copy_id),
                    )
                  } else {
                    target.write_transaction(tx)
                  }
                }) catch {
                  @async.TimeoutError => {
                    deadline = true
                    None
                  }
                  error => raise error
                }
                old_finished = true
                result
              })
              while requests.is_empty() {
                changed.wait()
              }
              defer {
                release = true
                changed.broadcast()
              }
              if action == "deadline" {
                assert_eq(old.wait(), None)
                assert_true(deadline)
                assert_true(old_finished)
                target.flush()
                // The local deadline released the lock, but the server handler
                // remains admitted. Do not claim remote cancellation.
                assert_eq(requests, ["insert"])
                release = true
                changed.broadcast()
                while requests.length() < 2 {
                  @async.sleep(1)
                }
                target.shutdown()
                group.return_immediately(())
              }
              old.cancel()
              @async.sleep(20)
              assert_false(old_finished)
              let discarded = group.spawn(() => {
                @async.handle_cancellation(() => {
                  if copy {
                    target.write_rows(
                      relation,
                      [[Text("2"), Text("discarded")]],
                      Some({ ..copy_id, sequence: 2, }),
                    )
                  } else {
                    target.write_transaction(replay)
                  }
                })
              })
              @async.sleep(5)
              discarded.cancel()
              assert_eq(discarded.wait(), None)
              assert_eq(requests, ["insert"])
              let next = group.spawn(() => {
                if action == "replay" {
                  assert_eq(target.write_transaction(replay), Durable)
                } else if action == "reset" {
                  assert_true(
                    try {
                      target.reset_table(relation, attempt="second")
                      false
                    } catch {
                      Manual("ClickHouse HTTP 500, exception unknown") => true
                      _ => false
                    },
                  )
                } else {
                  target.shutdown()
                }
                replay_finished = true
              })
              @async.sleep(20)
              assert_false(replay_finished)
              assert_eq(requests, ["insert"])
              release = true
              changed.broadcast()
              assert_eq(old.wait(), Some(Durable))
              next.wait()
              assert_eq(
                requests,
                if action == "replay" {
                  ["insert", "insert-finished", "truncate"]
                } else if action == "reset" {
                  ["insert", "insert-finished", "reset"]
                } else {
                  ["insert", "insert-finished"]
                },
              )
              target.shutdown()
              group.return_immediately(())
            })
          })
        })
      })
    }
  }
}

///|
async test "ClickHouse live cancelled INSERT drains before replay reset and shutdown" {
  if @env.get_env_var("MOONCDC_CLICKHOUSE_LIVE") != Some("1") {
    return
  }
  let remote = ChHttp::new(
    "http://127.0.0.1:18123", "cdc", "mooncdc-test-only", 10000,
  )
  for engine in [ClickHouseEngine::ReplacingMergeTree, MergeTree] {
    for action in ["replay", "reset", "shutdown"] {
      @async.with_timeout(15000, () => {
        let database = "mooncdc_cancel_" + pipeline_nonce()
        ignore(remote.query("CREATE DATABASE " + ch_quote(database, '"')))
        defer ignore(remote.query("DROP DATABASE " + ch_quote(database, '"')))
        let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
        defer server.close()
        let dir = @fs.tmpdir(prefix="mooncdc-ch-cancel-live-")
        defer @fs.rmdir(dir, recursive=true)
        @fs.mkdir(dir + "/state")
        with_file_schema_store(dir + "/schemas", identity="cancellation", schemas => {
          with_state_store(FileStateStore::new(dir + "/state"), session => {
            let target = ClickHouseDestination::new(
              endpoint="http://127.0.0.1:\{server.addr.port()}",
              user="cdc",
              password="mooncdc-test-only",
              database~,
              publication="pub",
              schema_store=schemas,
              engine~,
              timeout_ms=10000,
            )
            let changed = @async.CondVar::Cond()
            let mut admitted = false
            let mut release = false
            let mut completed = false
            let mut old_finished = false
            let mut next_finished = false
            @async.with_task_group(group => {
              group.spawn_bg(() => {
                server.run_forever(
                  (tcp, _) => {
                    let conn = @http.ServerConnection::new(tcp)
                    defer conn.close()
                    ignore(conn.read_request())
                    let request = conn.read_all().binary()
                    let mut split = 0
                    while split < request.length() && request[split] != b'\n' {
                      split += 1
                    }
                    assert_true(split < request.length())
                    let sql = @utf8.decode(request[:split])
                    let insert = sql.has_prefix("INSERT")
                    if insert {
                      admitted = true
                      changed.broadcast()
                      while !release {
                        changed.wait()
                      }
                    } else if admitted && !completed {
                      fail("new request overtook admitted INSERT")
                    }
                    let output = remote.query(
                      sql,
                      body=request[split + 1:].to_owned(),
                      insert~,
                    )
                    if insert {
                      completed = true
                    }
                    conn.send_response(200, "OK")
                    conn.write(output)
                    conn.end_response()
                  },
                  allow_failure=true,
                )
              })
              target.bind_state(session)
              target.startup()
              let schema = ch_test_schema(ch_test_columns(), 100)
              schemas.put(schema.schema)
              let relation = schema.schema.relation(["id", "value"])
              target.bind_snapshot_schema(relation, schema.schema)
              target.reset_table(relation, attempt="first")
              let table_name = target.qualified(target.tables[42U].name)
              let tx = {
                ..decoding_tx(200, [Insert(relation, [Text("1"), Text("old")])]),
                decoded_schemas: Some(Map([(0, [schema])])),
              }
              let old = group.spawn(() => {
                let result = @async.handle_cancellation(() => {
                  target.write_transaction(tx)
                })
                old_finished = true
                result
              })
              while !admitted {
                changed.wait()
              }
              defer {
                release = true
                changed.broadcast()
              }
              old.cancel()
              @async.sleep(20)
              assert_false(old_finished)
              let next = group.spawn(() => {
                match action {
                  "replay" => {
                    let tx = {
                      ..decoding_tx(210, [
                        Truncate(
                          [relation],
                          cascade=false,
                          restart_identity=false,
                        ),
                      ]),
                      decoded_schemas: Some(Map([(0, [schema])])),
                    }
                    assert_eq(target.write_transaction(tx), Durable)
                  }
                  "reset" => target.reset_table(relation, attempt="second")
                  _ => target.shutdown()
                }
                next_finished = true
              })
              @async.sleep(20)
              assert_false(next_finished)
              assert_false(completed)
              release = true
              changed.broadcast()
              assert_eq(old.wait(), Some(Durable))
              next.wait()
              assert_true(completed)
              assert_eq(
                remote
                .query("SELECT count() FROM " + table_name)
                .trim()
                .to_owned(),
                if action == "shutdown" {
                  "1"
                } else {
                  "0"
                },
              )
              target.shutdown()
              group.return_immediately(())
            })
          })
        })
        println(
          "CLICKHOUSE: cancelled INSERT, \{Repr(engine)}, \{action} ordering passed",
        )
      })
    }
  }
}
