///|
async fn live_wait_stream(sub : Subscription, minimum_events : Int) -> Unit {
  @async.with_timeout(15000, () => {
    while sub.stream_decoder
          .map(decoder => decoder.events < minimum_events)
          .unwrap_or(true) {
      @async.sleep(20)
    }
  })
  assert_true(sub.queue.is_empty())
  assert_eq(sub.diagnostics().received, sub.diagnostics().durable)
}

///|
async test "live v2 streaming commit nested rollback abort replay reconnect and limits" {
  if @env.get_env_var("MOONCDC_STREAMING_PORT") is Some(port) {
    let config : ConnectionConfig = {
      hostaddr: None,
      keepalive: TcpKeepaliveConfig::new(),
      host: "127.0.0.1",
      port: @string.parse_int(port),
      user: "cdc",
      password: "mooncdc-test-only",
      database: "postgres",
      security: LocalPlaintext,
      timeout_ms: 5000,
    }
    @async.with_timeout(90000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      let dir = @fs.tmpdir(prefix="mooncdc-stream-live-")
      defer @fs.rmdir(dir, recursive=true)
      let observations : Array[Json] = []
      let record = async fn(value : Json) -> Unit {
        observations.push(value)
        if @env.get_env_var("MOONCDC_STREAMING_EVIDENCE") is Some(path) {
          @fs.write_file(path, observations.to_json().stringify(indent=2))
        }
      }
      ignore(
        sql.query(
          "CREATE TABLE cdc_stream(id int PRIMARY KEY, value bytea, enabled boolean); ALTER TABLE cdc_stream REPLICA IDENTITY FULL; CREATE PUBLICATION cdc_stream FOR TABLE cdc_stream",
        ),
      )
      assert_eq(cell(sql.query("SHOW logical_decoding_work_mem"), 0, 0), "64kB")
      for binary in [false, true] {
        let start = fresh_slot(sql, "cdc_stream")
        let path = dir + (if binary { "/binary" } else { "/text" })
        let outside : Array[LogicalMessage] = []
        let seen : Array[String?] = []
        let first_end = subscribe(
          config,
          slot="cdc_stream",
          publication="cdc_stream",
          start=At(start),
          checkpoint_path=path,
          protocol=V2Streaming,
          binary~,
          messages=true,
          nontransactional_message=message => outside.push(message),
          sub => {
            ignore(
              sql.query(
                "BEGIN; INSERT INTO cdc_stream SELECT g,decode(repeat('00ff80',200),'hex'),true FROM generate_series(9000,9199)g; SELECT pg_log_standby_snapshot()",
              ),
            )
            live_wait_stream(sub, 50)
            record({
              "phase": "uncommitted-root",
              "binary": binary.to_json(),
              "buffered_events": sub.stream_decoder.unwrap().events.to_json(),
              "streams": sub.stream_decoder.unwrap().streams.length().to_json(),
              "checkpoint": start.to_string().to_json(),
              "queued": sub.queue.length().to_json(),
            })
            assert_eq(load_checkpoint(path, sub.source), start)
            ignore(sql.query("ROLLBACK"))
            @async.with_timeout(15000, () => {
              while sub.stream_decoder
                    .map(d => !d.streams.is_empty())
                    .unwrap_or(true) {
                @async.sleep(20)
              }
            })
            assert_true(sub.queue.is_empty())
            assert_eq(load_checkpoint(path, sub.source), start)
            record({
              "phase": "root-abort",
              "binary": binary.to_json(),
              "streams": sub.stream_decoder.unwrap().streams.length().to_json(),
              "checkpoint": start.to_string().to_json(),
              "queued": sub.queue.length().to_json(),
            })
            ignore(
              sql.query(
                "BEGIN; INSERT INTO cdc_stream VALUES(1,decode('00ff80','hex'),true); SAVEPOINT s1; INSERT INTO cdc_stream SELECT g,decode(repeat('00ff80',200),'hex'),false FROM generate_series(10000,10199)g; SELECT pg_log_standby_snapshot()",
              ),
            )
            live_wait_stream(sub, 50)
            ignore(
              sql.query(
                "SAVEPOINT s2; INSERT INTO cdc_stream SELECT g,decode(repeat('00ff80',200),'hex'),false FROM generate_series(20000,20199)g; SELECT pg_log_standby_snapshot()",
              ),
            )
            live_wait_stream(sub, 250)
            record({
              "phase": "nested-before-rollback",
              "binary": binary.to_json(),
              "buffered_events": sub.stream_decoder.unwrap().events.to_json(),
              "checkpoint": start.to_string().to_json(),
              "queued": sub.queue.length().to_json(),
            })
            ignore(
              sql.query(
                "RELEASE SAVEPOINT s2; ROLLBACK TO SAVEPOINT s1; INSERT INTO cdc_stream SELECT g,decode(repeat('00ff80',200),'hex'),true FROM generate_series(2,401)g; SELECT pg_logical_emit_message(true,'committed',decode('00ff80','hex')); SELECT pg_logical_emit_message(false,'outside','seen'); COMMIT",
              ),
            )
            let tx = sub.next()
            let rows = tx.changes.filter(change => change is Insert(_, _))
            assert_eq(rows.length(), 401)
            for index, row in rows {
              guard row is Insert(_, [id, value, enabled]) else {
                fail("three columns")
              }
              if binary {
                let id_bytes = @buffer.Buffer()
                id_bytes.write_int_be(index + 1)
                assert_eq(id, Binary(id_bytes.to_bytes()))
                assert_eq(enabled, Binary(b"\x01"))
                assert_true(value is Binary(_))
              } else {
                assert_eq(id, Text((index + 1).to_string()))
                assert_eq(enabled, Text("t"))
                assert_true(value is Text(_))
              }
              if index == 0 {
                assert_eq(
                  value,
                  if binary {
                    Binary(b"\x00\xff\x80")
                  } else {
                    Text("\\x00ff80")
                  },
                )
              } else {
                assert_eq(
                  value,
                  if binary {
                    Binary(cell_hex("00ff80".repeat(200)[:]))
                  } else {
                    Text("\\x" + "00ff80".repeat(200))
                  },
                )
              }
            }
            assert_true(
              tx.changes.any(change => {
                change
                is Message(
                  { prefix: "committed", content: b"\x00\xff\x80", .. }
                )
              }),
            )
            for index in 0..<tx.changes.length() {
              seen.push(tx.event_id(index))
            }
            assert_eq(outside.length(), 1)
            assert_eq(outside[0].prefix, "outside")
            assert_eq(sub.diagnostics().durable, start)
            record({
              "phase": "committed-unacknowledged",
              "binary": binary.to_json(),
              "id": tx.id.to_json(),
              "end_lsn": tx.end_lsn.to_string().to_json(),
              "rows": rows.length().to_json(),
              "stable_ids": seen.to_json(),
              "checkpoint": start.to_string().to_json(),
            })
            tx.end_lsn
          },
        )
        // No ack above: a fresh connection replays the exact stable DML/message IDs.
        subscribe(
          config,
          slot="cdc_stream",
          publication="cdc_stream",
          start=Resume,
          checkpoint_path=path,
          protocol=V2Streaming,
          binary~,
          messages=true,
          nontransactional_message=_ => (),
          sub => {
            let replay = sub.next()
            assert_eq(replay.end_lsn, first_end)
            assert_eq(
              replay.changes.filter(c => c is Insert(_, _)).length(),
              401,
            )
            // Relation retransmission may differ. Compare only stable event IDs.
            assert_eq(
              [
                for i in 0..<replay.changes.length() => replay.event_id(i)
              ].filter_map(id => id),
              seen.filter_map(id => id),
            )
            sub.ack(replay)
            record({
              "phase": "fresh-replay-ack",
              "binary": binary.to_json(),
              "id": replay.id.to_json(),
              "checkpoint": replay.end_lsn.to_string().to_json(),
            })
            assert_eq(sub.diagnostics().durable, first_end)
            ignore(
              sql.query(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name='cdc_stream'",
              ),
            )
            @async.with_timeout(15000, () => {
              while sub.diagnostics().reconnects == 0 {
                @async.sleep(20)
              }
              // Wait for this slot's new walsender before the next transaction.
              while cell(
                      sql.query(
                        "SELECT active::text FROM pg_replication_slots WHERE slot_name='cdc_stream'",
                      ),
                      0,
                      0,
                    ) !=
                    "true" {
                @async.sleep(20)
              }
            })
            ignore(
              sql.query(
                "BEGIN; INSERT INTO cdc_stream SELECT g,decode(repeat('00ff80',200),'hex'),true FROM generate_series(500,699)g; SELECT pg_log_standby_snapshot()",
              ),
            )
            live_wait_stream(sub, 50)
            assert_eq(load_checkpoint(path, sub.source), first_end)
            ignore(sql.query("COMMIT"))
            let next = sub.next()
            assert_eq(next.changes.filter(c => c is Insert(_, _)).length(), 200)
            sub.ack(next)
            record({
              "phase": "live-reconnect-ack",
              "binary": binary.to_json(),
              "id": next.id.to_json(),
              "checkpoint": next.end_lsn.to_string().to_json(),
              "reconnects": sub.diagnostics().reconnects.to_json(),
            })
            assert_eq(sub.diagnostics().durable, next.end_lsn)
          },
        )
        drop_slot(sql, "cdc_stream")
        ignore(sql.query("TRUNCATE cdc_stream"))
      }
      let start = fresh_slot(sql, "cdc_stream")
      let path = dir + "/limited"
      try
        subscribe(
          config,
          slot="cdc_stream",
          publication="cdc_stream",
          start=At(start),
          checkpoint_path=path,
          protocol=V2Streaming,
          max_transaction_events=10,
          async fn(sub : Subscription) -> Unit {
            ignore(
              sql.query(
                "BEGIN; INSERT INTO cdc_stream SELECT g,decode(repeat('00ff80',200),'hex'),true FROM generate_series(1,200)g; SELECT pg_log_standby_snapshot()",
              ),
            )
            ignore(sub.next())
            fail("limited transaction delivered")
          },
        )
      catch {
        LimitExceeded(_) => ()
        e => raise e
      } noraise {
        _ => fail("missing event limit")
      }
      ignore(sql.query("COMMIT"))
      let conn = connect(config, 1048576, true)
      defer conn.close()
      let source = identify(conn, config, "cdc_stream", "cdc_stream")
      assert_eq(load_checkpoint(path, source), start)
      record({
        "phase": "limit-rejected",
        "checkpoint": start.to_string().to_json(),
      })
      subscribe(
        config,
        slot="cdc_stream",
        publication="cdc_stream",
        start=Resume,
        checkpoint_path=path,
        protocol=V2Streaming,
        sub => {
          let tx = sub.next()
          assert_eq(tx.changes.filter(c => c is Insert(_, _)).length(), 200)
          sub.ack(tx)
          record({
            "phase": "limit-replay-ack",
            "id": tx.id.to_json(),
            "checkpoint": tx.end_lsn.to_string().to_json(),
            "rows": tx.changes.filter(c => c is Insert(_, _)).length().to_json(),
          })
        },
      )
      drop_slot(sql, "cdc_stream")
      ignore(sql.query("DROP PUBLICATION cdc_stream; DROP TABLE cdc_stream"))
      println(
        "STREAMING LIVE: text/binary provisional buffers, root/nested/released abort, stable replay IDs, walsender reconnect and limit/replay passed",
      )
    })
  }
}
