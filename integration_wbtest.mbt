///|
fn live_config() -> ConnectionConfig? raise {
  @env.get_env_var("MOONCDC_TEST_PORT").map(port => {
    host: "127.0.0.1",
    port: @string.parse_int(port),
    user: "cdc",
    password: "mooncdc-test-only",
    database: "postgres",
    security: LocalPlaintext,
    timeout_ms: 5000,
  })
}

///|
async fn fresh_slot(sql : Connection, name : String) -> Lsn {
  drop_slot(sql, name)
  let rows = sql.query(
    "SELECT lsn::text FROM pg_create_logical_replication_slot('\{name}', 'pgoutput')",
  )
  Lsn::parse(cell(rows, 0, 0))
}

///|
async fn drop_slot(sql : Connection, name : String) -> Unit {
  for _ in 0..<100 {
    let state = sql.query(
      "SELECT active::text FROM pg_replication_slots WHERE slot_name='\{name}'",
    )
    if state.is_empty() {
      return
    }
    if cell(state, 0, 0) == "false" {
      ignore(sql.query("SELECT pg_drop_replication_slot('\{name}')"))
      return
    }
    @async.sleep(20)
  }
  fail("test slot remained active after client closure")
}

///|
async test "live transactions acknowledgement recovery limits heartbeat reconnect" {
  if live_config() is Some(config) {
    @async.with_timeout(90000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      let dir = @fs.tmpdir(prefix="mooncdc-live-")
      defer @fs.rmdir(dir, recursive=true)
      ignore(sql.query("TRUNCATE items, extras"))
      let start = fresh_slot(sql, "cdc_order")
      let path = dir + "/order.checkpoint"
      let seen : Array[String] = []
      let first_end = subscribe(
        config,
        slot="cdc_order",
        publication="mooncdc_test",
        start=At(start),
        checkpoint_path=path,
        sub => {
          ignore(
            sql.query(
              "BEGIN; INSERT INTO items VALUES (101,'one'); INSERT INTO extras VALUES (201,NULL); COMMIT; BEGIN; INSERT INTO items VALUES (999,'rollback'); ROLLBACK;",
            ),
          )
          ignore(sql.query("UPDATE items SET value='two' WHERE id=101"))
          ignore(sql.query("DELETE FROM extras WHERE id=201"))
          let a = sub.next()
          let b = sub.next()
          let c = sub.next()
          assert_true(a.end_lsn < b.end_lsn && b.end_lsn < c.end_lsn)
          let inserted = a.changes.filter(change => change is Insert(_, _))
          assert_eq(inserted.length(), 2)
          assert_true(inserted[1] is Insert(_, [Text("201"), Null]))
          assert_true(
            b.changes.any(change => {
              change
              is Update(
                _,
                Some(Full([Text("101"), Text("one")])),
                [Text("101"), Text("two")]
              )
            }),
          )
          assert_true(
            c.changes.any(change => {
              change is Delete(_, Full([Text("201"), Null]))
            }),
          )
          seen.push(a.id)
          seen.push(b.id)
          seen.push(c.id)
          sub.ack(c)
          assert_eq(sub.diagnostics().durable, start)
          sub.ack(a)
          assert_eq(sub.diagnostics().durable, a.end_lsn)
          // Leave b incomplete; even c must replay after a process restart.
          a.end_lsn
        },
      )
      subscribe(
        config,
        slot="cdc_order",
        publication="mooncdc_test",
        start=Resume,
        checkpoint_path=path,
        sub => {
          assert_eq(sub.diagnostics().durable, first_end)
          let b = sub.next()
          let c = sub.next()
          assert_eq(b.id, seen[1])
          assert_eq(c.id, seen[2])
          sub.ack(c)
          sub.ack(b)
          assert_eq(sub.diagnostics().durable, c.end_lsn)
          // No traffic for longer than wal_sender_timeout: feedback keeps the slot alive.
          @async.sleep(6500)
          assert_eq(sub.diagnostics().reconnects, 0)
          ignore(sql.query("INSERT INTO items VALUES (102,'after idle')"))
          let d = sub.next()
          assert_true(
            d.changes.any(change => {
              change is Insert(_, [Text("102"), Text("after idle")])
            }),
          )
          // A slow consumer holding an unacknowledged transaction must still get heartbeats.
          @async.sleep(6500)
          assert_eq(sub.diagnostics().reconnects, 0)
          assert_eq(sub.diagnostics().outstanding_transactions, 1)
          sub.ack(d)
          @async.sleep(1200)
          let confirmed = sql.query(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='cdc_order'",
          )
          assert_eq(Lsn::parse(cell(confirmed, 0, 0)), d.end_lsn)
        },
      )
      println(
        "LIVE: multi-table, rollback exclusion, ordering, contiguous ack, restart replay, idle heartbeat passed",
      )
      let start = fresh_slot(sql, "cdc_reconnect")
      subscribe(
        config,
        slot="cdc_reconnect",
        publication="mooncdc_test",
        start=At(start),
        checkpoint_path=dir + "/reconnect",
        sub => {
          ignore(
            sql.query("INSERT INTO items VALUES (103,'before disconnect')"),
          )
          let a = sub.next()
          // Terminate this test's walsender only; preserve the unacknowledged delivery.
          ignore(
            sql.query(
              "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name='cdc_reconnect'",
            ),
          )
          @async.sleep(800)
          sub.ack(a)
          ignore(sql.query("INSERT INTO items VALUES (104,'after reconnect')"))
          let b = sub.next()
          assert_true(
            b.changes.any(change => {
              change is Insert(_, [Text("104"), Text("after reconnect")])
            }),
          )
          assert_true(sub.diagnostics().reconnects >= 1)
          sub.ack(b)
        },
      )
      println("LIVE: reconnect preserved pending delivery passed")
      let start = fresh_slot(sql, "cdc_limit")
      try
        subscribe(
          config,
          slot="cdc_limit",
          publication="mooncdc_test",
          start=At(start),
          checkpoint_path=dir + "/limit",
          max_frame_bytes=1024,
          max_transaction_bytes=4096,
          sub => {
            ignore(sql.query("INSERT INTO items VALUES (105,repeat('x',2000))"))
            ignore(sub.next())
          },
        )
      catch {
        LimitExceeded(_) => ()
        e => raise e
      } noraise {
        _ => fail("expected frame limit")
      }
      let control = connect(config, 1048576, true)
      defer control.close()
      let source = identify(control, config, "cdc_limit", "mooncdc_test")
      assert_eq(load_checkpoint(dir + "/limit", source), start)
      @async.with_timeout(2000, () => {
        while cell(
                sql.query(
                  "SELECT active::text FROM pg_replication_slots WHERE slot_name='cdc_limit'",
                ),
                0,
                0,
              ) !=
              "false" {
          @async.sleep(20)
        }
      })
      let start = fresh_slot(sql, "cdc_slow")
      try
        subscribe(
          config,
          slot="cdc_slow",
          publication="mooncdc_test",
          start=At(start),
          checkpoint_path=dir + "/slow",
          max_pending_transactions=1,
          _ => {
            ignore(sql.query("INSERT INTO items VALUES (106,'slow1')"))
            ignore(sql.query("INSERT INTO items VALUES (107,'slow2')"))
            @async.sleep(5000)
          },
        )
      catch {
        LimitExceeded(_) => ()
        e => raise e
      } noraise {
        _ => fail("expected queue limit")
      }
      let source = identify(control, config, "cdc_slow", "mooncdc_test")
      assert_eq(load_checkpoint(dir + "/slow", source), start)
      println("LIVE: frame and slow-consumer limits preserve checkpoint passed")
      for name in ["cdc_order", "cdc_reconnect", "cdc_limit", "cdc_slow"] {
        drop_slot(sql, name)
      }
    })
  }
}
