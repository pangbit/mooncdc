///|
test "copy slot monitor distinguishes loss compatibility and standby LSN absence" {
  let point : SnapshotPosition = {
    lsn: Lsn::parse("0/10"),
    source: pipeline_fixture().source,
  }
  let row = [
    Some("pgoutput"),
    Some(point.source.database),
    Some("reserved"),
    None,
    Some("0/30"),
  ]
  let changed = fn(index : Int, value : String?) -> Array[String?] {
    let updated = row.copy()
    updated[index] = value
    updated
  }
  assert_eq(copy_slot_lag([row], point), Some(32UL))
  assert_eq(copy_slot_lag([changed(4, Some("0/1"))], point), Some(0UL))
  assert_eq(copy_slot_lag([changed(4, None)], point), None)
  for
    rows in [[], [changed(2, Some("lost"))], [changed(3, Some("wal_removed"))]] {
    try copy_slot_lag(rows, point) catch {
      Retryable(_) => ()
      error => raise error
    } noraise {
      _ => fail("lost slot accepted")
    }
  }
  try copy_slot_lag([changed(0, Some("other"))], point) catch {
    UnsafeResume(_) => ()
    error => raise error
  } noraise {
    _ => fail("incompatible slot accepted")
  }
}

///|
async test "copy monitor owner cancellation joins an unfinished source handshake" {
  @async.with_timeout(2000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let config = ConnectionConfig::new(
      host="127.0.0.1",
      user="unused",
      port=server.addr.port(),
      security=LocalPlaintext,
      timeout_ms=60000,
    )
    let point : SnapshotPosition = {
      lsn: Lsn::parse("0/10"),
      source: pipeline_fixture().source,
    }
    let metrics = RuntimeMetrics::new()
    @async.with_task_group(group => {
      group.spawn_bg(() => {
        with_copy_monitor(config, "owned", 60000, metrics, notify => {
          notify(point)
          @async.sleep(60000)
        })
      })
      let (tcp, _) = server.accept()
      defer tcp.close()
      group.return_immediately(())
    })
    assert_true(metrics.copy_lags.is_empty())
  })
}

///|
async test "live copy monitor samples lag rejects missing slot and cancels COPY after real WAL loss" {
  if live_config() is Some(config) {
    @async.with_timeout(30000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(sql.query("ALTER SYSTEM SET max_slot_wal_keep_size='0'"))
      ignore(sql.query("SELECT pg_reload_conf()"))
      ignore(
        sql.query(
          "CREATE TABLE public.monitor_copy(id int PRIMARY KEY,value text); INSERT INTO public.monitor_copy SELECT x,repeat('a',100) FROM generate_series(1,1000) x; CREATE PUBLICATION monitor_copy FOR TABLE public.monitor_copy",
        ),
      )
      defer @async.protect_from_cancel(() => {
        ignore(
          sql.query(
            "DROP PUBLICATION monitor_copy; DROP TABLE public.monitor_copy",
          ),
        )
      })
      let metrics = RuntimeMetrics::new()
      let mut entered = false
      let mut released = false
      let mut reached_end = false
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          while !entered || metrics.copy_lags.is_empty() {
            @async.sleep(5)
          }
          ignore(
            sql.query(
              "UPDATE public.monitor_copy SET value='changed'; SELECT pg_switch_wal(); CHECKPOINT",
            ),
          )
          for _ in 0..<8 {
            let slots = sql.query(
              "SELECT wal_status FROM pg_replication_slots WHERE slot_name='monitor_copy'",
            )
            if slots.is_empty() || cell(slots, 0, 0) == "lost" {
              return
            }
            ignore(sql.query("SELECT pg_switch_wal(); CHECKPOINT"))
          }
          fail("copy fixture WAL was not lost")
        })
        try
          with_copy_monitor(config, "monitor_copy", 10, metrics, notify => {
            ignore(
              copy_snapshot_impl(
                config,
                slot="monitor_copy",
                publication="monitor_copy",
                batch_rows=32,
                max_frame_bytes=16777216,
                max_batch_bytes=67108864,
                temporary=true,
                table=None,
                on_snapshot=notify,
                event => {
                  match event {
                    TableRows(_, _) => {
                      entered = true
                      defer {
                        released = true
                      }
                      @async.sleep(60000)
                    }
                    TableEnd(_) => reached_end = true
                    _ => ()
                  }
                },
              ),
            )
          })
        catch {
          Retryable("table copy replication slot invalidated")
          | Retryable("table copy replication slot disappeared") => ()
          error => raise error
        } noraise {
          _ => fail("COPY survived invalidated slot")
        }
        group.return_immediately(())
      })
      assert_true(entered && released)
      assert_false(reached_end)
      assert_true(metrics.copy_lags.is_empty())
      assert_eq(
        metrics.counters["mooncdc_slot_invalidations_total{worker_type=\"table_sync\"}"],
        1UL,
      )
      let mut rows = 0
      ignore(
        copy_snapshot_impl(
          config,
          slot="monitor_copy_retry",
          publication="monitor_copy",
          temporary=true,
          table=None,
          max_frame_bytes=16777216,
          max_batch_bytes=67108864,
          batch_rows=32,
          event => if event is TableRows(_, batch) { rows += batch.length() },
        ),
      )
      assert_eq(rows, 1000)
      @async.with_timeout(config.timeout_ms, () => {
        while cell(
                sql.query(
                  "SELECT count(*)::text FROM pg_replication_slots WHERE slot_name IN ('monitor_copy','monitor_copy_retry')",
                ),
                0,
                0,
              ) !=
              "0" {
          @async.sleep(20)
        }
      })
      println(
        "LIVE: real COPY slot WAL loss cancels callback; fresh snapshot contains 1000 rows; slots and monitor released",
      )
    })
  }
}
