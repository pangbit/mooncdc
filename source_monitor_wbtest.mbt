///|
test "source lag samples clamp gaps preserve worker scopes and vanish on close" {
  assert_eq(source_current_lsn([[Some("0/30")]]), Some(Lsn::parse("0/30")))
  assert_eq(source_current_lsn([[None]]), None)
  for rows in [[], [[], []], [[Some("0/1"), None]], [[Some("broken")]]] {
    try source_current_lsn(rows) catch {
      Protocol(_) | InvalidConfig(_) => ()
      e => raise e
    } noraise {
      _ => fail("invalid source sample accepted")
    }
  }
  let control = ApplyControl()
  let apply = control.observe(WalApply(wal_sender_timeout_ms=60000UL))
  let table = control.observe(
    WalApply(wal_sender_timeout_ms=60000UL),
    worker=TableWorker("slot", 1),
  )
  control.metrics_data.source_monitors[apply.id] = {
    current: Some(Lsn::parse("0/30")),
    sampled: Some(@bench.monotonic_clock_start()),
    worker_type: "apply",
    progress: () => ({ value: 32UL, }, { value: 16UL, }),
  }
  control.metrics_data.source_monitors[table.id] = {
    current: Some(Lsn::parse("0/10")),
    sampled: Some(@bench.monotonic_clock_start()),
    worker_type: "table_sync",
    progress: () => ({ value: 48UL, }, { value: 32UL, }),
  }
  let text = control.prometheus()
  assert_true(
    text.contains(
      "mooncdc_source_lsn_monitors_active{worker_type=\"apply\"} 1\n",
    ),
  )
  assert_true(
    text.contains(
      "mooncdc_source_lsn_monitors_active{worker_type=\"table_sync\"} 1\n",
    ),
  )
  assert_true(
    text.contains(
      "mooncdc_current_wal_received_lag_bytes{worker_type=\"apply\"} 16\n",
    ),
  )
  assert_true(
    text.contains(
      "mooncdc_current_wal_durable_lag_bytes{worker_type=\"apply\"} 32\n",
    ),
  )
  assert_true(
    text.contains(
      "mooncdc_received_durable_gap_bytes{worker_type=\"apply\"} 16\n",
    ),
  )
  assert_true(
    text.contains(
      "mooncdc_current_wal_received_lag_bytes{worker_type=\"table_sync\"} 0\n",
    ),
  )
  assert_true(
    text.contains(
      "mooncdc_current_wal_durable_lag_bytes{worker_type=\"table_sync\"} 0\n",
    ),
  )
  assert_true(
    text.contains(
      "mooncdc_received_durable_gap_bytes{worker_type=\"table_sync\"} 16\n",
    ),
  )
  apply.close()
  table.close()
  assert_true(control.metrics_data.source_monitors.is_empty())
  assert_false(
    control.prometheus().contains("mooncdc_current_wal_received_lag_bytes"),
  )
}

///|
async test "source monitor cancellation joins unfinished handshake without waiting for timeout" {
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
    let control = ApplyControl()
    let activity = control.observe(WalApply(wal_sender_timeout_ms=60000UL))
    defer activity.close()
    @async.with_task_group(group => {
      group.spawn_bg(() => {
        with_source_monitor(
          config,
          activity,
          60000,
          () => ({ value: 16UL, }, { value: 16UL, }),
          () => @async.sleep(60000),
        )
      })
      let (tcp, _) = server.accept()
      defer tcp.close()
      group.return_immediately(())
    })
    assert_true(control.metrics_data.source_monitors.is_empty())
  })
}

///|
async test "source monitor failures remain observational and do not refresh blocked worker activity" {
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let config = ConnectionConfig::new(
      host="127.0.0.1",
      user="unused",
      port=server.addr.port(),
      security=LocalPlaintext,
      timeout_ms=100,
    )
    let control = ApplyControl()
    control.lifecycle = Running
    let activity = control.observe(WalApply(wal_sender_timeout_ms=1UL))
    defer activity.close()
    activity.destination_busy(true)
    @async.with_task_group(group => {
      group.spawn_bg(() => {
        for _ in 0..<2 {
          let (tcp, _) = server.accept()
          tcp.close()
        }
      })
      with_source_monitor(
        config,
        activity,
        10,
        () => ({ value: 32UL, }, { value: 16UL, }),
        () => {
          while control.metrics_data.counters
                .get(
                  "mooncdc_source_monitor_failures_total{worker_type=\"apply\"}",
                )
                .unwrap_or(0UL) <
                2UL {
            @async.sleep(5)
          }
          let observation = control.metrics_data.source_monitors[activity.id]
          assert_eq(
            (observation.progress)(),
            (Lsn::parse("0/20"), Lsn::parse("0/10")),
          )
          assert_eq(observation.current, None)
          @async.sleep(20)
          assert_false(control.health(stall_timeout_ms=10).ready)
        },
      )
      group.return_immediately(())
    })
    assert_true(control.metrics_data.source_monitors.is_empty())
  })
}
