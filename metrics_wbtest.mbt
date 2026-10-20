///|
test "count histograms keep their own bounds and expose cumulative samples independently of durations" {
  let control = ApplyControl()
  let bounds : Array[Double] = [0, 1, 8, 16, 64]
  for value in [0.0, 8.0, 20.0] {
    control.metrics_data.sample("mooncdc_probe_partitions", value, bounds)
  }
  bounds[1] = 99
  let body = control.prometheus()
  for
    expected in [
      "mooncdc_probe_partitions_bucket{le=\"0\"} 1\n", "mooncdc_probe_partitions_bucket{le=\"1\"} 1\n",
      "mooncdc_probe_partitions_bucket{le=\"8\"} 2\n", "mooncdc_probe_partitions_bucket{le=\"16\"} 2\n",
      "mooncdc_probe_partitions_bucket{le=\"64\"} 3\n", "mooncdc_probe_partitions_bucket{le=\"+Inf\"} 3\n",
      "mooncdc_probe_partitions_sum 28\n", "mooncdc_probe_partitions_count 3\n",
    ] {
    assert_true(body.contains(expected), msg=expected)
  }
  assert_false(body.contains("le=\"0.005\""))
  assert_false(ApplyControl().prometheus().contains("mooncdc_probe_partitions"))
  control.metrics_data.duration(
    "mooncdc_compat_seconds",
    @bench.monotonic_clock_start(),
  )
  assert_true(
    control.prometheus().contains("mooncdc_compat_seconds_bucket{le=\"0.005\"}"),
  )
}

///|
async test "metrics histograms are cumulative bounded isolated and released with activity" {
  let control = ApplyControl()
  let other = ApplyControl()
  let start = @bench.monotonic_clock_start()
  for status in ["accepted", "durable"] {
    let key = "mooncdc_destination_write_seconds{worker_type=\"apply\",replication_path=\"cdc\",status=\"\{status}\"}"
    control.metrics_data.duration(key, start)
    control.metrics_data.duration(key, start)
  }
  control.metrics_data.increment(
    "mooncdc_transactions_written_total{worker_type=\"apply\"}",
    amount=3UL,
  )
  let body = control.prometheus()
  assert_eq(
    body
    .split("\n")
    .filter(line => line == "# TYPE mooncdc_destination_write_seconds histogram")
    .count(),
    1,
  )
  assert_eq(
    body
    .split("\n")
    .filter(line => {
      line.has_prefix("mooncdc_destination_write_seconds_bucket") &&
      line.contains("le=\"+Inf\"} 2")
    })
    .count(),
    2,
  )
  assert_true(
    body.contains("mooncdc_transactions_written_total{worker_type=\"apply\"} 3"),
  )
  assert_false(
    other.prometheus().contains("mooncdc_transactions_written_total"),
  )
  let activity = control.observe(WalApply(wal_sender_timeout_ms=60000UL))
  control.metrics_data.subscriptions[activity.id] = (
    "apply",
    () => {
      reconnects: 0,
      received: { value: 100UL, },
      durable: { value: 90UL, },
      server_end: { value: 100UL, },
      outstanding_transactions: 2,
      queued_transactions: 1,
      wal_lag_bytes: 10UL,
    },
  )
  assert_true(
    control
    .prometheus()
    .contains("mooncdc_observed_wal_lag_bytes{worker_type=\"apply\"} 10\n"),
  )
  activity.close()
  assert_true(control.metrics_data.subscriptions.is_empty())
  assert_false(control.prometheus().contains("mooncdc_observed_wal_lag_bytes"))
}

///|
async test "metrics HTTP uses UTF8 byte lengths HEAD and all exit paths close sockets" {
  @async.with_timeout(5000, () => {
    let control = ApplyControl()
    let occupied = bind_health_listener(0)
    try
      with_metrics_server(control, port=occupied.addr.port(), fn() -> Unit raise {
        fail("occupied metrics port entered callback")
      })
    catch {
      @os_error.OSError(_, ..) => ()
      error => raise error
    } noraise {
      _ => fail("occupied port accepted")
    }
    occupied.close()
    for cancel in [false, true] {
      let listener = bind_health_listener(0)
      let port = listener.addr.port()
      let partial = @socket.Tcp::connect(
        @socket.Addr::parse("127.0.0.1:\{port}"),
      )
      defer partial.close()
      partial.write("GET /metrics HTTP/1.1\r\n")
      try
        @async.with_timeout(100, () => {
          with_metrics_listener(
            listener,
            () => control.prometheus(pipeline_id="中文"),
            () => {
              let tcp = @socket.Tcp::connect(
                @socket.Addr::parse("127.0.0.1:\{port}"),
              )
              defer tcp.close()
              tcp.write(
                "HEAD /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
              )
              let response = @utf8.decode(tcp.read_all().binary())
              let expected = @utf8.encode(
                control.prometheus(pipeline_id="中文"),
              ).length()
              assert_true(
                response.to_lower().contains("content-length: \{expected}\r\n"),
              )
              assert_true(response.has_suffix("\r\n\r\n"))
              if cancel {
                @async.sleep(60000)
              } else {
                raise Protocol("metrics callback failure")
              }
            },
          )
        })
      catch {
        @async.TimeoutError => assert_true(cancel)
        Protocol("metrics callback failure") => assert_false(cancel)
        error => raise error
      } noraise {
        _ => fail("metrics callback failure swallowed")
      }
      assert_eq(partial.read_some(), None)
      let reclaimed = bind_health_listener(port)
      reclaimed.close()
    }
  })
}
