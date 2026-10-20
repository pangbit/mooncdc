///|
test "Prometheus rendering is scoped read only and escapes bounded labels" {
  let control = @mooncdc.ApplyControl()
  let plain = control.prometheus()
  assert_true(plain.contains("# TYPE mooncdc_ready gauge\nmooncdc_ready 0\n"))
  let labels = control.prometheus(
    pipeline_id="管道\"\\\n",
    destination="clickhouse",
    project="isolated",
  )
  assert_true(
    labels.contains(
      "pipeline_id=\"管道\\\"\\\\\\n\",destination=\"clickhouse\",project=\"isolated\"",
    ),
  )
  assert_eq(control.health().lifecycle, @mooncdc.Initializing)
  assert_eq(control.activities().length(), 0)
  assert_eq(plain, @mooncdc.ApplyControl().prometheus())
  for value in ["x".repeat(1025), "中".repeat(342)] {
    try control.prometheus(pipeline_id=value) catch {
      @mooncdc.InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("oversize metrics label accepted")
    }
  }
  control.stop()
  assert_true(
    control.prometheus().contains("mooncdc_lifecycle{state=\"stopping\"} 1\n"),
  )
}

///|
async test "public metrics listener exposes scoped state and validates before running" {
  let control = @mooncdc.ApplyControl()
  for port in [0, -1, 65536] {
    try
      @mooncdc.with_metrics_server(control, port~, fn() -> Unit raise {
        fail("invalid port entered callback")
      })
    catch {
      @mooncdc.InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid metrics port accepted")
    }
  }
  let reserved = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
  let port = reserved.addr.port()
  reserved.close()
  @mooncdc.with_metrics_server(control, port~, pipeline_id="pipeline", () => {
    let url = "http://127.0.0.1:\{port}"
    let (response, body) = @http.get(url + "/metrics")
    assert_eq(response.code, 200)
    assert_eq(body.text(), control.prometheus(pipeline_id="pipeline"))
    let (response, body) = @http.get(url + "/health?probe=1")
    assert_eq(response.code, 200)
    assert_eq(body.text(), "OK")
    control.stop()
    let (_, body) = @http.get(url + "/arbitrary")
    assert_true(body.text().contains("state=\"stopping\"} 1"))
  })
  assert_eq(@mooncdc.with_metrics_server(control, port~, () => 42), 42)
}
