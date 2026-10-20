///|
async test "HTTP health rejects invalid configuration before running the host" {
  let control = @mooncdc.ApplyControl()
  for (port, timeout) in [(0, 1), (-1, 1), (65536, 1), (9001, 0), (9001, -1)] {
    try
      @mooncdc.with_health_server(control, port~, stall_timeout_ms=timeout, fn() -> Unit raise {
        fail("host must not run")
      })
    catch {
      @mooncdc.InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid health configuration accepted")
    }
  }
}

///|
test "pipeline health is read only and stop immediately removes readiness" {
  let control = @mooncdc.ApplyControl()
  assert_eq(control.health().lifecycle, @mooncdc.Initializing)
  assert_true(control.health().live)
  assert_false(control.health().ready)
  assert_eq(control.activities().length(), 0)
  for timeout in [0, -1] {
    try control.health(stall_timeout_ms=timeout) catch {
      @mooncdc.InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid health timeout accepted")
    }
  }
  control.stop()
  assert_eq(control.health().lifecycle, @mooncdc.Stopping)
  assert_eq(control.health().status, "stopping")
  assert_true(control.health().live)
  assert_false(control.health().ready)
}

///|
test "LSN roundtrip unsigned ordering and strict syntax" {
  for text in ["0/0", "FFFFFFFF/FFFFFFFF", "1/ABCDEF"] {
    assert_eq(@mooncdc.Lsn::parse(text).to_string(), text)
  }
  assert_true(
    @mooncdc.Lsn::parse("80000000/0") > @mooncdc.Lsn::parse("7FFFFFFF/FFFFFFFF"),
  )
  for invalid in ["", "0", "-1/0", "1/100000000", "0x1/0", "1/2/3", "1_0/0"] {
    try @mooncdc.Lsn::parse(invalid) catch {
      _ => ()
    } noraise {
      _ => fail("invalid LSN accepted")
    }
  }
}

///|
test "binary values remain distinct from text null and unchanged TOAST" {
  let value = @mooncdc.Binary(b"\x00\xff\x80")
  assert_true(value is @mooncdc.Binary(b"\x00\xff\x80"))
  assert_true(value != @mooncdc.Null)
  assert_true(value != @mooncdc.UnchangedToast)
  assert_true(value != @mooncdc.Text(""))
}

///|
async test "table error and manual retry APIs are available to consumers" {
  let failure : Error = @mooncdc.TableReplicationError::Retryable("temporary")
  assert_true(failure is @mooncdc.Retryable("temporary"))
  let phase = @mooncdc.TableSyncPhase::Errored(
    reason="blocked",
    retry=ManualRetry,
    attempts=2,
  )
  assert_true(phase is Errored(retry=ManualRetry, ..))
  let control = @mooncdc.ApplyControl()
  try control.retry_table(1U) catch {
    @mooncdc.InvalidConfig(_) => ()
    error => raise error
  } noraise {
    _ => fail("inactive pipeline accepted retry")
  }
  control.stop()
  assert_false(control.retry_table(1U))
}

///|
async test "snapshot validates limits before connecting" {
  let config : @mooncdc.ConnectionConfig = {
    hostaddr: None,
    keepalive: @mooncdc.TcpKeepaliveConfig::new(),
    host: "127.0.0.1",
    port: 1,
    user: "unused",
    password: "",
    database: "unused",
    security: @mooncdc.LocalPlaintext,
    timeout_ms: 1,
  }
  try
    @mooncdc.copy_snapshot(
      config,
      slot="snapshot",
      publication="snapshot",
      batch_rows=0,
      _ => (),
    )
  catch {
    @mooncdc.InvalidConfig(_) => ()
    e => raise e
  } noraise {
    _ => fail("invalid batch limit accepted")
  }
  for connections in [0, -1, 257, 2147483647] {
    try
      @mooncdc.copy_snapshot(
        config,
        slot="snapshot",
        publication="snapshot",
        copy_connections=connections,
        _ => (),
      )
    catch {
      @mooncdc.InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid copy connection limit accepted")
    }
  }
}

///|
async test "slot recreation behavior is an explicit public pipeline option" {
  let connection : @mooncdc.ConnectionConfig = {
    hostaddr: None,
    keepalive: @mooncdc.TcpKeepaliveConfig::new(),
    host: "127.0.0.1",
    port: 1,
    user: "unused",
    password: "",
    database: "unused",
    security: @mooncdc.LocalPlaintext,
    timeout_ms: 1,
  }
  for
    behavior in [
      @mooncdc.RejectInvalidatedSlot,
      @mooncdc.RecreateInvalidatedSlot,
    ] {
    let control = @mooncdc.ApplyControl()
    control.stop()
    @mooncdc.run_pipeline(
      connection,
      RejectDestinationForApiTest::{ },
      publication="pub",
      destination_id="target",
      control~,
      state_path="unused",
      invalidated_slot_behavior=behavior,
    )
  }
}

///|
async test "pipeline monitor and aggregate copy limits validate before stopped execution" {
  let connection = @mooncdc.ConnectionConfig::new(
    host="127.0.0.1",
    user="unused",
    port=1,
    security=@mooncdc.LocalPlaintext,
  )
  let control = @mooncdc.ApplyControl()
  control.stop()
  @mooncdc.run_pipeline(
    connection,
    RejectDestinationForApiTest::{ },
    publication="pub",
    destination_id="target",
    state_path="unused",
    control~,
    copy_concurrency=256,
    copy_connections_per_table=1,
    table_sync_monitor_refresh_interval_ms=1,
  )
  for (workers, interval) in [(257, 10000), (1, 0), (1, -1)] {
    try
      @mooncdc.run_pipeline(
        connection,
        RejectDestinationForApiTest::{ },
        publication="pub",
        destination_id="target",
        state_path="unused",
        control~,
        copy_concurrency=workers,
        table_sync_monitor_refresh_interval_ms=interval,
      )
    catch {
      @mooncdc.InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid monitor/copy limit accepted")
    }
  }
}

///|
struct RejectDestinationForApiTest {}

///|
impl @mooncdc.Destination for RejectDestinationForApiTest with fn reset_table(
  _,
  _,
  attempt~,
) {
  ignore(attempt)
  fail("stopped pipeline contacted destination")
}

///|
impl @mooncdc.Destination for RejectDestinationForApiTest with fn write_rows(
  _,
  _,
  _,
  _,
) {
  fail("stopped pipeline contacted destination")
}

///|
impl @mooncdc.Destination for RejectDestinationForApiTest with fn write_transaction(
  _,
  _,
) {
  fail("stopped pipeline contacted destination")
}

///|
impl @mooncdc.Destination for RejectDestinationForApiTest with fn flush(_) {
  fail("stopped pipeline contacted destination")
}
