///|
test "connection constructor defaults and invalid keepalive options are public" {
  let config = @mooncdc.ConnectionConfig::new(
    host="database.invalid",
    hostaddr="127.0.0.1",
    user="test",
  )
  assert_eq(config.host, "database.invalid")
  assert_eq(config.hostaddr, Some("127.0.0.1"))
  assert_eq(config.port, 5432)
  assert_true(config.security is @mooncdc.VerifyFull)
  assert_eq(
    config.keepalive,
    @mooncdc.TcpKeepaliveConfig::new(idle_secs=30, interval_secs=10, retries=3),
  )
  for
    keepalive in [
      @mooncdc.TcpKeepaliveConfig::new(idle_secs=0),
      @mooncdc.TcpKeepaliveConfig::new(interval_secs=-1),
      @mooncdc.TcpKeepaliveConfig::new(retries=0),
    ] {
    try
      @mooncdc.ConnectionConfig::new(
        host="database.invalid",
        user="test",
        keepalive~,
      )
    catch {
      @mooncdc.InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid keepalive accepted")
    }
  }
}
