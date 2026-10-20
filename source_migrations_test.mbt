///|
async test "source migration public API validates before opening a connection" {
  let config : @mooncdc.ConnectionConfig = {
    hostaddr: None,
    keepalive: @mooncdc.TcpKeepaliveConfig::new(),
    host: "127.0.0.1",
    port: 0,
    user: "test",
    password: "",
    database: "test",
    security: @mooncdc.LocalPlaintext,
    timeout_ms: 1000,
  }
  try @mooncdc.run_source_migrations(config) catch {
    @mooncdc.InvalidConfig(_) => ()
    error => raise error
  } noraise {
    _ => fail("invalid connection reached migration execution")
  }
}
