///|
async test "live SCRAM startup IDENTIFY_SYSTEM and COPY BOTH" {
  if @env.get_env_var("MOONCDC_TEST_PORT") is Some(port) {
    let cfg : ConnectionConfig = {
      host: "127.0.0.1",
      port: @string.parse_int(port),
      user: "cdc",
      password: "mooncdc-test-only",
      database: "postgres",
      security: LocalPlaintext,
      timeout_ms: 5000,
    }
    let conn = connect(cfg, 1048576, true)
    defer conn.close()
    let rows = conn.query("IDENTIFY_SYSTEM")
    assert_eq(rows.length(), 1)
    assert_eq(rows[0][3], Some("postgres"))
    let out = @buffer.Buffer()
    cstring(
      out, "START_REPLICATION SLOT mooncdc_test LOGICAL 0/0 (proto_version '1', publication_names 'mooncdc_test')",
    )
    conn.send(b'Q', out.to_bytes())
    let (tag, body) = conn.read()
    if tag == b'E' {
      raise server_error(body)
    }
    assert_eq(tag, b'W')
  }
}
