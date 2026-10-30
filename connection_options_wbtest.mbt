///|
extern "C" fn keepalive_value(fd : Int32, kind : Int32) -> Int32 = "mooncdc_keepalive_value"

///|
test "hostaddr parsing keeps numeric IPv4 IPv6 and plaintext boundaries" {
  let config = ConnectionConfig::new(
    host="127.0.0.1",
    user="test",
    security=LocalPlaintext,
  )
  for ip in ["127.0.0.1", "::1", "0:0:0:0:0:0:0:1"] {
    let config = { ..config, hostaddr: Some(ip), }
    validate_connection(config)
    assert_eq(connection_hostaddr(config).unwrap().port(), 5432)
  }
  for
    ip in [
      "", "localhost", "127.0.0.1:5432", "[::1]", "fe80::1%lo0", "127.0.0.1\u0000",
      "198.51.100.1",
    ] {
    try validate_connection({ ..config, hostaddr: Some(ip), }) catch {
      InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid or remote plaintext hostaddr accepted")
    }
  }
}

///|
async test "live hostaddr preserves TLS name and reads actual TCP keepalive settings" {
  if live_config() is Some(base) &&
    @env.get_env_var("MOONCDC_TEST_CA") is Some(ca) {
    let config = {
      ..base,
      host: "mooncdc-test.invalid",
      hostaddr: Some("127.0.0.1"),
      security: VerifyCAFile(ca),
    }
    for
      keepalive in [
        TcpKeepaliveConfig::new(),
        TcpKeepaliveConfig::new(idle_secs=7, interval_secs=4, retries=2),
      ] {
      let conn = connect({ ..config, keepalive, }, 1048576, true)
      defer conn.close()
      assert_eq(cell(conn.query("IDENTIFY_SYSTEM"), 0, 3), base.database)
      if !(@async.platform is Windows) {
        assert_true(keepalive_value(conn.tcp.fd(), 0) > 0)
        assert_eq(keepalive_value(conn.tcp.fd(), 1), keepalive.idle_secs)
        assert_eq(keepalive_value(conn.tcp.fd(), 2), keepalive.interval_secs)
        assert_eq(keepalive_value(conn.tcp.fd(), 3), keepalive.retries)
      }
    }
    try
      connect({ ..config, host: "wrong.mooncdc-test.invalid", }, 1048576, true)
    catch {
      @tls.TlsError(_) => ()
      error => raise error
    } noraise {
      conn => {
        conn.close()
        fail("hostaddr bypassed TLS hostname verification")
      }
    }
    println(
      "LIVE: numeric dialing with separate TLS hostname and kernel keepalive default/custom readback passed",
    )
  }
}
