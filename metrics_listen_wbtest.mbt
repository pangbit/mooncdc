///|
test "metrics listen defaults and IPv4 IPv6 address plans are offline" {
  assert_true(metrics_listener_address(None, 9000) is None)
  for
    (host, expected, ipv6) in [
      ("127.0.0.1", "127.0.0.1:19000", false),
      ("::1", "[::1]:19000", true),
    ] {
    validate_metrics_listen_address(host)
    guard metrics_listener_address(Some(host), 19000) is Some(address) else {
      fail("explicit metrics address missing")
    }
    assert_eq(address.to_string(), expected)
    assert_eq(address.port(), 19000)
    assert_eq(address.is_ipv6(), ipv6)
  }
}

///|
test "metrics listen invalid literals fail closed offline" {
  for
    host in [
      "", "localhost", " 127.0.0.1", "127.0.0.1\n", "127.1", "256.0.0.1", "127.0.0.1:9000",
      "[::1]", "[::1]:9000", ":::", "::gg", "fe80::1%1", "::ffff:999.0.0.1", "0x7f000001",
    ] {
    try validate_metrics_listen_address(host) catch {
      InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid metrics literal accepted")
    }
  }
}

///|
test "metrics listen invalid ports fail before a listener offline" {
  for port in [0, 65536] {
    try metrics_listener_address(Some("127.0.0.1"), port) catch {
      InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid metrics port accepted")
    }
  }
}

///|
async test "metrics listen invalid API address never enters callback offline" {
  let entered = Ref(false)
  for host in ["", "localhost", "::gg"] {
    try
      with_metrics_server(ApplyControl(), listen_address=host, () => {
        entered.val = true
      })
    catch {
      InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid metrics address reached server")
    }
  }
  assert_false(entered.val)
}
