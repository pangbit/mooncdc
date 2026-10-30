///|
test "health fallback accepts only unsupported socket families and protocols" {
  assert_true(
    health_ipv4_fallback(
      @os_error.OSError(errno_eafnosupport(), context="socket"),
    ),
  )
  assert_true(
    health_ipv4_fallback(
      @os_error.OSError(errno_eprotonosupport(), context="socket"),
    ),
  )
  assert_false(health_ipv4_fallback(@os_error.OSError(1, context="permission")))
  assert_false(health_ipv4_fallback(Protocol("unrelated")))
}

///|
async fn assert_health_http(
  url : String,
  verb : @http.RequestMethod,
  code : Int32,
  body : String,
) -> Unit {
  // Use literal TCP addresses: the dependency's URL client sends IPv6 brackets
  // to hostname resolution. Read the wire directly, including the HEAD body.
  let authority_and_path = url[7:]
  let separator = authority_and_path.find("/").unwrap()
  let authority = authority_and_path[:separator].to_owned()
  let path = authority_and_path[separator:].to_owned()
  let tcp = @socket.Tcp::connect(@socket.Addr::parse(authority))
  defer tcp.close()
  let verb = match verb {
    Get => "GET"
    Head => "HEAD"
    Post => "POST"
    _ => fail("unsupported test verb")
  }
  tcp.write(
    "\{verb} \{path} HTTP/1.1\r\nHost: \{authority}\r\nConnection: close\r\n\r\n",
  )
  let response = @utf8.decode(tcp.read_all().binary())
  let separator = response.find("\r\n\r\n").unwrap()
  let headers = response[:separator].to_owned().to_lower()
  assert_true(headers.has_prefix("http/1.1 \{code} "))
  assert_true(headers.contains("\r\ncontent-length: \{body.length()}"))
  assert_eq(
    response[separator + 4:].to_owned(),
    if verb == "HEAD" {
      ""
    } else {
      body
    },
  )
  if code == 405 {
    assert_true(headers.contains("\r\nallow: GET,HEAD".to_lower()))
  }
}

///|
async test "HTTP health exposes startup running inactivity and draining over dual stack" {
  @async.with_timeout(5000, () => {
    let control = ApplyControl()
    let listener = bind_health_listener(0)
    let port = listener.addr.port()
    let result = with_health_listener(control, listener, 10, () => {
      let hosts = if listener.addr.is_ipv6() {
        ["127.0.0.1", "[::1]"]
      } else {
        ["127.0.0.1"]
      }
      for host in hosts {
        let url = "http://\{host}:\{port}"
        assert_health_http(url + "/livez", Get, 200, "ok")
        assert_health_http(url + "/readyz", Get, 503, "initializing")
        assert_health_http(url + "/readyz?probe=1", Head, 503, "initializing")
        assert_health_http(url + "/unknown", Get, 404, "")
        assert_health_http(url + "/livez", Post, 405, "")
      }
      control.lifecycle = Running
      let url = "http://127.0.0.1:\{port}"
      let wal = control.observe(WalApply(wal_sender_timeout_ms=60000UL))
      assert_health_http(url + "/readyz", Get, 200, "ok")
      assert_health_http(url + "/readyz", Head, 200, "ok")
      wal.close()
      let observation = control.observe(InitialTableCopy)
      @async.sleep(30)
      assert_health_http(url + "/livez", Get, 503, "inactive")
      assert_health_http(url + "/readyz", Get, 503, "inactive")
      control.stop()
      assert_health_http(url + "/livez", Head, 200, "ok")
      assert_health_http(url + "/readyz", Get, 503, "stopping")
      observation.close()
      42
    })
    assert_eq(result, 42)
    let reclaimed = bind_health_listener(port)
    reclaimed.close()
  })
}

///|
async test "HTTP health preserves bind errors and closes on callback failure or cancellation" {
  @async.with_timeout(5000, () => {
    let control = ApplyControl()
    let occupied = bind_health_listener(0)
    defer occupied.close()
    let mut entered = false
    try
      with_health_server(control, port=occupied.addr.port(), () => {
        entered = true
      })
    catch {
      error => assert_false(health_ipv4_fallback(error))
    } noraise {
      _ => fail("occupied health port accepted")
    }
    assert_false(entered)
    for cancel in [false, true] {
      let listener = bind_health_listener(0)
      let port = listener.addr.port()
      let partial = @socket.Tcp::connect(
        @socket.Addr::parse("127.0.0.1:\{port}"),
      )
      defer partial.close()
      partial.write("GET /livez HTTP/1.1\r\n")
      try
        @async.with_timeout(100, async fn() -> Unit {
          with_health_listener(control, listener, 300000, () => {
            assert_health_http("http://127.0.0.1:\{port}/livez", Get, 200, "ok")
            if cancel {
              @async.sleep(60000)
            } else {
              raise Protocol("host failure")
            }
          })
        })
      catch {
        @async.TimeoutError => assert_true(cancel)
        Protocol(reason) => {
          assert_false(cancel)
          assert_eq(reason, "host failure")
        }
        error => raise error
      } noraise {
        _ => fail("host failure or cancellation swallowed")
      }
      assert_eq(partial.read_some(), None)
      let reclaimed = bind_health_listener(port)
      reclaimed.close()
    }
    assert_eq(control.health().status, "initializing")
  })
}
