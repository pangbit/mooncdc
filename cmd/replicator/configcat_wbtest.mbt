///|
fn configcat_fixture(
  base : String,
  custom~ : Bool = true,
  key~ : String = "abcdefghijklmnopqrstuv/ABCDEFGHIJKLMNOPQRSTUV",
) -> ConfigCatOptions {
  {
    sdk_key: key,
    default_user: "test-project",
    base_url: base,
    custom_url: custom,
    poll_ms: 10,
    timeout_ms: 1000,
  }
}

///|
async test "ConfigCat configuration preserves fixed defaults keys user and offline environment overrides" {
  assert_true(parse_config(fixture()).configcat is None)
  for
    key in [
      "abcdefghijklmnopqrstuv/ABCDEFGHIJKLMNOPQRSTUV",
      "configcat-sdk-1/abcdefghijklmnopqrstuv/ABCDEFGHIJKLMNOPQRSTUV",
      "é".repeat(11) + "/" + "a".repeat(22),
    ] {
    let config = parse_config(
      merge_config(fixture(), {
        "supabase": {
          "project_ref": "test-project",
          "configcat_sdk_key": Json::Json(key),
        },
      }),
    )
    config.validate()
    let options = config.configcat.unwrap()
    assert_eq(options.sdk_key, key)
    assert_eq(options.default_user, "test-project")
    assert_eq(options.base_url, "https://cdn-global.configcat.com")
    assert_false(options.custom_url)
    assert_eq(options.poll_ms, 60000)
    assert_eq(options.timeout_ms, 30000)
    assert_eq(config.active_reports().redact(key), "[redacted]")
  }
  let dir = @fs.tmpdir(prefix="mooncdc-configcat-offline-")
  defer @fs.rmdir(dir, recursive=true)
  @fs.write_file(dir + "/config.yaml", fixture().stringify())
  let config = load_config(Some(dir + "/config.yaml"), {
    "APP_SUPABASE__PROJECT_REF": "test-project",
    "APP_SUPABASE__CONFIGCAT_SDK_KEY": "configcat-proxy/SECRET",
    "APP_SUPABASE__CONFIGCAT_BASE_URL": "http://127.0.0.1:1/prefix/",
    "APP_SUPABASE__CONFIGCAT_POLL_INTERVAL_MS": "123",
    "APP_SUPABASE__CONFIGCAT_HTTP_TIMEOUT_MS": "456",
  })
  config.validate()
  assert_eq(config.configcat.unwrap().base_url, "http://127.0.0.1:1/prefix")
  assert_eq(config.configcat.unwrap().poll_ms, 123)
  assert_eq(config.configcat.unwrap().timeout_ms, 456)
  assert_eq(
    config.active_reports().redact("configcat-proxy/SECRET"),
    "[redacted]",
  )
  let invalids : Array[Json] = [
    { "configcat_sdk_key": "SECRET" },
    { "configcat_sdk_key": "" },
    { "configcat_sdk_key": "configcat-proxy/SECRET" },
    {
      "configcat_sdk_key": "configcat-proxy/",
      "configcat_base_url": "http://127.0.0.1:1",
    },
    { "configcat_base_url": "http://SECRET" },
    { "configcat_poll_interval_ms": 100 },
    { "configcat_http_timeout_ms": 100 },
    { "configcat_sdk_key": true },
    {
      "configcat_sdk_key": "abcdefghijklmnopqrstuv/ABCDEFGHIJKLMNOPQRSTUV",
      "configcat_base_url": "https://SECRET@example.com",
    },
    {
      "configcat_sdk_key": "abcdefghijklmnopqrstuv/ABCDEFGHIJKLMNOPQRSTUV",
      "configcat_poll_interval_ms": 0,
    },
    {
      "configcat_sdk_key": "abcdefghijklmnopqrstuv/ABCDEFGHIJKLMNOPQRSTUV",
      "configcat_http_timeout_ms": -1,
    },
  ]
  for invalid in invalids {
    try
      parse_config(
        merge_config(fixture(), {
          "supabase": merge_config({ "project_ref": "test-project" }, invalid),
        }),
      )
    catch {
      ConfigError(message) => assert_false(message.contains("SECRET"))
      error => raise error
    } noraise {
      _ => fail("invalid ConfigCat configuration accepted")
    }
  }
  assert_eq(configcat_key_path("a b/中?#\r\n"), "a%20b/%E4%B8%AD%3F%23%0D%0A")
}

///|
async test "ConfigCat ETag cache retains old document across transport status and malformed response failures" {
  @async.with_timeout(10000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let options = configcat_fixture(
      "http://127.0.0.1:\{server.addr.port()}/prefix",
    )
    let cache = ConfigCatCache::new(options)
    let cases = [
      (200, "{\"f\":{},\"s\":[]}", false),
      (304, "", false),
      (500, "RAW-SECRET", true),
      (200, "not-json SECRET", true),
      (200, "{\"f\":[],\"s\":[]}", true),
      (200, "{\"f\":{},\"p\":{\"r\":3}}", true),
      (302, "", true),
      (403, "SECRET", true),
      (404, "SECRET", true),
    ]
    @async.with_task_group(group => {
      group.spawn_bg(() => {
        for index, entry in cases {
          let (tcp, _) = server.accept()
          let conn = @http.ServerConnection::new(tcp)
          defer conn.close()
          let request = conn.read_request()
          assert_eq(request.meth, @http.Get)
          assert_eq(
            request.path,
            "/prefix/configuration-files/abcdefghijklmnopqrstuv/ABCDEFGHIJKLMNOPQRSTUV/config_v6.json",
          )
          assert_eq(
            request.headers["X-ConfigCat-UserAgent"],
            "ConfigCat-MoonCDC/a-0.1.0",
          )
          assert_true(request.headers.get("apikey") is None)
          assert_false(request.path.contains(options.default_user))
          if index == 0 {
            assert_true(request.headers.get("If-None-Match") is None)
          } else {
            assert_eq(request.headers["If-None-Match"], "etag-one")
          }
          conn.send_response(entry.0, "Controlled", extra_headers={
            "ETag": if index == 0 {
              "etag-one"
            } else {
              "etag-must-not-replace"
            },
            "Location": "http://127.0.0.1:1/unused",
          })
          if entry.0 != 304 {
            conn.write(entry.1)
          }
          conn.end_response()
        }
      })
      for index, entry in cases {
        cache.refreshed_ms = Some(0L)
        try cache.refresh(options) catch {
          _ => assert_true(entry.2)
        } noraise {
          _ => assert_false(entry.2)
        }
        assert_eq(cache.document, Some({ "f": {}, "s": [] }))
        assert_eq(cache.etag, "etag-one")
        if index <= 1 || entry.0 == 403 || entry.0 == 404 {
          assert_true(cache.refreshed_ms != Some(0L))
        } else {
          assert_eq(cache.refreshed_ms, Some(0L))
        }
      }
    })
  })
}

///|
async test "ConfigCat V6 governance custom and proxy preferences control bounded redirects" {
  @async.with_timeout(10000, () => {
    for
      (custom, redirect, proxy) in [
        (false, 0, false),
        (false, 1, false),
        (false, 2, false),
        (true, 1, false),
        (true, 2, false),
        (true, 2, true),
      ] {
      let old = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer old.close()
      let next = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer next.close()
      let old_url = "http://127.0.0.1:\{old.addr.port()}"
      let new_url = "http://127.0.0.1:\{next.addr.port()}"
      let options = configcat_fixture(
        old_url,
        custom~,
        key=if proxy {
          "configcat-proxy/controlled"
        } else {
          "abcdefghijklmnopqrstuv/ABCDEFGHIJKLMNOPQRSTUV"
        },
      )
      let cache = ConfigCatCache::new(options)
      let stay = custom && (proxy || redirect != 2)
      let first : Json = {
        "f": {},
        "p": { "u": Json::Json(new_url), "r": Json::Json(redirect) },
        "fixture": "old",
      }
      let last : Json = { "f": {}, "fixture": "new" }
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          for _ in 0..<(if stay { 2 } else { 1 }) {
            let (tcp, _) = old.accept()
            let conn = @http.ServerConnection::new(tcp)
            defer conn.close()
            ignore(conn.read_request())
            conn.send_response(200, "Controlled")
            conn.write(first.stringify())
            conn.end_response()
          }
        })
        if !stay {
          group.spawn_bg(() => {
            let (tcp, _) = next.accept()
            let conn = @http.ServerConnection::new(tcp)
            defer conn.close()
            ignore(conn.read_request())
            conn.send_response(200, "Controlled")
            conn.write(last.stringify())
            conn.end_response()
          })
        }
        cache.refresh(options)
        if redirect == 0 || stay {
          assert_eq(cache.document, Some(first))
          cache.refresh(options)
        }
        assert_eq(cache.document, Some(if stay { first } else { last }))
        assert_eq(cache.fetch_url, if stay { old_url } else { new_url })
      })
    }
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let options = configcat_fixture(
      "http://127.0.0.1:\{server.addr.port()}",
      custom=false,
    )
    let cache = ConfigCatCache::new(options)
    cache.document = Some({ "f": {}, "fixture": "retained" })
    cache.etag = "retained-etag"
    cache.refreshed_ms = Some(0L)
    let mut attempts = 0
    @async.with_task_group(group => {
      group.spawn_bg(() => {
        for _ in 0..<3 {
          let (tcp, _) = server.accept()
          let conn = @http.ServerConnection::new(tcp)
          defer conn.close()
          ignore(conn.read_request())
          attempts += 1
          conn.send_response(200, "Controlled")
          conn.write("{\"f\":{},\"p\":{\"r\":2}}")
          conn.end_response()
        }
      })
      try cache.refresh(options) catch {
        _ => ()
      } noraise {
        _ => fail("unbounded redirect accepted")
      }
      assert_eq(attempts, 3)
      assert_eq(cache.document, Some({ "f": {}, "fixture": "retained" }))
      assert_eq(cache.etag, "retained-etag")
      assert_eq(cache.refreshed_ms, Some(0L))
    })
  })
}

///|
async test "ConfigCat response limit timeout and scope completion close outstanding HTTP sockets" {
  @async.with_timeout(10000, () => {
    for mode in ["timeout", "limit", "normal", "failure", "cancel"] {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let options = {
        ..configcat_fixture("http://127.0.0.1:\{server.addr.port()}"),
        timeout_ms: if mode == "timeout" {
          100
        } else {
          5000
        },
      }
      let mut accepted = false
      let mut failed = false
      @async.with_task_group(group => {
        let worker = group.spawn(() => {
          if mode == "timeout" || mode == "limit" {
            let cache = ConfigCatCache::new(options)
            try cache.refresh(options) catch {
              _ => failed = true
            } noraise {
              _ => fail("invalid refresh succeeded")
            }
            assert_true(cache.document is None)
            assert_eq(cache.etag, "")
          } else {
            with_configcat(Some(options), () => {
              while !accepted {
                @async.sleep(1)
              }
              if mode == "failure" {
                fail("controlled-pipeline-error")
              }
              if mode == "cancel" {
                @async.sleep(60000)
              }
            }) catch {
              error => {
                assert_eq(mode, "failure")
                assert_true(
                  error.to_string().contains("controlled-pipeline-error"),
                )
                failed = true
              }
            }
          }
        })
        let (tcp, _) = server.accept()
        let conn = @http.ServerConnection::new(tcp)
        defer conn.close()
        ignore(conn.read_request())
        accepted = true
        if mode == "limit" {
          try {
            conn.send_response(200, "Controlled")
            conn.write("x".repeat(1048577))
            conn.end_response()
          } catch {
            _ => ()
          }
        }
        if mode == "cancel" {
          worker.cancel()
        }
        if mode != "limit" {
          assert_true(tcp.read_some() is None)
        }
        if mode == "cancel" {
          try worker.wait() catch {
            @async.WaitedTaskAlreadyCancelled => ()
            error => raise error
          } noraise {
            _ => fail("cancelled client scope returned successfully")
          }
        } else {
          worker.wait()
        }
        assert_eq(
          failed,
          mode == "timeout" || mode == "limit" || mode == "failure",
        )
      })
    }
  })
}

///|
async test "ConfigCat auto polling failures do not fail the pipeline and disabled client performs no IO" {
  let mut runs = 0
  with_configcat(None, () => runs += 1)
  assert_eq(runs, 1)
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let options = configcat_fixture("http://127.0.0.1:\{server.addr.port()}")
    let mut requests = 0
    @async.with_task_group(group => {
      group.spawn_bg(no_wait=true, () => {
        while true {
          let (tcp, _) = server.accept()
          let conn = @http.ServerConnection::new(tcp)
          defer conn.close()
          ignore(conn.read_request())
          requests += 1
          conn.send_response(503, "Controlled")
          conn.write("RAW-SECRET")
          conn.end_response()
        }
      })
      with_configcat(Some(options), () => {
        while requests < 3 {
          @async.sleep(1)
        }
        runs += 1
      })
    })
    assert_eq(runs, 2)
    assert_true(requests >= 3)
  })
}
