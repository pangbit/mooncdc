///|
test "optional reporting configuration is offline strict and excludes secrets from errors" {
  let config = parse_config(fixture())
  assert_true(config.reports.sentry is None)
  assert_true(config.reports.notification is None)
  let valid : Array[Json] = [
    { "sentry": Json::null(), "supabase": Json::null() },
    { "sentry": { "dsn": "https://public:private@example.com/prefix/42/" } },
    { "supabase": { "project_ref": "project", "api_key": "" } },
    {
      "supabase": {
        "project_ref": "project",
        "api_url": "https://example.com/prefix/",
        "api_key": "test-key",
      },
    },
  ]
  for value in valid {
    ignore(parse_config(merge_config(fixture(), value)))
  }
  let invalids : Array[Json] = [
    { "sentry": { "dsn": "SECRET" } },
    { "sentry": { "dsn": "ftp://SECRET@example.com/1" } },
    { "sentry": { "dsn": "https://SECRET@example.com" } },
    { "sentry": { "dsn": "https://SECRET@example.com:70000/1" } },
    { "sentry": { "dsn": "https://SECRET@example.com/1?q=secret" } },
    { "sentry": { "dsn": "https://secret\r\n@example.com/1" } },
    { "sentry": { "dsn": "https://secret,header@example.com/1" } },
    { "sentry": { "dsn": true } },
    { "sentry": false },
    { "supabase": { "project_ref": "../SECRET" } },
    {
      "supabase": {
        "project_ref": "project",
        "api_url": "",
        "api_key": "SECRET",
      },
    },
    {
      "supabase": { "project_ref": "project", "api_url": "https://example.com" },
    },
    {
      "supabase": {
        "project_ref": "project",
        "api_url": "https://example.com",
        "api_key": " \t",
      },
    },
    {
      "supabase": {
        "project_ref": "project",
        "api_url": "https://example.com",
        "api_key": "SECRET\r\n",
      },
    },
    { "supabase": { "project_ref": "project", "configcat_sdk_key": "SECRET" } },
  ]
  for value in invalids {
    try parse_config(merge_config(fixture(), value)) catch {
      ConfigError(message) => assert_false(message.contains("SECRET"))
      e => raise e
    } noraise {
      _ => fail("invalid report configuration accepted")
    }
  }
  let target = parse_sentry(
    "https://public:private@example.com:9443/prefix/42/",
  )
  assert_eq(target.endpoint, "https://example.com:9443/prefix/api/42/envelope/")
  assert_true(target.auth.contains("sentry_key=public"))
  assert_true(target.auth.contains("sentry_secret=private"))
}

///|
async test "report configuration metadata environment and secrets stay strings during offline validation" {
  let dir = @fs.tmpdir(prefix="mooncdc-report-config-")
  defer @fs.rmdir(dir, recursive=true)
  @fs.write_file(dir + "/config.json", fixture().stringify())
  let config = load_config(Some(dir + "/config.json"), {
    "APP_SENTRY__DSN": "http://00123@127.0.0.1:1/42",
    "APP_SUPABASE__PROJECT_REF": "project",
    "APP_SUPABASE__API_URL": "http://127.0.0.1:1/",
    "APP_SUPABASE__API_KEY": "true",
    "APP_ENVIRONMENT": "dev",
    "APP_VERSION": "test-version",
    "APP_SOURCE__PASSWORD": "source-secret",
    "APP_DESTINATION__PASSWORD": "target-secret",
  })
  config.validate()
  assert_eq(config.reports.environment, "dev")
  assert_eq(config.reports.version, Some("test-version"))
  assert_eq(config.reports.project, Some("project"))
  assert_eq(config.reports.notification.unwrap().api_key, "true")
  let reports = config.active_reports()
  let clean = reports.redact("source-secret target-secret 00123 true")
  assert_eq(clean, "[redacted] [redacted] [redacted] [redacted]")
}

///|
test "report envelopes use UTF8 byte lengths event identity and stable redacted notification keys" {
  let reports = parse_config(fixture()).active_reports()
  let message = "故障 é 😀\nsecond"
  let envelope = reports.envelope(message, Some(42U))
  let lines = envelope.split("\n").collect()
  assert_eq(lines.length(), 4)
  guard @json.parse(lines[0]) is { "event_id": String(id), .. } else {
    fail("event id")
  }
  assert_eq(id.length(), 32)
  assert_true(
    id.iter().all(c => (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')),
  )
  assert_eq(@json.parse(lines[1]), {
    "type": "event",
    "length": Json::Json(@utf8.encode(lines[2].to_owned()).length()),
  })
  guard @json.parse(lines[2])
    is {
      "event_id": String(event_id),
      "tags": { "table_id": "42", .. },
      "exception": { "values": [{ "value": String(details), .. }], .. },
      ..
    } else {
    fail("event payload")
  }
  assert_eq(event_id, id)
  assert_eq(details, message)
  let body = @json.parse(reports.notification_body(message))
  assert_eq(body, @json.parse(reports.notification_body(message)))
  guard body
    is {
      "dedup_key": String(key),
      "fields": { "notification_details": String(details), .. },
      ..
    } else {
    fail("notification payload")
  }
  assert_true(key.has_prefix("pipeline-error:test:"))
  assert_eq(key.length(), 84)
  assert_eq(details, message)
  assert_true(reports.redact("x".repeat(20000)).has_suffix(" [truncated]"))
}

///|
async test "controlled HTTP reports preserve endpoint headers and do not propagate status or response failures" {
  @async.with_timeout(15000, () => {
    for
      (code, reply) in [
        (200, "{\"message\":\"test\",\"deduplicated\":false}"),
        (200, "{}"),
        (429, "RAW-SECRET"),
        (503, "RAW-SECRET"),
      ] {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let port = server.addr.port()
      let config = parse_config(
        merge_config(fixture(), {
          "sentry": { "dsn": Json::Json("http://public@127.0.0.1:\{port}/42") },
          "supabase": {
            "project_ref": "project",
            "api_url": Json::Json("http://127.0.0.1:\{port}"),
            "api_key": "fake-key",
          },
        }),
      )
      let mut requests = 0
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          for _ in 0..<2 {
            let (tcp, _) = server.accept()
            let conn = @http.ServerConnection::new(tcp)
            defer conn.close()
            let request = conn.read_request()
            let body = conn.read_all().text()
            assert_eq(request.meth, @http.Post)
            if requests == 0 {
              assert_eq(request.path, "/api/42/envelope/")
              assert_true(
                request.headers["X-Sentry-Auth"].contains("sentry_key=public"),
              )
              assert_eq(
                request.headers["Content-Type"],
                "application/x-sentry-envelope",
              )
              assert_true(body.contains("ReplicationError"))
            } else {
              assert_eq(request.path, "/system/replication/project/emails")
              assert_eq(request.headers["apikey"], "fake-key")
              assert_eq(request.headers["Content-Type"], "application/json")
              assert_true(
                @json.parse(body)
                is { "template_alias": "etl-pipeline-notification", .. },
              )
            }
            requests += 1
            conn.send_response(code, "Test")
            conn.write(reply)
            conn.end_response()
          }
        })
        config
        .active_reports()
        .report("controlled error", Some(42U), timeout_ms=2000)
        assert_eq(requests, 2)
      })
    }
  })
}

///|
async test "report transport timeout response limits and cancellation join owned connections" {
  @async.with_timeout(5000, () => {
    for oversized in [false, true] {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let config = parse_config(
        merge_config(fixture(), {
          "supabase": {
            "project_ref": "project",
            "api_url": Json::Json("http://127.0.0.1:\{server.addr.port()}"),
            "api_key": "fake-key",
          },
        }),
      )
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          let (tcp, _) = server.accept()
          let conn = @http.ServerConnection::new(tcp)
          defer conn.close()
          ignore(conn.read_request())
          ignore(conn.read_all())
          if oversized {
            try {
              conn.send_response(200, "Test")
              conn.write("x".repeat(70000))
              conn.end_response()
            } catch {
              _ => ()
            }
          } else {
            @async.sleep(60000)
          }
        })
        if oversized {
          try
            report_post(
              config.reports.notification.unwrap().endpoint,
              "test",
              Map([]),
              1000,
            )
          catch {
            error =>
              assert_true(error.to_string().contains("report response limit"))
          } noraise {
            _ => fail("oversized response accepted")
          }
        } else {
          config.active_reports().report("error", None, timeout_ms=100)
        }
        group.return_immediately(())
      })
    }
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let mut accepted : @socket.Tcp? = None
    // Keep the peer open until the group cancels and joins report_post. A
    // closure-local defer would reset the socket before client cancellation.
    defer (if accepted is Some(tcp) { tcp.close() })
    @async.with_task_group(group => {
      group.spawn_bg(() => {
        ignore(
          report_post(
            "http://127.0.0.1:\{server.addr.port()}",
            "test",
            Map([]),
            60000,
          ),
        )
      })
      let (tcp, _) = server.accept()
      accepted = Some(tcp)
      group.return_immediately(())
    })
  })
}

///|
priv struct ReportingTestStore {
  mut owner : String?
  mut record : Bytes?
  mut reject_save : Bool
  mut reject_load : Bool
  mut saves : Int32
}

///|
impl @cdc.StateStore for ReportingTestStore with fn acquire(self, owner) {
  assert_true(self.owner is None)
  self.owner = Some(owner)
}

///|
impl @cdc.StateStore for ReportingTestStore with fn release(self, owner) {
  assert_eq(self.owner, Some(owner))
  self.owner = None
}

///|
impl @cdc.StateStore for ReportingTestStore with fn load(self, record) {
  if self.reject_load {
    fail("controlled state observation failure")
  }
  if record == @cdc.Pipeline {
    self.record
  } else {
    None
  }
}

///|
impl @cdc.StateStore for ReportingTestStore with fn save(
  self,
  owner,
  record,
  bytes,
) {
  assert_eq(self.owner, Some(owner))
  if self.reject_save {
    fail("controlled state save failure")
  }
  if record == @cdc.Pipeline {
    self.record = Some(bytes)
  }
  self.saves += 1
}

///|
fn reporting_state(phase : Json) -> Bytes {
  let state : Json = {
    "version": 1,
    "source": {
      "system_id": "1",
      "timeline": "1",
      "database": "test",
      "slot": "test_slot",
      "publication": "test_pub",
    },
    "destination_id": "test-target",
    "initial_lsn": "0/1",
    "checkpoint_initialized": true,
    "write_horizon": "0/1",
    "tables": [{ "oid": 42, "phase": phase }],
  }
  let body = @utf8.encode(state.stringify())
  @utf8.encode(@crypto.bytes_to_hex_string(@crypto.sha256(body)) + "\n") + body
}

///|
async test "reporting store reports new terminal states after persistence and suppresses timed retries unchanged errors and failed saves" {
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let reports = parse_config(
      merge_config(fixture(), {
        "supabase": {
          "project_ref": "project",
          "api_url": Json::Json("http://127.0.0.1:\{server.addr.port()}"),
          "api_key": "fake-key",
        },
      }),
    ).active_reports()
    let inner = ReportingTestStore::{
      owner: None,
      record: None,
      reject_save: false,
      reject_load: false,
      saves: 0,
    }
    let wrapper = ReportingStore::{
      inner,
      reports,
      known: Map([]),
      observation_ready: false,
    }
    let timed = reporting_state([
      "Errored",
      "timed",
      ToJson::to_json(@cdc.TimedRetry(100UL)),
      0,
    ])
    let manual = reporting_state([
      "Errored",
      "manual",
      ToJson::to_json(@cdc.ManualRetry),
      1,
    ])
    let permanent = reporting_state([
      "Errored",
      "permanent",
      ToJson::to_json(@cdc.NoRetry),
      0,
    ])
    let mut requests = 0
    @async.with_task_group(group => {
      group.spawn_bg(() => {
        for expected in [manual, permanent, permanent] {
          let (tcp, _) = server.accept()
          let conn = @http.ServerConnection::new(tcp)
          defer conn.close()
          ignore(conn.read_request())
          ignore(conn.read_all())
          assert_eq(inner.record, Some(expected))
          assert_true(
            @cdc.state_store_status(inner).unwrap().tables[0].phase
            is @cdc.Errored(retry~, ..) &&
            !(retry is @cdc.TimedRetry(_)),
          )
          requests += 1
          // Persisted state succeeds even when its notification fails.
          conn.send_response(503, "Controlled reporting failure")
          conn.end_response()
        }
      })
      @cdc.with_state_store(wrapper, _ => {
        let session : &@cdc.StateStore = wrapper
        let owner = inner.owner.unwrap()
        session.save(owner, @cdc.Pipeline, timed)
        assert_eq(requests, 0)
        inner.reject_save = true
        try session.save(owner, @cdc.Pipeline, manual) catch {
          error =>
            assert_true(
              error.to_string().contains("controlled state save failure"),
            )
        } noraise {
          _ => fail("save failure swallowed")
        }
        assert_eq(inner.record, Some(timed))
        assert_eq(requests, 0)
        inner.reject_save = false
        session.save(owner, @cdc.Pipeline, manual)
        assert_eq(requests, 1)
        session.save(owner, @cdc.Pipeline, manual)
        assert_eq(requests, 1)
        session.save(owner, @cdc.Pipeline, permanent)
        assert_eq(requests, 2)
        session.save(owner, @cdc.Pipeline, reporting_state("Pending"))
        session.save(owner, @cdc.Pipeline, permanent)
        assert_eq(requests, 3)
      })
    })
    assert_true(inner.owner is None)
    // A fresh ownership scope does not re-send a pre-existing terminal state.
    @cdc.with_state_store(wrapper, _ => {
      let session : &@cdc.StateStore = wrapper
      session.save(inner.owner.unwrap(), @cdc.Pipeline, permanent)
      assert_eq(requests, 3)
    })
    // Observation failure must not replace a successful state save.
    inner.reject_load = true
    @cdc.with_state_store(wrapper, _ => {
      let session : &@cdc.StateStore = wrapper
      session.save(inner.owner.unwrap(), @cdc.Pipeline, timed)
      assert_eq(inner.record, Some(timed))
    })
    assert_true(inner.owner is None)
    assert_eq(inner.saves, 8)
  })
}
