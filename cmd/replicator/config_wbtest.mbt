///|
test "core CLI rejects unpublished targets before reading their credentials" {
  for kind in ["bigquery", "snowflake", "iceberg", "ducklake", "unknown"] {
    try
      parse_config(
        merge_config(fixture(), {
          "destination": { "type": kind, "private_key": "do-not-read" },
        }),
      )
    catch {
      ConfigError(message) => {
        assert_true(message.contains("only clickhouse"))
        assert_false(message.contains("do-not-read"))
      }
      error => raise error
    } noraise {
      _ => fail("core accepted an unpublished destination")
    }
  }
}

///|
fn fixture() -> Json raise {
  let source =
    #|{"id":"test","publication":"pub","source":{"host":"127.0.0.1","port":1,"user":"cdc","security":"local_plaintext"},"destination":{"type":"clickhouse","url":"http://127.0.0.1:1","user":"cdc","database":"test"}}
  @json.parse(source)
}

///|
async test "metrics configuration defaults enables disables and rejects collisions offline" {
  assert_eq(parse_config(fixture()).metrics_port, Some(9000))
  for setting in [Json::null(), { "enabled": false }] {
    let configured = parse_config(
      merge_config(fixture(), { "metrics": setting }),
    )
    assert_true(configured.metrics_port is None)
    configured.validate()
  }
  let configured = parse_config(
    merge_config(fixture(), { "metrics": { "port": "19000" } }),
  )
  configured.validate()
  assert_eq(configured.metrics_port, Some(19000))
  let invalids : Array[Json] = [
    { "metrics": { "port": 0 } },
    { "metrics": { "port": 65536 } },
    { "metrics": { "port": 9001 }, "health": { "port": 9001 } },
    { "metrics": { "enabled": "no" } },
    { "metrics": { "password": "secret" } },
    { "metrics": false },
  ]
  for invalid in invalids {
    try parse_config(merge_config(fixture(), invalid)).validate() catch {
      ConfigError(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid metrics configuration accepted")
    }
  }
  let dir = @fs.tmpdir(prefix="mooncdc-metrics-config-")
  defer @fs.rmdir(dir, recursive=true)
  @fs.write_file(dir + "/config.yaml", fixture().stringify())
  let configured = load_config(Some(dir + "/config.yaml"), {
    "APP_METRICS__PORT": "19100",
  })
  assert_eq(configured.metrics_port, Some(19100))
  configured.validate()
  let disabled = load_config(Some(dir + "/config.yaml"), {
    "APP_METRICS__ENABLED": "false",
  })
  assert_true(disabled.metrics_port is None)
  disabled.validate()
}

///|
async test "memory configuration supports null disable numeric strings and strict hysteresis" {
  let disabled = parse_config(
    merge_config(fixture(), { "memory_backpressure": Json::null() }),
  )
  assert_true(disabled.memory_backpressure is None)
  disabled.validate()
  let configured = parse_config(
    merge_config(fixture(), {
      "memory_refresh_interval_ms": "25",
      "memory_backpressure": {
        "activate_threshold": "9e-1",
        "resume_threshold": "0.7",
      },
    }),
  )
  configured.validate()
  guard configured.memory_backpressure is Some(memory) else {
    fail("configured memory missing")
  }
  assert_eq(memory.activate_threshold, 0.9F)
  assert_eq(memory.resume_threshold, 0.7F)
  assert_eq(memory.refresh_interval_ms, 25)
  for
    invalid in [
      "{\"memory_backpressure\":{}}", "{\"memory_backpressure\":{\"activate_threshold\":0.8}}",
      "{\"memory_backpressure\":{\"activate_threshold\":0.8,\"resume_threshold\":0.8}}",
      "{\"memory_backpressure\":{\"activate_threshold\":0.8,\"resume_threshold\":-0.1}}",
      "{\"memory_backpressure\":{\"activate_threshold\":\"NaN\",\"resume_threshold\":0.7}}",
      "{\"memory_backpressure\":{\"activate_threshold\":\"0.8_5\",\"resume_threshold\":0.7}}",
      "{\"memory_backpressure\":{\"activate_threshold\":true,\"resume_threshold\":0.7}}",
      "{\"memory_backpressure\":false}", "{\"memory_backpressure\":null,\"memory_refresh_interval_ms\":0}",
    ] {
    try
      parse_config(merge_config(fixture(), @json.parse(invalid))).validate()
    catch {
      _ => ()
    } noraise {
      _ => fail("invalid memory configuration accepted")
    }
  }
  let dir = @fs.tmpdir(prefix="mooncdc-memory-config-")
  defer @fs.rmdir(dir, recursive=true)
  @fs.write_file(dir + "/config.yaml", fixture().stringify())
  let environment = load_config(Some(dir + "/config.yaml"), {
    "APP_MEMORY_REFRESH_INTERVAL_MS": "37",
    "APP_MEMORY_BACKPRESSURE__ACTIVATE_THRESHOLD": "0.9",
    "APP_MEMORY_BACKPRESSURE__RESUME_THRESHOLD": "0.6",
  })
  environment.validate()
  guard environment.memory_backpressure is Some(memory) else {
    fail("environment memory missing")
  }
  assert_eq(memory.refresh_interval_ms, 37)
  assert_eq(memory.activate_threshold, 0.9F)
  assert_eq(memory.resume_threshold, 0.6F)
}

///|
async test "configuration validation is offline and rejects invalid library options" {
  let config = parse_config(fixture())
  config.validate()
  assert_eq(config.retries, 5)
  assert_eq(config.copy_concurrency, 4)
  assert_eq(config.copy_connections_per_table, 4)
  assert_eq(config.copy_monitor_interval_ms, 10000)
  assert_true(config.batch is Some(_))
  assert_true(config.invalidated_slot_behavior is @cdc.RejectInvalidatedSlot)
  assert_true(config.source_migrations)
  assert_false(config.slot_failover)
  guard config.memory_backpressure is Some(memory) else {
    fail("default memory pressure missing")
  }
  assert_eq(memory.activate_threshold, 0.85F)
  assert_eq(memory.resume_threshold, 0.75F)
  assert_eq(memory.refresh_interval_ms, 100)
  assert_true(config.health is None)
  assert_eq(config.metrics_port, Some(9000))
  assert_true(config.copy_policy is @cdc.IncludeAllTables)
  for
    override_ in [
      "{\"max_table_retries\":0}", "{\"batch_rows\":0}", "{\"copy_concurrency\":0}",
      "{\"copy_connections_per_table\":0}", "{\"copy_connections_per_table\":257}",
      "{\"copy_concurrency\":257}", "{\"table_retry_delay_ms\":999}", "{\"table_retry_delay_ms\":86400001}",
      "{\"table_sync_monitor_refresh_interval_ms\":0}", "{\"copy_concurrency\":16,\"copy_connections_per_table\":17}",
      "{\"source\":{\"port\":65536}}", "{\"state\":{\"connection\":{\"host\":\"127.0.0.1\",\"port\":0,\"user\":\"cdc\"}}}",
      "{\"destination\":{\"url\":\"invalid\"}}", "{\"table_sync_copy\":{\"type\":\"include_tables\",\"table_ids\":[1,1]}}",
    ] {
    try
      parse_config(merge_config(fixture(), @json.parse(override_))).validate()
    catch {
      _ => ()
    } noraise {
      _ => fail("invalid configuration accepted: " + override_)
    }
  }
}

///|
test "configuration rejects unsupported and malformed options" {
  for
    override_ in [
      "{\"destination\":{\"type\":\"bigquery\"}}", "{\"telemetry\":{}}", "{\"source\":{\"timeout_ms\":1.5}}",
      "{\"invalidated_slot_behavior\":\"skip\"}", "{\"invalidated_slot_behavior\":true}",
      "{\"replication_slot\":{\"failover\":1}}", "{\"replication_slot\":{\"unknown\":false}}",
      "{\"source\":{\"password\":true}}", "{\"health\":{\"port\":0}}", "{\"id\":\"\"}",
      "{\"table_sync_copy\":{\"type\":\"skip_all_tables\",\"table_ids\":[]}}", "{\"table_sync_copy\":{\"type\":\"include_tables\",\"table_ids\":[4294967296]}}",
    ] {
    try parse_config(merge_config(fixture(), @json.parse(override_))) catch {
      ConfigError(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid configuration accepted: " + override_)
    }
  }
}

///|
async test "configuration layers preserve secret strings and select YAML before JSON" {
  let dir = @fs.tmpdir(prefix="mooncdc-config-")
  defer @fs.rmdir(dir, recursive=true)
  @fs.write_file(dir + "/base.json", fixture().stringify())
  @fs.write_file(
    dir + "/prod.json",
    "{\"batch_rows\":5,\"destination\":{\"database\":\"layered\"}}",
  )
  let vars = {
    "APP_CONFIG_DIR": dir,
    "APP_BATCH_ROWS": "7",
    "APP_COPY_CONNECTIONS_PER_TABLE": "3",
    "APP_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS": "23",
    "APP_INVALIDATED_SLOT_BEHAVIOR": "recreate",
    "APP_SOURCE__PASSWORD": "true",
    "APP_DESTINATION__PASSWORD": "00123",
    "APP_RUN_SOURCE_MIGRATIONS": "false",
    "APP_REPLICATION_SLOT__FAILOVER": "true",
    "APP_SOURCE__HOSTADDR": "127.0.0.1",
    "APP_SOURCE__KEEPALIVE__IDLE_SECS": "7",
  }
  let config = load_config(None, vars)
  assert_eq(config.batch_rows, 7)
  assert_eq(config.copy_connections_per_table, 3)
  assert_eq(config.copy_monitor_interval_ms, 23)
  assert_true(config.invalidated_slot_behavior is @cdc.RecreateInvalidatedSlot)
  assert_eq(config.database, "layered")
  assert_eq(config.source.password, "true")
  assert_eq(config.password, "00123")
  assert_false(config.source_migrations)
  assert_true(config.slot_failover)
  assert_eq(config.source.hostaddr, Some("127.0.0.1"))
  assert_eq(config.source.keepalive.idle_secs, 7)
  assert_eq(config.source.keepalive.interval_secs, 10)
  assert_eq(load_config(Some(dir + "/base.json"), vars).database, "test")
  @fs.write_file(dir + "/base.yml", fixture().stringify())
  @fs.write_file(dir + "/base.yaml", fixture().stringify())
  @fs.write_file(dir + "/prod.yml", "destination:\n  database: yml\n")
  assert_eq(load_config(None, vars).database, "yml")
  @fs.write_file(dir + "/prod.yaml", "destination:\n  database: yaml\n")
  assert_eq(load_config(None, vars).database, "yaml")
  assert_eq(load_config(Some(dir + "/base.yml"), vars).database, "test")
  @fs.write_file(dir + "/broken.json", "{\"password\": SUPER_SECRET")
  try load_config(Some(dir + "/broken.json"), Map([])) catch {
    ConfigError(message) =>
      assert_eq(message, "configuration file is not valid JSON")
    error => raise error
  } noraise {
    _ => fail("invalid JSON accepted")
  }
}

///|
async test "YAML aliases sequences strings and invalid input are handled without exposing values" {
  let dir = @fs.tmpdir(prefix="mooncdc-yaml-")
  defer @fs.rmdir(dir, recursive=true)
  let path = dir + "/config.yaml"
  let source =
    #|id: yaml
    #|publication: pub
    #|source: &source
    #|  host: 127.0.0.1
    #|  user: cdc
    #|  password: '00123'
    #|  security: local_plaintext
    #|state:
    #|  connection: *source
    #|destination:
    #|  type: clickhouse
    #|  url: http://127.0.0.1:1
    #|  user: cdc
    #|  database: yaml
    #|table_sync_copy:
    #|  type: include_tables
    #|  table_ids: [1, 0x2]
  @fs.write_file(path, source)
  let config = load_config(Some(path), Map([]))
  assert_eq(config.state.password, "00123")
  assert_true(config.copy_policy is @cdc.IncludeTables([1, 2]))
  for
    source in [
      "password: [SECRET", "password: SECRET\npassword: OTHER", "a: 1\n---\na: SECRET",
    ] {
    @fs.write_file(path, source)
    try load_config(Some(path), Map([])) catch {
      ConfigError(message) =>
        assert_eq(
          message, "configuration file is not valid YAML or exceeds parsing limits",
        )
      e => raise e
    } noraise {
      _ => fail("invalid YAML accepted")
    }
  }
}

///|
async test "cancellation requests stop and waits for protected work and cleanup" {
  let control = @cdc.ApplyControl()
  let mut started = false
  let mut drained = false
  @async.with_task_group(group => {
    let task = group.spawn(() => {
      drain_on_cancel(control, () => {
        started = true
        while control.health().status != "stopping" {
          @async.sleep(1)
        }
        @async.sleep(20)
        drained = true
      })
    })
    while !started {
      @async.sleep(1)
    }
    task.cancel()
    task.wait() catch {
      @async.WaitedTaskAlreadyCancelled => ()
      error => raise error
    }
  })
  assert_true(drained)
}
