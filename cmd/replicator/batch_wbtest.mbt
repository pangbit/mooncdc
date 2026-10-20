///|
async test "CLI batches default on and retain explicit null compatibility" {
  let legacy = parse_config(fixture())
  assert_true(legacy.batch is Some(_))
  assert_eq(legacy.memory_refresh_interval_ms, 100)
  legacy.validate()
  let disabled = parse_config(
    merge_config(fixture(), { "batch": Json::null() }),
  )
  assert_true(disabled.batch is None)
  disabled.validate()
  let enabled = parse_config(merge_config(fixture(), { "batch": {} }))
  guard enabled.batch is Some(batch) else { fail("batch object ignored") }
  let defaults = @cdc.BatchConfig::new()
  assert_eq(batch.memory_budget_ratio, defaults.memory_budget_ratio)
  assert_eq(batch.max_bytes, defaults.max_bytes)
  assert_eq(batch.max_fill_ms, defaults.max_fill_ms)
  enabled.validate()
}

///|
test "CLI batch bytes preserve exact UInt64 integers and reject lossy inputs" {
  for
    (text, expected) in [
      ("1", 1UL),
      ("9007199254740993", 9007199254740993UL),
      ("9223372036854775808", 9223372036854775808UL),
      ("18446744073709551615", 0xffffffffffffffffUL),
    ] {
    assert_eq(batch_bytes(@json.parse(text)), expected)
    assert_eq(batch_bytes(Json::string(text)), expected)
  }
  // YAML scalars without exact integer repr must fit the safe Double range.
  assert_eq(batch_bytes(Json::number(1048576.0)), 1048576UL)
  for
    value in [
      Json::number(9007199254740992.0),
      Json::number(-1.0),
      Json::number(1.5),
      Json::number(1.0 / 0.0),
      Json::number(0.0 / 0.0),
      @json.parse("18446744073709551616"),
      @json.parse("9007199254740993.0"),
      Json::string("18446744073709551616"),
      Json::string("-1"),
      Json::string("+1"),
      Json::string("1.5"),
      Json::string("1e6"),
      Json::string(""),
      Json::string(" 1"),
      Json::boolean(true),
      Json::null(),
    ] {
    try batch_bytes(value) catch {
      ConfigError(message) =>
        assert_eq(message, "batch.max_bytes: expected unsigned decimal integer")
      error => raise error
    } noraise {
      _ => fail("lossy batch byte input accepted")
    }
  }
}

///|
async test "CLI YAML batches and nested environment overrides share configuration" {
  let dir = @fs.tmpdir(prefix="mooncdc-cli-batch-")
  defer @fs.rmdir(dir, recursive=true)
  let path = dir + "/config.yaml"
  let source =
    #|id: batch
    #|publication: pub
    #|source: {host: 127.0.0.1, port: 1, user: test, security: local_plaintext}
    #|destination: {type: clickhouse, url: 'http://127.0.0.1:1', user: test, database: test}
    #|memory_backpressure: null
    #|memory_refresh_interval_ms: 23
    #|batch:
    #|  memory_budget_ratio: 0.5
    #|  max_bytes: '18446744073709551615'
    #|  max_fill_ms: 0
  @fs.write_file(path, source)
  let yaml = load_config(Some(path), Map([]))
  yaml.validate()
  assert_true(yaml.memory_backpressure is None)
  assert_eq(yaml.memory_refresh_interval_ms, 23)
  guard yaml.batch is Some(batch) else { fail("YAML batch ignored") }
  assert_eq(batch.memory_budget_ratio, 0.5F)
  assert_eq(batch.max_bytes, 0xffffffffffffffffUL)
  assert_eq(batch.max_fill_ms, 0)
  let override_ = load_config(Some(path), {
    "APP_BATCH__MEMORY_BUDGET_RATIO": "2.5e-1",
    "APP_BATCH__MAX_BYTES": "9007199254740993",
    "APP_BATCH__MAX_FILL_MS": "17",
    "APP_MEMORY_REFRESH_INTERVAL_MS": "29",
  })
  override_.validate()
  guard override_.batch is Some(batch) else {
    fail("environment batch ignored")
  }
  assert_eq(batch.memory_budget_ratio, 0.25F)
  assert_eq(batch.max_bytes, 9007199254740993UL)
  assert_eq(batch.max_fill_ms, 17)
  assert_eq(override_.memory_refresh_interval_ms, 29)
  @fs.write_file(path, fixture().stringify())
  let from_environment = load_config(Some(path), {
    "APP_BATCH__MAX_FILL_MS": "0",
  })
  from_environment.validate()
  guard from_environment.batch is Some(batch) else {
    fail("nested environment override did not enable batch")
  }
  assert_eq(batch.max_bytes, @cdc.BatchConfig::new().max_bytes)
  assert_eq(batch.max_fill_ms, 0)
}

///|
async test "CLI batch and refresh arguments reach stopped pipeline validation" {
  for
    invalid in [
      "{\"batch\":{\"memory_budget_ratio\":0}}", "{\"batch\":{\"memory_budget_ratio\":1.1}}",
      "{\"batch\":{\"max_bytes\":0}}", "{\"batch\":{\"max_fill_ms\":-1}}",
    ] {
    let config = parse_config(merge_config(fixture(), @json.parse(invalid)))
    try config.validate() catch {
      @cdc.InvalidConfig(message) =>
        assert_eq(message, "invalid batch memory budget or fill interval")
      error => raise error
    } noraise {
      _ => fail("batch was not passed into pipeline validation")
    }
  }
  let config = parse_config(
    merge_config(fixture(), { "batch": {}, "memory_backpressure": Json::null() }),
  )
  let invalid = { ..config, memory_refresh_interval_ms: 0, }
  try invalid.validate() catch {
    @cdc.InvalidConfig(message) =>
      assert_eq(message, "memory refresh interval must be positive")
    error => raise error
  } noraise {
    _ => fail("refresh interval was lost when pressure was disabled")
  }
}

///|
async test "CLI batch configuration rejects malformed fields without echoing values" {
  for
    invalid in [
      "{\"batch\":true}", "{\"batch\":[]}", "{\"batch\":\"DO_NOT_ECHO\"}", "{\"batch\":{\"unknown\":1}}",
      "{\"batch\":{\"memory_budget_ratio\":null}}", "{\"batch\":{\"memory_budget_ratio\":\"DO_NOT_ECHO\"}}",
      "{\"batch\":{\"memory_budget_ratio\":\"1e999\"}}", "{\"batch\":{\"max_bytes\":\"DO_NOT_ECHO\"}}",
      "{\"batch\":{\"max_fill_ms\":\"DO_NOT_ECHO\"}}", "{\"batch\":{\"max_fill_ms\":1.5}}",
      "{\"batch\":{\"max_fill_ms\":2147483648}}",
    ] {
    try
      parse_config(merge_config(fixture(), @json.parse(invalid))).validate()
    catch {
      ConfigError(message) => assert_false(message.contains("DO_NOT_ECHO"))
      error => raise error
    } noraise {
      _ => fail("malformed batch input accepted")
    }
  }
}
