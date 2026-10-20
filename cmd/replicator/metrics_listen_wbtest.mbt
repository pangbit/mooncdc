///|
test "metrics listen config preserves defaults and forwards explicit literals offline" {
  let defaults = parse_config(fixture())
  assert_eq(defaults.metrics_settings(), Some((9000, None)))
  for host in ["127.0.0.1", "::1"] {
    let config = parse_config(
      merge_config(fixture(), {
        "metrics": { "port": 19000, "listen_address": host },
      }),
    )
    assert_eq(config.metrics_settings(), Some((19000, Some(host))))
  }
  let disabled = parse_config(
    merge_config(fixture(), {
      "metrics": { "enabled": false, "listen_address": "127.0.0.1" },
    }),
  )
  assert_true(disabled.metrics_settings() is None)
  assert_true(
    parse_config(merge_config(fixture(), { "metrics": Json::null() })).metrics_settings()
    is None,
  )
}

///|
test "metrics listen config rejects invalid values even when disabled offline" {
  let bad : Array[Json] = [
    "",
    "localhost",
    "256.0.0.1",
    "127.0.0.1:9000",
    "[::1]",
    "::gg",
    "fe80::1%1",
    true,
    1,
    Json::null(),
  ]
  for value in bad {
    for enabled in [true, false] {
      try
        parse_config(
          merge_config(fixture(), {
            "metrics": { "enabled": enabled, "listen_address": value },
          }),
        )
      catch {
        ConfigError(message) =>
          assert_true(message.has_prefix("metrics.listen_address:"))
        error => raise error
      } noraise {
        _ => fail("invalid metrics listen address accepted")
      }
    }
  }
}

///|
async test "metrics listen JSON YAML and environment selection is offline" {
  let dir = @fs.tmpdir(prefix="mooncdc-metrics-listen-")
  defer @fs.rmdir(dir, recursive=true)
  @fs.write_file(
    dir + "/config.json",
    merge_config(fixture(), {
      "metrics": { "port": 19000, "listen_address": "127.0.0.1" },
    }).stringify(),
  )
  assert_eq(
    load_config(Some(dir + "/config.json"), Map([])).metrics_settings(),
    Some((19000, Some("127.0.0.1"))),
  )
  assert_eq(
    load_config(Some(dir + "/config.json"), {
      "APP_METRICS__LISTEN_ADDRESS": "::1",
    }).metrics_settings(),
    Some((19000, Some("::1"))),
  )
  let yaml =
    #|id: test
    #|publication: pub
    #|source: {host: 127.0.0.1, port: 1, user: cdc, security: local_plaintext}
    #|destination: {type: clickhouse, url: 'http://127.0.0.1:1', user: cdc, database: test}
    #|metrics: {port: 19001, listen_address: "::1"}
  @fs.write_file(dir + "/config.yaml", yaml)
  assert_eq(
    load_config(Some(dir + "/config.yaml"), Map([])).metrics_settings(),
    Some((19001, Some("::1"))),
  )
}
