///|
test "ClickHouse operation budgets floor server limits and reject overflow" {
  assert_eq(ch_timeout_seconds(0), "1")
  assert_eq(ch_timeout_seconds(999), "1")
  assert_eq(ch_timeout_seconds(1999), "1")
  assert_eq(ch_timeout_seconds(2000), "2")
  let http = ChHttp::new("http://127.0.0.1:1", "cdc", "secret", 260000)
  assert_eq(http.server_budget(Connectivity), 8000)
  assert_eq(http.server_budget(SchemaQuery), 16000)
  assert_eq(http.server_budget(Ddl), 128000)
  assert_eq(http.server_budget(InsertRequest), 256000)
  assert_true(http.query_settings(Ddl).contains("lock_acquire_timeout=128"))
  assert_false(
    http.query_settings(InsertRequest).contains("max_execution_time"),
  )
  for budget in [-1, 2147483647] {
    assert_true(
      try {
        ignore(
          ChHttp::new(
            "http://127.0.0.1:1",
            "cdc",
            "secret",
            260000,
            ddl_ms=budget,
          ),
        )
        false
      } catch {
        InvalidConfig(_) => true
        _ => false
      },
    )
  }
}

///|
async test "ClickHouse live server budgets and client deadline" {
  if @env.get_env_var("MOONCDC_CLICKHOUSE_LIVE") != Some("1") {
    return
  }
  let http = ChHttp::new(
    "http://127.0.0.1:18123", "cdc", "mooncdc-test-only", 260000,
  )
  for
    (operation, expected) in [
      (ChOperation::Connectivity, "8"),
      (SchemaQuery, "16"),
      (Ddl, "128"),
    ] {
    assert_eq(
      http
      .query("SELECT getSetting('max_execution_time')", operation~)
      .trim()
      .to_owned(),
      expected,
    )
  }
  assert_eq(
    http.query("SELECT getSetting('lock_acquire_timeout')").trim().to_owned(),
    "128",
  )
  assert_eq(
    http
    .query(
      "SELECT getSetting('http_send_timeout'),getSetting('http_receive_timeout'),getSetting('async_insert')",
      insert=true,
    )
    .trim()
    .to_owned(),
    "256\t256\tfalse",
  )
  let short = ChHttp::new(
    "http://127.0.0.1:18123",
    "cdc",
    "mooncdc-test-only",
    10000,
    schema_ms=500,
    epsilon_ms=4000,
  )
  assert_eq(
    short
    .query("SELECT getSetting('max_execution_time')", operation=SchemaQuery)
    .trim()
    .to_owned(),
    "1",
  )
  let deadline = ChHttp::new(
    "http://127.0.0.1:18123", "cdc", "mooncdc-test-only", 20,
  )
  assert_true(
    try {
      ignore(deadline.query("SELECT sleep(0.2)", operation=SchemaQuery))
      false
    } catch {
      @async.TimeoutError => true
      e => raise e
    },
  )
  assert_eq(
    http.query("SELECT 1", operation=Connectivity).trim().to_owned(),
    "1",
  )
}

///|
async test "ClickHouse live RowBinary names types and both engines" {
  if @env.get_env_var("MOONCDC_CLICKHOUSE_LIVE") != Some("1") {
    return
  }
  let http = ChHttp::new(
    "http://127.0.0.1:18123", "cdc", "mooncdc-test-only", 10000,
  )
  let db = "mooncdc_encoding_" +
    @crypto.bytes_to_hex_string(@env.rand(8).unwrap())
  ignore(http.query("CREATE DATABASE " + ch_quote(db, '"')))
  defer (ignore(http.query("DROP DATABASE " + ch_quote(db, '"'))) catch {
    _ => ()
  })
  let source : Array[(String, UInt, Bool)] = [
    ("id", 23U, false),
    ("uuid", 2950U, false),
    ("date", 1082U, false),
    ("ts", 1184U, false),
    ("json", 3802U, false),
    ("bytes", 17U, false),
    ("array", 1007U, true),
    ("nil", 25U, true),
    ("time", 1266U, false),
  ]
  let values : ReadOnlyArray[Cell] = [
    I32(1),
    Value::Text("550e8400-e29b-41d4-a716-446655440000").decode_text(2950U),
    Value::Text("1969-12-31").decode_text(1082U),
    Value::Text("2026-01-01 10:00:00.123456+02").decode_text(1184U),
    Value::Text("{\"z\":1e-400,\"a\":18446744073709551616}").decode_text(3802U),
    Value::Text("\\xdeadbeef").decode_text(17U),
    Value::Text("{1,NULL,-2}").decode_text(1007U),
    SqlNull,
    Value::Text("24:00:00+02").decode_text(1266U),
  ]
  for engine in [ClickHouseEngine::ReplacingMergeTree, MergeTree] {
    let name = if engine == ReplacingMergeTree { "state" } else { "log" }
    let table = ch_quote(db, '"') + "." + ch_quote(name, '"')
    let columns = source.map(c => (c.0, ch_type(c.1, c.2, false)))
    columns.append(ch_metadata_columns(engine))
    let layout = ChLayout::new(ReadOnlyArray::from_array(columns))
    let engine_sql = if engine == ReplacingMergeTree {
      "ReplacingMergeTree(_etl_version, _etl_deleted) ORDER BY id"
    } else {
      "MergeTree() ORDER BY tuple()"
    }
    ignore(
      http.query(
        "CREATE TABLE " +
        table +
        " (" +
        columns.map(c => ch_quote(c.0, '"') + " " + c.1).join(", ") +
        ") ENGINE=" +
        engine_sql +
        " SETTINGS non_replicated_deduplication_window=100",
      ),
    )
    let row = layout.row(values, engine, "INSERT", 0, 0)
    http.insert(table, layout, [row], 1, Some("snapshot-1"))
    http.insert(table, layout, [row], 1, Some("snapshot-1"))
    assert_eq(http.query("SELECT count() FROM " + table).trim().to_owned(), "1")
    // A distinct copy batch with identical contents must not be lost.
    http.insert(table, layout, [row], 1, Some("snapshot-2"))
    if engine == MergeTree {
      assert_eq(
        http.query("SELECT count() FROM " + table).trim().to_owned(),
        "2",
      )
    }
    let json = @json.parse(
      http.query(
        "SELECT id, toString(uuid) AS uuid, toString(date) AS date, toString(ts, 'UTC') AS ts, json, bytes, array, nil, time FROM " +
        table +
        " LIMIT 1 FORMAT JSONEachRow",
      ),
    )
    assert_eq(json, {
      "id": 1,
      "uuid": "550e8400-e29b-41d4-a716-446655440000",
      "date": "1969-12-31",
      "ts": "2026-01-01 08:00:00.123456",
      "json": "{\"a\":18446744073709551616,\"z\":1e-400}",
      "bytes": "deadbeef",
      "array": [1, null, -2],
      "nil": null,
      "time": "24:00:00+02",
    })
    let tombstone = layout.row(values, engine, "DELETE", 42, 3)
    http.insert(table, layout, [tombstone], 1024, None)
    if engine == ReplacingMergeTree {
      assert_eq(
        http
        .query("SELECT count() FROM " + table + " FINAL WHERE _etl_deleted=0")
        .trim()
        .to_owned(),
        "0",
      )
      assert_eq(
        http
        .query("SELECT toString(max(_etl_version)) FROM " + table)
        .trim()
        .to_owned(),
        "774763251095801167875",
      )
    } else {
      assert_eq(
        http
        .query(
          "SELECT cdc_operation FROM " +
          table +
          " WHERE cdc_lsn=42 AND cdc_tx_ordinal=3",
        )
        .trim()
        .to_owned(),
        "DELETE",
      )
    }
    let incompatible = ChLayout::new(
      ReadOnlyArray::from_array(
        columns.map(c => if c.0 == "id" { (c.0, "Int64") } else { c }),
      ),
    )
    assert_true(
      try {
        http.insert(table, incompatible, [row], 1024, None)
        false
      } catch {
        _ => true
      },
    )
    ignore(http.query("TRUNCATE TABLE " + table))
    assert_eq(http.query("SELECT count() FROM " + table).trim().to_owned(), "0")
  }
}
