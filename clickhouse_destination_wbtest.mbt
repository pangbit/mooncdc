///|
test "ClickHouse old keys TOAST and identity safety" {
  let base = ddl_snapshot(100, 50)
  let columns : Array[SchemaColumn] = [
    {
      name: "id",
      type_oid: 23U,
      type_modifier: -1,
      ordinal: 1,
      nullable: false,
      primary_key_ordinal: Some(1),
      default_expression: None,
    },
    {
      name: "text",
      type_oid: 25U,
      type_modifier: -1,
      ordinal: 2,
      nullable: false,
      primary_key_ordinal: None,
      default_expression: None,
    },
    {
      name: "items",
      type_oid: 1007U,
      type_modifier: -1,
      ordinal: 3,
      nullable: true,
      primary_key_ordinal: None,
      default_expression: None,
    },
  ]
  let schema = ReplicatedSchema::new(
    { ..base, columns: ReadOnlyArray::from_array(columns), },
    replicated=[true, true, true],
    identity=[true, false, false],
  )
  assert_eq(ch_old_row(schema, Key([Text("7"), Null, Null])), [
    I32(7),
    String(""),
    Array([]),
  ])
  let rows = ch_update_rows(schema, Some(Key([Text("7"), Null, Null])), [
    Text("8"),
    Text("new"),
    Text("{1,NULL}"),
  ])
  assert_eq(rows.length(), 2)
  assert_eq(rows[0].0, "DELETE")
  assert_eq(rows[1].1[0], I32(8))
  assert_eq(
    ch_update_rows(schema, None, [Text("8"), Text("new"), Text("{}")]).length(),
    1,
  )
  assert_eq(
    ch_update_rows(
      schema,
      Some(Full([Text("8"), Text("large"), Text("{1}")])),
      [Text("8"), UnchangedToast, Text("{}")],
    )[0].1[1],
    String("large"),
  )
  assert_true(
    try {
      ignore(
        ch_update_rows(schema, None, [Text("8"), UnchangedToast, Text("{}")]),
      )
      false
    } catch {
      Manual(_) => true
      _ => false
    },
  )
  let alternative = ReplicatedSchema::new(
    schema.schema,
    replicated=schema.replicated,
    identity=[false, true, false],
  )
  assert_true(
    try {
      ignore(ch_old_row(alternative, Key([Null, Text("key"), Null])))
      false
    } catch {
      Manual(_) => true
      _ => false
    },
  )
  let full = ReplicatedSchema::new(
    schema.schema,
    replicated=schema.replicated,
    identity=[true, true, true],
  )
  assert_true(
    try {
      ignore(ch_update_rows(full, None, [Text("8"), Text("new"), Text("{}")]))
      false
    } catch {
      Manual(_) => true
      _ => false
    },
  )
  assert_true(
    ch_key_equal(
      Value::Text("NaN").decode_text(701U),
      Value::Text("NaN").decode_text(701U),
    ),
  )
  assert_true(
    ch_key_equal(
      Value::Text("{NaN,NULL}").decode_text(1022U),
      Value::Text("{NaN,NULL}").decode_text(1022U),
    ),
  )
}

///|
priv struct ChDefaultVector {
  oid : UInt
  default_input : String
  result : Json?
} derive(ToJson, @json.FromJson)

///|
test "ClickHouse defaults preserve literal boundaries" {
  assert_eq(ch_default_expression("('don''t'::text)", 25U), Some("'don\\'t'"))
  assert_eq(
    ch_default_expression("'42.10'::numeric(10,2)", 1700U),
    Some("'42.10'"),
  )
  assert_eq(ch_default_expression("42::text", 25U), Some("'42'"))
  assert_eq(
    ch_default_expression(
      "'2026-01-01 12:00:00+02'::timestamp with time zone", 1184U,
    ),
    Some("parseDateTime64BestEffort('2026-01-01 12:00:00+02', 6, 'UTC')"),
  )
  for
    expression in [
      "nextval('id')", "(1 + 2)::int", "'x'; DROP TABLE t", "E'\\n'", "1e4", "ARRAY[1]",
      "((1) + (2))", "'select data'", "'a'::text::varchar",
    ] {
    assert_eq(ch_default_expression(expression, 23U), None)
  }
}

///|
async fn ch_wait_rows(http : ChHttp, query : String, expected : String) -> Unit {
  for ;; {
    if http.query(query).trim() == expected {
      break
    }
    @async.sleep(20)
  }
}

///|
async test "ClickHouse pipeline live snapshot DML and state restart" {
  if @env.get_env_var("MOONCDC_CLICKHOUSE_LIVE") != Some("1") {
    return
  }
  guard live_config() is Some(config) else {
    fail("ClickHouse pipeline test requires MOONCDC_TEST_PORT")
  }
  @async.with_timeout(90000, () => {
    let sql = connect(config, 16777216, false)
    defer sql.close()
    let http = ChHttp::new(
      "http://127.0.0.1:18123", "cdc", "mooncdc-test-only", 10000,
    )
    let db = "mooncdc_pipeline_" + pipeline_nonce()
    ignore(http.query("CREATE DATABASE " + ch_quote(db, '"')))
    defer (ignore(http.query("DROP DATABASE " + ch_quote(db, '"'))) catch {
      _ => ()
    })
    ignore(
      sql.query(
        "CREATE SCHEMA cdc_ch; CREATE TABLE cdc_ch.items(id int PRIMARY KEY, payload text NOT NULL, label text DEFAULT 'initial', nums int[]); ALTER TABLE cdc_ch.items REPLICA IDENTITY FULL; INSERT INTO cdc_ch.items VALUES(1,repeat('x',20000),'initial',ARRAY[1,NULL]); CREATE PUBLICATION cdc_ch FOR TABLE cdc_ch.items",
      ),
    )
    defer ignore(
      sql.query("DROP PUBLICATION cdc_ch; DROP SCHEMA cdc_ch CASCADE"),
    )
    for engine in [ClickHouseEngine::ReplacingMergeTree, MergeTree] {
      let dir = @fs.tmpdir(prefix="mooncdc-clickhouse-")
      defer @fs.rmdir(dir, recursive=true)
      @fs.mkdir(dir + "/state")
      let state = FileStateStore::new(dir + "/state")
      defer (if state_store_status(state) is Some(status) {
        drop_slot(sql, status.slot)
      })
      // Reset source between engines; each uses a fresh main slot and state.
      ignore(
        sql.query(
          "ALTER TABLE cdc_ch.items REPLICA IDENTITY FULL; TRUNCATE cdc_ch.items; INSERT INTO cdc_ch.items VALUES(1,repeat('x',20000),'initial',ARRAY[1,NULL])",
        ),
      )
      if engine == MergeTree {
        ignore(
          sql.query(
            "CREATE TABLE cdc_ch.no_key(value text); ALTER TABLE cdc_ch.no_key REPLICA IDENTITY FULL; INSERT INTO cdc_ch.no_key VALUES('duplicate'),('duplicate'); ALTER PUBLICATION cdc_ch ADD TABLE cdc_ch.no_key",
          ),
        )
      }
      let table = ch_quote(db, '"') + ".cdc__ch_items"
      with_file_schema_store(
        dir + "/schemas",
        identity=db + ch_engine_name(engine),
        schemas => {
          let make_target = fn() -> ClickHouseDestination raise {
            ClickHouseDestination::new(
              endpoint="http://127.0.0.1:18123",
              user="cdc",
              password="mooncdc-test-only",
              database=db,
              publication="cdc_ch",
              schema_store=schemas,
              engine~,
              max_insert_bytes=64,
              timeout_ms=10000,
            )
          }
          for pass in 0..<3 {
            let target = make_target()
            let control = ApplyControl()
            @async.with_task_group(group => {
              let writer = group.spawn(() => {
                for ;; {
                  if target.running &&
                    state_store_status(state) is Some(status) &&
                    !status.tables.is_empty() &&
                    status.tables.all(t => t.phase is (Ready(_) | Catchup(_))) {
                    break
                  }
                  @async.sleep(20)
                }
                if pass == 0 {
                  ignore(
                    sql.query(
                      "BEGIN; UPDATE cdc_ch.items SET id=2,label='changed' WHERE id=1; UPDATE cdc_ch.items SET label='toast kept' WHERE id=2; DELETE FROM cdc_ch.items WHERE id=2; INSERT INTO cdc_ch.items VALUES(3,'done','last',ARRAY[2,NULL]); COMMIT; BEGIN; INSERT INTO cdc_ch.items VALUES(4,'rollback','bad',ARRAY[]::int[]); ROLLBACK",
                    ),
                  )
                  if engine == MergeTree {
                    ignore(
                      sql.query(
                        "BEGIN; UPDATE cdc_ch.no_key SET value='updated'; DELETE FROM cdc_ch.no_key; COMMIT",
                      ),
                    )
                    ch_wait_rows(
                      http,
                      "SELECT count() FROM " +
                      ch_quote(db, '"') +
                      ".cdc__ch_no__key",
                      "6",
                    )
                  }
                } else if pass == 1 {
                  assert_eq(
                    http
                    .query("SELECT count() FROM " + table + " WHERE id=3")
                    .trim()
                    .to_owned(),
                    "1",
                  )
                  ignore(
                    sql.query(
                      "ALTER TABLE cdc_ch.items REPLICA IDENTITY DEFAULT; BEGIN; UPDATE cdc_ch.items SET id=6 WHERE id=3; DELETE FROM cdc_ch.items WHERE id=6; INSERT INTO cdc_ch.items VALUES(5,'resume','last',ARRAY[]::int[]); COMMIT",
                    ),
                  )
                } else {
                  ignore(
                    sql.query(
                      "BEGIN; TRUNCATE cdc_ch.items; INSERT INTO cdc_ch.items VALUES(9,'after truncate','last',ARRAY[]::int[]); COMMIT",
                    ),
                  )
                }
                let marker = if pass == 0 {
                  "3"
                } else if pass == 1 {
                  "5"
                } else {
                  "9"
                }
                ch_wait_rows(
                  http,
                  "SELECT count() FROM " +
                  table +
                  " WHERE id=" +
                  marker +
                  (if engine == MergeTree {
                    " AND cdc_operation='INSERT'"
                  } else {
                    " AND _etl_deleted=0"
                  }),
                  "1",
                )
                control.stop()
              })
              run_pipeline(
                config,
                target,
                publication="cdc_ch",
                destination_id=db,
                state_store=state,
                schema_store=schemas,
                control~,
                copy_concurrency=1,
                batch_rows=1,
              )
              writer.wait()
            })
            if engine == ReplacingMergeTree {
              assert_eq(
                http
                .query(
                  "SELECT groupArray(id) FROM (SELECT id FROM " +
                  table +
                  "__current ORDER BY id)",
                )
                .trim()
                .to_owned(),
                if pass == 0 {
                  "[3]"
                } else if pass == 1 {
                  "[5]"
                } else {
                  "[9]"
                },
              )
            } else {
              assert_eq(
                http.query("SELECT count() FROM " + table).trim().to_owned(),
                if pass == 0 {
                  "6"
                } else if pass == 1 {
                  "10"
                } else {
                  "1"
                },
              )
              if pass < 2 {
                assert_eq(
                  http
                  .query(
                    "SELECT count() FROM " +
                    table +
                    " WHERE cdc_operation='UPDATE' AND length(payload)=20000",
                  )
                  .trim()
                  .to_owned(),
                  "2",
                )
              }
              if pass == 1 {
                assert_eq(
                  http
                  .query(
                    "SELECT count() FROM " +
                    table +
                    " WHERE id=6 AND cdc_operation='DELETE' AND payload='' AND label IS NULL AND empty(nums)",
                  )
                  .trim()
                  .to_owned(),
                  "1",
                )
              }
            }
            assert_eq(
              http
              .query("SELECT count() FROM " + table + " WHERE id=4")
              .trim()
              .to_owned(),
              "0",
            )
          }
          // External drift must leave the WAL checkpoint conservative. A fresh
          // destination restores metadata, rejects the drift, then replays after repair.
          let before = StateStore::load(state, ReplicationCheckpoint)
          ignore(
            http.query(
              "ALTER TABLE " + table + " RENAME COLUMN label TO broken",
            ),
          )
          let target = make_target()
          try
            @async.with_task_group(group => {
              group.spawn_bg(() => {
                while !target.running {
                  @async.sleep(10)
                }
                ignore(
                  sql.query(
                    "INSERT INTO cdc_ch.items VALUES(10,'after repair','last',ARRAY[]::int[])",
                  ),
                )
              })
              run_pipeline(
                config,
                target,
                publication="cdc_ch",
                destination_id=db,
                state_store=state,
                schema_store=schemas,
                control=ApplyControl(),
                copy_concurrency=1,
              )
            })
          catch {
            Manual(
              "ClickHouse physical column name, order or type differs from schema"
            ) => ()
            e => raise e
          } noraise {
            _ => fail("ClickHouse drift accepted")
          }
          assert_eq(StateStore::load(state, ReplicationCheckpoint), before)
          ignore(
            http.query(
              "ALTER TABLE " + table + " RENAME COLUMN broken TO label",
            ),
          )
          let target = make_target()
          let control = ApplyControl()
          @async.with_task_group(group => {
            let watcher = group.spawn(() => {
              ch_wait_rows(
                http,
                "SELECT count() FROM " + table + " WHERE id=10",
                "1",
              )
              control.stop()
            })
            run_pipeline(
              config,
              target,
              publication="cdc_ch",
              destination_id=db,
              state_store=state,
              schema_store=schemas,
              control~,
              copy_concurrency=1,
            )
            watcher.wait()
          })
          if engine == ReplacingMergeTree {
            assert_eq(
              http
              .query("SELECT count() FROM " + table + "__current")
              .trim()
              .to_owned(),
              "2",
            )
          } else {
            assert_eq(
              http.query("SELECT count() FROM " + table).trim().to_owned(),
              "2",
            )
            assert_eq(
              http
              .query(
                "SELECT count() FROM " +
                ch_quote(db, '"') +
                ".cdc__ch_no__key WHERE value='duplicate'",
              )
              .trim()
              .to_owned(),
              "2",
            )
          }
        },
      )
      let status = state_store_status(state).unwrap()
      drop_slot(sql, status.slot)
      if engine == ReplacingMergeTree {
        ignore(http.query("DROP VIEW " + table + "__current"))
      }
      ignore(http.query("DROP TABLE " + table))
    }
    println(
      "CLICKHOUSE: snapshot, PK tombstones, FULL TOAST, key-only deletes, no-PK history, rollback, truncate, restart and drift/repair checkpoint safety passed",
    )
  }) catch {
    Manual(message) | Retryable(message) => fail(message)
    e => raise e
  }
}

///|
async test "ClickHouse default expressions pinned differential" {
  guard @env.get_env_var("MOONCDC_CHDEFAULT_PHASE") is Some(phase) else {
    return
  }
  let file = @env.get_env_var("MOONCDC_CHDEFAULT_FILE").unwrap()
  if phase == "generate" {
    let vectors : Array[ChDefaultVector] = []
    let expressions = [
      "", "NULL", "true", "FALSE", "42", "-42.10", "+.5", "1.", "1e4", "'42.10'",
      "'true'", "'pending'", "'don''t'", "'path\\name'", "'2026-01-01'", "'12:00:00'",
      "'12:00:00+02'", "'2026-01-01 12:30:00+02'", "'1 day'", "'{\"x\":1}'", "'x::y'",
      "'select data'", "'550e8400-e29b-41d4-a716-446655440000'", "nextval('seq')",
      "now()", "ARRAY[1]", "1 + 2", "(1) + (2)", "'a' || 'b'", "E'\\n'", "'unclosed",
      "'a'; SELECT 1", "(1)::int::text", "((1)::int)", "('a'::text)", "1::int[]",
      "'a'::\"custom\"", "1::schema.numeric(10,2)",
    ]
    for
      oid in [
        16U, 18U, 19U, 20U, 21U, 23U, 25U, 26U, 700U, 701U, 790U, 1700U, 1082U, 1083U,
        1266U, 1114U, 1184U, 1186U, 114U, 3802U, 2950U, 17U, 1007U, 1042U, 1043U,
      ] {
      for expression in expressions {
        for
          input in [
            expression,
            "(" + expression + ")",
            expression + "::text",
            "((" + expression + "::text))",
          ] {
          vectors.push({ oid, default_input: input, result: None, })
        }
      }
    }
    @fs.write_file(file, ToJson::to_json(vectors).stringify())
    println("CHDEFAULT generated \{vectors.length()} vectors")
  } else {
    let vectors : Array[ChDefaultVector] = @json.from_json(
      @json.parse(@fs.read_file(file).text()),
    )
    for vector in vectors {
      let actual : Json = {
        "value": match ch_default_expression(vector.default_input, vector.oid) {
          Some(value) => value.to_json()
          None => Json::null()
        },
      }
      assert_eq(
        actual,
        vector.result.unwrap(),
        msg="default oid=\{vector.oid} expression=\{vector.default_input} actual=\{actual.stringify()} expected=\{vector.result.unwrap().stringify()}",
      )
    }
    println("CHDEFAULT matched \{vectors.length()} vectors")
  }
}
