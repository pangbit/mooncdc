///|
priv struct CopyIudGate {
  mut held : Bool
  mut release : Bool
  mut admissions : Int32
  mut inserts : Int32
  mut snapshots : Int32
  mut resets : Int32
}

///|
fn copy_iud_metric(text : String, name : String) -> UInt64 raise {
  let mut total = 0UL
  for line in text.split("\n") {
    if line.has_prefix(name + "{") || line.has_prefix(name + " ") {
      total += @string.parse_uint64(line.split(" ").last().unwrap())
    }
  }
  total
}

///|
async fn copy_iud_copying(store : &StateStore, oid : UInt32) -> String {
  let status = state_store_status(store).unwrap()
  assert_eq(status.tables.length(), 1)
  assert_eq(status.tables[0].oid, oid)
  assert_true(status.tables[0].phase is Copying)
  status.slot
}

///|
// Inspect the intercepted RowBinary request against the three known snapshot
// rows. A nonempty payload, snapshot token, and zero WAL metadata are required.
fn copy_iud_snapshot_request(
  body : Bytes,
  engine : ClickHouseEngine,
) -> Unit raise {
  let newline = body.find(b"\n").unwrap()
  let sql = @utf8.decode(body[:newline].to_owned())
  assert_true(sql.contains(" SETTINGS insert_deduplication_token="))
  assert_true(sql.has_suffix(" FORMAT RowBinaryWithNamesAndTypes"))
  let columns = [("id", "Int32"), ("value", "String")]
  columns.append(ch_metadata_columns(engine))
  let layout = ChLayout::new(ReadOnlyArray::from_array(columns))
  let payload = body[newline + 1:].to_owned()
  let mut matched = false
  for (id, value) in [(1, "one"), (2, "two"), (3, "three")] {
    let expected = layout.header +
      layout.row([I32(id), String(value)], engine, "INSERT", 0, 0)
    matched = matched || payload == expected
  }
  assert_true(
    matched,
    msg="gate must intercept exactly one initial snapshot row",
  )
}

///|
// Exact history is checked independently of the current-state result. Distinct
// intermediate values identify both updates to id=1, so final-value equality
// cannot hide a missing intermediate transaction.
async fn copy_iud_history(
  http : ChHttp,
  table : String,
  current_view : String,
  engine : ClickHouseEngine,
  intervals : Array[(UInt64, UInt64)],
) -> (String, String) {
  let (operation, lsn, ordinal) = if engine == ReplacingMergeTree {
    (
      "if(_etl_deleted=1,'DELETE','WRITE')", "toUInt64(bitShiftRight(_etl_version,64))",
      "toUInt64(bitAnd(_etl_version,toUInt128('18446744073709551615')))",
    )
  } else {
    ("cdc_operation", "cdc_lsn", "cdc_tx_ordinal")
  }
  let result = http.query(
    "SELECT toString(id),value," +
    operation +
    ",toString(" +
    lsn +
    "),toString(" +
    ordinal +
    ") FROM " +
    table +
    " ORDER BY id,value," +
    lsn +
    " FORMAT JSONCompactEachRow",
  )
  let expected = [
    ("1", "one", "INSERT", -1),
    ("2", "two", "INSERT", -1),
    ("3", "three", "INSERT", -1),
    ("4", "four", "INSERT", 0),
    ("1", "one-mid", "UPDATE", 1),
    ("1", "one-final", "UPDATE", 2),
    ("2", "two", "DELETE", 3),
    ("4", "four-final", "UPDATE", 4),
  ]
  let seen : Map[Int32, Bool] = Map([])
  let identities : Map[String, Bool] = Map([])
  for line in result.split("\n").filter(line => !line.is_empty()) {
    guard @json.parse(line)
      is [
        String(id),
        String(value),
        String(op),
        String(lsn_text),
        String(ordinal_text),
      ] else {
      fail("unexpected COPY/IUD history row")
    }
    let actual_lsn = @string.parse_uint64(lsn_text)
    let actual_ordinal = @string.parse_uint64(ordinal_text)
    let index = expected
      .search_by(row => {
        let expected_op = if engine == ReplacingMergeTree && row.2 != "DELETE" {
          "WRITE"
        } else {
          row.2
        }
        row.0 == id && row.1 == value && expected_op == op
      })
      .unwrap()
    assert_false(seen.contains(index), msg="duplicate historical row")
    seen[index] = true
    let transaction = expected[index].3
    if transaction < 0 {
      assert_eq(actual_lsn, 0UL)
      assert_eq(actual_ordinal, 0UL)
    } else {
      let (before, after) = intervals[transaction]
      assert_true(
        before < actual_lsn && actual_lsn < after,
        msg="event commit LSN outside its source transaction interval",
      )
      // Transaction::event_id counts the single row event from zero;
      // ch_event_ordinal adds one for ETL's BEGIN ordinal.
      assert_eq(actual_ordinal, 1UL)
      let identity = lsn_text + "/" + ordinal_text
      assert_false(
        identities.contains(identity),
        msg="duplicate WAL event identity",
      )
      identities[identity] = true
    }
  }
  assert_eq(seen.length(), 8)
  assert_eq(identities.length(), 5)
  let current_query = if engine == ReplacingMergeTree {
    "SELECT toString(id),value FROM " +
    current_view +
    " ORDER BY id FORMAT JSONCompactEachRow"
  } else {
    "SELECT toString(id),tupleElement(latest,1) FROM (SELECT id,argMax(tuple(value,cdc_operation),tuple(cdc_lsn,cdc_tx_ordinal)) AS latest FROM " +
    table +
    " GROUP BY id) WHERE tupleElement(latest,2)!='DELETE' ORDER BY id FORMAT JSONCompactEachRow"
  }
  let current = http.query(current_query)
  let current_rows : Array[Json] = []
  for line in current.trim().split("\n") {
    current_rows.push(@json.parse(line))
  }
  let expected_current : Array[Json] = [
    ["1", "one-final"],
    ["3", "three"],
    ["4", "four-final"],
  ]
  assert_eq(current_rows, expected_current)
  (result, current)
}

///|
async fn copy_iud_case(
  cli : Bool,
  engine : ClickHouseEngine,
  case_name : String,
) -> Unit {
  if @env.get_env_var("MOONCDC_COPY_IUD_LIVE") != Some("1") {
    return
  }
  guard live_config() is Some(base) else {
    fail("COPY/IUD requires explicit MOONCDC_TEST_PORT")
  }
  guard @env.get_env_var("MOONCDC_CLICKHOUSE_PORT") is Some(ch_port) else {
    fail("COPY/IUD requires explicit MOONCDC_CLICKHOUSE_PORT")
  }
  guard @env.get_env_var("MOONCDC_COPY_IUD_CLI") is Some(executable) else {
    fail("COPY/IUD requires explicit MOONCDC_COPY_IUD_CLI")
  }
  guard @env.get_env_var("MOONCDC_COPY_IUD_EVIDENCE") is Some(evidence_root) else {
    fail("COPY/IUD requires explicit MOONCDC_COPY_IUD_EVIDENCE")
  }
  let evidence = evidence_root + "/" + case_name
  @fs.mkdir(evidence)
  let config = { ..base, timeout_ms: 30000, }
  let endpoint = clickhouse_test_endpoint(Some(ch_port))
  @async.with_timeout(90000, () => {
    let sql = connect(config, 16777216, false)
    defer sql.close()
    let version = @string.parse_int(
      cell(sql.query("SHOW server_version_num"), 0, 0),
    )
    assert_true(
      version >= 180000 && version < 190000,
      msg="COPY/IUD fixture requires PostgreSQL 18",
    )
    let name = "copyiud" + pipeline_nonce()
    let qualified_source = sql_identifier(name) + ".items"
    let store = PostgresStateStore::new(
      config,
      store_id=name + "/state",
      schema=name,
    )
    ignore(sql.query("CREATE SCHEMA " + sql_identifier(name)))
    defer @async.protect_from_cancel(() => {
      @async.with_timeout(15000, () => {
        // A cancelled DML query can leave its connection between protocol
        // messages. Cleanup owns a fresh connection instead of reusing it.
        let cleanup = connect(config, 16777216, false)
        defer cleanup.close()
        if state_store_status(store) is Some(status) {
          let prefix = copy_slot_prefix(status.slot)
          let slots = cleanup.query(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name=" +
            sql_literal(status.slot) +
            " OR left(slot_name," +
            prefix.length().to_string() +
            ")=" +
            sql_literal(prefix),
          )
          for row in slots {
            drop_slot(cleanup, row[0].unwrap())
          }
        }
        ignore(
          cleanup.query(
            "DROP PUBLICATION IF EXISTS " +
            sql_identifier(name) +
            "; DROP SCHEMA " +
            sql_identifier(name) +
            " CASCADE",
          ),
        )
        @fs.write_file(
          evidence + "/postgres-cleanup.json",
          Json::object({ "owned_schema": Json::Json(name), "removed": true }).stringify(),
        )
      })
    })
    ignore(
      sql.query(
        "CREATE TABLE " +
        qualified_source +
        "(id int PRIMARY KEY,value text NOT NULL); ALTER TABLE " +
        qualified_source +
        " REPLICA IDENTITY FULL; INSERT INTO " +
        qualified_source +
        " VALUES(1,'one'),(2,'two'),(3,'three'); CREATE PUBLICATION " +
        sql_identifier(name) +
        " FOR TABLE " +
        qualified_source,
      ),
    )
    let oid = @string.parse_uint(
      cell(
        sql.query(
          "SELECT " + sql_literal(name + ".items") + "::regclass::oid::text",
        ),
        0,
        0,
      ),
    )
    let http = ChHttp::new(endpoint, "cdc", "mooncdc-test-only", 30000)
    assert_eq(
      http
      .query(
        "SELECT count() FROM system.table_engines WHERE name IN ('ReplacingMergeTree','MergeTree')",
      )
      .trim()
      .to_owned(),
      "2",
    )
    @fs.write_file(
      evidence + "/versions.json",
      Json::object({
        "postgres_version_num": Json::Json(version),
        "clickhouse_version": Json::Json(
          http.query("SELECT version()").trim().to_owned(),
        ),
      }).stringify(),
    )
    ignore(http.query("CREATE DATABASE " + ch_quote(name, '"')))
    defer @async.protect_from_cancel(() => {
      ignore(http.query("DROP DATABASE " + ch_quote(name, '"')))
      @fs.write_file(
        evidence + "/clickhouse-cleanup.json",
        Json::object({ "owned_database": Json::Json(name), "removed": true }).stringify(),
      )
    })
    let table_name = ch_table_name(name, "items", engine)
    let table = ch_quote(name, '"') + "." + ch_quote(table_name, '"')
    let current = ch_quote(name, '"') +
      "." +
      ch_quote(table_name + "__current", '"')
    let temporary = @fs.tmpdir(prefix="mooncdc-copy-iud-")
    defer @async.protect_from_cancel(() => @fs.rmdir(temporary, recursive=true))
    let proxy = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer proxy.close()
    let gate = CopyIudGate::{
      held: false,
      release: false,
      admissions: 0,
      inserts: 0,
      snapshots: 0,
      resets: 0,
    }
    // Always release an admitted request before scoped children are cancelled.
    defer {
      gate.release = true
    }
    let proxy_endpoint = "http://127.0.0.1:" + proxy.addr.port().to_string()
    let exercise = async fn(
      metrics : async () -> String,
      alive : () -> Unit raise,
    ) -> Unit {
      while !gate.held {
        alive()
        @async.sleep(5)
      }
      assert_eq(gate.admissions, 1)
      assert_eq(gate.inserts, 1)
      assert_eq(gate.snapshots, 1)
      ignore(copy_iud_copying(store, oid))
      let before_metrics = metrics()
      assert_eq(
        copy_iud_metric(
          before_metrics, "mooncdc_copy_partitions_completed_total",
        ),
        0UL,
      )
      assert_eq(
        copy_iud_metric(before_metrics, "mooncdc_table_copies_completed_total"),
        0UL,
      )
      assert_eq(
        copy_iud_metric(before_metrics, "mooncdc_copy_rows_completed_total"),
        0UL,
      )
      assert_eq(
        copy_iud_metric(before_metrics, "mooncdc_copy_rows_received_total"),
        1UL,
      )
      assert_eq(
        copy_iud_metric(before_metrics, "mooncdc_copy_rows_written_total"),
        0UL,
      )
      assert_eq(
        http.query("SELECT count() FROM " + table).trim().to_owned(),
        "0",
      )
      @fs.write_file(evidence + "/before.metrics", before_metrics)
      @fs.write_file(
        evidence + "/before.pipeline",
        store.load(Pipeline).unwrap(),
      )
      // Prevent physical replacement from erasing intermediate versions before
      // assertions. This applies only to this fixture's owned table.
      if engine == ReplacingMergeTree {
        ignore(http.query("SYSTEM STOP MERGES " + table))
      }
      let intervals : Array[(UInt64, UInt64)] = []
      let commits : Array[Json] = []
      let operations = [
        "INSERT INTO " + qualified_source + " VALUES(4,'four')",
        "UPDATE " + qualified_source + " SET value='one-mid' WHERE id=1",
        "UPDATE " + qualified_source + " SET value='one-final' WHERE id=1",
        "DELETE FROM " + qualified_source + " WHERE id=2",
        "UPDATE " + qualified_source + " SET value='four-final' WHERE id=4",
      ]
      for index, operation in operations {
        let before = sql.query(
          "SELECT pg_current_wal_insert_lsn()::text,clock_timestamp()::text",
        )
        ignore(sql.query("BEGIN; " + operation + "; COMMIT"))
        let after = sql.query(
          "SELECT pg_current_wal_insert_lsn()::text,clock_timestamp()::text",
        )
        let low = Lsn::parse(cell(before, 0, 0)).value
        let high = Lsn::parse(cell(after, 0, 0)).value
        assert_true(low < high)
        intervals.push((low, high))
        commits.push({
          "index": Json::Json(index),
          "sql": Json::Json(operation),
          "before_lsn": Json::Json(cell(before, 0, 0)),
          "after_lsn": Json::Json(cell(after, 0, 0)),
          "before_time": Json::Json(cell(before, 0, 1)),
          "after_time": Json::Json(cell(after, 0, 1)),
        })
        ignore(copy_iud_copying(store, oid))
        assert_false(gate.release)
      }
      let after_metrics = metrics()
      assert_eq(
        copy_iud_metric(
          after_metrics, "mooncdc_copy_partitions_completed_total",
        ),
        0UL,
      )
      assert_eq(
        copy_iud_metric(after_metrics, "mooncdc_table_copies_completed_total"),
        0UL,
      )
      assert_eq(
        copy_iud_metric(after_metrics, "mooncdc_copy_rows_completed_total"),
        0UL,
      )
      assert_eq(
        copy_iud_metric(after_metrics, "mooncdc_copy_rows_received_total"),
        1UL,
      )
      assert_eq(
        copy_iud_metric(after_metrics, "mooncdc_copy_rows_written_total"),
        0UL,
      )
      assert_eq(gate.inserts, 1)
      assert_eq(
        http.query("SELECT count() FROM " + table).trim().to_owned(),
        "0",
      )
      @fs.write_file(
        evidence + "/commits.json",
        Json::Json(commits).stringify(indent=2),
      )
      @fs.write_file(evidence + "/held-after-dml.metrics", after_metrics)
      @fs.write_file(
        evidence + "/held-after-dml.pipeline",
        store.load(Pipeline).unwrap(),
      )
      @fs.write_file(
        evidence + "/release.json",
        Json::object({
          "gate_admissions": Json::Json(gate.admissions),
          "committed_transactions": 5,
          "copy_completed": 0,
          "source_time": Json::Json(
            cell(sql.query("SELECT clock_timestamp()::text"), 0, 0),
          ),
        }).stringify(),
      )
      gate.release = true
      for ;; {
        alive()
        let count = @string.parse_int(
          http.query("SELECT count() FROM " + table).trim(),
        )
        assert_true(count <= 8, msg="extra snapshot/WAL historical rows")
        if count == 8 &&
          state_store_status(store) is Some(status) &&
          status.tables.length() == 1 &&
          status.tables[0].phase is Ready(_) {
          break
        }
        @async.sleep(10)
      }
      let final_metrics = metrics()
      assert_eq(
        copy_iud_metric(
          final_metrics, "mooncdc_copy_partitions_completed_total",
        ),
        1UL,
      )
      assert_eq(
        copy_iud_metric(final_metrics, "mooncdc_table_copies_completed_total"),
        1UL,
      )
      assert_eq(
        copy_iud_metric(final_metrics, "mooncdc_copy_rows_completed_total"),
        3UL,
      )
      assert_eq(
        copy_iud_metric(final_metrics, "mooncdc_worker_errors_total"),
        0UL,
      )
      assert_eq(gate.admissions, 1)
      assert_eq(gate.resets, 1)
      assert_eq(gate.snapshots, 3)
      assert_eq(gate.inserts, 8)
      let (history, current_rows) = copy_iud_history(
        http, table, current, engine, intervals,
      )
      @fs.write_file(evidence + "/history.jsonl", history)
      @fs.write_file(evidence + "/current.jsonl", current_rows)
      @fs.write_file(evidence + "/final.metrics", final_metrics)
      @fs.write_file(
        evidence + "/final.pipeline",
        store.load(Pipeline).unwrap(),
      )
    }
    @async.with_task_group(proxy_group => {
      proxy_group.spawn_bg(() => {
        proxy.run_forever(
          (tcp, _) => {
            let conn = @http.ServerConnection::new(tcp)
            defer conn.close()
            let request = conn.read_request()
            let body = conn.read_all()
            let bytes = body.binary()
            if bytes.has_prefix(
                @utf8.encode("DROP TABLE IF EXISTS " + table + "\n"),
              ) {
              gate.resets += 1
            }
            if bytes.has_prefix(@utf8.encode("INSERT INTO " + table + " (")) {
              gate.inserts += 1
              let newline = bytes.find(b"\n").unwrap()
              let statement = @utf8.decode(bytes[:newline].to_owned())
              if statement.contains(" SETTINGS insert_deduplication_token=") {
                gate.snapshots += 1
              }
              if gate.inserts == 1 {
                copy_iud_snapshot_request(bytes, engine)
                @fs.write_file(evidence + "/gated-snapshot-request.bin", bytes)
                gate.admissions += 1
                gate.held = true
                while !gate.release {
                  @async.sleep(5)
                }
              }
            }
            let headers = Map([])
            for key, value in request.headers {
              if key.to_string().to_lower().has_prefix("x-clickhouse-") {
                headers[key] = value
              }
            }
            let (response, result) = @http.request(
              endpoint + request.path,
              request.meth,
              headers,
              body,
            )
            let response_headers = Map([])
            for key, value in response.headers {
              if key.to_string().to_lower().has_prefix("x-clickhouse-") ||
                key.to_string().to_lower() == "content-type" {
                response_headers[key] = value
              }
            }
            conn.send_response(
              response.code,
              response.reason,
              extra_headers=response_headers,
            )
            conn.write(result)
            conn.end_response()
          },
          allow_failure=false,
        )
      })
      if cli {
        let reservation = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
        let metrics_port = reservation.addr.port()
        reservation.close()
        let cli_config : Json = {
          "id": Json::Json(name),
          "publication": Json::Json(name),
          "run_source_migrations": false,
          "source": {
            "host": "127.0.0.1",
            "port": Json::Json(config.port),
            "user": "cdc",
            "password": "mooncdc-test-only",
            "database": "postgres",
            "security": "local_plaintext",
          },
          "state": { "schema": Json::Json(name) },
          "destination": {
            "type": "clickhouse",
            "url": Json::Json(proxy_endpoint),
            "user": "cdc",
            "password": "mooncdc-test-only",
            "database": Json::Json(name),
            "engine": Json::Json(
              if engine == ReplacingMergeTree {
                "replacing_merge_tree"
              } else {
                "merge_tree"
              },
            ),
          },
          "metrics": {
            "port": Json::Json(metrics_port),
            "listen_address": "127.0.0.1",
          },
          "copy_concurrency": 1,
          "copy_connections_per_table": 1,
          "batch_rows": 1,
          "max_table_retries": 1,
        }
        let path = temporary + "/config.json"
        @fs.write_file(path, cli_config.stringify())
        let output = @process.redirect_to_file(evidence + "/cli.log")
        defer output.close()
        @async.with_task_group(group => {
          let child = @process.spawn(
            group,
            executable,
            ["--config", path],
            stdout=output,
            stderr=output,
            cancel_handler=@process.graceful_cancel(timeout=3000),
          )
          defer {
            gate.release = true
          }
          exercise(
            () => {
              let (response, body) = @http.get(
                "http://127.0.0.1:" + metrics_port.to_string() + "/metrics",
              )
              assert_eq(response.code, 200)
              body.text()
            },
            () => {
              assert_true(
                child.try_wait() is None,
                msg="CLI exited before COPY/IUD assertions",
              )
            },
          )
          let signal = @process.spawn(group, "kill", [
            "-TERM",
            child.pid.to_string(),
          ])
          assert_eq(signal.wait(), 0)
          assert_eq(@async.with_timeout(10000, () => child.wait()), 0)
        })
      } else {
        let control = ApplyControl()
        let schemas_store = PostgresStateStore::new(
          config,
          store_id=name + "/schemas",
          schema=name,
        )
        with_state_schema_store(schemas_store, identity=name, schemas => {
          let target = ClickHouseDestination::new(
            endpoint=proxy_endpoint,
            user="cdc",
            password="mooncdc-test-only",
            database=name,
            publication=name,
            schema_store=schemas,
            engine~,
          )
          @async.with_task_group(group => {
            let worker = group.spawn(() => {
              run_pipeline(
                config,
                target,
                publication=name,
                destination_id=name,
                state_store=store,
                schema_store=schemas,
                control~,
                copy_concurrency=1,
                copy_connections_per_table=1,
                batch_rows=1,
                max_table_retries=1,
              )
            })
            defer {
              gate.release = true
              control.stop()
            }
            exercise(() => control.prometheus(), () => ())
            control.stop()
            worker.wait()
          })
        })
      }
      proxy_group.return_immediately(())
    })
    if engine == ReplacingMergeTree {
      ignore(http.query("SYSTEM START MERGES " + table))
    }
    println(
      "LIVE: COPY_IUD " +
      case_name +
      " PASS snapshot=3 commits_while_copying=5 history=8 unique_wal_ids=5 gate=1 retries=0",
    )
  })
}

///|
async test "live COPY IUD library ReplacingMergeTree" {
  copy_iud_case(false, ReplacingMergeTree, "library-rmt")
}

///|
async test "live COPY IUD library MergeTree" {
  copy_iud_case(false, MergeTree, "library-mt")
}

///|
async test "live COPY IUD CLI ReplacingMergeTree" {
  copy_iud_case(true, ReplacingMergeTree, "cli-rmt")
}

///|
async test "live COPY IUD CLI MergeTree" {
  copy_iud_case(true, MergeTree, "cli-mt")
}
