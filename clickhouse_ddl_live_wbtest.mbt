///|
async test "ClickHouse pipeline live upstream DDL and publication projection" {
  if @env.get_env_var("MOONCDC_CLICKHOUSE_LIVE") != Some("1") {
    return
  }
  guard live_config() is Some(config) else { return }
  @async.with_timeout(120000, () => {
    let sql = connect(config, 16777216, false)
    defer sql.close()
    assert_true(
      sql.query("SELECT oid FROM pg_namespace WHERE nspname='etl'").is_empty(),
    )
    ignore(sql.query("CREATE SCHEMA etl"))
    defer ignore(
      sql.query(
        "DROP EVENT TRIGGER IF EXISTS supabase_etl_ddl_message_trigger; DROP SCHEMA etl CASCADE",
      ),
    )
    ignore(
      sql.query(
        @fs.read_file("integration/reference/etl/schema-change-messages.sql").text(),
      ),
    )
    ignore(
      sql.query(
        @fs.read_file(
          "integration/reference/etl/publication-schema-change-messages.sql",
        ).text(),
      ),
    )
    let http = ChHttp::new(
      "http://127.0.0.1:18123", "cdc", "mooncdc-test-only", 10000,
    )
    let db = "mooncdc_ddl_pipeline_" + pipeline_nonce()
    ignore(http.query("CREATE DATABASE " + ch_quote(db, '"')))
    defer (ignore(http.query("DROP DATABASE " + ch_quote(db, '"'))) catch {
      _ => ()
    })
    for engine in [ClickHouseEngine::ReplacingMergeTree, MergeTree] {
      ignore(
        sql.query(
          "CREATE SCHEMA chddl; CREATE TABLE chddl.items(id int PRIMARY KEY, label varchar(20), value text NOT NULL); INSERT INTO chddl.items VALUES(1,'old','value'); CREATE PUBLICATION chddl FOR TABLE chddl.items",
        ),
      )
      defer ignore(
        sql.query("DROP PUBLICATION chddl; DROP SCHEMA chddl CASCADE"),
      )
      let dir = @fs.tmpdir(prefix="mooncdc-ddl-pipeline-")
      defer @fs.rmdir(dir, recursive=true)
      @fs.mkdir(dir + "/state")
      let state = FileStateStore::new(dir + "/state")
      defer (if state_store_status(state) is Some(status) {
        drop_slot(sql, status.slot)
      })
      with_file_schema_store(
        dir + "/schema",
        identity=db + ch_engine_name(engine),
        schemas => {
          let target = ClickHouseDestination::new(
            endpoint="http://127.0.0.1:18123",
            user="cdc",
            password="mooncdc-test-only",
            database=db,
            publication="chddl",
            schema_store=schemas,
            engine~,
            timeout_ms=10000,
          )
          let control = ApplyControl()
          let table = target.qualified("chddl_items")
          @async.with_task_group(group => {
            let writer = group.spawn(() => {
              for ;; {
                if state_store_status(state) is Some(status) &&
                  !status.tables.is_empty() &&
                  status.tables.all(t => t.phase is (Ready(_) | Catchup(_))) {
                  break
                }
                @async.sleep(20)
              }
              let steps = [
                "BEGIN; ALTER TABLE chddl.items ADD COLUMN extra text NOT NULL DEFAULT 'first'; INSERT INTO chddl.items(id,value) VALUES(2,'two'); COMMIT",
                "BEGIN; ALTER TABLE chddl.items ALTER COLUMN extra SET DEFAULT 'later'; INSERT INTO chddl.items(id,value) VALUES(3,'three'); COMMIT",
                "BEGIN; ALTER TABLE chddl.items RENAME COLUMN value TO caption; ALTER TABLE chddl.items DROP COLUMN label; INSERT INTO chddl.items(id,caption) VALUES(4,'four'); COMMIT",
                "BEGIN; ALTER TABLE chddl.items ALTER COLUMN caption DROP NOT NULL; INSERT INTO chddl.items(id,caption) VALUES(5,NULL); COMMIT",
                "BEGIN; UPDATE chddl.items SET caption='five' WHERE id=5; ALTER TABLE chddl.items ALTER COLUMN caption SET NOT NULL; INSERT INTO chddl.items(id,caption) VALUES(6,'six'); COMMIT",
                "BEGIN; ALTER PUBLICATION chddl SET TABLE chddl.items(id,extra); INSERT INTO chddl.items(id,caption) VALUES(7,'seven'); COMMIT",
                "BEGIN; ALTER PUBLICATION chddl SET TABLE chddl.items; INSERT INTO chddl.items(id,caption) VALUES(8,'eight'); COMMIT",
                "BEGIN; ALTER TABLE chddl.items RENAME TO renamed; INSERT INTO chddl.renamed(id,caption) VALUES(9,'nine'); COMMIT",
                "BEGIN; ALTER TABLE chddl.renamed ALTER COLUMN extra TYPE varchar(50); INSERT INTO chddl.renamed(id,caption) VALUES(10,'ten'); COMMIT",
              ]
              for i, statement in steps {
                ignore(sql.query(statement))
                ch_wait_rows(
                  http,
                  "SELECT count() FROM " +
                  table +
                  " WHERE id=" +
                  (i + 2).to_string(),
                  "1",
                )
                if i == 1 {
                  assert_eq(
                    http
                    .query("SELECT extra FROM " + table + " WHERE id=1")
                    .trim()
                    .to_owned(),
                    "first",
                  )
                  assert_eq(
                    http
                    .query("SELECT extra FROM " + table + " WHERE id=3")
                    .trim()
                    .to_owned(),
                    "later",
                  )
                }
                if i == 4 {
                  assert_true(
                    target
                    .read_columns("chddl_items")
                    .contains(("caption", "Nullable(String)")),
                  )
                }
                if i == 6 {
                  assert_eq(
                    http
                    .query(
                      "SELECT isNull(caption) FROM " + table + " WHERE id=1",
                    )
                    .trim()
                    .to_owned(),
                    "1",
                  )
                  assert_eq(
                    http
                    .query("SELECT caption FROM " + table + " WHERE id=8")
                    .trim()
                    .to_owned(),
                    "eight",
                  )
                }
              }
              control.stop()
            })
            run_pipeline(
              config,
              target,
              publication="chddl",
              destination_id=db,
              state_store=state,
              schema_store=schemas,
              control~,
              copy_concurrency=1,
            )
            writer.wait()
          })
          if engine == ReplacingMergeTree {
            assert_eq(
              http
              .query("SELECT count() FROM " + table + "__current")
              .trim()
              .to_owned(),
              "10",
            )
          } else {
            assert_eq(
              http.query("SELECT count() FROM " + table).trim().to_owned(),
              "11",
            )
          }
          // Reopen actual persisted state after a source table rename and schema changes.
          let resumed = ClickHouseDestination::new(
            endpoint="http://127.0.0.1:18123",
            user="cdc",
            password="mooncdc-test-only",
            database=db,
            publication="chddl",
            schema_store=schemas,
            engine~,
            timeout_ms=10000,
          )
          let stop = ApplyControl()
          @async.with_task_group(group => {
            let writer = group.spawn(() => {
              while !resumed.running {
                @async.sleep(20)
              }
              ignore(
                sql.query(
                  "INSERT INTO chddl.renamed(id,caption) VALUES(11,'resumed')",
                ),
              )
              ch_wait_rows(
                http,
                "SELECT count() FROM " + table + " WHERE id=11",
                "1",
              )
              stop.stop()
            })
            run_pipeline(
              config,
              resumed,
              publication="chddl",
              destination_id=db,
              state_store=state,
              schema_store=schemas,
              control=stop,
            )
            writer.wait()
          })
        },
      )
    }
    println(
      "CLICKHOUSE DDL: upstream PG triggers, ordered changes, defaults, projection, source rename and durable restart passed",
    )
  })
}
