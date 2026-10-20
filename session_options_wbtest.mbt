///|
test "worker session names preserve role pipeline and maximum table OID" {
  let (apply, _) = Replication.settings(worker=ApplyWorker("abc"))
  assert_eq(apply, "mooncdc_replication:apply:ba7816bf8f01cfea")
  let (table, _) = SnapshotExporter.settings(
    worker=TableWorker("abc", 4294967295U),
  )
  assert_eq(
    table, "mooncdc_snapshot_export:table_sync:ba7816bf8f01cfea:4294967295",
  )
  assert_true(@utf8.encode(table).length() <= 63)
  let (other, _) = Replication.settings(worker=ApplyWorker("other"))
  assert_false(apply == other)
  assert_eq(Replication.settings().0, "mooncdc_replication")
}

///|
async test "live session workloads override server defaults and preserve snapshot lifetime" {
  guard live_config() is Some(base) else { return }
  @async.with_timeout(90000, () => {
    let admin = connect(base, 16777216, false)
    defer admin.close()
    let name = "session_" + pipeline_nonce()
    ignore(admin.query("CREATE DATABASE \{name}"))
    defer ignore(admin.query("DROP DATABASE \{name}"))
    ignore(
      admin.query(
        "ALTER DATABASE \{name} SET statement_timeout='50ms'; ALTER DATABASE \{name} SET lock_timeout='50ms'; ALTER DATABASE \{name} SET idle_in_transaction_session_timeout='50ms'",
      ),
    )
    let config = { ..base, database: name, }
    for
      (workload, replication, expected) in [
        (
          Queries,
          false,
          [Some("30s"), Some("10s"), Some("1min"), Some("mooncdc_queries")],
        ),
        (
          StateStore,
          false,
          [Some("30s"), Some("10s"), Some("1min"), Some("mooncdc_state")],
        ),
        (
          SourceMigrations,
          false,
          [
            Some("30s"),
            Some("10s"),
            Some("1min"),
            Some("mooncdc_source_migrations"),
          ],
        ),
        (
          Replication,
          true,
          [Some("0"), Some("30s"), Some("0"), Some("mooncdc_replication")],
        ),
        (
          SnapshotReader,
          false,
          [Some("0"), Some("30s"), Some("0"), Some("mooncdc_snapshot")],
        ),
        (
          SnapshotExporter,
          true,
          [Some("0"), Some("0"), Some("0"), Some("mooncdc_snapshot_export")],
        ),
      ] {
      let conn = connect(config, 16777216, replication, workload~)
      defer conn.close()
      assert_eq(
        conn.query(
          "SELECT current_setting('statement_timeout'),current_setting('lock_timeout'),current_setting('idle_in_transaction_session_timeout'),current_setting('application_name')",
        ),
        [expected],
      )
      ignore(conn.query("SELECT pg_sleep(0.15)"))
      if !replication {
        ignore(conn.query("BEGIN"))
        @async.sleep(150)
        ignore(conn.query("COMMIT"))
      }
    }
    let sql = connect(config, 16777216, false)
    defer sql.close()
    ignore(
      sql.query(
        "CREATE TABLE items(id integer PRIMARY KEY); INSERT INTO items VALUES(1); CREATE PUBLICATION items FOR TABLE items",
      ),
    )
    let mut rows = 0
    let slot = name + "_snapshot"
    defer ignore(
      sql.query(
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='\{slot}'",
      ),
    )
    ignore(
      copy_snapshot(config, slot~, publication="items", event => {
        match event {
          TableBegin(_) => {
            // Observe the actual copy reader, not a separately configured probe.
            let active = sql.query(
              "SELECT current_query.state, current_query.xact_start IS NOT NULL FROM pg_stat_activity current_query WHERE datname=current_database() AND application_name='mooncdc_snapshot'",
            )
            assert_eq(active, [[Some("idle in transaction"), Some("t")]])
            // Exceed both the hostile 50ms server default and the bounded
            // query workload's 60s idle-transaction timeout.
            @async.sleep(61000)
          }
          TableRows(_, batch) => rows += batch.length()
          _ => ()
        }
      }),
    )
    assert_eq(rows, 1)
    // Stateful sessions inherit the bounded profile before acquiring a lease.
    let store = PostgresStateStore::new(config, store_id="session-test")
    store.acquire("owner")
    defer store.release("owner")
    assert_eq(
      sql.query(
        "SELECT application_name FROM pg_stat_activity WHERE datname=current_database() AND application_name='mooncdc_state'",
      ),
      [[Some("mooncdc_state")]],
    )
    println(
      "LIVE: six server timeout profiles, hostile database defaults, snapshot idle lifetime and state-session identity passed",
    )
  })
}
