///|
async test "physical standby skips source migrations before and after WAL replay" {
  guard @env.get_env_var("MOONCDC_STANDBY_PORT") is Some(port) else { return }
  guard live_config() is Some(primary) else {
    fail("primary fixture port required")
  }
  let standby = { ..primary, port: @string.parse_int(port), }
  @async.with_timeout(30000, () => {
    let writer = connect(primary, 16777216, false)
    defer writer.close()
    let reader = connect(standby, 16777216, false)
    defer reader.close()
    assert_eq(
      cell(writer.query("SELECT pg_is_in_recovery()::text"), 0, 0),
      "false",
    )
    assert_eq(
      cell(reader.query("SELECT pg_is_in_recovery()::text"), 0, 0),
      "true",
    )
    assert_eq(writer.query("SELECT to_regnamespace('etl')::text"), [[None]])
    assert_eq(reader.query("SELECT to_regnamespace('etl')::text"), [[None]])
    run_source_migrations(standby)
    assert_eq(reader.query("SELECT to_regnamespace('etl')::text"), [[None]])
    run_source_migrations(primary)
    let history_sql = "SELECT version::text,encode(checksum,'hex'),success::text,installed_on::text,execution_time::text FROM etl._sqlx_migrations ORDER BY version"
    let history = writer.query(history_sql)
    assert_eq(history.length(), 2)
    let target = cell(writer.query("SELECT pg_current_wal_lsn()::text"), 0, 0)
    while cell(
            reader.query(
              "SELECT (pg_last_wal_replay_lsn() >= '\{target}'::pg_lsn)::text",
            ),
            0,
            0,
          ) !=
          "true" {
      @async.sleep(20)
    }
    assert_eq(reader.query(history_sql), history)
    let definitions = "SELECT p.oid::text,p.proname,pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='etl' ORDER BY p.oid"
    let functions = reader.query(definitions)
    assert_true(!functions.is_empty())
    for _ in 0..<2 {
      run_source_migrations(standby)
    }
    assert_eq(reader.query(history_sql), history)
    assert_eq(reader.query(definitions), functions)
    assert_eq(writer.query(history_sql), history)
    assert_eq(cell(reader.query("SHOW transaction_read_only"), 0, 0), "on")
    println(
      "STANDBY: migration skip before and after primary WAL replay passed",
    )
  })
}
