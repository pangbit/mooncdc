///|
// A fresh process per invocation; orchestration is in migration-crash-test.mbtx.
async test "source migration process worker" {
  guard @env.get_env_var("MOONCDC_MIGRATION_DATABASE") is Some(database) else {
    return
  }
  guard live_config() is Some(base) else { fail("fixture port required") }
  let config = { ..base, database, }
  run_source_migrations(config)
  let sql = connect(config, 16777216, false)
  defer sql.close()
  for migration in source_migrations() {
    let checksum = @crypto.bytes_to_hex_string(
      @crypto.sha384(@utf8.encode(migration.sql)),
    )
    assert_eq(
      sql.query(
        "SELECT encode(checksum,'hex'),success::text FROM etl._sqlx_migrations WHERE version=\{migration.version}",
      ),
      [[Some(checksum), Some("true")]],
    )
  }
  assert_eq(
    cell(sql.query("SELECT count(*)::text FROM etl._sqlx_migrations"), 0, 0),
    "2",
  )
  println("MIGRATION worker completed")
}
