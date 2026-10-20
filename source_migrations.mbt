///|
priv struct SourceMigration {
  version : String
  description : String
  sql : String
}

///|
fn source_migrations() -> Array[SourceMigration] {
  [
    {
      version: "20260415100000",
      description: "schema change messages",
      sql: source_migration_sql_0,
    },
    {
      version: "20260724120000",
      description: "publication schema change messages",
      sql: source_migration_sql_1,
    },
  ]
}

///|
// SQLx 0.9.0 postgres migrator: CRC_32_ISO_HDLC(database) * 0x3d32ad9e.
fn migration_lock_id(database : String) -> Int64 {
  let mut crc = 0xffffffffU
  for byte in @utf8.encode(database) {
    crc = crc ^ byte.to_uint()
    for _ in 0..<8 {
      crc = (crc >> 1) ^ (if (crc & 1U) == 1U { 0xedb88320U } else { 0U })
    }
  }
  (crc ^ 0xffffffffU).to_int64() * 0x3d32ad9eL
}

///|
/// Install the pinned ETL source helpers and DDL event trigger. Requires an
/// appropriately privileged primary connection. Physical standbys are skipped.
/// Shares SQLx's database advisory lock, history schema and SHA-384 checksums;
/// unrelated successful history versions are preserved. Each pending migration
/// and its success record commit together. This does not install store tables.
/// Use `run_source_migrations(config)` before acquiring state stores in schema
/// `etl`, or `run_pipeline(..., source_migrations=true)` for library startup.
pub async fn run_source_migrations(
  connection : ConnectionConfig,
  control? : ApplyControl,
) -> Unit {
  validate_connection(connection)
  let conn = connect(
    connection,
    16777216,
    false,
    workload=SourceMigrations,
    metrics?=control.map(control => control.metrics_data),
  )
  defer conn.close()
  let query = async fn(sql : String) -> Array[Array[String?]] {
    @async.with_timeout(30000, () => conn.query(sql))
  }
  if cell(query("SELECT pg_is_in_recovery()::text"), 0, 0) == "true" {
    return
  }
  ignore(query("SET client_min_messages=warning; SET synchronous_commit=on"))
  let database = cell(query("SELECT current_database()"), 0, 0)
  ignore(query("SELECT pg_advisory_lock(\{migration_lock_id(database)})"))
  // Closing this connection releases the same session lock on every exit path.
  ignore(
    query(
      "CREATE SCHEMA IF NOT EXISTS etl; SET search_path=etl; CREATE TABLE IF NOT EXISTS etl._sqlx_migrations(version BIGINT PRIMARY KEY,description TEXT NOT NULL,installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),success BOOLEAN NOT NULL,checksum BYTEA NOT NULL,execution_time BIGINT NOT NULL)",
    ),
  )
  if !query(
      "SELECT version FROM etl._sqlx_migrations WHERE success=false LIMIT 1",
    ).is_empty() {
    raise UnsafeResume("dirty source migration history")
  }
  for migration in source_migrations() {
    let checksum = @crypto.bytes_to_hex_string(
      @crypto.sha384(@utf8.encode(migration.sql)),
    )
    let applied = query(
      "SELECT encode(checksum,'hex') FROM etl._sqlx_migrations WHERE version=\{migration.version}",
    )
    if !applied.is_empty() {
      if cell(applied, 0, 0) != checksum {
        raise UnsafeResume(
          "source migration checksum mismatch: " + migration.version,
        )
      }
      continue
    }
    let start = @bench.monotonic_clock_start()
    ignore(
      query(
        "BEGIN;\n" +
        migration.sql +
        "\nINSERT INTO etl._sqlx_migrations(version,description,success,checksum,execution_time) VALUES(\{migration.version},\{sql_literal(migration.description)},true,decode('\{checksum}','hex'),-1); COMMIT;",
      ),
    )
    let elapsed_ns = @bench.monotonic_clock_end(start) * 1000
    ignore(
      query(
        "UPDATE etl._sqlx_migrations SET execution_time=\{elapsed_ns} WHERE version=\{migration.version}",
      ),
    )
  }
}
