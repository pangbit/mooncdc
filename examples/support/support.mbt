///|
pub fn service() -> String {
  @env.get_env_var("MOONCDC_EXAMPLE_SERVICE").unwrap_or("pg18")
}

///|
pub fn config() -> @cdc.ConnectionConfig {
  {
    host: "127.0.0.1",
    port: if service() == "pg17" {
      55417
    } else {
      55418
    },
    user: "cdc",
    password: "mooncdc-test-only",
    database: "postgres",
    security: @cdc.LocalPlaintext,
    timeout_ms: 5000,
  }
}

///|
/// SQL is used only to prepare the repository's isolated Docker examples.
pub async fn sql(query : String) -> String {
  let result = @shell.Cmd("docker", [
    "compose",
    "-p",
    "mooncdc-test",
    "-f",
    "integration/compose.yaml",
    "exec",
    "-T",
    service(),
    "psql",
    "-U",
    "cdc",
    "-d",
    "postgres",
    "-v",
    "ON_ERROR_STOP=1",
    "-At",
    "-c",
    query,
  ]).output()
  result.check()
  result.stdout().trim().to_owned()
}

///|
pub async fn prepare(name : String, seed? : String = "") -> @cdc.Lsn {
  ignore(
    sql(
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='\{name}'",
    ),
  )
  ignore(
    sql(
      "DROP PUBLICATION IF EXISTS \{name}; DROP TABLE IF EXISTS \{name}; CREATE TABLE \{name} (id integer PRIMARY KEY, value text); ALTER TABLE \{name} REPLICA IDENTITY FULL; CREATE PUBLICATION \{name} FOR TABLE \{name}; \{seed}",
    ),
  )
  @cdc.Lsn::parse(
    sql(
      "SELECT lsn::text FROM pg_create_logical_replication_slot('\{name}', 'pgoutput')",
    ),
  )
}

///|
pub async fn cleanup(name : String) -> Unit {
  ignore(sql("SELECT pg_drop_replication_slot('\{name}')"))
  ignore(sql("DROP PUBLICATION \{name}; DROP TABLE \{name}"))
}

///|
pub fn key(row : ReadOnlyArray[@cdc.Value]) -> String raise {
  guard row.get(0) is Some(@cdc.Text(key)) else {
    fail("replica identity does not provide a usable id")
  }
  key
}
