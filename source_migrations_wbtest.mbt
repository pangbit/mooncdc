///|
async test "embedded source migrations preserve exact upstream bytes and SHA384" {
  let checksums = [
    "8f3ca7fc2c72d4d7beaf08f0d9df7c56f13f4d7ecfee61653820224d8b1b106a62117070898986bc5abc6dd17e38c6cb",
    "7436b4f425d25866dc15a6a1b37ed9fc4596d11a7ff813bac4e60e807b50c1d48223dac3be4fb50ccb7b1172a546752b",
  ]
  let files = ["schema-change-messages", "publication-schema-change-messages"]
  for i, migration in source_migrations() {
    assert_eq(
      migration.sql,
      @fs.read_file("integration/reference/etl/" + files[i] + ".sql").text(),
    )
    assert_eq(
      @crypto.bytes_to_hex_string(@crypto.sha384(@utf8.encode(migration.sql))),
      checksums[i],
    )
  }
  // The standard CRC-32 check vector, then SQLx's documented multiplier.
  assert_eq(migration_lock_id("123456789"), 0xcbf43926L * 0x3d32ad9eL)
}

///|
priv struct MigrationDestination {
  control : ApplyControl
}

///|
impl Destination for MigrationDestination with fn startup(self) {
  self.control.stop()
}

///|
impl Destination for MigrationDestination with fn reset_table(_, _, attempt~) {
  ignore(attempt)
  fail("unexpected migration test copy")
}

///|
impl Destination for MigrationDestination with fn write_rows(_, _, _, _) {
  fail("unexpected migration test rows")
}

///|
impl Destination for MigrationDestination with fn write_transaction(_, _) {
  fail("unexpected migration test WAL")
}

///|
impl Destination for MigrationDestination with fn flush(_) {
  ()
}

///|
async test "live source migrations upgrade validate lock rollback and pipeline startup" {
  if live_config() is Some(base) {
    @async.with_timeout(45000, () => {
      let control = ApplyControl()
      let admin = connect(base, 16777216, false)
      defer admin.close()
      let name = "migrations_" + pipeline_nonce()
      let role = name + "_role"
      ignore(
        admin.query("CREATE ROLE \{role} LOGIN PASSWORD 'migration-test-only'"),
      )
      defer ignore(admin.query("DROP ROLE \{role}"))
      ignore(admin.query("CREATE DATABASE \{name}"))
      defer ignore(admin.query("DROP DATABASE \{name}"))
      let config = { ..base, database: name, }
      let sql = connect(config, 16777216, false)
      defer sql.close()
      ignore(admin.query("GRANT CREATE ON DATABASE \{name} TO \{role}"))
      let restricted = {
        ..config,
        user: role,
        password: "migration-test-only",
      }
      try run_source_migrations(restricted, control~) catch {
        Server("42501", _) => ()
        error => raise error
      } noraise {
        _ => fail("non-superuser created event trigger")
      }
      assert_eq(
        cell(sql.query("SELECT count(*)::text FROM etl._sqlx_migrations"), 0, 0),
        "0",
      )
      assert_eq(
        cell(
          sql.query(
            "SELECT count(*)::text FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid WHERE n.nspname='etl'",
          ),
          0,
          0,
        ),
        "0",
      )
      ignore(sql.query("DROP SCHEMA etl CASCADE"))
      // Seed the first migration exactly as SQLx does, then upgrade with MoonCDC.
      let first = source_migrations()[0]
      let hash = @crypto.bytes_to_hex_string(
        @crypto.sha384(@utf8.encode(first.sql)),
      )
      ignore(
        sql.query(
          "CREATE SCHEMA etl; CREATE TABLE etl._sqlx_migrations(version bigint PRIMARY KEY,description text NOT NULL,installed_on timestamptz NOT NULL DEFAULT now(),success boolean NOT NULL,checksum bytea NOT NULL,execution_time bigint NOT NULL); BEGIN; " +
          first.sql +
          " INSERT INTO etl._sqlx_migrations VALUES(\{first.version},'schema change messages',now(),true,decode('\{hash}','hex'),-1); COMMIT",
        ),
      )
      let oid = cell(
        sql.query(
          "SELECT 'etl.describe_table_schema(oid)'::regprocedure::oid::text",
        ),
        0,
        0,
      )
      run_source_migrations(config, control~)
      assert_eq(
        cell(
          sql.query(
            "SELECT count(*)::text FROM etl._sqlx_migrations WHERE success",
          ),
          0,
          0,
        ),
        "2",
      )
      assert_eq(
        cell(
          sql.query(
            "SELECT 'etl.describe_table_schema(oid)'::regprocedure::oid::text",
          ),
          0,
          0,
        ),
        oid,
      )
      let history = sql.query(
        "SELECT version::text,installed_on::text,encode(checksum,'hex'),execution_time::text FROM etl._sqlx_migrations ORDER BY version",
      )
      ignore(
        sql.query(
          "INSERT INTO etl._sqlx_migrations VALUES(1,'other migration set',now(),true,''::bytea,0)",
        ),
      )
      run_source_migrations(config)
      assert_eq(
        sql.query(
          "SELECT version::text,installed_on::text,encode(checksum,'hex'),execution_time::text FROM etl._sqlx_migrations WHERE version<>1 ORDER BY version",
        ),
        history,
      )
      ignore(
        sql.query(
          "UPDATE etl._sqlx_migrations SET checksum=''::bytea WHERE version=\{first.version}",
        ),
      )
      try run_source_migrations(config) catch {
        UnsafeResume(message) => assert_true(message.contains("checksum"))
        error => raise error
      } noraise {
        _ => fail("checksum corruption accepted")
      }
      ignore(
        sql.query(
          "UPDATE etl._sqlx_migrations SET checksum=decode('\{hash}','hex') WHERE version=\{first.version}; UPDATE etl._sqlx_migrations SET success=false WHERE version=1",
        ),
      )
      try run_source_migrations(config) catch {
        UnsafeResume(message) => assert_true(message.contains("dirty"))
        error => raise error
      } noraise {
        _ => fail("dirty unrelated history accepted")
      }
      ignore(sql.query("DELETE FROM etl._sqlx_migrations WHERE version=1"))
      let lock = migration_lock_id(name)
      ignore(sql.query("SELECT pg_advisory_lock(\{lock})"))
      @async.with_task_group(group => {
        let mut finished = false
        let task = group.spawn(() => {
          let result = @async.handle_cancellation(() => {
            run_source_migrations(config)
          })
          finished = true
          result
        })
        while cell(
                sql.query(
                  "SELECT count(*)::text FROM pg_stat_activity WHERE datname='\{name}' AND application_name='mooncdc_source_migrations' AND wait_event='advisory'",
                ),
                0,
                0,
              ) !=
              "1" {
          @async.sleep(5)
        }
        assert_false(finished)
        task.cancel()
        assert_true(task.wait() is None)
      })
      ignore(sql.query("SELECT pg_advisory_unlock(\{lock})"))
      // A new concurrent run acquires the released/cancelled owner's locks.
      @async.with_task_group(group => {
        let one = group.spawn(() => run_source_migrations(config))
        let two = group.spawn(() => run_source_migrations(config))
        one.wait()
        two.wait()
      })
      ignore(
        sql.query(
          "DROP EVENT TRIGGER supabase_etl_ddl_message_trigger; DROP SCHEMA etl CASCADE; CREATE TABLE migration_items(id int PRIMARY KEY); CREATE PUBLICATION migration_test FOR TABLE migration_items",
        ),
      )
      let dir = @fs.tmpdir(prefix="mooncdc-source-migrations-")
      defer @fs.rmdir(dir, recursive=true)
      let control = ApplyControl()
      run_pipeline(
        config,
        MigrationDestination::{ control, },
        publication="migration_test",
        destination_id="migration-test",
        control~,
        state_path=dir + "/state",
        source_migrations=true,
      )
      let state = pipeline_status(dir + "/state")
      drop_slot(sql, state.slot)
      assert_eq(
        cell(
          sql.query(
            "SELECT count(*)::text FROM etl._sqlx_migrations WHERE success",
          ),
          0,
          0,
        ),
        "2",
      )
      assert_eq(
        cell(
          sql.query(
            "SELECT count(*)::text FROM pg_event_trigger WHERE evtname='supabase_etl_ddl_message_trigger'",
          ),
          0,
          0,
        ),
        "1",
      )
      println(
        "LIVE: source migration upgrade/checksum/dirty/permission rollback/lock cancellation/concurrent owner/pipeline startup passed",
      )
      assert_true(query_metric_count(control, Queries) > 0UL)
      assert_eq(query_metric_count(control, StateStore), 0UL)
      println("MIGRATION QUERY METRICS\n" + control.prometheus())
    }) catch {
      InvalidConfig(message) => fail(message)
      error => raise error
    }
  }
}
