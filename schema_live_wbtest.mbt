///|
priv struct SchemaProbe {
  store : &SchemaStore
  control : ApplyControl
  seen : Array[SchemaSnapshot]
  mut injected : Bool
  mut rows : Int
}

///|
impl Destination for SchemaProbe with fn reset_table(self, relation, attempt~) {
  assert_true(!attempt.is_empty())
  let versions = self.store.all()
  assert_eq(versions.length(), 1)
  assert_eq(versions[0].oid, relation.oid)
  assert_eq(versions[0].id.message_lsn.value, 0UL)
  assert_eq(versions[0].columns.length(), 3)
  assert_eq(versions[0].columns[1].primary_key_ordinal, None)
}

///|
impl Destination for SchemaProbe with fn write_rows(self, _relation, rows, _id) {
  if !rows.is_empty() && !self.injected {
    self.injected = true
    // Run after the snapshot transaction releases its table lock.
  }
  Durable
}

///|
impl Destination for SchemaProbe with fn write_transaction(self, tx) {
  for change in tx.changes {
    if change is Message(message) &&
      message.schema_snapshot(commit_lsn=tx.commit_lsn, publication="cdc_ddl")
      is Some(schema) {
      assert_eq(self.store.get(schema.oid, schema.id), Some(schema))
      self.seen.push(schema)
    }
    if change is Insert(_, _) {
      self.rows += 1
    }
  }
  if self.rows == 2 {
    self.control.stop()
  }
  Durable
}

///|
impl Destination for SchemaProbe with fn flush(_self) {
  ()
}

///|
async test "live upstream DDL trigger versions publication scope and pipeline persistence" {
  if live_config() is Some(config) {
    @async.with_timeout(40000, () => {
      let sql = connect(config, 16777216, false)
      defer sql.close()
      // Never replace pre-existing ETL helpers; this test requires an isolated DB.
      assert_true(
        sql.query("SELECT oid FROM pg_namespace WHERE nspname='etl'").is_empty(),
      )
      ignore(
        sql.query(
          "CREATE SCHEMA etl; CREATE SCHEMA cdc_ddl; CREATE TABLE cdc_ddl.items(a int NOT NULL,b text,c int NOT NULL,PRIMARY KEY(c,a) INCLUDE(b)); INSERT INTO cdc_ddl.items VALUES(1,'initial',1); CREATE PUBLICATION cdc_ddl FOR TABLE cdc_ddl.items; CREATE PUBLICATION cdc_ddl_other FOR TABLE cdc_ddl.items",
        ),
      )
      defer ignore(
        sql.query(
          "DROP EVENT TRIGGER IF EXISTS supabase_etl_ddl_message_trigger; DROP PUBLICATION cdc_ddl,cdc_ddl_other; DROP SCHEMA etl CASCADE; DROP SCHEMA cdc_ddl CASCADE",
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
      let dir = @fs.tmpdir(prefix="mooncdc-ddl-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/state"
      let control = ApplyControl()
      with_file_schema_store(dir + "/schemas", identity="ddl/source/target", store => {
        let probe : SchemaProbe = {
          store,
          control,
          seen: [],
          injected: false,
          rows: 0,
        }
        @async.with_task_group(group => {
          let writer = group.spawn(() => {
            while !probe.injected {
              @async.sleep(10)
            }
            // ALTER TABLE blocks until the initial snapshot releases ACCESS SHARE.
            ignore(
              sql.query(
                "BEGIN; ALTER TABLE cdc_ddl.items ADD COLUMN d text DEFAULT 'default'; ALTER TABLE cdc_ddl.items ALTER COLUMN d SET NOT NULL; INSERT INTO cdc_ddl.items VALUES(2,'new',2,'first'); COMMIT",
              ),
            )
            ignore(
              sql.query(
                "BEGIN; ALTER TABLE cdc_ddl.items ADD COLUMN rolled_back text; ROLLBACK",
              ),
            )
            ignore(
              sql.query(
                "ALTER PUBLICATION cdc_ddl_other SET TABLE cdc_ddl.items(a,c); ALTER PUBLICATION cdc_ddl SET TABLE cdc_ddl.items(a,c,d); INSERT INTO cdc_ddl.items(a,c,d) VALUES(3,3,'last')",
              ),
            )
          })
          run_pipeline(
            config,
            probe,
            publication="cdc_ddl",
            destination_id="schema-probe",
            state_path=path,
            control~,
            schema_store=store,
          )
          writer.wait()
        })
        assert_eq(probe.seen.length(), 3)
        assert_true(probe.seen[0].id < probe.seen[1].id)
        assert_eq(probe.seen[0].id.commit_lsn, probe.seen[1].id.commit_lsn)
        assert_true(probe.seen[1].id < probe.seen[2].id)
        assert_true(probe.seen[0].columns[3].nullable)
        assert_false(probe.seen[1].columns[3].nullable)
        assert_eq(probe.seen[1].columns[0].primary_key_ordinal, Some(2))
        assert_eq(probe.seen[1].columns[2].primary_key_ordinal, Some(1))
        assert_eq(probe.seen[1].columns[1].primary_key_ordinal, None)
        assert_true(probe.seen[1].columns[3].default_expression is Some(_))
        assert_eq(store.all().length(), 4)
        let initial = store.all()[0]
        let before = initial.project(initial.relation(["a", "b", "c"]))
        let added = probe.seen[0].project(
          probe.seen[0].relation(["a", "b", "c", "d"]),
        )
        let required = probe.seen[1].project(
          probe.seen[1].relation(["a", "b", "c", "d"]),
        )
        let filtered = probe.seen[2].project(
          probe.seen[2].relation(["a", "c", "d"]),
        )
        assert_true(
          before.plan_change(added).operations
          is [AddColumn(column, reason=TableSchema)] &&
          column.name == "d",
        )
        assert_true(
          added.plan_change(required).operations
          is [AlterColumn(kind=Nullability, ..)],
        )
        assert_true(
          required.plan_change(filtered).operations
          is [DropColumn(column, reason=Publication)] &&
          column.name == "b",
        )
      })
      with_file_schema_store(dir + "/schemas", identity="ddl/source/target", store => {
        assert_eq(store.all().length(), 4)
      })
      with_file_schema_store(dir + "/missing", identity="ddl/source/target", store => {
        let probe : SchemaProbe = {
          store,
          control: ApplyControl(),
          seen: [],
          injected: false,
          rows: 0,
        }
        try
          run_pipeline(
            config,
            probe,
            publication="cdc_ddl",
            destination_id="schema-probe",
            state_path=path,
            control=probe.control,
            schema_store=store,
          )
        catch {
          UnsafeResume("completed table has no retained schema") => ()
          e => raise e
        } noraise {
          _ => fail("missing schema accepted on resume")
        }
      })
      drop_slot(sql, pipeline_status(path).slot)
      println(
        "LIVE: unmodified upstream DDL trigger, ordered versions, rollback, publication scope and durable schema-before-target passed",
      )
    })
  }
}
