///|
priv struct ChFaultStore {
  inner : &StateStore
  mut remaining : Int?
}

///|
impl StateStore for ChFaultStore with fn acquire(self, owner) {
  self.inner.acquire(owner)
}

///|
impl StateStore for ChFaultStore with fn release(self, owner) {
  self.inner.release(owner)
}

///|
impl StateStore for ChFaultStore with fn load(self, key) {
  self.inner.load(key)
}

///|
impl StateStore for ChFaultStore with fn save(self, owner, key, bytes) {
  if key is TableMetadata(_) && self.remaining is Some(remaining) {
    if remaining == 0 {
      self.remaining = None
      raise CheckpointError("injected ClickHouse metadata write failure")
    }
    self.remaining = Some(remaining - 1)
  }
  self.inner.save(owner, key, bytes)
}

///|
fn ch_test_schema(
  columns : Array[SchemaColumn],
  version : UInt64,
  mask? : Array[Bool],
  name? : String = "ddl",
) -> ReplicatedSchema raise {
  let keys = columns.filter(c => c.primary_key_ordinal is Some(_))
  keys.sort_by((a, b) => {
    a.primary_key_ordinal.unwrap().compare(b.primary_key_ordinal.unwrap())
  })
  let schema = parse_schema(
    {
      "command_tag": "ALTER TABLE",
      "nspname": "public",
      "relname": name.to_json(),
      "oid": 42,
      "identity": {
        "primary_key_attnums": keys.map(c => c.ordinal).to_json(),
        "relreplident": "d",
        "replica_identity_index_attnums": [],
      },
      "columns": columns
      .map(c => {
        "attname": c.name.to_json(),
        "atttypid": c.type_oid.to_json(),
        "atttypmod": c.type_modifier.to_json(),
        "attnum": c.ordinal.to_json(),
        "attnotnull": (!c.nullable).to_json(),
        "default_expression": match c.default_expression {
          Some(s) => s.to_json()
          None => Json::null()
        },
      })
      .to_json(),
    },
    { commit_lsn: { value: version, }, message_lsn: { value: version - 1, }, },
  )
  let replicated = mask.unwrap_or(columns.map(_ => true))
  ReplicatedSchema::new(
    schema,
    replicated=ReadOnlyArray::from_array(replicated),
    identity=ReadOnlyArray::from_array(
      columns.mapi((i, c) => replicated[i] && c.primary_key_ordinal is Some(_)),
    ),
  )
}

///|
fn ch_test_columns() -> Array[SchemaColumn] {
  [
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
      name: "value",
      type_oid: 25U,
      type_modifier: -1,
      ordinal: 2,
      nullable: false,
      primary_key_ordinal: None,
      default_expression: None,
    },
  ]
}

///|
test "ClickHouse DDL validation and unambiguous recovery endpoints" {
  let columns = ch_test_columns()
  let before = ch_test_schema(columns, 100)
  let added = {
    ..columns[1],
    name: "extra",
    ordinal: 3,
    default_expression: Some("'initial'"),
  }
  assert_eq(
    ch_add_definition(added, TableSchema),
    "\"extra\" String DEFAULT 'initial'",
  )
  assert_eq(ch_add_definition(added, Publication), "\"extra\" Nullable(String)")
  assert_eq(
    ch_add_definition(
      { ..added, default_expression: Some("now()"), },
      TableSchema,
    ),
    "\"extra\" Nullable(String)",
  )
  let after = ch_test_schema(columns + [added], 200)
  assert_false(ch_recover_at_target(before, after, ["id", "value"]))
  assert_true(ch_recover_at_target(before, after, ["id", "value", "extra"]))
  assert_true(
    try {
      ignore(ch_recover_at_target(before, after, ["id", "extra"]))
      false
    } catch {
      Manual(_) => true
      _ => false
    },
  )
  let reused = ch_test_schema([columns[0], { ..added, name: "value", }], 200)
  assert_true(
    try {
      ignore(ch_recover_at_target(before, reused, ["id", "value"]))
      false
    } catch {
      Manual(_) => true
      _ => false
    },
  )
  for
    bad in [
      ch_test_schema([{ ..columns[0], name: "key", }, columns[1]], 200),
      ch_test_schema([{ ..columns[0], type_oid: 20U, }, columns[1]], 200),
      ch_test_schema([columns[0], { ..columns[1], name: "nested.value", }], 200),
    ] {
    assert_true(
      try {
        ignore(ch_validate_plan(before, bad, ReplacingMergeTree))
        false
      } catch {
        Manual(_) => true
        _ => false
      },
    )
  }
  let array = { ..added, type_oid: 1007U, }
  let hidden = ch_test_schema(columns + [array], 100, mask=[true, true, false])
  assert_true(
    try {
      ignore(
        ch_validate_plan(
          hidden,
          ch_test_schema(columns + [array], 200),
          MergeTree,
        ),
      )
      false
    } catch {
      Manual(_) => true
      _ => false
    },
  )
  ignore(
    ch_validate_plan(
      before,
      ch_test_schema(
        [columns[0], { ..columns[1], type_oid: 1043U, type_modifier: 54, }],
        200,
      ),
      ReplacingMergeTree,
    ),
  )
}

///|
async test "ClickHouse live DDL historical defaults and endpoint recovery" {
  if @env.get_env_var("MOONCDC_CLICKHOUSE_LIVE") != Some("1") {
    return
  }
  @async.with_timeout(60000, () => {
    let dir = @fs.tmpdir(prefix="mooncdc-ch-ddl-")
    defer @fs.rmdir(dir, recursive=true)
    let http = ChHttp::new(
      "http://127.0.0.1:18123", "cdc", "mooncdc-test-only", 10000,
    )
    let db = "mooncdc_ddl_" + pipeline_nonce()
    ignore(http.query("CREATE DATABASE " + ch_quote(db, '"')))
    defer (ignore(http.query("DROP DATABASE " + ch_quote(db, '"'))) catch {
      _ => ()
    })
    with_file_schema_store(dir + "/schema", identity=db, schemas => {
      for engine in [ClickHouseEngine::ReplacingMergeTree, MergeTree] {
        let state_dir = dir + "/" + ch_engine_name(engine)
        @fs.mkdir(state_dir)
        let state : ChFaultStore = {
          inner: FileStateStore::new(state_dir),
          remaining: None,
        }
        with_state_store(state, session => {
          let target = ClickHouseDestination::new(
            endpoint="http://127.0.0.1:18123",
            user="cdc",
            password="mooncdc-test-only",
            database=db,
            publication="p",
            schema_store=schemas,
            engine~,
            timeout_ms=10000,
          )
          target.bind_state(session)
          target.startup()
          defer target.shutdown()
          let columns = ch_test_columns()
          let before = ch_test_schema(columns, 100)
          let relation = before.schema.relation(["id", "value"])
          target.bind_snapshot_schema(relation, before.schema)
          state.remaining = Some(1)
          assert_true(
            try {
              target.reset_table(relation, attempt="copy")
              false
            } catch {
              CheckpointError("injected ClickHouse metadata write failure") =>
                true
              _ => false
            },
          )
          assert_eq(
            target.stored_record(before.schema.oid).unwrap().creating,
            Some(true),
          )
          target.shutdown()
          target.bind_state(session)
          target.startup()
          ignore(target.prepare_schema(before))
          // Records written before DDL support omitted these optional fields.
          guard ToJson::to_json(
              target.stored_record(before.schema.oid).unwrap(),
            )
            is Object(legacy) else {
            fail("metadata must be an object")
          }
          legacy.remove("previous")
          legacy.remove("table_name")
          legacy.remove("creating")
          session.put_metadata(before.schema.oid, Json::object(legacy))
          target.shutdown()
          target.bind_state(session)
          target.startup()
          ignore(target.prepare_schema(before))
          let table = target.qualified("public_ddl")
          ignore(
            http.query("INSERT INTO " + table + " (id,value) VALUES(1,'old')"),
          )
          let added = {
            ..columns[1],
            name: "added",
            ordinal: 3,
            default_expression: Some("'initial'"),
          }
          let after = ch_test_schema(
            [
              columns[0],
              { ..columns[1], name: "renamed", nullable: true, },
              added,
            ],
            200,
          )
          ignore(target.prepare_schema(after))
          assert_eq(
            http
            .query("SELECT renamed,added FROM " + table + " WHERE id=1")
            .trim()
            .to_owned(),
            "old\tinitial",
          )
          let changed = ch_test_schema(
            [
              after.schema.columns[0],
              after.schema.columns[1],
              { ..added, default_expression: Some("'later'"), nullable: true, },
            ],
            300,
          )
          ignore(target.prepare_schema(changed))
          assert_eq(
            http
            .query("SELECT added FROM " + table + " WHERE id=1")
            .trim()
            .to_owned(),
            "initial",
          )
          let tightened = ch_test_schema(
            [
              changed.schema.columns[0],
              { ..changed.schema.columns[1], nullable: false, },
              changed.schema.columns[2],
            ],
            400,
          )
          let layout = target.prepare_schema(tightened)
          assert_eq(layout.layout.columns[1].1, "Nullable(String)")
          // Persist Applying and simulate process loss at each supported endpoint.
          let extra = {
            ..added,
            name: "last",
            ordinal: 4,
            default_expression: None,
          }
          let next = ch_test_schema(
            tightened.schema.columns.iter().collect() + [extra],
            500,
          )
          let old = target.stored_record(before.schema.oid).unwrap()
          let pending = {
            ..target.table_record({ ..layout, schema: next, }),
            previous: Some(ToJson::to_json(old)),
          }
          session.put_metadata(before.schema.oid, ToJson::to_json(pending))
          target.shutdown()
          target.bind_state(session)
          target.startup()
          ignore(target.prepare_schema(next))
          assert_true(
            target.stored_record(before.schema.oid).unwrap().previous is None,
          )
          assert_eq(
            http
            .query("SELECT isNull(last) FROM " + table + " WHERE id=1")
            .trim()
            .to_owned(),
            "1",
          )
          let final_schema = ch_test_schema(
            next.schema.columns
            .iter()
            .collect()
            .map(c => if c.name == "last" { { ..c, name: "final", } } else { c }),
            600,
          )
          let old = target.stored_record(before.schema.oid).unwrap()
          let cached = target.tables[before.schema.oid]
          let pending = {
            ..target.table_record({ ..cached, schema: final_schema, }),
            previous: Some(ToJson::to_json(old)),
          }
          session.put_metadata(before.schema.oid, ToJson::to_json(pending))
          ignore(
            http.query("ALTER TABLE " + table + " RENAME COLUMN last TO final"),
          )
          target.shutdown()
          target.bind_state(session)
          target.startup()
          ignore(target.prepare_schema(final_schema))
          assert_true(
            target.stored_record(before.schema.oid).unwrap().previous is None,
          )
          // A partially completed two-column addition fails without changing metadata.
          let partial = ch_test_schema(
            final_schema.schema.columns.iter().collect() +
            [
              { ..extra, name: "p1", ordinal: 5, },
              { ..extra, name: "p2", ordinal: 6, },
            ],
            700,
          )
          let old = target.stored_record(before.schema.oid).unwrap()
          let cached = target.tables[before.schema.oid]
          let pending = {
            ..target.table_record({ ..cached, schema: partial, }),
            previous: Some(ToJson::to_json(old)),
          }
          let pending_json = ToJson::to_json(pending)
          session.put_metadata(before.schema.oid, pending_json)
          ignore(
            http.query(
              "ALTER TABLE " +
              table +
              " ADD COLUMN p1 Nullable(String) AFTER final",
            ),
          )
          target.shutdown()
          target.bind_state(session)
          target.startup()
          assert_true(
            try {
              ignore(target.prepare_schema(partial))
              false
            } catch {
              Manual("ClickHouse schema recovery found a partial DDL state") =>
                true
              _ => false
            },
          )
          assert_eq(session.get_metadata(before.schema.oid), Some(pending_json))
          // Explicit repair to the target endpoint lets a new owner continue.
          ignore(
            http.query(
              "ALTER TABLE " +
              table +
              " ADD COLUMN p2 Nullable(String) AFTER p1",
            ),
          )
          ignore(target.prepare_schema(partial))
          assert_true(
            try {
              ignore(target.prepare_schema(final_schema))
              false
            } catch {
              Manual(_) => true
              _ => false
            },
          )
          let renamed = ch_test_schema(
            partial.schema.columns.iter().collect(),
            800,
            name="new_name",
          )
          assert_eq(target.prepare_schema(renamed).name, "public_ddl")
          let final_columns = renamed.schema.columns.iter().collect() +
            [{ ..extra, name: "durability", ordinal: 7, }]
          let durable = ch_test_schema(final_columns, 900, name="new_name")
          let original_metadata = session.get_metadata(before.schema.oid)
          state.remaining = Some(0)
          assert_true(
            try {
              ignore(target.prepare_schema(durable))
              false
            } catch {
              CheckpointError("injected ClickHouse metadata write failure") =>
                true
              _ => false
            },
          )
          assert_eq(session.get_metadata(before.schema.oid), original_metadata)
          assert_false(
            target.read_columns("public_ddl").any(c => c.0 == "durability"),
          )
          state.remaining = Some(1)
          assert_true(
            try {
              ignore(target.prepare_schema(durable))
              false
            } catch {
              CheckpointError("injected ClickHouse metadata write failure") =>
                true
              _ => false
            },
          )
          assert_true(
            target.stored_record(before.schema.oid).unwrap().previous is Some(_),
          )
          assert_false(target.tables.contains(before.schema.oid))
          assert_true(
            target.read_columns("public_ddl").any(c => c.0 == "durability"),
          )
          target.shutdown()
          target.bind_state(session)
          target.startup()
          ignore(target.prepare_schema(durable))
          assert_true(
            target.stored_record(before.schema.oid).unwrap().previous is None,
          )
          let stable = session.get_metadata(before.schema.oid)
          let invalid = ch_test_schema(
            final_columns.map(c => {
              if c.name == "id" {
                { ..c, type_oid: 20U, }
              } else {
                c
              }
            }),
            1000,
            name="new_name",
          )
          assert_true(
            try {
              ignore(target.prepare_schema(invalid))
              false
            } catch {
              Manual("ClickHouse mapped type change requires resynchronization") =>
                true
              _ => false
            },
          )
          assert_eq(session.get_metadata(before.schema.oid), stable)
          assert_eq(target.read_columns("public_ddl")[0], ("id", "Int32"))
          if engine == ReplacingMergeTree {
            assert_eq(
              http
              .query("SELECT count() FROM " + table + "__current")
              .trim()
              .to_owned(),
              "1",
            )
          }
        })
      }
    })
  })
}
