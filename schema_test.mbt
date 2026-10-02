///|
test "schema exclusive bound is available to external consumers" {
  let checkpoint = @mooncdc.Lsn::parse("1/0")
  let bound = @mooncdc.SchemaId::before_lsn(checkpoint)
  assert_eq(bound.commit_lsn.to_string(), "0/FFFFFFFF")
  assert_eq(bound.message_lsn.to_string(), "FFFFFFFF/FFFFFFFF")
  try @mooncdc.SchemaId::before_lsn(@mooncdc.Lsn::parse("0/0")) catch {
    @mooncdc.InvalidConfig(_) => ()
    e => raise e
  } noraise {
    _ => fail("zero exclusive bound accepted")
  }
}

///|
async test "schema store public scope validates identity before IO" {
  try
    @mooncdc.with_file_schema_store("unused", identity="", store => {
      let versions : ReadOnlyArray[@mooncdc.SchemaSnapshot] = store.all()
      assert_eq(versions.length(), 0)
    })
  catch {
    @mooncdc.InvalidConfig(_) => ()
    e => raise e
  } noraise {
    _ => fail("empty identity accepted")
  }
}

///|
test "external consumer can project type and plan schema snapshots" {
  let schema = @mooncdc.SchemaSnapshot::from_json({
    "commit_lsn": "0/30",
    "message_lsn": "0/20",
    "payload": {
      "command_tag": "ALTER TABLE",
      "nspname": "public",
      "relname": "t",
      "oid": 42,
      "identity": {
        "primary_key_attnums": [1],
        "relreplident": "d",
        "replica_identity_index_attnums": [],
      },
      "columns": [
        {
          "attname": "id",
          "atttypid": 23,
          "atttypmod": -1,
          "attnum": 1,
          "attnotnull": true,
        },
      ],
    },
  })
  assert_eq(@mooncdc.SchemaSnapshot::from_json(schema.to_json()), schema)
  let projected = schema.project(schema.relation(["id"]))
  assert_eq(projected.identity_kind(), @mooncdc.PrimaryKey)
  assert_true(projected.all_primary_keys_replicated())
  assert_eq(projected.decode_row([@mooncdc.Text("42")]), [@mooncdc.I32(42)])
  assert_true(projected.plan_change(projected).operations.is_empty())
  for
    invalid in [
      Json::null(),
      { "commit_lsn": "invalid", "message_lsn": "0/20", "payload": {} },
    ] {
    try @mooncdc.SchemaSnapshot::from_json(invalid) catch {
      @mooncdc.Protocol(_) => ()
      e => raise e
    } noraise {
      _ => fail("invalid external schema accepted")
    }
  }
}
