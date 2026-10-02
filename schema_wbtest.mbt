///|
fn ddl_fixture(
  identity? : String = "d",
  command? : String = "ALTER TABLE",
  publication? : String,
) -> Json {
  {
    "command_tag": command.to_json(),
    "publication_name": match publication {
      Some(value) => value.to_json()
      None => Null
    },
    "nspname": "public",
    "relname": "items",
    "oid": 42,
    "identity": {
      "primary_key_attnums": [3, 1],
      "relreplident": identity.to_json(),
      "replica_identity_index_attnums": [2],
    },
    "columns": [
      {
        "attname": "third",
        "atttypid": 20,
        "atttypmod": -1,
        "attnum": 3,
        "attnotnull": true,
        "default_expression": "42",
      },
      {
        "attname": "first",
        "atttypid": 23,
        "atttypmod": -1,
        "attnum": 1,
        "attnotnull": true,
      },
      {
        "attname": "second",
        "atttypid": 25,
        "atttypmod": -1,
        "attnum": 2,
        "attnotnull": false,
        "default_expression": Null,
      },
    ],
    "future_field": { "ignored": true },
  }
}

///|
fn ddl_snapshot(commit : UInt64, message : UInt64) -> SchemaSnapshot raise {
  parse_schema(ddl_fixture(), {
    commit_lsn: { value: commit, },
    message_lsn: { value: message, },
  })
}

///|
test "schema identity order publication masks and strict payload validation" {
  for
    (mode, expected) in [
      ("d", [true, false]),
      ("i", [false, true]),
      ("f", [true, true]),
      ("n", [false, false]),
    ] {
    let schema = parse_schema(ddl_fixture(identity=mode), {
      commit_lsn: { value: 100, },
      message_lsn: { value: 50, },
    })
    assert_eq(schema.columns.map(c => c.name), ["first", "second", "third"])
    assert_eq(schema.columns[0].primary_key_ordinal, Some(2))
    assert_eq(schema.columns[2].primary_key_ordinal, Some(1))
    assert_eq(schema.columns[2].default_expression, Some("42"))
    assert_true(schema.columns[1].nullable)
    assert_eq(
      schema.relation(["second", "first"]).columns.map(c => c.key),
      ReadOnlyArray::from_array(expected),
    )
  }
  let msg : LogicalMessage = {
    transactional: true,
    lsn: { value: 50, },
    prefix: "supabase_etl_ddl",
    content: @utf8.encode(
      ddl_fixture(command="ALTER PUBLICATION", publication="pub").stringify(),
    ),
  }
  assert_true(
    msg.schema_snapshot(commit_lsn={ value: 100, }, publication="pub")
    is Some(_),
  )
  assert_eq(
    msg.schema_snapshot(commit_lsn={ value: 100, }, publication="other"),
    None,
  )
  let missing = {
    ..msg,
    content: @utf8.encode(ddl_fixture(command="ALTER PUBLICATION").stringify()),
  }
  assert_eq(
    missing.schema_snapshot(commit_lsn={ value: 100, }, publication="pub"),
    None,
  )
  assert_eq(
    { ..msg, prefix: "other", content: b"invalid", }.schema_snapshot(
      commit_lsn={ value: 100, },
      publication="pub",
    ),
    None,
  )
  for
    bad in [
      { ..msg, transactional: false, },
      { ..msg, content: b"\xff", },
      { ..msg, content: b"{}", },
      { ..msg, lsn: { value: 101, }, },
    ] {
    try
      bad.schema_snapshot(commit_lsn={ value: 100, }, publication="pub")
    catch {
      Protocol(_) => ()
      e => raise e
    } noraise {
      _ => fail("invalid DDL accepted")
    }
  }
  assert_true(ddl_snapshot(100, 90).id < ddl_snapshot(110, 10).id)
  assert_true(ddl_snapshot(100, 50).id < ddl_snapshot(100, 90).id)
  assert_true(ddl_snapshot(100, 90).id <= SchemaId::before_lsn({ value: 101, }))
  assert_true(ddl_snapshot(100, 50).id > SchemaId::before_lsn({ value: 100, }))
}

///|
async test "schema store persists replay versions and prunes by exclusive checkpoint" {
  let dir = @fs.tmpdir(prefix="mooncdc-schemas-")
  defer @fs.rmdir(dir, recursive=true)
  let path = dir + "/schemas"
  let versions = [
    ddl_snapshot(100, 90),
    ddl_snapshot(110, 10),
    ddl_snapshot(110, 100),
    ddl_snapshot(120, 20),
  ]
  with_file_schema_store(path, identity="source/target", store => {
    // Insert out of order: message LSN alone must never choose the latest version.
    for index in [3, 0, 2, 1] {
      store.put(versions[index])
    }
    store.put(versions[0])
    assert_eq(store.all().length(), 4)
    assert_eq(
      store.get(42U, SchemaId::before_lsn({ value: 110, })),
      Some(versions[0]),
    )
    assert_eq(store.get(42U, versions[1].id), Some(versions[1]))
    assert_eq(store.prune([(42U, SchemaId::before_lsn({ value: 111, }))]), 2)
    assert_eq(store.get(42U, versions[2].id), Some(versions[2]))
    assert_eq(store.get(42U, versions[3].id), Some(versions[3]))
    try with_file_schema_store(path, identity="source/target", _ => ()) catch {
      CheckpointError(_) => ()
      e => raise e
    } noraise {
      _ => fail("schema lock bypassed")
    }
  })
  with_file_schema_store(path, identity="source/target", store => {
    assert_eq(store.all().length(), 2)
    assert_eq(store.get(42U, versions[2].id), Some(versions[2]))
  })
  try with_file_schema_store(path, identity="wrong", _ => ()) catch {
    CheckpointError(_) => ()
    e => raise e
  } noraise {
    _ => fail("schema identity changed")
  }
  let bytes = @fs.read_file(path).text()
  @fs.write_file(path, bytes + "x")
  try with_file_schema_store(path, identity="source/target", _ => ()) catch {
    CheckpointError(_) => ()
    e => raise e
  } noraise {
    _ => fail("schema corruption ignored")
  }
}

///|
async test "schema store failed writes preserve cache and refuse conflicting replay" {
  let dir = @fs.tmpdir(prefix="mooncdc-schema-fail-")
  defer @fs.rmdir(dir, recursive=true)
  let path = dir + "/schemas"
  let first = ddl_snapshot(100, 90)
  let next = ddl_snapshot(110, 10)
  with_file_schema_store(path, identity="source/target", store => {
    store.put(first)
    guard first.payload is Object(object) else { fail("object fixture") }
    let enriched = object.copy()
    enriched["future_field"] = "new ignored metadata"
    store.put(parse_schema(Json::object(enriched), first.id))
    @fs.mkdir(path + ".tmp")
    try store.put(next) catch {
      _ => ()
    } noraise {
      _ => fail("failed schema write ignored")
    }
    assert_eq(store.all(), [first])
    assert_eq(
      read_schema_bytes(
        @utf8.encode(@fs.read_file(path).text()),
        "source/target",
      ),
      [first],
    )
    @fs.rmdir(path + ".tmp")
    store.put(next)
    let conflict = parse_schema(ddl_fixture(identity="f"), next.id)
    try store.put(conflict) catch {
      UnsafeResume(_) => ()
      e => raise e
    } noraise {
      _ => fail("schema version conflict ignored")
    }
  })
}
