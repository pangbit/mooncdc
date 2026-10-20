///|
async test "schema state records preserve durable cache locks pruning and identity" {
  let dir = @fs.tmpdir(prefix="mooncdc-state-schema-")
  defer @fs.rmdir(dir, recursive=true)
  let state = FileStateStore::new(dir)
  let other = FileStateStore::new(dir)
  let first = ddl_snapshot(100, 90)
  let next = ddl_snapshot(110, 100)
  with_state_schema_store(state, identity="source/target", schemas => {
    schemas.put(first)
    schemas.put(first)
    assert_eq(schemas.all(), [first])
    try
      with_state_schema_store(other, identity="source/target", _ => ())
    catch {
      CheckpointError(_) => ()
      error => raise error
    } noraise {
      _ => fail("concurrent schema owner accepted")
    }
    @fs.mkdir(dir + "/schemas.tmp")
    try schemas.put(next) catch {
      _ => ()
    } noraise {
      _ => fail("failed schema save accepted")
    }
    assert_eq(schemas.all(), [first])
    assert_eq(
      read_schema_bytes(state.load(Schemas).unwrap(), "source/target"),
      [first],
    )
    @fs.rmdir(dir + "/schemas.tmp")
    schemas.put(next)
    @fs.mkdir(dir + "/schemas.tmp")
    try schemas.prune([(42U, next.id)]) catch {
      _ => ()
    } noraise {
      _ => fail("failed prune accepted")
    }
    assert_eq(schemas.all(), [first, next])
    @fs.rmdir(dir + "/schemas.tmp")
    assert_eq(schemas.prune([(42U, next.id)]), 1)
    try schemas.put(parse_schema(ddl_fixture(identity="f"), next.id)) catch {
      UnsafeResume(_) => ()
      error => raise error
    } noraise {
      _ => fail("conflicting version accepted")
    }
  })
  with_state_schema_store(other, identity="source/target", schemas => {
    assert_eq(schemas.get(42U, next.id), Some(next))
    assert_eq(schemas.all(), [next])
  })
  let saved = state.load(Schemas)
  try with_state_schema_store(state, identity="wrong", _ => ()) catch {
    CheckpointError(message) =>
      assert_eq(message, "schema store identity or version")
    error => raise error
  } noraise {
    _ => fail("schema identity mismatch accepted")
  }
  assert_eq(state.load(Schemas), saved)
  @fs.write_file(dir + "/schemas", b"corrupt")
  try with_state_schema_store(state, identity="source/target", _ => ()) catch {
    CheckpointError(_) => ()
    error => raise error
  } noraise {
    _ => fail("schema corruption accepted")
  }
  with_state_store(other, _ => ())
}

///|
async test "live PostgreSQL schema records recover history and fence a lost owner" {
  if live_config() is Some(config) {
    @async.with_timeout(15000, () => {
      let sql = connect(config, 16777216, false)
      defer sql.close()
      let name = "schema_state_" + pipeline_nonce()
      ignore(sql.query("CREATE SCHEMA \{name}"))
      defer ignore(sql.query("DROP SCHEMA \{name} CASCADE"))
      let state = PostgresStateStore::new(
        config,
        store_id="schemas",
        schema=name,
      )
      let other = PostgresStateStore::new(
        config,
        store_id="schemas",
        schema=name,
      )
      let first = ddl_snapshot(100, 90)
      let next = ddl_snapshot(110, 100)
      with_state_schema_store(state, identity="source/target", schemas => {
        schemas.put(first)
        let pid = cell(state.query_owned("SELECT pg_backend_pid()::text"), 0, 0)
        ignore(sql.query("SELECT pg_terminate_backend(\{pid},5000)"))
        try schemas.put(next) catch {
          _ => ()
        } noraise {
          _ => fail("lost owner wrote schema")
        }
        assert_eq(schemas.all(), [first])
        with_state_schema_store(other, identity="source/target", recovered => {
          assert_eq(recovered.all(), [first])
          recovered.put(next)
          try schemas.put(next) catch {
            CheckpointError(_) => ()
            error => raise error
          } noraise {
            _ => fail("stale schema owner reconnected")
          }
          assert_eq(schemas.all(), [first])
          assert_eq(recovered.prune([(42U, next.id)]), 1)
        })
      })
      with_state_schema_store(state, identity="source/target", schemas => {
        assert_eq(schemas.all(), [next])
      })
      println(
        "LIVE: PostgreSQL schema history, durable prune and lost-owner fencing passed",
      )
    })
  }
}
