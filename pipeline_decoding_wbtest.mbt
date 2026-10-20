///|
fn decoding_tx(commit : UInt64, changes : ReadOnlyArray[Change]) -> Transaction {
  {
    id: "decoding/\{commit}",
    xid: 1U,
    commit_lsn: { value: commit, },
    end_lsn: { value: commit + 1, },
    commit_time_us: 0,
    changes,
    session: "test",
    sequence: 1,
    projected_ids: None,
    decoded_schemas: None,
  }
}

///|
fn decoding_message(lsn : UInt64) -> Change {
  Message({
    transactional: true,
    lsn: { value: lsn, },
    prefix: "supabase_etl_ddl",
    content: @utf8.encode(ddl_fixture().stringify()),
  })
}

///|
async test "decoding bootstrap excludes preloaded future schemas and preserves relation lineage" {
  let dir = @fs.tmpdir(prefix="mooncdc-decoding-")
  defer @fs.rmdir(dir, recursive=true)
  with_file_schema_store(dir + "/schemas", identity="decoder", schemas => {
    let initial = ddl_snapshot(100, 0)
    schemas.put(initial)
    schemas.put(ddl_snapshot(250, 240))
    let decoder = PipelineDecoding::new(schemas, { value: 150, })
    let relation = initial.relation(["first", "second"])
    let first = decoder.transaction(
      decoding_tx(300, [
        RelationChanged(relation),
        Insert(relation, [Text("1"), Text("a")]),
      ]),
      [],
      "pub",
    )
    assert_eq(first.replicated_schemas(0)[0].schema.id, initial.id)
    assert_eq(first.replicated_schemas(1)[0].schema.id, initial.id)
    let repeated = decoder.transaction(
      decoding_tx(310, [RelationChanged(relation)]),
      [],
      "pub",
    )
    assert_eq(repeated.replicated_schemas(0)[0].schema.id, initial.id)
    let tx = decoding_tx(400, [
      Insert(relation, [Text("1"), Text("before-ddl")]),
      decoding_message(350),
      decoding_message(360),
      Insert(relation, [Text("2"), Text("b")]),
    ])
    let resolved = decoder.transaction(tx, [], "pub")
    assert_eq(
      resolved.replicated_schemas(3)[0].schema.id,
      ddl_snapshot(400, 360).id,
    )
    assert_eq(resolved.replicated_schemas(0)[0].schema.id, initial.id)
    assert_eq(resolved.replicated_schemas(3)[0].replicated, [true, true, false])
    assert_eq(resolved.replicated_schemas(3)[0].identity, [true, false, false])
    assert_eq(resolved.event_id(3), tx.event_id(3))
    assert_eq(resolved.changes, tx.changes)
    assert_eq(first.replicated_schemas(1)[0].schema.id, initial.id)
    assert_eq(resolved.replicated_schemas(-1), [])
    assert_eq(tx.replicated_schemas(2), [])
  })
}

///|
async test "decoding handover restores masks and rejects absent future or corrupt state" {
  let dir = @fs.tmpdir(prefix="mooncdc-decoding-state-")
  defer @fs.rmdir(dir, recursive=true)
  with_file_schema_store(dir + "/schemas", identity="decoder", schemas => {
    let initial = ddl_snapshot(100, 0)
    schemas.put(initial)
    let relation = initial.relation(["first", "second"])
    let projected = initial.project(relation)
    let stored = StoredDecoding::capture(projected)
    let table : StoredTable = {
      oid: 42U,
      phase: Catchup("0/C8"),
      decoding: Some(stored),
    }
    let roundtrip = parse_pipeline_state(
      state_bytes({ ..pipeline_fixture(), tables: [table], }),
    )
    let decoder = PipelineDecoding::new(schemas, { value: 50, })
    let tx = decoding_tx(210, [Insert(relation, [Text("1"), Text("b")])])
    let resolved = decoder.transaction(tx, roundtrip.tables, "pub")
    assert_eq(resolved.replicated_schemas(0)[0], projected)
    let pending = PipelineDecoding::new(schemas, { value: 50, })
    let resolved = pending.transaction(
      decoding_tx(220, [
        decoding_message(215),
        Insert(relation, [Text("1"), Text("b")]),
      ]),
      [table],
      "pub",
    )
    assert_eq(
      resolved.replicated_schemas(1)[0].replicated,
      projected.replicated,
    )
    assert_eq(
      resolved.replicated_schemas(1)[0].schema.id,
      ddl_snapshot(220, 215).id,
    )
    for
      invalid in [
        { ..table, decoding: None, },
        { ..table, phase: Catchup("0/100"), },
        { ..table, decoding: Some({ ..stored, commit_lsn: "0/65", }), },
        { ..table, decoding: Some({ ..stored, commit_lsn: "0/100", }), },
        {
          ..table,
          decoding: Some({ ..stored, replicated: [true], identity: [true], }),
        },
        {
          ..table,
          decoding: Some({ ..stored, identity: [true, false, true], }),
        },
      ] {
      let fresh = PipelineDecoding::new(schemas, { value: 50, })
      try fresh.transaction(tx, [invalid], "pub") catch {
        UnsafeResume(_) | CheckpointError(_) => ()
        error => raise error
      } noraise {
        _ => fail("invalid decoder accepted")
      }
    }
    // Old state files omit the optional field; never infer masks from the catalog.
    let legacy : StoredTable = @json.from_json({
      "oid": 42,
      "phase": ToJson::to_json(table.phase),
    })
    assert_true(legacy.decoding is None)
  })
}

///|
async test "pending decoder requires relation before truncate and replacement masks win" {
  let dir = @fs.tmpdir(prefix="mooncdc-decoding-pending-")
  defer @fs.rmdir(dir, recursive=true)
  with_file_schema_store(dir + "/schemas", identity="decoder", schemas => {
    let initial = ddl_snapshot(100, 0)
    schemas.put(initial)
    let relation = initial.relation(["first", "second"])
    let decoder = PipelineDecoding::new(schemas, { value: 150, })
    ignore(
      decoder.transaction(
        decoding_tx(160, [RelationChanged(relation)]),
        [],
        "pub",
      ),
    )
    ignore(
      decoder.transaction(decoding_tx(200, [decoding_message(190)]), [], "pub"),
    )
    try
      decoder.transaction(
        decoding_tx(210, [
          Truncate([relation], cascade=false, restart_identity=false),
        ]),
        [],
        "pub",
      )
    catch {
      UnsafeResume(_) => ()
      error => raise error
    } noraise {
      _ => fail("truncate used pending fallback")
    }
    let changed = {
      ..relation,
      columns: relation.columns.map(c => { ..c, key: true, }),
    }
    let resolved = decoder.transaction(
      decoding_tx(220, [
        RelationChanged(changed),
        Truncate([changed], cascade=false, restart_identity=false),
      ]),
      [],
      "pub",
    )
    assert_eq(
      resolved.replicated_schemas(0)[0].schema.id,
      ddl_snapshot(200, 190).id,
    )
    assert_eq(resolved.replicated_schemas(1)[0].identity, [true, true, false])
  })
}

///|
async test "handover masks survive until durable checkpoint and materialized connection then permit pruning" {
  let dir = @fs.tmpdir(prefix="mooncdc-decoding-prune-")
  defer @fs.rmdir(dir, recursive=true)
  with_file_schema_store(dir + "/schemas", identity="decoder", schemas => {
    let initial = ddl_snapshot(100, 0)
    let current = ddl_snapshot(200, 190)
    for schema in [initial, current, ddl_snapshot(300, 290)] {
      schemas.put(schema)
    }
    let relation = initial.relation(["first", "second"])
    let store : PipelineStore = {
      path: dir + "/state",
      session: None,
      mutex: @async.Mutex(),
      state: {
        ..pipeline_fixture(),
        tables: [
          {
            oid: 42U,
            phase: Catchup("0/96"),
            decoding: Some(StoredDecoding::capture(initial.project(relation))),
          },
        ],
      },
    }
    store.update(state => state)
    store_checkpoint(
      store.path + ".checkpoint",
      store.state.source,
      { value: 120, },
      None,
    )
    let decoder = PipelineDecoding::new(schemas, { value: 120, })
    let target : CleanupDestination = {
      bounds: Map([(42U, current.id)]),
      fail: false,
    }
    let sink : PipelineDestination = {
      copy_monitor_interval_ms: 10000,
      memory: None,
      batch: None,
      destination: target,
      mutex: @async.Mutex(),
      store,
      connection: {
        hostaddr: None,
        keepalive: TcpKeepaliveConfig::new(),
        host: "127.0.0.1",
        port: 1,
        user: "unused",
        password: "",
        database: "unused",
        security: LocalPlaintext,
        timeout_ms: 100,
      },
      control: ApplyControl(),
      batch_rows: 1,
      copy_connections_per_table: 1,
      table_sync_copy: IncludeAllTables,
      slot_failover: false,
      schema_store: Some(schemas),
      frontier: None,
      main_lsn: { value: 250, },
      copy_jobs: [],
      copy_ready: @async.CondVar::Cond(),
      retry_delay_ms: 10,
      max_table_retries: 2,
      retry_attempts: Map([]),
      cleanup_candidates: Map([(42U, true)]),
      decoding: Some(decoder),
      copy_decoding: Map([]),
    }
    sink.cleanup_schemas()
    assert_true(store.state.tables[0].decoding is Some(_))
    assert_eq(schemas.all().length(), 3)
    store_checkpoint(
      store.path + ".checkpoint",
      store.state.source,
      { value: 250, },
      None,
    )
    sink.cleanup_schemas()
    assert_true(store.state.tables[0].decoding is Some(_))
    assert_eq(schemas.all().length(), 3)
    decoder.tables[42U] = Pending(current, None)
    sink.cleanup_candidates[42U] = true
    sink.cleanup_schemas()
    assert_true(store.state.tables[0].decoding is Some(_))
    decoder.tables[42U] = Complete(current.project(relation))
    @fs.mkdir(store.path + ".tmp")
    sink.cleanup_candidates[42U] = true
    sink.cleanup_schemas()
    assert_true(store.state.tables[0].decoding is Some(_))
    assert_eq(schemas.all().length(), 3)
    assert_true(sink.cleanup_candidates.contains(42U))
    @fs.rmdir(store.path + ".tmp")
    sink.cleanup_schemas()
    assert_true(read_pipeline_state(store.path).tables[0].decoding is None)
    assert_eq(schemas.all().map(schema => schema.id.commit_lsn.value), [
      200UL, 300UL,
    ])
  })
}
