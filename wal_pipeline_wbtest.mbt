///|
priv struct WalBoundaryDestination {
  status : WriteStatus
  batches : Array[WalBatch]
  mut flushes : Int32
}

///|
impl Destination for WalBoundaryDestination with fn supports_wal_batches(_self) {
  true
}

///|
impl Destination for WalBoundaryDestination with fn reset_table(
  _self,
  _r,
  attempt~,
) {
  ignore(attempt)
}

///|
impl Destination for WalBoundaryDestination with fn write_rows(
  _self,
  _r,
  _rows,
  _id,
) {
  Durable
}

///|
impl Destination for WalBoundaryDestination with fn write_transaction(
  _self,
  _tx,
) {
  fail("partial transaction callback")
}

///|
impl Destination for WalBoundaryDestination with fn write_wal_batch(self, batch) {
  self.batches.push(batch)
  self.status
}

///|
impl Destination for WalBoundaryDestination with fn flush(self) {
  self.flushes += 1
}

///|
fn wal_pipeline_sink(
  store : PipelineStore,
  destination : &Destination,
) -> PipelineDestination raise {
  {
    destination,
    store,
    mutex: @async.Mutex(),
    connection: ConnectionConfig::new(
      host="127.0.0.1",
      port=1,
      user="unused",
      database="unused",
      security=LocalPlaintext,
      timeout_ms=100,
    ),
    control: ApplyControl(),
    memory: None,
    batch: None,
    batch_rows: 1,
    copy_connections_per_table: 1,
    copy_monitor_interval_ms: 10000,
    table_sync_copy: IncludeAllTables,
    slot_failover: false,
    schema_store: None,
    frontier: None,
    main_lsn: Lsn::parse("0/10"),
    copy_jobs: [],
    copy_ready: @async.CondVar::Cond(),
    retry_delay_ms: 10,
    max_table_retries: 2,
    retry_attempts: Map([]),
    cleanup_candidates: Map([]),
    decoding: None,
    copy_decoding: Map([]),
  }
}

///|
async test "event pipeline never advances main or table handover at a partial durable barrier" {
  for status in [Accepted, Durable] {
    let dir = @fs.tmpdir(prefix="mooncdc-wal-frontier-")
    defer @fs.rmdir(dir, recursive=true)
    let store : PipelineStore = {
      path: dir + "/state",
      session: None,
      mutex: @async.Mutex(),
      state: pipeline_fixture(),
    }
    store.update(state => state)
    let target : WalBoundaryDestination = { status, batches: [], flushes: 0, }
    let sink = wal_pipeline_sink(store, target)
    assert_true(Destination::supports_wal_batches(sink))
    let decoder = Decoder::new(4096, 100, 10)
    ignore(decoder.decode_event(begin_fixture()))
    let relation = decoder
      .decode_event(relation_fixture("items"))
      .batch(store.state.source, "s", 1)
    ignore(Destination::write_wal_batch(sink, relation))
    let row = decoder
      .decode_event(row_fixture_for_wal())
      .batch(store.state.source, "s", 1)
    ignore(Destination::write_wal_batch(sink, row))
    Destination::flush(sink)
    assert_eq(sink.main_lsn, Lsn::parse("0/10"))
    assert_eq(sink.frontier, None)
    assert_true(
      read_pipeline_state(store.path).tables[0].phase is Catchup("0/50"),
    )
    assert_eq(
      target.batches[1].event_id(0),
      Some("system/db/owned_slot/0/64/0"),
    )
    let commit = decoder
      .decode_event(commit_fixture())
      .batch(store.state.source, "s", 1)
    ignore(Destination::write_wal_batch(sink, commit))
    if status == Accepted {
      assert_eq(sink.frontier, Some(Lsn::parse("0/65")))
      assert_true(read_pipeline_state(store.path).tables[0].phase is Catchup(_))
      Destination::flush(sink)
    }
    assert_eq(sink.main_lsn, Lsn::parse("0/65"))
    assert_eq(sink.frontier, None)
    assert_true(
      read_pipeline_state(store.path).tables[0].phase is Ready("0/50"),
    )
    assert_true(target.flushes > 0)
  }
}

///|
async test "event schema lineage and message identity survive batch splits within one transaction" {
  let dir = @fs.tmpdir(prefix="mooncdc-wal-schema-")
  defer @fs.rmdir(dir, recursive=true)
  with_file_schema_store(dir + "/schemas", identity="wal-schema", schemas => {
    let initial = ddl_snapshot(100, 0)
    schemas.put(initial)
    let relation = initial.relation(["first", "second"])
    let decoder = PipelineDecoding::new(schemas, { value: 150, })
    ignore(
      decoder.transaction(
        decoding_tx(300, [RelationChanged(relation)]),
        [],
        "pub",
      ),
    )
    let whole = decoding_tx(400, [
      Insert(relation, [Text("1"), Text("before")]),
      decoding_message(350),
      Insert(relation, [Text("2"), Text("between")]),
      decoding_message(360),
      Insert(relation, [Text("3"), Text("after")]),
    ])
    for index, change in whole.changes {
      let fragment : WalBatch = {
        id: whole.id,
        xid: whole.xid,
        commit_lsn: whole.commit_lsn,
        commit_time_us: whole.commit_time_us,
        changes: [change],
        end_lsn: None,
        session: "s",
        sequence: 1,
        ids: [whole.event_id(index)],
        decoded_schemas: None,
      }
      let resolved = fragment.with_events(
        decoder.transaction(fragment.internal_events(), [], "pub"),
      )
      assert_eq(resolved.event_id(0), whole.event_id(index))
      if change is Insert(_, _) {
        let schema = resolved.replicated_schemas(0)[0]
        assert_eq(
          schema.schema.id,
          if index == 0 {
            initial.id
          } else if index == 2 {
            ddl_snapshot(400, 350).id
          } else {
            ddl_snapshot(400, 360).id
          },
        )
        assert_eq(schema.replicated, [true, true, false])
        assert_eq(schema.identity, [true, false, false])
      }
    }
  })
}
