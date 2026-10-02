///|
async test "file state sessions fence ownership preserve exact metadata and reject rollback" {
  let dir = @fs.tmpdir(prefix="mooncdc-store-")
  defer @fs.rmdir(dir, recursive=true)
  let store = FileStateStore::new(dir)
  let other = FileStateStore::new(dir)
  let source = pipeline_fixture().source
  let escaped = with_state_store(store, session => {
    session.save_checkpoint(source, Lsn::parse("0/20"))
    try session.save_checkpoint(source, Lsn::parse("0/10")) catch {
      CheckpointError(_) => ()
      error => raise error
    } noraise {
      _ => fail("checkpoint rollback accepted")
    }
    try session.checkpoint({ ..source, slot: "another", }) catch {
      CheckpointError(_) => ()
      error => raise error
    } noraise {
      _ => fail("different lineage accepted")
    }
    assert_eq(session.checkpoint(source), Lsn::parse("0/20"))
    let value = pg_json("{\"offset\":18446744073709551616,\"tiny\":1e-400}")
    session.put_metadata(42U, value)
    assert_true(pg_json_equal(session.get_metadata(42U).unwrap(), value))
    try with_state_store(other, _ => ()) catch {
      CheckpointError(_) => ()
      error => raise error
    } noraise {
      _ => fail("second owner admitted")
    }
    session
  })
  with_state_store(other, session => {
    assert_eq(session.checkpoint(source), Lsn::parse("0/20"))
    try store.save(escaped.owner, Pipeline, b"stale") catch {
      CheckpointError(_) => ()
      error => raise error
    } noraise {
      _ => fail("stale owner wrote")
    }
    try escaped.put_metadata(42U, "stale") catch {
      CheckpointError(_) => ()
      error => raise error
    } noraise {
      _ => fail("escaped session wrote")
    }
    @fs.write_file(dir + "/table-42", "corrupt")
    try session.get_metadata(42U) catch {
      CheckpointError(_) => ()
      error => raise error
    } noraise {
      _ => fail("corrupt metadata accepted")
    }
    let pipeline : PipelineStore = {
      path: "",
      session: Some(session),
      state: pipeline_fixture(),
      mutex: @async.Mutex(),
    }
    pipeline.update(state => state)
    @fs.mkdir(dir + "/pipeline.tmp")
    try pipeline.set_phase(1U, Ready("0/80")) catch {
      _ => ()
    } noraise {
      _ => fail("failed store write ignored")
    }
    assert_true(pipeline.state.tables[0].phase is Catchup("0/50"))
    assert_true(
      state_store_status(other).unwrap().tables[0].phase is Catchup(lsn) &&
      lsn == Lsn::parse("0/50"),
    )
  })
  try
    @async.with_timeout(10, () => {
      with_state_store(store, _ => @async.sleep(1000))
    })
  catch {
    @async.TimeoutError => ()
    error => raise error
  } noraise {
    _ => fail("expected cancellation")
  }
  with_state_store(other, _ => ())
}

///|
async test "live PostgreSQL state store exclusive ownership and lost session never reconnects" {
  if live_config() is Some(config) {
    let sql = connect(config, 16777216, false)
    defer sql.close()
    ignore(sql.query("CREATE SCHEMA cdc_state_store"))
    defer ignore(sql.query("DROP SCHEMA cdc_state_store CASCADE"))
    let first = PostgresStateStore::new(
      config,
      store_id="quoted ' \\ key",
      schema="cdc_state_store",
      table="state\"table",
    )
    let second = PostgresStateStore::new(
      config,
      store_id="quoted ' \\ key",
      schema="cdc_state_store",
      table="state\"table",
    )
    assert_true(state_store_status(second) is None)
    let independent = PostgresStateStore::new(
      config,
      store_id="independent",
      schema="cdc_state_store",
      table="state\"table",
    )
    @async.all([
      () => {
        with_state_store(first, session => session.put_metadata(1U, "first"))
      },
      () => {
        with_state_store(independent, session => {
          session.put_metadata(1U, "independent")
        })
      },
    ])
    |> ignore
    with_state_store(independent, session => {
      assert_eq(session.get_metadata(1U), Some(Json::string("independent")))
    })
    with_state_store(first, session => {
      session.save(Pipeline, state_bytes(pipeline_fixture()))
      session.put_metadata(42U, "durable")
      assert_eq(state_store_status(second).unwrap().destination_id, "target")
      try with_state_store(second, _ => ()) catch {
        CheckpointError(_) => ()
        error => raise error
      } noraise {
        _ => fail("second PostgreSQL owner admitted")
      }
      let pid = cell(first.query_owned("SELECT pg_backend_pid()::text"), 0, 0)
      assert_eq(
        cell(sql.query("SELECT pg_terminate_backend(\{pid},5000)::text"), 0, 0),
        "true",
      )
      try session.put_metadata(42U, "uncertain") catch {
        _ => ()
      } noraise {
        _ => fail("lost owner wrote")
      }
      with_state_store(second, next => {
        assert_eq(next.get_metadata(42U), Some(Json::string("durable")))
        next.put_metadata(42U, "new owner")
        try session.put_metadata(42U, "stale") catch {
          CheckpointError(_) => ()
          error => raise error
        } noraise {
          _ => fail("old owner reconnected")
        }
      })
    })
    with_state_store(first, session => {
      assert_eq(session.get_metadata(42U), Some(Json::string("new owner")))
    })
    println(
      "LIVE: PostgreSQL state ownership, atomic records, quoted identity and closed-session fencing passed",
    )
  }
}
