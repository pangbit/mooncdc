///|
test "timeline ancestry uses the stored ancestor fork and rejects malformed history" {
  let history = "# history\n1\t0/50\tfirst promotion\n2  0/90  second promotion\n4\t0/90\tthird promotion\n"
  assert_eq(timeline_fork(history, 1U, 5U), Lsn::parse("0/50"))
  assert_eq(timeline_fork(history, 2U, 5U), Lsn::parse("0/90"))
  assert_eq(timeline_fork(history, 4U, 5U), Lsn::parse("0/90"))
  assert_eq(timeline_filename(2U), "00000002.history")
  assert_eq(timeline_filename(0xffffffffU), "FFFFFFFF.history")
  for
    malformed in [
      "",
      "# empty\n",
      "1\t0/50\treason",
      "1 0/50\n",
      "01 0/50 reason\n",
      "0 0/50 reason\n",
      "4294967296 0/50 reason\n",
      "2 0/50 reason\n",
      "1 0/50 reason\n1 0/60 reason\n",
      "1 0/50 reason\n2 0/40 reason\n",
      "1 0/50 reason\n5 0/60 reason\n",
      "1 0/0 reason\n",
      "1 nonsense reason\n",
      "1 0/50 reason\n".repeat(4097),
      "#".repeat(1048576) + "\n",
    ] {
    try timeline_fork(malformed, 1U, 5U) catch {
      _ => ()
    } noraise {
      _ => fail("malformed history accepted")
    }
  }
  try timeline_fork(history, 3U, 5U) catch {
    UnsafeResume(_) => ()
    error => raise error
  } noraise {
    _ => fail("unrelated ancestor accepted")
  }
}

///|
test "promotion proof includes unacknowledged writes snapshot and table cutoffs" {
  let state = { ..pipeline_fixture(), write_horizon: Some("0/60"), }
  assert_eq(
    required_ancestor_position(state, Lsn::parse("0/20")),
    Lsn::parse("0/70"),
  )
  assert_eq(
    required_ancestor_position(
      { ..state, write_horizon: Some("0/90"), },
      Lsn::parse("0/20"),
    ),
    Lsn::parse("0/90"),
  )
  assert_eq(
    required_ancestor_position(state, Lsn::parse("0/A0")),
    Lsn::parse("0/A0"),
  )
  assert_eq(
    required_ancestor_position(
      { ..state, initial_lsn: Some("0/B0"), },
      Lsn::parse("0/20"),
    ),
    Lsn::parse("0/B0"),
  )
  try required_ancestor_position(pipeline_fixture(), Lsn::parse("0/20")) catch {
    UnsafeResume(_) => ()
    error => raise error
  } noraise {
    _ => fail("unknown legacy horizon accepted")
  }
  let legacy = ToJson::to_json(pipeline_fixture())
  guard legacy is Object(fields) else { fail("object required") }
  fields.remove("write_horizon")
  fields.remove("promotion")
  let legacy : PipelineState = @json.from_json(Json::object(fields))
  assert_eq(parse_pipeline_state(state_bytes(legacy)).write_horizon, None)
}

///|
test "promotion intents reject inconsistent source position and horizon bindings" {
  let state = { ..pipeline_fixture(), write_horizon: Some("0/70"), }
  let intent : TimelinePromotion = {
    from: state.source,
    to: { ..state.source, timeline: "2", },
    position: "0/60",
    fork: "0/80",
    horizon: "0/70",
  }
  ignore(
    parse_pipeline_state(state_bytes({ ..state, promotion: Some(intent), })),
  )
  for
    bad in [
      { ..intent, from: { ..intent.from, database: "other", }, },
      { ..intent, to: { ..intent.to, slot: "other", }, },
      { ..intent, to: { ..intent.to, timeline: "1", }, },
      { ..intent, position: "0/90", },
      { ..intent, position: "0/0", },
      { ..intent, horizon: "0/90", },
      { ..intent, fork: "0/50", },
    ] {
    try
      parse_pipeline_state(state_bytes({ ..state, promotion: Some(bad), }))
    catch {
      CheckpointError(_) => ()
      error => raise error
    } noraise {
      _ => fail("inconsistent promotion intent accepted")
    }
  }
}

///|
async test "write horizon persists before partial target admission and never upgrades legacy state" {
  let dir = @fs.tmpdir(prefix="mooncdc-horizon-")
  defer @fs.rmdir(dir, recursive=true)
  let store : PipelineStore = {
    path: dir + "/state",
    session: None,
    mutex: @async.Mutex(),
    state: { ..pipeline_fixture(), write_horizon: Some("0/10"), },
  }
  store.update(state => state)
  store_checkpoint(
    store.path + ".checkpoint",
    store.state.source,
    Lsn::parse("0/10"),
    None,
  )
  let target : WalBoundaryDestination = {
    status: Durable,
    batches: [],
    flushes: 0,
  }
  let sink = wal_pipeline_sink(store, target)
  let decoder = Decoder::new(4096, 100, 10)
  ignore(decoder.decode_event(begin_fixture()))
  let relation = decoder
    .decode_event(relation_fixture("items"))
    .batch(store.state.source, "s", 1)
  assert_eq(
    wal_write_horizon(relation.internal_events(), Some(relation)),
    Lsn::parse("0/65"),
  )
  @fs.mkdir(store.path + ".tmp")
  try Destination::write_wal_batch(sink, relation) catch {
    _ => ()
  } noraise {
    _ => fail("failed horizon persistence admitted target write")
  }
  assert_eq(target.batches.length(), 0)
  assert_eq(store.state.write_horizon, Some("0/10"))
  @fs.rmdir(store.path + ".tmp")
  ignore(Destination::write_wal_batch(sink, relation))
  assert_eq(read_pipeline_state(store.path).write_horizon, Some("0/65"))
  assert_eq(
    load_checkpoint(store.path + ".checkpoint", store.state.source),
    Lsn::parse("0/10"),
  )
  store.update(state => { ..state, write_horizon: None, })
  store.record_write_horizon(Lsn::parse("0/FFFF"))
  assert_eq(read_pipeline_state(store.path).write_horizon, None)
}

///|
async test "promotion record boundaries recover without changing durable position or table progress" {
  for backend in ["legacy", "file"] {
    for phase in ["after_intent", "after_checkpoint", "after_complete"] {
      let dir = @fs.tmpdir(prefix="mooncdc-promotion-boundary-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/state"
      let state = { ..pipeline_fixture(), write_horizon: Some("0/70"), }
      let intent : TimelinePromotion = {
        from: state.source,
        to: { ..state.source, timeline: "2", },
        position: "0/70",
        fork: "0/80",
        horizon: "0/70",
      }
      let exercise = async fn(session : StateSession?) -> Unit {
        let store : PipelineStore = {
          path,
          session,
          state,
          mutex: @async.Mutex(),
        }
        store.update(state => state)
        store_checkpoint(
          path + ".checkpoint",
          state.source,
          Lsn::parse(intent.position),
          session,
        )
        try
          store.commit_promotion(intent, boundary=at => {
            if at == phase {
              raise PipelineProbeFailure
            }
          })
        catch {
          PipelineProbeFailure => ()
          error => raise error
        } noraise {
          _ => fail("boundary not injected")
        }
        let recovered = parse_pipeline_state(
          load_record(path, Pipeline, session).unwrap(),
        )
        let cp = parse_checkpoint_record(
          load_record(path + ".checkpoint", ReplicationCheckpoint, session).unwrap(),
        )
        assert_eq(cp.lsn, intent.position)
        assert_true(
          cp.source ==
          (if phase == "after_intent" { intent.from } else { intent.to }),
        )
        let next : PipelineStore = {
          path,
          session,
          state: recovered,
          mutex: @async.Mutex(),
        }
        if recovered.promotion is Some(pending) {
          next.commit_promotion(pending)
        }
        assert_true(next.state.source == intent.to)
        assert_true(next.state.promotion is None)
        assert_eq(
          stored_checkpoint(path + ".checkpoint", intent.to, session),
          Lsn::parse(intent.position),
        )
        assert_eq(next.state.tables.to_json(), state.tables.to_json())
        assert_eq(next.state.write_horizon, state.write_horizon)
      }
      if backend == "legacy" {
        exercise(None)
      } else {
        with_state_store(FileStateStore::new(dir), session => {
          exercise(Some(session))
        })
      }
    }
  }
}
