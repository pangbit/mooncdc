///|
fn wal_test_source() -> Source {
  {
    system_id: "system",
    timeline: "1",
    database: "db",
    slot: "slot",
    publication: "pub",
  }
}

///|
test "event decoder releases rows while retaining cumulative limits and immutable metadata" {
  let decoder = Decoder::new(1000000, 1002, 10)
  assert_true(decoder.decode_event(begin_fixture()) is Begin({ xid: 7, .. }))
  ignore(decoder.decode_event(relation_fixture("before")))
  let first = decoder.decode_event(
    tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n"),
  )
  for _ in 0..<999 {
    ignore(
      decoder.decode_event(tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n")),
    )
    assert_true(decoder.changes.is_empty())
  }
  ignore(decoder.decode_event(relation_fixture("after")))
  assert_true(
    first
    is Change(_, Insert({ name: "before", .. }, [Text("1"), Null]), Some(0)),
  )
  try
    decoder.decode_event(tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n"))
  catch {
    LimitExceeded("transaction events") => ()
    error => raise error
  } noraise {
    _ => fail("draining rows reset cumulative event count")
  }
  let begin = begin_fixture()
  let relation = relation_fixture("items")
  let row = tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n")
  let decoder = Decoder::new(
    begin.length() + relation.length() + row.length(),
    100,
    10,
  )
  ignore(decoder.decode_event(begin))
  ignore(decoder.decode_event(relation))
  ignore(decoder.decode_event(row))
  try decoder.decode_event(commit_fixture()) catch {
    LimitExceeded("transaction bytes") => ()
    error => raise error
  } noraise {
    _ => fail("draining rows reset cumulative byte count")
  }
}

///|
test "event fragments preserve global ordinals across metadata replay and table projection" {
  for repeat in [false, true] {
    let decoder = Decoder::new(4096, 100, 10)
    ignore(decoder.decode_event(begin_fixture()))
    ignore(decoder.decode_event(relation_fixture("items")))
    if repeat {
      ignore(decoder.decode_event(relation_fixture("items")))
    }
    let first = decoder
      .decode_event(tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n"))
      .batch(wal_test_source(), "session", 1)
    ignore(decoder.decode_event(b"Y\x00\x00\x00\x19public\x00custom\x00"))
    let second = decoder
      .decode_event(b"T\x00\x00\x00\x01\x00\x00\x00\x00\x01")
      .batch(wal_test_source(), "session", 1)
    assert_eq(first.event_id(0), Some("system/db/slot/0/64/0"))
    assert_eq(second.event_id(0), Some("system/db/slot/0/64/1"))
    let projected = project_tables(
      second.internal_events(),
      Map([(1U, Lsn::parse("0/10"))]),
      "pub",
    )
    assert_eq(
      second.with_events(projected).event_id(0),
      Some("system/db/slot/0/64/1/table/1"),
    )
    assert_eq(first.end_lsn, None)
    let boundary = decoder
      .decode_event(commit_fixture())
      .batch(wal_test_source(), "session", 1)
    assert_eq(boundary.end_lsn, Some(Lsn::parse("0/65")))
    assert_true(boundary.changes.is_empty())
    ignore(decoder.decode_event(begin_fixture(lsn=200)))
    ignore(decoder.decode_event(relation_fixture("items")))
    assert_true(
      decoder.decode_event(row_fixture_for_wal()) is Change(_, _, Some(0)),
    )
  }
}

///|
fn row_fixture_for_wal() -> Bytes {
  tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n")
}

///|
test "event decoder rejects malformed Commit after partial delivery and accumulator releases values" {
  for bad in [commit_fixture(lsn=101), commit_fixture() + b"x", b"C\x01", b"S"] {
    let decoder = Decoder::new(4096, 100, 10)
    let accumulator = WalAccumulator::new()
    accumulator.push(
      decoder.decode_event(begin_fixture()).batch(wal_test_source(), "s", 1),
    )
    accumulator.push(
      decoder
      .decode_event(relation_fixture("items"))
      .batch(wal_test_source(), "s", 1),
    )
    accumulator.push(
      decoder
      .decode_event(row_fixture_for_wal())
      .batch(wal_test_source(), "s", 1),
    )
    guard accumulator.take() is Some(partial) else {
      fail("partial batch missing")
    }
    assert_eq(partial.changes.length(), 2)
    assert_eq(partial.end_lsn, None)
    assert_true(accumulator.header is Some({ changes: [], ids: [], .. }))
    assert_true(accumulator.take() is None)
    try decoder.decode_event(bad) catch {
      Protocol(_) => ()
      error => raise error
    } noraise {
      _ => fail("malformed Commit accepted")
    }
  }
}
