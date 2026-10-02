///|
fn begin_fixture(lsn? : UInt64 = 100) -> Bytes {
  let b = @buffer.Buffer()
  b.write_byte(b'B')
  b.write_uint64_be(lsn)
  b.write_int64_be(42)
  b.write_int_be(7)
  b.to_bytes()
}

///|
fn commit_fixture(lsn? : UInt64 = 100) -> Bytes {
  let b = @buffer.Buffer()
  b.write_byte(b'C')
  b.write_byte(b'\x00')
  b.write_uint64_be(lsn)
  b.write_uint64_be(lsn + 1)
  b.write_int64_be(42)
  b.to_bytes()
}

///|
fn relation_fixture(name : String) -> Bytes {
  let b = @buffer.Buffer()
  b.write_byte(b'R')
  b.write_int_be(1)
  cstring(b, "public")
  cstring(b, name)
  b.write_byte(b'f')
  b.write_uint16_be(2)
  for (name, oid) in [("id", 23), ("value", 25)] {
    b.write_byte(b'\x01')
    cstring(b, name)
    b.write_int_be(oid)
    b.write_int_be(-1)
  }
  b.to_bytes()
}

///|
fn tuple_fixture(tag : Byte, fields : Bytes) -> Bytes {
  let b = @buffer.Buffer()
  b.write_byte(tag)
  b.write_int_be(1)
  b.write_bytes(fields)
  b.to_bytes()
}

///|
test "decoder emits only commit and snapshots relation changes" {
  let d = Decoder::new(4096, 100, 10)
  assert_eq(d.decode(begin_fixture()), None)
  ignore(d.decode(relation_fixture("before")))
  ignore(d.decode(tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n")))
  ignore(d.decode(relation_fixture("after")))
  ignore(d.decode(tuple_fixture(b'U', b"N\x00\x02t\x00\x00\x00\x011u")))
  let result = d.decode(commit_fixture())
  guard result is Some((7, _, _, 42, changes)) else { fail("missing commit") }
  assert_true(changes[1] is Insert({ name: "before", .. }, [Text("1"), Null]))
  assert_true(
    changes[3]
    is Update({ name: "after", .. }, None, [Text("1"), UnchangedToast]),
  )
}

///|
test "decoder rejects truncation at every offset unknown tags lengths and trailing data" {
  let fixtures = [
    relation_fixture("items"),
    tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n"),
    commit_fixture(),
  ]
  for full in fixtures {
    for n in 0..<full.length() {
      let d = Decoder::new(4096, 100, 10)
      ignore(d.decode(begin_fixture()))
      ignore(d.decode(relation_fixture("items")))
      try d.decode(full[:n].to_owned()) catch {
        _ => ()
      } noraise {
        _ => fail("truncated message accepted")
      }
    }
  }
  for
    bytes in [
      b"?",
      b"S",
      tuple_fixture(b'I', b"N\x00\x02t\xff\xff\xff\xffn"),
      tuple_fixture(b'I', b"N\x00\x02bn"),
      commit_fixture() + b"x",
    ] {
    let d = Decoder::new(4096, 100, 10)
    ignore(d.decode(begin_fixture()))
    ignore(d.decode(relation_fixture("items")))
    try d.decode(bytes) catch {
      _ => ()
    } noraise {
      _ => fail("invalid message accepted")
    }
  }
}

///|
test "decoder limits transaction events and metadata and checks transaction sequence" {
  let d = Decoder::new(32, 100, 10)
  ignore(d.decode(begin_fixture()))
  try d.decode(relation_fixture("items")) catch {
    LimitExceeded(_) => ()
    e => raise e
  } noraise {
    _ => fail("byte cap")
  }
  let d = Decoder::new(4096, 1, 10)
  ignore(d.decode(begin_fixture()))
  ignore(d.decode(relation_fixture("items")))
  try d.decode(relation_fixture("changed")) catch {
    LimitExceeded(_) => ()
    e => raise e
  } noraise {
    _ => fail("event cap")
  }
  let d = Decoder::new(4096, 100, 0)
  ignore(d.decode(begin_fixture()))
  try d.decode(relation_fixture("items")) catch {
    LimitExceeded(_) => ()
    e => raise e
  } noraise {
    _ => fail("metadata cap")
  }
  let d = Decoder::new(4096, 100, 10)
  try d.decode(commit_fixture()) catch {
    Protocol(_) => ()
    e => raise e
  } noraise {
    _ => fail("commit without begin")
  }
  ignore(d.decode(begin_fixture()))
  try d.decode(begin_fixture()) catch {
    Protocol(_) => ()
    e => raise e
  } noraise {
    _ => fail("nested begin")
  }
}

///|
test "delete key full truncate type and origin" {
  let d = Decoder::new(4096, 100, 10)
  ignore(d.decode(begin_fixture()))
  ignore(d.decode(relation_fixture("items")))
  ignore(d.decode(tuple_fixture(b'D', b"K\x00\x02t\x00\x00\x00\x011n")))
  ignore(d.decode(tuple_fixture(b'D', b"O\x00\x02t\x00\x00\x00\x011n")))
  ignore(d.decode(b"Y\x00\x00\x00\x19public\x00custom\x00"))
  ignore(d.decode(b"O\x00\x00\x00\x00\x00\x00\x00\x01origin\x00"))
  ignore(d.decode(b"T\x00\x00\x00\x01\x03\x00\x00\x00\x01"))
  guard d.decode(commit_fixture()) is Some((_, _, _, _, changes)) else {
    fail("commit")
  }
  assert_true(changes[1] is Delete(_, Key(_)))
  assert_true(changes[2] is Delete(_, Full(_)))
  assert_true(changes[3] is TypeChanged({ oid: 25, name: "custom", .. }))
  assert_true(changes[4] is Origin(_, "origin"))
  assert_true(changes[5] is Truncate([_], cascade=true, restart_identity=true))
}

///|
test "checkpoint corruption and source slot publication binding" {
  let source : Source = {
    system_id: "123",
    timeline: "1",
    database: "db",
    slot: "s",
    publication: "p",
  }
  let lsn = Lsn::parse("FFFF/FFFFFFFF")
  let bytes = checkpoint_bytes(source, lsn)
  assert_eq(parse_checkpoint(bytes, source), lsn)
  for bad in [b"", bytes[:64].to_owned(), bytes + b"x"] {
    try parse_checkpoint(bad, source) catch {
      CheckpointError(_) => ()
      e => raise e
    } noraise {
      _ => fail("corruption")
    }
  }
  for
    wrong in [
      { ..source, slot: "other", },
      { ..source, system_id: "456", },
      { ..source, publication: "other", },
      { ..source, database: "other", },
      { ..source, timeline: "2", },
    ] {
    try parse_checkpoint(bytes, wrong) catch {
      CheckpointError(_) => ()
      e => raise e
    } noraise {
      _ => fail("binding")
    }
  }
}
