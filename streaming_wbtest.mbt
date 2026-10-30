///|
fn stream_start(xid : UInt32, first~ : Bool = true) -> Bytes {
  let out = @buffer.Buffer()
  out.write_byte(b'S')
  out.write_uint_be(xid)
  out.write_byte(if first { b'\x01' } else { b'\x00' })
  out.to_bytes()
}

///|
fn stream_change(xid : UInt32, payload : Bytes) -> Bytes {
  let out = @buffer.Buffer()
  out.write_byte(payload[0])
  out.write_uint_be(xid)
  out.write_bytes(payload[1:])
  out.to_bytes()
}

///|
fn stream_commit(xid : UInt32, lsn~ : UInt64 = 100) -> Bytes {
  let out = @buffer.Buffer()
  out.write_byte(b'c')
  out.write_uint_be(xid)
  out.write_byte(b'\x00')
  out.write_uint64_be(lsn)
  out.write_uint64_be(lsn + 1)
  out.write_int64_be(42)
  out.to_bytes()
}

///|
fn stream_abort(xid : UInt32, subxid : UInt32) -> Bytes {
  let out = @buffer.Buffer()
  out.write_byte(b'A')
  out.write_uint_be(xid)
  out.write_uint_be(subxid)
  out.to_bytes()
}

///|
fn stream_insert(id : String) -> Bytes {
  tuple_fixture(b'I', b"N" + protocol_tuple([Text(id), Null]))
}

///|
test "v2 interleaves streams and ordinary transactions but exposes only commits" {
  let d = StreamDecoder::new(Decoder::new(4096, 100, 10), 4)
  for
    bytes in [
      stream_start(10),
      stream_change(10, relation_fixture("first")),
      stream_change(10, stream_insert("10")),
      b"E",
      stream_start(20),
      stream_change(20, relation_fixture("second")),
      stream_change(20, stream_insert("20")),
      b"E",
      begin_fixture(lsn=80),
      stream_insert("7"),
    ] {
    assert_eq(d.decode(bytes), None)
  }
  guard d.decode(commit_fixture(lsn=80)) is Some((7, _, _, 42, [Insert(r, _)])) else {
    fail("ordinary commit")
  }
  assert_eq(r.name, "second")
  guard d.decode(stream_commit(20, lsn=90))
    is Some((20, { value: 90, }, { value: 91, }, 42, [_, Insert(r, _)])) else {
    fail("stream 20 commit")
  }
  assert_eq(r.name, "second")
  assert_eq(d.decode(stream_start(10, first=false)), None)
  assert_eq(d.decode(stream_change(10, stream_insert("11"))), None)
  assert_eq(d.decode(b"E"), None)
  guard d.decode(stream_commit(10))
    is Some((10, _, _, _, [_, Insert(old, _), Insert(next, _)])) else {
    fail("stream 10 commit")
  }
  assert_eq(old.name, "first")
  assert_eq(next.name, "second")
  assert_eq(d.streams.length(), 0)
  assert_eq(d.bytes, 0)
  assert_eq(d.events, 0)
}

///|
test "v2 subabort truncates arrival offsets including nested and released subtransactions" {
  let d = StreamDecoder::new(Decoder::new(4096, 100, 10), 4)
  for
    bytes in [
      stream_start(10),
      stream_change(10, relation_fixture("items")),
      stream_change(10, stream_insert("keep")),
      stream_change(12, stream_insert("savepoint")),
      stream_change(11, stream_insert("nested-nonsorted")),
      stream_change(10, stream_insert("after-release")),
      b"E",
      stream_abort(10, 99),
      stream_abort(10, 12),
      stream_start(10, first=false),
      stream_change(13, stream_insert("after-rollback")),
      b"E",
    ] {
    assert_eq(d.decode(bytes), None)
  }
  guard d.decode(stream_commit(10)) is Some((_, _, _, _, changes)) else {
    fail("commit")
  }
  assert_eq(changes.length(), 3)
  assert_true(changes[1] is Insert(_, [Text("keep"), Null]))
  assert_true(changes[2] is Insert(_, [Text("after-rollback"), Null]))
  ignore(d.decode(stream_start(20)))
  ignore(d.decode(stream_change(20, stream_insert("abort"))))
  ignore(d.decode(b"E"))
  assert_eq(d.decode(stream_abort(20, 20)), None)
  assert_eq(d.streams.length(), 0)
  assert_eq(d.bytes, 0)
}

///|
test "v2 all streamed body types retain byte values and discard root aborts" {
  let d = StreamDecoder::new(Decoder::new(4096, 100, 10), 4)
  ignore(d.decode(stream_start(10)))
  for
    bytes in [
      relation_fixture("items"),
      b"Y\x00\x00\x00\x19public\x00custom\x00",
      tuple_fixture(b'I', b"N" + protocol_tuple([Binary(b"\x00\xff"), Null])),
      tuple_fixture(b'U', b"N" + protocol_tuple([Text("1"), UnchangedToast])),
      tuple_fixture(b'D', b"K" + protocol_tuple([Text("1"), Null])),
      b"T\x00\x00\x00\x01\x03\x00\x00\x00\x01",
      logical_fixture(true, b"\x00\xff"),
    ] {
    assert_eq(d.decode(stream_change(10, bytes)), None)
  }
  ignore(d.decode(b"O\x00\x00\x00\x00\x00\x00\x00\x01origin\x00"))
  ignore(d.decode(b"E"))
  guard d.decode(stream_commit(10)) is Some((_, _, _, _, changes)) else {
    fail("commit")
  }
  assert_eq(changes.length(), 8)
  assert_true(changes[2] is Insert(_, [Binary(b"\x00\xff"), Null]))
  assert_true(changes[3] is Update(_, None, [Text("1"), UnchangedToast]))
  assert_true(changes[4] is Delete(_, Key(_)))
  assert_true(changes[5] is Truncate([_], cascade=true, restart_identity=true))
  assert_true(changes[6] is Message({ content: b"\x00\xff", .. }))
  assert_true(changes[7] is Origin(_, "origin"))
}

///|
test "v2 bounds aggregate bytes events stream count and cumulative aborted quotas" {
  let d = StreamDecoder::new(Decoder::new(12, 100, 10), 2)
  ignore(d.decode(stream_start(10)))
  ignore(d.decode(b"E"))
  try d.decode(stream_start(20)) catch {
    LimitExceeded(_) => ()
    e => raise e
  } noraise {
    _ => fail("aggregate bytes")
  }
  let d = StreamDecoder::new(Decoder::new(4096, 100, 10), 1)
  ignore(d.decode(stream_start(10)))
  ignore(d.decode(b"E"))
  try d.decode(stream_start(20)) catch {
    LimitExceeded(_) => ()
    e => raise e
  } noraise {
    _ => fail("stream count")
  }
  let d = StreamDecoder::new(Decoder::new(4096, 2, 10), 4)
  ignore(d.decode(stream_start(10)))
  ignore(d.decode(stream_change(10, relation_fixture("items"))))
  ignore(d.decode(stream_change(11, stream_insert("abort"))))
  ignore(d.decode(b"E"))
  ignore(d.decode(stream_abort(10, 11)))
  assert_eq(d.streams[10].changes.length(), 1)
  ignore(d.decode(stream_start(10, first=false)))
  try d.decode(stream_change(10, stream_insert("more"))) catch {
    LimitExceeded(_) => ()
    e => raise e
  } noraise {
    _ => fail("subabort must not reset quota")
  }
  let d = StreamDecoder::new(Decoder::new(4096, 1, 10), 4)
  ignore(d.decode(stream_start(10)))
  ignore(d.decode(stream_change(10, relation_fixture("items"))))
  ignore(d.decode(b"E"))
  ignore(d.decode(begin_fixture()))
  try d.decode(stream_insert("ordinary")) catch {
    LimitExceeded(_) => ()
    e => raise e
  } noraise {
    _ => fail("ordinary shares aggregate events")
  }
  let d = StreamDecoder::new(Decoder::new(4096, 1, 10), 4)
  ignore(d.decode(stream_start(10)))
  ignore(d.decode(stream_change(10, relation_fixture("items"))))
  ignore(d.decode(b"E"))
  ignore(d.decode(stream_abort(10, 10)))
  ignore(d.decode(stream_start(20)))
  ignore(d.decode(stream_change(20, stream_insert("released"))))
}

///|
test "v2 strict boundary and body truncation rejects every offset and malformed state" {
  for
    full in [
      stream_start(20),
      b"E",
      stream_commit(10),
      stream_abort(10, 10),
      stream_change(10, relation_fixture("items")),
      stream_change(10, stream_insert("1")),
      stream_change(10, logical_fixture(true, b"content")),
    ] {
    for length in 0..<full.length() {
      let d = StreamDecoder::new(Decoder::new(4096, 100, 10), 4)
      ignore(d.decode(stream_start(10)))
      ignore(d.decode(stream_change(10, relation_fixture("items"))))
      if full[0] == b'S' || full[0] == b'c' || full[0] == b'A' {
        ignore(d.decode(b"E"))
      }
      try d.decode(full[:length].to_owned()) catch {
        Protocol(_) => ()
        e => raise e
      } noraise {
        _ => fail("truncated v2 accepted")
      }
    }
  }
  for
    bad in [
      b"E",
      stream_start(10, first=false),
      stream_start(0),
      b"S\x00\x00\x00\x0a\x02",
      stream_commit(99),
      stream_abort(99, 99),
      stream_start(10) + b"x",
    ] {
    let d = StreamDecoder::new(Decoder::new(4096, 100, 10), 4)
    try d.decode(bad) catch {
      Protocol(_) => ()
      e => raise e
    } noraise {
      _ => fail("malformed v2 accepted")
    }
  }
  for
    bad in [
      stream_start(20),
      begin_fixture(),
      commit_fixture(),
      stream_commit(10),
      stream_abort(10, 10),
      stream_change(0, stream_insert("1")),
      stream_change(10, logical_fixture(false, b"invalid")),
    ] {
    let d = StreamDecoder::new(Decoder::new(4096, 100, 10), 4)
    ignore(d.decode(stream_start(10)))
    ignore(d.decode(stream_change(10, relation_fixture("items"))))
    try d.decode(bad) catch {
      Protocol(_) => ()
      e => raise e
    } noraise {
      _ => fail("malformed active stream accepted")
    }
  }
  for
    bad in [
      stream_start(10),
      stream_commit(10, lsn=0),
      stream_abort(10, 0),
      b"c\x00\x00\x00\x0a\x01" + stream_commit(10)[6:].to_owned(),
      stream_abort(10, 10) + b"x",
    ] {
    let d = StreamDecoder::new(Decoder::new(4096, 100, 10), 4)
    ignore(d.decode(stream_start(10)))
    ignore(d.decode(b"E"))
    try d.decode(bad) catch {
      Protocol(_) => ()
      e => raise e
    } noraise {
      _ => fail("malformed stopped stream accepted")
    }
  }
}
