///|
priv struct StreamingVector {
  name : String
  hex : String
  valid : Bool
  in_stream : Bool
} derive(ToJson)

///|
fn streaming_vectors() -> Array[StreamingVector] raise {
  let vectors : Array[StreamingVector] = []
  let add = fn(
    name : String,
    bytes : Bytes,
    valid : Bool,
    in_stream : Bool,
  ) -> Unit {
    vectors.push({
      name,
      hex: @crypto.bytes_to_hex_string(bytes),
      valid,
      in_stream,
    })
  }
  for
    (name, bytes) in [
      ("start", stream_start(7)),
      ("continue", stream_start(7, first=false)),
      ("stop", b"E"),
      ("commit", stream_commit(7)),
      ("abort", stream_abort(7, 7)),
      ("subabort", stream_abort(7, 8)),
    ] {
    add(name, bytes, true, false)
    add(name + "-trailing", bytes + b"x", false, false)
    for n in 0..<bytes.length() {
      add(
        name + "-truncated-" + n.to_string(),
        bytes[:n].to_owned(),
        false,
        false,
      )
    }
  }
  for vector in protocol_vectors() {
    let bytes = cell_hex(vector.hex[:])
    // A streamed logical message is transactional; outside-stream messages are
    // exercised by the original protocol reference suite and live tests.
    if bytes.get(0) == Some(b'M') && bytes.get(1) == Some(b'\x00') {
      continue
    }
    add(
      "stream-" + vector.name,
      if bytes.is_empty() {
        bytes
      } else {
        stream_change(7, bytes)
      },
      vector.valid,
      true,
    )
  }
  for
    (name, bytes) in [
      ("relation", relation_fixture("protocol")),
      ("type", b"Y\x00\x00\x00\x19public\x00custom\x00"),
      ("truncate", b"T\x00\x00\x00\x01\x03\x00\x00\x00\x01"),
    ] {
    let full = stream_change(7, bytes)
    add(name, full, true, true)
    add(name + "-trailing", full + b"x", false, true)
    for n in 0..<full.length() {
      add(
        name + "-truncated-" + n.to_string(),
        full[:n].to_owned(),
        false,
        true,
      )
    }
  }
  add("start-bad-flag", b"S\x00\x00\x00\x07\x02", false, false)
  add(
    "commit-bad-flag",
    b"c\x00\x00\x00\x07\x01" + stream_commit(7)[6:].to_owned(),
    false,
    false,
  )
  vectors
}

///|
fn streaming_normalize(vector : StreamingVector) -> Json raise {
  let bytes = cell_hex(vector.hex[:])
  let d = StreamDecoder::new(Decoder::new(8192, 100, 10), 4)
  let c = Cursor::new(bytes)
  let tag = c.byte()
  if !vector.in_stream {
    if tag != b'S' || bytes.get(5) == Some(b'\x00') {
      ignore(d.decode(stream_start(7)))
      if tag != b'E' {
        ignore(d.decode(b"E"))
      }
    }
    let result = d.decode(bytes)
    return match tag {
      b'S' =>
        {
          "kind": "S",
          "xid": c.i32().reinterpret_as_uint().to_json(),
          "first": (c.byte() == b'\x01').to_json(),
        }
      b'E' => { "kind": "E" }
      b'A' =>
        {
          "kind": "A",
          "xid": c.i32().reinterpret_as_uint().to_json(),
          "subxid": c.i32().reinterpret_as_uint().to_json(),
        }
      b'c' => {
        guard result is Some((xid, commit, end, time, _)) else {
          fail("commit")
        }
        {
          "kind": "c",
          "xid": xid.to_json(),
          "commit": commit.to_string().to_json(),
          "end": end.to_string().to_json(),
          "time": time.to_json(),
        }
      }
      _ => fail("boundary")
    }
  }
  ignore(d.decode(stream_start(7)))
  ignore(d.decode(stream_change(7, relation_fixture("protocol"))))
  ignore(d.decode(bytes))
  ignore(d.decode(b"E"))
  guard d.decode(stream_commit(7)) is Some((_, _, _, _, changes)) else {
    fail("commit")
  }
  let xid = c.i32().reinterpret_as_uint()
  let change = changes[1]
  match change {
    Insert(_, _) | Update(_, _, _) | Delete(_, _) | Message(_) => {
      let value = protocol_normalize(
        Bytes::make(1, tag) + bytes[c.pos:].to_owned(),
      )
      guard value is Object(fields) else { fail("object") }
      fields["xid"] = xid.to_json()
      Json::object(fields)
    }
    RelationChanged(r) =>
      {
        "kind": "R",
        "xid": xid.to_json(),
        "relation": r.oid.to_json(),
        "namespace": r.namespace_.to_json(),
        "name": r.name.to_json(),
        "identity": r.replica_identity.to_int().to_json(),
        "columns": r.columns
        .map(col => {
          "name": col.name.to_json(),
          "oid": col.type_oid.to_json(),
          "modifier": col.type_modifier.to_json(),
          "key": col.key.to_json(),
        })
        .to_json(),
      }
    TypeChanged(t) =>
      {
        "kind": "Y",
        "xid": xid.to_json(),
        "oid": t.oid.to_json(),
        "namespace": t.namespace_.to_json(),
        "name": t.name.to_json(),
      }
    Truncate(relations, cascade~, restart_identity~) =>
      {
        "kind": "T",
        "xid": xid.to_json(),
        "relations": relations.map(r => r.oid).to_json(),
        "cascade": cascade.to_json(),
        "restart_identity": restart_identity.to_json(),
      }
    _ => fail("unexpected v2 change")
  }
}

///|
async test "pinned pglogrepl v2 finite streaming wire differential" {
  let vectors = streaming_vectors()
  for vector in vectors {
    try streaming_normalize(vector) catch {
      Protocol(_) => assert_false(vector.valid)
      e => raise e
    } noraise {
      _ => assert_true(vector.valid)
    }
  }
  let phase = @env.get_env_var("MOONCDC_STREAMING_REFERENCE_PHASE").unwrap_or(
    "verify",
  )
  let path = @env.get_env_var("MOONCDC_STREAMING_REFERENCE_FILE").unwrap_or(
    "integration/reference/pglogrepl-v2-expected.json",
  )
  if phase == "generate" {
    @fs.write_file(path, vectors.to_json().stringify(indent=2))
    println("STREAMING REFERENCE generated \{vectors.length()} vectors")
  } else {
    assert_eq(phase, "verify")
    let results : Array[ProtocolResult] = @json.from_json(
      @json.parse(@fs.read_file(path).text()),
    )
    assert_eq(results.length(), vectors.length())
    let mut valid = 0
    let invalid : Map[String, Int] = Map([])
    for index, result in results {
      let vector = vectors[index]
      assert_eq(result.name, vector.name)
      if vector.valid {
        assert_eq(result.status, "ok")
        assert_eq(result.value, streaming_normalize(vector))
        valid += 1
      } else {
        assert_true(["ok", "error", "panic"].contains(result.status))
        invalid[result.status] = invalid.get(result.status).unwrap_or(0) + 1
      }
    }
    println(
      "STREAMING REFERENCE matched \{valid} valid vectors; strict MoonCDC rejection versus Go: \{invalid.to_json().stringify()}",
    )
  }
}
