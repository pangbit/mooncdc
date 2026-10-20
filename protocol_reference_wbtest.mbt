///|
priv struct ProtocolVector {
  name : String
  hex : String
  valid : Bool
} derive(ToJson)

///|
priv struct ProtocolResult {
  name : String
  status : String
  value : Json
} derive(@json.FromJson)

///|
fn protocol_tuple(values : ReadOnlyArray[Value]) -> Bytes {
  let out = @buffer.Buffer()
  out.write_uint16_be(values.length().to_uint16())
  for value in values {
    match value {
      Null => out.write_byte(b'n')
      UnchangedToast => out.write_byte(b'u')
      Binary(_) | Text(_) => {
        let (tag, bytes) = match value {
          Binary(bytes) => (b'b', bytes)
          Text(text) => (b't', @utf8.encode(text))
          _ => abort("unreachable")
        }
        out.write_byte(tag)
        out.write_int_be(bytes.length())
        out.write_bytes(bytes)
      }
    }
  }
  out.to_bytes()
}

///|
fn protocol_vectors() -> Array[ProtocolVector] {
  let vectors : Array[ProtocolVector] = []
  let add = fn(name : String, bytes : Bytes, valid : Bool) -> Unit {
    vectors.push({ name, hex: @crypto.bytes_to_hex_string(bytes), valid, })
  }
  for
    index, value in [
      Binary(b""),
      Binary(b"\x00\xff\x80"),
      Binary(Bytes::makei(256, i => i.to_byte())),
      Text("你好🌙"),
      Null,
    ] {
    let tuple = protocol_tuple([value, Null])
    add("insert-\{index}", tuple_fixture(b'I', b"N" + tuple), true)
    add(
      "update-\{index}",
      tuple_fixture(b'U', b"N" + protocol_tuple([value, UnchangedToast])),
      true,
    )
    for kind in [b'K', b'O'] {
      let old = Bytes::make(1, kind) + tuple
      add(
        "update-old-\{kind.to_int()}-\{index}",
        tuple_fixture(b'U', old + b"N" + tuple),
        true,
      )
      add("delete-\{kind.to_int()}-\{index}", tuple_fixture(b'D', old), true)
    }
  }
  for flag in [false, true] {
    for
      index, content in [
        b"",
        b"\x00\xff\x80",
        @utf8.encode("DDL 中文"),
        Bytes::makei(256, i => i.to_byte()),
      ] {
      add("message-\{flag}-\{index}", logical_fixture(flag, content), true)
    }
  }
  let message = logical_fixture(true, b"\x00\xff")
  let tuple = tuple_fixture(
    b'I',
    b"N" + protocol_tuple([Binary(b"\x00\xff"), Null]),
  )
  add("message-flags", b"M\x02" + message[2:].to_owned(), false)
  add("message-trailing", message + b"x", false)
  add("tuple-trailing", tuple + b"x", false)
  add("tuple-unknown-type", tuple_fixture(b'I', b"N\x00\x02zn"), false)
  // These truncations retain only small declared lengths. Do not feed Go a
  // multi-gigabyte tuple length: its parser allocates before checking bounds.
  for (name, bytes) in [("message", message), ("tuple", tuple)] {
    for length in 0..<bytes.length() {
      add("\{name}-truncated-\{length}", bytes[:length].to_owned(), false)
    }
  }
  vectors
}

///|
fn protocol_values(values : ReadOnlyArray[Value]) -> Json {
  values
  .map(value => {
    match value {
      Binary(bytes) =>
        Json::array(["b", @crypto.bytes_to_hex_string(bytes).to_json()])
      Text(text) =>
        Json::array([
          "t",
          @crypto.bytes_to_hex_string(@utf8.encode(text)).to_json(),
        ])
      Null => Json::array(["n"])
      UnchangedToast => Json::array(["u"])
    }
  })
  .to_json()
}

///|
fn protocol_normalize(bytes : Bytes) -> Json raise {
  if bytes.length() > 0 && bytes[0] == b'M' {
    let cursor = Cursor::new(bytes)
    ignore(cursor.byte())
    let message = cursor.logical_message()
    cursor.done()
    return {
      "kind": "M",
      "lsn": message.lsn.to_string().to_json(),
      "transactional": message.transactional.to_json(),
      "prefix": message.prefix.to_json(),
      "content": @crypto.bytes_to_hex_string(message.content).to_json(),
    }
  }
  let decoder = Decoder::new(8192, 100, 10)
  ignore(decoder.decode(begin_fixture()))
  ignore(decoder.decode(relation_fixture("protocol")))
  ignore(decoder.decode(bytes))
  guard decoder.decode(commit_fixture()) is Some((_, _, _, _, changes)) else {
    fail("missing commit")
  }
  let (kind, relation, old, next) = match changes[1] {
    Insert(r, values) => ("I", r, None, protocol_values(values))
    Update(r, old, values) => ("U", r, old, protocol_values(values))
    Delete(r, old) => ("D", r, Some(old), Json::null())
    _ => fail("unexpected protocol message")
  }
  let (old_kind, old_values) : (Json, Json) = match old {
    Some(Key(values)) => ("K", protocol_values(values))
    Some(Full(values)) => ("O", protocol_values(values))
    None => (Json::null(), Json::null())
  }
  {
    "kind": kind.to_json(),
    "relation": relation.oid.to_json(),
    "old_kind": old_kind,
    "old": old_values,
    "new": next,
  }
}

///|
async test "pinned pglogrepl binary and logical message differential" {
  let vectors = protocol_vectors()
  for vector in vectors {
    let bytes = cell_hex(vector.hex[:])
    try protocol_normalize(bytes) catch {
      Protocol(_) => assert_false(vector.valid)
      error => raise error
    } noraise {
      _ => assert_true(vector.valid)
    }
  }
  let phase = @env.get_env_var("MOONCDC_PROTOCOL_PHASE").unwrap_or("verify")
  let path = @env.get_env_var("MOONCDC_PROTOCOL_FILE").unwrap_or(
    "integration/reference/pglogrepl-expected.json",
  )
  if phase == "generate" {
    @fs.write_file(path, vectors.to_json().stringify())
    println("PROTOCOL generated \{vectors.length()} vectors")
  } else {
    assert_eq(phase, "verify")
    let results : Array[ProtocolResult] = @json.from_json(
      @json.parse(@fs.read_file(path).text()),
    )
    assert_eq(results.length(), vectors.length())
    let mut valid = 0
    let invalid : Map[String, Int] = Map([])
    for i, result in results {
      let vector = vectors[i]
      assert_eq(result.name, vector.name)
      if vector.valid {
        assert_eq(result.status, "ok")
        assert_eq(result.value, protocol_normalize(cell_hex(vector.hex[:])))
        valid += 1
      } else {
        assert_true(
          result.status == "ok" ||
          result.status == "error" ||
          result.status == "panic",
        )
        invalid[result.status] = invalid.get(result.status).unwrap_or(0) + 1
      }
    }
    println(
      "PROTOCOL matched \{valid} valid vectors; strict MoonCDC rejection versus Go: \{invalid.to_json().stringify()}",
    )
  }
}
