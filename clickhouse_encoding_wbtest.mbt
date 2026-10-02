///|
priv struct ChVector {
  selector : Int
  oid : UInt
  input : String
  nullable : Bool
  result : Json?
} derive(ToJson, @json.FromJson)

///|
async test "ClickHouse pinned encoding differential" {
  let phase = @env.get_env_var("MOONCDC_CLICKHOUSE_PHASE").unwrap_or("")
  if phase == "" {
    return
  }
  let file = @env.get_env_var("MOONCDC_CLICKHOUSE_FILE").unwrap()
  if phase == "generate" {
    let vectors : Array[ChVector] = []
    for vector in codec_vectors() {
      for nullable in [false, true] {
        vectors.push({
          selector: vector.selector,
          oid: vector.oid,
          input: vector.input,
          nullable,
          result: None,
        })
      }
    }
    @fs.write_file(file, @utf8.encode(vectors.to_json().stringify()))
    println("CLICKHOUSE generated \{vectors.length()} vectors")
  } else if phase == "verify" {
    let vectors : Array[ChVector] = @json.from_json(
      @json.parse(@fs.read_file(file).text()),
    )
    for index, vector in vectors {
      let actual : Json = try {
        let out = @buffer.Buffer()
        ch_encode_cell(
          out,
          Value::Text(vector.input).decode_text(vector.oid),
          vector.nullable,
        )
        {
          "ok": true,
          "hex": @crypto.bytes_to_hex_string(out.to_bytes()).to_json(),
        }
      } catch {
        _ => { "ok": false }
      }
      assert_eq(
        actual,
        vector.result.unwrap(),
        msg="ClickHouse vector \{index}: \{vector.oid} \{vector.input} actual=\{actual.stringify()} expected=\{vector.result.unwrap().stringify()}",
      )
    }
    println("CLICKHOUSE matched \{vectors.length()} vectors")
  } else {
    fail("unknown ClickHouse phase")
  }
}

///|
test "ClickHouse wire layouts and refusal boundaries" {
  let layout = ChLayout::new([
    ("id", "Int32"),
    ("_etl_version", "UInt128"),
    ("_etl_deleted", "UInt8"),
  ])
  assert_eq(layout.column_list, "\"id\", \"_etl_version\", \"_etl_deleted\"")
  assert_eq(
    layout.header,
    b"\x03\x02id\x0c_etl_version\x0c_etl_deleted\x05Int32\x07UInt128\x05UInt8",
  )
  let row = layout.row(
    [I32(42)],
    ReplacingMergeTree,
    "DELETE",
    0x1122334455667788UL,
    0x8877665544332211UL,
  )
  assert_eq(
    @crypto.bytes_to_hex_string(row),
    "2a0000001122334455667788887766554433221101",
  )
  for
    (value, nullable) in [
      (Cell::SqlNull, false),
      (Cell::ToastUnchanged, true),
      (Cell::Uuid(b"short"), false),
      (Cell::Date(PosInfinity), false),
    ] {
    let rejected = try {
      ch_encode_cell(@buffer.Buffer(), value, nullable)
      false
    } catch {
      _ => true
    }
    assert_true(rejected)
  }
  let null = @buffer.Buffer()
  ch_encode_cell(null, SqlNull, true)
  assert_eq(null.to_bytes(), b"\x01")
  assert_eq(ch_type(23U, false, false), "Int32")
  assert_eq(ch_type(23U, false, true), "Nullable(Int32)")
  assert_eq(ch_type(1007U, true, true), "Array(Nullable(Int32))")
  assert_eq(ch_type(1187U, false, false), "Array(Nullable(String))")
  assert_eq(ch_table_name("a_b", "c_d", MergeTree), "a__b_c__d")
  assert_true(
    try {
      ignore(ch_table_name("a", "b_current", ReplacingMergeTree))
      false
    } catch {
      _ => true
    },
  )
  assert_eq(ch_quote("path\\x'\n", '\''), "'path\\\\x\\'\\n'")
}
