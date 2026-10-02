///|
priv struct CodecVector {
  selector : Int
  oid : UInt
  input : String
  result : Json?
} derive(ToJson, @json.FromJson)

///|
fn normalize_date(date : PgDate) -> Json {
  match date {
    Finite(year~, month~, day~) =>
      [year.to_json(), month.to_json(), day.to_json()]
    PosInfinity => "infinity"
    NegInfinity => "-infinity"
  }
}

///|
fn normalize_time(time : PgTime) -> Json {
  [time.seconds.to_json(), time.nanoseconds.to_json()]
}

///|
fn normalize_timestamp(value : PgTimestamp) -> Json {
  match value {
    Finite(date~, time~) => [normalize_date(date), normalize_time(time)]
    PosInfinity => "infinity"
    NegInfinity => "-infinity"
  }
}

///|
// Json equality can compare rounded Doubles even when their retained decimal
// representations differ. The reference enables arbitrary_precision: preserve
// the exact number representation instead of passing it through Double equality.
fn normalize_json(value : Json) -> Json {
  match value {
    Number(number, repr~) =>
      ["number", repr.unwrap_or(number.to_string()).to_json()]
    Object(fields) => {
      let normalized : Map[String, Json] = Map([])
      for key, value in fields {
        normalized[key] = normalize_json(value)
      }
      ["object", Json::object(normalized)]
    }
    Array(items) => ["array", items.map(normalize_json).to_json()]
    _ => value
  }
}

///|
fn normalize_cell(value : Cell) -> Json {
  match value {
    SqlNull => Json::null()
    ToastUnchanged => "toast"
    Bool(v) => ["bool", v.to_json()]
    String(v) => ["string", v.to_json()]
    I16(v) => ["i16", v.to_string().to_json()]
    I32(v) => ["i32", v.to_string().to_json()]
    I64(v) => ["i64", v.to_string().to_json()]
    U32(v) => ["u32", v.to_string().to_json()]
    F32(v) =>
      [
        "f32",
        (if v.is_nan() {
          "NaN"
        } else {
          v.reinterpret_as_uint().to_string(radix=16)
        }).to_json(),
      ]
    F64(v) =>
      [
        "f64",
        (if v.is_nan() {
          "NaN"
        } else {
          v.reinterpret_as_uint64().to_string(radix=16)
        }).to_json(),
      ]
    Numeric(v) => ["numeric", v.to_json()]
    Bytes(v) => ["bytes", @crypto.bytes_to_hex_string(v).to_json()]
    Uuid(v) => ["uuid", @crypto.bytes_to_hex_string(v).to_json()]
    Json(v) => ["json", normalize_json(v)]
    Date(v) => ["date", normalize_date(v)]
    Time(v) => ["time", normalize_time(v)]
    TimeTz(v, offset_seconds~) =>
      ["timetz", normalize_time(v), offset_seconds.to_json()]
    Timestamp(v) => ["timestamp", normalize_timestamp(v)]
    TimestampTz(v) => ["timestamptz", normalize_timestamp(v)]
    Array(values) =>
      ["array", Json::array(values.iter().map(normalize_cell).collect())]
  }
}

///|
fn codec_vectors() -> Array[CodecVector] {
  let pairs : Array[(UInt, UInt, Array[String])] = [
    (16U, 1000U, ["t", "f", "true", ""]),
    (21U, 1005U, ["-32768", "32767", "32768", "1_0", "0x10"]),
    (23U, 1007U, ["2147483647", "-2147483648", "2147483648", "+42"]),
    (
      20U,
      1016U,
      [
        "9223372036854775807", "-9223372036854775808", "9223372036854775808", "1_0",
      ],
    ),
    (
      700U,
      1021U,
      [
        "1.25", "-0", "1.40129846e-45", "NaN", "Infinity", "1_0", "bad", "1.0000000596046448",
        "1.0000000596046447", "-1.0000000596046448", "7.006492321624086e-46", "3.4028235677973366e38",
        " 1", "1 ", "0x1p2", "1e9999",
      ],
    ),
    (
      701U,
      1022U,
      ["-2.5", "-0", "1.7976931348623157e308", "NaN", "-Infinity", "1_0"],
    ),
    (
      1700U,
      1231U,
      [
        "12345678901234567890.0001", "NaN", "-Infinity", "1.00", "+1.20e-3", "-0.00",
        ".5", "1e", "bad", " 1_234.50e+1 ", "-NaN", "+NaN", "nan", "iNf", "+Inf",
        "0001.2300", "0e1000000000", "1e131072", "1e-16384", "1.2_0e1_0", "1._0",
        "1__0", "1e_2", "1e1073741824", "-0e-2", "1.234e2", "1.234e5",
      ],
    ),
    (17U, 1001U, ["\\x00ff", "\\x", "\\xabc", "\\xgg", "escape"]),
    (
      1082U,
      1182U,
      [
        "0001-01-01 BC", "12000-02-29", "infinity", "-infinity", "0000-01-01", "2023-02-29",
      ],
    ),
    (
      1083U,
      1183U,
      [
        "24:00:00", "12:34:56.123456789", "24:00:00.0000000000", "23:59:60", "24:00:01",
        "23:59:60.5", "12:00:60", "12:00:00.1234567890", "1:2:3", "12:00:00.",
      ],
    ),
    (1266U, 1270U, ["24:00:00-15:59:59", "12:34:56+0130", "00:00:00+16", "bad"]),
    (
      1114U,
      1115U,
      [
        "0001-01-01 00:00:00 BC", "12000-02-29 23:59:59.123456", "infinity", "2024-01-01 24:00:00",
      ],
    ),
    (
      1184U,
      1185U,
      [
        "0001-01-01 00:00:00+00:00:01", "2024-02-29 23:59:59-0130", "-infinity",
        "2024-01-01 00:00:00",
      ],
    ),
    (
      2950U,
      2951U,
      [
        "550e8400-e29b-41d4-a716-446655440000", "{550e8400-e29b-41d4-a716-446655440000}",
        "bad",
      ],
    ),
    (
      114U,
      199U,
      [
        "null",
        "{\"n\":9007199254740993,\"s\":\"汉字\"}",
        "{bad}",
        "18446744073709551615",
        "18446744073709551616",
        "-9223372036854775809",
        "-0",
        "0.0",
        "1e0",
        "1e400",
        "1e-400",
        "{\"x\":1,\"x\":2}",
        "-0.0",
        "1.2300E001",
        "0e-000",
        "1e9999999999999999999999",
        "{\"n1\":\"n1e400\",\"s\":\"s\",\"\":\"\",\"\\u0061\":1e-400,\"a\":2.00}",
        "{1:2}",
        "{01:2}",
        "1e",
        "1+2",
        "01",
        "-01",
        "+1",
        ".1",
        "1.",
        "{\"a\":1true}",
        "\"\\ud800\"",
        "\"\\udc00\"",
        "\"\\ud800\\udc00\"",
        "[".repeat(127) + "0" + "]".repeat(127),
        "[".repeat(128) + "0" + "]".repeat(128),
      ],
    ),
    (3802U, 3807U, ["null", "[1,true,\"x\"]", "{bad}"]),
    (26U, 1028U, ["4294967295", "4294967296", "-1", "+42"]),
    (25U, 1015U, ["NULL", "汉字", ""]),
  ]
  let vectors : Array[CodecVector] = []
  for index, (oid, array, inputs) in pairs {
    for input in inputs {
      vectors.push({ selector: index * 2, oid, input, result: None, })
      let quoted = input
        .replace_all(old="\\", new="\\\\")
        .replace_all(old="\"", new="\\\"")
      vectors.push({
        selector: index * 2 + 1,
        oid: array,
        input: "{\"" + quoted + "\",NULL}",
        result: None,
      })
    }
    let quoted = "\"" +
      inputs[0]
      .replace_all(old="\\", new="\\\\")
      .replace_all(old="\"", new="\\\"") +
      "\""
    for
      input in [
        "{}",
        "{NULL}",
        "{" + quoted + ",NULL}",
        "[0:1]={" + quoted + ",NULL}",
        "{{1}}",
      ] {
      vectors.push({ selector: index * 2 + 1, oid: array, input, result: None, })
    }
  }
  vectors
}

///|
async test "codec reference process worker" {
  if @env.get_env_var("MOONCDC_CODEC_PHASE") is Some(phase) {
    let path = @env.get_env_var("MOONCDC_CODEC_FILE").unwrap()
    if phase == "generate" {
      @fs.write_file(path, ToJson::to_json(codec_vectors()).stringify())
      println("CODEC generated vectors")
    } else {
      let rows : Array[CodecVector] = @json.from_json(
        @json.parse(@fs.read_file(path).text()),
      )
      let mut mismatches = 0
      for row in rows {
        let actual : Json = {
          "ok": true,
          "value": normalize_cell(Value::Text(row.input).decode_text(row.oid)),
        } catch {
          _ => { "ok": false }
        }
        if actual != row.result.unwrap() {
          println(
            "CODEC mismatch oid=\{row.oid} input=\{row.input.to_json().stringify()} actual=\{actual.stringify()} reference=\{row.result.unwrap().stringify()}",
          )
          mismatches += 1
        }
      }
      assert_eq(mismatches, 0)
      println("CODEC matched \{rows.length()} upstream vectors")
    }
  }
}
