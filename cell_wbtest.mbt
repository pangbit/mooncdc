///|
test "typed cells preserve scalar widths exact numeric and null semantics" {
  let cases : Array[(UInt, String, Cell)] = [
    (16U, "t", Bool(true)),
    (16U, "f", Bool(false)),
    (21U, "-32768", I16(Int16::from_int(-32768))),
    (23U, "2147483647", I32(2147483647)),
    (20U, "9223372036854775807", I64(9223372036854775807L)),
    (20U, "-9223372036854775808", I64(-9223372036854775808L)),
    (26U, "4294967295", U32(4294967295U)),
    (700U, "1.25", F32(1.25)),
    (700U, "1.0000000596046448", F32(1.0000001192092896)),
    (700U, "7.006492321624086e-46", F32(1.401298464324817e-45)),
    (701U, "-2.5", F64(-2.5)),
    (
      1700U,
      "12345678901234567890.000000000001",
      Numeric("12345678901234567890.000000000001"),
    ),
    (1700U, "NaN", Numeric("NaN")),
    (1700U, "-Infinity", Numeric("-Infinity")),
    (1700U, "+1.20e-3", Numeric("0.00120")),
    (1700U, "-0.00", Numeric("0.00")),
    (1700U, ".5", Numeric("0.5")),
    (1700U, " 1_234.50e+1 ", Numeric("12345.0")),
    (26U, "+42", U32(42U)),
    (17U, "\\x00fF", Bytes(b"\x00\xff")),
    (
      2950U,
      "550e8400-e29b-41d4-a716-446655440000",
      Uuid(b"\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00"),
    ),
    (99999U, "custom value", String("custom value")),
  ]
  for (oid, text, expected) in cases {
    assert_eq(Value::Text(text).decode_text(oid), expected)
  }
  assert_eq(Value::Null.decode_text(114U), Cell::SqlNull)
  assert_eq(Value::UnchangedToast.decode_text(25U), Cell::ToastUnchanged)
  assert_eq(Value::Text("null").decode_text(114U), Cell::Json(Json::null()))
  assert_true(
    Value::Text("NaN").decode_text(701U) is F64(value) && value.is_nan(),
  )
  assert_true(
    Value::Text("Infinity").decode_text(701U) is F64(value) && value.is_inf(),
  )
  for
    (oid, text) in [
      (16U, "true"),
      (23U, "0x10"),
      (20U, "1_000"),
      (21U, "32768"),
      (23U, "2147483648"),
      (20U, "9223372036854775808"),
      (26U, "-1"),
      (700U, "1_0"),
      (701U, "1_0"),
      (1700U, "1e131072"),
      (1700U, "1e-16384"),
      (1700U, "+NaN"),
      (1700U, "."),
      (1700U, "1e"),
      (1700U, "1x"),
      (17U, "\\xgg"),
      (17U, "\\x0"),
      (2950U, "invalid"),
      (114U, "{bad}"),
    ] {
    try Value::Text(text).decode_text(oid) catch {
      Conversion(_) => ()
      e => raise e
    } noraise {
      _ => fail("invalid cell accepted: \{oid}/\{text}")
    }
  }
}

///|
test "typed arrays preserve quoted null escapes and reject nested dimensions" {
  assert_eq(
    Value::Text("[0:2]={7,NULL,-8}").decode_text(1016U),
    Cell::Array([I64(7), SqlNull, I64(-8)]),
  )
  assert_eq(
    Value::Text("{\"NULL\",null,\"a,b\",\"{x}\",\"a\\\"b\",\"\\\\\",\"\"}").decode_text(
      1009U,
    ),
    Cell::Array([
      String("NULL"),
      SqlNull,
      String("a,b"),
      String("{x}"),
      String("a\"b"),
      String("\\"),
      String(""),
    ]),
  )
  assert_eq(Value::Text("{}").decode_text(1007U), Cell::Array([]))
  assert_eq(
    Value::Text("{happy,NULL}").decode_text(90000U, array_element_oid=90001U),
    Cell::Array([String("happy"), SqlNull]),
  )
  assert_eq(
    Value::Text("{NaN,-Infinity,1.00}").decode_text(1231U),
    Cell::Array([Numeric("NaN"), Numeric("-Infinity"), Numeric("1.00")]),
  )
  for
    text in [
      "{{1}}", "[1:1][1:1]={{1}}", "[:2]={1}", "[x:2]={1}", "{\"unfinished}", "{escaped\\}",
      "{1,invalid}",
    ] {
    try Value::Text(text).decode_text(1007U) catch {
      Conversion(_) => ()
      e => raise e
    } noraise {
      _ => fail("invalid array accepted")
    }
  }
}

///|
test "typed temporal values preserve BC infinity end of day and UTC offsets" {
  assert_eq(
    Value::Text("23:59:60.5").decode_text(1083U),
    Cell::Time({ seconds: 86399, nanoseconds: 1500000000, }),
  )
  assert_eq(
    Value::Text("24:00:00.0000000000").decode_text(1083U),
    Cell::Time({ seconds: 86400, nanoseconds: 0, }),
  )
  assert_eq(Value::Text("infinity").decode_text(1082U), Cell::Date(PosInfinity))
  assert_eq(
    Value::Text("-infinity").decode_text(1114U),
    Cell::Timestamp(NegInfinity),
  )
  assert_eq(
    Value::Text("0001-01-01 BC").decode_text(1082U),
    Cell::Date(Finite(year=0, month=1, day=1)),
  )
  assert_eq(
    Value::Text("12000-02-29").decode_text(1082U),
    Cell::Date(Finite(year=12000, month=2, day=29)),
  )
  assert_eq(
    Value::Text("24:00:00").decode_text(1083U),
    Cell::Time({ seconds: 86400, nanoseconds: 0, }),
  )
  assert_eq(
    Value::Text("12:34:56.123456").decode_text(1083U),
    Cell::Time({ seconds: 45296, nanoseconds: 123456000, }),
  )
  assert_eq(
    Value::Text("24:00:00-15:59:59").decode_text(1266U),
    Cell::TimeTz({ seconds: 86400, nanoseconds: 0, }, offset_seconds=-57599),
  )
  assert_eq(
    Value::Text("0001-01-01 00:00:00+00:00:01").decode_text(1184U),
    Cell::TimestampTz(
      Finite(date=Finite(year=0, month=12, day=31), time={
        seconds: 86399,
        nanoseconds: 0,
      }),
    ),
  )
  assert_eq(
    Value::Text("2024-02-29 23:59:59.1-0130").decode_text(1184U),
    Cell::TimestampTz(
      Finite(date=Finite(year=2024, month=3, day=1), time={
        seconds: 5399,
        nanoseconds: 100000000,
      }),
    ),
  )
  for
    (oid, text) in [
      (1082U, "0000-01-01"),
      (1082U, "2023-02-29"),
      (1082U, "262143-01-01"),
      (1083U, "24:00:00.1"),
      (1083U, "00:60:00"),
      (1266U, "01:00:00+16"),
      (1184U, "2024-01-01 00:00:00"),
      (1114U, "2024-01-01 24:00:00"),
    ] {
    try Value::Text(text).decode_text(oid) catch {
      Conversion(_) => ()
      e => raise e
    } noraise {
      _ => fail("invalid time accepted")
    }
  }
}
