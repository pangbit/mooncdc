///|
test "JSON numbers retain precision scale and exponent through nested values" {
  for
    (input, expected) in [
      ("0.0", "0.0"),
      ("-0", "0"),
      ("-0.0", "-0.0"),
      ("1E400", "1e+400"),
      ("1e-400", "1e-400"),
      ("1.2300E001", "1.2300e+001"),
      ("18446744073709551616", "18446744073709551616"),
      ("[1e-400,0.0]", "[1e-400,0.0]"),
    ] {
    guard Value::Text(input).decode_text(114U) is Json(value) else {
      fail("expected JSON cell")
    }
    assert_eq(value.stringify(), expected)
  }
  let tiny = Value::Text("{\"x\":[1e-400]}").decode_text(114U)
  let zero = Value::Text("{\"x\":[0]}").decode_text(114U)
  assert_true(tiny != zero)
  assert_true(
    Value::Text("0.0").decode_text(114U) != Value::Text("0").decode_text(114U),
  )
  assert_eq(
    Value::Text("1E400").decode_text(114U),
    Value::Text("1e+400").decode_text(114U),
  )
  assert_true(
    Value::Text("{\"x\":[9007199254740992]}").decode_text(114U) !=
    Value::Text("{\"x\":[9007199254740993]}").decode_text(114U),
  )
  assert_eq(
    Value::Text(
      "{\"n1\":\"n1e400\",\"s\":\"s\",\"\":\"\",\"\\u0061\":1e-400,\"a\":2.00}",
    ).decode_text(114U),
    Value::Text("{\"a\":2.00,\"\":\"\",\"s\":\"s\",\"n1\":\"n1e400\"}").decode_text(
      114U,
    ),
  )
}

///|
test "JSON token preservation still rejects invalid grammar and depth" {
  for
    input in [
      "{1:2}",
      "[01]",
      "[1+2]",
      "+1",
      "1.",
      "1e",
      "{\"a\":1true}",
      "\"\\ud800\"",
      "\"\\udc00\"",
      "\"unterminated\\",
      "汉字",
      "[".repeat(128) + "0" + "]".repeat(128),
    ] {
    try Value::Text(input).decode_text(114U) catch {
      Conversion(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid JSON accepted: " + input)
    }
  }
  Value::Text("[".repeat(127) + "0" + "]".repeat(127)).decode_text(114U)
  |> ignore
}
