///|
test "LSN roundtrip unsigned ordering and strict syntax" {
  for text in ["0/0", "FFFFFFFF/FFFFFFFF", "1/ABCDEF"] {
    assert_eq(@mooncdc.Lsn::parse(text).to_string(), text)
  }
  assert_true(
    @mooncdc.Lsn::parse("80000000/0") > @mooncdc.Lsn::parse("7FFFFFFF/FFFFFFFF"),
  )
  for invalid in ["", "0", "-1/0", "1/100000000", "0x1/0", "1/2/3", "1_0/0"] {
    try @mooncdc.Lsn::parse(invalid) catch {
      _ => ()
    } noraise {
      _ => fail("invalid LSN accepted")
    }
  }
}

///|
test "binary values remain distinct from text null and unchanged TOAST" {
  let value = @mooncdc.Binary(b"\x00\xff\x80")
  assert_true(value is @mooncdc.Binary(b"\x00\xff\x80"))
  assert_true(value != @mooncdc.Null)
  assert_true(value != @mooncdc.UnchangedToast)
  assert_true(value != @mooncdc.Text(""))
}
