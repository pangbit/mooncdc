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

///|
async test "snapshot validates limits before connecting" {
  let config : @mooncdc.ConnectionConfig = {
    host: "127.0.0.1",
    port: 1,
    user: "unused",
    password: "",
    database: "unused",
    security: @mooncdc.LocalPlaintext,
    timeout_ms: 1,
  }
  try
    @mooncdc.copy_snapshot(
      config,
      slot="snapshot",
      publication="snapshot",
      batch_rows=0,
      _ => (),
    )
  catch {
    @mooncdc.InvalidConfig(_) => ()
    e => raise e
  } noraise {
    _ => fail("invalid batch limit accepted")
  }
}
