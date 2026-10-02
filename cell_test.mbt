///|
test "typed cell API preserves SQL null JSON null and unchanged TOAST" {
  assert_eq(@mooncdc.Text("42").decode_text(23U), @mooncdc.I32(42))
  assert_eq(@mooncdc.Null.decode_text(25U), @mooncdc.SqlNull)
  assert_eq(@mooncdc.UnchangedToast.decode_text(25U), @mooncdc.ToastUnchanged)
  assert_true(
    @mooncdc.Text("null").decode_text(3802U) is @mooncdc.Cell::Json(_),
  )
  assert_true(
    @mooncdc.Text("24:00:00").decode_text(1083U) is @mooncdc.Time(time) &&
    time.seconds == 86400,
  )
  try @mooncdc.Binary(b"x").decode_text(25U) catch {
    @mooncdc.Conversion(_) => ()
    e => raise e
  } noraise {
    _ => fail("binary silently interpreted as text")
  }
}
