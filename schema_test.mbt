///|
test "schema exclusive bound is available to external consumers" {
  let checkpoint = @mooncdc.Lsn::parse("1/0")
  let bound = @mooncdc.SchemaId::before_lsn(checkpoint)
  assert_eq(bound.commit_lsn.to_string(), "0/FFFFFFFF")
  assert_eq(bound.message_lsn.to_string(), "FFFFFFFF/FFFFFFFF")
  try @mooncdc.SchemaId::before_lsn(@mooncdc.Lsn::parse("0/0")) catch {
    @mooncdc.InvalidConfig(_) => ()
    e => raise e
  } noraise {
    _ => fail("zero exclusive bound accepted")
  }
}

///|
async test "schema store public scope validates identity before IO" {
  try
    @mooncdc.with_file_schema_store("unused", identity="", store => {
      let versions : ReadOnlyArray[@mooncdc.SchemaSnapshot] = store.all()
      assert_eq(versions.length(), 0)
    })
  catch {
    @mooncdc.InvalidConfig(_) => ()
    e => raise e
  } noraise {
    _ => fail("empty identity accepted")
  }
}
