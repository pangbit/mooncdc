///|
test "SCRAM derives matching proofs for PostgreSQL-prepared Unicode passwords" {
  let first = "r=nonce-server,s=c2FsdA==,i=4096"
  assert_eq(
    scram_response("I\u00ADX\u00A0Cafe\u0301", "nonce", first),
    scram_response("IX Café", "nonce", first),
  )
  assert_false(
    scram_response("I\u00ADX\u00A0Cafe\u0301-wrong", "nonce", first) ==
    scram_response("IX Café", "nonce", first),
  )
}
