///|
async test "live verified TLS SCRAM bad credentials and untrusted certificate" {
  if live_config() is Some(config) &&
    @env.get_env_var("MOONCDC_TEST_CA") is Some(ca) {
    let secure = { ..config, security: VerifyCAFile(ca), }
    let conn = connect(secure, 1048576, true)
    defer conn.close()
    assert_eq(cell(conn.query("IDENTIFY_SYSTEM"), 0, 3), "postgres")
    try
      connect({ ..secure, password: "wrong-password", }, 1048576, true)
    catch {
      Server("28P01", _) => ()
      e => raise e
    } noraise {
      c => {
        c.close()
        fail("incorrect password accepted")
      }
    }
    try connect({ ..config, security: VerifyFull, }, 1048576, true) catch {
      @tls.TlsError(_) => ()
      e => raise e
    } noraise {
      c => {
        c.close()
        fail("untrusted certificate accepted")
      }
    }
    println(
      "LIVE: verified TLS, SCRAM, rejected bad password and untrusted certificate passed",
    )
  }
}
