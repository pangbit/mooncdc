///|
fn sha256(data : Bytes) -> Bytes {
  Bytes::from_array(@crypto.sha256(data)[:])
}

///|
fn hmac(key : Bytes, data : Bytes) -> Bytes {
  Bytes::from_array(@crypto.hmac(@crypto.SHA256::new(), key, data)[:])
}

///|
fn scram_response(
  password : String,
  nonce : String,
  first : String,
) -> (String, String) raise {
  let attrs : Map[String, String] = Map([])
  for part in first.split(",") {
    if part.length() < 3 || part[1] != '=' {
      raise Authentication("invalid SCRAM attribute")
    }
    let key = part[0:1].to_owned()
    if attrs.contains(key) {
      raise Authentication("duplicate SCRAM attribute")
    }
    attrs[key] = part[2:].to_owned()
  }
  guard attrs.get("r") is Some(server_nonce) &&
    attrs.get("s") is Some(salt_text) &&
    attrs.get("i") is Some(iter_text) else {
    raise Authentication("missing SCRAM attributes")
  }
  if attrs.contains("m") ||
    !server_nonce.has_prefix(nonce) ||
    server_nonce.length() <= nonce.length() {
    raise Authentication("invalid SCRAM nonce or mandatory extension")
  }
  let iterations = @string.parse_int(iter_text)
  if iterations < 4096 || iterations > 1000000 {
    raise Authentication("SCRAM iteration limit")
  }
  // PostgreSQL uses SASLprep; this first release accepts printable ASCII passwords only.
  for c in password {
    if c.to_int() < 32 || c.to_int() > 126 {
      raise Authentication("password must be printable ASCII")
    }
  }
  let salt = @base64.decode(salt_text)
  let key = @utf8.encode(password)
  let mut u = hmac(key, salt + b"\x00\x00\x00\x01")
  let acc = u.to_fixedarray()
  for _ in 1..<iterations {
    u = hmac(key, u)
    for i in 0..<32 {
      acc[i] = acc[i] ^ u[i]
    }
  }
  let salted = Bytes::from_array(acc[:])
  let client_key = hmac(salted, b"Client Key")
  let final_message = "c=biws,r=\{server_nonce}"
  let auth = @utf8.encode("n=,r=\{nonce},\{first},\{final_message}")
  let signature = hmac(sha256(client_key), auth)
  let proof = Bytes::makei(32, i => client_key[i] ^ signature[i])
  let server_signature = hmac(hmac(salted, b"Server Key"), auth)
  (
    final_message + ",p=" + @base64.encode(proof),
    "v=" + @base64.encode(server_signature),
  )
}
