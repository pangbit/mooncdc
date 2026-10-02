///|
/// TLS always verifies the server certificate and hostname when enabled.
pub(all) enum Security {
  LocalPlaintext
  VerifyFull
  VerifyCAFile(String)
} derive(Debug)

///|
/// Connection settings. Passwords are deliberately excluded from Debug output.
pub(all) struct ConnectionConfig {
  host : String
  port : Int
  user : String
  password : String
  database : String
  security : Security
  timeout_ms : Int
}

///|
priv struct Connection {
  tcp : @socket.Tcp
  tls : @tls.Tls?
  reader : &@io.Reader
  writer : &@io.Writer
  limit : Int
}

///|
fn Connection::close(self : Connection) -> Unit {
  if self.tls is Some(tls) {
    tls.close()
  }
  self.tcp.close()
}

///|
async fn Connection::read(self : Connection) -> (Byte, Bytes) {
  read_frame(self.reader, self.limit)
}

///|
async fn Connection::send(self : Connection, tag : Byte, body : Bytes) -> Unit {
  self.writer.write(packet(tag, body))
}

///|
fn validate_connection(config : ConnectionConfig) -> Unit raise {
  if config.port <= 0 || config.port > 65535 || config.timeout_ms <= 0 {
    raise InvalidConfig("port or timeout")
  }
  for value in [config.host, config.user, config.database, config.password] {
    if value.contains("\u0000") {
      raise InvalidConfig("NUL in connection configuration")
    }
  }
  if config.user.is_empty() || config.database.is_empty() {
    raise InvalidConfig("empty user or database")
  }
  if config.security is LocalPlaintext &&
    config.host != "127.0.0.1" &&
    config.host != "::1" {
    raise InvalidConfig("plaintext is restricted to numeric loopback addresses")
  }
}

///|
async fn connect(
  config : ConnectionConfig,
  limit : Int,
  replication : Bool,
) -> Connection {
  validate_connection(config)
  @async.with_timeout(config.timeout_ms, () => {
    let tcp = @socket.Tcp::connect_to_host(config.host, port=config.port)
    let mut tls_socket : @tls.Tls? = None
    errdefer {
      if tls_socket is Some(t) {
        t.close()
      }
      tcp.close()
    }
    let (reader, writer) : (&@io.Reader, &@io.Writer) = match config.security {
      LocalPlaintext => (tcp, tcp)
      _ => {
        tcp.write(b"\x00\x00\x00\x08\x04\xd2\x16\x2f")
        if tcp.read_exactly(1) != b"S" {
          raise Authentication("server refused TLS")
        }
        let trust = match config.security {
          VerifyCAFile(path) => @tls.CustomPemFile(path)
          _ => @tls.SystemRoot
        }
        let t = @tls.Tls::client(tcp, host=config.host, trust~)
        tls_socket = Some(t)
        (t, t)
      }
    }
    let conn = Connection::{ tcp, tls: tls_socket, reader, writer, limit, }
    let body = @buffer.Buffer()
    body.write_int_be(196608)
    for
      (k, v) in [
        ("user", config.user),
        ("database", config.database),
        ("client_encoding", "UTF8"),
        ("application_name", "mooncdc"),
      ] {
      cstring(body, k)
      cstring(body, v)
    }
    if replication {
      cstring(body, "replication")
      cstring(body, "database")
    }
    body.write_byte(b'\x00')
    let startup = @buffer.Buffer()
    startup.write_int_be(body.length() + 4)
    startup.write_bytes(body.to_bytes())
    writer.write(startup.to_bytes())
    let mut nonce = ""
    let mut expected = ""
    let mut authenticated = false
    let mut scram_verified = false
    for ;; {
      let (tag, payload) = conn.read()
      match tag {
        b'R' => {
          let c = Cursor::new(payload)
          match c.i32() {
            0 => {
              c.done()
              if !nonce.is_empty() && !scram_verified {
                raise Authentication("unverified SCRAM server")
              }
              authenticated = true
            }
            10 => {
              if !nonce.is_empty() {
                raise Authentication("duplicate SCRAM challenge")
              }
              let mut supported = false
              for ;; {
                let mechanism = c.cstring()
                if mechanism.is_empty() {
                  break
                }
                if mechanism == "SCRAM-SHA-256" {
                  supported = true
                }
              }
              c.done()
              if !supported {
                raise Authentication("SCRAM-SHA-256 required")
              }
              guard @env.rand(24) is Some(random) else {
                raise Authentication("secure entropy unavailable")
              }
              nonce = @base64.encode(random)
              let first = @utf8.encode("n,,n=,r=\{nonce}")
              let out = @buffer.Buffer()
              cstring(out, "SCRAM-SHA-256")
              out.write_int_be(first.length())
              out.write_bytes(first)
              conn.send(b'p', out.to_bytes())
            }
            11 => {
              if nonce.is_empty() || !expected.is_empty() {
                raise Authentication("unexpected SCRAM continuation")
              }
              let (response, signature) = scram_response(
                config.password,
                nonce,
                @utf8.decode(payload[4:]),
              )
              expected = signature
              conn.send(b'p', @utf8.encode(response))
            }
            12 => {
              if expected.is_empty() || @utf8.decode(payload[4:]) != expected {
                raise Authentication("SCRAM signature mismatch")
              }
              scram_verified = true
            }
            _ =>
              raise Authentication("only SCRAM-SHA-256 and trust are supported")
          }
        }
        b'E' => raise server_error(payload)
        b'S' | b'K' | b'N' => ()
        b'Z' => {
          if !authenticated || payload != b"I" {
            raise Protocol("invalid startup completion")
          }
          break
        }
        _ => raise Protocol("unexpected startup message")
      }
    }
    conn
  })
}

///|
async fn Connection::query(
  self : Connection,
  sql : String,
) -> Array[Array[String?]] {
  let out = @buffer.Buffer()
  cstring(out, sql)
  self.send(b'Q', out.to_bytes())
  let rows : Array[Array[String?]] = []
  for ;; {
    let (tag, body) = self.read()
    match tag {
      b'D' => {
        let c = Cursor::new(body)
        let row : Array[String?] = []
        for _ in 0..<c.u16() {
          let n = c.i32()
          row.push(
            if n == -1 {
              None
            } else {
              Some(@utf8.decode(c.take(n), ignore_bom=true))
            },
          )
        }
        c.done()
        rows.push(row)
        if rows.length() > 1024 {
          raise LimitExceeded("control query rows")
        }
      }
      b'E' => raise server_error(body)
      b'Z' => break
      b'T' | b'C' | b'N' | b'S' => ()
      _ => raise Protocol("unexpected control query message")
    }
  }
  rows
}
