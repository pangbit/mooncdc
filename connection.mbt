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
  hostaddr : String?
  port : Int32
  user : String
  password : String
  database : String
  security : Security
  timeout_ms : Int32
  keepalive : TcpKeepaliveConfig
}

///|
/// TCP probe settings in seconds, matching the pinned ETL replication defaults.
pub(all) struct TcpKeepaliveConfig {
  idle_secs : Int32
  interval_secs : Int32
  retries : Int32
} derive(Eq, Debug)

///|
pub extend TcpKeepaliveConfig with Eq::{equal, not_equal}

///|
pub extend TcpKeepaliveConfig with Debug::{to_repr}

///|
pub fn TcpKeepaliveConfig::new(
  idle_secs~ : Int32 = 30,
  interval_secs~ : Int32 = 10,
  retries~ : Int32 = 3,
) -> TcpKeepaliveConfig {
  { idle_secs, interval_secs, retries, }
}

///|
/// Construct validated connection settings. host remains the TLS server name;
/// optional numeric hostaddr overrides only the network destination.
pub fn ConnectionConfig::new(
  host~ : String,
  user~ : String,
  password~ : String = "",
  database~ : String = "postgres",
  port~ : Int32 = 5432,
  security~ : Security = VerifyFull,
  timeout_ms~ : Int32 = 5000,
  hostaddr? : String,
  keepalive~ : TcpKeepaliveConfig = TcpKeepaliveConfig::new(),
) -> ConnectionConfig raise {
  let config = {
    host,
    hostaddr,
    user,
    password,
    database,
    port,
    security,
    timeout_ms,
    keepalive,
  }
  validate_connection(config)
  config
}

///|
fn connection_hostaddr(config : ConnectionConfig) -> @socket.Addr? raise {
  config.hostaddr.map(ip => {
    if ip.contains("\u0000") {
      raise InvalidConfig("NUL in hostaddr")
    }
    if ip.contains("%") {
      raise InvalidConfig("hostaddr must not contain an IPv6 scope")
    }
    let address = if ip.contains(":") {
      "[\{ip}]:\{config.port}"
    } else {
      "\{ip}:\{config.port}"
    }
    @socket.Addr::parse(address) catch {
      _ =>
        raise InvalidConfig(
          "hostaddr must be a numeric IP address without a port",
        )
    }
  })
}

///|
priv struct QueryObservation {
  metrics : RuntimeMetrics
  pool : SessionWorkload
}

///|
fn QueryObservation::record(
  self : QueryObservation,
  started : @bench.Timestamp,
) -> Unit {
  let pool = if self.pool is StateStore {
    "postgres_store"
  } else {
    "out_of_band"
  }
  self.metrics.duration(
    "mooncdc_postgres_query_seconds{pool=\"" + pool + "\"}",
    started,
  )
}

///|
priv struct Connection {
  tcp : @socket.Tcp
  tls : @tls.Tls?
  reader : &@io.Reader
  writer : &@io.Writer
  limit : Int32
  frame_role : FrameConnectionRole
  queries : QueryObservation?
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
  read_frame(self.reader, self.limit, role=self.frame_role)
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
  if config.host.is_empty() {
    raise InvalidConfig("empty host")
  }
  if config.keepalive.idle_secs <= 0 ||
    config.keepalive.interval_secs <= 0 ||
    config.keepalive.retries <= 0 {
    raise InvalidConfig("TCP keepalive values must be positive")
  }
  let dial = connection_hostaddr(config)
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
  if config.security is LocalPlaintext &&
    dial is Some(address) &&
    address != @socket.Addr::parse("127.0.0.1:\{config.port}") &&
    address != @socket.Addr::parse("[::1]:\{config.port}") {
    raise InvalidConfig("plaintext hostaddr is restricted to loopback")
  }
}

///|
async fn connect(
  config : ConnectionConfig,
  limit : Int32,
  replication : Bool,
  workload? : SessionWorkload,
  worker? : SessionWorker,
  metrics? : RuntimeMetrics,
) -> Connection {
  validate_connection(config)
  let workload = workload.unwrap_or(
    if replication {
      Replication
    } else {
      Queries
    },
  )
  let (application_name, options) = workload.settings(worker?)
  @async.with_timeout(config.timeout_ms, () => {
    let tcp = match connection_hostaddr(config) {
      Some(address) => @socket.Tcp::connect(address)
      None => @socket.Tcp::connect_to_host(config.host, port=config.port)
    }
    let mut tls_socket : @tls.Tls? = None
    errdefer {
      if tls_socket is Some(t) {
        t.close()
      }
      tcp.close()
    }
    tcp.enable_keepalive(
      idle_before_keep_alive=config.keepalive.idle_secs,
      keep_alive_interval=config.keepalive.interval_secs,
      keep_alive_count=config.keepalive.retries,
    )
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
    let conn = Connection::{
      tcp,
      tls: tls_socket,
      reader,
      writer,
      limit,
      frame_role: frame_connection_role(workload, worker),
      queries: metrics.map(metrics => { metrics, pool: workload, }),
    }
    let body = @buffer.Buffer()
    body.write_int_be(196608)
    for
      (k, v) in [
        ("user", config.user),
        ("database", config.database),
        ("client_encoding", "UTF8"),
        ("application_name", application_name),
        ("options", options),
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
  max_result_bytes~ : Int32 = 67108864,
) -> Array[Array[String?]] {
  let started = @bench.monotonic_clock_start()
  defer (if self.queries is Some(observer) { observer.record(started) })
  let out = @buffer.Buffer()
  cstring(out, sql)
  self.send(b'Q', out.to_bytes())
  let rows : Array[Array[String?]] = []
  let mut bytes = 0
  for ;; {
    let (tag, body) = self.read()
    match tag {
      b'D' => {
        if body.length() > max_result_bytes - bytes {
          raise LimitExceeded("query result bytes")
        }
        bytes += body.length()
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
