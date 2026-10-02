///|
async fn mock_row(tcp : @socket.Tcp, fields : Array[String?]) -> Unit {
  let b = @buffer.Buffer()
  b.write_uint16_be(fields.length().to_uint16())
  for field in fields {
    match field {
      None => b.write_int_be(-1)
      Some(text) => {
        let bytes = @utf8.encode(text)
        b.write_int_be(bytes.length())
        b.write_bytes(bytes)
      }
    }
  }
  tcp.write(packet(b'D', b.to_bytes()) + packet(b'Z', b"I"))
}

///|
// A wire peer, not a PostgreSQL fixture. Each connection has a specific failure
// point so recovery has to complete authentication, identification and COPY BOTH.
async fn mock_replication(
  tcp : @socket.Tcp,
  mode : String,
  stream? : async (@socket.Tcp) -> Unit,
  rejected_slot? : Array[String?],
) -> Unit {
  let size = Cursor::new(tcp.read_exactly(4)).i32()
  ignore(tcp.read_exactly(size - 4))
  if mode == "startup" {
    return
  }
  tcp.write(packet(b'R', b"\x00\x00\x00\x00") + packet(b'Z', b"I"))
  for ;; {
    let (tag, body) = read_frame(tcp, 4096)
    assert_eq(tag, b'Q')
    let query = Cursor::new(body).cstring()
    if query == "IDENTIFY_SYSTEM" {
      if mode == "identify" {
        return
      }
      if mode.has_prefix("error") {
        tcp.write(
          packet(b'E', @utf8.encode("C57P03\u0000M\{mode}\u0000\u0000")),
        )
        assert_eq(tcp.read_some(), None)
        return
      }
      mock_row(tcp, [
        Some(if mode == "changed" { "other" } else { "system" }),
        Some("1"),
        Some("0/100"),
        Some("db"),
      ])
      if mode == "changed" {
        assert_eq(tcp.read_some(), None)
        return
      }
    } else if query.has_prefix("SELECT plugin") {
      if rejected_slot is Some(fields) {
        mock_row(tcp, fields)
        // Rejection must close before any publication query, START_REPLICATION,
        // or feedback is sent on this connection.
        assert_eq(tcp.read_some(), None)
        return
      }
      mock_row(tcp, [
        Some("pgoutput"),
        Some("db"),
        Some("0/10"),
        Some("0/10"),
        Some("reserved"),
        None,
        Some("false"),
      ])
    } else if query.has_prefix("SELECT pubname") {
      mock_row(tcp, [Some("pub")])
    } else {
      assert_true(query.has_prefix("START_REPLICATION"))
      if mode == "start" {
        return
      }
      if mode == "timeout" {
        // The client timeout must close this attempt before retrying.
        assert_eq(tcp.read_some(), None)
        return
      }
      tcp.write(packet(b'W', b"\x00\x00\x00"))
      if stream is Some(send) {
        send(tcp)
        return
      }
      if mode == "initial" {
        return
      }
      for
        data in [
          begin_fixture(),
          relation_fixture("items"),
          tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n"),
          commit_fixture(),
        ] {
        let out = @buffer.Buffer()
        out.write_byte(b'w')
        out.write_uint64_be(100)
        out.write_uint64_be(256)
        out.write_uint64_be(0)
        out.write_bytes(data)
        tcp.write(packet(b'd', out.to_bytes()))
      }
      assert_eq(tcp.read_some(), None)
      return
    }
  }
}

///|
async fn reconnect_scenario(plan : Array[String], expected : String) -> Unit {
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let dir = @fs.tmpdir(prefix="mooncdc-reconnect-")
    defer @fs.rmdir(dir, recursive=true)
    let config : ConnectionConfig = {
      host: "127.0.0.1",
      port: server.addr.port(),
      user: "user",
      password: "",
      database: "db",
      security: LocalPlaintext,
      timeout_ms: 200,
    }
    @async.with_task_group(group => {
      let peer = group.spawn(() => {
        for mode in ["initial"] + plan {
          let (tcp, _) = server.accept()
          defer tcp.close()
          mock_replication(tcp, mode)
        }
      })
      try
        subscribe(
          config,
          slot="slot",
          publication="pub",
          start=At(Lsn::parse("0/10")),
          checkpoint_path=dir + "/checkpoint",
          max_reconnects=if expected == "changed" { 3 } else { plan.length() },
          reconnect_delay_ms=1,
          heartbeat_ms=10000,
          sub => {
            let tx = sub.next()
            assert_eq(expected, "success")
            assert_eq(sub.diagnostics().reconnects, plan.length())
            assert_eq(tx.end_lsn.to_string(), "0/65")
            assert_true(tx.changes.any(c => c is Insert(_, [Text("1"), Null])))
            sub.ack(tx)
          },
        )
      catch {
        Server("57P03", message) => assert_eq(message, expected)
        UnsafeResume(_) => assert_eq(expected, "changed")
        e => raise e
      } noraise {
        _ => assert_eq(expected, "success")
      }
      peer.wait()
    })
  })
}

///|
async test "reconnect retries startup identify COPY and timeout failures" {
  reconnect_scenario(
    ["startup", "identify", "start", "timeout", "success"],
    "success",
  )
}

///|
async test "reconnect exhaustion returns last attempt error" {
  reconnect_scenario(["error1", "error2"], "error2")
}

///|
async test "reconnect refuses changed source without another attempt" {
  reconnect_scenario(["changed"], "changed")
}
