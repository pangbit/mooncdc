///|
fn logical_fixture(transactional : Bool, content : Bytes) -> Bytes {
  let out = @buffer.Buffer()
  out.write_byte(b'M')
  out.write_byte(if transactional { b'\x01' } else { b'\x00' })
  out.write_uint64_be(90)
  cstring(out, "schema")
  out.write_int_be(content.length())
  out.write_bytes(content)
  out.to_bytes()
}

///|
test "logical messages remain in transaction order with stable independent ids" {
  let decoder = Decoder::new(4096, 100, 10)
  ignore(decoder.decode(begin_fixture()))
  ignore(decoder.decode(logical_fixture(true, b"\x00\xff\x80")))
  ignore(decoder.decode(relation_fixture("items")))
  ignore(decoder.decode(tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n")))
  guard decoder.decode(commit_fixture())
    is Some((xid, commit_lsn, end_lsn, time, changes)) else {
    fail("missing commit")
  }
  guard changes[0] is Message(message) else { fail("missing message") }
  assert_eq(message.content, b"\x00\xff\x80")
  assert_eq(message.prefix, "schema")
  assert_true(message.transactional)
  let tx : Transaction = {
    id: "tx",
    xid,
    commit_lsn,
    end_lsn,
    commit_time_us: time,
    changes,
    session: "session",
    sequence: 1,
  }
  assert_eq(tx.event_id(0), Some("tx/message/0/5A"))
  assert_eq(tx.event_id(1), None)
  assert_eq(tx.event_id(2), Some("tx/0"))
}

///|
test "logical message parser rejects flags lengths and truncation" {
  let full = logical_fixture(true, b"\x00\xff")
  let invalid = [
    b"M\x02" + full[2:].to_owned(),
    full + b"x",
    full[:17].to_owned() + b"\xff\xff\xff\xff",
    logical_fixture(false, b""),
  ]
  for data in invalid + [ for i in 0..<full.length() => full[:i].to_owned() ] {
    let decoder = Decoder::new(4096, 100, 10)
    ignore(decoder.decode(begin_fixture()))
    try decoder.decode(data) catch {
      Protocol(_) => ()
      e => raise e
    } noraise {
      _ => fail("invalid logical message accepted")
    }
  }
}

///|
priv suberror TestMessageFailure {
  TestMessageFailure
}

///|
async test "nontransactional handler failure stops without ack or reconnect" {
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let dir = @fs.tmpdir(prefix="mooncdc-message-")
    defer @fs.rmdir(dir, recursive=true)
    let config : ConnectionConfig = {
      host: "127.0.0.1",
      port: server.addr.port(),
      user: "user",
      password: "",
      database: "db",
      security: LocalPlaintext,
      timeout_ms: 1000,
    }
    @async.with_task_group(group => {
      let peer = group.spawn(() => {
        let (tcp, _) = server.accept()
        defer tcp.close()
        mock_replication(tcp, "success", messages=true, stream=tcp => {
          let out = @buffer.Buffer()
          out.write_byte(b'w')
          out.write_uint64_be(90)
          out.write_uint64_be(256)
          out.write_uint64_be(0)
          out.write_bytes(logical_fixture(false, b"\x00\xff"))
          tcp.write(packet(b'd', out.to_bytes()))
          assert_eq(tcp.read_some(), None)
        })
      })
      try
        subscribe(
          config,
          slot="slot",
          publication="pub",
          start=At(Lsn::parse("0/10")),
          checkpoint_path=dir + "/checkpoint",
          messages=true,
          heartbeat_ms=10000,
          nontransactional_message=message => {
            assert_true(!message.transactional)
            assert_eq(message.content, b"\x00\xff")
            raise TestMessageFailure
          },
          sub => ignore(sub.next()),
        )
      catch {
        TestMessageFailure => ()
        e => raise e
      } noraise {
        _ => fail("handler failure swallowed")
      }
      peer.wait()
    })
    let source : Source = {
      system_id: "system",
      timeline: "1",
      database: "db",
      slot: "slot",
      publication: "pub",
    }
    assert_eq(load_checkpoint(dir + "/checkpoint", source), Lsn::parse("0/10"))
  })
}

///|
async test "live logical messages commit rollback and nontransactional delivery" {
  if live_config() is Some(config) {
    @async.with_timeout(15000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE TABLE cdc_messages(id int PRIMARY KEY); CREATE PUBLICATION cdc_messages FOR TABLE cdc_messages",
        ),
      )
      let start = fresh_slot(sql, "cdc_messages")
      let dir = @fs.tmpdir(prefix="mooncdc-messages-")
      defer @fs.rmdir(dir, recursive=true)
      let messages : Array[LogicalMessage] = []
      subscribe(
        config,
        slot="cdc_messages",
        publication="cdc_messages",
        start=At(start),
        checkpoint_path=dir + "/checkpoint",
        messages=true,
        nontransactional_message=message => messages.push(message),
        sub => {
          ignore(
            sql.query(
              "BEGIN; SELECT pg_logical_emit_message(true,'rolled_back','ignored'); SELECT pg_logical_emit_message(false,'outside',decode('00ff80','hex')); ROLLBACK; BEGIN; SELECT pg_logical_emit_message(true,'committed',decode('00ff80','hex')); INSERT INTO cdc_messages VALUES(1); COMMIT",
            ),
          )
          let tx = sub.next()
          assert_eq(messages.length(), 1)
          assert_eq(messages[0].prefix, "outside")
          assert_true(!messages[0].transactional)
          assert_eq(messages[0].content, b"\x00\xff\x80")
          let emitted = tx.changes.filter(c => c is Message(_))
          assert_eq(emitted.length(), 1)
          guard emitted[0] is Message(message) else { fail("missing message") }
          assert_eq(message.prefix, "committed")
          assert_true(message.transactional)
          assert_eq(message.content, b"\x00\xff\x80")
          assert_eq(sub.diagnostics().durable, start)
          sub.ack(tx)
          assert_eq(sub.diagnostics().durable, tx.end_lsn)
        },
      )
      drop_slot(sql, "cdc_messages")
      ignore(
        sql.query("DROP PUBLICATION cdc_messages; DROP TABLE cdc_messages"),
      )
      println(
        "LIVE: transactional commit/rollback, raw logical payload and nontransactional delivery passed",
      )
    })
  }
}
