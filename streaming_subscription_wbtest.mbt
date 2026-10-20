///|
async test "v2 reconnect discards provisional buffers and preserves all negotiation" {
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let dir = @fs.tmpdir(prefix="mooncdc-stream-reconnect-")
    defer @fs.rmdir(dir, recursive=true)
    let config : ConnectionConfig = {
      hostaddr: None,
      keepalive: TcpKeepaliveConfig::new(),
      host: "127.0.0.1",
      port: server.addr.port(),
      user: "user",
      password: "",
      database: "db",
      security: LocalPlaintext,
      timeout_ms: 1000,
    }
    let source : Source = {
      system_id: "system",
      timeline: "1",
      database: "db",
      slot: "slot",
      publication: "pub",
    }
    let checkpoint = dir + "/checkpoint"
    let outside : Array[LogicalMessage] = []
    @async.with_task_group(group => {
      let peer = group.spawn(() => {
        for attempt in 0..<2 {
          let (tcp, _) = server.accept()
          defer tcp.close()
          mock_replication(
            tcp,
            "success",
            protocol=V2Streaming,
            binary=true,
            messages=true,
            stream=tcp => {
              if attempt == 1 {
                assert_eq(
                  load_checkpoint(checkpoint, source),
                  Lsn::parse("0/10"),
                )
                wal_send(tcp, logical_fixture(false, b"outside"))
              }
              for
                bytes in [
                  stream_start(10),
                  stream_change(10, relation_fixture("items")),
                  stream_change(
                    10,
                    tuple_fixture(
                      b'I',
                      b"N" + protocol_tuple([Binary(b"\x00\xff"), Null]),
                    ),
                  ),
                ] {
                wal_send(tcp, bytes)
              }
              if attempt == 1 {
                wal_send(
                  tcp,
                  stream_change(10, logical_fixture(true, b"committed")),
                )
                wal_send(tcp, b"E")
                wal_send(tcp, stream_commit(10))
                assert_eq(tcp.read_some(), None)
              }
            },
          )
        }
      })
      subscribe(
        config,
        slot="slot",
        publication="pub",
        start=At(Lsn::parse("0/10")),
        checkpoint_path=checkpoint,
        protocol=V2Streaming,
        binary=true,
        messages=true,
        heartbeat_ms=10000,
        reconnect_delay_ms=1,
        max_reconnects=1,
        nontransactional_message=message => outside.push(message),
        sub => {
          let tx = sub.next()
          assert_eq(sub.diagnostics().reconnects, 1)
          assert_eq(sub.diagnostics().durable, Lsn::parse("0/10"))
          assert_eq(tx.changes.length(), 3)
          assert_true(tx.changes[1] is Insert(_, [Binary(b"\x00\xff"), Null]))
          assert_true(tx.changes[2] is Message({ content: b"committed", .. }))
          assert_eq(tx.event_id(1), Some("system/db/slot/0/64/0"))
          assert_eq(outside.length(), 1)
          assert_eq(outside[0].content, b"outside")
          sub.ack(tx)
          assert_eq(load_checkpoint(checkpoint, source), tx.end_lsn)
        },
      )
      peer.wait()
    })
  })
}

///|
async test "v2 aggregate limit is fatal with unchanged checkpoint and no provisional delivery" {
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let dir = @fs.tmpdir(prefix="mooncdc-stream-limit-")
    defer @fs.rmdir(dir, recursive=true)
    let config : ConnectionConfig = {
      hostaddr: None,
      keepalive: TcpKeepaliveConfig::new(),
      host: "127.0.0.1",
      port: server.addr.port(),
      user: "user",
      password: "",
      database: "db",
      security: LocalPlaintext,
      timeout_ms: 1000,
    }
    let checkpoint = dir + "/checkpoint"
    @async.with_task_group(group => {
      let peer = group.spawn(() => {
        let (tcp, _) = server.accept()
        defer tcp.close()
        mock_replication(tcp, "success", protocol=V2Streaming, stream=tcp => {
          for
            bytes in [
              stream_start(10),
              stream_change(10, relation_fixture("items")),
              b"E",
              stream_start(20),
              stream_change(20, stream_insert("over-limit")),
            ] {
            wal_send(tcp, bytes)
          }
          assert_eq(tcp.read_some(), None)
        })
      })
      try
        subscribe(
          config,
          slot="slot",
          publication="pub",
          start=At(Lsn::parse("0/10")),
          checkpoint_path=checkpoint,
          protocol=V2Streaming,
          max_transaction_events=1,
          heartbeat_ms=10000,
          max_reconnects=3,
          async fn(sub : Subscription) -> Unit {
            ignore(sub.next())
            fail("provisional transaction delivered")
          },
        )
      catch {
        LimitExceeded(_) => ()
        e => raise e
      } noraise {
        _ => fail("stream limit ignored")
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
    assert_eq(load_checkpoint(checkpoint, source), Lsn::parse("0/10"))
    let lock = record_lock(checkpoint, None).unwrap()
    lock.close()
  })
}
