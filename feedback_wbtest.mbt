///|
async test "feedback uses durable prefix after ack gaps write failure and keepalive" {
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let dir = @fs.tmpdir(prefix="mooncdc-feedback-")
    defer @fs.rmdir(dir, recursive=true)
    let path = dir + "/checkpoint"
    let config : ConnectionConfig = {
      host: "127.0.0.1",
      port: server.addr.port(),
      user: "user",
      password: "",
      database: "db",
      security: LocalPlaintext,
      timeout_ms: 1000,
    }
    let finished = @async.CondVar::Cond()
    let mut checked = false
    @async.with_task_group(group => {
      let peer = group.spawn(() => {
        let (tcp, _) = server.accept()
        defer tcp.close()
        mock_replication(tcp, "success", stream=tcp => {
          for lsn in [100UL, 200UL] {
            for
              data in [
                begin_fixture(lsn~),
                relation_fixture("items"),
                tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n"),
                commit_fixture(lsn~),
              ] {
              let out = @buffer.Buffer()
              out.write_byte(b'w')
              out.write_uint64_be(lsn)
              out.write_uint64_be(4096)
              out.write_int64_be(0)
              out.write_bytes(data)
              tcp.write(packet(b'd', out.to_bytes()))
            }
          }
          // Read actual CopyData bytes, not just Subscription diagnostics.
          for i, expected in [16UL, 16UL, 16UL, 201UL, 201UL] {
            if i == 4 {
              let keepalive = @buffer.Buffer()
              keepalive.write_byte(b'k')
              keepalive.write_uint64_be(8192)
              keepalive.write_int64_be(0)
              keepalive.write_byte(b'\x01')
              tcp.write(packet(b'd', keepalive.to_bytes()))
            }
            let (tag, body) = read_frame(tcp, 4096)
            assert_eq(tag, b'd')
            assert_eq(body.length(), 34)
            let cursor = Cursor::new(body)
            assert_eq(cursor.byte(), b'r')
            for _ in 0..<3 {
              assert_eq(cursor.u64(), expected)
            }
            assert_true(cursor.u64() > 0)
            assert_eq(cursor.byte(), b'\x00')
            cursor.done()
          }
          checked = true
          finished.broadcast()
          assert_eq(tcp.read_some(), None)
        })
      })
      subscribe(
        config,
        slot="slot",
        publication="pub",
        start=At(Lsn::parse("0/10")),
        checkpoint_path=path,
        max_reconnects=0,
        heartbeat_ms=10000,
        sub => {
          let a = sub.next()
          let b = sub.next()
          assert_eq(sub.diagnostics().received, b.end_lsn)
          sub.feedback()
          sub.ack(b)
          sub.feedback()
          @fs.mkdir(path + ".tmp")
          try sub.ack(a) catch {
            _ => ()
          } noraise {
            _ => fail("expected checkpoint write failure")
          }
          assert_eq(load_checkpoint(path, sub.source), Lsn::parse("0/10"))
          sub.feedback()
          @fs.rmdir(path + ".tmp")
          sub.ack(a)
          assert_eq(load_checkpoint(path, sub.source), b.end_lsn)
          assert_eq(sub.diagnostics().outstanding_transactions, 0)
          sub.feedback()
          while !checked {
            finished.wait()
          }
          assert_eq(sub.diagnostics().server_end, Lsn::parse("0/2000"))
          assert_eq(sub.diagnostics().durable, b.end_lsn)
        },
      )
      peer.wait()
    })
  })
}
