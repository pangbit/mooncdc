///|
async test "backpressure waits for durable prefix and resumes without losing transactions" {
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let dir = @fs.tmpdir(prefix="mooncdc-pressure-")
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
        mock_replication(tcp, "success", stream=tcp => {
          for lsn in [100UL, 200UL, 300UL, 400UL, 500UL] {
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
          assert_eq(tcp.read_some(), None)
        })
      })
      subscribe(
        config,
        slot="slot",
        publication="pub",
        start=At(Lsn::parse("0/10")),
        checkpoint_path=dir + "/checkpoint",
        max_reconnects=0,
        heartbeat_ms=10000,
        max_pending_transactions=2,
        backpressure=true,
        sub => {
          let a = sub.next()
          let b = sub.next()
          @async.sleep(20)
          assert_eq(sub.diagnostics().outstanding_transactions, 2)
          assert_eq(sub.diagnostics().received, b.end_lsn)
          sub.ack(b)
          @async.sleep(20)
          assert_eq(sub.diagnostics().outstanding_transactions, 2)
          assert_eq(sub.diagnostics().durable, Lsn::parse("0/10"))
          sub.ack(a)
          for expected in [301UL, 401UL, 501UL] {
            let tx = sub.next()
            assert_eq(tx.end_lsn.value, expected)
            sub.ack(tx)
          }
          assert_eq(sub.diagnostics().outstanding_transactions, 0)
        },
      )
      peer.wait()
    })
  })
}
