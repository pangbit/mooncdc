///|
async fn unsafe_slot_scenarios(reconnect : Bool) -> Unit {
  // Independently exercise both LSN fields and both invalidation signals.
  // These synthetic catalog rows isolate guards, not PostgreSQL state transitions.
  for
    (restart, confirmed, status, reason, expected) in [
      (
        "0/20",
        "0/10",
        "reserved",
        None,
        "slot has advanced beyond durable consumer position",
      ),
      (
        "0/10",
        "0/20",
        "reserved",
        None,
        "slot has advanced beyond durable consumer position",
      ),
      ("0/10", "0/10", "lost", None, "replication slot invalidated"),
      (
        "0/10",
        "0/10",
        "reserved",
        Some("wal_removed"),
        "replication slot invalidated",
      ),
    ] {
    @async.with_timeout(5000, () => {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let dir = @fs.tmpdir(prefix="mooncdc-unsafe-resume-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/checkpoint"
      let source : Source = {
        system_id: "system",
        timeline: "1",
        database: "db",
        slot: "slot",
        publication: "pub",
      }
      let start = Lsn::parse("0/10")
      save_checkpoint(path, source, start)
      let original = checkpoint_bytes(source, start)
      let config : ConnectionConfig = {
        host: "127.0.0.1",
        port: server.addr.port(),
        user: "user",
        password: "",
        database: "db",
        security: LocalPlaintext,
        timeout_ms: 1000,
      }
      let delivered = @async.CondVar::Cond()
      let mut saw_transaction = false
      let mut subscription : Subscription? = None
      @async.with_task_group(group => {
        let peer = group.spawn(() => {
          if reconnect {
            let (tcp, _) = server.accept()
            defer tcp.close()
            mock_replication(tcp, "success", stream=tcp => {
              for data in [begin_fixture(), commit_fixture()] {
                let out = @buffer.Buffer()
                out.write_byte(b'w')
                out.write_uint64_be(100)
                out.write_uint64_be(256)
                out.write_int64_be(0)
                out.write_bytes(data)
                tcp.write(packet(b'd', out.to_bytes()))
              }
              // Disconnect with received=0/65 but durable=0/10. A slot at
              // 0/20 must fail even though it is behind the received position.
              while !saw_transaction {
                delivered.wait()
              }
            })
          }
          let (tcp, _) = server.accept()
          defer tcp.close()
          mock_replication(tcp, "success", rejected_slot=[
            Some("pgoutput"),
            Some("db"),
            Some(restart),
            Some(confirmed),
            Some(status),
            reason,
            Some("false"),
          ])
        })
        try
          subscribe(
            config,
            slot="slot",
            publication="pub",
            start=Resume,
            checkpoint_path=path,
            max_reconnects=3,
            reconnect_delay_ms=1,
            heartbeat_ms=10000,
            sub => {
              assert_true(reconnect)
              subscription = Some(sub)
              let tx = sub.next()
              assert_eq(tx.end_lsn, Lsn::parse("0/65"))
              assert_eq(sub.diagnostics().durable, start)
              saw_transaction = true
              delivered.broadcast()
              ignore(sub.next())
            },
          )
        catch {
          UnsafeResume(message) => assert_eq(message, expected)
          e => raise e
        } noraise {
          _ => fail("unsafe slot accepted")
        }
        peer.wait()
      })
      assert_eq(saw_transaction, reconnect)
      if subscription is Some(sub) {
        assert_eq(sub.diagnostics().reconnects, 1)
        assert_eq(sub.diagnostics().durable, start)
        assert_eq(sub.diagnostics().outstanding_transactions, 1)
      }
      let file = @fs.open(path, mode=ReadOnly)
      defer file.close()
      assert_eq(file.size(), original.length().to_int64())
      assert_eq(file.read_exactly(original.length()), original)
      assert_eq(load_checkpoint(path, source), start)
      let lock = @fs.open(path + ".lock", mode=ReadWrite)
      defer lock.close()
      assert_true(lock.try_lock(Exclusive))
    })
  }
}

///|
async test "resume rejects advanced and invalidated slots before replication" {
  unsafe_slot_scenarios(false)
}

///|
async test "reconnect rejects unsafe slots against durable rather than received progress" {
  unsafe_slot_scenarios(true)
}
