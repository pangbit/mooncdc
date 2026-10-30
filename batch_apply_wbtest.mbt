///|
priv struct BatchApplyDestination {
  control : ApplyControl
  timed : Bool
  mut writes : Int32
  mut flushes : Int32
}

///|
impl Destination for BatchApplyDestination with fn reset_table(_, _, attempt~) {
  ignore(attempt)
}

///|
impl Destination for BatchApplyDestination with fn write_rows(_, _, _, _) {
  Durable
}

///|
impl Destination for BatchApplyDestination with fn write_transaction(self, _) {
  self.writes += 1
  if self.timed {
    @async.sleep(8)
  }
  if self.writes == 3 {
    // A continuously ready transaction queue must not reset the first-item
    // deadline. At least one prior batch must already have drained.
    assert_true(self.flushes > 0)
    self.control.stop()
  }
  Accepted
}

///|
impl Destination for BatchApplyDestination with fn flush(self) {
  self.flushes += 1
}

///|
async test "adaptive apply flushes by bytes or first-item age and releases queue slots" {
  for timed in [false, true] {
    @async.with_timeout(5000, () => {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let dir = @fs.tmpdir(prefix="mooncdc-batch-apply-")
      defer @fs.rmdir(dir, recursive=true)
      let config = ConnectionConfig::new(
        host="127.0.0.1",
        user="user",
        database="db",
        port=server.addr.port(),
        security=LocalPlaintext,
        timeout_ms=1000,
      )
      let read : async (@memory.Snapshot?) -> @memory.Refresh = _ => {
        snapshot: { used: 0UL, total: 1000000000UL, source: @memory.System, },
        fresh: true,
      }
      @memory.with_sampler(2, None, read, monitor => {
        let batch = BatchRuntime::new(
          monitor,
          BatchConfig::new(
            max_bytes=if timed { 33554432UL } else { 1UL },
            max_fill_ms=if timed { 10 } else { 10000 },
          ),
        )
        let destination : BatchApplyDestination = {
          control: ApplyControl(),
          timed,
          writes: 0,
          flushes: 0,
        }
        @async.with_task_group(group => {
          let peer = group.spawn(() => {
            let (tcp, _) = server.accept()
            defer tcp.close()
            mock_replication(tcp, "success", stream=tcp => {
              for lsn in [100UL, 200UL, 300UL] {
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
              while tcp.read_some() is Some(_) {
                ()
              }
            })
          })
          subscribe_impl(
            config,
            slot="slot",
            publication="pub",
            start=At(Lsn::parse("0/10")),
            checkpoint_path=dir + "/checkpoint",
            max_reconnects=0,
            heartbeat_ms=10000,
            memory=monitor,
            batch_governor=batch.governor,
            sub => {
              assert_eq(batch.governor.registered_slots(), 17UL)
              while sub.diagnostics().queued_transactions < 3 {
                @async.sleep(1)
              }
              sub.apply_to_impl(
                destination,
                control=destination.control,
                batch~,
                flush_interval_ms=10000,
              )
              assert_eq(sub.diagnostics().durable.value, 301UL)
            },
          )
          peer.wait()
        })
        assert_eq(batch.governor.registered_slots(), 0UL)
        assert_eq(destination.writes, 3)
        if timed {
          assert_true(destination.flushes >= 2)
        } else {
          assert_eq(destination.flushes, 3)
        }
      })
    })
  }
}
