///|
priv struct MemoryPressure {
  mut used : UInt64
}

///|
fn MemoryPressure::sample(
  self : MemoryPressure,
  _previous : @memory.Snapshot?,
) -> @memory.Refresh {
  {
    snapshot: { used: self.used, total: 100UL, source: @memory.ProcessCgroup, },
    fresh: true,
  }
}

///|
priv struct MemoryApplyDestination {
  pressure : MemoryPressure
  monitor : @memory.Monitor
  control : ApplyControl
  mut writes : Int32
  mut flushes : Int32
}

///|
impl Destination for MemoryApplyDestination with fn reset_table(_, _, attempt~) {
  ignore(attempt)
}

///|
impl Destination for MemoryApplyDestination with fn write_rows(_, _, _, _) {
  Durable
}

///|
impl Destination for MemoryApplyDestination with fn write_transaction(self, tx) {
  self.writes += 1
  assert_eq(tx.end_lsn.value, self.writes.to_uint64() * 100UL + 1UL)
  if self.writes == 1 {
    self.pressure.used = 90UL
    while !self.monitor.is_paused() {
      @async.sleep(1)
    }
  } else {
    assert_eq(self.flushes, 1)
    self.control.stop()
  }
  Accepted
}

///|
impl Destination for MemoryApplyDestination with fn flush(self) {
  if self.flushes == 0 {
    assert_true(self.monitor.is_paused())
    self.pressure.used = 74UL
  }
  self.flushes += 1
}

///|
async test "memory pressure pauses WAL reads but accepted writes flush and checkpoint" {
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let dir = @fs.tmpdir(prefix="mooncdc-memory-apply-")
    defer @fs.rmdir(dir, recursive=true)
    let config = ConnectionConfig::new(
      host="127.0.0.1",
      user="user",
      password="",
      database="db",
      port=server.addr.port(),
      security=LocalPlaintext,
      timeout_ms=1000,
    )
    let pressure = MemoryPressure::{ used: 90UL, }
    @memory.with_sampler(
      2,
      Some((0.85F, 0.75F)),
      previous => pressure.sample(previous),
      monitor => {
        let destination : MemoryApplyDestination = {
          pressure,
          monitor,
          control: ApplyControl(),
          writes: 0,
          flushes: 0,
        }
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
              // Acks send standby status frames before the connection closes.
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
            sub => {
              @async.sleep(20)
              assert_eq(sub.diagnostics().received, Lsn::parse("0/10"))
              assert_eq(sub.diagnostics().outstanding_transactions, 0)
              pressure.used = 74UL
              while sub.diagnostics().outstanding_transactions < 2 {
                @async.sleep(1)
              }
              sub.apply_to(
                destination,
                control=destination.control,
                flush_interval_ms=10000,
              )
              assert_eq(sub.diagnostics().durable.value, 201UL)
              assert_eq(sub.diagnostics().outstanding_transactions, 0)
            },
          )
          peer.wait()
          assert_eq(destination.writes, 2)
          assert_eq(destination.flushes, 2)
        })
      },
    )
  })
}

///|
async test "intentional source pressure does not hide a stalled destination" {
  let control = ApplyControl()
  control.lifecycle = Running
  let activity = control.observe(InitialTableCopy)
  defer activity.close()
  activity.source_paused(true)
  @async.sleep(20)
  assert_true(control.health(stall_timeout_ms=10).ready)
  activity.destination_busy(true)
  @async.sleep(20)
  assert_false(control.health(stall_timeout_ms=10).live)
  activity.destination_busy(false)
  assert_true(control.health(stall_timeout_ms=10).ready)
  activity.source_paused(false)
  assert_true(control.health(stall_timeout_ms=10).ready)
}

///|
test "memory backpressure configuration validates finite hysteresis before startup" {
  MemoryBackpressureConfig::new().validate()
  for
    config in [
      MemoryBackpressureConfig::new(activate_threshold=0.0F),
      MemoryBackpressureConfig::new(activate_threshold=0.75F),
      MemoryBackpressureConfig::new(resume_threshold=-0.1F),
      MemoryBackpressureConfig::new(refresh_interval_ms=0),
      MemoryBackpressureConfig::new(activate_threshold=0.0F / 0.0F),
    ] {
    try config.validate() catch {
      InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid pressure config accepted")
    }
  }
}

///|
async test "pressure keeps feedback conservative and graceful stop cancels blocked WAL read" {
  @async.with_timeout(5000, () => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let dir = @fs.tmpdir(prefix="mooncdc-memory-stop-")
    defer @fs.rmdir(dir, recursive=true)
    let config = ConnectionConfig::new(
      host="127.0.0.1",
      user="user",
      database="db",
      port=server.addr.port(),
      security=LocalPlaintext,
      timeout_ms=1000,
    )
    let pressure = MemoryPressure::{ used: 90UL, }
    @memory.with_sampler(
      2,
      Some((0.85F, 0.75F)),
      previous => pressure.sample(previous),
      monitor => {
        let destination : MemoryApplyDestination = {
          pressure,
          monitor,
          control: ApplyControl(),
          writes: 0,
          flushes: 0,
        }
        @async.with_task_group(group => {
          let peer = group.spawn(() => {
            let (tcp, _) = server.accept()
            defer tcp.close()
            mock_replication(tcp, "success", stream=tcp => {
              for _ in 0..<3 {
                let (tag, body) = read_frame(tcp, 1024)
                assert_eq(tag, b'd')
                let cursor = Cursor::new(body)
                assert_eq(cursor.byte(), b'r')
                for _ in 0..<3 {
                  assert_eq(cursor.u64(), 16UL)
                }
                assert_true(monitor.is_paused())
              }
              destination.control.stop()
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
            heartbeat_ms=5,
            memory=monitor,
            sub => {
              sub.apply_to(destination, control=destination.control)
              assert_eq(sub.diagnostics().durable.value, 16UL)
            },
          )
          peer.wait()
          assert_eq(destination.writes, 0)
          assert_eq(destination.flushes, 0)
        })
      },
    )
  })
}
