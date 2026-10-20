///|
priv struct WalProbeDestination {
  path : String
  source : Source
  control : ApplyControl
  pressure : MemoryPressure
  monitor : @memory.Monitor
  scenario : String
  ids : Array[String]
  durable_ids : Map[String, Unit]
  owned_ids : Array[String]
  delivered : @async.CondVar
  mut attempt : Int
  mut commit : Bool
  mut flushes : Int
  mut partial_flushes : Int
  mut commits : Int
}

///|
impl Destination for WalProbeDestination with fn supports_wal_batches(_self) {
  true
}

///|
impl Destination for WalProbeDestination with fn reset_table(
  _self,
  _r,
  attempt~,
) {
  ignore(attempt)
}

///|
impl Destination for WalProbeDestination with fn write_rows(
  _self,
  _r,
  _rows,
  _id,
) {
  Durable
}

///|
impl Destination for WalProbeDestination with fn write_transaction(_self, _tx) {
  fail("event path supplied a partial Transaction")
}

///|
impl Destination for WalProbeDestination with fn write_wal_batch(self, batch) {
  assert_eq(load_checkpoint(self.path, self.source), Lsn::parse("0/10"))
  self.commit = batch.end_lsn is Some(_)
  if self.commit {
    self.commits += 1
  }
  for index, change in batch.changes {
    if change is Insert(_, _) {
      let id = batch.event_id(index).unwrap()
      self.ids.push(id)
      self.owned_ids.push(id)
      if self.scenario == "pressure" && self.ids.length() == 1 {
        self.pressure.used = 90
        while !self.monitor.is_paused() {
          @async.sleep(1)
        }
      }
    }
  }
  Accepted
}

///|
impl Destination for WalProbeDestination with fn flush(self) {
  self.flushes += 1
  if !self.commit && !self.owned_ids.is_empty() {
    self.partial_flushes += 1
  }
  for id in self.owned_ids {
    self.durable_ids[id] = ()
  }
  self.owned_ids.clear()
  if self.monitor.is_paused() {
    self.pressure.used = 74
  }
  if self.scenario == "stop" && self.partial_flushes == 1 {
    self.control.stop()
  }
  if self.commit {
    self.control.stop()
  }
  self.delivered.broadcast()
}

///|
async fn wal_send(tcp : @socket.Tcp, data : Bytes) -> Unit {
  let out = @buffer.Buffer()
  out.write_byte(b'w')
  out.write_uint64_be(100)
  out.write_uint64_be(4096)
  out.write_int64_be(0)
  out.write_bytes(data)
  tcp.write(packet(b'd', out.to_bytes()))
}

///|
async test "event apply drains before Commit but checkpoints only after its barrier and replays stable IDs" {
  for
    scenario in ["bytes", "deadline", "pressure", "replay", "stop", "malformed"] {
    @async.with_timeout(8000, () => {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let dir = @fs.tmpdir(prefix="mooncdc-wal-apply-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/checkpoint"
      let source = wal_test_source()
      save_checkpoint(path, source, Lsn::parse("0/10"))
      let config = ConnectionConfig::new(
        host="127.0.0.1",
        port=server.addr.port(),
        user="user",
        database="db",
        security=LocalPlaintext,
        timeout_ms=1000,
      )
      let pressure = MemoryPressure::{ used: 0, }
      @memory.with_sampler(
        2,
        Some((0.85F, 0.75F)),
        _ => {
          snapshot: {
            used: pressure.used * 10000000UL,
            total: 1000000000UL,
            source: @memory.ProcessCgroup,
          },
          fresh: true,
        },
        monitor => {
          let batch = BatchRuntime::new(
            monitor,
            BatchConfig::new(
              max_bytes=if scenario == "deadline" { 1000000UL } else { 1UL },
              max_fill_ms=if scenario == "deadline" { 10 } else { 10000 },
            ),
          )
          let target : WalProbeDestination = {
            path,
            source,
            control: ApplyControl(),
            pressure,
            monitor,
            scenario,
            ids: [],
            durable_ids: Map([]),
            owned_ids: [],
            delivered: @async.CondVar::Cond(),
            attempt: 0,
            commit: false,
            flushes: 0,
            partial_flushes: 0,
            commits: 0,
          }
          @async.with_task_group(group => {
            let peer = group.spawn(() => {
              for attempt in 0..<(if scenario == "replay" { 2 } else { 1 }) {
                let (tcp, _) = server.accept()
                defer tcp.close()
                mock_replication(tcp, "success", stream=tcp => {
                  wal_send(tcp, begin_fixture())
                  wal_send(tcp, relation_fixture("items"))
                  if attempt == 1 {
                    wal_send(tcp, relation_fixture("items"))
                  }
                  wal_send(tcp, row_fixture_for_wal())
                  while target.partial_flushes <= attempt {
                    target.delivered.wait()
                  }
                  assert_eq(load_checkpoint(path, source), Lsn::parse("0/10"))
                  if scenario == "replay" && attempt == 0 {
                    return
                  }
                  if scenario == "stop" {
                    while tcp.read_some() is Some(_) {
                      ()
                    }
                    return
                  }
                  wal_send(tcp, b"Y\x00\x00\x00\x19public\x00custom\x00")
                  wal_send(tcp, row_fixture_for_wal())
                  if scenario == "malformed" {
                    while target.partial_flushes < 2 {
                      target.delivered.wait()
                    }
                  }
                  wal_send(
                    tcp,
                    if scenario == "malformed" {
                      commit_fixture(lsn=101)
                    } else {
                      commit_fixture()
                    },
                  )
                  while tcp.read_some() is Some(_) {
                    ()
                  }
                })
              }
            })
            let retry = ApplyRetry::new(
              target.control,
              1,
              if scenario == "replay" {
                1
              } else {
                0
              },
            )
            try
              retry.run(() => {
                target.attempt += 1
                subscribe_impl(
                  config,
                  slot="slot",
                  publication="pub",
                  start=Resume,
                  checkpoint_path=path,
                  worker_retry=true,
                  wal_events=true,
                  batch_governor=batch.governor,
                  memory=monitor,
                  heartbeat_ms=10000,
                  sub => {
                    assert_eq(batch.governor.registered_slots(), 2UL)
                    sub.apply_events(target, control=target.control, batch~)
                    assert_true(sub.wal_queue.length() <= 1)
                  },
                )
              })
            catch {
              Protocol("inconsistent commit boundary") =>
                assert_eq(scenario, "malformed")
              error => raise error
            } noraise {
              _ => assert_true(scenario != "malformed")
            }
            peer.wait()
            assert_eq(retry.retries, if scenario == "replay" { 1 } else { 0 })
          })
          assert_eq(batch.governor.registered_slots(), 0UL)
          assert_true(Destination::supports_wal_batches(target))
          assert_true(target.flushes > 0)
          assert_true(target.partial_flushes > 0)
          if scenario == "replay" {
            assert_eq(target.ids.length(), 3)
            assert_eq(target.ids[0], target.ids[1])
            assert_true(target.ids[2] != target.ids[1])
            assert_eq(target.attempt, 2)
          }
          assert_eq(
            load_checkpoint(path, source),
            Lsn::parse(
              if scenario == "stop" || scenario == "malformed" {
                "0/10"
              } else {
                "0/65"
              },
            ),
          )
          assert_eq(
            target.commits,
            if scenario == "stop" || scenario == "malformed" {
              0
            } else {
              1
            },
          )
          assert_eq(
            target.durable_ids.length(),
            if scenario == "stop" {
              1
            } else {
              2
            },
          )
          let lock = @fs.open(path + ".lock", mode=ReadWrite)
          defer lock.close()
          assert_true(lock.try_lock(Exclusive))
        },
      )
    })
  }
}
