///|
priv suberror TestDestinationFailure {
  TestDestinationFailure
  TestDestinationCancelled
}

///|
priv struct ProbeDestination {
  mode : String
  control : ApplyControl
  events : Array[String]
  pending : Array[String]
  durable : Array[String]
  batches : Array[TableCopyBatchId]
  mut writes : Int
  mut flushes : Int
}

///|
fn ProbeDestination::new(
  mode : String,
  control : ApplyControl,
) -> ProbeDestination {
  {
    mode,
    control,
    events: [],
    pending: [],
    durable: [],
    batches: [],
    writes: 0,
    flushes: 0,
  }
}

///|
impl Destination for ProbeDestination with fn startup(self) {
  self.events.push("startup")
}

///|
impl Destination for ProbeDestination with fn shutdown(self) {
  assert_true(self.pending.is_empty())
  self.events.push("shutdown")
}

///|
impl Destination for ProbeDestination with fn reset_table(
  self,
  _relation,
  attempt~,
) {
  assert_true(!attempt.is_empty())
  self.pending.clear()
  self.durable.clear()
  self.events.push("reset")
}

///|
impl Destination for ProbeDestination with fn write_rows(
  self,
  relation,
  rows,
  id,
) {
  if rows.is_empty() {
    assert_eq(id, None)
    self.events.push("empty")
  } else {
    guard id is Some(id) else { fail("batch ID missing") }
    assert_eq(id.relation_id, relation.oid)
    self.batches.push(id)
    for row in rows {
      guard row is [Text(key)] else { fail("unexpected snapshot row") }
      self.pending.push(key)
    }
  }
  Accepted
}

///|
impl Destination for ProbeDestination with fn write_transaction(self, tx) {
  self.writes += 1
  self.pending.push(tx.id)
  if self.writes == 3 {
    self.control.stop()
  }
  if self.mode == "cumulative" && self.writes == 3 {
    for item in self.pending {
      self.durable.push(item)
    }
    self.pending.clear()
    Durable
  } else {
    Accepted
  }
}

///|
impl Destination for ProbeDestination with fn flush(self) {
  self.flushes += 1
  if self.mode == "fail" {
    raise TestDestinationFailure
  }
  if self.mode == "cancel" {
    @async.sleep(10000)
  }
  for item in self.pending {
    self.durable.push(item)
  }
  self.pending.clear()
  if self.mode == "idle" {
    self.control.stop()
  }
}

///|
async test "destination cumulative durability idle flush stop and failure preserve checkpoints" {
  for
    mode in [
      "accepted", "threshold", "cumulative", "idle", "fail", "cancel", "empty",
    ] {
    @async.with_timeout(5000, () => {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let dir = @fs.tmpdir(prefix="mooncdc-destination-")
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
      let control = ApplyControl()
      let destination = ProbeDestination::new(mode, control)
      let count = if mode == "idle" {
        1
      } else if mode == "empty" {
        0
      } else {
        3
      }
      @async.with_task_group(group => {
        let peer = group.spawn(() => {
          let (tcp, _) = server.accept()
          defer tcp.close()
          mock_replication(tcp, "success", stream=tcp => {
            for i in 1..<=count {
              let lsn = i.to_uint64() * 100UL
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
                out.write_uint64_be(0)
                out.write_bytes(data)
                tcp.write(packet(b'd', out.to_bytes()))
              }
            }
            if mode == "empty" {
              @async.sleep(20)
              control.stop()
            }
            assert_eq(tcp.read_some(), None)
          })
        })
        try
          @async.with_timeout(1000, error=TestDestinationCancelled, () => {
            with_destination(destination, () => {
              subscribe(
                config,
                slot="slot",
                publication="pub",
                start=At(Lsn::parse("0/10")),
                checkpoint_path=dir + "/checkpoint",
                heartbeat_ms=10000,
                sub => {
                  sub.apply_to(
                    destination,
                    control~,
                    flush_every=if mode == "threshold" { 2 } else { 4 },
                    flush_interval_ms=20,
                  )
                },
              )
            })
          })
        catch {
          TestDestinationFailure => assert_eq(mode, "fail")
          TestDestinationCancelled => assert_eq(mode, "cancel")
          e => raise e
        } noraise {
          _ => assert_true(mode != "fail" && mode != "cancel")
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
      let expected : UInt64 = if mode == "fail" ||
        mode == "cancel" ||
        mode == "empty" {
        16
      } else {
        count.to_uint64() * 100UL + 1UL
      }
      assert_eq(load_checkpoint(dir + "/checkpoint", source).value, expected)
      assert_eq(destination.writes, count)
      assert_eq(
        destination.flushes,
        if mode == "cumulative" || mode == "empty" {
          0
        } else if mode == "threshold" {
          2
        } else {
          1
        },
      )
      assert_eq(
        destination.durable.length(),
        if mode == "fail" || mode == "cancel" {
          0
        } else {
          count
        },
      )
      assert_eq(
        destination.events,
        if mode == "fail" || mode == "cancel" {
          ["startup"]
        } else {
          ["startup", "shutdown"]
        },
      )
    })
  }
}

///|
async test "live destination snapshot batch identities reset and durable barrier" {
  if live_config() is Some(config) {
    @async.with_timeout(15000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE TABLE cdc_destination(id int PRIMARY KEY); INSERT INTO cdc_destination VALUES(1),(2),(3); CREATE PUBLICATION cdc_destination FOR TABLE cdc_destination",
        ),
      )
      let destination = ProbeDestination::new("snapshot", ApplyControl())
      let failed = ProbeDestination::new("fail", ApplyControl())
      try
        with_destination(failed, () => {
          ignore(
            copy_snapshot_to(
              config,
              failed,
              slot="cdc_destination",
              publication="cdc_destination",
            ),
          )
        })
      catch {
        TestDestinationFailure => ()
        e => raise e
      } noraise {
        _ => fail("failed durability barrier returned snapshot position")
      }
      assert_true(failed.durable.is_empty())
      assert_eq(failed.events, ["startup", "reset", "empty"])
      drop_slot(sql, "cdc_destination")
      let dir = @fs.tmpdir(prefix="mooncdc-destination-live-")
      defer @fs.rmdir(dir, recursive=true)
      with_destination(destination, () => {
        let mut previous = ""
        for round in 0..<2 {
          let point = copy_snapshot_to(
            config,
            destination,
            slot="cdc_destination",
            publication="cdc_destination",
            batch_rows=2,
          )
          assert_true(point.lsn.value > 0)
          assert_eq(destination.durable, ["1", "2", "3"])
          assert_true(destination.pending.is_empty())
          let id = destination.batches.last().unwrap()
          assert_eq(id.sequence, 2UL)
          assert_true(id.attempt != previous)
          previous = id.attempt
          if round == 1 {
            let path = dir + "/checkpoint"
            point.save_checkpoint(path)
            assert_eq(load_checkpoint(path, point.source), point.lsn)
            try point.save_checkpoint(path) catch {
              CheckpointError(_) => ()
              e => raise e
            } noraise {
              _ => fail("snapshot checkpoint overwritten")
            }
            subscribe(
              config,
              slot="cdc_destination",
              publication="cdc_destination",
              start=Resume,
              checkpoint_path=path,
              sub => {
                for id in 4..<=6 {
                  ignore(sql.query("INSERT INTO cdc_destination VALUES(\{id})"))
                }
                sub.apply_to(destination, control=destination.control)
                assert_true(sub.diagnostics().durable > point.lsn)
              },
            )
            assert_eq(destination.writes, 3)
            assert_eq(destination.durable.length(), 6)
          }
          drop_slot(sql, "cdc_destination")
        }
      })
      assert_eq(destination.flushes, 3)
      assert_eq(destination.batches.length(), 4)
      assert_eq(destination.events, [
        "startup", "reset", "empty", "reset", "empty", "shutdown",
      ])
      ignore(
        sql.query(
          "DROP PUBLICATION cdc_destination; DROP TABLE cdc_destination",
        ),
      )
      println(
        "LIVE: destination snapshot batch IDs, reset, cumulative flush and lifecycle passed",
      )
    })
  }
}
