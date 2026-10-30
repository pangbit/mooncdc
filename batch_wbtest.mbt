///|
test "batch configuration permits zero fill time and estimates native text bytes" {
  BatchConfig::new(max_fill_ms=0).validate()
  assert_eq(row_size_hint([Text("1")]), 42UL)
  assert_eq(row_size_hint([Null]), 24UL)
  assert_eq(row_size_hint([Binary(b"\x00\xff")]), 42UL)
  assert_eq(batch_add(0xffffffffffffffffUL, 1UL), 0xffffffffffffffffUL)
  for
    config in [
      BatchConfig::new(max_bytes=0UL),
      BatchConfig::new(memory_budget_ratio=0.0F),
      BatchConfig::new(max_fill_ms=-1),
    ] {
    try config.validate() catch {
      InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid batch config")
    }
  }
}

///|
fn batch_row_packet(text : String) -> Bytes {
  let bytes = @utf8.encode(text)
  let body = @buffer.Buffer()
  body.write_uint16_be(1)
  body.write_int_be(bytes.length())
  body.write_bytes(bytes)
  packet(b'D', body.to_bytes())
}

///|
async test "adaptive snapshot batches resize flush deadlines and retain partially read frames" {
  for mode in ["bytes", "capacity", "deadline", "error", "cancel"] {
    @async.with_timeout(5000, () => {
      let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
      defer server.close()
      let control = ApplyControl()
      let mut capacity = 1000UL
      let sizes : Array[Int32] = []
      let values : Array[String] = []
      let read : async (@memory.Snapshot?) -> @memory.Refresh = _ => {
        snapshot: { used: 0UL, total: capacity, source: @memory.System, },
        fresh: true,
      }
      @memory.with_sampler(1, None, read, monitor => {
        let batch = BatchRuntime::new(
          monitor,
          BatchConfig::new(
            max_bytes=84UL,
            max_fill_ms=if mode == "deadline" || mode == "cancel" {
              10
            } else {
              10000
            },
          ),
        )
        @async.with_task_group(group => {
          let peer = group.spawn(() => {
            let (tcp, _) = server.accept()
            defer tcp.close()
            let (tag, body) = read_frame(tcp, 1024)
            assert_eq(tag, b'Q')
            assert_eq(
              Cursor::new(body).cstring(),
              "FETCH FORWARD 4 FROM mooncdc_snapshot",
            )
            tcp.write(batch_row_packet("1"))
            if mode == "error" {
              tcp.write(packet(b'E', b"CXX000\x00Mfixture\x00\x00"))
            } else {
              let second = batch_row_packet("2")
              if mode == "deadline" || mode == "cancel" {
                // Leave a wire header/body partially consumed when the fill
                // timer fires. Cancelling that read would corrupt the stream.
                tcp.write(second[:6])
                while sizes.is_empty() {
                  @async.sleep(1)
                }
                assert_eq(sizes, [1])
                if mode == "cancel" {
                  assert_eq(tcp.read_some(), None)
                  return
                }
                tcp.write(second[6:])
              } else {
                tcp.write(second)
              }
              tcp.write(
                batch_row_packet("3") +
                batch_row_packet("4") +
                packet(b'C', b"FETCH 4\x00") +
                packet(b'Z', b"T"),
              )
            }
            assert_eq(tcp.read_some(), None)
          })
          {
            let tcp = @socket.Tcp::connect(server.addr)
            let connection : Connection = {
              tcp,
              tls: None,
              reader: tcp,
              writer: tcp,
              limit: 1048576,
              frame_role: Workload(SnapshotReader),
              queries: Some({
                metrics: control.metrics_data,
                pool: SnapshotReader,
              }),
            }
            defer connection.close()
            let run = async fn() -> Int32 {
              batch.governor.with_slots(1UL, () => {
                connection.fetch_snapshot_rows(4, 1, 1000, batch, None, rows => {
                  sizes.push(rows.length())
                  for row in rows {
                    guard row is [Text(value)] else { fail("row shape") }
                    values.push(value)
                  }
                  assert_eq(batch.governor.registered_slots(), 1UL)
                  if mode == "capacity" && sizes.length() == 1 {
                    capacity = 100UL
                    while monitor.current().total != capacity {
                      @async.sleep(1)
                    }
                  }
                  if mode == "cancel" {
                    @async.sleep(60000)
                  }
                })
              })
            }
            if mode == "error" {
              try run() catch {
                Server("XX000", _) => ()
                error => raise error
              } noraise {
                _ => fail("expected server failure")
              }
              assert_true(values.is_empty())
            } else if mode == "cancel" {
              assert_eq(@async.with_timeout_opt(100, run), None)
            } else {
              assert_eq(run(), 4)
              assert_eq(values, ["1", "2", "3", "4"])
              if mode == "bytes" {
                assert_eq(sizes, [2, 2])
              }
              if mode == "capacity" {
                assert_eq(sizes, [2, 1, 1])
              }
            }
            assert_eq(batch.governor.registered_slots(), 0UL)
            assert_eq(
              control.metrics_data.histograms["mooncdc_postgres_query_seconds{pool=\"out_of_band\"}"].count,
              1UL,
            )
            assert_true(
              control.metrics_data.histograms["mooncdc_postgres_query_seconds{pool=\"out_of_band\"}"].sum >
              0,
            )
          }
          peer.wait()
        })
      })
    })
  }
}
