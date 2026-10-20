///|
fn query_metric_count(control : ApplyControl, pool : SessionWorkload) -> UInt64 {
  let label = if pool is StateStore { "postgres_store" } else { "out_of_band" }
  control.metrics_data.histograms
  .get("mooncdc_postgres_query_seconds{pool=\"" + label + "\"}")
  .map_or(0UL, h => h.count)
}

///|
async test "query observation records one bounded sample for success batch failure limits timeout cancellation and no handshake sample" {
  for pool in [Queries, StateStore] {
    for
      mode in [
        "success", "error", "limit", "protocol", "timeout", "cancel", "disabled",
        "startup",
      ] {
      @async.with_timeout(3000, () => {
        let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
        defer server.close()
        let control = ApplyControl()
        let config = ConnectionConfig::new(
          host="127.0.0.1",
          user="query-fixture",
          password="secret-password",
          database="secret-database",
          port=server.addr.port(),
          security=LocalPlaintext,
        )
        let mut received = false
        @async.with_task_group(group => {
          let peer = group.spawn(() => {
            let (tcp, _) = server.accept()
            defer tcp.close()
            let size = Cursor::new(tcp.read_exactly(4)).i32()
            ignore(tcp.read_exactly(size - 4))
            if mode == "startup" {
              return
            }
            tcp.write(packet(b'R', b"\x00\x00\x00\x00") + packet(b'Z', b"I"))
            let (tag, body) = read_frame(tcp, 1048576)
            assert_eq(tag, b'Q')
            assert_eq(
              Cursor::new(body).cstring(),
              "SELECT 'secret-query'; SELECT 2",
            )
            received = true
            match mode {
              "success" | "disabled" => mock_row(tcp, [Some("ok")])
              "error" =>
                tcp.write(packet(b'E', b"C22012\x00Msecret-error\x00\x00"))
              // This oversized DataRow ends the request locally; do not leave a
              // synthetic ReadyForQuery unread and turn socket closure into RST.
              "limit" =>
                tcp.write(batch_row_packet("012345678901234567890123456789"))
              "protocol" => tcp.write(packet(b'X', b""))
              _ => ()
            }
            assert_eq(tcp.read_some(), None)
          })
          let metrics = if mode == "disabled" {
            None
          } else {
            Some(control.metrics_data)
          }
          if mode == "startup" {
            try connect(config, 1048576, false, workload=pool, metrics?) catch {
              _ => ()
            } noraise {
              conn => {
                conn.close()
                fail("failed startup connected")
              }
            }
          } else {
            let conn = connect(config, 1048576, false, workload=pool, metrics?)
            defer conn.close()
            assert_eq(query_metric_count(control, pool), 0UL)
            let run = async fn() -> Array[Array[String?]] {
              conn.query(
                "SELECT 'secret-query'; SELECT 2",
                max_result_bytes=if mode == "limit" { 8 } else { 1048576 },
              )
            }
            match mode {
              "success" | "disabled" => assert_eq(run(), [[Some("ok")]])
              "error" =>
                try run() catch {
                  Server("22012", _) => ()
                  e => raise e
                } noraise {
                  _ => fail("server failure ignored")
                }
              "limit" =>
                try run() catch {
                  LimitExceeded(_) => ()
                  e => raise e
                } noraise {
                  _ => fail("query limit ignored")
                }
              "protocol" =>
                try run() catch {
                  Protocol(_) => ()
                  e => raise e
                } noraise {
                  _ => fail("malformed frame accepted")
                }
              "timeout" =>
                try @async.with_timeout(30, run) catch {
                  @async.TimeoutError => ()
                  e => raise e
                } noraise {
                  _ => fail("query timeout ignored")
                }
              "cancel" => {
                let task = group.spawn(run)
                while !received {
                  @async.sleep(1)
                }
                task.cancel()
                try task.wait() catch {
                  @async.WaitedTaskAlreadyCancelled => ()
                  e => raise e
                } noraise {
                  _ => fail("cancelled query succeeded")
                }
              }
              _ => fail("unexpected query mode")
            }
            assert_eq(
              query_metric_count(control, pool),
              if mode == "disabled" {
                0UL
              } else {
                1UL
              },
            )
          }
          peer.wait()
        })
        let body = control.prometheus()
        for
          secret in [
            "secret-password", "secret-database", "secret-query", "secret-error",
            "query-fixture",
          ] {
          assert_false(body.contains(secret))
        }
        if mode == "startup" || mode == "disabled" {
          assert_false(body.contains("mooncdc_postgres_query_seconds"))
        }
        assert_false(
          ApplyControl().prometheus().contains("mooncdc_postgres_query_seconds"),
        )
      })
    }
  }
}

///|
async test "live PostgreSQL query metrics count explicit batches source monitors and server failures without granting progress" {
  if live_config() is Some(config) {
    @async.with_timeout(15000, () => {
      let control = ApplyControl()
      let conn = connect(config, 1048576, false, metrics=control.metrics_data)
      defer conn.close()
      assert_eq(query_metric_count(control, Queries), 0UL)
      assert_eq(conn.query("SELECT 'first'; SELECT 'second'"), [
        [Some("first")],
        [Some("second")],
      ])
      assert_eq(query_metric_count(control, Queries), 1UL)
      ignore(conn.query("SELECT pg_sleep(0.03)"))
      assert_eq(query_metric_count(control, Queries), 2UL)
      assert_true(
        control.metrics_data.histograms["mooncdc_postgres_query_seconds{pool=\"out_of_band\"}"].sum >=
        0.02,
      )
      assert_true(
        sample_source_lsn(config, metrics=control.metrics_data) is Some(_),
      )
      assert_eq(query_metric_count(control, Queries), 3UL)
      try conn.query("SELECT 1/0") catch {
        Server("22012", _) => ()
        e => raise e
      } noraise {
        _ => fail("actual server failure ignored")
      }
      assert_eq(query_metric_count(control, Queries), 4UL)
      let body = control.prometheus()
      assert_false(body.contains("mooncdc_checkpoint_updates_total"))
      assert_false(body.contains("mooncdc_transactions_durable_total"))
      assert_false(body.contains("postgres_store"))
      assert_false(body.contains("pg_sleep"))
      assert_true(control.activities().is_empty())
      println("QUERY METRICS\n" + body)
      println(
        "LIVE: actual PostgreSQL multi-statement batch one sample, monotonic timing, independent source query and server error preserve progress boundaries",
      )
    })
  }
}
