///|
test "CTID plans cover contiguous blocks with open ends and bounded work" {
  assert_true(ctid_ranges(0UL, 4).is_empty())
  assert_true(ctid_ranges(10UL, 1) is [OpenEnd(0UL)])
  assert_true(
    ctid_ranges(10UL, 3) is [OpenStart(3UL), Bounded(3UL, 6UL), OpenEnd(6UL)],
  )
  assert_true(ctid_ranges(2UL, 1024) is [OpenStart(1UL), OpenEnd(1UL)])
  let large = ctid_ranges(0xffffffffUL, 1024)
  assert_eq(large.length(), 1024)
  assert_eq(large[0].predicate(), "ctid < '(4194303,1)'::tid")
  assert_eq(large[1023].predicate(), "ctid >= '(4290772991,1)'::tid")
  for (blocks, partitions) in [(1UL, 0), (1UL, 1025), (0x100000000UL, 1)] {
    try ctid_ranges(blocks, partitions) catch {
      InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("invalid CTID plan accepted")
    }
  }
  let jobs = snapshot_copy_jobs(
    [
      { qualified: "a", blocks: 12UL, rows: 0UL, },
      { qualified: "b", blocks: 4UL, rows: 100UL, },
    ],
    2,
  )
  assert_eq(jobs.filter(job => job.qualified == "a").length(), 6)
  assert_eq(jobs.filter(job => job.qualified == "b").length(), 2)
  let rows = snapshot_copy_jobs(
    [
      { qualified: "a", blocks: 100UL, rows: 1500000UL, },
      { qualified: "b", blocks: 100UL, rows: 500000UL, },
    ],
    2,
  )
  assert_eq(rows.filter(job => job.qualified == "a").length(), 6)
  assert_eq(rows.filter(job => job.qualified == "b").length(), 2)
  assert_eq(
    snapshot_copy_jobs(
      [{ qualified: "a", blocks: 2000UL, rows: 0xffffffffffffffffUL, }],
      256,
    ).length(),
    1024,
  )
}

///|
async test "live copy partition metrics count completed readers independently of durable TableEnd and preserve empty skipped failure boundaries" {
  if live_config() is Some(config) {
    @async.with_timeout(30000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE SCHEMA copy_progress; CREATE TABLE copy_progress.items(id int PRIMARY KEY,payload text); CREATE PUBLICATION copy_progress FOR TABLE copy_progress.items(id)",
        ),
      )
      defer @async.protect_from_cancel(() => {
        drop_slot(sql, "copy_progress")
        ignore(
          sql.query(
            "DROP PUBLICATION copy_progress; DROP SCHEMA copy_progress CASCADE",
          ),
        )
      })
      for
        (connections, copy, empty) in [
          (1, true, false),
          (3, true, false),
          (3, true, true),
          (3, false, false),
        ] {
        ignore(sql.query("TRUNCATE copy_progress.items"))
        if !empty {
          ignore(
            sql.query(
              "INSERT INTO copy_progress.items SELECT i,repeat('x',512) FROM generate_series(1,64) i",
            ),
          )
        }
        let control = ApplyControl()
        let observed : Map[String, Bool] = Map([])
        ignore(
          copy_snapshot_impl(
            config,
            slot="copy_progress",
            publication="copy_progress",
            copy_connections=connections,
            batch_rows=8,
            max_frame_bytes=1048576,
            max_batch_bytes=1048576,
            temporary=false,
            table=None,
            activity_control=control,
            copy_rows=copy,
            event => {
              if event is TableRows(_, rows) {
                for row in rows {
                  guard row is [Text(id)] else {
                    fail("unexpected copy progress row")
                  }
                  assert_false(observed.contains(id))
                  observed[id] = true
                }
              }
            },
          ),
        )
        assert_eq(observed.length(), if copy && !empty { 64 } else { 0 })
        assert_eq(control.activities().length(), 0)
        let body = control.prometheus()
        assert_true(query_metric_count(control, Queries) > 0UL)
        assert_false(body.contains("copy_progress"))
        if !copy {
          assert_false(body.contains("mooncdc_copy_planned_partitions"))
          assert_false(body.contains("mooncdc_copy_partitions_completed_total"))
        } else {
          let labels = "{worker_type=\"table_sync\"}"
          let effective = control.metrics_data.histograms["mooncdc_copy_effective_partitions" +
          labels].sum.to_int()
          let planned = control.metrics_data.histograms["mooncdc_copy_planned_partitions" +
          labels].sum.to_int()
          if connections == 1 {
            assert_eq((planned, effective), (1, 1))
          } else if empty {
            assert_eq((planned, effective), (0, 0))
          } else {
            assert_eq(planned, 12)
            assert_true(effective > 1 && effective <= 12)
          }
          assert_eq(
            control.metrics_data.counters
            .get("mooncdc_copy_partitions_completed_total" + labels)
            .unwrap_or(0UL),
            effective.to_uint64(),
          )
          if effective > 0 {
            assert_eq(
              control.metrics_data.histograms["mooncdc_copy_partition_rows" +
              labels].sum,
              if empty {
                0.0
              } else {
                64.0
              },
            )
            assert_eq(
              control.metrics_data.histograms["mooncdc_copy_partition_seconds" +
              labels].count,
              effective.to_uint64(),
            )
          } else {
            assert_false(body.contains("mooncdc_copy_partition_rows"))
          }
        }
        println(
          "COPY METRICS connections=" +
          connections.to_string() +
          " copy=" +
          copy.to_string() +
          " empty=" +
          empty.to_string() +
          "\n" +
          body,
        )
        drop_slot(sql, "copy_progress")
      }
      for barrier_failure in [false, true] {
        let control = ApplyControl()
        if barrier_failure {
          let target = ProbeDestination::new("fail", control)
          try
            copy_snapshot_to_impl(
              config,
              target,
              slot="copy_progress",
              publication="copy_progress",
              copy_connections=3,
              batch_rows=8,
              max_frame_bytes=1048576,
              max_batch_bytes=1048576,
              temporary=false,
              table=None,
              activity_control=control,
            )
          catch {
            TestDestinationFailure => ()
            error => raise error
          } noraise {
            _ => fail("failed barrier returned snapshot")
          }
          assert_true(
            control
            .prometheus()
            .contains("mooncdc_copy_partitions_completed_total"),
          )
          assert_false(
            control
            .prometheus()
            .contains("mooncdc_table_copies_completed_total"),
          )
          assert_true(target.durable.is_empty())
        } else {
          try
            copy_snapshot_impl(
              config,
              slot="copy_progress",
              publication="copy_progress",
              copy_connections=3,
              batch_rows=8,
              max_frame_bytes=1048576,
              max_batch_bytes=1048576,
              temporary=false,
              table=None,
              activity_control=control,
              event => {
                if event is TableRows(_, _) {
                  raise TestDestinationFailure
                }
              },
            )
          catch {
            TestDestinationFailure => ()
            error => raise error
          } noraise {
            _ => fail("failed reader returned snapshot")
          }
          assert_false(
            control
            .prometheus()
            .contains("mooncdc_copy_partitions_completed_total"),
          )
          assert_false(
            control.prometheus().contains("mooncdc_copy_partition_rows"),
          )
        }
        assert_eq(control.activities().length(), 0)
        println(
          "COPY METRICS barrier_failure=" +
          barrier_failure.to_string() +
          "\n" +
          control.prometheus(),
        )
        drop_slot(sql, "copy_progress")
      }
      println(
        "LIVE: serial/parallel/empty/skipped copy partition metrics and reader versus TableEnd failure boundaries passed",
      )
    })
  }
}

///|
async test "live parallel snapshot shares one view serializes callbacks and hands off WAL" {
  if live_config() is Some(config) {
    @async.with_timeout(30000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE SCHEMA parallel_snapshot; CREATE TABLE parallel_snapshot.items(id int PRIMARY KEY,value text,hidden text); INSERT INTO parallel_snapshot.items SELECT i,repeat('x',512),'secret' FROM generate_series(-100,1000) i; CREATE PUBLICATION parallel_snapshot FOR TABLE parallel_snapshot.items(id,value) WHERE (id>0 OR id=1001)",
        ),
      )
      defer @async.protect_from_cancel(() => {
        drop_slot(sql, "parallel_snapshot")
        ignore(
          sql.query(
            "DROP PUBLICATION parallel_snapshot; DROP SCHEMA parallel_snapshot CASCADE",
          ),
        )
      })
      let rows : Map[String, Value] = Map([])
      let mut busy = false
      let mut observed = false
      let mut begins = 0
      let mut ends = 0
      let point = copy_snapshot(
        config,
        slot="parallel_snapshot",
        publication="parallel_snapshot",
        copy_connections=3,
        batch_rows=8,
        event => {
          assert_false(busy)
          busy = true
          defer {
            busy = false
          }
          match event {
            TableBegin(relation) => {
              assert_eq(relation.columns.length(), 2)
              begins += 1
              ignore(
                sql.query(
                  "BEGIN; UPDATE parallel_snapshot.items SET value='updated' WHERE id=1; DELETE FROM parallel_snapshot.items WHERE id=2; INSERT INTO parallel_snapshot.items VALUES(1001,'new','secret'); COMMIT",
                ),
              )
            }
            TableRows(_, batch) => {
              if !observed {
                while cell(
                        sql.query(
                          "SELECT count(*)::text FROM pg_stat_activity WHERE application_name='mooncdc_snapshot' AND datname=current_database()",
                        ),
                        0,
                        0,
                      ) !=
                      "4" {
                  @async.sleep(1)
                }
                observed = true
              }
              @async.sleep(1)
              for row in batch {
                guard row is [Text(id), value] else {
                  fail("parallel tuple shape")
                }
                assert_false(rows.contains(id))
                assert_eq(value, Text("x".repeat(512)))
                rows[id] = value
              }
            }
            TableEnd(_) => ends += 1
          }
        },
      )
      assert_true(observed)
      assert_eq((begins, ends, rows.length()), (1, 1, 1000))
      assert_true(
        rows.contains("1") && rows.contains("2") && !rows.contains("1001"),
      )
      let dir = @fs.tmpdir(prefix="mooncdc-parallel-wal-")
      defer @fs.rmdir(dir, recursive=true)
      subscribe(
        config,
        slot="parallel_snapshot",
        publication="parallel_snapshot",
        start=AfterSnapshot(point),
        checkpoint_path=dir + "/checkpoint",
        sub => {
          let tx = sub.next()
          let mut changes = 0
          for change in tx.changes {
            match change {
              Insert(_, [Text(id), value]) | Update(_, _, [Text(id), value]) => {
                rows[id] = value
                changes += 1
              }
              Delete(_, Key([Text(id), ..])) => {
                rows.remove(id)
                changes += 1
              }
              RelationChanged(_) => ()
              _ => fail("parallel handoff change")
            }
          }
          assert_eq(changes, 3)
          assert_eq(rows.length(), 1000)
          assert_eq(rows.get("1"), Some(Text("updated")))
          assert_false(rows.contains("2"))
          assert_eq(rows.get("1001"), Some(Text("new")))
          sub.ack(tx)
        },
      )
      println(
        "LIVE: three CTID readers share exported snapshot; serial callbacks, projection/filter and WAL I/U/D handoff passed",
      )
    })
  }
}

///|
async test "live parallel copy batch IDs durability failure cancellation and budget cleanup" {
  if live_config() is Some(config) {
    @async.with_timeout(30000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE SCHEMA parallel_target; CREATE TABLE parallel_target.items(id int PRIMARY KEY,padding text); INSERT INTO parallel_target.items SELECT i,repeat('x',512) FROM generate_series(1,1000) i; CREATE PUBLICATION parallel_target FOR TABLE parallel_target.items(id)",
        ),
      )
      defer @async.protect_from_cancel(() => {
        drop_slot(sql, "parallel_target")
        ignore(
          sql.query(
            "DROP PUBLICATION parallel_target; DROP SCHEMA parallel_target CASCADE",
          ),
        )
      })
      let probe = ProbeDestination::new("accepted", ApplyControl())
      ignore(
        copy_snapshot_to(
          config,
          probe,
          slot="parallel_target",
          publication="parallel_target",
          copy_connections=3,
          batch_rows=7,
        ),
      )
      assert_eq(probe.flushes, 1)
      assert_true(probe.pending.is_empty())
      assert_eq(probe.durable.length(), 1000)
      let unique : Map[String, Bool] = Map([])
      for id in probe.durable {
        assert_false(unique.contains(id))
        unique[id] = true
      }
      for i, id in probe.batches {
        assert_eq(id.sequence, (i + 1).to_uint64())
        assert_eq(id.attempt, probe.batches[0].attempt)
      }
      drop_slot(sql, "parallel_target")
      let failed = ProbeDestination::new("fail", ApplyControl())
      try
        copy_snapshot_to(
          config,
          failed,
          slot="parallel_target",
          publication="parallel_target",
          copy_connections=3,
        )
      catch {
        TestDestinationFailure => ()
        error => raise error
      } noraise {
        _ => fail("failed durability barrier returned handoff")
      }
      assert_true(failed.durable.is_empty())
      drop_slot(sql, "parallel_target")
      @memory.with_sampler(
        2,
        None,
        _ => {
          snapshot: { total: 1000000UL, used: 0UL, source: @memory.System, },
          fresh: true,
        },
        monitor => {
          let batch = BatchRuntime::new(
            monitor,
            BatchConfig::new(max_bytes=1UL, max_fill_ms=0),
          )
          for cancelled in [false, true] {
            let control = ApplyControl()
            let mut entered = false
            try
              @async.with_timeout(1500, () => {
                batch.governor.with_slots(1UL, () => {
                  ignore(
                    copy_snapshot_impl(
                      config,
                      slot="parallel_target",
                      publication="parallel_target",
                      copy_connections=3,
                      batch_rows=1,
                      max_frame_bytes=1048576,
                      max_batch_bytes=1048576,
                      temporary=false,
                      table=None,
                      activity_control=control,
                      batch~,
                      event => {
                        if event is TableRows(_, _) {
                          entered = true
                          assert_eq(batch.governor.registered_slots(), 3UL)
                          if cancelled {
                            @async.sleep(10000)
                          } else {
                            raise TestDestinationFailure
                          }
                        }
                      },
                    ),
                  )
                })
              })
            catch {
              TestDestinationFailure => assert_false(cancelled)
              @async.TimeoutError => assert_true(cancelled)
              error => raise error
            } noraise {
              _ => fail("failed parallel worker returned handoff")
            }
            assert_true(entered)
            assert_eq(batch.governor.registered_slots(), 0UL)
            assert_eq(control.activities().length(), 0)
            drop_slot(sql, "parallel_target")
            // Local handles are closed before the server removes its backends.
            // Keep the local cleanup checks immediate and bound this observation.
            @async.with_timeout(2000, () => {
              while cell(
                      sql.query(
                        "SELECT count(*)::text FROM pg_stat_activity WHERE application_name='mooncdc_snapshot' AND datname=current_database()",
                      ),
                      0,
                      0,
                    ) !=
                    "0" {
                @async.sleep(20)
              }
            })
          }
        },
      )
      println(
        "LIVE: unique parallel copy batch IDs, cumulative flush, failed barrier, worker failure/cancellation and all reader/activity/budget cleanup passed",
      )
    })
  }
}

///|
async test "live parallel snapshot drains pressure across all copy readers" {
  if live_config() is Some(config) {
    @async.with_timeout(15000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE SCHEMA parallel_pressure; CREATE TABLE parallel_pressure.items(id int PRIMARY KEY,padding text); INSERT INTO parallel_pressure.items SELECT i,repeat('x',2048) FROM generate_series(1,64) i; CREATE PUBLICATION parallel_pressure FOR TABLE parallel_pressure.items(id)",
        ),
      )
      defer @async.protect_from_cancel(() => {
        drop_slot(sql, "parallel_pressure")
        ignore(
          sql.query(
            "DROP PUBLICATION parallel_pressure; DROP SCHEMA parallel_pressure CASCADE",
          ),
        )
      })
      let pressure = MemoryPressure::{ used: 0UL, }
      @memory.with_sampler(
        2,
        Some((0.85F, 0.75F)),
        previous => pressure.sample(previous),
        monitor => {
          let destination : MemoryCopyDestination = {
            pressure,
            monitor,
            initial_pressure: false,
            pending: [],
            durable: [],
            writes: 0,
            flushes: 0,
          }
          let batch = BatchRuntime::new(
            monitor,
            BatchConfig::new(max_bytes=1UL, max_fill_ms=0),
          )
          let control = ApplyControl()
          batch.governor.with_slots(1UL, () => {
            ignore(
              copy_snapshot_to_impl(
                config,
                destination,
                slot="parallel_pressure",
                publication="parallel_pressure",
                copy_connections=3,
                batch_rows=1,
                max_frame_bytes=1048576,
                max_batch_bytes=1048576,
                temporary=false,
                table=None,
                activity_control=control,
                memory=monitor,
                batch~,
              ),
            )
          })
          assert_eq(destination.writes, 64)
          assert_eq(destination.flushes, 65)
          assert_true(destination.pending.is_empty())
          let unique : Map[String, Bool] = Map([])
          for id in destination.durable {
            assert_false(unique.contains(id))
            unique[id] = true
          }
          assert_eq(unique.length(), 64)
          assert_eq(batch.governor.registered_slots(), 0UL)
          assert_eq(control.activities().length(), 0)
        },
      )
      println(
        "LIVE: injected pressure drains Accepted batches and resumes all three CTID readers without write-lock deadlock",
      )
    })
  }
}
