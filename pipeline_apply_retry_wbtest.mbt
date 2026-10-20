///|
async test "apply retry budget preserves errors and stop cancels the delay" {
  for
    mode in [
      "write", "flush", "manual", "permanent", "state", "exhausted", "stop",
    ] {
    let control = ApplyControl()
    let mut attempts = 0
    let run = async fn() -> Unit {
      attempts += 1
      @async.sleep(1)
      if mode == "state" {
        // Even a transient-shaped persistence error has no destination marker.
        raise Retryable("state write")
      }
      if mode == "manual" {
        raise ApplyWriteFailed(Manual("manual"))
      }
      if mode == "permanent" {
        raise ApplyWriteFailed(Permanent("permanent"))
      }
      if mode == "exhausted" || mode == "stop" || attempts < 3 {
        if mode == "flush" {
          raise PipelineFlushFailed(Retryable("flush"))
        }
        raise ApplyWriteFailed(Retryable("write"))
      }
    }
    let succeeded = try {
      @async.with_timeout(2000, () => {
        @async.with_task_group(group => {
          if mode == "stop" {
            group.spawn_bg(() => {
              while attempts == 0 {
                @async.sleep(1)
              }
              @async.sleep(10)
              control.stop()
            })
          }
          let retry = ApplyRetry::new(
            control,
            if mode == "stop" {
              60000
            } else {
              1
            },
            2,
          )
          ignore(retry.run(run))
          group.return_immediately(())
        })
      })
      true
    } catch {
      Retryable(message) => {
        assert_eq(
          message,
          if mode == "state" {
            "state write"
          } else {
            "write"
          },
        )
        assert_true(mode == "state" || mode == "exhausted")
        false
      }
      Manual(_) => {
        assert_eq(mode, "manual")
        false
      }
      Permanent(_) => {
        assert_eq(mode, "permanent")
        false
      }
      error => raise error
    }
    assert_eq(succeeded, mode == "write" || mode == "flush" || mode == "stop")
    assert_eq(
      attempts,
      if mode == "write" || mode == "flush" || mode == "exhausted" {
        3
      } else {
        1
      },
    )
  }
}

///|
priv struct ApplyRetryProbe {
  target : PipelineProbe
  config : ConnectionConfig
  path : String
  mode : String
  mut failures : Int
  ids : Array[String]
  event_counts : Array[Int]
  mut before : Lsn?
  mut end : Lsn?
  mut startups : Int
  mut shutdowns : Int
  mut pending : Bool
}

///|
impl Destination for ApplyRetryProbe with fn startup(self) {
  self.startups += 1
}

///|
impl Destination for ApplyRetryProbe with fn shutdown(self) {
  self.shutdowns += 1
}

///|
impl Destination for ApplyRetryProbe with fn reset_table(
  self,
  relation,
  attempt~,
) {
  Destination::reset_table(self.target, relation, attempt~)
}

///|
impl Destination for ApplyRetryProbe with fn write_rows(
  self,
  relation,
  rows,
  id,
) {
  let result = Destination::write_rows(self.target, relation, rows, id)
  if self.mode == "copy_alive" && relation.name == "b" && !rows.is_empty() {
    @async.sleep(10)
  }
  result
}

///|
impl Destination for ApplyRetryProbe with fn write_transaction(self, tx) {
  // The projection may gain tables during retry; deduplicate individual events.
  for index, change in tx.changes {
    if tx.event_id(index) is Some(id) {
      ignore(
        Destination::write_transaction(self.target, {
          ..tx,
          id,
          changes: [change],
        }),
      )
    }
  }
  if tx.id.contains("/copy/") {
    return Durable
  }
  let business = tx.changes.any(change => {
    change is Insert(_, _) || change is Update(_, _, _)
  })
  if business {
    assert_false(tx.id.contains("/copy/"))
    let state = read_pipeline_state(self.path)
    let name = Replication.settings(worker=ApplyWorker(state.source.slot)).0
    assert_eq(
      cell(
        self.target.sql.query(
          "SELECT application_name FROM pg_stat_activity WHERE pid=(SELECT active_pid FROM pg_replication_slots WHERE slot_name='\{state.source.slot}')",
        ),
        0,
        0,
      ),
      name,
    )
    let checkpoint = load_checkpoint(self.path + ".checkpoint", state.source)
    if self.before is None {
      self.before = Some(checkpoint)
    }
    if self.ids.length() > 0 {
      assert_eq(tx.id, self.ids[0])
    }
    assert_eq(Some(checkpoint), self.before)
    self.ids.push(tx.id)
    self.event_counts.push(
      tx.changes
      .filter(change => change is Insert(_, _) || change is Update(_, _, _))
      .length(),
    )
    self.end = Some(tx.end_lsn)
    if self.mode == "source" && self.failures > 0 {
      self.failures -= 1
      let slot = state.source.slot
      let sql = connect(self.config, 16777216, false)
      defer sql.close()
      assert_eq(
        cell(
          sql.query(
            "SELECT pg_terminate_backend(active_pid)::text FROM pg_replication_slots WHERE slot_name='\{slot}'",
          ),
          0,
          0,
        ),
        "true",
      )
      @async.sleep(10000)
      fail("source failure did not interrupt the old apply attempt")
    }
    // Effects are durable above; simulate a lost reply before acknowledgement.
    if self.mode == "flush" {
      self.pending = true
      return Accepted
    }
    if self.failures > 0 {
      self.failures -= 1
      if self.mode == "manual" {
        raise Manual("injected apply schema failure")
      }
      raise Retryable("injected lost apply reply")
    }
  }
  Durable
}

///|
impl Destination for ApplyRetryProbe with fn flush(self) {
  if self.pending {
    if self.failures > 0 {
      self.failures -= 1
      raise Retryable("injected apply flush failure")
    }
    self.pending = false
  }
  Destination::flush(self.target)
}

///|
async test "live apply retries replay durable IDs without recopying completed tables" {
  if live_config() is Some(config) {
    for
      mode in ["write", "flush", "manual", "exhausted", "copy_alive", "source"] {
      @async.with_timeout(30000, () => {
        let sql = connect(config, 16777216, false)
        defer sql.close()
        ignore(
          sql.query(
            "CREATE SCHEMA apply_retry; CREATE TABLE apply_retry.a(id int PRIMARY KEY,value text); CREATE TABLE apply_retry.b(id int PRIMARY KEY,value text); INSERT INTO apply_retry.a VALUES(1,'before'); INSERT INTO apply_retry.b VALUES(1,'before'); CREATE PUBLICATION apply_retry FOR TABLE apply_retry.a,apply_retry.b",
          ),
        )
        if mode == "copy_alive" {
          ignore(
            sql.query(
              "INSERT INTO apply_retry.b SELECT i,'before' FROM generate_series(3,66) i",
            ),
          )
        }
        let dir = @fs.tmpdir(prefix="mooncdc-apply-retry-")
        defer @fs.rmdir(dir, recursive=true)
        let path = dir + "/state"
        let control = ApplyControl()
        let target = PipelineProbe::new(
          dir + "/target",
          sql,
          control,
          "unused",
          false,
          false,
          None,
          schema="apply_retry",
        )
        let probe : ApplyRetryProbe = {
          target,
          config,
          path,
          mode,
          failures: if mode == "exhausted" {
            10
          } else {
            1
          },
          ids: [],
          event_counts: [],
          before: None,
          end: None,
          startups: 0,
          shutdowns: 0,
          pending: false,
        }
        let mut copied_during_retry = false
        let succeeded = try {
          @async.with_task_group(group => {
            group.spawn_bg(() => {
              while !@fs.exists(path) ||
                    read_pipeline_state(path).tables.length() != 2 ||
                    !(if mode == "copy_alive" {
                      let tables = read_pipeline_state(path).tables
                      tables.any(table => table.phase is (Ready(_) | Catchup(_))) &&
                      tables.any(table => table.phase is Copying) &&
                      target.data.rows.get("b") is Some(rows) &&
                      !rows.is_empty()
                    } else {
                      read_pipeline_state(path).tables.all(table => {
                        table.phase is (Ready(_) | Catchup(_))
                      })
                    }) {
                @async.sleep(10)
              }
              ignore(
                sql.query(
                  "BEGIN; UPDATE apply_retry.a SET value='after' WHERE id=1; INSERT INTO apply_retry.b VALUES(2,'after'); COMMIT",
                ),
              )
              for ;; {
                @async.sleep(10)
                if probe.end is Some(end) {
                  let state = read_pipeline_state(path)
                  if mode == "copy_alive" &&
                    probe.ids.length() == 1 &&
                    state.tables.all(table => {
                      table.phase is (Ready(_) | Catchup(_))
                    }) {
                    copied_during_retry = true
                  }
                  if load_checkpoint(path + ".checkpoint", state.source) >= end {
                    control.stop()
                    break
                  }
                }
              }
            })
            run_pipeline(
              config,
              probe,
              publication="apply_retry",
              destination_id="apply-retry",
              state_path=path,
              control~,
              table_retry_delay_ms=if mode == "copy_alive" { 2000 } else { 20 },
              max_table_retries=2,
              copy_concurrency=1,
              batch_rows=1,
            )
            group.return_immediately(())
          })
          true
        } catch {
          Manual(_) => {
            assert_eq(mode, "manual")
            false
          }
          Retryable(_) => {
            assert_eq(mode, "exhausted")
            false
          }
          error => raise error
        }
        assert_eq(
          succeeded,
          mode == "write" ||
          mode == "flush" ||
          mode == "copy_alive" ||
          mode == "source",
        )
        if mode == "copy_alive" {
          assert_true(copied_during_retry)
          assert_eq(probe.event_counts, [1, 2])
        }
        assert_eq(
          probe.ids.length(),
          if mode == "manual" {
            1
          } else if mode == "exhausted" {
            3
          } else {
            2
          },
        )
        assert_eq(probe.startups, 1)
        assert_eq(probe.shutdowns, if succeeded { 1 } else { 0 })
        assert_eq(target.data.resets.get("a"), Some(1))
        assert_eq(target.data.resets.get("b"), Some(1))
        assert_probe_table(target, "a")
        assert_probe_table(target, "b")
        let state = read_pipeline_state(path)
        let checkpoint = load_checkpoint(path + ".checkpoint", state.source)
        if succeeded {
          assert_true(checkpoint >= probe.end.unwrap())
        } else {
          assert_eq(Some(checkpoint), probe.before)
        }
        drop_slot(sql, state.source.slot)
        ignore(
          sql.query(
            "DROP PUBLICATION apply_retry; DROP SCHEMA apply_retry CASCADE",
          ),
        )
        println(
          "LIVE: apply retry mode=\{mode}, stable IDs, conservative checkpoint and no recopy passed",
        )
      })
    }
  }
}
