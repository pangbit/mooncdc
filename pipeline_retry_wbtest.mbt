///|
test "table retry classification is bounded and error state roundtrips" {
  let transient : Array[Error] = [
    Retryable("destination unavailable"),
    Server("55P03", "lock"),
    Server("57P01", "shutdown"),
  ]
  for error in transient {
    assert_true(
      table_retry_policy(error, 0, 2, 10) is TimedRetry(at) && at >= @env.now(),
    )
    assert_eq(table_retry_policy(error, 2, 2, 10), ManualRetry)
  }
  let manual : Array[Error] = [
    Protocol("bad frame"),
    Conversion("bad cell"),
    Manual("schema"),
    Authentication("credentials"),
    Server("08P01", "protocol"),
    PipelineProbeFailure,
  ]
  for error in manual {
    assert_eq(table_retry_policy(error, 0, 2, 10), ManualRetry)
  }
  assert_eq(table_retry_policy(Permanent("unsupported"), 0, 2, 10), NoRetry)
  let state = {
    ..pipeline_fixture(),
    tables: [{ oid: 1U, phase: Errored("failure", TimedRetry(42UL), 2), }],
  }
  assert_true(
    parse_pipeline_state(state_bytes(state)).tables[0].phase
    is Errored("failure", TimedRetry(42UL), 2),
  )
}

///|
priv struct RetryProbe {
  target : PipelineProbe
  mode : String
  mut failures : Int
}

///|
async test "persisted timed retry survives restart and failed scheduling is not published" {
  for fail_write in [false, true] {
    let dir = @fs.tmpdir(prefix="mooncdc-retry-state-")
    defer @fs.rmdir(dir, recursive=true)
    let store : PipelineStore = {
      path: dir + "/state",
      state: {
        ..pipeline_fixture(),
        tables: [
          {
            oid: 1U,
            phase: Errored("retry later", TimedRetry(@env.now() + 150UL), 1),
          },
        ],
      },
      mutex: @async.Mutex(),
    }
    store.update(state => state)
    store.state = read_pipeline_state(store.path)
    if fail_write {
      @fs.mkdir(store.path + ".tmp")
    }
    let control = ApplyControl()
    let target = ProbeDestination::new("retry", control)
    let sink : PipelineDestination = {
      destination: target,
      mutex: @async.Mutex(),
      store,
      connection: {
        host: "127.0.0.1",
        port: 1,
        user: "unused",
        password: "",
        database: "unused",
        security: LocalPlaintext,
        timeout_ms: 100,
      },
      control,
      batch_rows: 1,
      schema_store: None,
      frontier: None,
      main_lsn: Lsn::parse("0/10"),
      copy_jobs: [],
      copy_ready: @async.CondVar::Cond(),
      retry_delay_ms: 10,
      max_table_retries: 2,
      retry_attempts: Map([]),
    }
    let succeeded = try {
      @async.with_timeout(2000, () => {
        @async.with_task_group(group => {
          group.spawn_bg(() => sink.schedule_retries())
          @async.sleep(20)
          assert_true(sink.copy_jobs.is_empty())
          assert_true(store.state.tables[0].phase is Errored(_, _, _))
          while sink.copy_jobs.is_empty() {
            @async.sleep(10)
          }
          group.return_immediately(())
        })
      })
      true
    } catch {
      @async.TimeoutError => fail("retry scheduler did not make progress")
      error => if fail_write { false } else { raise error }
    }
    assert_eq(succeeded, !fail_write)
    if fail_write {
      assert_true(
        read_pipeline_state(store.path).tables[0].phase is Errored(_, _, 1),
      )
      assert_true(sink.copy_jobs.is_empty())
      assert_true(sink.retry_attempts.is_empty())
    } else {
      assert_true(read_pipeline_state(store.path).tables[0].phase is Pending)
      assert_eq(sink.copy_jobs, [1U])
      assert_eq(sink.retry_attempts[1U], 2)
    }
  }
}

///|
impl Destination for RetryProbe with fn reset_table(self, relation, attempt~) {
  Destination::reset_table(self.target, relation, attempt~)
}

///|
impl Destination for RetryProbe with fn write_rows(self, relation, rows, id) {
  if relation.name == "b" && !rows.is_empty() && self.failures > 0 {
    self.failures -= 1
    // The next fresh attempt must erase this durable partial write.
    ignore(Destination::write_rows(self.target, relation, rows, id))
    match self.mode {
      "manual" => raise Manual("injected schema failure")
      "permanent" => raise Permanent("unsupported table")
      _ => raise Retryable("injected destination connection failure")
    }
  }
  Destination::write_rows(self.target, relation, rows, id)
}

///|
impl Destination for RetryProbe with fn write_transaction(self, tx) {
  Destination::write_transaction(self.target, tx)
}

///|
impl Destination for RetryProbe with fn flush(self) {
  Destination::flush(self.target)
}

///|
async test "live table errors isolate healthy apply and support bounded timed and manual retry" {
  if live_config() is Some(config) {
    for mode in ["auto", "exhausted", "manual", "permanent"] {
      @async.with_timeout(30000, () => {
        let sql = connect(config, 16777216, false)
        defer sql.close()
        ignore(
          sql.query(
            "CREATE SCHEMA pipeline_retry; CREATE TABLE pipeline_retry.a(id int PRIMARY KEY,value text); CREATE TABLE pipeline_retry.b(id int PRIMARY KEY,value text); INSERT INTO pipeline_retry.a VALUES(1,'before'); INSERT INTO pipeline_retry.b VALUES(1,'before'),(2,'before'); CREATE PUBLICATION pipeline_retry FOR TABLE pipeline_retry.a,pipeline_retry.b",
          ),
        )
        let dir = @fs.tmpdir(prefix="mooncdc-retry-")
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
          schema="pipeline_retry",
        )
        let probe : RetryProbe = {
          target,
          mode,
          failures: if mode == "auto" {
            2
          } else {
            3
          },
        }
        let mut saw_error = false
        let mut advanced_healthy = false
        let mut retried = false
        @async.with_task_group(group => {
          group.spawn_bg(() => {
            for ;; {
              @async.sleep(5)
              if !@fs.exists(path) {
                continue
              }
              let state = read_pipeline_state(path)
              if state.tables.length() != 2 {
                continue
              }
              let b = state.tables[1]
              if !saw_error && b.phase is Errored(_, _, _) {
                saw_error = true
                ignore(
                  sql.query(
                    "BEGIN; UPDATE pipeline_retry.a SET value='after'; DELETE FROM pipeline_retry.b WHERE id=1; INSERT INTO pipeline_retry.b VALUES(3,'after'); COMMIT",
                  ),
                )
              }
              if saw_error &&
                target.data.rows.get("a") is Some(rows) &&
                rows.get("1") == Some(Some("after")) &&
                load_checkpoint(path + ".checkpoint", state.source) >
                Lsn::parse(state.initial_lsn.unwrap()) {
                advanced_healthy = true
              }
              if mode != "auto" &&
                !retried &&
                advanced_healthy &&
                b.phase is Errored(_, policy, attempts) {
                if policy is TimedRetry(_) {
                  continue
                }
                assert_eq(attempts, if mode == "exhausted" { 2 } else { 0 })
                let resets = target.data.resets["b"]
                @async.sleep(50)
                assert_eq(target.data.resets["b"], resets)
                assert_false(control.retry_table(999999U))
                if mode == "permanent" {
                  assert_eq(policy, NoRetry)
                  assert_false(control.retry_table(b.oid))
                  control.stop()
                  break
                }
                assert_eq(policy, ManualRetry)
                probe.failures = 0
                assert_true(control.retry_table(b.oid))
                assert_false(control.retry_table(b.oid))
                retried = true
              }
              if advanced_healthy &&
                state.tables.all(table => table.phase is (Ready(_) | Catchup(_))) {
                control.stop()
                break
              }
            }
          })
          run_pipeline(
            config,
            probe,
            publication="pipeline_retry",
            destination_id="retry",
            state_path=path,
            control~,
            copy_concurrency=1,
            batch_rows=1,
            max_table_retries=2,
            table_retry_delay_ms=100,
          )
          group.return_immediately(())
        })
        assert_true(saw_error)
        assert_true(advanced_healthy)
        assert_eq(target.data.resets["a"], 1)
        assert_probe_table(target, "a")
        if mode != "permanent" {
          assert_probe_table(target, "b")
          assert_eq(
            target.data.resets["b"],
            match mode {
              "auto" => 3
              "exhausted" => 4
              _ => 2
            },
          )
        }
        assert_true(control.table_retry is None)
        drop_slot(sql, pipeline_status(path).slot)
        ignore(
          sql.query(
            "DROP PUBLICATION pipeline_retry; DROP SCHEMA pipeline_retry CASCADE",
          ),
        )
        println(
          "LIVE: isolated table error/retry mode=\{mode}, healthy checkpoint progress and reset convergence passed",
        )
      })
    }
  }
}
