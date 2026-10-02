///|
priv struct HandoverProbe {
  target : PipelineProbe
  state_path : String
  mut copied_b : Int
  mut main_end : Lsn?
  mut caught_up : Bool
  mut flushed_copy : Bool
  fail_flush : Bool
}

///|
impl Destination for HandoverProbe with fn reset_table(self, relation, attempt~) {
  Destination::reset_table(self.target, relation, attempt~)
}

///|
impl Destination for HandoverProbe with fn write_rows(self, relation, rows, id) {
  let result = Destination::write_rows(self.target, relation, rows, id)
  if relation.name == "b" && !rows.is_empty() {
    self.copied_b += rows.length()
    if self.copied_b == 1 {
      ignore(
        self.target.sql.query(
          "BEGIN; UPDATE pipeline_handover.a SET value='main' WHERE id=1; UPDATE pipeline_handover.b SET value='private' WHERE id=1; INSERT INTO pipeline_handover.b VALUES(999,'private'); DELETE FROM pipeline_handover.b WHERE id=128; COMMIT",
        ),
      )
    }
    @async.sleep(2)
  }
  result
}

///|
impl Destination for HandoverProbe with fn write_transaction(self, tx) {
  for index, change in tx.changes {
    match change {
      Update(relation, _, _) if relation.name == "a" => {
        assert_true(self.copied_b > 0 && self.copied_b < 128)
        assert_true(
          pipeline_status(self.state_path).tables.any(table => {
            table.phase is Copying
          }),
        )
        assert_false(tx.id.contains("/copy/"))
        self.main_end = Some(tx.end_lsn)
      }
      Insert(relation, _) | Update(relation, _, _) | Delete(relation, _) if relation.name ==
        "b" => {
        assert_true(tx.id.contains("/copy/"))
        let original = tx.id.split("/copy/").next().unwrap().to_owned()
        assert_true(tx.event_id(index).unwrap().has_prefix(original + "/"))
        self.caught_up = true
      }
      _ => ()
    }
  }
  ignore(Destination::write_transaction(self.target, tx))
  if tx.id.contains("/copy/") {
    Accepted
  } else {
    Durable
  }
}

///|
impl Destination for HandoverProbe with fn flush(self) {
  if self.caught_up && self.fail_flush {
    raise PipelineProbeFailure
  }
  Destination::flush(self.target)
  if self.caught_up && !self.flushed_copy {
    assert_true(
      pipeline_status(self.state_path).tables.any(table => {
        table.phase is Copying
      }),
    )
    self.flushed_copy = true
  }
}

///|
async test "live main apply continues while table copy catches up and hands over" {
  if live_config() is Some(config) {
    for fail_flush in [false, true] {
      @async.with_timeout(30000, () => {
        let sql = connect(config, 16777216, false)
        defer sql.close()
        ignore(
          sql.query(
            "CREATE SCHEMA pipeline_handover; CREATE TABLE pipeline_handover.a(id int PRIMARY KEY,value text); CREATE TABLE pipeline_handover.b(id int PRIMARY KEY,value text); INSERT INTO pipeline_handover.a VALUES(1,'before'); INSERT INTO pipeline_handover.b SELECT i,'before' FROM generate_series(1,128) i; CREATE PUBLICATION pipeline_handover FOR TABLE pipeline_handover.a,pipeline_handover.b",
          ),
        )
        let dir = @fs.tmpdir(prefix="mooncdc-handover-")
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
          schema="pipeline_handover",
        )
        let probe : HandoverProbe = {
          target,
          state_path: path,
          copied_b: 0,
          main_end: None,
          caught_up: false,
          flushed_copy: false,
          fail_flush,
        }
        let mut checkpoint_during_copy = false
        let succeeded = try {
          @async.with_task_group(group => {
            group.spawn_bg(() => {
              for ;; {
                @async.sleep(1)
                if probe.main_end is Some(position) {
                  let state = read_pipeline_state(path)
                  if state.tables.any(table => table.phase is Copying) &&
                    load_checkpoint(path + ".checkpoint", state.source) >=
                    position {
                    checkpoint_during_copy = true
                  }
                  if state.tables.all(table => {
                      table.phase is (Ready(_) | Catchup(_))
                    }) {
                    control.stop()
                    break
                  }
                }
              }
            })
            run_pipeline(
              config,
              probe,
              publication="pipeline_handover",
              destination_id="handover",
              state_path=path,
              control~,
              copy_concurrency=1,
              batch_rows=1,
            )
            group.return_immediately(())
          })
          true
        } catch {
          PipelineProbeFailure if fail_flush => false
          e => raise e
        }
        assert_eq(succeeded, !fail_flush)
        assert_true(checkpoint_during_copy)
        assert_true(probe.caught_up)
        assert_eq(probe.flushed_copy, !fail_flush)
        assert_probe_table(target, "a")
        assert_probe_table(target, "b")
        assert_eq(target.data.resets.get("a"), Some(1))
        assert_eq(target.data.resets.get("b"), Some(1))
        if fail_flush {
          let failed = read_pipeline_state(path)
          assert_true(failed.tables.any(table => table.phase is Copying))
          assert_true(
            load_checkpoint(path + ".checkpoint", failed.source) >
            Lsn::parse(failed.initial_lsn.unwrap()),
          )
          let recovered = PipelineProbe::new(
            dir + "/target",
            sql,
            ApplyControl(),
            "99",
            false,
            false,
            None,
            schema="pipeline_handover",
          )
          run_probe_pipeline(
            config,
            recovered,
            publication="pipeline_handover",
            destination_id="handover",
            state_path=path,
            batch_rows=1,
          )
          assert_eq(recovered.data.resets.get("a"), Some(1))
          assert_eq(recovered.data.resets.get("b"), Some(2))
          assert_probe_table(recovered, "a")
          assert_probe_table(recovered, "b")
        }
        drop_slot(sql, pipeline_status(path).slot)
        ignore(
          sql.query(
            "DROP PUBLICATION pipeline_handover; DROP SCHEMA pipeline_handover CASCADE",
          ),
        )
        println(
          "LIVE: concurrent apply, private WAL handover and advanced-checkpoint recovery (flush failure=\{fail_flush}) passed",
        )
      })
    }
  }
}
