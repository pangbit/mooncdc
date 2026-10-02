///|
async test "pipeline process crash worker" {
  if @env.get_env_var("MOONCDC_PIPELINE_PHASE") is Some(phase) {
    let config = live_config().unwrap()
    let dir = @env.get_env_var("MOONCDC_PIPELINE_DIR").unwrap()
    let path = dir + "/state"
    let sql = connect(config, 16777216, false)
    defer sql.close()
    match phase {
      "init" =>
        ignore(
          sql.query(
            "CREATE SCHEMA pipeline_crash; CREATE TABLE pipeline_crash.a(id int PRIMARY KEY,value text); CREATE TABLE pipeline_crash.b(id int PRIMARY KEY,value text); INSERT INTO pipeline_crash.a VALUES(1,'before'),(2,NULL); INSERT INTO pipeline_crash.b VALUES(1,'before'),(2,NULL); CREATE PUBLICATION pipeline_crash FOR TABLE pipeline_crash.a,pipeline_crash.b",
          ),
        )
      "crash" => {
        let control = ApplyControl()
        let probe = PipelineProbe::new(
          dir + "/target",
          sql,
          control,
          "unused",
          false,
          true,
          None,
          schema="pipeline_crash",
          crash_signal=dir + "/ready",
        )
        run_pipeline(
          config,
          probe,
          publication="pipeline_crash",
          destination_id="process-probe",
          state_path=path,
          control~,
          copy_concurrency=1,
          batch_rows=1,
        )
        fail("crash worker returned")
      }
      "recover" => {
        let before = read_pipeline_state(path)
        assert_true(before.tables[0].phase is (Catchup(_) | Ready(_)))
        assert_true(before.tables[1].phase is Copying)
        assert_true(
          load_checkpoint(path + ".checkpoint", before.source) >=
          Lsn::parse(before.initial_lsn.unwrap()),
        )
        ignore(
          sql.query(
            "BEGIN; UPDATE pipeline_crash.a SET value='after-kill' WHERE id=1; DELETE FROM pipeline_crash.a WHERE id=2; INSERT INTO pipeline_crash.b VALUES(3,'after-kill'); COMMIT",
          ),
        )
        let control = ApplyControl()
        let probe = PipelineProbe::new(
          dir + "/target",
          sql,
          control,
          "99",
          false,
          false,
          None,
          schema="pipeline_crash",
        )
        @async.with_timeout(30000, () => {
          run_probe_pipeline(
            config,
            probe,
            publication="pipeline_crash",
            destination_id="process-probe",
            state_path=path,
            batch_rows=1,
          )
        })
        assert_eq(probe.data.resets.get("a"), Some(1))
        assert_eq(probe.data.resets.get("b"), Some(2))
        assert_probe_table(probe, "a")
        assert_probe_table(probe, "b")
        assert_true(
          pipeline_status(path).tables.all(table => table.phase is Ready(_)),
        )
        assert_true(
          load_checkpoint(path + ".checkpoint", before.source) >
          Lsn::parse(before.initial_lsn.unwrap()),
        )
      }
      "cleanup" => {
        if @fs.exists(path) {
          drop_slot(sql, pipeline_status(path).slot)
        }
        ignore(
          sql.query(
            "DROP PUBLICATION pipeline_crash; DROP SCHEMA pipeline_crash CASCADE",
          ),
        )
      }
      _ => fail("unknown process phase")
    }
    println("PIPELINE worker completed \{phase}")
  }
}
