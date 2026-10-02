///|
async fn slot_safety_wait_inactive(sql : Connection, slot : String) -> Unit {
  @async.with_timeout(2000, () => {
    while cell(
            sql.query(
              "SELECT active::text FROM pg_replication_slots WHERE slot_name='\{slot}'",
            ),
            0,
            0,
          ) !=
          "false" {
      @async.sleep(20)
    }
  })
}

///|
async fn assert_slot_resume_rejected(
  config : ConnectionConfig,
  source : Source,
  path : String,
  start : Lsn,
  expected : String,
) -> Unit {
  subscribe(
    config,
    slot=source.slot,
    publication=source.publication,
    start=Resume,
    checkpoint_path=path,
    _ => fail("unsafe live slot accepted"),
  ) catch {
    UnsafeResume(message) => assert_eq(message, expected)
    e => raise e
  }
  let original = checkpoint_bytes(source, start)
  let file = @fs.open(path, mode=ReadOnly)
  defer file.close()
  assert_eq(file.size(), original.length().to_int64())
  assert_eq(file.read_exactly(original.length()), original)
  assert_eq(load_checkpoint(path, source), start)
  let lock = @fs.open(path + ".lock", mode=ReadWrite)
  defer lock.close()
  assert_true(lock.try_lock(Exclusive))
}

///|
async test "slot safety live advancement invalidation and quiet publication WAL" {
  if @env.get_env_var("MOONCDC_SLOT_TEST_PORT") is Some(port) {
    // This scenario deliberately releases WAL. Never point it at the ordinary
    // fixture or a shared database; use integration/slot-safety.compose.yaml.
    assert_true(port == "55428" || port == "55427")
    let config : ConnectionConfig = {
      host: "127.0.0.1",
      port: @string.parse_int(port),
      user: "cdc",
      password: "mooncdc-test-only",
      database: "mooncdc_slot_safety",
      security: LocalPlaintext,
      timeout_ms: 5000,
    }
    @async.with_timeout(60000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      let control = connect(config, 1048576, true)
      defer control.close()
      assert_eq(cell(sql.query("SHOW max_slot_wal_keep_size"), 0, 0), "0")
      assert_eq(cell(sql.query("SHOW wal_sender_timeout"), 0, 0), "5s")
      assert_eq(cell(sql.query("SHOW wal_segment_size"), 0, 0), "16MB")
      assert_true(
        sql.query("SELECT slot_name FROM pg_replication_slots").is_empty(),
      )
      println("SLOT ENV: " + cell(sql.query("SELECT version()"), 0, 0))
      ignore(
        sql.query(
          "CREATE TABLE cdc_safety(id int PRIMARY KEY); CREATE TABLE cdc_noise(value text); CREATE PUBLICATION cdc_safety FOR TABLE cdc_safety",
        ),
      )
      let dir = @fs.tmpdir(prefix="mooncdc-slot-safety-")
      defer @fs.rmdir(dir, recursive=true)

      // An external consumer/administrator advances the inactive source slot.
      let slot = "cdc_safety_advance"
      let start = fresh_slot(sql, slot)
      let path = dir + "/advance"
      let source = identify(control, config, slot, "cdc_safety")
      subscribe(
        config,
        slot~,
        publication="cdc_safety",
        start=At(start),
        checkpoint_path=path,
        _ => (),
      )
      slot_safety_wait_inactive(sql, slot)
      ignore(sql.query("INSERT INTO cdc_safety VALUES (1)"))
      let advanced = Lsn::parse(
        cell(
          sql.query(
            "SELECT end_lsn::text FROM pg_replication_slot_advance('cdc_safety_advance', pg_current_wal_lsn())",
          ),
          0,
          0,
        ),
      )
      assert_true(advanced > start)
      assert_slot_resume_rejected(
        config, source, path, start, "slot has advanced beyond durable consumer position",
      )
      println(
        "SLOT ADVANCE: checkpoint=\{start.to_string()} slot=\{advanced.to_string()} rejected=true checkpoint_unchanged=true",
      )
      drop_slot(sql, slot)

      // Quiet publication: only an unrelated table changes, across >1 sender timeout.
      ignore(sql.query("CHECKPOINT"))
      let slot = "cdc_safety_quiet"
      let start = fresh_slot(sql, slot)
      let path = dir + "/quiet"
      let source = identify(control, config, slot, "cdc_safety")
      subscribe(
        config,
        slot~,
        publication="cdc_safety",
        start=At(start),
        checkpoint_path=path,
        sub => {
          let initial = sql.query(
            "SELECT restart_lsn::text, confirmed_flush_lsn::text, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::text FROM pg_replication_slots WHERE slot_name='cdc_safety_quiet'",
          )
          let restart = cell(initial, 0, 0)
          let initial_bytes = @string.parse_int64(cell(initial, 0, 2))
          let mut previous_bytes = initial_bytes
          for sample in 1..<=3 {
            ignore(
              sql.query(
                "INSERT INTO cdc_noise SELECT repeat(md5(g::text),32) FROM generate_series(1,1000) g",
              ),
            )
            @async.sleep(2200)
            let state = sql.query(
              "SELECT restart_lsn::text, confirmed_flush_lsn::text, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::text, active::text FROM pg_replication_slots WHERE slot_name='cdc_safety_quiet'",
            )
            let retained = @string.parse_int64(cell(state, 0, 2))
            assert_eq(cell(state, 0, 0), restart)
            assert_eq(Lsn::parse(cell(state, 0, 1)), start)
            assert_eq(cell(state, 0, 3), "true")
            assert_true(retained > previous_bytes)
            previous_bytes = retained
            assert_eq(sub.diagnostics().reconnects, 0)
            assert_eq(sub.diagnostics().received, start)
            assert_eq(sub.diagnostics().durable, start)
            assert_eq(sub.diagnostics().outstanding_transactions, 0)
            println(
              "SLOT QUIET: sample=\{sample} restart=\{restart} confirmed=\{start.to_string()} retained_wal_bytes=\{retained} diagnostics=\{sub.diagnostics().to_json().stringify()}",
            )
          }
          assert_true(previous_bytes - initial_bytes > 1000000L)
          assert_true(sub.diagnostics().server_end > start)
          assert_eq(load_checkpoint(path, source), start)
        },
      )
      drop_slot(sql, slot)

      // The isolated server has max_slot_wal_keep_size=0. Each switch consumes
      // at most one 16 MiB segment; cap the attempt at eight, never an unbounded fill.
      ignore(sql.query("CHECKPOINT"))
      let slot = "cdc_safety_lost"
      let start = fresh_slot(sql, slot)
      let path = dir + "/lost"
      let source = identify(control, config, slot, "cdc_safety")
      subscribe(
        config,
        slot~,
        publication="cdc_safety",
        start=At(start),
        checkpoint_path=path,
        _ => (),
      )
      slot_safety_wait_inactive(sql, slot)
      let mut invalidated = false
      for attempt in 1..<=8 {
        ignore(sql.query("INSERT INTO cdc_noise VALUES ('switch WAL')"))
        ignore(sql.query("SELECT pg_switch_wal()"))
        ignore(sql.query("CHECKPOINT"))
        let state = sql.query(
          "SELECT wal_status, invalidation_reason, restart_lsn::text, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='cdc_safety_lost'",
        )
        println(
          "SLOT INVALIDATION: attempt=\{attempt} checkpoint=\{start.to_string()} state=\{state.to_json().stringify()}",
        )
        if cell(state, 0, 0) == "lost" {
          assert_eq(cell(state, 0, 1), "wal_removed")
          invalidated = true
          break
        }
      }
      assert_true(invalidated)
      assert_slot_resume_rejected(
        config, source, path, start, "replication slot invalidated",
      )
      println("SLOT LOST: rejected=true checkpoint_unchanged=true")
      drop_slot(sql, slot)
      ignore(
        sql.query(
          "DROP PUBLICATION cdc_safety; DROP TABLE cdc_safety, cdc_noise",
        ),
      )
      assert_true(
        sql.query("SELECT slot_name FROM pg_replication_slots").is_empty(),
      )
      println(
        "SLOT CLEANUP: no remaining slots; tables and publication removed",
      )
    })
  }
}
