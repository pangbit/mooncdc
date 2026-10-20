///|
async test "physical standby synchronizes failover slot and rejects old timeline checkpoint after promotion" {
  guard @env.get_env_var("MOONCDC_FAILOVER_PHASE") is Some(phase) &&
    (phase == "prepare" || phase == "promote") else {
    return
  }
  let primary = live_config().unwrap()
  let standby = {
    ..primary,
    port: @string.parse_int(@env.get_env_var("MOONCDC_STANDBY_PORT").unwrap()),
  }
  let dir = @env.get_env_var("MOONCDC_FAILOVER_DIR").unwrap()
  let path = dir + "/checkpoint"
  let slot = "mooncdc_failover"
  let publication = "mooncdc_failover"
  @async.with_timeout(90000, () => {
    let reader = connect(standby, 16777216, false)
    defer reader.close()
    assert_eq(
      cell(reader.query("SELECT pg_is_in_recovery()::text"), 0, 0),
      "true",
    )
    if phase == "prepare" {
      let writer = connect(primary, 16777216, false)
      defer writer.close()
      assert_eq(
        cell(writer.query("SHOW synchronized_standby_slots"), 0, 0),
        "mooncdc_physical",
      )
      assert_eq(
        cell(reader.query("SHOW primary_slot_name"), 0, 0),
        "mooncdc_physical",
      )
      assert_eq(cell(reader.query("SHOW hot_standby_feedback"), 0, 0), "on")
      assert_eq(cell(reader.query("SHOW sync_replication_slots"), 0, 0), "on")
      ignore(
        writer.query(
          "CREATE TABLE items(id integer PRIMARY KEY); INSERT INTO items VALUES(1); CREATE PUBLICATION mooncdc_failover FOR TABLE items",
        ),
      )
      let mut copied = 0
      let point = copy_snapshot(
        primary,
        slot~,
        publication~,
        slot_failover=true,
        event => if event is TableRows(_, rows) { copied += rows.length() },
      )
      assert_eq(copied, 1)
      point.save_checkpoint(path)
      @fs.write_file(dir + "/source", ToJson::to_json(point.source).stringify())
      let query = "SELECT failover::text,synced::text,temporary::text,invalidation_reason,confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='\{slot}'"
      // Decoding/acknowledging WAL after a running-xacts snapshot advances the
      // primary slot's catalog horizon, allowing the standby to persist it.
      for ;; {
        ignore(
          writer.query(
            "SELECT pg_log_standby_snapshot(); INSERT INTO items VALUES(2) ON CONFLICT(id) DO UPDATE SET id=excluded.id",
          ),
        )
        subscribe(
          primary,
          slot~,
          publication~,
          start=Resume,
          checkpoint_path=path,
          sub => {
            let tx = sub.next()
            assert_true(
              tx.changes.any(change => {
                change is Insert(_, [Text("2")]) ||
                change is Update(_, _, [Text("2")])
              }),
            )
            sub.ack(tx)
            while Lsn::parse(
                    cell(
                      writer.query(
                        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='\{slot}'",
                      ),
                      0,
                      0,
                    ),
                  ) <
                  tx.end_lsn {
              @async.sleep(20)
            }
          },
        )
        let rows = reader.query(query)
        if rows.length() == 1 &&
          rows[0][0] == Some("true") &&
          rows[0][1] == Some("true") &&
          rows[0][2] == Some("false") &&
          rows[0][3] == None {
          break
        }
        @async.sleep(500)
      }
      let checkpoint = load_checkpoint(path, point.source)
      assert_true(checkpoint > point.lsn)
      ignore(writer.query("SELECT pg_log_standby_snapshot(); CHECKPOINT"))
      for ;; {
        let rows = reader.query(query)
        if rows.length() == 1 &&
          rows[0][0] == Some("true") &&
          rows[0][1] == Some("true") &&
          rows[0][2] == Some("false") &&
          rows[0][3] == None &&
          rows[0][4] == Some(checkpoint.to_string()) {
          break
        }
        @async.sleep(100)
      }
      assert_eq(
        cell(reader.query("SELECT count(*)::text FROM items"), 0, 0),
        "2",
      )
    } else {
      let source : Source = @json.from_json(
        @json.parse(@fs.read_file(dir + "/source").text()),
      )
      let before = @fs.read_file(path).binary()
      let checkpoint = load_checkpoint(path, source)
      assert_eq(
        cell(reader.query("SELECT pg_promote(true,30)::text"), 0, 0),
        "true",
      )
      assert_eq(
        cell(reader.query("SELECT pg_is_in_recovery()::text"), 0, 0),
        "false",
      )
      let repl = connect(standby, 16777216, true)
      defer repl.close()
      let promoted = identify(repl, standby, slot, publication)
      assert_eq(promoted.system_id, source.system_id)
      assert_false(promoted.timeline == source.timeline)
      // Preserve the current safety boundary until ancestry/fork-safe recovery
      // is implemented; rejecting this checkpoint is intentional evidence.
      try
        subscribe(
          standby,
          slot~,
          publication~,
          start=Resume,
          checkpoint_path=path,
          fn(_ : Subscription) -> Unit raise {
            fail("old timeline reached consumer")
          },
        )
      catch {
        CheckpointError(_) => ()
        error => raise error
      } noraise {
        _ => fail("old timeline checkpoint accepted")
      }
      assert_eq(@fs.read_file(path).binary(), before)
      let rows = reader.query(
        "SELECT failover::text,temporary::text,invalidation_reason FROM pg_replication_slots WHERE slot_name='\{slot}'",
      )
      assert_eq(rows, [[Some("true"), Some("false"), None]])
      ignore(reader.query("INSERT INTO items VALUES(3)"))
      // An explicit new consumer proves that the promoted PostgreSQL slot is
      // usable, without changing or laundering the old checkpoint's identity.
      subscribe(
        standby,
        slot~,
        publication~,
        start=At(checkpoint),
        checkpoint_path=dir + "/explicit-new-consumer",
        sub => {
          let tx = sub.next()
          assert_true(
            tx.changes.any(change => change is Insert(_, [Text("3")])),
          )
          sub.ack(tx)
        },
      )
      assert_eq(@fs.read_file(path).binary(), before)
    }
  })
  println("FAILOVER: \{phase} passed")
}
