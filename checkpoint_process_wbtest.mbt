///|
// Executed as a separate native process by tools/checkpoint-crash-test.mbtx.
// The regular test suite leaves this worker inactive.
async test "checkpoint process worker" {
  if @env.get_env_var("MOONCDC_CHECKPOINT_DIR") is Some(dir) {
    let config = live_config().unwrap()
    let phase = @env.get_env_var("MOONCDC_CHECKPOINT_PHASE").unwrap()
    let stage = @env.get_env_var("MOONCDC_CHECKPOINT_STAGE").unwrap()
    let sql = connect(config, 1048576, false)
    defer sql.close()
    let name = "cdc_checkpoint_crash"
    let path = dir + "/checkpoint"
    if phase == "init" {
      drop_slot(sql, name)
      ignore(
        sql.query(
          "DROP PUBLICATION IF EXISTS cdc_checkpoint_crash; DROP TABLE IF EXISTS cdc_checkpoint_crash; CREATE TABLE cdc_checkpoint_crash(id int PRIMARY KEY, value text); CREATE PUBLICATION cdc_checkpoint_crash FOR TABLE cdc_checkpoint_crash",
        ),
      )
      let start = fresh_slot(sql, name)
      @fs.write_file(dir + "/start", start.to_string(), sync=Full)
      for id in 1..<=3 {
        ignore(
          sql.query(
            "INSERT INTO cdc_checkpoint_crash VALUES (\{id},'business-\{id}')",
          ),
        )
      }
    } else if phase == "cleanup" {
      drop_slot(sql, name)
      ignore(
        sql.query(
          "DROP PUBLICATION cdc_checkpoint_crash; DROP TABLE cdc_checkpoint_crash",
        ),
      )
    } else {
      let control = connect(config, 1048576, true)
      defer control.close()
      let source = identify(control, config, name, name)
      let old = Lsn::parse(@fs.read_file(dir + "/start").text())
      let next = if phase == "recover" {
        Lsn::parse(@fs.read_file(dir + "/next").text())
      } else {
        old
      }
      let after_rename = stage == "after_rename" ||
        stage == "after_directory_sync"
      if phase == "recover" {
        assert_eq(
          load_checkpoint(path, source),
          if after_rename {
            next
          } else {
            old
          },
        )
      }
      let journal = dir + "/events.json"
      let entries : Array[Json] = if @fs.exists(journal) {
        @json.from_json(@json.parse(@fs.read_file(journal).text()))
      } else {
        []
      }
      let mut duplicates = 0
      subscribe(
        config,
        slot=name,
        publication=name,
        start=if phase == "crash" { At(old) } else { Resume },
        checkpoint_path=path,
        heartbeat_ms=60000,
        sub => {
          while entries.length() < 3 {
            let tx = sub.next()
            if entries.any(entry => {
                entry is { "id": String(id), .. } && id == tx.id
              }) {
              duplicates += 1
            } else {
              let ids : Array[String] = []
              for change in tx.changes {
                if change is Insert(_, [Text(id), _]) {
                  ids.push(id)
                }
              }
              entries.push({
                "id": tx.id.to_json(),
                "business_ids": ids.to_json(),
                "transaction": tx.to_json(),
              })
              let file = @fs.open(
                journal,
                mode=WriteOnly,
                create_mode=CreateOrTruncate,
              )
              file.write(entries.to_json().stringify())
              file.sync()
              file.close()
              let parent = @fs.open(dir, mode=ReadOnly)
              parent.sync()
              parent.close()
            }
            if phase == "crash" {
              @fs.write_file(dir + "/next", tx.end_lsn.to_string(), sync=Full)
              // Exercise the exact checkpoint writer used by ack, inside a live
              // subscription. No feedback may advance before this writer completes.
              save_checkpoint(path, sub.source, tx.end_lsn, boundary=at => {
                if at == stage {
                  @fs.write_file(dir + "/ready", stage, sync=Full)
                  @async.sleep(60000)
                  fail("orchestrator did not terminate checkpoint writer")
                }
              })
              fail("checkpoint kill boundary was not reached")
            }
            sub.ack(tx)
          }
        },
      )
      assert_eq(duplicates, if after_rename { 0 } else { 1 })
      let ids : Array[String] = []
      for entry in entries {
        guard entry
          is {
            "business_ids": Array(values),
            "transaction": { "changes": Array(_), .. },
            ..
          } else {
          fail("transaction missing")
        }
        for value in values {
          guard value is String(id) else { fail("invalid business ID") }
          ids.push(id)
        }
      }
      assert_eq(ids, ["1", "2", "3"])
      println(
        "CHECKPOINT recovered \{stage}: IDs=1,2,3 duplicates=\{duplicates}",
      )
    }
    println("CHECKPOINT worker completed " + phase)
  }
}
