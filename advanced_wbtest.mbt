///|
async test "live metadata TOAST key changes truncate oversized transaction and unsafe resume" {
  if live_config() is Some(config) {
    @async.with_timeout(45000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      let control = connect(config, 1048576, true)
      defer control.close()
      ignore(
        sql.query(
          "DROP PUBLICATION IF EXISTS cdc_meta; DROP TABLE IF EXISTS cdc_meta; DROP TYPE IF EXISTS cdc_mood; CREATE TYPE cdc_mood AS ENUM ('a','b'); CREATE TABLE cdc_meta(id int PRIMARY KEY, value text, mood cdc_mood); CREATE PUBLICATION cdc_meta FOR TABLE cdc_meta;",
        ),
      )
      let start = fresh_slot(sql, "cdc_meta")
      let dir = @fs.tmpdir(prefix="mooncdc-advanced-")
      defer @fs.rmdir(dir, recursive=true)
      let path = dir + "/checkpoint"
      subscribe(
        config,
        slot="cdc_meta",
        publication="cdc_meta",
        start=At(start),
        checkpoint_path=path,
        sub => {
          ignore(
            sql.query(
              "INSERT INTO cdc_meta VALUES (1,(SELECT string_agg(md5(g::text),'') FROM generate_series(1,1000) g),'a')",
            ),
          )
          let first = sub.next()
          assert_true(
            first.changes.any(c => c is TypeChanged({ name: "cdc_mood", .. })),
          )
          sub.ack(first)
          ignore(sql.query("UPDATE cdc_meta SET mood='b' WHERE id=1"))
          let toast = sub.next()
          assert_true(
            toast.changes.any(c => {
              c is Update(_, None, [Text("1"), UnchangedToast, Text("b")])
            }),
          )
          sub.ack(toast)
          ignore(sql.query("UPDATE cdc_meta SET id=2 WHERE id=1"))
          let key = sub.next()
          assert_true(
            key.changes.any(c => {
              c is Update(_, Some(Key([Text("1"), ..])), [Text("2"), ..])
            }),
          )
          sub.ack(key)
          ignore(
            sql.query(
              "ALTER TABLE cdc_meta ADD COLUMN extra text; INSERT INTO cdc_meta VALUES (3,NULL,'a','new')",
            ),
          )
          let meta = sub.next()
          assert_true(
            meta.changes.any(c => {
              c
              is Insert(
                { columns, .. },
                [Text("3"), Null, Text("a"), Text("new")]
              ) &&
              columns.length() == 4
            }),
          )
          sub.ack(meta)
          ignore(sql.query("TRUNCATE cdc_meta RESTART IDENTITY CASCADE"))
          let truncated = sub.next()
          assert_true(
            truncated.changes.any(c => {
              c is Truncate([_], cascade=true, restart_identity=true)
            }),
          )
          sub.ack(truncated)
        },
      )
      // Reject a wrong publication before resuming any stream.
      try
        subscribe(
          config,
          slot="cdc_meta",
          publication="mooncdc_test",
          start=Resume,
          checkpoint_path=path,
          _ => (),
        )
      catch {
        CheckpointError(_) => ()
        e => raise e
      } noraise {
        _ => fail("mismatched publication accepted")
      }
      drop_slot(sql, "cdc_meta")
      try
        subscribe(
          config,
          slot="cdc_meta",
          publication="cdc_meta",
          start=Resume,
          checkpoint_path=path,
          _ => (),
        )
      catch {
        UnsafeResume(_) => ()
        e => raise e
      } noraise {
        _ => fail("missing slot accepted")
      }
      ignore(
        sql.query(
          "DROP PUBLICATION cdc_meta; DROP TABLE cdc_meta; DROP TYPE cdc_mood",
        ),
      )
      ignore(
        sql.query(
          "DROP PUBLICATION IF EXISTS cdc_large; DROP TABLE IF EXISTS cdc_large; CREATE TABLE cdc_large(id int PRIMARY KEY, value text); CREATE PUBLICATION cdc_large FOR TABLE cdc_large",
        ),
      )
      let start = fresh_slot(sql, "cdc_large")
      let path = dir + "/large"
      try
        subscribe(
          config,
          slot="cdc_large",
          publication="cdc_large",
          start=At(start),
          checkpoint_path=path,
          max_frame_bytes=1024,
          max_transaction_bytes=4096,
          sub => {
            ignore(
              sql.query(
                "INSERT INTO cdc_large SELECT g,repeat('x',100) FROM generate_series(10000,10199) g",
              ),
            )
            ignore(sub.next())
          },
        )
      catch {
        LimitExceeded(_) => ()
        e => raise e
      } noraise {
        _ => fail("transaction cap")
      }
      let source = identify(control, config, "cdc_large", "cdc_large")
      assert_eq(load_checkpoint(path, source), start)
      subscribe(
        config,
        slot="cdc_large",
        publication="cdc_large",
        start=Resume,
        checkpoint_path=path,
        sub => {
          let tx = sub.next()
          assert_eq(tx.changes.filter(c => c is Insert(_, _)).length(), 200)
          sub.ack(tx)
        },
      )
      drop_slot(sql, "cdc_large")
      ignore(sql.query("DROP PUBLICATION cdc_large; DROP TABLE cdc_large"))
      println(
        "LIVE: custom types, TOAST, key update, metadata change, truncate, unsafe recovery and oversized transaction replay passed",
      )
    })
  }
}
