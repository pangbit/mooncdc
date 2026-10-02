///|
async test "live cancellation checkpoint failure lock and stale acknowledgement" {
  if live_config() is Some(config) {
    let sql = connect(config, 1048576, false)
    defer sql.close()
    ignore(
      sql.query(
        "DROP PUBLICATION IF EXISTS cdc_lifecycle; DROP TABLE IF EXISTS cdc_lifecycle; CREATE TABLE cdc_lifecycle(id int PRIMARY KEY, value text); CREATE PUBLICATION cdc_lifecycle FOR TABLE cdc_lifecycle",
      ),
    )
    let start = fresh_slot(sql, "cdc_lifecycle")
    let dir = @fs.tmpdir(prefix="mooncdc-lifecycle-")
    defer @fs.rmdir(dir, recursive=true)
    let path = dir + "/checkpoint"
    let previous = subscribe(
      config,
      slot="cdc_lifecycle",
      publication="cdc_lifecycle",
      start=At(start),
      checkpoint_path=path,
      sub => {
        ignore(
          sql.query("INSERT INTO cdc_lifecycle VALUES (30000,'durability')"),
        )
        let tx = sub.next()
        // A real filesystem failure before rename cannot publish an acknowledgement.
        @fs.mkdir(path + ".tmp")
        try sub.ack(tx) catch {
          _ => ()
        } noraise {
          _ => fail("expected checkpoint write failure")
        }
        assert_eq(sub.diagnostics().durable, start)
        try
          subscribe(
            config,
            slot="cdc_lifecycle",
            publication="cdc_lifecycle",
            start=Resume,
            checkpoint_path=path,
            _ => (),
          )
        catch {
          CheckpointError(_) => ()
          e => raise e
        } noraise {
          _ => fail("checkpoint lock not exclusive")
        }
        tx
      },
    )
    @fs.rmdir(path + ".tmp")
    subscribe(
      config,
      slot="cdc_lifecycle",
      publication="cdc_lifecycle",
      start=Resume,
      checkpoint_path=path,
      sub => {
        let tx = sub.next()
        assert_eq(tx.id, previous.id)
        try sub.ack(previous) catch {
          InvalidConfig(_) => ()
          e => raise e
        } noraise {
          _ => fail("stale session token accepted")
        }
        sub.ack(tx)
        sub.ack(tx)
        assert_eq(sub.diagnostics().outstanding_transactions, 0)
      },
    )
    try
      @async.with_timeout(200, () => {
        subscribe(
          config,
          slot="cdc_lifecycle",
          publication="cdc_lifecycle",
          start=Resume,
          checkpoint_path=path,
          sub => ignore(sub.next()),
        )
      })
    catch {
      @async.TimeoutError => ()
      e => raise e
    } noraise {
      _ => fail("cancellation did not fire")
    }
    // A new subscription proves cancellation released the checkpoint lock.
    subscribe(
      config,
      slot="cdc_lifecycle",
      publication="cdc_lifecycle",
      start=Resume,
      checkpoint_path=path,
      sub => sub.close(),
    )
    drop_slot(sql, "cdc_lifecycle")
    ignore(
      sql.query("DROP PUBLICATION cdc_lifecycle; DROP TABLE cdc_lifecycle"),
    )
    println(
      "LIVE: checkpoint I/O failure, lock, stale ack, cancellation and close passed",
    )
  }
}
