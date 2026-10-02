///|
async fn main {
  let name = "example_index"
  let start = @support.prepare(name)
  defer @support.cleanup(name)
  let dir = @fs.tmpdir(prefix="mooncdc-index-")
  defer @fs.rmdir(dir, recursive=true)
  let index : Map[String, @cdc.Value] = Map([])
  let applied : Map[String, Unit] = Map([])
  @async.with_timeout(15000, () => {
    @cdc.subscribe(
      @support.config(),
      slot=name,
      publication=name,
      start=@cdc.At(start),
      checkpoint_path=dir + "/checkpoint",
      sub => {
        ignore(
          @support.sql(
            "BEGIN; INSERT INTO example_index VALUES (1,'first'),(2,'remove'); UPDATE example_index SET value='final' WHERE id=1; DELETE FROM example_index WHERE id=2; COMMIT; BEGIN; INSERT INTO example_index VALUES (3,'rollback'); ROLLBACK;",
          ),
        )
        let tx = sub.next()
        // Applying the same transaction twice demonstrates transaction-level idempotency.
        for _ in 0..<2 {
          if !applied.contains(tx.id) {
            for change in tx.changes {
              match change {
                @cdc.Insert(_, row) => index[@support.key(row)] = row[1]
                @cdc.Update(_, old, row) => {
                  match old {
                    Some(@cdc.Full(previous)) | Some(@cdc.Key(previous)) =>
                      if @support.key(previous) != @support.key(row) {
                        index.remove(@support.key(previous))
                      }
                    None => ()
                  }
                  if row[1] != @cdc.UnchangedToast {
                    index[@support.key(row)] = row[1]
                  }
                }
                @cdc.Delete(_, old) =>
                  match old {
                    @cdc.Full(row) | @cdc.Key(row) =>
                      index.remove(@support.key(row))
                  }
                @cdc.Truncate(_, ..) => index.clear()
                _ => ()
              }
            }
            applied[tx.id] = ()
          }
        }
        assert_eq(index.length(), 1)
        assert_eq(index.get("1"), Some(@cdc.Text("final")))
        assert_eq(
          @support.sql("SELECT id,value FROM example_index ORDER BY id"),
          "1|final",
        )
        sub.ack(tx)
        println(
          "Index matches database; duplicate application and rollback did not change it.",
        )
      },
    )
  })
}
