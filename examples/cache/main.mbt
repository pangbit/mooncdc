///|
async fn main {
  let name = "example_cache"
  let start = @support.prepare(
    name,
    seed="INSERT INTO example_cache VALUES (1,'a'),(2,'b')",
  )
  defer @support.cleanup(name)
  let dir = @fs.tmpdir(prefix="mooncdc-cache-")
  defer @fs.rmdir(dir, recursive=true)
  let cache : Map[String, String] = {
    "1": "cached a",
    "2": "cached b",
    "99": "unrelated",
  }
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
            "BEGIN; UPDATE example_cache SET value='changed' WHERE id=1; DELETE FROM example_cache WHERE id=2; COMMIT;",
          ),
        )
        let tx = sub.next()
        for change in tx.changes {
          match change {
            @cdc.Update(_, old, row) => {
              cache.remove(@support.key(row))
              match old {
                Some(@cdc.Full(previous)) | Some(@cdc.Key(previous)) =>
                  cache.remove(@support.key(previous))
                None =>
                  fail(
                    "old identity is unavailable: use REPLICA IDENTITY FULL for this example",
                  )
              }
            }
            @cdc.Delete(_, old) =>
              match old {
                @cdc.Full(row) | @cdc.Key(row) =>
                  cache.remove(@support.key(row))
              }
            @cdc.Insert(_, row) => cache.remove(@support.key(row))
            @cdc.Truncate(_, ..) => cache.clear()
            _ => ()
          }
        }
        assert_eq(cache.length(), 1)
        assert_eq(cache.get("99"), Some("unrelated"))
        sub.ack(tx)
        println(
          "Only affected cache keys invalidated; unrelated key 99 retained.",
        )
      },
    )
  })
}
