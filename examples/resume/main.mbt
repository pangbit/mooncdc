///|
async fn journal(path : String) -> Array[Json] {
  if !@fs.exists(path) {
    return []
  }
  @json.from_json(@json.parse(@fs.read_file(path).text()))
}

///|
async fn persist(path : String, tx : @cdc.Transaction) -> Bool {
  let entries = journal(path)
  if entries.any(entry => entry is { "id": String(id), .. } && id == tx.id) {
    return false
  }
  let business_ids : Array[String] = []
  for change in tx.changes {
    if change is @cdc.Insert(_, row) {
      business_ids.push(@support.key(row))
    }
  }
  entries.push({
    "id": tx.id.to_json(),
    "business_ids": business_ids.to_json(),
    "transaction": tx.to_json(),
  })
  let file = @fs.open(
    path + ".tmp",
    mode=WriteOnly,
    create_mode=CreateOrTruncate,
    permission=0o600,
  )
  defer file.close()
  file.write(entries.to_json().stringify())
  file.sync()
  @fs.rename(path + ".tmp", path)
  // The caller passes <directory>/events.json, so syncing the parent is explicit.
  let parent = path[:path.rev_find("/").unwrap()].to_owned()
  let directory = @fs.open(parent, mode=ReadOnly)
  defer directory.close()
  directory.sync()
  true
}

///|
async fn stop_at(dir : String, selected : String, stage : String) -> Unit {
  if selected == stage {
    @fs.write_file(dir + "/ready", stage, sync=Full)
    // The test orchestrator sends SIGKILL to this executable after reading ready.
    @async.sleep(60000)
    fail("crash orchestrator did not interrupt this process")
  }
}

///|
async fn main {
  let args = @env.args()
  guard args.length() >= 3 else {
    fail("usage: resume <init|recover|cleanup|crash> <directory> [crash-stage]")
  }
  let mode = args[1]
  let dir = args[2]
  let name = "example_resume"
  if mode == "init" {
    @fs.mkdir(dir, recursive=true, allow_exist=true)
    let start = @support.prepare(name)
    @fs.write_file(dir + "/start", start.to_string(), sync=Full)
    for id in 1..<=3 {
      ignore(
        @support.sql(
          "INSERT INTO example_resume VALUES (\{id},'business-\{id}')",
        ),
      )
    }
    return
  }
  if mode == "cleanup" {
    @support.cleanup(name)
    return
  }
  guard mode == "recover" || mode == "crash" else { fail("unknown mode") }
  let stage = args.get(3).unwrap_or("")
  let checkpoint = dir + "/checkpoint"
  let initial = if @fs.exists(checkpoint) {
    @cdc.Resume
  } else {
    @cdc.At(@cdc.Lsn::parse(@fs.read_file(dir + "/start").text()))
  }
  let mut duplicates = 0
  @async.with_timeout(70000, () => {
    @cdc.subscribe(
      @support.config(),
      slot=name,
      publication=name,
      start=initial,
      checkpoint_path=checkpoint,
      heartbeat_ms=if mode == "crash" && stage == "after_ack" {
        60000
      } else {
        1000
      },
      sub => {
        for ;; {
          let tx = sub.next()
          stop_at(dir, stage, "before_sink")
          if !persist(dir + "/events.json", tx) {
            duplicates += 1
          }
          stop_at(dir, stage, "after_sink")
          sub.ack(tx)
          stop_at(dir, stage, "after_ack")
          if stage == "after_feedback" {
            @async.sleep(1500)
            let confirmed = @cdc.Lsn::parse(
              @support.sql(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='example_resume'",
              ),
            )
            assert_eq(confirmed, tx.end_lsn)
            stop_at(dir, stage, "after_feedback")
          }
          if journal(dir + "/events.json").length() == 3 {
            break
          }
        }
      },
    )
  })
  let entries = journal(dir + "/events.json")
  let ids : Array[String] = []
  for entry in entries {
    guard entry is { "business_ids": Array(values), .. } else {
      fail("invalid journal")
    }
    for value in values {
      guard value is String(id) else { fail("invalid business id") }
      ids.push(id)
    }
  }
  assert_eq(ids, ["1", "2", "3"])
  println(
    "Resume verified: business IDs 1,2,3, complete transactions=3, duplicates=\{duplicates}",
  )
}
