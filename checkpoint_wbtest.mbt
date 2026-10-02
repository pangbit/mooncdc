///|
async test "atomic checkpoint interruption at every durable write boundary" {
  let dir = @fs.tmpdir(prefix="mooncdc-checkpoint-")
  defer @fs.rmdir(dir, recursive=true)
  let source : Source = {
    system_id: "system",
    timeline: "1",
    database: "db",
    slot: "slot",
    publication: "pub",
  }
  let old = Lsn::parse("0/10")
  let next = Lsn::parse("0/20")
  for
    stage in [
      "before_write", "after_write", "after_file_sync", "after_rename", "after_directory_sync",
    ] {
    let path = dir + "/" + stage
    save_checkpoint(path, source, old)
    try
      save_checkpoint(path, source, next, boundary=at => {
        if at == stage {
          raise CheckpointError("injected interruption")
        }
      })
    catch {
      CheckpointError(_) => ()
      e => raise e
    } noraise {
      _ => fail("fault was not injected")
    }
    let expected = if stage == "before_write" ||
      stage == "after_write" ||
      stage == "after_file_sync" {
      old
    } else {
      next
    }
    assert_eq(load_checkpoint(path, source), expected)
  }
}

///|
test "stable event ids ignore repeated relation metadata" {
  let d = Decoder::new(4096, 100, 10)
  ignore(d.decode(begin_fixture()))
  ignore(d.decode(relation_fixture("items")))
  ignore(d.decode(tuple_fixture(b'I', b"N\x00\x02t\x00\x00\x00\x011n")))
  guard d.decode(commit_fixture())
    is Some((xid, commit_lsn, end_lsn, time, changes)) else {
    fail("commit")
  }
  let a : Transaction = {
    id: "stable",
    xid,
    commit_lsn,
    end_lsn,
    commit_time_us: time,
    changes,
    session: "a",
    sequence: 1,
  }
  let b : Transaction = {
    ..a,
    changes: ReadOnlyArray::from_array(changes[1:]),
    session: "b",
  }
  assert_eq(a.event_id(0), None)
  assert_eq(a.event_id(1), b.event_id(0))
  assert_eq(a.event_id(1), Some("stable/0"))
  assert_eq(a.event_id(-1), None)
}
