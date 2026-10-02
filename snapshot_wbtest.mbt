///|
test "snapshot SQL quoting rejects NUL and preserves quotes" {
  assert_eq(sql_identifier("a\"b\"测"), "\"a\"\"b\"\"测\"")
  assert_eq(sql_literal("a'b\\c"), "E'a''b\\\\c'")
  for value in ["", "a\u0000b"] {
    try sql_identifier(value) catch {
      InvalidConfig(_) => ()
      e => raise e
    } noraise {
      _ => fail("unsafe identifier accepted")
    }
  }
  try sql_literal("a\u0000b") catch {
    InvalidConfig(_) => ()
    e => raise e
  } noraise {
    _ => fail("NUL literal accepted")
  }
}

///|
async test "live snapshot projection filter concurrent writes and WAL handoff" {
  if live_config() is Some(config) {
    @async.with_timeout(20000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      let ns = sql_identifier("snap\"测")
      let table = ns + "." + sql_identifier("rows'表")
      let column = sql_identifier("v\"alue")
      ignore(
        sql.query(
          "CREATE SCHEMA \{ns}; CREATE TABLE \{table}(id int, \{column} text, hidden text, PRIMARY KEY(id) INCLUDE(\{column})); CREATE TABLE \{ns}.empty(id int PRIMARY KEY); INSERT INTO \{table} VALUES (1,'one','secret'),(2,'two','secret'),(3,NULL,'secret'),(-1,'excluded','secret'); CREATE PUBLICATION cdc_snapshot FOR TABLE \{table}(id,\{column}) WHERE (id > 0), \{ns}.empty",
        ),
      )
      let state : Map[String, Value] = Map([])
      let begins : Array[String] = []
      let ends : Array[String] = []
      let relations : Array[Relation] = []
      let point = copy_snapshot(
        config,
        slot="cdc_snapshot",
        publication="cdc_snapshot",
        batch_rows=1,
        event => {
          match event {
            TableBegin(relation) => {
              begins.push(relation.name)
              if relation.name == "rows'表" {
                assert_eq(relation.columns.length(), 2)
                assert_true(relation.columns[0].key)
                assert_true(!relation.columns[1].key)
                assert_eq(relation.columns[1].name, "v\"alue")
                relations.push(relation)
                ignore(
                  sql.query(
                    "BEGIN; INSERT INTO \{table} VALUES (4,'four','secret'); UPDATE \{table} SET \{column}='updated' WHERE id=1; DELETE FROM \{table} WHERE id=2; COMMIT; BEGIN; INSERT INTO \{table} VALUES (5,'rollback','secret'); ROLLBACK",
                  ),
                )
              }
            }
            TableRows(relation, rows) => {
              assert_eq(relation.name, "rows'表")
              assert_eq(rows.length(), 1)
              for row in rows {
                guard row is [Text(id), value] else {
                  fail("snapshot row shape")
                }
                assert_true(!state.contains(id))
                state[id] = value
              }
            }
            TableEnd(relation) => ends.push(relation.name)
          }
        },
      )
      assert_eq(begins, ["empty", "rows'表"])
      assert_eq(ends, begins)
      assert_eq(state.length(), 3)
      assert_eq(state.get("1"), Some(Text("one")))
      assert_eq(state.get("2"), Some(Text("two")))
      assert_eq(state.get("3"), Some(Null))
      assert_true(point.lsn.value > 0)
      let dir = @fs.tmpdir(prefix="mooncdc-snapshot-")
      defer @fs.rmdir(dir, recursive=true)
      let wrong = SnapshotPosition::{
        lsn: point.lsn,
        source: { ..point.source, system_id: "wrong", },
      }
      subscribe(
        config,
        slot="cdc_snapshot",
        publication="cdc_snapshot",
        start=AfterSnapshot(wrong),
        checkpoint_path=dir + "/wrong",
        _ => fail("wrong source accepted"),
      ) catch {
        UnsafeResume(_) => ()
        e => raise e
      }
      assert_true(!@fs.exists(dir + "/wrong"))
      subscribe(
        config,
        slot="cdc_snapshot",
        publication="cdc_snapshot",
        start=AfterSnapshot(point),
        checkpoint_path=dir + "/checkpoint",
        sub => {
          let tx = sub.next()
          let mut events = 0
          for change in tx.changes {
            match change {
              Insert(relation, [Text(id), value])
              | Update(relation, _, [Text(id), value]) => {
                assert_eq(relation, relations[0])
                state[id] = value
                events += 1
              }
              Delete(_, Key([Text(id), ..])) => {
                state.remove(id)
                events += 1
              }
              RelationChanged(_) => ()
              _ => fail("unexpected incremental event")
            }
          }
          assert_eq(events, 3)
          assert_eq(state.length(), 3)
          let expected = sql.query(
            "SELECT id::text, \{column} FROM \{table} WHERE id>0",
          )
          assert_eq(expected.length(), state.length())
          for row in expected {
            let value = match row[1] {
              Some(s) => Text(s)
              None => Null
            }
            assert_eq(state.get(cell([row], 0, 0)), Some(value))
          }
          sub.ack(tx)
          assert_eq(sub.diagnostics().durable, tx.end_lsn)
        },
      )
      drop_slot(sql, "cdc_snapshot")
      ignore(
        sql.query("DROP PUBLICATION cdc_snapshot; DROP SCHEMA \{ns} CASCADE"),
      )
      println(
        "LIVE: exported snapshot, projection/filter, empty table, quoted identifiers, concurrent I/U/D, rollback and source-bound WAL handoff passed",
      )
    })
  }
}

///|
async test "live snapshot failure leaves slot unadvanced and retry explicit" {
  if live_config() is Some(config) {
    @async.with_timeout(15000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE TABLE cdc_snapshot_failure(id int PRIMARY KEY, value text); INSERT INTO cdc_snapshot_failure VALUES(1,repeat('x',2000)); CREATE PUBLICATION cdc_snapshot_failure FOR TABLE cdc_snapshot_failure",
        ),
      )
      let mut delivered = false
      try
        copy_snapshot(
          config,
          slot="cdc_snapshot_failure",
          publication="cdc_snapshot_failure",
          event => {
            if event is TableRows(_, _) {
              delivered = true
              raise InvalidConfig("target write failed")
            }
          },
        )
      catch {
        InvalidConfig("target write failed") => ()
        e => raise e
      } noraise {
        _ => fail("failed copy returned a position")
      }
      assert_true(delivered)
      let before = sql.query(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='cdc_snapshot_failure'",
      )
      try
        copy_snapshot(
          config,
          slot="cdc_snapshot_failure",
          publication="cdc_snapshot_failure",
          _ => fail("existing slot copied"),
        )
      catch {
        Server("42710", _) => ()
        e => raise e
      } noraise {
        _ => fail("existing slot reused")
      }
      assert_eq(
        sql.query(
          "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='cdc_snapshot_failure'",
        ),
        before,
      )
      drop_slot(sql, "cdc_snapshot_failure")
      try
        copy_snapshot(
          config,
          slot="cdc_snapshot_failure",
          publication="cdc_snapshot_failure",
          max_frame_bytes=1024,
          max_batch_bytes=1024,
          _ => (),
        )
      catch {
        LimitExceeded(_) => ()
        e => raise e
      } noraise {
        _ => fail("oversized snapshot row accepted")
      }
      drop_slot(sql, "cdc_snapshot_failure")
      ignore(
        sql.query(
          "UPDATE cdc_snapshot_failure SET value=repeat('x',700); INSERT INTO cdc_snapshot_failure VALUES(2,repeat('y',700))",
        ),
      )
      try
        copy_snapshot(
          config,
          slot="cdc_snapshot_failure",
          publication="cdc_snapshot_failure",
          max_frame_bytes=1024,
          max_batch_bytes=1024,
          _ => (),
        )
      catch {
        LimitExceeded("query result bytes") => ()
        e => raise e
      } noraise {
        _ => fail("oversized batch accepted")
      }
      drop_slot(sql, "cdc_snapshot_failure")
      let mut begun = false
      try
        @async.with_timeout(2000, () => {
          ignore(
            copy_snapshot(
              config,
              slot="cdc_snapshot_failure",
              publication="cdc_snapshot_failure",
              event => {
                if event is TableBegin(_) {
                  begun = true
                  @async.sleep(10000)
                }
              },
            ),
          )
        })
      catch {
        _ => assert_true(begun)
      } noraise {
        _ => fail("copy was not cancelled")
      }
      // drop_slot waits for backend release, proving cancellation closed the exporter.
      drop_slot(sql, "cdc_snapshot_failure")
      ignore(
        sql.query(
          "DROP PUBLICATION cdc_snapshot_failure; DROP TABLE cdc_snapshot_failure",
        ),
      )
      println(
        "LIVE: failed consumer, existing-slot refusal, oversized row and cancellation cleanup passed",
      )
    })
  }
}

///|
async test "live snapshot partitions and ordinary inheritance match pgoutput" {
  if live_config() is Some(config) {
    @async.with_timeout(20000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE SCHEMA cdc_parts; CREATE TABLE cdc_parts.root(id int PRIMARY KEY, value text) PARTITION BY RANGE(id); CREATE TABLE cdc_parts.low PARTITION OF cdc_parts.root FOR VALUES FROM(0) TO(10); CREATE TABLE cdc_parts.high PARTITION OF cdc_parts.root FOR VALUES FROM(10) TO(20); CREATE TABLE cdc_parts.parent(id int PRIMARY KEY, value text); CREATE TABLE cdc_parts.child() INHERITS(cdc_parts.parent); INSERT INTO cdc_parts.root VALUES(1,'low'),(11,'high'); INSERT INTO cdc_parts.parent VALUES(2,'parent'); INSERT INTO cdc_parts.child VALUES(3,'excluded')",
        ),
      )
      let dir = @fs.tmpdir(prefix="mooncdc-parts-")
      defer @fs.rmdir(dir, recursive=true)
      for via_root in [false, true] {
        ignore(
          sql.query(
            "CREATE PUBLICATION cdc_parts FOR TABLE cdc_parts.root, cdc_parts.parent WITH (publish_via_partition_root=\{via_root})",
          ),
        )
        let relations : Map[UInt, Relation] = Map([])
        let ids : Array[String] = []
        let point = copy_snapshot(
          config,
          slot="cdc_parts",
          publication="cdc_parts",
          batch_rows=1,
          event => {
            match event {
              TableBegin(r) => relations[r.oid] = r
              TableRows(_, rows) =>
                for row in rows {
                  guard row is [Text(id), _] else { fail("row shape") }
                  ids.push(id)
                }
              TableEnd(_) => ()
            }
          },
        )
        assert_eq(ids.length(), 4)
        assert_true(
          ids.contains("1") && ids.contains("11") && ids.contains("2"),
        )
        assert_true(ids.contains("3"))
        assert_eq(relations.length(), if via_root { 3 } else { 4 })
        subscribe(
          config,
          slot="cdc_parts",
          publication="cdc_parts",
          start=AfterSnapshot(point),
          checkpoint_path=dir + (if via_root { "/root" } else { "/leaf" }),
          sub => {
            ignore(
              sql.query(
                "BEGIN; INSERT INTO cdc_parts.root VALUES(12,'new'); INSERT INTO cdc_parts.parent VALUES(4,'new'); INSERT INTO cdc_parts.child VALUES(5,'excluded'); COMMIT",
              ),
            )
            let tx = sub.next()
            let mut inserted = 0
            for change in tx.changes {
              if change is Insert(r, [Text(id), _]) {
                assert_eq(relations.get(r.oid), Some(r))
                assert_eq(
                  r.name,
                  if id == "12" {
                    if via_root {
                      "root"
                    } else {
                      "high"
                    }
                  } else if id == "4" {
                    "parent"
                  } else {
                    "child"
                  },
                )
                inserted += 1
              }
            }
            assert_eq(inserted, 3)
            sub.ack(tx)
          },
        )
        drop_slot(sql, "cdc_parts")
        ignore(
          sql.query(
            "DROP PUBLICATION cdc_parts; DELETE FROM cdc_parts.root WHERE id=12; DELETE FROM ONLY cdc_parts.parent WHERE id=4; DELETE FROM cdc_parts.child WHERE id=5",
          ),
        )
      }
      ignore(sql.query("DROP SCHEMA cdc_parts CASCADE"))
      println(
        "LIVE: partition root/leaf identities and ordinary inheritance agree with pgoutput",
      )
    })
  }
}
