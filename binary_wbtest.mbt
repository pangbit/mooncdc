///|
test "binary tuple fields preserve bytes empty values null and unchanged TOAST" {
  let d = Decoder::new(4096, 100, 10)
  ignore(d.decode(begin_fixture()))
  ignore(d.decode(relation_fixture("binary")))
  ignore(
    d.decode(tuple_fixture(b'I', b"N\x00\x02b\x00\x00\x00\x03\x00\xff\x80n")),
  )
  ignore(d.decode(tuple_fixture(b'U', b"N\x00\x02b\x00\x00\x00\x00u")))
  guard d.decode(commit_fixture()) is Some((_, _, _, _, changes)) else {
    fail("missing commit")
  }
  assert_true(changes[1] is Insert(_, [Binary(b"\x00\xff\x80"), Null]))
  assert_true(changes[2] is Update(_, None, [Binary(b""), UnchangedToast]))
}

///|
test "binary tuple lengths reject truncation negative length and trailing bytes" {
  let full = tuple_fixture(b'I', b"N\x00\x02b\x00\x00\x00\x03\x00\xff\x80n")
  let invalid = [
    full + b"x",
    tuple_fixture(b'I', b"N\x00\x02b\xff\xff\xff\xffn"),
  ]
  for data in invalid + [ for n in 0..<full.length() => full[:n].to_owned() ] {
    let d = Decoder::new(4096, 100, 10)
    ignore(d.decode(begin_fixture()))
    ignore(d.decode(relation_fixture("binary")))
    try d.decode(data) catch {
      Protocol(_) => ()
      e => raise e
    } noraise {
      _ => fail("invalid binary field accepted")
    }
  }
}

///|
async test "live binary pgoutput values and text default" {
  if live_config() is Some(config) {
    @async.with_timeout(15000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      ignore(
        sql.query(
          "CREATE TABLE cdc_binary(id int PRIMARY KEY, value bytea, enabled boolean); CREATE PUBLICATION cdc_binary FOR TABLE cdc_binary",
        ),
      )
      let dir = @fs.tmpdir(prefix="mooncdc-binary-")
      defer @fs.rmdir(dir, recursive=true)
      for binary in [false, true] {
        let start = fresh_slot(sql, "cdc_binary")
        let path = dir + (if binary { "/binary" } else { "/text" })
        subscribe(
          config,
          slot="cdc_binary",
          publication="cdc_binary",
          start=At(start),
          checkpoint_path=path,
          binary~,
          sub => {
            ignore(
              sql.query(
                "INSERT INTO cdc_binary VALUES (42,decode('00ff80','hex'),true)",
              ),
            )
            let tx = sub.next()
            let data = tx.changes.filter(c => c is Insert(_, _))
            assert_eq(data.length(), 1)
            if binary {
              assert_true(
                data[0]
                is Insert(
                  _,
                  [
                    Binary(b"\x00\x00\x00\x2a"),
                    Binary(b"\x00\xff\x80"),
                    Binary(b"\x01"),
                  ]
                ),
              )
            } else {
              assert_true(
                data[0] is Insert(_, [Text("42"), Text("\\x00ff80"), Text("t")]),
              )
            }
            sub.ack(tx)
            assert_eq(sub.diagnostics().durable, tx.end_lsn)
          },
        )
        drop_slot(sql, "cdc_binary")
        ignore(sql.query("TRUNCATE cdc_binary"))
      }
      ignore(sql.query("DROP PUBLICATION cdc_binary; DROP TABLE cdc_binary"))
      println(
        "LIVE: binary int4/bytea/bool bytes, text default and durable ack passed",
      )
    })
  }
}
