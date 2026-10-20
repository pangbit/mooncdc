///|
test "table copy selection uses OIDs and rejects duplicate configuration" {
  for
    (policy, a, b) in [
      (IncludeAllTables, true, true),
      (SkipAllTables, false, false),
      (IncludeTables([1U]), true, false),
      (SkipTables([1U]), false, true),
      (IncludeTables([]), false, false),
      (SkipTables([]), true, true),
    ] {
    policy.validate()
    assert_eq(policy.should_copy_table(1U), a)
    assert_eq(policy.should_copy_table(2U), b)
  }
  for policy in [IncludeTables([1U, 1U]), SkipTables([2U, 2U])] {
    try policy.validate() catch {
      InvalidConfig(_) => ()
      error => raise error
    } noraise {
      _ => fail("duplicate copy table accepted")
    }
  }
}

///|
async test "live initial copy selection preserves schemas WAL and completed restart state" {
  if live_config() is Some(config) {
    @async.with_timeout(60000, () => {
      let sql = connect(config, 16777216, false)
      defer sql.close()
      let name = "copy_policy_" + pipeline_nonce()
      ignore(
        sql.query(
          "CREATE SCHEMA \{name}; CREATE TABLE \{name}.a(id int PRIMARY KEY,value text); CREATE TABLE \{name}.b(id int PRIMARY KEY,value text); CREATE PUBLICATION \{name} FOR TABLE \{name}.a,\{name}.b",
        ),
      )
      defer ignore(
        sql.query("DROP PUBLICATION \{name}; DROP SCHEMA \{name} CASCADE"),
      )
      let a = @string.parse_uint(
        cell(sql.query("SELECT '\{name}.a'::regclass::oid::text"), 0, 0),
      )
      for
        policy in [
          IncludeAllTables,
          SkipAllTables,
          IncludeTables([a]),
          SkipTables([a]),
        ] {
        ignore(
          sql.query(
            "TRUNCATE \{name}.a,\{name}.b; INSERT INTO \{name}.a VALUES(1,'old'); INSERT INTO \{name}.b VALUES(1,'old')",
          ),
        )
        let dir = @fs.tmpdir(prefix="mooncdc-copy-policy-")
        defer @fs.rmdir(dir, recursive=true)
        let path = dir + "/state"
        defer (if @fs.exists(path) {
          drop_slot(sql, pipeline_status(path).slot)
        })
        with_file_schema_store(dir + "/schemas", identity="copy-policy", schemas => {
          for sequence in [2, 3] {
            let control = ApplyControl()
            let target = PipelineProbe::new(
              dir + "/target",
              sql,
              control,
              sequence.to_string(),
              false,
              false,
              None,
              schema=name,
            )
            @async.with_task_group(group => {
              group.spawn_bg(() => {
                while !@fs.exists(path) ||
                      pipeline_status(path).tables.length() != 2 ||
                      !pipeline_status(path).tables.all(table => {
                        table.phase is (Ready(_) | Catchup(_))
                      }) {
                  @async.sleep(10)
                }
                ignore(
                  sql.query(
                    "BEGIN; INSERT INTO \{name}.a VALUES(\{sequence},'wal'); INSERT INTO \{name}.b VALUES(\{sequence},'wal'); COMMIT",
                  ),
                )
              })
              run_probe_pipeline(
                config,
                target,
                publication=name,
                destination_id="copy-policy",
                state_path=path,
                copy_connections_per_table=3,
                schema_store=schemas,
                table_sync_copy=if sequence == 2 {
                  policy
                } else {
                  IncludeAllTables
                },
              )
              group.return_immediately(())
            })
            for table in ["a", "b"] {
              let oid = @string.parse_uint(
                cell(
                  sql.query("SELECT '\{name}.\{table}'::regclass::oid::text"),
                  0,
                  0,
                ),
              )
              assert_eq(target.data.resets.get(table), Some(1))
              assert_eq(
                target.data.rows[table].contains("1"),
                policy.should_copy_table(oid),
              )
              assert_true(target.data.rows[table].contains("2"))
              if sequence == 3 {
                assert_true(target.data.rows[table].contains("3"))
              }
              assert_true(schemas.all().any(schema => schema.oid == oid))
            }
          }
        })
      }
      for fail_flush in [false, true] {
        let target = ProbeDestination::new(
          if fail_flush {
            "fail"
          } else {
            "accepted"
          },
          ApplyControl(),
        )
        let mut handed_over = false
        try
          ignore(
            copy_snapshot_to_impl(
              config,
              target,
              slot="skip_barrier_" + pipeline_nonce(),
              publication=name,
              batch_rows=1,
              max_frame_bytes=1048576,
              max_batch_bytes=1048576,
              temporary=true,
              table=Some(a),
              copy_rows=false,
              after_copy=(_, _) => handed_over = true,
            ),
          )
        catch {
          TestDestinationFailure => assert_true(fail_flush)
          error => raise error
        } noraise {
          _ => assert_false(fail_flush)
        }
        assert_eq(target.events, ["reset", "empty"])
        assert_eq(target.batches.length(), 0)
        assert_eq(target.flushes, 1)
        assert_eq(handed_over, !fail_flush)
      }
      println(
        "LIVE: all four copy policies, empty target/schema setup, later WAL and no recopy on restart passed",
      )
    })
  }
}
