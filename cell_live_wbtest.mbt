///|
fn assert_typed_row(
  relation : Relation,
  row : ReadOnlyArray[Value],
  enum_array : UInt,
  enum_scalar : UInt,
) -> Unit raise {
  let values : Map[String, Cell] = Map([])
  for i, column in relation.columns {
    values[column.name] = row[i].decode_text(
      column.type_oid,
      array_element_oid?=if column.type_oid == enum_array {
        Some(enum_scalar)
      } else {
        None
      },
    )
  }
  assert_eq(values["b"], Cell::Bool(true))
  assert_eq(values["i2"], Cell::I16(Int16::from_int(-32768)))
  assert_eq(values["i4"], Cell::I32(2147483647))
  assert_eq(values["i8"], Cell::I64(9223372036854775807L))
  assert_eq(values["o"], Cell::U32(4294967295U))
  assert_eq(values["f4"], Cell::F32(1.25))
  assert_true(values["f8"] is F64(value) && value.is_inf())
  assert_eq(values["n"], Cell::Numeric("12345678901234567890.000000000001"))
  assert_eq(values["bytes"], Cell::Bytes(b"\x00\xff"))
  assert_eq(values["d"], Cell::Date(Finite(year=0, month=1, day=1)))
  assert_eq(values["t"], Cell::Time({ seconds: 86400, nanoseconds: 0, }))
  assert_eq(
    values["tz"],
    Cell::TimeTz({ seconds: 86400, nanoseconds: 0, }, offset_seconds=-57599),
  )
  assert_eq(
    values["ts"],
    Cell::Timestamp(
      Finite(date=Finite(year=12000, month=2, day=29), time={
        seconds: 0,
        nanoseconds: 0,
      }),
    ),
  )
  assert_eq(
    values["tstz"],
    Cell::TimestampTz(
      Finite(date=Finite(year=2024, month=3, day=1), time={
        seconds: 5399,
        nanoseconds: 123456000,
      }),
    ),
  )
  assert_true(values["u"] is Uuid(bytes) && bytes.length() == 16)
  assert_eq(values["j"], Cell::Json(Json::null()))
  assert_eq(values["jb"], Cell::Json(Json::null()))
  assert_eq(values["ints"], Cell::Array([I64(9223372036854775807L), SqlNull]))
  assert_eq(
    values["texts"],
    Cell::Array([
      String("NULL"),
      SqlNull,
      String("a,b"),
      String("汉字"),
      String("\\"),
    ]),
  )
  assert_eq(
    values["numerics"],
    Cell::Array([Numeric("NaN"), Numeric("-Infinity"), Numeric("1.00")]),
  )
  assert_eq(values["byte_arrays"], Cell::Array([Bytes(b"\x00\xff"), SqlNull]))
  assert_eq(values["mood"], Cell::String("happy"))
  assert_eq(values["moods"], Cell::Array([String("happy"), SqlNull]))
  assert_eq(values["nothing"], Cell::SqlNull)
}

///|
async test "live typed cells agree between snapshot and WAL with stable session output" {
  if live_config() is Some(config) {
    @async.with_timeout(20000, () => {
      let sql = connect(config, 16777216, false)
      defer sql.close()
      assert_eq(cell(sql.query("SHOW TimeZone"), 0, 0), "UTC")
      assert_eq(cell(sql.query("SHOW DateStyle"), 0, 0), "ISO, YMD")
      assert_eq(cell(sql.query("SHOW bytea_output"), 0, 0), "hex")
      ignore(
        sql.query(
          "CREATE SCHEMA cdc_cells; CREATE TYPE cdc_cells.mood AS ENUM('happy','sad'); CREATE TABLE cdc_cells.items(id int PRIMARY KEY,b bool,i2 int2,i4 int4,i8 int8,o oid,f4 float4,f8 float8,n numeric,bytes bytea,d date,t time,tz timetz,ts timestamp,tstz timestamptz,u uuid,j json,jb jsonb,ints int8[],texts text[],numerics numeric[],byte_arrays bytea[],mood cdc_cells.mood,moods cdc_cells.mood[],nothing text); CREATE PUBLICATION cdc_cells FOR TABLE cdc_cells.items",
        ),
      )
      defer {
        drop_slot(sql, "cdc_cells")
        ignore(
          sql.query("DROP PUBLICATION cdc_cells; DROP SCHEMA cdc_cells CASCADE"),
        )
      }
      let insert =
        #|INSERT INTO cdc_cells.items VALUES(
        #|1,true,-32768,2147483647,9223372036854775807,4294967295,1.25,'Infinity',12345678901234567890.000000000001,decode('00ff','hex'),
        #|'0001-01-01 BC','24:00:00','24:00:00-15:59:59','12000-02-29 00:00:00','2024-02-29 23:59:59.123456-01:30',
        #|'550e8400-e29b-41d4-a716-446655440000','null','null',ARRAY[9223372036854775807,NULL]::int8[],ARRAY['NULL',NULL,'a,b','汉字',E'\\'],
        #|ARRAY['NaN','-Infinity','1.00']::numeric[],ARRAY[decode('00ff','hex'),NULL], 'happy',ARRAY['happy',NULL]::cdc_cells.mood[],NULL)
      ignore(sql.query(insert))
      let types = sql.query(
        "SELECT oid::text,typarray::text FROM pg_type WHERE oid='cdc_cells.mood'::regtype",
      )
      let scalar = @string.parse_uint(cell(types, 0, 0))
      let array = @string.parse_uint(cell(types, 0, 1))
      let mut copied = 0
      let point = copy_snapshot(
        config,
        slot="cdc_cells",
        publication="cdc_cells",
        event => {
          if event is TableRows(relation, rows) {
            for row in rows {
              assert_typed_row(relation, row, array, scalar)
              copied += 1
            }
          }
        },
      )
      assert_eq(copied, 1)
      let dir = @fs.tmpdir(prefix="mooncdc-cells-")
      defer @fs.rmdir(dir, recursive=true)
      subscribe(
        config,
        slot="cdc_cells",
        publication="cdc_cells",
        start=AfterSnapshot(point),
        checkpoint_path=dir + "/checkpoint",
        sub => {
          ignore(
            sql.query(
              "SET DateStyle='German, DMY'; SET TimeZone='Pacific/Auckland'; SET bytea_output='escape'; UPDATE cdc_cells.items SET id=2",
            ),
          )
          let tx = sub.next()
          let mut updates = 0
          for change in tx.changes {
            if change is Update(relation, _, row) {
              assert_typed_row(relation, row, array, scalar)
              updates += 1
            }
          }
          assert_eq(updates, 1)
          sub.ack(tx)
        },
      )
      println(
        "LIVE: typed scalar/array fidelity across snapshot and WAL, BC/infinity/24h/UTC and stable output settings passed",
      )
    })
  }
}
