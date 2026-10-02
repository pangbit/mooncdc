///|
priv struct PlanVector {
  before : Array[SchemaColumn]
  after : Array[SchemaColumn]
  before_mask : Array[Bool]
  after_mask : Array[Bool]
  lower : Bool
  result : Json?
} derive(ToJson)

///|
fn plan_schema(
  columns : Array[SchemaColumn],
  mask : Array[Bool],
) -> ReplicatedSchema raise {
  let source = ddl_snapshot(100, 50)
  let schema = { ..source, columns: ReadOnlyArray::from_array(columns), }
  ReplicatedSchema::new(
    schema,
    replicated=ReadOnlyArray::from_array(mask),
    identity=ReadOnlyArray::from_array(
      columns.mapi((i, c) => mask[i] && c.primary_key_ordinal is Some(_)),
    ),
  )
}

///|
fn normalize_operation(operation : SchemaOperation) -> Json {
  let reason_json = fn(reason : ColumnPresenceReason) -> Json {
    match reason {
      TableSchema => "table"
      Publication => "publication"
    }
  }
  match operation {
    DropColumn(column, reason~) =>
      ["drop", column.to_json(), reason_json(reason)]
    AddColumn(column, reason~) => ["add", column.to_json(), reason_json(reason)]
    AlterColumn(before~, after~, kind~) =>
      [
        "alter",
        before.to_json(),
        after.to_json(),
        match kind {
          Rename => "rename"
          DataType => "type"
          Nullability => "nullable"
          DefaultExpression => "default"
        },
      ]
  }
}

///|
fn plan_result(vector : PlanVector) -> Json {
  try {
    let before = plan_schema(vector.before, vector.before_mask)
    let after = plan_schema(vector.after, vector.after_mask)
    let plan = before.plan_change(after, map_name=name => {
      if vector.lower {
        name.to_lower()
      } else {
        name
      }
    })
    {
      "ok": true,
      "operations": plan.operations.map(normalize_operation).to_json(),
      "primary_key_changed": plan.primary_key_changed.to_json(),
      "cycles": plan.has_rename_cycles.to_json(),
    }
  } catch {
    _ => { "ok": false }
  }
}

///|
fn plan_vectors() -> Array[PlanVector] raise {
  let columns = ddl_snapshot(100, 50).columns.iter().collect()
  let vectors : Array[PlanVector] = []
  // Every pair of publication masks includes empty projections and PK omissions.
  for before in 0..<8 {
    for after in 0..<8 {
      vectors.push({
        before: columns,
        after: columns,
        before_mask: Array::makei(3, i => (before & (1 << i)) != 0),
        after_mask: Array::makei(3, i => (after & (1 << i)) != 0),
        lower: false,
        result: None,
      })
    }
  }
  // All permutations include rename chains and cycles, alongside metadata changes.
  for a in 0..<3 {
    for b in 0..<3 {
      for c in 0..<3 {
        if a == b || b == c || a == c {
          continue
        }
        let names = [columns[a].name, columns[b].name, columns[c].name]
        vectors.push({
          before: columns,
          after: columns.mapi((i, col) => {
            ..col,
            name: names[i],
            type_oid: 20U,
            nullable: !col.nullable,
            default_expression: Some("7"),
            primary_key_ordinal: None,
          }),
          before_mask: [true, true, true],
          after_mask: [true, true, true],
          lower: false,
          result: None,
        })
      }
    }
  }
  let replaced = [
    { ..columns[2], ordinal: 5, },
    { ..columns[1], ordinal: 4, name: "first", },
  ]
  vectors.push({
    before: columns,
    after: replaced,
    before_mask: [true, true, true],
    after_mask: [true, true],
    lower: false,
    result: None,
  })
  let collision = [
    { ..columns[0], name: "A", },
    { ..columns[1], name: "a", },
    columns[2],
  ]
  for lower in [false, true] {
    vectors.push({
      before: columns,
      after: collision,
      before_mask: [true, true, true],
      after_mask: [true, true, true],
      lower,
      result: None,
    })
    vectors.push({
      before: collision,
      after: columns,
      before_mask: [true, true, true],
      after_mask: [true, true, true],
      lower,
      result: None,
    })
  }
  let upper = columns.map(c => { ..c, name: c.name.to_upper(), })
  vectors.push({
    before: upper,
    after: columns,
    before_mask: [true, true, true],
    after_mask: [true, true, true],
    lower: true,
    result: None,
  })
  let reserved = columns +
    [{ ..columns[1], ordinal: 4, name: "supabase_etl_ddl_tmp_column_1_0", }]
  vectors.push({
    before: reserved,
    after: [
      { ..columns[0], name: columns[1].name, },
      { ..columns[1], name: columns[0].name, },
      columns[2],
      reserved[3],
    ],
    before_mask: [true, true, true, true],
    after_mask: [true, true, true, true],
    lower: false,
    result: None,
  })
  vectors
}

///|
test "schema plans execute without name collisions and alter one field at a time" {
  for vector in plan_vectors() {
    let expected = plan_result(vector)
    if expected is { "ok": false, .. } {
      assert_true(vector.lower)
      continue
    }
    let before = plan_schema(vector.before, vector.before_mask)
    let after = plan_schema(vector.after, vector.after_mask)
    let mapping = fn(name : String) -> String {
      if vector.lower {
        name.to_lower()
      } else {
        name
      }
    }
    let plan = before.plan_change(after, map_name=mapping)
    let active : Map[Int, SchemaColumn] = Map([])
    for column in mapped_columns(before, mapping) {
      active[column.ordinal] = column
    }
    for operation in plan.operations {
      match operation {
        DropColumn(column, ..) => {
          assert_eq(active.get(column.ordinal), Some(column))
          active.remove(column.ordinal)
        }
        AddColumn(column, ..) => {
          assert_false(active.values().any(c => c.name == column.name))
          active[column.ordinal] = column
        }
        AlterColumn(before~, after~, kind~) => {
          assert_eq(active.get(before.ordinal), Some(before))
          if kind == Rename {
            assert_false(active.values().any(c => c.name == after.name))
          }
          let only_changed = match kind {
            Rename => { ..before, name: after.name, }
            DataType =>
              {
                ..before,
                type_oid: after.type_oid,
                type_modifier: after.type_modifier,
              }
            Nullability => { ..before, nullable: after.nullable, }
            DefaultExpression =>
              { ..before, default_expression: after.default_expression, }
          }
          assert_eq(after, only_changed)
          active[after.ordinal] = after
        }
      }
    }
    assert_eq(active.length(), after.columns().length())
    for column in mapped_columns(after, mapping) {
      let actual = active[column.ordinal]
      // PK membership is source metadata, reported separately from executable DDL.
      assert_eq(
        { ..actual, primary_key_ordinal: column.primary_key_ordinal, },
        column,
      )
    }
  }
}

///|
test "schema projection keeps relation identity masks across metadata-only DDL" {
  let source = ddl_snapshot(100, 50)
  let primary = source.project(source.relation(["first", "third"]))
  assert_eq(primary.identity_kind(), PrimaryKey)
  assert_true(primary.all_primary_keys_replicated())
  assert_eq(primary.decode_row([Text("1"), Text("2")]), [
    Cell::I32(1),
    Cell::I64(2),
  ])
  let omitted = source.project(source.relation(["first", "second"]))
  assert_false(omitted.all_primary_keys_replicated())
  let changed = {
    ..source,
    columns: source.columns.map(c => { ..c, primary_key_ordinal: None, }),
  }
  let fallback = omitted.with_schema(changed)
  assert_eq(fallback.identity_kind(), AlternativeKey)
  assert_eq(fallback.identity, omitted.identity)
  assert_true(omitted.plan_change(fallback).primary_key_changed)
  assert_true(omitted.plan_change(fallback).operations.is_empty())
  try ReplicatedSchema::new(source, replicated=[true], identity=[true]) catch {
    Protocol(_) => ()
    e => raise e
  } noraise {
    _ => fail("bad mask accepted")
  }
}

///|
async test "schema reference process worker" {
  if @env.get_env_var("MOONCDC_SCHEMA_PHASE") is Some(phase) {
    let path = @env.get_env_var("MOONCDC_SCHEMA_FILE").unwrap()
    let vectors = plan_vectors()
    if phase == "generate" {
      @fs.write_file(path, vectors.to_json().stringify())
      println("SCHEMA generated vectors")
    } else {
      guard @json.parse(@fs.read_file(path).text()) is Array(expected) else {
        fail("schema comparison array")
      }
      assert_eq(expected.length(), vectors.length())
      for index, vector in vectors {
        guard expected[index] is { "result": result, .. } else {
          fail("missing schema reference result")
        }
        let actual = plan_result(vector)
        if actual != result {
          println(
            "SCHEMA mismatch index=\{index} actual=\{actual.stringify()} reference=\{result.stringify()}",
          )
        }
        assert_eq(actual, result)
      }
      println("SCHEMA matched \{vectors.length()} upstream vectors")
    }
  }
}
