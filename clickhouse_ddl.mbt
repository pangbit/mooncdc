///|
fn ch_add_definition(
  column : SchemaColumn,
  reason : ColumnPresenceReason,
) -> String raise {
  if reason == Publication && array_element(column.type_oid) is Some(_) {
    raise Manual(
      "ClickHouse publication array expansion requires resynchronization",
    )
  }
  let clause = if reason == Publication {
    ""
  } else {
    ch_default_clause(column)
  }
  ch_quote(column.name, '"') +
  " " +
  ch_type(
    column.type_oid,
    column.nullable,
    reason == Publication || clause.is_empty(),
  ) +
  clause
}

///|
fn ch_nested_parent(name : String) -> String? {
  let components = name.split(".").to_array()
  if components.length() == 1 {
    None
  } else {
    Some(components[:components.length() - 1].to_owned().join("."))
  }
}

///|
fn ch_validate_plan(
  before : ReplicatedSchema,
  after : ReplicatedSchema,
  engine : ClickHouseEngine,
) -> SchemaPlan raise {
  ch_validate_schema(after, engine)
  let plan = before.plan_change(after)
  if engine == ReplacingMergeTree && plan.primary_key_changed {
    raise Manual(
      "ClickHouse ReplacingMergeTree primary key change requires resynchronization",
    )
  }
  for operation in plan.operations {
    match operation {
      AddColumn(column, reason~) => ignore(ch_add_definition(column, reason))
      AlterColumn(before~, after~, kind=Rename) => {
        if ch_nested_parent(before.name) != ch_nested_parent(after.name) {
          raise Manual("ClickHouse rename cannot change a nested parent")
        }
        if engine == ReplacingMergeTree && before.primary_key_ordinal is Some(_) {
          raise Manual(
            "ClickHouse ReplacingMergeTree primary key rename requires resynchronization",
          )
        }
      }
      AlterColumn(before~, after~, kind=DataType) =>
        if ch_type(before.type_oid, false, false) !=
          ch_type(after.type_oid, false, false) {
          raise Manual(
            "ClickHouse mapped type change requires resynchronization",
          )
        }
      _ => ()
    }
  }
  plan
}

///|
// A target endpoint permits only the idempotent metadata suffix. Column-name
// reuse and intermediate prefixes cannot prove which logical values survived.
fn ch_recover_at_target(
  before : ReplicatedSchema,
  after : ReplicatedSchema,
  actual : Array[String],
) -> Bool raise {
  let plan = before.plan_change(after)
  let structural = plan.operations.any(op => {
    match op {
      DropColumn(_, ..) | AddColumn(_, ..) | AlterColumn(kind=Rename, ..) =>
        true
      _ => false
    }
  })
  if structural &&
    after
    .columns()
    .any(column => {
      before.schema.columns.any(old => {
        old.name == column.name && old.ordinal != column.ordinal
      })
    }) {
    raise Manual("ClickHouse schema recovery has ambiguous column name reuse")
  }
  let old_names = before.columns().map(c => c.name).iter().collect()
  let new_names = after.columns().map(c => c.name).iter().collect()
  if actual == new_names {
    true
  } else if actual == old_names {
    false
  } else {
    raise Manual("ClickHouse schema recovery found a partial DDL state")
  }
}

///|
async fn ClickHouseDestination::apply_plan(
  self : ClickHouseDestination,
  name : String,
  before : ReplicatedSchema,
  after : ReplicatedSchema,
  at_target : Bool,
) -> Unit {
  let plan = ch_validate_plan(before, after, self.engine)
  let names = (if at_target { after } else { before })
    .columns()
    .map(c => c.name)
    .iter()
    .collect()
  let positions : Map[String, Int] = Map([])
  for i, column in after.columns() {
    positions[column.name] = i
  }
  for operation in plan.operations {
    let statement = match operation {
      DropColumn(column, ..) if !at_target => {
        names.retain(n => n != column.name)
        Some("DROP COLUMN " + ch_quote(column.name, '"'))
      }
      AddColumn(column, reason~) if !at_target => {
        let mut position = names.length()
        for i, name in names {
          if positions.get(name) is Some(index) &&
            index > positions[column.name] {
            position = i
            break
          }
        }
        let anchor = if position == 0 {
          " FIRST"
        } else {
          " AFTER " + ch_quote(names[position - 1], '"')
        }
        names.insert(position, column.name)
        Some("ADD COLUMN " + ch_add_definition(column, reason) + anchor)
      }
      AlterColumn(before~, after~, kind=Rename) if !at_target => {
        for i, name in names {
          if name == before.name {
            names[i] = after.name
          }
        }
        Some(
          "RENAME COLUMN " +
          ch_quote(before.name, '"') +
          " TO " +
          ch_quote(after.name, '"'),
        )
      }
      AlterColumn(before~, after~, kind=Nullability) if !before.nullable &&
        after.nullable &&
        array_element(after.type_oid) is None =>
        Some(
          "MODIFY COLUMN " +
          ch_quote(after.name, '"') +
          " " +
          ch_type(after.type_oid, true, false),
        )
      // Defaults fill historical rows with their add-time value. Replacing a
      // default would rewrite history; tightening nullability is also skipped.
      _ => None
    }
    if statement is Some(statement) {
      ignore(
        self.http.query("ALTER TABLE " + self.qualified(name) + " " + statement),
      )
    }
  }
  self.create_view(after, name)
}

///|
fn ClickHouseDestination::record_name(
  self : ClickHouseDestination,
  record : ChStoredTable,
) -> String raise {
  match record.table_name {
    Some(name) => name
    None => {
      let schema = record.projected().schema
      ch_table_name(schema.namespace_, schema.name, self.engine)
    }
  }
}

///|
async fn ClickHouseDestination::prepare_schema(
  self : ClickHouseDestination,
  incoming : ReplicatedSchema,
) -> ChTable {
  let oid = incoming.schema.oid
  // Pending operations never have a cache entry, including after cancellation.
  if self.tables.get(oid) is Some(table) && table.schema == incoming {
    return table
  }
  guard self.stored_record(oid) is Some(record) else {
    raise UnsafeResume("ClickHouse table has no durable copy metadata")
  }
  let stored = record.projected()
  let name = self.record_name(record)
  if record.creating == Some(true) {
    if incoming != stored || record.previous is Some(_) {
      raise Manual("ClickHouse pending creation does not match incoming schema")
    }
    self.create_table(incoming, name, if_missing=true)
  } else if record.previous is Some(json) {
    if incoming != stored {
      raise Manual("ClickHouse pending DDL does not match incoming schema")
    }
    let previous : ChStoredTable = @json.from_json(json)
    if previous.previous is Some(_) ||
      previous.creating == Some(true) ||
      self.record_name(previous) != name ||
      previous.endpoint != record.endpoint ||
      previous.database != record.database ||
      previous.publication != record.publication ||
      previous.engine != record.engine ||
      previous.version != record.version {
      raise UnsafeResume("invalid ClickHouse pending DDL endpoints")
    }
    let before = previous.projected()
    ignore(ch_validate_plan(before, incoming, self.engine))
    let columns = self.read_columns(name)
    let metadata = ch_metadata_columns(self.engine)
    if columns.length() < metadata.length() {
      raise Manual("ClickHouse schema recovery is missing CDC columns")
    }
    let user_count = columns.length() - metadata.length()
    for i, column in metadata {
      if columns[user_count + i] != column {
        raise Manual("ClickHouse schema recovery has invalid CDC columns")
      }
    }
    let at_target = ch_recover_at_target(
      before,
      incoming,
      columns[:user_count].to_owned().map(c => c.0),
    )
    ignore(self.load_layout(if at_target { incoming } else { before }, name))
    self.apply_plan(name, before, incoming, at_target)
  } else {
    if incoming.schema.id < stored.schema.id ||
      (
        incoming.schema.id == stored.schema.id &&
        incoming.replicated != stored.replicated
      ) {
      raise Manual(
        "ClickHouse schema rewind or unordered publication mask change requires resynchronization",
      )
    }
    if incoming.schema.id == stored.schema.id &&
      incoming.schema != stored.schema {
      raise UnsafeResume(
        "ClickHouse schema content changed at the same version",
      )
    }
    ignore(ch_validate_plan(stored, incoming, self.engine))
    let previous = self.load_layout(stored, name)
    if stored != incoming {
      self.tables.remove(oid)
      let pending = {
        ..self.table_record({ ..previous, schema: incoming, }),
        previous: Some(ToJson::to_json(record)),
      }
      self.state().put_metadata(oid, ToJson::to_json(pending))
      self.apply_plan(name, stored, incoming, false)
    }
  }
  let table = self.load_layout(incoming, name)
  self.save_table(table)
  table
}
