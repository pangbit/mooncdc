///|
priv struct ChStoredTable {
  version : Int
  endpoint : String
  database : String
  publication : String
  engine : String
  schema : Json
  replicated : Array[Bool]
  identity : Array[Bool]
  table_name : String?
  previous : Json?
  creating : Bool?
} derive(ToJson, @json.FromJson)

///|
fn ch_engine_name(engine : ClickHouseEngine) -> String {
  match engine {
    ReplacingMergeTree => "ReplacingMergeTree"
    MergeTree => "MergeTree"
  }
}

///|
async fn ClickHouseDestination::stored_record(
  self : ClickHouseDestination,
  oid : UInt,
) -> ChStoredTable? {
  guard self.state().get_metadata(oid) is Some(json) else { return None }
  let record : ChStoredTable = @json.from_json(json) catch {
    _ => raise UnsafeResume("invalid ClickHouse metadata")
  }
  if record.version != 1 ||
    record.endpoint != self.http.endpoint ||
    record.database != self.database ||
    record.publication != self.publication ||
    record.engine != ch_engine_name(self.engine) {
    raise UnsafeResume("ClickHouse metadata target identity changed")
  }
  let schema = SchemaSnapshot::from_json(record.schema)
  if schema.oid != oid {
    raise UnsafeResume("ClickHouse metadata table identity changed")
  }
  Some(record)
}

///|
fn ChStoredTable::projected(self : ChStoredTable) -> ReplicatedSchema raise {
  ReplicatedSchema::new(
    SchemaSnapshot::from_json(self.schema),
    replicated=ReadOnlyArray::from_array(self.replicated),
    identity=ReadOnlyArray::from_array(self.identity),
  )
}

///|
fn ClickHouseDestination::table_record(
  self : ClickHouseDestination,
  table : ChTable,
) -> ChStoredTable {
  self.schema_record(table.schema, table.name)
}

///|
fn ClickHouseDestination::schema_record(
  self : ClickHouseDestination,
  schema : ReplicatedSchema,
  name : String,
) -> ChStoredTable {
  {
    version: 1,
    endpoint: self.http.endpoint,
    database: self.database,
    publication: self.publication,
    engine: ch_engine_name(self.engine),
    schema: schema.schema.to_json(),
    replicated: schema.replicated.iter().collect(),
    identity: schema.identity.iter().collect(),
    table_name: Some(name),
    previous: None,
    creating: None,
  }
}

///|
async fn ClickHouseDestination::save_table(
  self : ClickHouseDestination,
  table : ChTable,
) -> Unit {
  let record = self.table_record(table)
  self.state().put_metadata(table.schema.schema.oid, ToJson::to_json(record))
  self.tables[table.schema.schema.oid] = table
}

///|
async fn ClickHouseDestination::read_columns(
  self : ClickHouseDestination,
  name : String,
) -> Array[(String, String)] {
  let query = "SELECT name,type FROM system.columns WHERE database=" +
    ch_quote(self.database, '\'') +
    " AND table=" +
    ch_quote(name, '\'') +
    " AND default_kind IN ('','DEFAULT') ORDER BY position FORMAT JSONCompactEachRow"
  let response = self.http.query(query)
  let actual : Array[(String, String)] = []
  for line in response.split("\n") {
    if line.is_empty() {
      continue
    }
    guard @json.parse(line) is [String(name), String(typ)] else {
      raise Protocol("invalid ClickHouse column metadata")
    }
    actual.push((name, typ))
  }
  actual
}

///|
async fn ClickHouseDestination::load_layout(
  self : ClickHouseDestination,
  schema : ReplicatedSchema,
  name : String,
) -> ChTable {
  ch_validate_schema(schema, self.engine)
  let actual = self.read_columns(name)
  let expected = schema
    .columns()
    .iter()
    .map(c => (c.name, ch_type(c.type_oid, false, false)))
    .collect()
  expected.append(ch_metadata_columns(self.engine))
  if actual.length() != expected.length() {
    raise Manual("ClickHouse physical column count differs from schema")
  }
  for i, (name, typ) in actual {
    let scalar = if typ.has_prefix("Nullable(") && typ.has_suffix(")") {
      typ[9:typ.length() - 1].to_owned()
    } else {
      typ
    }
    if name != expected[i].0 ||
      scalar != expected[i].1 ||
      (i >= schema.columns().length() && typ != expected[i].1) {
      raise Manual(
        "ClickHouse physical column name, order or type differs from schema",
      )
    }
  }
  let engine = self.http.query(
    "SELECT engine FROM system.tables WHERE database=" +
    ch_quote(self.database, '\'') +
    " AND name=" +
    ch_quote(name, '\''),
  )
  if engine.trim() != ch_engine_name(self.engine) {
    raise Manual("ClickHouse table engine differs from configuration")
  }
  { schema, name, layout: ChLayout::new(ReadOnlyArray::from_array(actual)), }
}

///|
async fn ClickHouseDestination::create_table(
  self : ClickHouseDestination,
  schema : ReplicatedSchema,
  name : String,
  if_missing? : Bool = false,
) -> Unit {
  ch_validate_schema(schema, self.engine)
  let columns = schema.columns()
  let definitions = columns
    .iter()
    .map(c => {
      ch_quote(c.name, '"') +
      " " +
      ch_type(c.type_oid, c.nullable, false) +
      ch_default_clause(c)
    })
    .collect()
  for (name, typ) in ch_metadata_columns(self.engine) {
    definitions.push(ch_quote(name, '"') + " " + typ)
  }
  let keys = columns
    .filter(c => c.primary_key_ordinal is Some(_))
    .iter()
    .collect()
  keys.sort_by((a, b) => {
    a.primary_key_ordinal.unwrap().compare(b.primary_key_ordinal.unwrap())
  })
  let engine = match self.engine {
    ReplacingMergeTree =>
      "ReplacingMergeTree(_etl_version, _etl_deleted) ORDER BY (" +
      keys.map(c => ch_quote(c.name, '"')).join(", ") +
      ")"
    MergeTree => "MergeTree() ORDER BY tuple()"
  }
  ignore(
    self.http.query(
      (if if_missing { "CREATE TABLE IF NOT EXISTS " } else { "CREATE TABLE " }) +
      self.qualified(name) +
      " (" +
      definitions.join(", ") +
      ") ENGINE=" +
      engine,
    ),
  )
  self.create_view(schema, name)
}

///|
async fn ClickHouseDestination::create_view(
  self : ClickHouseDestination,
  schema : ReplicatedSchema,
  name : String,
) -> Unit {
  if self.engine == ReplacingMergeTree {
    let select = schema
      .columns()
      .iter()
      .map(c => ch_quote(c.name, '"'))
      .collect()
      .join(", ")
    ignore(
      self.http.query(
        "CREATE OR REPLACE VIEW " +
        self.qualified(name + "__current") +
        " AS SELECT " +
        select +
        " FROM " +
        self.qualified(name) +
        " FINAL WHERE _etl_deleted=0",
      ),
    )
  }
}

///|
async fn ClickHouseDestination::resolve_table(
  self : ClickHouseDestination,
  relation : Relation,
  commit : Lsn,
  current : Map[UInt, SchemaSnapshot],
) -> ChTable {
  let source = match current.get(relation.oid) {
    Some(schema) => schema
    None => {
      guard self.schemas.get(relation.oid, SchemaId::before_lsn(commit))
        is Some(schema) else {
        raise UnsafeResume("ClickHouse has no schema before transaction")
      }
      schema
    }
  }
  if relation.namespace_ != source.namespace_ || relation.name != source.name {
    raise Manual("ClickHouse relation name differs from versioned schema")
  }
  let projected = source.project(relation)
  let columns = projected.columns()
  for i, column in relation.columns {
    if columns[i].name != column.name ||
      columns[i].type_oid != column.type_oid ||
      columns[i].type_modifier != column.type_modifier {
      raise Manual(
        "ClickHouse relation types or ordering differ from versioned schema",
      )
    }
  }
  self.prepare_schema(projected)
}
