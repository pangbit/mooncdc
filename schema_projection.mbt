///|
pub(all) enum IdentityKind {
  Missing
  PrimaryKey
  FullRow
  AlternativeKey
} derive(Eq, Debug)

///|
/// Both masks have full-schema width. Identity is intersected with publication
/// membership, independently of catalog primary-key membership and ordering.
pub struct ReplicatedSchema {
  schema : SchemaSnapshot
  replicated : ReadOnlyArray[Bool]
  identity : ReadOnlyArray[Bool]
} derive(Eq, Debug)

///|
pub fn ReplicatedSchema::new(
  schema : SchemaSnapshot,
  replicated~ : ReadOnlyArray[Bool],
  identity~ : ReadOnlyArray[Bool],
) -> ReplicatedSchema raise {
  if replicated.length() != schema.columns.length() ||
    identity.length() != schema.columns.length() {
    raise Protocol("schema mask width mismatch")
  }
  for index, key in identity {
    if key && !replicated[index] {
      raise Protocol("identity column is not published")
    }
  }
  { schema, replicated, identity, }
}

///|
/// Build masks from the column membership and identity flags of a wire relation.
pub fn SchemaSnapshot::project(
  self : SchemaSnapshot,
  relation : Relation,
) -> ReplicatedSchema raise {
  if self.oid != relation.oid {
    raise Protocol("schema/relation table mismatch")
  }
  let names : Map[String, Bool] = Map([])
  for column in relation.columns {
    if names.contains(column.name) ||
      !self.columns.any(c => c.name == column.name) {
      raise Protocol("unknown or duplicate published column")
    }
    names[column.name] = column.key
  }
  ReplicatedSchema::new(
    self,
    replicated=self.columns.map(c => names.contains(c.name)),
    identity=self.columns.map(c => names.get(c.name).unwrap_or(false)),
  )
}

///|
/// Materialize a DDL snapshot using retained relation masks. A replacement wire
/// relation must be used if column count/membership changes.
pub fn ReplicatedSchema::with_schema(
  self : ReplicatedSchema,
  schema : SchemaSnapshot,
) -> ReplicatedSchema raise {
  if self.schema.oid != schema.oid {
    raise Protocol("schema table changed")
  }
  ReplicatedSchema::new(
    schema,
    replicated=self.replicated,
    identity=self.identity,
  )
}

///|
pub fn ReplicatedSchema::columns(
  self : ReplicatedSchema,
) -> ReadOnlyArray[SchemaColumn] {
  let columns : Array[SchemaColumn] = []
  for index, column in self.schema.columns {
    if self.replicated[index] {
      columns.push(column)
    }
  }
  ReadOnlyArray::from_array(columns)
}

///|
pub fn ReplicatedSchema::identity_kind(self : ReplicatedSchema) -> IdentityKind {
  let mut has_identity = false
  let mut primary = true
  let mut full = true
  for index, column in self.schema.columns {
    has_identity = has_identity || self.identity[index]
    primary = primary &&
      self.identity[index] ==
      (self.replicated[index] && column.primary_key_ordinal is Some(_))
    full = full && self.identity[index] == self.replicated[index]
  }
  if !has_identity {
    Missing
  } else if primary {
    PrimaryKey
  } else if full {
    FullRow
  } else {
    AlternativeKey
  }
}

///|
pub fn ReplicatedSchema::all_primary_keys_replicated(
  self : ReplicatedSchema,
) -> Bool {
  for index, column in self.schema.columns {
    if column.primary_key_ordinal is Some(_) && !self.replicated[index] {
      return false
    }
  }
  true
}

///|
pub fn ReplicatedSchema::decode_row(
  self : ReplicatedSchema,
  values : ReadOnlyArray[Value],
) -> ReadOnlyArray[Cell] raise {
  let columns = self.columns()
  if values.length() != columns.length() {
    raise Protocol("typed row width mismatch")
  }
  ReadOnlyArray::makei(values.length(), index => {
    values[index].decode_text(columns[index].type_oid)
  })
}
