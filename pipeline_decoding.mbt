///|
// Compact handover payload. The full snapshot belongs to SchemaStore.
priv struct StoredDecoding {
  commit_lsn : String
  message_lsn : String
  replicated : Array[Bool]
  identity : Array[Bool]
} derive(ToJson, @json.FromJson)

///|
fn StoredDecoding::capture(schema : ReplicatedSchema) -> StoredDecoding {
  {
    commit_lsn: schema.schema.id.commit_lsn.to_string(),
    message_lsn: schema.schema.id.message_lsn.to_string(),
    replicated: schema.replicated.iter().collect(),
    identity: schema.identity.iter().collect(),
  }
}

///|
fn StoredDecoding::snapshot_id(self : StoredDecoding) -> SchemaId raise {
  {
    commit_lsn: Lsn::parse(self.commit_lsn),
    message_lsn: Lsn::parse(self.message_lsn),
  }
}

///|
async fn StoredDecoding::restore(
  self : StoredDecoding,
  schemas : &SchemaStore,
  oid : UInt,
  cutoff : Lsn,
) -> ReplicatedSchema {
  let id = self.snapshot_id()
  // Initial-copy schemas use (snapshot LSN, 0); WAL schemas precede end_lsn.
  let frontier : SchemaId = { commit_lsn: cutoff, message_lsn: { value: 0, }, }
  if id > frontier {
    raise CheckpointError("decoding snapshot exceeds handover")
  }
  guard schemas.get(oid, id) is Some(schema) && schema.id == id else {
    raise CheckpointError("exact handover schema is missing")
  }
  ReplicatedSchema::new(
    schema,
    replicated=ReadOnlyArray::from_array(self.replicated),
    identity=ReadOnlyArray::from_array(self.identity),
  ) catch {
    error => raise CheckpointError("invalid handover masks: \{error}")
  }
}

///|
priv enum TableDecoding {
  Complete(ReplicatedSchema)
  Pending(SchemaSnapshot, ReplicatedSchema?)
}

///|
priv struct PipelineDecoding {
  schemas : &SchemaStore
  bootstrap : SchemaId
  tables : Map[UInt, TableDecoding]
}

///|
fn PipelineDecoding::new(
  schemas : &SchemaStore,
  bootstrap : Lsn,
) -> PipelineDecoding raise {
  { schemas, bootstrap: SchemaId::before_lsn(bootstrap), tables: Map([]), }
}

///|
async fn PipelineDecoding::handover_schema(
  self : PipelineDecoding,
  table : StoredTable?,
  position : Lsn,
) -> ReplicatedSchema? {
  guard table is Some(table) && table.decoding is Some(decoding) else {
    return None
  }
  match table.phase {
    Catchup(lsn) | Ready(lsn) if Lsn::parse(lsn) <= position =>
      Some(decoding.restore(self.schemas, table.oid, Lsn::parse(lsn)))
    _ => None
  }
}

///|
async fn PipelineDecoding::relation(
  self : PipelineDecoding,
  relation : Relation,
  table : StoredTable?,
  position : Lsn,
) -> ReplicatedSchema {
  let schema = match self.tables.get(relation.oid) {
    Some(Pending(schema, _)) => schema
    Some(Complete(schema)) => schema.schema
    None => {
      let mut bound = self.bootstrap
      if table is Some(table) {
        match table.phase {
          Catchup(lsn) | Ready(lsn) if Lsn::parse(lsn) <= position => {
            let cutoff = Lsn::parse(lsn)
            let handover : SchemaId = {
              commit_lsn: cutoff,
              message_lsn: { value: 0, },
            }
            if handover > bound {
              bound = handover
            }
          }
          _ => ()
        }
      }
      guard self.schemas.get(relation.oid, bound) is Some(schema) else {
        raise UnsafeResume("relation has no schema at connection bootstrap")
      }
      schema
    }
  }
  let projected = schema.project(relation)
  let columns = projected.columns()
  if relation.namespace_ != schema.namespace_ || relation.name != schema.name {
    raise Protocol("relation name differs from selected decoding schema")
  }
  for index, column in relation.columns {
    if columns[index].name != column.name ||
      columns[index].type_oid != column.type_oid ||
      columns[index].type_modifier != column.type_modifier {
      raise Protocol("relation columns differ from selected decoding schema")
    }
  }
  self.tables[relation.oid] = Complete(projected)
  projected
}

///|
async fn PipelineDecoding::row(
  self : PipelineDecoding,
  oid : UInt,
  table : StoredTable?,
  position : Lsn,
) -> ReplicatedSchema {
  let schema = match self.tables.get(oid) {
    Some(Complete(schema)) => schema
    Some(Pending(schema, previous)) => {
      let fallback = match previous {
        Some(value) => Some(value)
        None => self.handover_schema(table, position)
      }
      guard fallback is Some(previous) else {
        raise UnsafeResume("DDL row has no relation or handover masks")
      }
      previous.with_schema(schema)
    }
    None => {
      guard self.handover_schema(table, position) is Some(schema) else {
        raise UnsafeResume("row has no connection or handover decoder")
      }
      schema
    }
  }
  self.tables[oid] = Complete(schema)
  schema
}

///|
// Runs after ownership projection. Skipped tables never establish local state.
// Each delivered event retains its exact schema even if later events change it.
async fn PipelineDecoding::transaction(
  self : PipelineDecoding,
  tx : Transaction,
  stored : Array[StoredTable],
  publication : String,
) -> Transaction {
  let tables : Map[UInt, StoredTable] = Map([])
  for table in stored {
    tables[table.oid] = table
  }
  let resolved : Map[Int, ReadOnlyArray[ReplicatedSchema]] = Map([])
  for index, change in tx.changes {
    match change {
      Message(message) =>
        if message.schema_snapshot(commit_lsn=tx.commit_lsn, publication~)
          is Some(schema) {
          self.schemas.put(schema)
          let previous = match self.tables.get(schema.oid) {
            Some(Complete(value)) => Some(value)
            Some(Pending(_, previous)) => previous
            None => None
          }
          self.tables[schema.oid] = Pending(schema, previous)
        }
      RelationChanged(relation) =>
        resolved[index] = [
          self.relation(relation, tables.get(relation.oid), tx.commit_lsn),
        ]
      Insert(relation, _) | Update(relation, _, _) | Delete(relation, _) =>
        resolved[index] = [
          self.row(relation.oid, tables.get(relation.oid), tx.commit_lsn),
        ]
      Truncate(relations, ..) => {
        let schemas = relations.map(relation => {
          guard self.tables.get(relation.oid) is Some(Complete(schema)) else {
            raise UnsafeResume(
              "truncate requires a complete connection decoder",
            )
          }
          schema
        })
        resolved[index] = schemas
      }
      _ => ()
    }
  }
  { ..tx, decoded_schemas: Some(resolved), }
}
