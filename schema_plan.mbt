///|
pub(all) enum ColumnPresenceReason {
  TableSchema
  Publication
} derive(Eq, Debug)

///|
pub(all) enum ColumnAlteration {
  Rename
  DataType
  Nullability
  DefaultExpression
} derive(Eq, Debug)

///|
/// Alter changes exactly one field group; its before state includes earlier
/// operations. Names have already been mapped to the destination namespace.
pub(all) enum SchemaOperation {
  DropColumn(SchemaColumn, reason~ : ColumnPresenceReason)
  AddColumn(SchemaColumn, reason~ : ColumnPresenceReason)
  AlterColumn(
    before~ : SchemaColumn,
    after~ : SchemaColumn,
    kind~ : ColumnAlteration
  )
} derive(Eq, Debug)

///|
pub struct SchemaPlan {
  operations : ReadOnlyArray[SchemaOperation]
  primary_key_changed : Bool
  has_rename_cycles : Bool
} derive(Eq, Debug)

///|
fn mapped_columns(
  schema : ReplicatedSchema,
  map_name : (String) -> String,
) -> Array[SchemaColumn] raise {
  let seen : Map[String, Bool] = Map([])
  let columns : Array[SchemaColumn] = []
  for column in schema.columns() {
    let name = map_name(column.name)
    ignore(sql_identifier(name))
    if seen.contains(name) {
      raise Protocol("destination column name collision")
    }
    seen[name] = true
    columns.push({ ..column, name, })
  }
  columns.sort_by((a, b) => a.ordinal.compare(b.ordinal))
  columns
}

///|
/// Plan a destination transition by physical column ordinal. Drops precede
/// collision-free renames, additions, then type/nullability/default alterations.
/// map_name must match the destination's creation and data-write name mapping.
/// Primary-key changes are reported separately and never silently emitted as DDL.
pub fn ReplicatedSchema::plan_change(
  self : ReplicatedSchema,
  after : ReplicatedSchema,
  map_name? : (String) -> String = name => name,
) -> SchemaPlan raise {
  if self.schema.oid != after.schema.oid {
    raise Protocol("cannot plan across table identities")
  }
  let before_columns = mapped_columns(self, map_name)
  let after_columns = mapped_columns(after, map_name)
  let before_by_ordinal : Map[Int, SchemaColumn] = Map([])
  let after_by_ordinal : Map[Int, SchemaColumn] = Map([])
  let occupied : Map[String, Bool] = Map([])
  let reserved : Map[String, Bool] = Map([])
  for column in before_columns {
    before_by_ordinal[column.ordinal] = column
    occupied[column.name] = true
    reserved[column.name] = true
  }
  for column in after_columns {
    after_by_ordinal[column.ordinal] = column
    reserved[column.name] = true
  }
  let operations : Array[SchemaOperation] = []
  let pending : Array[(Int, String)] = []
  let mut primary_key_changed = false
  for column in before_columns {
    match after_by_ordinal.get(column.ordinal) {
      None => {
        operations.push(
          DropColumn(
            column,
            reason=if after.schema.columns.any(c => c.ordinal == column.ordinal) {
              Publication
            } else {
              TableSchema
            },
          ),
        )
        occupied.remove(column.name)
        primary_key_changed = primary_key_changed ||
          column.primary_key_ordinal is Some(_)
      }
      Some(next) => {
        if column.name != next.name {
          pending.push((column.ordinal, next.name))
        }
        primary_key_changed = primary_key_changed ||
          column.primary_key_ordinal != next.primary_key_ordinal
      }
    }
  }
  let mut temporary_sequence = 0
  let mut has_rename_cycles = false
  while !pending.is_empty() {
    let mut free = None
    for index, (_, name) in pending {
      if !occupied.contains(name) {
        free = Some(index)
        break
      }
    }
    let (ordinal, name) = if free is Some(index) {
      pending.remove(index)
    } else {
      has_rename_cycles = true
      let (ordinal, _) = pending[0]
      let name = for ;; {
        let name = "supabase_etl_ddl_tmp_column_\{ordinal}_\{temporary_sequence}"
        temporary_sequence += 1
        if !reserved.contains(name) {
          break name
        }
      }
      reserved[name] = true
      (ordinal, name)
    }
    let before = before_by_ordinal[ordinal]
    let next = { ..before, name, }
    operations.push(AlterColumn(before~, after=next, kind=Rename))
    occupied.remove(before.name)
    occupied[name] = true
    before_by_ordinal[ordinal] = next
  }
  for column in after_columns {
    if !before_by_ordinal.contains(column.ordinal) {
      operations.push(
        AddColumn(
          column,
          reason=if self.schema.columns.any(c => c.ordinal == column.ordinal) {
            Publication
          } else {
            TableSchema
          },
        ),
      )
      primary_key_changed = primary_key_changed ||
        column.primary_key_ordinal is Some(_)
    }
  }
  for column in before_columns {
    if after_by_ordinal.get(column.ordinal) is Some(endpoint) {
      let mut current = before_by_ordinal[column.ordinal]
      if current.type_oid != endpoint.type_oid ||
        current.type_modifier != endpoint.type_modifier {
        let next = {
          ..current,
          type_oid: endpoint.type_oid,
          type_modifier: endpoint.type_modifier,
        }
        operations.push(AlterColumn(before=current, after=next, kind=DataType))
        current = next
      }
      if current.nullable != endpoint.nullable {
        let next = { ..current, nullable: endpoint.nullable, }
        operations.push(
          AlterColumn(before=current, after=next, kind=Nullability),
        )
        current = next
      }
      if current.default_expression != endpoint.default_expression {
        let next = {
          ..current,
          default_expression: endpoint.default_expression,
        }
        operations.push(
          AlterColumn(before=current, after=next, kind=DefaultExpression),
        )
      }
    }
  }
  {
    operations: ReadOnlyArray::from_array(operations),
    primary_key_changed,
    has_rename_cycles,
  }
}
