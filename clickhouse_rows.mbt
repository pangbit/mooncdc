///|
fn ch_validate_schema(
  schema : ReplicatedSchema,
  engine : ClickHouseEngine,
) -> Unit raise {
  ignore(ch_table_name(schema.schema.namespace_, schema.schema.name, engine))
  let columns = schema.columns()
  if !schema.all_primary_keys_replicated() ||
    (
      engine == ReplacingMergeTree &&
      !columns.any(c => c.primary_key_ordinal is Some(_))
    ) {
    raise Manual("ClickHouse requires the complete source primary key")
  }
  let reserved = ch_metadata_columns(engine)
  for column in columns {
    if reserved.any(c => c.0 == column.name) {
      raise Manual("source column conflicts with ClickHouse CDC metadata")
    }
  }
}

///|
fn ch_default_cell(column : SchemaColumn) -> Cell {
  if array_element(column.type_oid) is Some(_) {
    return Array([])
  }
  if column.nullable {
    return SqlNull
  }
  let epoch = PgDate::Finite(year=1970, month=1, day=1)
  let midnight : PgTime = { seconds: 0, nanoseconds: 0, }
  match column.type_oid {
    16U => Bool(false)
    21U => I16(0)
    23U => I32(0)
    20U => I64(0)
    26U => U32(0)
    700U => F32(0)
    701U => F64(0)
    1082U => Date(epoch)
    1114U => Timestamp(Finite(date=epoch, time=midnight))
    1184U => TimestampTz(Finite(date=epoch, time=midnight))
    2950U =>
      Uuid(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
    _ => String("")
  }
}

///|
fn ch_old_row(
  schema : ReplicatedSchema,
  old : OldTuple,
) -> ReadOnlyArray[Cell] raise {
  match old {
    Full(values) => schema.decode_row(values)
    Key(values) => {
      if schema.identity_kind() != PrimaryKey {
        raise Manual("ClickHouse old key is not the source primary key")
      }
      let columns = schema.columns()
      // pgoutput's K tuple has full relation width; non-key fields are null.
      if columns.length() != values.length() {
        raise Protocol("ClickHouse old key width mismatch")
      }
      ReadOnlyArray::makei(columns.length(), i => {
        if columns[i].primary_key_ordinal is Some(_) {
          values[i].decode_text(columns[i].type_oid)
        } else {
          ch_default_cell(columns[i])
        }
      })
    }
  }
}

///|
fn ch_key_equal(a : Cell, b : Cell) -> Bool {
  if a == b {
    return true
  }
  match (a, b) {
    (F32(a), F32(b)) => a.is_nan() && b.is_nan()
    (F64(a), F64(b)) => a.is_nan() && b.is_nan()
    (Array(a), Array(b)) => {
      if a.length() != b.length() {
        return false
      }
      for i, value in a {
        if !ch_key_equal(value, b[i]) {
          return false
        }
      }
      true
    }
    _ => false
  }
}

///|
fn ch_update_rows(
  schema : ReplicatedSchema,
  old : OldTuple?,
  values : ReadOnlyArray[Value],
) -> Array[(String, ReadOnlyArray[Cell])] raise {
  let columns = schema.columns()
  if columns.length() != values.length() {
    raise Protocol("ClickHouse update width mismatch")
  }
  let restored = ReadOnlyArray::makei(values.length(), i => {
    if values[i] != UnchangedToast {
      return values[i]
    }
    guard old is Some(Full(previous)) &&
      previous.length() == values.length() &&
      previous[i] != UnchangedToast else {
      raise Manual(
        "ClickHouse update has unresolved TOAST; use replica identity FULL",
      )
    }
    previous[i]
  })
  let current = schema.decode_row(restored)
  let rows : Array[(String, ReadOnlyArray[Cell])] = []
  if columns.any(c => c.primary_key_ordinal is Some(_)) {
    match old {
      Some(old) => {
        let previous = ch_old_row(schema, old)
        let mut changed = false
        for i, column in columns {
          if column.primary_key_ordinal is Some(_) &&
            !ch_key_equal(previous[i], current[i]) {
            changed = true
          }
        }
        if changed {
          rows.push(("DELETE", previous))
        }
      }
      None =>
        if schema.identity_kind() != PrimaryKey {
          raise Manual(
            "ClickHouse update cannot prove old primary key unchanged",
          )
        }
    }
  }
  rows.push(("UPDATE", current))
  rows
}

///|
fn ch_event_ordinal(tx : Transaction, index : Int) -> UInt64 raise {
  guard tx.event_id(index) is Some(id) else {
    raise Protocol("missing data event identity")
  }
  let parts = id.split("/").to_array()
  let index = if parts.length() >= 3 && parts[parts.length() - 2] == "table" {
    parts.length() - 3
  } else {
    parts.length() - 1
  }
  let ordinal = @string.parse_uint64(parts[index])
  if ordinal == 0xffffffffffffffffUL {
    raise LimitExceeded("ClickHouse event ordinal")
  }
  // ETL assigns ordinal zero to BEGIN; metadata announcements do not consume it.
  ordinal + 1
}
