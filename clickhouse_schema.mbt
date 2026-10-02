///|
/// ReplacingMergeTree exposes current state through a FINAL view; MergeTree
/// retains insert/update/delete history. TRUNCATE clears either engine.
pub(all) enum ClickHouseEngine {
  ReplacingMergeTree
  MergeTree
} derive(Eq, Debug)

///|
pub extend ClickHouseEngine with Eq::{equal, not_equal}

///|
pub extend ClickHouseEngine with Debug::{to_repr}

///|
fn ch_quote(value : String, delimiter : Char) -> String {
  let out = StringBuilder()
  out.write_char(delimiter)
  for ch in value {
    match ch {
      '\\' => out.write_string("\\\\")
      '\u0000' => out.write_string("\\0")
      '\n' => out.write_string("\\n")
      '\r' => out.write_string("\\r")
      '\t' => out.write_string("\\t")
      ch if ch == delimiter => {
        out.write_char('\\')
        out.write_char(ch)
      }
      ch => out.write_char(ch)
    }
  }
  out.write_char(delimiter)
  out.to_string()
}

///|
fn ch_scalar_type(oid : UInt) -> String {
  match oid {
    16U => "Bool"
    21U => "Int16"
    23U => "Int32"
    20U => "Int64"
    700U => "Float32"
    701U => "Float64"
    1082U => "Date32"
    1114U => "DateTime64(6)"
    1184U => "DateTime64(6, 'UTC')"
    2950U => "UUID"
    26U => "UInt32"
    _ => "String"
  }
}

///|
fn ch_type(oid : UInt, nullable : Bool, force_nullable : Bool) -> String {
  match array_element(oid) {
    Some(element) => "Array(Nullable(" + ch_scalar_type(element) + "))"
    None => {
      let base = ch_scalar_type(oid)
      if nullable || force_nullable {
        "Nullable(" + base + ")"
      } else {
        base
      }
    }
  }
}

///|
fn ch_table_name(
  namespace_ : String,
  table : String,
  engine : ClickHouseEngine,
) -> String raise {
  for component in [namespace_, table] {
    if component.is_empty() ||
      component.has_prefix("_") ||
      component.has_suffix("_") ||
      component.contains("\"") ||
      component.contains(";") {
      raise InvalidConfig("unsupported ClickHouse source table name")
    }
  }
  let name = namespace_.replace_all(old="_", new="__") +
    "_" +
    table.replace_all(old="_", new="__")
  if engine == ReplacingMergeTree && name.has_suffix("__current") {
    raise InvalidConfig(
      "ClickHouse table conflicts with current-state view suffix",
    )
  }
  name
}

///|
fn ch_metadata_columns(engine : ClickHouseEngine) -> Array[(String, String)] {
  match engine {
    ReplacingMergeTree =>
      [("_etl_version", "UInt128"), ("_etl_deleted", "UInt8")]
    MergeTree =>
      [
        ("cdc_operation", "String"),
        ("cdc_lsn", "UInt64"),
        ("cdc_tx_ordinal", "UInt64"),
      ]
  }
}

///|
priv struct ChLayout {
  columns : ReadOnlyArray[(String, String)]
  column_list : String
  header : Bytes
}

///|
fn ChLayout::new(columns : ReadOnlyArray[(String, String)]) -> ChLayout raise {
  let seen : Map[String, Bool] = Map([])
  for (name, _) in columns {
    if seen.contains(name) {
      raise Conversion("duplicate ClickHouse column")
    }
    seen[name] = true
  }
  let out = @buffer.Buffer()
  out.write_leb128(columns.length().reinterpret_as_uint())
  for (name, _) in columns {
    ch_string(out, name)
  }
  for (_, typ) in columns {
    ch_string(out, typ)
  }
  {
    columns,
    column_list: columns
    .iter()
    .map(c => ch_quote(c.0, '"'))
    .collect()
    .join(", "),
    header: out.to_bytes(),
  }
}

///|
fn ChLayout::row(
  self : ChLayout,
  cells : ReadOnlyArray[Cell],
  engine : ClickHouseEngine,
  operation : String,
  commit_lsn : UInt64,
  ordinal : UInt64,
) -> Bytes raise {
  let metadata_width = ch_metadata_columns(engine).length()
  if cells.length() + metadata_width != self.columns.length() {
    raise Conversion("ClickHouse row width mismatch")
  }
  let out = @buffer.Buffer()
  for index, cell in cells {
    ch_encode_cell(out, cell, self.columns[index].1.has_prefix("Nullable("))
  }
  match engine {
    ReplacingMergeTree => {
      // UInt128 is little-endian: ordinal first, commit LSN second.
      out.write_uint64_le(ordinal)
      out.write_uint64_le(commit_lsn)
      out.write_byte(if operation == "DELETE" { 1 } else { 0 })
    }
    MergeTree => {
      ch_string(out, operation)
      out.write_uint64_le(commit_lsn)
      out.write_uint64_le(ordinal)
    }
  }
  out.to_bytes()
}
