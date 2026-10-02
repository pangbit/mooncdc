// Comparison fixture; all planning is performed by the pinned official ETL crate.
use etl::schema::{ColumnAlterationKind, ColumnNameMapping, ColumnPresenceChangeReason,
    ColumnSchema, ReplicatedTableSchema, ReplicationMask, SchemaOperation,
    TableId, TableName, TableSchema, Type};
use serde_json::{json, Value};
use std::sync::Arc;

fn schema(columns: &Value, mask: &Value) -> ReplicatedTableSchema {
    let columns = columns.as_array().unwrap().iter().map(|c| {
        ColumnSchema::new(c["name"].as_str().unwrap().to_owned(),
            Type::from_oid(c["type_oid"].as_u64().unwrap() as u32).unwrap(),
            c["type_modifier"].as_i64().unwrap() as i32,
            c["ordinal"].as_i64().unwrap() as i32, c["nullable"].as_bool().unwrap())
            .with_primary_key_ordinal_position(c.get("primary_key_ordinal").and_then(Value::as_i64).map(|n| n as i32))
            .with_default_expression_option(c.get("default_expression").and_then(Value::as_str).map(str::to_owned))
    }).collect();
    ReplicatedTableSchema::from_mask(Arc::new(TableSchema::new(TableId::new(42),
        TableName::new("public".to_owned(), "items".to_owned()), columns)),
        ReplicationMask::from_bytes(mask.as_array().unwrap().iter().map(|v| u8::from(v.as_bool().unwrap())).collect()))
}

fn column(c: &ColumnSchema) -> Value {
    let mut result = json!({"name": c.name, "type_oid": c.typ.oid(),
        "type_modifier": c.modifier, "ordinal": c.ordinal_position, "nullable": c.nullable});
    if let Some(pk) = c.primary_key_ordinal_position { result["primary_key_ordinal"] = json!(pk); }
    if let Some(default) = &c.default_expression { result["default_expression"] = json!(default); }
    result
}

fn reason(r: &ColumnPresenceChangeReason) -> &'static str {
    match r { ColumnPresenceChangeReason::TableSchema => "table", ColumnPresenceChangeReason::ReplicationMask => "publication" }
}

fn result(v: &Value) -> Value {
    let before = schema(&v["before"], &v["before_mask"]);
    let after = schema(&v["after"], &v["after_mask"]);
    let mapping = if v["lower"].as_bool().unwrap() { ColumnNameMapping::AsciiLowercase } else { ColumnNameMapping::Identity };
    let Ok(plan) = before.plan_schema_change(&after, mapping) else { return json!({"ok": false}); };
    let pk = plan.diff().altered_columns.iter().any(|c| c.primary_key_changed())
        || plan.diff().added_columns.iter().any(|c| c.after_column_schema.primary_key())
        || plan.diff().dropped_columns.iter().any(|c| c.before_column_schema.primary_key());
    let ops: Vec<Value> = plan.ordered_operations().iter().map(|op| match op {
        SchemaOperation::DropColumn { before_column_schema, reason: r } => json!(["drop", column(before_column_schema), reason(r)]),
        SchemaOperation::AddColumn { after_column_schema, reason: r } => json!(["add", column(after_column_schema), reason(r)]),
        SchemaOperation::AlterColumn { alteration } => json!(["alter", column(alteration.before_column_schema()), column(alteration.after_column_schema()),
            match alteration.kind() { ColumnAlterationKind::Rename => "rename", ColumnAlterationKind::Type => "type", ColumnAlterationKind::Nullability => "nullable", ColumnAlterationKind::Default => "default" }]),
    }).collect();
    json!({"ok": true, "operations": ops, "primary_key_changed": pk, "cycles": plan.has_rename_cycles()})
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mut vectors: Vec<Value> = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    for v in &mut vectors { v["result"] = result(v); }
    std::fs::write(&args[2], serde_json::to_vec(&vectors)?)?;
    Ok(())
}
