// Fixture program: include the pinned official encoder without changing it.
#[allow(dead_code)]
#[path = "../../etl-destinations/src/clickhouse/encoding.rs"]
mod encoding;

use serde_json::{json, Value};

#[allow(dead_code)]
mod clickhouse {
    pub mod sql { include!("../../etl-destinations/src/clickhouse/sql.rs"); }
    mod schema { include!("../../etl-destinations/src/clickhouse/schema.rs"); }
    pub fn default_expression(value: &str, oid: u32) -> Option<String> {
        schema::clickhouse_default_expression(value, &etl::schema::Type::from_oid(oid).unwrap())
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let inputs: Vec<Value> = serde_json::from_slice(&std::fs::read(&args[1]).unwrap()).unwrap();
    let outputs: Vec<_> = inputs.into_iter().map(|mut input| {
        if let Some(value) = input["default_input"].as_str() {
            input["result"] = json!({"value": clickhouse::default_expression(value, input["oid"].as_u64().unwrap() as u32)});
            return input;
        }
        let selector = input["selector"].as_u64().unwrap() as u8;
        let result = etl::fuzzing::parse_text_cell(selector, input["input"].as_str().unwrap())
            .and_then(encoding::cell_to_clickhouse_value)
            .and_then(|value| {
                let mut bytes = Vec::new();
                encoding::encode_to_row_binary(vec![value], &[input["nullable"].as_bool().unwrap()], &mut bytes)?;
                Ok(bytes.iter().map(|v| format!("{v:02x}")).collect::<String>())
            });
        input["result"] = match result {
            Ok(hex) => json!({"ok": true, "hex": hex}),
            Err(_) => json!({"ok": false}),
        };
        input
    }).collect();
    std::fs::write(&args[2], serde_json::to_vec_pretty(&outputs).unwrap()).unwrap();
}
