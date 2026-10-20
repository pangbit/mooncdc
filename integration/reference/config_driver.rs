// Differential fixture for ETL's pinned config 0.14.1 / yaml-rust2 0.8.1.
// Orchestration and fixture validation live in tools/config-reference-test.mbtx.
use serde_json::{json, Value};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cases: Vec<Value> = serde_json::from_str(&std::fs::read_to_string(&args[1]).unwrap()).unwrap();
    let results: Vec<Value> = cases.iter().map(|case| {
        let source = case["source"].as_str().unwrap();
        match config::Config::builder()
            .add_source(config::File::from_str(source, config::FileFormat::Yaml))
            .build()
            .and_then(|c| c.try_deserialize::<Value>()) {
            Ok(value) => json!({"name": case["name"], "value": value}),
            Err(_) => json!({"name": case["name"], "error": true}),
        }
    }).collect();
    std::fs::write(&args[2], serde_json::to_string_pretty(&results).unwrap() + "\n").unwrap();
}
