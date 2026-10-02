// Comparison fixture for the pinned, unmodified ETL library. Orchestration is in .mbtx.
use chrono::{Datelike, Timelike};
use etl::data::{ArrayCell, Cell, Date, PgTime, Timestamp};
use serde_json::{json, Value};

fn date(value: Date<chrono::NaiveDate>) -> Value {
    match value {
        Date::Value(d) => json!([d.year(), d.month(), d.day()]),
        Date::PosInfinity => json!("infinity"),
        Date::NegInfinity => json!("-infinity"),
    }
}

fn time(value: PgTime) -> Value {
    match value {
        PgTime::Value(t) => json!([t.num_seconds_from_midnight(), t.nanosecond()]),
        PgTime::EndOfDay => json!([86400, 0]),
    }
}

fn hex(value: &[u8]) -> String {
    value.iter().map(|v| format!("{v:02x}")).collect()
}

fn normalize(cell: Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(v) => json!(["bool", v]),
        Cell::String(v) => json!(["string", v]),
        Cell::I16(v) => json!(["i16", v.to_string()]),
        Cell::I32(v) => json!(["i32", v.to_string()]),
        Cell::I64(v) => json!(["i64", v.to_string()]),
        Cell::U32(v) => json!(["u32", v.to_string()]),
        Cell::F32(v) => json!(["f32", if v.is_nan() { "NaN".to_owned() } else { format!("{:x}", v.to_bits()) }]),
        Cell::F64(v) => json!(["f64", if v.is_nan() { "NaN".to_owned() } else { format!("{:x}", v.to_bits()) }]),
        Cell::Numeric(v) => json!(["numeric", v.to_string()]),
        Cell::Bytes(v) => json!(["bytes", hex(&v)]),
        Cell::Uuid(v) => json!(["uuid", hex(v.as_bytes())]),
        Cell::Json(v) => json!(["json", v]),
        Cell::Date(v) => json!(["date", date(v)]),
        Cell::Time(v) => json!(["time", time(v)]),
        Cell::TimeTz(v) => json!(["timetz", time(v.time()), v.offset().local_minus_utc()]),
        Cell::Timestamp(v) => json!(["timestamp", match v {
            Timestamp::Value(t) => json!([date(Date::Value(t.date())), time(PgTime::Value(t.time()))]),
            Timestamp::PosInfinity => json!("infinity"),
            Timestamp::NegInfinity => json!("-infinity"),
        }]),
        Cell::TimestampTz(v) => json!(["timestamptz", match v {
            Timestamp::Value(t) => json!([date(Date::Value(t.date_naive())), time(PgTime::Value(t.time()))]),
            Timestamp::PosInfinity => json!("infinity"),
            Timestamp::NegInfinity => json!("-infinity"),
        }]),
        Cell::Array(array) => {
            macro_rules! values {
                ($v:expr, $kind:ident) => {
                    $v.into_iter().map(|v| v.map(|v| normalize(Cell::$kind(v))).unwrap_or(Value::Null)).collect::<Vec<_>>()
                };
            }
            let items = match array {
                ArrayCell::Bool(v) => values!(v, Bool),
                ArrayCell::String(v) => values!(v, String),
                ArrayCell::I16(v) => values!(v, I16),
                ArrayCell::I32(v) => values!(v, I32),
                ArrayCell::I64(v) => values!(v, I64),
                ArrayCell::U32(v) => values!(v, U32),
                ArrayCell::F32(v) => values!(v, F32),
                ArrayCell::F64(v) => values!(v, F64),
                ArrayCell::Numeric(v) => values!(v, Numeric),
                ArrayCell::Bytes(v) => values!(v, Bytes),
                ArrayCell::Uuid(v) => values!(v, Uuid),
                ArrayCell::Json(v) => values!(v, Json),
                ArrayCell::Date(v) => values!(v, Date),
                ArrayCell::Time(v) => values!(v, Time),
                ArrayCell::TimeTz(v) => values!(v, TimeTz),
                ArrayCell::Timestamp(v) => values!(v, Timestamp),
                ArrayCell::TimestampTz(v) => values!(v, TimestampTz),
            };
            json!(["array", items])
        }
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let inputs: Vec<Value> = serde_json::from_slice(&std::fs::read(&args[1]).unwrap()).unwrap();
    let outputs: Vec<_> = inputs.into_iter().map(|mut input| {
        let selector = input["selector"].as_u64().unwrap() as u8;
        input["result"] = match etl::fuzzing::parse_text_cell(selector, input["input"].as_str().unwrap()) {
            Ok(value) => json!({"ok": true, "value": normalize(value)}),
            Err(_) => json!({"ok": false}),
        };
        input
    }).collect();
    std::fs::write(&args[2], serde_json::to_vec_pretty(&outputs).unwrap()).unwrap();
}
