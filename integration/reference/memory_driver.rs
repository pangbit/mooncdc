// Appended as a test adapter inside the pinned memory_monitor module. The
// production monitor, governor, and config validators remain unmodified.
#[test]
fn mooncdc_memory_reference() {
    use crate::runtime::batch_memory_governor::BatchMemoryGovernor;
    use etl_config::shared::{BatchConfig, Validate};
    use serde_json::{Value, json};
    let input: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("MOONCDC_MEMORY_INPUT").unwrap()).unwrap(),
    )
    .unwrap();
    let number = |v: &Value, key: &str| -> u64 { v[key].as_str().unwrap().parse().unwrap() };
    let batch: Vec<Value> = input["batch"].as_array().unwrap().iter().map(|v| {
        let capacity = number(v, "capacity");
        let ratio = v["ratio"].as_f64().unwrap() as f32;
        let slots = usize::try_from(number(v, "slots")).unwrap();
        let maximum = usize::try_from(number(v, "maximum")).unwrap();
        let config = BatchConfig { memory_budget_ratio: ratio, max_bytes: maximum, ..Default::default() };
        let monitor = MemoryMonitor::new_for_test();
        monitor.set_total_memory_bytes_for_test(capacity);
        let governor = BatchMemoryGovernor::new(1, monitor, ratio, maximum);
        let _slots = governor.register_batch_slots(slots);
        json!({"valid": config.validate().is_ok(), "target": governor.batch_size_target_bytes().to_string()})
    }).collect();
    let pressure: Vec<Value> = input["pressure"].as_array().unwrap().iter().map(|v| {
        let config = MemoryBackpressureConfig {
            activate_threshold: v["activate"].as_f64().unwrap() as f32,
            resume_threshold: v["resume_threshold"].as_f64().unwrap() as f32,
        };
        let snapshot = MemorySnapshot {
            used: number(v,"used"), total: number(v,"total"), source: MemorySnapshotSource::System,
        };
        json!({"valid": config.validate().is_ok(), "active": compute_next_backpressure_active(
            v["active"].as_bool().unwrap(), snapshot.used_percent(), config.activate_threshold, config.resume_threshold)})
    }).collect();
    std::fs::write(
        std::env::var("MOONCDC_MEMORY_OUTPUT").unwrap(),
        serde_json::to_vec_pretty(&json!({"batch":batch,"pressure":pressure})).unwrap(),
    )
    .unwrap();
}
