///|
/// Emergency source pausing. This does not reserve memory or change transaction
/// limits. Defaults follow ETL; omit the run_pipeline option to disable it.
pub struct MemoryBackpressureConfig {
  activate_threshold : Float
  resume_threshold : Float
  refresh_interval_ms : Int
}

///|
pub fn MemoryBackpressureConfig::new(
  activate_threshold? : Float = 0.85F,
  resume_threshold? : Float = 0.75F,
  refresh_interval_ms? : Int = 100,
) -> MemoryBackpressureConfig {
  { activate_threshold, resume_threshold, refresh_interval_ms, }
}

///|
fn MemoryBackpressureConfig::validate(
  self : MemoryBackpressureConfig,
) -> Unit raise {
  if self.refresh_interval_ms <= 0 ||
    !@memory.valid_thresholds(self.activate_threshold, self.resume_threshold) {
    raise InvalidConfig("invalid memory backpressure configuration")
  }
}

///|
async fn wait_source_capacity(
  monitor : @memory.Monitor?,
  activity : ActivityHandle?,
) -> Unit {
  if monitor is Some(monitor) && monitor.is_paused() {
    if activity is Some(activity) {
      activity.source_paused(true)
    }
    defer (if activity is Some(activity) { activity.source_paused(false) })
    monitor.wait_for_capacity()
  }
}
