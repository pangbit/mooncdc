///|
pub suberror ConfigError {
  ConfigError(String)
}

///|
/// Shared readings only; the with_monitor scope owns the sampling task.
pub struct Monitor {
  mut snapshot : Snapshot
  mut revision : UInt64
  mut active : Bool
  thresholds : (Float, Float)?
  changed : @async.CondVar
  mut activations : UInt64
  mut resumes : UInt64
}

///|
fn Monitor::new(snapshot : Snapshot, thresholds : (Float, Float)?) -> Monitor {
  let self = {
    snapshot,
    revision: 0UL,
    active: false,
    thresholds,
    changed: @async.CondVar::Cond(),
    activations: 0UL,
    resumes: 0UL,
  }
  self.publish({ snapshot, fresh: true, })
  self
}

///|
fn Monitor::publish(self : Monitor, refresh : Refresh) -> Unit {
  if !refresh.fresh {
    return
  }
  if self.snapshot.total != refresh.snapshot.total {
    self.revision += 1UL
  }
  self.snapshot = refresh.snapshot
  if self.thresholds is Some((activate, resume_threshold)) {
    let active = next_backpressure(
      self.active,
      self.snapshot.used,
      self.snapshot.total,
      activate,
      resume_threshold,
    )
    if active != self.active {
      if active {
        self.activations = saturating_add(self.activations, 1UL)
      } else {
        self.resumes = saturating_add(self.resumes, 1UL)
      }
      self.active = active
      self.changed.broadcast()
    }
  }
}

///|
pub fn Monitor::current(self : Monitor) -> Snapshot {
  self.snapshot
}

///|
pub fn Monitor::capacity_revision(self : Monitor) -> UInt64 {
  self.revision
}

///|
pub fn Monitor::is_paused(self : Monitor) -> Bool {
  self.active
}

///|
/// Exact published pressure transitions for this monitor, activation then resume.
pub fn Monitor::transitions(self : Monitor) -> (UInt64, UInt64) {
  (self.activations, self.resumes)
}

///|
/// Gate only new source polling. Consumers of already-owned batches must not
/// call this before draining or publishing durable progress.
pub async fn Monitor::wait_for_capacity(self : Monitor) -> Unit {
  while self.active {
    self.changed.wait()
  }
}

///|
/// Wake destination drain loops as soon as emergency pressure activates.
pub async fn Monitor::wait_for_pressure(self : Monitor) -> Unit {
  while !self.active {
    self.changed.wait()
  }
}

///|
pub async fn[T] with_monitor(
  interval_ms : Int,
  thresholds : (Float, Float)?,
  run : async (Monitor) -> T,
) -> T {
  with_sampler(interval_ms, thresholds, sample, run)
}

///|
pub async fn[T] with_sampler(
  interval_ms : Int,
  thresholds : (Float, Float)?,
  read : async (Snapshot?) -> Refresh,
  run : async (Monitor) -> T,
) -> T {
  if interval_ms <= 0 {
    raise ConfigError("memory refresh interval must be positive")
  }
  if thresholds is Some((activate, resume_threshold)) &&
    !valid_thresholds(activate, resume_threshold) {
    raise ConfigError("invalid memory backpressure thresholds")
  }
  let initial = read(None)
  let monitor = Monitor::new(initial.snapshot, thresholds)
  @async.with_task_group(group => {
    group.spawn_bg(no_wait=true, () => {
      while true {
        let started = @bench.monotonic_clock_start()
        monitor.publish(read(Some(monitor.snapshot)))
        // Skip missed ticks instead of running a backlog of refreshes. The
        // monotonic clock prevents wall-clock changes from affecting cadence.
        let elapsed = @bench.monotonic_clock_end(started) / 1000.0
        let remaining = interval_ms.to_double() - elapsed
        @async.sleep(if remaining > 0.0 { remaining.to_int() } else { 0 })
      }
    })
    run(monitor)
  })
}
