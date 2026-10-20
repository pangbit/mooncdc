///|
/// Advisory targets; no bytes are reserved and a single row can exceed target.
pub struct Governor {
  monitor : Monitor
  ratio : Float
  maximum : UInt64
  mut slots : UInt64
  mut revision : UInt64
  mut capacity : UInt64
  mut target : UInt64
}

///|
pub fn Governor::new(
  monitor : Monitor,
  ratio : Float,
  maximum : UInt64,
) -> Governor raise {
  if !valid_budget(ratio, maximum) {
    raise ConfigError("invalid batch memory budget")
  }
  {
    monitor,
    ratio,
    maximum,
    slots: 0UL,
    revision: monitor.revision,
    capacity: monitor.snapshot.total,
    target: batch_target(monitor.snapshot.total, ratio, 0UL, maximum),
  }
}

///|
fn Governor::recalculate(self : Governor) -> Unit {
  self.target = batch_target(
    self.capacity,
    self.ratio,
    self.slots,
    self.maximum,
  )
}

///|
pub fn Governor::target_bytes(self : Governor) -> UInt64 {
  if self.revision != self.monitor.revision {
    self.revision = self.monitor.revision
    self.capacity = self.monitor.snapshot.total
    self.recalculate()
  }
  self.target
}

///|
pub fn Governor::registered_slots(self : Governor) -> UInt64 {
  self.slots
}

///|
/// Register positions that can own accumulating or in-flight batches. The scope
/// must include their final drain. Every exit, including cancellation, releases
/// the slots; retaining a Governor handle cannot retain the registration.
pub async fn[T] Governor::with_slots(
  self : Governor,
  slots : UInt64,
  run : async () -> T,
) -> T {
  let slots = if slots == 0UL { 1UL } else { slots }
  if 0xffffffffffffffffUL - self.slots < slots {
    raise ConfigError("batch slot count overflow")
  }
  self.slots += slots
  self.recalculate()
  defer {
    self.slots -= slots
    self.recalculate()
  }
  run()
}
