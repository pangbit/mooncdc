///|
pub fn valid_budget(ratio : Float, maximum : UInt64) -> Bool {
  ratio > 0.0 && ratio <= 1.0 && maximum > 0UL
}

///|
pub fn valid_thresholds(activate : Float, resume_threshold : Float) -> Bool {
  activate > 0.0 &&
  activate <= 1.0 &&
  resume_threshold >= 0.0 &&
  resume_threshold < activate
}

///|
/// Advisory decoded-batch target, not a reservation or an allocation limit.
/// Callers validate the configuration; zero capacity still permits one item.
pub fn batch_target(
  capacity : UInt64,
  ratio : Float,
  slots : UInt64,
  maximum : UInt64,
) -> UInt64 {
  let bytes = capacity.to_double() * ratio.to_double()
  // Rust's f64 -> u64 saturates at 2^64. The current MoonBit to_uint64
  // saturates at 2^63, so convert the upper half explicitly.
  let global = if bytes.is_nan() || bytes <= 0.0 {
    0UL
  } else if bytes >= 18446744073709551616.0 {
    0xffffffffffffffffUL
  } else if bytes >= 9223372036854775808.0 {
    (bytes - 9223372036854775808.0).to_int64().reinterpret_as_uint64() +
    0x8000000000000000UL
  } else {
    bytes.to_int64().reinterpret_as_uint64()
  }
  let slots = if slots == 0UL { 1UL } else { slots }
  let target = global / slots
  let target = if target == 0UL { 1UL } else { target }
  let maximum = if maximum == 0UL { 1UL } else { maximum }
  if target < maximum {
    target
  } else {
    maximum
  }
}

///|
/// Usage is measured over the same capacity domain. Resume is strictly below
/// its threshold; equality remains paused. Unknown zero capacity is full.
pub fn next_backpressure(
  active : Bool,
  used : UInt64,
  total : UInt64,
  activate : Float,
  resume_threshold : Float,
) -> Bool {
  let ratio = Float::from_uint64(used) / Float::from_uint64(total)
  let ratio : Float = if ratio.is_nan() {
    1.0
  } else {
    Float::min(Float::max(ratio, 0.0), 1.0)
  }
  ratio >= (if active { resume_threshold } else { activate })
}
