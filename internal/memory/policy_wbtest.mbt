///|
test "memory budget and hysteresis validate finite ranges and preserve boundaries" {
  for ratio in [0.0F, -0.1F, 1.1F, 0.0F / 0.0F, 1.0F / 0.0F] {
    assert_false(valid_budget(ratio, 1UL))
  }
  assert_false(valid_budget(0.2, 0UL))
  assert_true(valid_budget(1.0, 1UL))
  for
    (activate, resume_threshold) in [
      (0.0F, 0.0F),
      (1.1F, 0.5F),
      (0.8F, 0.8F),
      (0.8F, -0.1F),
      (0.0F / 0.0F, 0.0F),
      (0.85F, 0.0F / 0.0F),
    ] {
    assert_false(valid_thresholds(activate, resume_threshold))
  }
  assert_true(valid_thresholds(1.0, 0.0))
  assert_true(next_backpressure(false, 85UL, 100UL, 0.85, 0.75))
  assert_false(next_backpressure(false, 84UL, 100UL, 0.85, 0.75))
  assert_true(next_backpressure(true, 75UL, 100UL, 0.85, 0.75))
  assert_false(next_backpressure(true, 74UL, 100UL, 0.85, 0.75))
  assert_true(next_backpressure(false, 0UL, 0UL, 0.85, 0.75))
  assert_true(next_backpressure(false, 101UL, 100UL, 0.85, 0.75))
}

///|
test "batch target divides capacity by slots independently of usage with a one byte floor" {
  assert_eq(batch_target(10000UL, 0.2, 1UL, 10000UL), 2000UL)
  assert_eq(batch_target(10000UL, 0.2, 4UL, 10000UL), 500UL)
  assert_eq(batch_target(6000UL, 0.2, 2UL, 10000UL), 600UL)
  assert_eq(batch_target(0UL, 0.2, 0UL, 0UL), 1UL)
  assert_eq(batch_target(100000000000UL, 0.2, 1UL, 33554432UL), 33554432UL)
  assert_eq(
    batch_target(0xffffffffffffffffUL, 1.0, 1UL, 0xffffffffffffffffUL),
    0xffffffffffffffffUL,
  )
  assert_eq(
    batch_target(0xffffffffffffffffUL, 0.5, 1UL, 0xffffffffffffffffUL),
    0x8000000000000000UL,
  )
}
