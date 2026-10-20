///|
priv struct BudgetVector {
  capacity : String
  ratio : Float
  slots : String
  maximum : String
} derive(ToJson)

///|
priv struct PressureVector {
  used : String
  total : String
  active : Bool
  activate : Float
  resume_threshold : Float
} derive(ToJson)

///|
async test "pinned ETL memory budget and backpressure differential" {
  let batches : Array[BudgetVector] = []
  for
    capacity in [
      0UL, 1UL, 10000UL, 9007199254740993UL, 0x8000000000000000UL, 0xffffffffffffffffUL,
    ] {
    for ratio in [0.0F, 0.2F, 0.5F, 1.0F, 1.1F] {
      for slots in [0UL, 1UL, 2UL, 17UL, 0xffffffffffffffffUL] {
        for maximum in [0UL, 1UL, 33554432UL, 0xffffffffffffffffUL] {
          batches.push({
            capacity: capacity.to_string(),
            ratio,
            slots: slots.to_string(),
            maximum: maximum.to_string(),
          })
        }
      }
    }
  }
  let pressures : Array[PressureVector] = []
  for active in [false, true] {
    for total in [0UL, 100UL, 0xffffffffffffffffUL] {
      for used in [0UL, 74UL, 75UL, 84UL, 85UL, 100UL, 0xffffffffffffffffUL] {
        for
          (activate, resume_threshold) in [
            (0.85F, 0.75F),
            (1.0F, 0.0F),
            (0.8F, 0.8F),
            (-0.1F, 0.0F),
          ] {
          pressures.push({
            used: used.to_string(),
            total: total.to_string(),
            active,
            activate,
            resume_threshold,
          })
        }
      }
    }
  }
  let phase = @env.get_env_var("MOONCDC_MEMORY_PHASE").unwrap_or("verify")
  let path = @env.get_env_var("MOONCDC_MEMORY_FILE").unwrap_or(
    "integration/reference/memory-expected.json",
  )
  if phase == "generate" {
    let input : Json = {
      "batch": batches.to_json(),
      "pressure": pressures.to_json(),
    }
    @fs.write_file(path, input.stringify())
    println(
      "MEMORY generated \{batches.length()} budget and \{pressures.length()} pressure vectors",
    )
  } else {
    assert_eq(phase, "verify")
    let result = @json.parse(@fs.read_file(path).text())
    let actual : Json = {
      "batch": batches
      .map(v => {
        let capacity = @string.parse_uint64(v.capacity)
        let slots = @string.parse_uint64(v.slots)
        let maximum = @string.parse_uint64(v.maximum)
        Json::object({
          "valid": valid_budget(v.ratio, maximum).to_json(),
          "target": batch_target(capacity, v.ratio, slots, maximum)
          .to_string()
          .to_json(),
        })
      })
      .to_json(),
      "pressure": pressures
      .map(v => {
        Json::object({
          "valid": valid_thresholds(v.activate, v.resume_threshold).to_json(),
          "active": next_backpressure(
            v.active,
            @string.parse_uint64(v.used),
            @string.parse_uint64(v.total),
            v.activate,
            v.resume_threshold,
          ).to_json(),
        })
      })
      .to_json(),
    }
    assert_eq(actual, result)
    println(
      "MEMORY matched \{batches.length()} budget and \{pressures.length()} pressure vectors",
    )
  }
}
