// Behavior follows PostgreSQL REL_18_0 src/common/saslprep.c, including its
// mapped-input checks. See data/README.md and data/POSTGRESQL-LICENSE.

///|
fn in_ranges(code : Int32, ranges : ReadOnlyArray[Int32]) -> Bool {
  for low = 0, high = ranges.length() / 2; low < high; {
    let mid = low + (high - low) / 2
    if code < ranges[mid * 2] {
      continue low, mid
    } else if code > ranges[mid * 2 + 1] {
      continue mid + 1, high
    } else {
      return true
    }
  } nobreak {
    false
  }
}

///|
/// Prepare a String password for PostgreSQL SCRAM. Preparation failure returns
/// the entire original password; it must never return a partially mapped value.
/// The connection API takes String, so arbitrary non-UTF-8 bytes are not exposed.
pub fn password(input : String) -> String {
  // PostgreSQL's ASCII shortcut also preserves ASCII controls and empty input.
  if input.iter().all(c => c.to_int() < 128) {
    return input
  }
  let mapped : Array[Int32] = []
  for c in input {
    let code = c.to_int()
    // The order is intentional: U+200B belongs to both tables and becomes space.
    if in_ranges(code, non_ascii_space_ranges) {
      mapped.push(0x20)
    } else if !in_ranges(code, commonly_mapped_to_nothing_ranges) {
      mapped.push(code)
    }
  }
  if mapped.is_empty() {
    return input
  }
  let mut has_randal = false
  let mut has_l = false
  for code in mapped {
    // PostgreSQL 18 tests the mapped INPUT, not the normalized output.
    // In particular U+0340 and post-3.2 compatibility characters fall back.
    if in_ranges(code, prohibited_output_ranges) ||
      in_ranges(code, unassigned_codepoint_ranges) {
      return input
    }
    has_randal = has_randal || in_ranges(code, randalcat_codepoint_ranges)
    has_l = has_l || in_ranges(code, lcat_codepoint_ranges)
  }
  if has_randal &&
    (
      has_l ||
      !in_ranges(mapped[0], randalcat_codepoint_ranges) ||
      !in_ranges(mapped[mapped.length() - 1], randalcat_codepoint_ranges)
    ) {
    return input
  }
  nfkc(mapped)
}
