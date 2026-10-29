// PostgreSQL-compatible NFKC, using the complete pinned Unicode 16.0 tables.
// Algorithm adapted from PostgreSQL REL_18_0 src/common/unicode_norm.c.
// PostgreSQL license: data/POSTGRESQL-LICENSE.

///|
fn decomposition_index(code : Int) -> Int? {
  for low = 0, high = decompositions.length() / 4; low < high; {
    let mid = low + (high - low) / 2
    let found = decompositions[mid * 4]
    if found < code {
      continue mid + 1, high
    } else if found > code {
      continue low, mid
    } else {
      return Some(mid * 4)
    }
  } nobreak {
    None
  }
}

///|
fn combining_class(code : Int) -> Int {
  match decomposition_index(code) {
    Some(i) => decompositions[i + 1]
    None => 0
  }
}

///|
fn decompose(code : Int, output : Array[Int]) -> Unit {
  // Hangul syllables use the algorithmic decomposition from UAX #15.
  if code >= 0xAC00 && code < 0xAC00 + 11172 {
    let index = code - 0xAC00
    output.push(0x1100 + index / 588)
    output.push(0x1161 + index % 588 / 28)
    if index % 28 != 0 {
      output.push(0x11A7 + index % 28)
    }
    return
  }
  guard decomposition_index(code) is Some(i) else {
    output.push(code)
    return
  }
  let flags = decompositions[i + 2]
  let size = flags & 31
  let offset = decompositions[i + 3]
  if size == 0 {
    output.push(code)
  } else if (flags & 64) != 0 {
    decompose(offset, output)
  } else {
    for j in 0..<size {
      decompose(decomposition_codepoints[offset + j], output)
    }
  }
}

///|
// Build the inverse of canonical, non-excluded two-codepoint decompositions.
// UInt64 packs two Unicode scalars without collisions or 32-bit overflow.
let compositions : Map[UInt64, Int] = {
  let result : Map[UInt64, Int] = Map([])
  for i in 0..<(decompositions.length() / 4) {
    let offset = i * 4
    let flags = decompositions[offset + 2]
    if (flags & 31) == 2 && (flags & 160) == 0 {
      let index = decompositions[offset + 3]
      let first = decomposition_codepoints[index]
      let second = decomposition_codepoints[index + 1]
      result[(first.to_uint64() << 32) | second.to_uint64()] = decompositions[offset]
    }
  }
  result
}

///|
fn compose(first : Int, second : Int) -> Int? {
  if first >= 0x1100 &&
    first < 0x1100 + 19 &&
    second >= 0x1161 &&
    second < 0x1161 + 21 {
    Some(0xAC00 + ((first - 0x1100) * 21 + second - 0x1161) * 28)
  } else if first >= 0xAC00 &&
    first < 0xAC00 + 11172 &&
    (first - 0xAC00) % 28 == 0 &&
    second > 0x11A7 &&
    second < 0x11A7 + 28 {
    Some(first + second - 0x11A7)
  } else {
    compositions.get((first.to_uint64() << 32) | second.to_uint64())
  }
}

///|
fn nfkc(input : Array[Int]) -> String {
  let decomposed : Array[Int] = []
  for code in input {
    decompose(code, decomposed)
  }
  // Stable canonical ordering; never move a combining mark across a starter.
  for i in 1..<decomposed.length() {
    let code = decomposed[i]
    let class = combining_class(code)
    let mut j = i
    if class != 0 {
      while j > 0 && combining_class(decomposed[j - 1]) > class {
        decomposed[j] = decomposed[j - 1]
        j -= 1
      }
      decomposed[j] = code
    }
  }
  if decomposed.is_empty() {
    return ""
  }
  let result = [decomposed[0]]
  let mut starter = 0
  let mut last_class = -1
  for i in 1..<decomposed.length() {
    let code = decomposed[i]
    let class = combining_class(code)
    if last_class < class && compose(result[starter], code) is Some(composite) {
      result[starter] = composite
    } else {
      if class == 0 {
        starter = result.length()
        last_class = -1
      } else {
        last_class = class
      }
      result.push(code)
    }
  }
  let output = StringBuilder()
  for code in result {
    output.write_char(code.to_char().unwrap())
  }
  output.to_string()
}
