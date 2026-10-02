///|
/// An unsigned PostgreSQL WAL position.
pub struct Lsn {
  value : UInt64
} derive(Eq, Compare, Debug, ToJson)

///|
/// Parse the PostgreSQL hexadecimal `high/low` notation.
pub fn Lsn::parse(text : String) -> Lsn raise {
  let parts = text.split("/").to_array()
  if parts.length() != 2 {
    raise InvalidConfig("invalid LSN")
  }
  for part in parts {
    if part.is_empty() || part.length() > 8 {
      raise InvalidConfig("invalid LSN component")
    }
    for c in part {
      if !(c >= '0' && c <= '9') &&
        !(c >= 'a' && c <= 'f') &&
        !(c >= 'A' && c <= 'F') {
        raise InvalidConfig("invalid LSN digit")
      }
    }
  }
  {
    value: (@string.parse_uint64(parts[0], base=16) << 32) |
    @string.parse_uint64(parts[1], base=16),
  }
}

///|
pub fn Lsn::to_string(self : Lsn) -> String {
  (self.value >> 32).to_string(radix=16).to_upper() +
  "/" +
  (self.value & 0xffffffffUL).to_string(radix=16).to_upper()
}
