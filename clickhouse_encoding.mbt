///|
fn ch_string(out : @buffer.Buffer, value : String) -> Unit {
  let bytes = @utf8.encode(value)
  out.write_leb128(bytes.length().reinterpret_as_uint())
  out.write_bytes(bytes)
}

///|
fn ch_date_days(date : PgDate) -> Int raise {
  guard date is Finite(year~, month~, day~) else {
    raise Conversion("ClickHouse cannot encode infinite dates")
  }
  if year < -262143 || year > 262142 || day < 1 || day > month_days(year, month) {
    raise Conversion("invalid ClickHouse date")
  }
  // Gregorian eras, including astronomical year zero. Floor division matters
  // for BC dates; MoonBit integer division truncates towards zero.
  let year = year - (if month <= 2 { 1 } else { 0 })
  let era = (if year >= 0 { year } else { year - 399 }) / 400
  let y = year - era * 400
  let m = month + (if month > 2 { -3 } else { 9 })
  era * 146097 +
  y * 365 +
  y / 4 -
  y / 100 +
  (153 * m + 2) / 5 +
  day -
  1 -
  719468
}

///|
fn ch_timestamp_micros(value : PgTimestamp) -> Int64 raise {
  guard value is Finite(date~, time~) else {
    raise Conversion("ClickHouse cannot encode infinite timestamps")
  }
  ch_validate_time(time)
  if time.seconds == 86400 {
    raise Conversion("timestamp requires normalized date")
  }
  ch_date_days(date).to_int64() * 86400000000L +
  time.seconds.to_int64() * 1000000L +
  (time.nanoseconds / 1000).to_int64()
}

///|
fn ch_validate_time(value : PgTime) -> Unit raise {
  if value.seconds < 0 ||
    value.seconds > 86400 ||
    value.nanoseconds < 0 ||
    value.nanoseconds >= 2000000000 ||
    (value.nanoseconds >= 1000000000 && value.seconds % 60 != 59) ||
    (value.seconds == 86400 && value.nanoseconds != 0) {
    raise Conversion("invalid ClickHouse time")
  }
}

///|
fn ch_pad(value : Int, width : Int) -> String {
  let text = value.to_string()
  "0".repeat(Int::max(width - text.length(), 0)) + text
}

///|
fn ch_time(value : PgTime) -> String raise {
  ch_validate_time(value)
  let nanos = value.nanoseconds % 1000000000
  let fraction = if nanos == 0 {
    ""
  } else if nanos % 1000000 == 0 {
    "." + ch_pad(nanos / 1000000, 3)
  } else if nanos % 1000 == 0 {
    "." + ch_pad(nanos / 1000, 6)
  } else {
    "." + ch_pad(nanos, 9)
  }
  ch_pad(value.seconds / 3600, 2) +
  ":" +
  ch_pad(value.seconds / 60 % 60, 2) +
  ":" +
  ch_pad(value.seconds % 60 + value.nanoseconds / 1000000000, 2) +
  fraction
}

///|
fn ch_timetz(value : PgTime, offset : Int) -> String raise {
  if offset <= -86400 || offset >= 86400 {
    raise Conversion("invalid UTC offset")
  }
  let magnitude = if offset < 0 { -offset } else { offset }
  ch_time(value) +
  (if offset < 0 { "-" } else { "+" }) +
  ch_pad(magnitude / 3600, 2) +
  (if magnitude % 3600 == 0 {
    ""
  } else {
    ":" + ch_pad(magnitude / 60 % 60, 2)
  }) +
  (if magnitude % 60 == 0 { "" } else { ":" + ch_pad(magnitude % 60, 2) })
}

///|
// serde_json's default map is ordered by UTF-8 bytes. Preserve its canonical
// ordering and arbitrary-precision number text in the destination String.
fn ch_json(value : Json) -> String {
  match value {
    Object(fields) => {
      let keys = fields.keys().collect()
      keys.sort_by((a, b) => {
        let a = @utf8.encode(a)
        let b = @utf8.encode(b)
        for i in 0..<Int::min(a.length(), b.length()) {
          if a[i] != b[i] {
            return a[i].compare(b[i])
          }
        }
        a.length().compare(b.length())
      })
      "{" +
      keys
      .map(key => key.to_json().stringify() + ":" + ch_json(fields[key]))
      .join(",") +
      "}"
    }
    Array(values) => "[" + values.map(ch_json).join(",") + "]"
    _ => value.stringify()
  }
}

///|
fn ch_encode_cell(
  out : @buffer.Buffer,
  value : Cell,
  nullable : Bool,
) -> Unit raise {
  if nullable {
    if value is SqlNull {
      out.write_byte(1)
      return
    }
    out.write_byte(0)
  }
  match value {
    SqlNull => raise Conversion("NULL for non-nullable ClickHouse column")
    ToastUnchanged => raise Conversion("unresolved TOAST for ClickHouse column")
    Bool(v) => out.write_byte(if v { 1 } else { 0 })
    I16(v) => out.write_int16_le(v)
    I32(v) => out.write_int_le(v)
    I64(v) => out.write_int64_le(v)
    U32(v) => out.write_uint_le(v)
    F32(v) => out.write_float_le(v)
    F64(v) => out.write_double_le(v)
    String(v) | Numeric(v) => ch_string(out, v)
    Time(v) => ch_string(out, ch_time(v))
    TimeTz(v, offset_seconds~) => ch_string(out, ch_timetz(v, offset_seconds))
    Date(v) => out.write_int_le(ch_date_days(v))
    Timestamp(v) | TimestampTz(v) => out.write_int64_le(ch_timestamp_micros(v))
    Json(v) => ch_string(out, ch_json(v))
    Bytes(v) => ch_string(out, @crypto.bytes_to_hex_string(v))
    Uuid(v) => {
      if v.length() != 16 {
        raise Conversion("ClickHouse UUID width")
      }
      for i in 0..<8 {
        out.write_byte(v[7 - i])
      }
      for i in 0..<8 {
        out.write_byte(v[15 - i])
      }
    }
    Array(values) => {
      out.write_leb128(values.length().reinterpret_as_uint())
      for value in values {
        ch_encode_cell(out, value, true)
      }
    }
  }
}
