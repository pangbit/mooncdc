///|
/// Typed PostgreSQL text values. Numeric uses exact canonical decimal text and scale.
/// Unknown scalar types retain text; TOAST is distinct from both SQL and JSON null.
/// JSON numbers retain exact text in Number.repr; its Double is approximate.
/// Cell equality compares that text recursively, including decimal scale.
pub(all) enum Cell {
  SqlNull
  ToastUnchanged
  Bool(Bool)
  String(String)
  I16(Int16)
  I32(Int)
  I64(Int64)
  U32(UInt)
  F32(Float)
  F64(Double)
  Numeric(String)
  Date(PgDate)
  Time(PgTime)
  TimeTz(PgTime, offset_seconds~ : Int)
  Timestamp(PgTimestamp)
  TimestampTz(PgTimestamp)
  Uuid(Bytes)
  Json(Json)
  Bytes(Bytes)
  Array(ReadOnlyArray[Cell])
} derive(Debug)

///|
pub impl Eq for Cell with fn equal(left, right) {
  match (left, right) {
    (SqlNull, SqlNull) | (ToastUnchanged, ToastUnchanged) => true
    (Bool(a), Bool(b)) => a == b
    (String(a), String(b)) | (Numeric(a), Numeric(b)) => a == b
    (I16(a), I16(b)) => a == b
    (I32(a), I32(b)) => a == b
    (I64(a), I64(b)) => a == b
    (U32(a), U32(b)) => a == b
    (F32(a), F32(b)) => a == b
    (F64(a), F64(b)) => a == b
    (Date(a), Date(b)) => a == b
    (Time(a), Time(b)) => a == b
    (TimeTz(a, offset_seconds=ao), TimeTz(b, offset_seconds=bo)) =>
      a == b && ao == bo
    (Timestamp(a), Timestamp(b)) | (TimestampTz(a), TimestampTz(b)) => a == b
    (Uuid(a), Uuid(b)) | (Bytes(a), Bytes(b)) => a == b
    (Json(a), Json(b)) => pg_json_equal(a, b)
    (Array(a), Array(b)) => a == b
    _ => false
  }
}

///|
pub(all) enum PgDate {
  Finite(year~ : Int, month~ : Int, day~ : Int)
  PosInfinity
  NegInfinity
} derive(Eq, Debug)

///|
/// seconds=86400 and nanoseconds=0 denotes the distinct 24:00:00 value.
/// A leap second uses second 59 with nanoseconds in 1000000000..<2000000000.
pub struct PgTime {
  seconds : Int
  nanoseconds : Int
} derive(Eq, Debug)

///|
/// Finite timestamps use astronomical years (1 BC = year 0).
/// TimestampTz cells normalize finite values to UTC.
pub(all) enum PgTimestamp {
  Finite(date~ : PgDate, time~ : PgTime)
  PosInfinity
  NegInfinity
} derive(Eq, Debug)

///|
fn array_element(oid : UInt) -> UInt? {
  match oid {
    1000U => Some(16U)
    1001U => Some(17U)
    1002U => Some(18U)
    1003U => Some(19U)
    1005U => Some(21U)
    1007U => Some(23U)
    1009U => Some(25U)
    1014U => Some(1042U)
    1015U => Some(1043U)
    1016U => Some(20U)
    1021U => Some(700U)
    1022U => Some(701U)
    1028U => Some(26U)
    1115U => Some(1114U)
    1182U => Some(1082U)
    1183U => Some(1083U)
    1185U => Some(1184U)
    1231U => Some(1700U)
    1270U => Some(1266U)
    199U => Some(114U)
    2951U => Some(2950U)
    3807U => Some(3802U)
    // Other built-in arrays recognized by the pinned rust-postgres type catalog
    // use ETL's String element fallback (including ranges, network and interval).
    143U
    | 271U
    | 629U
    | 651U
    | 719U
    | 775U
    | 791U
    | 1006U
    | 1008U
    | 1010U
    | 1011U
    | 1012U
    | 1013U
    | 1017U
    | 1018U
    | 1019U
    | 1020U
    | 1027U
    | 1034U
    | 1040U
    | 1041U
    | 1187U
    | 1263U
    | 1561U
    | 1563U
    | 2201U
    | 2207U
    | 2208U
    | 2209U
    | 2210U
    | 2211U
    | 2287U
    | 2949U
    | 3221U
    | 3643U
    | 3644U
    | 3645U
    | 3735U
    | 3770U
    | 3905U
    | 3907U
    | 3909U
    | 3911U
    | 3913U
    | 3927U
    | 4073U
    | 4090U
    | 4097U
    | 4192U
    | 5039U
    | 6150U
    | 6151U
    | 6152U
    | 6153U
    | 6155U
    | 6157U => Some(25U)
    _ => None
  }
}

///|
fn hex_nibble(byte : Byte) -> Int raise {
  match byte {
    b'0'..=b'9' => byte.to_int() - 48
    b'a'..=b'f' => byte.to_int() - 87
    b'A'..=b'F' => byte.to_int() - 55
    _ => raise Conversion("invalid hexadecimal value")
  }
}

///|
fn cell_hex(text : StringView) -> Bytes raise {
  let bytes = @utf8.encode(text)
  if bytes.length() % 2 != 0 {
    raise Conversion("odd hexadecimal length")
  }
  let out = @buffer.Buffer()
  for i = 0; i < bytes.length(); i = i + 2 {
    out.write_byte(
      (hex_nibble(bytes[i]) * 16 + hex_nibble(bytes[i + 1])).to_byte(),
    )
  }
  out.to_bytes()
}

///|
fn cell_uuid(text : String) -> Bytes raise {
  let text = if text.has_prefix("urn:uuid:") {
    text[9:]
  } else if text.has_prefix("{") && text.has_suffix("}") {
    text[1:text.length() - 1]
  } else {
    text[:]
  }
  if text.length() == 32 {
    return cell_hex(text)
  }
  if text.length() != 36 {
    raise Conversion("UUID length")
  }
  for index in [8, 13, 18, 23] {
    if text[index] != '-' {
      raise Conversion("UUID separators")
    }
  }
  cell_hex(text.to_owned().replace_all(old="-", new=""))
}

///|
fn numeric_text(text : String) -> String raise {
  let text = text.trim().to_owned().to_lower()
  let negative = text.has_prefix("-")
  let signed = negative || text.has_prefix("+")
  let value = if signed { text[1:].to_owned() } else { text }
  match value {
    "nan" if !signed => return "NaN"
    "inf" | "infinity" => return if negative { "-Infinity" } else { "Infinity" }
    _ => ()
  }
  lexmatch value {
    re"^([0-9](_?[0-9])*(\.([0-9](_?[0-9])*)?)?|\.[0-9](_?[0-9])*)(e(\+|-)?[0-9](_?[0-9])*)?$" =>
      ()
    _ => raise Conversion("invalid numeric value")
  }
  let parts = value.replace_all(old="_", new="").split("e").to_array()
  let exponent = if parts.length() == 2 {
    @string.parse_int64(parts[1])
  } else {
    0L
  }
  if exponent < -1073741823L || exponent > 1073741823L {
    raise Conversion("numeric exponent range")
  }
  let mantissa = parts[0].split(".").to_array()
  let fraction = if mantissa.length() == 2 {
    mantissa[1].to_owned()
  } else {
    ""
  }
  let digits = mantissa[0].to_owned() + fraction
  let scale = Int64::max(fraction.length().to_int64() - exponent, 0)
  if scale > 16383L {
    raise Conversion("numeric scale range")
  }
  let mut first = 0
  while first < digits.length() && digits[first] == '0' {
    first += 1
  }
  if first == digits.length() {
    return "0" +
      (if scale == 0 { "" } else { "." + "0".repeat(scale.to_int()) })
  }
  let position = mantissa[0].length().to_int64() + exponent
  let weight = position - first.to_int64() - 1
  // The first nonzero base-10000 group must fit PostgreSQL's signed i16 weight.
  if weight < -131072L || weight > 131071L {
    raise Conversion("numeric weight range")
  }
  let out = StringBuilder()
  if negative {
    out.write_string("-")
  }
  if position <= first.to_int64() {
    out.write_string("0")
  } else {
    for index = first.to_int64(); index < position; index = index + 1 {
      out.write_char(
        if index < digits.length().to_int64() {
          digits[index.to_int()].to_char().unwrap()
        } else {
          '0'
        },
      )
    }
  }
  if scale > 0 {
    out.write_string(".")
    for index = position; index < position + scale; index = index + 1 {
      out.write_char(
        if index >= 0 && index < digits.length().to_int64() {
          digits[index.to_int()].to_char().unwrap()
        } else {
          '0'
        },
      )
    }
  }
  out.to_string()
}

///|
fn scalar_cell(oid : UInt, text : String) -> Cell raise {
  if oid == 21U || oid == 23U || oid == 20U || oid == 26U {
    lexmatch text {
      re"^(\+|-)?[0-9]+$" => ()
      _ => raise Conversion("integer requires decimal digits")
    }
  }
  match oid {
    16U =>
      match text {
        "t" => Bool(true)
        "f" => Bool(false)
        _ => raise Conversion("boolean must be t or f")
      }
    21U => {
      let value = @string.parse_int(text, base=10)
      if value < -32768 || value > 32767 {
        raise Conversion("int2 overflow")
      }
      I16(Int16::from_int(value))
    }
    23U => I32(@string.parse_int(text, base=10))
    20U => I64(@string.parse_int64(text, base=10))
    26U =>
      U32(
        @string.parse_uint(
          if text.has_prefix("+") {
            text[1:]
          } else {
            text[:]
          },
          base=10,
        ),
      )
    700U => F32(Float::from_double(pg_float(text, single=true)))
    701U => F64(pg_float(text, single=false))
    1700U => Numeric(numeric_text(text))
    17U => {
      if !text.has_prefix("\\x") {
        raise Conversion("bytea requires hexadecimal output")
      }
      Bytes(cell_hex(text[2:]))
    }
    2950U => Uuid(cell_uuid(text))
    114U | 3802U => Json(pg_json(text))
    1082U => Date(pg_date(text))
    1083U => Time(pg_time(text))
    1266U => {
      let (clock_text, offset) = split_pg_offset(text)
      TimeTz(pg_time(clock_text), offset_seconds=offset)
    }
    1114U => Timestamp(pg_timestamp(text, false))
    1184U => TimestampTz(pg_timestamp(text, true))
    _ => String(text)
  }
}

///|
/// Decode text with its PostgreSQL OID. Supply an element OID for catalog-discovered
/// custom arrays. One-dimensional array lower bounds are discarded as in ETL.
pub fn Value::decode_text(
  self : Value,
  type_oid : UInt,
  array_element_oid? : UInt,
) -> Cell raise {
  try {
    match self {
      Null => SqlNull
      UnchangedToast => ToastUnchanged
      Binary(_) => raise Conversion("decode_text requires text transfer")
      Text(text) => {
        let element = match array_element_oid {
          Some(oid) => Some(oid)
          None => array_element(type_oid)
        }
        match element {
          Some(oid) => Array(pg_array(text, oid))
          None => scalar_cell(type_oid, text)
        }
      }
    }
  } catch {
    Conversion(message) => raise Conversion(message)
    _ => raise Conversion("invalid PostgreSQL text for type OID \{type_oid}")
  }
}
