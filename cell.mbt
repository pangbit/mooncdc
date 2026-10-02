///|
/// Typed PostgreSQL text values. Numeric retains exact decimal text, including scale.
/// Unknown scalar types retain text; TOAST is distinct from both SQL and JSON null.
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
} derive(Eq, Debug)

///|
pub(all) enum PgDate {
  Finite(year~ : Int, month~ : Int, day~ : Int)
  PosInfinity
  NegInfinity
} derive(Eq, Debug)

///|
/// seconds=86400 and nanoseconds=0 denotes the distinct 24:00:00 value.
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
  match text {
    "NaN" | "Infinity" | "-Infinity" => return text
    "+Infinity" => return "Infinity"
    _ => ()
  }
  lexmatch text {
    re"^(\+|-)?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE](\+|-)?[0-9]+)?$" => text
    _ => raise Conversion("invalid numeric value")
  }
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
    26U => U32(@string.parse_uint(text, base=10))
    700U => F32(Float::from_double(@string.parse_double(text)))
    701U => F64(@string.parse_double(text))
    1700U => Numeric(numeric_text(text))
    17U => {
      if !text.has_prefix("\\x") {
        raise Conversion("bytea requires hexadecimal output")
      }
      Bytes(cell_hex(text[2:]))
    }
    2950U => Uuid(cell_uuid(text))
    114U | 3802U => Json(@json.parse(text))
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
