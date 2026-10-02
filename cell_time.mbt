///|
fn decimal_part(text : StringView) -> Int raise {
  if text.is_empty() || !text.iter().all(ch => ch >= '0' && ch <= '9') {
    raise Conversion("temporal digits")
  }
  @string.parse_int(text)
}

///|
fn month_days(year : Int, month : Int) -> Int raise {
  match month {
    1 | 3 | 5 | 7 | 8 | 10 | 12 => 31
    4 | 6 | 9 | 11 => 30
    2 =>
      if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
        29
      } else {
        28
      }
    _ => raise Conversion("invalid month")
  }
}

///|
fn pg_date(text : String) -> PgDate raise {
  if text == "infinity" {
    return PosInfinity
  }
  if text == "-infinity" {
    return NegInfinity
  }
  let bc = text.has_suffix(" BC")
  let value = if bc { text[:text.length() - 3] } else { text[:] }
  let parts = value.split("-").to_array()
  if parts.length() != 3 || parts[1].length() != 2 || parts[2].length() != 2 {
    raise Conversion("date syntax")
  }
  let magnitude = decimal_part(parts[0])
  if magnitude == 0 {
    raise Conversion("PostgreSQL has no year zero")
  }
  let year = if bc { 1 - magnitude } else { magnitude }
  let month = decimal_part(parts[1])
  let day = decimal_part(parts[2])
  // Match the finite calendar range of the primary reference's chrono values.
  if year < -262143 ||
    year > 262142 ||
    day <= 0 ||
    day > month_days(year, month) {
    raise Conversion("date range")
  }
  Finite(year~, month~, day~)
}

///|
fn pg_time(text : String) -> PgTime raise {
  let parts = text.split(":").to_array()
  if parts.length() != 3 || parts[0].length() != 2 || parts[1].length() != 2 {
    raise Conversion("time syntax")
  }
  let seconds = parts[2].split(".").to_array()
  if seconds.length() > 2 || seconds[0].length() != 2 {
    raise Conversion("time seconds")
  }
  let hour = decimal_part(parts[0])
  let minute = decimal_part(parts[1])
  let second = decimal_part(seconds[0])
  let mut nanoseconds = 0
  if seconds.length() == 2 {
    let fraction = seconds[1]
    if fraction.is_empty() || fraction.length() > 9 {
      raise Conversion("time fraction")
    }
    nanoseconds = decimal_part(fraction)
    for _ in fraction.length()..<9 {
      nanoseconds *= 10
    }
  }
  if hour > 24 ||
    minute > 59 ||
    second > 59 ||
    (hour == 24 && (minute != 0 || second != 0 || nanoseconds != 0)) {
    raise Conversion("time range")
  }
  { seconds: hour * 3600 + minute * 60 + second, nanoseconds, }
}

///|
fn split_pg_offset(text : String) -> (String, Int) raise {
  let mut sign_at = text.length() - 1
  while sign_at >= 0 && text[sign_at] != '+' && text[sign_at] != '-' {
    sign_at -= 1
  }
  if sign_at <= 0 {
    raise Conversion("numeric UTC offset missing")
  }
  let value = text[sign_at + 1:]
  let parts = if value.contains(":") {
    value.split(":").to_array()
  } else {
    match value.length() {
      2 => [value]
      4 => [value[:2], value[2:]]
      6 => [value[:2], value[2:4], value[4:]]
      _ => raise Conversion("UTC offset shape")
    }
  }
  if parts.length() < 1 ||
    parts.length() > 3 ||
    parts.any(part => part.length() != 2) {
    raise Conversion("UTC offset shape")
  }
  let hour = decimal_part(parts[0])
  let minute = if parts.length() >= 2 { decimal_part(parts[1]) } else { 0 }
  let second = if parts.length() == 3 { decimal_part(parts[2]) } else { 0 }
  if hour >= 16 || minute >= 60 || second >= 60 {
    raise Conversion("UTC offset range")
  }
  let offset = hour * 3600 + minute * 60 + second
  (
    text[:sign_at].to_owned(),
    if text[sign_at] == '-' {
      -offset
    } else {
      offset
    },
  )
}

///|
fn shift_date(date : PgDate, delta : Int) -> PgDate raise {
  guard date is Finite(year~, month~, day~) else {
    raise Conversion("infinite local date")
  }
  let mut year = year
  let mut month = month
  let mut day = day + delta
  if day == 0 {
    month -= 1
    if month == 0 {
      month = 12
      year -= 1
    }
    day = month_days(year, month)
  } else if day > month_days(year, month) {
    day = 1
    month += 1
    if month == 13 {
      month = 1
      year += 1
    }
  }
  if year < -262143 || year > 262142 {
    raise Conversion("UTC timestamp calendar range")
  }
  Finite(year~, month~, day~)
}

///|
fn pg_timestamp(text : String, zoned : Bool) -> PgTimestamp raise {
  if text == "infinity" {
    return PosInfinity
  }
  if text == "-infinity" {
    return NegInfinity
  }
  let bc = text.has_suffix(" BC")
  let value = if bc { text[:text.length() - 3].to_owned() } else { text }
  let (clock_text, offset) = if zoned {
    split_pg_offset(value)
  } else {
    (value, 0)
  }
  let parts = clock_text.split(" ").to_array()
  if parts.length() != 2 {
    raise Conversion("timestamp syntax")
  }
  let date = pg_date(parts[0].to_owned() + (if bc { " BC" } else { "" }))
  let time = pg_time(parts[1].to_owned())
  if time.seconds == 86400 {
    raise Conversion("timestamp requires normalized date")
  }
  let seconds = time.seconds - offset
  let delta = if seconds < 0 { -1 } else if seconds >= 86400 { 1 } else { 0 }
  Finite(date=shift_date(date, delta), time={
    seconds: seconds - delta * 86400,
    nanoseconds: time.nanoseconds,
  })
}
