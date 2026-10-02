///|
// Portable literal subset of the pinned ETL default parser. SQL expressions
// are never copied into ClickHouse; decoded literals are quoted in its dialect.
fn ch_literal_end(value : String, start : Int) -> Int? {
  if start >= value.length() || value[start] != '\'' {
    return None
  }
  let mut i = start + 1
  while i < value.length() {
    if value[i] == '\'' {
      if i + 1 < value.length() && value[i + 1] == '\'' {
        i += 2
      } else {
        return Some(i + 1)
      }
    } else {
      i += 1
    }
  }
  None
}

///|
fn ch_normalize_default(input : String) -> String {
  let mut text = input.trim().to_owned()
  for ;; {
    let before = text
    let mut depth = 0
    let mut binary = false
    let mut i = 0
    while i < text.length() {
      let ch = text[i]
      if ch == '\'' {
        i = ch_literal_end(text, i).unwrap_or(text.length())
        continue
      }
      if ch == '(' {
        depth += 1
      }
      if ch == ')' {
        depth = Int::max(depth - 1, 0)
      }
      if depth == 0 &&
        (
          ch == '*' ||
          ch == '/' ||
          ch == '%' ||
          (i > 0 && (ch == '+' || ch == '-')) ||
          (ch == '|' && i + 1 < text.length() && text[i + 1] == '|')
        ) {
        binary = true
      }
      if depth == 0 && ch == ':' && i + 1 < text.length() && text[i + 1] == ':' {
        let suffix = text[i + 2:].trim()
        if !binary &&
          !suffix.is_empty() &&
          suffix
          .iter()
          .all(c => {
            (c >= 'a' && c <= 'z') ||
            (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') ||
            "_ \".[](),".contains(c.to_string())
          }) {
          text = text[:i].trim().to_owned()
        }
        break
      }
      i += 1
    }
    if text.has_prefix("(") && text.has_suffix(")") {
      depth = 0
      i = 0
      let mut outer = true
      while i < text.length() {
        let ch = text[i]
        if ch == '\'' {
          i = ch_literal_end(text, i).unwrap_or(text.length())
          continue
        }
        if ch == '(' {
          depth += 1
        }
        if ch == ')' {
          depth -= 1
          if depth < 0 || (depth == 0 && i != text.length() - 1) {
            outer = false
            break
          }
        }
        i += 1
      }
      if outer && depth == 0 {
        text = text[1:text.length() - 1].trim().to_owned()
      }
    }
    if text == before {
      return text
    }
  }
}

///|
fn ch_numeric_literal(value : String) -> Bool {
  lexmatch value {
    re"^(\+|-)?([0-9]+(\.[0-9]*)?|\.[0-9]+)$" => true
    _ => false
  }
}

///|
fn ch_default_expression(input : String, oid : UInt) -> String? {
  let value = ch_normalize_default(input)
  let lower = value.to_lower()
  if value.is_empty() ||
    lower == "null" ||
    lower.has_prefix("nextval(") ||
    lower.contains("select ") ||
    value.contains("::") ||
    lower.has_prefix("array[") ||
    lower.has_prefix("array ") {
    return None
  }
  let text_type = oid == 18U ||
    oid == 19U ||
    oid == 25U ||
    oid == 1042U ||
    oid == 1043U
  let numeric_type = oid == 21U ||
    oid == 23U ||
    oid == 20U ||
    oid == 700U ||
    oid == 701U ||
    oid == 26U
  if ch_literal_end(value, 0) == Some(value.length()) {
    let decoded = value[1:value.length() - 1]
      .to_owned()
      .replace_all(old="''", new="'")
    if oid == 16U {
      return if decoded.to_lower() == "true" || decoded.to_lower() == "false" {
        Some(decoded)
      } else {
        None
      }
    }
    if numeric_type || oid == 1700U {
      if !ch_numeric_literal(decoded) {
        return None
      }
      return Some(if oid == 1700U { ch_quote(decoded, '\'') } else { decoded })
    }
    let literal = ch_quote(decoded, '\'')
    match oid {
      1082U => Some("toDate32(" + literal + ")")
      1114U => Some("toDateTime64(" + literal + ", 6, 'UTC')")
      1184U => Some("parseDateTime64BestEffort(" + literal + ", 6, 'UTC')")
      1083U | 1266U | 1186U | 114U | 3802U | 790U | 2950U => Some(literal)
      _ => if text_type { Some(literal) } else { None }
    }
  } else if ch_numeric_literal(value) {
    if numeric_type {
      Some(value)
    } else if text_type || oid == 1700U || oid == 790U {
      Some(ch_quote(value, '\''))
    } else {
      None
    }
  } else if lower == "true" || lower == "false" {
    if oid == 16U {
      Some(value)
    } else if text_type {
      Some(ch_quote(value, '\''))
    } else {
      None
    }
  } else {
    None
  }
}

///|
fn ch_default_clause(column : SchemaColumn) -> String {
  match column.default_expression {
    Some(expression) =>
      match ch_default_expression(expression, column.type_oid) {
        Some(value) => " DEFAULT " + value
        None => ""
      }
    None => ""
  }
}
