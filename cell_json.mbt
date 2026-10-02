///|
// Encode every string token with an 's' prefix and every number as an 'n'
// string before using the core parser. Tagging *all* strings prevents collisions
// with user data. This keeps core's grammar/Unicode checks without its lossy
// Double conversion. Object keys must still be original string tokens.
fn pg_json(text : String) -> Json raise {
  let encoded = StringBuilder()
  let mut i = 0
  let mut depth = 0
  while i < text.length() {
    let start = i
    match text[i] {
      '"' => {
        encoded.write_string("\"s")
        i += 1
        let content = i
        while i < text.length() {
          if text[i] == '\\' {
            i += 2
          } else if text[i] == '"' {
            break
          } else {
            i += 1
          }
        }
        if i >= text.length() {
          raise Conversion("unterminated JSON string")
        }
        i += 1
        encoded.write_view(text[content:i])
      }
      '-' | '0'..='9' => {
        i += 1
        while i < text.length() {
          match text[i] {
            '0'..='9' | '.' | 'e' | 'E' | '+' | '-' => i += 1
            _ => break
          }
        }
        let token = text[start:i]
        lexmatch token {
          re"^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE](\+|-)?[0-9]+)?$" => ()
          _ => raise Conversion("invalid JSON number")
        }
        encoded.write_string("\"n")
        encoded.write_view(token)
        encoded.write_char('"')
      }
      _ => {
        // Non-ASCII outside a quoted string is invalid JSON.
        let unit = text[i]
        if unit > 127 {
          raise Conversion("invalid JSON token")
        }
        match unit {
          '{' | '[' => {
            depth += 1
            // serde_json permits at most 127 nested containers.
            if depth >= 128 {
              raise Conversion("JSON depth limit exceeded")
            }
          }
          '}' | ']' => depth -= 1
          _ => ()
        }
        encoded.write_char(unit.to_int().unsafe_to_char())
        i += 1
      }
    }
  }
  restore_pg_json(@json.parse(encoded.to_string()))
}

///|
fn restore_pg_json(value : Json) -> Json raise {
  match value {
    String(text) => {
      if text.has_prefix("s") {
        return Json::string(text[1:].to_owned())
      }
      let token = text[1:].to_owned()
      let mut repr = token.replace_all(old="E", new="e")
      if repr == "-0" {
        repr = "0"
      } else if repr.find("e") is Some(pos) {
        if repr[pos + 1] != '+' && repr[pos + 1] != '-' {
          repr = repr[:pos + 1].to_owned() + "+" + repr[pos + 1:].to_owned()
        }
      }
      Json::number(pg_float(token, single=false), repr~)
    }
    Array(items) => Json::array(items.map(restore_pg_json))
    Object(fields) => {
      let restored : Map[String, Json] = Map([])
      for key, value in fields {
        if !key.has_prefix("s") {
          raise Conversion("JSON object key must be a string")
        }
        restored[key[1:].to_owned()] = restore_pg_json(value)
      }
      Json::object(restored)
    }
    _ => value
  }
}

///|
fn pg_json_equal(left : Json, right : Json) -> Bool {
  match (left, right) {
    (Number(a, repr=ar), Number(b, repr=br)) =>
      ar.unwrap_or(a.to_string()) == br.unwrap_or(b.to_string())
    (Array(a), Array(b)) =>
      a.length() == b.length() &&
      a.iter().zip(b.iter()).all(pair => pg_json_equal(pair.0, pair.1))
    (Object(a), Object(b)) =>
      a.length() == b.length() &&
      a
      .iter()
      .all(pair => b.get(pair.0) is Some(value) && pg_json_equal(pair.1, value))
    _ => left == right
  }
}
