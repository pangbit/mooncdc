///|
fn pg_array(input : String, element : UInt) -> ReadOnlyArray[Cell] raise {
  let mut text = input[:]
  if text.has_prefix("[") {
    let mut end = 1
    while end < text.length() && text[end] != ']' {
      end += 1
    }
    if end + 1 >= text.length() || text[end + 1] != '=' {
      raise Conversion("array dimensions")
    }
    let dimensions = text[1:end].split(":").to_array()
    if dimensions.length() != 2 {
      raise Conversion("array dimensions")
    }
    for dimension in dimensions {
      lexmatch dimension {
        re"^-?[0-9]+$" => ()
        _ => raise Conversion("array bound")
      }
    }
    text = text[end + 2:]
  }
  if text.length() < 2 || !text.has_prefix("{") || !text.has_suffix("}") {
    raise Conversion("array braces or multiple dimensions")
  }
  if text.length() == 2 {
    return []
  }
  let values : Array[Cell] = []
  let value = StringBuilder()
  let mut quoted = false
  let mut escaped = false
  let mut was_quoted = false
  for ch in text[1:text.length() - 1].iter() {
    if escaped {
      value.write_char(ch)
      escaped = false
      continue
    }
    match ch {
      '\\' => escaped = true
      '"' => {
        quoted = !quoted
        was_quoted = true
      }
      '{' | '}' if !quoted =>
        raise Conversion("multidimensional array unsupported")
      ',' if !quoted => {
        let item = value.to_string()
        values.push(
          if !was_quoted && item.to_upper() == "NULL" {
            SqlNull
          } else {
            scalar_cell(element, item)
          },
        )
        value.reset()
        was_quoted = false
      }
      _ => value.write_char(ch)
    }
  }
  if quoted || escaped {
    raise Conversion("unterminated array quote or escape")
  }
  let item = value.to_string()
  values.push(
    if !was_quoted && item.to_upper() == "NULL" {
      SqlNull
    } else {
      scalar_cell(element, item)
    },
  )
  ReadOnlyArray::from_array(values)
}
