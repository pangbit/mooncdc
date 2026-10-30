///|
priv struct Value {
  json : Json
  key : (String, String)?
  size : Int32
  height : Int32
}

///|
fn invalid(mark : Marker, info : String) -> YamlError {
  YamlError(mark~, info~)
}

///|
fn string_value(text : String) -> Value {
  {
    json: Json::string(text),
    key: Some(("s" + text, text)),
    size: text.length() + 1,
    height: 0,
  }
}

///|
fn integer_value(value : Int64) -> Value {
  let text = value.to_string()
  {
    json: Json::number(value.to_double(), repr=text),
    key: Some(("i" + text, text)),
    size: 1,
    height: 0,
  }
}

///|
fn decimal_integer(text : String) -> Int64? {
  let digits = match text {
    ['+' | '-', .. rest] => rest
    _ => text[:]
  }
  if digits.is_empty() || !digits.iter().all(c => c.is_ascii_digit()) {
    return None
  }
  Some(@string.parse_int64(text, base=10)) catch {
    _ => None
  }
}

///|
fn scalar(
  value : String,
  style : TScalarStyle,
  tag : Tag?,
  mark : Marker,
) -> Value raise {
  let nil = { json: Json::null(), key: None, size: 1, height: 0, }
  if style != Plain {
    return string_value(value)
  }
  let kind = match tag {
    Some({ handle: "tag:yaml.org,2002:", suffix, }) => Some(suffix)
    Some(_) => return string_value(value)
    None => None
  }
  match kind {
    Some("null") => return nil
    Some("bool") =>
      return match value {
        "true" => { ..nil, json: Json::boolean(true), }
        "false" => { ..nil, json: Json::boolean(false), }
        _ => nil
      }
    Some("int") =>
      return decimal_integer(value).map(integer_value).unwrap_or(nil)
    Some("float") | None => ()
    Some(_) => return string_value(value)
  }
  if kind is None {
    match value {
      "" | "~" | "null" => return nil
      "true" => return { ..nil, json: Json::boolean(true), }
      "false" => return { ..nil, json: Json::boolean(false), }
      _ => ()
    }
    let integer = if value.contains("_") {
      None
    } else {
      match value {
        ['0', 'x', .. digits] =>
          Some(@string.parse_int64(digits, base=16)) catch {
            _ => None
          }
        ['0', 'o', .. digits] =>
          Some(@string.parse_int64(digits, base=8)) catch {
            _ => None
          }
        _ => decimal_integer(value)
      }
    }
    if integer is Some(n) {
      return integer_value(n)
    }
  }
  let number = if value
    .iter()
    .all(c => c.is_ascii_digit() || c is ('+' | '-' | '.' | 'e' | 'E')) {
    try @string.parse_double(value) catch {
      _ => None
    } noraise {
      n => Some(n)
    }
  } else {
    None
  }
  if [
      "inf", "+inf", "-inf", "infinity", "+infinity", "-infinity", "nan", "+nan",
      "-nan",
    ].contains(value.to_lower()) {
    raise invalid(mark, "non-finite number")
  }
  if number is Some(n) {
    if n.is_nan() || n.is_inf() {
      raise invalid(mark, "non-finite number")
    }
    return { ..nil, json: Json::number(n), }
  }
  if [
      ".inf", ".Inf", ".INF", "+.inf", "+.Inf", "+.INF", "-.inf", "-.Inf", "-.INF",
      ".nan", ".NAN",
    ].contains(value) {
    raise invalid(mark, "non-finite number")
  }
  if kind is Some("float") {
    nil
  } else {
    string_value(value)
  }
}

///|
fn read_node(
  parser : Parser,
  event : Event,
  mark : Marker,
  anchors : Map[Int32, Value],
  depth : Int32,
) -> Value raise {
  if depth > 64 {
    raise invalid(mark, "configuration nesting exceeds 64")
  }
  let (value, id) = match event {
    Scalar(value~, style~, id~, tag~) => (scalar(value, style, tag, mark), id)
    Alias(id~) =>
      (
        anchors
        .get(id)
        .unwrap_or({ json: Json::null(), key: None, size: 1, height: 0, }),
        0,
      )
    SequenceStart(id~, ..) => {
      let items = []
      let mut size = 1
      let mut height = 0
      while true {
        let (event, mark) = parser.next_token()
        if event is SequenceEnd {
          break
        }
        let node = read_node(parser, event, mark, anchors, depth + 1)
        size += node.size
        height = Int32::max(height, node.height + 1)
        if size > 1048576 {
          raise invalid(mark, "expanded configuration exceeds budget")
        }
        items.push(node.json)
      }
      ({ json: Json::array(items), key: None, size, height, }, id)
    }
    MappingStart(id~, ..) => {
      let fields = Map([])
      let keys = Map([])
      let mut size = 1
      let mut height = 0
      while true {
        let (event, mark) = parser.next_token()
        if event is MappingEnd {
          break
        }
        let key = read_node(parser, event, mark, anchors, depth + 1)
        guard key.key is Some((identity, text)) else {
          raise invalid(mark, "mapping key must be string or integer")
        }
        if keys.contains(identity) {
          raise invalid(mark, "duplicate mapping key")
        }
        keys[identity] = true
        let (event, mark) = parser.next_token()
        let node = read_node(parser, event, mark, anchors, depth + 1)
        size += key.size + node.size
        height = Int32::max(height, node.height + 1)
        if size > 1048576 {
          raise invalid(mark, "expanded configuration exceeds budget")
        }
        fields[text] = node.json
      }
      ({ json: Json::object(fields), key: None, size, height, }, id)
    }
    _ => raise invalid(mark, "expected configuration value")
  }
  if depth + value.height > 64 || value.size > 1048576 {
    raise invalid(mark, "expanded configuration exceeds budget")
  }
  if id > 0 {
    anchors[id] = value
  }
  value
}

///|
/// Read a single configuration mapping using the pinned config/yaml-rust2
/// scalar rules. Limits bound both syntax nesting and expanded aliases.
pub fn parse_config(source : String) -> Json raise {
  if source.length() > 1048576 {
    raise invalid(
      { index: 0, line: 1, col: 0, },
      "configuration exceeds input budget",
    )
  }
  let parser = Parser::new(source[:])
  let anchors = Map([])
  let mut root = Json::object(Map([]))
  let mut documents = 0
  while true {
    let (event, mark) = parser.next_token()
    match event {
      StreamStart => ()
      StreamEnd => break
      DocumentStart => {
        documents += 1
        if documents > 1 {
          raise invalid(mark, "multiple configuration documents")
        }
        let (event, mark) = parser.next_token()
        root = read_node(parser, event, mark, anchors, 0).json
        if !(root is Object(_)) {
          raise invalid(mark, "configuration root must be mapping")
        }
      }
      DocumentEnd => ()
      _ => raise invalid(mark, "expected configuration document")
    }
  }
  root
}
