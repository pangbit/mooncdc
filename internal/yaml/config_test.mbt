///|
test "configuration scalar rules match yaml-rust2 rather than YAML 1.1" {
  let value = @yaml.parse_config(
    "a: true\nb: True\nc: yes\nd: 0123\ne: 0o17\nf: 0x10\ng: null\nh: Null\ni: !!int invalid\nj: !!str 12\nk: \"true\"\nl: 1.5\n",
  )
  json_inspect(value, content={
    "a": true,
    "b": "True",
    "c": "yes",
    "d": 123,
    "e": 15,
    "f": 16,
    "g": null,
    "h": "Null",
    "i": null,
    "j": "12",
    "k": "true",
    "l": 1.5,
  })
}

///|
test "verbatim tags advance and preserve pinned URI escape behavior" {
  let value = @yaml.parse_config(
    "a: !<tag:yaml.org,2002:str> 12\nb: !<tag:example.com,%41> 3\nc: |\n  first\n  second\nd: >-\n  hello\n  world\n",
  )
  json_inspect(value, content={
    "a": "12",
    "b": "3",
    "c": "first\nsecond\n",
    "d": "hello world",
  })
  for source in ["a: !<%E4%B8%AD> 1", "a: !<%ED%A0%80> 1", "a: !<%FF> 1"] {
    try @yaml.parse_config(source) catch {
      _ => ()
    } noraise {
      _ => fail("invalid UTF8 tag accepted")
    }
  }
}

///|
test "alias expansion empty keys and integer keys" {
  let value = @yaml.parse_config("a: &x {\"\": 1, 2: [true, ~]}\nb: *x\n")
  json_inspect(value, content={
    "a": { "": 1, "2": [true, null] },
    "b": { "": 1, "2": [true, null] },
  })
  assert_true(@yaml.parse_config("") is Object(fields) && fields.is_empty())
}

///|
test "malformed configuration and resource limits fail" {
  for
    source in [
      "a: 1\na: 2", "a: *missing", "[]", "---\n", "a: 1\n---\nb: 2", "true: x", "a: .inf",
      "a: Infinity", "a: NaN",
    ] {
    try @yaml.parse_config(source) catch {
      _ => ()
    } noraise {
      _ => fail("invalid config accepted: " + source)
    }
  }
  let flow = "a: " + "[".repeat(70) + "0" + "]".repeat(70)
  let mut block = ""
  for i in 0..<70 {
    block += " ".repeat(i * 2) + "a:\n"
  }
  let mut aliases = "a0: &a0 [x,x,x,x]\n"
  for i in 1..<16 {
    aliases += "a\{i}: &a\{i} [*a\{i-1},*a\{i-1},*a\{i-1},*a\{i-1}]\n"
  }
  let mut alias_depth = "a0: &a0 [x]\n"
  for i in 1..<70 {
    alias_depth += "a\{i}: &a\{i} [*a\{i-1}]\n"
  }
  for source in [flow, block, aliases, alias_depth, "x".repeat(1048577)] {
    try @yaml.parse_config(source) catch {
      _ => ()
    } noraise {
      _ => fail("resource limit not enforced")
    }
  }
}
