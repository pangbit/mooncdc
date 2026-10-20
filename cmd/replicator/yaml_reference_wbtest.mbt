///|
async test "YAML config matches pinned Rust config fixture outcomes" {
  guard @json.parse(
      @fs.read_file("integration/reference/yaml-cases.json").text(),
    )
    is Array(cases) else {
    fail("cases")
  }
  guard @json.parse(
      @fs.read_file("integration/reference/yaml-expected.json").text(),
    )
    is Array(expected) else {
    fail("expected")
  }
  assert_eq(cases.length(), expected.length())
  for index, case in cases {
    guard case is { "name": String(name), "source": String(source), .. } else {
      fail("case")
    }
    let result = try @yaml.parse_config(source) catch {
      _ => None
    } noraise {
      v => Some(v)
    }
    match expected[index] {
      { "name": String(expected_name), "value": value, .. } => {
        assert_eq(name, expected_name)
        if result != Some(value) {
          println(
            "\{name}: actual=\{result.map(v => v.stringify()).unwrap_or("error")}; expected=\{value.stringify()}",
          )
        }
        assert_eq(result, Some(value), msg=name)
      }
      { "name": String(expected_name), "error": True, .. } => {
        assert_eq(name, expected_name)
        assert_true(result is None, msg=name)
      }
      _ => fail("expected outcome")
    }
  }
}
