///|
test "NFKC canonical ordering blocking exclusions and recursive composition" {
  for
    (input, expected) in [
      ("\u0301\u0323", "\u0323\u0301"),
      ("D\u0307\u0323", "\u1E0C\u0307"),
      ("A\u0305\u0301", "A\u0305\u0301"),
      ("A\u030A\u0301", "\u01FA"),
      ("\u0958", "\u0915\u093C"),
      ("\uAC01", "\uAC01"),
    ] {
    assert_eq(nfkc(input.iter().map(c => c.to_int()).collect()), expected)
  }
}
