///|
test "PostgreSQL password preparation maps and normalizes Unicode" {
  for
    (input, expected) in [
      ("secret", "secret"),
      ("", ""),
      ("a\u0007b", "a\u0007b"),
      ("密碼", "密碼"),
      ("I\u00ADX", "IX"),
      ("a\u00A0b", "a b"),
      ("a\u200Bb", "a b"),
      ("\u00AA", "a"),
      ("\u2168", "IX"),
      ("Cafe\u0301", "Café"),
      ("\u212B", "Å"),
      ("\u{1D400}", "A"),
      ("\u1100\u1161\u11A8", "각"),
      ("\u0627\u0031\u0628", "\u0627\u0031\u0628"),
      // Unicode 4.0 normalization correction, retained by PG18's Unicode 16.0.
      ("\u{2F868}", "\u36FC"),
    ] {
    assert_eq(@saslprep.password(input), expected)
  }
}

///|
test "PostgreSQL preparation failure preserves the entire original password" {
  for
    input in [
      "\u00AD", // Mapping to an empty password fails.
       "a\u00AD\u0007", // Prohibited control, including a mapping to undo.
       "a\u00A0\uE000", // Private use, including a space mapping to undo.
       "\u0340", // Mapped input is prohibited even though NFKC yields U+0300.
       "\u1D2C", // Unassigned in 3.2 even though NFKC yields ASCII A.
       "x\u00AD😀", // Supplementary unassigned-in-3.2 character.
       "\u0627a\u0628", // RandAL plus L.
       "1\u0627", // RandAL is not first.
       "\u06271", // RandAL is not last.
       "\u00AD\u0627a\u0628", // Bidi failure undoes prior mapping.
    ] {
    assert_eq(@saslprep.password(input), input)
  }
}
