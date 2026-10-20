# Configuration YAML syntax

The eight syntax/event files are vendored from
[moonbit-community/yaml.mbt](https://github.com/moonbit-community/yaml.mbt/tree/e2f1129249a7cc707aa1c9c39bd8bfe207c93821),
version 0.0.7, commit `e2f1129249a7cc707aa1c9c39bd8bfe207c93821`.
Archive SHA-256: `3a5d8124a04409a0176f976603225f5ffea75c42fe1c0bef50ef68bafe7ee471`.
The MIT license, including the original yaml-rust authors, is retained in `LICENSE`.

Local syntax changes: advance the lexer in `scan_verbatim_tag` to prevent an
infinite loop; lower flow nesting from 255 to 64. No original YAML loader or
emitter is used. `config.mbt` is MoonCDC's bounded configuration loader, matching
the scalar rules used by ETL's pinned `config 0.14.1` / `yaml-rust2 0.8.1`.
This includes lowercase-only implicit booleans, literal merge keys, and invalid
explicit scalar conversions becoming null. Empty mapping keys and integer keys
are supported; duplicate keys are rejected before string-key normalization.

The loader limits nesting to 64 and expanded node/scalar/key cost to 1,048,576.
Repeated aliases include their entire expanded cost and height, so shared nodes
cannot bypass the limits. The CLI also caps file bytes at 1 MiB. Non-finite
numbers and non-string/non-integer mapping keys are rejected; the reference may
return non-finite values or panic for unsupported keys. These are intentional
input-domain restrictions. The reference's multibyte URI tag escape behavior
is retained (the Chinese escape vector is rejected in both implementations).

`integration/reference/yaml-cases.json` and `yaml-expected.json` record 34 outcomes
from the fixed Rust dependencies. Run `moon run tools/config-reference-test.mbtx`
to regenerate the reference in a temporary project and verify the saved results
and MoonCDC behavior; `-- --record` deliberately refreshes the fixture.
