# Release review checklist

Version: 0.1.0 candidate; date: 2026-10-20. This is a gate list, not a publication
receipt. Bind each result to the reviewed source inventory and actual exit code.

- [ ] Review product/test changes against the public base, including native C
  dependencies, generated interfaces and all required license files.
- [ ] Confirm package contents against the explicit allowlist and scan all
  included text; raw logs, host dumps, archives and extensions stay excluded.
- [ ] Run affected offline checks after maintenance-tool/documentation changes.
- [ ] Record which historical results are reusable by byte identity and which
  new inputs need execution. See [validation](VALIDATION.md).
- [ ] Obtain explicit authorization for exact local commits and branch push/PR.
- [ ] Verify the resulting public source commit and its own GitHub CI receipt.
- [ ] Confirm the authenticated Mooncakes account's right to `pangbit`; repository
  ownership or anonymous namespace existence does not establish this right.
- [ ] Separately authorize an upload-type dry run and/or actual publication.
- [ ] After publication, run an exact-version registry consumer without local
  paths, workspace membership, symlinks or dependency overrides.

See [releasing](RELEASING.md) for the order and [testing](TESTING.md) for commands.
Do not infer completed gates from this unchecked template.
