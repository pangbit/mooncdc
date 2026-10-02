# Contributing

This project currently has a single maintainer. Small focused issues and pull
requests are welcome; discuss public API or protocol scope changes before coding.
Use English API names and commit subjects. User-facing documentation may be English
or Chinese. Never include credentials, private datasets, database directories or WAL.

1. Explain the observed behavior and the expected transaction/durability invariant.
2. Add a focused regression test and keep unrelated changes out of the patch.
3. Run `moon check --target native --deny-warn`, `moon test --target native`,
   `moon build --target native --release`, `moon info --target native`, `moon fmt`.
4. Protocol or recovery changes require the Docker PostgreSQL 18/17 suite described
   in [testing](docs/TESTING.md). Label simulated and real database evidence separately.
5. Review `pkg.generated.mbti`. Use a Conventional Commit such as
   `fix(checkpoint): preserve contiguous acknowledgement after write failure`.

Contributions are licensed under Apache-2.0, like the rest of the repository.
Do not hand-edit generated interfaces or refresh snapshots without inspecting them.
