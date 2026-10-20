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

Check a draft without creating a commit:
`python3 -B tools/commit-subject-check.py --subject "fix(checkpoint): preserve durable progress"`.
The checker accepts custom types, optional scopes and `!` before the colon.
It imposes no title-length limit or required body. It checks structure; English
wording, the chosen type and the accuracy of the summary still require review.
Non-ASCII letters produce a review hint, since English titles can quote Unicode
identifiers. Do not treat a structural pass as proof of English language.

CI checks only non-merge commits introduced by the push or PR. It does not lint
all historical commits, the PR title or GitHub's synthetic merge title. See
[development](docs/DEVELOPMENT.md#commit-subject-checks) for range selection and
local commands. No global hook installation is required.

Contributions are licensed under Apache-2.0, like the rest of the repository.
Do not hand-edit generated interfaces or refresh snapshots without inspecting them.
