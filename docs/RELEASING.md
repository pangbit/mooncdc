# Release procedure

Follow [MoonBit's publishing guide](https://docs.moonbitlang.com/en/latest/toolchain/moon/package-manage-tour.html).
This repository prepares version 0.1.0; it is not published by CI.

1. Run the unit, Docker PostgreSQL 18/17, TLS, three examples and SIGKILL recovery gates
   in [TESTING](TESTING.md). Resolve warnings and record the supported configurations.
2. Run `moon info --target native && moon fmt`; review every generated interface change.
   Run `moon fmt --check`, `moon build --target native --release`, `moon doc` and
   `moon package --list`. Inspect the archive for required sources, README, LICENSE,
   dependencies and examples, and for accidentally included secrets/artifacts.
3. Run `moon run tools/package-test.mbtx` to extract the archive into a temporary directory. Create a separate native MoonBit
   consumer using a workspace/local dependency on that extracted module. Compile and
   run public API calls to verify package ownership and archive completeness.
4. Review metadata: `pangbit/mooncdc`, semver, Apache-2.0, repository URL, description,
   keywords, README and native support. `moon publish --dry-run` packages and validates
   locally and contacts the registry without publishing. Record both the server response
   and CLI exit status; acceptance of a dry run is not publication or a version reservation.
5. Commit verified explicit paths with Conventional Commit subjects. Bind the report
   to the final source commit and environment. A report-only commit may refer to its
   parent source commit; it must not claim to test future code.
6. When authorized, push the exact commits and observe GitHub CI. A local green run
   does not mean remote CI passed. Account authentication and package version availability
   must be checked at publication time without printing tokens.
7. With explicit publication authorization, run `moon publish`, inspect the installed
   registry package in a clean consumer and tag the matching source revision. Update
   CHANGELOG and README only after publication is verified.

Keep GitHub push, successful CI and Mooncakes publication as separate reported states.
CI does not require registry credentials and must not expose publishing tokens to pull requests.
