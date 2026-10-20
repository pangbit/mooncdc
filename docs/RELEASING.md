> Current release: [PostgreSQL → ClickHouse library and CLI](RELEASE_SCOPE.md), with PG17/18 acceptance. C10–C13 and cloud validation are outside this release. Historical full-module evidence stays bound to its original source; new core artifacts require independent acceptance. Linux build stack: 64 MiB.

# Release procedure

Follow [MoonBit's publishing guide](https://docs.moonbitlang.com/en/latest/toolchain/moon/package-manage-tour.html).
This repository prepares version 0.1.0; it is not published by CI.

1. Review the unit, PostgreSQL 18/17, TLS, example and recovery gates in
   [TESTING](TESTING.md). Reuse only evidence bound to the accepted source, API,
   dependencies and enabled test bodies; record changed inputs and gaps. A
   documentation-only update does not silently invalidate matching product results.
2. Run `moon info --target native && moon fmt`; review every generated interface change.
   Run `moon fmt --check`, `moon build --target native --release`, `moon doc` and
   `moon package --list`. Inspect the archive for required sources, README, LICENSE,
   dependencies and examples, and for accidentally included secrets/artifacts.
3. When the package consumer gate needs execution, run `moon run tools/package-test.mbtx`
   from the reviewed clean checkout to extract the archive into a temporary directory. Create a separate native MoonBit
   consumer using a workspace/local dependency on that extracted module. Compile and
   run public API calls to verify package ownership and archive completeness.
4. Review metadata: `pangbit/mooncdc`, semver, Apache-2.0, repository URL, description,
   keywords, README and native support. **`moon publish --dry-run` uploads the package
   for server-side validation and requires explicit authorization. It is not a local
   package check.** Record both the server response
   and CLI exit status; acceptance of a dry run is not publication or a version reservation.
   Only after that authorization, use `moon run tools/publish-check.mbtx` for the checked preflight. It accepts an exact
   HTTP 202 dry-run success for the manifest's name/version. For backend
   `mooncake-bin 0.1.20260911 (7344121 2026-09-11)` only, it recognizes the observed exit-255
   wrapper error after successful extracted-package validation and that exact response.
   Unknown backends with nonzero status, other server statuses, wrong package/version,
   additional errors and authentication failures are rejected. Raw output/status are printed.
   This compatibility handling never runs a real publish or turns a failed validation green.
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

## Version binding and the remaining registry gate

The candidate is assembled on `release/0.1.0` from public base
`5a810d6c62e4ff4783c063159008e23760befc6a`, without merging the 181 local audit
commits. Accepted product provenance and the historical Linux result are recorded
in [validation scope](VALIDATION.md). Production identity can justify reuse;
changed tests/tools need new checks. Product inputs were introduced by
`44a41f8c98b761d127623ba4b65a530bf29c2591`; publication configuration and validation documentation
are reviewed in separate commits. Record the final branch commit and archive
hash in the PR, and verify that exact commit's CI result.

`git archive HEAD` exports the committed base while this review is uncommitted.
Use an explicit current-tree inventory for local checks. After an authorized
commit/push, verify exact public source targets and that commit's CI before
adding public permalinks or claiming remote acceptance.

Existing source-package consumer evidence uses an extracted module and a local
workspace. It proves offline public API/native-linking behavior at its recorded
source and archive hashes. It does not establish namespace permission, resolve
`pangbit/mooncdc@0.1.0` from Mooncakes, or validate the eventual uploaded archive.
An anonymous public lookup can show an indexed version; absence is not ownership
or a version reservation. Confirm the publishing account's right to `pangbit`
and recheck the exact version immediately before an authorized upload. Keep the
current name, version, repository ownership and licenses until explicitly changed.

After publication, create a consumer outside this repository and its workspace,
with a normal exact dependency `"pangbit/mooncdc@0.1.0"` in `moon.mod` and the
root package import `"pangbit/mooncdc"` in `moon.pkg`. Use the public README/API
consumer examples, run strict native check/test and debug/release build/run,
and retain the resolved version, registry archive checksum and actual exit codes.
There must be no `moon.work`, local path, symlink or dependency override pointing
back to this checkout or an extracted source package. This gate remains pending
until the real registry package exists and that consumer succeeds.

The installed `moon` delegates publication to a separate backend and reports its nonzero
exit as `moon publish failed`; see the [adapter at the tested revision](https://github.com/moonbitlang/moon/blob/914d7da08562120ae4d9ef181cdda58c4ff0c191/crates/moon/src/cli/mooncake_adapter.rs#L41-L67).
The workaround is confined to this project's dry-run preflight. Global tools are unmodified.
