# Validation scope and version binding

<!-- language-switch -->
[English](VALIDATION.md) | [简体中文](VALIDATION.zh-CN.md)

English is the authoritative version.


This records source-bound validation observations. It is not a registry
publication receipt by itself. The 0.1.1 release record precedes the historical 0.1.0
assembly and CI records below.

<a id="011-patch-candidate"></a>

## 0.1.1 release (2026-10-29)

The product source is `568fe7c387f04f1cc3f750749a2652cff8b31e0a`.
Public interfaces and dependency versions are unchanged. Directed Linux checks
used isolated loopback peers, the pinned MoonBit toolchain, a 64 MiB compilation
stack and an 8 MiB runtime stack:

| Fix / source | Executed result |
| --- | --- |
| ClickHouse exception headers, `fd8c7b7` | 3 tests / 44 scenarios passed; HTTP status and exception-header failures retain retry classification and error redaction |
| Admitted COPY cancellation ordering, `ce42912` | 2 tests / 12 scenarios passed across both engine configurations, including reset/replay, shutdown, request deadlines and cancellation before admission |
| Adjacent transaction boundary, `568fe7c` | 1 test / 4 combinations passed: At/Resume × Accepted/Durable, retaining adjacent transactions and filtering old/duplicate transactions |

At `568fe7c`, the four-package ordinary debug run passed 264 of 265 identities
(root 211, CLI 28, memory 18, YAML 8). The remaining migration test could not read
two tracked SQL fixtures omitted from the custom verification bundle. Supplying
those exact fixtures and rerunning only that test passed 1/1 without changing
product code or assertions. The ordinary gate is closed by 264 + 1 results, not
by a single all-green full run. Framework totals include environment-gated
live/reference tests whose bodies were not enabled; these are not fresh live
PostgreSQL or ClickHouse results. Owned remote fixtures were removed and cleanup
was verified.

Release source `0a38ed675a3e044f81bfd0c14a3f2e89a0d24cab` adds metadata,
documentation and the package driver's manifest-based version handling to that
product source. Its [CI run](https://github.com/pangbit/mooncdc/actions/runs/40812266552)
completed successfully in all six jobs: commit subjects, codec/reference checks,
Linux native, macOS native, PG17 and PG18. Native jobs include debug/release
execution and archive consumers. The existing enabled database jobs passed;
this is not a claim about every fault combination or deployment environment.
The [CI at `7233238`](https://github.com/pangbit/mooncdc/actions/runs/39536560268)
remains the pre-patch baseline. No additional manual database matrix was run.

`moon publish` exited 0 with HTTP 200. The exact 0.1.1 registry lookup succeeded,
with an unyanked package created at `2026-10-29T03:49:24.759462+00:00` and archive
SHA-256 `d592a730ecf3027783c6d764670b789df11f092992afb52a85fee664434d6b33`, identical
to the reviewed package. A fresh registry consumer outside the repository
resolved 0.1.1 and passed strict native checking and release build/run on macOS
ARM64. It used no workspace, local path or dependency override, and no database.
The three repaired source files and public interface matched the published
source. [Tag and Release `v0.1.1`](https://github.com/pangbit/mooncdc/releases/tag/v0.1.1)
point to `0a38ed6`. Later publication-record documentation does not change or
retest that source.

## Inputs and reuse

- Public base: `5a810d6c62e4ff4783c063159008e23760befc6a`.
- Accepted local product source: `b96a741c39119ebd6336b35cdc57f4f4f7c44977`.
  This identifier is provenance; that local history is not merged into this branch.
- Reviewed source archive: SHA-256
  `d64d82474a2af6d745cfe399d5fb2e21fa325353249c6494eb96b28605ade529`,
  666,047 bytes, 290 entries. The new tree changes documentation and test tooling;
  its eventual archive has a separate checksum and must be inspected again.
- Production sources, native stubs, package metadata and generated interfaces are
  compared byte for byte. Matching inputs permit reuse of the bound product
  evidence; changed tooling and documentation require their own checks.
- Earlier offline consumer evidence used archive SHA-256
  `f05c80d99bcbba76b4d1b4e9b05bbde5cd45b50167b1a9d3397e5ed8a5651e41`
  at `1ef841a5a4e3960d552338f0a54e6d7d04d7d70c`. It exercised an extracted
  local module, not a normal Mooncakes registry dependency.

## Historical Linux results, not a fresh run of this tree

The 2026-10-20 single diagnostic ran 03:11:51–03:15:44 UTC. The bound product
passed four library cells (PG17/18 × both ClickHouse engines) and eight CLI cells
(PG17/18 × JSON/YAML × both engines). All eight final data comparisons matched.
The 48 CLI child phases recorded 40 exits of 0 and eight deliberately injected
failure exits of 1; controller, supervisor and harness commands exited 0.
The servers were PostgreSQL 17.11/18.6 and ClickHouse 26.4.5.143.
Ordinary 260-test and public-API suites were not rerun in that diagnostic.

Resource observations must remain separate:

| Measurement | Meaning |
| --- | --- |
| 256 tasks | Controlled compiler-window budget and restored post-run parent limit |
| 264 tasks | Observed aggregate parent kernel peak during the database diagnostic |
| 512 tasks | Temporary ceiling approved for that one diagnostic; stop watermark 384 |
| 64 MiB / 8 MiB stack | Accepted Linux compilation / product runtime conditions |

CPU remained 4, memory 6 GiB and swap 0. Cleanup readback found no owned fixture
containers, networks, volumes or tasks; the parent task limit was restored to 256.
These observations are neither product minima nor permission to change host limits.
Exact peak composition was not sampled at the kernel's 264-task peak.

Prior interrupted attempts, compiler limits and harness failures do not become
product passes. The prior failures included a YAML/JSON wrapper mismatch,
supervisor cleanup race, insufficient aggregate task budget and an old 64 MiB
ELF archive limit. The diagnostic resolved its own preparation filename mismatch
before database execution. Wider fault combinations and other environments need
independent results.

## Public report consolidation

The old reports remain in the public base commit's history. This branch removes
their current-tree copies; it does not erase history.

| Previous `docs/reports/` file | Retained value / current destination |
| --- | --- |
| `DESTINATION_VALIDATION.md` | Durability and synthetic-test limits → [API](API.md#destinations-and-durability), [testing](TESTING.md) |
| `SLOT_SAFETY_VALIDATION.md` | Slot recovery safeguards → [API](API.md), [testing](TESTING.md) |
| `SNAPSHOT_VALIDATION.md` | Snapshot/WAL handover contract → [API](API.md#persistent-table-pipeline) |
| `REFERENCE_REVIEW.md` | Pinned reference method and independent implementation → [reference tests](REFERENCE_TESTS.md), [licenses](../THIRD_PARTY.md) |
| `FEATURE_PARITY_BATCH1.md` | Superseded progress list → [scope](RELEASE_SCOPE.md), [feature map](FEATURE_PARITY.md) |
| `VALIDATION.md` | 2026-10-02 checks at `332083729f00999661d2869c10fca81b3df88642`; historical counts/environment do not certify this release |
| `VALIDATION_FOLLOWUP.md` | Checks at `985f4d0767f9e8fa70c139d75a37be963b890fa1`; old CI/dry-run status does not publish this release |

Raw process lists, host paths, environment dumps, large logs and historical
archives are deliberately absent. The local review inventory retains checksums
and exact file provenance outside the release tree. Follow [testing](TESTING.md)
to produce new bounded local evidence; publish only a reviewed, redacted summary.

## Initial local release-tree checks (2026-10-20, before GitHub CI)

The initial local review kept all 108 production/API/metadata archive entries byte
equal to the accepted candidate. At that point all 208 MoonBit/C/interface/package
input files also matched the accepted local product commit. Strict frozen native
check passed. Ordinary discovery still lists 260 identities (206 root, 28 CLI,
18 memory, 8 YAML); this is discovery, not a new 260-test execution.

Changed active-promotion parser fixtures passed 33 tests. Package-driver parser
tests passed 7, CLI configuration parser tests 3, publishing-response parser
tests 1, and filesystem/allocation regressions 21. All actual commands exited 0.
The timeline-promotion standalone script compiled in test mode with zero test
bodies; no promotion/database result is claimed. Its metadata command now uses
portable `uname -a`. The old prebuilt-runtime launch mode is retired; its parser
regressions use synthetic fixtures instead of historical receipts.

The first standalone frozen attempt was refused because its separate `x` cache
was absent. Existing local async/x caches were copied into disposable build
directories; no dependency download or installation was used. The refusal is a
cache/environment result, not a product failure.

At the time of initial assembly the new CI configuration had not run on GitHub.
Subsequent CI follow-ups change test tooling, fixtures and the bounded server-side
cleanup observation, while production inputs remain unchanged. The release PR
records their exact source heads, fresh local checks and actual remote results.
No database, remote host, full 260-test runtime or registry consumer was
started in this local organization pass. Final interface/format/package checks
and full review inventories are recorded with actual exits outside tracked source.

## GitHub CI at the reviewed source

The reviewed release head was `dc29b03afc0ec15582330c38f92e57b4c6e7e603`.
Its [PR run](https://github.com/pangbit/mooncdc/actions/runs/39387450645) and
[push run](https://github.com/pangbit/mooncdc/actions/runs/39387444662) both completed
successfully. The normal merge commit
`7eb58ae7235c5c8c2f43cebfb1ee13bfae9dbf3b` has the same source tree.

The [first main attempt](https://github.com/pangbit/mooncdc/actions/runs/39393658718/attempts/1)
passed five jobs. Its macOS native release runtime step took about five seconds
and passed 258/260 ordinary tests; `slot_failover_wbtest.mbt:73` (copy-exit cleanup)
and `reconnect_wbtest.mbt:200` (reconnect retry) reported `TimeoutError`.
The former uses a 3000 ms outer timeout per scenario and 100 ms connection timeout;
the latter uses a 5000 ms outer timeout and 200 ms connection timeout.
The log does not identify the failing scenario or which timer produced the error.
The CI step's five-minute limit was not reached.

Exactly the failed job received [one retry](https://github.com/pangbit/mooncdc/actions/runs/39393658718/attempts/2).
Its release runtime step took about two seconds and passed 260/260.
Source, commands, assertions, timeouts, the pinned MoonBit toolchain, macOS image
and 8 MiB runtime stack were unchanged; the worker was different. Scheduling or
host load was not measured, so the cause remains unconfirmed. The first failure
is retained; the retry neither establishes nor fixes a product defect.

The successful main Linux job and macOS retry each passed 260/260 debug and
260/260 release ordinary tests and the full 65-command offline package audit.
Two nonzero commands were expected negative controls. PostgreSQL 17/18,
cross-language reference and commit-subject checks passed. The final six check
results were successful, retaining the first attempt's five successful jobs.
The existing Python Path-accessor reproduction was the sole filesystem-test skip.

These results belong to the named source inputs. A documentation-only follow-up
can reuse matching product evidence after byte comparisons of production sources,
native stubs, API, metadata, tests and CI inputs. Check changed Markdown examples,
links and licenses, then inspect and checksum the new archive. Its new checksum
does not imply a fresh product/database run or remote CI at the follow-up commit.

## Remaining release gates

Release product inputs were introduced by `44a41f8c98b761d127623ba4b65a530bf29c2591`.
Configuration and validation documentation are separate commits. Record the final
branch commit, source inventory, archive hash and actual validation exits in its
PR. The GitHub results above apply to their named commits. Namespace publishing
authority, any upload-type dry run, actual publication and a clean exact-version
registry consumer remain separate gates. Do not
claim the historical Linux result covers changes to test tools or future code.
