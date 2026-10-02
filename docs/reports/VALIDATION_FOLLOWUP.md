# Recovery and release follow-up validation

Date: 2026-10-02. Source revision:
**`985f4d0767f9e8fa70c139d75a37be963b890fa1`**.
This report follows the [initial candidate report](VALIDATION.md); the original report
remains a historical record. Local host, toolchain, dependencies and Docker image digests
are unchanged from that report. A documentation-only commit adds this evidence after
the tested source revision.

## Reconnect correction

One attempt now covers connection/authentication, IDENTIFY_SYSTEM and START_REPLICATION.
Transient failures and timeout in any stage close that attempt's socket and consume the
same bounded retry budget. Exhaustion returns the most recent attempt error. Fatal source
mismatch still stops immediately, even with retries remaining. The public interface is
unchanged (`moon info` produced no `.mbti` diff).

Regression evidence: running the new three-test suite against the previous implementation
failed two tests: mid-reconnect EOF surfaced ReaderClosed, and exhaustion reported error1
instead of error2. With the fix, all three passed. Local TCP peers cover startup EOF,
IDENTIFY_SYSTEM EOF, START_REPLICATION EOF, COPY-start timeout, eventual committed data,
exhaustion, source mismatch and failed socket closure. These peers are controlled protocol
tests, not substitutes for the real PostgreSQL suite.

## Checkpoint process interruption

`tools/checkpoint-crash-test.mbtx` discovers the native whitebox executable from
`moon test --build-only` and starts a separate owned worker for each case. The worker
reads a real PostgreSQL transaction, fsyncs the complete business transaction to its event
file, then calls the same private checkpoint writer used by ack. An asynchronous boundary
callback writes a ready marker and waits. The parent sends SIGKILL to that exact PID,
waits for termination, and starts a fresh recovery process.

No production environment-variable fault switch or new public test API was added. The
worker's environment controls exist only in whitebox test code. Regular suites leave
this worker inactive; the orchestration script explicitly executes it.

| Kill boundary | Recovered checkpoint | Duplicate deliveries, PG18 / PG17 | Final business IDs |
|---|---|---|---|
| Before temporary-file write | Previous durable LSN | 1 / 1 | 1, 2, 3 |
| After write, before file fsync | Previous durable LSN | 1 / 1 | 1, 2, 3 |
| After file fsync, before rename | Previous durable LSN | 1 / 1 | 1, 2, 3 |
| After rename, before directory fsync | New committed LSN | 0 / 0 | 1, 2, 3 |
| After directory fsync | New committed LSN | 0 / 0 | 1, 2, 3 |

Both PostgreSQL **18.6** and **17.11** passed all five cases. The four original
business/ack/feedback SIGKILL cases also passed again on both versions, for **18 successful
process-kill cases** in this follow-up run. All stored business IDs were exactly 1,2,3.
The OS remains running during SIGKILL; directory persistence across hardware power loss
is not claimed. Existing exception-injection tests are retained as a separate layer.

## Local gates

| Gate | Result |
|---|---|
| `moon check --target native --deny-warn` | Passed |
| Ordinary native suite | 21/21 runner entries passed: 15 active bodies; 5 live bodies and 1 process worker inactive |
| PG18 + TLS suite | 21/21: 20 active bodies; dedicated process worker inactive |
| PG17 + TLS suite | 21/21: 20 active bodies; dedicated process worker inactive |
| Native release build, API docs, interface and formatting | Passed; public interface unchanged |
| `moon test tools/publish-check.mbtx` | Passed, including wrong identity/version/status/backend and extra-error rejection |
| Extracted archive consumer | Passed on the updated package |
| Registry dry-run compatibility preflight | Passed with the raw CLI result preserved, as described below |

## Dry-run exit-code handling

The exact installed tools were inspected: `moon` 0.1.20260920 delegates publishing to
`mooncake-bin` 0.1.20260911 (7344121). The [upstream adapter](https://github.com/moonbitlang/moon/blob/914d7da08562120ae4d9ef181cdda58c4ff0c191/crates/moon/src/cli/mooncake_adapter.rs#L41-L67)
turns a backend nonzero exit into the observed wrapper error. Local evidence shows the
backend returning nonzero after successful extracted-package validation and an explicit
registry response: HTTP 202, dry-run completed successfully, no changes made.

`tools/publish-check.mbtx` confines compatibility handling to that known backend and
the exact success response for the name/version read from `moon.mod`. It rejects other
statuses, names, versions, additional errors, duplicate responses and unknown nonzero
backend outcomes. It invokes only `moon publish --dry-run`, prints the raw output and
exit 255, and exits successfully only when those strict conditions hold. A future CLI
with exit zero and the same exact accepted response also works. Global tools are unchanged.

Actual preflight result: registry dry-run accepted `pangbit/mooncdc@0.1.0`; wrapper exited
zero after recording the known backend exit 255. The upstream backend defect remains,
but it no longer prevents an evidence-based local preflight. This is not publication or
a reservation of the version. CI tests the parser without registry credentials and does
not execute the authenticated preflight.

## Remote delivery

Source commits were pushed to [pangbit/mooncdc](https://github.com/pangbit/mooncdc).
Remote `refs/heads/main` was read back at the exact source hash above.
GitHub run: [CI for this source](https://github.com/pangbit/mooncdc/actions/runs/36981966164).
All **four jobs passed** at that exact source revision:

| GitHub job | Verified result |
|---|---|
| native (ubuntu-latest) | Native checks/tests, preflight parser tests, release build, interface, formatting, docs and extracted-package consumer passed |
| native (macos-latest) | Same native gates passed |
| postgres (pg17, 55417) | Five live tests including TLS, index/cache examples, four business SIGKILL cases and five checkpoint SIGKILL cases passed |
| postgres (pg18, 55418) | Same complete PostgreSQL gates passed |

Thus Linux native support is now backed by remote execution, in addition to the local
macOS evidence. CI used moon 0.1.20260920 / moonc v0.10.14+7d59c7ec9, matching the local
toolchain. The complete workflow logs retain runner image details and dependency output.

The first run on `09b39fa` exposed a clean-runner setup omission: the registry index had
not been initialized. It failed dependency resolution before library tests ran. Commit
`934cad9` adds `moon update` to both job types and disables matrix fail-fast, so all
platforms produce independent results. Library source is unchanged by that CI correction.
The second run passed both native jobs and every live PostgreSQL scenario, but the
deliberately short-deadline TCP peers timed out when sharing the event loop with concurrent
SCRAM work on small runners. Commit `832fe21` runs the complete database suites with
`--no-parallelize`, but that alone did not eliminate interference: PG18 passed everything,
while one protocol peer still timed out in PG17's combined run. Commit `985f4d0` therefore
runs `--filter 'live*'` in database jobs, with all unit/fault tests still executed by the
native jobs in separate processes. All five real database scenarios, all unit/fault cases,
examples and crash scripts remain covered. No timeout assertion, production timeout or
retry limit was relaxed.

No real Mooncakes publication command, release tag or registry-installed consumer is
claimed. Follow [RELEASING](../RELEASING.md) for separately authorized publication.

## Traceability

- `b612f2f`: complete reconnect attempt retry and last-error regression tests.
- `5b77cff`: five checkpoint write boundaries with actual process termination and recovery.
- `09b39fa`: strict registry preflight, CI gates and supporting documentation.
- `934cad9`: initialize the registry index on clean runners and retain all matrix results.
- `832fe21`: isolate timed protocol peers from concurrent SCRAM work in database suites.
- `985f4d0`: use separate unit/fault and live invocations when the serial flag proved insufficient.

Ignored local evidence is under `.test-artifacts/validation/`: `pg18-followup.log`,
`pg17-followup.log`, `checkpoint-kill-pg18.log`, `checkpoint-kill-pg17.log`,
`recovery-pg18-followup.log`, `recovery-pg17-followup.log`, `package-followup.log`,
`publish-check.log`, `ci-watch.log`, `ci-watch-followup.log`, `ci-watch-final.log`,
`ci-watch-isolated.log` and `ci-accepted.log`. No test keys or build caches are committed
or packaged.
