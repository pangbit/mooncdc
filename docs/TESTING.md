# Validation

Use an isolated Docker environment. No host-compiled PostgreSQL or production database
is part of this workflow. Images are PostgreSQL 18.6 and 17.11; immutable image digests
and the source commit used for a run belong in the validation report.

```sh
moon update
docker compose -p mooncdc-test -f integration/compose.yaml up -d --wait
moon run tools/setup-db.mbtx pg18
moon run tools/setup-db.mbtx pg17
moon run tools/setup-tls.mbtx pg18
moon run tools/setup-tls.mbtx pg17
moon check --target native --deny-warn
moon test --target native
MOONCDC_TEST_PORT=55418 MOONCDC_TEST_CA=.test-artifacts/tls-pg18/server.crt moon test --target native --filter 'live*'
MOONCDC_TEST_PORT=55417 MOONCDC_TEST_CA=.test-artifacts/tls-pg17/server.crt moon test --target native --filter 'live*'
moon run examples/index
moon run examples/cache
moon run tools/recovery-test.mbtx
MOONCDC_EXAMPLE_SERVICE=pg17 moon run tools/recovery-test.mbtx
moon run tools/checkpoint-crash-test.mbtx
MOONCDC_EXAMPLE_SERVICE=pg17 moon run tools/checkpoint-crash-test.mbtx
moon test tools/publish-check.mbtx
moon build --target native --release
moon info --target native
moon fmt
moon fmt tools/setup-db.mbtx tools/setup-tls.mbtx tools/recovery-test.mbtx tools/package-test.mbtx tools/checkpoint-crash-test.mbtx tools/publish-check.mbtx
moon doc
moon package --list
moon run tools/package-test.mbtx
```

`setup-db` is intentionally a one-time initializer for fresh fixtures. Repeated test runs
clean/recreate their own named slots. Do not run two live suites against the same fixture
concurrently. The test role is a superuser only to create/drop fixtures and terminate its
own walsender. Real applications should use restricted roles.
Run the ordinary suite and the five live scenarios in separate invocations as shown:
CPU-heavy SCRAM handshakes can interfere with deliberately short deadlines of protocol
fault peers on small CI machines. `--no-parallelize` alone did not prevent this interference.
All protocol fault tests remain enabled in the ordinary suite with their original limits;
the `live*` filter ensures they never share a test process with real database handshakes.

`setup-tls` creates short-lived test credentials under ignored `.test-artifacts/` and
reloads SSL configuration only inside the selected container. OpenSSL CLI is needed for
this fixture; private keys must never be committed or packaged.

## Evidence layers

- Unit tests exercise every split point of protocol frames, truncation, invalid lengths,
  unsupported messages/fields, transaction ordering, metadata snapshots, NULL/TOAST,
  LSN boundaries, source binding and checkpoint checksums.
- Checkpoint boundary tests inject an exception before/after write, after file fsync, after
  rename and after directory fsync. They verify the old/new checkpoint remains parseable.
  These are simulated interrupted control flow, not power-loss tests.
- Live tests validate SCRAM, COPY BOTH, multi-table/rollback/ordering, contiguous ack,
  reconnect with pending work, idle/slow-consumer heartbeat, type/TOAST/schema/identity,
  truncate, missing slot/mismatched recovery and oversize transaction replay.
- Process tests SIGKILL a native consumer at four business/ack/feedback boundaries and
  verify exact IDs and duplicate bounds in a fresh process. Disk power loss is not claimed.
- `checkpoint-crash-test.mbtx` builds the native whitebox test worker, discovers its
  executable from `moon test --build-only`, and SIGKILLs that owned process at five internal
  checkpoint boundaries. It uses the real checkpoint writer with a live PostgreSQL stream,
  verifies old/new LSNs after restart, and checks complete transactions and IDs 1,2,3.
  Before rename, transaction 1 replays once; after rename, it does not replay. The process
  test is not a power-loss test; the OS remains running throughout.
- Local TCP protocol peers independently inject reconnect failures during startup,
  IDENTIFY_SYSTEM and START_REPLICATION, including timeout, exhaustion and source mismatch.
  These are deterministic protocol fault tests, not PostgreSQL compatibility evidence.
- The feedback TCP peer inspects all three wire positions before ack, across an ack gap,
  after a failed checkpoint write, after a successful retry, and on a requested keepalive
  reply. See the [cross-language reference review](reports/REFERENCE_REVIEW.md) for the
  protocol comparison and remaining scenario gaps.

Without `MOONCDC_TEST_PORT`, live test bodies are skipped. Without `MOONCDC_TEST_CA`,
the TLS test body is skipped. Record environment variables along with test totals;
the test runner does not have a separate skipped-body count.
The checkpoint process worker is inactive during ordinary suites; only its orchestrator
sets `MOONCDC_CHECKPOINT_DIR/PHASE/STAGE` to execute that body in a separate process.

## Cleanup

After preserving evidence, remove only the owned Compose project:

```sh
docker compose -p mooncdc-test -f integration/compose.yaml down -v
```

This deletes its test database data. Do not replace it with global Docker pruning.
The scripts leave no intentionally running consumer processes. PostgreSQL replication
slots retain WAL while inactive, so remove test slots or the owned containers after testing.
