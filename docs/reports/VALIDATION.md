# MoonCDC 0.1.0 candidate validation

Date: 2026-10-02. Tested source: **`332083729f00999661d2869c10fca81b3df88642`**.
This report is added in a documentation-only commit after that source revision.
The final tests ran against the identical source bytes before commit; `moon info` and
`moon fmt` produced no tracked differences after commit.

Local functional and packaging gates passed. GitHub CI has not run and the package
has **not been published**. The registry dry-run response and CLI failure are recorded
below rather than counted as a successful release.

## Environment

| Component | Observed value |
|---|---|
| Client host | macOS 27.0, build 26A428, arm64 |
| Backend | MoonBit native |
| moon / moonrun | 0.1.20260920, revision 914d7da |
| moonc | v0.10.14+7d59c7ec9, 2026-09-18 |
| Direct dependencies | moonbitlang/async 0.22.4; moonbitlang/x 0.5.5 |
| Database isolation | OrbStack Docker; Compose project `mooncdc-test` |
| Primary database | PostgreSQL 18.6, Debian 18.6-1.pgdg13+2, Linux aarch64 |
| Compatibility database | PostgreSQL 17.11, Debian 17.11-1.pgdg13+2, Linux aarch64 |
| Connection | Numeric loopback TCP, SCRAM-SHA-256; separate verified TLS tests |
| Server settings | wal_level=logical; 16 slots/senders; wal_sender_timeout=5s |

Pulled image repository digests:

- `postgres:18.6`: `sha256:5a5a84b19854a9ffaa54082c166ff4ec27473a361e496e5ea167f298f2da9722`
- `postgres:17.11`: `sha256:d74eeac9a635390a49bc21bd49fccd973de707e2a53a76ac49b552b8712ec46f`

The Docker proxy was repaired before pulling these images. No host-compiled PostgreSQL
was used for this acceptance run. No production or paid cloud database was involved.

## Results

Commands and fixture setup are reproducible from [TESTING](../TESTING.md).

| Gate | Evidence and outcome |
|---|---|
| Static checks | `moon check --target native --deny-warn` passed without warnings |
| Unit/doc tests | `moon test --target native`: 17/17 runner entries passed; 12 bodies executed, 5 environment-gated live bodies skipped |
| PostgreSQL 18 | `MOONCDC_TEST_PORT=55418 MOONCDC_TEST_CA=.test-artifacts/tls-pg18/server.crt moon test --target native`: 17/17, all bodies executed |
| PostgreSQL 17 | Same command with 55417 and tls-pg17: 17/17, all bodies executed |
| Native release build | `moon build --target native --release` passed |
| Interface and formatting | `moon info --target native`, `moon fmt`, `moon fmt --check`, and formatting all four `.mbtx` scripts passed |
| API documentation | `moon doc` passed |
| Index example | Passed on both databases; final map equals SQL result, repeated transaction is idempotent, rollback excluded |
| Cache example | Passed on both databases; keys 1 and 2 invalidated, unrelated key 99 retained |
| Process restart example | All four SIGKILL boundaries passed on both databases; details below |
| Package archive | Required source, metadata, LICENSE, README and examples present; nested build caches, dependency caches, test artifacts, keys and checkpoints excluded |
| Independent consumer | `moon run tools/package-test.mbtx` extracted the ZIP into a temporary workspace; separate consumer compiled without warnings and ran public LSN/value/subscription-validation APIs |

### Protocol and durability coverage

- Unit fixtures exercise every frame split point and pgoutput truncation offset, invalid
  lengths, unknown messages/fields, flags, transaction ordering and resource caps.
- Real databases verify initial SCRAM/COPY BOTH, multi-table committed transactions,
  rollback exclusion, insert/update/delete order and FULL identity old values.
- Real databases verify type OID/name metadata, unchanged TOAST, SQL NULL, key-only old
  tuples after key changes, relation changes after ALTER TABLE and truncate flags.
- Acknowledging transaction C before A/B does not cross the B gap. Reopening after A
  replays B/C with the same transaction IDs. Unit fixtures verify data-event IDs remain
  stable when relation announcements differ across replay.
- Terminating only the test's walsender exercises reconnect while a delivered transaction
  remains unacknowledged. Subsequent data is received and acknowledged.
- Idle and slow-consumer waits each last 6.5 seconds, longer than the server's 5-second
  timeout. Heartbeats keep the connection alive with zero reconnects in those phases.
- A 1 KiB frame cap, 4 KiB transaction cap and one-transaction queue cap fail explicitly.
  Checkpoints remain at their prior positions; oversized transactions replay after raising
  the cap. Socket/slot release is observed within a bounded wait.
- Live filesystem failure during ack preserves the in-memory durable position. Checkpoint
  lock exclusion, stale-session ack rejection, repeated ack, cancellation and immediate
  close/resume are checked. Missing slots and mismatched publication recovery fail.
- TLS tests authenticate and run IDENTIFY_SYSTEM using a trusted local test certificate;
  wrong passwords and an untrusted certificate are rejected. The change-stream tests use
  loopback plaintext; production PKI, client certificates and SCRAM-PLUS were not tested.

### Injected failures

Checkpoint unit tests inject exceptions before writing, after file fsync, after rename
and after parent-directory fsync. Recovery sees a valid old or new checkpoint as appropriate.
These tests simulate interrupted control flow; they are not hardware power-loss evidence.

The native resume example is actually killed with SIGKILL and restarted in a fresh process:

| Boundary | PostgreSQL 18.6 | PostgreSQL 17.11 | Final stored business IDs |
|---|---|---|---|
| Before business persistence | 0 duplicate deliveries | 0 duplicate deliveries | 1, 2, 3 |
| After business persistence, before ack | 1 duplicate, deduplicated | 1 duplicate, deduplicated | 1, 2, 3 |
| After durable ack, before feedback | 0 duplicates | 0 duplicates | 1, 2, 3 |
| After verified server feedback | 0 duplicates | 0 duplicates | 1, 2, 3 |

Each event-file entry retains its complete transaction. No business ID was silently omitted.
The duplicate row demonstrates at-least-once delivery; it is not an exactly-once claim.

## Publication status

- Public repository created: [pangbit/mooncdc](https://github.com/pangbit/mooncdc).
  At validation time it was empty; source commits were local and had not been pushed.
- CI definition covers native checks on Linux/macOS and PostgreSQL 18/17 integration,
  examples, TLS and crash recovery. It has not been executed remotely. The installer uses
  the official current release and logs its version; future CI toolchains may differ.
- `moon publish --dry-run` checked the extracted package successfully. The server returned
  HTTP **202 Accepted**, stating that the dry run succeeded for `pangbit/mooncdc` 0.1.0
  and made no changes. The CLI then printed `Error: moon publish failed` and exited **255**.
  This inconsistent client/server outcome remains an unresolved release-tooling gate.
  No real `moon publish` command or release tag was created.

Before release: authorize/push the reviewed commits, observe remote CI, resolve or verify
the CLI dry-run behavior with the release toolchain, then separately authorize publication
and validate the registry-installed package. A successful local ZIP consumer does not prove
that a registry package exists.

## Evidence limits and traceability

Only the listed macOS client and PostgreSQL configurations were exercised locally.
Linux client support is intended and covered by the unrun CI definition; Windows/Wasm/JS,
failover, initial snapshots, streaming in-progress transactions, two-phase commit, sustained
load/RSS benchmarks and hardware power loss are not claimed.

Local raw logs are retained under ignored `.test-artifacts/validation/`: `pg18.log`,
`pg17.log`, `recovery-pg18.log`, `recovery-pg17.log`, `package.log` and
`publish-dry-run.log`. They are not packaged or committed. Test TLS keys are likewise ignored.
The owned Docker test containers and volumes are removed after evidence capture.

Implementation history:

- `a3efea8`: original requirements and scaffold.
- `d6ad588`: native replication transport feasibility and protocol tests.
- `b5d95d4`: CDC decoder, bounded delivery, durable contiguous ack/recovery and live tests.
- `805df09`: three executable examples and process-kill recovery verification.
- `3320837`: open-source documentation, CI and clean package/consumer validation.
