# Development

MoonCDC is an independently implemented native MoonBit PostgreSQL → ClickHouse
library and CLI, licensed Apache-2.0. Both entries share `run_pipeline` and the
same durability/recovery contracts. See [scope](RELEASE_SCOPE.md).

## Invariants and acceptance

1. Public `Transaction` deliveries require the Commit message. Opt-in pgoutput v1
   `WalBatch` writes may expose fragments earlier; only a validated Commit plus
   the target barrier authorizes acknowledgement. Source rollbacks never enter
   either v1 path. Standalone v2 retains provisional buffers, truncates subaborts
   by arrival offset and requires Stream Commit before any delivery or ack.
2. The durable position advances only across a contiguous prefix of acknowledged
   deliveries. Receiving bytes is not business persistence.
3. Every resume validates system identity, database, slot and publication, and
   rejects a position no longer available from the slot.
4. Frames are length-checked before body allocation. Transactions and outstanding
   deliveries have explicit limits. An overrun fails without acknowledging data.
5. Stable identity uses source identity and commit position, not a process-local
   sequence or the transaction ID alone.
6. Promotion recovery requires both the durable checkpoint and every possible
   target/schema write to lie on the stored timeline's ancestry. Persist write
   horizons before admitting those writes; keep unknown legacy history unknown.


## Work sequence

Make focused source changes and keep generated interfaces in review. Use
`moon info --target native --frozen && moon fmt`, then the affected unit,
native-link and integration gates in [testing](TESTING.md). Any changed test
driver needs its own check; historical results do not certify a rewritten tool.

The public API is in `pkg.generated.mbti`; the CLI is in `cmd/replicator/`.
Keep native stubs listed by `moon.pkg` together with their sources and notices.
The YAML and memory packages have their own interfaces and third-party notices.
`integration/reference/` holds pinned comparison fixtures and migration SQL.

The source package deliberately excludes raw evidence and experimental cloud
extensions. Never merge a large audit history merely to assemble a release.
Use explicit file inventories and distinguish production, tests/tools,
documentation, generated build outputs and raw execution evidence.

Optional controlled-peer tests for error reporting and ConfigCat use synthetic
loopback services; they do not prove full external SDK/service compatibility.
Reference tests can require separately provisioned upstream caches, Rust and Go.
Keep source changes, pushes, CI receipts and registry publication separate.

## Commit subject checks

`tools/commit-subject-check.py` uses Python's standard library and read-only Git
queries. It follows CONTRIBUTING's basic Conventional Commit structure, including
custom types, optional scope, and `feat!:` / `feat(core)!:`. English wording and
type/summary accuracy remain human review; non-ASCII letters are only a hint.
There is no new type whitelist, mandatory scope, length limit or body template.

```sh
python3 -B tools/commit-subject-check_test.py
python3 -B tools/commit-subject-check.py --subject "fix(core): preserve durable progress"
python3 -B tools/commit-subject-check.py --base origin/main --head HEAD
```

An explicit base is required for local history checks. In GitHub CI, an existing
ref push checks `before..after`; a new ref checks `origin/<default-branch>..after`.
An initial default-branch push checks its head only. A PR checks its event's exact
`base.sha..head.sha`, not the checkout's synthetic merge commit. Merge subjects
are excluded, but their newly introduced non-merge commits remain in scope.
Deleted refs have no subjects. Missing Git objects fail with a request to fetch
the exact inputs; the checker never fetches or falls back to all history.

Checkout uses full history for this job. Manual workflow dispatch runs the
checker tests, but does not invent a historical commit boundary. The tests create
synthetic commits only in temporary fixture repositories and clean them up; they
do not change project commits, refs, indexes or hooks. The checker never rewrites
messages, uploads data or installs a global hook.
