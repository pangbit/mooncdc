# Testing

<!-- language-switch -->
[English](TESTING.md) | [简体中文](TESTING.zh-CN.md)

English is the authoritative version.


Use an isolated checkout and synthetic fixtures. The native module is PostgreSQL
→ ClickHouse; cloud extensions and historical machine-specific audit launchers
are outside this release tree. See [validation scope](VALIDATION.md) for precise
historical results and [reference tests](REFERENCE_TESTS.md) for upstream checks.

## Toolchain and offline local gates

The development and CI toolchain is `moonc v0.10.18+6c83a7f62` with
`moon 0.1.20261028 (e1e1f15)`. Dependencies are pinned in `moon.mod`.
Historical release results retain the toolchain recorded with each run.
Provision dependencies separately with `moon update` when network use is allowed;
the following commands use the existing cache and never publish a package.

```sh
moon version --all
moon check . cmd/replicator internal/memory internal/yaml internal/saslprep --target native --frozen --deny-warn
moon test . cmd/replicator internal/memory internal/yaml internal/saslprep --target native --frozen
moon build --target native --release --frozen
moon test tools/package-test.mbtx --target native --frozen
moon test tools/replicator-config-test.mbtx --target native --frozen
moon test tools/publish-check.mbtx --target native --frozen
moon test tools/cli-active-promotion-test.mbtx --target native --frozen
moon test tools/active-store-promotion-test.mbtx --target native --frozen
python3 -B tools/package-footprint-sample_test.py
moon info --target native --frozen && moon fmt . tools/*.mbtx
moon fmt --check . tools/*.mbtx
moon package --list --frozen
moon package --frozen
```

`moon test tools/publish-check.mbtx` executes parser tests only. Its `moon run`
entry uploads a registry dry run and needs separate authorization.
Standalone script test mode may require its existing async/x dependency cache
under `tools/_build/<script>.mbtx/.mooncakes`; do not turn a frozen-cache failure
into an implicit download. The active-promotion parser fixtures under
`tools/fixtures/` are synthetic and self-contained. Its obsolete runtime-only
execution profile is retired; normal full mode builds current sources.

Keep the previously validated 64 MiB Linux compilation soft stack while
checking the upgraded toolchain; this is not a newly measured minimum.
Build tests with `ulimit -S -s 65536` in a build subshell, then execute the
already-built tests with an 8 MiB soft stack in a separate runtime subshell.
Use `moon test ... --build-only` / `moon test ...` with identical inputs and
target directory so runtime does not trigger a compilation at the smaller stack.
Native unit discovery alone does not prove environment-gated live bodies ran.
Derive the expected test identities from this source, rather than freezing an old
17/21/43/260/439/451 total into a new acceptance gate.
The active-store driver requires the root, CLI, memory and YAML package groups,
then executes that same explicit selection and checks its exact discovered total.

## Package consumer

From a reviewed clean commit, `moon run tools/package-test.mbtx --target native --frozen` uses
`git archive`, checks archive contents and runs an extracted-module native
consumer. Its `-- --phase consumers` variant selects consumer checks. A dirty
review tree must instead be packaged and inventoried explicitly; `git archive`
would export the old committed base. Local workspace consumers do not establish
registry resolution or publishing permission. See [releasing](RELEASING.md).
Run the driver itself with the native backend so its owned/evidence directories
use the restricted `/tmp` namespace consistently on Linux and macOS.

## Isolated database tests

These commands create and mutate synthetic SQL fixtures. Provision an isolated
Docker engine and images first; never point them at production services.
Compose files bind test services to loopback. Image acquisition and any resource
reservation are explicit preparation, separate from running this local review.

```sh
docker compose -p mooncdc-test -f integration/compose.yaml up -d --wait pg18
moon run tools/setup-db.mbtx pg18
moon run tools/setup-tls.mbtx pg18
MOONCDC_TEST_PORT=55418 MOONCDC_TEST_CA=.test-artifacts/tls-pg18/server.crt moon run tools/live-test.mbtx
docker compose -p mooncdc-clickhouse-test -f integration/clickhouse.compose.yaml up -d --wait clickhouse
MOONCDC_EXAMPLE_SERVICE=pg18 moon run tools/replicator-test.mbtx
MOONCDC_EXAMPLE_SERVICE=pg18 MOONCDC_REPLICATOR_FORMAT=yaml MOONCDC_PIPELINE_FAILOVER=true moon run tools/replicator-test.mbtx
docker compose -p mooncdc-clickhouse-test -f integration/clickhouse.compose.yaml down -v
docker compose -p mooncdc-test -f integration/compose.yaml down -v
```

Repeat for `pg17` using port 55417 and its own TLS path. Cleanup belongs in an
always/finally step even when assertions fail. The maintained CI workflow has
separate PG17/18, TLS, recovery, standby, migration and slot-safety gates.
`tools/parallel-copy-test.mbtx`, `tools/core-postgres-test.mbtx`,
`tools/streaming-test.mbtx` and `tools/timeline-promotion-test.mbtx` provide further
owned-fixture audits; inspect their prerequisite images and toolchains first.
`tools/cli-active-promotion-test.mbtx` without runtime-only flags runs the current
source full audit, with `MOONCDC_ACTIVE_COPY=true` for active COPY or
`MOONCDC_ACTIVE_LOSE_STATE_OWNER=true` for the separate owner-loss matrix.

No command here changes TasksMax, pids.max or other host policy. The historical
256-task compiler budget, measured aggregate 264-task database peak and single
temporary 512-task diagnostic ceiling are distinct observations. A new constrained
run needs its own reservation, actual thread/process measurements and cleanup
readback; do not copy an old task limit as a product minimum.

## COPY with concurrent row changes

`tools/copy-iud-test.mbtx` runs four cases: library/CLI × ReplacingMergeTree/MergeTree.
Provision isolated PostgreSQL 18 with logical replication enabled and ClickHouse 26.4
first. Both endpoints must be loopback services using the synthetic credentials
`cdc` / `mooncdc-test-only`; PostgreSQL uses database `postgres`.

```sh
MOONCDC_TEST_PORT=55418 MOONCDC_CLICKHOUSE_PORT=18123 \
  moon run tools/copy-iud-test.mbtx --target native --frozen
```

The driver builds the test and CLI once, or accepts both
`MOONCDC_COPY_IUD_TEST_EXE` and `MOONCDC_COPY_IUD_CLI` as prebuilt executable paths.
Use the compilation/runtime stack settings above. The CLI tests require an
external `kill` executable on `PATH`. An optional `MOONCDC_COPY_IUD_EVIDENCE`
directory must already exist and be writable; otherwise the driver creates one.
The driver enables live execution and requires the exact test identity and PASS
marker for every case. Missing configuration or a skipped body cannot pass.

Each case gates the first real snapshot INSERT before forwarding it, commits
five I/U/D transactions while durable state is `Copying` and COPY completion is
zero, then checks all three snapshot rows, five WAL events and the final current
rows. Only the owned ReplacingMergeTree table has merges paused while raw history
is inspected; this does not promise permanent history retention in that engine.
The fixture cleans its own database/schema/slot namespaces. The caller owns the
external services and their cleanup. See the [directed results](VALIDATION.md#unreleased-follow-up).

## Evidence to retain

Record the source/dirty-tree inventory, toolchain/dependency hashes, enabled test
bodies, server/image versions, exact commands and actual exit codes. Separate
expected injected failures from unexpected failures and skipped/unknown steps.
Keep raw logs outside tracked source, for example in an ignored disposable test
directory. Review summaries for credentials, personal/customer data, host paths
and unrelated process information before sharing. `.gitignore`/`.moonignore`
cannot remove a file that was already committed.
