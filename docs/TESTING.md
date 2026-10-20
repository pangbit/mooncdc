# Testing

Use an isolated checkout and synthetic fixtures. The native module is PostgreSQL
→ ClickHouse; cloud extensions and historical machine-specific audit launchers
are outside this release tree. See [validation scope](VALIDATION.md) for precise
historical results and [reference tests](REFERENCE_TESTS.md) for upstream checks.

## Toolchain and offline local gates

The accepted toolchain is `moonc v0.10.14+7d59c7ec9` with
`moon 0.1.20260920 (914d7da)`. Dependencies are pinned in `moon.mod`.
Provision dependencies separately with `moon update` when network use is allowed;
the following commands use the existing cache and never publish a package.

```sh
moon version --all
moon check . cmd/replicator internal/memory internal/yaml --target native --frozen --deny-warn
moon test . cmd/replicator internal/memory internal/yaml --target native --frozen
moon build --target native --release --frozen
moon test tools/package-test.mbtx --target native --frozen
moon test tools/replicator-config-test.mbtx --target native --frozen
moon test tools/publish-check.mbtx --target native --frozen
moon test tools/cli-active-promotion-test.mbtx --target native --frozen
python3 -B tools/package-footprint-sample_test.py
moon info --target native --frozen && moon fmt
moon fmt --check
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

Linux compilation needs the recorded 64 MiB soft stack for this toolchain.
Build tests with `ulimit -S -s 65536` in a build subshell, then execute the
already-built tests with an 8 MiB soft stack in a separate runtime subshell.
Use `moon test ... --build-only` / `moon test ...` with identical inputs and
target directory so runtime does not trigger a compilation at the smaller stack.
Native unit discovery alone does not prove environment-gated live bodies ran.
Derive the expected test identities from this source, rather than freezing an old
17/21/43/260/439/451 total into a new acceptance gate.

## Package consumer

From a reviewed clean commit, `moon run tools/package-test.mbtx --frozen` uses
`git archive`, checks archive contents and runs an extracted-module native
consumer. Its `-- --phase consumers` variant selects consumer checks. A dirty
review tree must instead be packaged and inventoried explicitly; `git archive`
would export the old committed base. Local workspace consumers do not establish
registry resolution or publishing permission. See [releasing](RELEASING.md).

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

## Evidence to retain

Record the source/dirty-tree inventory, toolchain/dependency hashes, enabled test
bodies, server/image versions, exact commands and actual exit codes. Separate
expected injected failures from unexpected failures and skipped/unknown steps.
Keep raw logs outside tracked source, for example in an ignored disposable test
directory. Review summaries for credentials, personal/customer data, host paths
and unrelated process information before sharing. `.gitignore`/`.moonignore`
cannot remove a file that was already committed.
