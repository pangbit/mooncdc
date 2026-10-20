<a id="独立-replicator"></a>

# Standalone replicator

<!-- language-switch -->
[English](REPLICATOR.md) | [简体中文](REPLICATOR.zh-CN.md)

English is the authoritative version.


After `moon build --target native --release`, run
`_build/native/release/build/cmd/replicator/replicator.exe --config config.json`.
Add `--check` for configuration/library-argument validation only. It connects to
no services and does not establish CA-file usability. Startup failures identify
fields or causes on stderr and return 1. PostgreSQL errors retain SQLSTATE;
destination errors retain classification/status. Checks never echo passwords or invalid values.

MoonCDC uses its own JSON/YAML fields, not the full Supabase ETL configuration.
ClickHouse and PostgreSQL state/schema storage are supported; unknown fields and
unimplemented destinations are explicitly rejected.

```json
{
  "id": "orders",
  "publication": "orders_publication",
  "run_source_migrations": true,
  "replication_slot": { "failover": false },
  "invalidated_slot_behavior": "error",
  "memory_refresh_interval_ms": 100,
  "memory_backpressure": { "activate_threshold": 0.85, "resume_threshold": 0.75 },
  "batch": { "memory_budget_ratio": 0.2, "max_bytes": 33554432, "max_fill_ms": 10000 },
  "source": {
    "host": "postgres.example.com",
    "user": "replicator",
    "database": "orders",
    "security": "verify_full"
  },
  "state": { "schema": "etl", "table": "mooncdc_state" },
  "destination": {
    "type": "clickhouse",
    "url": "https://clickhouse.example.com:8443",
    "user": "replicator",
    "database": "orders",
    "engine": "replacing_merge_tree"
  },
  "health": { "port": 9001, "stall_timeout_ms": 300000 },
  "metrics": { "port": 9000 },
  "copy_concurrency": 4,
  "copy_connections_per_table": 4,
  "table_sync_monitor_refresh_interval_ms": 10000,
  "batch_rows": 256,
  "table_retry_delay_ms": 10000,
  "max_table_retries": 5,
  "table_sync_copy": { "type": "include_all_tables" }
}
```

Inject passwords through `APP_SOURCE__PASSWORD` and `APP_DESTINATION__PASSWORD`.
All `APP_` overrides preserve original strings, with strict separate integer parsing;
passwords such as `true` or `00123` are never coerced. Nested fields use `__` and
case-insensitive keys. Supply selection arrays through JSON/YAML.

Other destinations are outside this module/CLI. Core CLI rejects BigQuery,
Snowflake, Iceberg and DuckLake before reading their credentials.

`copy_concurrency` limits concurrent table copies: default 4, range 1–256.
`copy_connections_per_table` limits readers per table: default 4, range 1–256;
the product must not exceed 256. Its override is `APP_COPY_CONNECTIONS_PER_TABLE`.
Parallel readers import one snapshot and split CTID ranges; each table additionally
keeps exporter and metadata connections. These settings are not total database
connection counts. Target writes remain serial. Empty/skipped tables still receive
schema/target initialization and durability barriers. Row ordering is not guaranteed.
These CLI defaults match pinned ETL; library defaults remain 2/1 and can be overridden.
Copy-slot monitoring uses positive Int `table_sync_monitor_refresh_interval_ms`,
default 10000ms, overridden by `APP_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS`.
Each active copy has at most one independent sample connection, never running SQL
on its snapshot exporter. Missing/invalid slots cancel the copy and follow table
retry policy. General connection/query failures only increment counters and sampling
continues. Stop cancels and joins pending samples. The interval also controls
independent source-LSN sampling for main apply/private WAL catch-up; sampling
continues during source pause or blocked target write/flush. Samples update lag/age,
not acknowledgements or business activity. Each active WAL worker adds at most one
query connection, delays after completion, and does not accumulate ticks.

Without `--config`, select `base` then `${APP_ENVIRONMENT:-prod}` files from
`${APP_CONFIG_DIR:-configuration}`. For each, choose the first regular file in
`.yaml`, `.yml`, `.json` order; recursively merge objects, then apply environment
overrides. Only `dev`, `staging`, `prod` are allowed; both files are required.
Explicit `--config` skips layered files but retains environment overrides.
`.yaml`/`.yml` use YAML; other paths use JSON. File size is limited to 1 MiB.

YAML supports comments, block/flow collections, multiline strings, anchors and
aliases. The root is one mapping; an empty file means empty configuration.
Duplicate keys, multiple documents and invalid aliases are rejected. Only lowercase
`true`/`false` are implicit booleans; quote numeric/boolean-looking passwords.
`<<` is a literal key, without merge expansion. Nesting is limited to 64; expanded
node/scalar/key cost to 1,048,576. Aliases charge their full expansion. Non-finite
numbers and complex mapping keys are rejected. Errors never echo YAML source/values.

Connection defaults are port 5432, database postgres, timeout 5000ms and empty
password. `security` accepts `verify_full`, `ca_file` (with `ca_file` path) and
`local_plaintext` (numeric loopback only). Optional numeric IPv4/IPv6 `hostaddr`
separates dial address from TLS `host`; plaintext's actual address must also be
loopback. `keepalive` supports positive `idle_secs`, `interval_secs`, `retries`,
defaulting to 30, 10, 3. Overrides such as `APP_SOURCE__HOSTADDR` and
`APP_SOURCE__KEEPALIVE__IDLE_SECS` also apply to layered configuration.
`state.connection` provides a separate connection or defaults to source. Its
schema must already exist. **Exclude state tables from the source publication**;
`FOR ALL TABLES` requires a separate state database. `run_source_migrations`
defaults true, installing pinned upstream DDL helpers/trigger before state-store
acquisition. Creating event triggers requires superuser; restricted users should
disable it and ask an administrator to install them. Physical standbys skip
installation; install on primary and wait for replay. See [API](API.md).

`replication_slot.failover` defaults false, overridden by `APP_REPLICATION_SLOT__FAILOVER`.
It requires PostgreSQL 17+, enables failover on main/per-table slots and upgrades
existing main slots when needed; disabling does not downgrade them. Per-table
slots become persistent, with saved table state owning exit/restart cleanup.
Configure standby slot synchronization separately. After fencing the old primary,
a new process using original state storage/new source can recover promotion only
with ancestry/fork proof: checkpoint, snapshot, handover and durable write horizons
must lie within ancestor forks; main slot must already be valid, persistent and
failover-enabled. Missing legacy write horizons reject automatic rebinding.
On same-source timeline change during reconnect, the running main worker closes
and flushes its old scope, then repeats that proof. Healthy independent state
ownership remains; losing its connection fails, requiring a new process/owner.
New-process and same-PID transitions have PG17/18, JSON/YAML and both-engine
evidence; see [validation](VALIDATION.md) and [API](API.md). Active COPY/other state
backends, compound faults and automatic HA routing remain incomplete.

`invalidated_slot_behavior` defaults to `error`; explicitly choose `recreate` or
`APP_INVALIDATED_SLOT_BEHAVIOR`. Only an existing identity-matched invalidated
slot may be recreated. Missing slots, changed source/destination, corrupt checkpoints
and empty publications remain rejected. Recreation first persists all-table Pending
intent, then creates a main slot/checkpoint and reinitializes selected targets.
Skipped tables cannot recover lost WAL. Interrupted state uses version 2 and
requires supporting code plus explicit recreate; completion returns to version 1.
Runtime invalidation drains old target writes and restarts the source worker with
existing retry counts/delays. Recovery waits are initializing/unready and signals
can stop immediately. See [API](API.md).

Memory pressure is enabled by default: sample host/cgroup every 100ms, pause new
source reads at 0.85 and resume strictly below 0.75. `memory_backpressure: null`
disables pausing; omission retains the default. Explicit objects require both
`activate_threshold` and `resume_threshold` in `(0,1]` and `[0,1)`, with resume
below activate. `memory_refresh_interval_ms` is always a positive integer.
Overrides are `APP_MEMORY_REFRESH_INTERVAL_MS`,
`APP_MEMORY_BACKPRESSURE__ACTIVATE_THRESHOLD` and `APP_MEMORY_BACKPRESSURE__RESUME_THRESHOLD`;
when no object existed, supply both thresholds. Decimal/scientific numeric strings
are accepted. Pause does not stop Accepted-write drain, checkpoint or heartbeats;
this is not a hard memory cap. Library API remains opt-in; see [API](API.md).

Omitted `batch` or `{}` enables batching, matching pinned ETL; explicit `null`
retains disable behavior. Defaults use the library budget. JSON/YAML may override
individual fields, leaving others at library defaults:

```json
{ "batch": { "memory_budget_ratio": 0.2, "max_bytes": 33554432, "max_fill_ms": 10000 } }
```

Ratio is `(0,1]`, bytes positive UInt64, fill interval nonnegative Int milliseconds;
zero interval flushes each item. Ratio supports numbers/scientific strings; fill
interval supports integers/integer strings. `max_bytes` accepts JSON decimal integer
literals and decimal digit strings, not signed/fractional/exponent strings. Quote
large YAML byte integers. Non-exact numeric representations accept integers only
through `2^53-1` to avoid rounding. Overrides are `APP_BATCH__MEMORY_BUDGET_RATIO`,
`APP_BATCH__MAX_BYTES`, `APP_BATCH__MAX_FILL_MS`. Nested overrides can enable batching
from omission/null, retaining defaults for other fields.

Batching and pressure share `memory_refresh_interval_ms`; capacity refresh can
remain enabled without pressure pausing. Snapshots form/drain estimated-byte batches.
Batched ClickHouse main/private WAL decodes events and drains bounded batches,
preserving event IDs. Budgets are heuristic: one event can exceed them; they are
not heap caps. Destinations may see fragments before Commit, but checkpoint/handover
still requires valid Commit plus durable barrier. Explicit `batch: null` keeps the
whole-transaction path. See batch/WAL-event [validation](VALIDATION.md); this does
not include pgoutput v2 uncommitted streaming.

`id` must be unique within a state table and stable across restarts. The program
uses exclusive `<id>/state` and `<id>/schemas` namespaces. Changed identities
reject old-state recovery. Exit retains durable slots/state; deleting either is
not a normal restart step.

Health listening defaults off. When configured, it binds all interfaces and
exposes `/livez` and `/readyz`. SIGINT/SIGTERM request stop and await pipeline/target
drain. After async scopes close, success explicitly returns 0; drain errors return
1, never masked by the signal. During stop, health is live/unready. Operations
retain individual deadlines; there is no independent total drain deadline.

Metrics bind all interfaces on separate port 9000 by default. `metrics: null` or
`metrics: { enabled: false }` disables them. `metrics.port` is 1–65535 and must differ
from an enabled health port. `metrics.listen_address` accepts numeric IPv4 or
unbracketed IPv6, for example `metrics: { port: 9000, listen_address: "127.0.0.1" }`.
Omission keeps all-interface behavior; explicit addresses use neither DNS nor IPv4
fallback. Ports, whitespace and IPv6 scopes in the address are rejected, even when
metrics are disabled. Overrides are `APP_METRICS__PORT`, `APP_METRICS__ENABLED`,
`APP_METRICS__LISTEN_ADDRESS`. `/health` returns `OK`; other paths, including
`/metrics`, return Prometheus text. HEAD omits the body. Binding precedes pipeline
startup; success, error and cancellation close listeners/connections. Health/metrics
bind failure prevents source migration. Metrics have no authentication; deployment
controls reachability. See [metrics](METRICS.md). CLI shares one control across
migrations, pipeline, schema/state stores, timing SQL by two fixed pool categories.
Connection/authentication and state mutex waits are excluded; metrics do not change
state ownership or durability.

Optional error reporting defaults off:

```json
{
  "sentry": {"dsn": "https://public-key@sentry.example/42"},
  "supabase": {
    "project_ref": "project-ref",
    "api_url": "https://api.example",
    "api_key": "replace-through-environment"
  }
}
```

Inject secrets with `APP_SENTRY__DSN` and `APP_SUPABASE__API_KEY`. Without api_url,
Supabase supplies only a project label, not notifications; configured URLs require
a nonempty api_key. Terminal table errors persist before reporting; TimedRetry and
unchanged terminal states are suppressed. Final CLI failure is reported without
changing its exit code. Requests allow 10s and 64KiB responses. Failure emits fixed
diagnostics and does not replace state-write results. Reporting is awaited within
the scope; both services can add about 20s network delay per error. The API includes
`/emails`; real endpoints notify their configured recipients.

Known connection/destination/reporting secret literals are replaced in error text,
truncated to 16KiB. This is not arbitrary secret discovery. Notification dedup uses
SHA-256 of redacted text, not Rust DefaultHasher key bytes. DSN/URLs accept ASCII
DNS/IPv4, ports and path prefixes, without query/fragment; IPv6/IDNA are unsupported.
`APP_ENVIRONMENT` is dev/staging/prod and `APP_VERSION` optional reporting metadata;
both are reserved fields. Panic/backtrace, rate-limit caches and real-service
acceptance remain incomplete. See reporting [validation](VALIDATION.md).

`supabase.configcat_sdk_key` is optional; omission/null disables it. A configured
key owns a bounded ConfigCat poller: default global CDN, automatic 60s polling,
30s whole-round timeout. Two fixed SDK-key forms are supported; custom base URLs
also permit nonempty `configcat-proxy/` keys. Length is checked as UTF-8 bytes and
errors never echo the key. `project_ref` remains the default user identity and is
not sent to CDN. `--check` validates configuration without polling.

MoonCDC additionally supplies `supabase.configcat_base_url`,
`configcat_poll_interval_ms`, `configcat_http_timeout_ms` for proxies/controlled
fixtures. They are not pinned ETL fields, require an SDK key, and use positive Int
intervals/timeouts. `APP_SUPABASE__CONFIGCAT_*` overrides receive identical validation.
GET uses V6 paths and ETag; 200 caches, 304 retains ETag, network/status/parse failures
retain old JSON, and 403/404 only refresh existing-cache timestamps. V6 preference
redirects are limited to three; each response to 1MiB. Polls do not accumulate or
change replication configuration/acknowledgement. All exit paths await scope HTTP close.

The pinned ETL entrypoint only initializes/holds its client, without evaluating
flags. This adapter validates V6 root/preferences and retains original JSON. Full
SDK rules/evaluation/external cache, HTTP3xx redirects, IPv6/IDNA endpoints and real
CDN acceptance are unimplemented. Whole-round timeout is stricter than upstream
per-request timeout. See ConfigCat [validation](VALIDATION.md); this is not full C8 acceptance.

Full upstream file syntax, multiple destinations and a total stop deadline remain
unimplemented. See [configuration mapping](PIPELINE_CONFIG_MAPPING.md) for field
mapping, library defaults and local limits, and [validation](VALIDATION.md) for
commands/evidence, pinned YAML differences, limits and rejected inputs.
