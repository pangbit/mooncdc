# Supported feature map

This map describes the 0.1.0 candidate's implementation scope, not a completion
percentage or an assertion that every platform/fault combination has passed.
See [release scope](RELEASE_SCOPE.md), [API](API.md) and
[version-bound validation](VALIDATION.md).

| Capability | Library / CLI relationship | Limits |
| --- | --- | --- |
| PostgreSQL snapshot and pgoutput v1 CDC | Shared pipeline core | PG17/18 fixtures; at-least-once delivery |
| Durable checkpoint and recovery | Shared state/runtime contracts | Contiguous durable prefix; stale identity/owner rejected |
| ClickHouse RowBinary and DDL | Same destination implementation | ReplacingMergeTree and MergeTree; documented type/DDL subset |
| Parallel table COPY and WAL handover | Shared table state machine | Selection defaults/configuration may differ; no silent omission |
| Timeline/slot safety | Shared ancestry and recreation safeguards | Not automatic HA election or cross-system exactly-once |
| Health, activity, metrics and pressure control | Public APIs, CLI adapters | Activity/readiness do not authorize durability |
| JSON/YAML and environment configuration | CLI adapter | See configuration mapping and strict rejection tests |
| Standalone pgoutput v2 | Library subscription compatibility | Does not expand v1 pipeline promises |

BigQuery, Iceberg and Snowflake implementations and cloud validation are outside
this release tree. Optional reporting and ConfigCat cover the documented local
contracts; full service/SDK compatibility is not claimed. Historical stage
reports and old test totals do not certify the final release commit.
