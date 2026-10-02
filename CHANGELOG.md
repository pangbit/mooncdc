# Changelog

## 0.1.0 — Unreleased

- Integrate pluggable StateStore ownership and durable records into pipelines and
  subscriptions, with file/PostgreSQL backends and destination metadata sessions.
  Retain source-bound checkpoints and reject writes from closed/stale store owners.

- Persist table-copy failures with bounded timed, manual and disabled retry policies;
  keep healthy tables applying while failed tables await a fresh snapshot/reset.
  Expose live manual retry through ApplyControl and retain global durability failures.

- Preserve arbitrary-precision JSON numbers and equality, including underflow values,
  decimal scale and exponents; recognize generic built-in PostgreSQL arrays.
  Expand official ETL codec differential coverage to 558 vectors.

- Add schema projection/identity masks, typed row decoding, canonical schema JSON
  roundtrips and ordered column DDL plans with rename-cycle handling. Verify plans
  against 77 vectors evaluated by the pinned official ETL planner.

- Apply ready tables while bounded table workers copy and replay private WAL;
  persist handover cutoffs only after durability and recover unfinished copies even
  when the main checkpoint has advanced. Scope split truncate IDs by table.

- Add a reproducible 320-vector differential check against the pinned official ETL
  codec; align numeric normalization, float4 rounding/overflow, OID signs and leap seconds.

- Add typed PostgreSQL text cells, exact numeric text, temporal infinities/BC/24h,
  nullable arrays and stable session output formats for snapshots and replication.

- Add Supabase ETL DDL parsing, ordered schema versions, publication-scoped handling,
  full snapshot schemas and an atomic schema store with replay checks and retention bounds.

- Add a persistent per-table pipeline with concurrent independent snapshots, failed-copy
  restart, table cutoffs, dynamic WAL-driven discovery and opt-in bounded backpressure.

- Exclude index INCLUDE columns from snapshot replica-identity key flags.
- Add custom Destination contracts with Accepted/Durable writes, snapshot attempt IDs,
  cumulative/idle/stop flushes and durable snapshot checkpoints for later Resume.
- Add opt-in logical messages with raw payloads, commit-bound transactional delivery
  and explicit nontransactional handling; retain negotiation across reconnects.
- Add bounded, publication-aware consistent snapshots and source-bound `AfterSnapshot`
  handoff; retain failed bootstrap slots for explicit recovery.
- Add opt-in pgoutput binary transfer and raw Binary values; preserve negotiation on reconnect.

- Retry transient failures throughout reconnect startup, identification and COPY BOTH;
  close failed attempts and surface the latest error when the retry budget is exhausted.
- Add protocol fault regressions and real SIGKILL tests at five internal checkpoint boundaries.
- Add a strict registry dry-run compatibility check for the observed backend exit-code defect.

- Native PostgreSQL pgoutput v1 subscriptions with SCRAM-SHA-256 and verified TLS.
- Immutable committed transactions, table/type metadata and replay-stable data event IDs.
- Contiguous explicit acknowledgements and atomic source-bound checkpoints.
- Bounded frames, transactions and outstanding deliveries; heartbeats and bounded reconnects.
- Index, cache and durable event journal examples; PostgreSQL 18/17 integration tests.
