# Changelog

## 0.1.0 — Unreleased

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
