# Development

MoonCDC is independently implemented from the PostgreSQL wire and pgoutput
specifications. It is a native MoonBit library under Apache-2.0.

## Invariants and acceptance

1. A transaction becomes visible only after its Commit message. Rollbacks never
   enter the committed delivery queue.
2. The durable position advances only across a contiguous prefix of acknowledged
   deliveries. Receiving bytes is not business persistence.
3. Every resume validates system identity, database, slot and publication, and
   rejects a position no longer available from the slot.
4. Frames are length-checked before body allocation. Transactions and outstanding
   deliveries have explicit limits. An overrun fails without acknowledging data.
5. Stable identity uses source identity and commit position, not a process-local
   sequence or the transaction ID alone.

## Work sequence

- Verify native TCP, SCRAM authentication, `IDENTIFY_SYSTEM` and `COPY BOTH`
  against an isolated real PostgreSQL before finalizing the public API.
- Implement bounded pgoutput decoding, committed delivery and durable checkpoints.
- Test actual failure boundaries, PostgreSQL 18 and 17, and executable examples.
- Generate interfaces and docs, inspect package contents, test a separate consumer.
- Record source commit, toolchain, server versions and exact validation commands.

Use small local Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`).
Each implementation commit should describe one verifiable behavior. Stage explicit
paths. Push and Mooncakes publication are distinct release operations.

Automation is written as `.mbtx`; run it with `moon run tools/<name>.mbtx`.
Never use shared or production PostgreSQL for this repository's destructive tests.
